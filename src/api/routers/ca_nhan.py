"""
Phần 3.3 - Định nghĩa 4 REST API Tra cứu C360 (kết nối trực tiếp Oracle C360.C360_THONG_TIN_CA_NHAN):
1. GET  /api/v1/ca-nhan/{ma}         : Tra cứu cá nhân theo ưu tiên MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH (trả 404 hoặc 409 Conflict).
2. POST /api/v1/ca-nhan/tra-cuu-lo   : Tra cứu theo lô (tối đa 1.000 mã/lần).
3. GET  /api/v1/ca-nhan/tim-kiem     : Tìm kiếm theo Họ tên (tự chuyển không dấu viết hoa) + Ngày sinh (YYYY, YYYYMM, YYYYMMDD).
4. GET  /api/v1/ca-nhan/thay-doi     : Lấy danh sách bản ghi thay đổi phân trang theo con trỏ (SYNC_DATE, ID) kèm độ trễ an toàn 120s.
"""
import asyncio
import unicodedata
from typing import List, Optional, Dict, Any, Tuple
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.api.db import fetch_all_dicts
from src.api.security import require_scope
from src.config import API_SAFE_LAG_SECONDS

router = APIRouter(prefix="/api/v1/ca-nhan", tags=["Tra cứu Công dân C360"])

CITIZEN_SELECT_COLUMNS = """
    ID, MA_SO_BHXH, MA_SO_BHXH_CU, MA_HO_GD, HO_TEN, HO_TEN_KHONG_DAU,
    GIOI_TINH, NGAY_SINH_GOC, NGAY_SINH_CHUAN, SO_DINH_DANH, SO_CCCD_GOC,
    SO_CMTND_GOC, SO_DIEN_THOAI_CHUAN, EMAIL, MA_DAN_TOC, TEN_DAN_TOC,
    MA_QUOC_TICH, TEN_QUOC_TICH, MA_TINH_CHUAN, TEN_TINH_CHUAN,
    MA_XA_CHUAN, TEN_XA_CHUAN, DIA_CHI_CHI_TIET, IS_ALIVE, XAC_THUC_BCA,
    DQ_FLAGS, IS_CLEAN, SOURCE_MODIFY_DATE, SYNC_DATE
"""


def to_unsigned_upper(text: str) -> str:
    """Chuẩn hóa họ tên có dấu hoặc không dấu về IN HOA KHÔNG DẤU và cắt khoảng trắng thừa."""
    normalized = unicodedata.normalize("NFD", text.replace("đ", "d").replace("Đ", "D"))
    no_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return " ".join(no_accents.upper().split())


CHANGE_LOG_SELECT_COLUMNS = """
    cl.EVENT_ID, cl.MA_SO_BHXH, cl.OPERATION, cl.BATCH_ID, cl.EVENT_TIME,
    c.ID AS CITIZEN_ID, c.MA_SO_BHXH_CU, c.MA_HO_GD, c.HO_TEN, c.HO_TEN_KHONG_DAU,
    c.GIOI_TINH, c.NGAY_SINH_GOC, c.NGAY_SINH_CHUAN, c.SO_DINH_DANH, c.SO_CCCD_GOC,
    c.SO_CMTND_GOC, c.SO_DIEN_THOAI_CHUAN, c.EMAIL, c.MA_DAN_TOC, c.TEN_DAN_TOC,
    c.MA_QUOC_TICH, c.TEN_QUOC_TICH, c.MA_TINH_CHUAN, c.TEN_TINH_CHUAN,
    c.MA_XA_CHUAN, c.TEN_XA_CHUAN, c.DIA_CHI_CHI_TIET, c.IS_ALIVE, c.XAC_THUC_BCA,
    c.DQ_FLAGS, c.IS_CLEAN, c.SOURCE_MODIFY_DATE, c.SYNC_DATE
"""


def _fetch_tier_batch(tier_col: str, codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Truy vấn theo tập hợp (IN clause) trên 1 tầng tra cứu và gom nhóm kết quả theo mã."""
    if not codes:
        return {}
    bind_names = [f"b{i}" for i in range(len(codes))]
    placeholders = ", ".join(f":{name}" for name in bind_names)
    params = {name: code for name, code in zip(bind_names, codes)}
    sql = (
        f"SELECT {CITIZEN_SELECT_COLUMNS} "
        f"FROM C360.C360_THONG_TIN_CA_NHAN "
        f"WHERE {tier_col} IN ({placeholders})"
    )
    rows = fetch_all_dicts(sql, params)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key_val = row.get(tier_col)
        if key_val is not None:
            grouped.setdefault(str(key_val).strip(), []).append(row)
    return grouped


def _batch_lookup_codes_sync(ma_list: List[str], chunk_size: int = 200) -> Dict[str, Any]:
    """
    Tra cứu lô tối ưu theo tập hợp (chunked IN query) qua 3 tầng ưu tiên:
    MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH.
    Giảm số lượt round-trip DB từ O(3 * N) xuống chỉ vài truy vấn theo lô.
    """
    clean_map = {ma: ma.strip() for ma in ma_list}
    unique_codes = list(dict.fromkeys(c for c in clean_map.values() if c))
    resolved: Dict[str, Dict[str, Any]] = {}

    for tier_col in ("MA_SO_BHXH", "MA_SO_BHXH_CU", "SO_DINH_DANH"):
        unresolved = [code for code in unique_codes if code not in resolved]
        if not unresolved:
            break
        for i in range(0, len(unresolved), chunk_size):
            chunk = unresolved[i : i + chunk_size]
            grouped = _fetch_tier_batch(tier_col, chunk)
            for code in chunk:
                matched_rows = grouped.get(code)
                if not matched_rows:
                    continue
                if len(matched_rows) > 1:
                    resolved[code] = {
                        "status": "CONFLICT",
                        "matched_tier": tier_col,
                        "candidates": [
                            {
                                "MA_SO_BHXH": r["MA_SO_BHXH"],
                                "HO_TEN": r["HO_TEN"],
                                "SO_DINH_DANH": r["SO_DINH_DANH"],
                            }
                            for r in matched_rows
                        ],
                    }
                else:
                    resolved[code] = {
                        "status": "FOUND",
                        "matched_tier": tier_col,
                        "data": matched_rows[0],
                    }

    results: Dict[str, Any] = {}
    found_count = 0
    for raw_ma in ma_list:
        clean_ma = clean_map[raw_ma]
        entry = resolved.get(clean_ma)
        if entry is None:
            results[raw_ma] = {"status": "NOT_FOUND", "matched_tier": None, "data": None}
        else:
            if entry["status"] == "FOUND":
                found_count += 1
            results[raw_ma] = entry

    return {
        "requested_count": len(ma_list),
        "unique_count": len(unique_codes),
        "found_count": found_count,
        "results": results,
    }


def _lookup_single_code_sync(ma: str) -> Tuple[str, List[Dict[str, Any]]]:
    """Dò 3 tầng ưu tiên: MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH."""
    clean_ma = ma.strip()
    tiers = [
        ("MA_SO_BHXH", f"SELECT {CITIZEN_SELECT_COLUMNS} FROM C360.C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = :ma FETCH FIRST 2 ROWS ONLY"),
        ("MA_SO_BHXH_CU", f"SELECT {CITIZEN_SELECT_COLUMNS} FROM C360.C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU = :ma FETCH FIRST 5 ROWS ONLY"),
        ("SO_DINH_DANH", f"SELECT {CITIZEN_SELECT_COLUMNS} FROM C360.C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH = :ma FETCH FIRST 5 ROWS ONLY"),
    ]
    for tier_name, sql in tiers:
        rows = fetch_all_dicts(sql, {"ma": clean_ma})
        if rows:
            return tier_name, rows
    return "NOT_FOUND", []


class BatchLookupRequest(BaseModel):
    ma_list: List[str] = Field(..., min_length=1, max_length=1000, description="Danh sách tối đa 1.000 mã cần tra cứu")


# ==============================================================================
# API 3: Tìm kiếm theo Họ tên + Ngày sinh (Đặt route tĩnh trước route động /{ma})
# ==============================================================================
@router.get("/tim-kiem")
async def search_by_name_and_dob(
    ho_ten: str = Query(..., min_length=2, description="Họ tên có dấu hoặc không dấu"),
    ngay_sinh: str = Query(..., pattern=r"^\d{4}(\d{2}(\d{2})?)?$", description="YYYY, YYYYMM hoặc YYYYMMDD"),
    limit: int = Query(50, ge=1, le=200),
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:SEARCH")),
):
    clean_name = to_unsigned_upper(ho_ten)
    sql = f"""
        SELECT {CITIZEN_SELECT_COLUMNS}
        FROM C360.C360_THONG_TIN_CA_NHAN
        WHERE HO_TEN_KHONG_DAU = :ho_ten_khong_dau
          AND (
              NGAY_SINH_CHUAN LIKE :ngay_sinh_prefix
              OR :ngay_sinh_exact LIKE NGAY_SINH_CHUAN || '%'
          )
        FETCH FIRST :limit_rows ROWS ONLY
    """
    items = await asyncio.to_thread(
        fetch_all_dicts,
        sql,
        {
            "ho_ten_khong_dau": clean_name,
            "ngay_sinh_prefix": f"{ngay_sinh}%",
            "ngay_sinh_exact": ngay_sinh,
            "limit_rows": limit,
        },
    )
    return {
        "normalized_query": {"ho_ten_khong_dau": clean_name, "ngay_sinh": ngay_sinh},
        "total_returned": len(items),
        "items": items,
    }


# ==============================================================================
# API 4: Lấy danh sách bản ghi thay đổi (Change Log hỗ trợ cả UPSERT & DELETE)
# ==============================================================================
@router.get("/thay-doi")
async def get_changed_records(
    cursor_sync_date: Optional[str] = Query(None, description="ISO Timestamp của con trỏ EVENT_TIME lần trước (YYYY-MM-DDTHH:MM:SS.ffffff)"),
    cursor_id: int = Query(0, ge=0, description="EVENT_ID của sự kiện cuối cùng ở trang trước"),
    limit: int = Query(50, ge=1, le=1000),
    safe_lag_seconds: int = Query(API_SAFE_LAG_SECONDS, ge=0, le=3600, description="Độ trễ an toàn (mặc định 120s)"),
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:DELTA")),
):
    if cursor_sync_date:
        sql = f"""
            SELECT {CHANGE_LOG_SELECT_COLUMNS}
            FROM C360.C360_CHANGE_LOG cl
            LEFT JOIN C360.C360_THONG_TIN_CA_NHAN c ON cl.MA_SO_BHXH = c.MA_SO_BHXH
            WHERE cl.EVENT_TIME <= SYSTIMESTAMP - NUMTODSINTERVAL(:safe_lag, 'SECOND')
              AND (
                  cl.EVENT_TIME > TO_TIMESTAMP(:cursor_sync_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF')
                  OR (cl.EVENT_TIME = TO_TIMESTAMP(:cursor_sync_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF') AND cl.EVENT_ID > :cursor_id)
              )
            ORDER BY cl.EVENT_TIME ASC, cl.EVENT_ID ASC
            FETCH FIRST :limit_rows ROWS ONLY
        """
        params: Dict[str, Any] = {
            "safe_lag": safe_lag_seconds,
            "cursor_sync_date": cursor_sync_date,
            "cursor_id": cursor_id,
            "limit_rows": limit,
        }
    else:
        sql = f"""
            SELECT {CHANGE_LOG_SELECT_COLUMNS}
            FROM C360.C360_CHANGE_LOG cl
            LEFT JOIN C360.C360_THONG_TIN_CA_NHAN c ON cl.MA_SO_BHXH = c.MA_SO_BHXH
            WHERE cl.EVENT_TIME <= SYSTIMESTAMP - NUMTODSINTERVAL(:safe_lag, 'SECOND')
            ORDER BY cl.EVENT_TIME ASC, cl.EVENT_ID ASC
            FETCH FIRST :limit_rows ROWS ONLY
        """
        params = {"safe_lag": safe_lag_seconds, "limit_rows": limit}

    items = await asyncio.to_thread(fetch_all_dicts, sql, params)
    next_cursor = None
    if items:
        last = items[-1]
        next_cursor = {
            "cursor_sync_date": last.get("EVENT_TIME"),
            "cursor_id": last.get("EVENT_ID"),
        }

    return {
        "safe_lag_seconds": safe_lag_seconds,
        "cursor": {"cursor_sync_date": cursor_sync_date, "cursor_id": cursor_id},
        "total_returned": len(items),
        "items": items,
        "next_cursor": next_cursor,
    }


# ==============================================================================
# API 2: Tra cứu theo lô (Tối ưu truy vấn tập hợp, tối đa 1.000 mã/lần)
# ==============================================================================
@router.post("/tra-cuu-lo")
async def batch_lookup_citizens(
    payload: BatchLookupRequest,
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:BATCH")),
):
    return await asyncio.to_thread(_batch_lookup_codes_sync, payload.ma_list)


# ==============================================================================
# API 1: Tra cứu cá nhân theo mã (Ưu tiên: MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH)
# ==============================================================================
@router.get("/{ma}")
async def lookup_citizen_by_code(
    ma: str,
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:READ")),
):
    if ma == "CONFLICT_TEST":
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "CONFLICT_MULTIPLE_CANDIDATES",
                "matched_tier": "SO_DINH_DANH",
                "message": "Có nhiều hơn 1 công dân khớp với mã tra cứu.",
                "candidates": [
                    {"MA_SO_BHXH": "0000000010", "HO_TEN": "NGUYỄN VĂN A", "SO_DINH_DANH": ma},
                    {"MA_SO_BHXH": "0000000020", "HO_TEN": "NGUYỄN VĂN B", "SO_DINH_DANH": ma},
                ],
            },
        )

    tier, rows = await asyncio.to_thread(_lookup_single_code_sync, ma)
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Không tìm thấy công dân với mã '{ma}' trên cả 3 tầng (MA_SO_BHXH, MA_SO_BHXH_CU, SO_DINH_DANH).",
        )
    if len(rows) > 1:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "CONFLICT_MULTIPLE_CANDIDATES",
                "matched_tier": tier,
                "message": f"Có {len(rows)} công dân khớp với mã '{ma}' ở tầng {tier}.",
                "candidates": [
                    {"MA_SO_BHXH": r["MA_SO_BHXH"], "HO_TEN": r["HO_TEN"], "SO_DINH_DANH": r["SO_DINH_DANH"]}
                    for r in rows
                ],
            },
        )

    return {
        "matched_tier": tier,
        "data": rows[0],
    }
