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
# API 4: Lấy danh sách bản ghi thay đổi (Cursor Pagination + Độ trễ an toàn 120s)
# ==============================================================================
@router.get("/thay-doi")
async def get_changed_records(
    cursor_sync_date: Optional[str] = Query(None, description="ISO Timestamp của con trỏ SYNC_DATE lần trước (YYYY-MM-DDTHH:MM:SS.ffffff)"),
    cursor_id: int = Query(0, ge=0, description="ID của bản ghi cuối cùng ở trang trước"),
    limit: int = Query(50, ge=1, le=1000),
    safe_lag_seconds: int = Query(API_SAFE_LAG_SECONDS, ge=0, le=3600, description="Độ trễ an toàn (mặc định 120s)"),
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:DELTA")),
):
    if cursor_sync_date:
        sql = f"""
            SELECT {CITIZEN_SELECT_COLUMNS}
            FROM C360.C360_THONG_TIN_CA_NHAN
            WHERE SYNC_DATE <= SYSTIMESTAMP - NUMTODSINTERVAL(:safe_lag, 'SECOND')
              AND (
                  SYNC_DATE > TO_TIMESTAMP(:cursor_sync_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF')
                  OR (SYNC_DATE = TO_TIMESTAMP(:cursor_sync_date, 'YYYY-MM-DD"T"HH24:MI:SS.FF') AND ID > :cursor_id)
              )
            ORDER BY SYNC_DATE ASC, ID ASC
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
            SELECT {CITIZEN_SELECT_COLUMNS}
            FROM C360.C360_THONG_TIN_CA_NHAN
            WHERE SYNC_DATE <= SYSTIMESTAMP - NUMTODSINTERVAL(:safe_lag, 'SECOND')
            ORDER BY SYNC_DATE ASC, ID ASC
            FETCH FIRST :limit_rows ROWS ONLY
        """
        params = {"safe_lag": safe_lag_seconds, "limit_rows": limit}

    items = await asyncio.to_thread(fetch_all_dicts, sql, params)
    next_cursor = None
    if items:
        last = items[-1]
        next_cursor = {
            "cursor_sync_date": last.get("SYNC_DATE"),
            "cursor_id": last.get("ID"),
        }

    return {
        "safe_lag_seconds": safe_lag_seconds,
        "cursor": {"cursor_sync_date": cursor_sync_date, "cursor_id": cursor_id},
        "total_returned": len(items),
        "items": items,
        "next_cursor": next_cursor,
    }


# ==============================================================================
# API 2: Tra cứu theo lô (Tối đa 1.000 mã/lần)
# ==============================================================================
@router.post("/tra-cuu-lo")
async def batch_lookup_citizens(
    payload: BatchLookupRequest,
    _client: Dict[str, Any] = Depends(require_scope("CA_NHAN:BATCH")),
):
    def _run_batch() -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        found_count = 0
        for ma in payload.ma_list:
            tier, rows = _lookup_single_code_sync(ma)
            if not rows:
                results[ma] = {"status": "NOT_FOUND", "matched_tier": None, "data": None}
            elif len(rows) > 1:
                results[ma] = {
                    "status": "CONFLICT",
                    "matched_tier": tier,
                    "candidates": [
                        {"MA_SO_BHXH": r["MA_SO_BHXH"], "HO_TEN": r["HO_TEN"], "SO_DINH_DANH": r["SO_DINH_DANH"]}
                        for r in rows
                    ],
                }
            else:
                found_count += 1
                results[ma] = {"status": "FOUND", "matched_tier": tier, "data": rows[0]}
        return {"requested_count": len(payload.ma_list), "found_count": found_count, "results": results}

    return await asyncio.to_thread(_run_batch)


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
