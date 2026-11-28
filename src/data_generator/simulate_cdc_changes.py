"""
Phần 1.1 - Script 2: Đẩy dữ liệu thay đổi vào bảng nguồn (Change / Delta Generator)
Hỗ trợ cả hai đích:
  - `--target local` (mặc định): Oracle Local (`SOURCE_DB`)
  - `--target cloud`: Oracle Cloud Autonomous DB (`FULLLOAD_DB` qua Oracle Wallet)

Chế độ hoạt động:
  1. Đọc danh mục thật (`VSS_DM_DBHC`, `VSS_DM_DANTOC`, `VSS_DM_QUOC_TICH`) và mẫu công dân thật
     từ bảng nguồn `VSS_HO_GD_NHANKHAU` (có fallback an toàn nếu thiếu danh mục).
  2. Sinh đủ 8 nhóm kịch bản biến động CDC với nhiều biến thể thực tế, khác biệt nhau và log rõ
     giá trị trước/sau:
     - 1A: INSERT công dân mới (sạch, thiếu CCCD, lỗi ngày sinh/SĐT, lỗi địa bàn/danh mục, chờ duyệt, dân tộc/quốc tịch/sinh tử).
     - 1B: INSERT hộ gia đình mới cho công dân đang có (chuyển tỉnh/xã khác hoặc cùng tỉnh khác xã).
     - 2 : UPDATE sửa dữ liệu bẩn -> sạch theo đúng các lỗi thực tế của bản ghi (hoặc sửa từng nhóm trường).
     - 3A: UPDATE chuyển trạng thái sang bị lọc (TRANG_THAI_DUYET=4, TYPEID='GG'/'GT'/'GM', hoặc trả về chờ duyệt=1).
     - 3B: UPDATE duyệt hồ sơ chờ duyệt sang hợp lệ (TRANG_THAI_DUYET=2 hoặc 0, TYPEID=NULL).
     - 4 : UPDATE đổi mã công dân (MA_CD -> mã mới, lưu mã cũ vào MA_CD_OLD).
     - 5A: DELETE dòng hộ mới nhất của công dân có >= 2 hộ -> C360 tự động lùi về hộ cũ.
     - 5B: DELETE toàn bộ bản ghi của một công dân nằm trong tập sample/simulator (bảo vệ không xóa bừa ngoài tập mẫu).
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import oracledb

from src.config import FULLLOAD_DB, SOURCE_DB
from src.oracle_wallet import resolve_tns_alias, resolve_wallet_dir, validate_identifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc_simulator")

VALID_TARGETS = ("local", "cloud")
FILTERED_TYPEIDS = frozenset({"GT", "GM", "GG", "GK"})
FILTERED_DUYET_STATES = frozenset({1, 3, 4})
CCCD_PATTERN = re.compile(r"^\d{12}$")
PHONE_PATTERN = re.compile(r"^(0\d{9,10}|840\d{9,10}|84[1-9]\d{8,9}|[1-9]\d{8})$")
YYYY_PATTERN = re.compile(r"^(19|20)\d{2}$")
YYYYMM_PATTERN = re.compile(r"^(19|20)\d{2}(0[1-9]|1[0-2])$")
YYYYMMDD_PATTERN = re.compile(r"^(19|20)\d{6}$")

_DEFAULT_LAST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Đỗ", "Bùi"]
_DEFAULT_MID_NAMES = ["Văn", "Thị", "Minh", "Hoàng", "Ngọc", "Thanh", "Đức", "Quang"]
_DEFAULT_FIRST_NAMES = ["An", "Bình", "Cường", "Dũng", "Hà", "Hải", "Hùng", "Lan", "Linh", "Tuấn"]


@dataclass(frozen=True)
class TargetConnectionSpec:
    target: str
    schema: str
    table_name: str
    user: str
    password: str
    dsn: str
    wallet_dir: Optional[str] = None

    @property
    def full_table(self) -> str:
        return f"{self.schema}.{self.table_name}"

    @property
    def tag(self) -> str:
        return self.target.upper()


@dataclass(frozen=True)
class LocationItem:
    ma_tinh: str
    ma_xa: str
    ten_tinh: str = ""
    ten_xa: str = ""


@dataclass(frozen=True)
class DanTocItem:
    id: int
    ma: str
    ten: str


@dataclass(frozen=True)
class QuocTichItem:
    id: int
    ma: str
    ten: str


@dataclass
class VssReferenceContext:
    locations: List[LocationItem]
    dan_tocs: List[DanTocItem]
    quoc_tiches: List[QuocTichItem]
    last_names: List[str] = field(default_factory=lambda: list(_DEFAULT_LAST_NAMES))
    mid_names: List[str] = field(default_factory=lambda: list(_DEFAULT_MID_NAMES))
    first_names: List[str] = field(default_factory=lambda: list(_DEFAULT_FIRST_NAMES))
    source_summary: str = "fallback"

    @property
    def valid_location_pairs(self) -> set[Tuple[str, str]]:
        return {(loc.ma_tinh, loc.ma_xa) for loc in self.locations}

    @property
    def valid_dan_toc_ids(self) -> set[int]:
        return {dt.id for dt in self.dan_tocs}

    @property
    def valid_quoc_tich_codes(self) -> set[str]:
        codes: set[str] = set()
        for qt in self.quoc_tiches:
            codes.add(qt.ma.upper())
            codes.add(str(qt.id))
        return codes


@dataclass
class CitizenHousehold:
    id: str
    ma_ho_gd: str
    ma_tinh: Optional[str] = None
    ma_xa: Optional[str] = None
    modify_date: Any = None


@dataclass
class CitizenRecord:
    ma_cd: str
    ho_ten: str
    gioi_tinh: str
    ngay_sinh: Optional[int]
    so_cccd: Optional[str]
    so_cmtnd: Optional[str]
    phone_number: Optional[str]
    ma_tinh: Optional[str]
    ma_xa: Optional[str]
    quoc_tich: Optional[str]
    dm_dan_toc_id: Optional[int]
    is_alive: str = "1"
    typeid: Optional[str] = None
    trang_thai_duyet: Optional[int] = 2
    households: List[CitizenHousehold] = field(default_factory=list)
    from_vss: bool = True

    @property
    def is_valid(self) -> bool:
        typeid_norm = (self.typeid or "ZZ").strip().upper()
        duyet_norm = 0 if self.trang_thai_duyet is None else int(self.trang_thai_duyet)
        return typeid_norm not in FILTERED_TYPEIDS and duyet_norm not in FILTERED_DUYET_STATES

    @property
    def is_pending(self) -> bool:
        return self.trang_thai_duyet == 1

    @property
    def source_label(self) -> str:
        return "VSS_SAMPLE" if self.from_vss else "SIMULATED"


@dataclass(frozen=True)
class CdcScenario:
    key: str
    name: str
    description: str
    sql: str


_DEFAULT_LOCATIONS = [
    LocationItem("01TTT", "00004", "Thành phố Hà Nội", "Phường Ba Đình"),
    LocationItem("01TTT", "00008", "Thành phố Hà Nội", "Phường Ngọc Hà"),
    LocationItem("33TTT", "12457", "Tỉnh Hưng Yên", "Xã Phố Hiến"),
    LocationItem("79TTT", "26734", "Thành phố Hồ Chí Minh", "Phường Sài Gòn"),
]
_DEFAULT_DAN_TOCS = [
    DanTocItem(1, "01", "Kinh"),
    DanTocItem(2, "02", "Tày"),
    DanTocItem(3, "03", "Thái"),
]
_DEFAULT_QUOC_TICHES = [
    QuocTichItem(233, "VN", "Việt Nam"),
    QuocTichItem(100, "LA", "Lào"),
    QuocTichItem(101, "KR", "Hàn Quốc"),
    QuocTichItem(102, "JP", "Nhật Bản"),
]


def to_unsigned_upper(text: Optional[str]) -> str:
    """Chuẩn hóa chuỗi tiếng Việt sang IN HOA không dấu, gọn khoảng trắng."""
    if not text:
        return ""
    cleaned = " ".join(text.strip().split())
    cleaned = cleaned.replace("Đ", "D").replace("đ", "d")
    normalized = unicodedata.normalize("NFD", cleaned)
    ascii_only = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return ascii_only.upper()


def build_target_spec(target: str = "local") -> TargetConnectionSpec:
    """Xây dựng thông số kết nối tới Oracle Local (`SOURCE_DB`) hoặc Oracle Cloud (`FULLLOAD_DB`)."""
    normalized = (target or "local").strip().lower()
    if normalized not in VALID_TARGETS:
        raise ValueError(f"Unsupported target {target!r}. Expected one of: {VALID_TARGETS}")

    if normalized == "local":
        schema = validate_identifier(SOURCE_DB.schema or "VSS", "SOURCE_DB_SCHEMA")
        return TargetConnectionSpec(
            target="local",
            schema=schema,
            table_name="VSS_HO_GD_NHANKHAU",
            user=SOURCE_DB.user,
            password=SOURCE_DB.password,
            dsn=SOURCE_DB.dsn,
            wallet_dir=None,
        )

    if not FULLLOAD_DB.user or not FULLLOAD_DB.password:
        raise ValueError("Missing FULLLOAD_DB_USER or FULLLOAD_DB_PASSWORD in .env for --target cloud")

    wallet_dir = resolve_wallet_dir(FULLLOAD_DB.wallet_location)
    tns_alias = resolve_tns_alias(wallet_dir / "tnsnames.ora", FULLLOAD_DB.tns_alias)
    schema = validate_identifier(FULLLOAD_DB.schema or "VSS", "FULLLOAD_DB_SCHEMA")
    table_name = validate_identifier(FULLLOAD_DB.table or "VSS_HO_GD_NHANKHAU", "FULLLOAD_DB_TABLE")
    return TargetConnectionSpec(
        target="cloud",
        schema=schema,
        table_name=table_name,
        user=FULLLOAD_DB.user,
        password=FULLLOAD_DB.password,
        dsn=tns_alias,
        wallet_dir=str(wallet_dir),
    )


def open_oracle_connection(spec: TargetConnectionSpec) -> oracledb.Connection:
    """Mở kết nối Oracle tới Local hoặc Autonomous Cloud (kèm Wallet khi target='cloud')."""
    connect_kwargs: Dict[str, Any] = {
        "user": spec.user,
        "password": spec.password,
        "dsn": spec.dsn,
    }
    if spec.wallet_dir:
        connect_kwargs["config_dir"] = spec.wallet_dir
        connect_kwargs["wallet_location"] = spec.wallet_dir
        wallet_password = os.getenv("FULLLOAD_DB_WALLET_PASSWORD", "").strip()
        if wallet_password:
            connect_kwargs["wallet_password"] = wallet_password
    return oracledb.connect(**connect_kwargs)


def _is_valid_ngay_sinh(value: Optional[int]) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if len(text) < 4 or not text[:4].isdigit():
        return False
    year = int(text[:4])
    if year < 1900 or year > datetime.now().year:
        return False
    if YYYY_PATTERN.fullmatch(text) or YYYYMM_PATTERN.fullmatch(text):
        return True
    if text.endswith("0000") and len(text) == 8:
        return True
    if len(text) == 8 and text.endswith("00") and YYYYMM_PATTERN.fullmatch(text[:6]):
        return True
    if YYYYMMDD_PATTERN.fullmatch(text):
        try:
            datetime.strptime(text, "%Y%m%d")
            return True
        except ValueError:
            return False
    return False


def detect_citizen_dq_defects(record: CitizenRecord, ref_ctx: VssReferenceContext) -> List[str]:
    """Phát hiện các lỗi chất lượng dữ liệu (DQ_FLAGS) của một công dân theo quy tắc C360."""
    defects: List[str] = []
    cccd = (record.so_cccd or "").strip()
    cmtnd = (record.so_cmtnd or "").strip()
    if not CCCD_PATTERN.fullmatch(cccd) and not CCCD_PATTERN.fullmatch(cmtnd):
        defects.append("KHONG_SO_DINH_DANH")

    if not _is_valid_ngay_sinh(record.ngay_sinh):
        defects.append("NGAY_SINH_LOI")

    phone = (record.phone_number or "").replace(" ", "").strip()
    if not phone or not PHONE_PATTERN.fullmatch(phone):
        defects.append("SDT_LOI")

    ma_tinh = (record.ma_tinh or "").strip()
    ma_xa = (record.ma_xa or "").strip()
    if ref_ctx.valid_location_pairs and (ma_tinh, ma_xa) not in ref_ctx.valid_location_pairs:
        defects.append("TINH_XA_KHONG_MAP")

    quoc_tich = (record.quoc_tich or "").strip().upper()
    if not quoc_tich or (ref_ctx.valid_quoc_tich_codes and quoc_tich not in ref_ctx.valid_quoc_tich_codes):
        defects.append("QUOC_TICH_LOI")

    if record.dm_dan_toc_id is None or (
        ref_ctx.valid_dan_toc_ids and int(record.dm_dan_toc_id) not in ref_ctx.valid_dan_toc_ids
    ):
        defects.append("DAN_TOC_LOI")

    ho_ten = record.ho_ten or ""
    if ho_ten != " ".join(ho_ten.split()):
        defects.append("HO_TEN_KHOANG_TRANG")

    return defects


def _safe_query_rows(cur: oracledb.Cursor, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple[Any, ...]]:
    try:
        cur.execute(sql, params or {})
        return cur.fetchall()
    except Exception as exc:
        logger.debug("Reference query fallback triggered (%s): %s", type(exc).__name__, exc)
        return []


def load_vss_reference_context(
    cur: oracledb.Cursor,
    schema: str,
    table_name: str,
) -> VssReferenceContext:
    """
    Đọc danh mục thật (DBHC, Dân tộc, Quốc tịch) và từ vựng họ tên từ schema VSS.
    Nếu bảng danh mục nào chưa có hoặc rỗng, tự động trích xuất từ bảng chính hoặc fallback mặc định.
    """
    safe_schema = validate_identifier(schema, "schema")
    safe_table = validate_identifier(table_name, "table_name")
    full_table = f"{safe_schema}.{safe_table}"

    # 1. Địa bàn hành chính (Tỉnh/Xã)
    dbhc_rows = _safe_query_rows(
        cur,
        f"""
        SELECT x.MA_CHA, x.MA_DBHC, NVL(p.TEN, ''), NVL(x.TEN, '')
        FROM {safe_schema}.VSS_DM_DBHC x
        JOIN {safe_schema}.VSS_DM_DBHC p
          ON x.MA_CHA = p.MA_DBHC AND NVL(p.IS_CAP_XA, 0) = 0
        WHERE x.IS_CAP_XA = 1
          AND NVL(x.STATUS, '1') = '1'
          AND NVL(p.STATUS, '1') = '1'
          AND x.NGAY_HET_HIEU_LUC IS NULL
          AND p.NGAY_HET_HIEU_LUC IS NULL
          AND ROWNUM <= 200
        """,
    )
    if not dbhc_rows:
        dbhc_rows = _safe_query_rows(
            cur,
            f"""
            SELECT x.MA_CHA, x.MA_DBHC, NVL(p.TEN, ''), NVL(x.TEN, '')
            FROM {safe_schema}.VSS_DM_DBHC x
            LEFT JOIN {safe_schema}.VSS_DM_DBHC p
              ON x.MA_CHA = p.MA_DBHC
            WHERE x.MA_CHA IS NOT NULL
              AND x.MA_DBHC IS NOT NULL
              AND NVL(x.IS_CAP_XA, 1) = 1
              AND ROWNUM <= 200
            """,
        )
    locations: List[LocationItem] = []
    seen_loc: set[Tuple[str, str]] = set()
    for ma_tinh, ma_xa, ten_tinh, ten_xa in dbhc_rows:
        t_code = str(ma_tinh or "").strip()
        x_code = str(ma_xa or "").strip()
        if t_code and x_code and (t_code, x_code) not in seen_loc:
            seen_loc.add((t_code, x_code))
            locations.append(LocationItem(t_code, x_code, str(ten_tinh or "").strip(), str(ten_xa or "").strip()))

    loc_source = "VSS_DM_DBHC"
    if not locations:
        main_loc_rows = _safe_query_rows(
            cur,
            f"""
            SELECT DISTINCT TRIM(MA_TINH), TRIM(MA_XA)
            FROM {full_table}
            WHERE TRIM(MA_TINH) IS NOT NULL
              AND TRIM(MA_XA) IS NOT NULL
              AND ROWNUM <= 100
            """,
        )
        for ma_tinh, ma_xa in main_loc_rows:
            t_code = str(ma_tinh or "").strip()
            x_code = str(ma_xa or "").strip()
            if t_code and x_code and (t_code, x_code) not in seen_loc:
                seen_loc.add((t_code, x_code))
                locations.append(LocationItem(t_code, x_code))
        loc_source = full_table if locations else "default"
    if not locations:
        locations = list(_DEFAULT_LOCATIONS)

    # 2. Dân tộc
    dt_rows = _safe_query_rows(
        cur,
        f"""
        SELECT ID, MA, TEN
        FROM {safe_schema}.VSS_DM_DANTOC
        WHERE NGAY_HET_HL IS NULL AND ROWNUM <= 60
        """,
    )
    dan_tocs = [
        DanTocItem(int(row[0]), str(row[1] or "").strip(), str(row[2] or "").strip())
        for row in dt_rows
        if row[0] is not None
    ]
    if not dan_tocs:
        dan_tocs = list(_DEFAULT_DAN_TOCS)

    # 3. Quốc tịch
    qt_rows = _safe_query_rows(
        cur,
        f"""
        SELECT ID, MA, TEN
        FROM {safe_schema}.VSS_DM_QUOC_TICH
        WHERE MA IS NOT NULL AND ROWNUM <= 60
        """,
    )
    quoc_tiches = [
        QuocTichItem(int(row[0]), str(row[1]).strip().upper(), str(row[2] or "").strip())
        for row in qt_rows
        if row[0] is not None and row[1]
    ]
    if not quoc_tiches:
        quoc_tiches = list(_DEFAULT_QUOC_TICHES)

    # 4. Họ tên thật quan sát từ bảng chính để tên công dân mới tự nhiên như dữ liệu gốc
    name_rows = _safe_query_rows(
        cur,
        f"""
        SELECT DISTINCT TRIM(HO_TEN)
        FROM {full_table}
        WHERE HO_TEN IS NOT NULL AND ROWNUM <= 120
        """,
    )
    last_set: List[str] = []
    mid_set: List[str] = []
    first_set: List[str] = []
    for (raw_name,) in name_rows:
        parts = " ".join(str(raw_name or "").split()).split(" ")
        if len(parts) >= 2:
            if parts[0] not in last_set:
                last_set.append(parts[0])
            if parts[-1] not in first_set:
                first_set.append(parts[-1])
            for mid in parts[1:-1]:
                if mid not in mid_set:
                    mid_set.append(mid)

    summary = (
        f"locations={len(locations)} ({loc_source}), "
        f"dan_tocs={len(dan_tocs)}, quoc_tiches={len(quoc_tiches)}"
    )
    return VssReferenceContext(
        locations=locations,
        dan_tocs=dan_tocs,
        quoc_tiches=quoc_tiches,
        last_names=last_set if len(last_set) >= 3 else list(_DEFAULT_LAST_NAMES),
        mid_names=mid_set if len(mid_set) >= 3 else list(_DEFAULT_MID_NAMES),
        first_names=first_set if len(first_set) >= 3 else list(_DEFAULT_FIRST_NAMES),
        source_summary=summary,
    )


def _fetch_rows_for_ma_cds(
    cur: oracledb.Cursor,
    full_table: str,
    ma_cds: Sequence[str],
    chunk_size: int = 400,
) -> List[Tuple[Any, ...]]:
    all_rows: List[Tuple[Any, ...]] = []
    for start in range(0, len(ma_cds), chunk_size):
        chunk = list(ma_cds[start : start + chunk_size])
        placeholders = ", ".join(f":mc_{idx}" for idx in range(len(chunk)))
        params = {f"mc_{idx}": val for idx, val in enumerate(chunk)}
        cur.execute(
            f"""
            SELECT ID, MA_CD, MA_HO_GD, HO_TEN, GIOI_TINH, NGAY_SINH,
                   SO_CCCD, SO_CMTND, PHONE_NUMBER, MA_TINH, MA_XA,
                   QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                   MODIFY_DATE, CREATED_DATE
            FROM {full_table}
            WHERE MA_CD IN ({placeholders})
            ORDER BY MA_CD, MODIFY_DATE ASC NULLS LAST, CREATED_DATE ASC NULLS LAST, ID ASC
            """,
            params,
        )
        all_rows.extend(cur.fetchall())
    return all_rows


def load_citizen_sample_pool(
    cur: oracledb.Cursor,
    full_table: str,
    sample_size: int = 200,
) -> List[CitizenRecord]:
    """
    Lấy mẫu công dân thật từ bảng nguồn VSS_HO_GD_NHANKHAU theo 4 nhóm:
    sạch/hợp lệ, dữ liệu bẩn, chờ duyệt (TRANG_THAI_DUYET=1) và có nhiều hộ gia đình.
    Giới hạn quét bằng ROWNUM để chạy nhanh kể cả trên bảng lớn ở Oracle Cloud.
    """
    target_total = max(16, int(sample_size))
    clean_limit = max(8, target_total // 2)
    dirty_limit = max(4, target_total // 4)
    pending_limit = max(2, target_total // 8)
    multi_limit = max(2, target_total // 8)

    bucket_queries = [
        (
            f"""
            SELECT DISTINCT MA_CD FROM (
                SELECT MA_CD FROM {full_table}
                WHERE NVL(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
                  AND NVL(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
                  AND SO_CCCD IS NOT NULL
                  AND REGEXP_LIKE(TRIM(SO_CCCD), '^\\d{{12}}$')
                  AND NGAY_SINH IS NOT NULL
                  AND ROWNUM <= :scan_cap
            ) WHERE ROWNUM <= :lim
            """,
            {"scan_cap": max(clean_limit * 10, 500), "lim": clean_limit},
        ),
        (
            f"""
            SELECT DISTINCT MA_CD FROM (
                SELECT MA_CD FROM {full_table}
                WHERE NVL(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
                  AND NVL(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
                  AND (
                      SO_CCCD IS NULL
                      OR NOT REGEXP_LIKE(TRIM(SO_CCCD), '^\\d{{12}}$')
                      OR NGAY_SINH IS NULL
                      OR PHONE_NUMBER IS NULL
                      OR MA_TINH IS NULL
                      OR MA_XA IS NULL
                      OR QUOC_TICH IS NULL
                      OR DM_DAN_TOC_ID IS NULL
                  )
                  AND ROWNUM <= :scan_cap
            ) WHERE ROWNUM <= :lim
            """,
            {"scan_cap": max(dirty_limit * 20, 2000), "lim": dirty_limit},
        ),
        (
            f"""
            SELECT DISTINCT MA_CD FROM (
                SELECT MA_CD FROM {full_table}
                WHERE TRANG_THAI_DUYET = 1
                  AND ROWNUM <= :scan_cap
            ) WHERE ROWNUM <= :lim
            """,
            {"scan_cap": max(pending_limit * 20, 2000), "lim": pending_limit},
        ),
        (
            f"""
            SELECT MA_CD FROM (
                SELECT MA_CD FROM (
                    SELECT MA_CD FROM {full_table}
                    WHERE NVL(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
                      AND NVL(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
                      AND ROWNUM <= 10000
                )
                GROUP BY MA_CD
                HAVING COUNT(*) >= 2
            ) WHERE ROWNUM <= :lim
            """,
            {"lim": multi_limit},
        ),
    ]

    sampled_ma_cds: List[str] = []
    seen_ma_cds: set[str] = set()
    for sql, params in bucket_queries:
        for row in _safe_query_rows(cur, sql, params):
            ma_cd = str(row[0] or "").strip()
            if ma_cd and ma_cd not in seen_ma_cds:
                seen_ma_cds.add(ma_cd)
                sampled_ma_cds.append(ma_cd)

    if not sampled_ma_cds:
        return []

    raw_rows = _fetch_rows_for_ma_cds(cur, full_table, sampled_ma_cds)
    grouped: Dict[str, List[Tuple[Any, ...]]] = {}
    for row in raw_rows:
        ma_cd = str(row[1] or "").strip()
        if ma_cd:
            grouped.setdefault(ma_cd, []).append(row)

    records: List[CitizenRecord] = []
    for ma_cd, rows in grouped.items():
        latest = rows[-1]
        households = [
            CitizenHousehold(
                id=str(r[0] or "").strip(),
                ma_ho_gd=str(r[2] or "").strip(),
                ma_tinh=str(r[9]).strip() if r[9] is not None else None,
                ma_xa=str(r[10]).strip() if r[10] is not None else None,
                modify_date=r[16],
            )
            for r in rows
            if r[0] is not None
        ]
        if not households:
            continue
        records.append(
            CitizenRecord(
                ma_cd=ma_cd,
                ho_ten=str(latest[3] or "Công dân VSS"),
                gioi_tinh=str(latest[4] or "1").strip() or "1",
                ngay_sinh=int(latest[5]) if latest[5] is not None else None,
                so_cccd=str(latest[6]).strip() if latest[6] is not None else None,
                so_cmtnd=str(latest[7]).strip() if latest[7] is not None else None,
                phone_number=str(latest[8]).strip() if latest[8] is not None else None,
                ma_tinh=str(latest[9]).strip() if latest[9] is not None else None,
                ma_xa=str(latest[10]).strip() if latest[10] is not None else None,
                quoc_tich=str(latest[11]).strip() if latest[11] is not None else None,
                dm_dan_toc_id=int(latest[12]) if latest[12] is not None else None,
                is_alive=str(latest[13] or "1").strip() or "1",
                typeid=str(latest[14]).strip() if latest[14] is not None else None,
                trang_thai_duyet=int(latest[15]) if latest[15] is not None else None,
                households=households,
                from_vss=True,
            )
        )
    return records


def _generate_unique_ma_cd(seq: int, prefix: str = "SIM") -> str:
    return f"{prefix}{int(time.time()) % 1000000:06d}{seq:04d}"


def _random_birth_yyyymmdd(min_year: int = 1960, max_year: int = 2006) -> int:
    year = random.randint(min_year, max_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return year * 10000 + month * 100 + day


def _compose_name(ref_ctx: VssReferenceContext) -> str:
    return (
        f"{random.choice(ref_ctx.last_names)} "
        f"{random.choice(ref_ctx.mid_names)} "
        f"{random.choice(ref_ctx.first_names)}"
    )


def build_insert_variant_payload(
    seq: int,
    variant: str,
    ref_ctx: VssReferenceContext,
) -> Dict[str, Any]:
    """
    Sinh dữ liệu INSERT 1A đa dạng theo biến thể cụ thể, sử dụng danh mục thật từ VSS.
    """
    ma_cd = _generate_unique_ma_cd(seq)
    ho_ten = _compose_name(ref_ctx)
    loc = random.choice(ref_ctx.locations)
    vn_qt = next((qt.ma for qt in ref_ctx.quoc_tiches if qt.ma == "VN"), ref_ctx.quoc_tiches[0].ma)
    kinh_dt = next((dt.id for dt in ref_ctx.dan_tocs if dt.id == 1), ref_ctx.dan_tocs[0].id)

    payload: Dict[str, Any] = {
        "id": f"{ma_cd}_H1",
        "ma_cd": ma_cd,
        "ma_ho_gd": f"HGD_{ma_cd}_1",
        "ho_ten": ho_ten,
        "ho_ten_unsigned": to_unsigned_upper(ho_ten),
        "gioi_tinh": random.choice(["0", "1"]),
        "ngay_sinh": _random_birth_yyyymmdd(),
        "so_cccd": f"001{random.randint(100000000, 999999999)}",
        "phone_number": f"09{random.randint(10000000, 99999999)}",
        "ma_tinh": loc.ma_tinh,
        "ma_xa": loc.ma_xa,
        "quoc_tich": vn_qt,
        "dm_dan_toc_id": kinh_dt,
        "is_alive": "1",
        "dead_date": None,
        "typeid": None,
        "trang_thai_duyet": 2,
    }

    if variant == "1A_MISSING_CCCD":
        payload["ho_ten"] = f"  {ho_ten}  "
        payload["so_cccd"] = None if seq % 2 == 0 else f"{random.randint(100000000, 999999999)}"
    elif variant == "1A_BAD_BIRTH_AND_PHONE":
        payload["ngay_sinh"] = 19950230 if seq % 2 == 0 else 20990101
        payload["phone_number"] = "12345"
    elif variant == "1A_BAD_LOCATION_OR_CATALOG":
        payload["ma_xa"] = "99999"
        payload["quoc_tich"] = "XX"
        payload["dm_dan_toc_id"] = 9999
    elif variant == "1A_PENDING_APPROVAL":
        payload["trang_thai_duyet"] = 1
    elif variant == "1A_DIVERSE_CATALOG_OR_DECEASED":
        alt_dt = [dt for dt in ref_ctx.dan_tocs if dt.id != kinh_dt]
        alt_qt = [qt for qt in ref_ctx.quoc_tiches if qt.ma != vn_qt]
        if alt_dt:
            payload["dm_dan_toc_id"] = random.choice(alt_dt).id
        if alt_qt and seq % 2 == 0:
            payload["quoc_tich"] = random.choice(alt_qt).ma
        if seq % 3 == 0:
            payload["is_alive"] = "0"
            payload["dead_date"] = 20250115

    return payload


def execute_insert_new_citizen(
    cur: oracledb.Cursor,
    full_table: str,
    seq: int,
    variant: str,
    ref_ctx: VssReferenceContext,
) -> Tuple[CitizenRecord, int, str]:
    p = build_insert_variant_payload(seq=seq, variant=variant, ref_ctx=ref_ctx)
    cur.execute(
        f"""
        INSERT INTO {full_table} (
            ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
            NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
            QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, DEAD_DATE, TYPEID, TRANG_THAI_DUYET,
            CREATED_DATE, MODIFY_DATE
        ) VALUES (
            :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, :gioi_tinh,
            :ngay_sinh, :so_cccd, :phone_number, :ma_tinh, :ma_xa,
            :quoc_tich, :dm_dan_toc_id, :is_alive, :dead_date, :typeid, :trang_thai_duyet,
            SYSDATE, SYSDATE
        )
        """,
        p,
    )
    rowcount = cur.rowcount
    record = CitizenRecord(
        ma_cd=str(p["ma_cd"]),
        ho_ten=str(p["ho_ten"]),
        gioi_tinh=str(p["gioi_tinh"]),
        ngay_sinh=int(p["ngay_sinh"]) if p["ngay_sinh"] is not None else None,
        so_cccd=str(p["so_cccd"]) if p["so_cccd"] is not None else None,
        so_cmtnd=None,
        phone_number=str(p["phone_number"]) if p["phone_number"] is not None else None,
        ma_tinh=str(p["ma_tinh"]),
        ma_xa=str(p["ma_xa"]),
        quoc_tich=str(p["quoc_tich"]),
        dm_dan_toc_id=int(p["dm_dan_toc_id"]),
        is_alive=str(p["is_alive"]),
        typeid=p["typeid"],
        trang_thai_duyet=int(p["trang_thai_duyet"]),
        households=[CitizenHousehold(id=str(p["id"]), ma_ho_gd=str(p["ma_ho_gd"]), ma_tinh=str(p["ma_tinh"]), ma_xa=str(p["ma_xa"]))],
        from_vss=False,
    )
    summary = (
        f"MA_CD={record.ma_cd}, ho_ten={record.ho_ten.strip()!r}, "
        f"tinh/xa={record.ma_tinh}/{record.ma_xa}, qt={record.quoc_tich}, dt={record.dm_dan_toc_id}, "
        f"duyet={record.trang_thai_duyet}, cccd={record.so_cccd}"
    )
    return record, rowcount, summary


def execute_insert_new_household(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
    ref_ctx: VssReferenceContext,
    variant: str,
) -> Tuple[int, str]:
    ho_seq = len(target.households) + 1
    new_id = f"{target.ma_cd}_H{ho_seq}_{int(time.time()) % 1000}"[:40]
    new_ho = f"HGD_{target.ma_cd}_{ho_seq}"[:30]

    other_locs = [
        loc for loc in ref_ctx.locations
        if (loc.ma_tinh, loc.ma_xa) != (target.ma_tinh, target.ma_xa)
    ]
    if variant == "1B_RELOCATE_OTHER_PROVINCE":
        diff_prov = [loc for loc in other_locs if loc.ma_tinh != target.ma_tinh]
        chosen_loc = random.choice(diff_prov or other_locs or ref_ctx.locations)
    else:
        same_prov = [loc for loc in other_locs if loc.ma_tinh == target.ma_tinh]
        chosen_loc = random.choice(same_prov or other_locs or ref_ctx.locations)

    valid_cccd = target.so_cccd if CCCD_PATTERN.fullmatch((target.so_cccd or "").strip()) else f"001{random.randint(100000000, 999999999)}"
    valid_birth = target.ngay_sinh if _is_valid_ngay_sinh(target.ngay_sinh) else _random_birth_yyyymmdd()
    valid_phone = target.phone_number if PHONE_PATTERN.fullmatch((target.phone_number or "").replace(" ", "")) else f"09{random.randint(10000000, 99999999)}"
    valid_qt = target.quoc_tich if (target.quoc_tich or "").strip().upper() in ref_ctx.valid_quoc_tich_codes else ref_ctx.quoc_tiches[0].ma
    valid_dt = target.dm_dan_toc_id if target.dm_dan_toc_id in ref_ctx.valid_dan_toc_ids else ref_ctx.dan_tocs[0].id
    clean_name = " ".join(target.ho_ten.split()) or "Công dân VSS"

    cur.execute(
        f"""
        INSERT INTO {full_table} (
            ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
            NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
            QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
            CREATED_DATE, MODIFY_DATE
        ) VALUES (
            :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, :gioi_tinh,
            :ngay_sinh, :so_cccd, :phone_number, :ma_tinh, :ma_xa,
            :quoc_tich, :dm_dan_toc_id, :is_alive, NULL, 2,
            SYSDATE, SYSDATE + NUMTODSINTERVAL(:ho_seq, 'MINUTE')
        )
        """,
        {
            "id": new_id,
            "ma_cd": target.ma_cd,
            "ma_ho_gd": new_ho,
            "ho_ten": clean_name,
            "ho_ten_unsigned": to_unsigned_upper(clean_name),
            "gioi_tinh": target.gioi_tinh or "1",
            "ngay_sinh": valid_birth,
            "so_cccd": valid_cccd,
            "phone_number": valid_phone,
            "ma_tinh": chosen_loc.ma_tinh,
            "ma_xa": chosen_loc.ma_xa,
            "quoc_tich": valid_qt,
            "dm_dan_toc_id": valid_dt,
            "is_alive": target.is_alive or "1",
            "ho_seq": ho_seq,
        },
    )
    rowcount = cur.rowcount
    old_loc = f"{target.ma_tinh}/{target.ma_xa}"
    target.households.append(
        CitizenHousehold(id=new_id, ma_ho_gd=new_ho, ma_tinh=chosen_loc.ma_tinh, ma_xa=chosen_loc.ma_xa)
    )
    target.ma_tinh = chosen_loc.ma_tinh
    target.ma_xa = chosen_loc.ma_xa
    summary = (
        f"[{target.source_label}] MA_CD={target.ma_cd}, new_ho={new_ho}, "
        f"loc {old_loc} -> {chosen_loc.ma_tinh}/{chosen_loc.ma_xa}, total_ho={len(target.households)}"
    )
    return rowcount, summary


def build_fix_update_plan(
    target: CitizenRecord,
    ref_ctx: VssReferenceContext,
    variant: str,
) -> Tuple[str, Dict[str, Any], str]:
    """
    Tạo câu lệnh UPDATE làm sạch dữ liệu dựa trên lỗi thực tế của bản ghi và danh mục VSS.
    """
    defects = detect_citizen_dq_defects(target, ref_ctx)
    set_clauses: List[str] = ["MODIFY_DATE = SYSDATE"]
    params: Dict[str, Any] = {"ma_cd": target.ma_cd}
    changes: List[str] = []

    fix_cccd = variant in {"2_FIX_ACTUAL_DEFECTS_FULL", "2_FIX_CCCD_ONLY"} and (
        "KHONG_SO_DINH_DANH" in defects or variant == "2_FIX_CCCD_ONLY"
    )
    fix_loc = variant in {"2_FIX_ACTUAL_DEFECTS_FULL", "2_FIX_LOCATION_FROM_DBHC"} and (
        "TINH_XA_KHONG_MAP" in defects or variant == "2_FIX_LOCATION_FROM_DBHC"
    )
    fix_all = variant == "2_FIX_ACTUAL_DEFECTS_FULL"

    if fix_cccd:
        new_cccd = f"001{random.randint(100000000, 999999999)}"
        set_clauses.append("SO_CCCD = :so_cccd")
        params["so_cccd"] = new_cccd
        changes.append(f"SO_CCCD={target.so_cccd}->{new_cccd}")
        target.so_cccd = new_cccd

    if fix_all and "NGAY_SINH_LOI" in defects:
        new_birth = _random_birth_yyyymmdd()
        set_clauses.append("NGAY_SINH = :ngay_sinh")
        params["ngay_sinh"] = new_birth
        changes.append(f"NGAY_SINH={target.ngay_sinh}->{new_birth}")
        target.ngay_sinh = new_birth

    if fix_all and "SDT_LOI" in defects:
        new_phone = f"09{random.randint(10000000, 99999999)}"
        set_clauses.append("PHONE_NUMBER = :phone_number")
        params["phone_number"] = new_phone
        changes.append(f"PHONE={target.phone_number}->{new_phone}")
        target.phone_number = new_phone

    if fix_loc:
        loc = random.choice(ref_ctx.locations)
        set_clauses.extend(["MA_TINH = :ma_tinh", "MA_XA = :ma_xa", "MA_TINH_LH = :ma_tinh", "MA_XA_LH = :ma_xa"])
        params["ma_tinh"] = loc.ma_tinh
        params["ma_xa"] = loc.ma_xa
        changes.append(f"TINH/XA={target.ma_tinh}/{target.ma_xa}->{loc.ma_tinh}/{loc.ma_xa}")
        target.ma_tinh = loc.ma_tinh
        target.ma_xa = loc.ma_xa

    if fix_all and "QUOC_TICH_LOI" in defects:
        vn_qt = next((qt.ma for qt in ref_ctx.quoc_tiches if qt.ma == "VN"), ref_ctx.quoc_tiches[0].ma)
        set_clauses.append("QUOC_TICH = :quoc_tich")
        params["quoc_tich"] = vn_qt
        changes.append(f"QUOC_TICH={target.quoc_tich}->{vn_qt}")
        target.quoc_tich = vn_qt

    if fix_all and "DAN_TOC_LOI" in defects:
        kinh_dt = next((dt.id for dt in ref_ctx.dan_tocs if dt.id == 1), ref_ctx.dan_tocs[0].id)
        set_clauses.append("DM_DAN_TOC_ID = :dm_dan_toc_id")
        params["dm_dan_toc_id"] = kinh_dt
        changes.append(f"DAN_TOC={target.dm_dan_toc_id}->{kinh_dt}")
        target.dm_dan_toc_id = kinh_dt

    if fix_all and ("HO_TEN_KHOANG_TRANG" in defects or not changes):
        clean_name = " ".join((target.ho_ten or "Công dân VSS").split())
        set_clauses.extend(["HO_TEN = :ho_ten", "HO_TEN_UNSIGNED = :ho_ten_unsigned"])
        params["ho_ten"] = clean_name
        params["ho_ten_unsigned"] = to_unsigned_upper(clean_name)
        changes.append("HO_TEN_TRIMMED")
        target.ho_ten = clean_name

    return ", ".join(set_clauses), params, ", ".join(changes)


def execute_fix_dirty_citizen(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
    ref_ctx: VssReferenceContext,
    variant: str,
) -> Tuple[int, str]:
    before_defects = detect_citizen_dq_defects(target, ref_ctx)
    set_sql, params, diff_text = build_fix_update_plan(target, ref_ctx, variant)
    cur.execute(f"UPDATE {full_table} SET {set_sql} WHERE MA_CD = :ma_cd", params)
    after_defects = detect_citizen_dq_defects(target, ref_ctx)
    summary = (
        f"[{target.source_label}] MA_CD={target.ma_cd}, defects {before_defects} -> {after_defects} ({diff_text})"
    )
    return cur.rowcount, summary


def execute_mark_status_invalid(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
    variant: str,
) -> Tuple[int, str]:
    old_state = f"TYPEID={target.typeid}, DUYET={target.trang_thai_duyet}"
    if variant == "3A_REJECT_DUYET_4":
        new_typeid, new_duyet = target.typeid, 4
    elif variant == "3A_MARK_TYPEID_GT":
        new_typeid, new_duyet = "GT", target.trang_thai_duyet if target.trang_thai_duyet is not None else 2
    elif variant == "3A_REVERT_PENDING_1":
        new_typeid, new_duyet = None, 1
    else:
        new_typeid, new_duyet = "GG", 4

    cur.execute(
        f"""
        UPDATE {full_table}
        SET TYPEID = :typeid,
            TRANG_THAI_DUYET = :trang_thai_duyet,
            MODIFY_DATE = SYSDATE
        WHERE MA_CD = :ma_cd
        """,
        {"typeid": new_typeid, "trang_thai_duyet": new_duyet, "ma_cd": target.ma_cd},
    )
    target.typeid = new_typeid
    target.trang_thai_duyet = new_duyet
    summary = (
        f"[{target.source_label}] MA_CD={target.ma_cd}, {old_state} -> TYPEID={new_typeid}, DUYET={new_duyet}"
    )
    return cur.rowcount, summary


def execute_approve_pending(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
    variant: str,
) -> Tuple[int, str]:
    old_state = f"TYPEID={target.typeid}, DUYET={target.trang_thai_duyet}"
    new_duyet = 0 if variant == "3B_APPROVE_DUYET_0" else 2
    cur.execute(
        f"""
        UPDATE {full_table}
        SET TYPEID = NULL,
            TRANG_THAI_DUYET = :trang_thai_duyet,
            MODIFY_DATE = SYSDATE
        WHERE MA_CD = :ma_cd
        """,
        {"trang_thai_duyet": new_duyet, "ma_cd": target.ma_cd},
    )
    target.typeid = None
    target.trang_thai_duyet = new_duyet
    summary = f"[{target.source_label}] MA_CD={target.ma_cd}, {old_state} -> TYPEID=None, DUYET={new_duyet}"
    return cur.rowcount, summary


def execute_change_ma_cd(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
    seq: int,
) -> Tuple[int, str]:
    old_ma = target.ma_cd
    new_ma = _generate_unique_ma_cd(seq, prefix="REN")
    cur.execute(
        f"""
        UPDATE {full_table}
        SET MA_CD_OLD = MA_CD,
            MA_CD = :new_ma,
            TYPEID = NULL,
            TRANG_THAI_DUYET = CASE WHEN NVL(TRANG_THAI_DUYET, 0) IN (1, 3, 4) THEN 2 ELSE TRANG_THAI_DUYET END,
            MODIFY_DATE = SYSDATE
        WHERE MA_CD = :old_ma
        """,
        {"new_ma": new_ma, "old_ma": old_ma},
    )
    target.ma_cd = new_ma
    if target.trang_thai_duyet in FILTERED_DUYET_STATES:
        target.trang_thai_duyet = 2
    target.typeid = None
    return cur.rowcount, f"[{target.source_label}] MA_CD {old_ma} -> {new_ma}"


def execute_delete_latest_household(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
) -> Tuple[int, str]:
    latest = target.households.pop()
    cur.execute(
        f"DELETE FROM {full_table} WHERE ID = :id AND MA_CD = :ma_cd",
        {"id": latest.id, "ma_cd": target.ma_cd},
    )
    remaining = target.households[-1]
    if remaining.ma_tinh and remaining.ma_xa:
        target.ma_tinh = remaining.ma_tinh
        target.ma_xa = remaining.ma_xa
    summary = (
        f"[{target.source_label}] MA_CD={target.ma_cd}, "
        f"deleted_ho={latest.ma_ho_gd} (ID={latest.id}) -> rollback_to={remaining.ma_ho_gd}"
    )
    return cur.rowcount, summary


def execute_delete_all_records(
    cur: oracledb.Cursor,
    full_table: str,
    target: CitizenRecord,
) -> Tuple[int, str]:
    cur.execute(f"DELETE FROM {full_table} WHERE MA_CD = :ma_cd", {"ma_cd": target.ma_cd})
    return cur.rowcount, f"[{target.source_label}] MA_CD={target.ma_cd} (rows_in_pool={len(target.households)})"


# -----------------------------------------------------------------------------
# Kịch bản cố định (giữ tương thích ngược cho Local khi có sẵn mã 0000000001..5)
# -----------------------------------------------------------------------------

def build_cdc_scenarios(schema: Optional[str] = None) -> List[CdcScenario]:
    """
    Xây dựng danh sách 8 kịch bản biến động CDC chuẩn trên bảng nguồn <schema>.VSS_HO_GD_NHANKHAU.
    """
    table = f"{schema or SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
    return [
        CdcScenario(
            key="1A",
            name="SCENARIO_1A_INSERT_NEW_CITIZEN",
            description="Thêm mới hoàn toàn công dân sạch (MA_CD='9999000001')",
            sql=f"""
            MERGE INTO {table} tgt
            USING (SELECT '9999000001_HO_1' AS ID FROM DUAL) src
            ON (tgt.ID = src.ID)
            WHEN NOT MATCHED THEN INSERT (
                ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                CREATED_DATE, MODIFY_DATE
            ) VALUES (
                '9999000001_HO_1', '9999000001', 'HGD_NEW_000001',
                'Trần Thị Công Dân Mới', 'TRAN THI CONG DAN MOI', '0',
                19980520, '001198000001', '0901234567', '01TTT', '00004',
                'VN', 1, '1', NULL, 2,
                SYSDATE, SYSDATE + INTERVAL '1' HOUR
            )
            WHEN MATCHED THEN UPDATE SET
                MODIFY_DATE = SYSDATE + INTERVAL '1' HOUR
            """,
        ),
        CdcScenario(
            key="1B",
            name="SCENARIO_1B_INSERT_NEW_HOUSEHOLD",
            description="Thêm hộ mới (MA_HO_GD='HGD_NEW_999999', MODIFY_DATE mới hơn) cho công dân cũ MA_CD='0000000001'",
            sql=f"""
            MERGE INTO {table} tgt
            USING (SELECT '0000000001_NEW_HO' AS ID FROM DUAL) src
            ON (tgt.ID = src.ID)
            WHEN NOT MATCHED THEN INSERT (
                ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                CREATED_DATE, MODIFY_DATE
            ) VALUES (
                '0000000001_NEW_HO', '0000000001', 'HGD_NEW_999999',
                'Nguyễn Văn Công Dân 1', 'NGUYEN VAN CONG DAN 1', '1',
                19950815, '001095000001', '0912345678', '01TTT', '00004',
                'VN', 1, '1', NULL, 2,
                SYSDATE, SYSDATE + INTERVAL '2' HOUR
            )
            WHEN MATCHED THEN UPDATE SET
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            """,
        ),
        CdcScenario(
            key="2",
            name="SCENARIO_2_UPDATE_FIX_DQ_FLAGS",
            description="Sửa toàn bộ thông tin bẩn của MA_CD='0000000005' để gỡ hết cờ DQ_FLAGS",
            sql=f"""
            UPDATE {table}
            SET SO_CCCD = '001095999888',
                NGAY_SINH = 19950815,
                MA_TINH = '01TTT',
                MA_XA = '00004',
                MA_TINH_LH = '01TTT',
                MA_XA_LH = '00004',
                MA_TINH_KS = '01TTT',
                MA_XA_KS = '00004',
                PHONE_NUMBER = '0987654321',
                QUOC_TICH = 'VN',
                DM_DAN_TOC_ID = 1,
                TYPEID = NULL,
                TRANG_THAI_DUYET = 2,
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE MA_CD = '0000000005'
            """,
        ),
        CdcScenario(
            key="3A",
            name="SCENARIO_3A_UPDATE_STATUS_TO_INVALID",
            description="Chuyển MA_CD='0000000002' sang trạng thái bị lọc (TYPEID='GG', TRANG_THAI_DUYET=4)",
            sql=f"""
            UPDATE {table}
            SET TYPEID = 'GG',
                TRANG_THAI_DUYET = 4,
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE MA_CD = '0000000002'
            """,
        ),
        CdcScenario(
            key="3B",
            name="SCENARIO_3B_UPDATE_STATUS_PENDING_TO_APPROVED",
            description="Duyệt hồ sơ từ TRANG_THAI_DUYET=1 sang 2 (tối đa 10 dòng)",
            sql=f"""
            UPDATE {table}
            SET TYPEID = NULL,
                TRANG_THAI_DUYET = 2,
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE TRANG_THAI_DUYET = 1 AND ROWNUM <= 10
            """,
        ),
        CdcScenario(
            key="4",
            name="SCENARIO_4_CHANGE_MA_CD",
            description="Đổi MA_CD='0000000003' -> '9900000003' và lưu mã cũ vào MA_CD_OLD",
            sql=f"""
            UPDATE {table}
            SET MA_CD_OLD = '0000000003',
                MA_CD = '9900000003',
                TYPEID = NULL,
                TRANG_THAI_DUYET = 2,
                MODIFY_DATE = SYSDATE + INTERVAL '3' HOUR
            WHERE MA_CD = '0000000003'
            """,
        ),
        CdcScenario(
            key="5A",
            name="SCENARIO_5A_DELETE_LATEST_HOUSEHOLD_ROLLBACK_TO_OLDER",
            description="Xóa hộ mới nhất 'HGD_NEW_999999' của MA_CD='0000000001'",
            sql=f"""
            DELETE FROM {table}
            WHERE MA_CD = '0000000001' AND MA_HO_GD = 'HGD_NEW_999999'
            """,
        ),
        CdcScenario(
            key="5B",
            name="SCENARIO_5B_DELETE_ALL_RECORDS_OF_CITIZEN",
            description="Xóa toàn bộ các dòng của MA_CD='0000000004' ở nguồn",
            sql=f"""
            DELETE FROM {table}
            WHERE MA_CD = '0000000004'
            """,
        ),
    ]


def build_cdc_test_sql_statements() -> List[Tuple[str, str]]:
    """Giữ tương thích ngược: trả về danh sách (name, sql)."""
    return [(s.name, s.sql) for s in build_cdc_scenarios()]


def filter_scenarios(
    scenarios: List[CdcScenario], selected_keys: Optional[Iterable[str]] = None
) -> List[CdcScenario]:
    """Lọc danh sách kịch bản theo danh sách key (VD: ['1A', '2'])."""
    if not selected_keys:
        return scenarios
    wanted = {k.strip().upper() for k in selected_keys if k.strip()}
    if "ALL" in wanted:
        return scenarios
    valid_keys = {s.key.upper() for s in scenarios}
    invalid = wanted - valid_keys
    if invalid:
        raise ValueError(
            f"Unknown scenario key(s): {sorted(invalid)}. Valid keys: {sorted(valid_keys)}"
        )
    return [s for s in scenarios if s.key.upper() in wanted]


def _ensure_preconditions_for_deterministic_scenarios(
    cur: oracledb.Cursor,
    conn: oracledb.Connection,
    table: str,
    wanted_keys: set[str],
) -> None:
    if "1B" in wanted_keys or "5A" in wanted_keys:
        cur.execute(
            f"""
            UPDATE {table}
            SET TYPEID = NULL,
                TRANG_THAI_DUYET = 0,
                MODIFY_DATE = SYSDATE - INTERVAL '1' DAY
            WHERE MA_CD = '0000000001' AND MA_HO_GD <> 'HGD_NEW_999999'
            """
        )
        conn.commit()

    if "4" in wanted_keys:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE MA_CD = '0000000003'")
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                f"""
                UPDATE {table}
                SET MA_CD = '0000000003',
                    MA_CD_OLD = NULL,
                    MODIFY_DATE = SYSDATE
                WHERE MA_CD = '9900000003'
                """
            )
            conn.commit()

    if "5B" in wanted_keys:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE MA_CD = '0000000004'")
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                f"""
                INSERT INTO {table} (
                    ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                    NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                    QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                    CREATED_DATE, MODIFY_DATE
                ) VALUES (
                    '0000000004_HO_1', '0000000004', 'HGD_0000000040',
                    'Phạm Thị Công Dân 4', 'PHAM THI CONG DAN 4', '0',
                    19920310, '001192000004', '0912345604', '01TTT', '00004',
                    'VN', 1, '1', NULL, 2, SYSDATE - INTERVAL '1' DAY, SYSDATE
                )
                """
            )
            conn.commit()


def _has_deterministic_seed_records(cur: oracledb.Cursor, full_table: str) -> bool:
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT MA_CD)
        FROM {full_table}
        WHERE MA_CD IN ('0000000001', '0000000002', '0000000003', '0000000004', '0000000005')
        """
    )
    row = cur.fetchone()
    return int(row[0] if row else 0) >= 3


def run_batch_insert_new_citizens(
    batch_count: int,
    chunk_size: int = 5000,
    target: str = "local",
    ref_ctx: Optional[VssReferenceContext] = None,
) -> int:
    """
    Kịch bản 1A mở rộng: Sinh thêm một lô công dân mới dựa trên danh mục thật của VSS.
    """
    if batch_count <= 0:
        return 0

    spec = build_target_spec(target)
    full_table = spec.full_table
    prefix = f"B{int(time.time()) % 100000:05d}"
    sql = f"""
        INSERT INTO {full_table} (
            ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
            NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
            QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
            CREATED_DATE, MODIFY_DATE
        ) VALUES (
            :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, :gioi_tinh,
            :ngay_sinh, :so_cccd, :phone_number, :ma_tinh, :ma_xa,
            :quoc_tich, :dm_dan_toc_id, '1', NULL, 2, SYSDATE, SYSDATE
        )
    """
    total_inserted = 0
    with open_oracle_connection(spec) as conn:
        with conn.cursor() as cur:
            active_ctx = ref_ctx or load_vss_reference_context(cur, spec.schema, spec.table_name)
            vn_qt = next((qt.ma for qt in active_ctx.quoc_tiches if qt.ma == "VN"), active_ctx.quoc_tiches[0].ma)
            kinh_dt = next((dt.id for dt in active_ctx.dan_tocs if dt.id == 1), active_ctx.dan_tocs[0].id)

            rows: List[Dict[str, object]] = []
            for i in range(1, batch_count + 1):
                ma_cd = f"{prefix}{i:06d}"
                ho_ten = _compose_name(active_ctx)
                loc = random.choice(active_ctx.locations)
                rows.append({
                    "id": f"{ma_cd}_H1",
                    "ma_cd": ma_cd,
                    "ma_ho_gd": f"HGD_{ma_cd}",
                    "ho_ten": ho_ten,
                    "ho_ten_unsigned": to_unsigned_upper(ho_ten),
                    "gioi_tinh": "1" if i % 2 else "0",
                    "ngay_sinh": 19900101 + (i % 28),
                    "so_cccd": f"001{i:09d}",
                    "phone_number": f"09{i % 100000000:08d}",
                    "ma_tinh": loc.ma_tinh,
                    "ma_xa": loc.ma_xa,
                    "quoc_tich": vn_qt,
                    "dm_dan_toc_id": kinh_dt,
                })
                if len(rows) >= chunk_size or i == batch_count:
                    cur.executemany(sql, rows)
                    conn.commit()
                    total_inserted += len(rows)
                    logger.info(
                        "[%s][BATCH_INSERT] Inserted %d/%d new citizen rows into %s",
                        spec.tag,
                        total_inserted,
                        batch_count,
                        full_table,
                    )
                    rows.clear()
    return total_inserted


def _pick_distinct_citizen(
    candidates: List[CitizenRecord],
    used_ma_cds: set[str],
) -> Optional[CitizenRecord]:
    for c in candidates:
        if c.ma_cd not in used_ma_cds:
            return c
    return candidates[0] if candidates else None


def _run_vss_backed_scenarios(
    cur: oracledb.Cursor,
    conn: oracledb.Connection,
    spec: TargetConnectionSpec,
    scenarios: List[CdcScenario],
    commit_each: bool,
    delay_seconds: float,
    sample_size: int,
) -> List[Tuple[str, int]]:
    """
    Thực thi 8 kịch bản cố định trực tiếp trên dữ liệu thật đọc từ VSS (dùng cho Cloud hoặc khi
    chưa có các mã kiểm thử cứng 0000000001..0000000005).
    """
    full_table = spec.full_table
    ref_ctx = load_vss_reference_context(cur, spec.schema, spec.table_name)
    pool = load_citizen_sample_pool(cur, full_table, sample_size=sample_size)
    logger.info(
        "[%s] Loaded VSS context (%s) and %d citizen(s) from %s.",
        spec.tag,
        ref_ctx.source_summary,
        len(pool),
        full_table,
    )

    used_ma_cds: set[str] = set()
    created_for_1b: Optional[CitizenRecord] = None
    results: List[Tuple[str, int]] = []
    seq = 1

    for idx, scenario in enumerate(scenarios):
        key = scenario.key.upper()
        rowcount = 0
        detail = ""

        if key == "1A":
            new_rec, rowcount, detail = execute_insert_new_citizen(
                cur, full_table, seq=seq, variant="1A_CLEAN", ref_ctx=ref_ctx
            )
            seq += 1
            pool.append(new_rec)
            used_ma_cds.add(new_rec.ma_cd)

        elif key == "1B":
            valid_candidates = [c for c in pool if c.is_valid]
            target = _pick_distinct_citizen(valid_candidates, used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_CLEAN", ref_ctx=ref_ctx
                )
                seq += 1
                pool.append(target)
            rowcount, detail = execute_insert_new_household(
                cur, full_table, target=target, ref_ctx=ref_ctx, variant="1B_RELOCATE_OTHER_PROVINCE"
            )
            used_ma_cds.add(target.ma_cd)
            created_for_1b = target

        elif key == "2":
            dirty_candidates = [
                c for c in pool if c.is_valid and detect_citizen_dq_defects(c, ref_ctx)
            ]
            target = _pick_distinct_citizen(dirty_candidates or [c for c in pool if c.is_valid], used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_MISSING_CCCD", ref_ctx=ref_ctx
                )
                seq += 1
                pool.append(target)
            rowcount, detail = execute_fix_dirty_citizen(
                cur, full_table, target=target, ref_ctx=ref_ctx, variant="2_FIX_ACTUAL_DEFECTS_FULL"
            )
            used_ma_cds.add(target.ma_cd)

        elif key == "3A":
            valid_candidates = [c for c in pool if c.is_valid]
            target = _pick_distinct_citizen(valid_candidates, used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_CLEAN", ref_ctx=ref_ctx
                )
                seq += 1
                pool.append(target)
            rowcount, detail = execute_mark_status_invalid(
                cur, full_table, target=target, variant="3A_MARK_TYPEID_GG"
            )
            used_ma_cds.add(target.ma_cd)

        elif key == "3B":
            pending_candidates = [c for c in pool if not c.is_valid]
            target = _pick_distinct_citizen(pending_candidates, used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_PENDING_APPROVAL", ref_ctx=ref_ctx
                )
                seq += 1
                pool.append(target)
            rowcount, detail = execute_approve_pending(
                cur, full_table, target=target, variant="3B_APPROVE_DUYET_2"
            )
            used_ma_cds.add(target.ma_cd)

        elif key == "4":
            valid_candidates = [c for c in pool if c.is_valid]
            target = _pick_distinct_citizen(valid_candidates, used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_CLEAN", ref_ctx=ref_ctx
                )
                seq += 1
                pool.append(target)
            rowcount, detail = execute_change_ma_cd(cur, full_table, target=target, seq=seq)
            seq += 1
            used_ma_cds.add(target.ma_cd)

        elif key == "5A":
            target = created_for_1b if (created_for_1b and len(created_for_1b.households) >= 2) else None
            if target is None:
                multi_candidates = [c for c in pool if len(c.households) >= 2 and c.is_valid]
                target = _pick_distinct_citizen(multi_candidates, used_ma_cds)
            if target is not None and len(target.households) >= 2:
                rowcount, detail = execute_delete_latest_household(cur, full_table, target=target)
                used_ma_cds.add(target.ma_cd)
            else:
                detail = "No citizen with >= 2 households found in sample pool"

        elif key == "5B":
            target = _pick_distinct_citizen(pool, used_ma_cds)
            if target is None:
                target, _, _ = execute_insert_new_citizen(
                    cur, full_table, seq=seq, variant="1A_CLEAN", ref_ctx=ref_ctx
                )
                seq += 1
            else:
                pool.remove(target)
            rowcount, detail = execute_delete_all_records(cur, full_table, target=target)

        results.append((scenario.name, rowcount))
        if commit_each:
            conn.commit()

        log_fn = logger.warning if rowcount == 0 else logger.info
        log_fn(
            "[%s][%s] %s -> rowcount=%d%s | %s",
            spec.tag,
            scenario.key,
            scenario.name,
            rowcount,
            " (committed)" if commit_each else "",
            detail,
        )

        if delay_seconds > 0 and idx < len(scenarios) - 1:
            time.sleep(delay_seconds)

    if not commit_each:
        conn.commit()
        logger.info("[%s] Committed all %d CDC test scenarios in a single transaction.", spec.tag, len(scenarios))

    return results


def run_cdc_simulation(
    selected_keys: Optional[Iterable[str]] = None,
    commit_each: bool = True,
    delay_seconds: float = 0.0,
    insert_batch_size: int = 0,
    target: str = "local",
    sample_size: int = 200,
) -> List[Tuple[str, int]]:
    """
    Thực thi các kịch bản biến động CDC lên bảng nguồn Oracle (Local hoặc Cloud).
    Mặc định commit sau từng kịch bản để CDC bắt từng sự kiện độc lập.
    """
    spec = build_target_spec(target)
    scenarios = filter_scenarios(build_cdc_scenarios(spec.schema), selected_keys)
    wanted_keys = {s.key.upper() for s in scenarios}
    results: List[Tuple[str, int]] = []
    full_table = spec.full_table

    with open_oracle_connection(spec) as conn:
        try:
            with conn.cursor() as cur:
                use_deterministic = (
                    spec.target == "local"
                    and spec.table_name.upper() == "VSS_HO_GD_NHANKHAU"
                    and _has_deterministic_seed_records(cur, full_table)
                )
                if use_deterministic:
                    _ensure_preconditions_for_deterministic_scenarios(cur, conn, full_table, wanted_keys)
                    for idx, scenario in enumerate(scenarios):
                        cur.execute(scenario.sql.strip())
                        rowcount = cur.rowcount
                        results.append((scenario.name, rowcount))
                        if commit_each:
                            conn.commit()

                        if rowcount == 0:
                            logger.warning(
                                "[%s][%s] %s -> rowcount=0 (dữ liệu đã thay đổi trước đó hoặc không tồn tại)",
                                spec.tag,
                                scenario.key,
                                scenario.name,
                            )
                        else:
                            logger.info(
                                "[%s][%s] %s -> rowcount=%d%s",
                                spec.tag,
                                scenario.key,
                                scenario.name,
                                rowcount,
                                " (committed)" if commit_each else "",
                            )
                        if delay_seconds > 0 and idx < len(scenarios) - 1:
                            time.sleep(delay_seconds)

                    if not commit_each:
                        conn.commit()
                        logger.info("[%s] Committed all %d CDC test scenarios in a single transaction.", spec.tag, len(scenarios))
                else:
                    results.extend(
                        _run_vss_backed_scenarios(
                            cur=cur,
                            conn=conn,
                            spec=spec,
                            scenarios=scenarios,
                            commit_each=commit_each,
                            delay_seconds=delay_seconds,
                            sample_size=sample_size,
                        )
                    )
        except Exception:
            conn.rollback()
            logger.exception("[%s] CDC simulation failed; rolled back uncommitted changes.", spec.tag)
            raise

    if insert_batch_size > 0:
        inserted_batch = run_batch_insert_new_citizens(insert_batch_size, target=spec.target)
        results.append(("SCENARIO_1A_BATCH_INSERT_NEW_CITIZENS", inserted_batch))

    total_rows = sum(r for _, r in results)
    logger.info("[%s] Completed %d scenario(s), total affected rows=%d.", spec.tag, len(results), total_rows)
    return results


_INSERT_VARIANTS = (
    "1A_CLEAN",
    "1A_MISSING_CCCD",
    "1A_BAD_BIRTH_AND_PHONE",
    "1A_BAD_LOCATION_OR_CATALOG",
    "1A_PENDING_APPROVAL",
    "1A_DIVERSE_CATALOG_OR_DECEASED",
)
_HOUSEHOLD_VARIANTS = ("1B_RELOCATE_OTHER_PROVINCE", "1B_NEW_HOUSEHOLD_SAME_PROVINCE")
_FIX_VARIANTS = ("2_FIX_ACTUAL_DEFECTS_FULL", "2_FIX_CCCD_ONLY", "2_FIX_LOCATION_FROM_DBHC")
_INVALID_VARIANTS = ("3A_REJECT_DUYET_4", "3A_MARK_TYPEID_GG", "3A_MARK_TYPEID_GT", "3A_REVERT_PENDING_1")
_APPROVE_VARIANTS = ("3B_APPROVE_DUYET_2", "3B_APPROVE_DUYET_0")


def run_random_cdc_stream(
    interval_seconds: float = 2.0,
    max_events: Optional[int] = None,
    target: str = "local",
    sample_size: int = 200,
) -> int:
    """
    Chạy giả lập biến động CDC ngẫu nhiên liên tục từ dữ liệu thật đọc trên VSS (Local hoặc Cloud),
    bao phủ đủ 8 kịch bản với nhiều biến thể khác biệt nhau.
    """
    spec = build_target_spec(target)
    full_table = spec.full_table
    event_count = 0
    seq = 1

    logger.info(
        "[%s] Starting VSS-backed random CDC generator on %s (interval=%.1fs, max_events=%s, sample_size=%d). Press Ctrl+C to stop.",
        spec.tag,
        full_table,
        interval_seconds,
        max_events if max_events is not None else "unlimited",
        sample_size,
    )

    with open_oracle_connection(spec) as conn:
        try:
            with conn.cursor() as cur:
                ref_ctx = load_vss_reference_context(cur, spec.schema, spec.table_name)
                active_citizens = load_citizen_sample_pool(cur, full_table, sample_size=sample_size)
                logger.info(
                    "[%s] Bootstrapped from VSS: %s | sampled_citizens=%d",
                    spec.tag,
                    ref_ctx.source_summary,
                    len(active_citizens),
                )

                while max_events is None or event_count < max_events:
                    dirty_pool = [
                        c for c in active_citizens
                        if c.is_valid and detect_citizen_dq_defects(c, ref_ctx)
                    ]
                    pending_pool = [c for c in active_citizens if not c.is_valid]
                    multi_ho_pool = [c for c in active_citizens if len(c.households) >= 2 and c.is_valid]
                    valid_pool = [c for c in active_citizens if c.is_valid]

                    if len(active_citizens) < 4:
                        action = "1A_INSERT_NEW_CITIZEN"
                    else:
                        candidates: List[Tuple[str, float]] = [
                            ("1A_INSERT_NEW_CITIZEN", 0.22),
                            ("1B_INSERT_NEW_HOUSEHOLD", 0.14 if valid_pool else 0.0),
                            ("2_UPDATE_FIX_DQ_FLAGS", 0.16 if dirty_pool else 0.0),
                            ("3A_UPDATE_STATUS_TO_INVALID", 0.10 if valid_pool else 0.0),
                            ("3B_UPDATE_APPROVE_PENDING", 0.12 if pending_pool else 0.0),
                            ("4_CHANGE_MA_CD", 0.10 if valid_pool else 0.0),
                            ("5A_DELETE_LATEST_HOUSEHOLD", 0.08 if multi_ho_pool else 0.0),
                            ("5B_DELETE_ALL_RECORDS", 0.08 if len(active_citizens) > 5 else 0.0),
                        ]
                        names = [c[0] for c in candidates if c[1] > 0]
                        weights = [c[1] for c in candidates if c[1] > 0]
                        action = random.choices(names, weights=weights, k=1)[0]

                    if action == "1A_INSERT_NEW_CITIZEN":
                        variant = _INSERT_VARIANTS[(seq - 1) % len(_INSERT_VARIANTS)]
                        new_rec, rowcount, summary = execute_insert_new_citizen(
                            cur, full_table, seq=seq, variant=variant, ref_ctx=ref_ctx
                        )
                        seq += 1
                        conn.commit()
                        active_citizens.append(new_rec)
                        logger.info("[%s][RANDOM #%d][%s] INSERT -> rowcount=%d | %s", spec.tag, event_count + 1, variant, rowcount, summary)

                    elif action == "1B_INSERT_NEW_HOUSEHOLD":
                        chosen = random.choice(valid_pool)
                        variant = random.choice(_HOUSEHOLD_VARIANTS)
                        rowcount, summary = execute_insert_new_household(
                            cur, full_table, target=chosen, ref_ctx=ref_ctx, variant=variant
                        )
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][%s] INSERT_HO -> rowcount=%d | %s", spec.tag, event_count + 1, variant, rowcount, summary)

                    elif action == "2_UPDATE_FIX_DQ_FLAGS":
                        chosen = random.choice(dirty_pool)
                        defects = detect_citizen_dq_defects(chosen, ref_ctx)
                        if "KHONG_SO_DINH_DANH" in defects and len(defects) == 1:
                            variant = "2_FIX_CCCD_ONLY"
                        elif "TINH_XA_KHONG_MAP" in defects and len(defects) == 1:
                            variant = "2_FIX_LOCATION_FROM_DBHC"
                        else:
                            variant = random.choice(_FIX_VARIANTS) if event_count % 3 == 0 else "2_FIX_ACTUAL_DEFECTS_FULL"
                        rowcount, summary = execute_fix_dirty_citizen(
                            cur, full_table, target=chosen, ref_ctx=ref_ctx, variant=variant
                        )
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][%s] UPDATE_DQ -> rowcount=%d | %s", spec.tag, event_count + 1, variant, rowcount, summary)

                    elif action == "3A_UPDATE_STATUS_TO_INVALID":
                        chosen = random.choice(valid_pool)
                        variant = random.choice(_INVALID_VARIANTS)
                        rowcount, summary = execute_mark_status_invalid(
                            cur, full_table, target=chosen, variant=variant
                        )
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][%s] UPDATE_FILTER -> rowcount=%d | %s", spec.tag, event_count + 1, variant, rowcount, summary)

                    elif action == "3B_UPDATE_APPROVE_PENDING":
                        chosen = random.choice(pending_pool)
                        variant = random.choice(_APPROVE_VARIANTS)
                        rowcount, summary = execute_approve_pending(
                            cur, full_table, target=chosen, variant=variant
                        )
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][%s] UPDATE_APPROVE -> rowcount=%d | %s", spec.tag, event_count + 1, variant, rowcount, summary)

                    elif action == "4_CHANGE_MA_CD":
                        chosen = random.choice(valid_pool)
                        rowcount, summary = execute_change_ma_cd(cur, full_table, target=chosen, seq=seq)
                        seq += 1
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][4_CHANGE_MA_CD] UPDATE_KEY -> rowcount=%d | %s", spec.tag, event_count + 1, rowcount, summary)

                    elif action == "5A_DELETE_LATEST_HOUSEHOLD":
                        chosen = random.choice(multi_ho_pool)
                        rowcount, summary = execute_delete_latest_household(cur, full_table, target=chosen)
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][5A_DELETE_LATEST_HO] DELETE_HO -> rowcount=%d | %s", spec.tag, event_count + 1, rowcount, summary)

                    elif action == "5B_DELETE_ALL_RECORDS":
                        sim_first = [i for i, c in enumerate(active_citizens) if not c.from_vss]
                        idx = random.choice(sim_first) if sim_first else random.randrange(len(active_citizens))
                        chosen = active_citizens.pop(idx)
                        rowcount, summary = execute_delete_all_records(cur, full_table, target=chosen)
                        conn.commit()
                        logger.info("[%s][RANDOM #%d][5B_DELETE_CITIZEN] DELETE_ALL -> rowcount=%d | %s", spec.tag, event_count + 1, rowcount, summary)

                    event_count += 1
                    if max_events is None or event_count < max_events:
                        time.sleep(max(0.0, interval_seconds))
        except KeyboardInterrupt:
            logger.info("[%s] Stopped random CDC generator after %d event(s).", spec.tag, event_count)

    return event_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute CDC test scenarios against Oracle Local or Oracle Cloud Autonomous DB."
    )
    parser.add_argument(
        "--target",
        type=str,
        choices=VALID_TARGETS,
        default="local",
        help="Đích ghi dữ liệu CDC: 'local' (SOURCE_DB) hoặc 'cloud' (FULLLOAD_DB + Wallet). Default: local",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Số công dân mẫu đọc từ bảng VSS làm dữ liệu nền (cho --random và chế độ tự chọn VSS). Default: 200",
    )
    parser.add_argument(
        "--scenarios",
        type=str,
        default="all",
        help="Comma-separated scenario keys to run (e.g. 1A,1B,2,3A,3B,4,5A,5B or all). Default: all",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between scenarios (useful for CDC event separation). Default: 0",
    )
    parser.add_argument(
        "--single-tx",
        action="store_true",
        help="Run all selected scenarios in a single transaction instead of committing each",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=0,
        help="Sinh thêm một lô công dân mới (ví dụ 1000 hoặc 50000 dòng) theo yêu cầu 1A mở rộng. Default: 0",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print target configuration and selected scenarios without executing writes on Oracle",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Run continuous random CDC events covering all 8 scenarios with VSS-backed variants",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between random CDC events when --random is set. Default: 2.0",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N random events when --random is set. Default: unlimited",
    )
    return parser.parse_args()


_CLOUD_SCENARIO_SUMMARIES = {
    "1A": "INSERT công dân mới dùng danh mục thật từ VSS (DBHC, Dân tộc, Quốc tịch)",
    "1B": "Chọn công dân hợp lệ từ VSS để thêm dòng hộ mới ở địa bàn khác từ VSS_DM_DBHC",
    "2": "Chọn công dân đang có lỗi DQ từ VSS và sửa đúng các trường lỗi -> sạch",
    "3A": "Chọn công dân hợp lệ từ VSS và chuyển sang trạng thái bị lọc (TYPEID='GG', DUYET=4)",
    "3B": "Chọn hồ sơ đang chờ duyệt (TRANG_THAI_DUYET=1) từ VSS để duyệt hợp lệ (DUYET=2)",
    "4": "Chọn công dân hợp lệ từ VSS và đổi MA_CD -> mã mới (giữ MA_CD_OLD)",
    "5A": "Xóa dòng hộ mới nhất của công dân có >= 2 hộ từ VSS -> lùi về hộ cũ",
    "5B": "Xóa toàn bộ dòng của một công dân trong tập sample/simulator từ VSS",
}


if __name__ == "__main__":
    args = parse_args()

    if args.dry_run:
        if args.target == "cloud":
            schema_preview = FULLLOAD_DB.schema or "VSS"
            table_preview = FULLLOAD_DB.table or "VSS_HO_GD_NHANKHAU"
            logger.info(
                "[Dry-Run][CLOUD] Target=%s.%s, TNS=%s, Wallet=%s, sample_size=%d",
                schema_preview,
                table_preview,
                FULLLOAD_DB.tns_alias or "(auto *_high)",
                FULLLOAD_DB.wallet_location or "(unset)",
                args.sample_size,
            )
        else:
            logger.info(
                "[Dry-Run][LOCAL] Target=%s.VSS_HO_GD_NHANKHAU, DSN=%s, sample_size=%d",
                SOURCE_DB.schema,
                SOURCE_DB.dsn,
                args.sample_size,
            )

        if args.random:
            logger.info(
                "[Dry-Run] Random VSS-backed stream: interval=%.1fs, max_events=%s, insert_variants=%s",
                args.interval,
                args.max_events if args.max_events is not None else "unlimited",
                ", ".join(_INSERT_VARIANTS),
            )
        else:
            keys = [k.strip() for k in args.scenarios.split(",")] if args.scenarios else None
            selected = filter_scenarios(build_cdc_scenarios(), keys)
            logger.info("[Dry-Run] Prepared %d CDC test scenario(s):", len(selected))
            for s in selected:
                desc = _CLOUD_SCENARIO_SUMMARIES.get(s.key, s.description) if args.target == "cloud" else s.description
                logger.info("  [%s] %s - %s", s.key, s.name, desc)
    elif args.random:
        run_random_cdc_stream(
            interval_seconds=args.interval,
            max_events=args.max_events,
            target=args.target,
            sample_size=args.sample_size,
        )
    else:
        keys = [k.strip() for k in args.scenarios.split(",")] if args.scenarios else None
        run_cdc_simulation(
            selected_keys=keys,
            commit_each=not args.single_tx,
            delay_seconds=args.delay,
            insert_batch_size=args.insert_batch_size,
            target=args.target,
            sample_size=args.sample_size,
        )
