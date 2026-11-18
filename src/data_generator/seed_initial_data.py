"""
Phần 1.1 - Script 1: Initial Seed Generator

Sinh dữ liệu cho 3 bảng danh mục và bảng chính VSS_HO_GD_NHANKHAU.

Nguyên tắc thiết kế:
1. Có tập kịch bản bắt buộc (deterministic coverage) để bảo đảm các trường hợp biên
   xuất hiện khi sinh ít nhất 24 công dân logic.
2. Phần dữ liệu còn lại được sinh ngẫu nhiên có trọng số để mô phỏng phân bố thực tế.
3. Có seed để tái lập cùng một bộ dữ liệu khi debug.
4. Dry-run in thống kê chất lượng dữ liệu mà không kết nối Oracle.

Lưu ý:
- Các mã/địa bàn trong script là dữ liệu kiểm thử mô phỏng theo kịch bản thiết kế,
  không thay thế danh mục hành chính chính thức.
- Script giữ nguyên tên cột và kiểu giá trị theo phiên bản code người dùng cung cấp.
  Cần đối chiếu DDL thực tế trước khi chạy --truncate hoặc nạp dữ liệu.
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

try:
    import oracledb
except ImportError:  # Cho phép chạy --dry-run trên máy chưa cài Oracle driver.
    oracledb = None  # type: ignore[assignment]

from src.config import SEED_NUM_CITIZENS, SEED_BATCH_SIZE, SOURCE_DB


DEFAULT_SEED = 20261118
BASE_TIME = datetime(2026, 1, 1, 8, 0, 0)


# -----------------------------------------------------------------------------
# 1. Danh mục tham chiếu
# -----------------------------------------------------------------------------

def get_seed_categories() -> Dict[str, List[Dict[str, Any]]]:
    """Trả về dữ liệu mẫu cho 3 bảng danh mục.

    Các mã mở rộng chủ yếu nhằm kiểm thử join, chuẩn hóa mã và xử lý NULL/invalid.
    Không coi toàn bộ danh mục này là danh mục pháp lý đầy đủ.
    """
    dan_toc = [
        {"ID": 1, "CAN_CU": "QĐ 121-TCTK", "MA": "01", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Kinh"},
        {"ID": 2, "CAN_CU": "QĐ 121-TCTK", "MA": "02", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Tày"},
        {"ID": 3, "CAN_CU": "QĐ 121-TCTK", "MA": "03", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Thái"},
        {"ID": 4, "CAN_CU": "SEED-TEST", "MA": "04", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Hoa"},
        {"ID": 5, "CAN_CU": "SEED-TEST", "MA": "05", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Khmer"},
        {"ID": 6, "CAN_CU": "SEED-TEST", "MA": "06", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Mường"},
        {"ID": 7, "CAN_CU": "SEED-TEST", "MA": "07", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Nùng"},
        {"ID": 8, "CAN_CU": "SEED-TEST", "MA": "08", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Dao"},
    ]

    quoc_tich = [
        {"ID": 233, "MA": "VN", "TEN": "Việt Nam"},
        {"ID": 100, "MA": "LA", "TEN": "Lào"},
        {"ID": 101, "MA": "KR", "TEN": "Hàn Quốc"},
        {"ID": 102, "MA": "JP", "TEN": "Nhật Bản"},
        {"ID": 103, "MA": "CN", "TEN": "Trung Quốc"},
        {"ID": 104, "MA": "US", "TEN": "Hoa Kỳ"},
        {"ID": 105, "MA": "SG", "TEN": "Singapore"},
        {"ID": 106, "MA": "FR", "TEN": "Pháp"},
    ]

    effective_from = datetime(2025, 7, 1, 0, 0, 0)
    effective_to = datetime(2025, 6, 30, 23, 59, 59)
    dbhc = [
        # Tỉnh/thành hiện hành trong bộ dữ liệu mô phỏng.
        {"ID": 1, "MA_DBHC": "01TTT", "MA_CHA": None, "TEN": "Thành phố Hà Nội (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "01;28", "IS_CAP_XA": 0, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},
        {"ID": 2, "MA_DBHC": "33TTT", "MA_CHA": None, "TEN": "Tỉnh Hưng Yên (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "33;34", "IS_CAP_XA": 0, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},
        {"ID": 3, "MA_DBHC": "79TTT", "MA_CHA": None, "TEN": "Thành phố Hồ Chí Minh (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "79;74;77", "IS_CAP_XA": 0, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},

        # Xã/phường hiện hành.
        {"ID": 101, "MA_DBHC": "00004", "MA_CHA": "01TTT", "TEN": "Phường Ba Đình Mới (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "00013;00004;00019;00099", "IS_CAP_XA": 1, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},
        {"ID": 102, "MA_DBHC": "00008", "MA_CHA": "01TTT", "TEN": "Phường Ngọc Hà Mới (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "00008;00099", "IS_CAP_XA": 1, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},
        {"ID": 103, "MA_DBHC": "12457", "MA_CHA": "33TTT", "TEN": "Xã Phố Hiến Mới (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "12457;12458", "IS_CAP_XA": 1, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},
        {"ID": 104, "MA_DBHC": "26734", "MA_CHA": "79TTT", "TEN": "Phường Sài Gòn Mới (mẫu)", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "26734;26737", "IS_CAP_XA": 1, "NGAY_HIEU_LUC": effective_from, "NGAY_HET_HIEU_LUC": None},

        # Mã cũ có thời điểm hết hiệu lực, dùng để test tra cứu lịch sử.
        {"ID": 201, "MA_DBHC": "00013", "MA_CHA": "01TTT", "TEN": "Mã xã cũ 00013 (mẫu)", "TINH_TRANG": "0", "STATUS": "0", "MA_DBHC_SN": None, "IS_CAP_XA": 1, "NGAY_HIEU_LUC": datetime(2010, 1, 1), "NGAY_HET_HIEU_LUC": effective_to},
        {"ID": 202, "MA_DBHC": "00019", "MA_CHA": "01TTT", "TEN": "Mã xã cũ 00019 (mẫu)", "TINH_TRANG": "0", "STATUS": "0", "MA_DBHC_SN": None, "IS_CAP_XA": 1, "NGAY_HIEU_LUC": datetime(2010, 1, 1), "NGAY_HET_HIEU_LUC": effective_to},
        {"ID": 203, "MA_DBHC": "12458", "MA_CHA": "33TTT", "TEN": "Mã xã cũ 12458 (mẫu)", "TINH_TRANG": "0", "STATUS": "0", "MA_DBHC_SN": None, "IS_CAP_XA": 1, "NGAY_HIEU_LUC": datetime(2010, 1, 1), "NGAY_HET_HIEU_LUC": effective_to},
        {"ID": 204, "MA_DBHC": "26737", "MA_CHA": "79TTT", "TEN": "Mã xã cũ 26737 (mẫu)", "TINH_TRANG": "0", "STATUS": "0", "MA_DBHC_SN": None, "IS_CAP_XA": 1, "NGAY_HIEU_LUC": datetime(2010, 1, 1), "NGAY_HET_HIEU_LUC": effective_to},
    ]

    return {"dan_toc": dan_toc, "quoc_tich": quoc_tich, "dbhc": dbhc}


# -----------------------------------------------------------------------------
# 2. Các kịch bản dữ liệu bắt buộc
# -----------------------------------------------------------------------------

# Cố ý có các cặp tỉnh/xã đúng, mã cũ, mã thiếu số 0, mã không tồn tại,
# mã không xác định và trường hợp tỉnh không khớp với xã.
LOCATION_CASES: List[Tuple[Optional[str], Optional[str]]] = [
    ("01TTT", "00004"),
    ("01TTT", "00013"),
    ("01TTT", "00099"),
    ("01TTT", "00008"),
    ("01TTT", "4"),
    ("01TTT", "13"),
    ("33TTT", "12457"),
    ("33TTT", "12458"),
    ("34TTT", "12458"),
    ("79TTT", "26734"),
    ("79TTT", "26737"),
    ("79TTT", "00004"),  # Tỉnh lệch xã
    ("001", "00004"),
    ("034", "12458"),
    ("079", "26734"),
    ("01", "4"),
    ("1", "13"),
    ("34", "12457"),  # Tỉnh cũ + xã thuộc tỉnh khác
    ("79", "00099"),
    ("00TTT", "00000"),
    ("000", "99999"),
    ("00", None),
    ("99TTT", "99999"),
    (None, None),
]

BIRTH_DATE_CASES: List[Optional[int]] = [
    19950815,  # YYYYMMDD hợp lệ
    19881120,  # YYYYMMDD hợp lệ
    19950000,  # Chỉ có năm dạng YYYY0000
    1995,      # Chỉ có năm dạng YYYY
    19950800,  # YYYYMM00
    199508,    # YYYYMM
    19950230,  # Ngày không tồn tại
    17500101,  # Ngoài khoảng năm sinh hợp lý
    None,
]

PHONE_CASES: List[Optional[str]] = [
    "0912345678",      # 10 số chuẩn
    "84912345678",     # Đầu 84
    "840912345678",    # Đầu 840 bất thường
    "912345678",       # Thiếu số 0 đầu
    "0912 345 678",    # Có khoảng trắng
    "12345",           # Sai độ dài
    "09123ABCDE",      # Chứa chữ
    None,
]

NATIONALITY_CASES: List[Optional[str]] = ["VN", "233", "LA", "KR", "XX", None, "vn", " VN "]
ETHNICITY_CASES: List[Optional[int]] = [1, 2, 3, 4, 5, 9999, None]

# Mỗi loại giấy tờ đều có ít nhất một nhóm bản ghi riêng để kiểm thử.
DOCUMENT_CASES = ["CCCD_12", "CMTND_12", "CMND_9", "BOTH_NULL"]

# Bảo đảm tất cả nhánh lọc TYPEID và TRANG_THAI_DUYET đều được tạo.
FILTER_CASES: List[Tuple[Optional[str], Optional[int]]] = [
    ("GT", 0),
    ("GM", 0),
    ("GG", 2),
    ("GK", None),
    (None, 1),
    (None, 3),
    (None, 4),
    (None, 0),  # Không thuộc diện loại theo hai điều kiện trên
    (None, 2),  # Không thuộc diện loại theo hai điều kiện trên
]


# -----------------------------------------------------------------------------
# 3. Hàm sinh bản ghi
# -----------------------------------------------------------------------------

def _weighted_choice(
    rng: random.Random,
    choices: Sequence[Tuple[Any, float]],
) -> Any:
    """Chọn theo trọng số và kiểm tra tổng trọng số để tránh cấu hình sai."""
    values = [item[0] for item in choices]
    weights = [item[1] for item in choices]
    if not choices or any(weight < 0 for weight in weights):
        raise ValueError("Danh sách lựa chọn/trọng số không hợp lệ.")
    if abs(sum(weights) - 1.0) > 1e-8:
        raise ValueError(f"Tổng trọng số phải bằng 1.0, hiện tại là {sum(weights)}")
    return rng.choices(values, weights=weights, k=1)[0]


def _make_document_numbers(citizen_no: int, case_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Sinh số giấy tờ có độ dài ổn định, không phụ thuộc số lượng seed."""
    number_12 = f"{citizen_no % (10 ** 12):012d}"
    number_9 = f"{citizen_no % (10 ** 9):09d}"

    if case_name == "CCCD_12":
        return number_12, None
    if case_name == "CMTND_12":
        return None, number_12
    if case_name == "CMND_9":
        return None, number_9
    if case_name == "BOTH_NULL":
        return None, None
    raise ValueError(f"Loại giấy tờ không được hỗ trợ: {case_name}")


def _build_household_plan(num_citizens: int, rng: random.Random) -> Tuple[set[int], set[int]]:
    """Chọn chính xác xấp xỉ tỷ lệ công dân nhiều hộ.

    Mặc định mục tiêu 6% công dân có từ 2-3 hộ; trong đó khoảng 1% tổng số
    công dân có 3 hộ. Công dân 2 và 3 được giữ làm ca kiểm thử bắt buộc.
    Với bộ seed rất nhỏ, ưu tiên coverage nên tỷ lệ có thể cao hơn 6%.
    """
    if num_citizens < 2:
        return set(), set()

    target_multi = max(1, round(num_citizens * 0.06))
    if num_citizens >= 3:
        target_multi = max(2, target_multi)
    target_multi = min(target_multi, num_citizens - 1)

    multi_indices = {2}
    if num_citizens >= 3:
        multi_indices.add(3)

    remaining = [i for i in range(1, num_citizens + 1) if i not in multi_indices]
    need_more = max(0, target_multi - len(multi_indices))
    if need_more:
        multi_indices.update(rng.sample(remaining, k=min(need_more, len(remaining))))

    target_three = max(1, round(num_citizens * 0.01)) if num_citizens >= 3 else 0
    target_three = min(target_three, len(multi_indices))
    three_indices = {3} if num_citizens >= 3 else set()
    remaining_multi = sorted(multi_indices - three_indices)
    need_three = max(0, target_three - len(three_indices))
    if need_three:
        three_indices.update(rng.sample(remaining_multi, k=min(need_three, len(remaining_multi))))

    return multi_indices, three_indices


def _get_household_count(citizen_no: int, multi_indices: set[int], three_indices: set[int]) -> int:
    if citizen_no in three_indices:
        return 3
    if citizen_no in multi_indices:
        return 2
    return 1


def _make_household_code(citizen_no: int, household_idx: int) -> Optional[str]:
    """Tạo MA_HO_GD, gồm NULL và literal UNKNOWN để test lỗi NVL/COALESCE."""
    # Cùng một MA_CD nhưng một dòng NULL, một dòng 'UNKNOWN': cố ý tạo collision
    # khi logic downstream dùng NVL(MA_HO_GD, 'UNKNOWN').
    if citizen_no == 2:
        return None if household_idx == 0 else "UNKNOWN"

    if citizen_no % 50 == 0 and household_idx == 0:
        return None
    if citizen_no % 97 == 0 and household_idx == 0:
        return "UNKNOWN"
    return f"HGD_{citizen_no * 10 + household_idx:010d}"


def _make_filtered_values(
    citizen_no: int,
    household_idx: int,
    rng: random.Random,
) -> Tuple[Optional[str], Optional[int]]:
    """Ép các nhánh lọc ở nhóm đầu, các dòng còn lại có tỷ lệ lọc khoảng 3%."""
    if household_idx == 0 and citizen_no <= len(FILTER_CASES):
        return FILTER_CASES[citizen_no - 1]

    if rng.random() < 0.03:
        if rng.random() < 0.5:
            return rng.choice(["GT", "GM", "GG", "GK"]), rng.choice([0, 2, None])
        return None, rng.choice([1, 3, 4])

    return None, rng.choice([0, 2, None])


def _make_address_values(
    citizen_no: int,
    rng: random.Random,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Trả về địa chỉ thường trú, liên hệ và khai sinh.

    24 công dân đầu tiên được gán cặp tỉnh/xã cố định để bảo đảm phủ mã biên.
    Phần còn lại dùng phân bố có trọng số, nghiêng về các mã đang hoạt động.
    """
    if citizen_no <= len(LOCATION_CASES):
        idx = citizen_no - 1
        ma_tinh, ma_xa = LOCATION_CASES[idx]
    else:
        province_choices = [
            ("01TTT", 0.38), ("33TTT", 0.14), ("34TTT", 0.06),
            ("001", 0.06), ("034", 0.04), ("079", 0.04),
            ("01", 0.04), ("34", 0.03), ("79", 0.03),
            ("1", 0.04), ("79TTT", 0.05), ("00TTT", 0.02),
            ("000", 0.02), ("00", 0.01), ("99TTT", 0.02), (None, 0.02),
        ]
        commune_choices = [
            ("00004", 0.44), ("00013", 0.14), ("4", 0.07),
            ("13", 0.07), ("00099", 0.06), ("12457", 0.08),
            ("12458", 0.05), ("00000", 0.03), ("99999", 0.03), (None, 0.03),
        ]
        ma_tinh = _weighted_choice(rng, province_choices)
        ma_xa = _weighted_choice(rng, commune_choices)

    if citizen_no <= len(LOCATION_CASES):
        idx = citizen_no - 1
        lh_pair = LOCATION_CASES[(idx + 5) % len(LOCATION_CASES)] if citizen_no % 2 == 0 else (ma_tinh, ma_xa)
        ks_pair = LOCATION_CASES[(idx + 9) % len(LOCATION_CASES)] if citizen_no % 3 == 0 else (ma_tinh, ma_xa)
    else:
        lh_pair = (
            (rng.choice(LOCATION_CASES) if rng.random() < 0.20 else (ma_tinh, ma_xa))
        )
        ks_pair = (
            (rng.choice(LOCATION_CASES) if rng.random() < 0.20 else (ma_tinh, ma_xa))
        )

    return ma_tinh, ma_xa, lh_pair[0], lh_pair[1], ks_pair[0], ks_pair[1]


def _make_person_name(citizen_no: int) -> Tuple[str, str]:
    """Sinh tên chuẩn, tên thừa khoảng trắng, tab và tên không dấu."""
    variants = {
        0: ("  Nguyễn   Văn  An ", "  NGUYEN   VAN  AN "),
        1: ("Nguyễn\tVăn An", "NGUYEN\tVAN AN"),
        2: ("  Trần Thị Bình  ", "  TRAN THI BINH  "),
        3: ("Đỗ  Minh   Tuấn", "DO  MINH   TUAN"),
    }
    if citizen_no % 10 in variants:
        return variants[citizen_no % 10]
    return f"Nguyễn Văn Công Dân {citizen_no}", f"NGUYEN VAN CONG DAN {citizen_no}"


def generate_citizen_records(
    num_citizens: int = SEED_NUM_CITIZENS,
    seed: int = DEFAULT_SEED,
) -> List[Dict[str, Any]]:
    """Sinh dữ liệu công dân; num_citizens là số MA_CD logic, không phải số dòng.

    Tổng số dòng có thể lớn hơn num_citizens do một công dân có thể xuất hiện ở
    nhiều hộ gia đình.
    """
    if num_citizens <= 0:
        raise ValueError("num_citizens phải lớn hơn 0.")

    rng = random.Random(seed)
    records: List[Dict[str, Any]] = []
    multi_household_indices, three_household_indices = _build_household_plan(num_citizens, rng)

    for citizen_no in range(1, num_citizens + 1):
        ma_cd = f"{citizen_no:010d}"
        household_count = _get_household_count(citizen_no, multi_household_indices, three_household_indices)
        same_dates_for_tie = citizen_no in (2, 3) and household_count > 1

        birth_date = (
            BIRTH_DATE_CASES[(citizen_no - 1) % len(BIRTH_DATE_CASES)]
            if citizen_no <= len(BIRTH_DATE_CASES)
            else _weighted_choice(rng, [
            (19950815, 0.30), (19881120, 0.25), (19950000, 0.10),
            (1995, 0.03), (19950800, 0.08), (199508, 0.05),
            (19950230, 0.05), (17500101, 0.02), (None, 0.12),
        ])
        )
        phone = (
            PHONE_CASES[(citizen_no - 1) % len(PHONE_CASES)]
            if citizen_no <= len(PHONE_CASES)
            else _weighted_choice(rng, [
            ("0912345678", 0.65), ("84912345678", 0.07),
            ("840912345678", 0.01), ("912345678", 0.07),
            ("0912 345 678", 0.08), ("12345", 0.03),
            ("09123ABCDE", 0.02), (None, 0.07),
        ])
        )
        nationality = (
            NATIONALITY_CASES[(citizen_no - 1) % len(NATIONALITY_CASES)]
            if citizen_no <= len(NATIONALITY_CASES)
            else rng.choices(
                ["VN", "233", "LA", "KR", "JP", "CN", "US", "SG", "FR", "XX", None],
                weights=[0.50, 0.20, 0.05, 0.05, 0.04, 0.04, 0.03, 0.02, 0.01, 0.03, 0.03],
                k=1,
            )[0]
        )
        ethnicity_id = (
            ETHNICITY_CASES[(citizen_no - 1) % len(ETHNICITY_CASES)]
            if citizen_no <= len(ETHNICITY_CASES)
            else rng.choices(
                [1, 2, 3, 4, 5, 6, 7, 8, 9999, None],
                weights=[0.70, 0.10, 0.06, 0.04, 0.03, 0.02, 0.02, 0.01, 0.01, 0.01],
                k=1,
            )[0]
        )

        if citizen_no <= len(DOCUMENT_CASES):
            doc_case = DOCUMENT_CASES[citizen_no - 1]
        else:
            doc_case = _weighted_choice(rng, [
                ("CCCD_12", 0.82), ("CMTND_12", 0.06),
                ("CMND_9", 0.06), ("BOTH_NULL", 0.06),
            ])
        so_cccd, so_cmtnd = _make_document_numbers(citizen_no, doc_case)
        ma_tinh, ma_xa, ma_tinh_lh, ma_xa_lh, ma_tinh_ks, ma_xa_ks = _make_address_values(citizen_no, rng)
        ho_ten, ho_ten_unsigned = _make_person_name(citizen_no)

        for household_idx in range(household_count):
            ma_ho_gd = _make_household_code(citizen_no, household_idx)

            # Citizen 2 có cùng MODIFY_DATE và CREATED_DATE giữa hai dòng;
            # khi đó thuật toán chọn bản ghi phải dùng tiếp ID DESC để phá hòa.
            if same_dates_for_tie:
                created_dt = BASE_TIME + timedelta(days=2)
                mod_date = BASE_TIME + timedelta(days=15, minutes=30)
            else:
                created_dt = BASE_TIME + timedelta(days=citizen_no % 365, hours=household_idx)
                mod_date = BASE_TIME + timedelta(days=(citizen_no % 90) + household_idx * 10, minutes=rng.randint(1, 120))

            type_id, trang_thai_duyet = _make_filtered_values(citizen_no, household_idx, rng)
            is_dead = type_id == "GT"

            # Một số dòng cố ý có độ lệch giữa cờ xác thực và số CCCD để test rule.
            if citizen_no % 13 == 0:
                xac_thuc_bca = 1 if so_cccd is None else 0
            else:
                xac_thuc_bca = 1 if so_cccd is not None else 0

            # Trường hợp các hộ khác nhau có thông tin địa chỉ liên hệ khác nhau.
            dc_lien_lac = f"Số {citizen_no}, Đường Liên Lạc"
            if household_idx > 0:
                dc_lien_lac = f"Số {citizen_no}, Đường Liên Lạc - Hộ {household_idx + 1}"

            record = {
                "ID": f"{ma_cd}_{household_idx + 1:02d}",
                "MA_CD": ma_cd,
                "MA_CD_OLD": f"OLD{citizen_no:010d}" if citizen_no % 17 == 0 else None,
                "MA_HO_GD": ma_ho_gd,
                "HO_TEN": ho_ten,
                "HO_TEN_UNSIGNED": ho_ten_unsigned,
                "GIOI_TINH": (
                    ["1", "2", None, "0"][(citizen_no - 1) % 4]
                    if citizen_no <= 4 else rng.choices(["1", "2", None, "0"], weights=[0.49, 0.49, 0.01, 0.01], k=1)[0]
                ),
                "NGAY_SINH": birth_date,
                "SO_CCCD": so_cccd,
                "SO_CMTND": so_cmtnd,
                "PHONE_NUMBER": phone,
                "EMAIL_ACC": (
                    f"congdan{citizen_no}@vss.gov.vn" if citizen_no % 3 == 0 else
                    f" CONGDAN{citizen_no}@VSS.GOV.VN " if citizen_no % 29 == 0 else
                    "email-sai-dinh-dang" if citizen_no % 31 == 0 else None
                ),
                "DIA_CHI": f"Số {citizen_no}, Tổ dân phố {household_idx + 1}" if citizen_no % 23 != 0 else None,
                "DC_LIEN_LAC": dc_lien_lac if citizen_no % 19 != 0 else None,
                "ISCHU_HO": "1" if household_idx == 0 else "0",
                "QH_CHU_HO": "00" if household_idx == 0 else rng.choice(["01", "02", "03", None]),
                "TEN_QH_CHUHO": "Chủ hộ" if household_idx == 0 else rng.choice(["Vợ/Chồng", "Con", "Cha/Mẹ", None]),
                "MA_TINH": ma_tinh,
                "MA_XA": ma_xa,
                "MA_TINH_LH": ma_tinh_lh,
                "MA_XA_LH": ma_xa_lh,
                "MA_TINH_KS": ma_tinh_ks,
                "MA_XA_KS": ma_xa_ks,
                "QUOC_TICH": nationality,
                "DM_DAN_TOC_ID": ethnicity_id,
                "IS_ALIVE": "0" if is_dead else "1",
                "DEAD_DATE": 20251201 if is_dead else None,
                "XAC_THUC_BCA": xac_thuc_bca,
                "TYPEID": type_id,
                "TRANG_THAI_DUYET": trang_thai_duyet,
                "CREATED_DATE": created_dt,
                "MODIFY_DATE": mod_date,
            }
            records.append(record)

    return records


# -----------------------------------------------------------------------------
# 4. Batch, thống kê và kiểm tra nhanh
# -----------------------------------------------------------------------------

def chunked_records(
    records: List[Dict[str, Any]],
    batch_size: int = SEED_BATCH_SIZE,
) -> Iterator[List[Dict[str, Any]]]:
    if batch_size <= 0:
        raise ValueError("batch_size phải lớn hơn 0.")
    for idx in range(0, len(records), batch_size):
        yield records[idx:idx + batch_size]


def summarize_records(records: List[Dict[str, Any]], num_citizens: int) -> Dict[str, Any]:
    """Thống kê để xác nhận dữ liệu bẩn và kịch bản biên đã được sinh."""
    citizen_counter = Counter(row["MA_CD"] for row in records)
    household_counter = Counter(row["MA_HO_GD"] for row in records)

    document_profiles = Counter()
    for row in records:
        cccd = row["SO_CCCD"]
        cmtnd = row["SO_CMTND"]
        if cccd is not None and len(cccd) == 12 and cmtnd is None:
            document_profiles["CCCD_12_ONLY"] += 1
        elif cccd is None and cmtnd is not None and len(cmtnd) == 12:
            document_profiles["CMTND_12_ONLY"] += 1
        elif cccd is None and cmtnd is not None and len(cmtnd) == 9:
            document_profiles["CMND_9_ONLY"] += 1
        elif cccd is None and cmtnd is None:
            document_profiles["BOTH_NULL"] += 1
        else:
            document_profiles["OTHER_OR_MULTIPLE"] += 1

    return {
        "logical_citizens_requested": num_citizens,
        "distinct_MA_CD_generated": len(citizen_counter),
        "total_rows_generated": len(records),
        "citizens_in_multiple_rows": sum(1 for count in citizen_counter.values() if count > 1),
        "rows_with_null_MA_HO_GD": sum(row["MA_HO_GD"] is None for row in records),
        "rows_with_literal_UNKNOWN_MA_HO_GD": sum(row["MA_HO_GD"] == "UNKNOWN" for row in records),
        "rows_with_null_NGAY_SINH": sum(row["NGAY_SINH"] is None for row in records),
        "birth_date_values": dict(Counter(str(row["NGAY_SINH"]) if row["NGAY_SINH"] is not None else "NULL" for row in records)),
        "rows_with_null_phone": sum(row["PHONE_NUMBER"] is None for row in records),
        "document_profiles": dict(document_profiles),
        "rows_with_null_email": sum(row["EMAIL_ACC"] is None for row in records),
        "rows_with_null_both_documents": sum(row["SO_CCCD"] is None and row["SO_CMTND"] is None for row in records),
        "rows_with_TYPEID_filter_values": sum(row["TYPEID"] in {"GT", "GM", "GG", "GK"} for row in records),
        "rows_with_approval_filter_values": sum(row["TRANG_THAI_DUYET"] in {1, 3, 4} for row in records),
        "filtered_rows_union": sum(
            row["TYPEID"] in {"GT", "GM", "GG", "GK"} or row["TRANG_THAI_DUYET"] in {1, 3, 4}
            for row in records
        ),
        "nationality_values": dict(Counter(str(row["QUOC_TICH"]) if row["QUOC_TICH"] is not None else "NULL" for row in records)),
        "ethnicity_ids": dict(Counter(str(row["DM_DAN_TOC_ID"]) if row["DM_DAN_TOC_ID"] is not None else "NULL" for row in records)),
        "gender_values": dict(Counter(str(row["GIOI_TINH"]) if row["GIOI_TINH"] is not None else "NULL" for row in records)),
        "province_codes": sorted({row["MA_TINH"] for row in records if row["MA_TINH"] is not None}),
        "commune_codes": sorted({row["MA_XA"] for row in records if row["MA_XA"] is not None}),
        "rows_with_tied_created_and_modified_dates_for_citizen_2": sum(
            1 for row in records
            if row["MA_CD"] == f"{2:010d}" and row["CREATED_DATE"] == BASE_TIME + timedelta(days=2)
            and row["MODIFY_DATE"] == BASE_TIME + timedelta(days=15, minutes=30)
        ),
        "household_code_distinct_count_including_null": len(household_counter),
    }


def print_dry_run_report(
    categories: Dict[str, List[Dict[str, Any]]],
    records: List[Dict[str, Any]],
    num_citizens: int,
    batch_size: int,
    seed: int,
) -> None:
    summary = summarize_records(records, num_citizens)
    print("\n========== INITIAL SEED DRY-RUN ==========")
    print(f"Seed: {seed}")
    print(f"Danh mục: { {name: len(rows) for name, rows in categories.items()} }")
    print(f"Số công dân logic yêu cầu: {num_citizens}")
    print(f"Số dòng thực tế sinh ra: {len(records)}")
    print(f"Batch size: {batch_size}")
    print("\n--- Data quality summary ---")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print("\n--- 5 dòng mẫu ---")
    for row in records[:5]:
        print(json.dumps(row, ensure_ascii=False, default=str))
    print("==========================================\n")


# -----------------------------------------------------------------------------
# 5. SQL và nạp dữ liệu Oracle
# -----------------------------------------------------------------------------

def _build_sql(schema: str) -> Dict[str, str]:
    """Tạo SQL theo schema trong config; chỉ dùng tên bảng/cột cố định."""
    insert_citizen_sql = f"""
        INSERT INTO {schema}.VSS_HO_GD_NHANKHAU (
            ID, MA_CD, MA_CD_OLD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
            NGAY_SINH, SO_CCCD, SO_CMTND, PHONE_NUMBER, EMAIL_ACC, DIA_CHI, DC_LIEN_LAC,
            ISCHU_HO, QH_CHU_HO, TEN_QH_CHUHO, MA_TINH, MA_XA, MA_TINH_LH, MA_XA_LH,
            MA_TINH_KS, MA_XA_KS, QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, DEAD_DATE,
            XAC_THUC_BCA, TYPEID, TRANG_THAI_DUYET, CREATED_DATE, MODIFY_DATE
        ) VALUES (
            :ID, :MA_CD, :MA_CD_OLD, :MA_HO_GD, :HO_TEN, :HO_TEN_UNSIGNED, :GIOI_TINH,
            :NGAY_SINH, :SO_CCCD, :SO_CMTND, :PHONE_NUMBER, :EMAIL_ACC, :DIA_CHI, :DC_LIEN_LAC,
            :ISCHU_HO, :QH_CHU_HO, :TEN_QH_CHUHO, :MA_TINH, :MA_XA, :MA_TINH_LH, :MA_XA_LH,
            :MA_TINH_KS, :MA_XA_KS, :QUOC_TICH, :DM_DAN_TOC_ID, :IS_ALIVE, :DEAD_DATE,
            :XAC_THUC_BCA, :TYPEID, :TRANG_THAI_DUYET, :CREATED_DATE, :MODIFY_DATE
        )
    """

    merge_dantoc_sql = f"""
        MERGE INTO {schema}.VSS_DM_DANTOC dst
        USING (SELECT :ID AS ID FROM DUAL) src ON (dst.ID = src.ID)
        WHEN NOT MATCHED THEN
            INSERT (ID, CAN_CU, MA, NGAY_HL, NGAY_HET_HL, TEN)
            VALUES (:ID, :CAN_CU, :MA, :NGAY_HL, :NGAY_HET_HL, :TEN)
    """

    merge_quoctich_sql = f"""
        MERGE INTO {schema}.VSS_DM_QUOC_TICH dst
        USING (SELECT :ID AS ID FROM DUAL) src ON (dst.ID = src.ID)
        WHEN NOT MATCHED THEN
            INSERT (ID, MA, TEN)
            VALUES (:ID, :MA, :TEN)
    """

    merge_dbhc_sql = f"""
        MERGE INTO {schema}.VSS_DM_DBHC dst
        USING (SELECT :ID AS ID FROM DUAL) src ON (dst.ID = src.ID)
        WHEN NOT MATCHED THEN
            INSERT (
                ID, MA_DBHC, MA_CHA, TEN, TINH_TRANG, STATUS, MA_DBHC_SN,
                IS_CAP_XA, NGAY_HIEU_LUC, NGAY_HET_HIEU_LUC
            ) VALUES (
                :ID, :MA_DBHC, :MA_CHA, :TEN, :TINH_TRANG, :STATUS, :MA_DBHC_SN,
                :IS_CAP_XA, :NGAY_HIEU_LUC, :NGAY_HET_HIEU_LUC
            )
    """


    return {
        "insert_citizen": insert_citizen_sql,
        "merge_dantoc": merge_dantoc_sql,
        "merge_quoctich": merge_quoctich_sql,
        "merge_dbhc": merge_dbhc_sql,
    }


def _set_category_bind_sizes(cur: Any) -> None:
    """Ép kiểu bind cho các cột có thể NULL ngay từ dòng đầu của executemany."""
    cur.setinputsizes(
        ID=oracledb.DB_TYPE_NUMBER,
        CAN_CU=oracledb.DB_TYPE_VARCHAR,
        MA=oracledb.DB_TYPE_VARCHAR,
        NGAY_HL=oracledb.DB_TYPE_DATE,
        NGAY_HET_HL=oracledb.DB_TYPE_DATE,
        TEN=oracledb.DB_TYPE_VARCHAR,
    )


def _set_nationality_bind_sizes(cur: Any) -> None:
    cur.setinputsizes(
        ID=oracledb.DB_TYPE_NUMBER,
        MA=oracledb.DB_TYPE_VARCHAR,
        TEN=oracledb.DB_TYPE_VARCHAR,
    )


def _set_dbhc_bind_sizes(cur: Any) -> None:
    cur.setinputsizes(
        ID=oracledb.DB_TYPE_NUMBER,
        MA_DBHC=oracledb.DB_TYPE_VARCHAR,
        MA_CHA=oracledb.DB_TYPE_VARCHAR,
        TEN=oracledb.DB_TYPE_VARCHAR,
        TINH_TRANG=oracledb.DB_TYPE_VARCHAR,
        STATUS=oracledb.DB_TYPE_VARCHAR,
        MA_DBHC_SN=oracledb.DB_TYPE_VARCHAR,
        IS_CAP_XA=oracledb.DB_TYPE_NUMBER,
        NGAY_HIEU_LUC=oracledb.DB_TYPE_DATE,
        NGAY_HET_HIEU_LUC=oracledb.DB_TYPE_DATE,
    )


def _set_citizen_bind_sizes(cur: Any) -> None:
    """Khai báo kiểu bind để None không bị Oracle suy luận nhầm thành VARCHAR2."""
    string_columns = (
        "ID", "MA_CD", "MA_CD_OLD", "MA_HO_GD", "HO_TEN", "HO_TEN_UNSIGNED",
        "GIOI_TINH", "SO_CCCD", "SO_CMTND", "PHONE_NUMBER", "EMAIL_ACC",
        "DIA_CHI", "DC_LIEN_LAC", "ISCHU_HO", "QH_CHU_HO", "TEN_QH_CHUHO",
        "MA_TINH", "MA_XA", "MA_TINH_LH", "MA_XA_LH", "MA_TINH_KS", "MA_XA_KS",
        "QUOC_TICH", "IS_ALIVE", "TYPEID",
    )
    sizes = {name: oracledb.DB_TYPE_VARCHAR for name in string_columns}
    sizes.update({
        "NGAY_SINH": oracledb.DB_TYPE_NUMBER,
        "DM_DAN_TOC_ID": oracledb.DB_TYPE_NUMBER,
        "DEAD_DATE": oracledb.DB_TYPE_NUMBER,
        "XAC_THUC_BCA": oracledb.DB_TYPE_NUMBER,
        "TRANG_THAI_DUYET": oracledb.DB_TYPE_NUMBER,
        "CREATED_DATE": oracledb.DB_TYPE_DATE,
        "MODIFY_DATE": oracledb.DB_TYPE_DATE,
    })
    cur.setinputsizes(**sizes)


def seed_to_oracle(
    num_citizens: int = SEED_NUM_CITIZENS,
    batch_size: int = SEED_BATCH_SIZE,
    truncate: bool = False,
    seed: int = DEFAULT_SEED,
) -> None:
    """Sinh dữ liệu và nạp trực tiếp vào 4 bảng nguồn trên Oracle."""
    if batch_size <= 0:
        raise ValueError("batch_size phải lớn hơn 0.")

    if oracledb is None:
        raise RuntimeError("Chưa cài python-oracledb. Cài bằng: pip install oracledb")

    schema = SOURCE_DB.schema
    categories = get_seed_categories()
    citizen_rows = generate_citizen_records(num_citizens=num_citizens, seed=seed)
    sql = _build_sql(schema)

    print(f"[Seed] Connecting Oracle schema={schema}, DSN={SOURCE_DB.dsn}")
    with oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    ) as conn:
        try:
            with conn.cursor() as cur:
                if truncate:
                    # Xóa bảng chính trước để tránh vi phạm FK nếu có ràng buộc.
                    for table_name in (
                        "VSS_HO_GD_NHANKHAU",
                        "VSS_DM_DANTOC",
                        "VSS_DM_QUOC_TICH",
                        "VSS_DM_DBHC",
                    ):
                        cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                    print(f"[Seed] Truncated 4 tables in schema {schema}.")

                _set_category_bind_sizes(cur)
                cur.executemany(sql["merge_dantoc"], categories["dan_toc"])

                _set_nationality_bind_sizes(cur)
                cur.executemany(sql["merge_quoctich"], categories["quoc_tich"])

                _set_dbhc_bind_sizes(cur)
                cur.executemany(sql["merge_dbhc"], categories["dbhc"])
                conn.commit()
                print("[Seed] Merged categories:", {key: len(value) for key, value in categories.items()})

                inserted = 0
                _set_citizen_bind_sizes(cur)
                for batch_no, batch in enumerate(chunked_records(citizen_rows, batch_size), start=1):
                    cur.executemany(sql["insert_citizen"], batch)
                    conn.commit()
                    inserted += len(batch)
                    print(
                        f"[Seed] Batch {batch_no}: inserted {inserted}/{len(citizen_rows)} "
                        f"rows into {schema}.VSS_HO_GD_NHANKHAU"
                    )
        except Exception:
            conn.rollback()
            raise

    print(f"[Seed] Done. Total {len(citizen_rows)} rows for {num_citizens} logical citizens.")


# -----------------------------------------------------------------------------
# 6. CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate initial seed data for VSS Oracle source tables."
    )
    parser.add_argument(
        "--num-citizens", type=int, default=SEED_NUM_CITIZENS,
        help="Số MA_CD logic cần sinh; số dòng thực tế có thể lớn hơn do nhiều hộ.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=SEED_BATCH_SIZE,
        help="Số dòng mỗi lần executemany.",
    )
    parser.add_argument(
        "--seed", type=int, default=DEFAULT_SEED,
        help="Random seed để tái lập cùng bộ dữ liệu.",
    )
    parser.add_argument(
        "--truncate", action="store_true",
        help="TRUNCATE 4 bảng đích trước khi nạp. Thao tác phá hủy dữ liệu hiện có.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ sinh dữ liệu trong RAM, không kết nối Oracle.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.num_citizens <= 0:
        raise SystemExit("--num-citizens phải lớn hơn 0.")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size phải lớn hơn 0.")

    if args.dry_run:
        categories = get_seed_categories()
        records = generate_citizen_records(args.num_citizens, seed=args.seed)
        print_dry_run_report(
            categories=categories,
            records=records,
            num_citizens=args.num_citizens,
            batch_size=args.batch_size,
            seed=args.seed,
        )
    else:
        seed_to_oracle(
            num_citizens=args.num_citizens,
            batch_size=args.batch_size,
            truncate=args.truncate,
            seed=args.seed,
        )


if __name__ == "__main__":
    main()
