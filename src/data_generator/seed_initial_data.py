"""
Phần 1.1 - Script 1: Sinh dữ liệu ban đầu (Initial Seed Generator)
Sinh dữ liệu cho 3 bảng danh mục và bảng chính VSS.VSS_HO_GD_NHANKHAU,
chủ đích cài cắm đầy đủ các trường hợp dữ liệu thực tế & dữ liệu bẩn theo tỷ lệ.
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

from src.config import SEED_NUM_CITIZENS, SEED_BATCH_SIZE


def get_seed_categories() -> Dict[str, List[Dict[str, Any]]]:
    """
    Sinh dữ liệu chuẩn cho 3 bảng danh mục:
    - VSS_DM_DANTOC
    - VSS_DM_QUOC_TICH
    - VSS_DM_DBHC (Sau sáp nhập 2025: Tỉnh 'NNTTT', Xã 5 số, kèm MA_DBHC_SN và xã cũ bị tách)
    """
    dan_toc = [
        {"ID": 1, "CAN_CU": "QĐ 121-TCTK", "MA": "01", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Kinh"},
        {"ID": 2, "CAN_CU": "QĐ 121-TCTK", "MA": "02", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Tày"},
        {"ID": 3, "CAN_CU": "QĐ 121-TCTK", "MA": "03", "NGAY_HL": datetime(1979, 3, 2), "NGAY_HET_HL": None, "TEN": "Thái"},
    ]

    quoc_tich = [
        {"ID": 233, "MA": "VN", "TEN": "Việt Nam"},
        {"ID": 100, "MA": "LA", "TEN": "Lào"},
        {"ID": 101, "MA": "KR", "TEN": "Hàn Quốc"},
    ]

    # Danh mục Địa bàn hành chính (Tỉnh & Xã sau sáp nhập 2025)
    dbhc = [
        # Cấp Tỉnh (MA_CHA IS NULL)
        {"MA_DBHC": "01TTT", "MA_CHA": None, "TEN": "Thành phố Hà Nội", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "01;28"},
        {"MA_DBHC": "33TTT", "MA_CHA": None, "TEN": "Tỉnh Hưng Yên", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "33;34"},  # 34: Thái Bình cũ
        {"MA_DBHC": "79TTT", "MA_CHA": None, "TEN": "Thành phố Hồ Chí Minh", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "79;74;77"},
        # Cấp Xã/Phường (MA_CHA trỏ vào MA_DBHC của Tỉnh)
        # Phường 00004 gộp từ 00013;00004;00019; ngoài ra mã xã cũ 00099 bị tách sang cả 00004 và 00008 (-> MO_HO)
        {"MA_DBHC": "00004", "MA_CHA": "01TTT", "TEN": "Phường Ba Đình Mới", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "00013;00004;00019;00099"},
        {"MA_DBHC": "00008", "MA_CHA": "01TTT", "TEN": "Phường Ngọc Hà Mới", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "00008;00099"},
        {"MA_DBHC": "12457", "MA_CHA": "33TTT", "TEN": "Xã Phố Hiến Mới", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "12457;12458"},
        {"MA_DBHC": "26734", "MA_CHA": "79TTT", "TEN": "Phường Sài Gòn Mới", "TINH_TRANG": "1", "STATUS": "1", "MA_DBHC_SN": "26734;26737"},
    ]

    return {"dan_toc": dan_toc, "quoc_tich": quoc_tich, "dbhc": dbhc}


def generate_citizen_records(num_citizens: int = SEED_NUM_CITIZENS) -> List[Dict[str, Any]]:
    """
    Sinh danh sách bản ghi VSS_HO_GD_NHANKHAU với các trường hợp gài lỗi:
    1. Trùng công dân ở nhiều hộ (2-3 dòng MA_HO_GD khác nhau, MODIFY_DATE khác nhau).
    2. Dữ liệu bị lọc nghiệp vụ (2-3% có TYPEID IN ('GT','GM','GG','GK') hoặc TRANG_THAI_DUYET IN (1,3,4)).
    3. Đa dạng định dạng MA_TINH ('01TTT', '34TTT', '001', '01', '1', '00TTT', '99TTT', None).
    4. Đa dạng định dạng MA_XA ('00004', mã cũ '00013', mất số 0 đầu '4'/'13', mã mơ hồ '00099', Tỉnh lệch Xã, '99999').
    5. Đa dạng NGAY_SINH (8 số chuẩn, YYYY0000, YYYY, YYYYMM00, YYYYMM, sai lịch 19950230, 17500101).
    6. Đa dạng SO_CCCD / SO_CMTND, PHONE_NUMBER, QUOC_TICH ('VN', '233', 'XX'), DM_DAN_TOC_ID (1, 9999), HO_TEN thừa khoảng trắng.
    """
    records: List[Dict[str, Any]] = []
    base_time = datetime(2026, 1, 1, 8, 0, 0)

    tinh_choices = [
        ("01TTT", 0.50),  # Chuẩn hiện hành
        ("33TTT", 0.15),  # Chuẩn hiện hành
        ("34TTT", 0.08),  # Tỉnh cũ Thái Bình đã sáp nhập vào 33TTT
        ("001", 0.07),    # 3 chữ số
        ("01", 0.05),     # 2 chữ số
        ("1", 0.05),      # 1 chữ số
        ("79TTT", 0.04),  # TP.HCM (sẽ dùng để test Tỉnh lệch Xã khi ghép với xã 00004 của Hà Nội)
        ("00TTT", 0.03),  # Không xác định
        ("99TTT", 0.03),  # Mã tỉnh sai
    ]

    xa_choices = [
        ("00004", 0.50),  # Chuẩn 5 số hiện hành
        ("00013", 0.15),  # Xã cũ đã sáp nhập vào 00004
        ("4", 0.08),      # Mất số 0 ở đầu (của 00004)
        ("13", 0.07),     # Mất số 0 ở đầu (của 00013)
        ("00099", 0.05),  # Xã cũ bị tách ra 2 xã mới (-> MO_HO)
        ("12457", 0.08),  # Xã thuộc Hưng Yên
        ("00000", 0.04),  # Không xác định
        ("99999", 0.03),  # Mã xã sai
    ]

    ngay_sinh_choices = [
        19950815,  # Chuẩn YYYYMMDD
        19950000,  # Chỉ có năm YYYY0000
        1995,      # Chỉ có năm 4 số
        19950800,  # Chỉ có năm-tháng YYYYMM00
        199508,    # Chỉ có năm-tháng 6 số
        19950230,  # Sai lịch (30/02)
        17500101,  # Năm sinh ngoài khoảng hợp lý
    ]

    phone_choices = [
        "0912345678",
        "84912345678",
        "840912345678",
        "912345678",
        "0912 345 678",
        "12345",  # Số điện thoại lỗi
    ]

    for i in range(1, num_citizens + 1):
        ma_cd = f"{i:010d}"
        # 5% công dân xuất hiện ở 2-3 hộ gia đình khác nhau
        num_households = random.choices([1, 2, 3], weights=[0.94, 0.05, 0.01])[0]

        for h_idx in range(num_households):
            ma_ho_gd = f"HGD_{(i * 10 + h_idx):010d}"
            mod_date = base_time + timedelta(days=h_idx * 10, minutes=random.randint(1, 120))

            # Cài 3% dòng thuộc diện bị lọc nghiệp vụ (TYPEID giảm hoặc TRANG_THAI_DUYET chờ/từ chối/hủy)
            is_filtered = random.random() < 0.03
            type_id = random.choice(["GT", "GM", "GG", "GK"]) if (is_filtered and random.random() < 0.5) else None
            trang_thai_duyet = random.choice([1, 3, 4]) if (is_filtered and type_id is None) else random.choice([0, 2])

            ma_tinh = random.choices([x[0] for x in tinh_choices], weights=[x[1] for x in tinh_choices])[0]
            ma_xa = random.choices([x[0] for x in xa_choices], weights=[x[1] for x in xa_choices])[0]

            records.append({
                "ID": f"{ma_cd}_{h_idx + 1}",
                "MA_CD": ma_cd,
                "MA_CD_OLD": None,
                "MA_HO_GD": ma_ho_gd,
                "HO_TEN": "  Nguyễn   Văn  An " if i % 10 == 0 else f"Nguyễn Văn Công Dân {i}",
                "HO_TEN_UNSIGNED": "  NGUYEN   VAN  AN " if i % 10 == 0 else f"NGUYEN VAN CONG DAN {i}",
                "GIOI_TINH": "1" if i % 2 == 0 else "2",
                "NGAY_SINH": random.choice(ngay_sinh_choices),
                "SO_CCCD": f"001095{i:06d}" if i % 5 != 0 else None,
                "SO_CMTND": f"{i:09d}" if i % 5 == 0 else None,
                "PHONE_NUMBER": random.choice(phone_choices),
                "MA_TINH": ma_tinh,
                "MA_XA": ma_xa,
                "MA_TINH_LH": ma_tinh,
                "MA_XA_LH": ma_xa,
                "MA_TINH_KS": ma_tinh,
                "MA_XA_KS": ma_xa,
                "QUOC_TICH": random.choices(["VN", "233", "XX"], weights=[0.7, 0.25, 0.05])[0],
                "DM_DAN_TOC_ID": random.choices([1, 2, 9999], weights=[0.85, 0.10, 0.05])[0],
                "IS_ALIVE": "0" if type_id == "GT" else "1",
                "TYPEID": type_id,
                "TRANG_THAI_DUYET": trang_thai_duyet,
                "CREATED_DATE": base_time,
                "MODIFY_DATE": mod_date,
            })

    return records


if __name__ == "__main__":
    cats = get_seed_categories()
    sample_rows = generate_citizen_records(num_citizens=100)
    print(f"[Initial Seed Dry-Run] Categories: { {k: len(v) for k, v in cats.items()} }")
    print(f"[Initial Seed Dry-Run] Generated {len(sample_rows)} rows for 100 citizens (Batch size={SEED_BATCH_SIZE}).")
