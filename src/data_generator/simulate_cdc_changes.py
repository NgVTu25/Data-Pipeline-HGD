"""
Phần 1.1 - Script 2: Đẩy dữ liệu thay đổi vào bảng nguồn (Change / Delta Generator)
Dùng để kiểm thử luồng CDC -> Near Real-time C360 -> Đối soát, BI & API với đủ 5 kịch bản:
1. INSERT lô công dân mới + thêm hộ mới (MA_HO_GD mới, MODIFY_DATE mới hơn) cho công dân cũ.
2. UPDATE sửa thông tin bẩn thành sạch (bổ sung CCCD 12 số, sửa mã xã/ngày sinh) -> kiểm tra gỡ cờ DQ_FLAGS.
3. UPDATE chuyển trạng thái sang bị lọc (TRANG_THAI_DUYET = 4 hoặc TYPEID = 'GG') -> tự động biến mất khỏi C360;
   và ngược lại duyệt hồ sơ từ 1 -> 2 -> tự động xuất hiện trên C360.
4. UPDATE đổi mã công dân (đổi MA_CD sang mã mới, lưu mã cũ vào MA_CD_OLD) -> C360 tạo mã mới & xóa mã cũ.
5. DELETE xóa dòng hộ mới nhất của người có nhiều hộ (C360 tự lùi về hộ cũ) hoặc xóa hết các dòng của 1 MA_CD.
"""
import argparse
from typing import List, Tuple

import oracledb

from src.config import SOURCE_DB


def build_cdc_test_sql_statements() -> List[Tuple[str, str]]:
    """
    Trả về danh sách các câu lệnh SQL kịch bản biến động trên bảng nguồn VSS.VSS_HO_GD_NHANKHAU
    để thực thi kiểm thử End-to-End.
    """
    return [
        (
            "SCENARIO_1_INSERT_NEW_HOUSEHOLD",
            """
            INSERT INTO VSS.VSS_HO_GD_NHANKHAU (
                ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                CREATED_DATE, MODIFY_DATE
            ) VALUES (
                '0000000001_NEW_HO', '0000000001', 'HGD_NEW_999999',
                'Nguyễn Văn Công Dân 1', 'NGUYEN VAN CONG DAN 1', '1',
                19950815, '001095000001', '0912345678', '01TTT', '00004',
                'VN', 1, '1', NULL, 2,
                SYSDATE, SYSDATE + INTERVAL '1' HOUR
            )
            """,
        ),
        (
            "SCENARIO_2_UPDATE_FIX_DQ_FLAGS",
            """
            UPDATE VSS.VSS_HO_GD_NHANKHAU
            SET SO_CCCD = '001095999888',
                NGAY_SINH = 19950815,
                MA_TINH = '01TTT',
                MA_XA = '00004',
                PHONE_NUMBER = '0987654321',
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE MA_CD = '0000000005'
            """,
        ),
        (
            "SCENARIO_3A_UPDATE_STATUS_TO_INVALID",
            """
            UPDATE VSS.VSS_HO_GD_NHANKHAU
            SET TYPEID = 'GG',
                TRANG_THAI_DUYET = 4,
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE MA_CD = '0000000002'
            """,
        ),
        (
            "SCENARIO_3B_UPDATE_STATUS_PENDING_TO_APPROVED",
            """
            UPDATE VSS.VSS_HO_GD_NHANKHAU
            SET TYPEID = NULL,
                TRANG_THAI_DUYET = 2,
                MODIFY_DATE = SYSDATE + INTERVAL '2' HOUR
            WHERE TRANG_THAI_DUYET = 1 AND ROWNUM <= 10
            """,
        ),
        (
            "SCENARIO_4_CHANGE_MA_CD",
            """
            UPDATE VSS.VSS_HO_GD_NHANKHAU
            SET MA_CD_OLD = MA_CD,
                MA_CD = '9900000003',
                MODIFY_DATE = SYSDATE + INTERVAL '3' HOUR
            WHERE MA_CD = '0000000003'
            """,
        ),
        (
            "SCENARIO_5A_DELETE_LATEST_HOUSEHOLD_ROLLBACK_TO_OLDER",
            """
            DELETE FROM VSS.VSS_HO_GD_NHANKHAU
            WHERE MA_CD = '0000000001' AND MA_HO_GD = 'HGD_NEW_999999'
            """,
        ),
        (
            "SCENARIO_5B_DELETE_ALL_RECORDS_OF_CITIZEN",
            """
            DELETE FROM VSS.VSS_HO_GD_NHANKHAU
            WHERE MA_CD = '0000000004'
            """,
        ),
    ]


def run_cdc_simulation() -> None:
    """Thực thi các kịch bản biến động CDC trực tiếp lên bảng nguồn Oracle."""
    scenarios = build_cdc_test_sql_statements()
    with oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    ) as conn:
        with conn.cursor() as cur:
            for name, sql in scenarios:
                cur.execute(sql.strip())
                print(f"[CDC Delta Simulator] Executed {name}: rowcount={cur.rowcount}")
        conn.commit()
    print("[CDC Delta Simulator] Committed all CDC test scenarios.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute CDC test scenarios against Oracle VSS schema.")
    parser.add_argument("--dry-run", action="store_true", help="Print scenarios without executing on Oracle")
    args = parser.parse_args()

    if args.dry_run:
        scenarios = build_cdc_test_sql_statements()
        print(f"[CDC Delta Simulator Dry-Run] Prepared {len(scenarios)} CDC test scenarios:")
        for name, _ in scenarios:
            print(f"  - {name}")
    else:
        run_cdc_simulation()
