"""
Phần 1.1 - Script 2: Đẩy dữ liệu thay đổi vào bảng nguồn (Change / Delta Generator)
Dùng để kiểm thử luồng CDC -> Near Real-time C360 -> Đối soát, BI & API với đủ 5 kịch bản:
1. INSERT công dân mới (1A) + thêm hộ mới (1B: MA_HO_GD mới, MODIFY_DATE mới hơn) cho công dân cũ.
2. UPDATE sửa thông tin bẩn thành sạch (bổ sung CCCD 12 số, sửa mã xã/ngày sinh) -> kiểm tra gỡ cờ DQ_FLAGS.
3. UPDATE chuyển trạng thái sang bị lọc (3A: TRANG_THAI_DUYET = 4, TYPEID = 'GG') -> tự động biến mất khỏi C360;
   và ngược lại duyệt hồ sơ từ 1 -> 2 (3B) -> tự động xuất hiện trên C360.
4. UPDATE đổi mã công dân (đổi MA_CD sang mã mới, lưu mã cũ vào MA_CD_OLD) -> C360 tạo mã mới & xóa mã cũ.
5. DELETE xóa dòng hộ mới nhất của người có nhiều hộ (5A: C360 tự lùi về hộ cũ) hoặc xóa hết các dòng của 1 MA_CD (5B).
"""
import argparse
import logging
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import oracledb

from src.config import SOURCE_DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc_simulator")


@dataclass(frozen=True)
class CdcScenario:
    key: str
    name: str
    description: str
    sql: str


def build_cdc_scenarios(schema: Optional[str] = None) -> List[CdcScenario]:
    """
    Xây dựng danh sách kịch bản biến động CDC trên bảng nguồn <schema>.VSS_HO_GD_NHANKHAU.
    Dùng MERGE cho các kịch bản INSERT để đảm bảo idempotent khi chạy lại nhiều lần.
    """
    table = f"{schema or SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
    return [
        CdcScenario(
            key="1A",
            name="SCENARIO_1A_INSERT_NEW_CITIZEN",
            description="Thêm mới hoàn toàn 1 công dân sạch (MA_CD='9999000001')",
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
            """,
        ),
        CdcScenario(
            key="1B",
            name="SCENARIO_1B_INSERT_NEW_HOUSEHOLD",
            description="Thêm hộ mới (MODIFY_DATE mới hơn) cho công dân cũ MA_CD='0000000001'",
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
                SYSDATE, SYSDATE + INTERVAL '1' HOUR
            )
            """,
        ),
        CdcScenario(
            key="2",
            name="SCENARIO_2_UPDATE_FIX_DQ_FLAGS",
            description="Làm sạch dữ liệu bẩn của MA_CD='0000000005' để kiểm tra gỡ cờ DQ_FLAGS",
            sql=f"""
            UPDATE {table}
            SET SO_CCCD = '001095999888',
                NGAY_SINH = 19950815,
                MA_TINH = '01TTT',
                MA_XA = '00004',
                PHONE_NUMBER = '0987654321',
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
            description="Duyệt tối đa 10 hồ sơ từ TRANG_THAI_DUYET=1 sang 2",
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
            SET MA_CD_OLD = MA_CD,
                MA_CD = '9900000003',
                MODIFY_DATE = SYSDATE + INTERVAL '3' HOUR
            WHERE MA_CD = '0000000003'
            """,
        ),
        CdcScenario(
            key="5A",
            name="SCENARIO_5A_DELETE_LATEST_HOUSEHOLD_ROLLBACK_TO_OLDER",
            description="Xóa hộ mới nhất 'HGD_NEW_999999' của MA_CD='0000000001' để kiểm tra lùi về hộ cũ",
            sql=f"""
            DELETE FROM {table}
            WHERE MA_CD = '0000000001' AND MA_HO_GD = 'HGD_NEW_999999'
            """,
        ),
        CdcScenario(
            key="5B",
            name="SCENARIO_5B_DELETE_ALL_RECORDS_OF_CITIZEN",
            description="Xóa toàn bộ bản ghi của MA_CD='0000000004'",
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


def run_cdc_simulation(
    selected_keys: Optional[Iterable[str]] = None,
    commit_each: bool = True,
    delay_seconds: float = 0.0,
) -> List[Tuple[str, int]]:
    """
    Thực thi các kịch bản biến động CDC lên bảng nguồn Oracle.
    Mặc định commit sau từng kịch bản để CDC (Debezium/Redo Log) bắt từng sự kiện độc lập
    (tránh việc INSERT 1B và DELETE 5A triệt tiêu nhau trong cùng 1 transaction).
    """
    scenarios = filter_scenarios(build_cdc_scenarios(), selected_keys)
    results: List[Tuple[str, int]] = []

    with oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    ) as conn:
        try:
            with conn.cursor() as cur:
                for idx, scenario in enumerate(scenarios):
                    cur.execute(scenario.sql.strip())
                    rowcount = cur.rowcount
                    results.append((scenario.name, rowcount))

                    if commit_each:
                        conn.commit()

                    if rowcount == 0:
                        logger.warning(
                            "[%s] %s -> rowcount=0 (dữ liệu đã thay đổi trước đó hoặc không tồn tại)",
                            scenario.key,
                            scenario.name,
                        )
                    else:
                        logger.info(
                            "[%s] %s -> rowcount=%d%s",
                            scenario.key,
                            scenario.name,
                            rowcount,
                            " (committed)" if commit_each else "",
                        )

                    if delay_seconds > 0 and idx < len(scenarios) - 1:
                        time.sleep(delay_seconds)

            if not commit_each:
                conn.commit()
                logger.info("Committed all %d CDC test scenarios in a single transaction.", len(scenarios))
        except Exception:
            conn.rollback()
            logger.exception("CDC simulation failed; rolled back uncommitted changes.")
            raise

    total_rows = sum(r for _, r in results)
    logger.info("Completed %d scenario(s), total affected rows=%d.", len(results), total_rows)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute CDC test scenarios against Oracle source schema."
    )
    parser.add_argument(
        "--scenarios",
        type=str,
        default="all",
        help="Comma-separated scenario keys to run (e.g. 1A,1B,2 or all). Default: all",
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
        "--dry-run",
        action="store_true",
        help="Print selected scenarios without executing on Oracle",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    keys = [k.strip() for k in args.scenarios.split(",")] if args.scenarios else None

    if args.dry_run:
        selected = filter_scenarios(build_cdc_scenarios(), keys)
        logger.info("[Dry-Run] Prepared %d CDC test scenario(s) on schema '%s':", len(selected), SOURCE_DB.schema)
        for s in selected:
            logger.info("  [%s] %s - %s", s.key, s.name, s.description)
    else:
        run_cdc_simulation(
            selected_keys=keys,
            commit_each=not args.single_tx,
            delay_seconds=args.delay,
        )
