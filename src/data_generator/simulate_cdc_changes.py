"""
Phần 1.1 - Script 2: Đẩy dữ liệu thay đổi vào bảng nguồn (Change / Delta Generator)
Thực hiện đúng theo đặc tả tại `Yeucau_DataPipeline_HGD_C360_4Tuan.md` (Mục 3 - Phần 1.1 - Script 2):
Dùng để chạy sau khi pipeline đã lên, kiểm tra luồng CDC -> Near Real-time C360 -> Đối soát, BI & API với đủ 5 nhóm kịch bản:
1. Thêm nhân khẩu mới (INSERT):
   - 1A: Thêm công dân mới (hỗ trợ cả sinh theo lô lớn `--insert-batch-size`, ví dụ 1.000 - 50.000 dòng).
   - 1B: Công dân cũ đang hiệu lực phát sinh thêm dòng ở hộ gia đình mới (`MA_HO_GD` mới) với `MODIFY_DATE` mới hơn.
2. Cập nhật sửa thông tin bẩn -> sạch (UPDATE):
   - 2: Bổ sung CCCD 12 số cho người trước đó thiếu, sửa mã tỉnh/mã xã/ngày sinh/SĐT/quốc tịch/dân tộc từ sai thành đúng
        để kiểm tra cờ lỗi `DQ_FLAGS` tự động được gỡ (`IS_CLEAN = 0 -> 1`).
3. Cập nhật trạng thái hiệu lực <-> bị lọc (UPDATE):
   - 3A: Chuyển `TRANG_THAI_DUYET` sang `4` hoặc `TYPEID` sang `'GG'` -> tự động bị loại khỏi bảng C360.
   - 3B: Duyệt hồ sơ từ `TRANG_THAI_DUYET = 1` sang `2` (`TYPEID = NULL`) -> tự động xuất hiện trên bảng C360.
4. Đổi mã công dân (UPDATE cột `MA_CD`):
   - 4: Đổi `MA_CD` của bản ghi sang mã mới, lưu mã cũ vào `MA_CD_OLD` -> bảng C360 tạo mã mới và xóa sạch mã cũ.
5. Xóa bản ghi (DELETE):
   - 5A: Xóa bớt 1 dòng hộ mới nhất của người có nhiều hộ -> bảng C360 tự động lùi về lấy dòng hộ cũ còn lại.
   - 5B: Xóa toàn bộ các dòng của một công dân ở nguồn -> công dân đó bị xóa hoàn toàn khỏi bảng C360.
"""
from __future__ import annotations

import argparse
import logging
import random
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

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
            description="Sửa toàn bộ thông tin bẩn (bổ sung CCCD 12 số, sửa mã tỉnh/xã, ngày sinh, SĐT, quốc tịch, dân tộc) của MA_CD='0000000005' để gỡ hết cờ DQ_FLAGS",
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
            description="Chuyển MA_CD='0000000002' sang trạng thái bị lọc (TYPEID='GG', TRANG_THAI_DUYET=4) -> tự động biến mất khỏi C360",
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
            description="Duyệt hồ sơ từ TRANG_THAI_DUYET=1 sang 2 (tối đa 10 dòng) -> tự động xuất hiện trên C360",
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
            description="Đổi MA_CD='0000000003' -> '9900000003' và lưu mã cũ vào MA_CD_OLD -> C360 tạo mã mới & xóa mã cũ",
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
            description="Xóa hộ mới nhất 'HGD_NEW_999999' của MA_CD='0000000001' -> C360 tự động lùi về lấy dòng hộ cũ còn lại",
            sql=f"""
            DELETE FROM {table}
            WHERE MA_CD = '0000000001' AND MA_HO_GD = 'HGD_NEW_999999'
            """,
        ),
        CdcScenario(
            key="5B",
            name="SCENARIO_5B_DELETE_ALL_RECORDS_OF_CITIZEN",
            description="Xóa toàn bộ các dòng của MA_CD='0000000004' ở nguồn -> công dân bị xóa khỏi C360",
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
    """
    Đảm bảo dữ liệu nền của các mã kiểm thử cố định (0000000001..0000000005)
    đúng trạng thái trước khi chạy từng kịch bản để luôn có thể chạy lặp lại (idempotent)
    và phản ánh đúng yêu cầu nghiệp vụ trong Yeucau_DataPipeline_HGD_C360_4Tuan.md:
      - 0000000001 có dòng hộ cũ HGD_0000000010 hợp lệ (TYPEID=NULL, TRANG_THAI_DUYET=0)
        để sau khi 1B thêm hộ mới và 5A xóa hộ mới, C360 lùi về đúng hộ cũ HGD_0000000010.
      - 0000000003 nếu đã đổi sang 9900000003 ở lần chạy trước thì khôi phục về 0000000003 trước khi chạy kịch bản 4.
      - 0000000004 nếu đã bị xóa ở lần chạy trước thì khôi phục 1 dòng hợp lệ trước khi chạy kịch bản 5B.
    """
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


def run_batch_insert_new_citizens(batch_count: int, chunk_size: int = 5000) -> int:
    """
    Kịch bản 1A mở rộng theo Yeucau_DataPipeline_HGD_C360_4Tuan.md:
    'Thêm lô nhân khẩu mới (INSERT): Sinh thêm một lô công dân mới (ví dụ 50.000 - 100.000 dòng)'.
    """
    if batch_count <= 0:
        return 0

    table = f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
    prefix = f"B{int(time.time()) % 100000:05d}"
    sql = f"""
        INSERT INTO {table} (
            ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
            NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
            QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
            CREATED_DATE, MODIFY_DATE
        ) VALUES (
            :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, :gioi_tinh,
            :ngay_sinh, :so_cccd, :phone_number, :ma_tinh, :ma_xa,
            'VN', 1, '1', NULL, 2, SYSDATE, SYSDATE
        )
    """
    total_inserted = 0
    with oracledb.connect(user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn) as conn:
        with conn.cursor() as cur:
            rows: List[Dict[str, object]] = []
            for i in range(1, batch_count + 1):
                ma_cd = f"{prefix}{i:06d}"
                ho_ten = f"{random.choice(_LAST_NAMES)} {random.choice(_MID_NAMES)} {random.choice(_FIRST_NAMES)}"
                ma_tinh, ma_xa = random.choice(_LOCATIONS)
                rows.append({
                    "id": f"{ma_cd}_H1",
                    "ma_cd": ma_cd,
                    "ma_ho_gd": f"HGD_{ma_cd}",
                    "ho_ten": ho_ten,
                    "ho_ten_unsigned": ho_ten.upper(),
                    "gioi_tinh": "1" if i % 2 else "0",
                    "ngay_sinh": 19900101 + (i % 28),
                    "so_cccd": f"001{i:09d}",
                    "phone_number": f"09{i % 100000000:08d}",
                    "ma_tinh": ma_tinh,
                    "ma_xa": ma_xa,
                })
                if len(rows) >= chunk_size or i == batch_count:
                    cur.executemany(sql, rows)
                    conn.commit()
                    total_inserted += len(rows)
                    logger.info("[BATCH_INSERT] Inserted %d/%d new citizen rows into %s", total_inserted, batch_count, table)
                    rows.clear()
    return total_inserted


def run_cdc_simulation(
    selected_keys: Optional[Iterable[str]] = None,
    commit_each: bool = True,
    delay_seconds: float = 0.0,
    insert_batch_size: int = 0,
) -> List[Tuple[str, int]]:
    """
    Thực thi các kịch bản biến động CDC lên bảng nguồn Oracle.
    Mặc định commit sau từng kịch bản để CDC (Debezium/Redo Log) bắt từng sự kiện độc lập
    (tránh việc INSERT 1B và DELETE 5A triệt tiêu nhau trong cùng 1 transaction).
    """
    scenarios = filter_scenarios(build_cdc_scenarios(), selected_keys)
    wanted_keys = {s.key.upper() for s in scenarios}
    results: List[Tuple[str, int]] = []
    table = f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"

    with oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    ) as conn:
        try:
            with conn.cursor() as cur:
                _ensure_preconditions_for_deterministic_scenarios(cur, conn, table, wanted_keys)

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

    if insert_batch_size > 0:
        inserted_batch = run_batch_insert_new_citizens(insert_batch_size)
        results.append(("SCENARIO_1A_BATCH_INSERT_NEW_CITIZENS", inserted_batch))

    total_rows = sum(r for _, r in results)
    logger.info("Completed %d scenario(s), total affected rows=%d.", len(results), total_rows)
    return results


_LAST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Đỗ", "Bùi"]
_MID_NAMES = ["Văn", "Thị", "Minh", "Hoàng", "Ngọc", "Thanh", "Đức", "Quang"]
_FIRST_NAMES = ["An", "Bình", "Cường", "Dũng", "Hà", "Hải", "Hùng", "Lan", "Linh", "Tuấn"]
_LOCATIONS = [("01TTT", "00004"), ("33TTT", "12457"), ("79TTT", "26734"), ("01TTT", "00008")]


def _random_citizen_payload(seq: int, is_dirty: bool = False, is_pending: bool = False) -> Dict[str, object]:
    ma_cd = f"R{int(time.time()) % 100000:05d}{seq:04d}"
    ho_ten = f"{random.choice(_LAST_NAMES)} {random.choice(_MID_NAMES)} {random.choice(_FIRST_NAMES)}"
    ma_tinh, ma_xa = random.choice(_LOCATIONS)
    return {
        "id": f"{ma_cd}_H1",
        "ma_cd": ma_cd,
        "ma_ho_gd": f"HGD_{ma_cd}_1",
        "ho_ten": f"  {ho_ten}  " if is_dirty else ho_ten,
        "ho_ten_unsigned": ho_ten.upper(),
        "gioi_tinh": random.choice(["0", "1"]),
        "ngay_sinh": 19950230 if is_dirty else (random.randint(1965, 2005) * 10000 + random.randint(1, 12) * 100 + random.randint(1, 28)),
        "so_cccd": None if is_dirty else f"001{random.randint(100000000, 999999999)}",
        "phone_number": "12345" if is_dirty else f"09{random.randint(10000000, 99999999)}",
        "ma_tinh": "79TTT" if is_dirty else ma_tinh,
        "ma_xa": "00004" if is_dirty else ma_xa,
        "quoc_tich": "XX" if is_dirty else "VN",
        "dm_dan_toc_id": 9999 if is_dirty else 1,
        "typeid": None,
        "trang_thai_duyet": 1 if is_pending else 2,
    }


def run_random_cdc_stream(
    interval_seconds: float = 2.0,
    max_events: Optional[int] = None,
) -> int:
    """
    Chạy giả lập biến động CDC ngẫu nhiên liên tục bao phủ ĐỦ 8 kịch bản nghiệp vụ trong
    `Yeucau_DataPipeline_HGD_C360_4Tuan.md` (Mục 3 - Phần 1.1 - Script 2):
      - 1A: INSERT công dân mới (cả bản ghi sạch, bản ghi bẩn để test gỡ DQ_FLAGS, và bản ghi chờ duyệt TRANG_THAI_DUYET=1).
      - 1B: INSERT thêm dòng hộ gia đình thứ 2 (MA_HO_GD mới, MODIFY_DATE mới hơn) cho công dân đã có.
      - 2 : UPDATE sửa thông tin bẩn -> sạch (bổ sung CCCD 12 số, sửa mã tỉnh/xã, ngày sinh, SĐT, quốc tịch, dân tộc) để gỡ cờ DQ_FLAGS.
      - 3A: UPDATE chuyển trạng thái sang bị lọc (TYPEID='GG', TRANG_THAI_DUYET=4) -> biến mất khỏi C360.
      - 3B: UPDATE duyệt hồ sơ từ TRANG_THAI_DUYET=1 sang 2 (TYPEID=NULL) -> xuất hiện trên C360.
      - 4 : UPDATE đổi mã công dân (MA_CD sang mã mới, lưu mã cũ vào MA_CD_OLD) -> C360 tạo mã mới & xóa mã cũ.
      - 5A: DELETE xóa dòng hộ mới nhất của công dân có >= 2 hộ -> C360 tự động lùi về dòng hộ cũ còn lại.
      - 5B: DELETE xóa toàn bộ các dòng của 1 công dân -> công dân bị xóa khỏi C360.
    """
    table = f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
    # Pool quản lý các công dân đang sống do generator tạo để chạy đủ 8 kịch bản
    active_citizens: List[Dict[str, object]] = []
    event_count = 0
    seq = 1

    logger.info(
        "Starting random CDC generator (covering 1A, 1B, 2, 3A, 3B, 4, 5A, 5B) on %s (interval=%.1fs, max_events=%s). Press Ctrl+C to stop.",
        table,
        interval_seconds,
        max_events if max_events is not None else "unlimited",
    )

    with oracledb.connect(
        user=SOURCE_DB.user,
        password=SOURCE_DB.password,
        dsn=SOURCE_DB.dsn,
    ) as conn:
        try:
            with conn.cursor() as cur:
                while max_events is None or event_count < max_events:
                    dirty_pool = [c for c in active_citizens if c["is_dirty"] and c["is_valid"]]
                    pending_pool = [c for c in active_citizens if not c["is_valid"]]
                    multi_ho_pool = [c for c in active_citizens if len(c["households"]) >= 2 and c["is_valid"]]
                    valid_pool = [c for c in active_citizens if c["is_valid"]]

                    if len(active_citizens) < 4:
                        # Khởi tạo nhanh đủ mẫu: sạch, bẩn (cho case 2), chờ duyệt (cho case 3B)
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
                        mode = seq % 3  # 0: clean, 1: dirty (for case 2), 2: pending (for case 3B)
                        is_dirty = (mode == 1)
                        is_pending = (mode == 2)
                        p = _random_citizen_payload(seq, is_dirty=is_dirty, is_pending=is_pending)
                        seq += 1
                        cur.execute(
                            f"""
                            INSERT INTO {table} (
                                ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                                NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                                QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                                CREATED_DATE, MODIFY_DATE
                            ) VALUES (
                                :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, :gioi_tinh,
                                :ngay_sinh, :so_cccd, :phone_number, :ma_tinh, :ma_xa,
                                :quoc_tich, :dm_dan_toc_id, '1', :typeid, :trang_thai_duyet,
                                SYSDATE, SYSDATE
                            )
                            """,
                            p,
                        )
                        conn.commit()
                        active_citizens.append({
                            "ma_cd": str(p["ma_cd"]),
                            "ho_ten": str(p["ho_ten"]).strip(),
                            "households": [(str(p["id"]), str(p["ma_ho_gd"]))],
                            "is_dirty": is_dirty,
                            "is_valid": not is_pending,
                        })
                        tag = "DIRTY" if is_dirty else ("PENDING_DUYET=1" if is_pending else "CLEAN")
                        logger.info("[RANDOM #%d][1A] INSERT MA_CD=%s (%s, %s) -> rowcount=%d", event_count + 1, p["ma_cd"], str(p["ho_ten"]).strip(), tag, cur.rowcount)

                    elif action == "1B_INSERT_NEW_HOUSEHOLD":
                        target = random.choice(valid_pool)
                        ho_seq = len(target["households"]) + 1
                        new_id = f"{target['ma_cd']}_H{ho_seq}"
                        new_ho = f"HGD_{target['ma_cd']}_{ho_seq}"
                        ma_tinh, ma_xa = random.choice(_LOCATIONS)
                        cur.execute(
                            f"""
                            INSERT INTO {table} (
                                ID, MA_CD, MA_HO_GD, HO_TEN, HO_TEN_UNSIGNED, GIOI_TINH,
                                NGAY_SINH, SO_CCCD, PHONE_NUMBER, MA_TINH, MA_XA,
                                QUOC_TICH, DM_DAN_TOC_ID, IS_ALIVE, TYPEID, TRANG_THAI_DUYET,
                                CREATED_DATE, MODIFY_DATE
                            ) VALUES (
                                :id, :ma_cd, :ma_ho_gd, :ho_ten, :ho_ten_unsigned, '1',
                                19950815, :so_cccd, '0912345678', :ma_tinh, :ma_xa,
                                'VN', 1, '1', NULL, 2,
                                SYSDATE, SYSDATE + NUMTODSINTERVAL(:ho_seq, 'MINUTE')
                            )
                            """,
                            {
                                "id": new_id,
                                "ma_cd": target["ma_cd"],
                                "ma_ho_gd": new_ho,
                                "ho_ten": target["ho_ten"],
                                "ho_ten_unsigned": str(target["ho_ten"]).upper(),
                                "so_cccd": f"001{random.randint(100000000, 999999999)}",
                                "ma_tinh": ma_tinh,
                                "ma_xa": ma_xa,
                                "ho_seq": ho_seq,
                            },
                        )
                        conn.commit()
                        target["households"].append((new_id, new_ho))
                        logger.info("[RANDOM #%d][1B] INSERT_NEW_HOUSEHOLD MA_CD=%s (MA_HO_GD=%s, total_ho=%d) -> rowcount=%d", event_count + 1, target["ma_cd"], new_ho, len(target["households"]), cur.rowcount)

                    elif action == "2_UPDATE_FIX_DQ_FLAGS":
                        target = random.choice(dirty_pool)
                        new_cccd = f"001{random.randint(100000000, 999999999)}"
                        new_phone = f"09{random.randint(10000000, 99999999)}"
                        cur.execute(
                            f"""
                            UPDATE {table}
                            SET HO_TEN = TRIM(HO_TEN),
                                SO_CCCD = :cccd,
                                NGAY_SINH = 19950815,
                                PHONE_NUMBER = :phone,
                                MA_TINH = '01TTT',
                                MA_XA = '00004',
                                QUOC_TICH = 'VN',
                                DM_DAN_TOC_ID = 1,
                                MODIFY_DATE = SYSDATE
                            WHERE MA_CD = :ma_cd
                            """,
                            {"cccd": new_cccd, "phone": new_phone, "ma_cd": target["ma_cd"]},
                        )
                        conn.commit()
                        target["is_dirty"] = False
                        logger.info("[RANDOM #%d][2] UPDATE_FIX_DQ_FLAGS MA_CD=%s (cleaned CCCD=%s, Tinh/Xa=01TTT/00004) -> rowcount=%d", event_count + 1, target["ma_cd"], new_cccd, cur.rowcount)

                    elif action == "3A_UPDATE_STATUS_TO_INVALID":
                        target = random.choice(valid_pool)
                        cur.execute(
                            f"""
                            UPDATE {table}
                            SET TYPEID = 'GG',
                                TRANG_THAI_DUYET = 4,
                                MODIFY_DATE = SYSDATE
                            WHERE MA_CD = :ma_cd
                            """,
                            {"ma_cd": target["ma_cd"]},
                        )
                        conn.commit()
                        target["is_valid"] = False
                        logger.info("[RANDOM #%d][3A] UPDATE_STATUS_TO_INVALID MA_CD=%s (TYPEID='GG', DUYET=4) -> rowcount=%d", event_count + 1, target["ma_cd"], cur.rowcount)

                    elif action == "3B_UPDATE_APPROVE_PENDING":
                        target = random.choice(pending_pool)
                        cur.execute(
                            f"""
                            UPDATE {table}
                            SET TYPEID = NULL,
                                TRANG_THAI_DUYET = 2,
                                MODIFY_DATE = SYSDATE
                            WHERE MA_CD = :ma_cd
                            """,
                            {"ma_cd": target["ma_cd"]},
                        )
                        conn.commit()
                        target["is_valid"] = True
                        logger.info("[RANDOM #%d][3B] UPDATE_APPROVE_PENDING MA_CD=%s (DUYET -> 2, TYPEID -> NULL) -> rowcount=%d", event_count + 1, target["ma_cd"], cur.rowcount)

                    elif action == "4_CHANGE_MA_CD":
                        target = random.choice(valid_pool)
                        old_ma = str(target["ma_cd"])
                        new_ma = f"R{int(time.time()) % 100000:05d}{seq:04d}"
                        seq += 1
                        cur.execute(
                            f"""
                            UPDATE {table}
                            SET MA_CD_OLD = MA_CD,
                                MA_CD = :new_ma,
                                MODIFY_DATE = SYSDATE
                            WHERE MA_CD = :old_ma
                            """,
                            {"new_ma": new_ma, "old_ma": old_ma},
                        )
                        conn.commit()
                        target["ma_cd"] = new_ma
                        logger.info("[RANDOM #%d][4] CHANGE_MA_CD %s -> %s -> rowcount=%d", event_count + 1, old_ma, new_ma, cur.rowcount)

                    elif action == "5A_DELETE_LATEST_HOUSEHOLD":
                        target = random.choice(multi_ho_pool)
                        latest_id, latest_ho = target["households"].pop()
                        cur.execute(
                            f"DELETE FROM {table} WHERE ID = :id AND MA_CD = :ma_cd",
                            {"id": latest_id, "ma_cd": target["ma_cd"]},
                        )
                        conn.commit()
                        remaining_ho = target["households"][-1][1]
                        logger.info("[RANDOM #%d][5A] DELETE_LATEST_HOUSEHOLD MA_CD=%s (deleted=%s, rollback_to=%s) -> rowcount=%d", event_count + 1, target["ma_cd"], latest_ho, remaining_ho, cur.rowcount)

                    elif action == "5B_DELETE_ALL_RECORDS":
                        idx = random.randrange(len(active_citizens))
                        target = active_citizens.pop(idx)
                        cur.execute(
                            f"DELETE FROM {table} WHERE MA_CD = :ma_cd",
                            {"ma_cd": target["ma_cd"]},
                        )
                        conn.commit()
                        logger.info("[RANDOM #%d][5B] DELETE_ALL_RECORDS MA_CD=%s -> rowcount=%d", event_count + 1, target["ma_cd"], cur.rowcount)

                    event_count += 1
                    if max_events is None or event_count < max_events:
                        time.sleep(max(0.0, interval_seconds))
        except KeyboardInterrupt:
            logger.info("Stopped random CDC generator after %d event(s).", event_count)

    return event_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute CDC test scenarios against Oracle source schema (Yeucau_DataPipeline_HGD_C360_4Tuan.md)."
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
        help="Print selected scenarios without executing on Oracle",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Run continuous random CDC events covering all 8 scenarios (1A, 1B, 2, 3A, 3B, 4, 5A, 5B)",
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


if __name__ == "__main__":
    args = parse_args()

    if args.random:
        run_random_cdc_stream(
            interval_seconds=args.interval,
            max_events=args.max_events,
        )
    else:
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
                insert_batch_size=args.insert_batch_size,
            )
