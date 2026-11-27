"""
Phần 3.1 - Job / Script Đối soát dữ liệu giữa các tầng (Data Reconciliation)
Thực hiện 3 cấp đối soát tự động trực tiếp trên Oracle:
1. Đối soát Nguồn <-> Mirror:
   - Tổng số dòng giữa VSS.VSS_HO_GD_NHANKHAU và C360.MIRROR_VSS_HO_GD_NHANKHAU.
2. Đối soát Mirror <-> C360:
   - COUNT(DISTINCT MA_CD) hợp lệ ở Mirror khớp 100% với COUNT(*) trên C360_THONG_TIN_CA_NHAN.
   - Công dân có nhiều hộ (MA_HO_GD) lấy đúng dòng MODIFY_DATE mới nhất.
   - 0 bản ghi rò rỉ (orphan / invalid) trên C360.
3. Đối soát Chất lượng dữ liệu (DQ_FLAGS):
   - Thống kê số lượng bản ghi sạch hoàn toàn và bản ghi dính cờ lỗi.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict
import oracledb

from src.config import SOURCE_DB, TARGET_DB


RECONCILIATION_QUERIES: Dict[str, str] = {
    "CHECK_MIRROR_VS_C360_COUNT": """
        WITH PENDING_Q AS (
            SELECT DISTINCT MA_CD FROM C360.C360_SYNC_QUEUE
        ),
        MIRROR_VALID AS (
            SELECT COUNT(DISTINCT m.MA_CD) AS MIRROR_VALID_CITIZENS
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
              AND NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = m.MA_CD)
        ),
        C360_TOTAL AS (
            SELECT COUNT(*) AS C360_CITIZENS
            FROM C360.C360_THONG_TIN_CA_NHAN c
            WHERE NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = c.MA_SO_BHXH)
        )
        SELECT
            m.MIRROR_VALID_CITIZENS,
            c.C360_CITIZENS,
            (m.MIRROR_VALID_CITIZENS - c.C360_CITIZENS) AS DIFF_COUNT,
            (SELECT COUNT(*) FROM PENDING_Q) AS IN_FLIGHT_QUEUE_MACDS,
            CASE WHEN m.MIRROR_VALID_CITIZENS = c.C360_CITIZENS THEN 'PASS' ELSE 'FAIL' END AS STATUS
        FROM MIRROR_VALID m
        CROSS JOIN C360_TOTAL c
    """,
    "CHECK_MIRROR_VS_C360_KEY_DIFF": """
        WITH PENDING_Q AS (
            SELECT DISTINCT MA_CD FROM C360.C360_SYNC_QUEUE
        ),
        MIRROR_VALID_KEYS AS (
            SELECT DISTINCT m.MA_CD
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
              AND NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = m.MA_CD)
        ),
        C360_KEYS AS (
            SELECT c.MA_SO_BHXH AS MA_CD
            FROM C360.C360_THONG_TIN_CA_NHAN c
            WHERE NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = c.MA_SO_BHXH)
        )
        SELECT
            (SELECT COUNT(*) FROM MIRROR_VALID_KEYS mk
             WHERE NOT EXISTS (SELECT 1 FROM C360_KEYS ck WHERE ck.MA_CD = mk.MA_CD)) AS MISSING_IN_C360_COUNT,
            (SELECT COUNT(*) FROM C360_KEYS ck
             WHERE NOT EXISTS (SELECT 1 FROM MIRROR_VALID_KEYS mk WHERE mk.MA_CD = ck.MA_CD)) AS EXTRA_IN_C360_COUNT
        FROM DUAL
    """,
    "CHECK_MULTI_HOUSEHOLD_LATEST_PICK": """
        WITH PENDING_Q AS (
            SELECT DISTINCT MA_CD FROM C360.C360_SYNC_QUEUE
        ),
        MULTI_HO_LATEST AS (
            SELECT
                MA_CD,
                MA_HO_GD AS EXPECTED_MA_HO_GD,
                ID       AS EXPECTED_RECORD_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY MA_CD
                    ORDER BY MODIFY_DATE DESC NULLS LAST, CREATED_DATE DESC NULLS LAST, ID DESC NULLS LAST
                ) AS RN,
                COUNT(*) OVER (PARTITION BY MA_CD) AS HO_CNT
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE NVL(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
              AND NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = m.MA_CD)
        )
        SELECT
            COUNT(*) AS TOTAL_MULTI_HO_CITIZENS,
            NVL(SUM(CASE WHEN m.EXPECTED_RECORD_ID <> c.SOURCE_RECORD_ID THEN 1 ELSE 0 END), 0) AS MISMATCH_LATEST_COUNT
        FROM MULTI_HO_LATEST m
        JOIN C360.C360_THONG_TIN_CA_NHAN c ON m.MA_CD = c.MA_SO_BHXH
        WHERE m.RN = 1 AND m.HO_CNT > 1
    """,
    "CHECK_ZERO_INVALID_OR_ORPHAN_ON_C360": """
        WITH PENDING_Q AS (
            SELECT DISTINCT MA_CD FROM C360.C360_SYNC_QUEUE
        )
        SELECT COUNT(*) AS ORPHAN_OR_INVALID_COUNT
        FROM C360.C360_THONG_TIN_CA_NHAN c
        WHERE NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = c.MA_SO_BHXH)
          AND NOT EXISTS (
            SELECT 1
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE m.MA_CD = c.MA_SO_BHXH
              AND NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
        )
    """,
    "SUMMARY_DQ_FLAGS": """
        SELECT
            COUNT(*) AS TOTAL_C360_ROWS,
            NVL(SUM(CASE WHEN IS_CLEAN = 1 THEN 1 ELSE 0 END), 0) AS CLEAN_ROWS,
            NVL(SUM(CASE WHEN IS_CLEAN = 0 THEN 1 ELSE 0 END), 0) AS DIRTY_ROWS
        FROM C360.C360_THONG_TIN_CA_NHAN
    """,
    "CHECK_MIRROR_VS_C360_FIELD_FIDELITY": """
        WITH PENDING_Q AS (
            SELECT DISTINCT MA_CD FROM C360.C360_SYNC_QUEUE
        ),
        MIRROR_LATEST AS (
            SELECT
                m.*,
                ROW_NUMBER() OVER (
                    PARTITION BY m.MA_CD
                    ORDER BY m.MODIFY_DATE DESC NULLS LAST, m.CREATED_DATE DESC NULLS LAST, m.ID DESC NULLS LAST
                ) AS RN
            FROM C360.MIRROR_VSS_HO_GD_NHANKHAU m
            WHERE NVL(m.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
              AND NVL(m.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
              AND NOT EXISTS (SELECT 1 FROM PENDING_Q q WHERE q.MA_CD = m.MA_CD)
        )
        SELECT
            COUNT(*) AS TOTAL_COMPARED,
            NVL(SUM(CASE WHEN
                NVL(TRIM(m.MA_HO_GD), '~') <> NVL(TRIM(c.MA_HO_GD), '~')
                OR NVL(REGEXP_REPLACE(TRIM(m.HO_TEN), '\\s+', ' '), '~') <> NVL(TRIM(c.HO_TEN), '~')
                OR NVL(m.GIOI_TINH, '~') <> NVL(c.GIOI_TINH, '~')
                OR NVL(m.NGAY_SINH, -1) <> NVL(c.NGAY_SINH_GOC, -1)
                OR NVL(TRIM(m.SO_CCCD), '~') <> NVL(TRIM(c.SO_CCCD_GOC), '~')
                OR NVL(TRIM(m.SO_CMTND), '~') <> NVL(TRIM(c.SO_CMTND_GOC), '~')
                OR NVL(TRIM(m.PHONE_NUMBER), '~') <> NVL(TRIM(c.SO_DIEN_THOAI_GOC), '~')
                OR NVL(m.IS_ALIVE, '~') <> NVL(c.IS_ALIVE, '~')
                OR NVL(m.DEAD_DATE, -1) <> NVL(c.DEAD_DATE, -1)
                OR NVL(TRIM(m.MA_TINH), '~') <> NVL(TRIM(c.MA_TINH_GOC), '~')
                OR NVL(TRIM(m.MA_XA), '~') <> NVL(TRIM(c.MA_XA_GOC), '~')
            THEN 1 ELSE 0 END), 0) AS MISMATCH_CONTENT_COUNT
        FROM MIRROR_LATEST m
        JOIN C360.C360_THONG_TIN_CA_NHAN c ON m.MA_CD = c.MA_SO_BHXH
        WHERE m.RN = 1
    """,
}


def build_source_mirror_checksum_sql(schema_table: str) -> str:
    """Tạo SQL tính fingerprint nội dung các cột nghiệp vụ chính để đối soát Source <-> Mirror."""
    return f"""
        SELECT
            COUNT(DISTINCT MA_CD) AS DISTINCT_MACD_CNT,
            TO_CHAR(MAX(MODIFY_DATE), 'YYYY-MM-DD HH24:MI:SS') AS MAX_MODIFY_DATE,
            NVL(SUM(ORA_HASH(
                NVL(TRIM(MA_CD), '~') || '|' ||
                CASE WHEN MA_HO_GD IS NULL OR MA_HO_GD LIKE 'UNKNOWN%' THEN 'UNKNOWN' ELSE TRIM(MA_HO_GD) END || '|' ||
                NVL(TRIM(HO_TEN), '~') || '|' ||
                NVL(TO_CHAR(NGAY_SINH), '~') || '|' ||
                NVL(TRIM(SO_CCCD), '~') || '|' ||
                NVL(TRIM(MA_TINH), '~') || '|' ||
                NVL(TRIM(MA_XA), '~') || '|' ||
                NVL(TRIM(TYPEID), '~') || '|' ||
                NVL(TO_CHAR(TRANG_THAI_DUYET), '~')
            )), 0) AS CONTENT_CHECKSUM
        FROM {schema_table}
    """


def evaluate_source_mirror_checksum(
    source_stats: Dict[str, Any],
    mirror_stats: Dict[str, Any],
    in_flight_q: int = 0,
) -> Dict[str, Any]:
    """Đánh giá khớp nội dung Source <-> Mirror qua distinct MA_CD, MAX(MODIFY_DATE) và content checksum."""
    matched = (
        source_stats["distinct_macd_cnt"] == mirror_stats["distinct_macd_cnt"]
        and source_stats["max_modify_date"] == mirror_stats["max_modify_date"]
        and source_stats["content_checksum"] == mirror_stats["content_checksum"]
    )
    if matched:
        status = "PASS"
    elif in_flight_q > 0:
        status = "IN_PROGRESS"
    else:
        status = "FAIL"
    return {
        "source_distinct_macd": source_stats["distinct_macd_cnt"],
        "mirror_distinct_macd": mirror_stats["distinct_macd_cnt"],
        "source_max_modify_date": source_stats["max_modify_date"],
        "mirror_max_modify_date": mirror_stats["max_modify_date"],
        "source_content_checksum": source_stats["content_checksum"],
        "mirror_content_checksum": mirror_stats["content_checksum"],
        "status": status,
    }


def _fetch_checksum_stats(cur: Any, schema_table: str) -> Dict[str, Any]:
    cur.execute(build_source_mirror_checksum_sql(schema_table))
    row = cur.fetchone()
    return {
        "distinct_macd_cnt": int(row[0] or 0),
        "max_modify_date": str(row[1]) if row[1] is not None else None,
        "content_checksum": int(row[2] or 0),
    }


def run_reconciliation() -> Dict[str, Any]:
    """Chạy đầy đủ các bước đối soát 3 tầng và trả về báo cáo chi tiết."""
    report: Dict[str, Any] = {"checks": {}, "overall_status": "PASS"}

    with oracledb.connect(user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn) as src_conn:
        with src_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU")
            vss_rows = int(cur.fetchone()[0])
            cur.execute(f"""
                SELECT NVL(TRIM(MA_TINH), 'NULL') AS MA_TINH, COUNT(*) AS CNT
                FROM {SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU
                GROUP BY NVL(TRIM(MA_TINH), 'NULL')
            """)
            src_province_counts = {str(r[0]): int(r[1]) for r in cur.fetchall()}
            src_checksum_stats = _fetch_checksum_stats(cur, f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU")

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as tgt_conn:
        with tgt_conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_DB.schema}.MIRROR_VSS_HO_GD_NHANKHAU")
            mirror_rows = int(cur.fetchone()[0])
            cur.execute(f"""
                SELECT NVL(TRIM(MA_TINH), 'NULL') AS MA_TINH, COUNT(*) AS CNT
                FROM {TARGET_DB.schema}.MIRROR_VSS_HO_GD_NHANKHAU
                GROUP BY NVL(TRIM(MA_TINH), 'NULL')
            """)
            mirror_province_counts = {str(r[0]): int(r[1]) for r in cur.fetchall()}
            mirror_checksum_stats = _fetch_checksum_stats(
                cur, f"{TARGET_DB.schema}.MIRROR_VSS_HO_GD_NHANKHAU"
            )
            cur.execute("SELECT COUNT(DISTINCT MA_CD) FROM C360.C360_SYNC_QUEUE")
            in_flight_q = int(cur.fetchone()[0])

            diff_src_mirror = vss_rows - mirror_rows
            if diff_src_mirror == 0:
                src_mirror_status = "PASS"
            elif in_flight_q > 0 and abs(diff_src_mirror) <= 5:
                src_mirror_status = "IN_PROGRESS"
            else:
                src_mirror_status = "FAIL"

            report["checks"]["CHECK_SOURCE_VS_MIRROR_COUNT"] = {
                "vss_rows": vss_rows,
                "mirror_rows": mirror_rows,
                "diff_count": diff_src_mirror,
                "in_flight_queue_macds": in_flight_q,
                "status": src_mirror_status,
            }

            report["checks"]["CHECK_SOURCE_VS_MIRROR_CONTENT_CHECKSUM"] = evaluate_source_mirror_checksum(
                src_checksum_stats, mirror_checksum_stats, in_flight_q=in_flight_q
            )

            all_provinces = sorted(set(src_province_counts) | set(mirror_province_counts))
            mismatched_provinces = []
            for prov in all_provinces:
                sc = src_province_counts.get(prov, 0)
                mc = mirror_province_counts.get(prov, 0)
                if sc != mc:
                    mismatched_provinces.append({"ma_tinh": prov, "source": sc, "mirror": mc, "diff": sc - mc})

            if not mismatched_provinces:
                prov_status = "PASS"
            elif in_flight_q > 0 and sum(abs(p["diff"]) for p in mismatched_provinces) <= 5:
                prov_status = "IN_PROGRESS"
            else:
                prov_status = "FAIL"

            report["checks"]["CHECK_SOURCE_VS_MIRROR_BY_PROVINCE"] = {
                "total_provinces": len(all_provinces),
                "mismatched_count": len(mismatched_provinces),
                "mismatched_provinces": mismatched_provinces[:20],
                "status": prov_status,
            }

            for name, sql in RECONCILIATION_QUERIES.items():
                cur.execute(sql)
                cols = [d[0].lower() for d in cur.description]
                row = cur.fetchone()
                data = {col: (int(val) if isinstance(val, (int, float)) else val) for col, val in zip(cols, row)}

                if name == "CHECK_MIRROR_VS_C360_KEY_DIFF":
                    missing_cnt = data.get("missing_in_c360_count", 0)
                    extra_cnt = data.get("extra_in_c360_count", 0)
                    data["status"] = "PASS" if (missing_cnt == 0 and extra_cnt == 0) else "FAIL"
                elif name == "CHECK_MULTI_HOUSEHOLD_LATEST_PICK":
                    data["status"] = "PASS" if data.get("mismatch_latest_count", 0) == 0 else "FAIL"
                elif name == "CHECK_ZERO_INVALID_OR_ORPHAN_ON_C360":
                    data["status"] = "PASS" if data.get("orphan_or_invalid_count", 0) == 0 else "FAIL"
                elif name == "CHECK_MIRROR_VS_C360_FIELD_FIDELITY":
                    data["status"] = "PASS" if data.get("mismatch_content_count", 0) == 0 else "FAIL"
                elif name == "SUMMARY_DQ_FLAGS":
                    total = max(1, data.get("total_c360_rows", 0))
                    data["clean_pct"] = round(data.get("clean_rows", 0) * 100.0 / total, 2)
                    data["status"] = "INFO"

                report["checks"][name] = data

    statuses = [c.get("status") for c in report["checks"].values()]
    if "FAIL" in statuses:
        report["overall_status"] = "FAIL"
    elif "IN_PROGRESS" in statuses:
        report["overall_status"] = "IN_PROGRESS"

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy đối soát 3 tầng Nguồn <-> Mirror <-> C360.")
    parser.add_argument("--json", action="store_true", help="Xuất kết quả dạng JSON.")
    args = parser.parse_args()

    report = run_reconciliation()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print("=" * 72)
        print(f" BAO CAO DOI SOAT 3 TANG (OVERALL: {report['overall_status']})")
        print("=" * 72)
        for name, detail in report["checks"].items():
            print(f" [{detail.get('status', 'INFO'):4s}] {name}: {detail}")
        print("=" * 72)

    if report["overall_status"] == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    main()
