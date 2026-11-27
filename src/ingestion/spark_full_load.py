"""
Phần 1.2 - Job nạp toàn bộ (PySpark JDBC Full Load: Nguồn VSS -> Mirror C360)
Hỗ trợ đồng bộ điểm cắt (Cutover) nhất quán theo Oracle SCN (AS OF SCN)
cho cả bảng chính VSS_HO_GD_NHANKHAU và 3 bảng danh mục.
"""
import argparse
import os
import sys
from typing import Optional
import oracledb
from pyspark.sql import SparkSession
from src.config import SOURCE_DB, TARGET_DB, JDBC_NUM_PARTITIONS, JDBC_FETCH_SIZE
from src.ingestion.cutover_control import (
    CutoverState,
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_PIPELINE_NAME,
    determine_resume_action,
    get_cutover_state,
    upsert_cutover_state,
)

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ORACLE_JDBC_PACKAGE = "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09"
ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"

CATALOG_TABLES = [
    ("VSS_DM_DANTOC", "MIRROR_VSS_DM_DANTOC"),
    ("VSS_DM_QUOC_TICH", "MIRROR_VSS_DM_QUOC_TICH"),
    ("VSS_DM_DBHC", "MIRROR_VSS_DM_DBHC"),
]


CUSTOM_ORACLE_SCHEMA = (
    "NGAY_SINH DECIMAL(19,0), NGAY_KS DECIMAL(19,0), NGAY_CAP_CMTND DECIMAL(19,0), "
    "NGAY_CAP_HC DECIMAL(19,0), DEAD_DATE DECIMAL(19,0), NGAY_BATDAU DECIMAL(19,0), "
    "NGAY_HETHAN DECIMAL(19,0), NGAY_PHATSINH DECIMAL(19,0), NGAY_SINH_BK DECIMAL(19,0), "
    "LOCK_DATA DECIMAL(19,0), DM_DAN_TOC_ID DECIMAL(19,0), USER_DMS_ID DECIMAL(19,0), "
    "USER_DUYET_DMS_ID DECIMAL(19,0)"
)


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("HGD_VSS_To_Mirror_FullLoad")
        .config("spark.sql.shuffle.partitions", str(JDBC_NUM_PARTITIONS))
    )
    local_jar = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "drivers", "ojdbc11.jar")
    )
    if os.path.exists(local_jar):
        builder = (
            builder.config("spark.driver.extraClassPath", local_jar)
            .config("spark.executor.extraClassPath", local_jar)
        )
    else:
        builder = builder.config("spark.jars.packages", ORACLE_JDBC_PACKAGE)
    return builder.getOrCreate()


def get_current_source_scn() -> int:
    """Lấy CURRENT_SCN từ Oracle Source để làm mốc snapshot nhất quán và cutover CDC."""
    with oracledb.connect(
        user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn
    ) as conn:
        with conn.cursor() as cur:
            for query in (
                "SELECT CURRENT_SCN FROM V$DATABASE",
                "SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP) FROM DUAL",
            ):
                try:
                    cur.execute(query)
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return int(row[0])
                except oracledb.DatabaseError:
                    continue
            raise RuntimeError("Could not retrieve current SCN from Oracle Source")


def build_partitioned_source_query(
    table_name: str,
    num_partitions: int,
    snapshot_scn: Optional[int] = None,
) -> str:
    """
    Chia partition đều trên cột chuỗi MA_CD bằng hàm băm nội tại ORA_HASH.
    Nếu có snapshot_scn, đọc nhất quán tại đúng AS OF SCN {snapshot_scn}.
    Xử lý MA_HO_GD NULL an toàn theo ID để tránh va chạm khóa 'UNKNOWN'.
    """
    scn_clause = f" AS OF SCN {int(snapshot_scn)}" if snapshot_scn is not None else ""
    return f"""
    (
        SELECT t.*,
               NVL(t.MA_HO_GD, 'UNKNOWN_' || NVL(t.ID, 'NULL')) AS MA_HO_GD_SAFE,
               ORA_HASH(t.MA_CD, {num_partitions - 1}) AS SPARK_PART_ID
        FROM {table_name}{scn_clause} t
    ) src_alias
    """


def build_catalog_source_query(
    table_name: str,
    snapshot_scn: Optional[int] = None,
) -> str:
    """Tạo truy vấn đọc bảng danh mục tại cùng mốc AS OF SCN."""
    scn_clause = f" AS OF SCN {int(snapshot_scn)}" if snapshot_scn is not None else ""
    return f"(SELECT t.* FROM {table_name}{scn_clause} t) src_cat"


def full_load_catalog_tables(
    spark: SparkSession,
    snapshot_scn: Optional[int] = None,
) -> None:
    """Nạp toàn bộ 3 bảng danh mục từ Source -> Mirror tại cùng mốc SCN."""
    for src_name, tgt_name in CATALOG_TABLES:
        source_table = f"{SOURCE_DB.schema}.{src_name}"
        target_table = f"{TARGET_DB.schema}.{tgt_name}"
        query = build_catalog_source_query(source_table, snapshot_scn=snapshot_scn)

        df_cat = (
            spark.read.format("jdbc")
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("url", SOURCE_DB.jdbc_url)
            .option("user", SOURCE_DB.user)
            .option("password", SOURCE_DB.password)
            .option("dbtable", query)
            .option("fetchsize", str(JDBC_FETCH_SIZE))
            .load()
        )
        (
            df_cat.write.format("jdbc")
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("url", TARGET_DB.jdbc_url)
            .option("user", TARGET_DB.user)
            .option("password", TARGET_DB.password)
            .option("dbtable", target_table)
            .option("truncate", "true")
            .option("batchsize", str(JDBC_FETCH_SIZE))
            .mode("overwrite")
            .save()
        )
        print(f"[Full Load] Completed catalog table {target_table} (SCN={snapshot_scn}).")


def _safe_read_cutover_state() -> Optional[CutoverState]:
    try:
        with oracledb.connect(
            user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn
        ) as conn:
            return get_cutover_state(conn, DEFAULT_PIPELINE_NAME)
    except Exception as exc:
        print(f"[Full Load] Warning: could not read C360_CDC_CUTOVER_CONTROL ({exc}).")
        return None


def _safe_update_cutover_state(state: CutoverState) -> None:
    try:
        with oracledb.connect(
            user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn
        ) as conn:
            upsert_cutover_state(conn, state)
    except Exception as exc:
        print(f"[Full Load] Warning: could not update C360_CDC_CUTOVER_CONTROL ({exc}).")


def run_full_load(
    snapshot_scn: Optional[int] = None,
    include_catalogs: bool = True,
    resume: bool = False,
) -> int:
    if resume:
        existing_state = _safe_read_cutover_state()
        plan = determine_resume_action(existing_state)
        print(f"[Full Load][Resume] Action={plan['action']} | Reason={plan['reason']}")
        if plan["skip_full_load"] and plan["resume_scn"] is not None:
            print(
                f"[Full Load][Resume] Skipping Full Load. Continue CDC from SCN={plan['resume_scn']}."
            )
            return int(plan["resume_scn"])

    if snapshot_scn is None:
        snapshot_scn = get_current_source_scn()

    _safe_update_cutover_state(
        CutoverState(
            pipeline_name=DEFAULT_PIPELINE_NAME,
            snapshot_scn=snapshot_scn,
            cdc_start_scn=None,
            cdc_last_scn=None,
            kafka_topic=DEFAULT_KAFKA_TOPIC,
            connector_status="NOT_STARTED",
            cutover_status="FULL_LOAD_RUNNING",
            error_message=None,
        )
    )

    print(f"[Full Load] Starting consistent snapshot Full Load AS OF SCN = {snapshot_scn}")
    spark = create_spark_session()
    try:
        if include_catalogs:
            full_load_catalog_tables(spark, snapshot_scn=snapshot_scn)

        source_table = f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
        target_table = f"{TARGET_DB.schema}.MIRROR_VSS_HO_GD_NHANKHAU"

        # 1. Đọc DB nguồn theo SPARK_PART_ID tại đúng mốc snapshot_scn
        dbtable_subquery = build_partitioned_source_query(
            source_table,
            JDBC_NUM_PARTITIONS,
            snapshot_scn=snapshot_scn,
        )
        df_source = (
            spark.read.format("jdbc")
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("url", SOURCE_DB.jdbc_url)
            .option("user", SOURCE_DB.user)
            .option("password", SOURCE_DB.password)
            .option("dbtable", dbtable_subquery)
            .option("partitionColumn", "SPARK_PART_ID")
            .option("lowerBound", "0")
            .option("upperBound", str(JDBC_NUM_PARTITIONS - 1))
            .option("numPartitions", str(JDBC_NUM_PARTITIONS))
            .option("fetchsize", str(JDBC_FETCH_SIZE))
            .option("customSchema", CUSTOM_ORACLE_SCHEMA)
            .load()
            .drop("SPARK_PART_ID", "MA_HO_GD")
            .withColumnRenamed("MA_HO_GD_SAFE", "MA_HO_GD")
        )

        # 2. Ghi xuống bảng Mirror với mode("overwrite") + option("truncate", "true")
        (
            df_source.write.format("jdbc")
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("url", TARGET_DB.jdbc_url)
            .option("user", TARGET_DB.user)
            .option("password", TARGET_DB.password)
            .option("dbtable", target_table)
            .option("truncate", "true")
            .option("batchsize", str(JDBC_FETCH_SIZE))
            .mode("overwrite")
            .save()
        )

        _safe_update_cutover_state(
            CutoverState(
                pipeline_name=DEFAULT_PIPELINE_NAME,
                snapshot_scn=snapshot_scn,
                cdc_start_scn=snapshot_scn,
                cdc_last_scn=None,
                kafka_topic=DEFAULT_KAFKA_TOPIC,
                connector_status="NOT_STARTED",
                cutover_status="FULL_LOAD_DONE",
                error_message=None,
            )
        )

        print(
            f"[Full Load] Completed writing main & catalog tables at SCN={snapshot_scn}. "
            "Configure Debezium CDC start/cutover from this SCN, then build Unique Index."
        )
        return snapshot_scn
    except Exception as exc:
        _safe_update_cutover_state(
            CutoverState(
                pipeline_name=DEFAULT_PIPELINE_NAME,
                snapshot_scn=snapshot_scn,
                cdc_start_scn=None,
                cdc_last_scn=None,
                kafka_topic=DEFAULT_KAFKA_TOPIC,
                connector_status="NOT_STARTED",
                cutover_status="FAILED",
                error_message=str(exc),
            )
        )
        raise
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Full Load VSS -> Mirror với SCN Cutover.")
    parser.add_argument("--scn", type=int, default=None, help="Oracle SCN cố định để đọc AS OF SCN.")
    parser.add_argument("--skip-catalogs", action="store_true", help="Bỏ qua 3 bảng danh mục.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Kiểm tra bảng C360_CDC_CUTOVER_CONTROL để bỏ qua Full Load nếu đã chốt SCN.",
    )
    args = parser.parse_args()
    run_full_load(
        snapshot_scn=args.scn,
        include_catalogs=not args.skip_catalogs,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
