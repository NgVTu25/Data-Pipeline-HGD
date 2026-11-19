"""
Phần 1.2 - Job nạp toàn bộ (PySpark JDBC Full Load: Nguồn VSS -> Mirror C360)
"""
import os
import sys
from pyspark.sql import SparkSession
from src.config import SOURCE_DB, TARGET_DB, JDBC_NUM_PARTITIONS, JDBC_FETCH_SIZE

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ORACLE_JDBC_PACKAGE = "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09"
ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"


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


def build_partitioned_source_query(table_name: str, num_partitions: int) -> str:
    """
    Chia partition đều trên cột chuỗi MA_CD bằng hàm băm nội tại ORA_HASH
    """
    return f"""
    (
        SELECT t.*,
               NVL(t.MA_HO_GD, 'UNKNOWN') AS MA_HO_GD_SAFE,
               ORA_HASH(t.MA_CD, {num_partitions - 1}) AS SPARK_PART_ID
        FROM {table_name} t
    ) src_alias
    """


def run_full_load() -> None:
    spark = create_spark_session()
    source_table = f"{SOURCE_DB.schema}.VSS_HO_GD_NHANKHAU"
    target_table = f"{TARGET_DB.schema}.MIRROR_VSS_HO_GD_NHANKHAU"

    # 1. Đọ DB nguồn theo SPARK_PART_ID 
    dbtable_subquery = build_partitioned_source_query(source_table, JDBC_NUM_PARTITIONS)
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

    print("[Full Load] Completed writing main table. Ready for post-load row count verification & Unique Index build.")
    spark.stop()


if __name__ == "__main__":
    run_full_load()
