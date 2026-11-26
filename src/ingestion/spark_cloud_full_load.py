"""
Oracle Cloud Autonomous DB -> local Parquet.
Job benchmark Full Load độc lập; không ghi vào Mirror/C360 và không thay đổi luồng CDC Local.
Đọc trực tiếp từ thư mục Oracle Wallet đã giải nén và tự đọc/đối chiếu alias từ tnsnames.ora.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import SparkSession

from src.config import (
    FULLLOAD_DB,
    FULLLOAD_DRIVER_MEMORY,
    FULLLOAD_EXECUTOR_MEMORY,
    FULLLOAD_FETCH_SIZE,
    FULLLOAD_PARTITION_COLUMN,
    FULLLOAD_PARTITION_MODE,
    FULLLOAD_PARTITIONS,
    FULLLOAD_SPARK_MASTER,
    FULLLOAD_UPPER_BOUND,
    TARGET_DB,
)
from src.ingestion.spark_full_load import CATALOG_TABLES, CUSTOM_ORACLE_SCHEMA

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ORACLE_JDBC_PACKAGE = "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09"
ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"
VALID_PARTITION_MODES = frozenset({"none", "ora_hash", "range"})
VALID_TARGETS = frozenset({"parquet", "mirror"})
DEFAULT_MAX_PARTITIONS = 4
HARD_MAX_PARTITIONS = 16
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_$#]*")
TNS_ALIAS_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]+)\s*=")


def validate_identifier(value: str, name: str) -> str:
    """Kiểm tra tên schema/table/column hợp lệ theo chuẩn định danh Oracle để tránh SQL injection."""
    cleaned = (value or "").strip()
    if not IDENTIFIER_PATTERN.fullmatch(cleaned):
        raise ValueError(f"Invalid Oracle identifier: {name}={value!r}")
    return cleaned


def resolve_wallet_dir(wallet_location: str) -> Path:
    """Xác minh thư mục Oracle Wallet tồn tại và tìm thấy file tnsnames.ora."""
    cleaned = (wallet_location or "").strip()
    if not cleaned:
        raise FileNotFoundError("Missing FULLLOAD_DB_WALLET_LOCATION in .env or --wallet")

    root_dir = Path(cleaned)
    if not root_dir.is_dir():
        raise FileNotFoundError(f"Wallet directory not found: {wallet_location!r}")

    if (root_dir / "tnsnames.ora").is_file():
        return root_dir
    for child in sorted(root_dir.iterdir()):
        if child.is_dir() and (child / "tnsnames.ora").is_file():
            return child

    raise FileNotFoundError(
        f"Wallet directory does not contain tnsnames.ora: {str(root_dir)!r}"
    )


def parse_tns_aliases(tnsnames_path: Path) -> list[str]:
    """Đọc danh sách TNS alias có trong file tnsnames.ora."""
    if not tnsnames_path.is_file():
        raise FileNotFoundError(f"tnsnames.ora not found: {str(tnsnames_path)!r}")
    aliases: list[str] = []
    seen: set[str] = set()
    for line in tnsnames_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = TNS_ALIAS_PATTERN.match(stripped)
        if match:
            alias = match.group(1)
            key = alias.lower()
            if key not in seen:
                seen.add(key)
                aliases.append(alias)
    return aliases


def resolve_tns_alias(tnsnames_path: Path, configured_alias: str = "") -> str:
    """
    Đối chiếu TNS alias với tnsnames.ora trong thư mục Wallet.
    Nếu configured_alias để trống, tự động chọn alias ưu tiên *_high -> *_medium -> alias đầu tiên.
    """
    aliases = parse_tns_aliases(tnsnames_path)
    if not aliases:
        raise ValueError(f"No TNS aliases found in {str(tnsnames_path)!r}")

    requested = (configured_alias or "").strip()
    if requested:
        for alias in aliases:
            if alias.lower() == requested.lower():
                return alias
        raise ValueError(
            f"TNS alias {requested!r} not found in {tnsnames_path.name}. "
            f"Available aliases: {', '.join(aliases)}"
        )

    for suffix in ("_high", "_medium"):
        for alias in aliases:
            if alias.lower().endswith(suffix):
                return alias
    return aliases[0]


def collect_driver_jars(drivers_dir: Optional[str] = None) -> list[str]:
    """Thu thập toàn bộ file .jar trong thư mục drivers/ (ojdbc11.jar và các jar Oracle Wallet PKI nếu có)."""
    target_dir = (
        Path(drivers_dir)
        if drivers_dir is not None
        else Path(__file__).resolve().parents[2] / "drivers"
    )
    if not target_dir.is_dir():
        return []
    return sorted(str(p.resolve()) for p in target_dir.glob("*.jar") if p.is_file())


def configure_windows_hadoop_home() -> Optional[str]:
    """Tự cấu hình HADOOP_HOME nếu chạy trên Windows và có sẵn drivers/hadoop/bin/winutils.exe."""
    if os.name != "nt":
        return None
    existing = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if existing and os.path.isdir(existing):
        return existing

    hadoop_home = Path(__file__).resolve().parents[2] / "drivers" / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"
    if not winutils.is_file():
        return None

    hadoop_home_str = str(hadoop_home)
    bin_dir_str = str(hadoop_home / "bin")
    os.environ["HADOOP_HOME"] = hadoop_home_str
    os.environ["hadoop.home.dir"] = hadoop_home_str
    current_path = os.environ.get("PATH", "")
    if bin_dir_str not in current_path.split(os.pathsep):
        os.environ["PATH"] = f"{bin_dir_str}{os.pathsep}{current_path}"
    return hadoop_home_str


def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession cho Cloud Full Load với đầy đủ JDBC/Wallet jars."""
    hadoop_home = configure_windows_hadoop_home()
    builder = (
        SparkSession.builder.appName("BHXH_Cloud_Full_Load")
        .master(FULLLOAD_SPARK_MASTER)
        .config("spark.driver.memory", FULLLOAD_DRIVER_MEMORY)
        .config("spark.executor.memory", FULLLOAD_EXECUTOR_MEMORY)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.files.maxRecordsPerFile", "250000")
    )
    if hadoop_home:
        builder = builder.config("spark.hadoop.hadoop.home.dir", hadoop_home)

    driver_jars = collect_driver_jars()
    if driver_jars:
        jars_csv = ",".join(driver_jars)
        classpath = os.pathsep.join(driver_jars)
        builder = (
            builder.config("spark.jars", jars_csv)
            .config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
        )
    else:
        builder = builder.config("spark.jars.packages", ORACLE_JDBC_PACKAGE)

    return builder.getOrCreate()


def build_cloud_source_query(
    schema: str,
    table: str,
    limit: Optional[int] = None,
    partition_mode: str = "ora_hash",
    partition_column: str = "MA_CD",
    num_partitions: int = 1,
    snapshot_scn: Optional[int] = None,
    safe_ma_ho_gd: bool = False,
    where_clause: Optional[str] = None,
) -> str:
    """
    Tạo biểu thức dbtable cho Spark JDBC đọc từ Oracle Cloud.

    - `ora_hash` (mặc định): chia đều partition theo ORA_HASH(cột) — an toàn cho cả cột chuỗi
      như `MA_CD` hoặc `ID` VARCHAR2(40) trong `VSS_HO_GD_NHANKHAU`.
    - `range`: chia khoảng theo cột số nguyên (`partitionColumn` + `lowerBound`..`upperBound`).
    - `none` hoặc `num_partitions == 1`: đọc tuần tự 1 luồng (phù hợp smoke test nhỏ).
    """
    safe_schema = validate_identifier(schema, "schema")
    safe_table = validate_identifier(table, "table")
    mode = (partition_mode or "none").strip().lower()
    if mode not in VALID_PARTITION_MODES:
        raise ValueError(
            f"Unsupported partition_mode={partition_mode!r}. Expected one of {sorted(VALID_PARTITION_MODES)}"
        )
    if limit is not None and limit <= 0:
        raise ValueError("--limit must be greater than zero")

    qualified_table = f"{safe_schema}.{safe_table}"
    scn_clause = f" AS OF SCN {int(snapshot_scn)}" if snapshot_scn is not None else ""
    filters: list[str] = []
    if where_clause and where_clause.strip():
        filters.append(f"({where_clause.strip()})")
    if limit is not None:
        filters.append(f"ROWNUM <= {int(limit)}")

    if filters:
        where_sql = " AND ".join(filters)
        base_from = f"(SELECT * FROM {qualified_table}{scn_clause} WHERE {where_sql})"
    else:
        base_from = f"{qualified_table}{scn_clause}"

    extra_cols: list[str] = []
    if safe_ma_ho_gd:
        extra_cols.append("NVL(t.MA_HO_GD, 'UNKNOWN_' || NVL(t.ID, 'NULL')) AS MA_HO_GD_SAFE")

    if mode == "ora_hash" and num_partitions > 1:
        safe_col = validate_identifier(partition_column, "partition_column")
        extra_cols.append(f"ORA_HASH(t.{safe_col}, {num_partitions - 1}) AS SPARK_PART_ID")

    if mode == "range" and num_partitions > 1:
        validate_identifier(partition_column, "partition_column")

    if not extra_cols and not filters and snapshot_scn is None:
        return qualified_table

    select_cols = ", ".join(["t.*", *extra_cols])
    return f"(SELECT {select_cols} FROM {base_from} t) src_cloud"


def validate_run_params(
    limit: Optional[int],
    partitions: int,
    lower_bound: int,
    upper_bound: int,
    partition_mode: str,
    partition_column: str,
    allow_high_partitions: bool = False,
    wallet_location: Optional[str] = None,
) -> tuple[str, str, Optional[str]]:
    """Kiểm tra đầy đủ cấu hình kết nối Wallet và tham số phân vùng trước khi mở SparkSession."""
    if not FULLLOAD_DB.user or not FULLLOAD_DB.password:
        raise ValueError("Missing Oracle Cloud credentials in .env")
    effective_wallet = wallet_location if wallet_location is not None else FULLLOAD_DB.wallet_location
    resolve_wallet_dir(effective_wallet)

    if limit is not None and limit <= 0:
        raise ValueError("--limit must be greater than zero")

    mode = (partition_mode or "none").strip().lower()
    if mode not in VALID_PARTITION_MODES:
        raise ValueError(
            f"--partition-mode must be one of {sorted(VALID_PARTITION_MODES)}"
        )

    max_allowed = HARD_MAX_PARTITIONS if allow_high_partitions else DEFAULT_MAX_PARTITIONS
    if not 1 <= partitions <= max_allowed:
        raise ValueError(
            f"--partitions must be between 1 and {max_allowed} "
            "(pass --allow-high-partitions only after verifying ADB session limits)"
        )

    if mode == "range" and partitions > 1 and upper_bound <= lower_bound:
        raise ValueError("upper-bound must be greater than lower-bound")

    schema = validate_identifier(FULLLOAD_DB.schema, "schema")
    table = validate_identifier(FULLLOAD_DB.table, "table")
    validated_col = (
        validate_identifier(partition_column, "partition_column")
        if mode in {"ora_hash", "range"} and partitions > 1
        else None
    )
    return schema, table, validated_col


def sync_cloud_catalogs_to_mirror(
    spark: SparkSession,
    jdbc_url: str,
    schema: str,
    snapshot_scn: Optional[int] = None,
) -> None:
    """Nạp 3 bảng danh mục từ Oracle Cloud (schema VSS) xuống Mirror Oracle Local."""
    scn_clause = f" AS OF SCN {int(snapshot_scn)}" if snapshot_scn is not None else ""
    for src_name, tgt_name in CATALOG_TABLES:
        query = f"(SELECT t.* FROM {schema}.{src_name}{scn_clause} t) src_cat"
        target_table = f"{TARGET_DB.schema}.{tgt_name}"
        df_cat = (
            spark.read.format("jdbc")
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("url", jdbc_url)
            .option("user", FULLLOAD_DB.user)
            .option("password", FULLLOAD_DB.password)
            .option("dbtable", query)
            .option("fetchsize", str(FULLLOAD_FETCH_SIZE))
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
            .option("batchsize", str(FULLLOAD_FETCH_SIZE))
            .mode("overwrite")
            .save()
        )
        print(f"[Cloud -> Mirror] Completed catalog {target_table}.")


def run_full_load(
    limit: Optional[int],
    partitions: int,
    lower_bound: int,
    upper_bound: int,
    output_path: str,
    partition_mode: str = FULLLOAD_PARTITION_MODE,
    partition_column: str = FULLLOAD_PARTITION_COLUMN,
    snapshot_scn: Optional[int] = None,
    apply_vss_schema: bool = False,
    allow_high_partitions: bool = False,
    wallet_location: Optional[str] = None,
    tns_alias: Optional[str] = None,
    target: str = "parquet",
    skip_catalogs: bool = False,
    append_mode: bool = False,
    where_clause: Optional[str] = None,
) -> dict[str, Any]:
    """Đọc 1 lượt từ Oracle Cloud Autonomous DB và ghi ra Parquet local hoặc Oracle Local Mirror."""
    mode = (partition_mode or "none").strip().lower()
    target_mode = (target or "parquet").strip().lower()
    if target_mode not in VALID_TARGETS:
        raise ValueError(f"--target must be one of {sorted(VALID_TARGETS)}")

    effective_wallet = wallet_location if wallet_location is not None else FULLLOAD_DB.wallet_location
    requested_alias = tns_alias if tns_alias is not None else FULLLOAD_DB.tns_alias

    schema, table, validated_col = validate_run_params(
        limit=limit,
        partitions=partitions,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        partition_mode=mode,
        partition_column=partition_column,
        allow_high_partitions=allow_high_partitions,
        wallet_location=effective_wallet,
    )

    wallet_dir = resolve_wallet_dir(effective_wallet)
    effective_alias = resolve_tns_alias(wallet_dir / "tnsnames.ora", requested_alias)
    jdbc_url = FULLLOAD_DB.build_jdbc_url(effective_alias, str(wallet_dir))

    source_table = f"{schema}.{table}"
    safe_ma_ho_gd = target_mode == "mirror" and table.upper() == "VSS_HO_GD_NHANKHAU"
    dbtable = build_cloud_source_query(
        schema=schema,
        table=table,
        limit=limit,
        partition_mode=mode,
        partition_column=validated_col or partition_column,
        num_partitions=partitions,
        snapshot_scn=snapshot_scn,
        safe_ma_ho_gd=safe_ma_ho_gd,
        where_clause=where_clause,
    )
    use_vss_schema = apply_vss_schema or table.upper() == "VSS_HO_GD_NHANKHAU"

    spark = create_spark_session()
    try:
        if target_mode == "mirror" and not skip_catalogs:
            sync_cloud_catalogs_to_mirror(spark, jdbc_url=jdbc_url, schema=schema, snapshot_scn=snapshot_scn)

        reader = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", ORACLE_JDBC_DRIVER)
            .option("dbtable", dbtable)
            .option("user", FULLLOAD_DB.user)
            .option("password", FULLLOAD_DB.password)
            .option("fetchsize", str(FULLLOAD_FETCH_SIZE))
        )

        if use_vss_schema:
            reader = reader.option("customSchema", CUSTOM_ORACLE_SCHEMA)

        if mode == "ora_hash" and partitions > 1:
            reader = (
                reader.option("partitionColumn", "SPARK_PART_ID")
                .option("lowerBound", "0")
                .option("upperBound", str(partitions))
                .option("numPartitions", str(partitions))
            )
        elif mode == "range" and partitions > 1 and validated_col:
            reader = (
                reader.option("partitionColumn", validated_col)
                .option("lowerBound", str(lower_bound))
                .option("upperBound", str(upper_bound))
                .option("numPartitions", str(partitions))
            )

        print(f"[Cloud Full Load] Wallet: {wallet_dir}")
        print(f"[Cloud Full Load] TNS Alias: {effective_alias}")
        print(f"[Cloud Full Load] Source: {source_table}")
        print(f"[Cloud Full Load] Target: {target_mode}")
        print(f"[Cloud Full Load] Partition Mode: {mode} (partitions={partitions})")
        if snapshot_scn is not None:
            print(f"[Cloud Full Load] Snapshot SCN: {snapshot_scn}")
        if target_mode == "parquet":
            print(f"[Cloud Full Load] Output: {output_path}")

        started = time.perf_counter()
        df = reader.load()
        if mode == "ora_hash" and partitions > 1:
            df = df.drop("SPARK_PART_ID")
        if safe_ma_ho_gd:
            df = df.drop("MA_HO_GD").withColumnRenamed("MA_HO_GD_SAFE", "MA_HO_GD")

        if target_mode == "mirror":
            target_table = f"{TARGET_DB.schema}.MIRROR_{table.upper()}"
            writer = (
                df.write.format("jdbc")
                .option("driver", ORACLE_JDBC_DRIVER)
                .option("url", TARGET_DB.jdbc_url)
                .option("user", TARGET_DB.user)
                .option("password", TARGET_DB.password)
                .option("dbtable", target_table)
                .option("batchsize", str(FULLLOAD_FETCH_SIZE))
                .option("isolationLevel", "NONE")
            )
            if append_mode:
                writer.mode("append").save()
            else:
                writer.option("truncate", "true").mode("overwrite").save()
            elapsed = time.perf_counter() - started
            import oracledb
            with oracledb.connect(
                user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {target_table}")
                    row_count = int(cur.fetchone()[0])
            dest_desc = target_table
        else:
            # Ghi thẳng 1 pass để không đọc Oracle Cloud 2 lần; sau đó đếm lại từ Parquet local.
            df.write.mode("overwrite").parquet(output_path)
            elapsed = time.perf_counter() - started
            row_count = spark.read.parquet(output_path).count()
            dest_desc = output_path

        throughput = row_count / max(elapsed, 0.001)

        print(f"[Cloud Full Load] Rows: {row_count:,}")
        print(f"[Cloud Full Load] Duration: {elapsed:.2f} seconds")
        print(f"[Cloud Full Load] Throughput: {throughput:,.2f} rows/sec")
        print(f"[Cloud Full Load] Destination: {dest_desc}")

        return {
            "source": source_table,
            "target": target_mode,
            "destination": dest_desc,
            "tns_alias": effective_alias,
            "partition_mode": mode,
            "partitions": partitions,
            "rows": row_count,
            "duration_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(throughput, 2),
            "output_path": dest_desc,
            "snapshot_scn": snapshot_scn,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Oracle Cloud Autonomous DB Full Load to local Parquet"
    )
    parser.add_argument(
        "--wallet",
        default=None,
        help="Đường dẫn thư mục Wallet đã giải nén (mặc định lấy từ FULLLOAD_DB_WALLET_LOCATION).",
    )
    parser.add_argument(
        "--tns-alias",
        default=None,
        help="Tên TNS alias trong tnsnames.ora (mặc định lấy từ FULLLOAD_DB_TNS_ALIAS hoặc tự chọn *_high).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Giới hạn số dòng cho smoke test.")
    parser.add_argument(
        "--partitions",
        type=int,
        default=min(FULLLOAD_PARTITIONS, DEFAULT_MAX_PARTITIONS),
        help="Số partition JDBC song song (mặc định 2, tối đa 4 trên ADB Free).",
    )
    parser.add_argument(
        "--partition-mode",
        choices=sorted(VALID_PARTITION_MODES),
        default=FULLLOAD_PARTITION_MODE,
        help="Chiến lược phân vùng: ora_hash (khuyến nghị cho MA_CD/ID chuỗi), range (cột số), hoặc none.",
    )
    parser.add_argument(
        "--partition-column",
        default=FULLLOAD_PARTITION_COLUMN,
        help="Tên cột phân vùng (mặc định MA_CD).",
    )
    parser.add_argument("--lower-bound", type=int, default=1, help="Cận dưới khi dùng --partition-mode range.")
    parser.add_argument(
        "--upper-bound",
        type=int,
        default=FULLLOAD_UPPER_BOUND,
        help="Cận trên khi dùng --partition-mode range.",
    )
    parser.add_argument("--scn", type=int, default=None, help="Oracle SCN cố định để đọc AS OF SCN.")
    parser.add_argument(
        "--apply-vss-schema",
        action="store_true",
        help="Áp customSchema cho các cột NUMBER không có scale của bảng VSS_HO_GD_NHANKHAU.",
    )
    parser.add_argument(
        "--allow-high-partitions",
        action="store_true",
        help="Cho phép vượt ngưỡng 4 partitions (tối đa 16) khi ADB có đủ CPU/session.",
    )
    parser.add_argument(
        "--target",
        choices=sorted(VALID_TARGETS),
        default="parquet",
        help="Đích ghi dữ liệu: parquet (local benchmark) hoặc mirror (ghi vào C360.MIRROR_* trên Oracle Local).",
    )
    parser.add_argument(
        "--skip-catalogs",
        action="store_true",
        help="Khi --target mirror: bỏ qua nạp 3 bảng danh mục.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Khi --target mirror: ghi nối tiếp (mode append) thay vì truncate/overwrite.",
    )
    parser.add_argument(
        "--where",
        default=None,
        help="Điều kiện WHERE bổ sung khi đọc nguồn Cloud (ví dụ: \"MA_CD > '1003000000'\").",
    )
    parser.add_argument("--output", default=None, help="Thư mục Parquet đầu ra (khi --target parquet).")
    args = parser.parse_args()

    table_slug = (FULLLOAD_DB.table or "cloud_table").lower()
    output = args.output or f"data/bronze_cloud/{table_slug}_{int(time.time())}"

    run_full_load(
        limit=args.limit,
        partitions=args.partitions,
        lower_bound=args.lower_bound,
        upper_bound=args.upper_bound,
        output_path=output,
        partition_mode=args.partition_mode,
        partition_column=args.partition_column,
        snapshot_scn=args.scn,
        apply_vss_schema=args.apply_vss_schema,
        allow_high_partitions=args.allow_high_partitions,
        wallet_location=args.wallet,
        tns_alias=args.tns_alias,
        target=args.target,
        skip_catalogs=args.skip_catalogs,
        append_mode=args.append,
        where_clause=args.where,
    )


if __name__ == "__main__":
    main()
