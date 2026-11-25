"""
Quản lý cấu hình tập trung từ biến môi trường / file `.env`.
Tuyệt đối không hard-code thông tin nhạy cảm (Mật khẩu DB, API Key) trong code.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    service: str
    user: str
    password: str
    schema: str

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service}"


SOURCE_DB = DatabaseConfig(
    host=os.getenv("SOURCE_DB_HOST", "localhost"),
    port=int(os.getenv("SOURCE_DB_PORT", "1521")),
    service=os.getenv("SOURCE_DB_SERVICE", "FREEPDB1"),
    user=os.getenv("SOURCE_DB_USER", "vss_source"),
    password=os.getenv("SOURCE_DB_PASSWORD", ""),
    schema=os.getenv("SOURCE_DB_SCHEMA", "VSS"),
)

TARGET_DB = DatabaseConfig(
    host=os.getenv("TARGET_DB_HOST", "localhost"),
    port=int(os.getenv("TARGET_DB_PORT", "1521")),
    service=os.getenv("TARGET_DB_SERVICE", "FREEPDB1"),
    user=os.getenv("TARGET_DB_USER", "c360_target"),
    password=os.getenv("TARGET_DB_PASSWORD", ""),
    schema=os.getenv("TARGET_DB_SCHEMA", "C360"),
)


@dataclass(frozen=True)
class AutonomousDatabaseConfig:
    tns_alias: str
    user: str
    password: str
    wallet_location: str
    schema: str
    table: str

    @property
    def wallet_path_normalized(self) -> str:
        return self.wallet_location.strip().replace("\\", "/")

    @staticmethod
    def build_jdbc_url(tns_alias: str, wallet_dir: str) -> str:
        normalized_dir = wallet_dir.strip().replace("\\", "/")
        return f"jdbc:oracle:thin:@{tns_alias}?TNS_ADMIN={normalized_dir}"

    @property
    def jdbc_url(self) -> str:
        return self.build_jdbc_url(self.tns_alias, self.wallet_location)


FULLLOAD_DB = AutonomousDatabaseConfig(
    tns_alias=os.getenv("FULLLOAD_DB_TNS_ALIAS", "").strip(),
    user=os.getenv("FULLLOAD_DB_USER", "").strip(),
    password=os.getenv("FULLLOAD_DB_PASSWORD", ""),
    wallet_location=os.getenv("FULLLOAD_DB_WALLET_LOCATION", "").strip(),
    schema=os.getenv("FULLLOAD_DB_SCHEMA", "VSS").strip(),
    table=os.getenv("FULLLOAD_DB_TABLE", "VSS_HO_GD_NHANKHAU").strip(),
)

APP_ENV = os.getenv("APP_ENV", "development").lower()
SEED_NUM_CITIZENS = int(os.getenv("SEED_NUM_CITIZENS", "50000"))
SEED_BATCH_SIZE = int(os.getenv("SEED_BATCH_SIZE", "5000"))
JDBC_NUM_PARTITIONS = int(os.getenv("JDBC_NUM_PARTITIONS", "16"))
JDBC_FETCH_SIZE = int(os.getenv("JDBC_FETCH_SIZE", "10000"))
FULLLOAD_SPARK_MASTER = os.getenv("FULLLOAD_SPARK_MASTER", os.getenv("SPARK_MASTER", "local[2]"))
FULLLOAD_DRIVER_MEMORY = os.getenv("FULLLOAD_DRIVER_MEMORY", "2g")
FULLLOAD_EXECUTOR_MEMORY = os.getenv("FULLLOAD_EXECUTOR_MEMORY", "2g")
FULLLOAD_PARTITIONS = int(os.getenv("FULLLOAD_PARTITIONS", "2"))
FULLLOAD_FETCH_SIZE = int(os.getenv("FULLLOAD_FETCH_SIZE", "5000"))
FULLLOAD_PARTITION_MODE = os.getenv("FULLLOAD_PARTITION_MODE", "ora_hash").strip().lower()
FULLLOAD_PARTITION_COLUMN = os.getenv("FULLLOAD_PARTITION_COLUMN", "MA_CD").strip()
FULLLOAD_UPPER_BOUND = int(os.getenv("FULLLOAD_UPPER_BOUND", "10000000"))
API_SAFE_LAG_SECONDS = int(os.getenv("API_SAFE_LAG_SECONDS", "120"))
AUDIT_LOG_BATCH_SIZE = int(os.getenv("AUDIT_LOG_BATCH_SIZE", "100"))
AUDIT_LOG_FLUSH_INTERVAL_SEC = float(os.getenv("AUDIT_LOG_FLUSH_INTERVAL_SEC", "5.0"))
