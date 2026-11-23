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

APP_ENV = os.getenv("APP_ENV", "development").lower()
SEED_NUM_CITIZENS = int(os.getenv("SEED_NUM_CITIZENS", "50000"))
SEED_BATCH_SIZE = int(os.getenv("SEED_BATCH_SIZE", "5000"))
JDBC_NUM_PARTITIONS = int(os.getenv("JDBC_NUM_PARTITIONS", "16"))
JDBC_FETCH_SIZE = int(os.getenv("JDBC_FETCH_SIZE", "10000"))
API_SAFE_LAG_SECONDS = int(os.getenv("API_SAFE_LAG_SECONDS", "120"))
AUDIT_LOG_BATCH_SIZE = int(os.getenv("AUDIT_LOG_BATCH_SIZE", "100"))
AUDIT_LOG_FLUSH_INTERVAL_SEC = float(os.getenv("AUDIT_LOG_FLUSH_INTERVAL_SEC", "5.0"))
