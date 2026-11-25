"""
Unit tests kiểm chứng các cải tiến P0, P1, P2 trong Data-Pipeline-HGD.
Chạy độc lập bằng `python -m unittest tests/test_pipeline_improvements.py -v` (không cần Oracle/Spark chạy thật).
"""
import asyncio
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT_DIR = Path(__file__).resolve().parent.parent
VENV_SITE = ROOT_DIR / ".venv" / "Lib" / "site-packages"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if VENV_SITE.exists() and str(VENV_SITE) not in sys.path:
    sys.path.insert(0, str(VENV_SITE))

# Fallback stub cho oracledb / fastapi / pydantic nếu chạy ngoài .venv chuẩn version
if "oracledb" not in sys.modules:
    try:
        import oracledb  # noqa: F401
    except Exception:
        stub_oracle = types.ModuleType("oracledb")
        stub_oracle.ConnectionPool = object
        stub_oracle.Connection = object
        stub_oracle.Cursor = object
        sys.modules["oracledb"] = stub_oracle

try:
    import fastapi  # noqa: F401
    import pydantic  # noqa: F401
except Exception:
    class _DummyApp:
        def __init__(self, **_kwargs):
            pass

        def middleware(self, *_args, **_kwargs):
            return lambda fn: fn

        def include_router(self, *_args, **_kwargs):
            pass

    class _DummyRouter:
        def __init__(self, **_kwargs):
            pass

        def get(self, *_args, **_kwargs):
            return lambda fn: fn

        def post(self, *_args, **_kwargs):
            return lambda fn: fn

    class _DummyBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    stub_pydantic = types.ModuleType("pydantic")
    stub_pydantic.BaseModel = _DummyBaseModel
    stub_pydantic.Field = lambda default=None, **_kwargs: default
    sys.modules["pydantic"] = stub_pydantic

    stub_fastapi = types.ModuleType("fastapi")
    stub_fastapi.FastAPI = _DummyApp
    stub_fastapi.APIRouter = _DummyRouter
    stub_fastapi.Request = object
    stub_fastapi.HTTPException = Exception
    stub_fastapi.Depends = lambda x=None: x
    stub_fastapi.Query = lambda default=None, **_kwargs: default
    stub_fastapi.Header = lambda default=None, **_kwargs: default
    stub_fastapi.status = types.SimpleNamespace(
        HTTP_401_UNAUTHORIZED=401,
        HTTP_403_FORBIDDEN=403,
        HTTP_404_NOT_FOUND=404,
        HTTP_409_CONFLICT=409,
    )
    stub_responses = types.ModuleType("fastapi.responses")
    stub_responses.JSONResponse = dict
    sys.modules["fastapi"] = stub_fastapi
    sys.modules["fastapi.responses"] = stub_responses


class TestPipelineImprovements(unittest.TestCase):
    def test_build_partitioned_source_query_scn_and_null_safe_ma_ho_gd(self):
        # Đảm bảo import được ngay cả khi pyspark chưa load JVM
        if "pyspark" not in sys.modules:
            try:
                import pyspark.sql  # noqa: F401
            except Exception:
                pyspark_mod = types.ModuleType("pyspark")
                pyspark_sql = types.ModuleType("pyspark.sql")
                pyspark_sql.SparkSession = object
                sys.modules["pyspark"] = pyspark_mod
                sys.modules["pyspark.sql"] = pyspark_sql

        from src.ingestion.spark_full_load import build_partitioned_source_query

        q_with_scn = build_partitioned_source_query("VSS.VSS_HO_GD_NHANKHAU", 16, snapshot_scn=987654321)
        self.assertIn("AS OF SCN 987654321", q_with_scn)
        self.assertIn("NVL(t.MA_HO_GD, 'UNKNOWN_' || NVL(t.ID, 'NULL'))", q_with_scn)
        self.assertIn("ORA_HASH(t.MA_CD, 15)", q_with_scn)

        q_no_scn = build_partitioned_source_query("VSS.VSS_HO_GD_NHANKHAU", 8, snapshot_scn=None)
        self.assertNotIn("AS OF SCN", q_no_scn)
        self.assertIn("ORA_HASH(t.MA_CD, 7)", q_no_scn)

    def test_redact_query_params_masks_pii_only(self):
        from src.api.security import redact_query_params

        raw = "ho_ten=Nguy%E1%BB%85n+V%C4%83n+A&ngay_sinh=19900101&limit=50&safe_lag_seconds=120"
        redacted = redact_query_params(raw)
        self.assertIn("ho_ten=%2A%2A%2A", redacted)
        self.assertIn("ngay_sinh=%2A%2A%2A", redacted)
        self.assertIn("limit=50", redacted)
        self.assertIn("safe_lag_seconds=120", redacted)
        self.assertNotIn("19900101", redacted)
        self.assertNotIn("Nguy", redacted)
        self.assertEqual(redact_query_params(""), "")

    def test_to_unsigned_upper_vietnamese_normalization(self):
        from src.api.routers.ca_nhan import to_unsigned_upper

        self.assertEqual(to_unsigned_upper("  Nguyễn   Đình   Thắng  "), "NGUYEN DINH THANG")
        self.assertEqual(to_unsigned_upper("đặng thị ánh tuyết"), "DANG THI ANH TUYET")
        self.assertEqual(to_unsigned_upper("TRẦN  VĂN  B"), "TRAN VAN B")

    def test_batch_lookup_set_query_tiers_conflict_and_dedup(self):
        from src.api.routers import ca_nhan

        calls = []

        def fake_fetch_all(sql: str, params: dict):
            calls.append((sql, params))
            vals = set(params.values())
            rows = []
            if "WHERE MA_SO_BHXH IN" in sql:
                if "CD001" in vals:
                    rows.append({"MA_SO_BHXH": "CD001", "HO_TEN": "A", "SO_DINH_DANH": "001090000001"})
            elif "WHERE MA_SO_BHXH_CU IN" in sql:
                if "OLD002" in vals:
                    rows.append({
                        "MA_SO_BHXH": "CD002",
                        "MA_SO_BHXH_CU": "OLD002",
                        "HO_TEN": "B",
                        "SO_DINH_DANH": "001090000002",
                    })
            elif "WHERE SO_DINH_DANH IN" in sql:
                if "CCCD_DUP" in vals:
                    rows.append({"MA_SO_BHXH": "CD003A", "HO_TEN": "C1", "SO_DINH_DANH": "CCCD_DUP"})
                    rows.append({"MA_SO_BHXH": "CD003B", "HO_TEN": "C2", "SO_DINH_DANH": "CCCD_DUP"})
            return rows

        with patch.object(ca_nhan, "fetch_all_dicts", side_effect=fake_fetch_all):
            res = ca_nhan._batch_lookup_codes_sync(
                ["CD001", " CD001 ", "OLD002", "CCCD_DUP", "MISSING_999"],
                chunk_size=2,
            )

        self.assertEqual(res["requested_count"], 5)
        self.assertEqual(res["unique_count"], 4)
        self.assertEqual(res["found_count"], 3)  # CD001, ' CD001 ', OLD002
        self.assertEqual(res["results"]["CD001"]["status"], "FOUND")
        self.assertEqual(res["results"]["CD001"]["matched_tier"], "MA_SO_BHXH")
        self.assertEqual(res["results"]["OLD002"]["status"], "FOUND")
        self.assertEqual(res["results"]["OLD002"]["matched_tier"], "MA_SO_BHXH_CU")
        self.assertEqual(res["results"]["CCCD_DUP"]["status"], "CONFLICT")
        self.assertEqual(len(res["results"]["CCCD_DUP"]["candidates"]), 2)
        self.assertEqual(res["results"]["MISSING_999"]["status"], "NOT_FOUND")
        # CD001 đã khớp ở tầng 1 nên không bị truy vấn ở tầng 2 và tầng 3
        tier2_params = [p for sql, p in calls if "WHERE MA_SO_BHXH_CU IN" in sql]
        self.assertTrue(all("CD001" not in p.values() for p in tier2_params))

    def test_dev_api_key_only_seeded_in_development(self):
        from src.api import main as api_main

        async def _run_lifespan(env_value: str) -> MagicMock:
            mock_seed = MagicMock()
            with (
                patch.object(api_main, "APP_ENV", env_value),
                patch.object(api_main, "init_db_pool"),
                patch.object(api_main, "close_db_pool"),
                patch.object(api_main, "ensure_default_dev_api_key", mock_seed),
                patch.object(api_main.audit_logger, "start_worker", return_value=None),
                patch.object(api_main.audit_logger, "stop_worker", return_value=None),
            ):
                async with api_main.lifespan(api_main.app):
                    pass
            return mock_seed

        prod_seed = asyncio.run(_run_lifespan("production"))
        prod_seed.assert_not_called()

        dev_seed = asyncio.run(_run_lifespan("development"))
        dev_seed.assert_called_once()

    def test_async_audit_logger_tracks_dropped_count_when_full(self):
        from src.api.security import AsyncBatchAuditLogger

        logger_inst = AsyncBatchAuditLogger(batch_size=10, flush_interval=1.0)
        logger_inst.queue = asyncio.Queue(maxsize=2)
        logger_inst.enqueue({"endpoint": "/a"})
        logger_inst.enqueue({"endpoint": "/b"})
        logger_inst.enqueue({"endpoint": "/c"})

        metrics = logger_inst.get_metrics()
        self.assertEqual(metrics["enqueued"], 2)
        self.assertEqual(metrics["dropped"], 1)
        self.assertEqual(metrics["queue_size"], 2)

    def test_autonomous_db_config_jdbc_url_normalizes_windows_path(self):
        from src.config import AutonomousDatabaseConfig

        cfg = AutonomousDatabaseConfig(
            tns_alias="BHXH_TEST_DB_HIGH",
            user="BHXH_TEST",
            password="secret",
            wallet_location=r"D:\OracleCloud\Wallet\BHXH_TEST_DB",
            schema="VSS",
            table="VSS_HO_GD_NHANKHAU",
        )
        self.assertEqual(
            cfg.jdbc_url,
            "jdbc:oracle:thin:@BHXH_TEST_DB_HIGH?TNS_ADMIN=D:/OracleCloud/Wallet/BHXH_TEST_DB",
        )

    def test_cloud_full_load_query_builder_modes_scn_and_identifier_validation(self):
        if "pyspark" not in sys.modules:
            try:
                import pyspark.sql  # noqa: F401
            except Exception:
                pyspark_mod = types.ModuleType("pyspark")
                pyspark_sql = types.ModuleType("pyspark.sql")
                pyspark_sql.SparkSession = object
                sys.modules["pyspark"] = pyspark_mod
                sys.modules["pyspark.sql"] = pyspark_sql

        from src.ingestion.spark_cloud_full_load import (
            build_cloud_source_query,
            validate_identifier,
        )

        q_hash = build_cloud_source_query(
            schema="VSS",
            table="VSS_HO_GD_NHANKHAU",
            limit=10000,
            partition_mode="ora_hash",
            partition_column="MA_CD",
            num_partitions=2,
            snapshot_scn=123456789,
            safe_ma_ho_gd=True,
        )
        self.assertIn("AS OF SCN 123456789", q_hash)
        self.assertIn("WHERE ROWNUM <= 10000", q_hash)
        self.assertIn("ORA_HASH(t.MA_CD, 1) AS SPARK_PART_ID", q_hash)
        self.assertIn("MA_HO_GD_SAFE", q_hash)

        q_range = build_cloud_source_query(
            schema="BHXH_TEST",
            table="HO_SO",
            limit=None,
            partition_mode="range",
            partition_column="ID",
            num_partitions=2,
        )
        self.assertEqual(q_range, "BHXH_TEST.HO_SO")

        q_none = build_cloud_source_query(
            schema="VSS",
            table="VSS_HO_GD_NHANKHAU",
            limit=500,
            partition_mode="none",
            num_partitions=1,
        )
        self.assertIn("WHERE ROWNUM <= 500", q_none)
        self.assertNotIn("ORA_HASH", q_none)

        self.assertEqual(validate_identifier("VSS_HO_GD_NHANKHAU", "table"), "VSS_HO_GD_NHANKHAU")
        with self.assertRaises(ValueError):
            validate_identifier("HO_SO; DROP TABLE X", "table")

    def test_cloud_full_load_parameter_validation(self):
        from src.config import AutonomousDatabaseConfig
        from src.ingestion import spark_cloud_full_load as cloud_mod

        with tempfile.TemporaryDirectory() as tmp_dir:
            with open(os.path.join(tmp_dir, "tnsnames.ora"), "w", encoding="utf-8") as fh:
                fh.write("BHXH_TEST_DB_HIGH = (DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)))")

            valid_cfg = AutonomousDatabaseConfig(
                tns_alias="BHXH_TEST_DB_HIGH",
                user="BHXH_TEST",
                password="pwd",
                wallet_location=tmp_dir,
                schema="VSS",
                table="VSS_HO_GD_NHANKHAU",
            )
            with patch.object(cloud_mod, "FULLLOAD_DB", valid_cfg):
                schema, table, col = cloud_mod.validate_run_params(
                    limit=1000,
                    partitions=2,
                    lower_bound=1,
                    upper_bound=10000,
                    partition_mode="ora_hash",
                    partition_column="MA_CD",
                )
                self.assertEqual((schema, table, col), ("VSS", "VSS_HO_GD_NHANKHAU", "MA_CD"))

                with self.assertRaises(ValueError):
                    cloud_mod.validate_run_params(
                        limit=None,
                        partitions=8,
                        lower_bound=1,
                        upper_bound=10000,
                        partition_mode="ora_hash",
                        partition_column="MA_CD",
                    )

                with self.assertRaises(ValueError):
                    cloud_mod.validate_run_params(
                        limit=None,
                        partitions=2,
                        lower_bound=100,
                        upper_bound=100,
                        partition_mode="range",
                        partition_column="ID",
                    )

            missing_wallet_cfg = AutonomousDatabaseConfig(
                tns_alias="BHXH_TEST_DB_HIGH",
                user="BHXH_TEST",
                password="pwd",
                wallet_location=os.path.join(tmp_dir, "missing_wallet"),
                schema="VSS",
                table="VSS_HO_GD_NHANKHAU",
            )
            with patch.object(cloud_mod, "FULLLOAD_DB", missing_wallet_cfg):
                with self.assertRaises(FileNotFoundError):
                    cloud_mod.validate_run_params(
                        limit=None,
                        partitions=1,
                        lower_bound=1,
                        upper_bound=100,
                        partition_mode="none",
                        partition_column="MA_CD",
                    )

    def test_cloud_full_load_reads_wallet_dir_and_resolves_tns_alias(self):
        from src.ingestion import spark_cloud_full_load as cloud_mod

        tns_content = (
            "# Oracle Cloud Wallet tnsnames\n"
            "bhxhtest_medium = (DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(PORT=1522)))\n"
            "bhxhtest_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(PORT=1522)))\n"
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            nested_dir = Path(tmp_dir) / "Wallet_BHXHTEST"
            nested_dir.mkdir()
            (nested_dir / "tnsnames.ora").write_text(tns_content, encoding="utf-8")

            # Hỗ trợ cả trỏ trực tiếp lẫn thư mục cha lồng 1 cấp
            wallet_dir = cloud_mod.resolve_wallet_dir(tmp_dir)
            self.assertEqual(wallet_dir, nested_dir)
            self.assertTrue((wallet_dir / "tnsnames.ora").is_file())

            # Tự chọn *_high nếu để trống TNS alias
            self.assertEqual(cloud_mod.resolve_tns_alias(wallet_dir / "tnsnames.ora", ""), "bhxhtest_high")
            # Khớp không phân biệt hoa thường
            self.assertEqual(
                cloud_mod.resolve_tns_alias(wallet_dir / "tnsnames.ora", "BHXHTEST_MEDIUM"),
                "bhxhtest_medium",
            )
            with self.assertRaises(ValueError):
                cloud_mod.resolve_tns_alias(wallet_dir / "tnsnames.ora", "WRONG_ALIAS")


if __name__ == "__main__":
    unittest.main()
