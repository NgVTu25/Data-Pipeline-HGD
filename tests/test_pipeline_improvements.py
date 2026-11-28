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
            where_clause="MA_CD > '1003000000'",
        )
        self.assertIn("AS OF SCN 123456789", q_hash)
        self.assertIn("WHERE (MA_CD > '1003000000') AND ROWNUM <= 10000", q_hash)
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

    def test_cutover_control_determine_resume_action(self):
        from src.ingestion.cutover_control import CutoverState, determine_resume_action

        self.assertEqual(determine_resume_action(None)["action"], "RUN_FULL_LOAD")
        self.assertFalse(determine_resume_action(CutoverState(cutover_status="INIT"))["skip_full_load"])

        done_state = CutoverState(
            snapshot_scn=100050,
            cdc_start_scn=100050,
            connector_status="NOT_STARTED",
            cutover_status="FULL_LOAD_DONE",
        )
        plan_done = determine_resume_action(done_state)
        self.assertEqual(plan_done["action"], "RESUME_CDC_FROM_SNAPSHOT_SCN")
        self.assertTrue(plan_done["skip_full_load"])
        self.assertEqual(plan_done["resume_scn"], 100050)

        paused_cdc = CutoverState(
            snapshot_scn=100050,
            cdc_start_scn=100050,
            cdc_last_scn=100999,
            connector_status="PAUSED",
            cutover_status="CDC_ACTIVE",
        )
        plan_paused = determine_resume_action(paused_cdc)
        self.assertEqual(plan_paused["action"], "RESUME_CDC_FROM_LAST_SCN")
        self.assertEqual(plan_paused["resume_scn"], 100999)

        running_cdc = CutoverState(
            snapshot_scn=100050,
            cdc_start_scn=100050,
            cdc_last_scn=101200,
            connector_status="RUNNING",
            cutover_status="CDC_ACTIVE",
        )
        self.assertEqual(determine_resume_action(running_cdc)["action"], "MONITOR_RUNNING_CDC")

    def test_monitoring_distinguishes_four_delivery_states_without_concluding_lag_is_loss(self):
        from src.monitoring.pipeline_monitor import classify_pipeline_delivery_state

        # 1. Message chưa được Kafka Consumer đọc
        case1 = classify_pipeline_delivery_state({
            "kafka": {"consumer_lag": 42},
            "jdbc_sink": {"records_consumed": 100, "records_written": 100, "dlq_messages": 0, "task_state": "RUNNING"},
            "oracle_mirror": {"c360_pending_queue_count": 0},
        })
        self.assertEqual(case1["primary_state"], "UNCONSUMED_IN_KAFKA")
        self.assertFalse(case1["lag_implies_data_loss"])
        self.assertFalse(case1["possible_data_loss"])

        # 2. Message đã đọc nhưng Sink chưa ghi
        case2 = classify_pipeline_delivery_state({
            "kafka": {"consumer_lag": 0},
            "jdbc_sink": {"records_consumed": 120, "records_written": 100, "dlq_messages": 0, "task_state": "RUNNING"},
            "oracle_mirror": {"c360_pending_queue_count": 0},
        })
        self.assertEqual(case2["primary_state"], "CONSUMED_NOT_WRITTEN_BY_SINK")

        # 3. Message ghi lỗi và được đưa vào DLQ
        case3 = classify_pipeline_delivery_state({
            "kafka": {"consumer_lag": 0},
            "jdbc_sink": {"records_consumed": 120, "records_written": 117, "dlq_messages": 3, "task_state": "RUNNING"},
            "oracle_mirror": {"c360_pending_queue_count": 0},
        })
        self.assertEqual(case3["primary_state"], "ROUTED_TO_DLQ")
        self.assertTrue(case3["possible_data_loss"])

        # 4. Message đã ghi thành công vào Mirror nhưng chưa được tầng C360 xử lý
        case4 = classify_pipeline_delivery_state({
            "kafka": {"consumer_lag": 0},
            "jdbc_sink": {"records_consumed": 120, "records_written": 120, "dlq_messages": 0, "task_state": "RUNNING"},
            "oracle_mirror": {"c360_pending_queue_count": 15},
        })
        self.assertEqual(case4["primary_state"], "WRITTEN_TO_MIRROR_PENDING_C360")

        # Healthy
        healthy = classify_pipeline_delivery_state({
            "kafka": {"consumer_lag": 0},
            "jdbc_sink": {"records_consumed": 120, "records_written": 120, "dlq_messages": 0, "task_state": "RUNNING"},
            "oracle_mirror": {"c360_pending_queue_count": 0},
        })
        self.assertEqual(healthy["primary_state"], "HEALTHY_SYNCED")

    def test_jdbc_sink_config_has_dlq_and_retries(self):
        import json

        sink_path = ROOT_DIR / "src" / "ingestion" / "debezium" / "c360-mirror-jdbc-sink-connector.json"
        cfg = json.loads(sink_path.read_text(encoding="utf-8"))["config"]
        self.assertEqual(cfg["errors.tolerance"], "all")
        self.assertEqual(cfg["errors.deadletterqueue.topic.name"], "dlq.c360.mirror.vss_ho_gd_nhankhau")
        self.assertEqual(cfg["errors.deadletterqueue.context.headers.enable"], "true")
        self.assertEqual(cfg["max.retries"], "5")

    def test_reconciliation_content_fidelity_and_checksum(self):
        from src.reconciliation.reconcile_pipeline import (
            RECONCILIATION_QUERIES,
            evaluate_source_mirror_checksum,
        )

        fidelity_sql = RECONCILIATION_QUERIES["CHECK_MIRROR_VS_C360_FIELD_FIDELITY"]
        self.assertIn("MISMATCH_CONTENT_COUNT", fidelity_sql)
        self.assertIn("SO_CCCD_GOC", fidelity_sql)
        self.assertIn("NGAY_SINH_GOC", fidelity_sql)

        src = {"distinct_macd_cnt": 50000, "max_modify_date": "2026-11-27 10:00:00", "content_checksum": 987654321}
        mir_ok = {"distinct_macd_cnt": 50000, "max_modify_date": "2026-11-27 10:00:00", "content_checksum": 987654321}
        mir_bad = {"distinct_macd_cnt": 50000, "max_modify_date": "2026-11-27 10:00:00", "content_checksum": 111111111}

        self.assertEqual(evaluate_source_mirror_checksum(src, mir_ok, in_flight_q=0)["status"], "PASS")
        self.assertEqual(evaluate_source_mirror_checksum(src, mir_bad, in_flight_q=0)["status"], "FAIL")
        self.assertEqual(evaluate_source_mirror_checksum(src, mir_bad, in_flight_q=2)["status"], "IN_PROGRESS")

    def test_simulate_cdc_build_target_spec_cloud_wallet_and_local(self):
        from src.config import AutonomousDatabaseConfig, DatabaseConfig
        from src.data_generator import simulate_cdc_changes as sim_mod

        local_cfg = DatabaseConfig(
            host="localhost",
            port=1521,
            service="FREEPDB1",
            user="vss_source",
            password="pwd",
            schema="VSS",
        )
        with patch.object(sim_mod, "SOURCE_DB", local_cfg):
            local_spec = sim_mod.build_target_spec("local")
            self.assertEqual(local_spec.target, "local")
            self.assertEqual(local_spec.full_table, "VSS.VSS_HO_GD_NHANKHAU")
            self.assertIsNone(local_spec.wallet_dir)

        with tempfile.TemporaryDirectory() as tmp_dir:
            wallet_dir = Path(tmp_dir) / "Wallet_BHXHTEST"
            wallet_dir.mkdir()
            (wallet_dir / "tnsnames.ora").write_text(
                "bhxhcloud_medium = (DESCRIPTION=())\nbhxhcloud_high = (DESCRIPTION=())\n",
                encoding="utf-8",
            )
            cloud_cfg = AutonomousDatabaseConfig(
                tns_alias="",
                user="BHXH_TEST",
                password="secret",
                wallet_location=tmp_dir,
                schema="VSS",
                table="VSS_HO_GD_NHANKHAU",
            )
            with patch.object(sim_mod, "FULLLOAD_DB", cloud_cfg):
                cloud_spec = sim_mod.build_target_spec("cloud")
                self.assertEqual(cloud_spec.target, "cloud")
                self.assertEqual(cloud_spec.dsn, "bhxhcloud_high")
                self.assertEqual(cloud_spec.wallet_dir, str(wallet_dir))
                self.assertEqual(cloud_spec.full_table, "VSS.VSS_HO_GD_NHANKHAU")

    def test_simulate_cdc_loads_vss_context_detects_defects_and_builds_diverse_variants(self):
        from src.data_generator import simulate_cdc_changes as sim_mod

        class _FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, sql, _params=None):
                if "VSS_DM_DBHC" in sql:
                    self._rows = [("79TTT", "26734", "TP.HCM", "Phường Sài Gòn")]
                elif "VSS_DM_DANTOC" in sql:
                    self._rows = [(1, "01", "Kinh"), (2, "02", "Tày")]
                elif "VSS_DM_QUOC_TICH" in sql:
                    self._rows = [(233, "VN", "Việt Nam"), (101, "KR", "Hàn Quốc")]
                elif "SELECT DISTINCT TRIM(HO_TEN)" in sql:
                    self._rows = [
                        ("Lê Minh Quân",),
                        ("Phạm Ngọc Anh",),
                        ("Võ Thanh Thảo",),
                    ]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        ref_ctx = sim_mod.load_vss_reference_context(_FakeCursor(), "VSS", "VSS_HO_GD_NHANKHAU")
        self.assertEqual(ref_ctx.locations[0].ma_tinh, "79TTT")
        self.assertEqual(ref_ctx.locations[0].ma_xa, "26734")
        self.assertIn("Lê", ref_ctx.last_names)
        self.assertIn("Quân", ref_ctx.first_names)

        # Kiểm tra các biến thể INSERT 1A khác biệt nhau nhưng bám danh mục VSS
        p_clean = sim_mod.build_insert_variant_payload(1, "1A_CLEAN", ref_ctx)
        p_no_cccd = sim_mod.build_insert_variant_payload(2, "1A_MISSING_CCCD", ref_ctx)
        p_bad_birth = sim_mod.build_insert_variant_payload(3, "1A_BAD_BIRTH_AND_PHONE", ref_ctx)
        p_pending = sim_mod.build_insert_variant_payload(4, "1A_PENDING_APPROVAL", ref_ctx)
        p_diverse = sim_mod.build_insert_variant_payload(6, "1A_DIVERSE_CATALOG_OR_DECEASED", ref_ctx)

        self.assertEqual((p_clean["ma_tinh"], p_clean["ma_xa"]), ("79TTT", "26734"))
        self.assertIsNone(p_no_cccd["so_cccd"])
        self.assertEqual(p_bad_birth["phone_number"], "12345")
        self.assertEqual(p_pending["trang_thai_duyet"], 1)
        self.assertEqual(p_diverse["dm_dan_toc_id"], 2)
        self.assertEqual(p_diverse["is_alive"], "0")

        # Kiểm tra detect_citizen_dq_defects và build_fix_update_plan sửa đúng lỗi thực tế
        dirty_rec = sim_mod.CitizenRecord(
            ma_cd="CLOUD_0001",
            ho_ten="  Lê Minh Quân  ",
            gioi_tinh="1",
            ngay_sinh=19950230,
            so_cccd=None,
            so_cmtnd=None,
            phone_number="123",
            ma_tinh="99TTT",
            ma_xa="99999",
            quoc_tich="XX",
            dm_dan_toc_id=9999,
            households=[sim_mod.CitizenHousehold(id="CLOUD_0001_H1", ma_ho_gd="HGD_1")],
            from_vss=True,
        )
        defects = sim_mod.detect_citizen_dq_defects(dirty_rec, ref_ctx)
        self.assertEqual(
            set(defects),
            {
                "KHONG_SO_DINH_DANH",
                "NGAY_SINH_LOI",
                "SDT_LOI",
                "TINH_XA_KHONG_MAP",
                "QUOC_TICH_LOI",
                "DAN_TOC_LOI",
                "HO_TEN_KHOANG_TRANG",
            },
        )
        set_sql, params, _ = sim_mod.build_fix_update_plan(dirty_rec, ref_ctx, "2_FIX_ACTUAL_DEFECTS_FULL")
        self.assertIn("SO_CCCD = :so_cccd", set_sql)
        self.assertIn("MA_TINH = :ma_tinh", set_sql)
        self.assertEqual(params["ma_tinh"], "79TTT")
        self.assertEqual(params["ma_xa"], "26734")
        self.assertEqual(sim_mod.detect_citizen_dq_defects(dirty_rec, ref_ctx), [])

    def test_simulate_cdc_cloud_scenarios_and_random_stream_with_mock_oracle(self):
        from src.config import AutonomousDatabaseConfig
        from src.data_generator import simulate_cdc_changes as sim_mod

        class _MockCursor:
            def __init__(self):
                self.executed = []
                self._rows = []
                self.rowcount = 1

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, params=None):
                self.executed.append((sql.strip(), params))
                self.rowcount = 1
                if "VSS_DM_DBHC" in sql:
                    self._rows = [("01TTT", "00004", "Hà Nội", "Ba Đình"), ("79TTT", "26734", "TP.HCM", "Sài Gòn")]
                elif "VSS_DM_DANTOC" in sql:
                    self._rows = [(1, "01", "Kinh"), (2, "02", "Tày")]
                elif "VSS_DM_QUOC_TICH" in sql:
                    self._rows = [(233, "VN", "Việt Nam"), (101, "KR", "Hàn Quốc")]
                elif "SELECT DISTINCT TRIM(HO_TEN)" in sql:
                    self._rows = [("Nguyễn Văn An",), ("Trần Thị Bình",), ("Lê Hoàng Cường",)]
                elif "SELECT DISTINCT MA_CD FROM" in sql or "HAVING COUNT(*) >= 2" in sql:
                    if "TRANG_THAI_DUYET = 1" in sql:
                        self._rows = [("VSS_PEND_01",)]
                    elif "HAVING COUNT(*) >= 2" in sql:
                        self._rows = [("VSS_MULTI_01",)]
                    elif "SO_CCCD IS NULL" in sql:
                        self._rows = [("VSS_DIRTY_01",)]
                    else:
                        self._rows = [("VSS_CLEAN_01",), ("VSS_CLEAN_02",), ("VSS_CLEAN_03",)]
                elif "WHERE MA_CD IN (" in sql:
                    self._rows = [
                        ("VSS_CLEAN_01_H1", "VSS_CLEAN_01", "HGD_C1", "Nguyễn Văn An", "1", 19900101, "001090000001", None, "0901111111", "01TTT", "00004", "VN", 1, "1", None, 2, None, None),
                        ("VSS_CLEAN_02_H1", "VSS_CLEAN_02", "HGD_C2", "Trần Thị Bình", "0", 19910202, "001091000002", None, "0902222222", "01TTT", "00004", "VN", 1, "1", None, 2, None, None),
                        ("VSS_CLEAN_03_H1", "VSS_CLEAN_03", "HGD_C3", "Lê Hoàng Cường", "1", 19920303, "001092000003", None, "0903333333", "01TTT", "00004", "VN", 1, "1", None, 2, None, None),
                        ("VSS_DIRTY_01_H1", "VSS_DIRTY_01", "HGD_D1", "  Phạm Văn Bẩn  ", "1", 19950230, None, None, "123", "99TTT", "99999", "XX", 9999, "1", None, 2, None, None),
                        ("VSS_PEND_01_H1", "VSS_PEND_01", "HGD_P1", "Đỗ Chờ Duyệt", "0", 19930404, "001093000004", None, "0904444444", "01TTT", "00004", "VN", 1, "1", None, 1, None, None),
                        ("VSS_MULTI_01_H1", "VSS_MULTI_01", "HGD_M1", "Hoàng Nhiều Hộ", "1", 19880505, "001088000005", None, "0905555555", "01TTT", "00004", "VN", 1, "1", None, 2, None, None),
                        ("VSS_MULTI_01_H2", "VSS_MULTI_01", "HGD_M2", "Hoàng Nhiều Hộ", "1", 19880505, "001088000005", None, "0905555555", "79TTT", "26734", "VN", 1, "1", None, 2, None, None),
                    ]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

            def fetchone(self):
                return self._rows[0] if self._rows else (0,)

        class _MockConn:
            def __init__(self):
                self.cur = _MockCursor()
                self.commits = 0
                self.rollbacks = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def cursor(self):
                return self.cur

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "tnsnames.ora").write_text("bhxhcloud_high = (DESCRIPTION=())\n", encoding="utf-8")
            cloud_cfg = AutonomousDatabaseConfig(
                tns_alias="bhxhcloud_high",
                user="BHXH_TEST",
                password="secret",
                wallet_location=tmp_dir,
                schema="VSS",
                table="VSS_HO_GD_NHANKHAU",
            )
            mock_conn_scenarios = _MockConn()
            mock_conn_random = _MockConn()
            connect_calls = []

            def fake_connect(**kwargs):
                connect_calls.append(kwargs)
                return mock_conn_scenarios if len(connect_calls) == 1 else mock_conn_random

            with (
                patch.object(sim_mod, "FULLLOAD_DB", cloud_cfg),
                patch.object(sim_mod.oracledb, "connect", side_effect=fake_connect),
            ):
                results = sim_mod.run_cdc_simulation(
                    selected_keys=["1A", "1B", "2", "3A", "3B", "4", "5A", "5B"],
                    target="cloud",
                    sample_size=50,
                )
                self.assertEqual(len(results), 8)
                self.assertTrue(all(cnt == 1 for _, cnt in results))
                self.assertEqual(connect_calls[0]["config_dir"], tmp_dir)
                self.assertEqual(connect_calls[0]["wallet_location"], tmp_dir)
                self.assertEqual(connect_calls[0]["dsn"], "bhxhcloud_high")

                # Kiểm tra kịch bản 2 thực sự sửa bản ghi bẩn đọc từ VSS (VSS_DIRTY_01)
                update_params = [p for sql, p in mock_conn_scenarios.cur.executed if sql.startswith("UPDATE VSS.VSS_HO_GD_NHANKHAU") and p]
                self.assertTrue(any(p.get("ma_cd") == "VSS_DIRTY_01" for p in update_params))

                events = sim_mod.run_random_cdc_stream(
                    interval_seconds=0.0,
                    max_events=8,
                    target="cloud",
                    sample_size=50,
                )
                self.assertEqual(events, 8)
                self.assertEqual(mock_conn_random.commits, 8)


if __name__ == "__main__":
    unittest.main()
