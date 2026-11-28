"""
Unit tests cho module src.recovery.pipeline_recovery (không yêu cầu Docker/Oracle chạy thật).
Chạy: python -m unittest tests/test_pipeline_recovery.py -v
"""
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if "oracledb" not in sys.modules:
    try:
        import oracledb  # noqa: F401
    except Exception:
        stub_oracle = types.ModuleType("oracledb")
        stub_oracle.connect = MagicMock()
        sys.modules["oracledb"] = stub_oracle


class TestPipelineRecovery(unittest.TestCase):
    def test_classify_error_trace_transient_vs_fatal(self):
        from src.recovery.pipeline_recovery import classify_error_trace

        self.assertEqual(classify_error_trace("java.net.SocketTimeoutException: Read timed out"), "TRANSIENT")
        self.assertEqual(classify_error_trace("ORA-01013: user requested cancel of current operation"), "TRANSIENT")
        self.assertEqual(classify_error_trace("ORA-01291: missing logfile in LogMiner"), "FATAL")
        self.assertEqual(classify_error_trace("org.apache.kafka.connect.errors.DataException: invalid field"), "FATAL")
        self.assertEqual(classify_error_trace(None), "UNKNOWN")

    def test_parse_dlq_consumer_line_structured_and_fallback(self):
        from src.recovery.pipeline_recovery import parse_dlq_consumer_line

        structured = (
            "Partition:0\tOffset:42\t__connect.errors.exception.message:ORA-01400\t"
            "CD123\t{\"payload\":{\"after\":{\"MA_CD\":\"CD123\",\"MA_HO_GD\":\"HO1\"}}}"
        )
        parsed = parse_dlq_consumer_line(structured, "dlq.c360.mirror.vss_ho_gd_nhankhau")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["kafka_partition"], 0)
        self.assertEqual(parsed["kafka_offset"], 42)
        self.assertEqual(parsed["record_key"], "CD123")
        self.assertIn("ORA-01400", parsed["error_message"])
        self.assertIn("MA_CD", parsed["payload_json"])

        fallback = parse_dlq_consumer_line('{"MA_CD":"CD999"}', "dlq.test")
        self.assertIsNotNone(fallback)
        assert fallback is not None
        self.assertEqual(fallback["topic_name"], "dlq.test")
        self.assertEqual(fallback["payload_json"], '{"MA_CD":"CD999"}')

    def test_heal_connectors_skips_fatal_and_restarts_transient(self):
        from src.recovery import pipeline_recovery as pr

        statuses = {
            "vss-hgd-oracle-source-connector": {
                "name": "vss-hgd-oracle-source-connector",
                "exists": True,
                "connector_state": "RUNNING",
                "tasks": [{"id": 0, "state": "FAILED", "trace": "ORA-01291: missing logfile"}],
            },
            "c360-mirror-jdbc-sink-connector": {
                "name": "c360-mirror-jdbc-sink-connector",
                "exists": True,
                "connector_state": "RUNNING",
                "tasks": [{"id": 0, "state": "FAILED", "trace": "SocketTimeoutException"}],
            },
        }
        calls = []

        def fake_get(_url, name):
            # Sau khi restart, trả về RUNNING
            if any(c.endswith(f"/connectors/{name}/restart?includeTasks=true&onlyFailed=true") for c in calls):
                return {
                    "name": name,
                    "exists": True,
                    "connector_state": "RUNNING",
                    "tasks": [{"id": 0, "state": "RUNNING"}],
                }
            return statuses[name]

        def fake_http(url, method="GET", timeout=5.0):
            calls.append(url)
            return 202, {}

        with (
            patch.object(pr, "get_connector_status", side_effect=fake_get),
            patch.object(pr, "_http_json", side_effect=fake_http),
            patch.object(pr.time, "sleep"),
        ):
            res = pr.heal_connectors(
                connectors=["vss-hgd-oracle-source-connector", "c360-mirror-jdbc-sink-connector"],
                max_attempts=1,
                backoff_sec=0.0,
            )

        self.assertEqual(res["vss-hgd-oracle-source-connector"]["action"], "SKIPPED_FATAL")
        self.assertEqual(res["c360-mirror-jdbc-sink-connector"]["action"], "RESTARTED")
        self.assertTrue(res["c360-mirror-jdbc-sink-connector"]["healed"])

    def test_retry_failed_queue_requires_filter(self):
        from src.recovery.pipeline_recovery import retry_failed_queue

        with self.assertRaises(ValueError):
            retry_failed_queue(MagicMock())

    def test_reclaim_stale_sync_logs_marks_hung_batches_failed(self):
        from src.recovery.pipeline_recovery import reclaim_stale_sync_logs

        mock_cur = MagicMock()
        mock_cur.rowcount = 2
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        affected = reclaim_stale_sync_logs(mock_conn, stale_minutes=2)
        self.assertEqual(affected, 2)
        executed_sql = mock_cur.execute.call_args_list[0][0][0]
        self.assertIn("UPDATE C360.C360_SYNC_LOG", executed_sql)
        self.assertIn("HEARTBEAT_AT", executed_sql)
        self.assertIn("CLAIMED_BATCH_ID", executed_sql)
        self.assertIn("SET l.STATUS = 'FAILED'", executed_sql)

        with self.assertRaises(ValueError):
            reclaim_stale_sync_logs(mock_conn, stale_minutes=0)


if __name__ == "__main__":
    unittest.main()
