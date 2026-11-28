"""
Bộ mô phỏng lỗi thực tế (Live Docker Chaos) cho cả 4 tầng pipeline BHXH-HS.
Tác động trực tiếp lên Docker containers (hgd-kafka, hgd-debezium-connect, hgd-oracle)
để kiểm chứng cơ chế Auto-Heal, DLQ Replay, Queue Retry và Targeted Resync.

Chạy:
  python -m src.recovery.simulate_failures --scenario all
  python -m src.recovery.simulate_failures --scenario kafka_down
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import urllib.request
from typing import Any, Callable, Dict, List

import oracledb

from src.config import SOURCE_DB, TARGET_DB
from src.monitoring.pipeline_monitor import collect_kafka_metrics
from src.reconciliation.reconcile_pipeline import run_reconciliation
from src.recovery.pipeline_recovery import (
    DEFAULT_CONNECT_URL,
    DEFAULT_KAFKA_CONTAINER,
    check_kafka_broker,
    get_connector_status,
    ingest_dlq_to_oracle,
    replay_dlq,
    resync_macds,
    retry_failed_queue,
    run_auto_heal,
)

logger = logging.getLogger("c360_chaos_sim")


def _docker_cmd(args: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=False)


def _http_put(url: str, timeout: float = 15.0) -> int:
    req = urllib.request.Request(url, method="PUT")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return int(resp.status)


def simulate_kafka_down() -> Dict[str, Any]:
    """Kịch bản 1: Dừng hgd-kafka -> phát hiện mất kết nối -> khởi động lại -> auto-heal."""
    container = DEFAULT_KAFKA_CONTAINER
    try:
        p1 = _docker_cmd(["stop", container])
        if p1.returncode != 0:
            return {"scenario": "kafka_down", "status": "FAIL", "error": p1.stderr.strip()}
        time.sleep(2.0)

        broker_while_stopped = check_kafka_broker()
        kafka_metrics_stopped = collect_kafka_metrics()
    finally:
        _docker_cmd(["start", container])

    broker_after = False
    for _ in range(15):
        if check_kafka_broker():
            broker_after = True
            break
        time.sleep(2.0)

    time.sleep(3.0)
    heal_res = run_auto_heal(use_oracle=False)

    passed = (not broker_while_stopped) and (not kafka_metrics_stopped.get("broker_available")) and broker_after
    return {
        "scenario": "kafka_down",
        "status": "PASS" if passed else "FAIL",
        "broker_while_stopped": broker_while_stopped,
        "broker_after_restart": broker_after,
        "auto_heal": heal_res,
    }


def simulate_connector_failed() -> Dict[str, Any]:
    """Kịch bản 2: Đưa Sink Connector về PAUSED/ngắt quãng -> Auto-Heal khôi phục về RUNNING."""
    connector = "c360-mirror-jdbc-sink-connector"
    base = DEFAULT_CONNECT_URL.rstrip("/")
    before = get_connector_status(base, connector)
    if not before.get("exists"):
        return {
            "scenario": "connector_failed",
            "status": "FAIL",
            "error": f"Connector {connector} chưa được đăng ký trên {base}.",
        }

    try:
        _http_put(f"{base}/connectors/{connector}/pause")
        time.sleep(2.0)
        paused_status = get_connector_status(base, connector)
        heal_res = run_auto_heal(include_paused=True, use_oracle=False)
        after = get_connector_status(base, connector)
    finally:
        try:
            _http_put(f"{base}/connectors/{connector}/resume")
        except Exception:
            pass

    passed = paused_status.get("connector_state") == "PAUSED" and after.get("connector_state") == "RUNNING"
    return {
        "scenario": "connector_failed",
        "status": "PASS" if passed else "FAIL",
        "paused_state": paused_status.get("connector_state"),
        "after_heal_state": after.get("connector_state"),
        "auto_heal": heal_res,
    }


def simulate_dlq_poison_pill() -> Dict[str, Any]:
    """Kịch bản 3: Bắn message lỗi vào DLQ -> ingest-dlq -> replay-dlq -> dọn dữ liệu test."""
    test_macd = "CHAOS_DLQ_001"
    test_mahogd = "CHAOS_HO_001"
    dlq_topic = "dlq.c360.mirror.vss_ho_gd_nhankhau"
    payload = json.dumps(
        {
            "payload": {
                "after": {
                    "MA_CD": test_macd,
                    "MA_HO_GD": test_mahogd,
                    "HO_TEN": "NGUYEN CHAOS DLQ",
                    "TYPEID": "01",
                    "TRANG_THAI_DUYET": 0,
                }
            }
        }
    )

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as t_conn:
        try:
            proc = subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    DEFAULT_KAFKA_CONTAINER,
                    "kafka-console-producer",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--topic",
                    dlq_topic,
                    "--property",
                    "parse.key=true",
                    "--property",
                    "key.separator=\t",
                ],
                input=f"{test_macd}\t{payload}\n",
                text=True,
                capture_output=True,
                check=False,
            )
            if proc.returncode != 0:
                return {"scenario": "dlq_poison_pill", "status": "FAIL", "error": proc.stderr.strip()}

            time.sleep(1.5)
            ingest_res = ingest_dlq_to_oracle(t_conn, topics=[dlq_topic], max_messages=50)
            replay_res = replay_dlq(t_conn, mode="mirror-upsert")

            with t_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc",
                    {"mc": test_macd},
                )
                in_mirror = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM C360.C360_DLQ_ARCHIVE WHERE STATUS = 'REPLAYED' AND RECORD_KEY = :mc",
                    {"mc": test_macd},
                )
                replayed_rows = int(cur.fetchone()[0])

            passed = in_mirror >= 1 and (replay_res.get("replayed", 0) >= 1 or replayed_rows >= 1)
            return {
                "scenario": "dlq_poison_pill",
                "status": "PASS" if passed else "FAIL",
                "ingest": ingest_res,
                "replay": replay_res,
                "in_mirror": in_mirror,
            }
        finally:
            with t_conn.cursor() as cur:
                cur.execute("DELETE FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.C360_DLQ_ARCHIVE WHERE RECORD_KEY = :mc", {"mc": test_macd})
            t_conn.commit()


def simulate_queue_failed() -> Dict[str, Any]:
    """Kịch bản 4: Chèn bản ghi FAILED vào C360_SYNC_QUEUE -> retry-queue -> xác nhận reset về NEW."""
    test_macd = "CHAOS_Q_FAIL_001"
    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as t_conn:
        try:
            with t_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO C360.C360_SYNC_QUEUE (MA_CD, ACTION_TYPE, STATUS, RETRY_COUNT)
                    VALUES (:mc, 'U', 'FAILED', 5)
                    """,
                    {"mc": test_macd},
                )
            t_conn.commit()

            reset_count = retry_failed_queue(t_conn, ma_cd=test_macd)
            with t_conn.cursor() as cur:
                cur.execute(
                    "SELECT STATUS, RETRY_COUNT FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc",
                    {"mc": test_macd},
                )
                row = cur.fetchone()

            passed = reset_count >= 1 and row is not None and row[0] == "NEW" and int(row[1]) == 0
            return {
                "scenario": "queue_failed",
                "status": "PASS" if passed else "FAIL",
                "reset_count": reset_count,
                "row_after_retry": {"status": row[0], "retry_count": int(row[1])} if row else None,
            }
        finally:
            with t_conn.cursor() as cur:
                cur.execute("DELETE FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc", {"mc": test_macd})
            t_conn.commit()


def simulate_reconcile_drift() -> Dict[str, Any]:
    """Kịch bản 5: Tạo lệch giữa Source & Mirror -> reconcile phát hiện -> resync-macd vá lệch."""
    test_id = "CHAOS_ID_999999"
    test_macd = "CHAOS_DRIFT_001"
    test_mahogd = "CHAOS_HO_DRIFT"

    with (
        oracledb.connect(user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn) as s_conn,
        oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as t_conn,
    ):
        try:
            with s_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO VSS.VSS_HO_GD_NHANKHAU (
                        ID, MA_HO_GD, MA_CD, HO_TEN, TYPEID, TRANG_THAI_DUYET, MODIFY_DATE
                    ) VALUES (
                        :id, :mahogd, :macd, 'NGUYEN CHAOS DRIFT', '01', 0, SYSTIMESTAMP
                    )
                    """,
                    {"id": test_id, "mahogd": test_mahogd, "macd": test_macd},
                )
            s_conn.commit()

            # Xóa ở Mirror nếu trigger/CDC vừa kịp đẩy sang để giả lập trạng thái bị lệch/mất log
            with t_conn.cursor() as cur:
                cur.execute("DELETE FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc", {"mc": test_macd})
            t_conn.commit()

            resync_res = resync_macds(s_conn, t_conn, [test_macd], run_batch=False)
            with t_conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc",
                    {"mc": test_macd},
                )
                mirror_cnt = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc",
                    {"mc": test_macd},
                )
                queue_cnt = int(cur.fetchone()[0])

            passed = resync_res.get("resynced_rows", 0) >= 1 and mirror_cnt >= 1 and queue_cnt >= 1
            return {
                "scenario": "reconcile_drift",
                "status": "PASS" if passed else "FAIL",
                "resync": resync_res,
                "mirror_count_after_resync": mirror_cnt,
                "queue_count_after_resync": queue_cnt,
            }
        finally:
            with s_conn.cursor() as cur:
                cur.execute("DELETE FROM VSS.VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc", {"mc": test_macd})
            s_conn.commit()
            with t_conn.cursor() as cur:
                cur.execute("DELETE FROM C360.C360_SYNC_QUEUE WHERE MA_CD = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = :mc", {"mc": test_macd})
                cur.execute("DELETE FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :mc", {"mc": test_macd})
            t_conn.commit()


SCENARIOS: Dict[str, Callable[[], Dict[str, Any]]] = {
    "kafka_down": simulate_kafka_down,
    "connector_failed": simulate_connector_failed,
    "dlq_poison_pill": simulate_dlq_poison_pill,
    "queue_failed": simulate_queue_failed,
    "reconcile_drift": simulate_reconcile_drift,
}


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Live Docker Chaos Simulator cho BHXH-HS Pipeline")
    parser.add_argument(
        "--scenario",
        choices=[*SCENARIOS.keys(), "all"],
        default="all",
        help="Kịch bản lỗi muốn mô phỏng.",
    )
    args = parser.parse_args(argv)

    selected = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]
    results = []
    for name in selected:
        logger.info("Running chaos scenario: %s", name)
        try:
            res = SCENARIOS[name]()
        except Exception as exc:
            res = {"scenario": name, "status": "FAIL", "error": str(exc)}
        results.append(res)

    overall = "PASS" if all(r.get("status") == "PASS" for r in results) else "FAIL"
    print(json.dumps({"overall_status": overall, "results": results}, indent=2, ensure_ascii=False))
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
