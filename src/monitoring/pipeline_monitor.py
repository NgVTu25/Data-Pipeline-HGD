"""
Pipeline Monitoring 5 tầng:
1. Oracle Source: SCN hiện tại, SCN đã capture, tình trạng redo/archive log.
2. Debezium Source Connector: Connector state, task state, lỗi đọc log, số event phát ra.
3. Kafka: Topic partition, offset, consumer lag, broker availability.
4. JDBC Sink Connector: Records consumed, records written, retry, DLQ, lỗi SQL.
5. Oracle Mirror: Tổng bản ghi, số bản ghi insert/update/delete, thời điểm cập nhật cuối.

Đồng thời chẩn đoán và phân biệt rõ 4 trạng thái vận chuyển message (không chỉ nhìn consumer lag):
- UNCONSUMED_IN_KAFKA: Message chưa được Kafka Consumer đọc.
- CONSUMED_NOT_WRITTEN_BY_SINK: Message đã đọc nhưng Sink chưa ghi.
- ROUTED_TO_DLQ: Message ghi lỗi và được đưa vào DLQ.
- WRITTEN_TO_MIRROR_PENDING_C360: Message đã ghi thành công vào Mirror nhưng chưa được tầng C360 xử lý.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
import oracledb

from src.config import SOURCE_DB, TARGET_DB


DEFAULT_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_KAFKA_CONTAINER = os.getenv("KAFKA_DOCKER_CONTAINER", "hgd-kafka")
DEFAULT_SOURCE_CONNECTOR = "vss-hgd-oracle-source-connector"
DEFAULT_SINK_CONNECTOR = "c360-mirror-jdbc-sink-connector"
DEFAULT_CDC_TOPIC = "vss_cdc.VSS.VSS_HO_GD_NHANKHAU"
DEFAULT_DLQ_TOPIC = "dlq.c360.mirror.vss_ho_gd_nhankhau"
DEFAULT_SINK_CONSUMER_GROUP = f"connect-{DEFAULT_SINK_CONNECTOR}"


def _http_get_json(url: str, timeout: float = 5.0) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_connector_status(status_payload: Dict[str, Any]) -> Dict[str, Any]:
    connector_state = (status_payload.get("connector") or {}).get("state", "UNKNOWN")
    tasks = status_payload.get("tasks") or []
    task_states = [t.get("state", "UNKNOWN") for t in tasks]
    task_state = task_states[0] if task_states else "NO_TASK"
    traces = [t.get("trace") for t in tasks if t.get("trace")]
    return {
        "connector_state": connector_state,
        "task_state": task_state,
        "task_count": len(tasks),
        "error_trace": traces[0] if traces else None,
    }


def _check_tcp_endpoint(bootstrap_servers: str, timeout: float = 3.0) -> bool:
    first_broker = bootstrap_servers.split(",")[0].strip()
    if ":" in first_broker:
        host, port_str = first_broker.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = first_broker, 9092
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _run_kafka_cli(container: str, command: List[str], timeout: int = 10) -> str:
    proc = subprocess.run(
        ["docker", "exec", container, *command],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}")
    return proc.stdout


def _parse_topic_offsets(raw_output: str) -> Dict[int, int]:
    offsets: Dict[int, int] = {}
    for line in raw_output.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1].isdigit() and parts[2].lstrip("-").isdigit():
            offsets[int(parts[1])] = int(parts[2])
    return offsets


def _parse_consumer_group_output(raw_output: str, topic: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in raw_output.splitlines():
        cols = re.split(r"\s+", line.strip())
        if len(cols) >= 6 and cols[1] == topic and cols[2].isdigit():
            current_off = int(cols[3]) if cols[3].lstrip("-").isdigit() else 0
            end_off = int(cols[4]) if cols[4].lstrip("-").isdigit() else 0
            lag = int(cols[5]) if cols[5].lstrip("-").isdigit() else max(0, end_off - current_off)
            rows.append(
                {
                    "partition": int(cols[2]),
                    "committed_offset": current_off,
                    "end_offset": end_off,
                    "lag": lag,
                }
            )
    return rows


def collect_oracle_source_metrics() -> Dict[str, Any]:
    """1. Oracle Source: SCN hiện tại, SCN đã capture, redo/archive log availability."""
    metrics: Dict[str, Any] = {
        "available": False,
        "current_scn": None,
        "captured_scn": None,
        "snapshot_scn": None,
        "cdc_start_scn": None,
        "cutover_status": None,
        "log_mode": None,
        "archive_log_available": False,
        "archive_log_count": 0,
        "min_archive_scn": None,
        "max_archive_scn": None,
        "active_redo_groups": 0,
        "error": None,
    }
    try:
        with oracledb.connect(
            user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn
        ) as src_conn:
            with src_conn.cursor() as cur:
                metrics["available"] = True
                try:
                    cur.execute("SELECT CURRENT_SCN, LOG_MODE FROM V$DATABASE")
                    row = cur.fetchone()
                    if row:
                        metrics["current_scn"] = int(row[0]) if row[0] is not None else None
                        metrics["log_mode"] = str(row[1]) if row[1] is not None else None
                except oracledb.DatabaseError:
                    cur.execute("SELECT TIMESTAMP_TO_SCN(SYSTIMESTAMP) FROM DUAL")
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        metrics["current_scn"] = int(row[0])

                try:
                    cur.execute(
                        """
                        SELECT COUNT(*), MIN(FIRST_CHANGE#), MAX(NEXT_CHANGE#)
                        FROM V$ARCHIVED_LOG
                        WHERE STATUS = 'A' AND DELETED = 'NO'
                        """
                    )
                    arch_row = cur.fetchone()
                    if arch_row:
                        cnt = int(arch_row[0] or 0)
                        metrics["archive_log_count"] = cnt
                        metrics["archive_log_available"] = cnt > 0 or metrics["log_mode"] == "ARCHIVELOG"
                        metrics["min_archive_scn"] = int(arch_row[1]) if arch_row[1] is not None else None
                        metrics["max_archive_scn"] = int(arch_row[2]) if arch_row[2] is not None else None
                except oracledb.DatabaseError:
                    pass

                try:
                    cur.execute("SELECT COUNT(*) FROM V$LOG WHERE STATUS IN ('CURRENT', 'ACTIVE')")
                    redo_row = cur.fetchone()
                    metrics["active_redo_groups"] = int(redo_row[0] or 0) if redo_row else 0
                except oracledb.DatabaseError:
                    pass
    except Exception as exc:
        metrics["error"] = str(exc)

    try:
        with oracledb.connect(
            user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn
        ) as tgt_conn:
            with tgt_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT SNAPSHOT_SCN, CDC_START_SCN, CDC_LAST_SCN, CUTOVER_STATUS
                    FROM C360.C360_CDC_CUTOVER_CONTROL
                    WHERE PIPELINE_NAME = 'C360_CITIZEN_PIPELINE'
                    """
                )
                ctrl_row = cur.fetchone()
                if ctrl_row:
                    metrics["snapshot_scn"] = int(ctrl_row[0]) if ctrl_row[0] is not None else None
                    metrics["cdc_start_scn"] = int(ctrl_row[1]) if ctrl_row[1] is not None else None
                    metrics["captured_scn"] = (
                        int(ctrl_row[2])
                        if ctrl_row[2] is not None
                        else (int(ctrl_row[1]) if ctrl_row[1] is not None else metrics["snapshot_scn"])
                    )
                    metrics["cutover_status"] = str(ctrl_row[3]) if ctrl_row[3] is not None else None
    except Exception:
        pass

    return metrics


def collect_debezium_metrics(
    connect_url: str = DEFAULT_CONNECT_URL,
    connector_name: str = DEFAULT_SOURCE_CONNECTOR,
    topic_end_offset: Optional[int] = None,
) -> Dict[str, Any]:
    """2. Debezium: Connector state, task state, lỗi đọc log, số event phát ra."""
    metrics: Dict[str, Any] = {
        "connector_name": connector_name,
        "connector_state": "UNREACHABLE",
        "task_state": "UNKNOWN",
        "log_read_error": None,
        "events_emitted": topic_end_offset,
        "error": None,
    }
    try:
        payload = _http_get_json(f"{connect_url.rstrip('/')}/connectors/{connector_name}/status")
        parsed = _extract_connector_status(payload)
        metrics["connector_state"] = parsed["connector_state"]
        metrics["task_state"] = parsed["task_state"]
        metrics["log_read_error"] = parsed["error_trace"]
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        metrics["error"] = str(exc)
    return metrics


def collect_kafka_metrics(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_CDC_TOPIC,
    consumer_group: str = DEFAULT_SINK_CONSUMER_GROUP,
    kafka_container: str = DEFAULT_KAFKA_CONTAINER,
) -> Dict[str, Any]:
    """3. Kafka: Topic partition, offset, consumer lag, broker availability."""
    broker_up = _check_tcp_endpoint(bootstrap_servers)
    metrics: Dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "consumer_group": consumer_group,
        "broker_available": broker_up,
        "partitions": [],
        "total_end_offset": 0,
        "total_committed_offset": 0,
        "consumer_lag": 0,
        "error": None,
    }
    if not broker_up:
        metrics["error"] = f"Broker unreachable at {bootstrap_servers}"
        return metrics

    try:
        off_out = _run_kafka_cli(
            kafka_container,
            [
                "kafka-get-offsets",
                "--bootstrap-server",
                "kafka:29092",
                "--topic",
                topic,
            ],
        )
        end_offsets = _parse_topic_offsets(off_out)
        metrics["total_end_offset"] = sum(end_offsets.values())

        cg_rows: List[Dict[str, Any]] = []
        try:
            cg_out = _run_kafka_cli(
                kafka_container,
                [
                    "kafka-consumer-groups",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--describe",
                    "--group",
                    consumer_group,
                ],
            )
            cg_rows = _parse_consumer_group_output(cg_out, topic)
        except Exception:
            cg_rows = []

        if cg_rows:
            metrics["partitions"] = cg_rows
            metrics["total_committed_offset"] = sum(r["committed_offset"] for r in cg_rows)
            metrics["consumer_lag"] = sum(max(0, r["lag"]) for r in cg_rows)
        else:
            metrics["partitions"] = [
                {"partition": p, "committed_offset": 0, "end_offset": off, "lag": off}
                for p, off in sorted(end_offsets.items())
            ]
            metrics["consumer_lag"] = metrics["total_end_offset"]
    except Exception as exc:
        metrics["error"] = str(exc)

    return metrics


def collect_jdbc_sink_metrics(
    connect_url: str = DEFAULT_CONNECT_URL,
    connector_name: str = DEFAULT_SINK_CONNECTOR,
    dlq_topic: str = DEFAULT_DLQ_TOPIC,
    kafka_metrics: Optional[Dict[str, Any]] = None,
    kafka_container: str = DEFAULT_KAFKA_CONTAINER,
) -> Dict[str, Any]:
    """4. JDBC Sink: Records consumed, records written, retry, DLQ, lỗi SQL."""
    consumed = int((kafka_metrics or {}).get("total_committed_offset") or 0)
    dlq_messages = 0
    dlq_error = None
    if (kafka_metrics or {}).get("broker_available"):
        try:
            dlq_out = _run_kafka_cli(
                kafka_container,
                [
                    "kafka-get-offsets",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--topic",
                    dlq_topic,
                ],
            )
            dlq_messages = sum(_parse_topic_offsets(dlq_out).values())
        except Exception as exc:
            dlq_error = str(exc)

    metrics: Dict[str, Any] = {
        "connector_name": connector_name,
        "connector_state": "UNREACHABLE",
        "task_state": "UNKNOWN",
        "records_consumed": consumed,
        "records_written": max(0, consumed - dlq_messages),
        "retry_count": 0,
        "dlq_topic": dlq_topic,
        "dlq_messages": dlq_messages,
        "sql_error": None,
        "error": dlq_error,
    }
    try:
        payload = _http_get_json(f"{connect_url.rstrip('/')}/connectors/{connector_name}/status")
        parsed = _extract_connector_status(payload)
        metrics["connector_state"] = parsed["connector_state"]
        metrics["task_state"] = parsed["task_state"]
        metrics["sql_error"] = parsed["error_trace"]
        if parsed["task_state"] == "FAILED" and metrics["records_consumed"] > 0 and dlq_messages == 0:
            metrics["records_written"] = 0
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        metrics["error"] = str(exc)

    return metrics


def collect_oracle_mirror_metrics() -> Dict[str, Any]:
    """5. Oracle Mirror: Tổng bản ghi, số bản ghi insert/update/delete, thời điểm cập nhật cuối."""
    metrics: Dict[str, Any] = {
        "available": False,
        "total_rows": 0,
        "insert_count": 0,
        "update_count": 0,
        "delete_count": 0,
        "last_updated_at": None,
        "c360_pending_queue_count": 0,
        "error": None,
    }
    try:
        with oracledb.connect(
            user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn
        ) as conn:
            with conn.cursor() as cur:
                metrics["available"] = True
                cur.execute(
                    """
                    SELECT COUNT(*), TO_CHAR(MAX(MIRROR_SYNC_AT), 'YYYY-MM-DD HH24:MI:SS')
                    FROM C360.MIRROR_VSS_HO_GD_NHANKHAU
                    """
                )
                row = cur.fetchone()
                if row:
                    metrics["total_rows"] = int(row[0] or 0)
                    metrics["last_updated_at"] = str(row[1]) if row[1] is not None else None

                cur.execute(
                    """
                    SELECT
                        NVL(SUM(CASE WHEN ACTION_TYPE = 'I' THEN 1 ELSE 0 END), 0),
                        NVL(SUM(CASE WHEN ACTION_TYPE IN ('U', 'U_OLD_CD') THEN 1 ELSE 0 END), 0),
                        NVL(SUM(CASE WHEN ACTION_TYPE = 'D' THEN 1 ELSE 0 END), 0),
                        NVL(SUM(CASE WHEN STATUS IN ('NEW', 'PROCESSING', 'FAILED') THEN 1 ELSE 0 END), 0)
                    FROM C360.C360_SYNC_QUEUE
                    """
                )
                q_row = cur.fetchone()
                if q_row:
                    metrics["insert_count"] = int(q_row[0] or 0)
                    metrics["update_count"] = int(q_row[1] or 0)
                    metrics["delete_count"] = int(q_row[2] or 0)
                    metrics["c360_pending_queue_count"] = int(q_row[3] or 0)
    except Exception as exc:
        metrics["error"] = str(exc)

    return metrics


def classify_pipeline_delivery_state(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phân biệt chính xác 4 tình huống chậm/tắc nghẽn giữa Kafka -> Sink -> Mirror -> C360.
    Không bao giờ chỉ dựa vào consumer_lag để kết luận mất dữ liệu.
    """
    kafka = metrics.get("kafka") or {}
    sink = metrics.get("jdbc_sink") or {}
    mirror = metrics.get("oracle_mirror") or {}

    consumer_lag = int(kafka.get("consumer_lag") or 0)
    records_consumed = int(sink.get("records_consumed") or 0)
    records_written = int(sink.get("records_written") or 0)
    dlq_messages = int(sink.get("dlq_messages") or 0)
    sink_task_state = str(sink.get("task_state") or "UNKNOWN").upper()
    sql_error = sink.get("sql_error")
    c360_pending = int(mirror.get("c360_pending_queue_count") or 0)

    active_states: List[str] = []
    details: List[str] = []

    if consumer_lag > 0:
        active_states.append("UNCONSUMED_IN_KAFKA")
        details.append(
            f"Message chưa được Kafka Consumer đọc (consumer_lag={consumer_lag}); "
            "đây là độ trễ đọc trên Kafka, không đồng nghĩa mất dữ liệu."
        )

    unwritten_by_sink = max(0, records_consumed - records_written - dlq_messages)
    if unwritten_by_sink > 0 or (sink_task_state == "FAILED" and dlq_messages == 0) or (sql_error and dlq_messages == 0):
        active_states.append("CONSUMED_NOT_WRITTEN_BY_SINK")
        details.append(
            "Message đã đọc khỏi Kafka nhưng JDBC Sink chưa ghi vào Mirror "
            f"(consumed={records_consumed}, written={records_written}, task_state={sink_task_state})."
        )

    if dlq_messages > 0:
        active_states.append("ROUTED_TO_DLQ")
        details.append(
            f"Có {dlq_messages} message ghi lỗi SQL/schema đã được đưa vào DLQ "
            f"({sink.get('dlq_topic', DEFAULT_DLQ_TOPIC)}); cần replay sau khi sửa lỗi."
        )

    if c360_pending > 0:
        active_states.append("WRITTEN_TO_MIRROR_PENDING_C360")
        details.append(
            f"Message đã ghi thành công vào Oracle Mirror nhưng còn {c360_pending} bản ghi "
            "trong C360_SYNC_QUEUE chưa được tầng C360 xử lý."
        )

    if not active_states:
        primary_state = "HEALTHY_SYNCED"
        details.append("Toàn bộ 5 tầng thông suốt, không tồn đọng lag, DLQ hay hàng đợi C360.")
    elif "ROUTED_TO_DLQ" in active_states:
        primary_state = "ROUTED_TO_DLQ"
    elif "CONSUMED_NOT_WRITTEN_BY_SINK" in active_states:
        primary_state = "CONSUMED_NOT_WRITTEN_BY_SINK"
    elif "UNCONSUMED_IN_KAFKA" in active_states:
        primary_state = "UNCONSUMED_IN_KAFKA"
    else:
        primary_state = "WRITTEN_TO_MIRROR_PENDING_C360"

    return {
        "primary_state": primary_state,
        "active_states": active_states,
        "possible_data_loss": dlq_messages > 0 or bool(sql_error),
        "lag_implies_data_loss": False,
        "details": details,
    }


def collect_pipeline_monitoring(
    connect_url: str = DEFAULT_CONNECT_URL,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_CDC_TOPIC,
    dlq_topic: str = DEFAULT_DLQ_TOPIC,
    consumer_group: str = DEFAULT_SINK_CONSUMER_GROUP,
    source_connector: str = DEFAULT_SOURCE_CONNECTOR,
    sink_connector: str = DEFAULT_SINK_CONNECTOR,
    kafka_container: str = DEFAULT_KAFKA_CONTAINER,
) -> Dict[str, Any]:
    """Thu thập đầy đủ metrics 5 tầng và phân loại trạng thái vận chuyển."""
    oracle_source = collect_oracle_source_metrics()
    kafka_metrics = collect_kafka_metrics(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        consumer_group=consumer_group,
        kafka_container=kafka_container,
    )
    debezium_metrics = collect_debezium_metrics(
        connect_url=connect_url,
        connector_name=source_connector,
        topic_end_offset=kafka_metrics.get("total_end_offset"),
    )
    jdbc_sink_metrics = collect_jdbc_sink_metrics(
        connect_url=connect_url,
        connector_name=sink_connector,
        dlq_topic=dlq_topic,
        kafka_metrics=kafka_metrics,
        kafka_container=kafka_container,
    )
    oracle_mirror = collect_oracle_mirror_metrics()

    report: Dict[str, Any] = {
        "oracle_source": oracle_source,
        "debezium": debezium_metrics,
        "kafka": kafka_metrics,
        "jdbc_sink": jdbc_sink_metrics,
        "oracle_mirror": oracle_mirror,
    }
    report["diagnosis"] = classify_pipeline_delivery_state(report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Giám sát 5 tầng CDC Pipeline (Oracle -> Debezium -> Kafka -> Sink -> Mirror).")
    parser.add_argument("--json", action="store_true", help="Xuất báo cáo dạng JSON.")
    parser.add_argument("--connect-url", default=DEFAULT_CONNECT_URL)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_CDC_TOPIC)
    parser.add_argument("--dlq-topic", default=DEFAULT_DLQ_TOPIC)
    parser.add_argument("--consumer-group", default=DEFAULT_SINK_CONSUMER_GROUP)
    args = parser.parse_args()

    report = collect_pipeline_monitoring(
        connect_url=args.connect_url,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        dlq_topic=args.dlq_topic,
        consumer_group=args.consumer_group,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print("=" * 72)
        print(f" PIPELINE MONITORING (DIAGNOSIS: {report['diagnosis']['primary_state']})")
        print("=" * 72)
        for layer in ("oracle_source", "debezium", "kafka", "jdbc_sink", "oracle_mirror", "diagnosis"):
            print(f"[{layer}] {report[layer]}")
        print("=" * 72)


if __name__ == "__main__":
    main()
