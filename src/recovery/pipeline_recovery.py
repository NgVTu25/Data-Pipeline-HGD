"""
Hybrid Recovery Engine & CLI cho Pipeline BHXH-HS (4 tầng):
1. Auto-Heal (Lỗi tạm thời):
   - Kiểm tra Kafka broker & Debezium Source/Sink Connectors.
   - Tự động restart connector/task bị FAILED (với phân loại lỗi transient vs fatal + backoff).
   - Thu hồi dòng C360_SYNC_QUEUE bị kẹt PROCESSING quá hạn (stale lease).
2. Quarantine & Controlled Replay (Lỗi dữ liệu / DLQ / Queue FAILED):
   - Ingest message từ Kafka DLQ topic vào bảng C360.C360_DLQ_ARCHIVE.
   - Replay DLQ về topic gốc hoặc upsert trực tiếp vào C360.MIRROR_VSS_HO_GD_NHANKHAU.
   - Retry hoặc Quarantine các dòng C360.C360_SYNC_QUEUE bị FAILED.
3. Targeted Resync (Lệch dữ liệu Đối soát / Mất Archive Log):
   - Đồng bộ bù đích danh theo danh sách MA_CD từ VSS.VSS_HO_GD_NHANKHAU -> Mirror -> C360.

Chạy CLI:
  python -m src.recovery.pipeline_recovery auto-heal
  python -m src.recovery.pipeline_recovery ingest-dlq
  python -m src.recovery.pipeline_recovery replay-dlq --mode mirror-upsert
  python -m src.recovery.pipeline_recovery retry-queue --all-failed
  python -m src.recovery.pipeline_recovery resync-macd --auto-detect
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb

from src.config import SOURCE_DB, TARGET_DB

logger = logging.getLogger("c360_recovery")

DEFAULT_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_KAFKA_CONTAINER = os.getenv("KAFKA_DOCKER_CONTAINER", "hgd-kafka")
DEFAULT_CONNECTORS = [
    "vss-hgd-oracle-source-connector",
    "c360-mirror-jdbc-sink-connector",
    "c360-mirror-catalogs-jdbc-sink-connector",
]
DEFAULT_DLQ_TOPICS = [
    "dlq.c360.mirror.vss_ho_gd_nhankhau",
    "dlq.c360.mirror.catalogs",
]

TRANSIENT_ERROR_PATTERNS = (
    "communicationsexception",
    "sockettimeoutexception",
    "connectexception",
    "timeoutexception",
    "retriableexception",
    "disconnectexception",
    "connection reset",
    "broken pipe",
    "ora-01013",
    "ora-00060",
    "ora-12514",
    "ora-12541",
    "ora-03113",
    "ora-03114",
)

FATAL_ERROR_PATTERNS = (
    "ora-01291",
    "missing logfile",
    "archived log",
    "schemaexception",
    "dataexception",
    "ora-00904",
    "ora-01400",
    "ora-00001",
)


def classify_error_trace(trace: Optional[str]) -> str:
    """Phân loại stacktrace lỗi thành TRANSIENT, FATAL hoặc UNKNOWN."""
    if not trace:
        return "UNKNOWN"
    lowered = trace.lower()
    if any(pat in lowered for pat in FATAL_ERROR_PATTERNS):
        return "FATAL"
    if any(pat in lowered for pat in TRANSIENT_ERROR_PATTERNS):
        return "TRANSIENT"
    return "UNKNOWN"


def check_kafka_broker(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS, timeout: float = 3.0) -> bool:
    """Kiểm tra kết nối TCP tới Kafka broker đầu tiên."""
    first = bootstrap_servers.split(",")[0].strip()
    if ":" in first:
        host, port_str = first.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = first, 9092
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _http_json(url: str, method: str = "GET", timeout: float = 15.0) -> Tuple[int, Dict[str, Any]]:
    req = urllib.request.Request(url, method=method, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8").strip()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, {"error": body}


def get_connector_status(connect_url: str, connector_name: str) -> Dict[str, Any]:
    """Lấy trạng thái connector và các task từ Kafka Connect REST API."""
    status_code, payload = _http_json(f"{connect_url.rstrip('/')}/connectors/{connector_name}/status")
    if status_code == 404:
        return {"name": connector_name, "exists": False, "connector_state": "NOT_FOUND", "tasks": []}
    if status_code >= 400:
        return {
            "name": connector_name,
            "exists": False,
            "connector_state": "ERROR",
            "error": payload.get("error"),
            "tasks": [],
        }
    return {
        "name": connector_name,
        "exists": True,
        "connector_state": (payload.get("connector") or {}).get("state", "UNKNOWN"),
        "tasks": payload.get("tasks") or [],
    }


def log_recovery_event(
    conn: Optional[Any],
    layer: str,
    action: str,
    target_object: str,
    status: str,
    details: str,
) -> None:
    """Ghi log khôi phục vào C360.C360_RECOVERY_LOG (bỏ qua an toàn nếu bảng chưa tồn tại hoặc conn=None)."""
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO C360.C360_RECOVERY_LOG (LAYER, ACTION, TARGET_OBJECT, STATUS, DETAILS)
                VALUES (:layer, :action, :target_object, :status, :details)
                """,
                {
                    "layer": layer[:40],
                    "action": action[:60],
                    "target_object": target_object[:200],
                    "status": status[:20],
                    "details": details[:4000],
                },
            )
        conn.commit()
    except Exception as exc:
        logger.debug("Cannot write C360_RECOVERY_LOG (non-fatal): %s", exc)


def heal_connectors(
    connect_url: str = DEFAULT_CONNECT_URL,
    connectors: Sequence[str] = DEFAULT_CONNECTORS,
    max_attempts: int = 3,
    backoff_sec: float = 2.0,
    include_paused: bool = False,
    force: bool = False,
    conn: Optional[Any] = None,
) -> Dict[str, Any]:
    """Kiểm tra và tự động restart connector/task bị FAILED (hoặc PAUSED nếu include_paused=True)."""
    results: Dict[str, Any] = {}
    base = connect_url.rstrip("/")

    for name in connectors:
        st = get_connector_status(base, name)
        if not st["exists"]:
            results[name] = {"action": "SKIP", "reason": st["connector_state"]}
            continue

        c_state = st["connector_state"]
        tasks = st["tasks"]
        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
        paused_state = c_state == "PAUSED" or any(t.get("state") == "PAUSED" for t in tasks)

        needs_heal = (c_state == "FAILED") or bool(failed_tasks) or (include_paused and paused_state)
        if not needs_heal:
            results[name] = {"action": "NONE", "connector_state": c_state}
            continue

        traces = [t.get("trace") for t in failed_tasks if t.get("trace")]
        error_class = classify_error_trace(traces[0] if traces else None)
        if error_class == "FATAL" and not force:
            msg = "Skipped auto-restart because error is FATAL (requires manual/resync)."
            log_recovery_event(conn, "DEBEZIUM", "AUTO_HEAL_SKIP", name, "SKIPPED", msg)
            results[name] = {"action": "SKIPPED_FATAL", "error_class": error_class, "reason": msg}
            continue

        healed = False
        last_status = st
        for attempt in range(1, max_attempts + 1):
            if paused_state and include_paused:
                _http_json(f"{base}/connectors/{name}/resume", method="PUT")
            else:
                code, _ = _http_json(
                    f"{base}/connectors/{name}/restart?includeTasks=true&onlyFailed=true",
                    method="POST",
                )
                if code >= 400:
                    for t in failed_tasks:
                        _http_json(f"{base}/connectors/{name}/tasks/{t.get('id', 0)}/restart", method="POST")

            time.sleep(backoff_sec * attempt)
            last_status = get_connector_status(base, name)
            c_now = last_status["connector_state"]
            t_now = [t.get("state") for t in last_status["tasks"]]
            if c_now == "RUNNING" and all(s == "RUNNING" for s in t_now):
                healed = True
                break

        final_state = "SUCCESS" if healed else "FAILED"
        log_recovery_event(
            conn,
            "DEBEZIUM",
            "AUTO_HEAL_RESTART",
            name,
            final_state,
            f"error_class={error_class}, after_state={last_status['connector_state']}",
        )
        results[name] = {
            "action": "RESTARTED",
            "healed": healed,
            "error_class": error_class,
            "connector_state": last_status["connector_state"],
            "task_states": [t.get("state") for t in last_status["tasks"]],
        }

    return results


def reclaim_stale_processing_queue(conn: Any, stale_minutes: int = 5) -> int:
    """Thu hồi các dòng C360_SYNC_QUEUE bị kẹt PROCESSING quá lâu về NEW (hoặc FAILED nếu quá retry)."""
    sql = """
        UPDATE C360.C360_SYNC_QUEUE
        SET STATUS = CASE WHEN RETRY_COUNT + 1 >= 5 THEN 'FAILED' ELSE 'NEW' END,
            RETRY_COUNT = RETRY_COUNT + 1,
            CLAIMED_BATCH_ID = NULL,
            CLAIMED_AT = NULL
        WHERE STATUS = 'PROCESSING'
          AND CLAIMED_AT < SYSTIMESTAMP - NUMTODSINTERVAL(:stale_minutes, 'MINUTE')
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"stale_minutes": stale_minutes})
        affected = int(cur.rowcount or 0)
    conn.commit()
    if affected > 0:
        log_recovery_event(
            conn,
            "ORACLE_QUEUE",
            "RECLAIM_STALE_LEASE",
            "C360.C360_SYNC_QUEUE",
            "SUCCESS",
            f"Reclaimed {affected} stale PROCESSING rows (> {stale_minutes}m).",
        )
    return affected


def reclaim_stale_sync_logs(conn: Any, stale_minutes: int = 2) -> int:
    """
    Đánh dấu FAILED cho các bản ghi C360_SYNC_LOG bị treo (worker đã chết / mất heartbeat):
    - Quá `stale_minutes` không thấy HEARTBEAT_AT cập nhật (hoặc START_TIME nếu chưa có heartbeat), HOẶC
    - Với RUN_MODE = 'NEAR_RT': không còn dòng C360_SYNC_QUEUE nào giữ CLAIMED_BATCH_ID = l.BATCH_ID
      sau ít nhất `stale_minutes` (batch mồ côi do queue đã bị reclaim).
    """
    if stale_minutes <= 0:
        raise ValueError("stale_minutes phải > 0")

    sql = """
        UPDATE C360.C360_SYNC_LOG l
        SET l.STATUS = 'FAILED',
            l.END_TIME = SYSTIMESTAMP,
            l.ERROR_MESSAGE = NVL(
                l.ERROR_MESSAGE,
                'Marked FAILED by auto-heal: worker heartbeat expired or orphaned batch (> ' || :stale_minutes || 'm)'
            )
        WHERE l.STATUS = 'RUNNING'
          AND l.END_TIME IS NULL
          AND (
              NVL(l.HEARTBEAT_AT, l.START_TIME) < SYSTIMESTAMP - NUMTODSINTERVAL(:stale_minutes, 'MINUTE')
              OR (
                  l.RUN_MODE = 'NEAR_RT'
                  AND l.START_TIME < SYSTIMESTAMP - NUMTODSINTERVAL(:stale_minutes, 'MINUTE')
                  AND NOT EXISTS (
                      SELECT 1 FROM C360.C360_SYNC_QUEUE q
                      WHERE q.CLAIMED_BATCH_ID = l.BATCH_ID
                  )
              )
          )
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"stale_minutes": stale_minutes})
        affected = int(cur.rowcount or 0)
    conn.commit()
    if affected > 0:
        log_recovery_event(
            conn,
            "ORACLE_SYNC_LOG",
            "MARK_STALE_SYNC_LOG_FAILED",
            "C360.C360_SYNC_LOG",
            "SUCCESS",
            f"Marked {affected} dead/orphaned RUNNING batch(es) (> {stale_minutes}m) as FAILED.",
        )
    return affected


def run_auto_heal(
    connect_url: str = DEFAULT_CONNECT_URL,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    include_paused: bool = False,
    stale_minutes: int = 5,
    sync_log_stale_minutes: int = 2,
    use_oracle: bool = True,
) -> Dict[str, Any]:
    """Chạy chu trình Auto-Heal đầy đủ cho Kafka, Debezium Connectors, Oracle Queue và Sync Log."""
    kafka_up = check_kafka_broker(bootstrap_servers)
    conn = None
    reclaimed = 0
    marked_failed_logs = 0
    if use_oracle:
        try:
            conn = oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn)
            reclaimed = reclaim_stale_processing_queue(conn, stale_minutes=stale_minutes)
            marked_failed_logs = reclaim_stale_sync_logs(conn, stale_minutes=sync_log_stale_minutes)
        except Exception as exc:
            logger.warning("Oracle connection unavailable during auto-heal: %s", exc)

    try:
        connector_results = (
            heal_connectors(connect_url=connect_url, include_paused=include_paused, conn=conn)
            if kafka_up
            else {"error": "Kafka broker unreachable"}
        )
        return {
            "kafka_broker_up": kafka_up,
            "connectors": connector_results,
            "reclaimed_queue_rows": reclaimed,
            "marked_failed_sync_logs": marked_failed_logs,
        }
    finally:
        if conn is not None:
            conn.close()


def parse_dlq_consumer_line(raw_line: str, default_topic: str) -> Optional[Dict[str, Any]]:
    """
    Parse 1 dòng từ kafka-console-consumer với format:
    Partition:<p>\tOffset:<o>\t<headers_or_NO_HEADERS>\t<key_or_null>\t<payload_json>
    Hoặc dạng fallback JSON thuần.
    """
    line = raw_line.strip()
    if not line:
        return None

    parts = line.split("\t")
    if len(parts) >= 5 and parts[0].startswith("Partition:") and parts[1].startswith("Offset:"):
        partition = int(parts[0].split(":", 1)[1].strip())
        offset = int(parts[1].split(":", 1)[1].strip())
        headers_raw = parts[2].strip()
        if headers_raw.startswith("Headers:"):
            headers_raw = headers_raw.split(":", 1)[1].strip()
        key_raw = parts[3].strip()
        if key_raw.startswith("Key:"):
            key_raw = key_raw.split(":", 1)[1].strip()
        payload_str = "\t".join(parts[4:]).strip()
        return {
            "topic_name": default_topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "record_key": None if key_raw in ("", "null", "NO_KEY") else key_raw,
            "error_headers_json": "{}" if headers_raw in ("", "NO_HEADERS") else headers_raw,
            "payload_json": payload_str,
            "error_message": (
                "DLQ message captured"
                if headers_raw in ("", "NO_HEADERS")
                else headers_raw[:4000]
            ),
        }

    return {
        "topic_name": default_topic,
        "kafka_partition": 0,
        "kafka_offset": abs(hash(line)) % 10_000_000,
        "record_key": None,
        "error_headers_json": "{}",
        "payload_json": line,
        "error_message": "Raw DLQ payload",
    }


def ingest_dlq_to_oracle(
    conn: Any,
    topics: Sequence[str] = DEFAULT_DLQ_TOPICS,
    kafka_container: str = DEFAULT_KAFKA_CONTAINER,
    max_messages: int = 500,
) -> Dict[str, int]:
    """Đọc message từ các Kafka DLQ topic và lưu vào C360.C360_DLQ_ARCHIVE."""
    inserted = 0
    for topic in topics:
        cmd = [
            "docker",
            "exec",
            kafka_container,
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            str(max_messages),
            "--timeout-ms",
            "4000",
            "--property",
            "print.partition=true",
            "--property",
            "print.offset=true",
            "--property",
            "print.headers=true",
            "--property",
            "print.key=true",
            "--property",
            "key.separator=\t",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        for line in proc.stdout.splitlines():
            rec = parse_dlq_consumer_line(line, topic)
            if not rec:
                continue
            with conn.cursor() as cur:
                cur.execute(
                    """
                    MERGE INTO C360.C360_DLQ_ARCHIVE tgt
                    USING (
                        SELECT :topic_name AS TOPIC_NAME,
                               :kafka_partition AS KAFKA_PARTITION,
                               :kafka_offset AS KAFKA_OFFSET
                        FROM DUAL
                    ) src
                    ON (
                        tgt.TOPIC_NAME = src.TOPIC_NAME
                        AND tgt.KAFKA_PARTITION = src.KAFKA_PARTITION
                        AND tgt.KAFKA_OFFSET = src.KAFKA_OFFSET
                    )
                    WHEN NOT MATCHED THEN INSERT (
                        TOPIC_NAME, KAFKA_PARTITION, KAFKA_OFFSET, RECORD_KEY,
                        PAYLOAD_JSON, ERROR_HEADERS_JSON, ERROR_MESSAGE, STATUS
                    ) VALUES (
                        :topic_name, :kafka_partition, :kafka_offset, :record_key,
                        :payload_json, :error_headers_json, :error_message, 'NEW'
                    )
                    """,
                    rec,
                )
                inserted += int(cur.rowcount or 0)
        conn.commit()

    if inserted > 0:
        log_recovery_event(
            conn,
            "KAFKA_DLQ",
            "INGEST_DLQ",
            ",".join(topics),
            "SUCCESS",
            f"Ingested {inserted} new DLQ messages into C360.C360_DLQ_ARCHIVE.",
        )
    return {"ingested_count": inserted}


def _extract_after_record(payload_json: str) -> Optional[Dict[str, Any]]:
    data = json.loads(payload_json)
    if isinstance(data, dict) and "payload" in data and isinstance(data["payload"], dict):
        data = data["payload"]
    if isinstance(data, dict) and "after" in data and isinstance(data["after"], dict):
        return data["after"]
    if isinstance(data, dict):
        return data
    return None


def replay_dlq(
    conn: Any,
    dlq_id: Optional[int] = None,
    mode: str = "mirror-upsert",
    kafka_container: str = DEFAULT_KAFKA_CONTAINER,
    target_topic: str = "vss_cdc.VSS.VSS_HO_GD_NHANKHAU",
) -> Dict[str, Any]:
    """Replay các bản ghi DLQ trạng thái NEW (về Mirror trực tiếp hoặc đẩy lại vào Kafka topic)."""
    sql = """
        SELECT DLQ_ID, RECORD_KEY, PAYLOAD_JSON
        FROM C360.C360_DLQ_ARCHIVE
        WHERE STATUS = 'NEW'
    """
    params: Dict[str, Any] = {}
    if dlq_id is not None:
        sql += " AND DLQ_ID = :dlq_id"
        params["dlq_id"] = dlq_id
    sql += " ORDER BY DLQ_ID"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    replayed = 0
    failed = 0
    for row_id, rec_key, payload_lob in rows:
        payload_str = payload_lob.read() if hasattr(payload_lob, "read") else str(payload_lob or "")
        try:
            if mode == "topic":
                line = f"{rec_key or ''}\t{payload_str}"
                proc = subprocess.run(
                    [
                        "docker",
                        "exec",
                        "-i",
                        kafka_container,
                        "kafka-console-producer",
                        "--bootstrap-server",
                        "kafka:29092",
                        "--topic",
                        target_topic,
                        "--property",
                        "parse.key=true",
                        "--property",
                        "key.separator=\t",
                    ],
                    input=line + "\n",
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if proc.returncode != 0:
                    raise RuntimeError(proc.stderr or "kafka-console-producer failed")
            else:
                record = _extract_after_record(payload_str)
                if not record or not record.get("MA_CD") or not record.get("MA_HO_GD"):
                    raise ValueError("Payload thiếu khóa MA_CD hoặc MA_HO_GD để mirror-upsert")
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        MERGE INTO C360.MIRROR_VSS_HO_GD_NHANKHAU tgt
                        USING (
                            SELECT :MA_CD AS MA_CD,
                                   :MA_HO_GD AS MA_HO_GD,
                                   :HO_TEN AS HO_TEN,
                                   :TYPEID AS TYPEID,
                                   :TRANG_THAI_DUYET AS TRANG_THAI_DUYET
                            FROM DUAL
                        ) src
                        ON (tgt.MA_CD = src.MA_CD AND tgt.MA_HO_GD = src.MA_HO_GD)
                        WHEN MATCHED THEN UPDATE SET
                            tgt.HO_TEN = NVL(src.HO_TEN, tgt.HO_TEN),
                            tgt.TYPEID = NVL(src.TYPEID, tgt.TYPEID),
                            tgt.TRANG_THAI_DUYET = NVL(src.TRANG_THAI_DUYET, tgt.TRANG_THAI_DUYET),
                            tgt.MODIFY_DATE = SYSTIMESTAMP
                        WHEN NOT MATCHED THEN INSERT (
                            MA_CD, MA_HO_GD, HO_TEN, TYPEID, TRANG_THAI_DUYET, MODIFY_DATE
                        ) VALUES (
                            src.MA_CD, src.MA_HO_GD, NVL(src.HO_TEN, 'UNKNOWN'),
                            src.TYPEID, NVL(src.TRANG_THAI_DUYET, 0), SYSTIMESTAMP
                        )
                        """,
                        {
                            "MA_CD": str(record["MA_CD"]),
                            "MA_HO_GD": str(record["MA_HO_GD"]),
                            "HO_TEN": record.get("HO_TEN"),
                            "TYPEID": record.get("TYPEID"),
                            "TRANG_THAI_DUYET": record.get("TRANG_THAI_DUYET"),
                        },
                    )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE C360.C360_DLQ_ARCHIVE
                    SET STATUS = 'REPLAYED',
                        REPLAY_COUNT = REPLAY_COUNT + 1,
                        UPDATED_AT = SYSTIMESTAMP
                    WHERE DLQ_ID = :dlq_id
                    """,
                    {"dlq_id": row_id},
                )
            conn.commit()
            replayed += 1
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE C360.C360_DLQ_ARCHIVE
                    SET STATUS = 'FAILED',
                        REPLAY_COUNT = REPLAY_COUNT + 1,
                        ERROR_MESSAGE = :err,
                        UPDATED_AT = SYSTIMESTAMP
                    WHERE DLQ_ID = :dlq_id
                    """,
                    {"dlq_id": row_id, "err": str(exc)[:4000]},
                )
            conn.commit()
            failed += 1

    log_recovery_event(
        conn,
        "KAFKA_DLQ",
        f"REPLAY_DLQ_{mode.upper()}",
        target_topic,
        "SUCCESS" if failed == 0 else "PARTIAL",
        f"replayed={replayed}, failed={failed}",
    )
    return {"replayed": replayed, "failed": failed}


def retry_failed_queue(
    conn: Any,
    ma_cd: Optional[str] = None,
    queue_id: Optional[int] = None,
    all_failed: bool = False,
) -> int:
    """Reset các dòng C360_SYNC_QUEUE từ FAILED về NEW để batch xử lý lại."""
    if not (ma_cd or queue_id or all_failed):
        raise ValueError("Cần truyền --ma-cd, --queue-id hoặc --all-failed.")

    sql = """
        UPDATE C360.C360_SYNC_QUEUE
        SET STATUS = 'NEW',
            RETRY_COUNT = 0,
            CLAIMED_BATCH_ID = NULL,
            CLAIMED_AT = NULL
        WHERE STATUS = 'FAILED'
    """
    params: Dict[str, Any] = {}
    if ma_cd:
        sql += " AND MA_CD = :ma_cd"
        params["ma_cd"] = ma_cd
    if queue_id is not None:
        sql += " AND QUEUE_ID = :queue_id"
        params["queue_id"] = queue_id

    with conn.cursor() as cur:
        cur.execute(sql, params)
        affected = int(cur.rowcount or 0)
    conn.commit()

    log_recovery_event(
        conn,
        "ORACLE_QUEUE",
        "RETRY_FAILED_QUEUE",
        ma_cd or str(queue_id or "ALL_FAILED"),
        "SUCCESS",
        f"Reset {affected} FAILED queue rows to NEW.",
    )
    return affected


def quarantine_failed_queue(
    conn: Any,
    ma_cd: Optional[str] = None,
    queue_id: Optional[int] = None,
) -> int:
    """Cách ly (chuyển STATUS = 'IGNORED') cho các dòng poison pill trong C360_SYNC_QUEUE."""
    if not (ma_cd or queue_id):
        raise ValueError("Cần truyền --ma-cd hoặc --queue-id để quarantine.")

    sql = "UPDATE C360.C360_SYNC_QUEUE SET STATUS = 'IGNORED' WHERE STATUS = 'FAILED'"
    params: Dict[str, Any] = {}
    if ma_cd:
        sql += " AND MA_CD = :ma_cd"
        params["ma_cd"] = ma_cd
    if queue_id is not None:
        sql += " AND QUEUE_ID = :queue_id"
        params["queue_id"] = queue_id

    with conn.cursor() as cur:
        cur.execute(sql, params)
        affected = int(cur.rowcount or 0)
    conn.commit()

    log_recovery_event(
        conn,
        "ORACLE_QUEUE",
        "QUARANTINE_QUEUE",
        ma_cd or str(queue_id),
        "SUCCESS",
        f"Quarantined {affected} FAILED queue rows to IGNORED.",
    )
    return affected


def find_drifted_macds(conn_source: Any, conn_target: Any, limit: int = 500) -> List[str]:
    """Tìm danh sách MA_CD bị thiếu ở Mirror hoặc C360 so với Source."""
    with conn_source.cursor() as cur:
        cur.execute("SELECT DISTINCT MA_CD FROM VSS.VSS_HO_GD_NHANKHAU")
        source_keys = {str(r[0]) for r in cur.fetchall() if r[0]}

    with conn_target.cursor() as cur:
        cur.execute("SELECT DISTINCT MA_CD FROM C360.MIRROR_VSS_HO_GD_NHANKHAU")
        mirror_keys = {str(r[0]) for r in cur.fetchall() if r[0]}

    drifted = sorted(source_keys - mirror_keys)
    return drifted[:limit]


def resync_macds(
    conn_source: Any,
    conn_target: Any,
    macds: Sequence[str],
    run_batch: bool = True,
) -> Dict[str, Any]:
    """Đồng bộ bù đích danh theo danh sách MA_CD từ Source -> Mirror -> C360."""
    if not macds:
        return {"resynced_rows": 0, "macds_count": 0}

    synced_rows = 0
    with conn_source.cursor() as s_cur, conn_target.cursor() as t_cur:
        for ma_cd in macds:
            s_cur.execute(
                """
                SELECT ID, MA_HO_GD, MA_CD, HO_TEN, NGAY_SINH, GIOI_TINH, SO_CMTND,
                       PHONE_NUMBER, MA_TINH, MA_HUYEN, MA_XA, DIA_CHI,
                       TYPEID, TRANG_THAI_DUYET, MODIFY_DATE
                FROM VSS.VSS_HO_GD_NHANKHAU
                WHERE MA_CD = :ma_cd
                """,
                {"ma_cd": ma_cd},
            )
            cols = [d[0] for d in s_cur.description]
            rows = s_cur.fetchall()
            if not rows:
                t_cur.execute("DELETE FROM C360.MIRROR_VSS_HO_GD_NHANKHAU WHERE MA_CD = :ma_cd", {"ma_cd": ma_cd})
                continue

            for row in rows:
                rec = dict(zip(cols, row))
                t_cur.execute(
                    """
                    MERGE INTO C360.MIRROR_VSS_HO_GD_NHANKHAU tgt
                    USING (SELECT :MA_CD AS MA_CD, :MA_HO_GD AS MA_HO_GD FROM DUAL) src
                    ON (tgt.MA_CD = src.MA_CD AND tgt.MA_HO_GD = src.MA_HO_GD)
                    WHEN MATCHED THEN UPDATE SET
                        tgt.ID = :ID,
                        tgt.HO_TEN = :HO_TEN,
                        tgt.NGAY_SINH = :NGAY_SINH,
                        tgt.GIOI_TINH = :GIOI_TINH,
                        tgt.SO_CMTND = :SO_CMTND,
                        tgt.PHONE_NUMBER = :PHONE_NUMBER,
                        tgt.MA_TINH = :MA_TINH,
                        tgt.MA_HUYEN = :MA_HUYEN,
                        tgt.MA_XA = :MA_XA,
                        tgt.DIA_CHI = :DIA_CHI,
                        tgt.TYPEID = :TYPEID,
                        tgt.TRANG_THAI_DUYET = :TRANG_THAI_DUYET,
                        tgt.MODIFY_DATE = :MODIFY_DATE
                    WHEN NOT MATCHED THEN INSERT (
                        ID, MA_HO_GD, MA_CD, HO_TEN, NGAY_SINH, GIOI_TINH, SO_CMTND,
                        PHONE_NUMBER, MA_TINH, MA_HUYEN, MA_XA, DIA_CHI,
                        TYPEID, TRANG_THAI_DUYET, MODIFY_DATE
                    ) VALUES (
                        :ID, :MA_HO_GD, :MA_CD, :HO_TEN, :NGAY_SINH, :GIOI_TINH, :SO_CMTND,
                        :PHONE_NUMBER, :MA_TINH, :MA_HUYEN, :MA_XA, :DIA_CHI,
                        :TYPEID, :TRANG_THAI_DUYET, :MODIFY_DATE
                    )
                    """,
                    rec,
                )
                synced_rows += 1

    conn_target.commit()

    if run_batch:
        with conn_target.cursor() as cur:
            cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds => 0); END;")

    log_recovery_event(
        conn_target,
        "RECONCILIATION",
        "TARGETED_RESYNC_MACD",
        f"{len(macds)} MA_CD(s)",
        "SUCCESS",
        f"Resynced {synced_rows} rows across {len(macds)} MA_CDs.",
    )
    return {"resynced_rows": synced_rows, "macds_count": len(macds)}


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="BHXH-HS Hybrid Recovery CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_heal = sub.add_parser("auto-heal", help="Kiểm tra Kafka/Debezium và tự động restart task FAILED.")
    p_heal.add_argument("--include-paused", action="store_true")
    p_heal.add_argument("--stale-minutes", type=int, default=5)
    p_heal.add_argument("--sync-log-stale-minutes", type=int, default=2)

    p_ing = sub.add_parser("ingest-dlq", help="Đọc Kafka DLQ topic lưu vào C360_DLQ_ARCHIVE.")
    p_ing.add_argument("--max-messages", type=int, default=500)

    p_rep = sub.add_parser("replay-dlq", help="Replay bản ghi từ C360_DLQ_ARCHIVE.")
    p_rep.add_argument("--dlq-id", type=int, default=None)
    p_rep.add_argument("--mode", choices=["mirror-upsert", "topic"], default="mirror-upsert")

    p_ret = sub.add_parser("retry-queue", help="Reset bản ghi FAILED trong C360_SYNC_QUEUE về NEW.")
    p_ret.add_argument("--ma-cd", type=str, default=None)
    p_ret.add_argument("--queue-id", type=int, default=None)
    p_ret.add_argument("--all-failed", action="store_true")

    p_quar = sub.add_parser("quarantine-queue", help="Cách ly bản ghi FAILED sang IGNORED.")
    p_quar.add_argument("--ma-cd", type=str, default=None)
    p_quar.add_argument("--queue-id", type=int, default=None)

    p_res = sub.add_parser("resync-macd", help="Đồng bộ bù theo danh sách MA_CD từ Source -> Mirror -> C360.")
    p_res.add_argument("--ma-cd", action="append", default=[])
    p_res.add_argument("--auto-detect", action="store_true")
    p_res.add_argument("--no-batch", action="store_true")

    args = parser.parse_args(argv)

    if args.command == "auto-heal":
        res = run_auto_heal(
            include_paused=args.include_paused,
            stale_minutes=args.stale_minutes,
            sync_log_stale_minutes=args.sync_log_stale_minutes,
        )
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as t_conn:
        if args.command == "ingest-dlq":
            res = ingest_dlq_to_oracle(t_conn, max_messages=args.max_messages)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            return 0

        if args.command == "replay-dlq":
            res = replay_dlq(t_conn, dlq_id=args.dlq_id, mode=args.mode)
            print(json.dumps(res, indent=2, ensure_ascii=False))
            return 0

        if args.command == "retry-queue":
            affected = retry_failed_queue(
                t_conn, ma_cd=args.ma_cd, queue_id=args.queue_id, all_failed=args.all_failed
            )
            print(json.dumps({"reset_to_new": affected}, indent=2, ensure_ascii=False))
            return 0

        if args.command == "quarantine-queue":
            affected = quarantine_failed_queue(t_conn, ma_cd=args.ma_cd, queue_id=args.queue_id)
            print(json.dumps({"quarantined": affected}, indent=2, ensure_ascii=False))
            return 0

        if args.command == "resync-macd":
            with oracledb.connect(user=SOURCE_DB.user, password=SOURCE_DB.password, dsn=SOURCE_DB.dsn) as s_conn:
                macds = list(args.ma_cd)
                if args.auto_detect:
                    macds.extend(find_drifted_macds(s_conn, t_conn))
                res = resync_macds(s_conn, t_conn, macds, run_batch=not args.no_batch)
                print(json.dumps(res, indent=2, ensure_ascii=False))
                return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
