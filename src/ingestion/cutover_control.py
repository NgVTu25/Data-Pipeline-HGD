"""
Quản lý bảng điều khiển C360.C360_CDC_CUTOVER_CONTROL phục vụ Resume / Cutover
giữa bước Full Load (Oracle AS OF SCN) và luồng Debezium CDC (Kafka).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


DEFAULT_PIPELINE_NAME = "C360_CITIZEN_PIPELINE"
DEFAULT_KAFKA_TOPIC = "vss_cdc.VSS.VSS_HO_GD_NHANKHAU"


@dataclass
class CutoverState:
    """Trạng thái Cutover & SCN Checkpoint lưu trong C360.C360_CDC_CUTOVER_CONTROL."""

    pipeline_name: str = DEFAULT_PIPELINE_NAME
    snapshot_scn: Optional[int] = None
    cdc_start_scn: Optional[int] = None
    cdc_last_scn: Optional[int] = None
    kafka_topic: str = DEFAULT_KAFKA_TOPIC
    connector_status: str = "NOT_STARTED"
    cutover_status: str = "INIT"
    error_message: Optional[str] = None


def _to_int_or_none(value: Any) -> Optional[int]:
    return int(value) if value is not None else None


def get_cutover_state(
    conn: Any,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
) -> Optional[CutoverState]:
    """Đọc trạng thái hiện tại từ C360.C360_CDC_CUTOVER_CONTROL."""
    sql = """
        SELECT PIPELINE_NAME, SNAPSHOT_SCN, CDC_START_SCN, CDC_LAST_SCN,
               KAFKA_TOPIC, CONNECTOR_STATUS, CUTOVER_STATUS, ERROR_MESSAGE
        FROM C360.C360_CDC_CUTOVER_CONTROL
        WHERE PIPELINE_NAME = :pipeline_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"pipeline_name": pipeline_name})
        row = cur.fetchone()
    if not row:
        return None
    return CutoverState(
        pipeline_name=str(row[0]),
        snapshot_scn=_to_int_or_none(row[1]),
        cdc_start_scn=_to_int_or_none(row[2]),
        cdc_last_scn=_to_int_or_none(row[3]),
        kafka_topic=str(row[4] or DEFAULT_KAFKA_TOPIC),
        connector_status=str(row[5] or "NOT_STARTED"),
        cutover_status=str(row[6] or "INIT"),
        error_message=str(row[7]) if row[7] is not None else None,
    )


def upsert_cutover_state(conn: Any, state: CutoverState) -> None:
    """Ghi hoặc cập nhật checkpoint Cutover theo PIPELINE_NAME."""
    sql = """
        MERGE INTO C360.C360_CDC_CUTOVER_CONTROL tgt
        USING (
            SELECT :pipeline_name AS PIPELINE_NAME,
                   :snapshot_scn AS SNAPSHOT_SCN,
                   :cdc_start_scn AS CDC_START_SCN,
                   :cdc_last_scn AS CDC_LAST_SCN,
                   :kafka_topic AS KAFKA_TOPIC,
                   :connector_status AS CONNECTOR_STATUS,
                   :cutover_status AS CUTOVER_STATUS,
                   :error_message AS ERROR_MESSAGE
            FROM DUAL
        ) src
        ON (tgt.PIPELINE_NAME = src.PIPELINE_NAME)
        WHEN MATCHED THEN UPDATE SET
            tgt.SNAPSHOT_SCN = src.SNAPSHOT_SCN,
            tgt.CDC_START_SCN = src.CDC_START_SCN,
            tgt.CDC_LAST_SCN = src.CDC_LAST_SCN,
            tgt.KAFKA_TOPIC = src.KAFKA_TOPIC,
            tgt.CONNECTOR_STATUS = src.CONNECTOR_STATUS,
            tgt.CUTOVER_STATUS = src.CUTOVER_STATUS,
            tgt.ERROR_MESSAGE = src.ERROR_MESSAGE,
            tgt.UPDATED_AT = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            PIPELINE_NAME, SNAPSHOT_SCN, CDC_START_SCN, CDC_LAST_SCN,
            KAFKA_TOPIC, CONNECTOR_STATUS, CUTOVER_STATUS, ERROR_MESSAGE, UPDATED_AT
        ) VALUES (
            src.PIPELINE_NAME, src.SNAPSHOT_SCN, src.CDC_START_SCN, src.CDC_LAST_SCN,
            src.KAFKA_TOPIC, src.CONNECTOR_STATUS, src.CUTOVER_STATUS, src.ERROR_MESSAGE, SYSTIMESTAMP
        )
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "pipeline_name": state.pipeline_name,
                "snapshot_scn": state.snapshot_scn,
                "cdc_start_scn": state.cdc_start_scn,
                "cdc_last_scn": state.cdc_last_scn,
                "kafka_topic": state.kafka_topic,
                "connector_status": state.connector_status,
                "cutover_status": state.cutover_status,
                "error_message": state.error_message[:2000] if state.error_message else None,
            },
        )
    conn.commit()


def determine_resume_action(state: Optional[CutoverState]) -> Dict[str, Any]:
    """
    Xác định bước tiếp tục an toàn khi chạy lại pipeline dựa trên bảng control.
    Tránh việc chạy lại toàn bộ Full Load một cách tùy tiện khi snapshot đã hoàn tất.
    """
    if state is None or state.cutover_status in ("INIT", "FULL_LOAD_RUNNING"):
        return {
            "action": "RUN_FULL_LOAD",
            "resume_scn": None,
            "skip_full_load": False,
            "reason": "Chưa có Full Load hoàn chỉnh; cần chạy Full Load tại SCN mới.",
        }

    if state.cutover_status == "FAILED":
        if state.cdc_last_scn is not None:
            return {
                "action": "RESUME_CDC_FROM_LAST_SCN",
                "resume_scn": state.cdc_last_scn,
                "skip_full_load": True,
                "reason": "Full Load đã xong trước khi lỗi; tiếp tục CDC từ CDC_LAST_SCN.",
            }
        if state.snapshot_scn is not None and state.connector_status in ("NOT_STARTED", "FAILED", "PAUSED"):
            return {
                "action": "RESUME_CDC_FROM_SNAPSHOT_SCN",
                "resume_scn": state.cdc_start_scn or state.snapshot_scn,
                "skip_full_load": True,
                "reason": "Snapshot đã tồn tại; khôi phục CDC từ SNAPSHOT_SCN/CDC_START_SCN.",
            }
        return {
            "action": "RUN_FULL_LOAD",
            "resume_scn": None,
            "skip_full_load": False,
            "reason": "Full Load bị lỗi trước khi chốt SNAPSHOT_SCN; cần chạy lại Full Load.",
        }

    if state.cutover_status == "FULL_LOAD_DONE":
        resume_scn = state.cdc_start_scn or state.snapshot_scn
        if resume_scn is not None:
            return {
                "action": "RESUME_CDC_FROM_SNAPSHOT_SCN",
                "resume_scn": resume_scn,
                "skip_full_load": True,
                "reason": "Full Load đã hoàn tất; bỏ qua Full Load và bật CDC từ SNAPSHOT_SCN.",
            }
        return {
            "action": "RUN_FULL_LOAD",
            "resume_scn": None,
            "skip_full_load": False,
            "reason": "Trạng thái FULL_LOAD_DONE thiếu SNAPSHOT_SCN; cần chạy lại Full Load.",
        }

    if state.cutover_status == "CDC_ACTIVE":
        if state.cdc_last_scn is not None and state.connector_status != "RUNNING":
            return {
                "action": "RESUME_CDC_FROM_LAST_SCN",
                "resume_scn": state.cdc_last_scn,
                "skip_full_load": True,
                "reason": "CDC tạm dừng/lỗi sau khi đã xử lý một phần; tiếp tục từ CDC_LAST_SCN.",
            }
        if state.connector_status == "RUNNING":
            return {
                "action": "MONITOR_RUNNING_CDC",
                "resume_scn": state.cdc_last_scn or state.cdc_start_scn or state.snapshot_scn,
                "skip_full_load": True,
                "reason": "CDC connector đang RUNNING; giữ nguyên tiến trình và theo dõi lag.",
            }
        return {
            "action": "RESUME_CDC_FROM_SNAPSHOT_SCN",
            "resume_scn": state.cdc_start_scn or state.snapshot_scn,
            "skip_full_load": True,
            "reason": "CDC đã kích hoạt nhưng chưa có CDC_LAST_SCN; tiếp tục từ CDC_START_SCN.",
        }

    return {
        "action": "RUN_FULL_LOAD",
        "resume_scn": None,
        "skip_full_load": False,
        "reason": f"Trạng thái không xác định ({state.cutover_status}); chạy lại Full Load an toàn.",
    }
