"""
Phần 4 - Lập lịch Airflow (DAGs) điều phối toàn trình:
1. DAG 1 (`c360_near_realtime_sync`): Chạy mỗi phút gọi `C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH`.
2. DAG 2 (`c360_daily_map_and_reconcile`): Hàng ngày làm mới bảng Map DBHC (DELETE + INSERT atomic)
   và chạy Job Đối soát 3 tầng (Nguồn <-> Mirror <-> C360).

Hỗ trợ chạy trực tiếp trên Windows/Local (khi chưa cài Apache Airflow) bằng:
  python -m airflow.dags.dag_hgd_c360_pipeline [--loop] [--interval 10]
"""
from __future__ import annotations

import argparse
import logging
import os
import socket
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    HAS_AIRFLOW = True
except ImportError:
    DAG = None  # type: ignore[assignment]
    PythonOperator = None  # type: ignore[assignment]
    HAS_AIRFLOW = False


class _SyncLogHeartbeat:
    """Thread nền cập nhật HEARTBEAT_AT mỗi 30s cho batch đang RUNNING trên C360_SYNC_LOG."""

    def __init__(self, run_mode: str = "NEAR_RT", oracle_sid: Optional[int] = None, interval_sec: float = 30.0):
        self.run_mode = run_mode
        self.oracle_sid = oracle_sid
        self.interval_sec = interval_sec
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _beat_once(self) -> None:
        import oracledb
        from src.config import TARGET_DB

        with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as hb_conn:
            with hb_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE C360.C360_SYNC_LOG
                    SET HEARTBEAT_AT = SYSTIMESTAMP,
                        WORKER_ID = NVL(WORKER_ID, :worker_id),
                        ORACLE_SID = NVL(ORACLE_SID, :oracle_sid)
                    WHERE STATUS = 'RUNNING'
                      AND END_TIME IS NULL
                      AND RUN_MODE = :run_mode
                    """,
                    {
                        "worker_id": self.worker_id[:100],
                        "oracle_sid": self.oracle_sid,
                        "run_mode": self.run_mode,
                    },
                )
            hb_conn.commit()

    def _loop(self) -> None:
        while not self._stop_event.wait(self.interval_sec):
            try:
                self._beat_once()
            except Exception as exc:
                logging.getLogger("c360_heartbeat").debug("Heartbeat update skipped: %s", exc)

    def __enter__(self) -> "_SyncLogHeartbeat":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def _exec_near_realtime_batch() -> None:
    """Thực thi thực tế procedure C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH trên Oracle."""
    import oracledb
    from src.config import TARGET_DB

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT TO_NUMBER(SYS_CONTEXT('USERENV', 'SID')) FROM DUAL")
            sid_row = cur.fetchone()
            oracle_sid = int(sid_row[0]) if sid_row and sid_row[0] is not None else None
            with _SyncLogHeartbeat(run_mode="NEAR_RT", oracle_sid=oracle_sid):
                cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds => 2); END;")


def _exec_refresh_dbhc_maps() -> None:
    """Thực thi thực tế procedure C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS trên Oracle."""
    import oracledb
    from src.config import TARGET_DB

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS; END;")


def _exec_reconciliation() -> None:
    """Chạy đối soát 3 tầng và raise lỗi nếu phát hiện FAIL."""
    from src.reconciliation.reconcile_pipeline import run_reconciliation

    report = run_reconciliation()
    logging.getLogger("airflow.task").info("Reconciliation report: %s", report)
    if report["overall_status"] == "FAIL":
        raise RuntimeError(f"3-tier reconciliation FAILED: {report}")


def _exec_auto_heal() -> None:
    """Kiểm tra và tự động khôi phục Debezium connector / stale queue trước khi đồng bộ."""
    from src.recovery.pipeline_recovery import run_auto_heal

    result = run_auto_heal()
    logging.getLogger("airflow.task").info("Auto-heal result: %s", result)


if HAS_AIRFLOW:
    default_args = {
        "owner": "data_engineering_team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=5),
    }

    with DAG(
        dag_id="c360_near_realtime_sync",
        default_args=default_args,
        description="Đồng bộ Near Real-time mỗi phút từ C360_SYNC_QUEUE sang C360_THONG_TIN_CA_NHAN",
        schedule="* * * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["bhxh", "c360", "near_realtime"],
    ) as dag_near_realtime:

        auto_heal_connectors = PythonOperator(
            task_id="auto_heal_connectors",
            python_callable=_exec_auto_heal,
        )

        run_near_realtime_batch = PythonOperator(
            task_id="run_near_realtime_batch",
            python_callable=_exec_near_realtime_batch,
        )

        auto_heal_connectors >> run_near_realtime_batch

    with DAG(
        dag_id="c360_daily_map_and_reconcile",
        default_args={**default_args, "execution_timeout": timedelta(minutes=30)},
        description="Làm mới bảng Map DBHC (Tỉnh/Xã) và chạy đối soát tự động 3 tầng Nguồn <-> Mirror <-> C360",
        schedule="0 2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["bhxh", "c360", "reconciliation"],
    ) as dag_daily:

        refresh_dbhc_maps = PythonOperator(
            task_id="refresh_dbhc_maps_atomic",
            python_callable=_exec_refresh_dbhc_maps,
        )

        run_reconciliation = PythonOperator(
            task_id="run_3tier_reconciliation",
            python_callable=_exec_reconciliation,
        )

        refresh_dbhc_maps >> run_reconciliation


def run_local_pipeline_cycle(
    refresh_maps: bool = True,
    reconcile: bool = True,
    auto_heal: bool = False,
) -> None:
    """Chạy mô phỏng 1 chu kỳ điều phối trực tiếp trên Oracle khi không có Airflow."""
    import oracledb
    from src.config import TARGET_DB
    from src.reconciliation.reconcile_pipeline import run_reconciliation

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("c360_local_scheduler")

    if auto_heal:
        from src.recovery.pipeline_recovery import run_auto_heal

        heal_res = run_auto_heal()
        logger.info("[Task 0] Auto-heal completed: %s", heal_res)

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as conn:
        with conn.cursor() as cur:
            if refresh_maps:
                cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS; END;")
                logger.info("[Task 1] PRC_REFRESH_DBHC_MAPS completed.")

            cur.execute("SELECT COUNT(*) FROM C360.C360_SYNC_QUEUE")
            pending = int(cur.fetchone()[0])
            cur.execute("SELECT TO_NUMBER(SYS_CONTEXT('USERENV', 'SID')) FROM DUAL")
            sid_row = cur.fetchone()
            oracle_sid = int(sid_row[0]) if sid_row and sid_row[0] is not None else None
            with _SyncLogHeartbeat(run_mode="NEAR_RT", oracle_sid=oracle_sid):
                cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH(p_safety_lag_seconds => 0); END;")
            cur.execute("SELECT COUNT(*) FROM C360.C360_SYNC_QUEUE")
            remaining = int(cur.fetchone()[0])
            logger.info("[Task 2] PRC_RUN_NEAR_REALTIME_BATCH completed (queue: %d -> %d).", pending, remaining)

    if reconcile:
        report = run_reconciliation()
        logger.info("[Task 3] 3-tier reconciliation overall_status=%s", report["overall_status"])


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Local runner cho C360 Airflow DAG (khi chạy trên Windows).")
    parser.add_argument("--loop", action="store_true", help="Chạy vòng lặp Near Real-time liên tục.")
    parser.add_argument("--interval", type=float, default=10.0, help="Số giây nghỉ giữa mỗi vòng --loop (mặc định 10s).")
    parser.add_argument("--skip-reconcile", action="store_true", help="Bỏ qua bước đối soát 3 tầng.")
    parser.add_argument("--auto-heal", action="store_true", help="Bật tự động kiểm tra & khôi phục connector/stale queue.")
    args, _unknown = parser.parse_known_args(argv)

    if not args.loop:
        run_local_pipeline_cycle(refresh_maps=True, reconcile=not args.skip_reconcile, auto_heal=args.auto_heal)
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("Starting local Near Real-time loop (interval=%.1fs). Press Ctrl+C to stop.", args.interval)
    try:
        first = True
        while True:
            try:
                run_local_pipeline_cycle(refresh_maps=first, reconcile=False, auto_heal=args.auto_heal)
                first = False
            except Exception as exc:
                logging.exception("Cycle failed (will retry after %.1fs): %s", args.interval, exc)
            time.sleep(max(1.0, args.interval))
    except KeyboardInterrupt:
        logging.info("Stopped local scheduler.")


if __name__ == "__main__":
    main()
