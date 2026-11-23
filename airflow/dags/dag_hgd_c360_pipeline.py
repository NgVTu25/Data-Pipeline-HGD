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


def _exec_near_realtime_batch() -> None:
    """Thực thi thực tế procedure C360.PKG_C360_SYNC.PRC_RUN_NEAR_REALTIME_BATCH trên Oracle."""
    import oracledb
    from src.config import TARGET_DB

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as conn:
        with conn.cursor() as cur:
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

        run_near_realtime_batch = PythonOperator(
            task_id="run_near_realtime_batch",
            python_callable=_exec_near_realtime_batch,
        )

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


def run_local_pipeline_cycle(refresh_maps: bool = True, reconcile: bool = True) -> None:
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

    with oracledb.connect(user=TARGET_DB.user, password=TARGET_DB.password, dsn=TARGET_DB.dsn) as conn:
        with conn.cursor() as cur:
            if refresh_maps:
                cur.execute("BEGIN C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS; END;")
                logger.info("[Task 1] PRC_REFRESH_DBHC_MAPS completed.")

            cur.execute("SELECT COUNT(*) FROM C360.C360_SYNC_QUEUE")
            pending = int(cur.fetchone()[0])
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
    args, _unknown = parser.parse_known_args(argv)

    if not args.loop:
        run_local_pipeline_cycle(refresh_maps=True, reconcile=not args.skip_reconcile)
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.info("Starting local Near Real-time loop (interval=%.1fs). Press Ctrl+C to stop.", args.interval)
    try:
        first = True
        while True:
            run_local_pipeline_cycle(refresh_maps=first, reconcile=False)
            first = False
            time.sleep(max(1.0, args.interval))
    except KeyboardInterrupt:
        logging.info("Stopped local scheduler.")


if __name__ == "__main__":
    main()
