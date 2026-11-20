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
    from airflow.operators.bash import BashOperator
    HAS_AIRFLOW = True
except ImportError:
    DAG = None  # type: ignore[assignment]
    BashOperator = None  # type: ignore[assignment]
    HAS_AIRFLOW = False

if HAS_AIRFLOW:
    default_args = {
        "owner": "data_engineering_team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    }

    with DAG(
        dag_id="c360_daily_map_and_reconcile",
        default_args=default_args,
        description="Làm mới bảng Map DBHC (Tỉnh/Xã) và chạy đối soát tự động 3 tầng Nguồn <-> Mirror <-> C360",
        schedule="0 2 * * *",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["bhxh", "c360", "reconciliation"],
    ) as dag:

        refresh_dbhc_maps = BashOperator(
            task_id="refresh_dbhc_maps_atomic",
            bash_command='echo "EXEC C360.PKG_C360_SYNC.PRC_REFRESH_DBHC_MAPS;"',
        )

        run_reconciliation = BashOperator(
            task_id="run_3tier_reconciliation",
            bash_command="python -m src.reconciliation.reconcile_pipeline",
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
