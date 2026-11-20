"""Cho phép chạy `python -m airflow.dags` hoặc `python -m airflow.dags dag_hgd_c360_pipeline`."""
from airflow.dags.dag_hgd_c360_pipeline import main

if __name__ == "__main__":
    main()
