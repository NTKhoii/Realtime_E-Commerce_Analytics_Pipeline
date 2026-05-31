"""Sanity-check DAG: force a failure to verify the Telegram alert wiring."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from telegram_alert import send_telegram_alert


def deliberately_fail():
    raise RuntimeError("Cố tình lỗi để kiểm tra kênh báo động Telegram.")


default_args = {
    "owner": "data-platform",
    "retries": 0,
    "on_failure_callback": send_telegram_alert,
}

with DAG(
    dag_id="test_telegram_alert",
    description="Manual sanity check that Telegram alerts fire on task failure.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ops", "test"],
) as dag:
    PythonOperator(
        task_id="force_failure",
        python_callable=deliberately_fail,
    )
