"""Watchdog DAG: alerts when the streaming pipeline stops producing rows.

Runs every 5 minutes. Checks the freshest `inserted_at` in fact_product_views
against a configurable threshold; if no new rows arrived in that window, it
fails the task — which triggers the Telegram alert callback.
"""
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

from telegram_alert import send_telegram_alert


DB_CONFIG = {
    "host": os.environ.get("GLAMIRA_DB_HOST", "postgres"),
    "port": os.environ.get("GLAMIRA_DB_PORT", "5432"),
    "user": os.environ.get("GLAMIRA_DB_USER", "admin"),
    "password": os.environ.get("GLAMIRA_DB_PASSWORD", "password123"),
    "dbname": os.environ.get("GLAMIRA_DB_NAME", "glamira_db"),
}
THRESHOLD_MIN = int(os.environ.get("STREAMING_FRESHNESS_THRESHOLD_MINUTES", "10"))


def check_freshness():
    with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(inserted_at), COUNT(*) FROM fact_product_views;")
        last_seen, total = cur.fetchone()

    print(f"Total rows in fact_product_views: {total}")
    print(f"Latest inserted_at: {last_seen}")

    if last_seen is None:
        raise RuntimeError(
            "fact_product_views rỗng — luồng streaming chưa bao giờ ghi data."
        )

    now = datetime.now(timezone.utc)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)

    lag = now - last_seen
    print(f"Độ trễ so với hiện tại: {lag}")

    if lag > timedelta(minutes=THRESHOLD_MIN):
        raise RuntimeError(
            f"Luồng streaming có thể đã CHẾT: không có row mới trong "
            f"{lag.total_seconds() / 60:.1f} phút (ngưỡng {THRESHOLD_MIN} phút)."
        )

    print(f"OK — Luồng streaming còn sống. Lag = {lag.total_seconds():.0f}s.")


default_args = {
    "owner": "data-platform",
    "retries": 0,
    "on_failure_callback": send_telegram_alert,
}

with DAG(
    dag_id="streaming_watchdog",
    description="Cảnh báo Telegram nếu luồng Spark streaming ngừng ghi vào fact_product_views.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ops", "monitoring"],
) as dag:
    PythonOperator(
        task_id="check_fact_freshness",
        python_callable=check_freshness,
    )
