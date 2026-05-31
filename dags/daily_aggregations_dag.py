"""Daily aggregations: roll fact_product_views into report tables for Metabase."""
import os
from datetime import datetime, timedelta

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

TOP_N_PRODUCTS = 50

UPSERT_VIEWS_BY_COUNTRY = """
INSERT INTO agg_daily_views_by_country (sk_date, country_code, country_name, view_count, unique_products)
SELECT
    f.sk_date,
    COALESCE(l.country_code, 'UNKNOWN') AS country_code,
    COALESCE(l.country_name, 'UNKNOWN') AS country_name,
    COUNT(*) AS view_count,
    COUNT(DISTINCT f.sk_product) AS unique_products
FROM fact_product_views f
LEFT JOIN dim_location l ON l.sk_location = f.sk_location
WHERE f.sk_date = %(sk_date)s
GROUP BY f.sk_date, l.country_code, l.country_name
ON CONFLICT (sk_date, country_code) DO UPDATE SET
    country_name = EXCLUDED.country_name,
    view_count = EXCLUDED.view_count,
    unique_products = EXCLUDED.unique_products;
"""

UPSERT_TOP_PRODUCTS = """
INSERT INTO agg_daily_top_products (sk_date, product_id, view_count, rank)
SELECT sk_date, product_id, view_count, rank FROM (
    SELECT
        f.sk_date,
        p.product_id,
        COUNT(*) AS view_count,
        ROW_NUMBER() OVER (PARTITION BY f.sk_date ORDER BY COUNT(*) DESC) AS rank
    FROM fact_product_views f
    JOIN dim_product p ON p.sk_product = f.sk_product
    WHERE f.sk_date = %(sk_date)s
    GROUP BY f.sk_date, p.product_id
) ranked
WHERE rank <= %(top_n)s
ON CONFLICT (sk_date, product_id) DO UPDATE SET
    view_count = EXCLUDED.view_count,
    rank = EXCLUDED.rank;
"""

UPSERT_TRAFFIC_BREAKDOWN = """
INSERT INTO agg_daily_traffic_breakdown (sk_date, traffic_type, view_count)
SELECT
    f.sk_date,
    COALESCE(t.traffic_type, 'UNKNOWN') AS traffic_type,
    COUNT(*) AS view_count
FROM fact_product_views f
LEFT JOIN dim_traffic_source t ON t.sk_traffic = f.sk_traffic
WHERE f.sk_date = %(sk_date)s
GROUP BY f.sk_date, t.traffic_type
ON CONFLICT (sk_date, traffic_type) DO UPDATE SET
    view_count = EXCLUDED.view_count;
"""


def _sk_date_for(ds: str) -> int:
    return int(datetime.strptime(ds, "%Y-%m-%d").strftime("%Y%m%d"))


def build_aggregations(ds: str, **_):
    sk_date = _sk_date_for(ds)
    params = {"sk_date": sk_date, "top_n": TOP_N_PRODUCTS}
    print(f"Đang xây dựng aggregations cho sk_date={sk_date} ({ds})...")
    with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(UPSERT_VIEWS_BY_COUNTRY, params)
        print(f"  agg_daily_views_by_country: {cur.rowcount} rows")
        cur.execute(UPSERT_TOP_PRODUCTS, params)
        print(f"  agg_daily_top_products: {cur.rowcount} rows")
        cur.execute(UPSERT_TRAFFIC_BREAKDOWN, params)
        print(f"  agg_daily_traffic_breakdown: {cur.rowcount} rows")
        conn.commit()
    print(f"Xong aggregations cho ngày {ds}.")


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_telegram_alert,
}

with DAG(
    dag_id="daily_aggregations",
    description="Xây dựng bảng tổng hợp hàng ngày từ star schema cho Metabase.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="15 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "analytics"],
) as dag:
    PythonOperator(
        task_id="build_daily_aggregations",
        python_callable=build_aggregations,
    )
