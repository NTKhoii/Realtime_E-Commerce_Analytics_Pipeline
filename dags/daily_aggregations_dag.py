import os
import sys
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# Fix lỗi không tìm thấy module telegram_alert
sys.path.append('/opt/airflow/plugins')
from telegram_alert import send_telegram_alert

DB_CONFIG = {
    "host": os.environ.get("GLAMIRA_DB_HOST", "postgres"),
    "port": os.environ.get("GLAMIRA_DB_PORT", "5432"),
    "user": os.environ.get("GLAMIRA_DB_USER", "admin"),
    "password": os.environ.get("GLAMIRA_DB_PASSWORD", "password123"),
    "dbname": os.environ.get("GLAMIRA_DB_NAME", "glamira_db"),
}

TOP_N_PRODUCTS = 50
SQL_DIR = "/opt/airflow/dags/sql"

def _sk_date_for(ds: str) -> int:
    return int(datetime.strptime(ds, "%Y-%m-%d").strftime("%Y%m%d"))

def execute_sql_file(file_name: str, ds: str, **kwargs):
    """Đọc file SQL và thực thi"""
    sk_date = _sk_date_for(ds)
    params = {"sk_date": sk_date, "top_n": TOP_N_PRODUCTS}
    
    file_path = os.path.join(SQL_DIR, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        sql_query = f.read()

    print(f"Đang chạy {file_name} cho sk_date={sk_date} ({ds})...")
    with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute(sql_query, params)
        print(f"Hoàn thành {file_name}: {cur.rowcount} dòng được UPSERT.")
        conn.commit()

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

    # Task 1: Gom Quốc Gia
    task_country = PythonOperator(
        task_id="agg_views_by_country",
        python_callable=execute_sql_file,
        op_kwargs={"file_name": "views_by_country.sql"}
    )

    # Task 2: Gom Sản Phẩm HOT
    task_products = PythonOperator(
        task_id="agg_top_products",
        python_callable=execute_sql_file,
        op_kwargs={"file_name": "top_products.sql"}
    )

    # Task 3: Gom Traffic Nguồn
    task_traffic = PythonOperator(
        task_id="agg_traffic_breakdown",
        python_callable=execute_sql_file,
        op_kwargs={"file_name": "traffic_breakdown.sql"}
    )

    # Dòng định tuyến: Khai báo 3 Task này nằm trong ngoặc vuông để chạy SONG SONG cùng 1 lúc!
    [task_country, task_products, task_traffic]