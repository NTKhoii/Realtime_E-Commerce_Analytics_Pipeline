import psycopg2
from psycopg2 import OperationalError

DB_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "user": "admin",
    "password": "password123",
    "dbname": "glamira_db"
}

DDL_QUERIES = """
-- ==========================================
-- 1. TẠO CÁC BẢNG DIMENSION (BẢNG CHIỀU)
-- ==========================================

CREATE TABLE IF NOT EXISTS dim_date (
    sk_date INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    month INT,
    year INT
);

CREATE TABLE IF NOT EXISTS dim_time (
    sk_time INT PRIMARY KEY,
    hour INT,
    minute INT
);

CREATE TABLE IF NOT EXISTS dim_product (
    sk_product VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_store (
    sk_store VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255) NOT NULL,
    domain VARCHAR(255) 
);

CREATE TABLE IF NOT EXISTS dim_location (
    sk_location VARCHAR(255) PRIMARY KEY,
    city_name VARCHAR(255),
    region_name VARCHAR(255),
    country_name VARCHAR(255),
    country_code VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_device (
    sk_device VARCHAR(255) PRIMARY KEY,
    os VARCHAR(100),
    browser VARCHAR(100),
    resolution VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_traffic_source (
    sk_traffic VARCHAR(255) PRIMARY KEY,
    referrer_domain VARCHAR(500),
    traffic_type VARCHAR(100)
);

-- ==========================================
-- 2. TẠO BẢNG FACT
-- ==========================================

CREATE TABLE IF NOT EXISTS fact_product_views (
    view_id VARCHAR(255) PRIMARY KEY,
    sk_date INT REFERENCES dim_date(sk_date),
    sk_time INT REFERENCES dim_time(sk_time),
    sk_product VARCHAR(255) REFERENCES dim_product(sk_product),
    sk_store VARCHAR(255) REFERENCES dim_store(sk_store),
    sk_location VARCHAR(255) REFERENCES dim_location(sk_location),
    sk_device VARCHAR(255) REFERENCES dim_device(sk_device),
    sk_traffic VARCHAR(255) REFERENCES dim_traffic_source(sk_traffic),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Backfill cho database đã tồn tại trước khi có cột inserted_at
ALTER TABLE fact_product_views
    ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_fact_views_inserted_at ON fact_product_views(inserted_at);

-- ==========================================
-- 3. AGGREGATE TABLES (Built by Airflow daily job)
-- ==========================================

CREATE TABLE IF NOT EXISTS agg_daily_views_by_country (
    sk_date INT NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(255),
    view_count BIGINT NOT NULL,
    unique_products BIGINT NOT NULL,
    PRIMARY KEY (sk_date, country_code)
);

CREATE TABLE IF NOT EXISTS agg_daily_top_products (
    sk_date INT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    view_count BIGINT NOT NULL,
    rank INT NOT NULL,
    PRIMARY KEY (sk_date, product_id)
);

CREATE TABLE IF NOT EXISTS agg_daily_traffic_breakdown (
    sk_date INT NOT NULL,
    traffic_type VARCHAR(100) NOT NULL,
    view_count BIGINT NOT NULL,
    PRIMARY KEY (sk_date, traffic_type)
);
"""

def setup_database():
    print("Đang kết nối tới PostgreSQL")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("Đang khởi tạo Star Schema (Dim & Fact tables)")
        cursor.execute(DDL_QUERIES)
        
        print("Khởi tạo Database thành công! Các bảng đã sẵn sàng đón dữ liệu.")
        
    except OperationalError as e:
        print(f"Lỗi kết nối Database: {e}")
    except Exception as e:
        print(f"Lỗi thực thi SQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Đã đóng kết nối Database.")

if __name__ == "__main__":
    setup_database()