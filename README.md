# Realtime E-Commerce Analytics Pipeline

Pipeline streaming thời gian thực xử lý dữ liệu product views từ Glamira, chuyển đổi và lưu vào Data Mart theo mô hình Star Schema, hiển thị trên Metabase dashboard.

## Kiến trúc tổng quan

```
Kafka (SASL) ──► Spark Structured Streaming ──► PostgreSQL (Star Schema) ──► Metabase
                        │                              │
                   HDFS (checkpoint)             Airflow DAGs
                                                  ├─ streaming_watchdog   (giám sát)
                                                  ├─ start_streaming_job  (khởi chạy)
                                                  ├─ daily_aggregations   (tổng hợp)
                                                  └─ test_telegram_alert  (sanity check)
```

**Thành phần chính:**

| Service               | Port | Mô tả                                            |
|-----------------------|------|--------------------------------------------------|
| HDFS NameNode         | 9870 | Lưu checkpoint cho Spark streaming               |
| YARN ResourceManager  | 8088 | Quản lý tài nguyên Hadoop cluster                |
| Spark Client          | 4040 | Chạy streaming job (Spark UI)                    |
| PostgreSQL            | 5432 | Data Mart — Star Schema                          |
| Metabase              | 3000 | Dashboard trực quan hóa                          |
| Airflow Webserver     | 8080 | Orchestration & monitoring (login: admin/admin)  |

## Yêu cầu

- Docker & Docker Compose v2
- GNU Make
- File `data/reference/IP-COUNTRY-REGION-CITY.BIN` (IP2Location DB) — đặt trước khi `make run`
- (Tuỳ chọn) Telegram Bot Token để nhận cảnh báo

## Cấu hình

Tạo `.env` ở thư mục gốc:

```bash
KAFKA_PASSWORD=<Kafka SASL password>
POSTGRES_PASSWORD=password123
```

Kafka brokers và topic được cấu hình trong [config/spark.conf](config/spark.conf).

## Khởi chạy nhanh

```bash
# 1. Khởi động core stack (Hadoop + Spark + Postgres + Metabase)
make up

# 2. Tạo Star Schema + bảng aggregate
make setup-db
make setup-hdfs

# 3. Chạy streaming job trực tiếp (foreground)
make run
```

Mở Metabase tại http://localhost:3000 để xây dashboard, hoặc Spark UI tại http://localhost:4040 để theo dõi micro-batch.

## Sử dụng Airflow để orchestrate

```bash
# Khởi động Airflow (compose riêng, share streaming-network)
make airflow-up

# Cấu hình Telegram connections cho cảnh báo lỗi
export TELEGRAM_BOT_TOKEN=<token>
export TELEGRAM_CHAT_ID=<chat_id>
make airflow-conns
```

Truy cập http://localhost:8080 (admin/admin), bật các DAG:

| DAG | Lịch chạy | Vai trò |
|-----|-----------|---------|
| `start_streaming_job`  | Manual trigger | Chạy `product_view_stream.py` trong `spark-client` qua docker socket. Task chỉ kết thúc khi streaming query dừng. |
| `streaming_watchdog`   | `*/5 * * * *`  | Quét `fact_product_views.inserted_at`. Nếu không có row mới trong 10 phút (`STREAMING_FRESHNESS_THRESHOLD_MINUTES`) → fail task → bắn cảnh báo Telegram. |
| `daily_aggregations`   | `15 1 * * *`   | Roll-up dữ liệu theo ngày sang `agg_daily_views_by_country`, `agg_daily_top_products`, `agg_daily_traffic_breakdown` để Metabase query nhanh. |
| `test_telegram_alert`  | Manual trigger | Cố tình fail để verify kênh Telegram. |

## Cấu trúc dữ liệu

**Fact:** `fact_product_views` (view_id, sk_date, sk_time, sk_product, sk_store, sk_location, sk_device, sk_traffic, inserted_at)

**Dimensions:** `dim_date`, `dim_time`, `dim_product`, `dim_store`, `dim_location`, `dim_device`, `dim_traffic_source`

**Aggregate (build hàng ngày bởi Airflow):**
- `agg_daily_views_by_country`
- `agg_daily_top_products` (Top 50 / ngày)
- `agg_daily_traffic_breakdown`

## Bố cục thư mục

```
.
├── config/                  # spark.conf, log4j.properties
├── dags/                    # Airflow DAGs
├── data/reference/          # IP2Location .BIN (gitignored)
├── infrastructure/
│   ├── compose.yml          # Hadoop + Spark + Postgres + Metabase
│   ├── Dockerfile           # Image my-hadoop-cluster:3.3.6
│   ├── conf/                # core/hdfs/yarn/mapred-site.xml
│   ├── spark-client/        # Image client cho Spark
│   ├── init-db/             # SQL init khi tạo Postgres lần đầu
│   └── airflow/             # Compose Airflow riêng
├── plugins/telegram_alert.py
├── src/
│   ├── jobs/product_view_stream.py    # Spark streaming job chính
│   ├── scripts/
│   │   ├── setup_postgres.py          # DDL Star Schema + aggregates
│   │   └── preview_data.py
│   └── utils/                         # config / db / geo / transform helpers
└── Makefile
```

## Lệnh Make hữu dụng

```bash
make help            # liệt kê toàn bộ target
make bash            # shell vào spark-client
make preview-data    # xem nhanh data trong Postgres
make logs            # tail compose logs
make down            # stop services (giữ volume)
make reset           # stop + xoá volume (mất data)
make airflow-down    # stop Airflow
```

## Troubleshooting

**`failOnDataLoss` warnings từ Kafka** — đã set `failOnDataLoss=false` trong streaming job để tránh chết khi offset bị compact.

**HDFS safe mode** — `make run` tự gọi `dfsadmin -safemode leave` trước. Nếu vẫn safe mode, đợi DataNode register hoặc gọi tay.

**Watchdog báo động liên tục** — kiểm tra:
1. `start_streaming_job` DAG có đang chạy?
2. Spark UI (`localhost:4040`) — có streaming query active không?
3. Connection `Kafka` còn sống?

**Airflow không gọi được docker socket** — chắc chắn `/var/run/docker.sock` mount-able từ host (user có quyền `docker` group).
