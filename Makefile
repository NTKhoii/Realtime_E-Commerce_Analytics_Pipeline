.PHONY: up down reset setup-db setup-hdfs preview-data run bash \
        airflow-up airflow-down airflow-reset airflow-conns logs help

# ==========================================
# Core streaming stack (Hadoop + Spark + Postgres + Metabase)
# ==========================================
up:
	cd infrastructure && docker compose up -d

down:
	cd infrastructure && docker compose down

reset:
	cd infrastructure && docker compose down -v

setup-db:
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/scripts/setup_postgres.py"

setup-hdfs:
	docker exec -it namenode hdfs dfs -mkdir -p /tmp/checkpoints
	docker exec -it namenode hdfs dfs -chmod -R 777 /tmp

preview-data:
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/scripts/preview_data.py"

run:
	docker exec -it namenode hdfs dfsadmin -safemode leave || true
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/jobs/product_view_stream.py"

bash:
	docker exec -it spark-client /bin/bash

# ==========================================
# Airflow (separate compose, shares streaming-network)
# ==========================================
airflow-up:
	cd infrastructure/airflow && docker compose up -d

airflow-down:
	cd infrastructure/airflow && docker compose down

airflow-reset:
	cd infrastructure/airflow && docker compose down -v

# Tạo các connection Telegram cho plugin send_telegram_alert.
# Yêu cầu: export TELEGRAM_BOT_TOKEN=<token>; export TELEGRAM_CHAT_ID=<chat_id>
airflow-conns:
	@test -n "$$TELEGRAM_BOT_TOKEN" || (echo "ERROR: export TELEGRAM_BOT_TOKEN trước." && exit 1)
	@test -n "$$TELEGRAM_CHAT_ID"   || (echo "ERROR: export TELEGRAM_CHAT_ID trước."   && exit 1)
	docker exec airflow-webserver airflow connections delete telegram_alert_bot || true
	docker exec airflow-webserver airflow connections delete telegram_chat_id   || true
	docker exec airflow-webserver airflow connections add telegram_alert_bot \
	    --conn-type http \
	    --conn-host "https://api.telegram.org/bot$$TELEGRAM_BOT_TOKEN/sendMessage"
	docker exec airflow-webserver airflow connections add telegram_chat_id \
	    --conn-type http \
	    --conn-host "$$TELEGRAM_CHAT_ID"

logs:
	cd infrastructure && docker compose logs -f --tail=100

help:
	@echo "Core stack:       make up | down | reset"
	@echo "Bootstrap:        make setup-db | setup-hdfs"
	@echo "Streaming:        make run            (or trigger 'start_streaming_job' DAG)"
	@echo "Inspect:          make preview-data | bash | logs"
	@echo "Airflow:          make airflow-up | airflow-down | airflow-reset"
	@echo "Telegram:         TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... make airflow-conns"
	@echo ""
	@echo "URLs:"
	@echo "  HDFS NameNode      http://localhost:9870"
	@echo "  YARN ResourceMgr   http://localhost:8088"
	@echo "  Spark UI           http://localhost:4040"
	@echo "  Metabase           http://localhost:3000"
	@echo "  Airflow            http://localhost:8080  (admin/admin)"
