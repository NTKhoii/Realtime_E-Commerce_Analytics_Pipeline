.PHONY: up down setup-db run bash

up:
	cd infrastructure && docker compose up -d --build

down:
	cd infrastructure && docker compose down

# Thêm lệnh này để "Xóa sổ" toàn bộ data cũ, làm lại từ đầu
reset:
	cd infrastructure && docker compose down -v

setup-db:
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/scripts/setup_postgres.py"

preview-data:
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/scripts/preview_data.py"

setup-hdfs:
	docker exec -it namenode hdfs dfs -mkdir -p /tmp/checkpoints
	docker exec -it namenode hdfs dfs -chmod -R 777 /tmp

run:
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/jobs/product_view_stream.py"

bash:
	docker exec -it spark-client /bin/bash