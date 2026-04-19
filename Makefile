.PHONY: up down setup-db run clean bash

# Bật toàn bộ hệ thống
up:
	cd infrastructure && docker compose up -d --build

# Tắt toàn bộ hệ thống
down:
	cd infrastructure && docker compose down

# Chui vào bên trong container Spark để gõ lệnh (rất tiện khi muốn test code tay)
bash:
	docker exec -it spark-client /bin/bash

# Chạy file Python tạo bảng Postgres từ bên trong container Spark
setup-db:
	docker exec -it spark-client bash -c "cd /workspace && uv run python src/scripts/setup_postgres.py"

# Chạy luồng Spark Streaming từ bên trong container Spark
run:
	docker exec -it spark-client bash -c "cd /workspace && uv run python src/jobs/product_view_stream.py"

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache