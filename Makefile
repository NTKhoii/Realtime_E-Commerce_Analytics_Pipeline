.PHONY: up down setup-db run bash

up:
	cd infrastructure && docker compose up -d

down:
	cd infrastructure && docker compose down 

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
	docker exec -it namenode hdfs dfsadmin -safemode leave || true
	docker exec -it spark-client bash -c "cd /workspace && PYTHONPATH=/workspace python src/jobs/product_view_stream.py"

bash:
	docker exec -it spark-client /bin/bash

# http://localhost:9870/dfshealth.html#tab-overview
# http://localhost:8088/cluster
# http://localhost:4040/StreamingQuery/
# http://localhost:3000/dashboard/2-real-time-analytics-of-glamira-users