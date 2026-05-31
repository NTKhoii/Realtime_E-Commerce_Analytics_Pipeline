"""Manual DAG to launch the Spark streaming job inside the spark-client container.

Trigger this once after `make up` + `make setup-db` to start ingesting. The DAG
runs `python src/jobs/product_view_stream.py` inside the spark-client container
(via the Docker socket mounted into Airflow workers) and streams its logs into
the Airflow task log. The task only exits when the streaming query stops.
"""
from datetime import datetime

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator

from telegram_alert import send_telegram_alert


CONTAINER = "spark-client"
WORKDIR = "/workspace"
COMMAND = ["bash", "-lc", "PYTHONPATH=/workspace python src/jobs/product_view_stream.py"]


def launch_streaming_job():
    client = docker.from_env()
    container = client.containers.get(CONTAINER)
    print(f"Đang chạy lệnh trong container '{CONTAINER}': {COMMAND[-1]}")

    exec_id = client.api.exec_create(
        container.id,
        cmd=COMMAND,
        workdir=WORKDIR,
        stdout=True,
        stderr=True,
        tty=False,
    )["Id"]

    stream = client.api.exec_start(exec_id, stream=True, demux=False)
    for chunk in stream:
        try:
            print(chunk.decode("utf-8", errors="replace"), end="")
        except Exception:
            print(chunk)

    info = client.api.exec_inspect(exec_id)
    exit_code = info.get("ExitCode")
    print(f"\n[Streaming job kết thúc với exit code: {exit_code}]")
    if exit_code not in (0, None):
        raise RuntimeError(f"Streaming job thoát với mã lỗi {exit_code}.")


default_args = {
    "owner": "data-platform",
    "retries": 0,
    "on_failure_callback": send_telegram_alert,
}

with DAG(
    dag_id="start_streaming_job",
    description="Manual launcher: chạy product_view_stream.py trong spark-client.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "manual"],
) as dag:
    PythonOperator(
        task_id="launch_product_view_stream",
        python_callable=launch_streaming_job,
        execution_timeout=None,
    )
