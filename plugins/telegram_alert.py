"""Telegram alert callback for Airflow tasks.

Connections expected:
  telegram_alert_bot  — Host = https://api.telegram.org/bot<TOKEN>/sendMessage
  telegram_chat_id    — Host = <chat_id>   (e.g. -5153204772)

Wire it in a DAG via: default_args={"on_failure_callback": send_telegram_alert}
"""
import os

import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook


def _chat_id() -> str:
    try:
        return BaseHook.get_connection("telegram_chat_id").host
    except AirflowNotFoundException:
        env = os.environ.get("TELEGRAM_CHAT_ID")
        if not env:
            raise
        return env


def send_telegram_alert(context):
    """Triggered by Airflow when a task fails (on_failure_callback)."""
    try:
        bot_url = BaseHook.get_connection("telegram_alert_bot").host
        chat_id = _chat_id()
    except AirflowNotFoundException as e:
        print(f"[telegram_alert] Bỏ qua: chưa cấu hình connection ({e}).")
        return

    ti = context.get("task_instance")
    when = context.get("logical_date") or context.get("execution_date")
    when_str = when.strftime("%Y-%m-%d %H:%M:%S") if when else "unknown"

    msg = (
        "🚨 *BÁO ĐỘNG HỆ THỐNG STREAMING GLAMIRA* 🚨\n"
        f"- *DAG:* `{ti.dag_id}`\n"
        f"- *Task:* `{ti.task_id}`\n"
        f"- *Thời điểm:* {when_str}\n"
        f"- *Log:* [Mở log chi tiết]({ti.log_url})"
    )

    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}

    try:
        r = requests.post(bot_url, json=payload, timeout=10)
        r.raise_for_status()
        print("[telegram_alert] Đã gửi cảnh báo Telegram thành công!")
    except Exception as e:
        print(f"[telegram_alert] Lỗi khi gửi: {e}")
