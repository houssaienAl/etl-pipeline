"""Airflow failure callback: always logs, and posts to a webhook when configured."""
import logging
import os

import requests

log = logging.getLogger(__name__)


def notify_failure(context):
    ti = context["task_instance"]
    message = (
        f":red_circle: Airflow task failed\n"
        f"DAG: {ti.dag_id} | Task: {ti.task_id} | Run: {context.get('run_id')}\n"
        f"Error: {context.get('exception')}\nLogs: {ti.log_url}"
    )
    log.error(message)
    url = os.getenv("ALERT_WEBHOOK_URL")  # Slack-compatible incoming webhook
    if url:
        try:
            requests.post(url, json={"text": message}, timeout=10).raise_for_status()
        except requests.RequestException:
            log.exception("Could not deliver failure alert")
