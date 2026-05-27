"""
Загружает один 10-минутный слот транзакций из S3.
"""
from datetime import datetime, timedelta, timezone

import clickhouse_connect
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

S3_BASE = "https://storage.yandexcloud.net/npl-de18-lab8-data"

TRANSACTIONS_SCHEMA = (
    "transaction_id UInt32, user_id String, user_uuid String, "
    "amount Int32, currency String, transaction_type String, "
    "promo_code_id String, status String, created_at UInt32"
)


def _get_ch():
    conn = BaseHook.get_connection("clickhouse_lab08")
    return clickhouse_connect.get_client(host=conn.host, port=conn.port or 8123)


def _on_failure(context):
    #Отправляет сообщение в Slack при падении таска.
    try:
        webhook_url = Variable.get("slack_webhook_url", default_var="")
        if not webhook_url:
            return
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        exec_date = str(context["data_interval_start"])[:19]
        requests.post(
            webhook_url,
            json={"text": f":red_circle: *{dag_id}* / `{task_id}` failed | `{exec_date}`"},
            timeout=5,
        )
    except Exception:
        pass  


def load_slot(**context):
    dt = context["data_interval_start"]
    slot_date = dt.strftime("%Y-%m-%d")
    slot_time = dt.strftime("%H-%M")
    batch_id = f"{slot_date}_{slot_time}"
    url = f"{S3_BASE}/day={slot_date}/slot={slot_time}/transactions.jsonl"

    ch = _get_ch()

    if ch.query(
        "SELECT count() FROM lab08.transactions_raw WHERE _batch_id = %(b)s",
        parameters={"b": batch_id},
    ).first_row[0]:
        return

    if requests.head(url, timeout=10).status_code != 200:
        return

    ch.command(f"""
        INSERT INTO lab08.transactions_raw
            (transaction_id, user_id, user_uuid, amount, currency, transaction_type, promo_code_id, status, created_at, _batch_id, _source)
        SELECT
            transaction_id,
            toUInt32OrNull(nullIf(user_id, ''))       AS user_id,
            user_uuid,
            amount,
            currency,
            transaction_type,
            toUInt32OrNull(nullIf(promo_code_id, '')) AS promo_code_id,
            status,
            toDateTime(created_at)                     AS created_at,
            '{batch_id}'                               AS _batch_id,
            'batch_s3'                                 AS _source
        FROM url('{url}', 'JSONEachRow', '{TRANSACTIONS_SCHEMA}')
    """)


with DAG(
    dag_id="lab08_transactions",
    start_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    schedule_interval="*/10 * * * *",
    catchup=True,
    max_active_runs=4,
    tags=["lab08"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _on_failure,
    },
) as dag:
    PythonOperator(task_id="load_slot", python_callable=load_slot)
