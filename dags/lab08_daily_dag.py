"""
Ежедневный DAG: справочники + отмены + курсы валют.
"""
from datetime import datetime, timedelta, timezone

import clickhouse_connect
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

S3_BASE = "https://storage.yandexcloud.net/npl-de18-lab8-data"


def _get_ch():
    conn = BaseHook.get_connection("clickhouse_lab08")
    return clickhouse_connect.get_client(host=conn.host, port=conn.port or 8123)


def _on_failure(context):
    # Отправляем сообщение в Slack при падении.
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


def load_references(**_):
    ch = _get_ch()

    if ch.query("SELECT count() FROM lab08.users").first_row[0] == 0:
        ch.command(f"""
            INSERT INTO lab08.users
            SELECT toUInt32(user_id), toString(user_uuid), toUInt8(is_test_user)
            FROM url('{S3_BASE}/reference/users.jsonl', 'JSONEachRow',
                     'user_id UInt32, user_uuid String, is_test_user UInt8')
        """)

    if ch.query("SELECT count() FROM lab08.test_users").first_row[0] == 0:
        ch.command(f"""
            INSERT INTO lab08.test_users
            SELECT toString(test_user_uuid)
            FROM url('{S3_BASE}/reference/test_users.jsonl', 'JSONEachRow',
                     'test_user_uuid String')
        """)

    if ch.query("SELECT count() FROM lab08.promo_codes").first_row[0] == 0:
        ch.command(f"""
            INSERT INTO lab08.promo_codes
            SELECT toUInt32(promo_code_id), code, toUInt32(max_uses), toDate(expiry_date)
            FROM url('{S3_BASE}/reference/promo_codes.jsonl', 'JSONEachRow',
                     'promo_code_id UInt32, code String, max_uses UInt32, expiry_date String')
        """)


def load_cancellations(**context):
    batch_date = context["data_interval_start"].strftime("%Y-%m-%d")
    url = f"{S3_BASE}/cancellations/day={batch_date}/cancellations.jsonl"
    ch = _get_ch()

    if ch.query(
        "SELECT count() FROM lab08.cancellations_raw WHERE _batch_date = %(d)s",
        parameters={"d": batch_date},
    ).first_row[0]:
        return

    if requests.head(url, timeout=10).status_code != 200:
        return

    ch.command(f"""
        INSERT INTO lab08.cancellations_raw
            (cancellation_id, original_transaction_id, reason, cancelled_at, refund_amount, _batch_date, _source)
        SELECT
            toUInt32OrNull(toString(cancellation_id))  AS cancellation_id,
            toUInt32(original_transaction_id)           AS original_transaction_id,
            reason,
            parseDateTimeBestEffort(cancelled_at)       AS cancelled_at,
            toInt32(refund_amount)                      AS refund_amount,
            toDate('{batch_date}')                      AS _batch_date,
            'batch_s3'                                  AS _source
        FROM url('{url}', 'JSONEachRow',
                 'cancellation_id UInt32, original_transaction_id UInt32,
                  reason String, cancelled_at String, refund_amount Int32')
    """)


def load_exchange_rates(**context):
    batch_date = context["data_interval_start"].strftime("%Y-%m-%d")
    url = f"{S3_BASE}/exchange_rates/day={batch_date}/rates.jsonl"
    ch = _get_ch()

    if ch.query(
        "SELECT count() FROM lab08.exchange_rates_raw WHERE _batch_date = %(d)s",
        parameters={"d": batch_date},
    ).first_row[0]:
        return

    if requests.head(url, timeout=10).status_code != 200:
        return

    ch.command(f"""
        INSERT INTO lab08.exchange_rates_raw
            (update_id, rate_timestamp, rate_tgrk_punk, rate_tgrk_rub, _batch_date, _source)
        SELECT
            toUInt32OrNull(toString(update_id)) AS update_id,
            toDateTime(timestamp)               AS rate_timestamp,
            rate_tgrk_punk,
            rate_tgrk_rub,
            toDate('{batch_date}')              AS _batch_date,
            'batch_s3'                          AS _source
        FROM url('{url}', 'JSONEachRow',
                 'update_id UInt32, timestamp UInt32,
                  rate_tgrk_punk Float64, rate_tgrk_rub Float64')
    """)


with DAG(
    dag_id="lab08_daily",
    start_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["lab08"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _on_failure,
    },
) as dag:
    t_refs = PythonOperator(task_id="load_references", python_callable=load_references)
    t_cancel = PythonOperator(task_id="load_cancellations", python_callable=load_cancellations)
    t_rates = PythonOperator(task_id="load_exchange_rates", python_callable=load_exchange_rates)

    t_refs >> [t_cancel, t_rates]
