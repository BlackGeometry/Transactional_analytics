-- =====================================================================================================
-- Считываем все данные из топика lab08_transactions.
-- Один топик содержит три типа событий, различаем по полю _source.
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.kafka_events (
    -- поля транзакций
    transaction_id          Nullable(UInt32),
    user_id                 Nullable(String),
    user_uuid               Nullable(String),
    amount                  Nullable(Int32),
    currency                Nullable(String),
    transaction_type        Nullable(String),
    promo_code_id           Nullable(UInt32),
    status                  Nullable(String),
    created_at              Nullable(UInt32),
    -- поля отмен
    original_transaction_id Nullable(UInt32),
    reason                  Nullable(String),
    cancelled_at            Nullable(String),
    refund_amount           Nullable(Int32),
    -- поля курсов валют
    update_id               Nullable(UInt32),
    timestamp               Nullable(UInt32),
    rate_tgrk_punk          Nullable(Float64),
    rate_tgrk_rub           Nullable(Float64),
    -- общий маркер типа события
    _source                 String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list         = 'your-kafka-host:9092',
    kafka_topic_list          = 'lab08_transactions',
    kafka_group_name          = 'clickhouse_lab08',
    kafka_format              = 'JSONEachRow',
    kafka_skip_broken_messages = 1;

-- =====================================================================================================
-- MV: транзакции из Kafka --> transactions_raw
-- =====================================================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS lab08.kafka_transactions_mv
TO lab08.transactions_raw AS
SELECT
    transaction_id,
    toUInt32OrNull(nullIf(user_id, ''))                              AS user_id,
    ifNull(user_uuid, '')                                            AS user_uuid,
    ifNull(amount, 0)                                                AS amount,
    ifNull(currency, '')                                             AS currency,
    ifNull(transaction_type, '')                                     AS transaction_type,
    promo_code_id,
    ifNull(status, '')                                               AS status,
    toDateTime(ifNull(created_at, 0))                                AS created_at,
    concat('stream_', toString(toDate(toDateTime(ifNull(created_at, 0))))) AS _batch_id,
    'stream_kafka'                                                   AS _source
FROM lab08.kafka_events
WHERE _source = 'transaction'
  AND transaction_id IS NOT NULL;

-- =====================================================================================================
-- MV: отмены из Kafka --> cancellations_raw
-- =====================================================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS lab08.kafka_cancellations_mv
TO lab08.cancellations_raw AS
SELECT
    CAST(NULL, 'Nullable(UInt32)')                                   AS cancellation_id,
    ifNull(original_transaction_id, 0)                               AS original_transaction_id,
    ifNull(reason, '')                                               AS reason,
    parseDateTimeBestEffort(toString(cancelled_at))                  AS cancelled_at,
    ifNull(refund_amount, 0)                                         AS refund_amount,
    toDate(parseDateTimeBestEffort(toString(cancelled_at)))          AS _batch_date,
    'stream_kafka'                                                   AS _source
FROM lab08.kafka_events
WHERE _source = 'cancellation'
  AND original_transaction_id IS NOT NULL
  AND cancelled_at IS NOT NULL;

-- =====================================================================================================
-- MV: курсы валют из Kafka --> exchange_rates_raw
-- =====================================================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS lab08.kafka_rates_mv
TO lab08.exchange_rates_raw AS
SELECT
    update_id,
    toDateTime(ifNull(timestamp, 0))                                AS rate_timestamp,
    ifNull(rate_tgrk_punk, 0)                                       AS rate_tgrk_punk,
    ifNull(rate_tgrk_rub, 0)                                        AS rate_tgrk_rub,
    toDate(toDateTime(ifNull(timestamp, 0)))                        AS _batch_date,
    'stream_kafka'                                                  AS _source
FROM lab08.kafka_events
WHERE _source = 'exchange_rate';
