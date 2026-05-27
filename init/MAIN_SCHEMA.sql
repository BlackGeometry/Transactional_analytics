-- =====================================================================================================
-- Созданем БД 
-- =====================================================================================================

CREATE DATABASE IF NOT EXISTS lab08;

-- =====================================================================================================
-- Основная таблица для хранения сырых транзакций из S3 (батч) и Kafka (стрим, позже)
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.transactions_raw (
    transaction_id   UInt32,
    user_id          Nullable(UInt32),        -- NULL если guest (пустая строка в источнике)
    user_uuid        String,
    amount           Int32,                   
    currency         LowCardinality(String),  -- TGRK / PUNK / RUB
    transaction_type LowCardinality(String),  -- purchase / transfer / refund
    promo_code_id    Nullable(UInt32),
    status           LowCardinality(String),  -- completed / failed
    created_at       DateTime,
    _batch_id        String,                  -- ключ идемпотентности
    _source          LowCardinality(String),  -- Источник: batch_s3 / stream_kafka
    _loaded_at       DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (toDate(created_at), transaction_id);

-- =====================================================================================================
-- Таблица для отмен (батч — ежедневно; стрим — через Kafka, позже)
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.cancellations_raw (
    cancellation_id         Nullable(UInt32),       
    original_transaction_id UInt32,
    reason                  LowCardinality(String),
    cancelled_at            DateTime,               
    refund_amount           Int32,            
    _batch_date             Date,                   -- ключ идемпотентности
    _source                 LowCardinality(String), -- Источник: batch_s3 / stream_kafka
    _loaded_at              DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(cancelled_at)
ORDER BY (original_transaction_id);

-- =====================================================================================================
-- Курсы валют (2-3 обновления курса в день. Базовая валюта — TGRK, дополнительные — PUNK и RUB)
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.exchange_rates_raw (
    update_id       Nullable(UInt32),
    rate_timestamp  DateTime,
    rate_tgrk_punk  Float64,
    rate_tgrk_rub   Float64,
    _batch_date     Date,                   -- ключ идемпотентности
    _source         LowCardinality(String), -- Источник: batch_s3 / stream_kafka
    _loaded_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY rate_timestamp;
-- =====================================================================================================
-- Справочники (загружаются один раз)
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.users (
    user_id      UInt32,
    user_uuid    String,
    is_test_user UInt8   -- 0 / 1
) ENGINE = ReplacingMergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS lab08.test_users (
    test_user_uuid String
) ENGINE = ReplacingMergeTree()
ORDER BY test_user_uuid;

CREATE TABLE IF NOT EXISTS lab08.promo_codes (
    promo_code_id UInt32,
    code          String,
    max_uses      UInt32,
    expiry_date   Date
) ENGINE = ReplacingMergeTree()
ORDER BY promo_code_id;
