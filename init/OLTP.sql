-- =====================================================================================================
-- Обновляется автоматически из users каждые 5-6 мин.
-- =====================================================================================================
CREATE DICTIONARY IF NOT EXISTS lab08.users_dict (
    user_id      UInt32,
    user_uuid    String,
    is_test_user UInt8
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(TABLE 'users' DB 'lab08'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

-- =====================================================================================================
-- Чистый слой транзакций.
-- Запросы к этой таблице используют FINAL для гарантированного дедупа.
-- =====================================================================================================
CREATE TABLE IF NOT EXISTS lab08.transactions_clean (
    transaction_id   UInt32,
    user_id          UInt32,               -- NOT NULL: отфильтровано в MV
    user_uuid        String,
    amount           Int32,
    currency         LowCardinality(String),
    transaction_type LowCardinality(String),
    promo_code_id    Nullable(UInt32),
    status           LowCardinality(String),
    created_at       DateTime,
    _source          LowCardinality(String),
    _loaded_at       DateTime
) ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY transaction_id;

-- =====================================================================================================
-- MV: общие правила очистки, применяются при каждом INSERT в transactions_raw.
-- =====================================================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS lab08.transactions_clean_mv
TO lab08.transactions_clean AS
SELECT
    transaction_id,
    assumeNotNull(user_id)  AS user_id,   -- safe: user_id IS NOT NULL в WHERE
    user_uuid,
    amount,
    currency,
    transaction_type,
    promo_code_id,
    status,
    created_at,
    _source,
    _loaded_at
FROM lab08.transactions_raw
WHERE
    -- все ключевые поля заполнены
    user_id IS NOT NULL
    AND user_uuid != ''
    AND currency   != ''
    AND transaction_type != ''
    AND status     != ''
    AND amount != 0
    -- пользователь должен существовать в справочнике
    AND dictHas('lab08.users_dict', assumeNotNull(user_id))
    -- тестовые пользователи исключены из аналитических витрин
    AND dictGet('lab08.users_dict', 'is_test_user', assumeNotNull(user_id)) = 0;
