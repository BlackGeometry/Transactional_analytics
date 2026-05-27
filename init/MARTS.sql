-- =============================================================
-- Распределение транзакций по часам
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_hourly_distribution AS
SELECT
    toStartOfHour(created_at) AS dt,
    transaction_type,
    currency,
    count()                   AS tx_count,
    sum(amount)               AS total_amount
FROM lab08.transactions_clean FINAL
WHERE
    NOT (amount < 0 AND transaction_type != 'transfer')
GROUP BY dt, transaction_type, currency
ORDER BY dt;


-- =============================================================
-- Количество покупок по часам
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_purchases_hourly AS
SELECT
    toStartOfHour(created_at) AS dt,
    currency,
    count()                   AS tx_count,
    sum(amount)               AS total_amount
FROM lab08.transactions_clean FINAL
WHERE
    transaction_type = 'purchase'
    AND status       = 'completed'
    AND amount       > 0
GROUP BY dt, currency
ORDER BY dt;


-- =============================================================
-- Выручка в базовой валюте (TGRK) по дням
--
-- Алгоритм расчёта revenue:
--   purchase   +amount (только если amount > 0)
--   refund     -amount (вычитается из выручки)
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_revenue_daily AS
SELECT
    txs.date,
    sum(txs.tx_count)    AS tx_count,
    round(sum(
        txs.signed_native * multiIf(
            txs.currency = 'TGRK', 1,
            txs.currency = 'RUB',  1.0 / nullIf(r.rate_rub,  0),
            txs.currency = 'PUNK', 1.0 / nullIf(r.rate_punk, 0),
            1
        )
    ), 2)                AS revenue_tgrk
FROM (
    -- агрегируем транзакции по дате и валюте с нужным знаком
    SELECT
        toDate(created_at)                                  AS date,
        currency,
        count()                                             AS tx_count,
        sum(
            multiIf(transaction_type = 'refund', -1, 1) * amount
        )                                                   AS signed_native
    FROM lab08.transactions_clean FINAL
    WHERE
        status = 'completed'
        AND transaction_type IN ('purchase', 'refund')
        AND NOT (transaction_type = 'purchase' AND amount < 0)
    GROUP BY date, currency
) txs
LEFT JOIN (
    -- для каждой даты берём последний курс
    SELECT
        d.date                                                                     AS rate_date,
        argMaxIf(r.rate_tgrk_punk, r.rate_timestamp, toDate(r.rate_timestamp) <= d.date) AS rate_punk,
        argMaxIf(r.rate_tgrk_rub,  r.rate_timestamp, toDate(r.rate_timestamp) <= d.date) AS rate_rub
    FROM (SELECT DISTINCT toDate(created_at) AS date FROM lab08.transactions_clean FINAL) d
    CROSS JOIN lab08.exchange_rates_raw r
    GROUP BY d.date
) r ON txs.date = r.rate_date
GROUP BY txs.date
ORDER BY txs.date;


-- =============================================================
-- Анализ промокодов
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_promo_analysis AS
SELECT
    p.promo_code_id,
    p.code,
    p.max_uses,
    p.expiry_date,
    count(t.transaction_id)                                 AS actual_uses,
    round(count(t.transaction_id) * 100.0 / p.max_uses, 1)  AS usage_pct, 
    p.expiry_date < today()                                 AS is_expired,
    count(t.transaction_id) > p.max_uses                    AS is_over_limit
FROM lab08.promo_codes p
LEFT JOIN (
    SELECT promo_code_id, transaction_id
    FROM lab08.transactions_clean FINAL
    WHERE promo_code_id IS NOT NULL
      AND status = 'completed'
) t ON p.promo_code_id = t.promo_code_id
GROUP BY p.promo_code_id, p.code, p.max_uses, p.expiry_date
ORDER BY actual_uses DESC;


-- =============================================================
-- Отмены: процент, причины, среднее время до отмены
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_cancellations AS
SELECT
    toDate(c.cancelled_at)                                  AS date,
    c.reason,
    count()                                                 AS cancel_count,
    sum(c.refund_amount)                                    AS total_refund,
    round(avgIf(
        dateDiff('hour', t.created_at, c.cancelled_at),
        t.created_at IS NOT NULL
        AND t.created_at > toDateTime(0)
        AND c.cancelled_at >= t.created_at
    ), 1)                                                   AS avg_hours_to_cancel
FROM lab08.cancellations_raw c
LEFT JOIN (
    SELECT transaction_id, created_at
    FROM lab08.transactions_clean FINAL
) t ON c.original_transaction_id = t.transaction_id
GROUP BY date, reason
ORDER BY date, cancel_count DESC;


-- =============================================================
-- Тестовые vs реальные пользователи
-- =============================================================
CREATE VIEW IF NOT EXISTS lab08.mart_test_vs_real AS
SELECT
    toDate(t.created_at)                                                        AS date,
    dictGet('lab08.users_dict', 'is_test_user', assumeNotNull(t.user_id))       AS is_test,
    t.transaction_type,
    t.status,
    count()                                                                     AS tx_count,
    sum(t.amount)                                                               AS total_amount
FROM lab08.transactions_raw t
WHERE
    t.user_id IS NOT NULL
    AND dictHas('lab08.users_dict', assumeNotNull(t.user_id))
GROUP BY date, is_test, t.transaction_type, t.status
ORDER BY date, is_test;
