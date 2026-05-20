-- MV backing tables and materialized views for CH-light fraud detection.
-- Executed by setup_clickhouse_mv() in clickhouse_mv_selector.py (idempotent).
-- Must run after setup_clickhouse() has created the transactions table.

-- Backing table: gift card burst 30d  (bucket = 30 * 86400 = 2592000 s)
CREATE TABLE IF NOT EXISTS gb30_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

-- Backing table: gift card burst 45d  (bucket = 90 * 86400 = 3888000 s)
CREATE TABLE IF NOT EXISTS gb45_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

-- Backing table: spend velocity 7d  (bucket = 7 * 86400 = 604800 s)
CREATE TABLE IF NOT EXISTS sv7_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

-- MV: gift card burst 30d — fires on every INSERT to transactions
CREATE MATERIALIZED VIEW IF NOT EXISTS gb30_mv
TO gb30_counts AS
SELECT
    cc_num,
    intDiv(toUnixTimestamp(ts), 2592000) AS bucket,
    count() AS cnt
FROM transactions
WHERE category = 'gift card'
GROUP BY cc_num, bucket;

-- MV: gift card burst 45d
CREATE MATERIALIZED VIEW IF NOT EXISTS gb45_mv
TO gb45_counts AS
SELECT
    cc_num,
    intDiv(toUnixTimestamp(ts), 3888000) AS bucket,
    count() AS cnt
FROM transactions
WHERE category = 'gift card'
GROUP BY cc_num, bucket;

-- MV: spend velocity 7d — all categories
CREATE MATERIALIZED VIEW IF NOT EXISTS sv7_mv
TO sv7_counts AS
SELECT
    cc_num,
    intDiv(toUnixTimestamp(ts), 604800) AS bucket,
    count() AS cnt
FROM transactions
GROUP BY cc_num, bucket;

-- Backing table: displacement approx (day bucket, 3-day sliding window via expanded rows)
CREATE TABLE IF NOT EXISTS disp_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

-- MV: displacement approx — any transaction with non-zero shipping coords counts.
-- No customer JOIN needed: worst-case over-approximation of repeated_displacement.
-- Each transaction fans out to 3 consecutive day-buckets (matches CH-full window).
-- Note: UNION ALL in a single MV only fires the first SELECT in ClickHouse.
-- Use 3 separate MVs instead so each bucket offset is materialized independently.
CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d0
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400)     AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d1
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 1 AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;

CREATE MATERIALIZED VIEW IF NOT EXISTS disp_mv_d2
TO disp_counts AS
SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 2 AS bucket, count() AS cnt
FROM transactions WHERE shipping_lat != 0
GROUP BY cc_num, bucket;
