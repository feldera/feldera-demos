-- CH-light additional tables: SummingMergeTree backing tables for Materialized Views.
-- Executed after ch_full_tables.sql (which provides customers + transactions).
-- The MVs that write into these tables live in ch_light_views.sql.

CREATE TABLE IF NOT EXISTS gb30_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

CREATE TABLE IF NOT EXISTS gb45_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

CREATE TABLE IF NOT EXISTS sv7_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);

CREATE TABLE IF NOT EXISTS disp_counts (
    cc_num  UInt64,
    bucket  Int64,
    cnt     UInt64
) ENGINE = SummingMergeTree(cnt)
ORDER BY (cc_num, bucket);
