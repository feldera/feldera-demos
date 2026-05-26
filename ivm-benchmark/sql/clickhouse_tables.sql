-- CH-full schema: base tables only.
-- Threshold/priority CREATE FUNCTION preamble is generated from constants.py at setup time
-- (see engine_ch.py: ch_functions_sql()) — no hardcoded values here.
-- Executed by ClickHouseFullEngine.setup() (idempotent — all CREATE ... IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS customers (
    cc_num  UInt64,
    name    String,
    lat     Float64,
    long    Float64
) ENGINE = ReplacingMergeTree()
ORDER BY cc_num;

CREATE TABLE IF NOT EXISTS transactions (
    cc_num        UInt64,
    ts            DateTime,
    amt           Float64,
    category      LowCardinality(String),
    shipping_lat  Float64,
    shipping_long Float64
) ENGINE = MergeTree()
ORDER BY (cc_num, ts);
