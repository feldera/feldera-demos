-- CH-full schema: base tables and threshold/priority constants.
-- Executed by ClickHouseFullEngine.setup() (idempotent — all CREATE ... IF NOT EXISTS).
--
-- Threshold and priority constants (mirrors feldera_views.sql scalar functions).
-- ClickHouse lambda UDFs work in WHERE/SELECT but NOT in WINDOW RANGE bounds
-- (which require literal integers) — window-second values stay as annotated literals.

CREATE FUNCTION IF NOT EXISTS GB30      AS () -> toUInt32(20);
CREATE FUNCTION IF NOT EXISTS GB45      AS () -> toUInt32(20);
CREATE FUNCTION IF NOT EXISTS SV7       AS () -> toUInt32(20);
CREATE FUNCTION IF NOT EXISTS DISP      AS () -> toUInt32(10);

CREATE FUNCTION IF NOT EXISTS PRIO_GB30 AS () -> toUInt32(3);
CREATE FUNCTION IF NOT EXISTS PRIO_GB45 AS () -> toUInt32(4);
CREATE FUNCTION IF NOT EXISTS PRIO_SV7  AS () -> toUInt32(1);
CREATE FUNCTION IF NOT EXISTS PRIO_DISP AS () -> toUInt32(5);

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
