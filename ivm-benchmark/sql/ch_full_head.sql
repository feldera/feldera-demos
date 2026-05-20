-- CH-full head: signal detection CTEs + best_txn (with distance via customer JOIN).
-- Thresholds __GB30__ __GB45__ __SV7__ __DISP__ and priorities __PRIO_GB30__ __PRIO_GB45__ __PRIO_SV7__ __PRIO_DISP__ replaced by Python at setup time.
-- Concatenated with ch_view_tail.sql to form CREATE OR REPLACE VIEW fraud_signals_full.
-- Uses epoch-aligned buckets (same as Feldera and CH-light) so all engines are comparable.
CREATE OR REPLACE VIEW fraud_signals_full AS
WITH
enriched AS (
    SELECT
        t.cc_num, t.ts, t.amt, t.category,
        t.shipping_lat, t.shipping_long,
        abs(t.shipping_lat - c.lat) + abs(t.shipping_long - c.long) AS distance
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
),
gb30 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_30d' AS signal_type, __PRIO_GB30__ AS priority
    FROM enriched WHERE category = 'gift card'
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 2592000)
    HAVING count() >= __GB30__
),
gb45 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_45d' AS signal_type, __PRIO_GB45__ AS priority
    FROM enriched WHERE category = 'gift card'
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 3888000)
    HAVING count() >= __GB45__
),
sv7 AS (
    SELECT DISTINCT cc_num, 'spend_velocity_7d' AS signal_type, __PRIO_SV7__ AS priority
    FROM enriched
    GROUP BY cc_num, intDiv(toUnixTimestamp(ts), 604800)
    HAVING count() >= __SV7__
),
disp_expanded AS (
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400)     AS day_bucket FROM enriched WHERE distance > 20.0
    UNION ALL
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 1 AS day_bucket FROM enriched WHERE distance > 20.0
    UNION ALL
    SELECT cc_num, intDiv(toUnixTimestamp(ts), 86400) - 2 AS day_bucket FROM enriched WHERE distance > 20.0
),
disp AS (
    SELECT DISTINCT cc_num, 'repeated_displacement' AS signal_type, __PRIO_DISP__ AS priority
    FROM disp_expanded
    GROUP BY cc_num, day_bucket
    HAVING count() >= __DISP__
),
all_signals AS (
    SELECT cc_num, signal_type, priority FROM gb30
    UNION ALL SELECT cc_num, signal_type, priority FROM gb45
    UNION ALL SELECT cc_num, signal_type, priority FROM sv7
    UNION ALL SELECT cc_num, signal_type, priority FROM disp
),
best_txn AS (
    SELECT
        t.cc_num,
        max(t.ts)                                                                    AS latest_ts,
        argMax(t.amt,           t.ts)                                               AS max_amt,
        argMax(t.category,      t.ts)                                               AS category,
        argMax(t.shipping_lat,  t.ts)                                               AS shipping_lat,
        argMax(t.shipping_long, t.ts)                                               AS shipping_long,
        argMax(abs(t.shipping_lat - c.lat) + abs(t.shipping_long - c.long), t.ts)  AS distance
    FROM transactions AS t
    LEFT JOIN customers AS c USING (cc_num)
    GROUP BY t.cc_num
)
