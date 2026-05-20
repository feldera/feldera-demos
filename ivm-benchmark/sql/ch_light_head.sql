-- CH-light head v2 — same semantics as ch_light_head.sql, two optimizations:
--   1. Drop FINAL from the SummingMergeTree MV reads.
--      sum(cnt) over unmerged parts gives the same answer; FINAL forces a
--      synchronous part-merge at query time (serial, O(parts)).
--   2. Pre-filter best_txn to only cards that appear in some signal.
--      Cuts the transactions scan from O(all rows) to O(flagged rows).
-- Concatenated with ch_view_tail.sql to form CREATE OR REPLACE VIEW fraud_signals_light.
CREATE OR REPLACE VIEW fraud_signals_light AS
WITH
flagged_gb30 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_30d' AS signal_type, __PRIO_GB30__ AS priority
    FROM gb30_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __GB30__
),
flagged_gb45 AS (
    SELECT DISTINCT cc_num, 'gift_card_burst_45d' AS signal_type, __PRIO_GB45__ AS priority
    FROM gb45_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __GB45__
),
flagged_sv7 AS (
    SELECT DISTINCT cc_num, 'spend_velocity_7d' AS signal_type, __PRIO_SV7__ AS priority
    FROM sv7_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __SV7__
),
flagged_disp AS (
    SELECT DISTINCT cc_num, 'repeated_displacement' AS signal_type, __PRIO_DISP__ AS priority
    FROM disp_counts
    GROUP BY cc_num, bucket
    HAVING sum(cnt) >= __DISP__
),
all_signals AS (
    SELECT cc_num, signal_type, priority FROM flagged_gb30
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_gb45
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_sv7
    UNION ALL SELECT cc_num, signal_type, priority FROM flagged_disp
),
flagged_cards AS (
    SELECT DISTINCT cc_num FROM all_signals
),
best_txn AS (
    SELECT
        cc_num,
        max(ts)                  AS latest_ts,
        argMax(amt,           ts) AS max_amt,
        argMax(category,      ts) AS category,
        argMax(shipping_lat,  ts) AS shipping_lat,
        argMax(shipping_long, ts) AS shipping_long,
        0.0                       AS distance
    FROM transactions
    WHERE cc_num IN (SELECT cc_num FROM flagged_cards)
    GROUP BY cc_num
)
