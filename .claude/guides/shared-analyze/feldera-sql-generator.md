---
name: feldera-sql-generator
description: Rule-based reference for generating Feldera SQL views that compile and run correctly on the first attempt. Consult this before writing any detection or analytics views.
---

# Feldera SQL View Generation Rules

Consult this reference BEFORE writing any Feldera SQL view. Each rule has a code tag. When a rule applies, add a `-- NOTE: [TAG]` comment in the generated SQL so the reader knows why the pattern was chosen.

---

## [FV-TUMBLE-SOURCE] TUMBLE / HOP source must be a named relation

#### ⚠️ Problem

The data argument to `TUMBLE` or `HOP` must be a named table or view. Inline subqueries are not accepted and will produce a compiler error.

#### 🔄 Rule

- Never pass `TABLE (SELECT ...)` as the first argument to `TUMBLE` or `HOP`
- If filtering or joining is needed before windowing, create a helper view first, then reference it

#### 📌 Example

```sql
-- WRONG — inline subquery not accepted inside TUMBLE
FROM TABLE(TUMBLE(TABLE (SELECT * FROM t WHERE ...), DESCRIPTOR(ts), INTERVAL 1 HOUR))

-- CORRECT — reference a named view
CREATE VIEW txn_filtered AS SELECT * FROM TRANSACTION WHERE amt > 0;

FROM TABLE(TUMBLE(TABLE txn_filtered, DESCRIPTOR(ts), INTERVAL 1 HOUR))
```

---

## [FV-TUMBLE-NULL] Filter NULL timestamps before TUMBLE

#### ⚠️ Problem

When the timestamp column is nullable, `TUMBLE` silently drops rows with `NULL` timestamps (Feldera ≥ 0.282). Earlier versions panicked at runtime. Either way, NULL rows never contribute to window output, and the behaviour difference across versions makes it a reliability risk.

#### 🔄 Rules

- Always create a helper view that filters out null timestamps before feeding `TUMBLE` — this is safe on all Feldera versions
- Do NOT use `CAST(ts AS TIMESTAMP NOT NULL)` — that syntax is invalid in Feldera (→ [FV-CAST-NOTNULL])

#### 📌 Example

```sql
-- RISKY — ts is nullable; NULL rows are dropped silently (current) or crash (older versions)
FROM TABLE(TUMBLE(TABLE TRANSACTION, DESCRIPTOR(ts), INTERVAL 1 HOUR))

-- CORRECT — filter nulls in a helper view first
CREATE VIEW txn_notnull AS
SELECT * FROM TRANSACTION WHERE ts IS NOT NULL;  -- NOTE: [FV-TUMBLE-NULL]

CREATE VIEW my_window_view AS
SELECT cc_num, window_start, window_end, COUNT(*) AS cnt
FROM TABLE(TUMBLE(TABLE txn_notnull, DESCRIPTOR(ts), INTERVAL 1 HOUR))
GROUP BY cc_num, window_start, window_end;
```

---

## [FV-TUMBLE-INTERVAL] INTERVAL literal syntax inside TUMBLE

#### ⚠️ Problem

The only invalid form is putting the unit inside the quotes. Both bare and quoted integers work, and singular/plural does not matter.

#### 🔄 Rule

- The unit must be **outside** the quotes — `INTERVAL '10' MINUTES` not `INTERVAL '10 MINUTES'`
- Singular and plural units are both accepted in all cases

#### 📌 Example

```sql
INTERVAL 1 HOUR           -- OK
INTERVAL 6 HOURS          -- OK
INTERVAL '10' MINUTES     -- OK
INTERVAL '10' MINUTE      -- OK
INTERVAL '1 HOUR'         -- WRONG — unit inside quotes; compiler error
```

---

## [FV-TSDIFF] Timestamp subtraction is not supported — use TIMESTAMPDIFF

#### ⚠️ Problem

Feldera does not support `TIMESTAMP - TIMESTAMP` with the `-` operator. Using it produces a compiler error.

#### 🔄 Rule

- Replace `ts1 - ts2` comparisons with `TIMESTAMPDIFF(unit, ts1, ts2)`
- Available units: `SECOND`, `MINUTE`, `HOUR`, `DAY`

#### 📌 Example

```sql
-- WRONG
WHERE ts - prev_ts <= INTERVAL '10' MINUTES

-- CORRECT — NOTE: [FV-TSDIFF]
WHERE TIMESTAMPDIFF(MINUTE, prev_ts, ts) <= 10
WHERE TIMESTAMPDIFF(SECOND, prev_ts, ts) <= 600
```

---

## [FV-CAST-NOTNULL] NOT NULL is invalid inside CAST

#### ⚠️ Problem

`CAST(expr AS TYPE NOT NULL)` is not valid Feldera SQL. The compiler rejects the `NOT NULL` qualifier inside a cast.

#### 🔄 Rule

- Use plain type names only inside `CAST`
- To guarantee non-null output, filter nulls upstream (→ [FV-TUMBLE-NULL])

#### 📌 Example

```sql
-- WRONG
CAST(ts AS TIMESTAMP NOT NULL)

-- CORRECT — NOTE: [FV-CAST-NOTNULL]
CAST(ts AS TIMESTAMP)
CAST(NULL AS BIGINT)
CAST(NULL AS DECIMAL(38, 2))
```

---

## [FV-LAG-QUALIFY] LAG / LEAD filtering: QUALIFY works (≥ 0.282), subquery WHERE is safer

#### 📝 Note (Feldera ≥ 0.282)

`QUALIFY` compiles and produces correct results in Feldera 0.282+. Either pattern below is valid.
The subquery `WHERE` form is more portable across versions and SQL engines.

#### 🔄 Rule

- `QUALIFY` is acceptable on Feldera ≥ 0.282
- Use the subquery `WHERE` form when targeting multiple Feldera versions or unknown environments

#### 📌 Example

```sql
-- ACCEPTABLE on Feldera ≥ 0.282 — QUALIFY works correctly
SELECT cc_num, ts, amt,
       LAG(amt) OVER (PARTITION BY cc_num ORDER BY ts) AS prev_amt
FROM TRANSACTION
QUALIFY prev_amt IS NOT NULL AND prev_amt <= 5 AND amt >= 200;

-- CORRECT — NOTE: [FV-LAG-QUALIFY]
CREATE VIEW my_sequence_view AS
SELECT cc_num, ts, amt, prev_amt
FROM (
    SELECT
        cc_num,
        ts,
        amt,
        LAG(amt) OVER (PARTITION BY cc_num ORDER BY ts) AS prev_amt
    FROM TRANSACTION
    WHERE ts IS NOT NULL
)
WHERE prev_amt IS NOT NULL
  AND prev_amt <= 5
  AND amt >= 200;
```

---

## [FV-COUNT-DISTINCT] COUNT(DISTINCT …) works inside TUMBLE GROUP BY

#### 📝 Note

`COUNT(DISTINCT col)` is supported in `GROUP BY` after a `TUMBLE`. No workaround needed — this is here to prevent unnecessary rewrites.

#### 📌 Example

```sql
-- CORRECT — no rewrite needed
SELECT loc, window_start, COUNT(DISTINCT cc_num) AS card_count
FROM TABLE(TUMBLE(TABLE txn_notnull, DESCRIPTOR(ts), INTERVAL 6 HOURS))
GROUP BY loc, window_start
HAVING COUNT(DISTINCT cc_num) > 5;
```

---

## [FV-QUERY-MATERIALIZED] Only materialized tables and views can be queried with fda query

#### ⚠️ Problem

`fda query` (and ad-hoc SQL) can only SELECT from sources declared as materialized. Querying a regular `CREATE VIEW` produces:
```
Error: Tried to SELECT from a non-materialized source.
```

#### 🔄 Rules

- Use `CREATE MATERIALIZED VIEW` (not `CREATE VIEW`) for any view you need to query directly or check row counts on
- To check data distribution during threshold calibration, query a **materialized table** (e.g. `TRANSACTION`, `CUSTOMER`) or a materialized view — never a plain view
- `transaction_with_distance` and `transaction_with_aggregates` in the fraud demo are plain views — do not query them directly; query the signal views (which are materialized) instead

#### 📌 Example

```sql
-- WRONG — transaction_with_aggregates is a plain VIEW; fda query will fail
SELECT MIN(distance) FROM transaction_with_aggregates;

-- CORRECT — query a materialized table or materialized view  -- NOTE: [FV-QUERY-MATERIALIZED]
SELECT MIN(amt), MAX(amt) FROM transaction;
SELECT COUNT(*) FROM skimming_probe_charges;
```

---

## [FV-ST-DISTANCE-DEGREES] ST_DISTANCE returns degrees, not meters

#### ⚠️ Problem

`ST_DISTANCE(ST_POINT(lon, lat), ST_POINT(lon, lat))` in Feldera returns Euclidean distance in **degrees**, not meters. Using meter-based thresholds (e.g. `> 50000`) will never fire — max distance across the continental US is ~64 degrees.

#### 🔄 Rules

- Always use degree-based thresholds with `ST_DISTANCE`
- Approximate conversions: `> 0.5` ≈ 55 km, `> 5` ≈ 550 km, `> 20` ≈ 2200 km
- Do NOT use the comment in `TRANSACTION_WITH_AGGREGATES` as a reference — it incorrectly states "50,000 meters"

#### 📌 Example

```sql
-- WRONG — distance is in degrees, not meters; this never fires
WHERE distance > 500000

-- CORRECT — NOTE: [FV-ST-DISTANCE-DEGREES]
WHERE distance > 5    -- roughly 550 km from home
WHERE distance > 0.5  -- roughly 55 km from home
```

---

## [FV-COMPOSE] Prefer composing on top of existing views

#### 🔄 Rule

- When a view already computes an expensive join, window, or aggregate, build on it rather than recomputing from base tables
- Check what columns are available in upstream views before writing a new join or window

#### 📌 Example

```sql
-- TRANSACTION_WITH_DISTANCE already has: distance (degrees from home; ST_DISTANCE returns degrees)
-- TRANSACTION_WITH_AGGREGATES already has: avg_1day, count_1day (rolling 1-day aggregates; count_1day counts transactions with distance > 0.5°)

-- CORRECT — reuse pre-built rolling aggregate; NOTE: [FV-COMPOSE]
CREATE MATERIALIZED VIEW flagged_repeated_displacement AS
SELECT cc_num, ts, amt, distance, count_1day
FROM TRANSACTION_WITH_AGGREGATES
WHERE count_1day >= 1        -- at least one distant transaction today
  AND distance > 0.5         -- NOTE: [FV-ST-DISTANCE-DEGREES] ~55 km in degrees
  AND ts IS NOT NULL;
```

---

## [FV-UNION-TYPES] UNION branches must have compatible columns

#### ⚠️ Problem

`UNION ALL` fails when branches have different column counts, or when types are fundamentally incompatible (e.g., `INT` vs `BOOLEAN`, `GEOGRAPHY` vs `BIGINT`). Feldera auto-coerces numeric types (`DECIMAL`, `DOUBLE`, `BIGINT`, etc.) so mixing those is fine — but missing columns and incompatible types are not.

#### 🔄 Rules

- Every branch must project the **same number of columns** with compatible types
- For columns absent in some branches, use `CAST(NULL AS <type>)` matching the type used in other branches
- Do **not** mix `BOOLEAN` or `GEOGRAPHY` columns with numeric types across branches

#### 📌 Example

```sql
-- WRONG — different column count across branches
SELECT cc_num, ts, amt, signal_type FROM flagged_probe_charges
UNION ALL
SELECT cc_num, ts              FROM flagged_rapid_sequence;  -- missing amt, signal_type

-- CORRECT — pad missing columns with NULL  -- NOTE: [FV-UNION-TYPES]
SELECT cc_num, ts, amt,              signal_type FROM flagged_probe_charges
UNION ALL
SELECT cc_num, ts, CAST(NULL AS DOUBLE), signal_type FROM flagged_rapid_sequence;
```

---

## [FV-THRESHOLD-CALIBRATE] Calibrate thresholds against actual data density

#### ⚠️ Problem

A threshold that looks reasonable in the abstract (e.g. "flag users who access ≥ 30 files/hour") may never fire if the key space is too sparse, or fire for 100% of rows if the data is denser than expected.

#### 🔄 Rules

Before writing `HAVING` thresholds:
1. Estimate `events_per_cell = total_events / (distinct_entities × time_windows)` for each GROUP BY key
2. If `events_per_cell < threshold`, the view will return 0 rows — lower the threshold or widen the grouping
3. Joining before grouping multiplies the key space: grouping by `(user_id, parent_id)` with 1000 parent folders reduces expected events per cell by 1000×

After deploying, always verify with `SELECT COUNT(*) FROM <view>` — if 0, the threshold is too high.

#### 📌 Example

```
-- access_log: 1M events, 1000 users, 278 1-hour windows
-- events/user/hour = 1,000,000 / 1000 / 278 ≈ 3.6

-- WRONG — grouping by (user, parent_folder) with 1000 parent folders:
-- events/user/parent/hour = 3.6 / 1000 = 0.004 → threshold 30 never fires
GROUP BY user_id, parent_id, window_start HAVING COUNT(*) >= 30

-- CORRECT — NOTE: [FV-THRESHOLD-CALIBRATE]
-- group by user only; threshold 4 fires for ~37% of (user, window) pairs at λ=3.6
GROUP BY user_id, window_start HAVING COUNT(*) >= 4
```

---

## [FV-DATAGEN-SCALE] Datagen `scale` is a millisecond step — range must be wide enough

#### ⚠️ Problem

The `"scale": N` parameter sets the **millisecond gap between consecutive generated records**. Actual timestamp span = `(limit - 1) × scale` ms — but only when the declared range is wide enough. If `limit × scale > range_size_ms`, timestamps wrap modulo the range and collapse back toward the start. A `scale=60000` (1 min step) over a 10-second range produces all timestamps at 0 because 60,000 ms mod 10,000 ms = 0.

#### 🔄 Rules

- Make the `"range"` at least `limit × scale` ms wide, or timestamps will wrap and lose their spread
- Always verify actual timestamp span after starting the pipeline:
  `SELECT MIN(ts), MAX(ts) FROM <table>`
- Use the observed span (not the declared range) to calculate events per window

#### 📌 Example

```json
-- 1M records, scale=1000 → span = 1,000,000 × 1000ms ≈ 11.6 days
-- range is 1 year (>> 11.6 days) → no wrapping, span is correct
"ts": { "range": ["2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z"], "scale": 1000 }

-- WRONG — range=10s, scale=60000ms, limit=10 → 60,000ms mod 10,000ms = 0 → all timestamps at start
"ts": { "range": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:10Z"], "scale": 60000 }

-- NOTE: [FV-DATAGEN-SCALE]
-- Always verify: SELECT MIN(ts), MAX(ts) FROM access_log
-- → 2026-01-01 to 2026-01-12 → use 278 hours for density calc, not 8760
```

---

## [FV-DATAGEN-SEQUENTIAL] Datagen `limit` without `rate` generates uniform distributions — no anomalies

#### ⚠️ Problem

Datagen with `"limit": N` and no `"rate"` generates entity IDs (user_id, card_num, etc.) by cycling sequentially through the declared range. Every entity gets nearly identical event counts per window — there are no statistical outliers. Per-entity anomaly detection based on volume thresholds will either fire for all entities or none.

#### 🔄 Rules

- With a sequential datagen, volume-based per-entity signals (burst, exploration) will flag all or no entities uniformly
- To create realistic anomalies, add a separate datagen plan with a higher rate for a specific entity (the "attacker"), or use `"strategy": "zipf"` to introduce skew
- `"strategy": "zipf"` on entity IDs creates power-law distributions where a few entities dominate — much better for anomaly detection demos

#### 📌 Example

```json
// Uniform — all users identical, no anomalies detectable
{ "limit": 1000000, "fields": { "user_id": { "range": [0, 1000] } } }

// NOTE: [FV-DATAGEN-SEQUENTIAL]
// Better: zipf distribution creates heavy hitters
{ "rate": 1000, "fields": { "user_id": { "range": [0, 1000], "strategy": "zipf" } } }

// Or: plant a specific attacker
{ "rate": 100, "fields": { "user_id": { "values": [42] } } }  // user 42 is 100x busier
```

---

## Pre-deploy checklist

Run through this before calling `update_program` or `start`:

- [ ] **[FV-TUMBLE-SOURCE]** Every `TUMBLE` / `HOP` source is a named view or table — no inline `TABLE (SELECT ...)`
- [ ] **[FV-TUMBLE-NULL]** Every nullable timestamp column feeding `TUMBLE` is pre-filtered with `WHERE ts IS NOT NULL` (NULL rows are silently dropped on current Feldera, but the filter is still required for older versions and clarity)
- [ ] **[FV-TSDIFF]** No `ts1 - ts2` expressions — replaced with `TIMESTAMPDIFF`
- [ ] **[FV-CAST-NOTNULL]** No `CAST(... AS TYPE NOT NULL)` — stripped to plain `CAST(... AS TYPE)`
- [ ] **[FV-LAG-QUALIFY]** `LAG`/`LEAD` filter conditions: `QUALIFY` is safe on Feldera ≥ 0.282; use subquery `WHERE` for maximum portability
- [ ] **[FV-QUERY-MATERIALIZED]** Any view used for threshold calibration or row-count checks is declared as `MATERIALIZED VIEW` — plain views cannot be queried with `fda query`
- [ ] **[FV-UNION-TYPES]** Every UNION branch casts numeric and timestamp columns to the same explicit type — no implicit type mixing across branches
- [ ] **[FV-THRESHOLD-CALIBRATE]** Thresholds verified against `events_per_cell = total_events / (entities × windows)` — grouping by additional dimensions multiplies sparsity
- [ ] **[FV-DATAGEN-SCALE]** If using datagen with a `scale` parameter, range is at least `limit × scale` ms wide (or timestamps wrap); verified actual span with `SELECT MIN(ts), MAX(ts)` before estimating density
- [ ] **[FV-DATAGEN-SEQUENTIAL]** If using `"limit"` without `"rate"`, noted that all entities will have uniform distributions — use `"strategy": "zipf"` or plant an attacker if anomalies are needed
- [ ] `validate_file(...)` returns `[]` before calling `update_program` / `start`
