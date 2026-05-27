# Precise and Fast: Fraud Detection at Streaming Scale

<!-- TODO: add intro story -->

---

## The Fraud Signals

Four patterns show up consistently in payment fraud. Each one tells a specific story about how stolen card data gets monetized.

### Gift card burst — 30-day and 45-day windows

A stolen card rarely gets maxed out with a single large purchase — that triggers an immediate block. Instead, fraud rings buy a steady stream of gift cards: $50 here, $100 there, sometimes a dozen transactions a day. Gift cards are the preferred exit because they don't require shipping, can't be charged back once activated, and are resold instantly on secondary markets.

The signal: **N or more gift card transactions within a 30-day sliding window** (or 45 days for slower-moving rings).

### Spend velocity — 7-day window

Fraud rings don't just buy gift cards. Once they have working credentials they test and exhaust them across many categories. The velocity pattern captures this: **N or more transactions of any kind within a 7-day sliding window**. Normal cardholders have a rhythm. Compromised cards don't.

### Repeated displacement

This signal catches a different threat: card-present skimming. A skimmer clones your physical card and uses it far from where you live. The signal: **N or more transactions more than 20° from the cardholder's home address within a 3-day sliding window**.

This is computationally interesting. Checking displacement requires joining each transaction against the customer table to get the home address. ClickHouse and Feldera both perform this join exactly — it's what makes the detection precise.

---

## Why This Is Hard to Do Fast

The naive approach runs a window query over the full transaction history on every new batch. That query is O(N) — it grows linearly with the number of rows. On a small table it's instant. On months of history across millions of cardholders, it takes seconds per batch.

Feldera takes a different approach. It maintains the full computation graph incrementally — including cross-table joins and true sliding windows. When a new transaction arrives, Feldera propagates only the delta through every view that depends on it: the distance join, the windowed aggregates, the alert counts. The refresh time is O(delta), and the final count query hits a single precomputed row in O(1) regardless of history size.

---

## The Two Engines

### ClickHouse — full recompute

New rows are INSERTed, then a full window query scans all history on every batch.

- Detects all 4 signals with exact true sliding-window semantics
- Query time grows **O(N)** with total history size

### Feldera — Incremental View Maintenance

Each batch is wrapped in a transaction: push rows → commit. On commit, Feldera incrementally updates the full computation graph — cross-table joins included — processing only the new delta. After commit, `SELECT COUNT(*) FROM fraud_alert_details` reads a single precomputed materialized view.

- Detects all 4 signals with exact true sliding-window semantics including distance check
- Refresh time: **O(delta)** — IVM over new rows only
- Query time: **O(1)** — reads a precomputed materialized view

Both engines produce **identical alert counts** — this benchmark measures speed, not accuracy tradeoffs.

---

## Fraud signal definitions

| Signal | Definition | Window |
|--------|-----------|--------|
| `gift_card_burst_30d` | N+ gift card transactions | 30-day sliding |
| `gift_card_burst_45d` | N+ gift card transactions | 45-day sliding |
| `spend_velocity_7d` | N+ transactions (any category) | 7-day sliding |
| `repeated_displacement` | N+ transactions > 20° from home address | 3-day sliding |

Thresholds are defined in `constants.py` and injected as SQL scalar functions at setup time:

| Constant | Value | Signal |
|----------|-------|--------|
| `GIFT_BURST_30D_THRESHOLD` | 20 | `gift_card_burst_30d` |
| `GIFT_BURST_45D_THRESHOLD` | 20 | `gift_card_burst_45d` |
| `SPEND_VELOCITY_7D_THRESHOLD` | 20 | `spend_velocity_7d` |
| `DISPLACEMENT_THRESHOLD` | 10 | `repeated_displacement` |

---

## Timing model

Each step is measured in three phases:

| Column | ClickHouse | Feldera |
|--------|-----------|---------|
| `ins` | INSERT rows | push rows inside transaction |
| `ref` | — (scan happens at query time) | IVM: incremental view maintenance on commit |
| `qry` | full O(N) scan over all history | O(1) read from `fraud_alert_details` |
| **total** | `ins + qry` | `ins + ref + qry` |

`total` is the right comparison metric. For ClickHouse, all computation happens at query time and grows with history. For Feldera, computation happens at commit (`ref`) and `qry` is always O(1).

---

## Demo modes

| Mode | Engines | Story |
|------|---------|-------|
| `latency` / `full` | ClickHouse, Feldera | O(N) scan vs O(delta) IVM |

---

## SQL architecture

Both engines share the same logical pipeline structure:

```
TRANSACTION_WITH_DISTANCE        — enrich each transaction with Manhattan distance to home
    ↓
TRANSACTION_WITH_AGGREGATES      — compute all rolling window aggregates once (named WINDOW clauses)
    ↓
flagged_gift_card_burst_30d      — WHERE gift_count_30day >= GB30()
flagged_gift_card_burst_45d      — WHERE gift_count_45day >= GB45()
flagged_spend_velocity_7d        — WHERE txn_count_7day   >= SV7()
flagged_repeated_displacement    — WHERE disp_count_3day  >= DISP()
    ↓
fraud_alerts                     — UNION ALL of all four signal streams
    ↓
card_suspicion_score             — SUM of signal priorities per card (multi-signal cards rank higher)
    ↓
fraud_alert_details              — final enriched output (one row per flagged card)
```

Threshold and priority functions (`GB30()`, `PRIO_GB30()`, etc.) are generated from `constants.py` at setup time — no hardcoded values in SQL files.

**ClickHouse note**: `WINDOW RANGE` bounds require literal integer seconds (`604800` for 7 days). ClickHouse lambda UDFs work in `WHERE`/`SELECT` but not in `RANGE` bounds.

---

## SQL file layout

| File | Engine | Purpose |
|------|--------|---------|
| `clickhouse_tables.sql` | ClickHouse | `customers` and `transactions` table DDL |
| `clickhouse_views.sql` | ClickHouse | `fraud_signals_full` view with full pipeline |
| `clickhouse_query.sql` | ClickHouse | `SELECT count(DISTINCT cc_num) FROM fraud_signals_full` |
| `feldera_tables.sql` | Feldera | `CUSTOMER` and `TRANSACTION` table DDL |
| `feldera_views.sql` | Feldera | Full pipeline as incremental views + `fraud_alert_details` materialized view |
| `feldera_query.sql` | Feldera | `SELECT COUNT(*) FROM fraud_alert_details` |

---

## Prerequisites

```bash
pip install "feldera>=0.298" clickhouse-connect matplotlib python-dotenv requests
```

Start both services:

```bash
docker start clickhouse-server
docker start feldera
```

Or start fresh:

```bash
docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server

docker run -d --name feldera -p 8080:8080 \
  images.feldera.com/feldera/pipeline-manager:latest
```

---

## Quick start — mock mode (no DB needed)

```bash
python3 demo_runner.py --mock
python3 demo_runner.py --mock --steps 10 --output results.txt
```

---

## Real mode

### Data scales

| Scale | Transactions | Use |
|-------|-------------|-----|
| `data/0.1x` | ~600K rows | Quick smoke test |
| `data/1x` | ~6M rows | Standard demo |
| `data/10x` | ~60M rows | Large history |
| `data/100x` | ~600M rows | Maximum latency gap |

### Run

```bash
# Smoke test (0.1x data, both engines)
python3 demo_runner.py --data-dir data/0.1x --interval 0

# Standard benchmark with preloaded history
python3 demo_runner.py --data-dir data/1x --preload-rows 3000000 --steps 500 --batch-rows 2000 --interval 0 --output results.txt

# Plot results
python3 plot_results.py results.txt
```

---

## Key arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mock` | off | Simulate queries; no DB needed |
| `--output` | none | Save summary to file |
| `--mode` | `full` | `full` \| `latency` (both run same engines) |
| `--data-dir` | `data/0.1x` | Dataset scale directory |
| `--steps` | `50` | Number of streaming batches (cache layout) |
| `--max-steps` | none | Stop early after N steps |
| `--batch-rows` | none | Fix each batch to exactly N rows |
| `--preload-rows` | `0` | Rows of history loaded before streaming |
| `--interval` | `10` | Seconds between batches |
| `--sequential` | off | Run engines one at a time per step (clean isolated timing) |
| `--no-clickhouse` | off | Run Feldera only |
| `--no-feldera` | off | Run ClickHouse only |
| `--clickhouse-host` | `localhost` | ClickHouse host |
| `--clickhouse-port` | `8123` | ClickHouse HTTP port |
| `--clickhouse-database` | `fraud_detection` | ClickHouse database name |
| `--api-url` | `http://localhost:8080` | Feldera host URL |
| `--api-key` | none | Feldera API key (not needed for local Docker) |

---

## Sweep runner

`run_experiments.py` sweeps across combinations of preload sizes, step counts, and engines:

```bash
# Default sweep
python3 run_experiments.py

# Custom sweep
python3 run_experiments.py --preload-rows 0 3000000 --steps 500 --engines feldera clickhouse

# Single run
python3 run_experiments.py --preload-rows 3000000 --steps 500 --batch-rows 2000 --engines all --data-dir data/100x
```

Engine presets: `clickhouse`, `feldera`, `latency` (both), `all` (both).

---

## Output

After the run, a per-step summary table prints to the terminal:

```
  PRELOAD  ClickHouse: 205ms   Feldera: 14.8s (push=174ms, ivm=1.0s)
  STEP LATENCY SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  step  engine        ins  ref+qry      qry    total     n
──────────────────────────────────────────────────────────
     1  ClickHouse  158ms    46ms         —    204ms    45
        Feldera     281ms  1010ms        2ms  1293ms    45
──────────────────────────────────────────────────────────
   avg                ins  ref+qry      qry    total
──────────────────────────────────────────────────────────
        ClickHouse  158ms   120ms         —    279ms
        Feldera     283ms  1010ms        2ms  1296ms
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

- `ins`: time to push the batch into the engine
- `ref+qry`: for ClickHouse — full O(N) recompute at query time; for Feldera — `ref` = IVM commit, `qry` = O(1) count read
- `n`: new fraud alerts detected this step (identical for both engines)
- `total`: end-to-end latency — the primary comparison metric
