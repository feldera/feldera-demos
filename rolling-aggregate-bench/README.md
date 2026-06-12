# Rolling Aggregate Benchmark: IVM vs Full Scan at Streaming Scale

A benchmark comparing three engines on a live transaction stream:

- **Feldera** — Incremental View Maintenance (IVM), O(delta) per batch
- **ClickHouse** — full columnar scan per query, O(N)
- **PostgreSQL** — full window-function scan per query, O(N), single-threaded

All three engines detect all four fraud signals with exact true sliding-window semantics and produce
identical alert counts. The story is purely about speed.

---

## Benchmark Results

### 20M preload — 5M customers, 1K batch, 10 steps

| Engine | Avg latency/step | vs Feldera |
|--------|-----------------|------------|
| Feldera | 0.33 s | — |
| ClickHouse | 4.66 s | 14× slower |
| PostgreSQL | 145 s | 440× slower |

### 200M preload — 5M customers, 1K batch, 10 steps 
| Engine | Avg latency/step | vs Feldera |
|--------|-----------------|------------|
| Feldera | 1.76 s | — |
| ClickHouse | 32.0 s | 18× slower |
| PostgreSQL | 1,441 s | 819× slower |

### 1B preload — 5M customers, 1K batch, 10 steps 
| Engine | Avg latency/step | vs Feldera |
|--------|-----------------|------------|
| Feldera | 2.96 s | — |
| ClickHouse | 226 s | 76× slower |
| PostgreSQL | 8,631 s | 2,916× slower |

PostgreSQL window functions are not parallelizable; query time grows strictly O(N) with history size.
Feldera's IVM is O(delta) — independent of history depth.

---

## The Fraud Signals

Four rolling-window patterns, computed identically across all engines:

| Signal | Definition | Window |
|--------|-----------|--------|
| `gift_card_burst_30d` | N+ gift card transactions | 30-day sliding |
| `gift_card_burst_45d` | N+ gift card transactions | 45-day sliding |
| `spend_velocity_7d` | N+ transactions (any category) | 7-day sliding |
| `repeated_displacement` | N+ transactions > 20° from home | 3-day sliding |

Thresholds (N) are auto-calibrated per dataset by `run_bench.py` via a p99 scan and cached in
`data/<run>/thresholds.json`. They are injected as SQL scalar functions at setup time (`GB30()`,
`GB45()`, `SV7()`, `DISP()`) — no hardcoded values in SQL files.

---

## Why This Is Hard to Do Fast

The naive approach runs a window query over the full transaction history on every new batch. That
query is O(N) — it grows linearly with the number of rows. On a small table it's instant. On months
of history across millions of cardholders, it takes tens of seconds per batch.

Feldera maintains the full computation graph incrementally — including cross-table joins and true
sliding windows. When a new transaction arrives, Feldera propagates only the delta through every view
that depends on it: the distance join, the windowed aggregates, the alert counts. The refresh time is
O(delta), and the final count query hits a single precomputed row in O(1) regardless of history size.

---

## The Three Engines

### Feldera — Incremental View Maintenance

Each batch is wrapped in a transaction: push rows → commit. On commit, Feldera incrementally updates
the full computation graph — cross-table joins included. After commit, `SELECT COUNT(*) FROM
fraud_alert_details` reads a single precomputed materialized view.

- Refresh time: **O(delta)** — IVM over new rows only
- Query time: **O(1)** — reads a precomputed materialized view

### ClickHouse — full columnar recompute

New rows are INSERTed, then a full window query scans all history on every batch. Uses columnar
MergeTree storage sorted by `(cc_num, ts)`. Parallelized across all available cores.

- Query time: **O(N)** — grows with total history size
- Multi-threaded; fast columnar I/O

### PostgreSQL — full window-function scan

New rows are COPYed in, then a CTE-based window query scans all history. Window functions in
PostgreSQL are single-threaded and cannot be parallelized, making PG the slowest option at scale.

- Query time: **O(N)** — single-threaded window scan
- `work_mem='8GB'` per session avoids CTE disk spill
- Use `--postgres-steps 10` at large scales to limit wall time

All three engines produce **identical alert counts** — this benchmark measures speed, not accuracy.

---

## Timing model

Each step is measured in three phases:

| Column | ClickHouse | Feldera | PostgreSQL |
|--------|-----------|---------|------------|
| `ins` | INSERT rows | push rows inside transaction | COPY rows |
| `ref` | — (scan at query time) | IVM incremental commit | — (scan at query time) |
| `qry` | full O(N) scan | O(1) from `fraud_alert_details` | full O(N) window scan |
| **total** | `ins + qry` | `ins + ref + qry` | `ins + qry` |

`total` is the right comparison metric.

---

## Prerequisites

### Python

Python 3.10+ and the following packages:

```bash
pip install "feldera>=0.298" clickhouse-connect python-dotenv requests psycopg2-binary
```

`clickhouse-connect` must be installed even for Feldera-only runs — it is imported at module load time.
`psycopg2-binary` is required only for PostgreSQL runs.

### Feldera

Start an existing container or pull fresh:

```bash
# existing container
docker start feldera

# or pull fresh
docker run -d --name feldera -p 8080:8080 \
  images.feldera.com/feldera/pipeline-manager:latest
```

### ClickHouse

```bash
# existing container
docker start clickhouse-server

# or pull fresh
docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server
```

### PostgreSQL

PostgreSQL must be installed natively (not in Docker) and reachable at a Unix socket
(`/var/run/postgresql`). The benchmark uses `COPY FROM STDIN` for bulk loading, which
requires a local socket connection.

Recommended `postgresql.conf` settings:

```
shared_buffers = 4GB     # do NOT set higher — large values risk OOM with swap disabled
work_mem = 8GB           # keeps CTE window scans in memory at large scale
```

For large-scale runs (200M+), disable swap before starting to avoid accidental thrashing:

```bash
sudo swapoff -a
```

---

## Quick start

`src/run_bench.py` is the main entry point. It generates data, scans thresholds, and runs the
benchmark in one command. Data and thresholds are cached — reruns skip generation automatically.

```bash
# Quick smoke test — all three engines, 1M rows (~5 min)
python3 src/run_bench.py \
  --customers 500000 --preload 1000000 --batch-size 1000 --batches 10 \
  --sequential --mode feldera ch pg

# Feldera + ClickHouse at 200M rows (~2 hrs)
python3 src/run_bench.py \
  --customers 5000000 --preload 200000000 --batch-size 1000 --batches 10 \
  --sequential --mode feldera ch

# All three engines: Feldera + ClickHouse + PostgreSQL
# Warning: PG preload + 10 steps at 200M scale takes ~9 hrs total
python3 src/run_bench.py \
  --customers 5000000 --preload 200000000 --batch-size 1000 --batches 10 \
  --sequential --mode feldera ch pg

# PostgreSQL only
python3 src/run_bench.py \
  --customers 5000000 --preload 20000000 --batch-size 1000 --batches 10 \
  --sequential --mode pg

# Feldera only
python3 src/run_bench.py \
  --customers 5000000 --preload 200000000 --batch-size 1000 --batches 10 \
  --sequential --mode feldera
```

---

## `run_bench.py` arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--customers` | `10,000` | Number of synthetic credit card holders |
| `--preload` | `10,000` | Rows of history loaded before streaming starts |
| `--batch-size` | `1,000` | Rows per streaming batch |
| `--batches` | `10` | Number of streaming batches |
| `--data-dir` | auto | Override the generated data directory |
| `--seed` | `42` | Random seed for data generation |
| `--percentile` | `99` | Target percentile for threshold calibration |
| `--sample-cards` | `100,000` | Cards sampled for threshold scan (0 = all) |
| `--mode` | `feldera ch` | One or more engines to run: `feldera`, `ch`, `pg` (e.g. `--mode feldera ch` or `--mode feldera ch pg` for all three) |
| `--no-feldera` | off | Skip Feldera engine |
| `--no-clickhouse` | off | Skip ClickHouse engine |
| `--no-postgres` | off | Skip PostgreSQL engine |
| `--postgres-steps` | same as `--batches` | Limit PostgreSQL to this many steps (use 10 at large scale) |
| `--postgres-user` | `nina` | PostgreSQL username |
| `--max-rss-mb` | none | Feldera pipeline memory cap in MB (e.g. `80000` for 80 GB) |
| `--sequential` | off | Run engines one at a time for clean isolated timing |
| `--mock` | off | Simulate queries — no DB needed |

---

## SQL architecture

All three engines share the same logical fraud-detection pipeline:

```
TRANSACTION_WITH_DISTANCE        — join each transaction with customer home address
    ↓
TRANSACTION_WITH_AGGREGATES      — compute all rolling window aggregates (named WINDOW clauses)
    ↓
flagged_gift_card_burst_30d      — WHERE gift_count_30day >= GB30()
flagged_gift_card_burst_45d      — WHERE gift_count_45day >= GB45()
flagged_spend_velocity_7d        — WHERE txn_count_7day   >= SV7()
flagged_repeated_displacement    — WHERE disp_count_3day  >= DISP()
    ↓
fraud_alerts                     — UNION ALL of all four signal streams
    ↓
card_suspicion_score             — SUM of signal priorities per card
    ↓
fraud_alert_details              — final enriched output (one row per flagged card)
```

**ClickHouse note**: `WINDOW RANGE` bounds must be literal integer seconds. Window functions use
`toUnixTimestamp64Second(ts)` to support datasets with dates beyond 2106.

**PostgreSQL note**: Uses `INTERVAL '30 days'` RANGE windows. The CTE materializes once per query;
each of the four CTE scans reads from temp storage. Set `work_mem='8GB'` to keep it in memory.

### SQL file layout

| File | Engine | Purpose |
|------|--------|---------|
| `sql/clickhouse_tables.sql` | ClickHouse | `customers` and `transactions` DDL |
| `sql/clickhouse_views.sql` | ClickHouse | `fraud_signals_full` view — full O(N) pipeline |
| `sql/clickhouse_query.sql` | ClickHouse | `SELECT count(DISTINCT cc_num) FROM fraud_signals_full` |
| `sql/feldera_tables.sql` | Feldera | `CUSTOMER` and `TRANSACTION` DDL |
| `sql/feldera_views.sql` | Feldera | Full pipeline as incremental views + `fraud_alert_details` mat. view |
| `sql/feldera_query.sql` | Feldera | `SELECT COUNT(*) FROM fraud_alert_details` |
| `sql/postgres_tables.sql` | PostgreSQL | `customers` and `transactions` DDL with FK and index |
| `sql/postgres_views.sql` | PostgreSQL | `fraud_signals_full` view — full O(N) CTE pipeline |
| `sql/postgres_query.sql` | PostgreSQL | `SELECT COUNT(*) AS n_alerts FROM fraud_signals_full` |

### Source layout

```
src/
  constants.py          — thresholds, priorities, connection defaults for all engines
  gen_data.py           — generates customers.csv + transactions.csv + batches/
  scan_thresholds.py    — p-percentile threshold calibration from CSV data
  run_bench.py          — end-to-end: generate → calibrate → benchmark
  demo_runner.py        — benchmark loop (parallel or sequential engines)
  engine_base.py        — abstract engine interface
  engine_clickhouse.py  — ClickHouse full-scan engine
  engine_feldera.py     — Feldera IVM engine
  engine_postgres.py    — PostgreSQL full window-scan engine (COPY FROM STDIN bulk load)
sql/
  clickhouse_*.sql      — ClickHouse schema and query
  feldera_*.sql         — Feldera schema and query
  postgres_*.sql        — PostgreSQL schema and query
```

---

## Data generation

`src/gen_data.py` generates synthetic credit card data matching the schema used by all three engines:

- **Customers**: `cc_num`, `name`, `lat`, `long` (home coordinates)
- **Transactions**: `cc_num`, `ts`, `amt`, `category`, `shipping_lat`, `shipping_long`

Generated files:
```
data/<params>/
  customers.csv       — all customers (loaded once at setup)
  transactions.csv    — preload history
  batches/
    batch_001.csv     — streaming batches (pushed one at a time during benchmark)
    batch_002.csv
    ...
  thresholds.json     — cached p99 thresholds (auto-generated on first run)
```
