# Precise and Fast: Fraud Detection at Streaming Scale

Every second matters in card fraud. A stolen card number goes on sale in underground markets within hours of a breach. Automated bots buy gift cards — anonymous, instantly redeemable, impossible to reverse — to drain the balance before the victim notices. By the time a fraud analyst opens a ticket, the money is gone.

The response is to flag suspicious transactions as they arrive and route them for review. But review has a cost. Whether a human analyst looks at a case or an LLM makes the call, every alert you generate is a unit of work — and false positives are wasted spend.

**The numbers add up fast.** A useful Claude Sonnet triage call costs roughly $0.002 — a single flagged transaction is not enough context for a good decision, so the prompt includes the full picture: the customer profile (home location, account age, spend patterns), the last 10–20 transactions as history, the flagged transaction itself, and the signal that triggered it. That adds up to about 1,500 input tokens. Add 100 output tokens for the decision and reason, and each call costs approximately $0.002 at Claude Sonnet pricing ($0.80/M input, $4.00/M output).

Now run the math at two common transaction volumes:

| | 100K transactions/hr | 1M transactions/hr |
|--|---------------------|----------------------|
| Precise detection — 1% alert rate | 1,000 calls/hr · **$2.00/hr** · $17,520/yr | 10,000 calls/hr · **$20.00/hr** · $175,200/yr |
| Approximate detection — 3% alert rate | 3,000 calls/hr · **$6.00/hr** · $52,560/yr | 30,000 calls/hr · **$60.00/hr** · $525,600/yr |
| **Extra cost of false positives** | +$35,040/yr | **+$350,400/yr** |

The gap is not the detection engine cost — it's the downstream review cost that approximate detection silently inflates. At 1M transactions per hour, imprecise fraud signals generate roughly **$350,000 per year in unnecessary LLM calls** before a single analyst touches a case.

You want the alert set to be small and confident.

**This is the core tension.** You want queries complex enough to be accurate, and you want them to run fast on a live stream.

Three strategies compete here:

- **CH-full** runs the full, accurate detection query on every batch — but it scans all history every time. Query time grows O(N) as history accumulates. Accurate, but too slow to keep up with a real-time stream.

- **CH-light** uses ClickHouse Materialized Views to maintain pre-aggregated counts incrementally, so each batch costs O(delta). Fast — but ClickHouse MVs can only see the incoming row at refresh time, not other tables. The displacement signal needs to join against customer home addresses; without that join, CH-light falls back to flagging any transaction with a non-zero shipping address. The result is a larger alert set with more false positives. Every extra false positive is a wasted review call.

- **Feldera** maintains the full computation graph — cross-table joins included — incrementally over each delta. The same precise queries that CH-full runs in O(N) now run in O(delta), and the final count query is O(1) against a precomputed single-row view. Fast and accurate: the alert set stays small, confident, and cheap to review downstream.

This benchmark measures both dimensions — latency and alert count — across all three engines on the same transaction stream.

---

## The Fraud Signals

Four patterns show up consistently in payment fraud. Each one tells a specific story about how stolen card data gets monetized.

### Gift card burst — 30-day and 45-day windows

A stolen card rarely gets maxed out with a single large purchase — that triggers immediate block. Instead, fraud rings buy a steady stream of gift cards: $50 here, $100 there, sometimes a dozen transactions a day across different merchants. Gift cards are the preferred exit because they don't require shipping, can't be charged back once activated, and are resold instantly on secondary markets.

The signal is a count threshold: **N or more gift card transactions within a 30-day window** (or 45 days for slower-moving rings). A legitimate cardholder might buy a handful of gift cards around the holidays. A compromised card hits that threshold in days.

### Spend velocity — 7-day window

Fraud rings don't just buy gift cards. Once they have working card credentials, they test and exhaust them across many categories — electronics, travel, subscription services. The velocity pattern captures this: **N or more transactions of any kind within 7 days**. Normal cardholders have a rhythm. Compromised cards don't.

### Repeated displacement

This signal catches a different threat: card-present skimming. A skimmer clones your physical card and uses it at locations far from where you live. The signal: **N or more transactions more than 0.5 degrees (roughly 35 miles) from the cardholder's home address within a 3-day window**.

This one is computationally interesting. To check displacement, you need to JOIN the transaction against the customer table to get the home address. A naive materialized view can't do that join at insert time — it can only see the incoming row, not the customer record. So it falls back to an approximation: flag any transaction that has a non-zero shipping address. This produces false positives whenever someone ships a gift to another city. Exact detection requires joining the customer table on every incoming transaction — which is exactly what Feldera's incremental view maintenance handles natively.

---

## Why This Is Hard to Do Fast

The naive approach runs a window query over the full transaction history on every new batch. That query is O(N) — it grows linearly with the number of rows. On a small table it's instant. On six months of history across millions of cardholders, it takes seconds per batch.

The common fix is a materialized view: pre-aggregate the data into buckets and update the buckets incrementally as new rows arrive. ClickHouse does this with `SummingMergeTree` materialized views. The problem is that ClickHouse MVs fire at INSERT time, in isolation — they can't join other tables during refresh. That's the limitation that forces the displacement signal to approximate.

Feldera takes a different approach. It maintains the full computation graph incrementally — including cross-table joins. When a new transaction arrives, Feldera propagates only the delta through every view that depends on it: the distance join, the windowed aggregates, the alert counts. The query time is O(delta), and the final count query hits a single precomputed row in O(1) regardless of history size.

---

## The Three Engines

### CH-full — ClickHouse full recompute

New rows are INSERTed, then a full window query scans all history on every batch.

- Detects all 4 signals with exact sliding-window semantics
- Query time grows **O(N)** with total history

### CH-light — ClickHouse with Materialized Views

Four `SummingMergeTree` MVs maintain pre-aggregated counts. Queries read from the MVs, so detection latency is O(delta) — but with two accuracy compromises:

1. **Window alignment**: MVs use epoch-aligned buckets, not sliding windows. A burst that straddles a bucket boundary can be missed.
2. **No distance check**: The displacement signal can't join the customer table at MV refresh time, so it flags any transaction with non-zero shipping coordinates — a worst-case over-approximation that inflates false positives.

Both CH engines use the same thresholds. Because CH-light's approximations over-count, it flags more cards at identical settings — the accuracy demo makes this visible.

- Detects all 4 signals — but `repeated_displacement` is approximate
- Query time: **O(delta)** — reads pre-aggregated MVs

### Feldera — Incremental View Maintenance

Each batch is wrapped in a transaction: push rows → commit. On commit, Feldera incrementally updates the full computation graph — cross-table joins included — processing only the new delta. After commit, `SELECT n_alerts FROM fraud_alert_count` reads a single precomputed count.

- Detects all 4 signals with exact semantics including distance check
- Refresh time: **O(delta)** — IVM over new rows only
- Query time: **O(1)** — reads a precomputed single-row count

---

## Signal accuracy comparison

| Signal | CH-full | CH-light | Feldera |
|--------|---------|----------|---------|
| `gift_card_burst_30d` | exact sliding window | epoch bucket (may miss cross-boundary bursts) | epoch bucket (matches CH-full) |
| `gift_card_burst_45d` | exact sliding window | epoch bucket | epoch bucket (matches CH-full) |
| `spend_velocity_7d` | exact sliding window | epoch bucket | epoch bucket (matches CH-full) |
| `repeated_displacement` | exact distance JOIN | no distance check — over-approximation | exact distance JOIN |

---

## Fraud signal definitions

| Signal | Definition |
|--------|-----------|
| `gift_card_burst_30d` | N+ gift card transactions in any 30-day sliding window |
| `gift_card_burst_45d` | N+ gift card transactions in any 45-day sliding window |
| `spend_velocity_7d` | N+ transactions (any category) in any 7-day sliding window |
| `repeated_displacement` | N+ transactions > 0.5° from home address in any 3-day window |

Thresholds scale with dataset size (see `THRESHOLD_PROFILES` in `constants.py`):

| Scale | gb30 | gb45 | sv7 | disp |
|-------|------|------|-----|------|
| `0.1x` | 7 | 8 | 10 | 7 |
| `1x` | 14 | 16 | 18 | 13 |
| `10x` | 14 | 16 | 18 | 13 |

---

## Recency filter

All three engines apply a recency filter: only cards whose most recent transaction falls within **2 hours** of the global dataset max timestamp are counted. This ensures per-step alert counts reflect currently-active fraud rather than the full cumulative history.

The **representative transaction** for each flagged card is its most recent transaction (`MAX(ts)` per card). CH-full and Feldera both use this for the recency join.

---

## Timing model

Each step is measured in three phases:

| Column | CH-full / CH-light | Feldera |
|--------|-------------------|---------|
| `ins` | INSERT rows into ClickHouse | push rows inside `start_transaction` / before `commit_transaction` |
| `ref` | — (MV update is inside `ins`) | IVM: incremental view maintenance triggered by commit |
| `qry` | `SELECT n_alerts FROM fraud_alert_count_*` — full recompute | `SELECT n_alerts FROM fraud_alert_count` — reads precomputed single row |
| **total** | `ins + qry` | `ins + ref + qry` |

`total` is the right comparison metric. For CH, all computation happens at query time. For Feldera, computation happens at commit (`ref`) and `qry` is O(1). Both represent end-to-end latency from "data pushed" to "results available".

---

## Demo modes

| Mode | Engines | Story |
|------|---------|-------|
| `full` (default) | CH-full, CH-light, Feldera | All three side by side |
| `latency` | CH-full, Feldera | Speed: O(N) scan vs O(delta) IVM |
| `accuracy` | CH-light, Feldera | Completeness: approximate MVs vs exact IVM |

---

## SQL file layout

| File | Purpose |
|------|---------|
| `setup_clickhouse.sql` | DDL: `customers` and `transactions` tables |
| `setup_clickhouse_mv.sql` | DDL: `SummingMergeTree` MV backing tables + MVs |
| `ch_full_head.sql` | CH-full view DDL — RANGE signal CTEs + `best_txn` with distance JOIN |
| `ch_light_head.sql` | CH-light view DDL — MV signal CTEs + `best_txn` (no distance check) |
| `ch_view_tail.sql` | Shared view tail — `best_signals` CTE + final SELECT with 2h recency filter |
| `ch_full_query.sql` | `SELECT n_alerts FROM fraud_alert_count_full` |
| `ch_light_query.sql` | `SELECT n_alerts FROM fraud_alert_count_light` |
| `replay_at_feldera.sql` | Full Feldera pipeline — tables, epoch GROUP BY signal views, `fraud_alert_count` |
| `feldera_query.sql` | `SELECT n_alerts FROM fraud_alert_count` |

The CH view DDL is split into a per-engine head and a shared tail. Python concatenates them at setup time, substituting threshold placeholders (`__GB30__`, `__GB45__`, `__SV7__`, `__DISP__`).

In `replay_at_feldera.sql`, all intermediate views are plain `CREATE VIEW`. Only `fraud_alert_count` is `CREATE MATERIALIZED VIEW` — it's the only view queried externally, and materialization gives the O(1) count read.

---

## Prerequisites

```bash
pip install "feldera>=0.298" clickhouse-connect matplotlib python-dotenv requests
```

Start both services (existing Docker containers):

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

No API key needed for local Docker.

---

## Quick start — mock mode (no DB needed)

```bash
python3 demo_runner.py --mock
python3 demo_runner.py --mock --mode latency   # speed story
python3 demo_runner.py --mock --mode accuracy  # completeness story
python3 demo_runner.py --mock --steps 10 --output results.txt
```

---

## Real mode

### Data scales

| Scale | Transactions | Use |
|-------|-------------|-----|
| `data/0.1x` | ~600K rows | Quick smoke test |
| `data/1x` | ~6M rows | Standard demo |
| `data/10x` | ~60M rows | Maximum latency gap |

### Run

```bash
# Smoke test: all three engines, 0.1x data
python3 demo_runner.py --data-dir data/0.1x --interval 0

# Standard benchmark with output file (1x data)
python3 demo_runner.py --data-dir data/1x --interval 0 --output results.txt

# Max latency gap
python3 demo_runner.py --data-dir data/10x --interval 0

# Plot results from saved output
python3 plot_results.py results.txt
```

---

## Key arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mock` | off | Simulate queries; no DB needed |
| `--output` | none | Save summary table to file |
| `--mode` | `full` | `full` \| `latency` \| `accuracy` |
| `--data-dir` | `data/0.1x` | Dataset scale directory |
| `--steps` | `50` | Number of streaming batches |
| `--interval` | `10` | Seconds between batches |
| `--preload-days` | `0` | Days of history loaded before streaming starts |
| `--no-ch` | off | Run Feldera only |
| `--no-feldera` | off | Run ClickHouse engines only |
| `--ch-host` | `localhost` | ClickHouse host |
| `--ch-port` | `8123` | ClickHouse HTTP port |
| `--ch-database` | `fraud_detection` | ClickHouse database name |
| `--api-url` | `http://localhost:8080` | Feldera host URL |
| `--api-key` | none | Feldera API key (not needed for local Docker) |

---

## Output

After the run, a per-step summary table prints to the terminal:

```
  PRELOAD  CH-full: 205ms   Feldera: 14.8s (push=174ms, ivm=1.0s)
  STEP LATENCY SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  step  engine        ins  ref+qry      qry    total     n
──────────────────────────────────────────────────────────
     1  CH-full     158ms    46ms         —    204ms    45
        CH-light    158ms    32ms         —    190ms     0
        Feldera     281ms  1010ms        2ms  1293ms   101
──────────────────────────────────────────────────────────
   avg                ins  ref+qry      qry    total
──────────────────────────────────────────────────────────
        CH-full     158ms   120ms         —    279ms
        CH-light    158ms   112ms         —    270ms
        Feldera     283ms  1010ms        2ms  1296ms
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

- `ins`: time to push the batch into the engine
- `ref+qry`: for CH — full recompute at query time; for Feldera — `ref` = IVM commit time, `qry` = O(1) count read
- `n`: new fraud alerts detected this step
- `total`: end-to-end latency — the primary comparison metric
