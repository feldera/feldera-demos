# Agentic Tokens Demo — one detector, three engines, the cost of an inexact signal

The **same** fraud detector, at the **same thresholds**, runs on three engines. The only thing that
differs is how faithfully each engine can compute the detector's signals **incrementally** on a live
stream. Those fidelity differences turn into different accuracy — and, downstream, different dollar
cost, because **every flagged transaction is analysed by an LLM agent**, so over-flagging directly
runs up the bill (you can't tell a false positive from real fraud until you've paid to analyse it).

| Engine | How it maintains the detector's signals |
|--------|------------------------------------------|
| **Feldera** | Native IVM — exact trailing-window aggregates, ordered `LAG`, and exact distinct counts, kept fresh incrementally |
| **ClickHouse** | Real incremental `MATERIALIZED VIEW`s (`AggregatingMergeTree` + `countState`/`uniqState`) over **fixed** buckets; no ordered cross-row state |
| **Postgres** | No incremental view maintenance — a fixed-bucket recompute rollup kept inside a latency budget |

Each engine implements the same signals at the same thresholds, as faithfully as its mechanism allows.
The run reports what each actually flags — read the numbers off `results/score.csv`.

---

## Layout

```
agentic-tokens-demo/
├── src/                       gen_demo_data.py · runner.py · cost_estimate.py · plot.py
├── spec/                      fraud_spec.md · schema.md · constraints/<engine>.md  (the signals + each engine's limits)
├── generated/<engine>.sql     the per-engine detectors (committed; schema.<engine>.sql is composed at gen time)
├── data/                      generated dataset            (git-ignored, regenerable)
├── results/                   metrics.csv · score.csv · timeseries.png  (git-ignored, regenerable)
└── feldera-analyze-tokens.md  the agentic runbook (how Claude Code drives it; see "How it works")
```

---

## Prerequisites

### Python (3.10+)
```bash
pip install "feldera>=0.298" clickhouse-connect psycopg2-binary python-dotenv matplotlib
```

### Feldera — HTTP API on `:8080`
```bash
docker start feldera 2>/dev/null || docker run -d --name feldera -p 8080:8080 \
  images.feldera.com/feldera/pipeline-manager:latest
```

### ClickHouse — HTTP on `:8123`, user `demo` / database `fraud_detection_light`
```bash
docker start clickhouse-server 2>/dev/null || docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
docker exec -i clickhouse-server clickhouse-client --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS fraud_detection_light;
CREATE USER IF NOT EXISTS demo IDENTIFIED WITH no_password;
GRANT ALL ON fraud_detection_light.* TO demo;
SQL
```

### PostgreSQL — native (not Docker), Unix socket, database `tokens_demo`
Native so the runner can bulk-load via `COPY FROM STDIN` over the local socket.
```bash
createdb tokens_demo
```
The runner connects over the local socket as your current OS user (Postgres peer auth); set `PGUSER`
to override. Make sure that role can access `tokens_demo` (`createdb` as that user already grants it).

Verify all three are up:
```bash
curl -sf http://localhost:8080/v0/config >/dev/null && echo "Feldera: up"
psql -d postgres -c "SELECT 1" >/dev/null 2>&1 && echo "PostgreSQL: up"
curl -sf 'http://localhost:8123/?user=demo&query=SELECT%201' >/dev/null && echo "ClickHouse: up"
```

---

## Run it

All commands run from `agentic-tokens-demo/`.

```bash
# 1. Generate 90 days at 1 day/batch (also composes generated/schema.<engine>.sql from the committed
#    detectors). ~105 planted fraud cards + 10,000 borderline; Step 2 preloads 30 days, measures 60.
python3 src/gen_demo_data.py --out-dir data/demo --customers 200000 --borderline 10000 \
    --batches 90 --batch-rows 20000 --stream-days 90

# 2. Preload 30 days of history (unmeasured), then stream + measure the next 60 (1 step = 1 day),
#    scoring each engine's final flagged set vs ground truth.
python3 src/runner.py --data data/demo --preload 30 --steps 60

# 3. Price the total LLM analysis cost (every flagged txn × $/txn), across frontier models.
python3 src/cost_estimate.py

# 4. Plot suspicious-transactions-per-step + cumulative cost  ->  results/timeseries.png
python3 src/plot.py
```

**No-preload variant** (simpler; the first ~25 measured days are a quiet warm-up while cards
accumulate history):
```bash
python3 src/gen_demo_data.py --out-dir data/demo --customers 200000 --borderline 10000 \
    --batches 100 --batch-rows 20000
python3 src/runner.py --data data/demo --steps 100
python3 src/cost_estimate.py && python3 src/plot.py
```

> Run engines **one at a time** (the runner already does, in a single process). The Postgres,
> ClickHouse, and Feldera instances are shared — don't run two `runner.py` at once or they collide.

---

## What you get

- **`results/score.csv`** — per engine: flagged / TP / FP / FN / precision / recall / suspicious-txn
  & false-positive-txn counts, scored against `labels.csv`. Read the actual numbers off this file —
  the demo doesn't pre-state them.
- **`cost_estimate.py` output** — total LLM-analysis \$ per engine (every suspicious txn × $/txn)
  across Opus 4.8, Sonnet 4.6, Haiku 4.5, GPT-5.5, Gemini 3.1 Pro, DeepSeek-V4-Flash. Each engine's
  bill scales with how many transactions it flags; the score's false-positive counts show how much of
  a bill is analysing legitimate cards that were over-flagged.
- **`results/timeseries.png`** — top: suspicious transactions per step; bottom: cumulative LLM cost
  per engine, across the streaming steps.

To tweak: `gen_demo_data.py --help` (populations, scale, window) · `runner.py --preload/--steps` ·
`cost_estimate.py --tokens-in/--tokens-out`.

---

## How it works (the agentic angle)

This is the `agentic-fraud-detection` pattern with **no API key** — **Claude Code is the agent**. The
detectors in `generated/<engine>.sql` are written by reading the spec (`spec/fraud_spec.md`) and each
engine's capability card (`spec/constraints/<engine>.md`), then implementing the signals **as
faithfully as that engine allows**. The cards are grounded in the engines' own docs (ClickHouse
incremental/refreshable MV + `uniq`; Feldera IVM + `LATENESS`; PostgreSQL `REFRESH MATERIALIZED VIEW`),
so the constraints aren't invented. To re-derive the detectors from scratch, follow
**`feldera-analyze-tokens.md`** (or run `/run_tokens_demo`); after editing a detector, recompose with
`python3 src/gen_demo_data.py --schema-only`.
