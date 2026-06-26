---
description: Multi-engine fraud demo — ONE detector, same thresholds on every engine; engines differ only in how faithfully they can compute the signals. You read the spec and each engine's constraint card and implement accordingly. No API key.
---

# Multi-Engine Fraud Detection — same detector, different signal fidelity

You (Claude Code) are the agent. There is **no API key** — you do the translation work in-session,
exactly like `agentic-fraud-detection`.

You do **not** begin knowing the signals, the thresholds, or what each engine can and cannot do. You
**read** all of that from the spec and the per-engine constraint cards under `spec/`, and work out the
implementation yourself. Don't pre-judge the results or quote expected numbers — run the steps and
report what they actually show.

Before starting, show the user this overview:

> **Same detector, same thresholds, three engines — only the signal fidelity differs.**
>
> One fraud detector, defined once in the spec, runs on Feldera, ClickHouse, and Postgres at the
> **same thresholds**. The engines differ only in **how faithfully each can compute the detector's
> signals** incrementally — some an engine maintains exactly, some only approximately, some not at
> all. Those differences surface as different precision/recall and, downstream, different dollar
> cost: an LLM analyzes **every flagged transaction**, so the more an engine flags, the higher its
> bill. The spec defines the signals; each engine's constraint card defines its limits; the run
> reveals the gap — don't pre-judge which way.

Then show the plan:

```
┌───────────────────────────────────────────────────────────────┐
│  PREPARATION                                                  │
│   Step 1 ─► Verify engines (PostgreSQL · ClickHouse · Feldera)│
│   Step 2 ─► Generate the dataset                              │
│   Step 3 ─► Read the spec (signals + thresholds)              │
│  THE AGENT                                                    │
│   Step 4 ─► Write the detector per engine (honor each card)   │
│   Step 5 ─► Stream batches; score precision / recall / FP     │
│   Step 6 ─► Price each engine's flags across frontier models  │
│   Step 7 ─► Plot workload/step + cumulative LLM cost          │
└───────────────────────────────────────────────────────────────┘
```

All commands run from `agentic-tokens-demo/`.

---

## Prerequisites

The demo reuses the engine adapters and data generator from the sibling **`rolling-aggregate-bench/src/`**,
so that directory must sit next to this one.

### Python  (3.10+)

```bash
pip install "feldera>=0.298" clickhouse-connect psycopg2-binary python-dotenv matplotlib
```
`clickhouse-connect`, `psycopg2-binary`, and `python-dotenv` are imported by the shared engine
adapters even for a single-engine run; `matplotlib` is used by `src/plot.py` (Step 7).

### Feldera  — HTTP API on `:8080`

```bash
docker start feldera 2>/dev/null || docker run -d --name feldera -p 8080:8080 \
  images.feldera.com/feldera/pipeline-manager:latest
```

### ClickHouse  — HTTP on `:8123`, connected as user `demo` / database `fraud_detection_light`

```bash
docker start clickhouse-server 2>/dev/null || docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
```
The runner connects as user **`demo`** (no password) to database **`fraud_detection_light`** — create
them once on a fresh server:

```bash
docker exec -i clickhouse-server clickhouse-client --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS fraud_detection_light;
CREATE USER IF NOT EXISTS demo IDENTIFIED WITH no_password;
GRANT ALL ON fraud_detection_light.* TO demo;
SQL
```
(If `CREATE USER` is denied, enable access management for the `default` user —
`<access_management>1</access_management>` in `users.xml` — or provision `demo` via your server's user config.)

### PostgreSQL  — native, Unix socket `/var/run/postgresql`, database `tokens_demo`

Installed **natively** (not Docker) so the runner can bulk-load via `COPY FROM STDIN` over the local
socket. Create the database once (the runner creates and drops the tables itself):

```bash
createdb tokens_demo
```
The runner connects over the local socket as your current OS user (Postgres peer auth); set `PGUSER`
to override. Ensure that role can access `tokens_demo` (running `createdb` as that user grants it).

---

## Step 1: Verify engines

```bash
curl -sf http://localhost:8080/v0/config >/dev/null && echo "Feldera: up" || echo "Feldera: DOWN"
psql -d postgres -c "SELECT 1" >/dev/null 2>&1 && echo "PostgreSQL: up" || echo "PostgreSQL: DOWN"
curl -sf 'http://localhost:8123/?user=demo&query=SELECT%201' >/dev/null && echo "ClickHouse: up" || echo "ClickHouse: DOWN"
```
Do not proceed until all three respond.

## Step 2: Generate the dataset

```bash
python3 src/gen_demo_data.py --out-dir data/demo --customers 200000 --borderline 10000 \
    --batches 90 --batch-rows 20000 --stream-days 90
```
Writes **90 days at 1 day per batch** (Step 5 then preloads 30 days as history and measures the next
60). `data/demo/` gets `customers.csv`, the streamed `batches/`, and `labels.csv` (the ground truth —
an input to the **scorer**, never to your detectors); the per-engine table contract goes to
`generated/schema.<engine>.sql` (all SQL lives in `generated/`, separate from the data). Write your
detectors from the **spec**, not by inspecting the planted data.

## Step 3: Read the spec

Read **`spec/fraud_spec.md`** — it defines the signals the detector flags on and their thresholds; and
**`spec/schema.md`** for the column contract. These files are the source of truth — don't reproduce
them here. The thresholds are identical for every engine; what varies (Step 4) is whether an engine
can compute each signal exactly, approximately, or not at all.

---

## Step 4: Write the detector per engine  ✍️  (this is the agent's job)

For **each** engine — `feldera`, `postgres_ivm`, `clickhouse_ivm` — read:

- its capability card **`spec/constraints/<engine>.md`** — what that engine can and cannot maintain
  incrementally, and
- the table contract at the top of **`generated/schema.<engine>.sql`**,

then write **`generated/<engine>.sql`** implementing the spec's signals **as faithfully as that engine
allows** — exactly where it can, with an approximation where it must, and dropping a signal it cannot
express at all. The card states the limits; **you** decide the implementation. Each detector must
expose a **`flagged_card`** view returning the set of suspicious `cc_num`.

Then compose the detectors into the per-engine schema files (tables + views) — the program the runner
actually executes:

```bash
python3 src/gen_demo_data.py --schema-only
```

Announce, per engine, which signals you implemented exactly, which approximately, and which you had to
drop — and why, citing its card. After writing all three, pause:

> Detectors written — same thresholds everywhere, each honoring its engine's constraint card.
> ⏎ Type **next** to stream the data and score them.

## Step 5: Stream batches & score

```bash
python3 src/runner.py --data data/demo --preload 30 --steps 60
```
**Preloads 30 days of history** into each engine (bulk, unmeasured — so the rolling windows / buckets
are already warm and the measured window is active from step 1), then streams + measures the next
**60 days** (1 step = 1 day). It queries each engine's `flagged_card` after every measured batch
(per-step suspicious-transaction counts → `results/metrics.csv`) and scores the final
flagged set against `labels.csv` → `results/score.csv` (flagged, TP, FP, FN,
precision, recall, suspicious-txn and FP-txn counts). Report the table as printed; let the numbers
speak.

## Step 6: Price the analysis cost

```bash
python3 src/cost_estimate.py
```
Every flagged (suspicious) transaction is sent to an LLM agent for analysis (an agentic token profile
— ~12,000 in + 2,000 out, tunable with `--tokens-in/--tokens-out`) — you can't tell a false positive
from real fraud until you've analysed it, so **all flagged transactions cost money**. From
`results/score.csv` this prices the **total** analysis workload (`suspicious txns × $/txn`) **per engine
across frontier models** (Opus 4.8, Sonnet 4.6, Haiku 4.5, GPT-5.5, Gemini 3.1 Pro, DeepSeek-V4-Flash).
Each engine's bill scales with how many transactions it flags; the score's false-positive counts show
how much of each bill is analysing legitimate cards that were over-flagged.

## Step 7: Visualize — workload & cost over the stream

```bash
python3 src/plot.py
```
Writes `results/timeseries.png` — two stacked panels across the streaming steps: suspicious
transactions **per step** (the new LLM workload each batch) and **cumulative** LLM cost per engine.
To share it, embed the PNG in a self-contained HTML artifact (e.g. `results/plots.html`).

---

## Wrap-up

The runner best-effort stops the Feldera pipeline on exit (a full delete needs a storage clear, so a
Stopped `tokens-demo-runner` may linger — harmless; the next run recreates it). PostgreSQL `tokens_demo`
and the ClickHouse `tok_*` tables + `flagged_card` view are dropped and recreated on the next run.
`results/score.csv` (precision/recall + FP) and `results/timeseries.png` are the headline artifacts.
