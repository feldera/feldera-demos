---
description: Run the IVM Benchmark — Feldera vs ClickHouse fraud detection, side by side.
---

# IVM Benchmark — Feldera vs ClickHouse

Before starting, show the user this overview:

> **Feldera IVM Benchmark**
>
> This benchmark runs two fraud-detection engines side by side on a live transaction stream:
> **ClickHouse** (full columnar scan, O(N)) and **Feldera** (Incremental View Maintenance, O(delta)).
>
> Both engines detect all four fraud signals with exact true sliding-window semantics and produce
> identical alert counts. The story is purely about speed: ClickHouse query latency grows linearly
> as history accumulates; Feldera's IVM commit is O(delta) and the final query is always O(1).

Then show the plan:

```
┌──────────────────────────────────────────────────────────┐
│                     IVM BENCHMARK                        │
│                                                          │
│  Step 1 ──► Check prerequisites                          │
│                   │                                      │
│  Step 2 ──► Start services (or mock mode)                │
│                   │                                      │
│  Step 3 ──► Run the benchmark                            │
│                   │                                      │
│  Step 4 ──► Interpret results                            │
└──────────────────────────────────────────────────────────┘
```

---

## Step 1: Check prerequisites

Run silently, report only failures:

```bash
cd /home/nina/projects/feldera-demos/ivm-benchmark
python3 -c "import feldera, clickhouse_connect, matplotlib, dotenv, requests; print('OK')"
```

If any import fails, install the missing packages:

```bash
pip install feldera clickhouse-connect matplotlib python-dotenv requests
```

---

## Step 2: Start services

### Mock mode

No services needed — skip to Step 3 with `--mock`.

### Real mode

Check whether the Docker containers are already running:

```bash
docker ps --format '{{.Names}}' | grep -E 'clickhouse-server|feldera'
```

If both appear in the output, tell the user:

> Both services are running — proceeding.

If either is missing, start it:

```bash
# ClickHouse
docker start clickhouse-server 2>/dev/null || \
  docker run -d --name clickhouse-server \
    -p 8123:8123 -p 9000:9000 \
    clickhouse/clickhouse-server

# Feldera
docker start feldera 2>/dev/null || \
  docker run -d --name feldera -p 8080:8080 \
    images.feldera.com/feldera/pipeline-manager:latest
```

Wait 3 seconds, then verify both containers appear in `docker ps`.

---

## Step 3: Run the benchmark

Ask the user: **Mock mode** (no database needed, simulated latency) or **real mode** (live ClickHouse + Feldera)?

### Suggested commands

**Quick demo (mock, no DB needed):**
```bash
python3 demo_runner.py --mock --steps 20
```

**Real mode, smoke test (0.1x data):**
```bash
python3 demo_runner.py --data-dir data/0.1x --interval 0 --steps 10 --sequential
```

**Real mode, standard benchmark (1x data with preloaded history):**
```bash
python3 demo_runner.py --data-dir data/1x --preload-rows 3000000 --steps 500 --batch-rows 2000 --interval 0 --output results.txt
```

**Feldera only:**
```bash
python3 demo_runner.py --no-clickhouse --data-dir data/0.1x --interval 0
```

**ClickHouse only:**
```bash
python3 demo_runner.py --no-feldera --data-dir data/0.1x --interval 0
```

Tell the user the command you are about to run, then execute it in the foreground so the user can see the output.

---

## Step 4: Interpret results

After the run completes, explain the summary table to the user:

> **Reading the results:**
>
> - `ins` — time to push the batch into the engine
> - `ref+qry` — for ClickHouse: full O(N) recompute at query time; for Feldera: `ref` = IVM incremental commit, `qry` = O(1) read from precomputed `fraud_alert_details`
> - `total` = `ins + ref + qry` — the fair end-to-end comparison
> - `n` — new fraud alerts detected this step (identical for both engines)
>
> **What to look for:**
>
> As the step number increases and history accumulates, ClickHouse's `ref+qry` grows (O(N) scan).
> Feldera's stays flat — O(delta) IVM at commit, O(1) query. The gap widens with larger datasets.

Point out the key numbers from the actual output: which engine was fastest, what the latency gap was, and how many alerts were found.

---

## Signal reference

| Signal | Definition | Window |
|--------|-----------|--------|
| `gift_card_burst_30d` | 20+ gift card transactions | 30-day sliding |
| `gift_card_burst_45d` | 20+ gift card transactions | 45-day sliding |
| `spend_velocity_7d` | 20+ transactions (any category) | 7-day sliding |
| `repeated_displacement` | 10+ transactions > 20° from home | 3-day sliding |

All four signals use exact true sliding windows in both engines. Both engines join the `customers` table for the displacement distance check. Alert counts are identical.

Suspicion score per card = `SUM` of all fired signal priorities. Cards triggering multiple signals simultaneously rank higher in the review queue.

---

## Wrap-up

After interpreting results, offer:

> **Want to see a bigger gap?**
>
> - `--data-dir data/10x` — 10× more history; ClickHouse latency grows noticeably
> - `--data-dir data/100x` — 100× history; maximum latency gap
> - `--preload-rows 3000000` — pre-load 3M rows of history before streaming
> - `--steps 500 --batch-rows 2000` — long run to see the O(N) curve clearly
>
> Or run a full experiment sweep:
> ```bash
> python3 run_experiments.py --data-dir data/100x --preload-rows 3000000 --steps 500 --batch-rows 2000 --engines all
> ```
