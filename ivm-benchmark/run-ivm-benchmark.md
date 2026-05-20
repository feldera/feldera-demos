---
description: Run the IVM Benchmark — Feldera vs ClickHouse fraud detection, side by side.
---

# IVM Benchmark — Feldera vs ClickHouse

Before starting, show the user this overview:

> **Feldera IVM Benchmark**
>
> This benchmark runs three fraud-detection engines side by side on a live transaction stream:
> **CH-full** (ClickHouse full columnar scan), **CH-light** (ClickHouse Materialized Views), and **Feldera** (Incremental View Maintenance).
>
> The story: ClickHouse full-scan latency grows O(N) as history accumulates.
> ClickHouse Materialized Views reduce that to O(delta) — but hit a hard limit:
> MVs cannot JOIN other tables at refresh time, so one fraud signal is structurally impossible.
> Feldera's IVM handles cross-table JOINs incrementally, staying fast *and* complete.

Then show the plan:

```
┌──────────────────────────────────────────────────────────┐
│                     IVM BENCHMARK                        │
│                                                          │
│  Step 1 ──► Check prerequisites                          │
│                   │                                      │
│  Step 2 ──► Choose demo mode                             │
│                   │                                      │
│  Step 3 ──► Start services (or mock mode)                │
│                   │                                      │
│  Step 4 ──► Run the benchmark                            │
│                   │                                      │
│  Step 5 ──► Interpret results                            │
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

## Step 2: Choose demo mode

Ask the user which story they want to tell:

> **Which benchmark mode?**
>
> - `latency` — Speed story: CH-full O(N) scan vs Feldera O(delta) IVM (2 engines)
> - `accuracy` — Completeness story: CH-light 3-signal MV vs Feldera 4-signal IVM (2 engines)
> - `full` — Both stories at once: all three engines side by side (default)
>
> Also ask: **Mock mode** (no database needed, simulated data) or **real mode** (live ClickHouse + Feldera)?
>
> Type the mode name, or press Enter for `full` + `real`.

Wait for the user's answer before continuing.

---

## Step 3: Start services

### Mock mode

No services needed — skip to Step 4 with `--mock`.

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

## Step 4: Run the benchmark

Assemble the command from the user's choices in Step 2.

### Base command

```bash
cd /home/nina/projects/feldera-demos/ivm-benchmark
python3 demo_runner.py [OPTIONS]
```

### Option mapping

| User choice | Flag(s) to add |
|-------------|---------------|
| mock mode | `--mock` |
| mode = `latency` | `--mode latency` |
| mode = `accuracy` | `--mode accuracy` |
| mode = `full` | _(default, no flag needed)_ |
| no display (remote/headless) | `--no-plot` |
| save results | `--output results.txt` |

### Suggested starter commands

**Quick demo (mock, no DB needed):**
```bash
python3 demo_runner.py --mock --mode full
```

**Headless on remote machine:**
```bash
python3 demo_runner.py --mock --mode full --no-plot --output results.txt
```

**Real mode, latency story:**
```bash
python3 demo_runner.py --mode latency --preload-days 30 --steps 40
```

**Real mode, full story, save output:**
```bash
python3 demo_runner.py --mode full --preload-days 30 --steps 40 --no-plot --output results.txt
```

Tell the user the command you are about to run, then execute it. Run it in the foreground so the user can see the output.

---

## Step 5: Interpret results

After the run completes, explain the summary table to the user:

> **Reading the results:**
>
> - `ins` — time to push the batch into the engine
> - `ref+qry` — for CH engines: full recompute happens here at query time; for Feldera: `ref` = IVM incremental commit, `qry` = trivial view read
> - `total` = `ins + ref + qry` — the fair end-to-end comparison
>
> **What to look for:**
>
> **Latency story** (`CH-full` vs `Feldera`): As step number increases and history accumulates, CH-full's `ref+qry` grows (O(N) scan). Feldera's stays flat (O(delta) IVM).
>
> **Accuracy story** (`CH-light` vs `Feldera`): CH-light shows more alerts (lower thresholds compensate for the missing signal). Feldera detects all 4 signals including `repeated_displacement`, which requires a JOIN to the `customers` table — structurally impossible for ClickHouse Materialized Views.

Point out the key numbers from the actual output: which engine was fastest, what the latency gap was, and how many alerts each engine found.

---

## Signal reference

| Signal | Definition | CH-full | CH-light | Feldera |
|--------|-----------|---------|----------|---------|
| `gift_card_burst_30d` | 7+ gift card txns in 30-day window | ✓ | ✓ MV-backed | ✓ IVM |
| `gift_card_burst_90d` | 11+ gift card txns in 90-day window | ✓ | ✓ MV-backed | ✓ IVM |
| `spend_velocity_7d` | 10+ txns in 7-day window | ✓ | ✓ MV-backed | ✓ IVM |
| `repeated_displacement` | 7+ txns > 0.5° from home in 3-day window | ✓ | **✗ impossible** | ✓ IVM |

The `repeated_displacement` signal requires computing distance from the cardholder's home address (`customers` table). ClickHouse MVs fire at INSERT time and cannot JOIN another table — this signal is architecturally unavailable in CH-light.

---

## Wrap-up

After interpreting results, offer:

> **Want to go deeper?**
>
> - `--data-dir data/1x` — 10× more history; latency gap becomes dramatic
> - `--data-dir data/10x` — 100× history; maximum latency gap
> - `--steps 80` — longer run to see the O(N) curve clearly
>
> Or run a full experiment sweep:
> ```bash
> python3 run_experiments.py
> ```
