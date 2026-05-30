# Cost optimization

Shrink a Feldera pipeline's CPU and memory allocation once the initial
backfill is done — pay for backfill capacity only while you actually need it,
then run the steady state on a fraction of the resources.

## Flow

1. Create the pipeline (`tpch.sql`) with generous resources for backfill
   (4 GB / 4 cores by default).
2. Start the pipeline. Wait until every input connector has finished its
   initial snapshot — detected via `end_of_input` on each input endpoint
   (the equivalent of `delta_phase` on CDC sources, which the script has a
   commented-out branch for once we swap parquet → Delta Lake / S3).
3. Stop the pipeline with `force=False` so Feldera writes a checkpoint.
4. Patch the runtime config to the steady-state envelope (1 GB / 1 core).
5. Start the pipeline again. It resumes from the checkpoint and runs at the
   smaller cost.

The orchestration lives in [`run.py`](run.py). Resource sizes, pipeline
name, and worker count are constants at the top of the script.

## Prerequisites

- A reachable Feldera instance.
- TPC-H parquet files served at `http://localhost:8000/{lineitem,orders,part,customer,supplier,partsupp,nation,region}.parquet`
  — `tpch.sql` reads from those URLs. Generate them however you prefer
  (DuckDB's `tpch` extension, `dbgen`, or a copy from S3) and run
  `python -m http.server 8000` from the directory that holds them.
- [`uv`](https://docs.astral.sh/uv/) for running the script.

## Run

```bash
# point at your Feldera instance (defaults shown)
export FELDERA_HOST=http://localhost:8080
# export FELDERA_API_KEY=apikey:...      # only for remote/cloud instances

uv run cost-optimization/run.py
```

## What to look for

While the script runs you'll see, in order:

- `creating pipeline ... with backfill resources (4096 MB / 4 cores)`
- `starting pipeline (backfill phase)`
- progress lines like `3/8 done; waiting on [...]` until all input
  connectors report `end_of_input`
- `stopping pipeline gracefully (force=False) — Feldera will checkpoint`
- `patching runtime config to steady-state resources (1024 MB / 1 cores)`
- `restarting pipeline at steady-state cost — resumes from checkpoint`

After the run, the pipeline `cost-optimization-tpch` keeps running with the
smaller resource envelope.
