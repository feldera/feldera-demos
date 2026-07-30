# Concurrent Bootstrapping demo — agentic runbook

This file tells Claude Code how to drive the concurrent-bootstrapping demo end to end. Follow it
exactly. All Feldera interaction goes through the Feldera Python SDK (via `run.py`); do not call the
REST API directly.

The demo shows, side by side, what happens when you add new views to a running pipeline and restart:
- **conventional** bootstrap freezes the whole pipeline (every view, even untouched ones) until the
  new views finish building;
- **concurrent** bootstrap keeps existing views live on the incoming stream while the new views
  backfill in the background.

## Environment

- Feldera must be running on `http://localhost:8080` with a build that supports concurrent
  bootstrapping (platform >= 0.323 / enterprise).
- The Python SDK must be importable. `concurrent_bootstrap` is only in the >= 0.323 SDK (PyPI is
  behind), so use the local checkout on `PYTHONPATH`. Define a shell alias for every command below:

  ```bash
  cd concurrent-bootstrapping
  export PYTHONPATH="$HOME/projects/feldera/python"
  PY="uv run --no-project --with pyarrow run.py"      # or: python run.py, with feldera importable
  ```

## Steps

1. **Backfill both pipelines** (one-time; reused across repeated demos). Takes ~1 min for the default
   20M events per pipeline. Bump `--size` for a longer, more dramatic bootstrap (~20M ≈ 25-30s,
   ~60M ≈ 85s):

   ```bash
   $PY setup            # add --size N to change the backfill
   ```

2. **Start the dashboard** in the background and tell the user to open it:

   ```bash
   $PY serve            # serves http://localhost:8090
   ```

   Open `http://localhost:8090`. Both columns should show `RUNNING`… actually both are `STOPPED`
   after setup — that's expected; they start the instant you bootstrap in the next step.

3. **Fire the bootstrap.** This stages the new views on both pipelines and restarts them — the left
   with conventional bootstrap, the right with concurrent:

   ```bash
   $PY bootstrap
   ```

4. **Narrate what the dashboard shows** while the bootstrap runs (~25-30s at the default size):

   | | bootstrap-conventional (left) | bootstrap-concurrent (right) |
   |--|-------------------------------|------------------------------|
   | runtime banner | `BOOTSTRAPPING` (red) | `CONCURRENT BOOTSTRAP` (blue) → `SYNCHRONIZING` |
   | ingest gauge | drops to **0 — PAUSED** | stays up — **LIVE** (~2.5k rec/s), sparkline moves |
   | existing view panels (`orders`) | **🔴 FROZEN** — counts stop | **🟢 LIVE** — counts keep climbing |
   | new view panels (`clickstream`) | ⏳ bootstrapping… | ⏳ bootstrapping… |
   | when it finishes | everything jumps at once | new panels light up; existing never paused |

   The point: conventional stops the world even for the `orders` views the change never touched;
   concurrent keeps ingesting and the `orders` panels keep ticking up the whole time (ad-hoc queries
   hit the serving circuit and return correct, live values), while the new `clickstream` views build
   in the background.

5. **Repeat if asked.** Roll both pipelines back to the pre-bootstrap checkpoint and fire again:

   ```bash
   $PY reset
   $PY bootstrap
   ```

6. **Clean up** when done:

   ```bash
   $PY teardown
   ```

## Notes / gotchas (already handled by `run.py`, do not undo)

- All tables are `materialized = 'true'` and carry no `LATENESS` — both are required for bootstrapping.
- The new views are added without touching any table, which is what concurrent bootstrap requires.
- The existing views aggregate `orders` (not `clickstream`), so they stay independent of the new
  views and remain live and queryable during a concurrent bootstrap.
- Dashboard query policy: it queries the existing views during a concurrent bootstrap (correct live
  values) and while running, but skips them during the conventional stop-the-world (queries block
  there); new views are queried only once running. Liveness is also shown by the ingest gauge, whose
  throughput is smoothed over a rolling window so the LIVE/FROZEN badge doesn't flicker.
