# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "feldera>=0.292.0",
# ]
# ///
"""
Cost-optimization demo: shrink a Feldera pipeline's resource footprint
once backfill is complete.

Flow (matches cost-optimization/README.md):
  1. Create the pipeline with generous resources for the backfill phase.
  2. Start it; wait until every input connector has fully ingested its
     snapshot (end_of_input == True).
  3. Stop gracefully (force=False) so Feldera writes a checkpoint.
  4. Patch the runtime config to a smaller resource envelope.
  5. Start again — Feldera resumes from the checkpoint at steady-state cost.

Env:
  FELDERA_HOST     default http://localhost:8080
  FELDERA_API_KEY  optional
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig, Resources

PIPELINE_NAME = "cost-optimization-tpch"
SQL_FILE = Path(__file__).parent / "tpch.sql"

WORKERS = 4

BACKFILL_RESOURCES = Resources(
    cpu_cores_min=4,
    cpu_cores_max=4,
    memory_mb_min=4096,
    memory_mb_max=4096,
)

STEADY_RESOURCES = Resources(
    cpu_cores_min=1,
    cpu_cores_max=1,
    memory_mb_min=1024,
    memory_mb_max=1024,
)

POLL_INTERVAL_S = 2.0
BACKFILL_TIMEOUT_S = 60 * 30


def log(msg: str) -> None:
    print(f"[cost-opt] {msg}", flush=True)


def make_client() -> FelderaClient:
    host = os.environ.get("FELDERA_HOST", "http://localhost:8080")
    api_key = os.environ.get("FELDERA_API_KEY")
    log(f"connecting to {host}")
    return FelderaClient(host, api_key=api_key)


def backfill_complete(pipeline) -> tuple[bool, str]:
    """All input connectors have finished their initial snapshot."""
    stats = pipeline.stats()
    inputs = stats.inputs or []
    if not inputs:
        return False, "no input connectors reporting yet"

    pending = []
    for ep in inputs:
        m = ep.metrics
        end_of_input = bool(getattr(m, "end_of_input", False)) if m else False

        # Future CDC sources (e.g. Delta Lake / S3 with snapshot_and_follow)
        # will expose `delta_phase` instead of `end_of_input`. When we move
        # the connectors over, swap the check above for the block below:
        #
        # delta_phase = getattr(m, "delta_phase", None) if m else None
        # done = delta_phase in ("follow", "replay")  # past initial snapshot
        # if not done:
        #     pending.append(ep.endpoint_name)

        if not end_of_input:
            pending.append(ep.endpoint_name or "<unnamed>")

    if pending:
        return False, f"{len(inputs) - len(pending)}/{len(inputs)} done; waiting on {pending}"
    return True, f"all {len(inputs)} input connectors finished"


def wait_for_backfill(pipeline) -> None:
    log("waiting for backfill to complete...")
    deadline = time.monotonic() + BACKFILL_TIMEOUT_S
    last_msg = ""
    while time.monotonic() < deadline:
        done, msg = backfill_complete(pipeline)
        if msg != last_msg:
            log(msg)
            last_msg = msg
        if done:
            return
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(f"backfill did not complete within {BACKFILL_TIMEOUT_S}s")


def main() -> int:
    if not SQL_FILE.exists():
        log(f"missing SQL file: {SQL_FILE}")
        return 1
    sql = SQL_FILE.read_text()

    client = make_client()

    log(f"creating pipeline '{PIPELINE_NAME}' with backfill resources "
        f"({BACKFILL_RESOURCES.memory_mb_max} MB / {BACKFILL_RESOURCES.cpu_cores_max} cores)")
    pipeline = PipelineBuilder(
        client,
        name=PIPELINE_NAME,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=WORKERS,
            storage=True,
            resources=BACKFILL_RESOURCES,
        ),
    ).create_or_replace()

    log("starting pipeline (backfill phase)")
    pipeline.start()

    wait_for_backfill(pipeline)

    log("stopping pipeline gracefully (force=False) — Feldera will checkpoint")
    pipeline.stop(force=False)

    log(f"patching runtime config to steady-state resources "
        f"({STEADY_RESOURCES.memory_mb_max} MB / {STEADY_RESOURCES.cpu_cores_max} cores)")
    pipeline.set_runtime_config(
        RuntimeConfig(
            workers=WORKERS,
            storage=True,
            resources=STEADY_RESOURCES,
        )
    )

    log("restarting pipeline at steady-state cost — resumes from checkpoint")
    pipeline.start()

    # sanity check; should be done already
    status, msg = backfill_complete(pipeline)
    log(f"final check: backfill complete={status}; {msg}")

    log("done. pipeline is running with reduced resources.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
