#!/usr/bin/env python3
"""
engine_feldera.py — Feldera SDK wrapper + FraudEngine implementation.

All Feldera interactions go through the high-level feldera.pipeline.Pipeline API.

Timing model inside push_step():

  _t_start      — before start_transaction()
  _t_data_ready — after all chunks pushed (data buffered in Feldera)
  _t_commit     — after commit_transaction() returns (IVM complete)

  insert_time()  = _t_data_ready - _t_start
  refresh_time() = _t_commit     - _t_data_ready
"""

import csv
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig

from constants import FELDERA_PIPELINE_NAME as _PIPELINE, SIGNAL_PRIORITY
from engine_base import FraudEngine

logging.getLogger("feldera").setLevel(logging.ERROR)

try:
    from dotenv import dotenv_values
    _env = dotenv_values(Path(__file__).parents[2] / ".env")
except ImportError:
    _env = {}

# ── Connection defaults ────────────────────────────────────────────────────────

DEFAULT_API_URL = _env.get("FELDERA_HOST", "http://localhost:8080").rstrip("/")
DEFAULT_API_KEY = _env.get("FELDERA_API_KEY", os.getenv("FELDERA_API_KEY", "")) or None

# ── SQL ────────────────────────────────────────────────────────────────────────

_SQL_DIR   = Path(__file__).parent / "sql"
_SQL_FILE  = _SQL_DIR / "replay_at_feldera.sql"
_QUERY_SQL = (_SQL_DIR / "feldera_query.sql").read_text()

_CHUNK_SIZE   = 1_000_000   # rows per HTTP POST — keeps memory bounded during large preloads
_PUSH_WORKERS = 16          # parallel HTTP POST threads during push


# ── Low-level Feldera engine ───────────────────────────────────────────────────

class _FelderaEngine:
    """High-level Pipeline API wrapper: deploy, push, commit, query."""

    def __init__(self, api_url: str, api_key):
        self._client       = FelderaClient(url=api_url, api_key=api_key, timeout=None)
        self._pipeline     = None          # feldera.pipeline.Pipeline
        self._txn_id       = None
        self._t_start      = 0.0
        self._t_data_ready = 0.0
        self._t_commit     = 0.0

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def setup(self, sql: str, runtime_config: RuntimeConfig | None = None) -> None:
        cfg = runtime_config or RuntimeConfig(
            min_batch_size_records=0,
            max_buffering_delay_usecs=0,
            storage=False,
        )
        self._pipeline = (
            PipelineBuilder(self._client, _PIPELINE, sql=sql, runtime_config=cfg)
            .create_or_replace()
        )
        self._pipeline.start(wait=True)

    # ── Data push ──────────────────────────────────────────────────────────────

    def _push_chunk(self, table: str, chunk: list[dict], update_format: str) -> None:
        for attempt in range(5):
            try:
                self._pipeline.input_json(
                    table, chunk,
                    update_format=update_format,
                    force=True, wait=True,
                )
                return
            except Exception:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def push(self, table: str, rows: list[dict], update_format: str = "raw") -> None:
        if not rows:
            return
        chunks = [rows[i: i + _CHUNK_SIZE] for i in range(0, len(rows), _CHUNK_SIZE)]
        with ThreadPoolExecutor(max_workers=_PUSH_WORKERS) as pool:
            futs = [pool.submit(self._push_chunk, table, c, update_format) for c in chunks]
            for f in as_completed(futs):
                f.result()

    # ── Transaction ────────────────────────────────────────────────────────────

    def start_transaction(self) -> None:
        self._t_start = time.perf_counter()
        self._txn_id  = self._pipeline.start_transaction()

    def commit_transaction(self) -> None:
        # SDK polls at 1-second intervals — too coarse for latency measurement.
        # Commit without wait, then poll at 20 ms for accurate IVM timing.
        self._pipeline.commit_transaction(self._txn_id, wait=False)
        while True:
            stats = self._pipeline.client.get_pipeline_stats(self._pipeline.name)
            if stats["global_metrics"]["transaction_id"] != self._txn_id:
                break
            time.sleep(0.02)
        self._t_commit = time.perf_counter()

    def mark_data_ready(self) -> None:
        self._t_data_ready = time.perf_counter()

    @property
    def insert_time(self) -> float:
        return self._t_data_ready - self._t_start

    @property
    def refresh_time(self) -> float:
        return self._t_commit - self._t_data_ready

    # ── Query ──────────────────────────────────────────────────────────────────

    def query(self, sql: str) -> list[dict]:
        return list(self._pipeline.query(sql))


# ── Pipeline setup helpers ─────────────────────────────────────────────────────

def setup_and_start(api_url: str, api_key,
                    gb30: int, gb45: int, sv7: int, disp: int,
                    prio: dict) -> _FelderaEngine:
    """Deploy the fraud-detection SQL with substituted thresholds and priorities; return engine."""
    sql = (
        _SQL_FILE.read_text()
        .replace("HAVING COUNT(*) >= __GB30__", f"HAVING COUNT(*) >= {gb30}")
        .replace("HAVING COUNT(*) >= __GB45__", f"HAVING COUNT(*) >= {gb45}")
        .replace("HAVING COUNT(*) >= __SV7__",  f"HAVING COUNT(*) >= {sv7}")
        .replace("HAVING COUNT(*) >= __DISP__", f"HAVING COUNT(*) >= {disp}")
        .replace("__PRIO_GB30__", str(prio["gift_card_burst_30d"]))
        .replace("__PRIO_GB45__", str(prio["gift_card_burst_45d"]))
        .replace("__PRIO_SV7__",  str(prio["spend_velocity_7d"]))
        .replace("__PRIO_DISP__", str(prio["repeated_displacement"]))
    )
    print(f"[feldera] thresholds: gb30={gb30} gb45={gb45} sv7={sv7} disp={disp}")
    engine = _FelderaEngine(api_url, api_key)
    engine.setup(sql)
    print(f"[feldera] Pipeline '{_PIPELINE}' running.")
    return engine


def _read_customers(data_dir) -> list[dict]:
    rows = []
    with open(Path(data_dir) / "customers.csv", newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "cc_num": int(row["cc_num"]),
                "name":   row["name"],
                "lat":    float(row["lat"])  if row["lat"]  else None,
                "long":   float(row["long"]) if row["long"] else None,
            })
    return rows


# ── Query helper ───────────────────────────────────────────────────────────────

def select_from_feldera(engine: _FelderaEngine,
                        win_start: datetime, win_end: datetime,  # noqa: ARG001
                        limit: int = None) -> tuple[list[dict], float]:
    """Read pre-computed fraud alerts from the materialized view.

    Two query shapes:
      (a) count-only: single row with 'n_alerts' — synthesize N stub dicts
          with deterministic cc_nums so the demo's seen_cc_nums dedup reports
          the per-step delta instead of cumulative count.
      (b) full rows with cc_num/signal_type/...
    """
    t0      = time.perf_counter()
    rows    = engine.query(_QUERY_SQL)
    elapsed = time.perf_counter() - t0

    if not rows:
        return [], elapsed

    if len(rows) == 1 and "n_alerts" in rows[0] and "cc_num" not in rows[0]:
        n = int(rows[0]["n_alerts"])
        results = [{
            "cc_num":          -(i + 1),
            "ts":              win_start,
            "amt":             0.0,
            "category":        "unknown",
            "shipping_lat":    0.0,
            "shipping_long":   0.0,
            "distance":        0.0,
            "avg_7day":        0.0,
            "signal_type":     "count",
            "confidence":      "high",
            "review_priority": 0.0,
        } for i in range(n)]
        return (results[:limit] if limit else results), elapsed

    results = []
    for row in sorted(rows,
                      key=lambda r: SIGNAL_PRIORITY.get(r["signal_type"], 0) * 1000
                                    + min(float(r.get("amt") or 0), 9999),
                      reverse=True):
        results.append({
            "cc_num":          row["cc_num"],
            "ts":              row.get("ts", win_start),
            "amt":             float(row.get("amt") or row.get("alert_amt") or 0),
            "category":        row.get("category", "unknown"),
            "shipping_lat":    float(row.get("shipping_lat") or 0),
            "shipping_long":   float(row.get("shipping_long") or 0),
            "distance":        float(row.get("distance") or 0),
            "avg_7day":        float(row.get("avg_7day") or 0),
            "signal_type":     row["signal_type"],
            "confidence":      "high",
            "review_priority": SIGNAL_PRIORITY.get(row["signal_type"], 1) * 1000
                               + min(float(row.get("amt") or 0), 9999),
        })

    return (results[:limit] if limit else results), elapsed


# ── FraudEngine implementation ─────────────────────────────────────────────────

class FelderaFraudEngine(FraudEngine):
    sim_id     = 2
    name       = "Feldera"
    storage_id = "feldera"

    def __init__(self, api_url: str = DEFAULT_API_URL, api_key=DEFAULT_API_KEY):
        self._api_url        = api_url
        self._api_key        = api_key
        self._engine         = None
        self._preload_push_t = 0.0
        self._preload_ivm_t  = 0.0

    def preload_push_time(self) -> float:
        return self._preload_push_t

    def preload_ivm_time(self) -> float:
        return self._preload_ivm_t

    def setup(self, preload_path: "Path | None", data_dir: Path) -> None:
        import constants as _c
        self._engine = setup_and_start(
            self._api_url, self._api_key,
            gb30=_c.GIFT_BURST_30D_THRESHOLD,
            gb45=_c.GIFT_BURST_45D_THRESHOLD,
            sv7=_c.SPEND_VELOCITY_7D_THRESHOLD,
            disp=_c.DISPLACEMENT_THRESHOLD,
            prio=_c.SIGNAL_PRIORITY,
        )
        customers = _read_customers(data_dir)
        txn_rows  = _parse_rows(preload_path) if preload_path is not None else []

        t0 = time.perf_counter()
        self._engine.start_transaction()
        self._engine.push("CUSTOMER",    customers)
        self._engine.push("TRANSACTION", txn_rows)
        self._engine.mark_data_ready()
        t_push_done = time.perf_counter()
        self._engine.commit_transaction()
        t_commit_done = time.perf_counter()
        self._preload_push_t = t_push_done   - t0
        self._preload_ivm_t  = t_commit_done - t_push_done
        print(f"[feldera] {len(customers):,} customers + {len(txn_rows):,} preload rows"
              f"  push={self._preload_push_t:.1f}s  ivm={self._preload_ivm_t:.1f}s")

    def push_step(self, rows: list[dict]) -> None:
        self._engine.start_transaction()
        self._engine.push("TRANSACTION", rows)
        self._engine.mark_data_ready()
        self._engine.commit_transaction()

    def insert_time(self) -> float:
        return self._engine.insert_time

    def refresh_time(self) -> float:
        return self._engine.refresh_time

    def query(self, win_start: datetime, win_end: datetime) -> tuple[list[dict], float]:
        return select_from_feldera(self._engine, win_start, win_end)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_rows(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "category":      row["category"],
                "ts":            row["ts"],
                "amt":           float(row["amt"])           if row["amt"]           else None,
                "cc_num":        int(row["cc_num"]),
                "shipping_lat":  float(row["shipping_lat"])  if row["shipping_lat"]  else None,
                "shipping_long": float(row["shipping_long"]) if row["shipping_long"] else None,
            })
    return rows
