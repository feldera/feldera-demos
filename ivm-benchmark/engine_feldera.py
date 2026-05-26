#!/usr/bin/env python3
"""
engine_feldera.py — Feldera SDK wrapper + FraudEngine implementation.

All Feldera interactions go through the high-level feldera.pipeline.Pipeline API.

Timing model inside push_step():

  _t_start      — before start_transaction()
  _t_data_ready — after wait_for_ingestion() returns (all rows in pipeline)
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

from constants import FELDERA_PIPELINE_NAME as _PIPELINE, SIGNAL_PRIORITY, feldera_functions_sql
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

_SQL_DIR  = Path(__file__).parent / "sql"
_TABLES_FILE = _SQL_DIR / "feldera_tables.sql"
_VIEWS_FILE  = _SQL_DIR / "feldera_views.sql"
_QUERY    = (_SQL_DIR / "feldera_query.sql").read_text().strip()

_CHUNK_SIZE   = 250_000   # rows per HTTP POST — keeps memory bounded during large preloads
_PUSH_WORKERS = 10          # parallel HTTP POST threads during push


# ── Low-level Feldera engine ───────────────────────────────────────────────────

class _FelderaEngine:
    """High-level Pipeline API wrapper: deploy, push, commit, query."""

    def __init__(self, api_url: str, api_key):
        self._client       = FelderaClient(url=api_url, api_key=api_key, timeout=None)
        self._pipeline     = None          # feldera.pipeline.Pipeline
        self._pre_count    = 0
        self._t_start      = 0.0
        self._t_data_ready = 0.0
        self._t_commit     = 0.0

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def setup(self, sql: str, runtime_config: RuntimeConfig | None = None) -> None:
        cfg = runtime_config or RuntimeConfig(
            workers=16
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
                    force=True, wait=False,
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
        self._t_start   = time.perf_counter()
        self._pre_count = self._pipeline.stats().global_metrics.total_processed_records
        self._pipeline.start_transaction()

    def wait_for_ingestion(self, n_rows: int) -> None:
        while True:
            cur = self._pipeline.stats().global_metrics.total_processed_records
            if cur - self._pre_count >= n_rows:
                break
            time.sleep(0.02)
        self._t_data_ready = time.perf_counter()

    def commit_transaction(self) -> None:
        # IVM already ran (wait_for_ingestion). We just need transaction_id to
        # advance. Bypass the SDK's 1s polling floor with our own 20ms loop.
        pre_txn = self._pipeline.client.get_pipeline_stats(self._pipeline.name)["global_metrics"]["transaction_id"]
        self._pipeline.commit_transaction(wait=False)
        while True:
            gm = self._pipeline.client.get_pipeline_stats(self._pipeline.name)["global_metrics"]
            if gm["transaction_id"] != pre_txn and gm.get("transaction_status") != "CommitInProgress":
                break
            time.sleep(0.02)
        self._t_commit = time.perf_counter()

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
    """Deploy the fraud-detection SQL; threshold/priority functions generated from constants."""
    sql = (
        _TABLES_FILE.read_text()
        + "\n" + feldera_functions_sql(gb30, gb45, sv7, disp, prio)
        + "\n" + _VIEWS_FILE.read_text()
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
                        win_start: datetime, win_end: datetime,
                        limit: int = None) -> tuple[list[dict], float]:
    """Count fraud alerts whose latest transaction falls within [win_start, win_end)."""
    sql = _QUERY
    t0      = time.perf_counter()
    rows    = engine.query(sql)
    elapsed = time.perf_counter() - t0

    if not rows:
        return [], elapsed

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


# ── FraudEngine implementation ─────────────────────────────────────────────────

class FelderaFraudEngine(FraudEngine):
    sim_id     = 1
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

        t0 = time.perf_counter()
        self._engine.start_transaction()
        self._engine.push("CUSTOMER", customers)
        n_txn = 0
        if preload_path is not None:
            n_txn = _stream_push(self._engine, preload_path)
        self._engine.wait_for_ingestion(len(customers) + n_txn)
        t_push_done = time.perf_counter()
        self._engine.commit_transaction()
        t_commit_done = time.perf_counter()
        self._preload_push_t = t_push_done   - t0
        self._preload_ivm_t  = t_commit_done - t_push_done
        print(f"[feldera] {len(customers):,} customers + {n_txn:,} preload rows"
              f"  push={self._preload_push_t:.1f}s  ivm={self._preload_ivm_t:.1f}s")

    def push_step(self, rows: list[dict]) -> None:
        self._engine.start_transaction()
        self._engine.push("TRANSACTION", rows)
        self._engine.wait_for_ingestion(len(rows))
        self._engine.commit_transaction()

    def insert_time(self) -> float:
        return self._engine.insert_time

    def refresh_time(self) -> float:
        return self._engine.refresh_time

    def query(self, win_start: datetime, win_end: datetime) -> tuple[list[dict], float]:
        return select_from_feldera(self._engine, win_start, win_end)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _stream_push(engine: _FelderaEngine, path: Path) -> int:
    """Stream-push a preload CSV into Feldera without loading it all into memory.

    Reads _CHUNK_SIZE rows at a time and submits each chunk to the engine's
    thread pool, keeping at most _PUSH_WORKERS chunks in flight simultaneously.
    Returns total row count pushed.
    """
    total = 0
    chunk: list[dict] = []
    pending = []

    def _flush(c):
        engine.push("TRANSACTION", c)

    with ThreadPoolExecutor(max_workers=_PUSH_WORKERS) as pool:
        with open(path, newline="") as f:
            for raw in csv.DictReader(f):
                chunk.append({
                    "category":      raw["category"],
                    "ts":            raw["ts"],
                    "amt":           float(raw["amt"])           if raw["amt"]           else None,
                    "cc_num":        int(raw["cc_num"]),
                    "shipping_lat":  float(raw["shipping_lat"])  if raw["shipping_lat"]  else None,
                    "shipping_long": float(raw["shipping_long"]) if raw["shipping_long"] else None,
                })
                if len(chunk) >= _CHUNK_SIZE:
                    pending.append(pool.submit(_flush, chunk))
                    total += len(chunk)
                    chunk = []
                    if len(pending) >= _PUSH_WORKERS:
                        pending.pop(0).result()
        if chunk:
            pending.append(pool.submit(_flush, chunk))
            total += len(chunk)
        for f in pending:
            f.result()

    return total


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
