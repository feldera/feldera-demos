#!/usr/bin/env python3
"""
engine_base.py — Abstract FraudEngine interface.

Any database can be plugged in by subclassing FraudEngine and implementing
setup(), push_step(), and query().  The coordinator in demo_runner.py handles
all threading, fairness barriers, and metric collection.

storage_id: engines that write to the same physical storage (e.g. CH-full and
CH-light both write to the ClickHouse `transactions` table) share a value.
The coordinator calls push_step() only on the first engine in each storage
group (the "primary"); secondaries rely on the primary's write to update their
own read path (e.g. materialized views).
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path


class FraudEngine(ABC):
    sim_id:     int   # index into SIM_NAMES in constants.py
    name:       str   # human-readable label, used in log lines
    storage_id: str   # shared key for engines writing to the same DB/table

    def setup(self, preload_path: "Path | None", data_dir: Path) -> None:
        """
        One-time setup before the streaming loop begins.

        Primary engine:  create / truncate schema, load customers, load preload.
        Secondary engine: connect and verify the client (primary already set up schema).
        """

    def push_step(self, rows: list[dict]) -> None:
        """
        Insert one streaming batch.

        rows: [{"category", "ts", "amt", "cc_num", "shipping_lat", "shipping_long"}, ...]

        Primary:   write to storage and record timing.
        Secondary: no-op — the primary's write already updated MVs / shared tables.
        """

    def insert_time(self) -> float:
        """Seconds from push_step() start until data is buffered / ACK'd.  0.0 for secondaries."""
        return 0.0

    def refresh_time(self) -> float:
        """IVM materialization seconds (commit fence − data-ready fence).  0.0 for CH engines."""
        return 0.0

    def preload_push_time(self) -> float:
        """Preload HTTP delivery time (data-in-flight). For CH: total INSERT time."""
        return 0.0

    def preload_ivm_time(self) -> float:
        """Preload server-side processing time. For Feldera: commit_transaction duration. For CH: 0."""
        return 0.0

    @abstractmethod
    def query(self, win_start: datetime, win_end: datetime) -> tuple[list[dict], float]:
        """
        Run the fraud-detection SELECT over [win_start, win_end).
        Returns (results, elapsed_seconds).
        Elapsed is HTTP + serialization only — not IVM cost.
        """
        ...

