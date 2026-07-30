"""
SDK-backed dashboard server for the concurrent-bootstrapping demo.

A background poller samples each pipeline through the Feldera Python SDK (never the
REST API directly) and caches a JSON snapshot of ready-to-plot panels. The single-page
BI dashboard polls `/api/state`; the browser never talks to Feldera. One poller thread
per pipeline so a slow sample on one side never stalls the other.
"""
import json
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from feldera import FelderaClient, Pipeline, RetryConfig

HIST = 90            # throughput-sparkline samples
NEW_EVERY = 4        # sample the (heavier) new-view panels once every N ticks
BASE_HORIZON = 45.0  # seconds for a delta panel's tallest bar to fill, at the reference size
BASE_SIZE = 20_000_000

# Panel = one chart. `kind`: "hbars" (categorical), "vbars" (time series), "kpi" (big numbers).
# The query returns the plottable rows/row directly, so the browser stays generic.
PANELS = {
    "gold_live_orders": {
        "kind": "hbars", "title": "Live orders by status", "unit": "orders", "delta": True,
        "query": "SELECT order_status AS label, orders AS value FROM gold_live_orders",
        "order": ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"],
    },
    "gold_order_revenue": {
        "kind": "vbars", "title": "Revenue by day", "unit": "$",
        "query": ("SELECT CAST(day AS VARCHAR) AS label, revenue AS value "
                  "FROM gold_order_revenue ORDER BY day DESC LIMIT 14"),
        "reverse": True,
    },
    "gold_product_daily_reach": {
        "kind": "vbars", "title": "Live clicks by day", "unit": "clicks", "delta": True,
        "query": ("SELECT CAST(day AS VARCHAR) AS label, SUM(events) AS value "
                  "FROM gold_product_daily_reach GROUP BY day ORDER BY day DESC LIMIT 14"),
        "reverse": True,
    },
    "gold_user_stats": {
        "kind": "kpi", "title": "Audience",
        "query": ("SELECT COUNT(*) AS users, SUM(events) AS events, SUM(purchases) AS purchases "
                  "FROM gold_user_stats"),
        "metrics": [("users", "active users"), ("events", "clicks"), ("purchases", "purchases")],
    },
    "gold_user_product_engagement": {
        "kind": "kpi", "title": "Engagement matrix",
        "query": "SELECT COUNT(*) AS pairs FROM gold_user_product_engagement",
        "metrics": [("pairs", "user × product pairs")],
    },
}


class Poller(threading.Thread):
    def __init__(self, host, name, existing_views, new_views, size=BASE_SIZE):
        super().__init__(daemon=True)
        self.host, self.name = host, name
        self.existing_views, self.new_views = existing_views, new_views
        # Existing-view delta panels (orders) fill DURING the bootstrap, so their fill horizon
        # tracks the bootstrap duration (~linear in --size): the tallest bar reaches ~2/3 at
        # cutover for any size and never saturates early. New-view panels (clicks) fill AFTER
        # cutover in real time, so a bigger dataset shouldn't slow them -- fixed horizon.
        boot_horizon = max(20.0, BASE_HORIZON * size / BASE_SIZE)
        self.horizon = {v: boot_horizon for v in existing_views}
        self.horizon.update({v: BASE_HORIZON for v in new_views})
        # Short timeout + no retries so a slow request during a stop/transition can never
        # stall the poller (it fails fast, and the next tick recovers).
        self.client = FelderaClient(host, timeout=5, retry_config=RetryConfig(max_retries=0))
        self.lock = threading.Lock()
        self.last_panel = {v: None for v in existing_views + new_views}
        self.snapshot = {"runtime": "unknown", "existing": [], "new": [], "throughput": 0}
        self.proc_hist = deque(maxlen=64)          # (t, processed) -> smoothed throughput/live
        self.thr_hist = deque(maxlen=HIST)
        self.ticks = 0
        self.baseline = {}         # delta panels: per-status counts captured at run start
        self.baseline_t = {}       # and when they were captured (for the growth scale)
        self.rebaseline = {}       # armed while stopped; re-capture on the next fresh query

    def _panel_data(self, p, view):
        """Run the panel's query and shape it into rows (bars) or metrics (kpi)."""
        spec = PANELS[view]
        try:
            rows = list(p.query(spec["query"]))
        except Exception:
            return None
        if spec["kind"] == "kpi":
            r = rows[0] if rows else {}
            metrics = [{"label": lbl, "value": int(r.get(col) or 0)} for col, lbl in spec["metrics"]]
            return {"metrics": metrics, "total": sum(m["value"] for m in metrics)}
        data = [{"label": str(x["label"]), "value": int(x["value"] or 0)} for x in rows]
        if spec.get("order"):
            rank = {s: i for i, s in enumerate(spec["order"])}
            data.sort(key=lambda d: rank.get(d["label"], 999))
        elif spec.get("reverse"):
            data.reverse()          # query is DESC; reverse to chronological
        return {"rows": data, "total": sum(d["value"] for d in data)}

    def _panel(self, view, data, ready):
        spec = PANELS[view]
        return {
            "view": view, "kind": spec["kind"], "title": spec["title"], "unit": spec.get("unit", ""),
            "ready": ready,
            "rows": (data or {}).get("rows", []),
            "metrics": (data or {}).get("metrics", []),
            "total": (data or {}).get("total", 0),
            "scale": (data or {}).get("scale"),
        }

    def _apply_delta(self, view, data, rt, now, fresh):
        """Recast a cumulative panel as 'orders received since this run started'.

        The raw counts grow proportionally, so normalizing to their own max leaves the bars
        frozen while the numbers climb. Subtracting a baseline makes the bars fill from empty
        as the live stream flows (and stay empty on the frozen side). The baseline is the first
        FRESH query after the pipeline (re)starts — never the stale cached value — so a
        checkpoint restore that resets the cumulative can't leave the delta clamped at 0.
        The scale grows the tallest bar toward full over ~HORIZON seconds, so it keeps rising
        instead of pinning to a co-growing max.
        """
        cur = {r["label"]: r["value"] for r in data["rows"]}
        empty = {"rows": [{"label": r["label"], "value": 0} for r in data["rows"]],
                 "total": 0, "scale": None}
        if rt in ("STOPPED", "unknown"):
            self.rebaseline[view] = True          # arm; re-capture on the next fresh query
        if self.rebaseline.get(view, True):
            if not fresh:
                return empty                      # frozen / not yet serving -> empty bars
            self.baseline[view] = cur
            self.baseline_t[view] = now
            self.rebaseline[view] = False
        base = self.baseline[view]
        rows = [{"label": r["label"], "value": max(0, r["value"] - base.get(r["label"], 0))}
                for r in data["rows"]]
        top = max((r["value"] for r in rows), default=0)
        elapsed = max(now - self.baseline_t.get(view, now), 1.0)
        horizon = self.horizon.get(view, BASE_HORIZON)
        scale = int(top * horizon / elapsed) if top > 0 else None
        return {"rows": rows, "total": sum(r["value"] for r in rows), "scale": scale}

    def run(self):
        while True:
            try:
                self._tick()
            except Exception as e:
                with self.lock:
                    self.snapshot["error"] = str(e)[:200]
            time.sleep(0.5)

    def _idle_snapshot(self, rt):
        self.proc_hist.clear()
        self.thr_hist.append(0)
        now = time.time()
        if rt in ("STOPPED", "unknown"):        # new delta panels are silent here, so they never
            for v in self.new_views:            # pass through _apply_delta -> arm their re-baseline
                if PANELS[v].get("delta"):
                    self.rebaseline[v] = True
        existing = []
        for v in self.existing_views:
            d = self.last_panel.get(v)
            if d is not None and PANELS[v].get("delta"):
                d = self._apply_delta(v, d, rt, now, False)   # not fresh; arms re-baseline
            existing.append(self._panel(v, d, d is not None))
        with self.lock:
            self.snapshot = {
                "runtime": rt, "bootstrap_progress": None, "processed": 0, "throughput": 0,
                "throughput_history": list(self.thr_hist), "live": False, "t": now,
                "existing": existing,
                "new": [self._panel(v, None, False) for v in self.new_views],
            }

    def _tick(self):
        p = Pipeline.get(self.name, self.client)
        try:
            s = p.deployment_runtime_status()
            rt = str(s).split(".")[-1] if s is not None else "unknown"
        except Exception:
            rt = "unknown"
        try:
            g = p.stats().global_metrics
        except Exception:
            self._idle_snapshot("STOPPED" if rt == "unknown" else rt)
            return

        now = time.time()
        proc = g.total_processed_records or 0
        self.proc_hist.append((now, proc))
        while len(self.proc_hist) > 2 and now - self.proc_hist[0][0] > 3.0:
            self.proc_hist.popleft()
        t_old, p_old = self.proc_hist[0]
        thr = max(0, (proc - p_old) / max(now - t_old, 1e-3))
        advanced = proc - p_old
        self.thr_hist.append(int(thr))
        self.ticks += 1

        # Query policy: existing views (over `orders`, untouched) stay live through a
        # concurrent bootstrap; skip only the conventional stop-the-world. New views are
        # queried only once running, and less often (they are heavier and change slowly).
        query_existing = rt in ("RUNNING", "PAUSED", "CONCURRENTBOOTSTRAPPING", "SYNCHRONIZING")
        query_new = rt in ("RUNNING", "PAUSED") and self.ticks % NEW_EVERY == 0
        running = rt in ("RUNNING", "PAUSED")

        existing = []
        for v in self.existing_views:
            fresh = False
            if query_existing:
                d = self._panel_data(p, v)
                if d is not None:
                    self.last_panel[v] = d
                    fresh = True
            d = self.last_panel.get(v)
            if d is not None and PANELS[v].get("delta"):
                d = self._apply_delta(v, d, rt, now, fresh)
            existing.append(self._panel(v, d, d is not None))

        new = []
        for v in self.new_views:
            fresh = False
            if query_new:
                d = self._panel_data(p, v)
                if d and d.get("total", 0) > 0:
                    self.last_panel[v] = d
                    fresh = True
            elif not running:
                self.last_panel[v] = None      # silent while bootstrapping
            d = self.last_panel.get(v)
            if d is not None and PANELS[v].get("delta"):
                d = self._apply_delta(v, d, rt, now, fresh)
            new.append(self._panel(v, d, d is not None))

        prog = g.concurrent_bootstrap_progress
        frac = None
        if isinstance(prog, dict):
            tot = prog.get("completed", 0) + prog.get("in_progress", 0) + prog.get("remaining", 0)
            frac = prog.get("completed", 0) / tot if tot else None

        with self.lock:
            self.snapshot = {
                "runtime": rt, "bootstrap_progress": frac, "processed": proc,
                "throughput": int(thr), "throughput_history": list(self.thr_hist),
                "live": advanced > 0, "t": now, "existing": existing, "new": new,
            }

    def get(self):
        with self.lock:
            return dict(self.snapshot)


def serve(host, pipelines, existing_views, new_views, port=8090, size=BASE_SIZE):
    pollers = {n: Poller(host, n, existing_views, new_views, size) for n in pipelines}
    for p in pollers.values():
        p.start()
    index = (Path(__file__).parent / "dashboard" / "index.html").read_text()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path.startswith("/api/state"):
                body = json.dumps({"pipelines": {n: pollers[n].get() for n in pipelines}}).encode()
                ctype = "application/json"
            else:
                body = index.encode()
                ctype = "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    print(f"[serve] dashboard on http://localhost:{port}  (Ctrl-C to stop)")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
