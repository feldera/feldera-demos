#!/usr/bin/env python3
"""
plot_results.py — Plot fraud-detection benchmark results.

Parses one or more results*.txt files (the pretty-printed output of
`demo_runner.py --output FILE`) and produces one PNG per metric, with
subplots side-by-side for each scale and one line per engine.

Usage:
    python3 plot_results.py results.txt results_10x.txt
    python3 plot_results.py results.txt:1x results_10x.txt:10x   # explicit labels
"""

import re
import sys
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    sys.exit("Required: pip install pandas matplotlib")


ENGINE_COLORS = {
    "CH-full":  "#e74c3c",
    "CH-light": "#f39c12",
    "Feldera":  "#2ecc71",
}

# (col, label, unit, scale-factor) — engine column semantics:
#   ins  = insert time
#   ref  = IVM refresh (Feldera only; CH = 0)
#   qry  = query time (CH-full/light = full recompute, Feldera = trivial read)
#   total = ins+ref+qry
#   n    = new alerts this step
METRICS = [
    ("insert_ms",  "insert (ms)"),
    ("refresh_ms", "refresh / IVM (ms)"),
    ("query_ms",   "query (ms)"),
    ("total_ms",   "total (ms)"),
    ("new_alerts", "new alerts / step"),
]


def _parse_time(s: str) -> float:
    """Parse '949ms' / '1.50s' / '—' → milliseconds (or NaN)."""
    s = s.strip()
    if not s or s == "—":
        return float("nan")
    if s.endswith("ms"):
        return float(s[:-2])
    if s.endswith("s"):
        return float(s[:-1]) * 1000.0
    return float(s)


# Step rows look like:
#      1  CH-full     8.42s    2.56s        —   10.98s    10
#         CH-light    8.42s    1.38s        —    9.79s    10
#         Feldera     5.94s    3.72s     12ms    9.67s   130
# Columns: [step?] engine, ins, ref+qry-col, qry-col, total, n
ROW_RE = re.compile(
    r"^\s*(?P<step>\d+)?\s+"
    r"(?P<engine>CH-full|CH-light|Feldera)\s+"
    r"(?P<ins>\S+)\s+(?P<col3>\S+)\s+(?P<col4>\S+)\s+(?P<total>\S+)\s+(?P<n>\d+)\s*$"
)


def parse_results(path: Path, scale_label: str | None = None):
    text = path.read_text()
    rows = []
    current_step = None
    # Halt when we hit the "avg" header — rows below shouldn't be treated as steps.
    for line in text.splitlines():
        if re.match(r"^\s*avg\s", line):
            break
        m = ROW_RE.match(line)
        if not m:
            continue
        if m.group("step"):
            current_step = int(m.group("step"))
        engine = m.group("engine")
        ins = _parse_time(m.group("ins"))
        c3  = _parse_time(m.group("col3"))
        c4  = _parse_time(m.group("col4"))
        total = _parse_time(m.group("total"))
        n   = int(m.group("n"))
        # CH-full/CH-light: col3 = full-recompute query, col4 = '—' (NaN)
        # Feldera: col3 = refresh (IVM), col4 = query (trivial read)
        if engine.startswith("CH"):
            refresh_ms = 0.0
            query_ms   = c3
        else:
            refresh_ms = c3
            query_ms   = c4
        rows.append({
            "scale":    scale_label or path.stem,
            "step":     current_step,
            "engine":   engine,
            "insert_ms":  ins,
            "refresh_ms": refresh_ms,
            "query_ms":   query_ms,
            "total_ms":   total,
            "new_alerts": n,
        })

    preload = {}
    m = re.search(
        r"PRELOAD\s+(.+)$",
        text, flags=re.MULTILINE,
    )
    if m:
        for part in re.findall(r"(CH-full|CH-light|Feldera):\s*([0-9.]+s)", m.group(1)):
            preload[part[0]] = _parse_time(part[1]) / 1000.0   # seconds

    split_meta = {}
    rm = re.search(
        r"^# rows: preload=([\d,]+)\s+batches=(\d+) × ~([\d,]+)/step",
        text, flags=re.MULTILINE,
    )
    if rm:
        split_meta = {
            "preload_rows":   int(rm.group(1).replace(",", "")),
            "n_batches":      int(rm.group(2)),
            "rows_per_batch": int(rm.group(3).replace(",", "")),
        }
    else:
        fm = re.match(r"([\d.]+x)_pr(\d+)_s(\d+)_", path.stem)
        if fm:
            scale_dir, preload_rows, n_steps = fm.group(1), fm.group(2), fm.group(3)
            meta_path = (Path(__file__).parent / "data" / scale_dir / ".cache"
                         / f"pr{preload_rows}_s{n_steps}" / "meta.json")
            if meta_path.exists():
                import json
                meta = json.loads(meta_path.read_text())
                split_meta = {
                    "preload_rows": meta["preload"]["n_rows"],
                    "n_batches":    len(meta["batches"]),
                    "rows_per_batch": meta["batches"][0]["n_rows"] if meta["batches"] else 0,
                }
    return rows, preload, split_meta


def plot_metric(df: "pd.DataFrame", col: str, label: str,
                scales: list[str], engines: list[str],
                preloads: dict, out_path: Path,
                split_metas: "dict | None" = None):
    split_metas = split_metas or {}
    n_cols = len(scales)
    fig, axes = plt.subplots(
        1, n_cols,
        figsize=(6 * n_cols, 4.8),
        sharex=True, sharey=True,
        squeeze=False,
    )
    header = ""
    for s in scales:
        sm = split_metas.get(s) or {}
        if sm:
            header = (f"  ·  preload: {sm['preload_rows']:,} rows"
                      f"  ·  streaming: {sm['n_batches']} steps × ~{sm['rows_per_batch']:,} rows/step")
            break
    # Per-engine preload-load times across all scales (first occurrence wins).
    preload_by_engine: dict[str, float] = {}
    for pl in preloads.values():
        for e, t in (pl or {}).items():
            preload_by_engine.setdefault(e, t)
    if preload_by_engine:
        order = ["CH-full", "CH-light", "Feldera"]
        ordered = [(e, preload_by_engine[e]) for e in order if e in preload_by_engine]
        pre_str = ", ".join(f"{e}={t:.0f}s" for e, t in ordered)
        header += f"  ·  preload time: {pre_str}"
    fig.suptitle(f"{label}  ·  fraud-detection demo{header}",
                 fontsize=11, fontweight="bold")

    for c, scale in enumerate(scales):
        ax = axes[0][c]
        sub_scale = df[df["scale"] == scale]
        for eng in engines:
            sub = sub_scale[sub_scale["engine"] == eng].sort_values("step")
            if sub.empty:
                continue
            # Step 1 captures all preload-history-flagged cards at once; the
            # spike dominates the y-axis and hides the streaming pattern.
            if col == "new_alerts":
                sub = sub[sub["step"] > 1]
            ax.plot(
                sub["step"], sub[col],
                color=ENGINE_COLORS.get(eng, "#888888"),
                linewidth=1.6, marker="o", markersize=3.5,
                label=eng,
            )
        preload_str = ""
        if scale in preloads and preloads[scale]:
            parts = [f"{e}={t:.0f}s" for e, t in preloads[scale].items()]
            preload_str = f"  preload: {', '.join(parts)}"
        alert_parts = []
        for eng in engines:
            sub = sub_scale[sub_scale["engine"] == eng].sort_values("step")
            if sub.empty:
                continue
            s1 = int(sub[sub["step"] == 1]["new_alerts"].iloc[0]) if (sub["step"] == 1).any() else 0
            stream = sub[sub["step"] > 1]["new_alerts"]
            if len(stream) == 0:
                continue
            med = int(stream.median())
            avg = int(stream.mean())
            alert_parts.append(f"{eng}: step1={s1:,}, median={med}, avg={avg}")
        alert_line = "\n".join(alert_parts)
        ax.set_title(f"scale={scale}{preload_str}\n{alert_line}",
                     fontsize=8)
        ax.set_xlabel("step", fontsize=9)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8, loc="best")

    axes[0][0].set_ylabel(label, fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)

    all_rows = []
    preloads = {}
    split_metas = {}
    for arg in sys.argv[1:]:
        if ":" in arg:
            path_str, label = arg.split(":", 1)
        else:
            path_str = arg
            # Strip per-engine suffixes (_ch-full, _ch-light, _feldera) so that
            # all three files for the same run share one scale label.
            stem = re.sub(r"_(ch-full|ch-light|feldera)$", "", Path(path_str).stem)
            label = stem
        path = Path(path_str)
        rows, preload, split_meta = parse_results(path, label)
        if not rows:
            print(f"  WARN: no rows parsed from {path}", file=sys.stderr)
            continue
        print(f"  Parsed {len(rows):4d} rows from {path} (scale={label})")
        all_rows.extend(rows)
        preloads.setdefault(label, {}).update(preload)
        split_metas[label] = split_meta

    if not all_rows:
        sys.exit("No data parsed.")

    df = pd.DataFrame(all_rows)
    out_dir = Path(sys.argv[1].split(":")[0]).parent / "plots"
    out_dir.mkdir(exist_ok=True)

    # Stable scale ordering: try numeric sort on the leading number.
    def scale_key(s):
        m = re.match(r"([\d.]+)x", s)
        return float(m.group(1)) if m else float("inf")
    scales  = sorted(df["scale"].unique(), key=scale_key)
    engines = [e for e in ["CH-full", "CH-light", "Feldera"]
               if e in df["engine"].unique()]

    print(f"\nScales: {scales}    Engines: {engines}\n")

    for col, label in METRICS:
        slug = col.replace("_", "-")
        out_path = out_dir / f"plot_{slug}.png"
        plot_metric(df, col, label, scales, engines, preloads,
                    out_path, split_metas)

    print("\nDone.")


if __name__ == "__main__":
    main()
