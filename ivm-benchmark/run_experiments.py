#!/usr/bin/env python3
"""
run_experiments.py — Sweep runner for the Feldera vs ClickHouse benchmark.

Runs demo_runner.py headlessly across combinations of:
  --preload-days  (history loaded before streaming)
  --steps         (number of streaming batches)
  --engines       1 = Feldera only
                  2 = CH-full + Feldera  (latency story)
                  3 = CH-full + CH-light + Feldera  (full comparison)

Results are saved to experiments/<tag>.txt.

Usage:
    # Run the default sweep (all combinations of preload/steps/engines)
    python3 run_experiments.py

    # Custom sweep
    python3 run_experiments.py --preload 0 30 --steps 20 40 --engines 2 3

    # Single run
    python3 run_experiments.py --preload 30 --steps 40 --engines 3

    # Different data scale
    python3 run_experiments.py --data-dir data/1x --preload 30 --steps 40 --engines 3
"""

import argparse
import subprocess
import sys
import time
from itertools import product
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).parent / "experiments"

MEM_LIMIT_GB    = 96
MEM_LIMIT_BYTES = MEM_LIMIT_GB * 1024 ** 3


def _set_mem_limit():
    import resource
    resource.setrlimit(resource.RLIMIT_AS, (MEM_LIMIT_BYTES, MEM_LIMIT_BYTES))

# Preset name → (demo_runner.py flags, short label).
# Single-engine presets isolate one engine; combined presets run side-by-side.
ENGINE_PRESETS = {
    "ch-full":  (["--mode", "latency",  "--no-feldera"], "ch-full"),
    "ch-light": (["--mode", "accuracy", "--no-feldera"], "ch-light"),
    "feldera":  (["--no-ch"],                            "feldera"),
    "latency":  (["--mode", "latency"],                  "ch-full+feldera"),
    "accuracy": (["--mode", "accuracy"],                 "ch-light+feldera"),
    "all":      (["--mode", "full"],                     "all"),
}


def _run_one(preload: int, steps: int, engines: str, data_dir: str,
             extra_args: list[str]) -> tuple[bool, float]:
    EXPERIMENTS_DIR.mkdir(parents=True, exist_ok=True)

    flags, label_short = ENGINE_PRESETS[engines]
    scale = Path(data_dir).name
    tag   = f"{scale}_p{preload}_s{steps}_{engines}"
    out   = EXPERIMENTS_DIR / f"{tag}.txt"

    cmd = [
        sys.executable, "demo_runner.py",
        "--preload-days", str(preload),
        "--steps",        str(steps),
        "--data-dir",     data_dir,
        "--output",       str(out),
    ] + flags + extra_args

    label = f"preload={preload}d  steps={steps}  engines={label_short}  scale={scale}"
    print(f"\n{'━'*60}")
    print(f"  {label}")
    print(f"  output → {out}")
    print(f"{'━'*60}")

    t0  = time.perf_counter()
    ret = subprocess.run(cmd, cwd=Path(__file__).parent, preexec_fn=_set_mem_limit)
    elapsed = time.perf_counter() - t0

    ok = ret.returncode == 0
    status = "OK" if ok else f"FAILED (exit {ret.returncode})"
    print(f"\n  [{status}]  wall time {elapsed:.1f}s")
    return ok, elapsed


def main():
    parser = argparse.ArgumentParser(description="Sweep runner for the fraud detection benchmark")
    parser.add_argument("--preload",   type=int, nargs="+", default=[0, 30],
                        metavar="DAYS",  help="Preload days to sweep (default: 0 30)")
    parser.add_argument("--steps",     type=int, nargs="+", default=[40],
                        metavar="N",     help="Step counts to sweep (default: 40)")
    parser.add_argument("--engines",   type=str, nargs="+", default=["all"],
                        choices=list(ENGINE_PRESETS.keys()),
                        metavar="PRESET",
                        help="Engine preset(s) to run; each value = one experiment in the sweep. "
                             "Choices: ch-full, ch-light, feldera (single engine); "
                             "latency (CH-full+Feldera), accuracy (CH-light+Feldera), "
                             "all (CH-full+CH-light+Feldera). (default: all)")
    parser.add_argument("--data-dir",  default="data/0.1x",
                        help="Dataset scale (default: data/0.1x)")
    parser.add_argument("--mock",      action="store_true",
                        help="Pass --mock to demo_runner (no DB needed)")
    parser.add_argument("--interval",  type=float, default=None,
                        help="Seconds between batches (passed to demo_runner)")
    parser.add_argument("--max-steps", type=int, default=None,
                        help="Halt streaming after this many steps regardless of "
                             "--steps. Use to keep the cache layout from a larger "
                             "--steps value but stop early (e.g. --steps 5000 "
                             "--max-steps 10).")
    args = parser.parse_args()

    extra = []
    if args.mock:
        extra += ["--mock"]
    if args.interval is not None:
        extra += ["--interval", str(args.interval)]
    if args.max_steps is not None:
        extra += ["--max-steps", str(args.max_steps)]

    combos  = list(product(args.preload, args.steps, args.engines))
    n_total = len(combos)
    print(f"\nRunning {n_total} experiment(s)  [mem limit: {MEM_LIMIT_GB} GB per process]:")
    for preload, steps, engines in combos:
        scale = Path(args.data_dir).name
        print(f"  preload={preload}d  steps={steps}  engines={ENGINE_PRESETS[engines][1]}  scale={scale}")

    t_sweep = time.perf_counter()
    results = []
    for preload, steps, engines in combos:
        ok, elapsed = _run_one(preload, steps, engines, args.data_dir, extra)
        results.append((preload, steps, engines, ok, elapsed))

    # ── Summary ────────────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - t_sweep
    print(f"\n{'━'*60}")
    print(f"  SWEEP COMPLETE  —  {n_total} experiments in {total_elapsed:.1f}s")
    print(f"{'━'*60}")
    for preload, steps, engines, ok, elapsed in results:
        scale = Path(args.data_dir).name
        tag   = f"{scale}_p{preload}_s{steps}_{engines}"
        status = "✓" if ok else "✗"
        print(f"  {status}  {tag:<40}  {elapsed:6.1f}s")
    print()

    n_failed = sum(1 for *_, ok, _ in results if not ok)
    if n_failed:
        print(f"  {n_failed} experiment(s) failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
