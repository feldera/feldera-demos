#!/usr/bin/env python3
"""
Record the demo end to end into a video (headless Chromium via Playwright).

Resets both pipelines, starts recording the dashboard, fires `bootstrap`, and stops
~10s after both pipelines reach RUNNING -- capturing the whole story: initial STOPPED
state -> conventional freezes / concurrent stays live -> "all views are live" banner.

Outputs a .webm; if `ffmpeg` is on PATH it also writes a same-named .mp4 (H.264).

Prerequisites: Feldera up, both pipelines already `setup`, and the dashboard server
running (`run.py serve`). Run with Playwright + the Feldera SDK available, e.g.:

    uv run --no-project --with 'pyarrow>=17' --with playwright \
        --with-editable ~/projects/feldera/python python record.py --out demo.webm
    # first run only, to fetch the browser:
    uv run --no-project --with playwright python -m playwright install chromium

Common sizes: --width 1920 --height 1080 (wide/16:9)  ·  --width 1280 --height 1120 (narrow column).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

BOOT = {"BOOTSTRAPPING", "CONCURRENTBOOTSTRAPPING", "SYNCHRONIZING", "REPLAYING", "INITIALIZING"}
HERE = os.path.dirname(os.path.abspath(__file__))


def states(dashboard):
    d = json.load(urllib.request.urlopen(dashboard + "/api/state", timeout=5))["pipelines"]
    return [s["runtime"] for s in d.values()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--width", type=int, default=1280)
    ap.add_argument("--height", type=int, default=1120)
    ap.add_argument("--dashboard", default="http://localhost:8090")
    ap.add_argument("--out", default="demo.webm")
    ap.add_argument("--python", default=sys.executable,
                    help="python that can run run.py (defaults to the current interpreter)")
    args = ap.parse_args()
    from playwright.sync_api import sync_playwright

    def drive(*cmd):
        subprocess.run([args.python, "run.py", *cmd], cwd=HERE, capture_output=True)

    print("[record] reset to a clean STOPPED start")
    drive("reset")
    time.sleep(3)

    vdir = os.path.join(HERE, ".rec")
    shutil.rmtree(vdir, ignore_errors=True)
    os.makedirs(vdir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": args.width, "height": args.height},
                                  record_video_dir=vdir,
                                  record_video_size={"width": args.width, "height": args.height})
        page = ctx.new_page()
        page.goto(args.dashboard + "/", wait_until="load")
        time.sleep(4)                                  # show the initial STOPPED state
        print("[record] firing bootstrap")
        subprocess.Popen([args.python, "run.py", "bootstrap"], cwd=HERE,
                         stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        seen_boot, t0 = False, time.time()
        while time.time() - t0 < 600:
            try:
                st = states(args.dashboard)
            except Exception:
                st = []
            if any(x in BOOT for x in st):
                seen_boot = True
            if seen_boot and st and all(x == "RUNNING" for x in st):
                print(f"[record] both RUNNING at +{round(time.time()-t0)}s")
                break
            time.sleep(1)
        time.sleep(11)                                 # ~10s after completion (captures the banner)
        page.close()
        ctx.close()                                    # flushes the video
        browser.close()

    webm = sorted(os.path.join(vdir, f) for f in os.listdir(vdir) if f.endswith(".webm"))[-1]
    out = os.path.abspath(args.out)
    shutil.move(webm, out)
    shutil.rmtree(vdir, ignore_errors=True)
    print(f"[record] wrote {out}")

    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        mp4 = os.path.splitext(out)[0] + ".mp4"
        subprocess.run([ffmpeg, "-y", "-i", out, "-c:v", "libx264", "-pix_fmt", "yuv420p",
                        "-movflags", "+faststart", "-an", mp4], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"[record] wrote {mp4}")
    else:
        print("[record] ffmpeg not found; skipping mp4 (webm plays in Chrome/VLC)")


if __name__ == "__main__":
    main()
