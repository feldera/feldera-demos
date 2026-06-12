Run the IVM benchmark from the `rolling-aggregate-bench/` directory.

Ask the user which scale and mode to use, then run:

```bash
cd /home/nina/projects/feldera-demos/rolling-aggregate-bench
python3 src/run_bench.py \
  --customers <CUSTOMERS> --preload <PRELOAD> --batch-size 1000 --batches <BATCHES> \
  --sequential --mode <MODE>
```

Common options:

| Goal | `--customers` | `--preload` | `--batches` | `--mode` | Notes |
|------|-------------|------------|-------------|---------|-------|
| All three engines, 1M |   `500000` |      `1000000` | `10` | `feldera ch pg` | any machine |
| All three engines, 20M |  `500000` |   `20000000` | `10` | `feldera ch pg` | 16+ cores, 32 GB RAM |
| All three engines, 200M | `500000` | `200000000` | `10` | `feldera ch pg` | 32+ cores, 128 GB RAM |
| All three engines, 1B | `5000000` | `1000000000` | `10` | `feldera ch pg` | 64+ cores, 256 GB RAM; add `--max-rss-mb 80000` |

Add `--postgres-user gz` if running as a different user. Add `--max-rss-mb 80000` for 1B-scale runs.

Run the command in the foreground so the user can see live output. After it completes, show the summary table and point out the fastest engine and the speedup ratios.
