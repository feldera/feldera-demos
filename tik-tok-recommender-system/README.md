# TikTok Recommender System Demo

Real-time feature engineering for a TikTok-style recommender: a generator produces synthetic user/video interaction events into Kafka (Redpanda), and Feldera computes per-video and per-user rolling aggregates (1-hour, 1-day, 7-day windows) over the interaction stream using incremental SQL.

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Data flow

```
tiktok-gen (Rust)  ──► Kafka/Redpanda topic: interactions (CSV)  ──► Feldera
                                                                │
                                                                ▼
                                                     video_agg  +  user_agg
                                                (rolling counts / averages)
```

## Steps

### 0. Shut down any previous instance

```bash
docker compose -f tik-tok-recommender-system/docker-compose.yml down -v
```

This stops all containers and removes their volumes. Safe to run even if nothing is currently up.

### 1. Start the infrastructure (Redpanda + Feldera)

```bash
docker compose -f tik-tok-recommender-system/docker-compose.yml up -d --build --wait redpanda feldera
```

This starts two services and waits for them to be healthy:

| Service | Port | Purpose |
|---------|------|---------|
| redpanda | 19092 (external Kafka) | Kafka-compatible broker |
| feldera | 8080 | Feldera pipeline manager + runtime |

The first `up` builds the Rust `tiktok-gen` image and pulls the Feldera image (~2 GiB), which can take a few minutes.

### 2. Start the event generator

```bash
docker compose -f tik-tok-recommender-system/docker-compose.yml up -d --build tiktok-gen
```

This starts the third service:

| Service | Port | Purpose |
|---------|------|---------|
| tiktok-gen | (no port) | Rust generator — publishes ~1M interactions, exits |

Because Feldera is already up, you can open the Web UI at <http://localhost:8080> and watch the `interactions` Kafka topic be populated as the pipeline ingests events in the next step.

To change the number of generated events, edit the `-I` argument in `docker-compose.yml` before starting (default: `1000000`).

### 3. Run the demo

```bash
uv run tik-tok-recommender-system/run.py
```

`run.py` has a `# /// script` PEP 723 preamble, so `uv` resolves the `feldera` dependency automatically — no virtualenv setup required.

What you will see:

1. **Pipeline creation** — submits the Feldera pipeline (`tiktok_recsys`) with one Kafka-backed input table (`interactions`) and two materialized views (`video_agg`, `user_agg`). Each view computes rolling counts and average watch times over 1-hour, 1-day, and 7-day windows, partitioned by video / user.
2. **Pipeline start** — compiles the SQL program (optimized Rust codegen; first compile can take ~1 minute) and starts the runtime.
3. **Idle wait** — streams events from the Kafka topic; `wait_for_idle` returns after the generator has finished and the pipeline is caught up.
4. **Sample output** — queries `video_agg` and `user_agg` ad-hoc for the top-5 most-interacted videos/users and the total `interactions` row count.

Example output:

```
Feldera API:       http://localhost:8080
Kafka bootstrap:   redpanda:9092
Kafka topic:       interactions

Creating pipeline 'tiktok_recsys'...
Starting pipeline (this compiles the SQL program; first run may take a minute)...
Pipeline running — monitor at http://localhost:8080/pipelines/tiktok_recsys

Waiting for the pipeline to finish processing the Kafka backlog...
Pipeline idle after 7.8s

--- Sample rows from video_agg (top 5 by interaction_len_d) ---
{'video_id': 421, 'interaction_type': 'share', 'interaction_len_h': 5, 'interaction_len_d': 133, ...}
...

--- Sample rows from user_agg (top 5 by interaction_len_d) ---
{'user_id': 73, 'interaction_type': 'view', 'interaction_len_h': 9, 'interaction_len_d': 136, ...}
...

--- Interaction count ---
{'n': 1000000}

Pipeline 'tiktok_recsys' left RUNNING so you can explore it in the Web UI: ...
```

Make sure the user sees the sample `video_agg` / `user_agg` rows and the total interaction count. `run.py` exits without stopping the pipeline.

## 4. Summary

Next steps for the user:

### Visit the Feldera Web UI at <http://localhost:8080>

Browse the `tiktok_recsys` pipeline, inspect the SQL program, and watch throughput / memory in the Performance tab. In the Ad-Hoc Query tab, try:

```sql
-- Most-engaged videos in the past day
SELECT video_id, interaction_type, interaction_len_d, average_watch_time_d
FROM video_agg
ORDER BY interaction_len_d DESC
LIMIT 10;

-- Most-engaged users in the past week
SELECT user_id, interaction_type, interaction_len_w, average_watch_time_w
FROM user_agg
ORDER BY interaction_len_w DESC
LIMIT 10;

-- Distribution of interaction types
SELECT interaction_type, count(*) FROM interactions GROUP BY interaction_type;
```

### Stream more events into Kafka

Re-run the Rust generator in a one-shot container to push additional interactions into the same topic while the pipeline is running — the views will incrementally update:

```bash
docker compose -f tik-tok-recommender-system/docker-compose.yml run --rm tiktok-gen \
    -U 1000 -V 1000 -I 100000 -B redpanda:9092
```

(Omit `--delete-topic-if-exists` so the topic is reused.)

### Try modifying the SQL

Edit `SQL_TEMPLATE` in `run.py` — add a new window (e.g., 5-minute rolling), a join with a separate videos table, or a different aggregate — and re-run. `create_or_replace` will transparently swap the pipeline.

### Important: clean up when done

```bash
docker compose -f tik-tok-recommender-system/docker-compose.yml down -v
```

This stops all containers and removes their volumes (including the Kafka topic data).
