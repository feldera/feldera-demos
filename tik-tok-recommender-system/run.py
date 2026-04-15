# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "feldera",
# ]
# ///
#
# Real-time TikTok-style recommender feature pipeline.
#
# Ingests a stream of user/video interaction events (produced into Kafka by the
# Rust `tiktok-gen` service — see docker-compose.yml) and computes per-video and
# per-user rolling aggregates (hour/day/week) over the interaction stream.
#
# Start the services (Feldera, RedPanda, tiktok-gen):
# > docker compose -f tik-tok-recommender-system/docker-compose.yml up -d --build --wait
#
# Run this script:
# > uv run tik-tok-recommender-system/run.py
#
# Clean up:
# > docker compose -f tik-tok-recommender-system/docker-compose.yml down -v

import argparse
import time

from feldera import FelderaClient, PipelineBuilder
from feldera.runtime_config import RuntimeConfig


PIPELINE_NAME = "tiktok_recsys"
DEFAULT_API_URL = "http://localhost:8080"
# `redpanda:9092` is the broker's internal listener, reachable from the Feldera
# container (both live on the compose network). Override via --kafka-bootstrap
# if running Feldera outside docker-compose.
DEFAULT_KAFKA_BOOTSTRAP = "redpanda:9092"
DEFAULT_TOPIC = "interactions"


SQL_TEMPLATE = """
CREATE TABLE interactions (
    interaction_id BIGINT,
    user_id INT,
    video_id INT,
    category_id INT,
    interaction_type STRING,
    watch_time INT,
    interaction_date TIMESTAMP LATENESS INTERVAL 15 MINUTES,
    interaction_month TIMESTAMP
) WITH (
    'materialized' = 'true',
    'connectors' = '[{{
        "name": "kafka-interactions",
        "transport": {{
            "name": "kafka_input",
            "config": {{
                "topics": ["{topic}"],
                "bootstrap.servers": "{bootstrap}",
                "auto.offset.reset": "earliest",
                "poller_threads": 12
            }}
        }},
        "format": {{"name": "csv", "config": {{}}}}
    }}]'
);

CREATE MATERIALIZED VIEW video_agg AS (SELECT
    video_id,
    interaction_type,
    count(*) OVER hour  AS interaction_len_h,
    count(*) OVER day   AS interaction_len_d,
    count(*) OVER week  AS interaction_len_w,
    avg(watch_time) OVER hour  AS average_watch_time_h,
    avg(watch_time) OVER day   AS average_watch_time_d,
    avg(watch_time) OVER week  AS average_watch_time_w,
    interaction_date AS hour_start
FROM interactions
WINDOW
    hour AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW),
    day  AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' DAY  PRECEDING AND CURRENT ROW),
    week AS (PARTITION BY video_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY  PRECEDING AND CURRENT ROW));

CREATE MATERIALIZED VIEW user_agg AS (SELECT
    user_id,
    interaction_type,
    count(*) OVER hour  AS interaction_len_h,
    count(*) OVER day   AS interaction_len_d,
    count(*) OVER week  AS interaction_len_w,
    avg(watch_time) OVER hour  AS average_watch_time_h,
    avg(watch_time) OVER day   AS average_watch_time_d,
    avg(watch_time) OVER week  AS average_watch_time_w,
    interaction_date AS hour_start
FROM interactions
WINDOW
    hour AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW),
    day  AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '1' DAY  PRECEDING AND CURRENT ROW),
    week AS (PARTITION BY user_id ORDER BY interaction_date RANGE BETWEEN INTERVAL '7' DAY  PRECEDING AND CURRENT ROW));
"""


def build_sql(bootstrap: str, topic: str) -> str:
    return SQL_TEMPLATE.format(bootstrap=bootstrap, topic=topic)


def main():
    parser = argparse.ArgumentParser(
        description="Run the TikTok recommender Feldera pipeline."
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Feldera API URL (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=DEFAULT_KAFKA_BOOTSTRAP,
        help=(
            "Kafka bootstrap servers reachable from the Feldera pipeline "
            f"(default: {DEFAULT_KAFKA_BOOTSTRAP})"
        ),
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic with interaction events (default: {DEFAULT_TOPIC})",
    )
    args = parser.parse_args()

    print(f"Feldera API:       {args.api_url}")
    print(f"Kafka bootstrap:   {args.kafka_bootstrap}")
    print(f"Kafka topic:       {args.topic}")

    client = FelderaClient(args.api_url)

    sql = build_sql(args.kafka_bootstrap, args.topic)
    runtime_config = RuntimeConfig(storage=False, workers=10)

    print(f"\nCreating pipeline '{PIPELINE_NAME}'...")
    pipeline = PipelineBuilder(
        client, name=PIPELINE_NAME, sql=sql, runtime_config=runtime_config
    ).create_or_replace()

    print("Starting pipeline (this compiles the SQL program; first run may take a minute)...")
    pipeline.start()
    print(f"Pipeline running — monitor at {args.api_url}/pipelines/{PIPELINE_NAME}")

    print("\nWaiting for the pipeline to finish processing the Kafka backlog...")
    start_time = time.time()
    pipeline.wait_for_idle(idle_interval_s=2)
    elapsed = time.time() - start_time
    print(f"Pipeline idle after {elapsed:.1f}s")

    print("\n--- Sample rows from video_agg (top 5 by interaction_len_d) ---")
    for row in pipeline.query(
        "SELECT video_id, interaction_type, interaction_len_h, interaction_len_d, "
        "interaction_len_w, average_watch_time_d "
        "FROM video_agg ORDER BY interaction_len_d DESC LIMIT 5"
    ):
        print(row)

    print("\n--- Sample rows from user_agg (top 5 by interaction_len_d) ---")
    for row in pipeline.query(
        "SELECT user_id, interaction_type, interaction_len_h, interaction_len_d, "
        "interaction_len_w, average_watch_time_d "
        "FROM user_agg ORDER BY interaction_len_d DESC LIMIT 5"
    ):
        print(row)

    print("\n--- Interaction count ---")
    for row in pipeline.query("SELECT count(*) AS n FROM interactions"):
        print(row)

    print(
        f"\nPipeline '{PIPELINE_NAME}' left RUNNING so you can explore it in the"
        f" Web UI: {args.api_url}/pipelines/{PIPELINE_NAME}"
    )
    print(
        "To stop it, either stop the pipeline in the UI or run:\n"
        f"    docker compose -f tik-tok-recommender-system/docker-compose.yml down -v"
    )


if __name__ == "__main__":
    main()
