# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests==2.32.4",
#     "psycopg[binary]==3.2.1",
#     "feldera",
# ]
# ///
#
# Replicate a Postgres source table into Feldera via the Debezium Postgres
# source connector and Kafka (RedPanda), using both JSON and Avro formats
# in parallel for comparison.
#
# Start the services:
# > docker compose -f debezium-postgres/docker-compose.yml up -d --build --wait
#
# Run this script:
# > uv run debezium-postgres/run.py --api-url=http://localhost:8080 --start
#
# Clean up:
# > docker compose -f debezium-postgres/docker-compose.yml down -v

import os
import time
import requests
import argparse
import psycopg
from feldera import FelderaClient, PipelineBuilder, Pipeline
from typing import Dict, List


SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

TEST_SCHEMA = "test_schema"
TEST_TABLE = "test_table"
# Reduced from 500000 so the demo completes in ~30 seconds. Raise if you want
# to showcase CDC throughput on larger volumes.
NUM_RECORDS = 10000
PIPELINE_NAME = "demo-debezium-postgres-pipeline"
JSON_CONNECTOR_NAME = "test-connector-json"
AVRO_CONNECTOR_NAME = "test-connector-avro"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        default="http://localhost:8080",
        help="Feldera API URL (e.g., http://localhost:8080)",
    )
    parser.add_argument(
        "--start", action="store_true", default=False, help="Start the Feldera pipeline"
    )
    parser.add_argument(
        "--kafka-url-from-pipeline",
        default="redpanda:9092",
        help="Kafka broker address reachable from the pipeline",
    )
    parser.add_argument(
        "--registry-url-from-pipeline",
        default="http://redpanda:8081",
        help="Schema registry address reachable from the pipeline",
    )
    parser.add_argument(
        "--registry-url-from-connect",
        default="http://redpanda:8081",
        help="Schema registry address reachable from the Kafka Connect server",
    )
    args = parser.parse_args()

    populate_database()

    # JSON-format connector
    json_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "table.include.list": f"{TEST_SCHEMA}.*",
        "topic.prefix": "json",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
    }
    create_debezium_postgres_connector(
        JSON_CONNECTOR_NAME,
        json_config,
        [f"json.{TEST_SCHEMA}.{TEST_TABLE}"],
    )

    # Avro-format connector (uses its own Postgres replication slot)
    avro_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "table.include.list": f"{TEST_SCHEMA}.*",
        "topic.prefix": "avro",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        "slot.name": "debezium_slot_1",
        "key.converter.schema.registry.url": args.registry_url_from_connect,
        "value.converter.schema.registry.url": args.registry_url_from_connect,
    }
    create_debezium_postgres_connector(
        AVRO_CONNECTOR_NAME,
        avro_config,
        [f"avro.{TEST_SCHEMA}.{TEST_TABLE}"],
    )

    pipeline = create_feldera_pipeline(
        args.api_url,
        args.kafka_url_from_pipeline,
        args.registry_url_from_pipeline,
        args.start,
    )

    if args.start:
        validate_results(pipeline)


def populate_database():
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")
    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}") as conn:
        with conn.cursor() as cur:
            print(f"(Re-)creating test schema '{TEST_SCHEMA}'")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TEST_SCHEMA}.{TEST_TABLE}(
                        id INT PRIMARY KEY,
                        bi BIGINT,
                        s VARCHAR,
                        d DOUBLE PRECISION,
                        f REAL,
                        i INT,
                        b BOOLEAN,
                        ts TIMESTAMP,
                        dt DATE,
                        json1 JSON,
                        json2 JSON,
                        uuid_ UUID)""")
            cur.execute(f"DELETE FROM {TEST_SCHEMA}.{TEST_TABLE}")

            print(f"Populating '{TEST_SCHEMA}.{TEST_TABLE}' with {NUM_RECORDS} records")
            for i in range(NUM_RECORDS):
                cur.execute(f"""
                            INSERT INTO {TEST_SCHEMA}.{TEST_TABLE}
                                    (id, bi, s, d, f, i, b, ts, dt, json1, json2, uuid_)
                            VALUES({i}, {i * 100}, 'foo{i}', {i}.01, {i}.1, {i}, true, '2024-08-30 10:30:00', '2024-08-30', '{{"foo":"bar"}}', '{{"foo":"bar"}}', '123e4567-e89b-12d3-a456-426614174000')
                    """)
                if i > 0 and i % 1000 == 0:
                    print(f"{i} records")


def create_debezium_postgres_connector(
    connector_name: str, config: Dict, expected_topics: List[str]
):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print(f"Deleting old connector {connector_name}")
    requests.delete(f"{connect_server}/connectors/{connector_name}")

    print(f"Creating connector {connector_name}")
    payload = {"name": connector_name, "config": config}
    requests.post(f"{connect_server}/connectors", json=payload).raise_for_status()

    print(f"Checking {connector_name} connector status")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/{connector_name}/status")
        if response.ok:
            status = response.json()
            if status["connector"]["state"] != "RUNNING":
                raise Exception(f"Unexpected connector state: {status}")
            if len(status["tasks"]) == 0:
                print("Waiting for connector task")
                time.sleep(1)
                continue
            if status["tasks"][0]["state"] != "RUNNING":
                raise Exception(f"Unexpected task state: {status}")
            break
        else:
            if time.time() - start_time >= 10:
                raise Exception("Timeout waiting for connector creation")
            print("Waiting for connector creation")
            time.sleep(1)

    # Wait for the connector to publish expected topics (drives snapshot completion).
    print(f"Waiting for {connector_name} to create Kafka topics")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/{connector_name}/topics")
        if not response.ok:
            raise Exception(f"Error retrieving topic list from connector: {response}")
        topics = response.json()[connector_name]["topics"]
        if all(t in topics for t in expected_topics):
            print(f"Topics ready: {topics}")
            break
        if time.time() - start_time >= 30:
            raise Exception(f"Timeout waiting for topic creation. Current: {topics}")
        print(f"Waiting for topics (have {len(topics)})")
        time.sleep(1)


def create_feldera_pipeline(
    api_url: str, kafka_url: str, registry_url: str, start_pipeline: bool
) -> Pipeline:
    client = FelderaClient(api_url)
    sql = (
        open(PROJECT_SQL)
        .read()
        .replace("[REPLACE-BOOTSTRAP-SERVERS]", kafka_url)
        .replace("[REPLACE-REGISTRY-URL]", registry_url)
    )

    print("Creating the pipeline...")
    pipeline = PipelineBuilder(client, name=PIPELINE_NAME, sql=sql).create_or_replace()

    if start_pipeline:
        print("Starting the pipeline...")
        pipeline.start()
        print("Pipeline started")

    return pipeline


def validate_results(pipeline: Pipeline):
    """Query Postgres source and both Feldera tables to show sample rows."""
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")

    print(f"\n--- Postgres source: {TEST_SCHEMA}.{TEST_TABLE} ---")
    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}") as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TEST_SCHEMA}.{TEST_TABLE}")
            print(f"Row count: {cur.fetchone()[0]}")
            print("\n-- First 5 rows --")
            cur.execute(
                f"SELECT id, bi, s, d, b, ts, uuid_ "
                f"FROM {TEST_SCHEMA}.{TEST_TABLE} ORDER BY id LIMIT 5"
            )
            for row in cur.fetchall():
                print(row)

    print("\n--- Feldera tables (replicated from Postgres via CDC) ---")
    for table in ("json_test_table", "avro_test_table"):
        print(f"\n-- {table} --")
        # Retry until the count matches or we time out.
        for attempt in range(60):
            try:
                rows = list(pipeline.query(f"SELECT count(*) AS n FROM {table}"))
                count = rows[0]["n"] if rows else 0
                if count >= NUM_RECORDS:
                    print(f"Row count: {count}")
                    print("First 5 rows:")
                    for row in pipeline.query(
                        f"SELECT id, bi, s, d, b FROM {table} ORDER BY id LIMIT 5"
                    ):
                        print(row)
                    break
                if attempt == 0 or attempt % 5 == 0:
                    print(f"(count={count}/{NUM_RECORDS}, waiting for CDC to replicate...)")
            except Exception as e:
                if attempt == 0:
                    print(f"(query failed: {e}, retrying...)")
            time.sleep(2)
        else:
            print("(replication did not complete within timeout — CDC might still be catching up)")


if __name__ == "__main__":
    main()
