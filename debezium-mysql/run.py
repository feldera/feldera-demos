# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests==2.32.4",
#     "pymysql==1.1.1",
#     "cryptography==44.0.0",
#     "feldera",
# ]
# ///
#
# Replicate a MySQL inventory database into Feldera via the Debezium MySQL
# source connector and Kafka (RedPanda).
#
# Start the services:
# > docker compose -f debezium-mysql/docker-compose.yml up -d --build --wait
#
# Run this script:
# > uv run debezium-mysql/run.py --api-url=http://localhost:8080 --start
#
# Clean up:
# > docker compose -f debezium-mysql/docker-compose.yml down -v

import os
import time
import requests
import argparse
import pymysql
from feldera import PipelineBuilder, FelderaClient, Pipeline


SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

PIPELINE_NAME = "debezium-mysql"
CONNECTOR_NAME = "inventory-connector"

EXPECTED_TOPICS = [
    "inventory.inventory.orders",
    "inventory.inventory.addresses",
    "inventory.inventory.customers",
    "inventory.inventory.products",
    "inventory.inventory.products_on_hand",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        default="http://localhost:8080",
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    parser.add_argument(
        "--start", action="store_true", default=False, help="Start the Feldera pipeline"
    )
    args = parser.parse_args()

    create_debezium_mysql_connector()
    pipeline = create_feldera_pipeline(args.api_url, args.start)

    if args.start:
        validate_results(pipeline)


def create_debezium_mysql_connector():
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print(f"Deleting old connector {CONNECTOR_NAME}")
    requests.delete(f"{connect_server}/connectors/{CONNECTOR_NAME}")

    print("Creating connector")
    config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "mysql",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "dbz",
            "database.server.id": "184054",
            "database.server.name": "dbserver1",
            "database.include.list": "inventory",
            "database.history.kafka.bootstrap.servers": "redpanda:9092",
            "topic.prefix": "inventory",
            "schema.history.internal.kafka.topic": "schema-changes.inventory.internal",
            "schema.history.internal.kafka.bootstrap.servers": "redpanda:9092",
            "include.schema.changes": "true",
            "decimal.handling.mode": "string",
        },
    }
    requests.post(f"{connect_server}/connectors", json=config).raise_for_status()

    print("Checking connector status")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/{CONNECTOR_NAME}/status")
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

    # Wait for the 5 expected Kafka topics to be created by the connector snapshot.
    print("Waiting for the connector to create Kafka topics")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/{CONNECTOR_NAME}/topics")
        if not response.ok:
            raise Exception(f"Error retrieving topic list from connector: {response}")
        topics = response.json()[CONNECTOR_NAME]["topics"]
        if all(t in topics for t in EXPECTED_TOPICS):
            print(f"All expected topics created.")
            break
        if time.time() - start_time >= 30:
            raise Exception(f"Timeout waiting for topic creation. Current: {topics}")
        print(f"Waiting for topics (have {len(topics)}/{len(EXPECTED_TOPICS)})")
        time.sleep(1)


def create_feldera_pipeline(api_url: str, start_pipeline: bool) -> Pipeline:
    pipeline_to_kafka_server = "redpanda:9092"
    program_sql = (
        open(PROJECT_SQL)
        .read()
        .replace("[REPLACE-BOOTSTRAP-SERVERS]", pipeline_to_kafka_server)
    )

    client = FelderaClient(api_url)
    print("Creating the pipeline...")
    pipeline = PipelineBuilder(
        client, name=PIPELINE_NAME, sql=program_sql
    ).create_or_replace()

    if start_pipeline:
        print("Starting the pipeline...")
        pipeline.start()
        print("Pipeline started")

    return pipeline


def validate_results(pipeline: Pipeline):
    """Query MySQL source and Feldera tables to show sample rows from each."""
    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_port = int(os.getenv("MYSQL_PORT", "3306"))

    print("\n--- MySQL source: inventory database ---")
    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user="root",
        password="debezium",
        database="inventory",
    )
    try:
        with conn.cursor() as cur:
            for table in ("customers", "products"):
                print(f"\n-- {table} (5 rows) --")
                cur.execute(f"SELECT * FROM {table} LIMIT 5")
                for row in cur.fetchall():
                    print(row)
    finally:
        conn.close()

    print("\n--- Feldera tables (replicated from MySQL via CDC) ---")
    for table in ("customers", "products"):
        print(f"\n-- {table} (5 rows) --")
        # Retry a few times since CDC might still be catching up.
        for attempt in range(20):
            try:
                rows = list(pipeline.query(f"SELECT * FROM {table} LIMIT 5"))
                if rows:
                    for row in rows:
                        print(row)
                    break
            except Exception as e:
                if attempt == 0:
                    print(f"(query failed: {e}, retrying...)")
            time.sleep(2)
        else:
            print("(no rows replicated within timeout — CDC might still be catching up)")


if __name__ == "__main__":
    main()
