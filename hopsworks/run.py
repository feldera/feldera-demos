# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "hopsworks==4.7.4",
#     "confluent-kafka==2.14.0",
#     "faker==40.13.0",
#     "httpimport==1.4.1",
#     "xgboost==3.2.0",
#     "pandas==2.3.3",
#     "scikit-learn==1.8.0",
#     "joblib==1.5.3",
#     "pyarrow==23.0.1",
#     "feldera",
# ]
# ///
#
# Hopsworks + Feldera streaming feature pipeline demo.
#
# Drives a four-step end-to-end fraud-detection pipeline:
#   setup     -> generate synthetic data, push to Hopsworks Kafka + Feature Group
#   pipeline  -> build & run the Feldera SQL streaming feature pipeline
#   train     -> train + register + deploy an XGBoost classifier
#   inference -> predict on a few credit-card numbers via the online deployment
#
# Required environment:
#   HOPSWORKS_API_KEY   API key from https://eu-west.cloud.hopsworks.ai/account/api
#
# Optional environment:
#   HOPSWORKS_HOST      Hopsworks host (default: eu-west.cloud.hopsworks.ai)
#   FELDERA_URL         Feldera API URL (default: http://localhost:8080)
#
# Start Feldera:
# > docker compose -f hopsworks/docker-compose.yml up -d --wait
#
# Run all four steps end-to-end:
# > HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py all
#
# Or run individual steps:
# > HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py setup
# > HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py pipeline
# > HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py train
# > HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py inference
#
# Clean up:
# > docker compose -f hopsworks/docker-compose.yml down -v

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_HOPSWORKS_HOST = "eu-west.cloud.hopsworks.ai"
FELDERA_PIPELINE_NAME = "hopsworks_kafka"
PROFILE_FG_NAME = "profile"
DEPLOYMENT_NAME = "fraudonlinemodeldeployment"
MODEL_NAME = "xgboost_fraud_streaming_model"
PREDICT_SCRIPT = SCRIPT_DIR / "predict_example.py"

# Per-project Kafka SSL certs are downloaded by the hopsworks client into this
# folder on the host. The Feldera container has the same folder bind-mounted at
# HOPSWORKS_SECRETS_CONTAINER_DIR (see hopsworks/docker-compose.yml). When we
# build the Kafka connector config that Feldera will consume, we rewrite cert
# paths from the host-side absolute path to the container-side absolute path.
HOPSWORKS_SECRETS_HOST_DIR = SCRIPT_DIR / "hopsworks-secrets"
HOPSWORKS_SECRETS_CONTAINER_DIR = "/hopsworks-secrets"


def _login_hopsworks(host: str, api_key: str):
    """Log in to Hopsworks, downloading SSL certs into the local secrets folder."""
    HOPSWORKS_SECRETS_HOST_DIR.mkdir(parents=True, exist_ok=True)
    import hopsworks
    return hopsworks.login(
        host=host,
        api_key_value=api_key,
        cert_folder=str(HOPSWORKS_SECRETS_HOST_DIR),
    )


def _create_topic_idempotent(kafka_api, name: str, schema: dict):
    """Create a Kafka topic + schema, tolerating 409 conflicts.

    Hopsworks SaaS deletes Kafka topics asynchronously: a `delete_topic` call
    can return success while ZooKeeper still holds the topic for a few seconds.
    A subsequent re-create then fails with HTTP 409 'Kafka topic already exists
    in ZooKeeper'. Treat that as success — the topic ends up where we want.
    """
    from hopsworks_common.client.exceptions import RestAPIError
    try:
        kafka_api.create_schema(name, schema)
    except RestAPIError as e:
        if "already exists" not in str(e) and "190" not in str(e):
            raise
        print(f"  schema {name!r} already exists — reusing")
    try:
        kafka_api.create_topic(name, name, 1, replicas=1, partitions=1)
    except RestAPIError as e:
        if "already exists" not in str(e) and "190003" not in str(e):
            raise
        print(f"  topic {name!r} already in ZooKeeper — reusing")


def _rewrite_cert_paths_for_container(config: dict) -> dict:
    """Rewrite host-side SSL cert paths to the container-side mount path.

    `kafka_engine.get_kafka_config()` returns absolute file paths (ssl.ca.location,
    ssl.certificate.location, ssl.key.location, ...). Depending on the hsfs
    version these may live under our `cert_folder=` (HOPSWORKS_SECRETS_HOST_DIR)
    or under `/tmp/kafka_sc_*`. Feldera reads these paths from inside its
    container, which only has HOPSWORKS_SECRETS_HOST_DIR bind-mounted at
    HOPSWORKS_SECRETS_CONTAINER_DIR. So for any absolute cert path we see, we
    copy the file into HOPSWORKS_SECRETS_HOST_DIR (if it isn't already there)
    and rewrite the path to the container-side equivalent.
    """
    import shutil

    host_prefix = str(HOPSWORKS_SECRETS_HOST_DIR)
    HOPSWORKS_SECRETS_HOST_DIR.mkdir(parents=True, exist_ok=True)
    rewritten = {}
    for key, value in config.items():
        if not isinstance(value, str):
            rewritten[key] = value
            continue
        if value.startswith(host_prefix):
            rewritten[key] = HOPSWORKS_SECRETS_CONTAINER_DIR + value[len(host_prefix):]
            continue
        # Absolute path to an existing file outside the host secrets dir
        # (e.g. /tmp/kafka_sc_*.pem). Stage it into the secrets dir so the
        # Feldera container can read it via the bind mount.
        if value.startswith("/") and os.path.isfile(value):
            basename = os.path.basename(value)
            staged = HOPSWORKS_SECRETS_HOST_DIR / basename
            try:
                shutil.copyfile(value, staged)
            except OSError as e:
                print(f"  warning: could not stage cert {value} -> {staged}: {e}")
                rewritten[key] = value
                continue
            rewritten[key] = f"{HOPSWORKS_SECRETS_CONTAINER_DIR}/{basename}"
            continue
        rewritten[key] = value
    return rewritten


def main():
    parser = argparse.ArgumentParser(
        description="Hopsworks + Feldera streaming feature pipeline demo runner",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="Step 0: generate data, create profile FG + Kafka topic")
    sub.add_parser("pipeline", help="Step 1: build & run the Feldera streaming feature pipeline")
    sub.add_parser("train", help="Step 2: train + register + deploy XGBoost fraud model")
    sub.add_parser("inference", help="Step 3: predict on a few credit-card numbers")
    sub.add_parser("all", help="Run setup -> pipeline -> train -> inference in sequence")
    sub.add_parser("cleanup", help="Delete every Hopsworks resource the demo creates (FGs, FV, model, deployment, Kafka topics)")
    args = parser.parse_args()

    api_key = os.environ.get("HOPSWORKS_API_KEY")
    if not api_key:
        print("ERROR: HOPSWORKS_API_KEY environment variable is not set.")
        print("Sign up at https://app.hopsworks.ai and generate a key at")
        print("https://eu-west.cloud.hopsworks.ai/account/api , then re-run with:")
        print("  HOPSWORKS_API_KEY=apikey:... uv run hopsworks/run.py <cmd>")
        sys.exit(1)
    hopsworks_host = os.environ.get("HOPSWORKS_HOST", DEFAULT_HOPSWORKS_HOST)
    feldera_url = os.environ.get("FELDERA_URL", "http://localhost:8080")

    if args.cmd == "all":
        cmd_setup(api_key, hopsworks_host)
        cmd_pipeline(api_key, hopsworks_host, feldera_url)
        cmd_train(api_key, hopsworks_host)
        cmd_inference(api_key, hopsworks_host)
    elif args.cmd == "setup":
        cmd_setup(api_key, hopsworks_host)
    elif args.cmd == "pipeline":
        cmd_pipeline(api_key, hopsworks_host, feldera_url)
    elif args.cmd == "train":
        cmd_train(api_key, hopsworks_host)
    elif args.cmd == "inference":
        cmd_inference(api_key, hopsworks_host)
    elif args.cmd == "cleanup":
        cmd_cleanup(api_key, hopsworks_host)


# -----------------------------------------------------------------------------
# Step 0: Simulate data, push to Hopsworks Kafka + Feature Group
# Mirrors 0_simulate_data.ipynb
# -----------------------------------------------------------------------------
def cmd_setup(api_key: str, hopsworks_host: str):
    print("\n=== [0/3] SETUP: generating synthetic data and pushing to Hopsworks ===")
    print(
        "\n"
        "This step prepares the Hopsworks side of the demo. We generate synthetic\n"
        "cardholder profiles + credit-card transactions, push the static profiles\n"
        "into a `profile` Feature Group (online-enabled), create a Kafka topic +\n"
        "Avro schema in Hopsworks-managed Kafka, and stream all transactions into\n"
        "that topic — simulating an upstream payment processor. After this step\n"
        "Hopsworks holds the static reference data and a live stream of raw\n"
        "transactions ready for Feldera to consume.\n"
    )

    import httpimport
    from confluent_kafka import Producer

    print("Loading the synthetic_data module from the Hopsworks tutorials repo...")
    url = (
        "https://raw.githubusercontent.com/logicalclocks/hopsworks-tutorials/"
        "master/integrations/pyspark_streaming/synthetic_data"
    )
    synthetic_data = httpimport.load("synthetic_data", url)

    print("Generating simulated profiles + transactions...")
    data_simulator = synthetic_data.synthetic_data()
    profiles_df, trans_df = data_simulator.create_simulated_transactions()
    print(f"  -> {len(profiles_df)} profiles, {len(trans_df)} transactions")

    print(f"Logging in to Hopsworks at {hopsworks_host}...")
    project = _login_hopsworks(hopsworks_host, api_key)
    kafka_api = project.get_kafka_api()
    fs = project.get_feature_store()

    print(f"Creating/updating profile feature group (project_id={project.id})")
    profile_fg = fs.get_or_create_feature_group(
        name=PROFILE_FG_NAME,
        primary_key=["cc_num"],
        partition_key=["cc_provider"],
        online_enabled=True,
        version=1,
    )
    # In hsfs 4.x, `overwrite=True` clears the FG data via a `POST .../clear`
    # request which temporarily tears down the online-ingestion endpoint and
    # makes the very next insert race with re-registration (404). Plain insert
    # is safe here because the cleanup step (or a fresh project) starts with
    # an empty FG, and re-runs are idempotent on the (cc_num) primary key.
    # `wait=True` blocks until the offline materialization job completes —
    # required because cmd_pipeline reads this FG immediately afterwards.
    profile_fg.insert(profiles_df, wait=True)

    kafka_input_topic = f"transactions_topic_{project.id}"

    schema = {
        "type": "record",
        "name": kafka_input_topic,
        "namespace": "io.hops.examples.feldera.example",
        "fields": [
            {"name": "tid", "type": ["null", "string"]},
            {"name": "date_time", "type": ["null", {"type": "string", "logicalType": "timestamp-micros"}]},
            {"name": "cc_num", "type": ["null", "string"]},
            {"name": "category", "type": ["null", "string"]},
            {"name": "amount", "type": ["null", "double"]},
            {"name": "latitude", "type": ["null", "double"]},
            {"name": "longitude", "type": ["null", "double"]},
            {"name": "city", "type": ["null", "string"]},
            {"name": "country", "type": ["null", "string"]},
            {"name": "fraud_label", "type": ["null", "int"]},
        ],
    }

    existing_topics = [t.name for t in kafka_api.get_topics()]
    if kafka_input_topic not in existing_topics:
        print(f"Creating Kafka topic + schema {kafka_input_topic!r}")
        _create_topic_idempotent(kafka_api, kafka_input_topic, schema)
    else:
        print(f"Kafka topic {kafka_input_topic!r} already exists — reusing")

    from hsfs.core import kafka_engine
    kafka_config = kafka_engine.get_kafka_config(fs.id, {})

    print(f"Producing {len(trans_df)} transactions to {kafka_input_topic!r}...")
    trans_df = trans_df.rename(columns={"datetime": "date_time"})
    trans_df["tid"] = trans_df["tid"].astype("string")
    trans_df["date_time"] = trans_df["date_time"].astype("datetime64[s]").astype("string")
    trans_df["cc_num"] = trans_df["cc_num"].astype("string")
    trans_df["category"] = trans_df["category"].astype("string")
    trans_df["amount"] = trans_df["amount"].astype("double")
    trans_df["latitude"] = trans_df["latitude"].astype("double")
    trans_df["longitude"] = trans_df["longitude"].astype("double")
    trans_df["city"] = trans_df["city"].astype("string")
    trans_df["country"] = trans_df["country"].astype("string")
    trans_df["fraud_label"] = trans_df["fraud_label"].astype("int")

    producer = Producer(kafka_config)
    for index, transaction in trans_df.iterrows():
        producer.produce(kafka_input_topic, transaction.to_json())
        if index % 5000 == 0:
            producer.flush()
            print(f"  ...sent {index} / {len(trans_df)}")
    producer.flush()
    print(f"Done. {len(trans_df)} transactions queued in {kafka_input_topic!r}.")


# -----------------------------------------------------------------------------
# Step 1: Feldera streaming feature pipeline
# Mirrors 1_feature_pipeline.ipynb
# -----------------------------------------------------------------------------
def cmd_pipeline(api_key: str, hopsworks_host: str, feldera_url: str):
    print(f"\n=== [1/3] PIPELINE: starting Feldera pipeline against {feldera_url} ===")
    print(
        "\n"
        "This step is the core of the demo: real-time feature engineering with\n"
        "Feldera. We build a SQL pipeline with two input tables (TRANSACTIONS,\n"
        "PROFILES) and two output views:\n"
        "  - COMBINED enriches each transaction with cardholder demographics\n"
        "    (age at transaction, days until card expires).\n"
        "  - WINDOWED produces 4-hour hopping-window aggregates per cc_num\n"
        "    (avg / stddev amount, transaction count).\n"
        "PROFILES is loaded from the `profile` Hopsworks FG via input_pandas;\n"
        "TRANSACTIONS streams in from Hopsworks Kafka. Both output views write\n"
        "back to Hopsworks Kafka in Avro format. We let the pipeline run for 60s,\n"
        "then schedule the Hopsworks materialization jobs that periodically push\n"
        "the Kafka data into the offline + online feature stores.\n"
    )

    from feldera import FelderaClient, PipelineBuilder
    from hsfs.core import kafka_engine
    from hsfs.feature import Feature

    print(f"Logging in to Hopsworks at {hopsworks_host}...")
    project = _login_hopsworks(hopsworks_host, api_key)
    kafka_api = project.get_kafka_api()
    fs = project.get_feature_store()

    kafka_output_topics = [
        f"transactions_fraud_streaming_fg_{project.id}",
        f"transactions_aggs_fraud_streaming_fg_{project.id}",
    ]
    kafka_input_topic = f"transactions_topic_{project.id}"

    # --- COMBINED feature group: enriched per-transaction features
    print(f"Creating combined feature group {kafka_output_topics[0]!r}")
    combined_fg = fs.get_or_create_feature_group(
        name=kafka_output_topics[0],
        primary_key=["cc_num"],
        online_enabled=True,
        version=1,
        topic_name=kafka_output_topics[0],
        event_time="date_time",
        stream=True,
        features=[
            Feature("tid", type="string"),
            Feature("date_time", type="timestamp"),
            Feature("cc_num", type="string"),
            Feature("category", type="string"),
            Feature("amount", type="double"),
            Feature("latitude", type="double"),
            Feature("longitude", type="double"),
            Feature("city", type="string"),
            Feature("country", type="string"),
            Feature("fraud_label", type="int"),
            Feature("age_at_transaction", type="int"),
            Feature("days_until_card_expires", type="int"),
            Feature("cc_expiration_date", type="timestamp"),
        ],
    )
    try:
        combined_fg.save()
    except Exception as e:
        print(f"  combined_fg.save(): {e}")

    existing_topics = [t.name for t in kafka_api.get_topics()]
    if kafka_output_topics[0] not in existing_topics:
        _create_topic_idempotent(
            kafka_api, kafka_output_topics[0], json.loads(combined_fg.avro_schema)
        )

    # --- WINDOWED feature group: 4h hopping-window aggregates
    print(f"Creating windowed feature group {kafka_output_topics[1]!r}")
    windowed_fg = fs.get_or_create_feature_group(
        name=str(kafka_output_topics[1]),
        primary_key=["cc_num"],
        online_enabled=True,
        version=1,
        topic_name=kafka_output_topics[1],
        event_time="date_time",
        stream=True,
        features=[
            Feature("avg_amt", type="double"),
            Feature("trans", type="bigint"),
            Feature("stddev_amt", type="double"),
            Feature("date_time", type="timestamp"),
            Feature("cc_num", type="string"),
        ],
    )
    try:
        windowed_fg.save()
    except Exception as e:
        print(f"  windowed_fg.save(): {e}")

    existing_topics = [t.name for t in kafka_api.get_topics()]
    if kafka_output_topics[1] not in existing_topics:
        _create_topic_idempotent(
            kafka_api, kafka_output_topics[1], json.loads(windowed_fg.avro_schema)
        )

    # --- Build Feldera SQL pipeline with Hopsworks Kafka source/sinks
    # `kafka_config` from hsfs references PEM files in the host secrets dir.
    # Rewrite to the container mount path so Feldera (in Docker) can read them.
    kafka_config = _rewrite_cert_paths_for_container(
        kafka_engine.get_kafka_config(fs.id, {})
    )

    transaction_source_config = json.dumps(
        {
            "transport": {
                "name": "kafka_input",
                "config": kafka_config | {"topics": [kafka_input_topic], "auto.offset.reset": "earliest"},
            },
            "format": {"name": "json", "config": {"update_format": "raw", "array": False}},
        }
    )

    def make_sink_config(fg):
        return kafka_config | {
            "topic": fg.topic_name,
            "auto.offset.reset": "earliest",
            "headers": [
                {"key": "projectId", "value": str(project.id)},
                {"key": "featureGroupId", "value": str(fg.id)},
                {"key": "subjectId", "value": str(fg.subject["id"])},
            ],
        }

    combined_sink_config = json.dumps(
        {
            "transport": {"name": "kafka_output", "config": make_sink_config(combined_fg)},
            "format": {
                "name": "avro",
                "config": {"schema": combined_fg.avro_schema, "skip_schema_id": True},
            },
        }
    )
    windowed_sink_config = json.dumps(
        {
            "transport": {"name": "kafka_output", "config": make_sink_config(windowed_fg)},
            "format": {
                "name": "avro",
                "config": {"schema": windowed_fg.avro_schema, "skip_schema_id": True},
            },
        }
    )

    sql = build_sql(transaction_source_config, combined_sink_config, windowed_sink_config)

    print(f"Connecting to Feldera at {feldera_url}")
    client = FelderaClient(feldera_url)
    print(f"Creating Feldera pipeline {FELDERA_PIPELINE_NAME!r}")
    pipeline = PipelineBuilder(
        client, name=FELDERA_PIPELINE_NAME, sql=sql
    ).create_or_replace()

    print("Starting Feldera pipeline...")
    pipeline.start()

    # Push the static profile data into the PROFILES table.
    profile_fg = fs.get_or_create_feature_group(name=PROFILE_FG_NAME, version=1)
    profile_df = profile_fg.read()
    print(f"Pushing {len(profile_df)} profiles into Feldera PROFILES table")
    pipeline.input_pandas("PROFILES", profile_df)

    print("Letting the pipeline run for 60 seconds (it streams transactions from Hopsworks Kafka)...")
    time.sleep(60)
    pipeline.stop(force=True)
    print("Feldera pipeline stopped.")

    # Schedule + trigger Hopsworks materialization jobs so the Kafka data
    # gets pushed into the offline + online stores.
    print("Scheduling materialization jobs (every 10 minutes) and triggering an immediate run")
    for fg in (combined_fg, windowed_fg):
        try:
            fg.materialization_job.schedule(
                cron_expression="0 /10 * ? * * *",
                start_time=datetime.datetime.now(tz=datetime.timezone.utc),
            )
            fg.materialization_job.run()
        except Exception as e:
            print(f"  materialization_job for {fg.name}: {e}")


def build_sql(transaction_source_config: str, combined_sink_config: str, windowed_sink_config: str) -> str:
    return f"""
    CREATE TABLE TRANSACTIONS(
        tid STRING,
        date_time TIMESTAMP,
        cc_num STRING,
        category STRING,
        amount DECIMAL(38, 2),
        latitude DOUBLE,
        longitude DOUBLE,
        city STRING,
        country STRING,
        fraud_label INT
    ) WITH (
        'connectors' = '[{transaction_source_config}]'
    );

    CREATE TABLE PROFILES(
        cc_num STRING,
        cc_provider STRING,
        cc_type STRING,
        cc_expiration_date STRING,
        name STRING,
        mail STRING,
        birthdate TIMESTAMP,
        age INT,
        city STRING,
        country_of_residence STRING
    );

    -- Convert credit card expiration date from MM/YY formatted string to a TIMESTAMP.
    CREATE LOCAL VIEW CC_EXPIRATION as
        SELECT
            cc_num,
            CAST(
                CONCAT('20', SUBSTRING(cc_expiration_date, 4, 2), '-',
                       SUBSTRING(cc_expiration_date, 1, 2), '-01 00:00:00')
                AS TIMESTAMP
            ) AS cc_expiration_date
        FROM PROFILES;

    -- Enrich each transaction with the cardholder's age and days-until-expiry.
    -- The combined_fg Avro schema declares `amount` as DOUBLE, while the
    -- TRANSACTIONS table types it as DECIMAL(38,2) (i128) — cast explicitly.
    CREATE VIEW COMBINED
    WITH ('connectors' = '[{combined_sink_config}]')
    AS
        SELECT
            T1.tid,
            T1.date_time,
            T1.cc_num,
            T1.category,
            CAST(T1.amount AS DOUBLE) AS amount,
            T1.latitude,
            T1.longitude,
            T1.city,
            T1.country,
            T1.fraud_label,
            T2.cc_expiration_date,
            TIMESTAMPDIFF(YEAR, T3.birthdate, T1.date_time) age_at_transaction,
            TIMESTAMPDIFF(DAY, T1.date_time, T2.cc_expiration_date) days_until_card_expires
        FROM
            TRANSACTIONS T1 JOIN cc_expiration T2 ON T1.cc_num = T2.cc_num
            JOIN PROFILES T3 ON T1.cc_num = T3.cc_num;

    -- 4-hour hopping window over transactions, stepping every 1 hour.
    CREATE LOCAL VIEW HOP as
        SELECT *
        FROM TABLE(HOP(TABLE TRANSACTIONS, DESCRIPTOR(date_time), INTERVAL 4 HOURS, INTERVAL 1 HOURS));

    CREATE LOCAL VIEW AGG as
        SELECT
            AVG(amount) AS avg_amt,
            STDDEV(amount) as stddev_amt,
            COUNT(cc_num) as trans,
            ARRAY_AGG(date_time) as moments,
            cc_num
        FROM hop
        GROUP BY cc_num, window_start;

    -- AVG/STDDEV over DECIMAL(38,2) produce DECIMAL (i128); the windowed FG's
    -- Avro schema declares avg_amt/stddev_amt as DOUBLE, so cast explicitly.
    CREATE VIEW WINDOWED
    WITH ('connectors' = '[{windowed_sink_config}]')
    AS
        SELECT
            CAST(avg_amt AS DOUBLE) AS avg_amt,
            trans,
            CAST(COALESCE(stddev_amt, 0) AS DOUBLE) AS stddev_amt,
            date_time,
            cc_num
        FROM agg CROSS JOIN UNNEST(moments) as date_time;
    """


# -----------------------------------------------------------------------------
# Step 2: Train + register + deploy XGBoost model
# Mirrors 2_training.ipynb
# -----------------------------------------------------------------------------
def cmd_train(api_key: str, hopsworks_host: str):
    print("\n=== [2/3] TRAIN: training XGBoost classifier and deploying to Hopsworks ===")
    print(
        "\n"
        "This step trains a fraud-detection model on the features Feldera produced.\n"
        "We build a Hopsworks Feature View by joining the two streaming FGs (with\n"
        "a label_encoder transformation on `category`), produce a chronological\n"
        "train/test split, train an XGBoost binary classifier, and report\n"
        "confusion matrix + macro F1. The trained model is saved, registered in\n"
        "the Hopsworks Model Registry with input/output schemas + an example\n"
        "input, and finally deployed via Hopsworks Model Serving using\n"
        "`predict_example.py` as the predictor (which fetches feature vectors\n"
        "from the online store and runs the model). This deployment is what we\n"
        "exercise in the inference step.\n"
    )

    import joblib
    import xgboost as xgb
    from hsml.model_schema import ModelSchema
    from hsml.schema import Schema
    from sklearn.metrics import confusion_matrix, f1_score

    print(f"Logging in to Hopsworks at {hopsworks_host}...")
    project = _login_hopsworks(hopsworks_host, api_key)
    fs = project.get_feature_store()

    print("Retrieving feature groups produced by the Feldera pipeline...")
    trans_fg = fs.get_feature_group(
        name=f"transactions_fraud_streaming_fg_{project.id}", version=1
    )
    window_aggs_fg = fs.get_feature_group(
        name=f"transactions_aggs_fraud_streaming_fg_{project.id}", version=1
    )

    selected_features = trans_fg.select(
        ["fraud_label", "category", "amount", "date_time",
         "age_at_transaction", "days_until_card_expires"]
    ).join(window_aggs_fg.select_except(["cc_num", "date_time"]))

    label_encoder = fs.get_transformation_function(name="label_encoder")

    print("Creating/getting feature view 'transactions_view_streaming_fv'")
    feature_view = fs.get_or_create_feature_view(
        name="transactions_view_streaming_fv",
        version=1,
        query=selected_features,
        labels=["fraud_label"],
        transformation_functions=[label_encoder("category")],
    )

    print("Building train/test split...")
    X_train, X_test, y_train, y_test = feature_view.train_test_split(test_size=0.2)

    X_train = X_train.sort_values("date_time")
    y_train = y_train.reindex(X_train.index)
    X_test = X_test.sort_values("date_time")
    y_test = y_test.reindex(X_test.index)

    X_train.drop(["date_time"], axis=1, inplace=True)
    X_test.drop(["date_time"], axis=1, inplace=True)

    print(f"Training XGBoost on {len(X_train)} rows...")
    clf = xgb.XGBClassifier()
    clf.fit(X_train.values, y_train)

    y_pred_test = clf.predict(X_test.values)
    metrics = {"f1_score": float(f1_score(y_test, y_pred_test, average="macro"))}
    print(f"\nF1 score (macro): {metrics['f1_score']:.4f}")

    cm = confusion_matrix(y_test, y_pred_test)
    print("Confusion matrix [[TN FP] [FN TP]]:")
    print(cm)

    # --- Register the model
    model_dir = SCRIPT_DIR / "fraud_streaming_model"
    model_dir.mkdir(exist_ok=True)
    joblib.dump(clf, model_dir / "xgboost_fraud_streaming_model.pkl")

    input_schema = Schema(X_train.values)
    output_schema = Schema(y_train)
    model_schema = ModelSchema(input_schema=input_schema, output_schema=output_schema)

    mr = project.get_model_registry()
    print(f"Registering model {MODEL_NAME!r}")
    fraud_model = mr.python.create_model(
        name=MODEL_NAME,
        metrics=metrics,
        model_schema=model_schema,
        input_example=X_train.sample(),
        description="Fraud streaming predictor (Feldera + Hopsworks)",
    )
    fraud_model.save(str(model_dir))

    # --- Deploy the predictor
    if not PREDICT_SCRIPT.exists():
        print(f"ERROR: {PREDICT_SCRIPT} is missing — deployment cannot proceed.")
        sys.exit(1)

    dataset_api = project.get_dataset_api()
    uploaded_path = dataset_api.upload(str(PREDICT_SCRIPT), "Models", overwrite=True)
    predictor_script_path = os.path.join("/Projects", project.name, uploaded_path)

    print(f"Deploying model as {DEPLOYMENT_NAME!r}")
    deployment = fraud_model.deploy(
        name=DEPLOYMENT_NAME,
        script_file=predictor_script_path,
    )

    print("Waiting for deployment to warm up (45s)...")
    time.sleep(45)
    deployment.start(await_running=300)
    print(f"Deployment state: {deployment.get_state().describe()}")


# -----------------------------------------------------------------------------
# Step 3: Run a few inference predictions against the deployment
# Mirrors 3_inference.ipynb
# -----------------------------------------------------------------------------
def cmd_inference(api_key: str, hopsworks_host: str):
    print("\n=== [3/3] INFERENCE: predicting fraud against the deployed model ===")
    print(
        "\n"
        "This step exercises the online predictor we deployed in the train step.\n"
        "We pick 5 credit-card numbers from the streaming feature group, then for\n"
        "each one call deployment.predict() — Hopsworks looks up the latest\n"
        "feature vector from the online store and runs it through the XGBoost\n"
        "model. Each prediction is FRAUD (1) or OK (0). Finally we stop the\n"
        "deployment to free serving resources.\n"
    )

    print(f"Logging in to Hopsworks at {hopsworks_host}...")
    project = _login_hopsworks(hopsworks_host, api_key)
    fs = project.get_feature_store()

    trans_fg = fs.get_feature_group(
        name=f"transactions_fraud_streaming_fg_{project.id}", version=1
    )
    cc_nums = trans_fg.select("cc_num").show(5).cc_num.values
    print(f"Sampling 5 cc_num values: {list(cc_nums)}")

    ms = project.get_model_serving()
    deployment = ms.get_deployment(DEPLOYMENT_NAME)
    print(f"Starting deployment {DEPLOYMENT_NAME!r}...")
    deployment.start(await_running=300)

    print("\nPredictions:")
    for cc_num in cc_nums:
        try:
            result = deployment.predict(inputs=[int(cc_num)])
            verdict = "FRAUD" if result["predictions"][0] == 1 else "OK"
            print(f"  cc_num={cc_num}  ->  {verdict}  (raw: {result['predictions']})")
        except Exception as e:
            print(f"  cc_num={cc_num}  ->  ERROR: {e}")

    print("\nStopping deployment to free resources...")
    deployment.stop(await_stopped=180)
    print("Done.")


# -----------------------------------------------------------------------------
# Cleanup: delete every Hopsworks resource the demo creates
# -----------------------------------------------------------------------------
def cmd_cleanup(api_key: str, hopsworks_host: str):
    print("\n=== CLEANUP: deleting Hopsworks resources created by this demo ===")
    print(
        "\n"
        "Hopsworks state is normally reused across runs (`get_or_create_*`), so\n"
        "this step is only needed if you want a fresh slate. Deletes happen in\n"
        "dependency order: deployment -> model versions -> feature view ->\n"
        "feature groups -> Kafka topics + schema subjects. Errors on individual\n"
        "resources (e.g. not present) are tolerated, so cleanup is safe to re-run.\n"
    )

    print(f"Logging in to Hopsworks at {hopsworks_host}...")
    project = _login_hopsworks(hopsworks_host, api_key)
    pid = project.id
    print(f"Project: {project.name} (id={pid})")

    fg_names = [
        PROFILE_FG_NAME,
        f"transactions_fraud_streaming_fg_{pid}",
        f"transactions_aggs_fraud_streaming_fg_{pid}",
    ]
    topic_names = [
        f"transactions_topic_{pid}",
        f"transactions_fraud_streaming_fg_{pid}",
        f"transactions_aggs_fraud_streaming_fg_{pid}",
    ]

    # --- Deployment
    try:
        ms = project.get_model_serving()
        dep = ms.get_deployment(DEPLOYMENT_NAME)
        if dep is not None:
            print(f"Deleting deployment {DEPLOYMENT_NAME!r}")
            try:
                dep.stop(await_stopped=120)
            except Exception as e:
                print(f"  stop ignored: {e}")
            dep.delete(force=True)
        else:
            print(f"Deployment {DEPLOYMENT_NAME!r}: not present")
    except Exception as e:
        print(f"  deployment cleanup error: {e}")

    # --- Model (all versions)
    try:
        mr = project.get_model_registry()
        models = mr.get_models(MODEL_NAME)
        if models:
            for m in models:
                print(f"Deleting model {m.name} v{m.version}")
                m.delete()
        else:
            print(f"Model {MODEL_NAME!r}: not present")
    except Exception as e:
        print(f"  model cleanup error: {e}")

    # --- Feature view (must go before the FGs it references)
    fs = project.get_feature_store()
    try:
        for fv in fs.get_feature_views(name="transactions_view_streaming_fv"):
            print(f"Deleting feature view {fv.name} v{fv.version}")
            fv.delete()
    except Exception as e:
        print(f"  feature view cleanup error: {e}")

    # --- Feature groups (delete every version)
    for name in fg_names:
        try:
            fgs = fs.get_feature_groups(name=name)
            if not fgs:
                print(f"FG {name!r}: not present")
                continue
            for fg in fgs:
                print(f"Deleting FG {fg.name} v{fg.version} (id={fg.id})")
                fg.delete()
        except Exception as e:
            print(f"  fg {name}: {e}")

    # --- Kafka topics + schema subjects
    try:
        kafka_api = project.get_kafka_api()
        topics_by_name = {t.name: t for t in kafka_api.get_topics()}
        for topic in topic_names:
            if topic in topics_by_name:
                print(f"Deleting Kafka topic {topic!r}")
                try:
                    topics_by_name[topic].delete()
                except Exception as e:
                    print(f"  delete topic {topic}: {e}")
            else:
                print(f"Kafka topic {topic!r}: not present")
    except Exception as e:
        print(f"  kafka cleanup error: {e}")

    print("\nCleanup complete.")


if __name__ == "__main__":
    main()
