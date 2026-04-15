# Fraud Detection (Delta Lake) Demo

A real-time **feature engineering** demo: Feldera ingests credit-card transactions and cardholder demographics from public S3 Delta tables, computes feature vectors with rolling-window SQL aggregates, trains an XGBoost fraud classifier, and then runs the **same SQL queries** on a streaming inference dataset.

The default end-to-end runner is [`run.py`](./run.py). An interactive variant of the same flow lives in [`notebook.ipynb`](./notebook.ipynb) for users who want to step through cell-by-cell.

Companion blog post: <https://www.feldera.com/blog/feature-engineering-part2/>

## Prerequisites

- Docker (with Compose v2)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Data flow

```
s3://feldera-fraud-detection-data/{transaction_train,demographics_train}
                     │
                     ▼
       Feldera pipeline: fraud_detection_training
       (computes FEATURE view via rolling-window SQL)
                     │
                     ▼ pandas DataFrame
       XGBoost classifier  ────►  trained model
                                       │
                                       ▼
s3://feldera-fraud-detection-data/{transaction_infer,demographics_infer}
                     │
                     ▼
       Feldera pipeline: fraud_detection_inference
       (snapshot_and_follow on the transaction Delta log)
                     │
                     ▼ streaming feature vectors
                model.predict() ───► fraud / not-fraud
```

## Steps

### 0. Shut down any previous instance

```bash
docker compose -f fraud-detection-delta-lake/docker-compose.yml down -v
```

This stops the Feldera container and removes its volumes. Safe to run even if nothing is up.

### 1. Start Feldera

```bash
docker compose -f fraud-detection-delta-lake/docker-compose.yml up -d --wait
```

Wait until the service is healthy:

```bash
docker compose -f fraud-detection-delta-lake/docker-compose.yml ps
```

The feldera pipeline-manager image is around 2 GiB so the first pull may take a few minutes on slow networks.

### 2. Run the demo

```bash
uv run fraud-detection-delta-lake/run.py
```

`run.py` has a `# /// script` PEP 723 preamble, so `uv` resolves all Python deps (`pandas`, `scikit-learn`, `xgboost`, `feldera`) automatically — no virtualenv setup required.

What you will see:

1. **Training pipeline** — creates the `fraud_detection_training` Feldera pipeline with the `TRANSACTION` and `DEMOGRAPHICS` tables and a `FEATURE` view that computes rolling 1-day / 7-day / 30-day spend averages, day-of-week patterns, and 24-hour transaction frequency. Runs to completion against the training Delta tables in S3.
2. **Model training** — pulls the computed feature vectors into a Pandas DataFrame, trains an XGBoost classifier, and prints a confusion matrix plus precision / recall / F1.
3. **Inference pipeline** — creates the `fraud_detection_inference` pipeline that streams from the inference Delta table in `snapshot_and_follow` mode. For each chunk of newly-computed feature vectors it feeds them to the trained model in real time and prints the verdicts. Runs for 60 seconds, then shuts down.

Example output (truncated):

```
Running the training pipeline. Point your browser to http://localhost:8080/pipelines/fraud_detection_training/ to monitor the status of the pipeline.
Computed 1234567 feature vectors
Training the model
Testing the trained model
Confusion matrix:
[[245102    149]
 [   523    876]]
Precision: 85.46%
Recall: 62.62%
F1 Score: 72.27%

Running the inference pipeline for 60 seconds

Received 4096 feature vectors.
Confusion matrix:
[[4080   2]
 [   3  11]]
...
Shutting down the inference pipeline after 60 seconds
```

### 3. Inspect results

While `run.py` is running (especially during the 60-second inference window) you can monitor the live pipelines in the Feldera Web UI:

- Training pipeline: <http://localhost:8080/pipelines/fraud_detection_training>
- Inference pipeline: <http://localhost:8080/pipelines/fraud_detection_inference>

In the Ad-Hoc Query tab try (while the inference pipeline is running):

```sql
-- Sample of the streaming feature vectors
SELECT cc_num, hour_of_day, is_night, avg_spend_pd, avg_spend_pw, trans_freq_24, is_fraud
FROM feature
LIMIT 10;

-- Distribution of fraud vs. legit in the computed features
SELECT is_fraud, count(*) FROM feature GROUP BY is_fraud;
```

Make sure the user sees the precision / recall / F1 scores from the training step, plus the per-chunk inference verdicts during the streaming run.

## 4. Summary

Next steps for the user:

### Visit the Feldera Web UI at <http://localhost:8080>

Browse the two pipelines (`fraud_detection_training` and `fraud_detection_inference`), inspect the SQL programs, and watch throughput / memory in the Performance tab while the inference pipeline is running.

### Try modifying the SQL

The same `build_program(...)` function is used in both `run.py` and the notebook. Try adding a new feature column (e.g., a 1-hour rolling average, or merchant-category one-hot flags), re-run the training pipeline, and see how it changes precision/recall.

### Alternative: step through the demo interactively in Jupyter

If you'd rather walk through the same flow cell-by-cell with full Markdown explanations, use the notebook instead of `run.py`:

```bash
uv run --with jupyterlab \
       --with pandas==2.2.2 \
       --with scikit-learn==1.5.1 \
       --with xgboost==2.1.1 \
       --with feldera \
       jupyter lab fraud-detection-delta-lake/notebook.ipynb \
       --port=8888 --no-browser \
       --IdentityProvider.token='' --ServerApp.password=''
```

Then open <http://localhost:8888/lab/tree/notebook.ipynb> and run the cells top-to-bottom (Shift+Enter).

> **For Claude Code:** prefer `run.py` for the default flow — it's a single foreground command with deterministic output. Only launch Jupyter if the user explicitly asks for the interactive notebook (in which case start it in the background and report the URL above).

### Important: clean up when done

```bash
docker compose -f fraud-detection-delta-lake/docker-compose.yml down -v
```

This stops the container and removes its volumes. (If you launched Jupyter, also Ctrl+C in the terminal where it's running.)
