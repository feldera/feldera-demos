---
description: Run /feldera-analyze pre-loaded with the fraud demo. No arguments needed.
---

# Feldera Analyze — Fraud Detection Demo

Before starting, show the user this overview:

> **Feldera Fraud Detection Demo**
>
> Imagine a bank's transaction stream is live — and a new card skimming attack has just been reported.
> How quickly can you detect it?
>
> In this demo, we take a real attack report, let the agent figure out what signals to look for,
> and watch it write and deploy the detection logic in real time — no manual SQL required.
> At the end, a fraud investigator scans the stream and flags suspicious cards automatically.

Then show the step-by-step plan:

```
┌─────────────────────────────────────────────────────────────┐
│                      PREPARATION                            │
│                                                             │
│  Step 1 ──► Check fda CLI                                   │
│                    │                                        │
│  Step 2 ──► Start Feldera (Docker or remote)                │
│                    │                                        │
│  Step 3 ──► Verify SQL Compiler                             │
│                    │                                        │
│  Step 4 ──► Load & Start Fraud Detection Pipeline           │
│                    │                                        │
│  Step 5 ──► Load New Attack Details                         │
└─────────────────────────────────────────────────────────────┘
```

Let's get started — running preparation steps now.

---

## Steps 1–3: Feldera setup

Read and follow `agentic-guides/setup/feldera-setup-docker.md` in full before continuing.
The `<FELDERA_HOST>` and `<FELDERA_API_KEY>` values resolved there apply to all commands below.

---

## Step 4: Load & Start Fraud Detection Pipeline

Tell the user: "🚀 **Loading fraud detection pipeline...**"

Read `agentic-fraud-detection/fraud_init.md` silently — no questions to the user.

If the file is missing or `ProgramPath` is absent, stop and tell the user what is missing.

Load these values automatically:
- **ProgramPath** — path to the SQL file (default: `agentic-fraud-detection/programs/fraud_detection_demo.sql`)
- **Pipeline name** — derived from the `ProgramPath` filename stem; this is also the deploy target
- **PatternDescription** — fetch `PatternURL` using the fetch script (WebFetch is blocked by news sites):

```bash
python3 utils/utils.py <PatternURL>
```

If the fetch fails, stop and tell the user the URL is not accessible and ask them to paste the article text directly.

Read and follow `agentic-guides/setup/feldera-load-pipeline.md`.

---

## Step 5: Load New Attack Details

Tell the user: "🔍 **Loading attack details...**"

Tell the user: "  ✅ Attack details loaded — will update pipeline: `<pipeline_name>`"

---

## Pause: Invite the user to explore

Tell the user:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **All set! Feldera is running at `<FELDERA_HOST>`**
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> 👉 Open the Web Console at **`<FELDERA_HOST>`** → click **`<pipeline_name>`** → browse the tables and views to see live data flowing in.
>
> ⏎ **Type `next` to launch the fraud pattern analysis.**
> Demo attack report: **Secret Service finds skimming devices** (`<PatternURL>`)

Wait for the user to type **next**, then show:

```
┌───────────────────────────────────────────────────────────────┐
│                   FRAUD ANALYSIS                              │
│                                                               │
│  📰 Step 1 ──► Load attack pattern                            │
│                    │                                          │
│  🗺️ Step 2 ──► Map attack signals to pipeline schema          │
│                    │                                          │
│  🧠 Step 3 ──► Generate SQL views to capture skimming signals │
│                    │                                          │
│  🚀 Step 4 ──► Validate & deploy expanded pipeline            │
│                    │                                          │
│  🔗 Step 5 ──► Build unified fraud_alerts view                │
│                    │                                          │
│  🔍 Step 6 ──► Launch live fraud investigator                 │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

For each step, announce it to the user with its icon and number before starting work:
- `📰 [1/6] Loading attack pattern...`
- `🗺️  [2/6] Mapping signals to schema...`
- `🧠 [3/6] Generating SQL detection views...`
- `🚀 [4/6] Validating & deploying...`
- `🔗 [5/6] Building fraud_alerts view...`
- `🔍 [6/6] Launching fraud investigator...`

Do **not** show raw tool calls or URLs to the user — use the step announcement instead.

---

## Core analysis (delegates to feldera-analyze.md — user-facing steps [1/6]–[4/6])

Read and follow `agentic-guides/shared-analyze/feldera-analyze.md` in full, using the values collected above.
User-facing steps [5/6] (fraud_alerts view) and [6/6] (investigator) are defined below.

### Fraud schema notes — read before generating views

| Column | Where | What it actually computes |
|--------|-------|--------------------------|
| `distance` | `TRANSACTION_WITH_DISTANCE`, `TRANSACTION_WITH_AGGREGATES` | `ST_DISTANCE` in degrees (not meters). Max across continental US ≈ 64°. Use `> 0.5` for ~55 km, `> 5` for ~550 km. |
| `count_1day` | `TRANSACTION_WITH_AGGREGATES` | Count of transactions with `distance > 0.5°` in the past day — **not** total daily transactions. With synthetic data spanning many years each card has ≤ 1 tx/day, so this is almost always 0 or 1. Use `>= 1`, not `> 10`. |
| `avg_1day` | `TRANSACTION_WITH_AGGREGATES` | Rolling 1-day average spend. Use as a baseline multiplier (e.g. `amt > avg_1day * 3`) rather than an absolute threshold. |

After deploying, immediately check `SELECT COUNT(*) FROM <view>` to confirm views are non-empty. If empty, lower thresholds or switch to standalone column conditions (e.g. `amt < 5.00`) that don't depend on rolling window sparsity.

After completing 🧠 **[3/6]**, pause and tell the user:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **Detection views generated** — review them above.
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> These views will be added to the **`<pipeline_name>`** pipeline and will start flagging suspicious transactions in real time.
>
> ⏎ **Type `next` to validate and deploy, or suggest changes.**

Wait for the user to type **next** (or incorporate any changes they suggest) before proceeding to 🚀 **[4/6]**.

---

## Analysis Step 5: Add fraud_alerts unified view

After the pipeline is running, build a `fraud_alerts` UNION view dynamically from the signal views actually generated — do **not** hardcode view names or columns.

### 5a — Identify the signal views

Collect every signal view name and its SELECT-list columns.

### 5b — Find common columns

For each column, check whether it appears in every signal view. A column is "common" if it can be projected from all branches, using `CAST(NULL AS <type>)` only when absent in a branch.

Always include:
- The identity column (e.g. `cc_num`)
- A timestamp column — use `ts` if present, `window_start AS ts` for window-based views
- An amount column (`amt`, `total_amt AS amt`, etc.) if available in all branches — use `CAST(NULL AS DOUBLE) AS amt` in branches where it is absent. Feldera auto-coerces numeric types so mixing `DECIMAL`/`DOUBLE` across branches is fine, but missing columns must be padded with a typed NULL.

### 5c — Generate and append the UNION

Write a `CREATE MATERIALIZED VIEW fraud_alerts AS ...` that UNIONs all signal views using the common columns, plus `'<signal_label>' AS signal_type` derived from each view name.

Append it to `$RUN_DIR/program.sql`, then read and follow `agentic-guides/setup/feldera-redeploy.md`.

After `fraud_alerts` is deployed and `<pipeline_name>` is running, pause and show the user a summary:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **Fraud detection pipeline is live!**
>
> **Pipeline:** `<pipeline_name>` — running at `<FELDERA_HOST>`
>
> **Detection views deployed:**
> _(list each signal view with a one-line description of what it detects)_
>
> **Unified view:** `fraud_alerts` — combines all signals into a single stream
>
> 👉 Open the Web Console at **`<FELDERA_HOST>`** → click **`<pipeline_name>`** → go to the **Change Stream** tab → select `fraud_alerts` to see flagged transactions in real time.
>
> ⏎ **Type `next` to launch the live fraud investigator.**
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Wait for the user to type **next** before proceeding to Analysis Step 6.

---

## Analysis Step 6: Launch the fraud investigator

Before launching, tell the user:

> **Launching fraud investigator**
> Will run for up to **30 seconds** or **100 cards**, whichever comes first.
> Press Ctrl+C to stop early.

Then run:

```bash
python3 agentic-fraud-detection/fraud_investigator.py <pipeline_name> --max-cards 100
```

This runs in the foreground. No API key required — classification is rule-based.
Do not background it — the user should see it running.

---

## Wrap-up: Stop the pipeline

After the investigator finishes, remind the user to stop the pipeline:

> **Demo complete!** When you're done exploring, stop the pipeline to free resources:
>
> ```bash
> fda --host <FELDERA_HOST> stop <pipeline_name>
> ```
