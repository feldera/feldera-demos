---
description: "Run /feldera-analyze pre-loaded with the Fine-Grained Authorization (FGA) demo. No arguments needed."
---

# Feldera Analyze — Fine-Grained Authorization Demo

Before starting, show the user this overview:

> **Feldera Fine-Grained Authorization Demo**
>
> Imagine a shared file system where thousands of users access millions of files every day —
> governed by group permissions that cascade through folder hierarchies.
>
> How do you spot when something is wrong?
>
> In this demo, we take a set of real-world access anomaly patterns and let the agent map them
> to a live authorization pipeline — writing detection logic in real time, no manual SQL required.
> At the end, a live investigator scans the access stream and flags suspicious users automatically.

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
│  Step 4 ──► Load & Start FGA Pipeline                       │
│                    │                                        │
│  Step 5 ──► Load Access Anomaly Patterns                    │
└─────────────────────────────────────────────────────────────┘
```

Let's get started — running preparation steps now.

---

## Steps 1–3: Feldera setup

Read and follow `agentic-guides/setup/feldera-setup-docker.md` in full before continuing.
The `<FELDERA_HOST>` and `<FELDERA_API_KEY>` values resolved there apply to all commands below.

---

## Step 4: Load & Start FGA Pipeline

Tell the user: "🚀 **Loading Fine-Grained Authorization pipeline...**"

Read `agentic-fine-grained-access/fga_init.md` silently — no questions to the user.

If the file is missing or `ProgramPath` is absent, stop and tell the user what is missing.

Load these values automatically:
- **ProgramPath** — path to the SQL file (default: `agentic-fine-grained-access/programs/fga.sql`)
- **Pipeline name** — derived from the `ProgramPath` filename stem: `fga`
- **PatternDescription** — `PatternURL` is a local file path; read it directly with the Read tool (do not use `utils.py`)

Read and follow `agentic-guides/setup/feldera-load-pipeline.md`.

---

## Step 5: Load Access Anomaly Patterns

Tell the user: "🔍 **Loading access anomaly patterns...**"

Tell the user: "  ✅ Access patterns loaded — will update pipeline: `<pipeline_name>`"

---

## Pause: Invite the user to explore

Tell the user:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **All set! Feldera is running at `<FELDERA_HOST>`**
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> 👉 Open the Web Console at **`<FELDERA_HOST>`** → click **`<pipeline_name>`** → browse the tables and views to see live data flowing in.
>
> The pipeline resolves group-based read/write permissions across a folder hierarchy using recursive SQL,
> and logs every access request in real time.
>
> ⏎ **Type `next` to launch the access anomaly analysis.**

Wait for the user to type **next**, then show:

```
┌───────────────────────────────────────────────────────────────┐
│                   ACCESS ANOMALY ANALYSIS                     │
│                                                               │
│  📰 Step 1 ──► Load anomaly patterns                          │
│                    │                                          │
│  🗺️ Step 2 ──► Map signals to pipeline schema                 │
│                    │                                          │
│  🧠 Step 3 ──► Generate SQL views to capture anomaly signals  │
│                    │                                          │
│  🚀 Step 4 ──► Validate & deploy expanded pipeline            │
│                    │                                          │
│  🔗 Step 5 ──► Build unified access_alerts view               │
│                    │                                          │
│  🔍 Step 6 ──► Launch live access investigator                │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

For each step, announce it to the user with its icon and number before starting work:
- `📰 [1/6] Loading anomaly patterns...`
- `🗺️  [2/6] Mapping signals to schema...`
- `🧠 [3/6] Generating SQL detection views...`
- `🚀 [4/6] Validating & deploying...`
- `🔗 [5/6] Building access_alerts view...`
- `🔍 [6/6] Launching access investigator (with real-time blocking)...`

Do **not** show raw tool calls or file paths to the user — use the step announcement instead.

---

## Core analysis (delegates to feldera-analyze.md — user-facing steps [1/6]–[4/6])

Read and follow `agentic-guides/shared-analyze/feldera-analyze.md` in full, using the values collected above.
User-facing steps [5/6] (access_alerts view) and [6/6] (investigator) are defined below.

### FGA schema notes — read before generating views

| Column | Where | What it actually contains |
|--------|-------|--------------------------|
| `ts` | `access_log` | Timestamp of the access request — use `TUMBLE` or `HOP` windows for burst/hot-folder detection |
| `access_request` | `access_log` | Either `'read'` or `'write'` — filter to distinguish read vs write anomalies |
| `file_id` | `access_log` | References `files.id` — join with `files` to get `parent_id` for subtree scoping |
| `user_can_read` / `user_can_write` | materialized views | Use to detect unauthorized access — join with `access_log` on `user_id` and `file_id` |
| `parent_id` | `files` | Folder hierarchy — group by `parent_id` to scope detections to subtrees |

**Key design guidance:**
- Use `TUMBLE` windows on `ts` for burst and hot-folder detection (e.g., 1-hour windows)
- Use `HOP` windows for exploration detection over a longer baseline
- Join `access_log` with `user_can_read`/`user_can_write` to detect unauthorized access attempts
- Group by `(user_id, files.parent_id)` to scope to subtrees rather than individual files
- The data generator produces 1M access events spanning 2026, plus a planted attacker (user_id=42) with 500 events in a single 1-hour window accessing ~500 distinct parent folders. Set thresholds that let normal users pass but catch user 42 — e.g., enumeration threshold ≥ 20 distinct folders/hour (normal max ≈ 4), hot-folder threshold ≥ 20 distinct users/6h.

**REQUIRED: include `metric_value BIGINT` in every signal view.**
This column carries the key count that triggered the signal (e.g., `folder_count` for enumeration, `user_count` for hot-folder). The investigator reads `MAX(metric_value)` to gauge severity.

After deploying, immediately check `SELECT COUNT(*) FROM <view>` to confirm views are non-empty.
If empty, lower thresholds — synthetic data is uniformly distributed so bursts are moderate.
Also verify `SELECT user_id, metric_value FROM <view> ORDER BY metric_value DESC LIMIT 5` — user 42 should appear at the top for enumeration views.

After completing 🧠 **[3/6]**, pause and tell the user:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **Detection views generated** — review them above.
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> These views will be added to the **`<pipeline_name>`** pipeline and will start flagging suspicious access in real time.
>
> ⏎ **Type `next` to validate and deploy, or suggest changes.**

Wait for the user to type **next** (or incorporate any changes they suggest) before proceeding to 🚀 **[4/6]**.

---

## Analysis Step 5: Add access_alerts unified view

After the pipeline is running, build an `access_alerts` UNION view dynamically from the signal views actually generated — do **not** hardcode view names or columns.

### 5a — Identify the signal views

Collect every signal view name and its SELECT-list columns.

### 5b — Find common columns

For each column, check whether it appears in every signal view. Always include:
- An identity column (`user_id`)
- A timestamp column — use `ts` if present, `window_start AS ts` for window-based views
- A file identifier (`file_id`) if available — use `CAST(NULL AS BIGINT) AS file_id` where absent
- `metric_value BIGINT` — the key count from each view (e.g., `folder_count`, `user_count`); use `CAST(NULL AS BIGINT) AS metric_value` where absent

### 5c — Generate and append the UNION

Write a `CREATE MATERIALIZED VIEW access_alerts AS ...` that UNIONs all signal views using the common columns, plus `'<signal_label>' AS signal_type` derived from each view name. The `metric_value` column must be present in all branches — pad with `CAST(NULL AS BIGINT)` where a view does not have one.

Append it to `$RUN_DIR/program.sql`, then read and follow `agentic-guides/setup/feldera-redeploy.md`.

After `access_alerts` is deployed and `<pipeline_name>` is running, pause and show the user a summary:

> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
> **Access anomaly detection pipeline is live!**
>
> **Pipeline:** `<pipeline_name>` — running at `<FELDERA_HOST>`
>
> **Detection views deployed:**
> _(list each signal view with a one-line description of what it detects)_
>
> **Unified view:** `access_alerts` — combines all signals into a single stream
>
> 👉 Open the Web Console at **`<FELDERA_HOST>`** → click **`<pipeline_name>`** → go to the **Change Stream** tab → select `access_alerts` to see flagged access events in real time.
>
> ⏎ **Type `next` to launch the live access investigator.**
> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Wait for the user to type **next** before proceeding to Analysis Step 6.

---

## Analysis Step 6: Launch the access investigator with blocking

Before launching, tell the user:

> **Launching access investigator with real-time blocking**
> SUSPICIOUS users will have `is_banned=true` pushed to the `users` table immediately.
> Feldera recomputes `user_can_read` and `user_can_write` in real time — their access is revoked within milliseconds.
> Will run for up to **30 seconds** or **200 users**, whichever comes first.
> Press Ctrl+C to stop early.

Then run:

```bash
python3 agentic-fine-grained-access/fga_investigator.py <pipeline_name> --max-users 200 --block
```

This runs in the foreground. No API key required — classification is rule-based.
Do not background it — the user should see it running.

After it finishes, show the user how to verify that blocked users have been removed from the permission views:

```bash
fda --host <FELDERA_HOST> query <pipeline_name> \
  "SELECT u.id, u.name, u.is_banned FROM users u WHERE u.is_banned = true LIMIT 10" \
  --format json
```

---

## Wrap-up: Stop the pipeline

After the investigator finishes, remind the user to stop the pipeline:

> **Demo complete!** When you're done exploring, stop the pipeline to free resources:
>
> ```bash
> fda --host <FELDERA_HOST> stop <pipeline_name>
> ```
