---
description: "Add real-time detection views to an existing Feldera pipeline from any pattern description — fraud, anomaly detection, policy violations, etc. Setup-agnostic: assumes FELDERA_HOST, ProgramPath, and PatternDescription are already in context."
---

# Feldera Analyze — Detection View Engine

Given `<FELDERA_HOST>`, `<ProgramPath>`, and `<PatternDescription>` already in context, this skill:
1. Reads the schema from the SQL file
2. Fetches and analyzes the pattern
3. Generates Feldera SQL detection views
4. Validates and deploys the updated pipeline

---

## Step 1: Read schema

Read `<ProgramPath>` to understand all available tables, columns, types, and relationships.
No need to fetch the schema from Feldera.

---

## Step 2: Fetch pattern description and SQL docs

Load the **pattern description**, then fetch relevant **Feldera SQL docs**:

**Pattern description:**
- If `<PatternDescription>` is **pasted text or inline content**: use as-is — do not fetch anything.
- If `<PatternDescription>` is a **URL**: fetch it using the fetch script (WebFetch is blocked by many news sites):

```bash
python3 utils/utils.py <PatternDescription>
```

If the fetch fails, tell the user the URL is not accessible and ask them to paste the article or describe the pattern in plain text. Do not proceed until you have the pattern content.

**Feldera SQL docs** — fetch only pages relevant to the pattern. Skip any that fail silently.

| Construct | URL |
|-----------|-----|
| Window aggregates (TUMBLE, HOP) | https://docs.feldera.com/sql/streaming |
| Aggregates (SUM, AVG, COUNT...) | https://docs.feldera.com/sql/aggregates |
| Date / time functions | https://docs.feldera.com/sql/datetime |
| Table functions (TUMBLE, HOP syntax) | https://docs.feldera.com/sql/table |
| SQL types | https://docs.feldera.com/sql/types |

---

## Step 3: Analyze and generate detection SQL

Before writing any SQL, consult `agentic-guides/shared-analyze/feldera-sql-generator.md` for rules and known pitfalls.

### 3a — Column audit

List every column across all tables and views. For each column ask:
> "Could this column indicate the pattern, narrow it down, or distinguish it from normal behaviour?"

Pay attention to columns that are easy to overlook:
- **Categorical / enum columns** — type, category, status, role, action
- **Identity / grouping columns** — user, device, merchant, location
- **Temporal columns** — timestamps, intervals
- **Numeric columns** — amounts, counts, scores

Only exclude a column if you can explicitly state why it is irrelevant to the pattern.

### 3b — Relevance check (show to user)

Before generating any views, assess whether the pattern can actually be detected from this schema.

Ask yourself:
- Does the pattern require data that exists in any table or view?
- Can at least one key signal be expressed using available columns?

**If the pattern is unrelated to the schema** (e.g., detecting network intrusions in a financial transactions pipeline, or analyzing sentiment in a telemetry stream) — stop and tell the user:

> **Warning: pattern may not match this schema.**
> The pattern describes `<what the pattern detects>`, but the pipeline contains `<what the schema contains>`.
> No columns appear to map to the key signals. Proceeding may produce views that return 0 rows.
> Suggest a pattern that fits this data, or confirm you want to continue anyway.

Wait for the user to confirm before proceeding.

**If the pattern is a partial match** (some signals can be detected, others cannot) — continue but note which signals are unsupported, and only generate views for the signals that have column coverage.

### 3c — Pattern summary (show to user)

- **Pattern name** — what is being detected
- **How it works** — how the pattern manifests in data (2-3 sentences)
- **Key signals** — the specific observable indicators in the event stream
- **Schema mapping** — for each signal, list every column used and why

### 3d — Detection views (show to user)

- One `CREATE VIEW` per distinct signal
- Name views to reflect what they detect: `flagged_<signal>`, `suspicious_<signal>`, `violated_<policy>`, `anomalous_<metric>`
- Give each view a 2-sentence description explaining what it detects and why
- Build only on existing tables/views — do not invent new columns or tables
- `TUMBLE` for non-overlapping fixed windows (burst/rate detection), `HOP` for overlapping/sliding windows (baseline comparisons), `LAG`/`LEAD` for row-to-row sequences, `JOIN` to relate entities — see `feldera-sql-generator.md` for pitfalls
- Apply all rules from `agentic-guides/shared-analyze/feldera-sql-generator.md`

---

## Step 4: Validate and deploy

Append the new views to the base SQL and write to a working file. By default use a temp directory — do **not** create `demo_runs/` unless the user explicitly asks to save the run:

```bash
RUN_DIR=$(mktemp -d)
# write base SQL + new views to $RUN_DIR/program.sql
```

If the user asks to save the run artifacts, use a timestamped folder instead:

```bash
RUN_DIR=$(dirname <ProgramPath>)/../demo_runs/$(date +%Y%m%d_%H%M%S)
mkdir -p $RUN_DIR
# write base SQL + new views to $RUN_DIR/program.sql
```

Then read and follow `agentic-guides/setup/feldera-redeploy.md`.

---

## Error handling

| Error | How to detect | Action |
|-------|--------------|--------|
| WebFetch fails (403/timeout) | Non-200 response or empty content | Ask user to paste the content directly |
| SQL compilation error | `validate_file()` returns errors | Fix the view, re-validate — see `feldera-redeploy.md` error handling |
| Rust compilation error | `deployment_status` ≠ `Running` after start | Read `deployment_error` from `fda status`, fix the view, re-validate, redeploy |
| Views deploy but return 0 rows | `SELECT COUNT(*) FROM <view>` = 0 | Check threshold semantics — consult `feldera-sql-generator.md` `[FV-ST-DISTANCE-DEGREES]` and `[FV-COMPOSE]` rules; lower thresholds or use standalone column conditions |
