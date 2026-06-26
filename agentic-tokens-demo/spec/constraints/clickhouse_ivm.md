# Engine constraints — ClickHouse incremental materialized view (IVM)

**What it is.** A ClickHouse `MATERIALIZED VIEW` is a genuine incremental mechanism — per the
ClickHouse docs, *"a materialized view is just a trigger that runs a query on blocks of data as
they're inserted into a table."* The view's `SELECT` runs on **each newly-inserted block** of the
source table and writes its result into a **target table**; with an `AggregatingMergeTree` (or
`SummingMergeTree`) target, per-key partial aggregates accumulate and **merge asynchronously**. So
the view is kept up to date **incrementally, in proportion to the new data** — not by recomputing
over history.

**Incremental?** Yes — natively, but only for the shapes below.

**Can maintain incrementally.**
- **Aggregates into FIXED buckets.** A `GROUP BY` over a fixed key (e.g. `toStartOfDay(ts)`, a fixed
  N-day bucket) backed by `SummingMergeTree`, or by `AggregatingMergeTree` with the `-State` /
  `-Merge` combinators (`sumState`, `countState`, `avgState`, `quantileState`, …); the target is
  queried with the matching `-Merge`.
- **Distinct / cardinality.** `uniqState` + `uniqMerge` maintain a distinct count across parts and
  buckets. `uniq` is **exact for small cardinalities** (it keeps an explicit set up to a threshold)
  and only becomes a **HyperLogLog estimate at large scale** — so a "how many distinct X" measure is
  maintainable, exact when the counts are small. (`uniqExactState` is always exact but far heavier.)
- **A lookup against a STATIC dimension.** A `JOIN` is allowed, but **only inserts to the leftmost
  (source) table trigger the view**; right-side tables are read at insert time and behave as a
  static dimension — later changes to them are **not** reflected.

**Cannot maintain incrementally (per the docs).**
- **Rolling / sliding time windows.** Only fixed `GROUP BY` buckets are supported — there is no
  "trailing N days ending now". Activity that straddles a bucket boundary is split between buckets.
- **Window functions** — `OVER`, `LAG` / `LEAD`, and any ordered comparison of a row to another row
  for the same key. The view sees only the current block, with no ordering across the stream.
- **Joins to a changing / streaming table.** Only the static-dimension case works; because the
  right side doesn't trigger, the view reflects the dimension's insert-time state and never updates
  when it changes.

**Escape hatch (NOT incremental).** A **refreshable** materialized view (ClickHouse 23.12+) re-runs
the full `SELECT` on a **schedule** and atomically swaps in the result. It can express shapes the
incremental MV can't (rolling logic, joins between changing tables) — but it is a **periodic full
recompute**: its cost grows with the whole dataset and its result is only as fresh as the last
refresh, not "as of now".

**Consequence (general).**
- Logic needing a **trailing window** must be **approximated with fixed buckets** — coarse, with
  boundary effects, and never equal to a true rolling window.
- Logic needing a **distinct / cardinality** measure *can* be maintained, but only as an
  **approximate** (HyperLogLog) estimate.
- Logic needing **ordered cross-row state, window functions, or a join to a changing table** is
  **not expressible** in an incremental MV — it must be dropped, or moved to a refreshable
  (full-recompute) view, which forfeits incrementality.

**References** (these constraints are taken from the ClickHouse docs, not invented):
- Incremental materialized view — <https://clickhouse.com/docs/materialized-view/incremental-materialized-view>
  - *"a ClickHouse materialized view is just a trigger that runs a query on blocks of data as they're inserted into a table."*
  - *"the materialized view only triggers on inserts to the source table (the left-most table in the query). Right-side tables in JOINs don't trigger updates"* — the static-dimension join.
  - `AggregatingMergeTree` + `-State`/`-Merge` combinators accumulate partial aggregates; the examples use only fixed `GROUP BY` buckets (no rolling/sliding windows, no window functions).
- `uniq` aggregate (distinct/cardinality via adaptive sampling — exact for small counts, approximate at scale) — <https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/uniq>
- Refreshable materialized view (the non-incremental, scheduled full-recompute escape hatch) — <https://clickhouse.com/docs/sql-reference/statements/create/view#refreshable-materialized-view>
