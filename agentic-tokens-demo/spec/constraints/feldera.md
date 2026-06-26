# Engine constraints — Feldera

**What it is.** A streaming SQL engine with native *incremental view maintenance* (IVM): every
view is kept continuously up to date as new rows arrive, with work proportional to the change —
not to the size of the accumulated history.

**Incremental?** Yes — natively, across essentially the full SQL surface below.

**Can maintain incrementally.**
- **Aggregations** — `COUNT` / `SUM` / `AVG` / … grouped by any key.
- **Joins** — including joins to other **changing / streaming** tables (not only a static
  dimension), kept consistent as either side updates.
- **True rolling / sliding windows** — trailing-window aggregates per key that end "now"
  (`… OVER (PARTITION BY key ORDER BY ts RANGE BETWEEN INTERVAL N … PRECEDING AND CURRENT ROW)`).
- **Ordered, cross-row operations** — `LAG` / `LEAD` and window functions over a per-key ordering,
  so a row can be compared to the **previous / next** row for that key. Feldera keeps the ordered
  per-key state, not just folded aggregates.
- **Composition** — aggregates of joins of windows, etc., all maintained together.

**Constraints.** None that materially limit expressiveness for this kind of workload. Minor
operational notes only:
- Window ranges must be **constant** intervals (express horizons as `30 DAYS`, not `1 MONTH`).
- Declaring how late data may arrive is a memory/correctness tuning knob, not a limit on what can
  be expressed.

**Consequence.** Whatever the logic calls for — windowed, ordered, or joined — it translates
**faithfully and exactly** and stays fresh incrementally at flat per-update cost.

**References** (these capabilities are taken from the Feldera docs, not invented):
- What is Feldera? — <https://docs.feldera.com/>
  - *"When the pipeline receives changes, Feldera incrementally updates all the views by only looking at the changes and it completely avoids recomputing over older data."*
  - *"Our engine is the only one in existence that can evaluate full SQL syntax and semantics completely incrementally. This includes joins and aggregates, group by, correlated subqueries, window functions, complex data types, time series operators, UDFs, and recursive queries."* — i.e. the rolling windows, ordered `LAG`/`LEAD`, and joins this card relies on are all maintained incrementally.
- Time-series extensions / `LATENESS` — <https://docs.feldera.com/sql/streaming/>
  - *"LATENESS does not affect the output of the program"* and *"does NOT delay computation"*; Feldera *"takes advantage of LATENESS annotations to garbage collect old records … allowing evaluating complex queries over unbounded streams using bounded storage."* — so `LATENESS` is a memory/correctness knob, not a limit on expressiveness.
- SQL grammar / function index (`OVER`, `RANGE` windows, `LAG`/`LEAD`) — <https://docs.feldera.com/sql/grammar/>
