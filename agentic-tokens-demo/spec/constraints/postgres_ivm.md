# Engine constraints — PostgreSQL (fixed-bucket rollup under a low-latency budget)

**The requirement.** This is a *real-time* fraud detector: every incoming batch must be scored within
a tight **latency budget** — you can't make a card wait seconds for a verdict. That budget, not a
missing SQL feature, is what constrains the Postgres detector.

**Why Postgres is constrained.** Plain PostgreSQL has **no incremental view maintenance**: a derived
view is brought up to date only by a **full recompute** (`REFRESH MATERIALIZED VIEW` "completely
replaces the contents of a materialized view"), and an ad-hoc query recomputes from scratch too.
PostgreSQL *can* express every signal exactly — a true trailing window (`… OVER … RANGE`), an exact
`COUNT(DISTINCT …)`, an ordered `LAG` — but each of those, recomputed over the **whole, ever-growing**
transaction history on every batch, is **O(N)** and gets steadily slower. None of them hold a fixed
low-latency budget as the stream grows.

**So, to stay inside the budget, the detector is driven to the cheapest shapes:**
- **A single fixed calendar bucket** per windowed signal — a plain `GROUP BY … HAVING count(*)`, the
  cheapest, index-friendly aggregate. Not a true trailing window (and not even two adjacent buckets):
  a single bucket **splits** boundary-straddling activity, so the per-bucket threshold is **lowered**
  to still catch it — a coarse approximation that can flag legitimate cards.
- **Drop the expensive signals.** The per-day **distinct-location** fan-out (a distinct/cardinality
  count) and the **impossible-travel** ordered `LAG` are the costliest to recompute, so under the
  latency budget they are **omitted** → **missed fraud**.
- Equi-joins to the static customer dimension are cheap and stay (the displacement signal).

**Consequence.** Latency, not expressiveness, is the binding constraint — Postgres trades **accuracy
for speed**: a single coarse bucket plus two dropped signals means it can both over-flag and miss
fraud. And because even the cheap rollup is a full recompute, its per-batch latency **still grows with
the data**, so it doesn't actually hold the budget as the stream scales.

**References**
- PostgreSQL materialized views are refreshed by **full recompute**, not maintained incrementally —
  <https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html>
  - *"`REFRESH MATERIALIZED VIEW` completely replaces the contents of a materialized view."*
