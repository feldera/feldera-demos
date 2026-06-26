# Fraud Detection Brief — Compromised Credit Cards

*A plain-language description of the fraud, the signals that reveal it, and how to combine
them into a detector. This is the analyst's intent; it says **what** to detect and **why**,
not how to write it in any particular query language.*

> **Note (this build):** the multi-engine demo implements **all five signals below** at their stated
> thresholds, applied **identically** by every engine (a card is flagged if it trips ANY signal).
> The engines differ only in how faithfully each can compute a given signal — an exact rolling window,
> ordered `LAG`, or distinct count vs. a noisy bucket proxy or an omitted signal — so the comparison
> isolates *signal fidelity*. Each engine's limits are in `spec/constraints/<engine>.md`.

---

## 1. The problem

Criminals steal credit-card numbers — skimmers on gas pumps and ATMs, breaches, phishing —
and then resell or drain them. A compromised card tends to follow a recognizable arc:

- It gets **drained quickly** once the thief knows it works — a short, intense burst of
  spending, far above the cardholder's normal rhythm.
- The stolen value is often **converted into gift cards**, which are easy to launder, resell,
  and spend anonymously. So a card that suddenly buys a lot of gift cards over a sustained
  period is a strong tell.
- The card is frequently **used far from where the cardholder actually lives** — the data is
  sold and used in another region, so purchases ship to places nowhere near the home address.

No single transaction proves fraud. The pattern lives in **how a card's transactions
accumulate over time**, per card, within a time window. A good detector watches each card's
recent history and raises a card the moment its behavior matches one of these patterns.

---

## 2. The data you have

Two tables. Everything is keyed by the card number (`cc_num`).

- **transactions** — one row per purchase:
  - `category` — merchant type (e.g. `gift card`, `grocery`, `travel`, `games`)
  - `ts` — when the purchase happened
  - `amt` — amount
  - `cc_num` — the card
  - `shipping_lat`, `shipping_long` — where the purchase shipped to
- **customer** — one row per card holder:
  - `cc_num`, `name`
  - `lat`, `long` — the cardholder's **home** location

"Far from home" is the distance between a purchase's shipping location and the cardholder's
home: `|shipping_lat − home_lat| + |shipping_long − home_long|`. A value above **0.5** counts
as far.

---

## 3. The signals

Signals 1–3 each look at **one card's transactions inside a trailing time window** and fire when a
count crosses a threshold. Signals 4 and 5 differ in *kind*: signal 4 compares a card's
**consecutive** transactions in time order, and signal 5 counts **distinct** locations (a
cardinality). The thresholds below are the analyst's recommended sensitivity —
tuned to catch real crews without burying reviewers in false alarms. They are the dial: set
them too low and ordinary customers get flagged; too high and slow, careful fraud slips
through.

| # | Signal | The tell | Window | Fires when |
|---|--------|----------|--------|-----------|
| 1 | **Gift-card burst (30-day)** | sustained gift-card buying | trailing **30 days** | **≥ 23** `gift card` purchases by the card |
| 2 | **Spending velocity (7-day)** | rapid draining once the card works | trailing **7 days** | **≥ 35** purchases of any kind by the card |
| 3 | **Repeated displacement (3-day)** | card used in a different region | trailing **3 days** | **≥ 25** purchases that ship **far from home** (distance > 0.5) |
| 4 | **Impossible travel** | a cloned card used in two places at once | between a card's **consecutive** purchases (time-ordered) | its two **back-to-back** purchases ship **> 1.0 apart** yet occur **< 1 hour** apart — implied travel faster than physically possible |
| 5 | **Geographic fan-out** | one stolen card shipped to many drop addresses at once (reshipping mules / card-testing) | within a single **day** | the card ships to **≥ 10 distinct locations** that day |

Notes on intent:

- Signal 1 is about **gift cards specifically** — the card is drained into gift cards, so it's the
  **rolling** count over the trailing 30 days that matters: a card spreading its gift-card buys
  thinly should still be caught, not only one that bursts within a single calendar month.
- Signal 2 is the opposite — a **short, sharp** spike of activity of any kind.
- Signal 3 needs the **home location**, so it must combine each purchase with the customer's
  home to compute distance before counting.
- Signal 4 is **not a window count at all** — it pairs each transaction with the card's
  **immediately preceding** transaction (a per-card *ordered sequence*) and measures distance ÷
  time between the two. It needs the *previous row in time order*, not an aggregate over a window —
  this separates engines that can maintain ordered, cross-row state from those that only fold rows
  into bucketed aggregates.
- Signal 5 counts **distinct locations**, not a number of events — a **cardinality**. Whether an
  engine can maintain a "how many distinct X" measure incrementally (exactly, only approximately,
  or not at all) varies sharply, so this signal separates engines by their distinct-count
  capability.

---

## 4. Building the detector

Combine the signals into a single decision:

1. **Flag a card if it trips ANY of the five signals.** They are independent tells; matching
   even one is enough to warrant review. (A card may trip several — that only raises
   confidence.)
2. The signals are of three kinds: **1–3 are per-card trailing-window counts** ending "now";
   **signal 4 is a per-card ordered-sequence comparison** (this transaction vs. the previous one);
   **signal 5 is a per-card distinct-location count** (a cardinality). The detector is evaluated
   repeatedly as new transactions arrive; each evaluation asks, for every card, whether any
   threshold is currently crossed.
3. **Only surface cards that are active in the period under review** — a card flagged by its
   history should be reported alongside a representative recent transaction (e.g. its
   highest-value purchase in the window) so a reviewer has concrete context: the card, the
   amount, the merchant category, the shipping location, and which signal fired.
4. **Output one row per flagged card**, carrying that context plus the signal that triggered
   it and a confidence (e.g. `high` for an exact long-window hit).

The output is a **shortlist of suspicious cards** handed to a human (or an LLM agent) for
investigation. The detector's job is precision *and* recall: catch the real fraud (don't miss
slow, spread-out laundering) while keeping the shortlist tight (don't flood reviewers with
ordinary high-volume customers). A detector that can't compute a true trailing-window count —
and approximates it with fixed calendar buckets, or can only see a few recent days — will be
forced to choose between missing fraud and over-flagging, and both are costly.
