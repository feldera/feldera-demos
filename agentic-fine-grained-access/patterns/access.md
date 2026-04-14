# Access Anomaly Patterns

## Pattern 1: Rapid Enumeration Attack

A user accesses an unusually large number of distinct parent folders within a single 1-hour window.
Normal users browse a small set of folders — an attacker systematically enumerates the file tree,
touching hundreds of different subtrees in a short time.

**Signals:**
- High number of distinct `parent_id` values per user in a 1-hour TUMBLE window
- Normal users: ≤ 4 distinct folders/hour
- Threshold: ≥ 20 distinct folders/hour (attacker reaches ~500)

**Key metric:** `folder_count` — distinct parent folders accessed in the window.

---

## Pattern 2: Hot Folder Anomaly

An unusually high number of distinct users access the same parent folder within a 6-hour window.
Normally a folder is accessed by a small team. A sudden spike in distinct accessors indicates
credential sharing, a leaked share link, or coordinated exfiltration.

**Signals:**
- High number of distinct `user_id` values per `parent_id` in a 6-hour TUMBLE window
- Normal folders: ~20–40 distinct users/6h (1M events × 1000 parents × 46 windows ≈ 22 events/cell)
- Anomalous: ≥ 40 distinct users/6h

**Key metric:** `user_count` — distinct users accessing the folder in the window.
