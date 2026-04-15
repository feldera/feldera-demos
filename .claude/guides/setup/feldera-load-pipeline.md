---
description: "Load a named pipeline from a SQL file: create it if new, or reset it to the base SQL if it already exists (clears stale views from previous demo runs)."
---

# Feldera Load Pipeline

Assumes `<FELDERA_HOST>`, `<pipeline_name>`, and `<ProgramPath>` are already in context.

```bash
fda --host <FELDERA_HOST> status <pipeline_name> 2>/dev/null && echo "EXISTS" || echo "NEW"
```

- If **NEW**:
  - Tell the user: "  ⏳ Submitting SQL program..."
  - Run `fda --host <FELDERA_HOST> create <pipeline_name> <ProgramPath>`
  - Tell the user: "  ⏳ Compiling & starting..."
  - Run `fda --host <FELDERA_HOST> start <pipeline_name>`
  - Tell the user: "  ✅ Pipeline `<pipeline_name>` is running"

- If **EXISTS** — always reset to the base SQL (previous runs may have left stale signal views):
  - Tell the user: "  ⏳ Resetting pipeline to base SQL..."
  - Run `fda --host <FELDERA_HOST> stop <pipeline_name>`
  - Run `fda --host <FELDERA_HOST> program set <pipeline_name> <ProgramPath>`
  - Tell the user: "  ⏳ Starting..."
  - Run `fda --host <FELDERA_HOST> start <pipeline_name>`
  - Tell the user: "  ✅ Pipeline `<pipeline_name>` is running"
