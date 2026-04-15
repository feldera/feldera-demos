---
description: "Validate $RUN_DIR/program.sql against the Feldera compiler, then stop → set program → start the pipeline and poll until Running."
---

# Feldera Redeploy

Assumes `<FELDERA_HOST>`, `<pipeline_name>`, and `$RUN_DIR/program.sql` are already in context.

## Validate

```bash
PYTHONPATH=$(git rev-parse --show-toplevel)/utils python3 -c "
from pipeline_manager import validate_file
errors = validate_file('$RUN_DIR/program.sql')
if errors:
    print('Errors:'); [print(' ', e) for e in errors]
else:
    print('OK')
"
```

- If `OK`: show the new views with "✓ compiled OK" and proceed to deploy.
- If errors: fix the offending view(s), re-validate — do not deploy until clean. Retry up to 3 times before asking the user.

## Deploy

```bash
fda --host <FELDERA_HOST> stop <pipeline_name>
fda --host <FELDERA_HOST> program set <pipeline_name> $RUN_DIR/program.sql
fda --host <FELDERA_HOST> start <pipeline_name>

# Poll until deployment_status is Running or an error state (up to 60s)
for i in $(seq 1 60); do
  STATUS=$(fda --host <FELDERA_HOST> status <pipeline_name> 2>/dev/null)
  echo "$STATUS" | grep -q "Running" && break
  echo "$STATUS" | grep -qE "Failed|Error" && break
  sleep 1
done
fda --host <FELDERA_HOST> status <pipeline_name>
```

Check `deployment_status` in the final output:
- If `Running` — report success with the run folder path and names of the new views now live.
- If not `Running` — read `deployment_error` from the status output. This is a **Rust compilation error** (distinct from SQL errors caught by `validate_file`). Fix the offending view, re-validate, and redeploy. Retry up to 3 times before asking the user.
