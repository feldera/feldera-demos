"""
Atom: create, update, start, stop, and delete Feldera pipelines via `fda`.
Also provides SQL compilation via the Feldera Docker compiler.

Used by: all workflow scripts that need to push SQL to Feldera.
"""

import os
import subprocess
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass

FELDERA_HOST    = os.environ.get("FELDERA_HOST", "http://localhost:8080")
FELDERA_API_KEY = os.environ.get("FELDERA_API_KEY", "") or os.environ.get("FDA_API_KEY", "")


def _fda(*args, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["fda", "--host", FELDERA_HOST]
    if FELDERA_API_KEY:
        cmd += ["--auth", FELDERA_API_KEY]
    cmd += list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"fda error ({' '.join(cmd)}):\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result


def exists(pipeline_name: str) -> bool:
    """Return True if the pipeline exists."""
    return _fda("status", pipeline_name, check=False).returncode == 0


def create(pipeline_name: str, sql_file: str) -> None:
    """Create a new pipeline from a SQL file. Exits on error."""
    _fda("create", pipeline_name, sql_file)
    print(f"Pipeline '{pipeline_name}' created.")


def update_program(pipeline_name: str, sql_file: str) -> None:
    """Update the SQL program of an existing pipeline."""
    _fda("stop", pipeline_name, check=False)
    _fda("program", "set", pipeline_name, sql_file)
    print(f"Pipeline '{pipeline_name}' program updated.")


def start(pipeline_name: str) -> None:
    """Start a pipeline. Exits on error."""
    _fda("start", pipeline_name)
    print(f"Pipeline '{pipeline_name}' started.")


def stop(pipeline_name: str) -> None:
    """Stop a pipeline. Exits on error."""
    _fda("stop", pipeline_name)
    print(f"Pipeline '{pipeline_name}' stopped.")


def delete(pipeline_name: str) -> None:
    """Delete a pipeline. Exits on error."""
    _fda("delete", pipeline_name)
    print(f"Pipeline '{pipeline_name}' deleted.")


def status(pipeline_name: str) -> str:
    """Return the raw status output for a pipeline."""
    return _fda("status", pipeline_name).stdout


def create_or_update(pipeline_name: str, sql_file: str) -> None:
    """Create the pipeline if it doesn't exist, otherwise update its program."""
    if exists(pipeline_name):
        update_program(pipeline_name, sql_file)
    else:
        create(pipeline_name, sql_file)


def validate(sql: str) -> list[str]:
    """
    Validate Feldera SQL using the Docker compiler (submits as __validate_tmp__).
    Returns a list of error strings, empty if compilation succeeded.
    """
    import json
    import time
    import urllib.request
    import urllib.error

    base_url = FELDERA_HOST
    pipeline = "__validate_tmp__"

    def api(method, path, body=None):
        url = f"{base_url}{path}"
        data = json.dumps(body).encode() if body else None
        headers = {"Content-Type": "application/json"} if data else {}
        if FELDERA_API_KEY:
            headers["Authorization"] = f"Bearer {FELDERA_API_KEY}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError:
            return {}

    # Clean up any leftover pipeline from a previous interrupted run
    try:
        api("DELETE", f"/v0/pipelines/{pipeline}")
    except Exception:
        pass

    try:
        api("PUT", f"/v0/pipelines/{pipeline}", {
            "name": pipeline, "program_code": sql,
            "runtime_config": {}, "program_config": {}
        })
        for _ in range(60):
            d = api("GET", f"/v0/pipelines/{pipeline}")
            s = d.get("program_status", "")
            if s == "Success":
                return []
            if "Error" in s:
                return [str(d.get("program_error", s))]
            time.sleep(3)
        return ["Compilation timed out"]
    finally:
        try:
            api("DELETE", f"/v0/pipelines/{pipeline}")
        except Exception:
            pass


def validate_file(sql_file: str) -> list[str]:
    """Validate a Feldera SQL file using the Docker compiler."""
    return validate(Path(sql_file).read_text())


def query_view(pipeline_name: str, sql: str) -> list[dict]:
    """Execute an ad-hoc SQL query against a running pipeline. Returns a list of row dicts."""
    import json
    result = _fda("query", pipeline_name, sql, "--format", "json", check=False)
    if result.returncode != 0:
        print(f"query error: {result.stderr}", file=sys.stderr)
        return []
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows
