---
description: "Generic Feldera setup: check fda CLI, resolve host/key, start Docker or connect to remote. Called by other Feldera skills before any demo-specific steps."
---

# Feldera Setup

Run these steps before any demo-specific work. Show the user each step as it runs.

---

## Step 1: Check fda CLI

Tell the user: "🖥️ **[1/3] Checking fda CLI...**"

Run `fda --version 2>/dev/null || true` and check the output:
- If not found: tell the user "  ❌ fda CLI not installed. Install v0.282.0: `cargo install fda --version 0.282.0`" and **stop**.
- If a version is returned: tell the user "  ✅ fda CLI found — `<version>`"
  - If the version is **not** `0.282.0`: tell the user "  ⚠️ Expected fda v0.282.0 but found `<version>`. Unexpected behaviour may occur." (do not stop — continue with the installed version)

---

## Step 2: Resolve Feldera host and API key

Run:

```bash
grep -E '^FELDERA_HOST=' .env 2>/dev/null | tail -1 | cut -d'=' -f2-
grep -E '^FELDERA_API_KEY=' .env 2>/dev/null | tail -1 | cut -d'=' -f2-
```

Read both values:
- `FELDERA_HOST` — the host found in `.env`, or empty if absent
- `FELDERA_API_KEY` — the key found in `.env`, or empty if absent

**If `FELDERA_API_KEY` is already set**, skip the question — use it with `FELDERA_HOST` as read from `.env` and go to Step 3 Option A.

**Otherwise** (no API key), always ask the user — even if `FELDERA_HOST` is already in `.env`:

> How would you like to run Feldera?
>
> **1 — Docker** (runs locally, no account needed)
> **2 — Remote Feldera instance** (e.g. try.feldera.com, your company's deployment — you'll need a host URL and API key)

Wait for their answer, then:

- If **1 (Docker)**: set `FELDERA_HOST=http://localhost:8080`, `FELDERA_API_KEY` stays empty.
- If **2 (Remote)**: ask "Enter your Feldera host URL (e.g. `https://try.feldera.com`):" and wait. Then ask "Paste your API key:" and wait.
  Set `FELDERA_HOST=<URL they entered>` and `FELDERA_API_KEY=<key they pasted>`.
  Then write both lines to `.env` so they persist:
  ```bash
  echo "FELDERA_HOST=<url>" >> .env
  echo "FELDERA_API_KEY=<key>" >> .env
  ```

Store both as variables in your context. For all subsequent steps:
- Replace `<FELDERA_HOST>` with the resolved host value in every command
- Every `fda` call must include `--host <FELDERA_HOST>`
- Only add `--auth <FELDERA_API_KEY>` when the key is **non-empty** — never pass `--auth ""`
- Every `curl` call must use `<FELDERA_HOST>` as the base URL, and add `-H "Authorization: Bearer <FELDERA_API_KEY>"` when the key is non-empty

---

## Step 3: Start Feldera

Tell the user: "🖥️ **[2/3] Starting Feldera...**"

### Option A — Remote host (`FELDERA_API_KEY` is set)

If `FELDERA_API_KEY` is **non-empty**, skip Docker entirely:

```bash
fda --host <FELDERA_HOST> --auth <FELDERA_API_KEY> pipelines 2>/dev/null && echo "OK" || echo "UNREACHABLE"
```

- If `OK`: tell the user "  ✅ Connected to Feldera at `<FELDERA_HOST>`"
- If `UNREACHABLE`: tell the user "  ❌ Cannot reach `<FELDERA_HOST>`. Check your `FELDERA_HOST` and `FELDERA_API_KEY` in `.env`." and **stop**.

### Option B — Docker (`FELDERA_API_KEY` is empty)

If `FELDERA_API_KEY` is **empty**, run Feldera locally with Docker.

Check if Feldera is already reachable:

```bash
fda --host <FELDERA_HOST> pipelines 2>/dev/null && echo "OK" || echo "UNREACHABLE"
```

**If output is `OK`:** tell the user: "  ✅ Feldera already running at `<FELDERA_HOST>`"

**If `UNREACHABLE`:**
- Check Docker is installed: `docker info 2>/dev/null && echo "OK" || echo "MISSING"`
  - If `MISSING`: tell the user "  ⚠️ Docker is not installed or not running. Install it from https://docs.docker.com/get-docker/ then re-run." and **stop**.
- Check if a Feldera container is already running: `docker ps --filter "ancestor=images.feldera.com/feldera/pipeline-manager:0.282.0" --format "{{.ID}}" 2>/dev/null`
  - If a container ID is returned: tell the user "  ✅ Feldera container is running but not yet ready — waiting..." and skip the pull/run steps below
  - If no container is running:
    - Check if port 8080 is already in use: `lsof -ti:8080 2>/dev/null || ss -tlnp 2>/dev/null | grep ':8080'`
      - If something is using port 8080: tell the user "  ⚠️ Port 8080 is already in use by another process. Stop it first, or set a different `FELDERA_HOST` in `.env`." and **stop**.
    - Tell the user: "  ⏳ Pulling Feldera Docker image..."
    - Run `docker pull images.feldera.com/feldera/pipeline-manager:0.282.0`
    - Tell the user: "  ⏳ Starting Feldera container..."
    - Run `docker run -p 8080:8080 --rm -d images.feldera.com/feldera/pipeline-manager:0.282.0`
- Poll until fda can connect (up to 30 seconds): `for i in $(seq 1 30); do fda --host <FELDERA_HOST> pipelines 2>/dev/null && break || sleep 1; done`
- Tell the user: "  ✅ Feldera is running at `<FELDERA_HOST>`"

---

## Step 4: Verify compiler

Tell the user: "🔧 **[3/3] Verifying SQL compiler...**"

- Run `curl -s <FELDERA_HOST>/v0/config` (add `-H "Authorization: Bearer <FELDERA_API_KEY>"` if key is non-empty) to read the platform version (`version` field)
- Tell the user: "  ✅ Compiler ready — Feldera v`<version>`"
