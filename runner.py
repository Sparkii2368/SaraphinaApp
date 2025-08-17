from dotenv import load_dotenv
load_dotenv()

import os, time, subprocess, sys, requests
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from supabase import create_client

# --- Policy imports (Step 2 modules) ---
from guardrails import policy_check
from policy_loader import load_policy, get_policy_version

# --- CONFIG ---
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
WORKER_SCRIPT = os.getenv("WORKER_SCRIPT", "ingest_worker.py")
HEALTH_TABLE = os.getenv("HEALTH_TABLE", "memory_docs")
TIME_COLUMN = os.getenv("HEALTH_TIME_COLUMN", "timestamp")

RUN_INTERVAL_SECS   = int(os.getenv("RUN_INTERVAL_SECS", 3600))  # 1h between runs
HEALTH_LOOKBACK_SECS= int(os.getenv("HEALTH_LOOKBACK_SECS", 900)) # 15m window by default
MIN_NEW_ROWS        = int(os.getenv("MIN_NEW_ROWS", 1))
MAX_FAIL_RATIO      = float(os.getenv("MAX_FAIL_RATIO", 0.3))
FAILURE_EXIT_AFTER  = int(os.getenv("FAILURE_EXIT_AFTER", 2))
METRICS_URL         = os.getenv("METRICS_URL", "http://localhost:9110/metrics")
REQUIRE_HEARTBEAT   = os.getenv("REQUIRE_HEARTBEAT", "true").lower() in ("1","true","yes")
TIMEOUT             = float(os.getenv("RUNNER_HTTP_TIMEOUT", "20"))

# --- UTILITIES ---
def utc_stamp():
    return datetime.now(timezone.utc).isoformat()

def is_valid_url(u: str) -> bool:
    parts = urlparse(u)
    return parts.scheme in ("http", "https") and bool(parts.netloc)

# --- METRICS ---
def fetch_metrics() -> dict:
    metrics = {}
    try:
        r = requests.get(METRICS_URL, timeout=TIMEOUT)
        r.raise_for_status()
        for line in r.text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) == 2:
                name, val = parts
                try:
                    if name in ("ingest_success_total","ingest_fail_total","ingest_heartbeat"):
                        metrics[name] = float(val)
                except ValueError:
                    continue
    except requests.RequestException as e:
        print(f"[{utc_stamp()}] ⚠️ Metrics fetch failed: {e}")
    return metrics

# --- POLICY SANITY ---
def policy_sanity_check():
    """Verify policy.yaml is present, parseable, and has required schema keys."""
    try:
        pol = load_policy(force_reload=True)
        required_top_keys = {"allowlist", "denylist", "overrides", "logging"}
        missing = required_top_keys - pol.keys()
        if missing:
            print(f"[{utc_stamp()}] ⚠️ Policy missing keys: {missing}")
            return False
        print(f"[{utc_stamp()}] 📜 Policy OK — version {get_policy_version()}")
        return True
    except Exception as e:
        print(f"[{utc_stamp()}] ❌ Policy load failed: {e}")
        return False

# --- TASKS ---
def run_ingest():
    start_time = datetime.now(timezone.utc)
    print(f"[{utc_stamp()}] 🚀 Starting ingestion run under policy v{get_policy_version()}")

    # Optional pre-flight: verify seeds are policy compliant
    seeds_env = os.getenv("SEED_URLS", "")
    if seeds_env:
        for u in [s.strip() for s in seeds_env.split(",") if s.strip()]:
            if not is_valid_url(u):
                print(f"[{utc_stamp()}] ⚠️ Skipping invalid seed URL: {u}")
                continue
            ok, decision = policy_check(urlparse(u).hostname or "", "read")
            if not ok:
                print(f"[{utc_stamp()}] ❌ Seed URL blocked by policy: {u} | decision={decision}")
                return False

    try:
        result = subprocess.run(
            ["python", WORKER_SCRIPT],
            capture_output=True,
            text=True,
            check=False
        )
    except Exception as e:
        print(f"[{utc_stamp()}] ❌ Failed to launch worker: {e}")
        return False

    if result.stdout:
        print(result.stdout.strip())

    if result.returncode != 0:
        print(f"[{utc_stamp()}] ❌ Worker exited with {result.returncode}")
        return False
    else:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        print(f"[{utc_stamp()}] ✅ Run complete in {duration:.2f} seconds.")
        return True

def health_check():
    """Check rows in last lookback window + metrics sanity + policy sanity."""
    print(f"[{utc_stamp()}] 🔍 Running health check…")
    ok = True

    # Check policy validity first
    if not policy_sanity_check():
        ok = False

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=HEALTH_LOOKBACK_SECS)
        rows = supabase.table(HEALTH_TABLE) \
                       .select(f"id,{TIME_COLUMN}") \
                       .gte(TIME_COLUMN, cutoff.isoformat()) \
                       .execute()
        new_rows = len(rows.data)
        freshness_ok = new_rows >= MIN_NEW_ROWS
        print(f"[{utc_stamp()}] 📈 Rows in window: {new_rows} (>= {MIN_NEW_ROWS}? {'Y' if freshness_ok else 'N'})")
        if not freshness_ok:
            ok = False

        m = fetch_metrics()
        if m:
            succ = m.get("ingest_success_total", 0.0)
            fail = m.get("ingest_fail_total", 0.0)
            hb   = m.get("ingest_heartbeat", 0.0)

            fail_ratio = (fail / (succ + fail)) if (succ + fail) > 0 else 0
            if REQUIRE_HEARTBEAT and hb < 1.0:
                print(f"[{utc_stamp()}] ⚠️ Heartbeat missing/low")
                ok = False
            if fail_ratio > MAX_FAIL_RATIO:
                print(f"[{utc_stamp()}] ⚠️ Fail ratio {fail_ratio:.2f} > {MAX_FAIL_RATIO:.2f}")
                ok = False
    except Exception as e:
        print(f"[{utc_stamp()}] ❌ Health check error: {e}")
        ok = False
    return ok

# --- MAIN LOOP ---
if __name__ == "__main__":
    consecutive_failures = 0
    while True:
        run_ok = run_ingest()
        health_ok = health_check()

        if run_ok and health_ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            print(f"[{utc_stamp()}] ⚠️ Consecutive failures: {consecutive_failures}")
            if consecutive_failures >= FAILURE_EXIT_AFTER:
                print(f"[{utc_stamp()}] ❌ Exiting after {FAILURE_EXIT_AFTER} failures for self-heal.")
                sys.exit(2)

        print(f"[{utc_stamp()}] 💤 Sleeping for {RUN_INTERVAL_SECS/60:.0f} minutes…\n")
        time.sleep(RUN_INTERVAL_SECS)
