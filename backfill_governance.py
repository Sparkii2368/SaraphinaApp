# backfill_governance.py
from dotenv import load_dotenv
load_dotenv()

import os, json, hashlib, logging, sys, math
from urllib.parse import urlparse
import requests, yaml
from datetime import datetime, timezone

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_API_KEY = os.environ.get("SUPABASE_API_KEY", "").strip()
TABLE = os.environ.get("SUPABASE_TABLE", "memory_docs")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_API_KEY"); sys.exit(1)

# ---- load policy + seeds
POLICY_PATH = os.getenv("POLICY_PATH", "policy.yaml")
try:
    with open(POLICY_PATH, "r", encoding="utf-8") as f:
        pol = yaml.safe_load(f) or {}
except Exception:
    pol = {}
allowlist = pol.get("allowlist", [])
denylist = pol.get("denylist", [])
try:
    with open("seeds.json", "r", encoding="utf-8") as f:
        seeds = json.load(f)
except Exception:
    seeds = {}
allow_domains = set(seeds.get("allow_domains", []))

def host_allowed(u: str):
    from fnmatch import fnmatch
    host = urlparse(u).hostname or ""
    for pat in denylist:
        if fnmatch(host, pat): return False, "denylist"
    for pat in allowlist:
        if fnmatch(host, pat): return True, "allowlist"
    # seeds allow_domains is a softer allow if policy.yaml didn’t include it
    if host in allow_domains: return True, "allowlist"
    return False, "not_allowed"

def content_hash(text: str):
    return hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest()

session = requests.Session()
session.headers.update({
    "apikey": SUPABASE_API_KEY,
    "Authorization": f"Bearer {SUPABASE_API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
})

def fetch_batch(limit=500, offset=0):
    # policy_tag=is.null OR provenance=is.null OR content_hash=is.null OR trust_score=is.null
    # PostgREST: or=(cond1,cond2,...) syntax
    or_filter = "or=(policy_tag.is.null,provenance.is.null,content_hash.is.null,trust_score.is.null)"
    sel = "id,url,label,text,timestamp,policy_tag,provenance,content_hash,trust_score"
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?select={sel}&{or_filter}&order=timestamp.desc&limit={limit}&offset={offset}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def upsert(rows):
    if not rows: return
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=id"
    r = session.post(url, json=rows, headers={"Prefer": "resolution=merge-duplicates"}, timeout=60)
    if r.status_code >= 400:
        try:
            logging.error("Upsert failed %s: %s", r.status_code, r.json())
        except Exception:
            logging.error("Upsert failed %s: %s", r.status_code, r.text)
        r.raise_for_status()

def backfill():
    logging.info("Starting governance backfill…")
    offset = 0
    batch_size = int(os.getenv("BACKFILL_BATCH", "500"))
    total_updates = 0
    while True:
        rows = fetch_batch(limit=batch_size, offset=offset)
        if not rows:
            break
        updates = []
        for r in rows:
            url = r.get("url") or ""
            ok, tag = host_allowed(url) if url else (False, "not_allowed")
            ts = r.get("timestamp") or datetime.now(timezone.utc).isoformat()
            prov = r.get("provenance") or {
                "source_url": url,
                "fetched_at": ts,
                "method": "unknown",
                "robots_ok": None
            }
            # Fill missing fields only; don’t overwrite existing
            patch = {"id": r["id"]}
            if r.get("policy_tag") in (None, ""):
                patch["policy_tag"] = tag
            if r.get("trust_score") is None:
                patch["trust_score"] = 0.5
            if r.get("content_hash") in (None, ""):
                patch["content_hash"] = content_hash(r.get("text") or "")
            if r.get("provenance") in (None, {}):
                patch["provenance"] = prov
            if len(patch) > 1:
                updates.append(patch)

        if updates:
            upsert(updates)
            total_updates += len(updates)
            logging.info("Backfilled %d rows (offset=%d)", len(updates), offset)
        # If we got fewer than batch_size, we’re likely done; still advance offset to avoid loops
        offset += batch_size
        if len(rows) < batch_size:
            break
    logging.info("Backfill complete. Rows updated: %d", total_updates)

if __name__ == "__main__":
    try:
        backfill()
    except KeyboardInterrupt:
        sys.exit(130)
