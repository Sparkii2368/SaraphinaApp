#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
import time
import hashlib
import sys
import threading
import logging
import atexit
from urllib.parse import urlparse, urljoin
from urllib import robotparser
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
import feedparser
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException

# memory_engine integration
from memory_engine import log_policy_decision

# Centralized policy gate + loader
from guardrails import policy_check
from policy_loader import load_policy, get_policy_version

# Connector
from connectors.wikipedia import WikipediaConnector

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
_t0 = time.perf_counter()

def mark(msg: str):
    logging.info("%s | t=%.2fs", msg, time.perf_counter() - _t0)

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------
def load_config() -> dict:
    config = {
        "enable_wikipedia": True,
        "batch_size": 50,
    }
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                file_cfg = json.load(f)
            config.update(file_cfg)
        except Exception as e:
            logging.warning("Could not load config.json: %s", e)
    if "ENABLE_WIKIPEDIA" in os.environ:
        config["enable_wikipedia"] = os.environ["ENABLE_WIKIPEDIA"].lower() in ("1","true","yes")
    if "BATCH_SIZE" in os.environ:
        try:
            config["batch_size"] = int(os.environ["BATCH_SIZE"])
        except ValueError:
            logging.warning("Invalid BATCH_SIZE in env, using default.")
    return config

cfg = load_config()

# ----------------------------------------------------------------------
# Policy helpers
# ----------------------------------------------------------------------
def is_valid_url(u: str) -> bool:
    try:
        parts = urlparse(u)
        return parts.scheme in ("http", "https") and bool(parts.netloc)
    except Exception:
        return False

def host_allowed(u: str) -> tuple[bool,str]:
    host = urlparse(u).hostname or ""
    return policy_check(host, "read")

def normalize_links(base_url: str, links: list[str], max_links: int = 100) -> list[str]:
    out, seen = [], set()
    for href in links[:max_links]:
        try:
            abs_u = urljoin(base_url, href)
        except Exception:
            continue
        if not is_valid_url(abs_u):
            continue
        ok, decision = host_allowed(abs_u)
        if not ok:
            ingest_fail.labels(reason=f"policy:{decision}").inc()
            continue
        if abs_u not in seen:
            seen.add(abs_u)
            out.append(abs_u)
    return out

def _provenance_policy():
    pol = load_policy()
    req = pol.get("provenance_requirements", {}) or {}
    return {
        "required_fields": list(req.get("required_fields", [])),
        "enforce_on": list(req.get("enforce_on", [])),
        "block_if_missing": bool(req.get("block_if_missing", False)),
    }

def enforce_provenance(row: dict, decision: str, collector_id: str):
    prov_pol = _provenance_policy()
    required = prov_pol["required_fields"]
    enforce_on = set(p.lower() for p in prov_pol["enforce_on"])
    block_if_missing = prov_pol["block_if_missing"]

    enforce = True
    if enforce_on:
        context = "allowlist" if decision.startswith(("ALLOW","OVERRIDE")) else "denylist"
        enforce = context in enforce_on

    row.setdefault("provenance", {})
    prov = row["provenance"]
    prov.setdefault("source_url", row.get("url"))
    prov.setdefault("ingestion_timestamp", row.get("timestamp"))
    prov.setdefault("content_hash", row.get("content_hash"))
    prov.setdefault("license", "unknown")
    prov.setdefault("collector_id", collector_id)

    if not enforce or not required:
        return True, []

    missing = [k for k in required if not prov.get(k)]
    if missing:
        if block_if_missing:
            return False, missing
        else:
            logging.warning("Provenance missing (non-blocking): %s for id=%s", missing, row.get("id"))
    return True, []

# ---------- rate limiting ----------
class DomainRateLimiter:
    def __init__(self, per_domain_rps: float=1.0, global_rps_cap: int=20):
        self.per_domain_interval = 1.0 / max(per_domain_rps, 0.0001)
        self.global_interval = 1.0 / max(global_rps_cap, 1)
        self._last_domain = {}
        self._last_global = 0.0
    def wait(self, domain: str):
        now = time.time()
        delay_d = max(0.0, self._last_domain.get(domain,0.0) + self.per_domain_interval - now)
        delay_g = max(0.0, self._last_global + self.global_interval - now)
        delay = max(delay_d, delay_g)
        if delay>0: time.sleep(delay)
        now2 = time.time()
        self._last_domain[domain] = now2
        self._last_global = now2

_rl: DomainRateLimiter | None = None

# ---------- metrics ----------
METRICS_PORT = int(os.getenv("METRICS_PORT", "9110"))
METRICS_ADDR = os.getenv("METRICS_ADDR", "0.0.0.0")
METRICS_LINGER_SECS = int(os.getenv("METRICS_LINGER_SECS", "120"))

ingest_success = Counter("ingest_success_total","Successful page ingests")
ingest_fail = Counter("ingest_fail_total","Failed ingests",["reason"])
ingest_bytes = Counter("ingest_bytes_total","Total bytes ingested (page text)")
ingest_latency = Histogram("ingest_latency_seconds","Ingest latency per page (seconds)")
heartbeat_gauge = Gauge("ingest_heartbeat","Heartbeat 1=alive, 0=down")
raw_store_success = Counter("raw_store_success_total","Successful raw doc stores")
raw_store_fail = Counter("raw_store_fail_total","Failed raw doc stores",["reason"])

_stop_heartbeat = threading.Event()
def heartbeat():
    while not _stop_heartbeat.is_set():
        heartbeat_gauge.set(1.0)
        time.sleep(5)

@atexit.register
def _shutdown_metrics():
    try:
        heartbeat_gauge.set(0.0)
    except Exception:
        pass
    _stop_heartbeat.set()

# ---------- HTTP session ----------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_API_KEY = os.environ.get("SUPABASE_API_KEY", "").strip()
TABLE = os.environ.get("SUPABASE_TABLE", "memory_docs")
RAW_TABLE = os.environ.get("SUPABASE_RAW_TABLE", "memory_raw_docs")
USER_AGENT = os.getenv("USER_AGENT", "SaraphinaBot/1.0 (+contact: your-email@example.com)")
COLLECTOR_ID = os.getenv("COLLECTOR_ID", "ingest_worker")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_API_KEY in environment.")
    sys.exit(1)

PER_DOMAIN_RPS = float(os.getenv("PER_DOMAIN_RPS", "1"))
GLOBAL_RPS_CAP = int(os.getenv("GLOBAL_RPS_CAP", "20"))
DISCOVERY_MAX_LINKS = int(os.getenv("DISCOVERY_MAX_LINKS", "100"))
MIN_CHARS = int(os.getenv("MIN_DOC_CHARS", "300"))
BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", cfg.get("batch_size", 50)))

CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "5"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "25"))
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "HEAD", "OPTIONS"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ---------- all helpers & processing ----------
# ... [rest of your functions like fetch_html, store_raw_doc, process_url, chunk_text, detect_lang_safe remain the same]

# ---------- main ----------
def main():
    global _rl
    _rl = DomainRateLimiter(PER_DOMAIN_RPS, GLOBAL_RPS_CAP)

    mark("Booting ingest worker")
    try:
        with open("seeds.json", "r", encoding="utf-8") as f:
            seeds_cfg = json.load(f)
    except Exception as e:
        logging.error("Failed to read seeds.json: %s", e)
        sys.exit(1)

    feeds = seeds_cfg.get("feeds", [])
    allow_domains = set(seeds_cfg.get("allow_domains", []))
    max_pages = int(seeds_cfg.get("max_pages_per_run", 20))
    mark(f"Config loaded | feeds={len(feeds)} allow_domains={len(allow_domains)} max_pages={max_pages}")

    urls = []
    for furl in feeds:
        try:
            urls.extend(from_feed(furl))
        except Exception as e:
            logging.warning("Feed error %s: %s", furl, e)

    before = len(urls)
    urls = [u for u in urls if u]
    urls = list(dict.fromkeys(urls))
    if allow_domains:
        urls = [u for u in urls if (urlparse(u).netloc in allow_domains)]
    urls = [u for u in urls if is_valid_url(u) and host_allowed(u)[0]]

    mark(f"Collected URLs | before={before} after_filter={len(urls)}")

    seen, batch = set(), []
    seen_chunk_hashes: set[str] = set()
    pages_processed = 0

    # FEED/HTML ingestion
    for idx, u in enumerate(urls, start=1):
        if pages_processed >= max_pages:
            break
        if u in seen:
            continue
        seen.add(u)

        start_t = time.time()
        rows, bytes_total = process_url(u, seen_chunk_hashes)

        if rows:
            batch.extend(rows)
            ingest_success.inc(len(rows))
            ingest_bytes.inc(bytes_total)
            pages_processed += 1

        ingest_latency.observe(max(time.time() - start_t, 0.0))

        if len(batch) >= BATCH_SIZE:
            try:
                upsert_many(batch)
            finally:
                batch = []

    # Wikipedia ingestion
    if cfg.get("enable_wikipedia", True):
        wiki_connector = WikipediaConnector()
        for task in wiki_connector.fetch_tasks():
            raw = wiki_connector.fetch(task)
            for item in wiki_connector.parse(raw):
                batch.append(item)
                ingest_success.inc()
                ingest_bytes.inc(len(item.get("content", "").encode("utf-8", errors="ignore")))
            if len(batch) >= BATCH_SIZE:
                try:
                    upsert_many(batch)
                finally:
                    batch = []

    # Final flush
    if batch:
        upsert_many(batch)

if __name__ == "__main__":
    try:
        # start metrics server
        start_http_server(METRICS_PORT, addr=METRICS_ADDR)
        logging.info("Metrics server on %s:%d", METRICS_ADDR, METRICS_PORT)

        threading.Thread(target=heartbeat, daemon=True).start()
        main()

        if METRICS_LINGER_SECS > 0:
            logging.info(
                "Lingering %ds to keep /metrics alive for health check",
                METRICS_LINGER_SECS
            )
            end = time.time() + METRICS_LINGER_SECS
            while time.time() < end:
                heartbeat_gauge.set(1.0)
                time.sleep(1)
    except KeyboardInterrupt:
        mark("Interrupted by user")
        sys.exit(130)
    finally:
        _stop_heartbeat.set()
        heartbeat_gauge.set(0.0)