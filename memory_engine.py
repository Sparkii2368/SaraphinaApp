#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()

import os
import sys
import json
import time
import re
import logging
import threading
import hashlib
import atexit

from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib import robotparser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import feedparser
from langdetect import detect, LangDetectException

# --- Policy & Provenance (your existing modules) ---
from guardrails import policy_check
from policy_loader import load_policy, get_policy_version
from memory_engine import log_policy_decision

# --- Prometheus metrics ---
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ----------------------------------------------------------------------
# Logging setup
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
# Config loader (env > config.json > defaults)
# ----------------------------------------------------------------------
def load_config():
    cfg = {
        "enable_wikipedia": True,
        "batch_size": 50,
    }
    # file overrides
    if os.path.exists("config.json"):
        try:
            with open("config.json","r",encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as e:
            logging.warning("Could not parse config.json: %s", e)
    # env overrides
    if "ENABLE_WIKIPEDIA" in os.environ:
        cfg["enable_wikipedia"] = os.environ["ENABLE_WIKIPEDIA"].lower() in ("1","true","yes")
    if "BATCH_SIZE" in os.environ:
        try:
            cfg["batch_size"] = int(os.environ["BATCH_SIZE"])
        except Exception:
            logging.warning("Invalid BATCH_SIZE in env, using default=%d", cfg["batch_size"])
    return cfg

cfg = load_config()
BATCH_SIZE = cfg["batch_size"]

# ----------------------------------------------------------------------
# Metrics definitions
# ----------------------------------------------------------------------
METRICS_ADDR = os.getenv("METRICS_ADDR", "0.0.0.0")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9110"))
METRICS_LINGER_SECS = int(os.getenv("METRICS_LINGER_SECS", "120"))

ingest_success    = Counter("ingest_success_total",    "Successful document ingests")
ingest_fail       = Counter("ingest_fail_total",       "Failed ingests", ["reason"])
ingest_bytes      = Counter("ingest_bytes_total",      "Total bytes ingested")
ingest_latency    = Histogram("ingest_latency_seconds","Per-doc ingest latency")
heartbeat_gauge   = Gauge("ingest_heartbeat",          "1=alive, 0=down")
raw_store_success = Counter("raw_store_success_total", "Successful raw doc stores")
raw_store_fail    = Counter("raw_store_fail_total",    "Failed raw doc stores", ["reason"])

# keep heartbeat going until exit
_stop_event = threading.Event()
def heartbeat():
    while not _stop_event.is_set():
        heartbeat_gauge.set(1)
        time.sleep(5)

@atexit.register
def shutdown():
    heartbeat_gauge.set(0)
    _stop_event.set()

# ----------------------------------------------------------------------
# Rate limiter
# ----------------------------------------------------------------------
class DomainRateLimiter:
    def __init__(self, per_domain_rps=1.0, global_rps=20):
        self.per_int = 1.0 / max(per_domain_rps,0.0001)
        self.glob_int = 1.0 / max(global_rps,1)
        self.last_dom = {}
        self.last_glob = 0.0

    def wait(self, domain):
        now = time.time()
        ld = self.last_dom.get(domain,0)
        d1 = max(0, ld + self.per_int - now)
        dg = max(0, self.last_glob + self.glob_int - now)
        delay = max(d1, dg)
        if delay>0: time.sleep(delay)
        t = time.time()
        self.last_dom[domain] = t
        self.last_glob = t

_rl = None

# ----------------------------------------------------------------------
# HTTP + DB session setup
# ----------------------------------------------------------------------
SUPABASE_URL     = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY", "").strip()
TABLE            = os.getenv("SUPABASE_TABLE", "memory_docs")
RAW_TABLE        = os.getenv("SUPABASE_RAW_TABLE","memory_raw_docs")
USER_AGENT       = os.getenv("USER_AGENT", "SaraphinaBot/1.0 (+contact:you@example.com)")
COLLECTOR_ID     = os.getenv("COLLECTOR_ID", "ingest_worker")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    logging.error("SUPABASE_URL or API key missing")
    sys.exit(1)

CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT","5"))
READ_TIMEOUT    = float(os.getenv("READ_TIMEOUT","25"))
TIMEOUT         = (CONNECT_TIMEOUT, READ_TIMEOUT)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
retries = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429,500,502,503,504],
    allowed_methods=["GET","POST","HEAD","OPTIONS"],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

def http_get(url):
    return session.get(url, timeout=TIMEOUT)

def db_headers():
    return {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def upsert_many(rows):
    if not rows: 
        return
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=id"
    mark(f"Upserting {len(rows)} rows")
    try:
        r = session.post(
            url,
            headers={**db_headers(),"Prefer":"resolution=merge-duplicates,return=representation"},
            data=json.dumps(rows),
            timeout=TIMEOUT
        )
        if r.status_code>=400:
            logging.error("Upsert failed %s: %s", r.status_code, r.text)
            r.raise_for_status()
        mark("Upsert OK")
    except Exception as e:
        logging.error("Upsert exception: %s", e)
        raise

def store_raw_doc(doc_id, raw_text, **meta):
    payload = {"id": doc_id, "raw_text": raw_text}
    payload.update({k:v for k,v in meta.items() if v is not None})
    try:
        url = f"{SUPABASE_URL}/rest/v1/{RAW_TABLE}?on_conflict=id"
        r = session.post(
            url,
            headers={**db_headers(),"Prefer":"resolution=merge-duplicates"},
            data=json.dumps(payload),
            timeout=TIMEOUT
        )
        if r.status_code>=400:
            raw_store_fail.labels(reason=f"http_{r.status_code}").inc()
            logging.warning("Raw store failed %s", r.status_code)
            return False
        raw_store_success.inc()
        return True
    except Exception as e:
        raw_store_fail.labels(reason="net_or_other").inc()
        logging.warning("Raw store error: %s", e)
        return False

# ----------------------------------------------------------------------
# Text & link utils
# ----------------------------------------------------------------------
def is_valid_url(u):
    try:
        p = urlparse(u)
        return p.scheme in ("http","https") and bool(p.netloc)
    except:
        return False

def host_allowed(u):
    host = urlparse(u).hostname or ""
    return policy_check(host,"read")

def allowed_by_robots(u):
    p = urlparse(u)
    rp_url = f"{p.scheme}://{p.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        if _rl: _rl.wait(p.netloc)
        resp = http_get(rp_url)
        if resp.status_code>=400:
            return True
        rp.parse(resp.text.splitlines())
        return rp.can_fetch(USER_AGENT,u)
    except:
        return True

def chunk_text(txt, max_chars=1200, overlap=200):
    sents = re.split(r'(?<=[.!?])\s+', txt.strip())
    chunks, cur = [], ""
    for s in sents:
        if len(cur)+len(s)+1 <= max_chars:
            cur = (cur+" "+s).strip()
        else:
            if cur: chunks.append(cur)
            cur = (cur[-overlap:]+ " "+s).strip() if overlap and cur else s
    if cur: chunks.append(cur)
    return chunks

def detect_lang_safe(txt):
    try:
        return detect(txt)
    except LangDetectException:
        return "und"

def checksum_for(url, chunk):
    h = hashlib.sha256()
    h.update(url.encode("utf-8"))
    h.update(b"\0")
    h.update(chunk.encode("utf-8"))
    return h.hexdigest()

def content_hash(chunk):
    return hashlib.sha256(chunk.encode("utf-8",errors="ignore")).hexdigest()

# ----------------------------------------------------------------------
# Provenance enforcement
# ----------------------------------------------------------------------
def _prov_policy():
    pol = load_policy()
    req = pol.get("provenance_requirements",{}) or {}
    return {
        "required_fields": list(req.get("required_fields",[])),
        "enforce_on": set(p.lower() for p in req.get("enforce_on",[])),
        "block_if_missing": bool(req.get("block_if_missing",False))
    }

def enforce_provenance(row, decision, collector_id):
    pol = _prov_policy()
    prov = row.setdefault("provenance",{})
    prov.setdefault("source_url",row.get("url"))
    prov.setdefault("ingestion_timestamp",row.get("timestamp"))
    prov.setdefault("content_hash",row.get("content_hash"))
    prov.setdefault("license","unknown")
    prov.setdefault("collector_id",collector_id)

    if not pol["required_fields"]:
        return True, []

    context = "allowlist" if decision.startswith(("ALLOW","OVERRIDE")) else "denylist"
    if pol["enforce_on"] and context not in pol["enforce_on"]:
        return True, []

    missing = [f for f in pol["required_fields"] if not prov.get(f)]
    if missing:
        if pol["block_if_missing"]:
            return False, missing
        logging.warning("Provenance non-blocking missing: %s", missing)
    return True, []

# ----------------------------------------------------------------------
# Page processor
# ----------------------------------------------------------------------
def process_url(u, seen_hashes):
    if not is_valid_url(u):
        ingest_fail.labels(reason="invalid_url").inc()
        return [],0

    ok,dec = host_allowed(u)
    if not ok:
        ingest_fail.labels(reason=f"policy:{dec}").inc()
        return [],0

    if not allowed_by_robots(u):
        ingest_fail.labels(reason="robots_block").inc()
        return [],0

    try:
        title,txt = fetch_html(u)
    except requests.HTTPError as e:
        ingest_fail.labels(reason=f"http_{e.response.status_code if e.response else 'err'}").inc()
        return [],0
    except Exception:
        ingest_fail.labels(reason="fetch_error").inc()
        return [],0

    if not txt or len(txt)< int(os.getenv("MIN_DOC_CHARS","300")):
        ingest_fail.labels(reason="too_short").inc()
        return [],0

    lang = detect_lang_safe(txt)
    ts   = datetime.now(timezone.utc).isoformat()
    domain = urlparse(u).netloc
    bytes_total = len(txt.encode("utf-8"))

    # raw store
    full_ck = checksum_for(u, txt)
    raw_id  = f"{domain}::{full_ck[:12]}::raw"
    store_raw_doc(
        raw_id, txt,
        url=u, title=title, lang=lang, timestamp=ts,
        policy_tag=dec, content_hash=content_hash(txt),
        collector_id=COLLECTOR_ID
    )

    chunks=[]
    for i,chunk in enumerate(chunk_text(txt)):
        chash = content_hash(chunk)
        if chash in seen_hashes:
            continue
        seen_hashes.add(chash)

        ck = checksum_for(u, chunk)
        row = {
            "id": f"{domain}::{ck[:12]}::{i:03d}",
            "label": domain,
            "text": chunk,
            "tags": [lang,"auto"],
            "source":"crawler",
            "timestamp":ts,
            "score":0,
            "url":u,
            "title":title,
            "lang":lang,
            "checksum":ck,
            "content_hash":chash,
            "policy_tag":dec,
            "trust_score":0.5,
            "provenance":{}
        }
        okp,miss = enforce_provenance(row,dec,COLLECTOR_ID)
        log_policy_decision(
            actor="ingest_worker",
            target=u,
            action="ingest",
            decision=dec if okp else f"DENY_MISSING_{'_'.join(miss).upper()}",
            reason="provenance",
            meta={"doc_id":row["id"],"policy_version":get_policy_version(),"missing_fields":miss}
        )
        if not okp:
            ingest_fail.labels(reason="provenance").inc()
            continue
        chunks.append(row)

    return chunks, bytes_total

# ----------------------------------------------------------------------
# RSS/Atom feed helper
# ----------------------------------------------------------------------
def from_feed(feed_url, limit=20):
    mark(f"Fetching feed {feed_url}")
    try:
        r = http_get(feed_url); r.raise_for_status()
    except:
        ingest_fail.labels(reason="feed_fetch").inc()
        return []
    fp = feedparser.parse(r.content)
    return [e.link for e in fp.entries[:limit] if getattr(e,"link",None)]

# ----------------------------------------------------------------------
# HTML fetch (uses same stripping as process_url)
# ----------------------------------------------------------------------
def fetch_html(url):
    netloc = urlparse(url).netloc
    if _rl: _rl.wait(netloc)
    mark(f"GET {url}")
    r = http_get(url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    for tag in soup(["script","style","nav","footer","header","aside","form"]):
        tag.decompose()
    text = " ".join(soup.get_text(separator=" ").split())
    mark(f"Fetched {url} status={r.status_code} chars={len(text)}")
    return title, text

# ----------------------------------------------------------------------
# Main ingestion loop
# ----------------------------------------------------------------------
def main():
    global _rl
    _rl = DomainRateLimiter(PER_DOMAIN_RPS, GLOBAL_RPS_CAP)

    mark("Starting ingest_worker")
    try:
        with open("seeds.json","r",encoding="utf-8") as f:
            seeds = json.load(f)
    except Exception as e:
        logging.error("Could not read seeds.json: %s", e)
        sys.exit(1)

    feeds    = seeds.get("feeds",[])
    domains  = set(seeds.get("allow_domains",[]))
    max_run  = int(seeds.get("max_pages_per_run",20))
    wiki_list= seeds.get("wikipedia_pages",[])

    mark(f"Loaded seeds: {len(feeds)} feeds, allow_domains={len(domains)}, max_pages={max_run}, wiki={len(wiki_list)}")

    # collect feed URLs
    urls = []
    for feed in feeds:
        urls.extend(from_feed(feed))
    before = len(urls)
    urls = list(dict.fromkeys(u for u in urls if u))
    if domains:
        urls = [u for u in urls if urlparse(u).netloc in domains]
    mark(f"Feed URLs: before={before} after_filter={len(urls)}")

    seen, seen_hashes, batch = set(), set(), []
    pages = 0

    # ----- feed/HTML phase -----
    for u in urls:
        if pages >= max_run:
            break
        if u in seen:
            continue
        seen.add(u)

        t0 = time.time()
        rows, bts = process_url(u, seen_hashes)
        ingest_latency.observe(max(time.time()-t0,0))

        if rows:
            batch.extend(rows)
            ingest_success.inc(len(rows))
            ingest_bytes.inc(bts)
            pages += 1

        if len(batch) >= BATCH_SIZE:
            upsert_many(batch)
            batch.clear()

    # ----- Wikipedia phase -----
    if cfg["enable_wikipedia"] and wiki_list:
        mark("Starting Wikipedia ingestion")
        for title in wiki_list:
            page = title.replace(" ","_")
            url  = f"https://en.wikipedia.org/api/rest_v1/page/summary/{page}"
            try:
                r = session.get(url,timeout=TIMEOUT)
                r.raise_for_status()
                data = r.json()
                txt  = data.get("extract","")
                # wrap into same pipeline as HTML pages
                w_rows, w_bts = [], len(txt.encode("utf-8"))
                ck_full = checksum_for(url,txt)
                raw_id  = f"wiki::{ck_full[:12]}::raw"
                store_raw_doc(raw_id,txt,url=url,title=data.get("title"),lang=data.get("lang"),
                              timestamp=datetime.now(timezone.utc).isoformat(),
                              policy_tag="ALLOWED",content_hash=content_hash(txt),
                              collector_id=COLLECTOR_ID)
                for i,ch in enumerate(chunk_text(txt)):
                    chash = content_hash(ch)
                    if chash in seen_hashes: continue
                    seen_hashes.add(chash)
                    ck = checksum_for(url,ch)
                    row = {
                        "id": f"wiki::{ck[:12]}::{i:03d}",
                        "label":"wiki",
                        "text":ch,
                        "tags":[data.get("lang","und"),"wiki"],
                        "source":"wikipedia",
                        "timestamp":datetime.now(timezone.utc).isoformat(),
                        "score":0,"url":url,
                        "title":data.get("title"),"lang":data.get("lang"),
                        "checksum":ck,"content_hash":chash,
                        "policy_tag":"ALLOW","trust_score":0.5,
                        "provenance":{}
                    }
                    ok,miss = enforce_provenance(row,"ALLOW",COLLECTOR_ID)
                    if not ok:
                        ingest_fail.labels(reason="provenance").inc()
                        continue
                    w_rows.append(row)
                batch.extend(w_rows)
                ingest_success.inc(len(w_rows))
                ingest_bytes.inc(w_bts)
            except Exception as e:
                ingest_fail.labels(reason="wiki_fetch").inc()
                logging.warning("Wiki fail %s: %s", title, e)

            if len(batch) >= BATCH_SIZE:
                upsert_many(batch)
                batch.clear()

    # final flush
    if batch:
        upsert_many(batch)

# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        start_http_server(METRICS_PORT, addr=METRICS_ADDR)
        logging.info("Metrics on %s:%d", METRICS_ADDR, METRICS_PORT)
        threading.Thread(target=heartbeat, daemon=True).start()
        main()
        if METRICS_LINGER_SECS>0:
            logging.info("Lingering %ds for metrics", METRICS_LINGER_SECS)
            end = time.time()+METRICS_LINGER_SECS
            while time.time()<end:
                heartbeat_gauge.set(1)
                time.sleep(1)
    except KeyboardInterrupt:
        mark("Interrupted by user")
        sys.exit(130)
    finally:
        _stop_event.set()
        heartbeat_gauge.set(0)
