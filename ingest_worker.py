# ingest_worker.py
from dotenv import load_dotenv
load_dotenv()

import os, json, re, time, hashlib, sys, threading, logging
from urllib.parse import urlparse, urljoin
from urllib import robotparser
from datetime import datetime, timezone

import requests, feedparser
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Centralized policy gate + loader (from Step 2)
from guardrails import policy_check
from policy_loader import load_policy

# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# ---------- logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
_t0 = time.perf_counter()
def mark(msg: str):
    logging.info("%s | t=%.2fs", msg, time.perf_counter() - _t0)

# ---------- policy helpers ----------
def is_valid_url(u: str) -> bool:
    try:
        parts = urlparse(u)
        return parts.scheme in ("http", "https") and bool(parts.netloc)
    except Exception:
        return False

def host_allowed(u: str) -> tuple[bool, str]:
    host = urlparse(u).hostname or ""
    ok, decision = policy_check(host, "read")
    return ok, decision

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
            logging.debug("Policy blocked URL: %s | decision=%s", abs_u, decision)
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
        "enforce_on": list(req.get("enforce_on", [])),  # e.g., ["allowlist"]
        "block_if_missing": bool(req.get("block_if_missing", False)),
    }

def enforce_provenance(row: dict, decision: str, collector_id: str):
    """
    Populate and validate provenance per policy.
    decision: policy decision string from policy_check (e.g., ALLOW_MATCH, DENY_..., OVERRIDE_...)
    """
    prov_pol = _provenance_policy()
    required = prov_pol["required_fields"]
    enforce_on = set(p.lower() for p in prov_pol["enforce_on"])
    block_if_missing = prov_pol["block_if_missing"]

    # Decide whether to enforce for this row
    enforce = True
    if enforce_on:
        # Treat ALLOW and OVERRIDE as "allowlist" context for enforcement purposes
        context = "allowlist" if decision.startswith("ALLOW") or decision.startswith("OVERRIDE") else "denylist"
        enforce = context in enforce_on

    # Ensure provenance object exists
    row.setdefault("provenance", {})
    prov = row["provenance"]

    # Fill recommended fields so checks pass
    prov.setdefault("source_url", row.get("url"))
    prov.setdefault("ingestion_timestamp", row.get("timestamp"))
    prov.setdefault("content_hash", row.get("content_hash"))
    prov.setdefault("license", "unknown")  # domain-specific detectors can refine later
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
    def __init__(self, per_domain_rps: float = 1.0, global_rps_cap: int = 20):
        self.per_domain_interval = 1.0 / max(per_domain_rps, 0.0001)
        self.global_interval = 1.0 / max(global_rps_cap, 1)
        self._last_domain = {}
        self._last_global = 0.0

    def wait(self, domain: str):
        now = time.time()
        last_d = self._last_domain.get(domain, 0.0)
        delay_d = max(0.0, last_d + self.per_domain_interval - now)
        delay_g = max(0.0, self._last_global + self.global_interval - now)
        delay = max(delay_d, delay_g)
        if delay > 0:
            time.sleep(delay)
        now2 = time.time()
        self._last_domain[domain] = now2
        self._last_global = now2

_rl: DomainRateLimiter | None = None

# ---------- metrics ----------
METRICS_PORT = int(os.getenv("METRICS_PORT", "9110"))
ingest_success = Counter("ingest_success_total", "Successful page ingests")
ingest_fail = Counter("ingest_fail_total", "Failed ingests", ["reason"])
ingest_bytes = Counter("ingest_bytes_total", "Total bytes ingested (page text)")
ingest_latency = Histogram("ingest_latency_seconds", "Ingest latency per page (seconds)")
heartbeat_gauge = Gauge("ingest_heartbeat", "Heartbeat 1=alive")

try:
    start_http_server(METRICS_PORT)
    logging.info("Metrics server on :%d", METRICS_PORT)
except Exception as e:
    logging.warning("Metrics server failed: %s", e)

def heartbeat():
    while True:
        time.sleep(10)
        heartbeat_gauge.set(1.0)
        mark("Heartbeat")

threading.Thread(target=heartbeat, daemon=True).start()

# ---------- config / env ----------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_API_KEY = os.environ.get("SUPABASE_API_KEY", "").strip()
TABLE = os.environ.get("SUPABASE_TABLE", "memory_docs")
USER_AGENT = os.getenv("USER_AGENT", "SaraphinaBot/1.0 (+contact: your-email@example.com)")
COLLECTOR_ID = os.getenv("COLLECTOR_ID", "ingest_worker")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_API_KEY in environment.")
    sys.exit(1)

PER_DOMAIN_RPS = float(os.getenv("PER_DOMAIN_RPS", "1"))
GLOBAL_RPS_CAP = int(os.getenv("GLOBAL_RPS_CAP", "20"))
DISCOVERY_MAX_LINKS = int(os.getenv("DISCOVERY_MAX_LINKS", "100"))
MIN_CHARS = int(os.getenv("MIN_DOC_CHARS", "300"))

# ---------- HTTP session with retries & timeouts ----------
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

def http_get(url: str):
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
    mark(f"Upserting {len(rows)} rows to {TABLE}")
    try:
        r = session.post(
            url,
            headers={**db_headers(), "Prefer": "resolution=merge-duplicates,return=representation"},
            data=json.dumps(rows),
            timeout=TIMEOUT,
        )
        if r.status_code >= 400:
            try:
                logging.error("Upsert failed (%s): %s", r.status_code, r.json())
            except Exception:
                logging.error("Upsert failed (%s): %s", r.status_code, r.text)
            r.raise_for_status()
        mark(f"Upsert complete ({len(rows)} rows)")
    except requests.RequestException as e:
        body = getattr(e, "response", None).text if getattr(e, "response", None) else ""
        logging.error("Upsert exception: %s | Response: %s", str(e), body)
        raise

# ---------- HTML fetch & robots ----------
def allowed_by_robots(url):
    p = urlparse(url)
    robots = f"{p.scheme}://{p.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        # pace robots fetch as well
        if _rl:
            _rl.wait(p.netloc or "unknown")
        mark(f"Fetching robots: {robots}")
        resp = http_get(robots)
        if resp.status_code >= 400:
            logging.warning("Robots fetch %s returned %s; assuming allowed for dev", robots, resp.status_code)
            return True
        rp.parse(resp.text.splitlines())
        allowed = rp.can_fetch(USER_AGENT, url)
        mark(f"Robots check for {url} -> {allowed}")
        return allowed
    except Exception as e:
        logging.warning("Robots error for %s: %s; assuming allowed for dev", robots, e)
        return True

def fetch_html(url):
    netloc = urlparse(url).netloc or "unknown"
    if _rl:
        _rl.wait(netloc)
    mark(f"GET {url}")
    r = http_get(url)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    for t in soup(["script", "style", "nav", "footer", "header", "aside", "form"]):
        t.decompose()
    text = " ".join(soup.get_text(separator=" ").split())
    mark(f"Fetched {url} status={r.status_code} chars={len(text)} title='{title[:60]}'")
    return title, text

# ---------- text utils ----------
def chunk_text(text, max_chars=1200, overlap=200):
    sentences = re.split(r'(?<=[\.\!\?])\s+', text.strip())
    chunks, cur = [], ""
    for s in sentences:
        if len(cur) + len(s) + 1 <= max_chars:
            cur = (cur + " " + s).strip()
        else:
            if cur:
                chunks.append(cur)
            cur = (cur[-overlap:] + " " + s).strip() if overlap > 0 and cur else s
    if cur:
        chunks.append(cur)
    return chunks

def detect_lang_safe(text):
    try:
        return detect(text)
    except LangDetectException:
        return "und"

def checksum_for(url, chunk):
    h = hashlib.sha256()
    h.update(url.encode("utf-8"))
    h.update(b"\x00")
    h.update(chunk.encode("utf-8"))
    return h.hexdigest()

def content_hash(chunk):
    return hashlib.sha256(chunk.encode("utf-8", errors="ignore")).hexdigest()

# ---------- processing ----------
def process_url(u: str, seen_chunk_hashes: set[str]) -> tuple[list[dict], int]:
    # Validate and policy gate
    if not is_valid_url(u):
        ingest_fail.labels(reason="invalid_url").inc()
        mark(f"Invalid URL format: {u}")
        return [], 0
    ok, decision = host_allowed(u)
    if not ok:
        ingest_fail.labels(reason=f"policy:{decision}").inc()
        mark(f"Policy blocked: {u} | decision={decision}")
        return [], 0

    mark(f"Process start: {u}")
    if not allowed_by_robots(u):
        ingest_fail.labels(reason="robots_block").inc()
        mark(f"Disallowed by robots: {u}")
        return [], 0

    try:
        title, text = fetch_html(u)
    except requests.HTTPError as e:
        ingest_fail.labels(reason=f"http_{getattr(e.response,'status_code', 'err')}").inc()
        mark(f"HTTP error for {u}: {e}")
        return [], 0
    except requests.RequestException as e:
        ingest_fail.labels(reason="fetch_net").inc()
        mark(f"Network error for {u}: {e}")
        return [], 0
    except Exception as e:
        ingest_fail.labels(reason="fetch_other").inc()
        logging.exception("Unexpected fetch error for %s: %s", u, e)
        return [], 0

    if not text or len(text) < MIN_CHARS:
        ingest_fail.labels(reason="too_short").inc()
        mark(f"Too short to index ({len(text)} chars): {u}")
        return [], 0

    lang = detect_lang_safe(text)
    chunks = chunk_text(text)
    ts = datetime.now(timezone.utc).isoformat()
    label = urlparse(u).netloc
    rows, bytes_total = [], len(text.encode("utf-8", errors="ignore"))

    for i, ch in enumerate(chunks):
        chash = content_hash(ch)
        if chash in seen_chunk_hashes:
            # skip duplicate chunk content seen earlier in this run
            continue
        seen_chunk_hashes.add(chash)

        ck = checksum_for(u, ch)  # stable per-url chunk id

        row = {
            "id": f"{label}::{ck[:12]}::{i:03d}",
            "label": label,
            "text": ch,
            "tags": [lang, "auto"],
            "source": "crawler",
            "timestamp": ts,
            "score": 0,
            "url": u,
            "title": title,
            "lang": lang,
            "checksum": ck,            # url+chunk-based id hash (existing)
            "content_hash": chash,     # pure chunk-content hash (for cross-run dedupe)
            "policy_tag": decision,    # policy decision code from host_allowed(u)
            "trust_score": 0.5,        # starter value; refine later
            "provenance": {            # lineage for auditability (filled/refined below)
                "source_url": u,
                "fetched_at": ts,
                "method": "http_get",
                "robots_ok": True
            }
        }

        # Enforce provenance per policy v1.2 (adds required fields and validates)
        ok_prov, missing = enforce_provenance(row, decision=decision, collector_id=COLLECTOR_ID)
        if not ok_prov:
            ingest_fail.labels(reason="provenance").inc()
            logging.warning("Dropping row for provenance missing fields=%s | id=%s | url=%s", missing, row["id"], u)
            continue

        rows.append(row)

    mark(f"Chunked {u} -> {len(rows)} chunks lang={lang}")
    return rows, bytes_total

def from_feed(feed_url, limit=20):
    mark(f"Fetch feed: {feed_url}")
    try:
        r = http_get(feed_url)
        r.raise_for_status()
    except requests.RequestException as e:
        ingest_fail.labels(reason="feed_fetch").inc()
        raise e
    fp = feedparser.parse(r.content)
    links = [e.get("link") for e in fp.entries[:limit] if e.get("link")]
    mark(f"Feed entries: {len(links)} from {feed_url}")
    return links

# ---------- main ----------
def main():
    global _rl
    _rl = DomainRateLimiter(PER_DOMAIN_RPS, GLOBAL_RPS_CAP)

    mark("Booting ingest worker")
    try:
        with open("seeds.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        logging.error("Failed to read seeds.json: %s", e)
        sys.exit(1)

    feeds = cfg.get("feeds", [])
    allow_domains = set(cfg.get("allow_domains", []))
    max_pages = int(cfg.get("max_pages_per_run", 20))
    mark(f"Config loaded | feeds={len(feeds)} allow_domains={len(allow_domains)} max_pages={max_pages}")

    urls = []
    for furl in feeds:
        try:
            urls.extend(from_feed(furl))
        except Exception as e:
            logging.warning("Feed error %s: %s", furl, e)

    if not urls:
        logging.warning("No URLs discovered from feeds. Check seeds.json.")
        return

    before = len(urls)
    # basic hygiene: drop blanks, dedup
    urls = [u for u in urls if u]
    urls = list(dict.fromkeys(urls))

    # apply seeds allow_domains if present
    if allow_domains:
        urls = [u for u in urls if (urlparse(u).netloc in allow_domains)]

    # also apply policy allowlist/denylist (centralized)
    urls = [u for u in urls if is_valid_url(u) and host_allowed(u)[0]]

    mark(f"Collected URLs | before={before} after_filter={len(urls)}")

    seen, batch, count = set(), [], 0
    seen_chunk_hashes: set[str] = set()

    for idx, u in enumerate(urls, start=1):
        if u in seen:
            continue
        seen.add(u)

        start = time.time()
        try:
            rows, bytes_total = process_url(u, seen_chunk_hashes)
        except Exception as e:
            ingest_fail.labels(reason="processing").inc()
            logging.exception("Processing error on %s: %s", u, e)
            rows, bytes_total = [], 0

        if rows:
            batch.extend(rows)
            count += 1
            ingest_bytes.inc(bytes_total)
            ingest_success.inc()
            ingest_latency.observe(time.time() - start)

        # upsert in batches or when hitting max_pages
        if len(batch) >= 20 or count >= max_pages:
            try:
                upsert_many(batch)
            except Exception:
                ingest_fail.labels(reason="upsert").inc()
            batch = []

        if count >= max_pages:
            mark(f"Reached page cap ({max_pages}); stopping.")
            break

        # polite pacing between pages (rate limiter already paces requests)
        time.sleep(0.1)

    if batch:
        try:
            upsert_many(batch)
        except Exception:
            ingest_fail.labels(reason="upsert").inc()

    mark(f"Done. Processed pages={count}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        mark("Interrupted by user")
        sys.exit(130)

# --- Optional one-shot test (set to True to test DB write quickly) ---
if False:
    ts = "2025-01-01T00:00:00Z"
    test_text = "Hello world"
    test_row = [{
        "id": "test::abc123::000",
        "label": "example.com",
        "text": test_text,
        "tags": ["en","auto"],
        "source": "crawler",
        "timestamp": ts,
        "score": 0,
        "url": "https://example.com/hello",
        "title": "Hello",
        "lang": "en",
        "checksum": "abc123",
        "content_hash": hashlib.sha256(test_text.encode("utf-8")).hexdigest(),
        "policy_tag": "ALLOW_MATCH",
        "trust_score": 0.5,
        "provenance": {
            "source_url": "https://example.com/hello",
            "fetched_at": ts,
            "method": "http_get",
            "robots_ok": True,
            "ingestion_timestamp": ts,
            "content_hash": hashlib.sha256(test_text.encode("utf-8")).hexdigest(),
            "license": "unknown",
            "collector_id": "ingest_worker"
        }
    }]
    upsert_many(test_row)
    mark("Test insert complete")
    sys.exit(0)
