
from dotenv import load_dotenv
load_dotenv()

import os, json, re, time, hashlib, sys, threading, logging, atexit
from urllib.parse import urlparse, urljoin
from urllib import robotparser
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests, feedparser
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

memory_engine integration
from memoryengine import logpolicy_decision
from guardrails import policy_check
from policyloader import loadpolicy, getpolicyversion
from connectors.wikipedia import WikipediaConnector
from prometheusclient import Counter, Histogram, Gauge, starthttp_server

----------------------------------------------------------------------

Logging

----------------------------------------------------------------------
LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
t0 = time.perfcounter()
def mark(msg: str):
    logging.info("%s | t=%.2fs", msg, time.perfcounter() - t0)

----------------------------------------------------------------------

Configuration

----------------------------------------------------------------------
def load_config() -> dict:
    cfg = {"enablewikipedia": True, "batchsize": 50}
    if os.path.exists("config.json"):
        try:
            with open("config.json","r",encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as e:
            logging.warning("Could not load config.json: %s", e)
    if "ENABLE_WIKIPEDIA" in os.environ:
        cfg["enablewikipedia"] = os.environ["ENABLEWIKIPEDIA"].lower() in ("1","true","yes")
    if "BATCH_SIZE" in os.environ:
        try:
            cfg["batchsize"] = int(os.environ["BATCHSIZE"])
        except ValueError:
            logging.warning("Invalid BATCH_SIZE in env, using default.")
    return cfg

cfg = load_config()

----------------------------------------------------------------------

Policy helpers

----------------------------------------------------------------------
def isvalidurl(u: str) -> bool:
    try:
        parts = urlparse(u)
        return parts.scheme in ("http","https") and bool(parts.netloc)
    except Exception:
        return False

def host_allowed(u: str) -> tuple[bool,str]:
    host = urlparse(u).hostname or ""
    return policy_check(host, "read")

def provenancepolicy():
    pol = load_policy()
    req = pol.get("provenance_requirements", {}) or {}
    return {
        "requiredfields": list(req.get("requiredfields", [])),
        "enforceon": list(req.get("enforceon", [])),
        "blockifmissing": bool(req.get("blockifmissing", False)),
    }

def enforceprovenance(row: dict, decision: str, collectorid: str):
    provpol = provenance_policy()
    required = provpol["requiredfields"]
    enforceon = set(p.lower() for p in provpol["enforce_on"])
    blockifmissing = provpol["blockif_missing"]

    enforce = True
    if enforce_on:
        context = "allowlist" if decision.startswith(("ALLOW","OVERRIDE")) else "denylist"
        enforce = context in enforce_on

    row.setdefault("provenance", {})
    prov = row["provenance"]
    prov.setdefault("source_url", row.get("url"))
    prov.setdefault("ingestion_timestamp", row.get("timestamp"))
    prov.setdefault("contenthash", row.get("contenthash"))
    prov.setdefault("license","unknown")
    prov.setdefault("collectorid", collectorid)

    if not enforce or not required:
        return True, []

    missing = [k for k in required if not prov.get(k)]
    if missing:
        if blockifmissing:
            return False, missing
        else:
            logging.warning("Provenance missing (non-blocking): %s for id=%s", missing, row.get("id"))
    return True, []

---------- rate limiting ----------
class DomainRateLimiter:
    def init(self, perdomainrps: float=1.0, globalrpscap: int=20):
        self.perdomaininterval = 1.0 / max(perdomainrps,0.0001)
        self.globalinterval = 1.0 / max(globalrps_cap,1)
        self.lastdomain = {}
        self.lastglobal = 0.0
    def wait(self, domain: str):
        now = time.time()
        delayd = max(0.0, self.lastdomain.get(domain,0.0)+self.perdomain_interval - now)
        delayg = max(0.0, self.lastglobal + self.globalinterval - now)
        delay = max(delayd, delayg)
        if delay>0: time.sleep(delay)
        now2 = time.time()
        self.lastdomain[domain] = now2
        self.lastglobal = now2

_rl: DomainRateLimiter | None = None

---------- metrics ----------
METRICSPORT = int(os.getenv("METRICSPORT","9110"))
METRICSADDR = os.getenv("METRICSADDR","0.0.0.0")
METRICSLINGERSECS = int(os.getenv("METRICSLINGERSECS","120"))

ingestsuccess = Counter("ingestsuccess_total","Successful page ingests")
ingestfail = Counter("ingestfail_total","Failed ingests",["reason"])
ingestbytes = Counter("ingestbytes_total","Total bytes ingested (page text)")
ingestlatency = Histogram("ingestlatency_seconds","Ingest latency per page (seconds)")
heartbeatgauge = Gauge("ingestheartbeat","Heartbeat 1=alive,0=down")
rawstoresuccess = Counter("rawstoresuccess_total","Successful raw doc stores")
rawstorefail = Counter("rawstorefail_total","Failed raw doc stores",["reason"])

stopheartbeat = threading.Event()
def heartbeat():
    while not stopheartbeat.is_set():
        heartbeat_gauge.set(1.0)
        time.sleep(5)

@atexit.register
def shutdownmetrics():
    try: heartbeat_gauge.set(0.0)
    except: pass
    stopheartbeat.set()

---------- HTTP session ----------
SUPABASEURL = os.environ.get("SUPABASEURL","").strip()
SUPABASEAPIKEY = os.environ.get("SUPABASEAPIKEY","").strip()
TABLE = os.environ.get("SUPABASETABLE","memorydocs")
RAWTABLE = os.environ.get("SUPABASERAWTABLE","memoryraw_docs")
USERAGENT = os.getenv("USERAGENT","SaraphinaBot/1.0 (+contact: your-email@example.com)")
COLLECTORID = os.getenv("COLLECTORID","ingest_worker")

if not SUPABASEURL or not SUPABASEAPI_KEY:
    logging.error("Missing SUPABASEURL or SUPABASEAPI_KEY in environment.")
    sys.exit(1)

PERDOMAINRPS = float(os.getenv("PERDOMAINRPS","1"))
GLOBALRPSCAP = int(os.getenv("GLOBALRPSCAP","20"))
DISCOVERYMAXLINKS = int(os.getenv("DISCOVERYMAXLINKS","100"))
MINCHARS = int(os.getenv("MINDOC_CHARS","300"))
BATCHSIZE = int(os.getenv("UPSERTBATCHSIZE", cfg.get("batchsize",50)))
CONNECTTIMEOUT = float(os.getenv("CONNECTTIMEOUT","5"))
READTIMEOUT = float(os.getenv("READTIMEOUT","25"))
TIMEOUT = (CONNECTTIMEOUT, READTIMEOUT)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
adapter = HTTPAdapter(max_retries=Retry(
    total=3, backofffactor=0.5, statusforcelist=[429,500,502,503,504],
    allowedmethods=["GET","POST","HEAD","OPTIONS"], raiseon_status=False))
session.mount("http://", adapter)
session.mount("https://", adapter)

---------- helpers ----------
def http_get(url:str): return session.get(url, timeout=TIMEOUT)
def dbheaders(): return {"apikey": SUPABASEAPIKEY, "Authorization":f"Bearer {SUPABASEAPI_KEY}","Content-Type":"application/json","Accept":"application/json"}

def checksum_for(url,chunk): return hashlib.sha256(url.encode()+b"\x00"+chunk.encode()).hexdigest()
def content_hash(chunk): return hashlib.sha256(chunk.encode("utf-8",errors="ignore")).hexdigest()
def detectlangsafe(text): 
    try: return detect(text)
    except LangDetectException: return "und"

def chunktext(text,maxchars=1200,overlap=200):
    sentences=re.split(r'(?<=[\.\!\?])\s+',text.strip())
    chunks,cur=[], ""
    for s in sentences:
        if len(cur)+len(s)+1 <= max_chars: cur=(cur+" "+s).strip()
        else:
            if cur: chunks.append(cur)
            cur=(cur[-overlap:]+" "+s).strip() if overlap>0 and cur else s
    if cur: chunks.append(cur)
    return chunks

---------- processing ----------
def processurl(u:str, seenchunk_hashes:set[str]) -> tuple[List[dict],int]:
    if not isvalidurl(u): 
        ingestfail.labels(reason="invalidurl").inc()
        return [],0

    ok, decision = host_allowed(u)
    if not ok:
        ingest_fail.labels(reason=f"policy:{decision}").inc()
        logpolicydecision(actor="ingestworker", target=u, action="ingest", decision=decision, reason="Blocked by host policy", meta={"policyversion": getpolicyversion()})
        return [],0

    # robots
    p=urlparse(u); rp=robotparser.RobotFileParser()
    try: 
        if rl: rl.wait(p.netloc or "unknown")
        resp=http_get(f"{p.scheme}://{p.netloc}/robots.txt")
        if resp.status_code<400: rp.parse(resp.text.splitlines())
    except: pass
    if not rp.canfetch(USERAGENT,u): 
        ingestfail.labels(reason="robotsblock").inc()
        return [],0

    try:
        r=httpget(u); r.raisefor_status()
        soup=BeautifulSoup(r.text,"html.parser")
        for t in soup(["script","style","nav","footer","header","aside","form"]): t.decompose()
        text=" ".join(soup.get_text(separator=" ").split())
        title=(soup.title.string.strip() if soup.title else "")
    except Exception as e:
        ingestfail.labels(reason="fetcherror").inc()
        logging.warning("Fetch error: %s", e)
        return [],0
    if len(text)<MINCHARS: ingestfail.labels(reason="too_short").inc(); return [],0

    ts=datetime.now(timezone.utc).isoformat()
    lang=detectlangsafe(text)
    bytes_total=len(text.encode("utf-8",errors="ignore"))
    label=urlparse(u).netloc

    # store raw doc
    rawid=f"{label}::{checksumfor(u,text)[:12]}::raw"
    storerawdoc(rawid,text,url=u,title=title,lang=lang,timestamp=ts,policytag=decision,contenthash=contenthash(text),collectorid=COLLECTORID)

    # chunk & build rows
    rows=[]
    for i,ch in enumerate(chunk_text(text)):
        chash=content_hash(ch)
        if chash in seenchunkhashes: continue
        seenchunkhashes.add(chash)
        ck=checksum_for(u,ch)
        row={"id":f"{label}::{ck[:12]}::{i:03d}", "label":label,"text":ch,"tags":[lang,"auto"],"source":"crawler","timestamp":ts,"score":0,"url":u,"title":title,"lang":lang,"checksum":ck,"contenthash":chash,"policytag":decision,"trustscore":0.5,"provenance":{"sourceurl":u,"fetchedat":ts,"method":"httpget","robots_ok":True}}
        okprov,missing=enforceprovenance(row,decision,collectorid=COLLECTORID)
        logpolicydecision(actor="ingestworker", target=row.get("url","unknown"), action="ingest", decision=decision if okprov else f"DENYMISSING{''.join(missing).upper()}", reason="Provenance validation", meta={"docid":row.get("id"),"policyversion":getpolicyversion(),"missingfields":missing})
        if not okprov: ingestfail.labels(reason="provenance").inc(); continue
        rows.append(row)
    return rows, bytes_total compare and check resend and upgrade ingest worker