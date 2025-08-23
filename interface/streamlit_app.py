# streamlit_app.py
# Saraphina v3 — Unified with worker/runner, observable, and precise

import os
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import streamlit as st
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from joblib import dump, load

# ──────────────────────────────────────────────────────────────────────────────
# Boot & config
# ──────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Saraphina — v3", page_icon="✨", layout="wide")
load_dotenv()

def cfg(name: str, default=None):
    v = os.getenv(name)
    if v is None:
        try:
            v = st.secrets.get(name, None)
        except Exception:
            v = None
    return v if v is not None else default

APP_NAME = "Saraphina v3"
CACHE_PREFIX = "saraphina_v3"

SUPABASE_URL = cfg("SUPABASE_URL") or cfg("SUPABASE_PROJECT_URL")
SUPABASE_API_KEY = cfg("SUPABASE_API_KEY") or cfg("SUPABASE_KEY")
SUPABASE_TABLE = cfg("SUPABASE_TABLE", "memory_docs")

DEFAULT_VIBE = cfg("BRAND_VIBE", "Neutral")
DEFAULT_RECALL_DEPTH = int(cfg("RECALL_DEPTH", 4))
DEBUG_MODE = str(cfg("DEBUG_MODE", "False")).lower() == "true"

MAX_PREVIEW_ROWS = int(cfg("MAX_PREVIEW_ROWS", 200))

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def mask_key(key: Optional[str]) -> str:
    if not key:
        return "∅"
    if len(key) <= 8:
        return key[:2] + "…" + key[-2:]
    return key[:4] + "…" + key[-4:]

def short_hash(s: str, n: int = 10) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:n]

def parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def normalize_tags(tags_val: Any) -> List[str]:
    # Accepts jsonb list, text[], stringified JSON, or None
    if tags_val is None:
        return []
    if isinstance(tags_val, list):
        return [str(x).strip() for x in tags_val if str(x).strip()]
    if isinstance(tags_val, str):
        s = tags_val.strip()
        if s.startswith("{") and s.endswith("}"):  # text[]
            inner = s[1:-1]
            if not inner:
                return []
            parts, cur, in_quote = [], "", False
            for ch in inner:
                if ch == '"':
                    in_quote = not in_quote
                elif ch == "," and not in_quote:
                    parts.append(cur); cur = ""; continue
                cur += ch
            if cur:
                parts.append(cur)
            return [p.strip().strip('"').strip() for p in parts if p.strip().strip('"').strip()]
        if s.startswith("[") and s.endswith("]"):  # stringified JSON
            try:
                arr = json.loads(s)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                return [s]
        return [s]
    return [str(tags_val)]

# ──────────────────────────────────────────────────────────────────────────────
# Supabase client (PostgREST)
# ──────────────────────────────────────────────────────────────────────────────

class Supabase:
    def __init__(self, url: Optional[str], key: Optional[str], table: str):
        self.url = url.rstrip("/") if url else None
        self.key = key
        self.table = table
        self.timeout = 25
        self.retries = 3
        self.last_status: Optional[int] = None
        self.last_error: Optional[str] = None

    def enabled(self) -> bool:
        return bool(self.url and self.key and self.table)

    def headers(self) -> Dict[str, str]:
        return {
            "apikey": self.key or "",
            "Authorization": f"Bearer {self.key or ''}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "count=exact",
        }

    def _request(self, method: str, path: str, **kwargs) -> Optional[requests.Response]:
        if not self.enabled():
            self.last_error = "Supabase disabled (missing URL/API key/table)"
            self.last_status = None
            return None
        url = f"{self.url}{path}"
        headers = {**self.headers(), **kwargs.pop("headers", {})}
        last_exc = None
        for attempt in range(1, self.retries + 1):
            try:
                r = requests.request(method, url, headers=headers, timeout=self.timeout, **kwargs)
                self.last_status = r.status_code
                if r.status_code >= 400:
                    self.last_error = f"{r.status_code} {r.text}"
                else:
                    self.last_error = None
                return r
            except Exception as e:
                last_exc = e
                time.sleep(min(2.0 * attempt, 3.0))
        self.last_error = f"Network error: {last_exc}"
        self.last_status = None
        return None

    def ping(self) -> Tuple[bool, str]:
        r = self._request("GET", "/rest/v1/")
        if r is None:
            return False, self.last_error or "Unknown"
        if 200 <= r.status_code < 400:
            return True, "OK"
        return False, self.last_error or f"HTTP {r.status_code}"

    def count(self) -> Tuple[Optional[int], Optional[str]]:
        r = self._request("GET", f"/rest/v1/{self.table}?select=id&limit=1")
        if r is None:
            return None, self.last_error
        if r.status_code >= 400:
            return None, self.last_error
        cr = r.headers.get("Content-Range", "")
        if "/" in cr:
            try:
                return int(cr.split("/")[-1]), None
            except Exception:
                pass
        try:
            return len(r.json()), None
        except Exception:
            return None, "Count parse failed"

    def fetch_all(self, select: str = "*") -> Tuple[List[Dict[str, Any]], Optional[str]]:
        r = self._request("GET", f"/rest/v1/{self.table}?select={select}")
        if r is None or r.status_code >= 400:
            return [], self.last_error or "Fetch failed"
        try:
            return r.json(), None
        except Exception as e:
            return [], f"JSON decode error: {e}"

    def fetch_since(self, since_iso: str, select: str = "*") -> Tuple[List[Dict[str, Any]], Optional[str]]:
        q = f"/rest/v1/{self.table}?timestamp=gte.{since_iso}&select={select}"
        r = self._request("GET", q)
        if r is None or r.status_code >= 400:
            return [], self.last_error or "Fetch failed"
        try:
            return r.json(), None
        except Exception as e:
            return [], f"JSON decode error: {e}"

    def upsert(self, rows: List[Dict[str, Any]]) -> Optional[str]:
        r = self._request(
            "POST",
            f"/rest/v1/{self.table}",
            headers={"Prefer": "resolution=merge-duplicates,return=representation"},
            data=json.dumps(rows),
        )
        if r is None or r.status_code >= 400:
            return self.last_error or "Upsert failed"
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Memory engine (TF‑IDF + kNN)
# ──────────────────────────────────────────────────────────────────────────────

class Memory:
    def __init__(self):
        self.docs: List[Dict[str, Any]] = []
        self.vectorizer: Optional[TfidfVectorizer] = None
        self.nn: Optional[NearestNeighbors] = None
        self.X = None

    def size(self) -> int:
        return len(self.docs)

    def normalize(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = {
                "id": str(r.get("id")) if r.get("id") is not None else None,
                "label": str(r.get("label", "")).strip(),
                "text": (r.get("text") or ""),
                "tags": normalize_tags(r.get("tags")),
                "source": str(r.get("source", "unknown")).strip(),
                "timestamp": r.get("timestamp"),
                "score": r.get("score", 0),
            }
            if not d["id"]:
                d["id"] = f"{d['label']}::{short_hash(d['text'], 12)}"
            ts = d.get("timestamp")
            if isinstance(ts, (int, float)):
                try:
                    d["timestamp"] = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
                except Exception:
                    d["timestamp"] = None
            elif isinstance(ts, str):
                d["timestamp"] = ts
            else:
                d["timestamp"] = None
            out.append(d)
        out.sort(key=lambda x: (x.get("timestamp") or "", x.get("id") or ""))
        return out

    def retrain(self, n_neighbors: int = 8):
        texts = [d["text"] for d in self.docs if (d.get("text") or "").strip()]
        if not texts:
            self.vectorizer = None
            self.nn = None
            self.X = None
            return
        self.vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=16000)
        self.X = self.vectorizer.fit_transform(texts)
        self.nn = NearestNeighbors(n_neighbors=n_neighbors, metric="cosine")
        self.nn.fit(self.X)

    def retrieve(self, query: str, k: int) -> List[Dict[str, Any]]:
        if not query.strip() or self.vectorizer is None or self.nn is None or self.X is None or not self.docs:
            return []
        qv = self.vectorizer.transform([query])
        kk = max(1, min(k, self.X.shape[0]))
        dists, idxs = self.nn.kneighbors(qv, n_neighbors=kk)
        sims = 1 - dists[0]
        res: List[Dict[str, Any]] = []
        for sim, idx in zip(sims, idxs[0]):
            res.append({**self.docs[idx], "similarity": float(sim)})
        return res

    def save_local(self):
        try:
            with open(f"{CACHE_PREFIX}_docs.json", "w", encoding="utf-8") as f:
                json.dump(self.docs, f, ensure_ascii=False, indent=2)
            if self.vectorizer is not None: dump(self.vectorizer, f"{CACHE_PREFIX}_tfidf.joblib")
            if self.nn is not None: dump(self.nn, f"{CACHE_PREFIX}_nn.joblib")
            if self.X is not None: dump(self.X, f"{CACHE_PREFIX}_X.joblib")
        except Exception:
            pass

    def load_local(self) -> bool:
        try:
            with open(f"{CACHE_PREFIX}_docs.json", "r", encoding="utf-8") as f:
                self.docs = json.load(f)
            self.vectorizer = load(f"{CACHE_PREFIX}_tfidf.joblib")
            self.nn = load(f"{CACHE_PREFIX}_nn.joblib")
            self.X = load(f"{CACHE_PREFIX}_X.joblib")
            return True
        except Exception:
            self.docs = []
            self.vectorizer = None
            self.nn = None
            self.X = None
            return False

# ──────────────────────────────────────────────────────────────────────────────
# App state
# ──────────────────────────────────────────────────────────────────────────────

sb = Supabase(SUPABASE_URL, SUPABASE_API_KEY, SUPABASE_TABLE)
mem = Memory()

if "last_sync_iso" not in st.session_state:
    st.session_state.last_sync_iso = None

# ──────────────────────────────────────────────────────────────────────────────
# UI — header & sidebar
# ──────────────────────────────────────────────────────────────────────────────

st.title(APP_NAME)
st.caption("Reads the same Supabase as worker/runner • In‑app diagnostics • Deterministic memory")

with st.sidebar:
    st.header("Settings")
    col_a, col_b = st.columns(2)
    with col_a:
        vibe = st.selectbox("Brand vibe", ["Neutral", "Warm", "Bold"], index=["Neutral","Warm","Bold"].index(DEFAULT_VIBE) if DEFAULT_VIBE in ["Neutral","Warm","Bold"] else 0)
    with col_b:
        recall_k = st.slider("Recall depth (k)", 1, 10, value=DEFAULT_RECALL_DEPTH if 1 <= DEFAULT_RECALL_DEPTH <= 10 else 4)

    st.markdown("---")
    st.subheader("Connection")
    st.text_input("Supabase URL", value=sb.url or "", disabled=True)
    st.text_input("API key", value=mask_key(sb.key), disabled=True)
    st.text_input("Table", value=sb.table, disabled=True)

    col_c1, col_c2 = st.columns(2)
    with col_c1:
        if st.button("Ping Supabase"):
            ok, msg = sb.ping()
            st.success("Supabase OK") if ok else st.error(f"Ping failed: {msg}")
    with col_c2:
        if st.button("Clear local cache"):
            removed = 0
            for fn in [f"{CACHE_PREFIX}_docs.json", f"{CACHE_PREFIX}_tfidf.joblib", f"{CACHE_PREFIX}_nn.joblib", f"{CACHE_PREFIX}_X.joblib"]:
                if os.path.exists(fn):
                    os.remove(fn); removed += 1
            mem.docs, mem.vectorizer, mem.nn, mem.X = [], None, None, None
            st.success(f"Cleared {removed} cache files")

# ──────────────────────────────────────────────────────────────────────────────
# Sync controls
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("---")
st.header("Synchronization")

col_s1, col_s2, col_s3 = st.columns([1,1,2])

with col_s1:
    if st.button("Pull all"):
        rows, err = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
        if err:
            st.error(f"Fetch failed: {err}")
        else:
            mem.docs = mem.normalize(rows)
            mem.retrain(n_neighbors=max(2, recall_k))
            mem.save_local()
            st.session_state.last_sync_iso = now_iso()
            st.success(f"Loaded {mem.size()} docs • Vectorizer ready: {'Yes' if mem.vectorizer else 'No'}")

with col_s2:
    if st.button("Delta since last sync"):
        if not st.session_state.last_sync_iso:
            st.info("No prior sync; performing full pull.")
            rows, err = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
        else:
            rows, err = sb.fetch_since(st.session_state.last_sync_iso, select="id,label,text,tags,source,timestamp,score")
        if err:
            st.error(f"Delta fetch failed: {err}")
        else:
            incoming = mem.normalize(rows)
            index = {d["id"]: i for i, d in enumerate(mem.docs)}
            added, updated = 0, 0
            for d in incoming:
                if d["id"] in index:
                    mem.docs[index[d["id"]]] = d; updated += 1
                else:
                    mem.docs.append(d); added += 1
            mem.docs.sort(key=lambda x: (x.get("timestamp") or "", x.get("id") or ""))
            mem.retrain(n_neighbors=max(2, recall_k))
            mem.save_local()
            st.session_state.last_sync_iso = now_iso()
            st.success(f"Delta applied • Added {added} • Updated {updated} • Total {mem.size()}")

with col_s3:
    with st.expander("Connection diagnostics", expanded=False):
        total, err = sb.count()
        if err:
            st.error(f"Count error: {err}")
        else:
            st.write(f"Rows visible with current key: {total}")
        st.write("Last HTTP status:", sb.last_status)
        st.write("Last error:", sb.last_error or "None")
        rows, err2 = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
        if err2:
            st.error(f"Sample fetch error: {err2}")
        else:
            st.caption(f"Raw preview (up to {min(MAX_PREVIEW_ROWS, len(rows))} rows)")
            st.json(rows[: min(MAX_PREVIEW_ROWS, len(rows))])

# ──────────────────────────────────────────────────────────────────────────────
# Memory health & inspector
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("---")
st.header("Memory health")

col_h1, col_h2, col_h3, col_h4 = st.columns(4)
col_h1.metric("Docs in memory", mem.size())
latest_ts = max((parse_ts(d.get("timestamp")) for d in mem.docs if d.get("timestamp")), default=None) if mem.docs else None
col_h2.metric("Latest update", latest_ts.isoformat() if latest_ts else "n/a")
col_h3.metric("Unique labels", len({(d.get("label") or '').strip() for d in mem.docs}))
col_h4.metric("Vectorizer ready", "Yes" if mem.vectorizer is not None else "No")

with st.expander("Inspect memory", expanded=False):
    labels_sorted = sorted({d.get("label") or "" for d in mem.docs})
    sources_sorted = sorted({d.get("source") or "" for d in mem.docs})
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sel_label = st.selectbox("Filter by label", ["(any)"] + labels_sorted, index=0)
    with col_f2:
        sel_source = st.selectbox("Filter by source", ["(any)"] + sources_sorted, index=0)

    filtered = mem.docs
    if sel_label != "(any)":
        filtered = [d for d in filtered if (d.get("label") or "") == sel_label]
    if sel_source != "(any)":
        filtered = [d for d in filtered if (d.get("source") or "") == sel_source]

    st.caption(f"Showing {len(filtered)} of {mem.size()}")
    for d in filtered[:200]:
        snippet = (d.get("text") or "")[:240] + ("…" if len(d.get("text") or "") > 240 else "")
        st.markdown(f"- **ID:** {d.get('id')} • **Label:** {d.get('label')} • **Source:** {d.get('source')} • **Tags:** {', '.join(d.get('tags', [])) or '—'}")
        st.write(snippet)

# ──────────────────────────────────────────────────────────────────────────────
# Ask Saraphina
# ──────────────────────────────────────────────────────────────────────────────

def apply_vibe(text: str, vibe: str) -> str:
    v = (vibe or "Neutral").lower()
    if v == "bold":
        return f"{text}\n\nBottom line: move now."
    if v == "warm":
        return f"{text}\n\nYou’ve got this—steady and human."
    return text

st.markdown("---")
st.header("Ask Saraphina")

q = st.text_input("Your question")
if q:
    hits = mem.retrieve(q, recall_k)
    if not hits:
        st.warning("No relevant memory yet or retriever not trained.")
    else:
        bullets = "\n".join([f"- {h.get('text')} (from {h.get('label')})" for h in hits])
        answer = f"Question: {q}\nRecommendations:\n{bullets}"
        st.text_area("Answer", value=apply_vibe(answer, vibe), height=240)
        with st.expander("Retrieved context"):
            for h in hits:
                st.markdown(f"- **Sim:** {h.get('similarity', 0):.3f} • **Label:** {h.get('label')} • **Source:** {h.get('source')}")
                st.write(h.get("text"))

# ──────────────────────────────────────────────────────────────────────────────
# Teach (optional write‑back that aligns with worker/runner schema)
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("---")
st.header("Teach Saraphina (optional)")

with st.form("teach_form"):
    t_label = st.text_input("Label")
    t_text = st.text_area("Content")
    t_tags = st.text_input("Tags (comma-separated)")
    t_source = st.text_input("Source", value="manual")
    submit = st.form_submit_button("Upsert to Supabase and refresh")
    if submit:
        if not sb.enabled():
            st.error("Supabase is disabled. Configure URL and API key.")
        elif not t_label.strip() or not t_text.strip():
            st.warning("Please provide both label and content.")
        else:
            ts = now_iso()
            row = {
                "id": f"{t_label.strip()}::{short_hash(t_text.strip(), 12)}",  # stable id to avoid dupes
                "label": t_label.strip(),
                "text": t_text.strip(),
                "tags": [x.strip() for x in t_tags.split(",") if x.strip()],
                "source": t_source.strip(),
                "timestamp": ts,
                "score": 0,
            }
            err = sb.upsert([row])
            if err:
                st.error(f"Upsert failed: {err}")
            else:
                st.success("Upserted. Pulling latest…")
                rows, err2 = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
                if err2:
                    st.error(f"Refresh failed: {err2}")
                else:
                    mem.docs = mem.normalize(rows)
                    mem.retrain(n_neighbors=max(2, recall_k))
                    mem.save_local()
                    st.session_state.last_sync_iso = now_iso()
                    st.info(f"Memory now at {mem.size()} docs.")

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("---")
st.header("Diagnostics")

col_d1, col_d2 = st.columns(2)

with col_d1:
    st.subheader("Environment")
    st.json({
        "cloud_enabled": sb.enabled(),
        "supabase_url": sb.url,
        "table": sb.table,
        "api_key_masked": mask_key(sb.key),
        "debug": DEBUG_MODE,
        "last_sync_iso": st.session_state.last_sync_iso,
        "docs_in_memory": mem.size(),
        "vectorizer_ready": mem.vectorizer is not None,
        "nn_ready": mem.nn is not None,
    })

with col_d2:
    st.subheader("Raw Supabase preview")
    rows, err = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
    if err:
        st.error(err)
    else:
        st.write(f"Rows fetched: {len(rows)}")
        st.json(rows[: min(5, len(rows))])

# Load local cache on cold start; otherwise do an initial pull if possible
if mem.size() == 0 and os.path.exists(f"{CACHE_PREFIX}_docs.json"):
    mem.load_local()
if mem.size() == 0 and sb.enabled():
    rows, err = sb.fetch_all(select="id,label,text,tags,source,timestamp,score")
    if not err:
        mem.docs = mem.normalize(rows)
        mem.retrain(n_neighbors=max(2, DEFAULT_RECALL_DEPTH))
        mem.save_local()
