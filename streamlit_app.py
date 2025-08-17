import os
import json
import re
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from urllib.parse import urlparse

import streamlit as st
from dotenv import load_dotenv
from joblib import dump, load
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from bs4 import BeautifulSoup

# ------------------ Config ------------------
load_dotenv()  # Load .env if present

st.set_page_config(page_title="Saraphina — Living Knowledge", page_icon="✨", layout="wide")

def cfg(name: str, default=None):
    """Resolve config from env first, then st.secrets, else default."""
    val = os.getenv(name)
    if val is None:
        try:
            val = st.secrets.get(name, None)
        except Exception:
            val = None
    return val if val is not None else default

SUPABASE_URL = cfg("SUPABASE_URL") or cfg("SUPABASE_PROJECT_URL")
SUPABASE_API_KEY = cfg("SUPABASE_API_KEY") or cfg("SUPABASE_KEY")
SUPABASE_TABLE = cfg("SUPABASE_TABLE", "memory_docs")
DEFAULT_VIBE = cfg("BRAND_VIBE", "Neutral")
DEFAULT_DEPTH = int(cfg("RECALL_DEPTH", 3))
DEBUG_MODE = str(cfg("DEBUG_MODE", "False")).lower() == "true"
PREFIX = "saraphina"

# ------------------ State ------------------
memory_docs: List[dict] = []
vectorizer: Optional[TfidfVectorizer] = None
retriever: Optional[NearestNeighbors] = None
X = None

# ------------------ Supabase ------------------
def cloud_enabled():
    return bool(SUPABASE_URL and SUPABASE_API_KEY)

def db_headers():
    return {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def db_fetch_all():
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?select=*"
    r = requests.get(url, headers=db_headers(), timeout=30)
    r.raise_for_status()
    items = r.json()
    items.sort(key=lambda d: d.get("id", ""))
    return items

def db_upsert_many(docs: List[dict]):
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    r = requests.post(
        url,
        headers={**db_headers(), "Prefer": "resolution=merge-duplicates"},
        data=json.dumps(docs),
        timeout=30
    )
    r.raise_for_status()
    return True

# ------------------ Local Fallback ------------------
def local_save():
    if vectorizer is not None and retriever is not None and X is not None:
        dump(vectorizer, f"{PREFIX}_tfidf.joblib")
        dump(retriever, f"{PREFIX}_retriever.joblib")
        dump(X, f"{PREFIX}_X.joblib")
        with open(f"{PREFIX}_docs.json", "w", encoding="utf-8") as f:
            json.dump(memory_docs, f, ensure_ascii=False, indent=2)

def local_load():
    global vectorizer, retriever, X, memory_docs
    try:
        vectorizer = load(f"{PREFIX}_tfidf.joblib")
        retriever = load(f"{PREFIX}_retriever.joblib")
        X = load(f"{PREFIX}_X.joblib")
        with open(f"{PREFIX}_docs.json", "r", encoding="utf-8") as f:
            memory_docs = json.load(f)
        return True
    except Exception:
        memory_docs = []
        vectorizer = None
        retriever = None
        X = None
        return False

# ------------------ Helpers ------------------
def safe_text(s) -> str:
    try:
        return (s or "").strip()
    except Exception:
        return ""

def parse_iso_ts(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.strip().replace("Z", ""))
    except Exception:
        return None

# ------------------ Core ML ------------------
def chunk_text(text: str, max_chars: int = 800, overlap: int = 150) -> List[str]:
    sentences = re.split(r'(?<=[\.\!\?])\s+', (text or "").strip())
    chunks, cur = [], ""
    for s in sentences:
        if not s:
            continue
        if len(cur) + len(s) + 1 <= max_chars:
            cur = (cur + " " + s).strip()
        else:
            if cur:
                chunks.append(cur)
            cur = (cur[-overlap:] + " " + s).strip() if overlap > 0 and cur else s
    if cur:
        chunks.append(cur)
    return [c for c in chunks if c.strip()]

def retrain_corpus(recall_depth: int):
    global vectorizer, retriever, X
    texts = [safe_text(d.get("text")) for d in memory_docs]
    texts = [t for t in texts if t]
    if not texts:
        vectorizer = None
        retriever = None
        X = None
        return
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=8000)
    X = vectorizer.fit_transform(texts)
    retriever = NearestNeighbors(n_neighbors=max(1, recall_depth), metric="cosine")
    retriever.fit(X)

def retrieve(query: str, recall_depth: int):
    if retriever is None or vectorizer is None or X is None or not memory_docs:
        return []
    qv = vectorizer.transform([query])
    k = min(max(1, recall_depth), X.shape[0])
    dists, idxs = retriever.kneighbors(qv, n_neighbors=k)
    sims = 1 - dists[0]
    results = []
    for sim, idx in zip(sims, idxs[0]):
        doc = memory_docs[idx]
        results.append({**doc, "similarity": float(sim)})
    return results

def apply_vibe(text: str, vibe: str) -> str:
    v = (vibe or "Neutral").lower()
    if v == "bold":
        return f"{text}\n\nBottom line: take decisive action."
    if v == "warm":
        return f"{text}\n\nYou’ve got this—keep it human and thoughtful."
    return text

def synthesize_answer(query: str, vibe: str, recall_depth: int):
    hits = retrieve(query, recall_depth)
    if not hits:
        return "I don't have knowledge on that yet.", []
    bullets = [f"- {h['text']} (from {h.get('label', 'unknown')})" for h in hits]
    base = f"Question: {query}\nRecommendations:\n" + "\n".join(bullets)
    return apply_vibe(base, vibe), hits

def teach_text(text: str, label: str, tags: Optional[List[str]], source: str, recall_depth: int):
    ts = datetime.utcnow().isoformat()
    chunks = chunk_text(text)
    existing = [d for d in memory_docs if d.get("label") == label]
    start_idx = len(existing)
    new_docs = []
    for i, ch in enumerate(chunks):
        doc_id = f"{label}::{(start_idx + i):03d}"
        d = {
            "id": doc_id,
            "label": label,
            "text": ch,
            "tags": tags or [],
            "source": source,
            "timestamp": ts,
            "score": 0,
        }
        memory_docs.append(d)
        new_docs.append(d)
    if cloud_enabled():
        db_upsert_many(new_docs)
    else:
        local_save()
    retrain_corpus(recall_depth)

def fetch_url_text(url: str) -> str:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style", "nav", "footer", "header", "aside"]):
            t.decompose()
        text = " ".join(soup.get_text(separator=" ").split())
        return text[:200000]
    except Exception as e:
        return f"ERROR: {e}"

def load_brain(recall_depth: int):
    global memory_docs
    if cloud_enabled():
        try:
            memory_docs = db_fetch_all()
        except Exception as e:
            st.error(f"Cloud load failed: {e}")
            if not memory_docs:
                local_load()
    else:
        local_load()
    retrain_corpus(recall_depth)

# ------------------ UI ------------------
st.title("Saraphina — Living Knowledge")

with st.sidebar:
    st.header("Settings")
    vibe = st.selectbox(
        "Brand vibe",
        ["Neutral", "Warm", "Bold"],
        index=["Neutral", "Warm", "Bold"].index(DEFAULT_VIBE)
    )
    recall_depth = st.slider("Recall depth (k)", 1, 8, value=DEFAULT_DEPTH)
    if st.button("Reload brain"):
        load_brain(recall_depth)
        st.success("Brain reloaded.")
    st.markdown("---")
    st.caption(f"Cloud: {'ON' if cloud_enabled() else 'OFF'} • Debug: {'ON' if DEBUG_MODE else 'OFF'}")
    host = urlparse(SUPABASE_URL or "").netloc or "unknown"
    st.caption(f"Supabase: {host} • Table: {SUPABASE_TABLE}")
    st.caption(f"Memory size: {len(memory_docs)} docs")
