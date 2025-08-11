import os
import json
import re
import requests
from datetime import datetime
from typing import List, Optional

import streamlit as st
from joblib import dump, load
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from bs4 import BeautifulSoup

# ------------------ Config ------------------
st.set_page_config(page_title="Saraphina — Living Knowledge", page_icon="✨", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL")
SUPABASE_API_KEY = st.secrets.get("SUPABASE_API_KEY")
SUPABASE_TABLE = st.secrets.get("SUPABASE_TABLE", "memory_docs")
DEFAULT_VIBE = st.secrets.get("BRAND_VIBE", "Neutral")
DEFAULT_DEPTH = int(st.secrets.get("RECALL_DEPTH", 3))
DEBUG_MODE = str(st.secrets.get("DEBUG_MODE", "False")).lower() == "true"
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

# ------------------ Core ML ------------------
def chunk_text(text: str, max_chars: int = 800, overlap: int = 150) -> List[str]:
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

def retrain_corpus(recall_depth: int):
    global vectorizer, retriever, X
    texts = [d["text"] for d in memory_docs] or [""]
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
    vibe = st.selectbox("Brand vibe", ["Neutral", "Warm", "Bold"], index=["Neutral", "Warm", "Bold"].index(DEFAULT_VIBE))
    recall_depth = st.slider("Recall depth (k)", 1, 8, value=DEFAULT_DEPTH)
    if st.button("Reload brain"):
        load_brain(recall_depth)
        st.success("Brain reloaded.")
    st.markdown("---")
    st.caption(f"Cloud: {'ON' if cloud_enabled() else 'OFF'} • Debug: {'ON' if DEBUG_MODE else 'OFF'}")
    st.caption(f"Memory size: {len(memory_docs)} docs")

if retriever is None or vectorizer is None or not memory_docs:
    load_brain(recall_depth)

# ---- Teach ----
st.subheader("Teach Saraphina")
with st.form("teach_form"):
    teach_label = st.text_input("Label", value="")  # ✅ closed parentheses, added a default
    teach_text_input = st.text_area("Content", value="")
    teach_tags = st.text_input("Tags (comma-separated)", value="")
    teach_source = st.text_input("Source", value="manual")

    submitted = st.form_submit_button("Teach")
    if submitted:
        if not teach_label.strip():
            st.warning("Please enter a label.")
        elif not teach_text_input.strip():
            st.warning("Please enter some content to teach.")
        else:
            tags_list = [t.strip() for t in teach_tags.split(",") if t.strip()]
            teach_text(teach_text_input, teach_label.strip(), tags_list, teach_source.strip(), recall_depth)
            st.success(f"Taught: {teach_label}")