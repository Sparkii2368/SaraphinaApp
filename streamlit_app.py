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
st.set_page_config(
    page_title="Saraphina — Living Knowledge",
    page_icon="✨",
    layout="wide",
)

# Secrets (for Streamlit Cloud)
SUPABASE_URL = st.secrets.get("SUPABASE_URL", None)
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", None)

# Default app settings (can override via secrets)
DEFAULT_VIBE = st.secrets.get("BRAND_VIBE", "Neutral")
DEFAULT_DEPTH = int(st.secrets.get("RECALL_DEPTH", 3))
DEBUG_MODE = str(st.secrets.get("DEBUG_MODE", "False")).lower() == "true"

PREFIX = "saraphina"  # local file prefix if not using Supabase

# ------------------ In-memory state ------------------
memory_docs: List[dict] = []
vectorizer: Optional[TfidfVectorizer] = None
retriever: Optional[NearestNeighbors] = None
X = None

# ------------------ Cloud persistence (Supabase REST) ------------------
def cloud_enabled():
    return bool(SUPABASE_URL and SUPABASE_KEY)

def db_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def db_fetch_all():
    url = f"{SUPABASE_URL}/rest/v1/memory_docs?select=*"
    r = requests.get(url, headers=db_headers(), timeout=30)
    r.raise_for_status()
    items = r.json()
    items.sort(key=lambda d: d.get("id", ""))
    return items

def db_upsert_many(docs: List[dict]):
    url = f"{SUPABASE_URL}/rest/v1/memory_docs"
    r = requests.post(
        url,
        headers={**db_headers(), "Prefer": "resolution=merge-duplicates"},
        data=json.dumps(docs),
        timeout=30
    )
    r.raise_for_status()
    return True

# ------------------ Local persistence (fallback) ------------------
def local_save():
    if vectorizer is None or retriever is None or X is None:
        return
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
        memory_docs = [
            {
                "id": "linkedInPost::000",
                "label": "linkedInPost",
                "text": "Polished LinkedIn branding angle for Yourz-Uniquely: benefits + clear CTA + distinct aesthetic hook.",
                "tags": ["branding","LinkedIn"],
                "source": "seed",
                "timestamp": datetime.utcnow().isoformat(),
                "score": 0,
            },
            {
                "id": "cricutDesign::000",
                "label": "cricutDesign",
                "text": "Cricut tip: use offset layers and registration marks for precise multi-color vinyl alignment.",
                "tags": ["Cricut","craft"],
                "source": "seed",
                "timestamp": datetime.utcnow().isoformat(),
                "score": 0,
            },
            {
                "id": "DLL_fix::000",
                "label": "DLL_fix",
                "text": "DLL troubleshooting: match Python/NumPy bitness, clean venv, reinstall wheels; verify PATH conflicts.",
                "tags": ["troubleshooting","DLL"],
                "source": "seed",
                "timestamp": datetime.utcnow().isoformat(),
                "score": 0,
            },
        ]
        return False

# ------------------ Core ML ------------------
def retrain_corpus(recall_depth: int):
    global vectorizer, retriever, X
    texts = [d["text"] for d in memory_docs] or [""]
    vectorizer = TfidfVectorizer(ngram_range=(1,2), max_features=8000)
    X = vectorizer.fit_transform(texts)
    retriever = NearestNeighbors(n_neighbors=max(1, recall_depth), metric="cosine")
    retriever.fit(X)

def chunk_text(text: str,