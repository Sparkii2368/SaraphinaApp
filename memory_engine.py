# memory_engine.py
import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

class MemoryEngineError(Exception):
    pass

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _hash_content(content: str) -> str:
    """Generate a deterministic SHA256 hash for deduplication."""
    return hashlib.sha256(content.encode("utf-8", errors="ignore")).hexdigest()

class SupabaseMemoryAdapter:
    """
    REST adapter for Supabase memory storage, with governance + dedupe fields.
    Requires:
      - SUPABASE_URL
      - SUPABASE_API_KEY
      - SUPABASE_TABLE (optional, default: memory_docs)
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        table: str = "memory_docs",
        timeout: float = 10.0
    ):
        if base_url.endswith("/"):
            base_url = base_url.rstrip("/")
        self.rest_base = f"{base_url}/rest/v1"
        self.api_key = api_key
        self.table = table
        self.timeout = timeout
        self.min_trust = float(os.getenv("MIN_TRUST_SCORE", "0.5"))

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

    def _inject_governance_fields(
        self,
        metadata: Optional[Dict[str, Any]],
        content: str
    ) -> Dict[str, Any]:
        """Ensure governance + dedupe fields exist."""
        md = metadata.copy() if metadata else {}
        md.setdefault("policy_tag", "ungoverned")
        md.setdefault("trust_score", 0.0)
        md.setdefault("provenance", {"source": "unknown"})
        md.setdefault("content_hash", _hash_content(content))
        return md

    def save_memory(
        self,
        user_id: str,
        topic: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        payload = {
            "user_id": user_id,
            "topic": topic,
            "content": content,
            **self._inject_governance_fields(metadata, content),
            "timestamp": _utc_now_iso()
        }
        url = f"{self.rest_base}/{self.table}"
        resp = requests.post(url, headers=self._headers,
                             data=json.dumps(payload), timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"save_memory failed: {resp.status_code} {resp.text}")
        data = resp.json()
        return data[0] if isinstance(data, list) and data else data

    def fetch_memories(
        self,
        user_id: Optional[str] = None,
        topic: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
        order: str = "desc"
    ) -> List[Dict[str, Any]]:
        """Fetch rows with governance/trust filters and dedupe by content_hash."""
        params = {
            "select": "id,topic,content,policy_tag,trust_score,content_hash,provenance,timestamp",
            "order": f"timestamp.{order}",
            "limit": str(limit * 3)  # overshoot for dedupe
        }
        if user_id:
            params["user_id"] = f"eq.{user_id}"
        if topic:
            params["topic"] = f"eq.{topic}"
        if search:
            params["content"] = f"ilike.*{search}*"
        # Governance filters
        params["policy_tag"] = "eq.allowlist"
        params["trust_score"] = f"gte.{self.min_trust}"

        url = f"{self.rest_base}/{self.table}"
        resp = requests.get(url, headers=self._headers, params=params, timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"fetch_memories failed: {resp.status_code} {resp.text}")
        rows = resp.json()

        seen_hashes = set()
        deduped = []
        for r in rows:
            chash = r.get("content_hash")
            if not chash or chash in seen_hashes:
                continue
            seen_hashes.add(chash)
            deduped.append(r)
            if len(deduped) >= limit:
                break
        return deduped

    def delete_memory(self, memory_id: str, user_id: Optional[str] = None) -> int:
        params = {"id": f"eq.{memory_id}"}
        if user_id:
            params["user_id"] = f"eq.{user_id}"
        url = f"{self.rest_base}/{self.table}"
        headers = {**self._headers, "Prefer": "return=representation"}
        resp = requests.delete(url, headers=headers, params=params, timeout=self.timeout)
        if resp.status_code not in (200, 204):
            raise MemoryEngineError(f"delete_memory failed: {resp.status_code} {resp.text}")
        try:
            return len(resp.json())
        except Exception:
            return 0

    def upsert_memory(
        self,
        record: Dict[str, Any],
        on_conflict: str = "id"
    ) -> Dict[str, Any]:
        # Inject governance if missing
        if "policy_tag" not in record or "trust_score" not in record or "content_hash" not in record:
            record.update(self._inject_governance_fields(record.get("metadata", {}), record.get("content", "")))
        record.setdefault("timestamp", _utc_now_iso())

        url = f"{self.rest_base}/{self.table}"
        headers = {**self._headers, "Prefer": "resolution=merge-duplicates,return=representation"}
        params = {"on_conflict": on_conflict}
        resp = requests.post(url, headers=headers, params=params,
                             data=json.dumps(record), timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"upsert_memory failed: {resp.status_code} {resp.text}")
        data = resp.json()
        return data[0] if isinstance(data, list) and data else data

    def ping(self) -> bool:
        """Lightweight connectivity check to Supabase table."""
        try:
            url = f"{self.rest_base}/{self.table}"
            resp = requests.head(url, headers=self._headers, timeout=5)
            return resp.ok
        except Exception:
            return False

def get_memory_engine(
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    table: Optional[str] = None
) -> SupabaseMemoryAdapter:
    base_url = base_url or os.getenv("SUPABASE_URL")
    api_key = api_key or os.getenv("SUPABASE_API_KEY")
    table = table or os.getenv("SUPABASE_TABLE", "memory_docs")
    if not base_url or not api_key:
        raise MemoryEngineError("Missing SUPABASE_URL or SUPABASE_API_KEY.")
    return SupabaseMemoryAdapter(base_url=base_url, api_key=api_key, table=table)

if __name__ == "__main__":
    engine = get_memory_engine()
    print("Connected to:", engine.rest_base)
    print("Health check:", "OK" if engine.ping() else "FAIL")
    # Example governance-aware fetch
    results = engine.fetch_memories(search="test", limit=5)
    for r in results:
        print(r["timestamp"], r["policy_tag"], r["trust_score"], r["provenance"])
# --- Policy events (append-only JSONL with in-memory cache) ---
import os, json, threading, time
from collections import deque
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

_POLICY_EVENT_LOG = os.getenv("POLICY_EVENT_LOG", "policy_events.jsonl")
_POLICY_EVENT_MAX = int(os.getenv("POLICY_EVENT_MAX", "5000"))
_policy_event_lock = threading.Lock()
_policy_event_buf: deque = deque(maxlen=min(_POLICY_EVENT_MAX, 10000))

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log_policy_decision(*, actor: str, target: str, action: str, decision: str,
                        reason: Optional[str] = None, meta: Optional[Dict[str, Any]] = None):
    evt = {
        "ts": _utc_iso(),
        "type": "policy_decision",
        "actor": actor,            # e.g., "runner" or "ingest_worker"
        "target": target,          # hostname or URL
        "action": action,          # "read", "write", "execute"
        "decision": decision,      # ALLOW_MATCH, DENY_*, OVERRIDE_*
        "reason": reason or "",
        "meta": meta or {},
    }
    line = json.dumps(evt, ensure_ascii=False)
    with _policy_event_lock:
        _policy_event_buf.append(evt)
        try:
            with open(_POLICY_EVENT_LOG, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            # Don't raise; logging must never crash the worker/runner
            pass
    return evt

def get_recent_policy_events(limit: int = 200) -> List[Dict[str, Any]]:
    # Fast path: return from in-memory buffer if it’s warm and sufficient
    with _policy_event_lock:
        if len(_policy_event_buf) >= min(limit, _policy_event_buf.maxlen):
            return list(_policy_event_buf)[-limit:]
    # Slow path: read tail from file if present
    events: List[Dict[str, Any]] = []
    try:
        with open(_POLICY_EVENT_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                    events.append(evt)
                except Exception:
                    continue
        return events[-limit:]
    except FileNotFoundError:
        return list(_policy_event_buf)[-limit:]
