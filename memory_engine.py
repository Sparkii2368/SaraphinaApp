# memory_engine.py
import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests


class MemoryEngineError(Exception):
    pass


class SupabaseMemoryAdapter:
    """
    A lightweight REST adapter for Supabase.
    Requires:
      - SUPABASE_URL (e.g., https://<ref>.supabase.co)
      - SUPABASE_API_KEY (anon key for public or service_role for server-side)
      - SUPABASE_TABLE (default: memory_docs)
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        table: str = "memory_docs",
        timeout: float = 10.0
    ):
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        self.rest_base = f"{base_url}/rest/v1"
        self.api_key = api_key
        self.table = table
        self.timeout = timeout

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"  # return inserted/updated rows
        }

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
            "metadata": metadata or {},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        url = f"{self.rest_base}/{self.table}"
        resp = requests.post(url, headers=self._headers, data=json.dumps(payload), timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"save_memory failed: {resp.status_code} {resp.text}")
        data = resp.json()
        return data[0] if isinstance(data, list) and data else data

    def fetch_memories(
        self,
        user_id: str,
        topic: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
        order: str = "desc"  # or "asc"
    ) -> List[Dict[str, Any]]:
        """
        Filter by user_id, optionally topic, optionally fuzzy search on content.
        """
        params = {
            "select": "*",
            "user_id": f"eq.{user_id}",
            "order": f"timestamp.{order}",
            "limit": str(limit)
        }
        if topic:
            params["topic"] = f"eq.{topic}"
        if search:
            # ilike uses % wildcard. Supabase REST uses * for wildcard in URL.
            params["content"] = f"ilike.*{search}*"

        url = f"{self.rest_base}/{self.table}"
        resp = requests.get(url, headers=self._headers, params=params, timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"fetch_memories failed: {resp.status_code} {resp.text}")
        return resp.json()

    def delete_memory(self, memory_id: str, user_id: Optional[str] = None) -> int:
        """
        Deletes a row by id, optionally scoping by user_id.
        Returns number of deleted rows.
        """
        params = {
            "id": f"eq.{memory_id}"
        }
        if user_id:
            params["user_id"] = f"eq.{user_id}"

        url = f"{self.rest_base}/{self.table}"
        # Prefer header to return count
        headers = {**self._headers, "Prefer": "return=representation"}
        resp = requests.delete(url, headers=headers, params=params, timeout=self.timeout)
        if resp.status_code not in (200, 204):
            raise MemoryEngineError(f"delete_memory failed: {resp.status_code} {resp.text}")
        try:
            data = resp.json()
            return len(data)
        except Exception:
            # 204 No Content path
            return 0

    def upsert_memory(
        self,
        record: Dict[str, Any],
        on_conflict: str = "id"
    ) -> Dict[str, Any]:
        """
        Upsert a record. Ensure `record` contains the conflict key.
        """
        url = f"{self.rest_base}/{self.table}"
        headers = {
            **self._headers,
            "Prefer": "resolution=merge-duplicates,return=representation"
        }
        params = {"on_conflict": on_conflict}
        resp = requests.post(url, headers=headers, params=params, data=json.dumps(record), timeout=self.timeout)
        if not resp.ok:
            raise MemoryEngineError(f"upsert_memory failed: {resp.status_code} {resp.text}")
        data = resp.json()
        return data[0] if isinstance(data, list) and data else data


def get_memory_engine(
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    table: Optional[str] = None
) -> SupabaseMemoryAdapter:
    """
    Factory to instantiate the Supabase adapter from env vars or provided args.
    Env vars:
      - SUPABASE_URL
      - SUPABASE_API_KEY
      - SUPABASE_TABLE (optional, default memory_docs)
    """
    base_url = base_url or os.getenv("SUPABASE_URL")
    api_key = api_key or os.getenv("SUPABASE_API_KEY")
    table = table or os.getenv("SUPABASE_TABLE", "memory_docs")

    if not base_url or not api_key:
        raise MemoryEngineError("Missing SUPABASE_URL or SUPABASE_API_KEY.")

    return SupabaseMemoryAdapter(base_url=base_url, api_key=api_key, table=table)


# Quick self-test (optional): run `python memory_engine.py`
if __name__ == "__main__":
    # Provide env vars before running, or hardcode for a quick smoke test.
    engine = get_memory_engine()
    print("Connected to:", engine.rest_base)
