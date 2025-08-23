# embedding/adapter.py
from __future__ import annotations
from typing import Dict, Any

def embedding_status() -> Dict[str, Any]:
    return {"ok": True, "provider": "stub", "version": "phase0"}
