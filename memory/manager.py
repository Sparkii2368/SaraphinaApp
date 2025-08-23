# memory/manager.py
from __future__ import annotations
from typing import Dict, Any

def memory_status() -> Dict[str, Any]:
    try:
        import memory_engine  # your existing module
        # Optionally, probe something lightweight if available
        return {"ok": True, "engine": "memory_engine", "details": getattr(memory_engine, "__name__", "memory_engine")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
