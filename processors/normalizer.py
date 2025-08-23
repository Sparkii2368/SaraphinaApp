# processors/normalizer.py
from __future__ import annotations
from typing import Dict, Any

def processor_status() -> Dict[str, Any]:
    return {"ok": True, "components": ["normalizer"], "version": "phase0"}
