# core/heartbeat.py
from __future__ import annotations
from typing import Dict, Any
from .identity import identity, now_iso
from .logging import log_event

def heartbeat(summary: Dict[str, Any]) -> Dict[str, Any]:
    event = {
        "time": now_iso(),
        "identity": identity(),
        "summary": summary,
    }
    log_event("heartbeat", event)
    return event
