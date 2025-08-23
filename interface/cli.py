# interface/cli.py
from __future__ import annotations
from typing import Dict, Any

def interface_status() -> Dict[str, Any]:
    return {"ok": True, "ui": ["cli", "streamlit_stub"], "version": "phase0"}
