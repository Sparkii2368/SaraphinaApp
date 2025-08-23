# safety/compliance.py
from __future__ import annotations
import os
from typing import Dict, Any

# Bridge to your existing policy loader
try:
    import policy_loader  # local file
except Exception:
    policy_loader = None

POLICY_PATH = os.getenv("SARAPHINA_POLICY_PATH", "policy.yaml")

def policy_status() -> Dict[str, Any]:
    if policy_loader is None:
        return {"ok": False, "error": "policy_loader.py not importable"}
    try:
        policy = policy_loader.load_policy(POLICY_PATH)  # assumes your function name
        if not isinstance(policy, dict):
            return {"ok": False, "error": "policy not a dict", "path": POLICY_PATH}
        return {"ok": True, "path": POLICY_PATH, "rules": len(policy) if hasattr(policy, "__len__") else None}
    except Exception as e:
        return {"ok": False, "error": str(e), "path": POLICY_PATH}
