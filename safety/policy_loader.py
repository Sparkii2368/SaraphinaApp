# policy_loader.py
import yaml, os, time
from typing import Dict, Any

_POLICY_CACHE: Dict[str, Any] = {}
_POLICY_MTIME = 0
_POLICY_PATH = os.path.join(os.path.dirname(__file__), "policy.yaml")

def load_policy(force_reload=False) -> Dict[str, Any]:
    global _POLICY_CACHE, _POLICY_MTIME
    try:
        mtime = os.path.getmtime(_POLICY_PATH)
        if force_reload or mtime != _POLICY_MTIME:
            with open(_POLICY_PATH, "r", encoding="utf-8") as f:
                _POLICY_CACHE = yaml.safe_load(f)
            _POLICY_MTIME = mtime
        return _POLICY_CACHE
    except Exception as e:
        raise RuntimeError(f"Failed to load policy.yaml: {e}")

def get_policy_version() -> str:
    policy = load_policy()
    return str(policy.get("policyversion", "unknown"))
