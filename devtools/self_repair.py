# devtools/self_repair.py
from __future__ import annotations
import os
from typing import Dict, Any

REQUIRED_DIRS = [
    "core","memory","processors","embedding","reasoning","security",
    "network","sensory","interface","translation","learning","devtools",
    "safety","config","logs","tests","connectors","pages"
]

def doctor() -> Dict[str, Any]:
    missing = [d for d in REQUIRED_DIRS if not os.path.isdir(d)]
    writeable_logs = os.access(os.path.join("logs"), os.W_OK) if os.path.isdir("logs") else False
    return {
        "ok": len(missing) == 0 and writeable_logs,
        "missing_dirs": missing,
        "logs_writeable": writeable_logs,
    }
