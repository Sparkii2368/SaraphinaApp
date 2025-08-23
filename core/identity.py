# core/identity.py
from __future__ import annotations
import os
import socket
import uuid
import subprocess
import datetime as dt
import platform
from typing import Optional, Dict

APP_NAME = os.getenv("SARAPHINA_APP_NAME", "SaraphinaApp")
VERSION = os.getenv("SARAPHINA_VERSION", "0.0.1-phase0")
INSTANCE_ID = os.getenv("SARAPHINA_INSTANCE_ID", str(uuid.uuid4()))
HOSTNAME = socket.gethostname()
USER = os.getenv("USERNAME") or os.getenv("USER")
ORIGIN = "Sparkii"  # simplified origin

def _git(cmd: str) -> Optional[str]:
    """Run a git command and return its stripped output, or None if unavailable."""
    try:
        return subprocess.check_output(cmd.split(), stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None

def git_info() -> Dict[str, Optional[str]]:
    """Gather basic git repository state for provenance tracking."""
    return {
        "branch": _git("git rev-parse --abbrev-ref HEAD"),
        "commit": _git("git rev-parse HEAD"),
        "short": _git("git rev-parse --short HEAD"),
        "dirty": "yes" if _git("git status --porcelain") else "no",
    }

def now_iso() -> str:
    """Current UTC time in ISO 8601 format with timezone."""
    return dt.datetime.now(dt.UTC).isoformat()

def identity() -> Dict[str, str]:
    """Return Saraphina's identity manifest."""
    info = git_info()
    return {
        "app": APP_NAME,
        "version": VERSION,
        "instance_id": INSTANCE_ID,
        "hostname": HOSTNAME,
        "user": USER,
        "origin": ORIGIN,
        "platform": platform.platform(),
        "time": now_iso(),
        "git_branch": info.get("branch"),
        "git_commit": info.get("commit"),
        "git_short": info.get("short"),
        "git_dirty": info.get("dirty"),
    }

if __name__ == "__main__":
    import json
    print(json.dumps(identity(), indent=2))
