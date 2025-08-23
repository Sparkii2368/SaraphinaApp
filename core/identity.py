# core/identity.py
from __future__ import annotations
import os, socket, uuid, subprocess, datetime as dt
from typing import Optional, Dict

APP_NAME = os.getenv("SARAPHINA_APP_NAME", "SaraphinaApp")
VERSION = os.getenv("SARAPHINA_VERSION", "0.0.1-phase0")
INSTANCE_ID = os.getenv("SARAPHINA_INSTANCE_ID", str(uuid.uuid4()))
HOSTNAME = socket.gethostname()

def _git(cmd: str) -> Optional[str]:
    try:
        return subprocess.check_output(cmd.split(), stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return None

def git_info() -> Dict[str, Optional[str]]:
    return {
        "branch": _git("git rev-parse --abbrev-ref HEAD"),
        "commit": _git("git rev-parse HEAD"),
        "short": _git("git rev-parse --short HEAD"),
        "dirty": _git("git status --porcelain"),
    }

def now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

def identity() -> Dict[str, str]:
    info = git_info()
    return {
        "app": APP_NAME,
        "version": VERSION,
        "instance_id": INSTANCE_ID,
        "hostname": HOSTNAME,
        "time": now_iso(),
        "git_branch": info.get("branch"),
        "git_commit": info.get("commit"),
        "git_short": info.get("short"),
        "git_dirty": "yes" if info.get("dirty") else "no",
    }
