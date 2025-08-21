#!/usr/bin/env python3
"""
memory_engine.py

Provides append-only JSONL logging of policy decisions with:
  - Rotation when file grows beyond a threshold
  - Timestamped, retained backups
  - Bounded, memory-safe tail reads handling arbitrarily long lines
  - Full read-all and CLI/test harness
  - Context echo to stdout if enabled

Environment variables:
  MEMORY_ENGINE_LOG_PATH         Path to JSONL file (default: ./policy_decisions.jsonl)
  MEMORY_ENGINE_MAX_LOG_SIZE     Max bytes before rotation (default: 10 MB)
  MEMORY_ENGINE_BACKUP_RETENTION How many rotated backups to keep (default: 5)
  MEMORY_ENGINE_STDOUT           If “1”/“true”, echo each decision to stdout
"""
import os
import sys
import json
import socket
import logging
import argparse
import threading

from pathlib import Path
from datetime import datetime, timezone
from collections import deque
from typing import Any, Dict, List, Optional

# ---- Configuration ----
LOG_PATH = Path(os.getenv("MEMORY_ENGINE_LOG_PATH", "policy_decisions.jsonl")).expanduser()
MAX_LOG_SIZE = int(os.getenv("MEMORY_ENGINE_MAX_LOG_SIZE", str(10 * 1024 * 1024)))
BACKUP_RETENTION = int(os.getenv("MEMORY_ENGINE_BACKUP_RETENTION", "5"))
ECHO_STDOUT = os.getenv("MEMORY_ENGINE_STDOUT", "").lower() in ("1", "true", "yes")

# Ensure parent directory exists
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# Serialization lock
_LOCK = threading.Lock()

# Module logger
logger = logging.getLogger("memory_engine")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ---- Helpers ----
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False)
    except Exception:
        return json.dumps(str(data), ensure_ascii=False)

def _coerce_meta(meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if meta is None:
        return {}
    if isinstance(meta, dict):
        try:
            json.dumps(meta)
            return meta
        except Exception:
            return {k: str(v) for k, v in meta.items()}
    return {"_meta": str(meta)}

# ---- Public API ----
def log_policy_decision(
    actor: str,
    target: str,
    action: str,
    decision: str,
    reason: str,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Append a policy decision to the JSONL log.

    Parameters
    ----------
    actor : str        Who made the decision (e.g. "ingest_worker")
    target : str       Subject of the decision (URL, doc_id, etc.)
    action : str       Action type ("ingest", "fetch", ...)
    decision : str     Outcome label ("ALLOW", "DENY_*", ...)
    reason : str       Human-readable rationale
    meta : dict, opt   Additional context

    Returns
    -------
    bool
        True on success, False on I/O failure
    """
    record: Dict[str, Any] = {
        "ts":       _now_iso(),
        "actor":    actor,
        "target":   target,
        "action":   action,
        "decision": decision,
        "reason":   reason,
        "meta":     _coerce_meta(meta),
        "host":     socket.gethostname(),
        "pid":      os.getpid(),
    }

    # Optional echo to stdout
    if ECHO_STDOUT:
        try:
            logger.info(
                "policy_decision actor=%s target=%s action=%s decision=%s reason=%s meta=%s",
                actor,
                target,
                action,
                decision,
                reason,
                _safe_json_dumps(record["meta"]),
            )
        except Exception:
            pass

    line = _safe_json_dumps(record)
    # Write under lock, then rotate
    try:
        with _LOCK, open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logger.warning("memory_engine: write failed: %s", e)
        return False

    _rotate_if_needed()
    return True

def read_policy_decisions() -> List[Dict[str, Any]]:
    """
    Read all policy decision records from the JSONL log.
    """
    if not LOG_PATH.exists():
        return []

    out: List[Dict[str, Any]] = []
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    out.append(json.loads(raw))
                except Exception:
                    logger.debug("Skipping malformed JSON in read_all")
    except Exception as e:
        logger.error("Failed to read policy decisions: %s", e)
    return out

def tail_policy_decisions(n: int = 100) -> List[Dict[str, Any]]:
    """
    Return the last `n` records from the JSONL log in order oldest→newest.

    Memory-bounded and handles arbitrarily long lines across chunk boundaries.
    """
    if not LOG_PATH.exists():
        return []

    results: deque[Dict[str, Any]] = deque(maxlen=n)
    try:
        file_size = LOG_PATH.stat().st_size
        chunk = 8192
        buffer = b""
        pos = file_size

        with open(LOG_PATH, "rb") as f:
            while pos > 0 and len(results) < n:
                read_size = min(chunk, pos)
                pos -= read_size
                f.seek(pos)
                data = f.read(read_size)
                buffer = data + buffer

                lines = buffer.split(b"\n")
                buffer = lines[0]  # partial or complete

                for raw in reversed(lines[1:]):
                    if not raw:
                        continue
                    try:
                        text = raw.decode("utf-8", errors="replace")
                        results.appendleft(json.loads(text))
                        if len(results) >= n:
                            break
                    except Exception:
                        continue

                # If we saw no newline, grow chunk to capture long lines
                if b"\n" not in data and pos > 0:
                    chunk = min(chunk * 2, file_size)

            # leftover buffer
            if buffer and len(results) < n:
                try:
                    rec = json.loads(buffer.decode("utf-8", errors="replace"))
                    results.appendleft(rec)
                except Exception:
                    pass

    except Exception as e:
        logger.error("Failed to tail policy decisions: %s", e)

    return list(results)

# ---- Rotation & Backup Pruning ----
def _rotate_if_needed() -> None:
    """
    Rotate the log file if it exceeds MAX_LOG_SIZE, then prune old backups.
    """
    try:
        if not LOG_PATH.exists() or LOG_PATH.stat().st_size <= MAX_LOG_SIZE:
            return

        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        backup = LOG_PATH.with_name(f"{LOG_PATH.stem}.{stamp}.jsonl")
        os.replace(LOG_PATH, backup)
        logger.info("Rotated policy log to %s", backup.name)
        _prune_backups()
    except Exception as e:
        logger.error("Error rotating log: %s", e)

def _prune_backups() -> None:
    """
    Keep only BACKUP_RETENTION newest rotated backups.
    """
    try:
        pattern = f"{LOG_PATH.stem}.*.jsonl"
        all_b = sorted(
            LOG_PATH.parent.glob(pattern),
            key=lambda p: p.name,
            reverse=True
        )
        for old in all_b[BACKUP_RETENTION:]:
            try:
                old.unlink()
                logger.info("Deleted old backup %s", old.name)
            except Exception:
                logger.debug("Failed to delete backup %s", old.name)
    except Exception as e:
        logger.error("Error pruning backups: %s", e)

# ---- CLI / Test Harness ----
def _cli() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="memory_engine utility")
    subs = p.add_subparsers(dest="cmd")

    subs.add_parser("read", help="Read all policy decisions")

    tailp = subs.add_parser("tail", help="Tail last N policy decisions")
    tailp.add_argument("-n", type=int, default=100)

    subs.add_parser("prune", help="Prune old backups")

    wp = subs.add_parser("write", help="Write a policy decision by fields")
    wp.add_argument("actor")
    wp.add_argument("target")
    wp.add_argument("action")
    wp.add_argument("decision")
    wp.add_argument("reason")
    wp.add_argument("--meta", default="{}", help="JSON string for meta")

    args = p.parse_args()

    if args.cmd == "read":
        for r in read_policy_decisions():
            print(json.dumps(r, separators=(",", ":")))
    elif args.cmd == "tail":
        for r in tail_policy_decisions(args.n):
            print(json.dumps(r, separators=(",", ":")))
    elif args.cmd == "prune":
        _prune_backups()
    elif args.cmd == "write":
        try:
            meta = json.loads(args.meta)
        except Exception:
            meta = {}
        log_policy_decision(
            actor=args.actor,
            target=args.target,
            action=args.action,
            decision=args.decision,
            reason=args.reason,
            meta=meta,
        )
        print("Wrote policy decision.")
    else:
        p.print_help()

if __name__ == "__main__":
    _cli()
