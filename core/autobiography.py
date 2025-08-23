# core/autobiography.py
"""
Saraphina Autobiography Module
Keeps a chronological, append-only journal of key events, milestones, and reflections.
Ready for future Phase 1 self-analysis.
"""

from __future__ import annotations
from datetime import datetime, UTC
from pathlib import Path
import json
from typing import List, Dict

# Journal lives alongside runtime logs
JOURNAL_FILE = Path(__file__).parent.parent / "logs" / "autobiography.jsonl"


def log_event(event: str, details: Dict | None = None) -> None:
    """
    Append a timestamped autobiographical entry.
    :param event: Short description of what happened.
    :param details: Optional dict with extra context (status, metrics, etc.).
    """
    entry = {
        "time": datetime.now(UTC).isoformat(),
        "event": event,
        "details": details or {},
    }
    JOURNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    with JOURNAL_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def recent_entries(limit: int = 5) -> List[Dict]:
    """
    Retrieve the most recent autobiographical entries.
    """
    if not JOURNAL_FILE.exists():
        return []
    with JOURNAL_FILE.open(encoding="utf-8") as f:
        lines = f.readlines()[-limit:]
    return [json.loads(line) for line in lines]


if __name__ == "__main__":
    # Example: mark Phase 0 completion
    log_event("Phase 0 foundation locked", {
        "status": "ok",
        "notes": "Identity, logging, CLI entrypoints complete"
    })
    print(json.dumps(recent_entries(), indent=2))
