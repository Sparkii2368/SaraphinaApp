"""
core/insights.py
Phase 1: Self‑narration and trend awareness.
Reads runtime + autobiography logs to generate trends and detect anomalies
without modifying any existing Phase 0 behavior.
"""

from __future__ import annotations
import json
import os
from datetime import datetime, timedelta
from collections import Counter
from typing import Dict, Any, List, Optional, Tuple

# Align with your repo paths (Phase 0 prints refer to logs/runtime.jsonl)
RUNTIME_LOG = os.getenv("SARAPHINA_RUNTIME_LOG", "logs/runtime.jsonl")
AUTOBIO_LOG = os.getenv("SARAPHINA_AUTOBIO_LOG", "logs/autobiography.jsonl")

# Optional rolling baselines snapshot (kept minimal for Phase 1)
BASELINES_PATH = os.getenv("SARAPHINA_BASELINES", "logs/baselines.json")


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    """Load a JSONL file into a list of dicts, newest last."""
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except Exception:
                # Treat malformed line as non-fatal
                continue
    return out


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp string to datetime (naive UTC)."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _event_time(e: Dict[str, Any]) -> Optional[datetime]:
    """Handle both 'timestamp' and 'time' keys defensively."""
    return _parse_timestamp(e.get("timestamp") or e.get("time"))


def summarize_autobiography(limit: int = 5) -> List[str]:
    """
    Return the most recent 'limit' autobiography entries as brief lines.
    """
    entries = _load_jsonl(AUTOBIO_LOG)
    entries.sort(key=lambda e: _event_time(e) or datetime.min, reverse=True)
    lines: List[str] = []
    for e in entries[:limit]:
        t = e.get("time") or e.get("timestamp") or ""
        ev = e.get("event") or ""
        details = e.get("details") or {}
        brief = details if isinstance(details, str) else (
            details.get("summary")
            if isinstance(details, dict) and "summary" in details
            else details
        )
        lines.append(f"{t} — {ev} {brief if brief else ''}".rstrip())
    return lines


def _heartbeat_times(events: List[Dict[str, Any]]) -> List[datetime]:
    times = []
    for e in events:
        if (e.get("event") == "heartbeat") or (e.get("event") == "smoke" and e.get("heartbeat")):
            t = _event_time(e)
            if t:
                times.append(t)
    times.sort()
    return times


def detect_anomalies(runtime_events: List[Dict[str, Any]]) -> List[str]:
    """
    Inspect runtime events for unusual gaps and repeated errors.
    Keeps logic intentionally simple for Phase 1.
    """
    anomalies: List[str] = []

    # Heartbeat gap detection (> 2h between beats)
    hb_times = _heartbeat_times(runtime_events)
    for prev, curr in zip(hb_times, hb_times[1:]):
        gap = curr - prev
        if gap > timedelta(hours=2):
            anomalies.append(f"Heartbeat gap {gap} between {prev.isoformat()} and {curr.isoformat()}")

    # Error repetition (same message > 3 times)
    error_events = [e for e in runtime_events if (e.get("level") == "error") or ("error" in (e.get("details") or {}))]
    if error_events:
        def _msg(e: Dict[str, Any]) -> str:
            if isinstance(e.get("details"), dict) and e["details"].get("error"):
                return str(e["details"]["error"])
            return str(e.get("message") or e.get("details") or "Unknown error")
        counts = Counter(_msg(e) for e in error_events)
        for msg, count in counts.items():
            if count > 3:
                anomalies.append(f"Repeated error '{msg}' x{count}")

    return anomalies


def _last_event_time(events: List[Dict[str, Any]], name: str) -> Optional[str]:
    for e in reversed(events):
        if e.get("event") == name:
            t = _event_time(e)
            return t.isoformat() if t else None
    return None


def _count_in_window(events: List[Dict[str, Any]], name: str, window: timedelta) -> int:
    now = datetime.utcnow()
    c = 0
    for e in events:
        if e.get("event") != name:
            continue
        t = _event_time(e)
        if t and now - t <= window:
            c += 1
    return c


def _load_baselines() -> Dict[str, Any]:
    if not os.path.exists(BASELINES_PATH):
        return {}
    try:
        with open(BASELINES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_baselines(data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(BASELINES_PATH), exist_ok=True)
    with open(BASELINES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def update_baselines(report: Dict[str, Any]) -> None:
    """
    Minimal rolling baselines for Phase 1: record last 24h heartbeat count.
    """
    baselines = _load_baselines()
    baselines["heartbeat_count_24h"] = report.get("heartbeat_count_24h", 0)
    baselines["last_updated"] = datetime.utcnow().isoformat()
    _save_baselines(baselines)


def generate_trends_report() -> Dict[str, Any]:
    """
    Build a structured report of operational trends from logs.
    """
    runtime_events = _load_jsonl(RUNTIME_LOG)

    last_heartbeat = _last_event_time(runtime_events, "heartbeat")
    last_doctor = _last_event_time(runtime_events, "doctor")

    hb_24h = _count_in_window(runtime_events, "heartbeat", timedelta(hours=24))
    anomalies = detect_anomalies(runtime_events)
    autobio = summarize_autobiography(limit=5)

    report: Dict[str, Any] = {
        "heartbeat_count_24h": hb_24h,
        "last_heartbeat": last_heartbeat,
        "last_doctor_run": last_doctor,
        "recent_autobiography": autobio,
        "anomalies": anomalies or ["None detected"]
    }

    # Update tiny rolling baseline snapshot
    try:
        update_baselines(report)
    except Exception:
        pass

    return report


if __name__ == "__main__":
    print(json.dumps(generate_trends_report(), indent=2))
