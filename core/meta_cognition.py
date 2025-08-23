"""
core/meta_cognition.py
Phase 1: Confidence scoring and audit triggers (minimal, additive).
Consumes runtime logs (and insights) to score recent actions and
produce an audit report with recommendations.
"""

from __future__ import annotations
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from core.insights import (
    generate_trends_report,
    _load_jsonl as _load_jsonl_insights,   # reuse safe loader
    _event_time as _event_time_insights,   # reuse time parsing
)

RUNTIME_LOG = os.getenv("SARAPHINA_RUNTIME_LOG", "logs/runtime.jsonl")


def _score_event(e: Dict[str, Any]) -> float:
    """
    Heuristic confidence scoring for Phase 1.
    Later phases may replace with calibrated models.
    """
    ev = str(e.get("event") or "").lower()
    details = e.get("details") or {}
    level = str(e.get("level") or "").lower()

    # Base scores by event
    base = {
        "heartbeat": 0.98,
        "status": 0.95,
        "doctor": 1.00 if (isinstance(details, dict) and details.get("diagnostics") == "all green") else 0.85,
    }.get(ev, 0.90)

    # Penalties
    penalty = 0.0
    if level == "error":
        penalty += 0.3
    if isinstance(details, dict) and details.get("error"):
        penalty += 0.25
    if isinstance(details, dict) and details.get("retries", 0) > 0:
        penalty += min(0.2, 0.05 * int(details.get("retries", 0)))

    score = max(0.0, min(1.0, base - penalty))
    return score


def _window(events: List[Dict[str, Any]], hours: int = 24) -> List[Dict[str, Any]]:
    now = datetime.utcnow()
    out = []
    for e in events:
        t = _event_time_insights(e)
        if t and now - t <= timedelta(hours=hours):
            out.append(e)
    return out


def _average(xs: List[float]) -> Optional[float]:
    return round(sum(xs) / len(xs), 3) if xs else None


def _per_event_confidence(events: List[Dict[str, Any]]) -> Dict[str, float]:
    buckets: Dict[str, List[float]] = {}
    for e in events:
        ev = str(e.get("event") or "").lower()
        buckets.setdefault(ev, []).append(_score_event(e))
    return {k: _average(v) for k, v in buckets.items() if v}


def generate_audit_report(threshold: float = 0.9) -> Dict[str, Any]:
    """
    Combine confidence analysis with insights anomalies to produce
    a compact audit report and suggested next actions.
    """
    events = _load_jsonl_insights(RUNTIME_LOG)
    recent = _window(events, hours=24)

    per_event = _per_event_confidence(recent)
    all_scores = [s for s in per_event.values() if s is not None]
    avg_conf = _average(all_scores) if all_scores else None

    insights = generate_trends_report()
    anomalies = [a for a in insights.get("anomalies", []) if a != "None detected"]

    triggers: List[str] = []
    recs: List[str] = []

    if avg_conf is not None and avg_conf < threshold:
        triggers.append(f"Average confidence {avg_conf} below threshold {threshold}")
        recs.append("Run --doctor for deeper diagnostics and increase logging verbosity temporarily")

    if anomalies:
        triggers.append(f"Detected {len(anomalies)} anomaly(ies)")
        recs.append("Inspect heartbeat schedule and recent error messages; consider network/process stability checks")

    # If no explicit trigger, still provide a healthy summary
    if not triggers and avg_conf is not None and avg_conf >= threshold:
        recs.append("Confidence within normal range; no immediate action required")

    report: Dict[str, Any] = {
        "average_confidence_24h": avg_conf,
        "per_event_confidence_24h": per_event,
        "anomalies": anomalies,
        "triggers": triggers,
        "recommendations": recs,
    }
    return report


if __name__ == "__main__":
    print(json.dumps(generate_audit_report(), indent=2))
