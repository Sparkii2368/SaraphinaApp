# core/insights.py
"""
Phase 1.1: Self-narration with rolling baselines and anomaly history.

Enhancements:
- Encapsulated into InsightsEngine class
- Stronger typing & docstrings
- Centralized thresholds
- Safer JSON handling
- Detect anomalies vs old baseline before updating
- Backward-compatible module-level generate_trends_report shim
- Synthetic anomaly injection + CLI utility
- Clear synthetic anomalies utility
"""

from __future__ import annotations

import json
import importlib
logging = importlib.import_module("logging")

import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict, Literal

# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------
__all__ = ["InsightsEngine", "generate_trends_report", "Anomaly"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s]: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & Thresholds (env overrides for tunability)
# ---------------------------------------------------------------------------
EMA_ALPHA: float = float(os.getenv("SARAPHINA_BASELINE_ALPHA", "0.3"))
HEARTBEAT_GAP_HOURS = float(os.getenv("SARAPHINA_HEARTBEAT_GAP_HOURS", "2"))
ERROR_REPEAT_THRESHOLD = int(os.getenv("SARAPHINA_ERROR_REPEAT_THRESHOLD", "3"))
HEARTBEAT_DROP_RATIO = float(os.getenv("SARAPHINA_HEARTBEAT_DROP_RATIO", "0.30"))  # >= 30% drop vs baseline
DOCTOR_OVERDUE_FACTOR = float(os.getenv("SARAPHINA_DOCTOR_OVERDUE_FACTOR", "2.0"))
DOCTOR_MIN_INTERVAL_HOURS = float(os.getenv("SARAPHINA_DOCTOR_MIN_INTERVAL_HOURS", "8.0"))

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
class Anomaly(TypedDict):
    type: Literal[
        "heartbeat_gap",
        "repeated_error",
        "low_heartbeat_activity",
        "doctor_overdue",
        "system_error",
        "unknown",
    ]
    message: str
    severity: Literal["info", "warning", "critical"]


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class InsightsEngine:
    """
    Reads runtime + autobiography logs, computes rolling baselines (EMA),
    detects anomalies, persists anomaly history, and produces a trends report.
    """

    def __init__(
        self,
        runtime_log: str = os.getenv("SARAPHINA_RUNTIME_LOG", "logs/runtime.jsonl"),
        autobio_log: str = os.getenv("SARAPHINA_AUTOBIO_LOG", "logs/autobiography.jsonl"),
        baselines_path: str = os.getenv("SARAPHINA_BASELINES", "logs/baselines.json"),
        anomaly_history: str = os.getenv("SARAPHINA_ANOMALY_HISTORY", "logs/anomaly_history.jsonl"),
    ):
        self.runtime_log = runtime_log
        self.autobio_log = autobio_log
        self.baselines_path = baselines_path
        self.anomaly_history = anomaly_history

    # --------------------------- File Helpers ------------------------------

    @staticmethod
    def _ensure_dir(path: str) -> None:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)

    def _load_jsonl(self, path: str) -> List[Dict[str, Any]]:
        """
        Load JSONL (oldest first). Malformed lines are skipped with a warning.
        """
        if not os.path.exists(path):
            return []
        out: List[Dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                s = line.strip()
                if not s:
                    continue
                try:
                    out.append(json.loads(s))
                except json.JSONDecodeError as e:
                    logger.warning("Skipping malformed JSON in %s line %d: %s", path, lineno, e)
        return out

    def _append_jsonl(self, path: str, obj: Dict[str, Any]) -> None:
        self._ensure_dir(path)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # --------------------------- Time Helpers ------------------------------

    @staticmethod
    def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
        """
        Parse ISO-8601 into UTC-naive datetime.
        Returns None on failure.
        """
        if not ts:
            return None
        try:
            s = ts
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    @classmethod
    def _event_time(cls, e: Dict[str, Any]) -> Optional[datetime]:
        return cls._parse_timestamp(e.get("timestamp") or e.get("time"))

    # --------------------------- Baselines ---------------------------------

    def _load_baselines(self) -> Dict[str, Any]:
        if not os.path.exists(self.baselines_path):
            return {}
        try:
            with open(self.baselines_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("Failed to load baselines from %s: %s", self.baselines_path, e)
            return {}

    def _save_baselines(self, data: Dict[str, Any]) -> None:
        self._ensure_dir(self.baselines_path)
        with open(self.baselines_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def _ema_update(old: Optional[float], current: Optional[float], alpha: float = EMA_ALPHA) -> Optional[float]:
        if current is None:
            return old
        if old is None:
            return current
        return round(alpha * current + (1 - alpha) * old, 6)

    def update_baselines(self, heartbeat_24h: int, doctor_interval_hours: Optional[float]) -> Dict[str, Any]:
        """
        Update EMA baselines for heartbeat count (24h) and doctor interval (hours).
        Returns the updated baselines dict.
        """
        baselines = self._load_baselines()
        baselines["heartbeat_count_24h_baseline"] = self._ema_update(
            baselines.get("heartbeat_count_24h_baseline"), float(heartbeat_24h)
        )
        if doctor_interval_hours is not None:
            baselines["doctor_interval_hours_baseline"] = self._ema_update(
                baselines.get("doctor_interval_hours_baseline"), float(doctor_interval_hours)
            )
        baselines["last_updated"] = datetime.utcnow().isoformat()
        self._save_baselines(baselines)
        return baselines

    # ------------------------ Event Extraction -----------------------------

    def _heartbeat_times(self, events: List[Dict[str, Any]]) -> List[datetime]:
        times: List[datetime] = []
        for e in events:
            ev = (e.get("event") or "").lower()
            if ev == "heartbeat" or (ev == "smoke" and bool(e.get("heartbeat"))):
                t = self._event_time(e)
                if t:
                    times.append(t)
        times.sort()
        return times

    def _doctor_times(self, events: List[Dict[str, Any]]) -> List[datetime]:
        times = [self._event_time(e) for e in events if (e.get("event") or "").lower() == "doctor"]
        times = [t for t in times if t]
        times.sort()
        return times

    @staticmethod
    def _last_event_time_iso(events: List[Dict[str, Any]], name: str) -> Optional[str]:
        for e in reversed(events):
            if (e.get("event") or "").lower() == name.lower():
                t = InsightsEngine._event_time(e)  # type: ignore[arg-type]
                return t.isoformat() if t else None
        return None

    def _count_in_window(self, events: List[Dict[str, Any]], name: str, window: timedelta) -> int:
        now = datetime.utcnow()
        c = 0
        for e in events:
            if (e.get("event") or "").lower() != name.lower():
                continue
            t = self._event_time(e)
            if t and (now - t) <= window:
                c += 1
        return c

    # --------------------------- Anomalies ---------------------------------

    def detect_anomalies(self, events: List[Dict[str, Any]], baselines: Dict[str, Any]) -> List[Anomaly]:
        """
        Inspect events for anomalies. Returns structured anomalies.
        """
        anomalies: List[Anomaly] = []

        # 1) heartbeat gaps
        hb_times = self._heartbeat_times(events)
        for prev, curr in zip(hb_times, hb_times[1:]):
            if curr - prev > timedelta(hours=HEARTBEAT_GAP_HOURS):
                anomalies.append({
                    "type": "heartbeat_gap",
                    "message": f"Gap {curr - prev} between {prev.isoformat()} and {curr.isoformat()}",
                    "severity": "critical",
                })

        # 2) repeated errors
        error_events = [
            e for e in events
            if (e.get("level") or "").lower() == "error"
            or (isinstance(e.get("details"), dict) and e["details"].get("error"))
        ]
        if error_events:
            def _msg(e: Dict[str, Any]) -> str:
                d = e.get("details")
                if isinstance(d, dict) and d.get("error"):
                    return str(d["error"])
                return str(e.get("message") or d or "Unknown error")

            counts = Counter(_msg(e) for e in error_events)
            for msg, count in counts.items():
                if count > ERROR_REPEAT_THRESHOLD:
                    anomalies.append({
                        "type": "repeated_error",
                        "message": f"'{msg}' seen {count} times",
                        "severity": "warning",
                    })

        # 3) low heartbeat activity vs baseline
        hb_24h = self._count_in_window(events, "heartbeat", timedelta(hours=24))
        hb_baseline = baselines.get("heartbeat_count_24h_baseline")
        if hb_baseline and hb_baseline > 0:
            delta_ratio = (hb_24h - hb_baseline) / hb_baseline
            if delta_ratio <= -HEARTBEAT_DROP_RATIO:
                anomalies.append({
                    "type": "low_heartbeat_activity",
                    "message": f"Heartbeat 24h {hb_24h} vs baseline {hb_baseline:.2f} ({delta_ratio:.0%})",
                    "severity": "warning",
                })

        # 4) doctor overdue vs baseline
        dts = self._doctor_times(events)
        if len(dts) >= 2:
            interval_hours = (dts[-1] - dts[-2]).total_seconds() / 3600.0
            doc_baseline = baselines.get("doctor_interval_hours_baseline")
            if doc_baseline and doc_baseline > 0:
                if interval_hours > max(DOCTOR_MIN_INTERVAL_HOURS, DOCTOR_OVERDUE_FACTOR * doc_baseline):
                    anomalies.append({
                        "type": "doctor_overdue",
                        "message": f"Doctor interval {interval_hours:.2f}h vs baseline {doc_baseline:.2f}h",
                        "severity": "critical",
                    })

        return anomalies

    # --------------------------- Public API --------------------------------

    def summarize_autobiography(self, limit: int = 5) -> List[str]:
        entries = self._load_jsonl(self.autobio_log)
        entries.sort(key=lambda e: self._event_time(e) or datetime.min, reverse=True)
        lines: List[str] = []
        for e in entries[:limit]:
            t = e.get("time") or e.get("timestamp") or ""
            ev = e.get("event") or ""
            details = e.get("details")
            brief = (
                details if isinstance(details, str)
                else (details.get("summary") if isinstance(details, dict) and "summary" in details else str(details or ""))
            )
            lines.append(f"{t} — {ev} {brief}".strip())
        return lines

    def get_recent_anomaly_stats(self, window_hours: int = 6) -> Dict[str, int]:
        events = self._load_jsonl(self.anomaly_history)
        cutoff = datetime.utcnow() - timedelta(hours=window_hours)
        counts = Counter()
        for e in events:
            t = self._parse_timestamp(e.get("time"))
            if t and t >= cutoff:
                counts[e.get("type", "unknown")] += 1
        return dict(counts)

    def generate_trends_report(self) -> Dict[str, Any]:
        runtime_events = self._load_jsonl(self.runtime_log)

        last_heartbeat = self._last_event_time_iso(runtime_events, "heartbeat")
        last_doctor = self._last_event_time_iso(runtime_events, "doctor")
        hb_24h = self._count_in_window(runtime_events, "heartbeat", timedelta(hours=24))

        # doctor interval
        doc_interval_hours: Optional[float] = None
        dts = self._doctor_times(runtime_events)
        if len(dts) >= 2:
            doc_interval_hours = (dts[-1] - dts[-2]).total_seconds() / 3600.0

        # detect anomalies
        baselines_before = self._load_baselines()
        anomalies_struct = self.detect_anomalies(runtime_events, baselines_before)

        # persist anomalies
        if anomalies_struct:
            now_iso = datetime.utcnow().isoformat()
            for a in anomalies_struct:
                self._append_jsonl(self.anomaly_history, {"time": now_iso, **a})

        # update baselines
        baselines_after = self.update_baselines(hb_24h, doc_interval_hours)

        autobio = self.summarize_autobiography(limit=5)
        recent_anomaly_stats = self.get_recent_anomaly_stats(window_hours=6)
        anomalies_text = [a["message"] for a in anomalies_struct] if anomalies_struct else ["None detected"]

        return {
            "heartbeat_count_24h": hb_24h,
            "last_heartbeat": last_heartbeat,
            "last_doctor_run": last_doctor,
            "recent_autobiography": autobio,
            "anomalies": anomalies_text,
            "anomalies_structured": anomalies_struct,
            "baselines": {
                "heartbeat_count_24h_baseline": baselines_after.get("heartbeat_count_24h_baseline"),
                "doctor_interval_hours_baseline": baselines_after.get("doctor_interval_hours_baseline"),
                "last_updated": baselines_after.get("last_updated"),
            },
            "recent_anomaly_counts_6h": recent_anomaly_stats,
        }

    # --------------------------- Test / Dev Utilities -------------------------

    def inject_synthetic_anomalies(
        self,
        anomalies: Optional[List[Dict[str, str]]] = None,
        count: int = 3
    ) -> None:
        """
        Append synthetic anomalies directly to anomaly_history.jsonl
        in the correct schema (time, type, severity, message).
        """
        self._ensure_dir(self.anomaly_history)

        now_iso = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

        if anomalies is None:
            anomalies = [
                {
                    "time": now_iso,
                    "type": "system_error",
                    "severity": "critical",
                    "message": "Synthetic anomaly injection for Phase 1.1 validation",
                },
                {
                    "time": now_iso,
                    "type": "heartbeat_gap",
                    "severity": "critical",
                    "message": "Simulated missed heartbeat > 2x baseline",
                },
                {
                    "time": now_iso,
                    "type": "doctor_overdue",
                    "severity": "critical",
                    "message": "Forced overdue doctor run trigger",
                },
            ][:count]

        for ev in anomalies:
            self._append_jsonl(self.anomaly_history, ev)

        logger.info("Injected %d synthetic anomalies at %s", len(anomalies), now_iso)

    def clear_synthetic_anomalies(self) -> None:
        """
        Remove any anomalies tagged as synthetic from anomaly_history.jsonl.
        """
        entries = self._load_jsonl(self.anomaly_history)
        filtered = [e for e in entries if not e.get("message", "").startswith("Synthetic anomaly injection")]
        with open(self.anomaly_history, "w", encoding="utf-8") as f:
            for e in filtered:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        logger.info("Cleared %d synthetic anomalies", len(entries) - len(filtered))


# ---------------------------------------------------------------------------
# Backward-compatible module-level API
# ---------------------------------------------------------------------------
def generate_trends_report(
    runtime_log: Optional[str] = None,
    autobio_log: Optional[str] = None,
    baselines_path: Optional[str] = None,
    anomaly_history: Optional[str] = None,
) -> Dict[str, Any]:
    engine = InsightsEngine(
        runtime_log=runtime_log or os.getenv("SARAPHINA_RUNTIME_LOG", "logs/runtime.jsonl"),
        autobio_log=autobio_log or os.getenv("SARAPHINA_AUTOBIO_LOG", "logs/autobiography.jsonl"),
        baselines_path=baselines_path or os.getenv("SARAPHINA_BASELINES", "logs/baselines.json"),
        anomaly_history=anomaly_history or os.getenv("SARAPHINA_ANOMALY_HISTORY", "logs/anomaly_history.jsonl"),
    )
    return engine.generate_trends_report()


# ---------------------------------------------------------------------------
# CLI utility
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if "--inject-anomalies" in sys.argv:
        InsightsEngine().inject_synthetic_anomalies()
        sys.exit(0)

    if "--clear-synthetic-anomalies" in sys.argv:
        InsightsEngine().clear_synthetic_anomalies()
        sys.exit(0)

    report = generate_trends_report()
    logger.info("Trends Report:\n%s", json.dumps(report, indent=2))
