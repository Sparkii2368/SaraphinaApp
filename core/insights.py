# core/insights.py
"""
Phase 1.1: Self-narration with rolling baselines and anomaly history.

Enhancements now include:
- Severity weights baked in at source
- Always-present doctor interval
- First-seen timestamps persisted
- Pluggable anomaly detectors
- Machine-friendly output mode (via wrapper/CLI)
"""

from __future__ import annotations

import json
import os, sys, logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict, Literal

# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------
__all__ = ["InsightsEngine", "generate_trends_report", "Anomaly"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s]: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & Thresholds
# ---------------------------------------------------------------------------
EMA_ALPHA: float = float(os.getenv("SARAPHINA_BASELINE_ALPHA", "0.3"))
HEARTBEAT_GAP_HOURS = float(os.getenv("SARAPHINA_HEARTBEAT_GAP_HOURS", "2"))
ERROR_REPEAT_THRESHOLD = int(os.getenv("SARAPHINA_ERROR_REPEAT_THRESHOLD", "3"))
HEARTBEAT_DROP_RATIO = float(os.getenv("SARAPHINA_HEARTBEAT_DROP_RATIO", "0.30"))
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

    # --------------------------- Anomaly History ---------------------------
    def _load_anomaly_history(self) -> Dict[str, Any]:
        if not os.path.exists(self.anomaly_history):
            return {}
        try:
            with open(self.anomaly_history, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}

    def _save_anomaly_history(self, history: Dict[str, Any]) -> None:
        self._ensure_dir(self.anomaly_history)
        with open(self.anomaly_history, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)

    def _get_first_seen(self, anomaly_id: str) -> Optional[str]:
        return self._load_anomaly_history().get(anomaly_id, {}).get("first_seen")

    def _persist_first_seen(self, anomaly_id: str, ts: str) -> None:
        history = self._load_anomaly_history()
        history.setdefault(anomaly_id, {})["first_seen"] = ts
        self._save_anomaly_history(history)

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
        return sorted([t for t in times if t])

    @staticmethod
    def _last_event_time_iso(events: List[Dict[str, Any]], name: str) -> Optional[str]:
        for e in reversed(events):
            if (e.get("event") or "").lower() == name.lower():
                t = InsightsEngine._event_time(e)  # type: ignore
                return t.isoformat() if t else None
        return None

    def _count_in_window(self, events: List[Dict[str, Any]], name: str, window: timedelta) -> int:
        now = datetime.utcnow()
        return sum(
            1 for e in events
            if (e.get("event") or "").lower() == name.lower()
            and self._event_time(e) and (now - self._event_time(e)) <= window
        )



       # --------------------------- Anomalies ---------------------------------
    def check_heartbeat_gaps(self, events, baselines) -> list[dict]:
        anomalies = []
        hb_times = self._heartbeat_times(events)
        for prev, curr in zip(hb_times, hb_times[1:]):
            gap = (curr - prev).total_seconds() / 3600.0
            if gap > HEARTBEAT_GAP_HOURS:
                anomalies.append({
                    "type": "heartbeat_gap",
                    "message": f"Heartbeat gap of {gap:.1f} hours",
                    "severity": "warning" if gap < 12 else "critical"
                })
        return anomalies

    def check_repeated_errors(self, events, baselines) -> list[dict]:
        anomalies = []
        err_counts = Counter(e.get("message") for e in events if e.get("level") == "error")
        for msg, count in err_counts.items():
            if count >= ERROR_REPEAT_THRESHOLD:
                anomalies.append({
                    "type": "repeated_error",
                    "message": f"Error '{msg}' repeated {count} times in last 24h",
                    "severity": "critical" if count >= ERROR_REPEAT_THRESHOLD * 2 else "warning"
                })
        return anomalies

    def check_low_heartbeat_activity(self, events, baselines) -> list[dict]:
        anomalies = []
        count_24h = self._count_in_window(events, "heartbeat", timedelta(hours=24))
        hb_baseline = baselines.get("heartbeat_count_24h_baseline")
        if hb_baseline and count_24h < (1 - HEARTBEAT_DROP_RATIO) * hb_baseline:
            anomalies.append({
                "type": "low_heartbeat_activity",
                "message": f"Heartbeat count dropped to {count_24h} (baseline {hb_baseline:.1f})",
                "severity": "warning"
            })
        return anomalies

    def check_doctor_overdue(self, events, baselines) -> list[dict]:
        anomalies = []
        doctor_times = self._doctor_times(events)
        if doctor_times:
            last_doctor = doctor_times[-1]
            interval_h = (datetime.utcnow() - last_doctor).total_seconds() / 3600.0
            baseline_int = baselines.get("doctor_interval_hours_baseline")
            if interval_h > max(DOCTOR_MIN_INTERVAL_HOURS,
                                (baseline_int or 0) * DOCTOR_OVERDUE_FACTOR):
                anomalies.append({
                    "type": "doctor_overdue",
                    "message": f"Doctor event overdue: {interval_h:.1f} hours since last",
                    "severity": "critical"
                })
        return anomalies

    # Detector registry
    @property
    def detectors(self):
        return [
            self.check_heartbeat_gaps,
            self.check_repeated_errors,
            self.check_low_heartbeat_activity,
            self.check_doctor_overdue
        ]

    def detect_anomalies(self, events, baselines) -> list[dict]:
        anomalies = []
        for detector in self.detectors:
            anomalies.extend(detector(events, baselines))
        return anomalies

    # ------------------------ Trends Report --------------------------------
    def compute_doctor_interval(self, events) -> Optional[float]:
        doctor_times = self._doctor_times(events)
        if len(doctor_times) >= 2:
            return (doctor_times[-1] - doctor_times[-2]).total_seconds() / 3600.0
        elif len(doctor_times) == 1:
            return (datetime.utcnow() - doctor_times[-1]).total_seconds() / 3600.0
        return None

    def generate_trends_report(self) -> Dict[str, Any]:
        events = self._load_jsonl(self.runtime_log) + self._load_jsonl(self.autobio_log)
        baselines = self._load_baselines()
        doctor_interval = self.compute_doctor_interval(events)

        # Update baselines
        hb_count = self._count_in_window(events, "heartbeat", timedelta(hours=24))
        baselines = self.update_baselines(hb_count, doctor_interval)

        # Detect anomalies
        anomalies_structured = self.detect_anomalies(events, baselines)

        # Enrich anomalies
        severity_map = {"low": 0.2, "medium": 0.5, "high": 0.8,
                        "warning": 0.5, "critical": 1.0, "info": 0.1}
        now_iso = datetime.utcnow().isoformat()
        for anomaly in anomalies_structured:
            anomaly["severity_score"] = severity_map.get(anomaly.get("severity"), 0.0)
            anomaly["doctor_interval"] = anomaly.get("doctor_interval", doctor_interval)
            anomaly_id = f"{anomaly.get('type')}::{anomaly.get('message')}"
            stored = self._get_first_seen(anomaly_id)
            if stored:
                anomaly["first_seen"] = stored
            else:
                anomaly["first_seen"] = now_iso
                self._persist_first_seen(anomaly_id, now_iso)

        return {
            "timestamp": now_iso,
            "doctor_interval": doctor_interval,
            "anomalies_structured": anomalies_structured,
            "anomalies": [a["message"] for a in anomalies_structured]
        }


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


## ---------------------------------------------------------------------------
# Backward-compatible module-level API
# ---------------------------------------------------------------------------
from typing import Optional, Dict, Any
import os, sys, json, logging
from datetime import datetime

logger = logging.getLogger(__name__)

def generate_trends_report(
    runtime_log: Optional[str] = None,
    autobio_log: Optional[str] = None,
    baselines_path: Optional[str] = None,
    anomaly_history: Optional[str] = None,
    as_json: bool = False,
) -> Dict[str, Any]:
    """
    Generate a trends report from the InsightsEngine with added enrichments:
      - Numeric severity scores
      - Always-present doctor interval
      - First-seen timestamps for anomalies (if not already present)
    
    If as_json=True, returns machine-friendly JSON structure without decoration.
    """
    engine = InsightsEngine(
        runtime_log=runtime_log or os.getenv("SARAPHINA_RUNTIME_LOG", "logs/runtime.jsonl"),
        autobio_log=autobio_log or os.getenv("SARAPHINA_AUTOBIO_LOG", "logs/autobiography.jsonl"),
        baselines_path=baselines_path or os.getenv("SARAPHINA_BASELINES", "logs/baselines.json"),
        anomaly_history=anomaly_history or os.getenv("SARAPHINA_ANOMALY_HISTORY", "logs/anomaly_history.jsonl"),
    )

    report = engine.generate_trends_report()

    # -----------------------------------------------------------------------
    # Enrich anomalies
    # -----------------------------------------------------------------------
    severity_map = {"low": 0.2, "medium": 0.5, "high": 0.8, "critical": 1.0}
    now_iso = datetime.utcnow().isoformat()

    for item in report.get("anomalies", []):
        # Numeric severity score
        item["severity_score"] = severity_map.get(item.get("severity"), 0.0)

        # Ensure doctor_interval always present
        if "doctor_interval" not in item:
            item["doctor_interval"] = report.get("doctor_interval", None)

        # Inject first_seen if missing
        if "first_seen" not in item:
            item["first_seen"] = now_iso

    # Machine-friendly mode: return raw dict or CLI will handle printing
    if as_json:
        return report

    return report


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

    # Detect JSON output flag
    json_mode = "--json" in sys.argv

    report = generate_trends_report(as_json=json_mode)

    if json_mode:
        print(json.dumps(report, indent=2))
    else:
        logger.info("Trends Report:\n%s", json.dumps(report, indent=2))
