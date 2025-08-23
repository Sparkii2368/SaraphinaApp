"""
Meta-Cognition Engine — Phase 1.4

Purpose:
- Consume InsightsEngine as single source of health/trends.
- Compute blended health: anomaly health + confidence-in-decision (trend/history).
- Severity-scaled confidence penalty curve (replaces flat cap).
- Determine mode with hysteresis (normal, degraded, safe_mode).
- Narrate (concise/causal/both) and include adaptive prevalence and deltas.
- Dispatch pluggable actions via registry; simulate by default.
- Auto-queue self-patch items on degraded/safe_mode with trigger/priority and error_trace when actions repeatedly fail.
- Adaptive weighting (optional) to dampen alert fatigue.
- Debug subcommand to dump raw calculations (anomaly health, confidence base/penalty, blended, weights, prevalence, deltas).

Drop-in file: core/meta_cognition.py
Requires: core/insights.py (Phase 1.1+)
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import traceback
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from math import exp
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Protocol, runtime_checkable, Mapping

from core.insights import InsightsEngine

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
_LOG_LEVEL = os.getenv("SARAPHINA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(levelname)s [%(name)s]: %(message)s",
)
logger = logging.getLogger("meta_cognition")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def ensure_dir(path: Path) -> None:
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True)

def append_jsonl(path: Path, obj: Mapping[str, Any]) -> None:
    ensure_dir(path)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def read_json(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.debug("read_json(%s) failed: %s", path, e)
    return {}

def write_json(path: Path, data: Dict[str, Any]) -> None:
    try:
        ensure_dir(path)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        logger.error("write_json(%s) failed: %s", path, e)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    # Health thresholds
    soft_threshold: float = float(os.getenv("SARAPHINA_HEALTH_SOFT_THRESHOLD", "80"))
    hard_threshold: float = float(os.getenv("SARAPHINA_HEALTH_HARD_THRESHOLD", "50"))
    hysteresis_gap: float = float(os.getenv("SARAPHINA_MODE_HYSTERESIS", "5"))

    # Confidence blending and penalty curve
    confidence_weight: float = float(os.getenv("SARAPHINA_CONFIDENCE_WEIGHT", "0.2"))  # 0..1
    confidence_history_limit: int = int(os.getenv("SARAPHINA_CONFIDENCE_HISTORY", "200"))
    min_actions_for_confidence: int = int(os.getenv("SARAPHINA_MIN_ACTIONS_FOR_CONFIDENCE", "5"))
    conf_penalty_max: float = float(os.getenv("SARAPHINA_CONF_PENALTY_MAX", "0.35"))   # max confidence penalty
    conf_penalty_k: float = float(os.getenv("SARAPHINA_CONF_PENALTY_K", "0.004"))      # severity curve steepness

    # Anomaly weights (base)
    weight_heartbeat_gap: float = float(os.getenv("SARAPHINA_WEIGHT_HEARTBEAT_GAP", "25"))
    weight_repeated_error: float = float(os.getenv("SARAPHINA_WEIGHT_REPEATED_ERROR", "15"))
    weight_low_heartbeat: float = float(os.getenv("SARAPHINA_WEIGHT_LOW_HEARTBEAT", "20"))
    weight_doctor_overdue: float = float(os.getenv("SARAPHINA_WEIGHT_DOCTOR_OVERDUE", "25"))
    weight_unknown: float = float(os.getenv("SARAPHINA_WEIGHT_UNKNOWN", "10"))

    # Scoring window
    window_hours: int = int(os.getenv("SARAPHINA_META_WINDOW_HOURS", "6"))

    # Paths
    autobio_log: Path = Path(os.getenv("SARAPHINA_AUTOBIO_LOG", "logs/autobiography.jsonl"))
    state_path: Path = Path(os.getenv("SARAPHINA_META_STATE", "logs/meta_state.json"))
    action_history: Path = Path(os.getenv("SARAPHINA_ACTION_HISTORY", "logs/action_history.jsonl"))
    patch_queue: Path = Path(os.getenv("SARAPHINA_PATCH_QUEUE", "logs/self_patch_queue.jsonl"))
    adaptive_state: Path = Path(os.getenv("SARAPHINA_ADAPTIVE_STATE", "logs/adaptive_state.json"))

    # Narration
    default_narration_style: str = os.getenv("SARAPHINA_NARRATION_STYLE", "concise")  # concise|causal|both

    # Actions/plugins
    plugins: str = os.getenv("SARAPHINA_ACTION_PLUGINS", "").strip()
    enable_default_actions: bool = os.getenv("SARAPHINA_ENABLE_DEFAULT_ACTIONS", "1") not in ("0", "false", "False")

    # Adaptive weighting
    adaptive_enable: bool = os.getenv("SARAPHINA_ADAPTIVE_ENABLE", "0") in ("1", "true", "True")
    adaptive_sensitivity: float = float(os.getenv("SARAPHINA_ADAPTIVE_SENSITIVITY", "0.15"))

    # Self-patch enrichment
    patch_fail_window: int = int(os.getenv("SARAPHINA_PATCH_FAIL_WINDOW", "50"))  # lookback N action records
    patch_fail_threshold: int = int(os.getenv("SARAPHINA_PATCH_FAIL_THRESHOLD", "3"))

    def weights(self) -> Dict[str, float]:
        return {
            "heartbeat_gap": self.weight_heartbeat_gap,
            "repeated_error": self.weight_repeated_error,
            "low_heartbeat_activity": self.weight_low_heartbeat,
            "doctor_overdue": self.weight_doctor_overdue,
            "unknown": self.weight_unknown,
        }

    def validate(self) -> None:
        if not (0 <= self.hard_threshold <= self.soft_threshold <= 100):
            raise ValueError("Thresholds must satisfy: 0 <= hard <= soft <= 100")
        if not (0.0 <= self.confidence_weight <= 1.0):
            raise ValueError("confidence_weight must be in [0,1]")
        if self.window_hours <= 0:
            raise ValueError("window_hours must be positive")
        if self.default_narration_style not in ("concise", "causal", "both"):
            raise ValueError("default_narration_style must be one of: concise, causal, both")
        for k, v in self.weights().items():
            if v < 0:
                raise ValueError(f"Weight for {k} must be non-negative")

# ---------------------------------------------------------------------
# Types & Registry
# ---------------------------------------------------------------------
@runtime_checkable
class Action(Protocol):
    def __call__(self, context: Dict[str, Any]) -> Tuple[str, bool]:
        """
        context includes: {"simulate": bool, "assessment": dict, "trends": dict}
        returns: (action_name, success)
        """

@dataclass
class HealthAssessment:
    time: str
    health_score: float            # blended 0..100
    anomaly_health: float          # anomaly-only 0..100
    confidence: float              # 0..1
    mode: str
    window_hours: int
    anomaly_counts_window: Dict[str, int]
    trends: Dict[str, Any]
    mode_reason: str = ""
    suggestions: List[str] = field(default_factory=list)
    adaptive_deltas: Optional[Dict[str, float]] = None
    adaptive_prevalence: Optional[Dict[str, float]] = None  # normalized 0..1 prevalence for audit

@dataclass
class NarrationEntry:
    time: str
    event: str = "narration"
    details: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ActionResult:
    action: str
    success: bool
    error: Optional[str] = None
    tb: Optional[str] = None

class ActionRegistry:
    def __init__(self) -> None:
        self._by_mode: Dict[str, List[Action]] = {"normal": [], "degraded": [], "safe_mode": []}

    def register(self, mode: str, action: Action) -> None:
        self._by_mode.setdefault(mode, []).append(action)

    def for_mode(self, mode: str) -> List[Action]:
        return list(self._by_mode.get(mode, []))

ACTIONS = ActionRegistry()

def action(mode: str) -> Any:
    def decorator(fn: Action) -> Action:
        ACTIONS.register(mode, fn)
        return fn
    return decorator

# ---------------------------------------------------------------------
# Default Actions (simulated by default)
# ---------------------------------------------------------------------
def _log_action(name: str, level: int = logging.WARNING) -> Action:
    def _fn(context: Dict[str, Any]) -> Tuple[str, bool]:
        simulate = context.get("simulate", True)
        logger.log(level, "Action: %s (simulated=%s)", name, simulate)
        return name, True
    return _fn

def restart_minor_services(context: Dict[str, Any]) -> Tuple[str, bool]:
    return _log_action("restart_minor_services")(context)

def halt_non_essentials(context: Dict[str, Any]) -> Tuple[str, bool]:
    return _log_action("halt_non_essentials")(context)

def alert_operator(context: Dict[str, Any]) -> Tuple[str, bool]:
    return _log_action("alert_operator", level=logging.ERROR)(context)

def _load_plugins(config: Config) -> None:
    if config.enable_default_actions:
        ACTIONS.register("degraded", restart_minor_services)
        ACTIONS.register("safe_mode", halt_non_essentials)
        ACTIONS.register("safe_mode", alert_operator)
    if not config.plugins:
        return
    for mod in [m.strip() for m in config.plugins.split(",") if m.strip()]:
        try:
            importlib.import_module(mod)
            logger.info("Loaded action plugin: %s", mod)
        except Exception as e:
            logger.error("Failed to load plugin %s: %s", mod, e)

# ---------------------------------------------------------------------
# Confidence components and severity curve
# ---------------------------------------------------------------------
def calc_severity(counts: Dict[str, int], weights: Dict[str, float]) -> float:
    return sum(weights.get(k, weights["unknown"]) * float(v) for k, v in counts.items())

def conf_penalty_from_severity(severity: float, k: float, max_penalty: float) -> float:
    # Monotonic, smooth, asymptotic curve: penalty = max*(1 - exp(-k*severity))
    penalty = max_penalty * (1.0 - exp(-k * max(0.0, severity)))
    return clamp(penalty, 0.0, max_penalty)

def read_action_history(path: Path, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    rows.append(json.loads(s))
                except Exception:
                    continue
    except Exception as e:
        logger.debug("read_action_history failed: %s", e)
    return rows[-limit:]

def blended_confidence_components(trends: Dict[str, Any], counts: Dict[str, int], cfg: Config) -> Dict[str, Any]:
    # Trend-based
    trend_conf: Optional[float] = None
    if isinstance(trends.get("success_rate"), (int, float)):
        val = float(trends["success_rate"])
        trend_conf = (val / 100.0) if val > 1.0 else val
    elif isinstance(trends.get("action_outcomes"), list):
        outs = [o for o in trends["action_outcomes"] if isinstance(o, dict) and "success" in o]
        if outs:
            trend_conf = sum(1 for o in outs if o.get("success") is True) / len(outs)

    # History-based
    hist_rows = read_action_history(cfg.action_history, cfg.confidence_history_limit)
    if hist_rows and len(hist_rows) >= cfg.min_actions_for_confidence:
        successes = sum(1 for r in hist_rows if r.get("success") is True)
        hist_conf = successes / len(hist_rows)
    else:
        hist_conf = 1.0  # optimistic bootstrap

    base_conf = 0.7 * trend_conf + 0.3 * hist_conf if trend_conf is not None else hist_conf

    weights = cfg.weights()
    severity = calc_severity(counts, weights)
    penalty = conf_penalty_from_severity(severity, cfg.conf_penalty_k, cfg.conf_penalty_max)

    final_conf = clamp(base_conf - penalty, 0.0, 1.0)
    return {
        "trend_conf": trend_conf,
        "hist_conf": hist_conf,
        "base_conf": base_conf,
        "severity": severity,
        "penalty": penalty,
        "final_conf": final_conf,
    }

# ---------------------------------------------------------------------
# Adaptive weights (EMA prevalence; normalized prevalence; deltas)
# ---------------------------------------------------------------------
def adapt_weights(
    base: Dict[str, float],
    counts: Dict[str, int],
    sensitivity: float,
    state_path: Path,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
    if sensitivity <= 0:
        return base, {k: 0.0 for k in base}, {k: 0.0 for k in base}

    state = read_json(state_path)
    prev = state.get("prevalence", {})
    alpha = 0.3

    prevalence_raw: Dict[str, float] = {}
    for k in base:
        curr = float(counts.get(k, 0))
        old = float(prev.get(k, 0.0))
        prevalence_raw[k] = round(alpha * curr + (1 - alpha) * old, 6)

    max_prev = max(prevalence_raw.values(), default=0.0)
    prevalence_norm: Dict[str, float] = {k: (v / max_prev if max_prev > 0 else 0.0) for k, v in prevalence_raw.items()}

    adjusted: Dict[str, float] = {}
    deltas: Dict[str, float] = {}
    for k, w in base.items():
        p = prevalence_norm[k]  # 0..1
        factor = 1.0 - (p * sensitivity) + ((1.0 - p) * sensitivity * 0.2)
        new_w = max(0.0, round(w * factor, 3))
        adjusted[k] = new_w
        deltas[k] = round(new_w - w, 3)

    write_json(state_path, {"updated": iso_z(utcnow()), "prevalence": prevalence_raw, "weights": adjusted})
    return adjusted, deltas, prevalence_norm

# ---------------------------------------------------------------------
# Scoring & Mode (with hysteresis)
# ---------------------------------------------------------------------
def anomaly_health_score(counts: Dict[str, int], weights: Dict[str, float]) -> float:
    score = 100.0
    for t, c in counts.items():
        score -= weights.get(t, weights.get("unknown", 10.0)) * float(c)
    return clamp(score, 0.0, 100.0)

def blended_health(anom_health: float, confidence: float, conf_weight: float) -> float:
    return clamp((1 - conf_weight) * anom_health + conf_weight * (confidence * 100.0), 0.0, 100.0)

def _determine_mode_simple(score: float, cfg: Config) -> str:
    if score < cfg.hard_threshold:
        return "safe_mode"
    if score < cfg.soft_threshold:
        return "degraded"
    return "normal"

def determine_mode(score: float, cfg: Config, last_mode: Optional[str]) -> Tuple[str, str]:
    desired = _determine_mode_simple(score, cfg)
    if cfg.hysteresis_gap <= 0 or not last_mode or last_mode == desired:
        return desired, "no_hysteresis" if cfg.hysteresis_gap <= 0 else "no_change"

    soft_up = min(100.0, cfg.soft_threshold + cfg.hysteresis_gap)
    hard_up = min(100.0, cfg.hard_threshold + cfg.hysteresis_gap)

    reason = "hysteresis_applied"
    if last_mode == "safe_mode":
        if score >= soft_up:
            return "normal", reason
        if score >= hard_up:
            return "degraded", reason
        return "safe_mode", reason
    if last_mode == "degraded":
        if score >= soft_up:
            return "normal", reason
        if score < cfg.hard_threshold:
            return "safe_mode", "downgrade_immediate"
        return "degraded", reason
    if last_mode == "normal":
        if score < cfg.hard_threshold:
            return "safe_mode", "downgrade_immediate"
        if score < cfg.soft_threshold:
            return "degraded", "downgrade_immediate"
        return "normal", reason
    return desired, "unknown_last_mode"

# ---------------------------------------------------------------------
# Suggestions & Causal Narration
# ---------------------------------------------------------------------
def suggestions_from_counts(counts: Dict[str, int], cfg: Config) -> List[str]:
    mapping = {
        "heartbeat_gap": "Check heartbeat emitter/scheduler; consider watchdog restart.",
        "repeated_error": "Inspect error traces; rollback or patch failing component.",
        "low_heartbeat_activity": "Investigate ingestion/backpressure; scale upstream if needed.",
        "doctor_overdue": "Run health doctor and audit scheduler/cron jobs.",
    }
    scored: List[Tuple[str, float]] = []
    for k, v in counts.items():
        if v > 0 and k in mapping:
            w = cfg.weights().get(k, cfg.weight_unknown)
            scored.append((mapping[k], w * v))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in scored] or (["Review Insights anomalies and prioritize highest-weighted items."]
                                      if sum(counts.values()) > 0 else [])

def compose_causal_text(
    score: float,
    mode: str,
    window_hours: int,
    counts: Dict[str, int],
    trends: Dict[str, Any],
    confidence: float,
    suggestions: List[str],
) -> str:
    top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
    top_str = ", ".join(f"{k}×{v}" for k, v in top) if top else "none"
    hints: List[str] = []
    if counts.get("heartbeat_gap"):
        hints.append("missed heartbeats → scheduler/network pause")
    if counts.get("repeated_error"):
        hints.append("repeating errors → persistent failure or bad input")
    if counts.get("low_heartbeat_activity"):
        hints.append("reduced activity → ingestion/backpressure")
    if counts.get("doctor_overdue"):
        hints.append("doctor overdue → scheduler drift")
    last_doc = trends.get("last_doctor_run")
    last_hb = trends.get("last_heartbeat")
    if last_doc:
        hints.append(f"last doctor at {last_doc}")
    if last_hb:
        hints.append(f"last heartbeat at {last_hb}")
    cause = "; ".join(hints) if hints else "no dominant causal signal"
    next_step = suggestions[0] if suggestions else "No suggested next step."
    return (
        f"Health {score:.0f} — {mode} (conf={confidence:.2f}). "
        f"Last {window_hours}h anomalies: {top_str}. Likely causes: {cause}. Next: {next_step}"
    )

def format_narration_text(
    score: float,
    mode: str,
    window_hours: int,
    counts: Dict[str, int],
    recent_anoms: List[str],
    style: str,
    trends: Dict[str, Any],
    confidence: float,
    suggestions: List[str],
) -> str:
    if style not in ("concise", "causal", "both"):
        style = "concise"

    if not counts:
        concise = f"Health {score:.0f} — {mode}. No anomalies in the last {window_hours}h. (conf={confidence:.2f})"
    else:
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
        top_parts = ", ".join(f"{k}×{v}" for k, v in top)
        recent_preview = "; ".join(recent_anoms[:2]) if recent_anoms else "n/a"
        concise = (
            f"Health {score:.0f} — {mode}. Last {window_hours}h anomalies: {top_parts}. "
            f"Recent: {recent_preview} (conf={confidence:.2f})"
        )

    if style == "concise":
        return concise

    causal = compose_causal_text(score, mode, window_hours, counts, trends, confidence, suggestions)
    if style == "causal":
        return causal

    return f"{concise} | {causal}"

# ---------------------------------------------------------------------
# Action failure summary (for self-patch enrichment)
# ---------------------------------------------------------------------
def recent_failure_summary(history: List[Dict[str, Any]], threshold: int) -> Optional[Dict[str, Any]]:
    # Aggregate failures by action; keep last error/tb seen
    fails: Dict[str, Dict[str, Any]] = {}
    for rec in history:
        if not isinstance(rec, dict):
            continue
        name = str(rec.get("action") or "unknown")
        success = bool(rec.get("success", False))
        if success:
            continue
        d = fails.setdefault(name, {"count": 0, "last_error": None, "last_tb": None})
        d["count"] += 1
        d["last_error"] = rec.get("error") or d["last_error"]
        d["last_tb"] = rec.get("tb") or d["last_tb"]
    if not fails:
        return None
    # pick top failing action by count
    top_name, info = max(fails.items(), key=lambda kv: kv[1]["count"])
    if info["count"] >= threshold:
        return {"action": top_name, "count": info["count"], "error": info["last_error"], "tb": info["last_tb"]}
    return None

# ---------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------
class MetaCognition:
    def __init__(self, config: Optional[Config] = None, insights: Optional[InsightsEngine] = None) -> None:
        self.config = config or Config()
        self.config.validate()
        self.insights = insights or InsightsEngine()
        _load_plugins(self.config)

    def assess(self, window_hours: Optional[int] = None) -> HealthAssessment:
        assessment, _ = self.assess_debug(window_hours=window_hours)
        return assessment

    def assess_debug(self, window_hours: Optional[int] = None) -> Tuple[HealthAssessment, Dict[str, Any]]:
        cfg = self.config
        win = int(window_hours or cfg.window_hours)

        # Pull current trends (may persist anomalies)
        trends = self.insights.generate_trends_report()

        # Windowed anomaly counts
        counts = self.insights.get_recent_anomaly_stats(window_hours=win)

        # Adaptive weights and prevalence
        base_weights = cfg.weights()
        adj_weights, deltas, prevalence = adapt_weights(
            base_weights, counts, cfg.adaptive_sensitivity if cfg.adaptive_enable else 0.0, cfg.adaptive_state
        )

        # Scores
        a_health = anomaly_health_score(counts, adj_weights)
        conf_parts = blended_confidence_components(trends, counts, cfg)
        conf_final = conf_parts["final_conf"]
        blended = blended_health(a_health, conf_final, cfg.confidence_weight)

        # Mode with hysteresis
        state = read_json(cfg.state_path)
        last_mode = state.get("last_mode")
        mode, reason = determine_mode(blended, cfg, last_mode)
        write_json(cfg.state_path, {"last_mode": mode, "time": iso_z(utcnow())})

        # Suggestions
        suggestions = suggestions_from_counts(counts, cfg)

        assessment = HealthAssessment(
            time=iso_z(utcnow()),
            health_score=round(blended, 2),
            anomaly_health=round(a_health, 2),
            confidence=round(conf_final, 4),
            mode=mode,
            window_hours=win,
            anomaly_counts_window=counts,
            trends=trends,
            mode_reason=reason if last_mode != mode else "no_change",
            suggestions=suggestions,
            adaptive_deltas=deltas if cfg.adaptive_enable else None,
            adaptive_prevalence=prevalence if cfg.adaptive_enable else None,
        )

        debug: Dict[str, Any] = {
            "window_hours": win,
            "counts": counts,
            "base_weights": base_weights,
            "adjusted_weights": adj_weights,
            "adaptive_deltas": deltas if cfg.adaptive_enable else {},
            "adaptive_prevalence": prevalence if cfg.adaptive_enable else {},
            "anomaly_health": a_health,
            "confidence_components": conf_parts,
            "confidence_weight": cfg.confidence_weight,
            "blended_score": blended,
            "thresholds": {"soft": cfg.soft_threshold, "hard": cfg.hard_threshold, "hysteresis_gap": cfg.hysteresis_gap},
            "mode_eval": {"last_mode": last_mode, "new_mode": mode, "reason": assessment.mode_reason},
        }
        return assessment, debug

    def narrate(self, assessment: HealthAssessment, append: bool = True, style: Optional[str] = None) -> NarrationEntry:
        cfg = self.config
        style_final = style or cfg.default_narration_style

        recent_msgs: List[str] = []
        if isinstance(assessment.trends.get("anomalies"), list):
            recent_msgs = [str(m) for m in assessment.trends["anomalies"] if isinstance(m, str)]

        text = format_narration_text(
            score=assessment.health_score,
            mode=assessment.mode,
            window_hours=assessment.window_hours,
            counts=assessment.anomaly_counts_window,
            recent_anoms=recent_msgs,
            style=style_final,
            trends=assessment.trends,
            confidence=assessment.confidence,
            suggestions=assessment.suggestions,
        )

        details: Dict[str, Any] = {
            "summary": text,
            "mode": assessment.mode,
            "score": assessment.health_score,
            "anomaly_health": assessment.anomaly_health,
            "confidence": assessment.confidence,
            "window_hours": assessment.window_hours,
            "anomaly_counts": assessment.anomaly_counts_window,
            "mode_reason": assessment.mode_reason,
            "suggestions": assessment.suggestions,
            "narration_style": style_final,
        }
        if assessment.adaptive_deltas is not None:
            details["adaptive_deltas"] = assessment.adaptive_deltas
        if assessment.adaptive_prevalence is not None:
            details["adaptive_prevalence"] = assessment.adaptive_prevalence

        entry = NarrationEntry(time=assessment.time, details=details)

        if append:
            append_jsonl(cfg.autobio_log, asdict(entry))
            logger.info("Narration appended to %s", cfg.autobio_log)

        return entry

    def _queue_self_patch(self, assessment: HealthAssessment) -> None:
        cfg = self.config
        if not cfg.patch_queue:
            return
        if assessment.mode not in ("degraded", "safe_mode"):
            return

        # Include error_trace if repeated action failures detected in recent history
        hist = read_action_history(cfg.action_history, cfg.patch_fail_window)
        fail_summary = recent_failure_summary(hist, cfg.patch_fail_threshold)

        item: Dict[str, Any] = {
            "time": assessment.time,
            "trigger": assessment.mode_reason or "mode_change",
            "mode": assessment.mode,
            "priority": 2 if assessment.mode == "safe_mode" else 1,
            "score": assessment.health_score,
            "anomaly_health": assessment.anomaly_health,
            "confidence": assessment.confidence,
            "top_anomalies": sorted(assessment.anomaly_counts_window.items(), key=lambda kv: kv[1], reverse=True)[:3],
            "suggestions": assessment.suggestions,
            "context": {
                "window_hours": assessment.window_hours,
                "pointers": {
                    "last_doctor_run": assessment.trends.get("last_doctor_run"),
                    "last_heartbeat": assessment.trends.get("last_heartbeat"),
                },
                "adaptive_deltas": assessment.adaptive_deltas or {},
                "adaptive_prevalence": assessment.adaptive_prevalence or {},
            },
        }
        if fail_summary:
            item["error_trace"] = {
                "action": fail_summary["action"],
                "fail_count": fail_summary["count"],
                "error": fail_summary.get("error"),
                "traceback": fail_summary.get("tb"),
            }

        append_jsonl(cfg.patch_queue, item)
        logger.warning("Queued self-patch recommendation -> %s", cfg.patch_queue)

    def respond(self, assessment: HealthAssessment, execute: bool = False) -> List[ActionResult]:
        ctx = {"assessment": asdict(assessment), "trends": assessment.trends, "simulate": not execute}
        results: List[ActionResult] = []

        for act in ACTIONS.for_mode(assessment.mode):
            try:
                name, ok = act(ctx)
                results.append(ActionResult(action=name, success=bool(ok)))
            except Exception as e:
                tb = traceback.format_exc()
                logger.error("Action exception: %s", e)
                results.append(ActionResult(action=getattr(act, "__name__", "action"), success=False, error=str(e), tb=tb))

        # Record outcomes (including errors) for confidence loop
        if results:
            now = iso_z(utcnow())
            for r in results:
                append_jsonl(self.config.action_history, {"time": now, **asdict(r)})

        # Queue self-patch if unhealthy
        self._queue_self_patch(assessment)

        if results:
            logger.info("Dispatched %d action(s) for mode=%s (execute=%s)", len(results), assessment.mode, execute)
        else:
            logger.info("No actions registered for mode=%s", assessment.mode)
        return results

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Saraphina Meta-Cognition — Phase 1.4")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--window-hours", type=int, default=None, help="Scoring window in hours (override config).")
        sp.add_argument("--quiet", action="store_true", help="Compact JSON output (for machines).")
        sp.add_argument("--out", type=Path, help="Write JSON output to file.")
        sp.add_argument("--narration-style", choices=["concise", "causal", "both"], help="Override narration style.")

    sp_status = sub.add_parser("status", help="Show mode, score, and top anomalies.")
    add_common(sp_status)

    sp_assess = sub.add_parser("assess", help="Run full assessment and print JSON.")
    add_common(sp_assess)

    sp_narr = sub.add_parser("narrate", help="Assess and append narration.")
    add_common(sp_narr)

    sp_resp = sub.add_parser("respond", help="Assess, narrate, and dispatch actions.")
    add_common(sp_resp)
    sp_resp.add_argument("--execute", action="store_true", help="Execute actions (default simulates).")

    sp_debug = sub.add_parser("debug", help="Dump raw calculations for diagnosis.")
    add_common(sp_debug)

    return p

def _print_or_write(obj: Any, out: Optional[Path], quiet: bool) -> None:
    data = json.dumps(obj, indent=None if quiet else 2)
    if out:
        ensure_dir(out)
        out.write_text(data, encoding="utf-8")
    else:
        print(data)

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    cfg = Config()
    # Allow runtime overrides
    if args.window_hours is not None or args.narration_style:
        cfg = Config(
            soft_threshold=cfg.soft_threshold,
            hard_threshold=cfg.hard_threshold,
            hysteresis_gap=cfg.hysteresis_gap,
            confidence_weight=cfg.confidence_weight,
            confidence_history_limit=cfg.confidence_history_limit,
            min_actions_for_confidence=cfg.min_actions_for_confidence,
            conf_penalty_max=cfg.conf_penalty_max,
            conf_penalty_k=cfg.conf_penalty_k,
            weight_heartbeat_gap=cfg.weight_heartbeat_gap,
            weight_repeated_error=cfg.weight_repeated_error,
            weight_low_heartbeat=cfg.weight_low_heartbeat,
            weight_doctor_overdue=cfg.weight_doctor_overdue,
            weight_unknown=cfg.weight_unknown,
            window_hours=args.window_hours if args.window_hours is not None else cfg.window_hours,
            autobio_log=cfg.autobio_log,
            state_path=cfg.state_path,
            action_history=cfg.action_history,
            patch_queue=cfg.patch_queue,
            adaptive_state=cfg.adaptive_state,
            default_narration_style=args.narration_style or cfg.default_narration_style,
            plugins=cfg.plugins,
            enable_default_actions=cfg.enable_default_actions,
            adaptive_enable=cfg.adaptive_enable,
            adaptive_sensitivity=cfg.adaptive_sensitivity,
            patch_fail_window=cfg.patch_fail_window,
            patch_fail_threshold=cfg.patch_fail_threshold,
        )

    engine = MetaCognition(config=cfg)

    # Exit codes: 0=normal, 1=degraded, 2=safe_mode, 3=unexpected
    exit_map = {"normal": 0, "degraded": 1, "safe_mode": 2}

    if args.cmd == "status":
        a = engine.assess(window_hours=args.window_hours)
        recent_msgs = [str(m) for m in a.trends.get("anomalies", []) if isinstance(m, str)]
        text = format_narration_text(
            score=a.health_score,
            mode=a.mode,
            window_hours=a.window_hours,
            counts=a.anomaly_counts_window,
            recent_anoms=recent_msgs,
            style=cfg.default_narration_style,
            trends=a.trends,
            confidence=a.confidence,
            suggestions=a.suggestions,
        )
        if args.quiet:
            _print_or_write({"mode": a.mode, "score": a.health_score, "confidence": a.confidence}, args.out, True)
        else:
            print(text)
        raise SystemExit(exit_map.get(a.mode, 3))

    if args.cmd == "assess":
        a = engine.assess(window_hours=args.window_hours)
        _print_or_write(asdict(a), args.out, args.quiet)
        raise SystemExit(exit_map.get(a.mode, 3))

    if args.cmd == "narrate":
        a = engine.assess(window_hours=args.window_hours)
        entry = engine.narrate(a, append=True, style=args.narration_style)
        _print_or_write(asdict(entry), args.out, args.quiet)
        raise SystemExit(exit_map.get(a.mode, 3))

    if args.cmd == "respond":
        a = engine.assess(window_hours=args.window_hours)
        entry = engine.narrate(a, append=True, style=args.narration_style)
        results = [asdict(r) for r in engine.respond(a, execute=getattr(args, "execute", False))]
        payload = {"assessment": asdict(a), "narration": asdict(entry), "actions": results}
        _print_or_write(payload, args.out, args.quiet)
        raise SystemExit(exit_map.get(a.mode, 3))

    if args.cmd == "debug":
        a, dbg = engine.assess_debug(window_hours=args.window_hours)
        out = {
            "assessment": asdict(a),
            "debug": dbg,
        }
        _print_or_write(out, args.out, args.quiet)
        raise SystemExit(exit_map.get(a.mode, 3))

if __name__ == "__main__":
    main()
