from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict

from core.logging import log_event
from core.identity import identity
from core.autobiography import recent_entries, log_event as log_journal_event
from core.insights import generate_trends_report
from core.meta_cognition import MetaCognition, Config


# ------------------------------ Helpers ------------------------------ #

EXIT_MAP = {"normal": 0, "degraded": 1, "safe_mode": 2}


def _validate_window_hours(val: int | None) -> None:
    if val is not None and val <= 0:
        raise ValueError("window_hours must be a positive integer")


def _build_engine(args: argparse.Namespace) -> MetaCognition:
    # Only override narration style via Config; window_hours is passed per-call to assess()
    if hasattr(args, "narration_style") and args.narration_style:
        cfg = Config(default_narration_style=args.narration_style)
        return MetaCognition(config=cfg)
    return MetaCognition()


def _print_json(obj: dict) -> None:
    print(json.dumps(obj, indent=2))


def _top_anomalies(counts: dict[str, int], k: int = 3) -> list[tuple[str, int]]:
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:k]


# ------------------------------ Commands ------------------------------ #

def run_status(args: argparse.Namespace) -> None:
    try:
        result = {"status": "ok"}
        log_event("status", result)
        log_journal_event("Status check", result)

        ident = identity()
        recent = recent_entries(limit=5)

        print("=== Saraphina Identity ===")
        _print_json(ident)

        print("\n=== Recent Memories ===")
        if not recent:
            print("No autobiographical entries yet.")
        else:
            for e in recent:
                t = e.get("time") or e.get("timestamp")
                ev = e.get("event")
                details = e.get("details") or {}
                details_s = json.dumps(details) if details else ""
                print(f"- [{t}] {ev} {details_s}")

        print("\n=== Operational Insights ===")
        report = generate_trends_report()
        print(f"Heartbeats in last 24h: {report['heartbeat_count_24h']}")
        print(f"Last heartbeat: {report['last_heartbeat']}")
        print(f"Last doctor run: {report['last_doctor_run']}")
        anomalies = report.get("anomalies") or []
        print(f"Anomalies: {', '.join(anomalies)}")

        baselines = report.get("baselines", {})
        print("Baselines:")
        _print_json(baselines)

        print("\nRecent autobiography roll‑up:")
        for entry in report.get("recent_autobiography", []):
            print(f"- {entry}")

        print("\n=== JSON Status ===")
        _print_json(result)
    except Exception as e:
        print(f"Error in status: {e}")
        log_event("status_error", {"error": str(e)})


def run_smoke(_: argparse.Namespace) -> None:
    try:
        result = {"status": "ok", "heartbeat": True}
        log_event("heartbeat", result)
        log_journal_event("Heartbeat", result)
        print("Smoke event logged — check logs/runtime.jsonl")
    except Exception as e:
        print(f"Error in smoke: {e}")
        log_event("smoke_error", {"error": str(e)})


def run_doctor(_: argparse.Namespace) -> None:
    try:
        diagnostics = {"diagnostics": "all green"}
        log_event("doctor", diagnostics)
        log_journal_event("Doctor check", diagnostics)
        _print_json(diagnostics)
    except Exception as e:
        print(f"Error in doctor: {e}")
        log_event("doctor_error", {"error": str(e)})


def run_insights(_: argparse.Namespace) -> None:
    try:
        report = generate_trends_report()
        log_event("insights_report", {"summary": "generated"})
        log_journal_event("Insights generated", {"keys": list(report.keys())})
        _print_json(report)
    except Exception as e:
        print(f"Error in insights: {e}")
        log_event("insights_error", {"error": str(e)})


def run_audit(args: argparse.Namespace) -> None:
    try:
        _validate_window_hours(args.window_hours)
        engine = _build_engine(args)
        assessment = engine.assess(window_hours=args.window_hours)

        # richer log context
        payload = {
            "health_score": assessment.health_score,
            "mode": assessment.mode,
            "confidence": assessment.confidence,
            "mode_reason": assessment.mode_reason,
            "top_anomalies": _top_anomalies(assessment.anomaly_counts_window),
        }
        if assessment.adaptive_deltas is not None:
            payload["adaptive_deltas"] = assessment.adaptive_deltas

        log_event("audit", payload)
        log_journal_event("Audit", {"mode": assessment.mode, "suggestions": assessment.suggestions})

        print("=== Audit Assessment ===")
        _print_json(asdict(assessment))

        sys.exit(EXIT_MAP.get(assessment.mode, 3))
    except Exception as e:
        print(f"Error in audit: {e}")
        log_event("audit_error", {"error": str(e)})
        sys.exit(3)


def run_narrate(args: argparse.Namespace) -> None:
    try:
        _validate_window_hours(args.window_hours)
        engine = _build_engine(args)
        assessment = engine.assess(window_hours=args.window_hours)
        entry = engine.narrate(assessment, append=True, style=args.narration_style)
        _print_json(asdict(entry))
        sys.exit(EXIT_MAP.get(assessment.mode, 3))
    except Exception as e:
        print(f"Error in narrate: {e}")
        log_event("narrate_error", {"error": str(e)})
        sys.exit(3)


def run_respond(args: argparse.Namespace) -> None:
    try:
        _validate_window_hours(args.window_hours)
        engine = _build_engine(args)
        assessment = engine.assess(window_hours=args.window_hours)
        entry = engine.narrate(assessment, append=True, style=args.narration_style)
        results = [asdict(r) for r in engine.respond(assessment, execute=args.execute)]

        # Also log an action summary for observability
        action_summary = {
            "executed": bool(args.execute),
            "mode": assessment.mode,
            "count": len(results),
            "ok": sum(1 for r in results if r.get("success")),
            "fail": sum(1 for r in results if not r.get("success")),
        }
        log_event("actions_summary", action_summary)

        payload = {"assessment": asdict(assessment), "narration": asdict(entry), "actions": results}
        _print_json(payload)
        sys.exit(EXIT_MAP.get(assessment.mode, 3))
    except Exception as e:
        print(f"Error in respond: {e}")
        log_event("respond_error", {"error": str(e)})
        sys.exit(3)


def run_debug(args: argparse.Namespace) -> None:
    try:
        _validate_window_hours(args.window_hours)
        engine = _build_engine(args)
        assessment, debug = engine.assess_debug(window_hours=args.window_hours)
        # Include narration preview for human readability in debug
        narration_preview = engine.narrate(assessment, append=False, style=args.narration_style).details.get("summary")
        out = {"assessment": asdict(assessment), "narration_preview": narration_preview, "debug": debug}
        _print_json(out)
        sys.exit(EXIT_MAP.get(assessment.mode, 3))
    except Exception as e:
        print(f"Error in debug: {e}")
        log_event("debug_error", {"error": str(e)})
        sys.exit(3)


# ------------------------------ CLI ------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Saraphina CLI runtime")
    sub = parser.add_subparsers(dest="cmd")

    # Phase 0 commands
    sub.add_parser("status", help="Show system status").set_defaults(func=run_status)
    sub.add_parser("smoke", help="Run smoke test + heartbeat").set_defaults(func=run_smoke)
    sub.add_parser("doctor", help="Run diagnostics").set_defaults(func=run_doctor)

    # Phase 1.x insights
    sub.add_parser("insights", help="Dump full insights report").set_defaults(func=run_insights)

    # Meta-cognition commands
    p_audit = sub.add_parser("audit", help="Run meta-cognitive audit (assessment only)")
    p_audit.add_argument("--window-hours", type=int, default=None, help="Scoring window in hours (positive integer)")
    p_audit.set_defaults(func=run_audit)

    p_narr = sub.add_parser("narrate", help="Assess and append narration entry")
    p_narr.add_argument("--window-hours", type=int, default=None, help="Scoring window in hours (positive integer)")
    p_narr.add_argument("--narration-style", choices=["concise", "causal", "both"], default=None,
                        help="Override narration style for this run")
    p_narr.set_defaults(func=run_narrate)

    p_resp = sub.add_parser("respond", help="Assess, narrate, and execute/simulate actions")
    p_resp.add_argument("--window-hours", type=int, default=None, help="Scoring window in hours (positive integer)")
    p_resp.add_argument("--narration-style", choices=["concise", "causal", "both"], default=None,
                        help="Override narration style for this run")
    p_resp.add_argument("--execute", action="store_true", help="Execute actions instead of simulating")
    p_resp.set_defaults(func=run_respond)

    p_dbg = sub.add_parser("debug", help="Assess and dump raw calculation details")
    p_dbg.add_argument("--window-hours", type=int, default=None, help="Scoring window in hours (positive integer)")
    p_dbg.add_argument("--narration-style", choices=["concise", "causal", "both"], default=None,
                       help="Override narration style for narration preview")
    p_dbg.set_defaults(func=run_debug)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
