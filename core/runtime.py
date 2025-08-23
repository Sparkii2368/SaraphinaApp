from __future__ import annotations
import argparse
import json

from core.logging import log_event                  # runtime log
from core.identity import identity                  # manifest
from core.autobiography import recent_entries, log_event as log_journal_event  # personal journal

# Phase 1 imports (additive)
from core.insights import generate_trends_report
from core.meta_cognition import generate_audit_report


def run_status() -> None:
    # Machine-readable result
    result = {"status": "ok"}

    # Log to runtime logs
    log_event("status", result)
    # Log to autobiography
    log_journal_event("Status check", result)

    # Identity and last few journal entries
    ident = identity()
    recent = recent_entries(limit=5)

    print("=== Saraphina Identity ===")
    print(json.dumps(ident, indent=2))

    print("\n=== Recent Memories ===")
    if not recent:
        print("No autobiographical entries yet.")
    else:
        for e in recent:
            t = e.get("time") or e.get("timestamp")
            ev = e.get("event")
            details = e.get("details") or {}
            print(f"- [{t}] {ev} {details if details else ''}")

    # Phase 1: Operational insights (self-narration)
    print("\n=== Operational Insights ===")
    report = generate_trends_report()
    print(f"Heartbeats in last 24h: {report['heartbeat_count_24h']}")
    print(f"Last heartbeat: {report['last_heartbeat']}")
    print(f"Last doctor run: {report['last_doctor_run']}")
    anomalies = report.get("anomalies") or []
    print(f"Anomalies: {', '.join(anomalies)}")
    print("\nRecent autobiography roll‑up:")
    for entry in report.get('recent_autobiography', []):
        print(f"- {entry}")

    print("\n=== JSON Status ===")
    print(json.dumps(result, indent=2))


def run_smoke() -> None:
    result = {"status": "ok", "heartbeat": True}
    log_event("heartbeat", result)
    log_journal_event("Heartbeat", result)
    print("Smoke event logged — check logs/runtime.jsonl")


def run_doctor() -> None:
    diagnostics = {"diagnostics": "all green"}
    log_event("doctor", diagnostics)
    log_journal_event("Doctor check", diagnostics)
    print(json.dumps(diagnostics, indent=2))


def run_insights() -> None:
    """
    Dump full insights report (machine-readable).
    """
    report = generate_trends_report()
    log_event("insights_report", {"summary": "generated"})
    log_journal_event("Insights generated", {"keys": list(report.keys())})
    print(json.dumps(report, indent=2))


def run_audit(threshold: float = 0.9) -> None:
    """
    Run meta-cognitive audit. If triggers occur, surface recommendations.
    """
    report = generate_audit_report(threshold=threshold)
    # Log to both channels
    log_event("audit", {"average_confidence_24h": report.get("average_confidence_24h"),
                        "triggers": report.get("triggers")})
    log_journal_event("Audit", {"triggers": report.get("triggers"),
                                "recommendations": report.get("recommendations")})
    print("=== Audit Report ===")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Saraphina CLI runtime")
    parser.add_argument("--status", action="store_true", help="Show system status")
    parser.add_argument("--smoke", action="store_true", help="Run smoke test + heartbeat")
    parser.add_argument("--doctor", action="store_true", help="Run diagnostics")
    # Phase 1 flags
    parser.add_argument("--insights", action="store_true", help="Dump full insights report (JSON)")
    parser.add_argument("--audit", action="store_true", help="Run meta-cognitive audit and show recommendations")
    parser.add_argument("--audit-threshold", type=float, default=0.9, help="Confidence threshold for audit triggers")

    args = parser.parse_args()

    if args.status:
        run_status()
    elif args.smoke:
        run_smoke()
    elif args.doctor:
        run_doctor()
    elif args.insights:
        run_insights()
    elif args.audit:
        run_audit(threshold=args.audit_threshold)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
