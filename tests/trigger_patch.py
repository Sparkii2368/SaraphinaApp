from core import meta_cognition
from datetime import datetime, UTC
import os

# Current UTC timestamp (timezone-aware)
now = datetime.now(UTC).isoformat()

assessment = meta_cognition.HealthAssessment(
    time=now,
    health_score=42.0,
    anomaly_health=30.0,
    confidence=0.4,
    mode="degraded",
    window_hours=6,
    anomaly_counts_window={"test_anomaly": 1},
    trends={
        "last_doctor_run": now,
        "last_heartbeat": now
    },
    mode_reason="synthetic_test_trigger",
    suggestions=["Simulated fix: Adjust threshold handling"],
    adaptive_deltas={"heartbeat_gap": -0.5},
    adaptive_prevalence={"test_anomaly": 1.0}
)

# Ensure patch queue file exists
queue_path = "logs/self_patch_queue.jsonl"
os.makedirs(os.path.dirname(queue_path), exist_ok=True)
open(queue_path, "a").close()

# Queue the synthetic patch event
mc = meta_cognition.MetaCognition()
mc._queue_self_patch(assessment)

print(f"Queued patch event in: {queue_path}\n---")
with open(queue_path) as f:
    for line in f:
        print(line.strip())
