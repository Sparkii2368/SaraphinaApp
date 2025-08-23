# tests/smoke_phase0.py
from core.runtime import build_registry

def test_health_ok():
    r = build_registry()
    summary = r.health()
    assert "components" in summary
    assert summary["ok"] is True
