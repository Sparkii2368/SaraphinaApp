# core/registry.py
from __future__ import annotations
from typing import Callable, Dict, List, Any

HealthFn = Callable[[], Dict[str, Any]]

class Registry:
    def __init__(self) -> None:
        self._checks: Dict[str, HealthFn] = {}

    def register(self, name: str, health_fn: HealthFn) -> None:
        self._checks[name] = health_fn

    def health(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"components": {}, "ok": True}
        for name, fn in self._checks.items():
            try:
                res = fn() or {}
                ok = bool(res.get("ok", True))
                out["components"][name] = res
                if not ok:
                    out["ok"] = False
            except Exception as e:
                out["components"][name] = {"ok": False, "error": str(e)}
                out["ok"] = False
        return out
