from __future__ import annotations
import json
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

# --- Always anchor logs to the project root ---
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # two levels up from /core
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

RUNTIME_LOG = os.path.join(LOG_DIR, "runtime.jsonl")

# Ensure the file exists before first write
if not os.path.exists(RUNTIME_LOG):
    open(RUNTIME_LOG, "a", encoding="utf-8").close()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        # Unpack any structured extras passed via `extra`
        if hasattr(record, "extra"):
            payload.update(record.extra)
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str = "saraphina") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    # File handler (rotates at ~2MB)
    fh = RotatingFileHandler(RUNTIME_LOG, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(JsonFormatter())

    # Console handler
    sh = logging.StreamHandler()
    sh.setFormatter(JsonFormatter())

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger


def log_event(event_type: str, data: Dict[str, Any]) -> None:
    logger = get_logger()
    logger.info(event_type, extra={"extra": {"event": event_type, **data}})
