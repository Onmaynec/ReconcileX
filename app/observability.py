"""Structured request logs and bounded in-process latency/error metrics."""
import json
import logging
import sys
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from statistics import median

from app.request_context import request_id


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "requestId": getattr(record, "request_id", request_id.get()),
        }
        for attribute in ("method", "path", "status_code", "duration_ms"):
            value = getattr(record, attribute, None)
            if value is not None:
                payload[attribute] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging():
    logger = logging.getLogger("reconcilex")
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


class LocalMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._durations = defaultdict(lambda: deque(maxlen=10000))
        self._responses = defaultdict(lambda: {"count": 0, "errors": 0})

    def record(self, route, status, duration_ms):
        with self._lock:
            self._durations[route].append(duration_ms)
            key = self._responses[route]
            key["count"] += 1
            if status >= 400:
                key["errors"] += 1

    def snapshot(self):
        with self._lock:
            response = []
            for key, values in self._durations.items():
                ordered = sorted(values)
                n = len(ordered)
                totals = self._responses[key]
                response.append({
                    "route": key, "count": totals["count"],
                    "errors": totals["errors"],
                    "p50_ms": round(ordered[(n - 1) // 2], 3),
                    "p95_ms": round(ordered[max(0, ((95 * n + 99) // 100) - 1)], 3),
                })
        return sorted(response, key=lambda item: item["route"])


metrics = LocalMetrics()
