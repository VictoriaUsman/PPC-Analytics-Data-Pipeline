"""Shared structured logging setup for every component of the pipeline.

The reference POS Pipeline mixes bare print() (connectors, Glue job, infra scripts) with
one file using the logging module — fine for boilerplate, but it means none of those print
lines are queryable as structured fields in CloudWatch Logs Insights. This pipeline
standardizes on `logging` everywhere, emitting one JSON object per line, so any field
(profile_id, ad_product, report_id, duration_ms) can be filtered/aggregated in Logs Insights
without regex-parsing free text.

Never pass a secret (refresh token, access token, webhook URL) as a log field or into the
message string. `get_logger` scrubs nothing automatically — callers are responsible for
that, same discipline the reference pipeline's Teams-notifier used deliberately (catching
and re-raising an exception to strip a credential-bearing URL out of what reaches
CloudWatch Logs).
"""

import json
import logging
import os
import sys


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured (e.g. re-imported within the same Lambda container)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    logger.propagate = False
    return logger


def log_fields(**fields) -> dict:
    """Attach structured fields to a log call: logger.info("msg", extra=log_fields(profile_id=x))."""
    return {"fields": fields}
