import json
import logging
import os
import sys
from datetime import datetime, timezone


def time_now():
    return datetime.now(timezone.utc).isoformat()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        doc = {
            "@timestamp": time_now(),
            "log.level": record.levelname.lower(),  # severity of event
            "message": record.getMessage(),  # log message
            "logger": record.name,
        }

        if record.exc_info:
            doc.setdefault("app", {})
            doc["app"]["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": record.getMessage(),
                "stack_trace": self.formatException(record.exc_info),
            }
        return json.dumps(doc, ensure_ascii=False)


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    root_logger.setLevel(level)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)

    if os.getenv("LOG_FORMAT", "json").lower() == "json":
        stdout_handler.setFormatter(JsonFormatter())
    else:
        stdout_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )

    root_logger.addHandler(stdout_handler)


def get_logger(name: str):
    if not logging.getLogger().handlers:
        setup_logging()
    return logging.getLogger(name)
