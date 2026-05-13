import logging
from collections import deque
from datetime import datetime
from threading import Lock

LOG_BUFFER_LIMIT = 5000
_LOG_BUFFER = deque(maxlen=LOG_BUFFER_LIMIT)
_LOG_LOCK = Lock()


class InMemoryLogHandler(logging.Handler):
    """Small ring-buffer handler for the frontend live log console."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "timestamp": datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "message": record.getMessage(),
            }
            with _LOG_LOCK:
                _LOG_BUFFER.append(entry)
        except Exception:
            self.handleError(record)


def get_recent_logs() -> list[dict[str, str]]:
    with _LOG_LOCK:
        return list(_LOG_BUFFER)


def clear_logs() -> None:
    with _LOG_LOCK:
        _LOG_BUFFER.clear()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_root_logger = logging.getLogger()
if not any(getattr(handler, "_ba_tool_memory_handler", False) for handler in _root_logger.handlers):
    _memory_handler = InMemoryLogHandler(level=logging.INFO)
    _memory_handler._ba_tool_memory_handler = True
    _root_logger.addHandler(_memory_handler)

logger = logging.getLogger(__name__)
