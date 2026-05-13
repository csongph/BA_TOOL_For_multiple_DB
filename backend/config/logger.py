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
                "name": record.name
            }
            with _LOG_LOCK:
                _LOG_BUFFER.append(entry)
            
            # เช็คระดับความสำคัญเพื่อทำการ Alert ออกที่หน้า Terminal ทันที
            if record.levelno >= logging.WARNING:
                symbol = "❌" if record.levelno >= logging.ERROR else "⚠️"
                print(f"\n{symbol}  [{entry['level']}] {entry['message']} (in {record.name})\n")
                
        except Exception:
            self.handleError(record)

def get_recent_logs(only_errors: bool = False) -> list[dict[str, str]]:
    """ดึงข้อมูล Log ล่าสุดจาก Buffer"""
    with _LOG_LOCK:
        if only_errors:
            # คืนค่าเฉพาะระดับ Warning ขึ้นไป
            return [log for log in _LOG_BUFFER if log["level"] in ("WARNING", "ERROR", "CRITICAL")]
        return list(_LOG_BUFFER)

def clear_logs() -> None:
    """ล้างข้อมูล Log ใน Buffer ทั้งหมด"""
    with _LOG_LOCK:
        _LOG_BUFFER.clear()

# ตั้งค่า Logging พื้นฐานของ Python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_root_logger = logging.getLogger()

# ตรวจสอบเพื่อไม่ให้เพิ่ม Handler ซ้ำซ้อน
if not any(getattr(handler, "_ba_tool_memory_handler", False) for handler in _root_logger.handlers):
    _memory_handler = InMemoryLogHandler(level=logging.INFO)
    _memory_handler._ba_tool_memory_handler = True
    _root_logger.addHandler(_memory_handler)

# สร้าง logger instance สำหรับโปรเจกต์
logger = logging.getLogger("ba_tool")