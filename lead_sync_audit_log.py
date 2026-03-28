"""
Дополнительная строка в .txt: дата — id сделки — OK / ERROR (сценарий amo_add_lead_owner_sync).
"""
import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from config import AMO_LEAD_SYNC_RESULT_LOG

_lock = threading.Lock()


def append_amo_lead_sync_audit_line(lead_id: int, outcome: str) -> None:
    """outcome: OK или ERROR. Путь из AMO_LEAD_SYNC_RESULT_LOG; None — не писать."""
    path = AMO_LEAD_SYNC_RESULT_LOG
    if not path:
        return
    if outcome not in ("OK", "ERROR"):
        raise ValueError(f"outcome must be OK or ERROR, got {outcome!r}")
    dt = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{dt} - {lead_id} - {outcome}\n"
    full = os.path.abspath(path)
    parent = os.path.dirname(full)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with _lock:
        with open(full, "a", encoding="utf-8") as f:
            f.write(line)
