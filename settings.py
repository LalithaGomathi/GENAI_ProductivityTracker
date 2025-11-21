from dataclasses import dataclass
from datetime import time

@dataclass
class DefaultSettings:
    default_shift_start: time = time(9, 0)
    default_shift_end: time = time(18, 0)
    overlap_rule: str = "split_time"
    timezone: str = "Asia/Kolkata"

    ticket_columns = {
        "agent": "agent",
        "ticket_id": "ticket_id",
        "category": "category",
        "start_ts": "start_ts",
        "end_ts": "end_ts"
    }

    call_columns = {
        "agent": "agent",
        "call_id": "call_id",
        "category": "category",
        "start_ts": "start_ts",
        "end_ts": "end_ts",
        "duration_seconds": "duration_seconds"
    }

    schedule_columns = {
        "agent": "agent",
        "date": "date",
        "shift_start": "shift_start",
        "shift_end": "shift_end"
    }
