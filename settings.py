# settings.py
from dataclasses import dataclass
from datetime import time

@dataclass
class DefaultSettings:
    # If schedule missing, assume default shift hours per agent per day
    default_shift_start: time = time(9, 0)
    default_shift_end: time = time(18, 0)
    # Overlap rule: "count_full" or "split_time"
    overlap_rule: str = "split_time"
    # Timezone label (informational)
    timezone: str = "Asia/Kolkata"

    # Column names expected in uploads (you can remap in UI)
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
        "duration_seconds": "duration_seconds"  # optional; computed if missing
    }

    schedule_columns = {
        "agent": "agent",
        "date": "date",
        "shift_start": "shift_start",
        "shift_end": "shift_end"
    }
