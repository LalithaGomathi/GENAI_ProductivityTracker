import pandas as pd
import numpy as np
from datetime import time
import json, yaml
from typing import Dict, Tuple
from settings import DefaultSettings

def load_category_mapping(path: str) -> Dict[str, list]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"Other": []}

def load_app_config(path: str) -> Dict:
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

def parse_datetimes(df, start_col, end_col, tz_name):
    df = df.copy()
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
    df = df.dropna(subset=[start_col,end_col])
    if getattr(df[start_col].dt, "tz", None) is None:
        df[start_col] = df[start_col].dt.tz_localize(tz_name, errors="coerce")
    if getattr(df[end_col].dt, "tz", None) is None:
        df[end_col] = df[end_col].dt.tz_localize(tz_name, errors="coerce")
    return df

def normalize_calls(df_calls, settings, tz_name):
    df = parse_datetimes(df_calls, settings.call_columns["start_ts"], settings.call_columns["end_ts"], tz_name)
    dur_col = settings.call_columns["duration_seconds"]
    if dur_col not in df.columns:
        df[dur_col] = (df[settings.call_columns["end_ts"]] - df[settings.call_columns["start_ts"]]).dt.total_seconds()
    df[dur_col] = df[dur_col].fillna(0).clip(lower=0)
    return df

def normalize_tickets(df_tickets, settings, tz_name):
    df = parse_datetimes(df_tickets
