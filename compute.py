import pandas as pd
import numpy as np
import time
import json, yaml
from settings import DefaultSettings

# ---------- Config loaders ----------
def load_category_mapping(path: str):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[compute] Failed to load category mapping: {e}")
        return {"Other": []}

def load_app_config(path: str):
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"[compute] Failed to load app config: {e}")
        return {}

# ---------- Normalization helpers ----------
def parse_datetimes(df, start_col, end_col, tz_name):
    t0 = time.time()
    df = df.copy()
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
    df = df.dropna(subset=[start_col, end_col])
    try:
        if getattr(df[start_col].dt, "tz", None) is None:
            df[start_col] = df[start_col].dt.tz_localize(tz_name, errors="coerce")
        if getattr(df[end_col].dt, "tz", None) is None:
            df[end_col] = df[end_col].dt.tz_localize(tz_name, errors="coerce")
    except Exception as e:
        print(f"[compute] tz_localize error: {e}")
    print(f"[compute] parse_datetimes done in {time.time()-t0:.2f}s for {len(df)} rows")
    return df

def normalize_calls(df_calls, settings, tz_name):
    t0 = time.time()
    df = parse_datetimes(df_calls, settings.call_columns["start_ts"], settings.call_columns["end_ts"], tz_name)
    dur_col = settings.call_columns["duration_seconds"]
    if dur_col not in df.columns:
        df[dur_col] = (df[settings.call_columns["end_ts"]] - df[settings.call_columns["start_ts"]]).dt.total_seconds()
    df[dur_col] = df[dur_col].fillna(0).clip(lower=0)
    print(f"[compute] normalize_calls done in {time.time()-t0:.2f}s for {len(df)} rows")
    return df

def normalize_tickets(df_tickets, settings, tz_name):
    t0 = time.time()
    df = parse_datetimes(df_tickets, settings.ticket_columns["start_ts"], settings.ticket_columns["end_ts"], tz_name)
    df["duration_seconds"] = (df[settings.ticket_columns["end_ts"]] - df[settings.ticket_columns["start_ts"]]).dt.total_seconds()
    df["duration_seconds"] = df["duration_seconds"].fillna(0).clip(lower=0)
    print(f"[compute] normalize_tickets done in {time.time()-t0:.2f}s for {len(df)} rows")
    return df

def normalize_schedule(df_sched, settings):
    t0 = time.time()
    df = df_sched.copy()
    df[settings.schedule_columns["date"]] = pd.to_datetime(df[settings.schedule_columns["date"]], errors="coerce").dt.date
    print(f"[compute] normalize_schedule done in {time.time()-t0:.2f}s for {len(df)} rows")
    return df

# ---------- Category mapping ----------
def apply_category_mapping(df, category_col, mapping):
    t0 = time.time()
    reverse_map = {str(v).lower(): k for k, vs in mapping.items() for v in vs}
    def map_fn(x):
        if pd.isna(x):
            return "Other"
        return reverse_map.get(str(x).lower(), "Other")
    df["category_mapped"] = df[category_col].apply(map_fn)
    print(f"[compute] apply_category_mapping done in {time.time()-t0:.2f}s for {len(df)} rows")
    return df

# ---------- Overlap adjustment ----------
def overlap_adjust(events, rule):
    t0 = time.time()
    out = []
    for agent, group in events.groupby("agent"):
        g = group.sort_values("start_ts").reset_index(drop=True)
        if rule == "count_full":
            g["productive_seconds"] = g["duration_seconds"].clip(lower=0)
            out.append(g)
            continue
        boundaries = sorted
