# compute.py (safe + debug version)

import pandas as pd
import numpy as np
from datetime import time
import json
import yaml
from typing import Dict, Tuple
from settings import DefaultSettings

# ---------- Config loaders ----------
def load_category_mapping(path: str) -> Dict[str, list]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[compute] Failed to load category mapping: {e}")
        return {"Other": []}

def load_app_config(path: str) -> Dict:
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"[compute] Failed to load app config: {e}")
        return {}

# ---------- Normalization helpers ----------
def parse_datetimes(df: pd.DataFrame, start_col: str, end_col: str, tz_name: str) -> pd.DataFrame:
    df = df.copy()
    try:
        df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
        df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
        # Localize if naive
        if getattr(df[start_col].dt, "tz", None) is None:
            df[start_col] = df[start_col].dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="NaT")
        if getattr(df[end_col].dt, "tz", None) is None:
            df[end_col] = df[end_col].dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="NaT")
    except Exception as e:
        print(f"[compute] parse_datetimes error: {e}")
    return df

def normalize_calls(df_calls: pd.DataFrame, settings: DefaultSettings, tz_name: str) -> pd.DataFrame:
    df = df_calls.copy()
    df = parse_datetimes(df, settings.call_columns["start_ts"], settings.call_columns["end_ts"], tz_name)
    dur_col = settings.call_columns["duration_seconds"]
    if dur_col not in df.columns or df[dur_col].isna().any():
        df[dur_col] = (df[settings.call_columns["end_ts"]] - df[settings.call_columns["start_ts"]]).dt.total_seconds()
    df[dur_col] = df[dur_col].fillna(0).clip(lower=0)
    return df

def normalize_tickets(df_tickets: pd.DataFrame, settings: DefaultSettings, tz_name: str) -> pd.DataFrame:
    df = df_tickets.copy()
    df = parse_datetimes(df, settings.ticket_columns["start_ts"], settings.ticket_columns["end_ts"], tz_name)
    df["duration_seconds"] = (df[settings.ticket_columns["end_ts"]] - df[settings.ticket_columns["start_ts"]]).dt.total_seconds()
    df["duration_seconds"] = df["duration_seconds"].fillna(0).clip(lower=0)
    return df

def normalize_schedule(df_sched: pd.DataFrame, settings: DefaultSettings) -> pd.DataFrame:
    df = df_sched.copy()
    try:
        df[settings.schedule_columns["date"]] = pd.to_datetime(df[settings.schedule_columns["date"]], errors="coerce").dt.date
    except Exception as e:
        print(f"[compute] normalize_schedule error: {e}")
    return df

def build_default_schedule(agents: pd.Series, dates: pd.Series, settings: DefaultSettings, tz_name: str, team_series: pd.Series = None) -> pd.DataFrame:
    rows = []
    start_t = settings.default_shift_start
    end_t = settings.default_shift_end
    team_map = {}
    if team_series is not None and agents is not None:
        for a in agents.unique():
            try:
                team_map[a] = team_series[agents == a].dropna().iloc[0]
            except Exception:
                team_map[a] = None
    for a in agents.unique():
        for d in pd.Series(dates.unique()).dropna().sort_values():
            rows.append({
                settings.schedule_columns["agent"]: a,
                settings.schedule_columns["date"]: d,
                settings.schedule_columns["shift_start"]: start_t.strftime("%H:%M"),
                settings.schedule_columns["shift_end"]: end_t.strftime("%H:%M"),
                "team": team_map.get(a, None)
            })
    return pd.DataFrame(rows)

# ---------- Category mapping ----------
def apply_category_mapping(df: pd.DataFrame, category_col: str, mapping: Dict[str, list]) -> pd.DataFrame:
    df = df.copy()
    reverse_map = {}
    for canonical, variants in mapping.items():
        for v in variants:
            reverse_map[str(v).lower()] = canonical
    def map_fn(x):
        if pd.isna(x):
            return "Other"
        x_str = str(x).lower()
        return reverse_map.get(x_str, x if x in mapping.keys() else "Other")
    df["category_mapped"] = df[category_col].apply(map_fn)
    return df

# ---------- Overlap adjustment ----------
def overlap_adjust(events: pd.DataFrame, rule: str) -> pd.DataFrame:
    out = []
    for agent, group in events.groupby("agent"):
        g = group.sort_values("start_ts").reset_index(drop=True)
        if rule == "count_full":
            g["productive_seconds"] = g["duration_seconds"].clip(lower=0)
            out.append(g)
            continue
        # split_time
        boundaries = sorted(set(list(g["start_ts"].dropna()) + list(g["end_ts"].dropna())))
        alloc = np.zeros(len(g))
        for i in range(len(boundaries)-1):
            seg_start = boundaries[i]
            seg_end = boundaries[i+1]
            seg_len = (seg_end - seg_start).total_seconds()
            if seg_len <= 0:
                continue
            active = g[(g["start_ts"] < seg_end) & (g["end_ts"] > seg_start)]
            if len(active) > 0:
                share = seg_len / len(active)
                for idx in active.index:
                    alloc[idx] += share
        g["productive_seconds"] = alloc
        out.append(g)
    if out:
        return pd.concat(out, ignore_index=True)
    return events.assign(productive_seconds=events["duration_seconds"].clip(lower=0))

# ---------- KPI computation ----------
def compute_kpis(df_tickets: pd.DataFrame,
                 df_calls: pd.DataFrame,
                 df_schedule: pd.DataFrame,
                 settings: DefaultSettings,
                 tz_name: str,
                 team_field: str = "team") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("[compute] Starting KPI computation...")

    df_t = normalize_tickets(df_tickets, settings, tz_name)
    df_c = normalize_calls(df_calls, settings, tz_name)

    mapping = load_category_mapping("config/category_mapping.json")
    df_t = apply_category_mapping(df_t, settings.ticket_columns["category"], mapping)
    df_c = apply_category_mapping(df_c, settings.call_columns["category"], mapping)

    events_t = df_t.rename(columns={
        settings.ticket_columns["agent"]: "agent",
        settings.ticket_columns["start_ts"]: "start_ts",
        settings.ticket_columns["end_ts"]: "end_ts"
    })
    events_t["source"] = "Ticket"
    events_t["team"] = df_t[team_field] if team_field in df_t.columns else None

    events_c = df_c.rename(columns={
        settings.call_columns["agent"]: "agent",
        settings.call_columns["start_ts"]: "start_ts",
        settings.call_columns["end_ts"]: "end_ts"
    })
    events_c["source"] = "Call"
    events_c["team"] = df_c[team_field] if team_field in df_c.columns else None

    events = pd.concat([
        events_t[["agent","start_ts","end_ts","duration_seconds","category_mapped","source","team"]],
        events_c[["agent","start_ts","end_ts","duration_seconds","category_mapped","source","team"]]
    ], ignore_index=True).dropna(subset=["agent","start_ts","end_ts"])

    if df_schedule is None or df_schedule.empty:
        dates = events["start_ts"].dt.date
        agents = events["agent"]
        df_schedule = build_default_schedule(agents, dates, settings, tz_name, team_series=events["team"])
    df_s = normalize_schedule(df_schedule, settings)

    rows = []
    for _, r in df_s.iterrows():
        try:
            agent = r[settings.schedule_columns["agent"]]
            date = r[settings.schedule_columns["date"]]
            team = r.get("team", None)
            start_t = time.fromisoformat(str(r[settings.schedule_columns["shift_start"]]))
            end_t = time.fromisoformat(str(r[settings.schedule_columns["shift_end"]]))
            start_dt = pd.to_datetime(f"{date} {start_t.strftime('%H:%M')}").tz_localize(tz_name)
            end_dt = pd.to_datetime(f"{date} {end_t.strftime('%H:%M')}").tz_localize(tz_name)
            rows.append({"agent": agent, "date": date, "shift_start": start_dt, "shift_end": end_dt, "team": team
