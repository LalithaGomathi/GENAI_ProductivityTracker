# compute.py
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from dateutil import tz
import json
import yaml
from typing import Dict, Tuple

from settings import DefaultSettings

def load_category_mapping(path: str) -> Dict[str, list]:
    with open(path, "r") as f:
        return json.load(f)

def load_app_config(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def parse_datetimes(df: pd.DataFrame, start_col: str, end_col: str, tz_name: str) -> pd.DataFrame:
    df = df.copy()
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")
    # Optional: ensure timezone awareness
    if df[start_col].dt.tz is None:
        df[start_col] = df[start_col].dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="NaT")
    if df[end_col].dt.tz is None:
        df[end_col] = df[end_col].dt.tz_localize(tz_name, ambiguous="NaT", nonexistent="NaT")
    return df

def normalize_calls(df_calls: pd.DataFrame, settings: DefaultSettings, tz_name: str) -> pd.DataFrame:
    df = df_calls.copy()
    df = parse_datetimes(df, settings.call_columns["start_ts"], settings.call_columns["end_ts"], tz_name)
    # Compute duration if missing
    dur_col = settings.call_columns["duration_seconds"]
    if dur_col not in df.columns or df[dur_col].isna().any():
        df[dur_col] = (df[settings.call_columns["end_ts"]] - df[settings.call_columns["start_ts"]]).dt.total_seconds()
    return df

def normalize_tickets(df_tickets: pd.DataFrame, settings: DefaultSettings, tz_name: str) -> pd.DataFrame:
    df = df_tickets.copy()
    df = parse_datetimes(df, settings.ticket_columns["start_ts"], settings.ticket_columns["end_ts"], tz_name)
    df["duration_seconds"] = (df[settings.ticket_columns["end_ts"]] - df[settings.ticket_columns["start_ts"]]).dt.total_seconds()
    return df

def normalize_schedule(df_sched: pd.DataFrame, settings: DefaultSettings) -> pd.DataFrame:
    df = df_sched.copy()
    df[settings.schedule_columns["date"]] = pd.to_datetime(df[settings.schedule_columns["date"]]).dt.date
    # Shift times are local naive times; combine into datetimes later by date
    return df

def build_default_schedule(agents: pd.Series, dates: pd.Series, settings: DefaultSettings, tz_name: str, team_series: pd.Series = None) -> pd.DataFrame:
    rows = []
    start_t = settings.default_shift_start
    end_t = settings.default_shift_end
    for a in agents.unique():
        for d in dates.unique():
            rows.append({
                settings.schedule_columns["agent"]: a,
                settings.schedule_columns["date"]: d,
                settings.schedule_columns["shift_start"]: start_t.strftime("%H:%M"),
                settings.schedule_columns["shift_end"]: end_t.strftime("%H:%M"),
                "team": team_series[agents == a].iloc[0] if team_series is not None else None
            })
    df = pd.DataFrame(rows)
    return df

def apply_category_mapping(df: pd.DataFrame, category_col: str, mapping: Dict[str, list]) -> pd.DataFrame:
    df = df.copy()
    reverse_map = {}
    for canonical, variants in mapping.items():
        for v in variants:
            reverse_map[v.lower()] = canonical
    def map_fn(x):
        if pd.isna(x):
            return "Other"
        return reverse_map.get(str(x).lower(), x if x in mapping.keys() else "Other")
    df["category_mapped"] = df[category_col].apply(map_fn)
    return df

def overlap_adjust(events: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    events columns: agent, start_ts, end_ts, duration_seconds, source ('Ticket'/'Call'), category_mapped
    rule: 'count_full' or 'split_time'
    """
    out = []
    for agent, group in events.groupby("agent"):
        # Sort by start times
        g = group.sort_values("start_ts").reset_index(drop=True)
        if rule == "count_full":
            # Sum durations as-is
            g["productive_seconds"] = g["duration_seconds"].clip(lower=0)
            out.append(g)
        else:
            # split_time: proportionally split overlapping windows so total time within overlapping regions equals span length
            # Build a timeline with boundary points
            boundaries = sorted(set(list(g["start_ts"]) + list(g["end_ts"])))
            # For each adjacent interval, determine active events and assign proportional time
            alloc = np.zeros(len(g))
            for i in range(len(boundaries)-1):
                seg_start = boundaries[i]
                seg_end = boundaries[i+1]
                seg_len = (seg_end - seg_start).total_seconds()
                active = g[(g["start_ts"] < seg_end) & (g["end_ts"] > seg_start)]
                if seg_len > 0 and len(active) > 0:
                    share = seg_len / len(active)
                    for idx in active.index:
                        alloc[idx] += share
            g["productive_seconds"] = alloc
            out.append(g)
    return pd.concat(out, ignore_index=True)

def compute_kpis(df_tickets: pd.DataFrame,
                 df_calls: pd.DataFrame,
                 df_schedule: pd.DataFrame,
                 settings: DefaultSettings,
                 tz_name: str,
                 team_field: str = "team") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Normalize
    df_t = normalize_tickets(df_tickets, settings, tz_name)
    df_c = normalize_calls(df_calls, settings, tz_name)

    # Category mapping
    mapping = load_category_mapping("config/category_mapping.json")
    df_t = apply_category_mapping(df_t, settings.ticket_columns["category"], mapping)
    df_c = apply_category_mapping(df_c, settings.call_columns["category"], mapping)

    # Standardize columns and merge sources
    t_cols = settings.ticket_columns
    c_cols = settings.call_columns
    events_t = df_t.rename(columns={
        t_cols["agent"]: "agent",
        t_cols["start_ts"]: "start_ts",
        t_cols["end_ts"]: "end_ts"
    })
    events_t["source"] = "Ticket"
    events_t["team"] = df_t[team_field] if team_field in df_t.columns else None

    events_c = df_c.rename(columns={
        c_cols["agent"]: "agent",
        c_cols["start_ts"]: "start_ts",
        c_cols["end_ts"]: "end_ts"
    })
    events_c["source"] = "Call"
    events_c["team"] = df_c[team_field] if team_field in df_c.columns else None

    events = pd.concat([events_t[["agent","start_ts","end_ts","duration_seconds","category_mapped","source","team"]],
                        events_c[["agent","start_ts","end_ts","duration_seconds","category_mapped","source","team"]]],
                       ignore_index=True)
    events = events.dropna(subset=["agent","start_ts","end_ts"])

    # Schedule
    if df_schedule is None or df_schedule.empty:
        # Build default schedule from events
        dates = events["start_ts"].dt.date
        agents = events["agent"]
        df_schedule = build_default_schedule(agents, dates, settings, tz_name, team_series=events["team"])
    df_s = normalize_schedule(df_schedule, settings)

    # Expand schedule into datetime ranges
    rows = []
    for _, r in df_s.iterrows():
        agent = r[settings.schedule_columns["agent"]]
        date = r[settings.schedule_columns["date"]]
        team = r["team"] if "team" in df_s.columns else None
        start_t = time.fromisoformat(str(r[settings.schedule_columns["shift_start"]]))
        end_t = time.fromisoformat(str(r[settings.schedule_columns["shift_end"]]))
        start_dt = pd.to_datetime(f"{date} {start_t.strftime('%H:%M')}").tz_localize(tz_name)
        end_dt = pd.to_datetime(f"{date} {end_t.strftime('%H:%M')}").tz_localize(tz_name)
        rows.append({"agent": agent, "date": date, "shift_start": start_dt, "shift_end": end_dt, "team": team})
    schedule = pd.DataFrame(rows)

    # Clip events to schedule windows (same-day only)
    events["date"] = events["start_ts"].dt.date
    merged = events.merge(schedule, on=["agent","date"], how="left", suffixes=("","_sched"))

    # If no schedule (either missing or mismatch), keep default shift hours by date created above
    merged["sched_start"] = merged["shift_start"]
    merged["sched_end"] = merged["shift_end"]

    # Clip durations to scheduled time bounds for "productive during shift"
    clipped_start = np.maximum(merged["start_ts"].view("int64"), merged["sched_start"].view("int64"))
    clipped_end = np.minimum(merged["end_ts"].view("int64"), merged["sched_end"].view("int64"))
    merged["clipped_duration"] = ((clipped_end - clipped_start) / 1e9).clip(lower=0)

    # Use overlap rule within each agent-date on clipped events
    clipped_events = merged[["agent","date","start_ts","end_ts","clipped_duration","category_mapped","source","team"]].copy()
    clipped_events = clipped_events.rename(columns={"clipped_duration": "duration_seconds"})
    adjusted = overlap_adjust(clipped_events, settings.overlap_rule)

    # Compute scheduled seconds per agent per date
    sched_seconds = schedule.copy()
    sched_seconds["scheduled_seconds"] = (sched_seconds["shift_end"] - sched_seconds["shift_start"]).dt.total_seconds()

    # Daily per-agent productives
    daily_prod = adjusted.groupby(["agent","date","team"], dropna=False)["productive_seconds"].sum().reset_index()
    daily = daily_prod.merge(sched_seconds[["agent","date","scheduled_seconds","team"]], on=["agent","date","team"], how="left")

    daily["idle_seconds"] = (daily["scheduled_seconds"] - daily["productive_seconds"]).clip(lower=0)
    daily["utilization_pct"] = np.where(daily["scheduled_seconds"] > 0,
                                        100 * daily["productive_seconds"] / daily["scheduled_seconds"],
                                        np.nan)

    # Category AHT (average handling time) across tickets and calls
    cat_aht = adjusted.groupby(["category_mapped","source"], dropna=False)["productive_seconds"].mean().reset_index()
    cat_aht = cat_aht.rename(columns={"productive_seconds": "avg_handle_seconds"})

    # Heatmap data: count/seconds by day/hour
    adjusted["hour"] = adjusted["start_ts"].dt.hour
    heatmap = adjusted.groupby(["date","hour","team"], dropna=False)["productive_seconds"].sum().reset_index()

    return daily, cat_aht, heatmap
