import pandas as pd
import numpy as np
from datetime import time
import json, yaml
from settings import DefaultSettings

def load_category_mapping(path: str):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"Other": []}

def load_app_config(path: str):
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
    df = parse_datetimes(df_tickets, settings.ticket_columns["start_ts"], settings.ticket_columns["end_ts"], tz_name)
    df["duration_seconds"] = (df[settings.ticket_columns["end_ts"]] - df[settings.ticket_columns["start_ts"]]).dt.total_seconds()
    df["duration_seconds"] = df["duration_seconds"].fillna(0).clip(lower=0)
    return df

def normalize_schedule(df_sched, settings):
    df = df_sched.copy()
    df[settings.schedule_columns["date"]] = pd.to_datetime(df[settings.schedule_columns["date"]], errors="coerce").dt.date
    return df

def apply_category_mapping(df, category_col, mapping):
    reverse_map = {str(v).lower(): k for k, vs in mapping.items() for v in vs}
    df["category_mapped"] = df[category_col].apply(lambda x: reverse_map.get(str(x).lower(), "Other") if pd.notna(x) else "Other")
    return df

def overlap_adjust(events, rule):
    out = []
    for agent, group in events.groupby("agent"):
        g = group.sort_values("start_ts").reset_index(drop=True)
        if rule == "count_full":
            g["productive_seconds"] = g["duration_seconds"].clip(lower=0)
            out.append(g)
            continue
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
    return pd.concat(out, ignore_index=True) if out else events.assign(productive_seconds=events["duration_seconds"].clip(lower=0))

def compute_kpis(df_tickets, df_calls, df_schedule, settings, tz_name, team_field="team"):
    df_t = normalize_tickets(df_tickets, settings, tz_name)
    df_c = normalize_calls(df_calls, settings, tz_name)
    mapping = load_category_mapping("config/category_mapping.json")
    df_t = apply_category_mapping(df_t, settings.ticket_columns["category"], mapping)
    df_c = apply_category_mapping(df_c, settings.call_columns["category"], mapping)

    events = pd.concat([
        df_t.rename(columns={settings.ticket_columns["agent"]: "agent", settings.ticket_columns["start_ts"]: "start_ts", settings.ticket_columns["end_ts"]: "end_ts"})[["agent","start_ts","end_ts","duration_seconds","category_mapped"]],
        df_c.rename(columns={settings.call_columns["agent"]: "agent", settings.call_columns["start_ts"]: "start_ts", settings.call_columns["end_ts"]: "end_ts"})[["agent","start_ts","end_ts","duration_seconds","category_mapped"]]
    ], ignore_index=True)

    events["source"] = events.apply(lambda r: "Ticket" if "T-" in str(r.get("agent","")) else "Call", axis=1)
    events["team"] = None
    events = events.dropna(subset=["agent","start_ts","end_ts"])
    events["date"] = events["start_ts"].dt.date

    adjusted = overlap_adjust(events, settings.overlap_rule)
    daily = adjusted.groupby(["agent","date"], dropna=False)["productive_seconds"].sum().reset_index()
    daily["scheduled_seconds"] = 9*3600  # default 9h shift
    daily["idle_seconds"] = (daily["scheduled_seconds"] - daily["productive_seconds"]).clip(lower=0)
    daily["utilization_pct"] = np.where(daily["scheduled_seconds"] > 0, 100*daily["productive_seconds"]/daily["scheduled_seconds"], np.nan)

    cat_aht = adjusted.groupby(["category_mapped","source"], dropna=False)["productive_seconds"].mean().reset_index()
    cat_aht = cat_aht.rename(columns={"productive_seconds":"avg_handle_seconds"})

    adjusted["hour"] = adjusted["start_ts"].dt.hour
    heatmap = adjusted.groupby(["date","hour"], dropna=False)["productive_seconds"].sum().reset_index()

    return daily, cat_aht, heatmap
