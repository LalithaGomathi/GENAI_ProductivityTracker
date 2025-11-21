# app.py
import streamlit as st
import pandas as pd
import altair as alt
import os
from settings import DefaultSettings
from compute import compute_kpis, load_app_config

st.set_page_config(page_title="Agent Productivity Tracker", layout="wide")
st.title("Agent Productivity Tracker")

# Ensure exports dir
os.makedirs("exports", exist_ok=True)

settings = DefaultSettings()
app_cfg = load_app_config("config/app_config.yaml")

# Uploaders
st.sidebar.header("Upload data")
tickets_file = st.sidebar.file_uploader("Ticket logs CSV", type=["csv"])
calls_file = st.sidebar.file_uploader("Call logs CSV", type=["csv"])
sched_file = st.sidebar.file_uploader("Agent schedule CSV (optional)", type=["csv"])

st.sidebar.markdown("---")
from ui import sidebar_settings, filters
default_start, default_end, overlap_rule, tz_name = sidebar_settings(settings)
settings.default_shift_start = default_start
settings.default_shift_end = default_end
settings.overlap_rule = overlap_rule
settings.timezone = tz_name

# Load sample if not uploaded
def read_csv_or_sample(file, sample_path):
    if file is not None:
        return pd.read_csv(file)
    return pd.read_csv(sample_path)

df_tickets = read_csv_or_sample(tickets_file, "sample_data/tickets_sample.csv")
df_calls = read_csv_or_sample(calls_file, "sample_data/calls_sample.csv")
df_schedule = None
if sched_file is not None:
    df_schedule = pd.read_csv(sched_file)

team_field = "team"  # based on sample; can be absent

# Compute
daily, cat_aht, heatmap = compute_kpis(df_tickets, df_calls, df_schedule, settings, tz_name, team_field=team_field)

# Filters
sel_agents, sel_teams, date_range = filters(daily)

def apply_filters(df):
    dfx = df.copy()
    if sel_agents:
        dfx = dfx[dfx["agent"].isin(sel_agents)]
    if sel_teams:
        dfx = dfx[dfx["team"].isin(sel_teams)]
    if isinstance(date_range, tuple) and len(date_range) == 2 and all(date_range):
        start_d, end_d = date_range
        dfx = dfx[(dfx["date"] >= start_d) & (dfx["date"] <= end_d)]
    return dfx

daily_f = apply_filters(daily)
heatmap_f = apply_filters(heatmap)

# KPI section
st.subheader("Per-agent summary")
kpi_cols = st.columns(4)
for i, agent in enumerate(sorted(daily_f["agent"].unique())):
    agent_df = daily_f[daily_f["agent"] == agent]
    prod = agent_df["productive_seconds"].sum()
    sched = agent_df["scheduled_seconds"].sum()
    idle = agent_df["idle_seconds"].sum()
    util = (100 * prod / sched) if sched > 0 else 0
    with kpi_cols[i % 4]:
        st.metric(label=f"{agent} • Productive time", value=f"{int(prod//3600)}h {int((prod%3600)//60)}m")
        st.metric(label=f"{agent} • Scheduled time", value=f"{int(sched//3600)}h {int((sched%3600)//60)}m")
        st.metric(label=f"{agent} • Utilization %", value=f"{util:.1f}%")
        st.metric(label=f"{agent} • Idle time", value=f"{int(idle//3600)}h {int((idle%3600)//60)}m")

# Team view: heatmap
st.subheader("Team view: busiest hours heatmap")
if not heatmap_f.empty:
    heatmap_f["date_str"] = heatmap_f["date"].astype(str)
    heat = alt.Chart(heatmap_f).mark_rect().encode(
        x=alt.X("hour:O", title="Hour of day"),
        y=alt.Y("date_str:O", title="Date"),
        color=alt.Color("productive_seconds:Q", title="Productive seconds", scale=alt.Scale(scheme="blues")),
        tooltip=["team","date_str","hour","productive_seconds"]
    ).properties(height=300)
    st.altair_chart(heat, use_container_width=True)
else:
    st.info("No data for selected filters.")

# Category-level averages
st.subheader("Average handling time by category")
cat_view = cat_aht.copy()
cat_view["avg_handle_minutes"] = (cat_view["avg_handle_seconds"] / 60).round(1)
st.dataframe(cat_view[["category_mapped","source","avg_handle_minutes"]], use_container_width=True)

# Compare two agents or two weeks
st.subheader("Compare view")
comp_cols = st.columns(2)
with comp_cols[0]:
    st.caption("Compare agents")
    agents_all = sorted(daily["agent"].unique().tolist())
    a1 = st.selectbox("Agent A", agents_all, index=0 if agents_all else None)
    a2 = st.selectbox("Agent B", agents_all, index=1 if len(agents_all) > 1 else 0)
    df_a1 = daily[daily["agent"] == a1]
    df_a2 = daily[daily["agent"] == a2]
    prod_a1 = df_a1["productive_seconds"].sum(); sched_a1 = df_a1["scheduled_seconds"].sum()
    prod_a2 = df_a2["productive_seconds"].sum(); sched_a2 = df_a2["scheduled_seconds"].sum()
    util_a1 = 100 * prod_a1 / sched_a1 if sched_a1 > 0 else 0
    util_a2 = 100 * prod_a2 / sched_a2 if sched_a2 > 0 else 0
    st.write(f"{a1}: Util {util_a1:.1f}% • Prod {int(prod_a1//3600)}h {int((prod_a1%3600)//60)}m")
    st.write(f"{a2}: Util {util_a2:.1f}% • Prod {int(prod_a2//3600)}h {int((prod_a2%3600)//60)}m")

with comp_cols[1]:
    st.caption("Compare weeks")
    # Week grouping by ISO week
    daily["iso_week"] = pd.to_datetime(daily["date"]).dt.isocalendar().week
    weeks = sorted(daily["iso_week"].unique().tolist())
    w1 = st.selectbox("Week 1", weeks, index=0 if weeks else None)
    w2 = st.selectbox("Week 2", weeks, index=1 if len(weeks) > 1 else 0)
    df_w1 = daily[daily["iso_week"] == w1]
    df_w2 = daily[daily["iso_week"] == w2]
    prod_w1 = df_w1["productive_seconds"].sum(); sched_w1 = df_w1["scheduled_seconds"].sum()
    prod_w2 = df_w2["productive_seconds"].sum(); sched_w2 = df_w2["scheduled_seconds"].sum()
    util_w1 = 100 * prod_w1 / sched_w1 if sched_w1 > 0 else 0
    util_w2 = 100 * prod_w2 / sched_w2 if sched_w2 > 0 else 0
    st.write(f"Week {w1}: Util {util_w1:.1f}%")
    st.write(f"Week {w2}: Util {util_w2:.1f}%")

# Export per-agent report
st.subheader("Export per-agent CSV")
export_agents = st.multiselect("Select agents to export", sorted(daily["agent"].unique().tolist()), default=sorted(daily["agent"].unique().tolist()))
if st.button("Export CSV"):
    export_df = daily[daily["agent"].isin(export_agents)].copy()
    export_df["productive_minutes"] = (export_df["productive_seconds"] / 60).round(1)
    export_df["scheduled_minutes"] = (export_df["scheduled_seconds"] / 60).round(1)
    export_df["idle_minutes"] = (export_df["idle_seconds"] / 60).round(1)
    export_path = os.path.join("exports", "per_agent_report.csv")
    export_df[["agent","team","date","productive_minutes","scheduled_minutes","idle_minutes","utilization_pct"]].to_csv(export_path, index=False)
    st.success(f"Exported to {export_path}")
    st.download_button("Download here", data=export_df.to_csv(index=False), file_name="per_agent_report.csv", mime="text/csv")

# Raw tables for debugging
with st.expander("Raw daily data"):
    st.dataframe(daily, use_container_width=True)
with st.expander("Raw heatmap data"):
    st.dataframe(heatmap, use_container_width=True)
