import streamlit as st
import pandas as pd
import altair as alt
import os
from settings import DefaultSettings
from compute import compute_kpis, load_app_config
from ui import sidebar_settings, filters

st.set_page_config(page_title="Agent Productivity Tracker (Debug Mode)", layout="wide")
st.title("Agent Productivity Tracker – Debug Mode")

os.makedirs("exports", exist_ok=True)

settings = DefaultSettings()
app_cfg = load_app_config("config/app_config.yaml")

# Sidebar uploaders
st.sidebar.header("Upload data")
tickets_file = st.sidebar.file_uploader("Ticket logs CSV", type=["csv"])
calls_file = st.sidebar.file_uploader("Call logs CSV", type=["csv"])
sched_file = st.sidebar.file_uploader("Agent schedule CSV (optional)", type=["csv"])

# Sidebar settings
st.sidebar.markdown("---")
default_start, default_end, overlap_rule, tz_name = sidebar_settings(settings)
settings.default_shift_start = default_start
settings.default_shift_end = default_end
settings.overlap_rule = overlap_rule
settings.timezone = tz_name

def read_csv_or_sample(file, sample_path, label):
    if file is not None:
        st.write(f"✅ Loaded {label}: {file.name}")
        return pd.read_csv(file)
    st.write(f"⚠️ Using sample {label}: {sample_path}")
    return pd.read_csv(sample_path)

# Load data
st.write("🔄 Loading data files...")
df_tickets = read_csv_or_sample(tickets_file, "sample_data/tickets_sample.csv", "Tickets")
df_calls = read_csv_or_sample(calls_file, "sample_data/calls_sample.csv", "Calls")
df_schedule = None
if sched_file is not None:
    st.write(f"✅ Loaded Schedule: {sched_file.name}")
    df_schedule = pd.read_csv(sched_file)
else:
    st.write("⚠️ No schedule uploaded, using default 09:00–18:00 shifts")

team_field = "team"

# Compute KPIs
st.write("🚀 Starting KPI computation...")
try:
    daily, cat_aht, heatmap = compute_kpis(
        df_tickets, df_calls, df_schedule, settings, tz_name, team_field=team_field
    )
    st.success("✅ KPI computation complete")
except Exception as e:
    st.error(f"❌ Error during KPI computation: {e}")
    st.stop()

# Filters
st.write("🔄 Applying filters...")
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
st.success("✅ Filters applied")

# KPI section
st.subheader("Per-agent summary (Debug)")
if daily_f.empty:
    st.info("ℹ️ No data after filters.")
else:
    st.write(f"📊 Rendering KPIs for {len(daily_f['agent'].unique())} agents")
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

# Heatmap
st.subheader("Team view: busiest hours heatmap (Debug)")
if not heatmap_f.empty:
    st.write(f"📊 Rendering heatmap for {heatmap_f['date'].nunique()} days")
    heatmap_f["date_str"] = heatmap_f["date"].astype(str)
    heat = alt.Chart(heatmap_f).mark_rect().encode(
        x=alt.X("hour:O", title="Hour of day"),
        y=alt.Y("date_str:O", title="Date"),
        color=alt.Color("productive_seconds:Q", title="Productive seconds", scale=alt.Scale(scheme="blues")),
        tooltip=["team","date_str","hour","productive_seconds"]
    ).properties(height=300)
    st.altair_chart(heat, use_container_width=True)
else:
    st.info("ℹ️ No heatmap data for selected filters.")

# Category averages
st.subheader("Average handling time by category (Debug)")
if not cat_aht.empty:
    st.write(f"📊 Rendering {len(cat_aht)} category averages")
    cat_view = cat_aht.copy()
    cat_view["avg_handle_minutes"] = (cat_view["avg_handle_seconds"] / 60).round(1)
    st.dataframe(cat_view[["category_mapped","source","avg_handle_minutes"]], use_container_width=True)
else:
    st.info("ℹ️ No category data available.")

# Export
st.subheader("Export per-agent CSV (Debug)")
export_agents = st.multiselect("Select agents to export", sorted(daily["agent"].unique().tolist()), default=sorted(daily["agent"].unique().tolist()))
if st.button("Export CSV"):
    export_df = daily[daily["agent"].isin(export_agents)].copy()
    export_df["productive_minutes"] = (export_df["productive_seconds"] / 60).round(1)
    export_df["scheduled_minutes"] = (export_df["scheduled_seconds"] / 60).round(1)
    export_df["idle_minutes"] = (export_df["idle_seconds"] / 60).round(1)
    export_path = os.path.join("exports", "per_agent_report.csv")
    export_df[["agent","team","date","productive_minutes","scheduled_minutes","idle_minutes","utilization_pct"]].to_csv(export_path, index=False)
    st.success(f"✅ Exported to {export_path}")
    st.download_button("Download here", data=export_df.to_csv(index=False), file_name="per_agent_report.csv", mime="text/csv")

# Debug raw tables
with st.expander("Raw daily data (Debug)"):
    st.dataframe(daily, use_container_width=True)
with st.expander("Raw heatmap data (Debug)"):
    st.dataframe(heatmap, use_container_width=True)
