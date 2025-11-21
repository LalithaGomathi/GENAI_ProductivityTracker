# ui.py
import streamlit as st
import pandas as pd
from settings import DefaultSettings

def sidebar_settings(settings: DefaultSettings):
    st.sidebar.header("Settings")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        default_start = st.time_input("Default shift start", value=settings.default_shift_start)
    with col2:
        default_end = st.time_input("Default shift end", value=settings.default_shift_end)
    overlap_rule = st.sidebar.selectbox("Overlap rule", ["split_time", "count_full"], index=0)
    tz_name = st.sidebar.text_input("Timezone", value=settings.timezone)

    st.sidebar.caption("If schedule upload is missing, these defaults apply per agent per day.")
    return default_start, default_end, overlap_rule, tz_name

def filters(df_daily: pd.DataFrame):
    st.subheader("Filters")
    agents = sorted(df_daily["agent"].dropna().unique().tolist())
    teams = sorted(df_daily["team"].dropna().unique().tolist())
    dates = sorted(df_daily["date"].dropna().unique().tolist())

    sel_agents = st.multiselect("Agents", agents, default=agents[:2] if len(agents) >= 2 else agents)
    sel_teams = st.multiselect("Teams", teams, default=teams)
    date_range = st.date_input("Date range", value=(min(dates) if dates else None, max(dates) if dates else None))
    return sel_agents, sel_teams, date_range
