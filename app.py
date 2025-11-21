import streamlit as st
import pandas as pd
import altair as alt
import os
from settings import DefaultSettings
from compute import compute_kpis, load_app_config
from ui import sidebar_settings, filters

st.set_page_config(page_title="Agent Productivity Tracker", layout="wide")
st.title("Agent Productivity Tracker")

# Ensure exports dir exists
os.makedirs("exports", exist_ok=True)

settings = DefaultSettings()
app_cfg = load_app_config("config/app_config.yaml")

# Uploaders
st.sidebar.header("Upload data")
tickets_file = st.sidebar.file_uploader("Ticket logs CSV", type=["csv"])
calls_file = st.sidebar.file_uploader("Call logs CSV", type=["csv"])
sched_file = st.sidebar.file_uploader("Agent schedule CSV (optional)", type=["csv"])

# Settings
st.sidebar.markdown("---")
default_start, default_end, overlap_rule, tz_name = sidebar_settings(settings)
settings.default_shift_start = default_start
settings.default_shift_end = default_end
settings.overlap_rule = overlap_rule
settings.timezone = tz_name

# Helper to read sample if no file
def read_csv_or_sample(file, sample_path):
    if file is not None:
        st.write(f"Loading {file.name}...")
        return pd.read_csv(file)
    st.write(f"Using sample {sample_path}")
    return pd.read_csv(sample_path)

# Load data
df_tickets = read
