# README.md

Agent Productivity Tracker

What it does:
- Combines ticket activity and phone-call time to compute productive minutes/hours, scheduled time, utilization %, idle time, and average handling time by category.
- Provides a heatmap (day/hour) to visualize busy times.
- Offers per-agent filters, team filters, compare views, and CSV export.

How to run:
1. Create and activate a Python virtual environment.
2. Install dependencies: `pip install -r requirements.txt`.
3. Start the app: `streamlit run app.py`.
4. Upload your CSVs or use `sample_data/` to demo instantly.

Data expectations:

Tickets CSV:
- Columns: agent, ticket_id, category, start_ts, end_ts, (optional) team
- Timestamps: local time (we localize to Asia/Kolkata by default; configurable).
- Each row represents one handling window. If you have multiple work intervals per ticket, include multiple rows per ticket_id.

Calls CSV:
- Columns: agent, call_id, category, start_ts, end_ts, duration_seconds (optional), (optional) team
- duration_seconds is auto-computed if missing.

Schedule CSV (optional):
- Columns: agent, date (YYYY-MM-DD), shift_start (HH:MM), shift_end (HH:MM), (optional) team
- If omitted, default shift 09:00–18:00 is applied per agent per day.

Settings:
- Overlap rule:
  - split_time: Overlapping windows are split proportionally so total time never double-counts.
  - count_full: Overlapping windows are fully counted (can inflate productivity). Use carefully.
- Category mapping: edit `config/category_mapping.json` to unify labels across systems.

Outputs:
- Per-agent KPIs: Productive Time, Scheduled Time, Utilization %, Idle Time.
- Team view: Heatmap by day/hour.
- Table: Average handling time by category (minutes).
- Export: `exports/per_agent_report.csv` or via in-app download button.

Notes:
- If you integrate ServiceNow or AWS Connect later, ingest their exports mapped to these columns.
- Ensure timestamps have consistent timezone and formats.
- For large files, consider batching or pre-aggregation to hourly buckets.

