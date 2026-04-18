import os
import json
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# =========================
# PAGE CONFIG
# =========================

st.set_page_config(
    page_title="Power BI Refresh History",
    layout="wide"
)

st.title("RCSAs Power BI Semantic Model – Refresh History")
st.caption("Incremental refresh audit & monitoring")

# =========================
# LOAD ENV VARIABLES (ROBUST)
# =========================

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "Arish_qa_data_validation.env"

if not ENV_PATH.exists():
    st.error(f".env file not found at: {ENV_PATH}")
    st.stop()

load_dotenv(ENV_PATH)

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
    st.error("Database environment variables are missing or empty.")
    st.stop()

MYSQL_CONNECTION_STRING = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}"
)

# =========================
# DB ENGINE
# =========================

@st.cache_resource
def get_engine():
    return create_engine(MYSQL_CONNECTION_STRING, pool_pre_ping=True)

engine = get_engine()

# =========================
# LOAD DATA (pandas 2.x SAFE)
# =========================

@st.cache_data(ttl=300)
def load_data():
    query = """
        SELECT
            refresh_id,
            workspace_id,
            dataset_id,
            refresh_type,
            start_time,
            end_time,
            duration_seconds,
            status,
            error_details,
            exported_at_utc
        FROM powerbi_refresh_history
        ORDER BY start_time DESC
    """

    # IMPORTANT: pandas 2.x requires DBAPI connection
    conn = engine.raw_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    # Ensure datetime types
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce")
    df["exported_at_utc"] = pd.to_datetime(df["exported_at_utc"], errors="coerce")

    return df


df = load_data()

if df.empty:
    st.warning("No refresh history data found.")
    st.stop()

# =========================
# SIDEBAR FILTERS
# =========================

st.sidebar.header("🔍 Filters")

status_filter = st.sidebar.multiselect(
    "Refresh Status",
    options=sorted(df["status"].dropna().unique()),
    default=sorted(df["status"].dropna().unique())
)

refresh_type_filter = st.sidebar.multiselect(
    "Refresh Type",
    options=sorted(df["refresh_type"].dropna().unique()),
    default=sorted(df["refresh_type"].dropna().unique())
)

min_date = df["start_time"].min().date()
max_date = df["start_time"].max().date()

date_range = st.sidebar.date_input(
    "Start Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# =========================
# APPLY FILTERS
# =========================

filtered_df = df[
    (df["status"].isin(status_filter)) &
    (df["refresh_type"].isin(refresh_type_filter)) &
    (df["start_time"].dt.date >= date_range[0]) &
    (df["start_time"].dt.date <= date_range[1])
]

# =========================
# KPI METRICS
# =========================

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Refreshes", len(filtered_df))
col2.metric("Completed", (filtered_df["status"] == "Completed").sum())
col3.metric("Failed", (filtered_df["status"] == "Failed").sum())

avg_duration = filtered_df["duration_seconds"].mean()
col4.metric(
    "Avg Duration (sec)",
    f"{int(avg_duration)}" if pd.notna(avg_duration) else "N/A"
)

st.divider()

# =========================
# MAIN TABLE
# =========================

st.subheader("📋 Refresh History")

display_df = filtered_df.copy()
display_df["duration_minutes"] = (
    display_df["duration_seconds"] / 60
).round(2)

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

# =========================
# FAILED REFRESH DETAILS
# =========================

st.subheader("❌ Failed Refresh Details")

failed_df = filtered_df[filtered_df["status"] == "Failed"]

if failed_df.empty:
    st.info("No failed refreshes in the selected range.")
else:
    for _, row in failed_df.iterrows():
        with st.expander(f"Refresh ID: {row['refresh_id']}"):
            st.write("**Start Time:**", row["start_time"])
            st.write("**End Time:**", row["end_time"])
            st.write("**Duration (sec):**", row["duration_seconds"])

            if row["error_details"]:
                try:
                    st.json(json.loads(row["error_details"]))
                except Exception:
                    st.text(row["error_details"])
            else:
                st.info("No error details available.")

# =========================
# EXPORT
# =========================

st.divider()
st.download_button(
    label="⬇️ Download Filtered Data (CSV)",
    data=filtered_df.to_csv(index=False),
    file_name="powerbi_refresh_history_filtered.csv",
    mime="text/csv"
)
