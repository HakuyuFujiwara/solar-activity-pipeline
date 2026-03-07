"""Streamlit dashboard for solar activity data visualization.

Provides an interactive view of ingested solar observations, anomaly flags,
and data source coverage. Replaces the manual process of eyeballing .dat
files for verification.

Run with: streamlit run src/dashboard/app.py
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

from src.config import settings

# Page config
st.set_page_config(
    page_title="Solar Activity Pipeline",
    page_icon="☀️",
    layout="wide",
)


@st.cache_resource
def get_engine():
    """Create and cache the database engine."""
    return create_engine(settings.database_url)


def load_observations(engine, start: date, end: date) -> pd.DataFrame:
    """Load observations from database into a DataFrame."""
    query = text("""
        SELECT observation_date, source, ra, international_sunspot_number as isn,
               f10_7, ap_index
        FROM solar_observations
        WHERE observation_date >= :start AND observation_date <= :end
        ORDER BY observation_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start": start, "end": end})
    return df


def load_anomalies(engine, start: date, end: date) -> pd.DataFrame:
    """Load anomalies from database into a DataFrame."""
    query = text("""
        SELECT observation_date, field, value, zscore, mean, std, severity
        FROM anomalies
        WHERE observation_date >= :start AND observation_date <= :end
        ORDER BY observation_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start": start, "end": end})
    return df


def main() -> None:
    """Main dashboard entry point."""
    st.title("☀️ Solar Activity Pipeline Dashboard")
    st.caption(
        "Automated monitoring of solar activity indices from AAVSO, NOAA, and SILSO"
    )

    engine = get_engine()

    # Sidebar filters
    st.sidebar.header("Filters")
    default_end = date.today()
    default_start = default_end - timedelta(days=365)

    start_date = st.sidebar.date_input("Start date", value=default_start)
    end_date = st.sidebar.date_input("End date", value=default_end)

    if start_date >= end_date:
        st.error("Start date must be before end date.")
        return

    # Load data
    try:
        obs_df = load_observations(engine, start_date, end_date)
        anom_df = load_anomalies(engine, start_date, end_date)
    except Exception as exc:
        st.warning(
            f"Could not load data. Run the pipeline first: "
            f"`python -m src.pipeline --start-date 2025-03-01 --end-date 2025-03-31`"
            f"\n\nError: {exc}"
        )
        return

    if obs_df.empty:
        st.info("No data found for this date range. Run the pipeline to ingest data.")
        return

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Days with Data", obs_df["observation_date"].nunique())
    with col2:
        st.metric("Active Sources", obs_df["source"].nunique())
    with col3:
        st.metric("Total Records", len(obs_df))
    with col4:
        anomaly_count = len(anom_df) if not anom_df.empty else 0
        st.metric("Anomalies", anomaly_count)

    st.divider()

    # Time series chart
    st.subheader("Solar Activity Over Time")
    metric = st.selectbox(
        "Select metric",
        ["ra", "isn", "f10_7"],
        format_func=lambda x: {
            "ra": "Relative Sunspot Number (Ra) — AAVSO",
            "isn": "International Sunspot Number (ISN) — SILSO",
            "f10_7": "F10.7 Radio Flux — NOAA",
        }.get(x, x),
    )

    plot_df = obs_df[obs_df[metric].notna()].copy()
    if plot_df.empty:
        st.info(f"No {metric} data available for this date range.")
    else:
        fig = px.line(
            plot_df,
            x="observation_date",
            y=metric,
            color="source",
            markers=True,
            title=f"{metric.upper()} Over Time",
            labels={"observation_date": "Date", metric: metric.upper()},
        )

        # Add anomaly markers if any
        if not anom_df.empty:
            field_anomalies = anom_df[anom_df["field"] == metric]
            if not field_anomalies.empty:
                fig.add_trace(
                    go.Scatter(
                        x=field_anomalies["observation_date"],
                        y=field_anomalies["value"],
                        mode="markers",
                        marker=dict(size=14, color="red", symbol="x", line=dict(width=2)),
                        name="Anomaly",
                    )
                )

        fig.update_layout(height=450, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Ra vs ISN comparison
    st.subheader("AAVSO Ra vs SILSO ISN Comparison")

    aavso_df = obs_df[obs_df["source"] == "aavso"][["observation_date", "ra"]].dropna()
    silso_df = obs_df[obs_df["source"] == "silso"][["observation_date", "isn"]].dropna()

    if not aavso_df.empty and not silso_df.empty:
        merged = pd.merge(aavso_df, silso_df, on="observation_date", how="inner")
        if not merged.empty:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=merged["observation_date"], y=merged["ra"],
                mode="lines+markers", name="AAVSO Ra",
            ))
            fig2.add_trace(go.Scatter(
                x=merged["observation_date"], y=merged["isn"],
                mode="lines+markers", name="SILSO ISN",
            ))
            fig2.update_layout(
                height=400,
                title="Daily Comparison: Ra vs ISN",
                xaxis_title="Date",
                yaxis_title="Sunspot Number",
                hovermode="x unified",
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No overlapping dates between AAVSO and SILSO data.")
    else:
        st.info("Need both AAVSO and SILSO data for comparison.")

    st.divider()

    # Source coverage
    st.subheader("Data Source Coverage")
    coverage = obs_df.groupby("source")["observation_date"].nunique().reset_index()
    coverage.columns = ["Source", "Days"]
    fig3 = px.bar(coverage, x="Source", y="Days", color="Source", title="Days of Data by Source")
    fig3.update_layout(height=300)
    st.plotly_chart(fig3, use_container_width=True)

    # Raw data explorer
    with st.expander("📊 Raw Data Explorer"):
        st.dataframe(obs_df, use_container_width=True)

    # Anomalies table
    if not anom_df.empty:
        with st.expander("⚠️ Detected Anomalies"):
            st.dataframe(anom_df, use_container_width=True)


if __name__ == "__main__":
    main()

