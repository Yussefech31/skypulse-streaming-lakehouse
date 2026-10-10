"""SkyPulse - Interactive Live Aircraft Telemetry Lakehouse Dashboard."""

from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

from src.ingestion.opensky_client import OpenSkyClient
from src.lakehouse.storage import LakehouseStorage
from src.streaming.anomaly_detector import AnomalyDetector
from src.streaming.window_aggregations import WindowAggregator

st.set_page_config(
    page_title="SkyPulse | Streaming Aircraft Lakehouse",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .main { background-color: #0e1117; }
    .metric-card {
        background-color: #1a1c24;
        border-radius: 8px;
        padding: 16px;
        border: 1px solid #2d3139;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=15)
def load_data():
    """Load latest telemetry snapshot and compute metrics."""
    client = OpenSkyClient()
    records = client.fetch_telemetry_batch()

    rows = [r.model_dump() for r in records]
    df = pd.DataFrame(rows)

    # Detect anomalies
    detector = AnomalyDetector()
    anomalies = detector.process_batch(records)
    anom_df = pd.DataFrame([a.to_dict() for a in anomalies]) if anomalies else pd.DataFrame()

    # Window aggregations
    aggregator = WindowAggregator(window_minutes=5)
    metrics = aggregator.compute_windowed_aggregations(records)
    agg_df = pd.DataFrame([m.to_dict() for m in metrics]) if metrics else pd.DataFrame()

    return df, anom_df, agg_df


def main():
    st.title("✈️ SkyPulse — Live Aircraft Telemetry Lakehouse")
    st.caption(
        "Real-time ADS-B telemetry ingestion from OpenSky Network → Kafka → "
        "Stream Processing → Lakehouse Parquet/Delta on MinIO."
    )

    df, anom_df, agg_df = load_data()

    if df.empty:
        st.warning("No telemetry records currently available.")
        return

    # Filter out records without valid coordinates for map
    map_df = df.dropna(subset=["latitude", "longitude"]).copy()

    # Sidebar controls
    st.sidebar.header("🕹️ Stream Filters")
    selected_country = st.sidebar.selectbox(
        "Filter by Origin Country",
        options=["All Countries"] + sorted(df["origin_country"].unique()),
    )
    min_altitude = st.sidebar.slider(
        "Min Altitude (meters)",
        min_value=0,
        max_value=15000,
        value=0,
        step=500,
    )

    if selected_country != "All Countries":
        map_df = map_df[map_df["origin_country"] == selected_country]

    if "baro_altitude" in map_df.columns:
        map_df = map_df[map_df["baro_altitude"].fillna(0) >= min_altitude]

    # KPI Top Row
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Tracked Aircraft", f"{len(df):,}")
    with col2:
        airborne = int((~df["on_ground"]).sum())
        st.metric("Airborne Flights", f"{airborne:,}", f"{(airborne / len(df) * 100):.1f}%")
    with col3:
        avg_alt = df["baro_altitude"].dropna().mean()
        st.metric("Avg Cruise Altitude", f"{avg_alt:.0f} m" if pd.notna(avg_alt) else "N/A")
    with col4:
        avg_speed = (df["velocity"].dropna() * 3.6).mean()
        st.metric("Avg Groundspeed", f"{avg_speed:.0f} km/h" if pd.notna(avg_speed) else "N/A")
    with col5:
        st.metric("Anomalies Flagged", f"{len(anom_df):,}", delta_color="inverse")

    st.markdown("---")

    # Map Section
    st.subheader("🌐 Real-Time 3D Airspace Map")

    # PyDeck Map Layer
    view_state = pdk.ViewState(
        latitude=float(map_df["latitude"].mean()) if not map_df.empty else 32.0,
        longitude=float(map_df["longitude"].mean()) if not map_df.empty else -5.0,
        zoom=2.5,
        pitch=35,
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position=["longitude", "latitude"],
        get_color="[255, (255 - (velocity * 0.8)), 50, 180]",
        get_radius=25000,
        pickable=True,
        auto_highlight=True,
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": """
            <b>Flight Callsign:</b> {callsign}<br/>
            <b>ICAO24:</b> {icao24}<br/>
            <b>Origin:</b> {origin_country}<br/>
            <b>Altitude:</b> {baro_altitude} m<br/>
            <b>Velocity:</b> {velocity} m/s ({velocity_kmh} km/h)<br/>
            <b>Vertical Rate:</b> {vertical_rate} m/s
            """,
            "style": {"backgroundColor": "#1e212b", "color": "#ffffff"},
        },
    )
    st.pydeck_chart(deck)

    # Analytics Section
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📊 Top Origin Countries (Active Flights)")
        top_countries = df["origin_country"].value_counts().head(10)
        st.bar_chart(top_countries)

    with col_right:
        st.subheader("⚡ 5-Minute Window Aggregation Rollup")
        if not agg_df.empty:
            display_cols = [
                "region",
                "unique_aircraft_count",
                "avg_altitude_m",
                "avg_velocity_mps",
                "climbing_count",
                "descending_count",
            ]
            st.dataframe(agg_df[display_cols].head(10), use_container_width=True)
        else:
            st.info("No window aggregations computed yet.")

    # Anomaly Feed
    st.subheader("🚨 Real-Time Flight Anomalies Feed")
    if not anom_df.empty:
        st.dataframe(
            anom_df[
                [
                    "timestamp",
                    "severity",
                    "anomaly_type",
                    "icao24",
                    "callsign",
                    "origin_country",
                    "description",
                ]
            ],
            use_container_width=True,
        )
    else:
        st.success("No active anomalies detected across the current telemetry window.")

    # Lakehouse Storage Stats
    st.subheader("🏛️ Lakehouse Architecture & Storage Health")
    storage = LakehouseStorage()
    silver_files = list(storage.silver_dir.glob("**/*.parquet"))
    gold_files = list(storage.gold_agg_dir.glob("**/*.parquet"))

    st.write(f"- **Silver Parquet Partitions (Raw Enriched):** {len(silver_files)} partition files")
    st.write(
        f"- **Gold Parquet Partitions (Aggregations & Marts):** {len(gold_files)} partition files"
    )
    st.write(
        "- **Exactly-Once Semantic:** Checkpoint offsets tracked per Kafka topic-partition with idempotent Parquet partition overwrites."
    )


if __name__ == "__main__":
    main()
