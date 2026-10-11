"""SkyPulse - Interactive Live Aircraft Telemetry Lakehouse Dashboard."""

from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

from dashboard.airplane_icon import (
    AIRPLANE_ICON_DATA_URI,
    AIRPLANE_ICON_MAPPING,
    project_destination_coordinates,
)
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
    .legend-badge {
        display: inline-block;
        padding: 4px 10px;
        margin-right: 8px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
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

    st.sidebar.markdown("---")
    st.sidebar.header("🗺️ Map & Visualization")
    show_destinations = st.sidebar.checkbox("✈️ Show Destination Lines", value=True)
    line_style = st.sidebar.selectbox(
        "Destination Line Style",
        options=["3D Great-Circle Arcs", "Direct Trajectory Vectors", "Both (Arcs + Vectors)"],
        index=0,
    )
    lookahead_km = st.sidebar.slider(
        "Lookahead Distance (km)",
        min_value=80,
        max_value=500,
        value=250,
        step=20,
        help="Projected flight horizon based on current heading and groundspeed",
    )
    plane_icon_size = st.sidebar.slider(
        "Aircraft Icon Size (px)",
        min_value=18,
        max_value=46,
        value=28,
        step=2,
    )

    if selected_country != "All Countries":
        map_df = map_df[map_df["origin_country"] == selected_country]

    if "baro_altitude" in map_df.columns:
        map_df = map_df[map_df["baro_altitude"].fillna(0) >= min_altitude]

    # Geodetic destination calculations & rotation heading angles
    map_df = project_destination_coordinates(map_df, default_distance_km=lookahead_km)
    airborne_df = map_df[~map_df["on_ground"].fillna(False)].copy()

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
    st.subheader("🌐 Real-Time 3D Airspace Map with Flight Trajectories")

    # PyDeck Map Layers
    view_state = pdk.ViewState(
        latitude=float(map_df["latitude"].mean()) if not map_df.empty else 34.0,
        longitude=float(map_df["longitude"].mean()) if not map_df.empty else -4.0,
        zoom=3.0,
        pitch=40,
    )

    layers = []

    # 1. Destination Lines / Arcs
    if show_destinations and not airborne_df.empty:
        if line_style in ["3D Great-Circle Arcs", "Both (Arcs + Vectors)"]:
            arc_layer = pdk.Layer(
                "ArcLayer",
                data=airborne_df,
                get_source_position=["longitude", "latitude"],
                get_target_position=["dest_longitude", "dest_latitude"],
                get_source_color=[0, 229, 255, 190],     # Cyan glow at aircraft
                get_target_color=[255, 95, 109, 130],    # Coral red at destination
                get_width=2.5,
                pickable=True,
                auto_highlight=True,
            )
            layers.append(arc_layer)

        if line_style in ["Direct Trajectory Vectors", "Both (Arcs + Vectors)"]:
            vector_layer = pdk.Layer(
                "LineLayer",
                data=airborne_df,
                get_source_position=["longitude", "latitude"],
                get_target_position=["dest_longitude", "dest_latitude"],
                get_color=[0, 229, 255, 150],
                get_width=2.0,
                pickable=True,
            )
            layers.append(vector_layer)

        # Destination Beacon Points
        dest_point_layer = pdk.Layer(
            "ScatterplotLayer",
            data=airborne_df,
            get_position=["dest_longitude", "dest_latitude"],
            get_color=[255, 95, 109, 180],
            get_radius=5000,
            pickable=True,
        )
        layers.append(dest_point_layer)

    # 2. Airplane IconLayer (oriented to true heading)
    plane_layer = pdk.Layer(
        "IconLayer",
        data=map_df,
        icon_atlas=AIRPLANE_ICON_DATA_URI,
        icon_mapping=AIRPLANE_ICON_MAPPING,
        get_icon="icon",
        get_position=["longitude", "latitude"],
        get_color="plane_color",
        get_size=plane_icon_size,
        get_angle="deck_angle",
        pickable=True,
        auto_highlight=True,
    )
    layers.append(plane_layer)

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip={
            "html": """
            <div style="font-family: sans-serif; line-height: 1.45; font-size: 13px;">
                <b style="color: #00e5ff; font-size: 14px;">✈️ Flight: {callsign}</b><br/>
                <b>ICAO24:</b> <code>{icao24}</code> &nbsp;|&nbsp; <b>Country:</b> {origin_country}<br/>
                <b>Altitude:</b> {baro_altitude} m ({altitude_ft} ft)<br/>
                <b>Groundspeed:</b> {velocity_kmh} km/h ({velocity_kts} kts)<br/>
                <b>Heading / Track:</b> {heading_deg}°<br/>
                <b>Vertical Rate:</b> {vertical_rate} m/s<br/>
                <hr style="border: 0; border-top: 1px solid #3d4251; margin: 5px 0;"/>
                <b style="color: #ff5f6d;">📍 Projected Destination:</b> {dest_latitude}°, {dest_longitude}°<br/>
                <b>Flight Horizon:</b> ~{proj_distance_km} km ahead
            </div>
            """,
            "style": {
                "backgroundColor": "#13161f",
                "color": "#e0e6ed",
                "borderRadius": "8px",
                "padding": "12px",
                "border": "1px solid #2d3345",
            },
        },
    )
    st.pydeck_chart(deck)

    # Map Legend & Controls Note
    col_leg1, col_leg2, col_leg3 = st.columns(3)
    with col_leg1:
        st.caption("✈️ **Aircraft Icon**: Oriented to heading direction (0° North).")
    with col_leg2:
        st.caption("🟡 **Amber**: Cruise (>9000m) &nbsp;|&nbsp; 🔵 **Cyan**: Mid-alt &nbsp;|&nbsp; 🟢 **Green**: Climb/Descent")
    with col_leg3:
        st.caption("🌈 **Destination Arcs**: Cyan (Aircraft) ➔ Coral Red (Projected Destination).")

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
            st.dataframe(agg_df[display_cols].head(10), width="stretch")
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
            width="stretch",
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
