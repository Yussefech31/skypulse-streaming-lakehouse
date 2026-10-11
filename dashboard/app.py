"""SkyPulse - Interactive Live Aircraft Telemetry Lakehouse Dashboard."""

from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

from dashboard.airplane_icon import (
    AIRPLANE_ICON_MAPPING,
    AIRPLANE_ICON_URL,
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


@st.cache_data(ttl=20)
def load_data():
    """Load latest telemetry snapshot, enrich with real origin/dest airports, and compute metrics."""
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

    # Enrich flights with real origin and destination airports and airline networks
    valid_coords = df.dropna(subset=["latitude", "longitude"]).copy()
    if not valid_coords.empty:
        df = project_destination_coordinates(df)

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
    st.sidebar.header("🕹️ Flight Stream Filters")
    all_origin_countries = ["All Origin Countries"] + sorted(
        [str(c) for c in df["origin_country"].dropna().unique() if str(c)]
    )
    selected_origin = st.sidebar.selectbox("Filter by Registration Country", options=all_origin_countries)

    dest_countries_list = ["All Destination Countries"]
    if "dest_country" in map_df.columns:
        dest_countries_list += sorted(
            [str(c) for c in map_df["dest_country"].dropna().unique() if str(c)]
        )
    selected_dest = st.sidebar.selectbox("Filter by Destination Country", options=dest_countries_list)

    min_altitude = st.sidebar.slider(
        "Min Altitude (meters)",
        min_value=0,
        max_value=15000,
        value=0,
        step=500,
    )

    st.sidebar.markdown("---")
    st.sidebar.header("🗺️ Map & Flight Paths")
    show_destinations = st.sidebar.checkbox("✈️ Show Flight Destination Arcs", value=True)
    route_mode = st.sidebar.selectbox(
        "Flight Trajectory Mode",
        options=[
            "Aircraft ➔ Destination Airport (Active Leg)",
            "Origin ➔ Aircraft ➔ Destination (Full Leg)",
            "Origin ➔ Destination (Air Corridor)",
        ],
        index=0,
    )
    show_airport_pins = st.sidebar.checkbox("📍 Show Airport Destination Pins", value=True)
    plane_icon_size = st.sidebar.slider(
        "Aircraft Icon Size (px)",
        min_value=18,
        max_value=46,
        value=28,
        step=2,
    )

    if selected_origin != "All Origin Countries":
        map_df = map_df[map_df["origin_country"] == selected_origin]

    if selected_dest != "All Destination Countries" and "dest_country" in map_df.columns:
        map_df = map_df[map_df["dest_country"] == selected_dest]

    if "baro_altitude" in map_df.columns:
        map_df = map_df[map_df["baro_altitude"].fillna(0) >= min_altitude]

    # Prune map data to required display attributes for high performance
    display_cols = [
        "longitude",
        "latitude",
        "dest_longitude",
        "dest_latitude",
        "orig_longitude",
        "orig_latitude",
        "deck_angle",
        "plane_color",
        "icon",
        "callsign",
        "icao24",
        "origin_country",
        "orig_iata",
        "orig_name",
        "orig_city",
        "orig_country",
        "dest_iata",
        "dest_name",
        "dest_city",
        "dest_country",
        "airline_name",
        "route_label",
        "dist_to_dest_km",
        "total_route_km",
        "flight_progress_pct",
        "baro_altitude",
        "altitude_ft",
        "velocity_kmh",
        "velocity_kts",
        "heading_deg",
        "vertical_rate",
        "on_ground",
    ]
    avail_cols = [c for c in display_cols if c in map_df.columns]
    map_df = map_df[avail_cols].copy()
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
    st.subheader("🌐 Real-Time 3D Airspace Map with Real Flight Routes")

    # PyDeck Map View
    view_state = pdk.ViewState(
        latitude=float(map_df["latitude"].mean()) if not map_df.empty else 34.0,
        longitude=float(map_df["longitude"].mean()) if not map_df.empty else -4.0,
        zoom=3.0,
        pitch=40,
    )

    layers = []

    # 1. Real Route Arcs
    if show_destinations and not airborne_df.empty:
        if route_mode == "Aircraft ➔ Destination Airport (Active Leg)":
            arc_layer = pdk.Layer(
                "ArcLayer",
                data=airborne_df,
                get_source_position=["longitude", "latitude"],
                get_target_position=["dest_longitude", "dest_latitude"],
                get_source_color=[0, 229, 255, 190],     # Cyan glow at aircraft
                get_target_color=[255, 95, 109, 170],    # Coral red at destination airport
                get_width=2.5,
                pickable=True,
                auto_highlight=True,
            )
            layers.append(arc_layer)

        elif route_mode == "Origin ➔ Aircraft ➔ Destination (Full Leg)":
            # Origin to Aircraft (completed leg)
            leg1_layer = pdk.Layer(
                "ArcLayer",
                data=airborne_df,
                get_source_position=["orig_longitude", "orig_latitude"],
                get_target_position=["longitude", "latitude"],
                get_source_color=[0, 255, 194, 110],    # Emerald at origin
                get_target_color=[0, 229, 255, 180],    # Cyan at aircraft
                get_width=1.8,
                pickable=True,
            )
            # Aircraft to Destination (remaining leg)
            leg2_layer = pdk.Layer(
                "ArcLayer",
                data=airborne_df,
                get_source_position=["longitude", "latitude"],
                get_target_position=["dest_longitude", "dest_latitude"],
                get_source_color=[0, 229, 255, 190],    # Cyan at aircraft
                get_target_color=[255, 95, 109, 180],   # Coral at destination
                get_width=2.5,
                pickable=True,
                auto_highlight=True,
            )
            layers.extend([leg1_layer, leg2_layer])

        elif route_mode == "Origin ➔ Destination (Air Corridor)":
            corridor_layer = pdk.Layer(
                "ArcLayer",
                data=airborne_df,
                get_source_position=["orig_longitude", "orig_latitude"],
                get_target_position=["dest_longitude", "dest_latitude"],
                get_source_color=[0, 255, 194, 150],
                get_target_color=[255, 95, 109, 170],
                get_width=2.2,
                pickable=True,
                auto_highlight=True,
            )
            layers.append(corridor_layer)

    # 2. Real Airport Destination & Origin Pins
    if show_airport_pins and not airborne_df.empty:
        # Extract unique destination airports
        unique_dests = airborne_df.drop_duplicates(subset=["dest_iata"]).copy()
        dest_beacons = pdk.Layer(
            "ScatterplotLayer",
            data=unique_dests,
            get_position=["dest_longitude", "dest_latitude"],
            get_color=[255, 95, 109, 210],
            get_radius=12000,
            pickable=True,
        )
        dest_labels = pdk.Layer(
            "TextLayer",
            data=unique_dests,
            get_position=["dest_longitude", "dest_latitude"],
            get_text="dest_iata",
            get_size=13,
            get_color=[255, 255, 255, 255],
            get_pixel_offset=[0, -14],
        )
        layers.extend([dest_beacons, dest_labels])

    # 3. Airplane IconLayer (oriented to true heading)
    plane_layer = pdk.Layer(
        "IconLayer",
        data=map_df,
        icon_atlas=AIRPLANE_ICON_URL,
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
            <div style="font-family: sans-serif; line-height: 1.5; font-size: 13px;">
                <b style="color: #00e5ff; font-size: 15px;">✈️ Flight: {callsign} &nbsp;·&nbsp; {airline_name}</b><br/>
                <div style="margin: 6px 0; padding: 8px 10px; background: rgba(255, 255, 255, 0.08); border-radius: 6px; border: 1px solid rgba(255, 255, 255, 0.14);">
                    <span style="color: #00ffc2; font-weight: 700;">🛫 Origin:</span> <b>{orig_iata}</b> — {orig_name}<br/>
                    <span style="color: #94a3b8; font-size: 11px; margin-left: 20px;">{orig_city}, {orig_country}</span><br/>
                    <span style="color: #ff5f6d; font-weight: 700;">🛬 Destination:</span> <b>{dest_iata}</b> — {dest_name}<br/>
                    <span style="color: #94a3b8; font-size: 11px; margin-left: 20px;">{dest_city}, {dest_country}</span><br/>
                    <div style="margin-top: 5px; font-size: 11px; color: #cbd5e1;">
                        <b>Route:</b> {route_label} &nbsp;|&nbsp; <b>Remaining:</b> {dist_to_dest_km} km ({flight_progress_pct}%)
                    </div>
                </div>
                <b>ICAO24:</b> <code>{icao24}</code> &nbsp;|&nbsp; <b>Registration Country:</b> {origin_country}<br/>
                <b>Altitude:</b> {baro_altitude} m ({altitude_ft} ft) &nbsp;|&nbsp; <b>Groundspeed:</b> {velocity_kmh} km/h ({velocity_kts} kts)<br/>
                <b>Heading / Track:</b> {heading_deg}° &nbsp;|&nbsp; <b>Vertical Rate:</b> {vertical_rate} m/s
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

    # Map Legend & Airport Info
    col_leg1, col_leg2, col_leg3 = st.columns(3)
    with col_leg1:
        st.caption("✈️ **Aircraft Icon**: Oriented to heading direction (0° North).")
    with col_leg2:
        st.caption("🟡 **Amber**: Cruise (>9000m) &nbsp;|&nbsp; 🔵 **Cyan**: Mid-alt &nbsp;|&nbsp; 🟢 **Green**: Climb/Descent")
    with col_leg3:
        st.caption("🛫 **Origin** (Emerald) ➔ 🛬 **Destination Airport** (Coral Red runway beacon).")

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
