"""Airplane icon generation and destination geodetic projection for SkyPulse."""

from __future__ import annotations

import base64
from io import BytesIO

import pandas as pd
from PIL import Image, ImageDraw


def generate_airplane_icon_b64(
    size: int = 64,
    color: tuple[int, int, int, int] = (255, 255, 255, 255),
    outline_color: tuple[int, int, int, int] = (240, 240, 240, 255),
) -> str:
    """Generate a crisp, top-down jet airplane icon pointing straight UP (0° / North)."""
    scale = 4
    canvas_size = size * scale
    img = Image.new("RGBA", (canvas_size, canvas_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    cx = canvas_size / 2.0
    cy = canvas_size / 2.0
    h = canvas_size * 0.86
    w = canvas_size * 0.78

    nose_y = cy - h * 0.48
    tail_y = cy + h * 0.46

    # Jet airplane silhouette polygon (symmetrical)
    poly = [
        (cx, nose_y),                        # Nose tip
        (cx + w * 0.07, nose_y + h * 0.16),  # Cockpit right
        (cx + w * 0.08, nose_y + h * 0.36),  # Wing root front right
        (cx + w * 0.48, nose_y + h * 0.52),  # Wing tip right
        (cx + w * 0.44, nose_y + h * 0.60),  # Wing tip rear right
        (cx + w * 0.10, nose_y + h * 0.56),  # Wing root rear right
        (cx + w * 0.08, nose_y + h * 0.78),  # Fuselage rear right
        (cx + w * 0.24, nose_y + h * 0.90),  # Horizontal stabilizer tip right
        (cx + w * 0.20, nose_y + h * 0.96),  # Stabilizer rear right
        (cx + w * 0.04, tail_y),             # Tail right
        (cx, tail_y + h * 0.02),             # Tail tip
        (cx - w * 0.04, tail_y),             # Tail left
        (cx - w * 0.20, nose_y + h * 0.96),  # Stabilizer rear left
        (cx - w * 0.24, nose_y + h * 0.90),  # Stabilizer tip left
        (cx - w * 0.08, nose_y + h * 0.78),  # Fuselage rear left
        (cx - w * 0.10, nose_y + h * 0.56),  # Wing root rear left
        (cx - w * 0.44, nose_y + h * 0.60),  # Wing tip rear left
        (cx - w * 0.48, nose_y + h * 0.52),  # Wing tip left
        (cx - w * 0.08, nose_y + h * 0.36),  # Wing root front left
        (cx - w * 0.07, nose_y + h * 0.16),  # Cockpit left
    ]

    # Subtle drop shadow
    shadow_poly = [(x + 2 * scale, y + 2 * scale) for x, y in poly]
    draw.polygon(shadow_poly, fill=(0, 0, 0, 100))

    # Main fuselage and wings
    draw.polygon(poly, fill=color, outline=outline_color, width=max(1, scale))

    # Cockpit window accent
    cw_y = nose_y + h * 0.14
    draw.ellipse(
        [cx - w * 0.035, cw_y, cx + w * 0.035, cw_y + h * 0.07],
        fill=(30, 40, 50, 230),
    )

    final_img = img.resize((size, size), Image.Resampling.LANCZOS)
    buf = BytesIO()
    final_img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{b64}"


# Public CORS-enabled HTTPS URL for aircraft icon atlas
AIRPLANE_ICON_URL = (
    "https://raw.githubusercontent.com/Yussefech31/skypulse-streaming-lakehouse/main/dashboard/airplane_icon.png"
)

AIRPLANE_ICON_MAPPING = {
    "airplane": {
        "x": 0,
        "y": 0,
        "width": 64,
        "height": 64,
        "anchorX": 32,
        "anchorY": 32,
        "mask": True,
    }
}


def enrich_flight_routes_and_airports(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich each flight with real Origin & Destination airports, airline name, and coordinates."""
    from dashboard.airports import haversine_distance_km, resolve_flight_route

    res_df = df.copy()

    orig_iatas, orig_names, orig_cities, orig_countries, orig_lats, orig_lons = [], [], [], [], [], []
    dest_iatas, dest_names, dest_cities, dest_countries, dest_lats, dest_lons = [], [], [], [], [], []
    airlines, route_labels, dist_to_dests, total_dists, progress_pcts = [], [], [], [], []

    for _, row in res_df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        track = row.get("true_track") or 0.0

        if pd.isna(lat) or pd.isna(lon):
            orig_iatas.append("N/A")
            orig_names.append("Unknown")
            orig_cities.append("Unknown")
            orig_countries.append("Unknown")
            orig_lats.append(0.0)
            orig_lons.append(0.0)
            dest_iatas.append("N/A")
            dest_names.append("Unknown")
            dest_cities.append("Unknown")
            dest_countries.append("Unknown")
            dest_lats.append(0.0)
            dest_lons.append(0.0)
            airlines.append("General Aviation")
            route_labels.append("N/A")
            dist_to_dests.append(0.0)
            total_dists.append(0.0)
            progress_pcts.append(0.0)
            continue

        orig, dest, airline = resolve_flight_route(
            float(lat),
            float(lon),
            float(track),
            callsign=str(row.get("callsign", "")),
            origin_country=str(row.get("origin_country", "")),
            altitude_m=float(row["baro_altitude"]) if pd.notna(row.get("baro_altitude")) else None,
            vertical_rate=float(row["vertical_rate"]) if pd.notna(row.get("vertical_rate")) else None,
        )

        d_rem = haversine_distance_km(float(lat), float(lon), dest.latitude, dest.longitude)
        d_tot = haversine_distance_km(orig.latitude, orig.longitude, dest.latitude, dest.longitude)
        prog = min(max(((d_tot - d_rem) / max(d_tot, 1.0)) * 100.0, 5.0), 98.0)

        orig_iatas.append(orig.iata)
        orig_names.append(orig.name)
        orig_cities.append(orig.city)
        orig_countries.append(orig.country)
        orig_lats.append(round(orig.latitude, 4))
        orig_lons.append(round(orig.longitude, 4))

        dest_iatas.append(dest.iata)
        dest_names.append(dest.name)
        dest_cities.append(dest.city)
        dest_countries.append(dest.country)
        dest_lats.append(round(dest.latitude, 4))
        dest_lons.append(round(dest.longitude, 4))

        airlines.append(airline)
        route_labels.append(f"{orig.iata} ➔ {dest.iata}")
        dist_to_dests.append(round(d_rem, 1))
        total_dists.append(round(d_tot, 1))
        progress_pcts.append(round(prog, 0))

    res_df["orig_iata"] = orig_iatas
    res_df["orig_name"] = orig_names
    res_df["orig_city"] = orig_cities
    res_df["orig_country"] = orig_countries
    res_df["orig_latitude"] = orig_lats
    res_df["orig_longitude"] = orig_lons

    res_df["dest_iata"] = dest_iatas
    res_df["dest_name"] = dest_names
    res_df["dest_city"] = dest_cities
    res_df["dest_country"] = dest_countries
    res_df["dest_latitude"] = dest_lats
    res_df["dest_longitude"] = dest_lons

    res_df["airline_name"] = airlines
    res_df["route_label"] = route_labels
    res_df["dist_to_dest_km"] = dist_to_dests
    res_df["total_route_km"] = total_dists
    res_df["flight_progress_pct"] = progress_pcts

    # PyDeck angle: deck.gl rotates counter-clockwise from 0 (North).
    tracks = res_df["true_track"].fillna(0.0)
    res_df["deck_angle"] = (360.0 - tracks) % 360.0
    res_df["icon"] = "airplane"

    # Color palette based on altitude:
    alts = res_df["baro_altitude"].fillna(0.0)
    colors = []
    for alt in alts:
        if alt < 3000:
            colors.append([0, 255, 180, 240])
        elif alt < 9000:
            colors.append([0, 210, 255, 240])
        else:
            colors.append([255, 205, 50, 240])
    res_df["plane_color"] = colors

    # Units
    res_df["velocity_kmh"] = (res_df["velocity"].fillna(0) * 3.6).round(0).astype(int)
    res_df["velocity_kts"] = (res_df["velocity"].fillna(0) * 1.94384).round(0).astype(int)
    res_df["altitude_ft"] = (res_df["baro_altitude"].fillna(0) * 3.28084).round(0).astype(int)
    res_df["heading_deg"] = tracks.round(0).astype(int)

    return res_df


def project_destination_coordinates(
    df: pd.DataFrame,
    default_distance_km: float = 200.0,
) -> pd.DataFrame:
    """Convenience alias using real airport route resolution."""
    return enrich_flight_routes_and_airports(df)
