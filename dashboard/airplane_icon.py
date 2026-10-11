"""Airplane icon generation and destination geodetic projection for SkyPulse."""

from __future__ import annotations

import base64
from io import BytesIO

import numpy as np
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


# Global cached icon atlas & mapping
AIRPLANE_ICON_DATA_URI = generate_airplane_icon_b64(size=64)

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


def project_destination_coordinates(
    df: pd.DataFrame,
    default_distance_km: float = 200.0,
) -> pd.DataFrame:
    """Project destination coordinates along each aircraft's heading using Great Circle geodesy."""
    res_df = df.copy()

    # Earth radius in kilometers
    r_earth = 6371.0

    # Dynamic lookahead distance proportional to velocity if available, else default
    if "velocity" in res_df.columns:
        # velocity in m/s -> km in 20 minutes: v * 1.2
        vel = res_df["velocity"].fillna(200.0)
        dist_km = np.clip(vel * 1.2, 80.0, 450.0)
    else:
        dist_km = np.full(len(res_df), default_distance_km)

    res_df["proj_distance_km"] = dist_km.round(1)
    d_rad = dist_km / r_earth

    lat_rad = np.radians(res_df["latitude"].fillna(0.0))
    lon_rad = np.radians(res_df["longitude"].fillna(0.0))

    # Heading (track) in radians clockwise from North
    track_deg = res_df["true_track"].fillna(0.0)
    track_rad = np.radians(track_deg)

    # Great Circle destination formulas
    dest_lat_rad = np.arcsin(
        np.sin(lat_rad) * np.cos(d_rad)
        + np.cos(lat_rad) * np.sin(d_rad) * np.cos(track_rad)
    )
    dest_lon_rad = lon_rad + np.arctan2(
        np.sin(track_rad) * np.sin(d_rad) * np.cos(lat_rad),
        np.cos(d_rad) - np.sin(lat_rad) * np.sin(dest_lat_rad),
    )

    res_df["dest_latitude"] = np.degrees(dest_lat_rad).round(4)
    res_df["dest_longitude"] = ((np.degrees(dest_lon_rad) + 540) % 360 - 180).round(4)

    # PyDeck angle: deck.gl rotates counter-clockwise from 0 (North).
    # Heading is clockwise from North -> (360 - track_deg) % 360
    res_df["deck_angle"] = (360.0 - track_deg) % 360.0
    res_df["icon"] = "airplane"

    # Color palette based on altitude:
    # Low (< 3000m): Greenish-cyan [0, 255, 180, 240]
    # Mid (3000-9000m): Electric Cyan [0, 210, 255, 240]
    # High cruise (> 9000m): Bright Amber / Gold [255, 200, 60, 240]
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

    # Formatted columns for tooltip
    res_df["velocity_kmh"] = (res_df["velocity"].fillna(0) * 3.6).round(0).astype(int)
    res_df["velocity_kts"] = (res_df["velocity"].fillna(0) * 1.94384).round(0).astype(int)
    res_df["altitude_ft"] = (res_df["baro_altitude"].fillna(0) * 3.28084).round(0).astype(int)
    res_df["heading_deg"] = track_deg.round(0).astype(int)

    return res_df
