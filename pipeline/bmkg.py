"""Fetch, parse and validate the BMKG 'latest earthquakes' feed (M5.0+, last 15 events)."""
from datetime import datetime

import requests

FEED_URL = "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"

# Rough bounding box of BMKG's monitoring area (Indonesia plus surrounding seas).
LAT_RANGE = (-15.0, 10.0)
LON_RANGE = (90.0, 145.0)


def fetch(url: str = FEED_URL, timeout: int = 30) -> dict:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def parse(feed: dict) -> list[dict]:
    """Turn raw feed records into typed rows. Raises on a record with an unexpected shape."""
    rows = []
    for rec in feed["Infogempa"]["gempa"]:
        lat, lon = (float(x) for x in rec["Coordinates"].split(","))
        rows.append(
            {
                "event_time": datetime.fromisoformat(rec["DateTime"]),
                "latitude": lat,
                "longitude": lon,
                "magnitude": float(rec["Magnitude"]),
                "depth_km": int(rec["Kedalaman"].split()[0]),  # "120 km" -> 120
                "region": rec["Wilayah"].strip(),
                "tsunami_potential": rec["Potensi"].strip(),
            }
        )
    return rows


def validate(row: dict) -> list[str]:
    """Return the problems found in one parsed row (empty list = valid)."""
    problems = []
    if row["event_time"].tzinfo is None:
        problems.append("event_time has no timezone")
    if not 0 <= row["magnitude"] <= 10:
        problems.append(f"magnitude out of range: {row['magnitude']}")
    if not 0 <= row["depth_km"] <= 800:
        problems.append(f"depth out of range: {row['depth_km']}")
    if not (LAT_RANGE[0] <= row["latitude"] <= LAT_RANGE[1] and LON_RANGE[0] <= row["longitude"] <= LON_RANGE[1]):
        problems.append(f"coordinates outside BMKG area: {row['latitude']},{row['longitude']}")
    if not row["region"]:
        problems.append("region is empty")
    return problems
