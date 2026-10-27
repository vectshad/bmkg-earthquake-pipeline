"""Data quality checks run after each load. Each query returns the number of bad rows (0 = pass)."""

CHECKS = {
    "magnitude_in_range": "SELECT count(*) FROM core.earthquakes WHERE magnitude NOT BETWEEN 0 AND 10",
    "depth_in_range": "SELECT count(*) FROM core.earthquakes WHERE depth_km NOT BETWEEN 0 AND 800",
    "coordinates_in_bmkg_area": """
        SELECT count(*) FROM core.earthquakes
        WHERE latitude NOT BETWEEN -15 AND 10 OR longitude NOT BETWEEN 90 AND 145""",
    "no_future_events": "SELECT count(*) FROM core.earthquakes WHERE event_time > now() + interval '1 hour'",
    "latest_snapshot_not_empty": """
        SELECT count(*) FROM (
            SELECT row_count FROM raw.feed_snapshots ORDER BY snapshot_id DESC LIMIT 1
        ) s WHERE coalesce(row_count, 0) = 0""",
    "latest_snapshot_no_rejects": """
        SELECT coalesce(sum(rejected_count), 0) FROM (
            SELECT rejected_count FROM raw.feed_snapshots ORDER BY snapshot_id DESC LIMIT 1
        ) s""",
}
