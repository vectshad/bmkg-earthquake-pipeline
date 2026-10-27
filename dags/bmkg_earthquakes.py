"""Hourly ELT of the BMKG earthquake feed into Postgres: raw snapshot -> core table -> DQ checks."""
import json
import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from pipeline import bmkg, dq

CONN_ID = "quakes_db"
log = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO core.earthquakes
    (event_time, latitude, longitude, magnitude, depth_km, region, tsunami_potential, last_snapshot_id)
VALUES (%(event_time)s, %(latitude)s, %(longitude)s, %(magnitude)s, %(depth_km)s,
        %(region)s, %(tsunami_potential)s, %(snapshot_id)s)
ON CONFLICT (event_time, latitude, longitude) DO UPDATE SET
    -- BMKG can revise an event after first publishing it; keep the latest values.
    magnitude         = EXCLUDED.magnitude,
    depth_km          = EXCLUDED.depth_km,
    region            = EXCLUDED.region,
    tsunami_potential = EXCLUDED.tsunami_potential,
    last_seen_at      = now(),
    last_snapshot_id  = EXCLUDED.last_snapshot_id
"""


@dag(schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1, tags=["bmkg"])
def bmkg_earthquakes():
    @task(retries=3)
    def extract() -> int:
        """Fetch the feed and store the raw JSON. Returns the snapshot id."""
        payload = bmkg.fetch()
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        # psycopg2: leaving the `with conn` block commits the transaction.
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.feed_snapshots (payload) VALUES (%s) RETURNING snapshot_id",
                (json.dumps(payload),),
            )
            return cur.fetchone()[0]

    @task
    def load(snapshot_id: int) -> None:
        """Parse the stored snapshot, upsert valid rows, and record counts. Invalid rows stay in raw only."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM raw.feed_snapshots WHERE snapshot_id = %s", (snapshot_id,))
            rows = bmkg.parse(cur.fetchone()[0])
            valid, rejected = [], 0
            for r in rows:
                problems = bmkg.validate(r)
                if problems:
                    rejected += 1
                    log.warning("Rejected %s: %s", r["event_time"], problems)
                else:
                    valid.append(r | {"snapshot_id": snapshot_id})
            cur.executemany(UPSERT_SQL, valid)
            cur.execute(
                "UPDATE raw.feed_snapshots SET row_count = %s, rejected_count = %s WHERE snapshot_id = %s",
                (len(rows), rejected, snapshot_id),
            )
        log.info("Snapshot %s: %s rows, %s loaded, %s rejected", snapshot_id, len(rows), len(valid), rejected)

    @task
    def dq_checks() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        failed = {}
        for name, sql in dq.CHECKS.items():
            bad = hook.get_first(sql)[0]
            log.info("DQ %-28s bad rows: %s", name, bad)
            if bad:
                failed[name] = bad
        if failed:
            raise ValueError(f"Data quality checks failed: {failed}")

    load(extract()) >> dq_checks()


bmkg_earthquakes()
