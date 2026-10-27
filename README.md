# BMKG Earthquake Pipeline

An hourly Airflow pipeline that loads Indonesia's public earthquake feed from [BMKG](https://data.bmkg.go.id/) into Postgres, with data quality checks after every load.

BMKG's feed (`gempaterkini.json`) only ever lists the **latest 15 earthquakes of magnitude 5.0+**. Older events drop off, so this pipeline captures each snapshot and builds up the full history over time.

## Architecture

```
BMKG feed (JSON, no API key)
        │  extract  (hourly, 3 retries)
        ▼
raw.feed_snapshots     every response stored as-is (JSONB), so any load can be replayed
        │  load     parse → validate → upsert
        ▼
core.earthquakes       one row per event, key = (event_time, latitude, longitude)
        │  dq_checks
        ▼
fail the run if any check finds bad rows
```

Design choices:
- **Raw first (ELT).** The raw JSON is stored before any parsing, so a parser bug never loses data. Fix the code and reload from `raw`.
- **Idempotent upsert.** Each event shows up in many hourly snapshots. `ON CONFLICT ... DO UPDATE` keeps one row per event, and keeps BMKG's latest values if it revises magnitude or depth.
- **Bad rows don't block good ones.** Rows that fail validation aren't loaded into `core`. They're counted in `raw.feed_snapshots.rejected_count`, and the DQ step then fails the run, so someone looks at them.

## Data quality checks

Run after every load ([pipeline/dq.py](pipeline/dq.py)). Each check counts bad rows, and any count above 0 fails the run.

| Check | Rule |
|---|---|
| `magnitude_in_range` | 0 ≤ magnitude ≤ 10 |
| `depth_in_range` | 0 ≤ depth ≤ 800 km |
| `coordinates_in_bmkg_area` | latitude −15…10, longitude 90…145 |
| `no_future_events` | event time isn't more than 1 hour in the future |
| `latest_snapshot_not_empty` | the latest feed response had at least 1 event |
| `latest_snapshot_no_rejects` | no row in the latest snapshot failed validation |

The same row-level rules run in Python before loading ([pipeline/bmkg.py](pipeline/bmkg.py)), and pytest covers them.

There's deliberately no freshness check on event time: M5.0+ earthquakes can be days apart, so "no new event in 24 hours" is normal.

## Tech stack

- Apache Airflow 3.1 (TaskFlow API), Python 3.12
- PostgreSQL 16
- Docker Compose
- pytest

## Run locally

Requires Docker Desktop.

```bash
docker compose up -d
```

- Airflow UI: http://localhost:8081 (local-only setup, no login). The `bmkg_earthquakes` DAG starts unpaused and runs hourly. Trigger it by hand for a first run.
- Postgres: `localhost:5433`, user `postgres`, password `postgres` (local defaults only).

Query the results:

```bash
docker compose exec postgres psql -U postgres -d quakes -c "SELECT event_time, magnitude, depth_km, region FROM core.earthquakes ORDER BY event_time DESC LIMIT 10;"
```

Stop, and delete the data:

```bash
docker compose down -v
```

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest
```

The tests cover feed parsing and validation, using a saved sample of the feed in `tests/fixtures/`.

## Project structure

```
dags/bmkg_earthquakes.py   Airflow DAG: extract → load → dq_checks
pipeline/bmkg.py           fetch, parse and validate the feed
pipeline/dq.py             SQL data quality checks
sql/init/01_init.sql       databases, schemas and tables (runs on first start)
tests/                     pytest tests and a sample feed
```

## Possible next steps

- A Metabase dashboard: map of events, magnitude over time
- Alerting on DQ failure (email or Slack)
- A daily summary table by region

## Data source

Earthquake data © [BMKG](https://data.bmkg.go.id/) (Badan Meteorologi, Klimatologi, dan Geofisika), used from its public open data feed.
