-- Runs once, when the Postgres container starts with an empty volume.
CREATE DATABASE airflow;   -- Airflow metadata
CREATE DATABASE quakes;    -- pipeline data

\connect quakes

CREATE SCHEMA raw;
CREATE SCHEMA core;

-- Every feed response, stored as-is so any load can be replayed.
CREATE TABLE raw.feed_snapshots (
    snapshot_id    BIGSERIAL PRIMARY KEY,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload        JSONB       NOT NULL,
    row_count      INT,        -- filled by the load task
    rejected_count INT         -- rows that failed validation
);

-- One row per earthquake. The feed only shows the latest 15, so history builds up run by run.
CREATE TABLE core.earthquakes (
    event_time         TIMESTAMPTZ  NOT NULL,
    latitude           NUMERIC(6,2) NOT NULL,
    longitude          NUMERIC(6,2) NOT NULL,
    magnitude          NUMERIC(3,1) NOT NULL,
    depth_km           INT          NOT NULL,
    region             TEXT         NOT NULL,
    tsunami_potential  TEXT,
    first_seen_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_snapshot_id   BIGINT       NOT NULL REFERENCES raw.feed_snapshots (snapshot_id),
    PRIMARY KEY (event_time, latitude, longitude)
);

CREATE INDEX ON core.earthquakes (magnitude);
