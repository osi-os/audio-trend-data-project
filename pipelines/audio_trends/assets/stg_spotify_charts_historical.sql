/* @bruin
name: audio_trends.stg_spotify_charts_historical
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.load_raw_data

columns:
  - name: track_id
    checks:
      - name: not_null
  - name: chart_date
    checks:
      - name: not_null
  - name: position
    checks:
      - name: positive

@bruin */

-- ---------------------------------------------------------
-- STAGING: Spotify Charts Historical (2013–2023)
-- This dataset covers an earlier time period than the 73
-- Countries dataset, giving us overlap with podcast reviews
-- (Jan 2022 – Mar 2023).
--
-- Key difference from the 73 Countries dataset: this one
-- has artist_genres but no audio features (danceability, etc.).
-- We use it for genre-based analysis against podcast categories.
--
-- date column is stored as nanosecond INTEGER in parquet,
-- same as the other datasets.
-- ---------------------------------------------------------

SELECT
    track_id,
    name                                            AS track_name,
    artists,
    artist_genres,
    CAST(position AS INT64)                         AS position,
    country,
    DATE(TIMESTAMP_MICROS(CAST(date / 1000 AS INT64))) AS chart_date,
    CAST(streams AS INT64)                          AS streams,
    CAST(duration AS INT64)                         AS duration_ms,
    explicit                                        AS is_explicit,

    -- Time dimensions for aggregation
    FORMAT_DATE('%Y-%m', DATE(TIMESTAMP_MICROS(CAST(date / 1000 AS INT64)))) AS chart_month

FROM `audio-patterns.audio_trends.raw_spotify_charts_historical`

WHERE date IS NOT NULL
  AND position IS NOT NULL
  AND name IS NOT NULL