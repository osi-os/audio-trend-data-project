/* @bruin
name: audio_trends.fct_music_podcast_correlation
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.stg_spotify_charts_historical
  - audio_trends.stg_podcast_reviews
  - audio_trends.stg_podcast_shows

@bruin */

-- ---------------------------------------------------------
-- MART: Music–Podcast Correlation
--
-- This model uses the HISTORICAL Spotify charts (2022–2023)
-- which overlap with podcast reviews (Jan 2022 – Mar 2023),
-- giving us 15 months of cross-content data.
--
-- The historical dataset includes artist_genres, which lets
-- us explore: "In months when certain music genres dominate
-- the charts, do certain podcast categories see more reviews?"
--
-- For example: when Hip-Hop dominates the music charts, do
-- True Crime or Comedy podcasts see a review spike?
--
-- APPROACH:
-- 1. Extract top music genres per month from chart data
-- 2. Get podcast review volume per month
-- 3. Cross-join with podcast category landscape from shows
-- 4. Dashboard computes correlations across this table
-- ---------------------------------------------------------

WITH monthly_music_genres AS (
    -- Monthly music genre profile from historical charts
    -- artist_genres contains comma-separated genres per track;
    -- we count chart appearances per genre per month
    SELECT
        chart_month                             AS trend_month,
        COUNT(*)                                AS total_chart_entries,
        COUNT(DISTINCT track_id)                AS unique_tracks,
        COUNT(DISTINCT artists)                 AS unique_artists,
        COUNT(DISTINCT country)                 AS countries_active,
        ROUND(AVG(streams), 0)                  AS avg_streams,

        -- Genre signals: count how often genre keywords appear
        -- in chart entries each month
        COUNTIF(LOWER(artist_genres) LIKE '%pop%')          AS pop_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%hip hop%'
             OR LOWER(artist_genres) LIKE '%rap%')          AS hiphop_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%rock%')         AS rock_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%latin%'
             OR LOWER(artist_genres) LIKE '%reggaeton%')    AS latin_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%r&b%'
             OR LOWER(artist_genres) LIKE '%soul%')         AS rnb_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%electronic%'
             OR LOWER(artist_genres) LIKE '%edm%'
             OR LOWER(artist_genres) LIKE '%house%')        AS electronic_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%country%')      AS country_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%indie%')        AS indie_entries,
        COUNTIF(LOWER(artist_genres) LIKE '%k-pop%'
             OR LOWER(artist_genres) LIKE '%korean%')       AS kpop_entries

    FROM `audio-patterns.audio_trends.stg_spotify_charts_historical`
    GROUP BY chart_month
),

monthly_reviews AS (
    -- Monthly podcast review volume
    SELECT
        review_month                        AS trend_month,
        COUNT(*)                            AS review_count,
        COUNT(DISTINCT podcast_id)          AS podcasts_reviewed,
        COUNT(DISTINCT author_id)           AS unique_reviewers,
        ROUND(AVG(rating), 2)               AS avg_rating
    FROM `audio-patterns.audio_trends.stg_podcast_reviews`
    GROUP BY review_month
),

category_summary AS (
    -- Podcast category landscape
    SELECT
        category,
        COUNT(DISTINCT podcast_id)          AS shows_in_category,
        SUM(SAFE_CAST(ratings_count AS INT64)) AS category_total_ratings
    FROM `audio-patterns.audio_trends.stg_podcast_shows`
    GROUP BY category
    HAVING SUM(SAFE_CAST(ratings_count AS INT64)) >= 1000
)

-- Join on overlapping months (Jan 2022 – Mar 2023)
SELECT
    mg.trend_month,
    cs.category,
    cs.shows_in_category,
    cs.category_total_ratings,

    -- Podcast review activity
    mr.review_count,
    mr.podcasts_reviewed,
    mr.unique_reviewers,
    mr.avg_rating,

    -- Music chart volume
    mg.total_chart_entries,
    mg.unique_tracks,
    mg.unique_artists,
    mg.avg_streams,

    -- Music genre signals (for correlation with podcast categories)
    mg.pop_entries,
    mg.hiphop_entries,
    mg.rock_entries,
    mg.latin_entries,
    mg.rnb_entries,
    mg.electronic_entries,
    mg.country_entries,
    mg.indie_entries,
    mg.kpop_entries,

    -- Genre percentages (normalized by total entries)
    ROUND(SAFE_DIVIDE(mg.pop_entries, mg.total_chart_entries), 4)        AS pop_pct,
    ROUND(SAFE_DIVIDE(mg.hiphop_entries, mg.total_chart_entries), 4)     AS hiphop_pct,
    ROUND(SAFE_DIVIDE(mg.rock_entries, mg.total_chart_entries), 4)       AS rock_pct,
    ROUND(SAFE_DIVIDE(mg.latin_entries, mg.total_chart_entries), 4)      AS latin_pct,
    ROUND(SAFE_DIVIDE(mg.rnb_entries, mg.total_chart_entries), 4)        AS rnb_pct,
    ROUND(SAFE_DIVIDE(mg.electronic_entries, mg.total_chart_entries), 4) AS electronic_pct,
    ROUND(SAFE_DIVIDE(mg.country_entries, mg.total_chart_entries), 4)    AS country_pct,
    ROUND(SAFE_DIVIDE(mg.indie_entries, mg.total_chart_entries), 4)      AS indie_pct,
    ROUND(SAFE_DIVIDE(mg.kpop_entries, mg.total_chart_entries), 4)       AS kpop_pct

FROM monthly_music_genres mg
INNER JOIN monthly_reviews mr
    ON mg.trend_month = mr.trend_month
CROSS JOIN category_summary cs

ORDER BY mg.trend_month, cs.category_total_ratings DESC