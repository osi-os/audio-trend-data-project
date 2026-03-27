/* @bruin
name: audio_trends.fct_audio_ecosystem
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.fct_music_trends
  - audio_trends.fct_podcast_trends
  - audio_trends.stg_spotify_charts_historical
  - audio_trends.stg_podcast_reviews

@bruin */

-- ---------------------------------------------------------
-- MART: Audio Ecosystem Comparison
-- The centerpiece of the project. Combines music and podcast
-- data across two time windows:
--
--   Window 1 (2022–2023): Historical Spotify charts + podcast reviews
--   Window 2 (2024–2025): Current Spotify charts + podcast charts
--
-- Uses UNION ALL to stack both windows into one timeline,
-- giving the dashboard a continuous (though gapped) view
-- of the audio ecosystem.
-- ---------------------------------------------------------

WITH historical_music AS (
    -- Window 1: Historical charts aggregated by month
    SELECT
        chart_month                         AS trend_month,
        COUNT(*)                            AS music_chart_entries,
        COUNT(DISTINCT country)             AS countries_active,
        COUNT(DISTINCT track_id)            AS music_unique_tracks,
        COUNT(DISTINCT artists)             AS music_unique_artists,
        CAST(NULL AS FLOAT64)               AS music_avg_popularity,
        CAST(NULL AS FLOAT64)               AS music_avg_danceability,
        CAST(NULL AS FLOAT64)               AS music_avg_energy,
        CAST(NULL AS FLOAT64)               AS music_avg_speechiness,
        CAST(NULL AS FLOAT64)               AS music_avg_valence,
        CAST(NULL AS FLOAT64)               AS music_avg_acousticness,
        CAST(NULL AS FLOAT64)               AS music_explicit_ratio
    FROM `audio-patterns.audio_trends.stg_spotify_charts_historical`
    GROUP BY chart_month
),

historical_podcasts AS (
    -- Window 1: Podcast reviews aggregated by month
    SELECT
        review_month                        AS trend_month,
        COUNT(*)                            AS podcast_total_reviews,
        COUNT(DISTINCT podcast_id)          AS podcast_shows_reviewed,
        COUNT(DISTINCT author_id)           AS podcast_unique_reviewers,
        ROUND(AVG(rating), 2)               AS podcast_avg_rating,
        CAST(NULL AS INT64)                 AS podcast_categories_active,
        CAST(NULL AS INT64)                 AS podcast_chart_entries,
        CAST(NULL AS INT64)                 AS podcast_shows_charting,
        CAST(NULL AS STRING)                AS podcast_top_category
    FROM `audio-patterns.audio_trends.stg_podcast_reviews`
    GROUP BY review_month
),

window_1 AS (
    -- Join historical music + podcast reviews (2022-2023)
    SELECT
        COALESCE(m.trend_month, p.trend_month) AS trend_month,
        m.music_chart_entries,
        m.countries_active,
        m.music_unique_tracks,
        m.music_unique_artists,
        m.music_avg_popularity,
        m.music_avg_danceability,
        m.music_avg_energy,
        m.music_avg_speechiness,
        m.music_avg_valence,
        m.music_avg_acousticness,
        m.music_explicit_ratio,
        p.podcast_total_reviews,
        p.podcast_shows_reviewed,
        p.podcast_unique_reviewers,
        p.podcast_avg_rating,
        p.podcast_categories_active,
        p.podcast_chart_entries,
        p.podcast_shows_charting,
        p.podcast_top_category,
        'reviews' AS podcast_data_source
    FROM historical_music m
    FULL OUTER JOIN historical_podcasts p
        ON m.trend_month = p.trend_month
),

current_music AS (
    -- Window 2: Current 73 countries charts
    SELECT
        trend_month,
        SUM(total_chart_entries)     AS music_chart_entries,
        COUNT(DISTINCT country)      AS countries_active,
        SUM(unique_tracks)           AS music_unique_tracks,
        SUM(unique_artists)          AS music_unique_artists,
        ROUND(AVG(avg_popularity), 1)       AS music_avg_popularity,
        ROUND(AVG(avg_danceability), 3)     AS music_avg_danceability,
        ROUND(AVG(avg_energy), 3)           AS music_avg_energy,
        ROUND(AVG(avg_speechiness), 3)      AS music_avg_speechiness,
        ROUND(AVG(avg_valence), 3)          AS music_avg_valence,
        ROUND(AVG(avg_acousticness), 3)     AS music_avg_acousticness,
        ROUND(AVG(explicit_ratio), 3)       AS music_explicit_ratio
    FROM `audio-patterns.audio_trends.fct_music_trends`
    GROUP BY trend_month
),

current_podcasts AS (
    -- Window 2: Podcast chart data
    SELECT
        trend_month,
        CAST(NULL AS INT64)                 AS podcast_total_reviews,
        CAST(NULL AS INT64)                 AS podcast_shows_reviewed,
        CAST(NULL AS INT64)                 AS podcast_unique_reviewers,
        CAST(NULL AS FLOAT64)               AS podcast_avg_rating,
        COUNT(DISTINCT category)            AS podcast_categories_active,
        MAX(total_chart_entries)             AS podcast_chart_entries,
        MAX(unique_shows_charting)           AS podcast_shows_charting,
        ARRAY_AGG(category ORDER BY category_total_ratings DESC LIMIT 1)[OFFSET(0)]
                                            AS podcast_top_category
    FROM `audio-patterns.audio_trends.fct_podcast_trends`
    GROUP BY trend_month
),

window_2 AS (
    -- Join current music + podcast charts (2023-2025)
    SELECT
        COALESCE(m.trend_month, p.trend_month) AS trend_month,
        m.music_chart_entries,
        m.countries_active,
        m.music_unique_tracks,
        m.music_unique_artists,
        m.music_avg_popularity,
        m.music_avg_danceability,
        m.music_avg_energy,
        m.music_avg_speechiness,
        m.music_avg_valence,
        m.music_avg_acousticness,
        m.music_explicit_ratio,
        p.podcast_total_reviews,
        p.podcast_shows_reviewed,
        p.podcast_unique_reviewers,
        p.podcast_avg_rating,
        p.podcast_categories_active,
        p.podcast_chart_entries,
        p.podcast_shows_charting,
        p.podcast_top_category,
        'charts' AS podcast_data_source
    FROM current_music m
    FULL OUTER JOIN current_podcasts p
        ON m.trend_month = p.trend_month
)

-- Stack both windows into one timeline
SELECT
    trend_month,
    music_chart_entries,
    countries_active,
    music_unique_tracks,
    music_unique_artists,
    music_avg_popularity,
    music_avg_danceability,
    music_avg_energy,
    music_avg_speechiness,
    music_avg_valence,
    music_avg_acousticness,
    music_explicit_ratio,
    podcast_total_reviews,
    podcast_shows_reviewed,
    podcast_unique_reviewers,
    podcast_avg_rating,
    podcast_categories_active,
    podcast_chart_entries,
    podcast_shows_charting,
    podcast_top_category,
    podcast_data_source,
    ROUND(
        SAFE_DIVIDE(podcast_total_reviews, music_chart_entries), 4
    ) AS podcast_to_music_ratio
FROM window_1

UNION ALL

SELECT
    trend_month,
    music_chart_entries,
    countries_active,
    music_unique_tracks,
    music_unique_artists,
    music_avg_popularity,
    music_avg_danceability,
    music_avg_energy,
    music_avg_speechiness,
    music_avg_valence,
    music_avg_acousticness,
    music_explicit_ratio,
    podcast_total_reviews,
    podcast_shows_reviewed,
    podcast_unique_reviewers,
    podcast_avg_rating,
    podcast_categories_active,
    podcast_chart_entries,
    podcast_shows_charting,
    podcast_top_category,
    podcast_data_source,
    ROUND(
        SAFE_DIVIDE(podcast_total_reviews, music_chart_entries), 4
    ) AS podcast_to_music_ratio
FROM window_2

ORDER BY trend_month