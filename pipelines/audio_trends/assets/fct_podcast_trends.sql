/* @bruin
name: audio_trends.fct_podcast_trends
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.stg_podcast_reviews
  - audio_trends.stg_podcast_shows
  - audio_trends.stg_podcast_charts

@bruin */

-- ---------------------------------------------------------
-- MART: Podcast Trends
-- Combines podcast reviews, shows, and charts into monthly
-- summaries. This powers the "podcast side" of the dashboard.
--
-- DATA QUALITY NOTE:
-- The podcast reviews and podcast shows datasets use different
-- podcast_id hash schemes with zero overlap, so a direct join
-- between them is not possible. This is a known issue with the
-- source Kaggle datasets (thoughtvector/podcastreviews).
--
-- WORKAROUND:
-- Instead of joining reviews to shows by podcast_id, we build
-- trends from three independent sources:
--
--   1. Review trends by month — volume, ratings, reviewer
--      counts over time (from stg_podcast_reviews)
--   2. Category landscape — which categories exist, how many
--      shows, and their aggregate ratings (from stg_podcast_shows)
--   3. Chart trends by month — what's actively charting on
--      Spotify, how many unique shows (from stg_podcast_charts)
--
-- We CROSS JOIN the category summary with the monthly time
-- series so the dashboard can display category-level data
-- alongside temporal trends. The category data is static
-- (not time-varying), but pairing it with monthly rows lets
-- us build heatmaps and comparisons in a single table.
--
-- Key metrics:
--   - Monthly review volume and average ratings
--   - Monthly chart activity (entries, unique shows)
--   - Category-level show counts and aggregate ratings
--   - Episode duration trends
-- ---------------------------------------------------------

WITH review_monthly AS (
    -- Monthly review activity across all podcasts
    -- We can't break this down by category (no join), but the
    -- volume and rating trends are still valuable for showing
    -- overall podcast engagement over time.
    SELECT
        review_month                    AS trend_month,
        COUNT(*)                        AS review_count,
        COUNT(DISTINCT podcast_id)      AS podcasts_reviewed,
        COUNT(DISTINCT author_id)       AS unique_reviewers,
        ROUND(AVG(rating), 2)           AS avg_rating,
        COUNTIF(rating = 5)             AS five_star_count,
        COUNTIF(rating = 1)             AS one_star_count
    FROM `audio-patterns.audio_trends.stg_podcast_reviews`
    GROUP BY review_month
),

chart_monthly AS (
    -- Monthly chart activity from Spotify podcast charts
    -- Shows what's actively trending each month
    SELECT
        chart_month                     AS trend_month,
        COUNT(*)                        AS chart_entries,
        COUNT(DISTINCT show_name)       AS unique_shows_charting,
        ROUND(AVG(duration_ms) / 60000.0, 1) AS avg_episode_duration_minutes
    FROM `audio-patterns.audio_trends.stg_podcast_charts`
    GROUP BY chart_month
),

category_summary AS (
    -- Category landscape from the shows/metadata table
    -- This gives us the breakdown of the podcast ecosystem
    -- by genre/category — aggregated across all time since
    -- shows don't have temporal data
    SELECT
        category,
        COUNT(DISTINCT podcast_id)              AS shows_in_category,
        ROUND(AVG(average_rating), 2)           AS category_avg_rating,
        SUM(SAFE_CAST(ratings_count AS INT64))  AS category_total_ratings
    FROM `audio-patterns.audio_trends.stg_podcast_shows`
    GROUP BY category
    HAVING COUNT(DISTINCT podcast_id) >= 10  -- Filter out tiny categories
)

-- Final output: each month gets review + chart metrics,
-- crossed with category info for the dashboard
SELECT
    COALESCE(r.trend_month, c.trend_month) AS trend_month,

    -- Review metrics (how engaged are podcast listeners this month?)
    r.review_count,
    r.podcasts_reviewed,
    r.unique_reviewers,
    r.avg_rating,
    r.five_star_count,
    r.one_star_count,

    -- Chart metrics (what's trending on Spotify this month?)
    c.chart_entries                  AS total_chart_entries,
    c.unique_shows_charting,
    c.avg_episode_duration_minutes,

    -- Category info (static landscape, repeated per month for dashboard joins)
    cs.category,
    cs.shows_in_category,
    cs.category_avg_rating,
    cs.category_total_ratings

FROM review_monthly r
FULL OUTER JOIN chart_monthly c
    ON r.trend_month = c.trend_month
CROSS JOIN category_summary cs

ORDER BY trend_month, category_total_ratings DESC