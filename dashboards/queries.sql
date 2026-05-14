-- =====================================================================
-- News Big Data Platform — BI Dashboard Queries
-- Target: PostgreSQL warehouse, schema "gold"
-- Tool  : Metabase (Native query) / Grafana (PostgreSQL datasource)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. TRENDING NEWS TOPICS  (Tendances d'actualité)
--    Top keywords over the last 7 days, ranked by total occurrences.
--    Visual : horizontal bar chart, X = total_occurrences, Y = keyword
-- ---------------------------------------------------------------------
SELECT
    keyword,
    SUM(n_occurrences) AS total_occurrences,
    COUNT(DISTINCT day) AS days_seen
FROM gold.top_keywords
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY keyword
ORDER BY total_occurrences DESC
LIMIT 25;


-- ---------------------------------------------------------------------
-- 1.bis TRENDING TIME-SERIES  (line / area chart)
--    Daily occurrences for the top 10 keywords of the last 7 days.
--    Visual : multi-series line, X = day, Y = n_occurrences, series = keyword
-- ---------------------------------------------------------------------
WITH top10 AS (
    SELECT keyword
    FROM gold.top_keywords
    WHERE day >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY keyword
    ORDER BY SUM(n_occurrences) DESC
    LIMIT 10
)
SELECT k.day, k.keyword, k.n_occurrences
FROM gold.top_keywords k
JOIN top10 t USING (keyword)
WHERE k.day >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY k.day, k.keyword;


-- ---------------------------------------------------------------------
-- 2. NUMBER OF ARTICLES PER SOURCE  (Nombre articles par source)
--    Visual : bar chart, X = source, Y = n_articles
-- ---------------------------------------------------------------------
SELECT
    source,
    n_articles,
    last_seen_at
FROM gold.articles_per_source
ORDER BY n_articles DESC;


-- ---------------------------------------------------------------------
-- 2.bis ARTICLES PER SOURCE OVER TIME  (stacked area)
--    Visual : stacked area, X = day, Y = n_articles, series = source
-- ---------------------------------------------------------------------
SELECT day, source, n_articles
FROM gold.articles_per_day
WHERE day >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY day, source;


-- ---------------------------------------------------------------------
-- 3. MOST FREQUENT KEYWORDS  (Mots clés les plus fréquents — all-time)
--    Visual : word-cloud (Metabase community viz) OR horizontal bar
-- ---------------------------------------------------------------------
SELECT
    keyword,
    SUM(n_occurrences) AS n_occurrences
FROM gold.top_keywords
GROUP BY keyword
ORDER BY n_occurrences DESC
LIMIT 50;


-- ---------------------------------------------------------------------
-- 4. ARTICLES PER DAY  (KPI : volume timeline)
--    Visual : line chart, X = day, Y = total
-- ---------------------------------------------------------------------
SELECT
    day,
    SUM(n_articles) AS total_articles
FROM gold.articles_per_day
GROUP BY day
ORDER BY day;


-- ---------------------------------------------------------------------
-- 5. ARTICLES PER THEME  (Top categories last 7 days)
--    Visual : pie / donut, X = theme, Y = n_articles
-- ---------------------------------------------------------------------
SELECT
    theme,
    SUM(n_articles) AS n_articles
FROM gold.articles_per_theme
WHERE day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY theme
ORDER BY n_articles DESC
LIMIT 15;


-- ---------------------------------------------------------------------
-- 6. ARTICLES PER COUNTRY  (geographic distribution)
--    Visual : map (Metabase region map, ISO-2 codes)
-- ---------------------------------------------------------------------
SELECT
    country,
    SUM(n_articles) AS n_articles
FROM gold.articles_per_country
WHERE day >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY country
ORDER BY n_articles DESC;


-- ---------------------------------------------------------------------
-- 7. KPI TILES  (single-number cards)
-- ---------------------------------------------------------------------
-- Total articles ingested
SELECT COUNT(*) AS total_articles FROM gold.articles;

-- Active sources
SELECT COUNT(*) AS active_sources FROM gold.articles_per_source;

-- Articles ingested in the last 24 hours
SELECT COUNT(*) AS last_24h
FROM gold.articles
WHERE published_at >= NOW() - INTERVAL '24 hours';

-- Languages distribution
SELECT language, COUNT(*) AS n_articles
FROM gold.articles
GROUP BY language
ORDER BY n_articles DESC;


-- ---------------------------------------------------------------------
-- 8. LATEST ARTICLES TABLE  (drill-down list)
-- ---------------------------------------------------------------------
SELECT
    published_at,
    source,
    category,
    language,
    title,
    url
FROM gold.articles
ORDER BY published_at DESC NULLS LAST
LIMIT 100;
