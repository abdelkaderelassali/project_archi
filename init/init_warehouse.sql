-- Bootstrap schema for analytical (Gold) tables.
-- Populated later by the ELT pipeline (Airflow DAG).

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.articles (
    id            TEXT PRIMARY KEY,
    title         TEXT,
    author        TEXT,
    published_at  TIMESTAMPTZ,
    category      TEXT,
    source        TEXT,
    url           TEXT,
    language      TEXT,
    content       TEXT,
    word_count    INTEGER,
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.articles_by_day (
    day           DATE,
    source        TEXT,
    n_articles    INTEGER,
    PRIMARY KEY (day, source)
);

CREATE TABLE IF NOT EXISTS gold.articles_by_source (
    source        TEXT PRIMARY KEY,
    n_articles    INTEGER,
    last_seen_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS gold.articles_by_category (
    category      TEXT,
    day           DATE,
    n_articles    INTEGER,
    PRIMARY KEY (category, day)
);

CREATE TABLE IF NOT EXISTS gold.top_keywords (
    keyword       TEXT,
    day           DATE,
    n_occurrences INTEGER,
    PRIMARY KEY (keyword, day)
);

CREATE TABLE IF NOT EXISTS gold.dq_results (
    run_at        TIMESTAMPTZ DEFAULT NOW(),
    check_name    TEXT,
    dimension     TEXT,
    passed        BOOLEAN,
    details       JSONB
);
