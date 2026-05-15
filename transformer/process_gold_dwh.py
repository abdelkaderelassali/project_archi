"""
Gold builder + Data Warehouse loader.

Pipeline:
  silver/ (JSON in MinIO)
      |--> aggregate in pandas
      |--> persist parquet under  gold/<table>/<table>.parquet
      `--> truncate-and-load Postgres  schema "gold"

Analytical tables produced:
  * articles_per_day        (day, source, n_articles)
  * articles_per_source     (source, n_articles, last_seen_at)
  * articles_per_theme      (theme, day, n_articles)             # from `categorie`
  * articles_per_country    (country, day, n_articles)           # via source -> country
  * top_keywords            (keyword, day, n_occurrences)        # trending subjects
  * articles                (cleaned silver mirror, for BI drill-down)

Idempotent: each run rebuilds the gold layer from silver, so re-runs are safe.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Iterable, Optional

import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "newslake")

PG_HOST = os.getenv("WAREHOUSE_HOST", os.getenv("POSTGRES_HOST", "warehouse"))
PG_PORT = os.getenv("WAREHOUSE_PORT", os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("WAREHOUSE_USER", os.getenv("POSTGRES_USER", "warehouse"))
PG_PASS = os.getenv("WAREHOUSE_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse"))
PG_DB = os.getenv("WAREHOUSE_DB", os.getenv("POSTGRES_DB", "warehouse"))

TOP_KEYWORDS_PER_DAY = int(os.getenv("TOP_KEYWORDS_PER_DAY", "50"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)
log = logging.getLogger("gold-dwh")

# --------------------------------------------------------------------------- #
# Source -> country mapping (ISO-3166 alpha-2)
# Extend here when you onboard new adapters.
# --------------------------------------------------------------------------- #
SOURCE_COUNTRY = {
    "Hespress": "MA",
    "Akhbarona": "MA",
    "Lakom": "MA",
    "Barlamane": "MA",
    "BBC News": "GB",
    "BBC": "GB",
    "CNN": "US",
    "Reuters": "GB",
    "Al Jazeera": "QA",
}

# Lightweight multilingual stopword list (en/fr/ar). Good enough for trending.
STOPWORDS = set("""
the a an and or of to in on for with at by from as is are was were be been being
this that these those it its their there here we you i he she they them his her
not no but if then else so do does did has have had will would can could should
about after again all also am any because before between both during each few
more most other our out over own same some such than too very what when where
which who why how
le la les un une des de du au aux et ou en sur dans pour avec par à ce ces ce
cet cette il elle ils elles nous vous je tu on son sa ses leur leurs y l c'est
qu pas plus mais ne ni si comme aussi très bien tout tous toute toutes a
في من على إلى عن مع هذا هذه ذلك تلك أن إن كان كانت يكون تكون قد قد كل بعض
""".split())

# Word: at least 3 letters, supports Arabic, Latin, accented chars
WORD_RE = re.compile(r"[A-Za-zÀ-ÿ\u0600-\u06FF]{3,}", re.UNICODE)


# --------------------------------------------------------------------------- #
# IO helpers
# --------------------------------------------------------------------------- #
def build_minio() -> Minio:
    secure = MINIO_ENDPOINT.startswith("https://")
    host = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")
    return Minio(host, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=secure)


def build_engine() -> Engine:
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True, future=True)


def iter_silver(client: Minio) -> Iterable[dict]:
    """Stream every silver JSON object as a dict."""
    objs = client.list_objects(MINIO_BUCKET, prefix="silver/", recursive=True)
    for obj in objs:
        if not obj.object_name.endswith(".json"):
            continue
        if "_rejected" in obj.object_name:
            continue
        try:
            resp = client.get_object(MINIO_BUCKET, obj.object_name)
            try:
                yield json.loads(resp.read().decode("utf-8"))
            finally:
                resp.close()
                resp.release_conn()
        except Exception as exc:                                       # noqa: BLE001
            log.warning("skip %s: %s", obj.object_name, exc)


def read_parquet(client: Minio, key: str) -> pd.DataFrame:
    try:
        resp = client.get_object(MINIO_BUCKET, key)
        try:
            return pd.read_parquet(io.BytesIO(resp.read()))
        finally:
            resp.close()
            resp.release_conn()
    except Exception as exc:                                           # noqa: BLE001
        log.warning("read_parquet %s failed: %s", key, exc)
        return pd.DataFrame()


def write_parquet(client: Minio, df: pd.DataFrame, key: str) -> None:
    if df.empty:
        log.info("skip %s (empty)", key)
        return
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    body = buf.getvalue()
    client.put_object(
        MINIO_BUCKET, key,
        data=io.BytesIO(body), length=len(body),
        content_type="application/octet-stream",
    )
    log.info("wrote %s (%d rows)", key, len(df))


# --------------------------------------------------------------------------- #
# Build silver dataframe
# --------------------------------------------------------------------------- #
def load_silver(client: Minio) -> pd.DataFrame:
    rows = list(iter_silver(client))
    if not rows:
        log.warning("no silver records found")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Normalise columns we rely on
    df["published_at"] = pd.to_datetime(df.get("date_publication"),
                                        errors="coerce", utc=True)
    df["day"] = df["published_at"].dt.date
    df["source"] = df.get("source").fillna("unknown")
    df["categorie"] = df.get("categorie").fillna("Uncategorized")
    df["language"] = df.get("language").fillna("unknown")
    df["country"] = df["source"].map(SOURCE_COUNTRY).fillna("XX")
    df["contenu"] = df.get("contenu").fillna("")
    df["titre_article"] = df.get("titre_article").fillna("")
    df["word_count"] = df.get("word_count").fillna(0).astype(int)

    # Drop rows with no usable date for time-based aggregations
    df = df[df["day"].notna()].copy()
    log.info("silver loaded: %d rows", len(df))
    return df


# --------------------------------------------------------------------------- #
# Aggregations
# --------------------------------------------------------------------------- #
def agg_articles_per_day(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(["day", "source"])
              .size().reset_index(name="n_articles")
              .sort_values(["day", "n_articles"], ascending=[False, False]))


def agg_articles_per_source(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("source")
    return pd.DataFrame({
        "source": g.size().index,
        "n_articles": g.size().values,
        "last_seen_at": g["published_at"].max().values,
    }).sort_values("n_articles", ascending=False)


def agg_articles_per_theme(df: pd.DataFrame) -> pd.DataFrame:
    return (df.rename(columns={"categorie": "theme"})
              .groupby(["theme", "day"])
              .size().reset_index(name="n_articles")
              .sort_values(["day", "n_articles"], ascending=[False, False]))


def agg_articles_per_country(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(["country", "day"])
              .size().reset_index(name="n_articles")
              .sort_values(["day", "n_articles"], ascending=[False, False]))


def agg_top_keywords(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for day, sub in df.groupby("day"):
        text = " ".join((sub["titre_article"] + " " + sub["contenu"]).tolist())
        tokens = (t.lower() for t in WORD_RE.findall(text))
        tokens = (t for t in tokens if t not in STOPWORDS)
        for keyword, n in Counter(tokens).most_common(TOP_KEYWORDS_PER_DAY):
            rows.append({"keyword": keyword, "day": day, "n_occurrences": int(n)})
    return pd.DataFrame(rows)


def project_articles(df: pd.DataFrame) -> pd.DataFrame:
    """Silver mirror with the columns the warehouse cares about."""
    out = pd.DataFrame({
        "id":           df["id"],
        "title":        df["titre_article"],
        "author":       df.get("auteur"),
        "published_at": df["published_at"],
        "category":     df["categorie"],
        "source":       df["source"],
        "url":          df["url"],
        "language":     df["language"],
        "content":      df["contenu"],
        "word_count":   df["word_count"],
    })
    return out.drop_duplicates(subset=["id"])


# --------------------------------------------------------------------------- #
# Warehouse load
# --------------------------------------------------------------------------- #
DDL = """
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

CREATE TABLE IF NOT EXISTS gold.articles_per_day (
    day         DATE NOT NULL,
    source      TEXT NOT NULL,
    n_articles  INTEGER NOT NULL,
    PRIMARY KEY (day, source)
);

CREATE TABLE IF NOT EXISTS gold.articles_per_source (
    source        TEXT PRIMARY KEY,
    n_articles    INTEGER NOT NULL,
    last_seen_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS gold.articles_per_theme (
    theme       TEXT NOT NULL,
    day         DATE NOT NULL,
    n_articles  INTEGER NOT NULL,
    PRIMARY KEY (theme, day)
);

CREATE TABLE IF NOT EXISTS gold.articles_per_country (
    country     TEXT NOT NULL,
    day         DATE NOT NULL,
    n_articles  INTEGER NOT NULL,
    PRIMARY KEY (country, day)
);

CREATE TABLE IF NOT EXISTS gold.top_keywords (
    keyword        TEXT NOT NULL,
    day            DATE NOT NULL,
    n_occurrences  INTEGER NOT NULL,
    PRIMARY KEY (keyword, day)
);
"""


def truncate_and_load(engine: Engine, table: str, df: pd.DataFrame) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE gold.{table}"))
        if not df.empty:
            cols = list(df.columns)
            placeholders = ", ".join([f":{c}" for c in cols])
            insert_sql = text(f"INSERT INTO gold.{table} ({', '.join(cols)}) VALUES ({placeholders})")
            df_clean = df.where(pd.notnull(df), None)
            records = df_clean.to_dict(orient="records")
            conn.execute(insert_sql, records)
    log.info("warehouse %-25s loaded %d rows", f"gold.{table}", len(df))


def upsert_articles(engine: Engine, df: pd.DataFrame) -> None:
    """Upsert silver mirror to keep history without duplicates."""
    if df.empty:
        log.info("warehouse gold.articles : no rows to upsert")
        return
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS _stage_articles"))
        conn.execute(text("CREATE TABLE _stage_articles "
                          "(LIKE gold.articles INCLUDING DEFAULTS)"))
        
        cols = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in cols])
        insert_sql = text(f"INSERT INTO _stage_articles ({', '.join(cols)}) VALUES ({placeholders})")
        df_clean = df.where(pd.notnull(df), None)
        records = df_clean.to_dict(orient="records")
        conn.execute(insert_sql, records)
        
        conn.execute(text("""
            INSERT INTO gold.articles
                (id, title, author, published_at, category, source, url,
                 language, content, word_count)
            SELECT id, title, author, published_at, category, source, url,
                   language, content, word_count
            FROM _stage_articles
            ON CONFLICT (id) DO UPDATE SET
                title         = EXCLUDED.title,
                author        = EXCLUDED.author,
                published_at  = EXCLUDED.published_at,
                category      = EXCLUDED.category,
                source        = EXCLUDED.source,
                url           = EXCLUDED.url,
                language      = EXCLUDED.language,
                content       = EXCLUDED.content,
                word_count    = EXCLUDED.word_count,
                ingested_at   = NOW();
        """))
        conn.execute(text("DROP TABLE _stage_articles"))
    log.info("warehouse gold.articles    upserted %d rows", len(df))


# --------------------------------------------------------------------------- #
# Phase 1 : silver -> aggregates -> gold parquet in MinIO
# --------------------------------------------------------------------------- #
GOLD_KEYS = {
    "articles_per_day":     "gold/articles_per_day/articles_per_day.parquet",
    "articles_per_source":  "gold/articles_per_source/articles_per_source.parquet",
    "articles_per_theme":   "gold/articles_per_theme/articles_per_theme.parquet",
    "articles_per_country": "gold/articles_per_country/articles_per_country.parquet",
    "top_keywords":         "gold/top_keywords/top_keywords.parquet",
    "articles":             "gold/articles/articles.parquet",
}


def build_gold() -> dict:
    """Read silver from MinIO, build all analytical tables, persist to gold/."""
    started = datetime.now(timezone.utc)
    client = build_minio()
    silver = load_silver(client)
    if silver.empty:
        log.warning("build_gold: nothing to do")
        return {"rows": 0}

    frames = {
        "articles_per_day":     agg_articles_per_day(silver),
        "articles_per_source":  agg_articles_per_source(silver),
        "articles_per_theme":   agg_articles_per_theme(silver),
        "articles_per_country": agg_articles_per_country(silver),
        "top_keywords":         agg_top_keywords(silver),
        "articles":             project_articles(silver),
    }
    for name, df in frames.items():
        write_parquet(client, df, GOLD_KEYS[name])

    summary = {"rows_silver": int(len(silver)),
               **{k: int(len(v)) for k, v in frames.items()},
               "elapsed_sec": round(
                   (datetime.now(timezone.utc) - started).total_seconds(), 2)}
    log.info("build_gold done %s", summary)
    return summary


# --------------------------------------------------------------------------- #
# Phase 2 : gold parquet (MinIO) -> Postgres warehouse
# --------------------------------------------------------------------------- #
def load_warehouse() -> dict:
    """Read gold parquet from MinIO and load into Postgres warehouse."""
    started = datetime.now(timezone.utc)
    client = build_minio()
    engine = build_engine()

    with engine.begin() as conn:
        for stmt in DDL.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

    frames = {name: read_parquet(client, key) for name, key in GOLD_KEYS.items()}

    truncate_and_load(engine, "articles_per_day",     frames["articles_per_day"])
    truncate_and_load(engine, "articles_per_source",  frames["articles_per_source"])
    truncate_and_load(engine, "articles_per_theme",   frames["articles_per_theme"])
    truncate_and_load(engine, "articles_per_country", frames["articles_per_country"])
    truncate_and_load(engine, "top_keywords",         frames["top_keywords"])
    upsert_articles(engine, frames["articles"])

    summary = {**{k: int(len(v)) for k, v in frames.items()},
               "elapsed_sec": round(
                   (datetime.now(timezone.utc) - started).total_seconds(), 2)}
    log.info("load_warehouse done %s", summary)
    return summary


def run() -> dict:
    """Composite: gold then warehouse. Used when invoked directly."""
    return {"build_gold": build_gold(), "load_warehouse": load_warehouse()}


if __name__ == "__main__":
    run()
