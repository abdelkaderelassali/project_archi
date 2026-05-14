"""
news_pipeline_dag.py
====================

Hourly batch orchestration of the medallion pipeline:

    scrape_articles
        |
        v
    bronze_silver_process   (Kafka -> Bronze + DQ + Silver)
        |
        v
    build_gold              (Silver -> Gold parquet in MinIO)
        |
        v
    load_warehouse          (Gold parquet -> Postgres)

Each task is a one-shot PythonOperator that invokes a callable defined in
the project's existing modules (scraper.scraper, transformer.process_*).
The same modules are used by the long-running streaming services, so batch
and streaming code paths stay in lock-step.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# --------------------------------------------------------------------------- #
# Make the source modules importable. The compose file mounts:
#   ./scraper      -> /opt/airflow/jobs/scraper
#   ./transformer  -> /opt/airflow/jobs/transformer
# --------------------------------------------------------------------------- #
JOBS_ROOT = Path("/opt/airflow/jobs")
for sub in ("scraper", "transformer"):
    p = str(JOBS_ROOT / sub)
    if p not in sys.path:
        sys.path.insert(0, p)


# --------------------------------------------------------------------------- #
# Task callables (thin wrappers - logic lives in the source modules)
# --------------------------------------------------------------------------- #
def task_scrape(**_) -> int:
    """Crawl every adapter once and publish to Kafka topic raw_articles."""
    import scraper                                                     # noqa: WPS433
    n = scraper.run_one_cycle()
    print(f"[scrape] published {n} articles to Kafka")
    return n


def task_bronze_silver(**_) -> dict:
    """Drain Kafka, persist Bronze, run DQ, persist Silver. Exits when idle."""
    import process_bronze_silver as bs                                 # noqa: WPS433
    counters = bs.run_batch(idle_timeout_seconds=30, max_seconds=600)
    print(f"[bronze_silver] counters={counters}")
    return counters


def task_build_gold(**_) -> dict:
    """Aggregate Silver into Gold parquet files in MinIO."""
    import process_gold_dwh as gold                                    # noqa: WPS433
    summary = gold.build_gold()
    print(f"[gold] {summary}")
    return summary


def task_load_warehouse(**_) -> dict:
    """Load Gold parquet from MinIO into the Postgres warehouse."""
    import process_gold_dwh as gold                                    # noqa: WPS433
    summary = gold.load_warehouse()
    print(f"[warehouse] {summary}")
    return summary


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="news_pipeline",
    description="Hourly news scraping + medallion + warehouse load",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["news", "medallion", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    scrape = PythonOperator(
        task_id="scrape_articles",
        python_callable=task_scrape,
    )

    bronze_silver = PythonOperator(
        task_id="bronze_silver_process",
        python_callable=task_bronze_silver,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
    )

    end = EmptyOperator(task_id="end")

    # Linear dependency chain: scrape -> bronze/silver -> gold -> warehouse
    start >> scrape >> bronze_silver >> build_gold >> load_warehouse >> end
