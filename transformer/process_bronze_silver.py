"""
Medallion processor : Kafka(raw_articles) -> MinIO(bronze) -> DQ -> MinIO(silver)

Bronze  : raw JSON event, one object per article, partitioned by source/date.
Silver  : same article enriched with cleaned content + detected language,
          written as JSON, partitioned by source/date.
Rejects : DQ failures are persisted under  bronze/_rejected/...  with the
          reason, so nothing is silently dropped (governance / lineage).

Run as a long-running streaming worker. Idempotent on the Kafka offset
(per-consumer-group) and on the object key (sha1(url)) inside MinIO.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import signal
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple

from bs4 import BeautifulSoup
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from langdetect import DetectorFactory, LangDetectException, detect
from minio import Minio
from minio.error import S3Error

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ARTICLES", "raw_articles")
KAFKA_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bronze-silver-processor")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minio12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "newslake")

# DQ thresholds
MIN_CONTENT_CHARS = int(os.getenv("DQ_MIN_CONTENT_CHARS", "100"))

DetectorFactory.seed = 0  # deterministic langdetect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)
log = logging.getLogger("bronze-silver")


# --------------------------------------------------------------------------- #
# MinIO client
# --------------------------------------------------------------------------- #
def build_minio() -> Minio:
    secure = MINIO_ENDPOINT.startswith("https://")
    host = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")
    client = Minio(host, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=secure)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    return client


def put_json(client: Minio, key: str, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        key,
        data=io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )


# --------------------------------------------------------------------------- #
# Kafka consumer
# --------------------------------------------------------------------------- #
def build_consumer() -> KafkaConsumer:
    last: Optional[Exception] = None
    for attempt in range(1, 21):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP,
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
            )
        except NoBrokersAvailable as exc:
            last = exc
            log.warning("Kafka not ready (attempt %d/20): %s", attempt, exc)
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka: {last}")


# --------------------------------------------------------------------------- #
# Data Quality
# --------------------------------------------------------------------------- #
@dataclass
class DQResult:
    passed: bool
    reason: Optional[str] = None


def dq_check(article: dict) -> DQResult:
    """Completeness + validity gates required by the spec."""
    title = (article.get("titre_article") or "").strip()
    if not title:
        return DQResult(False, "missing_title")

    date = article.get("date_publication")
    if not date:
        return DQResult(False, "missing_date")
    try:
        # validity: must be parseable ISO-ish date
        datetime.fromisoformat(date.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return DQResult(False, "invalid_date")

    content = (article.get("contenu") or "").strip()
    if len(content) < MIN_CONTENT_CHARS:
        return DQResult(False, "content_too_short")

    return DQResult(True)


# --------------------------------------------------------------------------- #
# Cleaning + enrichment (Silver)
# --------------------------------------------------------------------------- #
WS_RE = re.compile(r"\s+")
URL_RE = re.compile(r"https?://\S+")


def strip_html(text: str) -> str:
    if not text:
        return ""
    return BeautifulSoup(text, "lxml").get_text(separator=" ", strip=True)


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = URL_RE.sub(" ", text)
    text = WS_RE.sub(" ", text).strip()
    return text


def detect_language(text: str) -> Optional[str]:
    sample = (text or "").strip()
    if len(sample) < 20:
        return None
    try:
        return detect(sample[:2000])
    except LangDetectException:
        return None


def to_silver(article: dict) -> dict:
    raw_title = article.get("titre_article", "") or ""
    raw_content = article.get("contenu", "") or ""

    title_clean = normalize_text(strip_html(raw_title))
    content_clean = normalize_text(strip_html(raw_content))
    lang = detect_language(content_clean) or detect_language(title_clean)

    return {
        **article,
        "titre_article": title_clean,
        "contenu": content_clean,
        "language": lang,
        "word_count": len(content_clean.split()),
        "char_count": len(content_clean),
        "silver_processed_at": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------- #
# Object keys
# --------------------------------------------------------------------------- #
def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", (s or "unknown")).strip("_") or "unknown"


def _partition_date(article: dict) -> str:
    raw = article.get("date_publication") or article.get("scraped_at")
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:                                                   # noqa: BLE001
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def bronze_key(article: dict) -> str:
    return (f"bronze/source={_safe(article.get('source', 'unknown'))}"
            f"/dt={_partition_date(article)}"
            f"/{article.get('id', 'noid')}.json")


def silver_key(article: dict) -> str:
    return (f"silver/source={_safe(article.get('source', 'unknown'))}"
            f"/dt={_partition_date(article)}"
            f"/{article.get('id', 'noid')}.json")


def reject_key(article: dict, reason: str) -> str:
    return (f"bronze/_rejected/reason={reason}"
            f"/dt={_partition_date(article)}"
            f"/{article.get('id', 'noid')}.json")


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
_running = True


def _stop(_sig, _frm):
    global _running
    log.info("shutdown signal received")
    _running = False


def process(article: dict, client: Minio) -> Tuple[str, Optional[str]]:
    """Land bronze, run DQ, land silver if passing. Returns (status, reason)."""
    # 1) Bronze - persist as-is, no transformation
    try:
        put_json(client, bronze_key(article), article)
    except S3Error as exc:
        log.error("bronze write failed for %s: %s", article.get("id"), exc)
        return "bronze_error", str(exc)

    # 2) Data Quality gate
    dq = dq_check(article)
    if not dq.passed:
        rejected = {**article, "dq_reason": dq.reason,
                    "dq_checked_at": datetime.now(timezone.utc).isoformat()}
        try:
            put_json(client, reject_key(article, dq.reason), rejected)
        except S3Error as exc:
            log.error("reject write failed: %s", exc)
        return "rejected", dq.reason

    # 3) Silver - clean + enrich
    silver = to_silver(article)
    try:
        put_json(client, silver_key(silver), silver)
    except S3Error as exc:
        log.error("silver write failed for %s: %s", article.get("id"), exc)
        return "silver_error", str(exc)

    return "ok", None


def run_batch(idle_timeout_seconds: int = 30, max_seconds: int = 600) -> dict:
    """Drain Kafka -> Bronze + DQ + Silver, exit when topic is quiet.

    Stops after `idle_timeout_seconds` with no new messages, or after
    `max_seconds` total wall time. Used by Airflow as a one-shot task.
    """
    client = build_minio()
    consumer = build_consumer()
    counters = {"ok": 0, "rejected": 0, "bronze_error": 0, "silver_error": 0}
    started = time.time()
    last_msg = time.time()
    try:
        while True:
            if time.time() - started > max_seconds:
                log.info("batch max_seconds reached")
                break
            polled = consumer.poll(timeout_ms=1000, max_records=200)
            if not polled:
                if time.time() - last_msg > idle_timeout_seconds:
                    break
                continue
            for _tp, msgs in polled.items():
                for msg in msgs:
                    article = msg.value or {}
                    status, _ = process(article, client)
                    counters[status] = counters.get(status, 0) + 1
            consumer.commit()
            last_msg = time.time()
    finally:
        consumer.close()
    log.info("batch counters %s", counters)
    return counters


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    client = build_minio()
    consumer = build_consumer()
    log.info("processor started: topic=%s group=%s bucket=%s",
             KAFKA_TOPIC, KAFKA_GROUP, MINIO_BUCKET)

    counters = {"ok": 0, "rejected": 0, "bronze_error": 0, "silver_error": 0}
    last_log = time.time()

    try:
        for msg in consumer:
            if not _running:
                break
            article = msg.value or {}
            status, reason = process(article, client)
            counters[status] = counters.get(status, 0) + 1

            if status == "rejected":
                log.info("DQ reject [%s] %s", reason, article.get("url"))
            elif status == "ok":
                log.debug("ok %s", article.get("url"))

            if time.time() - last_log > 30:
                log.info("counters %s", counters)
                last_log = time.time()
    finally:
        consumer.close()
        log.info("final counters %s", counters)
        log.info("processor stopped")


if __name__ == "__main__":
    sys.exit(main())
