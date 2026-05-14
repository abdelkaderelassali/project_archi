"""
News scraper -> Kafka producer.

Targets:
  * Hespress    (Maroc)         https://www.hespress.com/feed
  * BBC News    (International) http://feeds.bbci.co.uk/news/rss.xml

Output: JSON event per article on Kafka topic `raw_articles`
        with the exact schema required by the project spec:

  {
    "titre_article":     str,
    "auteur":            str | None,
    "date_publication":  ISO-8601 str | None,
    "categorie":         str | None,
    "contenu":           str,
    "source":            str,
    "url":               str,
    # technical metadata
    "id":                sha1(url),
    "scraped_at":        ISO-8601 str
  }
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_ARTICLES", "raw_articles")
INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "3600"))
MAX_PER_FEED = int(os.getenv("SCRAPE_MAX_ARTICLES_PER_FEED", "15"))
FETCH_CONTENT = os.getenv("SCRAPE_FETCH_CONTENT", "true").lower() == "true"
HTTP_TIMEOUT = int(os.getenv("SCRAPE_HTTP_TIMEOUT", "20"))

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 NewsLakeBot/1.0"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)
log = logging.getLogger("scraper")


# --------------------------------------------------------------------------- #
# Domain model
# --------------------------------------------------------------------------- #
@dataclass
class Article:
    titre_article: str
    auteur: Optional[str]
    date_publication: Optional[str]
    categorie: Optional[str]
    contenu: str
    source: str
    url: str
    id: str
    scraped_at: str

    @classmethod
    def build(
        cls,
        title: str,
        author: Optional[str],
        published: Optional[str],
        category: Optional[str],
        content: str,
        source: str,
        url: str,
    ) -> "Article":
        return cls(
            titre_article=(title or "").strip(),
            auteur=(author or None),
            date_publication=published,
            categorie=(category or None),
            contenu=(content or "").strip(),
            source=source,
            url=url,
            id=hashlib.sha1(url.encode("utf-8")).hexdigest(),
            scraped_at=datetime.now(timezone.utc).isoformat(),
        )


# --------------------------------------------------------------------------- #
# HTTP helper
# --------------------------------------------------------------------------- #
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en,fr,ar;q=0.8"})


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def http_get(url: str) -> requests.Response:
    resp = SESSION.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp


def safe_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return dateparser.parse(value).astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# Adapter base
# --------------------------------------------------------------------------- #
class NewsAdapter(ABC):
    name: str
    feed_url: str

    @abstractmethod
    def parse_feed(self, xml: str) -> List[dict]:
        """Return a list of dicts with at least: title, link, pubDate, category."""

    def fetch_content(self, url: str) -> tuple[str, Optional[str]]:
        """Return (content_text, author) by parsing the article page."""
        return "", None

    def crawl(self) -> Iterable[Article]:
        log.info("[%s] fetching feed %s", self.name, self.feed_url)
        try:
            xml = http_get(self.feed_url).text
        except Exception as exc:                                       # noqa: BLE001
            log.error("[%s] feed fetch failed: %s", self.name, exc)
            return

        items = self.parse_feed(xml)[:MAX_PER_FEED]
        log.info("[%s] %d items in feed", self.name, len(items))

        for item in items:
            url = item.get("link")
            if not url:
                continue
            content, author = item.get("summary", ""), item.get("author")

            if FETCH_CONTENT:
                try:
                    fetched_content, fetched_author = self.fetch_content(url)
                    if fetched_content:
                        content = fetched_content
                    if not author and fetched_author:
                        author = fetched_author
                except Exception as exc:                                # noqa: BLE001
                    log.warning("[%s] content fetch failed for %s: %s",
                                self.name, url, exc)

            yield Article.build(
                title=item.get("title", ""),
                author=author,
                published=safe_iso(item.get("pubDate")),
                category=item.get("category"),
                content=content,
                source=self.name,
                url=url,
            )


# --------------------------------------------------------------------------- #
# Hespress (Maroc)
# --------------------------------------------------------------------------- #
class HespressAdapter(NewsAdapter):
    name = "Hespress"
    feed_url = "https://www.hespress.com/feed"

    def parse_feed(self, xml: str) -> List[dict]:
        soup = BeautifulSoup(xml, "lxml-xml")
        results: List[dict] = []
        for it in soup.find_all("item"):
            results.append({
                "title":    (it.title.text       if it.title       else "").strip(),
                "link":     (it.link.text        if it.link        else "").strip(),
                "pubDate":  (it.pubDate.text     if it.pubDate     else None),
                "category": (it.category.text    if it.category    else None),
                "summary":  (it.description.text if it.description else "").strip(),
                "author":   (it.find("dc:creator").text
                             if it.find("dc:creator") else None),
            })
        return results

    def fetch_content(self, url: str) -> tuple[str, Optional[str]]:
        html = http_get(url).text
        soup = BeautifulSoup(html, "lxml")

        # Article body lives in <div class="article-content"> on Hespress
        body = soup.select_one("div.article-content") or soup.find("article")
        text = "\n".join(p.get_text(" ", strip=True)
                         for p in (body.find_all("p") if body else []))

        author_tag = soup.select_one("small.author a") or soup.select_one(".author a")
        author = author_tag.get_text(strip=True) if author_tag else None
        return text, author


# --------------------------------------------------------------------------- #
# BBC News (International)
# --------------------------------------------------------------------------- #
class BBCAdapter(NewsAdapter):
    name = "BBC News"
    feed_url = "http://feeds.bbci.co.uk/news/rss.xml"

    def parse_feed(self, xml: str) -> List[dict]:
        soup = BeautifulSoup(xml, "lxml-xml")
        results: List[dict] = []
        # BBC top-level <category> describes the feed; per-item we use <category> if present
        for it in soup.find_all("item"):
            results.append({
                "title":    (it.title.text       if it.title       else "").strip(),
                "link":     (it.link.text        if it.link        else "").strip(),
                "pubDate":  (it.pubDate.text     if it.pubDate     else None),
                "category": (it.category.text    if it.category    else "News"),
                "summary":  (it.description.text if it.description else "").strip(),
                "author":   None,  # BBC RSS does not expose author
            })
        return results

    def fetch_content(self, url: str) -> tuple[str, Optional[str]]:
        html = http_get(url).text
        soup = BeautifulSoup(html, "lxml")

        # Modern BBC article body: <article> with <div data-component="text-block">
        blocks = soup.select('article div[data-component="text-block"] p')
        if not blocks:
            blocks = soup.select("article p")
        text = "\n".join(p.get_text(" ", strip=True) for p in blocks)

        # Byline
        byline = soup.select_one('[data-component="byline-block"]') \
                 or soup.select_one(".ssrcss-68pt20-Text-TextContributorName")
        author = byline.get_text(" ", strip=True) if byline else None
        return text, author


# --------------------------------------------------------------------------- #
# Reuters - placeholder (RSS feeds discontinued, site blocks scrapers)
# --------------------------------------------------------------------------- #
class ReutersAdapter(NewsAdapter):                                     # pragma: no cover
    """Kept as documentation - Reuters now requires a paid Connect API.
    Implement here using their licensed endpoint when available."""
    name = "Reuters"
    feed_url = ""

    def parse_feed(self, xml: str) -> List[dict]:
        return []


# --------------------------------------------------------------------------- #
# Kafka producer
# --------------------------------------------------------------------------- #
def build_producer() -> KafkaProducer:
    last_err: Optional[Exception] = None
    for attempt in range(1, 21):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=200,
                retries=5,
            )
        except NoBrokersAvailable as exc:
            last_err = exc
            log.warning("Kafka not ready (attempt %d/20): %s", attempt, exc)
            time.sleep(5)
    raise RuntimeError(f"Could not connect to Kafka: {last_err}")


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
ADAPTERS: List[NewsAdapter] = [HespressAdapter(), BBCAdapter()]

_running = True


def _stop(_sig, _frm):
    global _running
    log.info("shutdown signal received")
    _running = False


def run_once(producer: KafkaProducer) -> int:
    sent = 0
    for adapter in ADAPTERS:
        for article in adapter.crawl():
            if not article.titre_article or not article.url:
                continue
            payload = asdict(article)
            producer.send(KAFKA_TOPIC, key=article.id, value=payload)
            sent += 1
            log.info("-> %s | %s | %s",
                     article.source, article.titre_article[:80], article.url)
    producer.flush()
    return sent


def run_one_cycle() -> int:
    """One-shot scrape: build producer, crawl all adapters once, close.
    Returns the number of articles published. Used by Airflow."""
    producer = build_producer()
    try:
        return run_once(producer)
    finally:
        producer.flush()
        producer.close()


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = build_producer()
    log.info("scraper started: topic=%s interval=%ds adapters=%s",
             KAFKA_TOPIC, INTERVAL, [a.name for a in ADAPTERS])

    while _running:
        started = time.time()
        try:
            n = run_once(producer)
            log.info("cycle done: %d articles published", n)
        except Exception as exc:                                        # noqa: BLE001
            log.exception("cycle failed: %s", exc)

        # Sleep with periodic wakeups so SIGTERM is responsive
        elapsed = time.time() - started
        remaining = max(0, INTERVAL - elapsed)
        while _running and remaining > 0:
            time.sleep(min(5, remaining))
            remaining -= 5

    producer.close()
    log.info("scraper stopped cleanly")


if __name__ == "__main__":
    sys.exit(main())
