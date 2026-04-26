"""
NewsAPI Producer
================
Fetches real-time top-headlines from NewsAPI every POLL_INTERVAL seconds
and publishes each article as a JSON message to Kafka topic `news-raw`.

Message schema
--------------
{
    "url":          "<article URL>",
    "source":       "<outlet name>",
    "published_at": "<ISO-8601 timestamp>",
    "fetched_at":   "<ISO-8601 UTC timestamp>",
    "text":         "<title + description + content concatenated>"
}

Usage
-----
    python news_producer.py [bootstrap_servers]

    bootstrap_servers  Comma-separated Kafka host:port list.
                       Default: localhost:9092

Prerequisites
-------------
    pip install newsapi-python kafka-python
    Environment variable API_KEY must be set (or edit the fallback below).
"""

import itertools
import json
import os
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from newsapi import NewsApiClient

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("API_KEY", "864886f839ee48219caf2bf41dcd108a")
TOPIC          = "news-raw"
POLL_INTERVAL  = 60          # seconds between NewsAPI calls
PAGE_SIZE      = 100         # max articles per API call (NewsAPI hard limit)
MAX_SEEN_URLS  = 50_000      # cap the dedup set to avoid unbounded memory growth

# Rotate through these queries so each poll fetches a different slice of news
SEARCH_QUERIES = [
    "technology", "politics", "economy", "sports", "science",
    "health", "climate", "AI", "war", "election",
    "business", "entertainment", "crime", "education", "energy",
]


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a Kafka producer with JSON value serialisation."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
            )
            print(f"[producer] Connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/5), retrying in 5s …")
            time.sleep(5)
    raise RuntimeError(f"Cannot reach Kafka at {bootstrap_servers} after 5 attempts.")


def article_to_text(article: dict) -> str:
    """Concatenate title, description, and content into a single string."""
    parts = [
        article.get("title") or "",
        article.get("description") or "",
        article.get("content") or "",
    ]
    return " ".join(p for p in parts if p).strip()


def fetch_headlines(newsapi: NewsApiClient, query: str) -> list:
    """Return articles from get_everything, falling back to top-headlines on error."""
    try:
        response = newsapi.get_everything(
            q=str(query),
            language="en",
            sort_by="publishedAt",
            page_size=PAGE_SIZE,
        )
        if response.get("status") != "ok":
            raise ValueError(response.get("message", "non-ok status"))
        return response.get("articles") or []
    except Exception as exc:
        print(f"[producer] get_everything failed ({exc}), falling back to top-headlines", file=sys.stderr)
    try:
        response = newsapi.get_top_headlines(language="en", page_size=PAGE_SIZE)
        return response.get("articles") or []
    except Exception as exc:
        print(f"[producer] NewsAPI error: {exc}", file=sys.stderr)
        return []


def main() -> None:
    bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"

    newsapi  = NewsApiClient(api_key=API_KEY)
    producer = build_producer(bootstrap_servers)

    seen_urls: set = set()
    query_cycle = itertools.cycle(SEARCH_QUERIES)
    print(f"[producer] Polling NewsAPI every {POLL_INTERVAL}s → topic '{TOPIC}' …")

    while True:
        query     = next(query_cycle)
        articles  = fetch_headlines(newsapi, query)
        new_count = 0

        for article in articles:
            url = article.get("url") or ""
            if not url or url in seen_urls:
                continue

            text = article_to_text(article)
            if not text:
                continue

            # Trim dedup set if it grows too large (rolling window)
            if len(seen_urls) >= MAX_SEEN_URLS:
                seen_urls.clear()
            seen_urls.add(url)

            message = {
                "url":          url,
                "source":       (article.get("source") or {}).get("name", ""),
                "published_at": article.get("publishedAt") or "",
                "fetched_at":   datetime.now(timezone.utc).isoformat(),
                "text":         text,
            }
            producer.send(TOPIC, key=url, value=message)
            new_count += 1

        producer.flush()
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        print(
            f"[producer] {ts}  query={query:<14}  sent={new_count:>3}  "
            f"total_seen={len(seen_urls):>6}  sleeping {POLL_INTERVAL}s …"
        )
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
