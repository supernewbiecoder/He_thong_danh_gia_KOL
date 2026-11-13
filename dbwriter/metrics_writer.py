"""Module 4: store the results of Module 3 (kol.metrics.*) in PostgreSQL.

    kol.metrics.kol-scores -> kol.kol_scores (latest per KOL) + kol.kol_score_history (per minute)
    kol.metrics.keywords   -> kol.keyword_metrics (latest per keyword)

Same delivery rule as main.py: offsets are committed only after the PostgreSQL transaction commits.
An update never overwrites a newer one (updated_at guard), so a replay after a crash is harmless.

    python metrics_writer.py          # DATABASE_URL is required
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError

import main as writer

LOG = logging.getLogger("kol.metrics_writer")

SCORES_SQL = """
INSERT INTO kol.kol_scores (
    kol_id, platform, handle, followers, total_events, window_events, window_minutes,
    event_breakdown, metrics_total, metrics_window, engagement_score, window_score,
    velocity_per_min, top_hashtags, last_event_at, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (kol_id) DO UPDATE SET
    platform         = EXCLUDED.platform,
    handle           = COALESCE(EXCLUDED.handle, kol.kol_scores.handle),
    followers        = COALESCE(EXCLUDED.followers, kol.kol_scores.followers),
    total_events     = EXCLUDED.total_events,
    window_events    = EXCLUDED.window_events,
    window_minutes   = EXCLUDED.window_minutes,
    event_breakdown  = EXCLUDED.event_breakdown,
    metrics_total    = EXCLUDED.metrics_total,
    metrics_window   = EXCLUDED.metrics_window,
    engagement_score = EXCLUDED.engagement_score,
    window_score     = EXCLUDED.window_score,
    velocity_per_min = EXCLUDED.velocity_per_min,
    top_hashtags     = EXCLUDED.top_hashtags,
    last_event_at    = COALESCE(EXCLUDED.last_event_at, kol.kol_scores.last_event_at),
    updated_at       = EXCLUDED.updated_at
WHERE kol.kol_scores.updated_at <= EXCLUDED.updated_at
"""

HISTORY_SQL = """
INSERT INTO kol.kol_score_history (kol_id, bucket, engagement_score, window_score, velocity_per_min, total_events)
VALUES (%s, date_trunc('minute', %s::timestamptz), %s, %s, %s, %s)
ON CONFLICT (kol_id, bucket) DO UPDATE SET
    engagement_score = EXCLUDED.engagement_score,
    window_score     = EXCLUDED.window_score,
    velocity_per_min = EXCLUDED.velocity_per_min,
    total_events     = EXCLUDED.total_events
"""

KEYWORDS_SQL = """
INSERT INTO kol.keyword_metrics (
    keyword, is_hashtag, count, total_count, window_minutes, velocity_per_min, platforms, unique_kols, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (keyword) DO UPDATE SET
    is_hashtag       = kol.keyword_metrics.is_hashtag OR EXCLUDED.is_hashtag,
    count            = EXCLUDED.count,
    total_count      = GREATEST(kol.keyword_metrics.total_count, EXCLUDED.total_count),
    window_minutes   = EXCLUDED.window_minutes,
    velocity_per_min = EXCLUDED.velocity_per_min,
    platforms        = EXCLUDED.platforms,
    unique_kols      = EXCLUDED.unique_kols,
    updated_at       = EXCLUDED.updated_at
WHERE kol.keyword_metrics.updated_at <= EXCLUDED.updated_at
"""

PRUNE_SQL = "DELETE FROM kol.kol_score_history WHERE bucket < now() - make_interval(days => %s)"


def optional_time(value: Any) -> datetime | None:
    try:
        return writer.timestamp(value) if value else None
    except ValueError:
        return None


def score_rows(payload: dict[str, Any]) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    updated_at = writer.timestamp(payload["updated_at"])
    followers = payload.get("followers")
    score = (
        payload["kol_id"],
        payload.get("platform") or payload["kol_id"].split(":", 1)[0],
        payload.get("handle") or None,
        followers if isinstance(followers, int) else None,
        int(payload.get("total_events") or 0),
        int(payload.get("window_events") or 0),
        int(payload.get("window_minutes") or 15),
        writer.as_json(payload.get("event_breakdown"), "{}"),
        writer.as_json(payload.get("metrics_total"), "{}"),
        writer.as_json(payload.get("metrics_window"), "{}"),
        float(payload.get("engagement_score") or 0),
        float(payload.get("window_score") or 0),
        float(payload.get("velocity_per_min") or 0),
        writer.as_list(payload.get("top_hashtags")),
        optional_time(payload.get("last_event_at")),
        updated_at,
    )
    history = (
        payload["kol_id"], updated_at,
        float(payload.get("engagement_score") or 0),
        float(payload.get("window_score") or 0),
        float(payload.get("velocity_per_min") or 0),
        int(payload.get("total_events") or 0),
    )
    return score, history


def keyword_row(payload: dict[str, Any]) -> tuple[Any, ...]:
    return (
        payload["keyword"],
        bool(payload.get("is_hashtag")),
        int(payload.get("count") or 0),
        int(payload.get("total_count") or 0),
        int(payload.get("window_minutes") or 15),
        float(payload.get("velocity_per_min") or 0),
        writer.as_json(payload.get("platforms"), "{}"),
        int(payload.get("unique_kols") or 0),
        writer.timestamp(payload["updated_at"]),
    )


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        LOG.error("DATABASE_URL is not set (example: postgresql://kol_app:PASSWORD@kol-db-rw:5432/kol)")
        return 2

    scores_topic = os.getenv("SCORES_TOPIC", "kol.metrics.kol-scores")
    keywords_topic = os.getenv("KEYWORDS_TOPIC", "kol.metrics.keywords")
    batch_size = int(os.getenv("METRICS_WRITER_BATCH_SIZE", "500"))
    retention_days = int(os.getenv("HISTORY_RETENTION_DAYS", "7"))

    def stop(*_: Any) -> None:
        writer.STOP = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"),
        "group.id": os.getenv("METRICS_WRITER_GROUP", "postgres-metrics-writer"),
        "client.id": "metrics-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    store = writer.Store(dsn, writer.SCHEMA_FILE.read_text(encoding="utf-8"),
                         float(os.getenv("DB_WRITER_RETRY_SECONDS", "5")))
    store.connect()
    consumer.subscribe([scores_topic, keywords_topic])
    LOG.info("writing %s and %s into PostgreSQL", scores_topic, keywords_topic)

    stored = 0
    last_prune = 0.0
    try:
        while not writer.STOP:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            scores: list[tuple[Any, ...]] = []
            history: list[tuple[Any, ...]] = []
            keywords: list[tuple[Any, ...]] = []
            for message in messages:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        LOG.warning("consumer error: %s", message.error())
                    continue
                try:
                    payload = json.loads(message.value())
                    if message.topic() == scores_topic:
                        score, point = score_rows(payload)
                        scores.append(score)
                        history.append(point)
                    else:
                        keywords.append(keyword_row(payload))
                except (ValueError, TypeError, KeyError) as exc:
                    # Module 3 output is internal; log and skip rather than block the partition.
                    LOG.warning("skipping malformed metrics record on %s: %s", message.topic(), exc)

            if scores or keywords:
                store.save_rows([(SCORES_SQL, scores), (HISTORY_SQL, history), (KEYWORDS_SQL, keywords)])
                stored += len(scores) + len(keywords)
            if messages:
                consumer.commit(asynchronous=False)

            if time.monotonic() - last_prune > 3600:
                store.save_rows([(PRUNE_SQL, [(retention_days,)])])
                last_prune = time.monotonic()
                LOG.info("stored %d metrics record(s) so far; history older than %d day(s) pruned",
                         stored, retention_days)
    finally:
        consumer.close()
        store.close()
    LOG.info("done: stored %d metrics record(s)", stored)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
