"""Module 4: read normalized social events from Kafka and store them in PostgreSQL.

Delivery semantics are at-least-once: the consumer commits offsets only after the batch has been
committed to PostgreSQL, and every row is an upsert on ``event_id``, so a replay after a crash or
rebalance does not duplicate data. Messages that cannot be normalized go to ``kol.dlq`` with the
reason and the original payload, so a bad producer does not block the partition.

    python main.py                 # DATABASE_URL is required
    DB_WRITER_ONCE=1 python main.py   # drain what is available, then exit (smoke test)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaError, Producer

LOG = logging.getLogger("kol.dbwriter")
STOP = False
SCHEMA_FILE = Path(__file__).with_name("schema.sql")

REQUIRED_FIELDS = (
    "event_id",
    "platform",
    "event_type",
    "content_id",
    "kol_id",
    "created_at",
    "ingested_at",
)

INSERT_SQL = """
INSERT INTO kol.social_events (
    event_id, platform, event_type, content_id, parent_id, kol_id,
    author_id, author_handle, author_name, author_followers,
    text, hashtags, mentions, tags, metrics, lang, default_language, category_id,
    root_id, cid, rev, links, facets, media, topics, content_details,
    live_viewers, author_videos, author_total_views,
    created_at, ingested_at, collector, collector_version
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s
)
ON CONFLICT (event_id) DO UPDATE SET
    text              = EXCLUDED.text,
    hashtags          = EXCLUDED.hashtags,
    mentions          = EXCLUDED.mentions,
    tags              = EXCLUDED.tags,
    metrics           = EXCLUDED.metrics,
    topics            = EXCLUDED.topics,
    media             = EXCLUDED.media,
    facets            = EXCLUDED.facets,
    content_details   = EXCLUDED.content_details,
    cid               = EXCLUDED.cid,
    rev               = EXCLUDED.rev,
    live_viewers      = EXCLUDED.live_viewers,
    author_followers  = COALESCE(EXCLUDED.author_followers, kol.social_events.author_followers),
    author_videos     = COALESCE(EXCLUDED.author_videos, kol.social_events.author_videos),
    author_total_views = COALESCE(EXCLUDED.author_total_views, kol.social_events.author_total_views),
    ingested_at       = EXCLUDED.ingested_at
"""


def split_env(name: str, default: list[str]) -> list[str]:
    values = [value.strip() for value in os.getenv(name, "").split(",") if value.strip()]
    return values or default


def timestamp(value: str) -> datetime:
    """Parse the ISO-8601 timestamps the collectors emit (they always carry UTC)."""
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def as_list(value: Any) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def as_json(value: Any, default: str) -> str:
    """jsonb columns take a JSON string; anything unexpected falls back to the empty default."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return default


def parse_event(raw: bytes) -> dict[str, Any]:
    """Validate one Kafka payload. Raises ValueError for anything that cannot be stored."""
    event = json.loads(raw)
    if not isinstance(event, dict):
        raise ValueError("payload is not a JSON object")
    missing = [field for field in REQUIRED_FIELDS if not event.get(field)]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    author = event.get("author") or {}
    if not author.get("id"):
        raise ValueError("missing author.id")
    try:
        event["event_id"] = UUID(str(event["event_id"]))
    except ValueError as exc:
        raise ValueError(f"event_id is not a UUID: {event['event_id']!r}") from exc
    try:
        timestamp(event["created_at"])
        timestamp(event["ingested_at"])
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {exc}") from exc
    return event


def to_row(event: dict[str, Any]) -> tuple[Any, ...]:
    author = event.get("author") or {}
    source = event.get("source") or {}
    followers = author.get("followers")
    return (
        event["event_id"],
        event["platform"],
        event["event_type"],
        event["content_id"],
        event.get("parent_id"),
        event["kol_id"],
        author["id"],
        author.get("handle") or "",
        author.get("display_name") or "",
        followers if isinstance(followers, int) else None,
        event.get("text") or "",
        as_list(event.get("hashtags")),
        as_list(event.get("mentions")),
        as_list(event.get("tags")),
        as_json(event.get("metrics"), "{}"),
        event.get("lang") or "",
        event.get("default_language") or "",
        event.get("category_id") or "",
        event.get("root_id"),
        event.get("cid") or "",
        event.get("rev") or "",
        as_list(event.get("links")),
        as_json(event.get("facets"), "[]"),
        as_json(event.get("media"), "[]"),
        as_list(event.get("topics")),
        as_json(event.get("content_details"), "{}"),
        event.get("live_viewers") if isinstance(event.get("live_viewers"), int) else None,
        event.get("author_videos") if isinstance(event.get("author_videos"), int) else None,
        event.get("author_total_views") if isinstance(event.get("author_total_views"), int) else None,
        timestamp(event["created_at"]),
        timestamp(event["ingested_at"]),
        source.get("collector") or "",
        source.get("version") or "",
    )


class Store:
    """PostgreSQL connection that survives restarts of the database container."""

    def __init__(self, dsn: str, schema_sql: str, retry_seconds: float) -> None:
        self.dsn = dsn
        self.schema_sql = schema_sql
        self.retry_seconds = retry_seconds
        self.conn: psycopg.Connection | None = None

    def connect(self) -> None:
        while not STOP:
            try:
                self.conn = psycopg.connect(self.dsn, autocommit=False)
                with self.conn.transaction():
                    self.conn.execute(self.schema_sql)
                LOG.info("connected to PostgreSQL, schema is ready")
                return
            except psycopg.Error as exc:
                LOG.error("PostgreSQL unavailable (%s), retrying in %.0fs", exc, self.retry_seconds)
                time.sleep(self.retry_seconds)

    def save(self, events: list[dict[str, Any]]) -> None:
        """Insert a batch, blocking until it is committed. Offsets are only committed afterwards."""
        self.save_rows([(INSERT_SQL, [to_row(event) for event in events])])

    def save_rows(self, statements: list[tuple[str, list[tuple[Any, ...]]]]) -> None:
        """Run every (sql, rows) pair in ONE transaction, retrying until PostgreSQL accepts it."""
        count = sum(len(rows) for _, rows in statements)
        while not STOP:
            try:
                if self.conn is None or self.conn.closed:
                    self.connect()
                    if self.conn is None:
                        return
                with self.conn.transaction():
                    with self.conn.cursor() as cursor:
                        for sql, rows in statements:
                            if rows:
                                cursor.executemany(sql, rows)
                return
            except psycopg.Error as exc:
                LOG.error("write of %d row(s) failed (%s), reconnecting", count, exc)
                if self.conn is not None:
                    try:
                        self.conn.close()
                    except psycopg.Error:
                        pass
                self.conn = None
                time.sleep(self.retry_seconds)

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except psycopg.Error:
                pass


def send_to_dlq(producer: Producer, raw: bytes, error: Exception) -> None:
    record = {
        "source": "db-writer",
        "error": str(error),
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "value": raw.decode("utf-8", "replace"),
    }
    producer.produce(
        os.getenv("DB_WRITER_DLQ_TOPIC", "kol.dlq"),
        value=json.dumps(record, ensure_ascii=False).encode(),
    )
    producer.poll(0)


def main() -> int:
    global STOP

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        LOG.error("DATABASE_URL is not set (example: postgresql://kol_app:PASSWORD@postgres:5432/kol)")
        return 2

    batch_size = int(os.getenv("DB_WRITER_BATCH_SIZE", "500"))
    retry_seconds = float(os.getenv("DB_WRITER_RETRY_SECONDS", "5"))
    topics = split_env("DB_WRITER_TOPICS", ["kol.raw.bluesky", "kol.raw.youtube"])

    def stop(*_: Any) -> None:
        global STOP
        STOP = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
        ),
        "group.id": os.getenv("DB_WRITER_GROUP", "postgres-writer"),
        "client.id": os.getenv("DB_WRITER_CLIENT_ID", "db-writer"),
        "auto.offset.reset": os.getenv("DB_WRITER_OFFSET_RESET", "earliest"),
        # Offsets are committed by hand, after PostgreSQL committed the batch.
        "enable.auto.commit": False,
        "enable.partition.eof": False,
    })
    dlq = Producer({
        "bootstrap.servers": os.getenv(
            "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
        ),
        "client.id": f"{os.getenv('DB_WRITER_CLIENT_ID', 'db-writer')}-dlq",
        "acks": "all",
    })
    store = Store(dsn, SCHEMA_FILE.read_text(encoding="utf-8"), retry_seconds)
    store.connect()

    consumer.subscribe(topics)
    LOG.info("writing %s into PostgreSQL (group %s)", ", ".join(topics), os.getenv("DB_WRITER_GROUP", "postgres-writer"))

    stored = 0
    dead_lettered = 0
    once = os.getenv("DB_WRITER_ONCE", "0") == "1"
    empty_polls = 0

    try:
        while not STOP:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            if not messages:
                empty_polls += 1
                if once and empty_polls >= 2:
                    break
                continue
            empty_polls = 0

            valid: list[dict[str, Any]] = []
            invalid: list[tuple[bytes, Exception]] = []
            for message in messages:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        LOG.warning("consumer error: %s", message.error())
                    continue
                try:
                    valid.append(parse_event(message.value()))
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    invalid.append((message.value(), exc))

            if valid:
                store.save(valid)
                stored += len(valid)
            for raw, exc in invalid:
                LOG.warning("dead-lettering an event: %s", exc)
                send_to_dlq(dlq, raw, exc)
                dead_lettered += 1

            consumer.commit(asynchronous=False)
            if stored and stored % 5000 < batch_size:
                LOG.info("stored %d event(s), %d dead-lettered", stored, dead_lettered)
    finally:
        consumer.close()
        dlq.flush(10)
        store.close()

    LOG.info("done: stored %d event(s), %d dead-lettered", stored, dead_lettered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
