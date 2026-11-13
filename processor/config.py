"""Configuration and CLI arguments for Module 3 (Stream Processor)."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class ScoringWeights:
    """Weights used to calculate a KOL's engagement score."""

    post_event: float = 5.0
    comment_event: float = 2.0
    reply_event: float = 2.0
    like_event: float = 1.0
    repost_event: float = 3.0
    follow_event: float = 4.0
    delete_event: float = 0.0

    like_metric: float = 1.0
    comment_metric: float = 3.0
    share_metric: float = 5.0
    view_metric: float = 0.01

    def event_base_weight(self, event_type: str) -> float:
        mapping = {
            "post": self.post_event,
            "comment": self.comment_event,
            "reply": self.reply_event,
            "like": self.like_event,
            "repost": self.repost_event,
            "follow": self.follow_event,
            "delete": self.delete_event,
        }
        return mapping.get(event_type, 1.0)

    def calculate_event_score(self, event_type: str, metrics: dict) -> float:
        likes = max(0, int(metrics.get("likes") or 0))
        comments = max(0, int(metrics.get("comments") or 0))
        shares = max(0, int(metrics.get("shares") or 0))
        views = max(0, int(metrics.get("views") or 0))

        base = self.event_base_weight(event_type)
        interaction = (
            likes * self.like_metric
            + comments * self.comment_metric
            + shares * self.share_metric
            + views * self.view_metric
        )
        return round(base + interaction, 4)


@dataclass(frozen=True)
class ProcessorConfig:
    bootstrap_servers: str
    group_id: str
    input_topics: list[str]
    scores_topic: str
    keywords_topic: str
    dlq_topic: str
    offset_reset: str
    window_minutes: int
    flush_interval_sec: float
    top_keywords_limit: int
    dedup_cache_size: int
    batch_size: int
    poll_timeout_sec: float
    metrics_port: int
    extract_text_keywords: bool
    weights: ScoringWeights


def parse_config(argv: list[str] | None = None) -> ProcessorConfig:
    p = argparse.ArgumentParser(
        description="Module 3: Real-time Stream Processor for KOL scoring and trending keywords."
    )
    p.add_argument(
        "--bootstrap",
        default=env("BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"),
        help="Kafka bootstrap servers [BOOTSTRAP_SERVERS]",
    )
    p.add_argument(
        "--group",
        default=env("KAFKA_GROUP_ID", "stream-processor"),
        help="Kafka consumer group ID [KAFKA_GROUP_ID]",
    )
    p.add_argument(
        "--input-topics",
        default=env("INPUT_TOPICS", "kol.raw.bluesky,kol.raw.youtube"),
        help="Comma-separated input topics [INPUT_TOPICS]",
    )
    p.add_argument(
        "--scores-topic",
        default=env("SCORES_TOPIC", "kol.metrics.kol-scores"),
        help="Output topic for KOL scores [SCORES_TOPIC]",
    )
    p.add_argument(
        "--keywords-topic",
        default=env("KEYWORDS_TOPIC", "kol.metrics.keywords"),
        help="Output topic for keyword frequencies [KEYWORDS_TOPIC]",
    )
    p.add_argument(
        "--dlq-topic",
        default=env("DLQ_TOPIC", "kol.dlq"),
        help="Dead-letter queue topic for malformed events [DLQ_TOPIC]",
    )
    p.add_argument(
        "--offset-reset",
        default=env("OFFSET_RESET", "earliest"),
        choices=("earliest", "latest"),
        help="Consumer auto.offset.reset policy [OFFSET_RESET]",
    )
    p.add_argument(
        "--window-minutes",
        type=int,
        default=int(env("WINDOW_MINUTES", "15")),
        help="Sliding window size in minutes [WINDOW_MINUTES]",
    )
    p.add_argument(
        "--flush-interval",
        type=float,
        default=float(env("FLUSH_INTERVAL_SEC", "5.0")),
        help="Seconds between publishing aggregated metrics & committing offsets [FLUSH_INTERVAL_SEC]",
    )
    p.add_argument(
        "--top-keywords",
        type=int,
        default=int(env("TOP_KEYWORDS_LIMIT", "100")),
        help="Maximum trending keywords published per flush cycle [TOP_KEYWORDS_LIMIT]",
    )
    p.add_argument(
        "--dedup-size",
        type=int,
        default=int(env("DEDUP_CACHE_SIZE", "200000")),
        help="Max recent event_ids kept in memory for deduplication [DEDUP_CACHE_SIZE]",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(env("CONSUMER_BATCH_SIZE", "500")),
        help="Max messages consumed per poll batch [CONSUMER_BATCH_SIZE]",
    )
    p.add_argument(
        "--poll-timeout",
        type=float,
        default=float(env("CONSUMER_POLL_TIMEOUT_SEC", "1.0")),
        help="Consumer consume() timeout in seconds [CONSUMER_POLL_TIMEOUT_SEC]",
    )
    p.add_argument(
        "--metrics-port",
        type=int,
        default=int(env("METRICS_PORT", "8000")),
        help="Prometheus metrics HTTP port, 0 = disabled [METRICS_PORT]",
    )
    p.add_argument(
        "--no-text-keywords",
        action="store_true",
        default=env("EXTRACT_TEXT_KEYWORDS", "true").lower() in ("0", "false", "no"),
        help="Only count explicit hashtags, skip tokenizing event text",
    )

    args = p.parse_args(argv)
    topics = [t.strip() for t in args.input_topics.split(",") if t.strip()]

    weights = ScoringWeights(
        post_event=float(env("WEIGHT_POST_EVENT", "5.0")),
        comment_event=float(env("WEIGHT_COMMENT_EVENT", "2.0")),
        reply_event=float(env("WEIGHT_REPLY_EVENT", "2.0")),
        like_event=float(env("WEIGHT_LIKE_EVENT", "1.0")),
        repost_event=float(env("WEIGHT_REPOST_EVENT", "3.0")),
        follow_event=float(env("WEIGHT_FOLLOW_EVENT", "4.0")),
        delete_event=float(env("WEIGHT_DELETE_EVENT", "0.0")),
        like_metric=float(env("WEIGHT_LIKE_METRIC", "1.0")),
        comment_metric=float(env("WEIGHT_COMMENT_METRIC", "3.0")),
        share_metric=float(env("WEIGHT_SHARE_METRIC", "5.0")),
        view_metric=float(env("WEIGHT_VIEW_METRIC", "0.01")),
    )

    return ProcessorConfig(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        input_topics=topics,
        scores_topic=args.scores_topic,
        keywords_topic=args.keywords_topic,
        dlq_topic=args.dlq_topic,
        offset_reset=args.offset_reset,
        window_minutes=max(1, args.window_minutes),
        flush_interval_sec=max(0.5, args.flush_interval),
        top_keywords_limit=max(1, args.top_keywords),
        dedup_cache_size=max(1000, args.dedup_size),
        batch_size=max(1, args.batch_size),
        poll_timeout_sec=max(0.1, args.poll_timeout),
        metrics_port=args.metrics_port,
        extract_text_keywords=not args.no_text_keywords,
        weights=weights,
    )
