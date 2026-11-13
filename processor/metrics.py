"""Prometheus metrics exposed by Module 3 (stream-processor) on :8000/metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

EVENTS_CONSUMED = Counter(
    "processor_events_consumed_total",
    "Total raw social events consumed and validated by Module 3",
    ["platform", "event_type"],
)

EVENTS_DEDUPLICATED = Counter(
    "processor_events_deduplicated_total",
    "Total duplicate events skipped based on event_id cache",
)

DLQ_MESSAGES = Counter(
    "processor_dlq_messages_total",
    "Total malformed or invalid messages routed to kol.dlq",
    ["reason"],
)

SCORES_PUBLISHED = Counter(
    "processor_kol_scores_published_total",
    "Total KOL score updates published to kol.metrics.kol-scores",
)

KEYWORDS_PUBLISHED = Counter(
    "processor_keywords_published_total",
    "Total trending keyword updates published to kol.metrics.keywords",
)

PROCESSING_LATENCY = Histogram(
    "processor_event_processing_seconds",
    "Time spent validating and updating in-memory state per event",
    buckets=LATENCY_BUCKETS,
)

E2E_LATENCY = Histogram(
    "processor_e2e_latency_seconds",
    "End-to-end latency from Kafka record timestamp to stream processor ingestion",
    buckets=LATENCY_BUCKETS,
)

ACTIVE_KOLS = Gauge(
    "processor_active_kols",
    "Number of distinct KOLs currently tracked in memory",
)

ACTIVE_KEYWORDS = Gauge(
    "processor_active_keywords",
    "Number of distinct keywords/hashtags currently tracked in memory",
)

DEDUP_CACHE_ENTRIES = Gauge(
    "processor_dedup_cache_entries",
    "Number of event_ids currently stored in the deduplication LRU cache",
)

ASSIGNED_PARTITIONS = Gauge(
    "processor_assigned_partitions",
    "Number of Kafka partitions currently assigned to this consumer instance",
)

REBALANCES = Counter(
    "processor_rebalances_total",
    "Total consumer group partition assignment/revocation events",
    ["event"],
)

KOLS_EVICTED = Counter(
    "processor_kols_evicted_total",
    "KOLs dropped from memory because MAX_ACTIVE_KOLS was reached (least recently active first)",
)

KOLS_RESTORED = Counter(
    "processor_kols_restored_total",
    "KOLs whose cumulative totals were restored from PostgreSQL (kol.kol_scores)",
)
