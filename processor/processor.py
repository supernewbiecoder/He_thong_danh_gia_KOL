"""Main Kafka Consumer -> In-Memory Aggregator -> Kafka Producer loop for Module 3.

Usage:
    python -m processor.processor
    python processor/processor.py --bootstrap 127.0.0.1:9092 --flush-interval 5
"""

from __future__ import annotations

import json
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Allow running directly as `python processor/processor.py` or as `python -m processor.processor`
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, KafkaError, Producer, TIMESTAMP_NOT_AVAILABLE
from prometheus_client import start_http_server

from processor.config import ProcessorConfig, parse_config
from processor.metrics import (
    ACTIVE_KEYWORDS,
    ACTIVE_KOLS,
    ASSIGNED_PARTITIONS,
    DEDUP_CACHE_ENTRIES,
    DLQ_MESSAGES,
    E2E_LATENCY,
    EVENTS_CONSUMED,
    EVENTS_DEDUPLICATED,
    KEYWORDS_PUBLISHED,
    KOLS_EVICTED,
    KOLS_RESTORED,
    PROCESSING_LATENCY,
    REBALANCES,
    SCORES_PUBLISHED,
)
from processor.state import EventValidationError, StreamState
from processor.store import ScoreStore

stop_event = threading.Event()


def build_producer(config: ProcessorConfig, instance_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"stream-processor-producer-{instance_id}",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 10,
            "batch.size": 131072,
            "queue.buffering.max.messages": 200_000,
        }
    )


def build_consumer(config: ProcessorConfig, instance_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.group_id,
            "client.id": f"stream-processor-consumer-{instance_id}",
            "auto.offset.reset": config.offset_reset,
            "enable.auto.commit": False,
        }
    )


def publish_json(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    encoded_key = key.encode("utf-8")
    encoded_val = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def on_delivery(err, _msg) -> None:
        if err is not None:
            print(f"[processor] delivery failed on {topic}: {err}", file=sys.stderr)

    while True:
        try:
            producer.produce(topic, key=encoded_key, value=encoded_val, on_delivery=on_delivery)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.1)


def send_to_dlq(
    producer: Producer,
    dlq_topic: str,
    source_topic: str,
    partition: int,
    offset: int,
    raw_bytes: bytes | None,
    reason: str,
    error_detail: str,
) -> None:
    DLQ_MESSAGES.labels(reason=reason).inc()
    raw_text = ""
    if raw_bytes is not None:
        raw_text = raw_bytes.decode("utf-8", errors="replace")[:4096]
    envelope = {
        "module": "stream-processor",
        "source_topic": source_topic,
        "partition": partition,
        "offset": offset,
        "reason": reason,
        "error": error_detail,
        "raw_payload": raw_text,
        "failed_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    publish_json(producer, dlq_topic, f"{source_topic}:{partition}:{offset}", envelope)


def flush_and_commit(
    state: StreamState,
    producer: Producer,
    consumer: Consumer,
    config: ProcessorConfig,
    has_uncommitted_offsets: bool,
    sync_commit: bool = False,
) -> tuple[int, int]:
    """Publish dirty KOL scores and top keywords, flush producer callbacks, and commit offsets."""
    now_ts = time.time()
    kol_outputs, keyword_outputs = state.collect_dirty_outputs(
        now_ts=now_ts,
        top_keywords_limit=config.top_keywords_limit,
    )

    for kol_id, payload in kol_outputs:
        publish_json(producer, config.scores_topic, kol_id, payload)
        SCORES_PUBLISHED.inc()

    for keyword, payload in keyword_outputs:
        publish_json(producer, config.keywords_topic, keyword, payload)
        KEYWORDS_PUBLISHED.inc()

    if kol_outputs or keyword_outputs:
        producer.poll(0)

    if has_uncommitted_offsets:
        if sync_commit:
            producer.flush(10.0)
        try:
            consumer.commit(asynchronous=not sync_commit)
        except Exception as exc:
            # Ignore _NO_OFFSET when no messages have been read yet
            if "_NO_OFFSET" not in str(exc):
                print(f"[processor] offset commit warning: {exc}", file=sys.stderr)

    # Only records published above can be evicted: their totals are already on their way to
    # PostgreSQL, where seed_kol finds them again.
    evicted = state.evict_inactive()
    if evicted:
        KOLS_EVICTED.inc(evicted)

    ACTIVE_KOLS.set(len(state.kols))
    ACTIVE_KEYWORDS.set(len(state.keywords))
    DEDUP_CACHE_ENTRIES.set(len(state.dedup))

    return len(kol_outputs), len(keyword_outputs)


def run_processor(config: ProcessorConfig) -> None:
    instance_id = f"{socket.gethostname()}-{int(time.time()) % 10000}"
    state = StreamState(
        window_minutes=config.window_minutes,
        dedup_cache_size=config.dedup_cache_size,
        extract_text_keywords=config.extract_text_keywords,
        weights=config.weights,
        max_active_kols=config.max_active_kols,
        min_keyword_kols=config.min_keyword_kols,
    )
    store = ScoreStore(config.database_url) if config.database_url else None
    producer = build_producer(config, instance_id)
    consumer = build_consumer(config, instance_id)

    has_uncommitted = False

    def on_assign(_c: Consumer, partitions: list) -> None:
        REBALANCES.labels(event="assign").inc()
        ASSIGNED_PARTITIONS.set(len(partitions))
        parts_str = ", ".join(f"{p.topic}[{p.partition}]" for p in partitions)
        print(f"[processor] assigned {len(partitions)} partition(s): {parts_str}")

    def on_revoke(_c: Consumer, partitions: list) -> None:
        nonlocal has_uncommitted
        REBALANCES.labels(event="revoke").inc()
        print(f"[processor] revoking {len(partitions)} partition(s), flushing state before rebalance...")
        flush_and_commit(
            state=state,
            producer=producer,
            consumer=consumer,
            config=config,
            has_uncommitted_offsets=has_uncommitted,
            sync_commit=True,
        )
        has_uncommitted = False
        ASSIGNED_PARTITIONS.set(0)

    consumer.subscribe(config.input_topics, on_assign=on_assign, on_revoke=on_revoke)
    print(
        f"[processor] started group={config.group_id} topics={config.input_topics} "
        f"window={config.window_minutes}m flush={config.flush_interval_sec}s "
        f"bootstrap={config.bootstrap_servers}"
    )

    last_flush = time.monotonic()
    last_log = last_flush
    processed_since_log = 0

    try:
        while not stop_event.is_set():
            msgs = consumer.consume(
                num_messages=config.batch_size,
                timeout=config.poll_timeout_sec,
            )

            now_wall = time.time()
            decoded: list[tuple[Any, bytes | None, Any, float]] = []
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[processor] consumer error: {msg.error()}", file=sys.stderr)
                    continue

                has_uncommitted = True
                ts_type, ts_ms = msg.timestamp()
                if ts_type != TIMESTAMP_NOT_AVAILABLE and ts_ms > 0:
                    E2E_LATENCY.observe(max(0.0, now_wall - ts_ms / 1000.0))

                t0 = time.perf_counter()
                raw_bytes = msg.value()
                try:
                    payload = json.loads(raw_bytes) if raw_bytes else None
                except (ValueError, TypeError) as exc:
                    send_to_dlq(
                        producer=producer,
                        dlq_topic=config.dlq_topic,
                        source_topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        raw_bytes=raw_bytes,
                        reason="invalid_json",
                        error_detail=str(exc),
                    )
                    continue
                decoded.append((msg, raw_bytes, payload, t0))

            # One query per batch restores the totals of every KOL that is not in RAM yet.
            if store is not None:
                unknown = sorted({
                    payload["kol_id"] for _, _, payload, _ in decoded
                    if isinstance(payload, dict) and isinstance(payload.get("kol_id"), str)
                    and payload["kol_id"] not in state.kols
                })
                for kol_id, saved in store.load(unknown).items():
                    if state.seed_kol(kol_id, saved):
                        KOLS_RESTORED.inc()

            for msg, raw_bytes, payload, t0 in decoded:
                try:
                    is_new = state.process_event(payload, now_ts=now_wall)
                except EventValidationError as exc:
                    send_to_dlq(
                        producer=producer,
                        dlq_topic=config.dlq_topic,
                        source_topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        raw_bytes=raw_bytes,
                        reason="schema_validation",
                        error_detail=str(exc),
                    )
                    continue

                PROCESSING_LATENCY.observe(time.perf_counter() - t0)
                if is_new:
                    EVENTS_CONSUMED.labels(
                        platform=str(payload["platform"]),
                        event_type=str(payload["event_type"]),
                    ).inc()
                    processed_since_log += 1
                else:
                    EVENTS_DEDUPLICATED.inc()

            now_mono = time.monotonic()
            if now_mono - last_flush >= config.flush_interval_sec:
                kol_count, kw_count = flush_and_commit(
                    state=state,
                    producer=producer,
                    consumer=consumer,
                    config=config,
                    has_uncommitted_offsets=has_uncommitted,
                    sync_commit=False,
                )
                has_uncommitted = False
                last_flush = now_mono

                if now_mono - last_log >= 5.0:
                    rate = processed_since_log / (now_mono - last_log)
                    print(
                        f"[processor] rate={rate:7.1f} ev/s | active_kols={len(state.kols)} "
                        f"active_keywords={len(state.keywords)} | flushed kols={kol_count} "
                        f"keywords={kw_count}"
                    )
                    processed_since_log = 0
                    last_log = now_mono
    finally:
        print("[processor] shutting down, flushing final state and committing offsets...")
        try:
            flush_and_commit(
                state=state,
                producer=producer,
                consumer=consumer,
                config=config,
                has_uncommitted_offsets=has_uncommitted,
                sync_commit=True,
            )
        finally:
            producer.flush(15.0)
            consumer.close()
            if store is not None:
                store.close()
        print("[processor] stopped cleanly.")


def main(argv: list[str] | None = None) -> None:
    config = parse_config(argv)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    if config.metrics_port > 0:
        start_http_server(config.metrics_port)
        print(f"[processor] Prometheus metrics listening on :{config.metrics_port}/metrics")

    run_processor(config)


if __name__ == "__main__":
    main()
