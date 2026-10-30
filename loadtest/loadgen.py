"""Load generator for the KOL platform Kafka cluster.

Produces synthetic social-media events (same shape as config/schemas/social-event.schema.json)
at a controlled rate, or consumes them while measuring end-to-end latency. Metrics are exposed
in Prometheus format on :8000 so the Grafana dashboard shows the test live.

Examples:
    python loadgen.py produce --bootstrap localhost:9092 --rate 5000 --duration 120
    python loadgen.py consume --bootstrap localhost:9092 --group loadgen-consumer
    python loadgen.py produce --rate 0 --duration 60      # unthrottled: find max throughput

Every option can also be set with an environment variable (see --help), which is how
Docker Compose and Kubernetes configure it.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer, TIMESTAMP_NOT_AVAILABLE
from prometheus_client import Counter, Gauge, Histogram, start_http_server

LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)

SENT = Counter("loadgen_messages_sent", "Messages acknowledged by Kafka", ["topic"])
SEND_ERRORS = Counter("loadgen_send_errors", "Messages Kafka failed to acknowledge", ["topic"])
ACK_LATENCY = Histogram("loadgen_ack_latency_seconds", "produce() -> broker ack", buckets=LATENCY_BUCKETS)
CONSUMED = Counter("loadgen_messages_consumed", "Messages consumed", ["topic"])
E2E_LATENCY = Histogram("loadgen_e2e_latency_seconds", "Record CreateTime -> consumed", buckets=LATENCY_BUCKETS)
TARGET_RATE = Gauge("loadgen_target_rate", "Configured target rate (msg/s, 0 = unlimited)")

PLATFORMS = ("bluesky", "youtube", "reddit")
EVENT_TYPES = ("post", "comment", "reply")
HASHTAGS = ("#review", "#unboxing", "#skincare", "#gaming", "#food", "#travel", "#tech",
            "#fashion", "#music", "#vlog", "#livestream", "#giveaway", "#ootd", "#fitness")
WORDS = ("san pham", "rat tot", "dang mua", "khong nen", "chat luong", "gia re", "review",
         "trai nghiem", "tuyet voi", "that vong", "highly recommend", "great video",
         "love this", "not worth it", "best", "worst", "amazing", "subscribe", "link", "deal")

stop_event = threading.Event()


def env(name: str, default):
    return os.getenv(name, default)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", nargs="?", choices=("produce", "consume"), default=env("LOADGEN_MODE", "produce"))
    p.add_argument("--bootstrap", default=env("BOOTSTRAP_SERVERS", "localhost:9092"),
                   help="Kafka bootstrap servers [BOOTSTRAP_SERVERS]")
    p.add_argument("--topic", default=env("LOADGEN_TOPIC", "kol.loadtest"), help="[LOADGEN_TOPIC]")
    p.add_argument("--rate", type=int, default=int(env("LOADGEN_RATE", 1000)),
                   help="Target messages/s per process, 0 = unlimited [LOADGEN_RATE]")
    p.add_argument("--duration", type=int, default=int(env("LOADGEN_DURATION", 60)),
                   help="Seconds to run, 0 = until stopped [LOADGEN_DURATION]")
    p.add_argument("--padding-bytes", type=int, default=int(env("LOADGEN_PADDING_BYTES", 0)),
                   help="Extra bytes added to each message to simulate bigger payloads [LOADGEN_PADDING_BYTES]")
    p.add_argument("--kols", type=int, default=int(env("LOADGEN_KOLS", 500)),
                   help="Number of distinct KOLs (message keys) [LOADGEN_KOLS]")
    p.add_argument("--acks", default=env("LOADGEN_ACKS", "all"), choices=("0", "1", "all"), help="[LOADGEN_ACKS]")
    p.add_argument("--group", default=env("LOADGEN_GROUP", "loadgen-consumer"), help="Consumer group [LOADGEN_GROUP]")
    p.add_argument("--offset-reset", default=env("LOADGEN_OFFSET_RESET", "latest"), choices=("earliest", "latest"),
                   help="Where a new consumer group starts [LOADGEN_OFFSET_RESET]")
    p.add_argument("--process-delay-ms", type=float, default=float(env("LOADGEN_PROCESS_DELAY_MS", 0)),
                   help="Simulated CPU work per message (busy loop), to create lag / CPU load [LOADGEN_PROCESS_DELAY_MS]")
    p.add_argument("--metrics-port", type=int, default=int(env("LOADGEN_METRICS_PORT", 8000)),
                   help="Prometheus metrics port, 0 = disabled [LOADGEN_METRICS_PORT]")
    p.add_argument("--report", default=env("LOADGEN_REPORT", ""),
                   help="Write a JSON summary to this path when finished [LOADGEN_REPORT]")
    return p.parse_args()


class LatencySample:
    """Bounded reservoir sample so percentiles stay cheap during long runs."""

    def __init__(self, size: int = 100_000):
        self.size = size
        self.values: list[float] = []
        self.seen = 0

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self.values) < self.size:
            self.values.append(value)
        else:
            i = random.randrange(self.seen)
            if i < self.size:
                self.values[i] = value

    def percentiles(self) -> dict:
        if not self.values:
            return {}
        data = sorted(self.values)
        pick = lambda q: round(data[min(len(data) - 1, int(q * len(data)))] * 1000, 2)
        return {"p50_ms": pick(0.50), "p95_ms": pick(0.95), "p99_ms": pick(0.99), "max_ms": round(data[-1] * 1000, 2)}


def make_event(rng: random.Random, kol_count: int, padding: str) -> dict:
    platform = rng.choice(PLATFORMS)
    kol = rng.randrange(kol_count)
    event_type = rng.choice(EVENT_TYPES)
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    tags = rng.sample(HASHTAGS, k=rng.randint(0, 3))
    text = " ".join(rng.choices(WORDS, k=rng.randint(5, 20))) + " " + " ".join(tags)
    event = {
        "event_id": str(uuid.uuid4()),
        "platform": platform,
        "event_type": event_type,
        "content_id": f"{platform}-{rng.getrandbits(48):x}",
        "parent_id": None if event_type == "post" else f"{platform}-{rng.getrandbits(48):x}",
        "kol_id": f"{platform}:kol-{kol}",
        "author": {
            "id": f"{platform}:user-{rng.randrange(1_000_000)}",
            "handle": f"user{rng.randrange(1_000_000)}",
            "followers": rng.randint(0, 2_000_000),
        },
        "text": text,
        "hashtags": [t[1:] for t in tags],
        "mentions": [],
        "metrics": {
            "likes": rng.randint(0, 50_000),
            "shares": rng.randint(0, 5_000),
            "comments": rng.randint(0, 3_000),
            "views": rng.randint(0, 2_000_000),
        },
        "lang": rng.choice(("vi", "en")),
        "created_at": now,
        "ingested_at": now,
        "source": {"collector": "loadgen", "version": "1.0"},
    }
    if padding:
        event["padding"] = padding
    return event


def run_producer(args: argparse.Namespace) -> dict:
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": f"loadgen-producer-{socket.gethostname()}",
        "acks": args.acks,
        "enable.idempotence": args.acks == "all",
        "linger.ms": 5,
        "batch.size": 131072,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
    })
    TARGET_RATE.set(args.rate)
    rng = random.Random()
    padding = "x" * args.padding_bytes
    stats = {"produced": 0, "acked": 0, "errors": 0}
    latencies = LatencySample()
    sent_counter = SENT.labels(args.topic)
    error_counter = SEND_ERRORS.labels(args.topic)

    def on_delivery(err, _msg, started: float) -> None:
        if err is not None:
            stats["errors"] += 1
            error_counter.inc()
            if stats["errors"] <= 10:
                print(f"[produce] delivery failed: {err}", file=sys.stderr)
            return
        latency = time.monotonic() - started
        stats["acked"] += 1
        sent_counter.inc()
        ACK_LATENCY.observe(latency)
        latencies.add(latency)

    print(f"[produce] topic={args.topic} rate={args.rate or 'unlimited'} msg/s "
          f"duration={args.duration or 'infinite'}s acks={args.acks} bootstrap={args.bootstrap}")
    start = time.monotonic()
    last_report, last_acked = start, 0

    while not stop_event.is_set():
        now = time.monotonic()
        elapsed = now - start
        if args.duration and elapsed >= args.duration:
            break
        if now - last_report >= 5:
            rate = (stats["acked"] - last_acked) / (now - last_report)
            print(f"[produce] t={elapsed:6.0f}s acked={stats['acked']:>10} rate={rate:9.0f} msg/s "
                  f"errors={stats['errors']} queue={len(producer)}")
            last_report, last_acked = now, stats["acked"]
        if args.rate:
            wait = (stats["produced"] + 1) / args.rate - elapsed
            if wait > 0:
                producer.poll(min(wait, 0.05))
                continue

        event = make_event(rng, args.kols, padding)
        try:
            producer.produce(
                args.topic,
                key=event["kol_id"].encode(),
                value=json.dumps(event, separators=(",", ":")).encode(),
                on_delivery=lambda err, msg, t=time.monotonic(): on_delivery(err, msg, t),
            )
        except BufferError:
            producer.poll(0.1)  # local queue full: broker cannot keep up, back off
            continue
        stats["produced"] += 1
        producer.poll(0)

    print("[produce] flushing...")
    remaining = producer.flush(30)
    elapsed = time.monotonic() - start
    return {
        "mode": "produce", "topic": args.topic, "target_rate": args.rate, "acks": args.acks,
        "duration_s": round(elapsed, 1), "produced": stats["produced"], "acked": stats["acked"],
        "errors": stats["errors"], "unflushed": remaining,
        "throughput_msg_s": round(stats["acked"] / elapsed, 1) if elapsed else 0,
        "ack_latency": latencies.percentiles(),
    }


def run_consumer(args: argparse.Namespace) -> dict:
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "client.id": f"loadgen-consumer-{socket.gethostname()}",
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
    })
    consumer.subscribe([args.topic], on_assign=lambda _c, parts: print(
        f"[consume] assigned partitions: {sorted(p.partition for p in parts)}"))
    print(f"[consume] topic={args.topic} group={args.group} bootstrap={args.bootstrap} "
          f"process_delay={args.process_delay_ms}ms")

    consumed_counter = CONSUMED.labels(args.topic)
    latencies = LatencySample()
    consumed = 0
    start = time.monotonic()
    last_report, last_consumed = start, 0
    delay = args.process_delay_ms / 1000

    try:
        while not stop_event.is_set():
            now = time.monotonic()
            if args.duration and now - start >= args.duration:
                break
            if now - last_report >= 5:
                rate = (consumed - last_consumed) / (now - last_report)
                print(f"[consume] t={now - start:6.0f}s consumed={consumed:>10} rate={rate:9.0f} msg/s")
                last_report, last_consumed = now, consumed

            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[consume] error: {msg.error()}", file=sys.stderr)
                    continue
                ts_type, ts_ms = msg.timestamp()
                if ts_type != TIMESTAMP_NOT_AVAILABLE:
                    latency = max(0.0, time.time() - ts_ms / 1000)
                    E2E_LATENCY.observe(latency)
                    latencies.add(latency)
                try:
                    json.loads(msg.value())  # parse like a real consumer would
                except ValueError:
                    pass  # e.g. random payloads written by scripts/perf-test.sh
                consumed += 1
                consumed_counter.inc()
                if delay:
                    # Busy-wait instead of sleep so the work shows up as CPU (drives the HPA)
                    end = time.perf_counter() + delay
                    while time.perf_counter() < end:
                        pass
    finally:
        consumer.close()

    elapsed = time.monotonic() - start
    return {
        "mode": "consume", "topic": args.topic, "group": args.group, "duration_s": round(elapsed, 1),
        "consumed": consumed, "throughput_msg_s": round(consumed / elapsed, 1) if elapsed else 0,
        "e2e_latency": latencies.percentiles(),
    }


def main() -> None:
    args = parse_args()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())
    if args.metrics_port:
        start_http_server(args.metrics_port)
        print(f"[loadgen] metrics on :{args.metrics_port}/metrics")

    summary = run_producer(args) if args.mode == "produce" else run_consumer(args)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print("[loadgen] summary:\n" + json.dumps(summary, indent=2))

    if args.report:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
            with open(args.report, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
            print(f"[loadgen] report written to {args.report}")
        except OSError as exc:
            print(f"[loadgen] could not write report {args.report}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
