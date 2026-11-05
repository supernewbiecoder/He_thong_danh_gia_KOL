"""Checks that this machine can use the team-shared Kafka and PostgreSQL.

    pip install confluent-kafka "psycopg[binary]"
    python scripts/check_shared.py --kafka HOST:9092,HOST:9093,HOST:9094 --postgres postgresql://...

Kafka: lists the topics, then writes and reads back one message on kol.dev.sandbox.
PostgreSQL: logs in and reports whether this account can create tables in schema kol.
"""

from __future__ import annotations

import argparse
import sys
import uuid

SANDBOX_TOPIC = "kol.dev.sandbox"
TIMEOUT_SECONDS = 30


def check_kafka(bootstrap: str) -> None:
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient

    metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=TIMEOUT_SECONDS)
    print(f"  brokers: {sorted(b.host + ':' + str(b.port) for b in metadata.brokers.values())}")
    print(f"  topics:  {sorted(t for t in metadata.topics if t.startswith('kol.'))}")

    marker = str(uuid.uuid4())
    delivered = []
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    producer.produce(SANDBOX_TOPIC, value=marker.encode(), key=b"check",
                     on_delivery=lambda err, msg: delivered.append((err, msg)))
    producer.flush(TIMEOUT_SECONDS)
    if not delivered or delivered[0][0] is not None:
        raise RuntimeError(f"write failed: {delivered[0][0] if delivered else 'timeout'}")
    partition, offset = delivered[0][1].partition(), delivered[0][1].offset()
    print(f"  wrote to {SANDBOX_TOPIC}[{partition}] at offset {offset}")

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"check-{marker}", "enable.auto.commit": False})
    try:
        consumer.assign([TopicPartition(SANDBOX_TOPIC, partition, offset)])
        msg = consumer.poll(TIMEOUT_SECONDS)
        if msg is None:
            raise RuntimeError("read timed out")
        if msg.error():
            raise KafkaException(msg.error())
        if msg.value().decode() != marker:
            raise RuntimeError("read back a different message")
        print("  read the same message back")
    finally:
        consumer.close()


def check_postgres(url: str) -> None:
    import psycopg

    with psycopg.connect(url, connect_timeout=TIMEOUT_SECONDS) as conn:
        row = conn.execute("SELECT current_user, version()").fetchone()
        print(f"  logged in as {row[0]}: {row[1].split(',')[0]}")
        can_create = conn.execute("SELECT has_schema_privilege('kol', 'CREATE')").fetchone()[0]
        print(f"  can create tables in schema kol: {'yes' if can_create else 'no (read-only)'}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kafka", help="Kafka bootstrap servers")
    parser.add_argument("--postgres", help="PostgreSQL connection URL")
    args = parser.parse_args()
    if not args.kafka and not args.postgres:
        parser.error("give --kafka, --postgres or both")

    checks = []
    if args.kafka:
        checks.append(("Kafka", check_kafka, args.kafka))
    if args.postgres:
        checks.append(("PostgreSQL", check_postgres, args.postgres))

    failed = False
    for name, check, target in checks:
        print(f"{name}:")
        try:
            check(target)
            print("  OK")
        except Exception as exc:  # report every failure and keep going with the next check
            failed = True
            print(f"  FAILED: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
