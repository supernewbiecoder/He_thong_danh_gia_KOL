"""CLI utility to inspect live KOL scores and trending keywords from Kafka topics.

Usage:
    python processor/view_leaderboard.py
    python processor/view_leaderboard.py --bootstrap 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 --wait 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read latest KOL leaderboard and trending keywords from Kafka.")
    p.add_argument(
        "--bootstrap",
        default=os.getenv("BOOTSTRAP_SERVERS", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"),
        help="Kafka bootstrap servers [BOOTSTRAP_SERVERS]",
    )
    p.add_argument("--scores-topic", default="kol.metrics.kol-scores")
    p.add_argument("--keywords-topic", default="kol.metrics.keywords")
    p.add_argument("--top", type=int, default=10, help="Number of top rows to display")
    p.add_argument("--wait", type=float, default=4.0, help="Seconds to scan Kafka topics from beginning")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"leaderboard-viewer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.scores_topic, args.keywords_topic])

    kols: dict[str, dict] = {}
    keywords: dict[str, dict] = {}

    deadline = time.monotonic() + args.wait
    try:
        while time.monotonic() < deadline:
            msgs = consumer.consume(num_messages=500, timeout=0.5)
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Kafka error: {msg.error()}", file=sys.stderr)
                    continue
                try:
                    data = json.loads(msg.value())
                except Exception:
                    continue
                if msg.topic() == args.scores_topic and "kol_id" in data:
                    kols[data["kol_id"]] = data
                elif msg.topic() == args.keywords_topic and "keyword" in data:
                    keywords[data["keyword"]] = data
    finally:
        consumer.close()

    top_kols = sorted(kols.values(), key=lambda x: x.get("engagement_score", 0), reverse=True)[: args.top]
    top_kws = sorted(keywords.values(), key=lambda x: (x.get("count", 0), x.get("total_count", 0)), reverse=True)[
        : args.top
    ]

    print("=" * 90)
    print(f"KẾT QUẢ THỰC TẾ TRÊN KAFKA ({args.bootstrap})")
    print(f"Tổng số KOL đã tính điểm: {len(kols)} | Tổng số từ khóa/hashtag: {len(keywords)}")
    print("=" * 90)

    print(f"\nTOP {args.top} KOL THEO ĐIỂM TƯƠNG TÁC ({args.scores_topic}):")
    if not top_kols:
        print("  (Chưa có dữ liệu - hãy chạy stream-processor và bắn dữ liệu vào kol.raw.*)")
    else:
        print(f"{'Hạng':<5} {'KOL ID':<28} {'Nền tảng':<10} {'Events':<8} {'Điểm (Score)':>14} {'Velocity/phút':>15}")
        print("-" * 90)
        for idx, item in enumerate(top_kols, start=1):
            print(
                f"#{idx:<4} {str(item.get('kol_id')):<28} {str(item.get('platform')):<10} "
                f"{item.get('total_events', 0):<8} {item.get('engagement_score', 0.0):>14.2f} "
                f"{item.get('velocity_per_min', 0.0):>15.2f}"
            )

    print(f"\nTOP {args.top} TỪ KHÓA & HASHTAG THỊNH HÀNH ({args.keywords_topic}):")
    if not top_kws:
        print("  (Chưa có dữ liệu)")
    else:
        print(f"{'Hạng':<5} {'Từ khóa':<20} {'Loại':<10} {'Lượt (15p)':<12} {'Số KOL':<8} {'Phân bổ nền tảng'}")
        print("-" * 90)
        for idx, item in enumerate(top_kws, start=1):
            kind = "#hashtag" if item.get("is_hashtag") else "keyword"
            plats = item.get("platforms") or {}
            plat_str = f"BS={plats.get('bluesky', 0)}, YT={plats.get('youtube', 0)}"
            print(
                f"#{idx:<4} {str(item.get('keyword')):<20} {kind:<10} "
                f"{item.get('count', 0):<12} {item.get('unique_kols', 0):<8} {plat_str}"
            )


if __name__ == "__main__":
    main()
