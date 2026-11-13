"""Standalone local simulation for Module 3 (no Kafka/Docker required).

Generates realistic multi-platform social events in-memory, feeds them into Module 3's
StreamState, and prints the resulting Top KOL Leaderboard and Trending Keywords/Hashtags.

Run:
    python3 processor/demo_local.py
"""

from __future__ import annotations

import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from processor.state import StreamState

SAMPLE_KOLS = [
    ("youtube:UC_khoai_lang_thang", "youtube", "@KhoaiLangThang", 2_450_000),
    ("youtube:UC_schannel_vn", "youtube", "@Schannel", 3_100_000),
    ("bluesky:did:plc:tech_reviewer", "bluesky", "@techreview.bsky.social", 85_000),
    ("bluesky:did:plc:beauty_guru", "bluesky", "@hanabeauty.bsky.social", 142_000),
    ("bluesky:did:plc:vn_tech_insider", "bluesky", "@vntech.bsky.social", 45_000),
    ("youtube:UC_gaming_pro", "youtube", "@MixiGaming", 7_500_000),
]

SAMPLE_TEXTS = [
    ("Review chi tiết iPhone 17 Pro sau 2 tuần sử dụng, pin cực trâu #tech #iphone17 #review", ["tech", "iphone17", "review"]),
    ("Top 5 kem chống nắng tốt nhất mùa hè cho da dầu mụn #skincare #beauty #lamdep", ["skincare", "beauty", "lamdep"]),
    ("Vlog khám phá ẩm thực đường phố Sài Gòn đêm mưa tuyệt vời #food #travel #saigon", ["food", "travel", "saigon"]),
    ("Highlight buổi livestream tối qua cùng anh em, cảm ơn mọi người #gaming #livestream", ["gaming", "livestream"]),
    ("Đánh giá laptop AI thế hệ mới: có đáng nâng cấp năm nay? #tech #laptop #ai", ["tech", "laptop", "ai"]),
    ("Chia sẻ kinh nghiệm chăm sóc da nhạy cảm và phục hồi da #skincare #routine", ["skincare", "routine"]),
]


def generate_synthetic_event(rng: random.Random) -> dict:
    kol_id, platform, handle, followers = rng.choice(SAMPLE_KOLS)
    text, tags = rng.choice(SAMPLE_TEXTS)
    event_type = rng.choices(["post", "comment", "reply"], weights=[2, 5, 3])[0]
    now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    scale = 10 if event_type == "post" else 1
    return {
        "event_id": str(uuid.uuid4()),
        "platform": platform,
        "event_type": event_type,
        "content_id": f"{platform}-{rng.getrandbits(32):x}",
        "parent_id": None if event_type == "post" else f"{platform}-post-1",
        "kol_id": kol_id,
        "author": {
            "id": kol_id if event_type == "post" else f"{platform}:viewer-{rng.randint(1, 999)}",
            "handle": handle if event_type == "post" else f"@viewer_{rng.randint(1, 999)}",
            "followers": followers if event_type == "post" else rng.randint(10, 500),
        },
        "text": text,
        "hashtags": tags,
        "mentions": [],
        "metrics": {
            "likes": rng.randint(5, 300) * scale,
            "shares": rng.randint(0, 40) * scale,
            "comments": rng.randint(1, 80) * scale,
            "views": rng.randint(100, 5000) * scale,
        },
        "lang": "vi",
        "created_at": now_iso,
        "ingested_at": now_iso,
        "source": {"collector": f"{platform}-worker", "version": "1.0"},
    }


def main() -> None:
    rng = random.Random(42)
    state = StreamState(window_minutes=15)
    now_ts = time.time()

    total_events = 250
    for i in range(total_events):
        ev = generate_synthetic_event(rng)
        state.process_event(ev, now_ts=now_ts + i * 0.1)

    kol_outputs, kw_outputs = state.collect_dirty_outputs(
        now_ts=now_ts + total_events * 0.1,
        top_keywords_limit=10,
    )

    kol_sorted = sorted(kol_outputs, key=lambda x: x[1]["engagement_score"], reverse=True)

    print("=" * 88)
    print(f"DEMO MODULE 3 (IN-MEMORY STREAM PROCESSOR) - Đã xử lý {total_events} sự kiện mạng xã hội")
    print("=" * 88)
    print("\n1. BẢNG XẾP HẠNG ĐIỂM TƯƠNG TÁC KOL (Topic: kol.metrics.kol-scores):")
    print(f"{'Hạng':<5} {'KOL ID':<32} {'Handle':<24} {'Events':<7} {'Score':>10} {'Velocity/phút':>14}")
    print("-" * 88)
    for idx, (kol_id, payload) in enumerate(kol_sorted, start=1):
        print(
            f"#{idx:<4} {kol_id:<32} {str(payload['handle']):<24} "
            f"{payload['window_events']:<7} {payload['engagement_score']:>10.1f} "
            f"{payload['velocity_per_min']:>14.2f}"
        )

    print("\n2. TOP TỪ KHÓA & HASHTAG THỊNH HÀNH (Topic: kol.metrics.keywords):")
    print(f"{'Hạng':<5} {'Từ khóa':<18} {'Loại':<10} {'Lượt (15p)':<12} {'Số KOL':<8} {'Phân bổ nền tảng'}")
    print("-" * 88)
    for idx, (kw, payload) in enumerate(kw_outputs, start=1):
        kind = "#hashtag" if payload["is_hashtag"] else "keyword"
        plats = payload["platforms"]
        plat_str = f"YT={plats.get('youtube', 0)}, BS={plats.get('bluesky', 0)}"
        print(
            f"#{idx:<4} {kw:<18} {kind:<10} {payload['count']:<12} "
            f"{payload['unique_kols']:<8} {plat_str}"
        )

    print("\n3. MẪU JSON BẢN GHI XUẤT RA KAFKA (kol.metrics.kol-scores):")
    print(json.dumps(kol_sorted[0][1], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
