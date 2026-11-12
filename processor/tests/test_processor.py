"""Unit tests for Module 3 (Stream Processor): validation, deduplication, KOL scoring, and keywords."""

from __future__ import annotations

import unittest

from processor.config import ScoringWeights, parse_config
from processor.state import EventValidationError, StreamState, validate_event
from processor.tokenizer import extract_keywords_and_hashtags


def sample_event(
    event_id: str = "evt-1",
    platform: str = "youtube",
    event_type: str = "comment",
    kol_id: str = "youtube:kol-101",
    likes: int = 10,
    comments: int = 2,
    shares: int = 1,
    views: int = 100,
    hashtags: list[str] | None = None,
    text: str = "Review sản phẩm kem dưỡng rất tốt #Skincare #Beauty2026",
) -> dict:
    return {
        "event_id": event_id,
        "platform": platform,
        "event_type": event_type,
        "content_id": "content-123",
        "parent_id": "parent-123",
        "kol_id": kol_id,
        "author": {
            "id": "youtube:user-1",
            "handle": "@kol_reviewer",
            "followers": 50000,
        },
        "text": text,
        "hashtags": hashtags if hashtags is not None else ["skincare"],
        "mentions": ["@brand"],
        "metrics": {
            "likes": likes,
            "shares": shares,
            "comments": comments,
            "views": views,
        },
        "lang": "vi",
        "created_at": "2026-11-12T10:00:00.000Z",
        "ingested_at": "2026-11-12T10:00:01.000Z",
        "source": {"collector": "unit-test", "version": "1.0"},
    }


class TestStreamProcessor(unittest.TestCase):
    def test_parse_config_defaults_and_custom_args(self) -> None:
        cfg = parse_config(["--bootstrap", "kafka:9092", "--dedup-size", "5000"])
        self.assertEqual(cfg.bootstrap_servers, "kafka:9092")
        self.assertEqual(cfg.dedup_cache_size, 5000)
        self.assertEqual(cfg.input_topics, ["kol.raw.bluesky", "kol.raw.youtube"])

    def test_validate_event_success_and_failures(self) -> None:
        ev = sample_event()
        self.assertEqual(validate_event(ev)["kol_id"], "youtube:kol-101")

        with self.assertRaises(EventValidationError):
            validate_event("not-a-dict")

        bad_missing = sample_event()
        del bad_missing["kol_id"]
        with self.assertRaises(EventValidationError):
            validate_event(bad_missing)

        bad_platform = sample_event(platform="tiktok")
        with self.assertRaises(EventValidationError):
            validate_event(bad_platform)

    def test_tokenizer_extracts_hashtags_and_filters_stopwords(self) -> None:
        ev = sample_event(
            hashtags=["#Review", "Skincare"],
            text="Sản phẩm này của mình rất tốt cho làn da #Beauty2026 https://example.com @user1",
        )
        pairs = dict(extract_keywords_and_hashtags(ev, extract_from_text=True))
        # Explicit and inline hashtags must be normalized and marked True
        self.assertTrue(pairs.get("review"))
        self.assertTrue(pairs.get("skincare"))
        self.assertTrue(pairs.get("beauty2026"))
        # Stopwords ("của", "mình", "rất", "cho", "này") and URLs/mentions must be excluded
        self.assertNotIn("của", pairs)
        self.assertNotIn("mình", pairs)
        self.assertNotIn("http", pairs)
        self.assertNotIn("user1", pairs)
        # Meaningful words are kept with is_hashtag=False
        self.assertIn("sản", pairs)
        self.assertFalse(pairs["sản"])

    def test_deduplication_skips_duplicate_event_ids(self) -> None:
        state = StreamState(window_minutes=15, dedup_cache_size=100)
        ev = sample_event(event_id="dup-1", likes=10, comments=0, shares=0, views=0)

        self.assertTrue(state.process_event(ev, now_ts=1000.0))
        self.assertFalse(state.process_event(ev, now_ts=1001.0))
        self.assertEqual(state.kols["youtube:kol-101"].total_events, 1)
        self.assertEqual(state.kols["youtube:kol-101"].total_likes, 10)

    def test_kol_scoring_and_sliding_window_eviction(self) -> None:
        weights = ScoringWeights(
            comment_event=2.0,
            like_metric=1.0,
            comment_metric=3.0,
            share_metric=5.0,
            view_metric=0.01,
        )
        state = StreamState(window_minutes=10, weights=weights)

        # Event 1 at t = 100s: base(2) + 10*1 + 2*3 + 1*5 + 100*0.01 = 2 + 10 + 6 + 5 + 1 = 24.0
        ev1 = sample_event(event_id="e1", likes=10, comments=2, shares=1, views=100)
        state.process_event(ev1, now_ts=100.0)

        # Event 2 at t = 500s: same score = 24.0
        ev2 = sample_event(event_id="e2", likes=10, comments=2, shares=1, views=100)
        state.process_event(ev2, now_ts=500.0)

        kol_outs, kw_outs = state.collect_dirty_outputs(now_ts=500.0)
        self.assertEqual(len(kol_outs), 1)
        _, kol_payload = kol_outs[0]
        self.assertEqual(kol_payload["total_events"], 2)
        self.assertEqual(kol_payload["window_events"], 2)
        self.assertAlmostEqual(kol_payload["engagement_score"], 48.0)
        self.assertAlmostEqual(kol_payload["window_score"], 48.0)
        self.assertTrue(len(kw_outs) > 0)

        # Advance time to t = 750s (window is 600s -> cutoff = 150s, so ev1 at t=100s expires)
        kol_outs_after, _ = state.collect_dirty_outputs(now_ts=750.0)
        self.assertEqual(len(kol_outs_after), 1)
        _, after_payload = kol_outs_after[0]
        # Cumulative score stays 48.0, but window score drops to 24.0
        self.assertEqual(after_payload["total_events"], 2)
        self.assertEqual(after_payload["window_events"], 1)
        self.assertAlmostEqual(after_payload["engagement_score"], 48.0)
        self.assertAlmostEqual(after_payload["window_score"], 24.0)
        self.assertEqual(after_payload["metrics_window"]["likes"], 10)
        self.assertEqual(after_payload["metrics_total"]["likes"], 20)


if __name__ == "__main__":
    unittest.main()
