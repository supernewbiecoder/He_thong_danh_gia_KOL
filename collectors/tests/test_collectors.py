"""Unit tests for Module 2: Bluesky attribution and the YouTube quota-saving watchlist.

    python -m unittest discover -s collectors/tests -v
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

from collectors.main import BlueskyCollector, YouTubeCollector

KOL = "did:plc:kolauthor"
FAN = "did:plc:fan"
POST_URI = f"at://{KOL}/app.bsky.feed.post/3kpost"


def commit(collection: str, record: dict, did: str = FAN, operation: str = "create") -> dict:
    return {
        "kind": "commit",
        "did": did,
        "time_us": 1_731_400_000_000_000,
        "commit": {"collection": collection, "operation": operation, "rkey": "3krkey",
                   "cid": "bafy", "rev": "3krev", "record": record},
    }


class TestBlueskyAttribution(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["BLUESKY_ENRICH_INTERACTIONS"] = "false"
        self.collector = BlueskyCollector(publisher=mock.Mock())

    def normalize(self, payload: dict) -> dict:
        event = self.collector.normalize_firehose_event(payload)
        self.assertIsNotNone(event)
        return event

    def test_post_belongs_to_its_author(self) -> None:
        event = self.normalize(commit("app.bsky.feed.post", {"text": "hello"}, did=KOL))
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")
        self.assertEqual(event["author"]["id"], KOL)

    def test_like_is_scored_for_the_liked_post_author(self) -> None:
        event = self.normalize(commit("app.bsky.feed.like", {"subject": {"uri": POST_URI}}))
        self.assertEqual(event["event_type"], "like")
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")
        self.assertEqual(event["author"]["id"], FAN)

    def test_repost_is_scored_for_the_original_author(self) -> None:
        event = self.normalize(commit("app.bsky.feed.repost", {"subject": {"uri": POST_URI}}))
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")

    def test_reply_is_scored_for_the_parent_author(self) -> None:
        record = {"text": "great post", "reply": {"parent": {"uri": POST_URI}, "root": {"uri": POST_URI}}}
        event = self.normalize(commit("app.bsky.feed.post", record))
        self.assertEqual(event["event_type"], "reply")
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")
        self.assertEqual(event["author"]["id"], FAN)

    def test_follow_is_scored_for_the_followed_account(self) -> None:
        event = self.normalize(commit("app.bsky.graph.follow", {"subject": KOL}))
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")

    def test_delete_stays_with_the_actor(self) -> None:
        event = self.normalize(commit("app.bsky.feed.post", {}, did=KOL, operation="delete"))
        self.assertEqual(event["kol_id"], f"bluesky:{KOL}")

    def test_like_without_a_valid_subject_falls_back_to_the_actor(self) -> None:
        event = self.normalize(commit("app.bsky.feed.like", {"subject": {"uri": "not-an-at-uri"}}))
        self.assertEqual(event["kol_id"], f"bluesky:{FAN}")


class TestYouTubeWatchlist(unittest.TestCase):
    def setUp(self) -> None:
        self.env = mock.patch.dict(os.environ, {
            "YOUTUBE_API_KEY": "key",
            "YOUTUBE_CHANNEL_IDS": "UCpinned",
            "YOUTUBE_SEARCH_QUERIES": "music",
            "YOUTUBE_INCLUDE_COMMENTS": "false",
            "YOUTUBE_MAX_CHANNELS": "2",
        })
        self.env.start()
        self.collector = YouTubeCollector(publisher=mock.Mock())
        self.calls: list[str] = []

        def fake_request(_method: str, url: str, **kwargs) -> dict:
            endpoint = url.rsplit("/", 1)[-1]
            params = kwargs.get("params", {})
            self.calls.append(endpoint)
            if endpoint == "search":
                return {"items": [
                    {"id": {"videoId": "vidA"}, "snippet": {"channelId": "UCfound1"}},
                    {"id": {"videoId": "vidB"}, "snippet": {"channelId": "UCfound2"}},
                ]}
            if endpoint == "channels" and params.get("part") == "contentDetails":
                return {"items": [{"id": cid, "contentDetails": {"relatedPlaylists": {"uploads": f"UU{cid}"}}}
                                  for cid in params["id"].split(",")]}
            if endpoint == "playlistItems":
                return {"items": [{"contentDetails": {"videoId": f"new-{params['playlistId']}"}}]}
            if endpoint == "videos":
                return {"items": [{"id": vid, "snippet": {"channelId": "UCpinned", "title": vid}, "statistics": {}}
                                  for vid in params["id"].split(",")]}
            return {"items": []}

        self.collector.request = fake_request  # type: ignore[method-assign]

    def tearDown(self) -> None:
        self.env.stop()

    def test_search_runs_once_and_watched_channels_are_polled_cheaply(self) -> None:
        first = list(self.collector.collect())
        self.assertEqual(self.calls.count("search"), 1)
        # Pinned channel survives the watchlist limit of 2; one discovered channel is dropped.
        self.assertIn("UCpinned", self.collector.watch)
        self.assertEqual(len(self.collector.watch), 2)
        self.assertTrue(any(e["content_id"] == "vidA" for e in first))

        self.calls.clear()
        second = list(self.collector.collect())
        self.assertNotIn("search", self.calls)          # not due again for 24 hours
        self.assertEqual(self.calls.count("playlistItems"), 2)
        self.assertLess(self.collector.quota_used, 10)  # vs. 100+ units for a single search
        self.assertTrue(any(e["content_id"].startswith("new-UU") for e in second))


if __name__ == "__main__":
    unittest.main()
