"""Collect YouTube and Bluesky data and publish normalized events to Kafka.

The collector is intentionally polling-based: it works locally without platform-specific
streaming credentials, while the adapters can later be replaced by firehose consumers.
Run once with ``python main.py --once`` to verify credentials and event normalization.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
import websocket
from confluent_kafka import Producer

LOG = logging.getLogger("kol.collectors")
STOP = False
HTTP_TIMEOUT = float(os.getenv("COLLECTOR_HTTP_TIMEOUT", "20"))
POLL_SECONDS = int(os.getenv("COLLECTOR_POLL_SECONDS", "60"))
VERSION = "1.0.0"


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def split_env(name: str) -> list[str]:
    return [value.strip() for value in os.getenv(name, "").split(",") if value.strip()]


def text_parts(text: str) -> tuple[list[str], list[str]]:
    text = text or ""
    hashtags = sorted({match.lower() for match in re.findall(r"#([\w-]+)", text, re.UNICODE)})
    mentions = sorted({match for match in re.findall(r"(?<!\w)@([\w.-]+)", text)})
    return hashtags, mentions


def iso_time(value: str | None) -> str:
    if not value:
        return now()
    return value.replace("+00:00", "Z") if value.endswith("+00:00") else value


class EventBuilder:
    def __init__(self, platform: str, collector: str) -> None:
        self.platform = platform
        self.collector = collector

    def build(
        self,
        *,
        event_type: str,
        content_id: str,
        kol_id: str,
        author_id: str,
        handle: str = "",
        display_name: str = "",
        text: str = "",
        created_at: str | None = None,
        parent_id: str | None = None,
        followers: int | None = None,
        metrics: dict[str, int] | None = None,
        lang: str | None = None,
        hashtags: list[str] | None = None,
        mentions: list[str] | None = None,
    ) -> dict[str, Any]:
        # Facets carry authoritative tags/mentions (with byte offsets, correct for CJK and
        # emoji); the regex is only a fallback when the platform did not send facets.
        derived_tags, derived_mentions = text_parts(text)
        hashtags = sorted({tag for tag in hashtags if tag}) if hashtags else derived_tags
        mentions = sorted({item for item in mentions if item}) if mentions else derived_mentions
        stable_id = hashlib.sha256(f"{self.platform}:{content_id}:{event_type}".encode()).hexdigest()
        return {
            "event_id": str(uuid.UUID(stable_id[:32])),
            "platform": self.platform,
            "event_type": event_type,
            "content_id": content_id,
            "parent_id": parent_id,
            "kol_id": kol_id,
            "author": {
                "id": author_id,
                "handle": handle,
                "display_name": display_name,
                "followers": followers,
            },
            "text": text,
            "hashtags": hashtags,
            "mentions": mentions,
            "metrics": metrics or {},
            "lang": lang or "",
            "created_at": iso_time(created_at),
            "ingested_at": now(),
            "source": {"collector": self.collector, "version": VERSION},
        }


class KafkaPublisher:
    """Shared Kafka producer with bounded de-duplication and delivery tracking.

    ``delivery_errors`` only grows: a caller that needs a no-loss guarantee (the Bluesky firehose)
    compares it before and after a checkpoint and refuses to persist a cursor when it increased.
    """

    def __init__(self) -> None:
        self.producer = Producer({
            "bootstrap.servers": os.getenv(
                "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
            ),
            "client.id": os.getenv("COLLECTOR_CLIENT_ID", "kol-collectors"),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 10,
            "batch.size": 131072,
        })
        # event_id is deterministic, so the cache only saves a Kafka round-trip for events the
        # same process has already seen. Bounded: a firehose would otherwise grow it forever.
        self.seen: OrderedDict[str, None] = OrderedDict()
        self.seen_limit = max(1000, int(os.getenv("COLLECTOR_DEDUPE_CACHE", "50000")))
        self.published = 0
        self.delivery_errors = 0
        self._lock = threading.Lock()

    def _on_delivery(self, err: Any, _message: Any) -> None:
        if err is not None:
            with self._lock:
                self.delivery_errors += 1
            LOG.error("delivery failed: %s", err)

    def _is_duplicate(self, event_id: str) -> bool:
        with self._lock:
            if event_id in self.seen:
                self.seen.move_to_end(event_id)
                return True
            self.seen[event_id] = None
            if len(self.seen) > self.seen_limit:
                self.seen.popitem(last=False)
            return False

    def _produce(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)

    def publish(self, event: dict[str, Any]) -> None:
        if self._is_duplicate(event["event_id"]):
            return
        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode()
        self._produce(
            f"kol.raw.{event['platform']}", payload, key=event["kol_id"].encode()
        )
        with self._lock:
            self.published += 1

    def publish_to_dlq(self, value: Any, error: Exception, source: str) -> None:
        """Never let one bad record block the stream or stop the process."""
        record = {
            "source": source,
            "error": str(error),
            "ingested_at": now(),
            "value": value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str),
        }
        self._produce("kol.dlq", json.dumps(record, ensure_ascii=False).encode())

    def flush(self, timeout: float = 30) -> bool:
        """True when Kafka confirmed every queued message (flush returns the undelivered count)."""
        return self.producer.flush(timeout) == 0


class Collector:
    name = "collector"

    def __init__(self, publisher: KafkaPublisher) -> None:
        self.publisher = publisher
        self.session = requests.Session()

    def request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        response = self.session.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response.json()

    def collect(self) -> Iterable[dict[str, Any]]:
        raise NotImplementedError

    def run(self) -> int:
        count = 0
        for event in self.collect():
            self.publisher.publish(event)
            count += 1
        LOG.info("%s collected %d event(s)", self.name, count)
        return count


class YouTubeCollector(Collector):
    name = "youtube-worker"
    api = "https://www.googleapis.com/youtube/v3"
    # contentDetails/topicDetails/liveStreamingDetails cost no extra quota: videos.list charges
    # one unit per call no matter how many parts are requested.
    video_parts = "snippet,statistics,contentDetails,topicDetails,liveStreamingDetails"
    # Units charged per call (YouTube Data API quota table). The daily free quota is 10,000 units,
    # so search (100) is the call to avoid: channels are followed through their uploads playlist.
    QUOTA_COST = {"search": 100, "videos": 1, "channels": 1, "playlistItems": 1,
                  "commentThreads": 1, "comments": 1}

    def __init__(self, publisher: KafkaPublisher) -> None:
        super().__init__(publisher)
        # Channels followed every cycle: the configured ones plus those found by keyword search.
        self.watch: OrderedDict[str, None] = OrderedDict()
        self.pinned: set[str] = set()
        self.uploads: dict[str, str] = {}
        self.last_search = 0.0
        self.quota_used = 0

    def api_get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        self.quota_used += self.QUOTA_COST.get(endpoint, 1)
        return self.request("GET", f"{self.api}/{endpoint}", params=params)

    def watch_channel(self, channel_id: str, pinned: bool = False) -> None:
        if not channel_id:
            return
        if pinned:
            self.pinned.add(channel_id)
        self.watch[channel_id] = None
        self.watch.move_to_end(channel_id)
        limit = max(1, int(os.getenv("YOUTUBE_MAX_CHANNELS", "25")))
        for candidate in list(self.watch):
            if len(self.watch) <= limit:
                break
            if candidate not in self.pinned:
                del self.watch[candidate]

    @staticmethod
    def duration_seconds(value: str | None) -> int:
        """ISO-8601 duration (PT1H2M3S) to seconds; 0 when the field is missing or malformed."""
        match = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", value or "")
        if not match:
            return 0
        days, hours, minutes, seconds = (int(part) if part else 0 for part in match.groups())
        return days * 86400 + hours * 3600 + minutes * 60 + seconds

    @staticmethod
    def topic_names(details: dict[str, Any]) -> list[str]:
        """Wikipedia topic URLs / Freebase ids -> readable topic names for keyword analysis."""
        names: list[str] = []
        for url in details.get("topicCategories") or []:
            name = str(url).rsplit("/", 1)[-1].replace("_", " ").strip()
            if name and name not in names:
                names.append(name)
        for freebase in details.get("topicIds") or []:
            if freebase and freebase not in names:
                names.append(str(freebase))
        return names

    def search_pages(self) -> int:
        raw = os.getenv("YOUTUBE_SEARCH_MAX_PAGES") or os.getenv("YOUTUBE_MAX_PAGES", "1")
        return max(1, int(raw))

    def comment_pages(self) -> int:
        raw = os.getenv("YOUTUBE_COMMENT_MAX_PAGES") or os.getenv("YOUTUBE_MAX_PAGES", "1")
        return max(1, int(raw))

    def channel_stats(self, api_key: str, channel_ids: set[str]) -> dict[str, dict[str, Any]]:
        """One channels.list call per 50 channels gives subscriber/video/view totals for scoring."""
        stats: dict[str, dict[str, Any]] = {}
        ids = sorted(cid for cid in channel_ids if cid and cid != "unknown")
        for start in range(0, len(ids), 50):
            chunk = ids[start:start + 50]
            try:
                data = self.api_get("channels", {
                    "key": api_key, "part": "snippet,statistics", "id": ",".join(chunk),
                })
            except requests.RequestException as exc:
                LOG.warning("could not fetch %d YouTube channel(s): %s", len(chunk), exc)
                continue
            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                statistics = item.get("statistics", {})
                stats[item.get("id", "")] = {
                    "handle": snippet.get("customUrl") or snippet.get("title", ""),
                    "display_name": snippet.get("title", ""),
                    "followers": None if statistics.get("hiddenSubscriberCount")
                    else int(statistics.get("subscriberCount", 0)),
                    "videos": int(statistics.get("videoCount", 0)),
                    "total_views": int(statistics.get("viewCount", 0)),
                }
        return stats

    def collect(self) -> Iterable[dict[str, Any]]:
        api_key = os.getenv("YOUTUBE_API_KEY")
        channels = split_env("YOUTUBE_CHANNEL_IDS")
        queries = split_env("YOUTUBE_SEARCH_QUERIES")
        if not api_key or not (channels or queries):
            LOG.warning("YouTube disabled: set YOUTUBE_API_KEY and YOUTUBE_CHANNEL_IDS or YOUTUBE_SEARCH_QUERIES")
            return

        self.quota_used = 0
        for channel_id in channels:
            self.watch_channel(channel_id, pinned=True)

        video_ids: list[str] = []
        if queries and self.search_due():
            video_ids.extend(self.search_videos(api_key, queries))
        video_ids.extend(self.watched_uploads(api_key))

        discovered: dict[str, dict[str, Any]] = {}
        unique_ids = list(dict.fromkeys(video_ids))
        for start in range(0, len(unique_ids), 50):
            try:
                details = self.api_get("videos", {
                    "key": api_key, "part": self.video_parts, "id": ",".join(unique_ids[start:start + 50]),
                })
            except requests.RequestException as exc:
                LOG.error("YouTube videos lookup failed: %s", exc)
                continue
            for item in details.get("items", []):
                discovered[item["id"]] = item

        # Channel statistics are fetched once per cycle, after all queries are done.
        channel_ids = {item.get("snippet", {}).get("channelId", "") for item in discovered.values()}
        stats = self.channel_stats(api_key, channel_ids)
        include_comments = os.getenv("YOUTUBE_INCLUDE_COMMENTS", "false").lower() == "true"

        for item in discovered.values():
            channel_id = item.get("snippet", {}).get("channelId", "unknown")
            yield self.video_event(item, channel_id, stats.get(channel_id))
            if include_comments:
                yield from self.comment_events(api_key, item, channel_id)
        LOG.info("YouTube cycle: %d video(s) from %d watched channel(s), ~%d quota unit(s) used",
                 len(discovered), len(self.watch), self.quota_used)

    def search_due(self) -> bool:
        """Keyword search costs 100 units per page, so it runs at most once per interval."""
        interval = int(os.getenv("YOUTUBE_SEARCH_INTERVAL_SECONDS", "86400"))
        return self.last_search == 0.0 or time.monotonic() - self.last_search >= interval

    def search_videos(self, api_key: str, queries: list[str]) -> list[str]:
        """Discover videos by keyword; their channels join the watchlist for the next cycles."""
        self.last_search = time.monotonic()
        video_ids: list[str] = []
        for query in queries:
            page_token = None
            for _ in range(self.search_pages()):
                params = {"key": api_key, "part": "snippet", "q": query, "type": "video",
                          "maxResults": 50, "order": os.getenv("YOUTUBE_SEARCH_ORDER", "relevance")}
                if page_token:
                    params["pageToken"] = page_token
                try:
                    page = self.api_get("search", params)
                except requests.RequestException as exc:
                    # One failing query (quota, bad parameter) must not abort the other queries.
                    LOG.error("YouTube search failed for %r: %s", query, exc)
                    break
                for item in page.get("items", []):
                    video_id = item.get("id", {}).get("videoId")
                    if video_id:
                        video_ids.append(video_id)
                    self.watch_channel(item.get("snippet", {}).get("channelId", ""))
                page_token = page.get("nextPageToken")
                if not page_token:
                    break
        return video_ids

    def watched_uploads(self, api_key: str) -> list[str]:
        """Newest uploads of every watched channel: 1 unit per channel instead of 100 per search."""
        missing = [cid for cid in self.watch if cid not in self.uploads]
        for start in range(0, len(missing), 50):
            try:
                data = self.api_get("channels", {
                    "key": api_key, "part": "contentDetails", "id": ",".join(missing[start:start + 50]),
                })
            except requests.RequestException as exc:
                LOG.warning("could not resolve uploads playlists: %s", exc)
                continue
            for item in data.get("items", []):
                playlist = item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
                if playlist:
                    self.uploads[item["id"]] = playlist

        recent = max(1, min(50, int(os.getenv("YOUTUBE_RECENT_VIDEOS", "5"))))
        video_ids: list[str] = []
        for channel_id in self.watch:
            playlist = self.uploads.get(channel_id)
            if not playlist:
                continue
            try:
                page = self.api_get("playlistItems", {
                    "key": api_key, "part": "contentDetails", "playlistId": playlist, "maxResults": recent,
                })
            except requests.RequestException as exc:
                LOG.warning("uploads of channel %s unavailable: %s", channel_id, exc)
                continue
            video_ids.extend(item["contentDetails"]["videoId"] for item in page.get("items", [])
                             if item.get("contentDetails", {}).get("videoId"))
        return video_ids

    def video_event(self, item: dict[str, Any], channel_id: str,
                    channel: dict[str, Any] | None) -> dict[str, Any]:
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        details = item.get("contentDetails", {})
        live = item.get("liveStreamingDetails", {})
        thumbnails = snippet.get("thumbnails") or {}
        thumbnail = (thumbnails.get("maxres") or thumbnails.get("high")
                     or thumbnails.get("medium") or {})
        event = EventBuilder("youtube", self.name).build(
            event_type="post", content_id=item["id"], kol_id=f"youtube:{channel_id}",
            author_id=f"youtube:{channel_id}",
            handle=(channel or {}).get("handle") or snippet.get("channelTitle", ""),
            display_name=(channel or {}).get("display_name") or snippet.get("channelTitle", ""),
            followers=(channel or {}).get("followers"),
            text=f"{snippet.get('title', '')}\n{snippet.get('description', '')}".strip(),
            created_at=snippet.get("publishedAt"),
            metrics={"likes": int(statistics.get("likeCount", 0)),
                     "comments": int(statistics.get("commentCount", 0)),
                     "views": int(statistics.get("viewCount", 0))},
        )
        event.update({
            "tags": snippet.get("tags", []),
            "default_language": snippet.get("defaultLanguage") or snippet.get("defaultAudioLanguage", ""),
            "category_id": snippet.get("categoryId", ""),
            "topics": self.topic_names(item.get("topicDetails") or {}),
            "content_details": {
                "duration_seconds": self.duration_seconds(details.get("duration")),
                "definition": details.get("definition", ""),
                "caption": details.get("caption") == "true",
                "licensed_content": bool(details.get("licensedContent")),
            },
            "live_viewers": int(live["concurrentViewers"]) if live.get("concurrentViewers") else None,
            "media": ([{"type": "image", "url": thumbnail.get("url", ""), "alt": ""}]
                      if thumbnail.get("url") else []),
            "author_videos": (channel or {}).get("videos"),
            "author_total_views": (channel or {}).get("total_views"),
        })
        return event

    def comment_events(self, api_key: str, video: dict[str, Any],
                       channel_id: str) -> Iterable[dict[str, Any]]:
        builder = EventBuilder("youtube", self.name)
        include_replies = os.getenv("YOUTUBE_INCLUDE_REPLIES", "false").lower() == "true"
        page_token = None
        for _ in range(self.comment_pages()):
            params = {"key": api_key, "part": "snippet", "videoId": video["id"],
                      "maxResults": 100, "textFormat": "plainText"}
            if page_token:
                params["pageToken"] = page_token
            try:
                page = self.api_get("commentThreads", params)
            except requests.RequestException as exc:
                # Comments disabled on the video, or quota: the video event is already yielded.
                LOG.warning("comments unavailable for video %s: %s", video["id"], exc)
                return
            for thread in page.get("items", []):
                top = thread["snippet"]["topLevelComment"]
                yield self.comment_event(builder, top, video["id"], channel_id, video["id"], "comment")
                if include_replies and int(thread["snippet"].get("totalReplyCount", 0)) > 0:
                    yield from self.reply_events(api_key, builder, top["id"], video["id"], channel_id)
            page_token = page.get("nextPageToken")
            if not page_token:
                break

    def reply_events(self, api_key: str, builder: EventBuilder, parent_comment_id: str,
                     video_id: str, channel_id: str) -> Iterable[dict[str, Any]]:
        max_pages = max(1, int(os.getenv("YOUTUBE_MAX_REPLY_PAGES", "1")))
        page_token = None
        for _ in range(max_pages):
            params = {"key": api_key, "part": "snippet", "parentId": parent_comment_id,
                      "maxResults": 100, "textFormat": "plainText"}
            if page_token:
                params["pageToken"] = page_token
            try:
                page = self.api_get("comments", params)
            except requests.RequestException as exc:
                LOG.warning("replies unavailable for comment %s: %s", parent_comment_id, exc)
                return
            for item in page.get("items", []):
                yield self.comment_event(builder, item, video_id, channel_id,
                                         parent_comment_id, "reply")
            page_token = page.get("nextPageToken")
            if not page_token:
                break

    @staticmethod
    def comment_event(builder: EventBuilder, item: dict[str, Any], video_id: str,
                      channel_id: str, parent_id: str, event_type: str) -> dict[str, Any]:
        snippet = item.get("snippet", {})
        return builder.build(
            event_type=event_type, content_id=item.get("id", ""), parent_id=parent_id,
            kol_id=f"youtube:{channel_id}",
            author_id=f"youtube:{snippet.get('authorChannelId', {}).get('value', 'unknown')}",
            handle=snippet.get("authorDisplayName", ""),
            display_name=snippet.get("authorDisplayName", ""),
            text=snippet.get("textDisplay", ""), created_at=snippet.get("publishedAt"),
            metrics={"likes": int(snippet.get("likeCount", 0))},
        )


class BlueskyCollector(Collector):
    name = "bluesky-worker"
    api = "https://public.api.bsky.app/xrpc"

    def __init__(self, publisher: KafkaPublisher) -> None:
        super().__init__(publisher)
        self.post_cache: dict[str, dict[str, str]] = {}
        # did -> (profile fields, fetched_at). TTL keeps follower counts reasonably fresh without
        # spending one API call per event.
        self.author_cache: dict[str, tuple[dict[str, Any], float]] = {}

    # ---------------------------------------------------------------- enrichment
    def resolve_subject(self, subject_uri: str) -> dict[str, str]:
        cached = self.post_cache.get(subject_uri)
        if cached:
            return cached
        if os.getenv("BLUESKY_ENRICH_INTERACTIONS", "false").lower() != "true":
            return {}
        try:
            data = self.request("GET", f"{self.api}/app.bsky.feed.getPosts",
                                params={"uris": subject_uri})
        except requests.RequestException as exc:
            LOG.warning("could not enrich Bluesky subject %s: %s", subject_uri, exc)
            return {}
        post = (data.get("posts") or [{}])[0]
        record = post.get("record") or {}
        source = {"text": record.get("text") or "", "lang": (record.get("langs") or [""])[0]}
        if source["text"]:
            max_cache = int(os.getenv("BLUESKY_POST_CACHE_SIZE", "100000"))
            self.post_cache[subject_uri] = source
            if len(self.post_cache) > max_cache:
                self.post_cache.pop(next(iter(self.post_cache)))
        return source

    def resolve_authors(self, dids: set[str]) -> None:
        """Hydrate handles/display names/follower counts, one getProfiles call per 25 authors.

        The firehose carries only the author DID, so without this every Bluesky event would show up
        with an empty handle and no follower count in the dashboard.
        """
        if os.getenv("BLUESKY_ENRICH_AUTHORS", "true").lower() != "true":
            return
        ttl = int(os.getenv("BLUESKY_AUTHOR_CACHE_SECONDS", "3600"))
        now_ts = time.time()
        missing = [did for did in dids
                   if did and now_ts - self.author_cache.get(did, ({}, 0.0))[1] > ttl]
        for start in range(0, len(missing), 25):
            chunk = missing[start:start + 25]
            params = [("actors", did) for did in chunk]
            try:
                data = self.request("GET", f"{self.api}/app.bsky.actor.getProfiles", params=params)
            except requests.RequestException as exc:
                LOG.warning("could not enrich %d Bluesky author(s): %s", len(chunk), exc)
                continue
            for profile in data.get("profiles", []):
                did = profile.get("did", "")
                if not did:
                    continue
                self.author_cache[did] = ({
                    "handle": profile.get("handle", ""),
                    "display_name": profile.get("displayName", ""),
                    "followers": profile.get("followersCount"),
                }, now_ts)

    def apply_author(self, event: dict[str, Any]) -> None:
        info = self.author_cache.get(event["author"]["id"], ({}, 0.0))[0]
        if not info:
            return
        author = event["author"]
        author["handle"] = author.get("handle") or info.get("handle", "")
        author["display_name"] = author.get("display_name") or info.get("display_name", "")
        if author.get("followers") is None and info.get("followers") is not None:
            author["followers"] = info["followers"]

    def publish_batch(self, events: list[dict[str, Any]]) -> None:
        """Enrich the whole batch before publishing, so ordering and the P0 cursor logic hold.

        Only the KOL's own events (posts, deletes) are looked up: on a like or a follow the author
        is a fan, whose profile Module 3 never uses. Looking up every fan made the firehose an
        HTTP-bound loop of a few events per second.
        """
        missing = {event["author"]["id"] for event in events
                   if event["author"].get("id") and not event["author"].get("handle")
                   and event["kol_id"] == f"bluesky:{event['author']['id']}"}
        if missing:
            self.resolve_authors(missing)
        for event in events:
            self.apply_author(event)
            self.publisher.publish(event)

    # ---------------------------------------------------------------- payload parsing
    @staticmethod
    def extract_media(embed: dict[str, Any] | None) -> list[dict[str, str]]:
        """Images / video / external link / quoted post. Handles both the record and view form."""
        media: list[dict[str, str]] = []
        if not isinstance(embed, dict):
            return media
        for image in embed.get("images") or []:
            media.append({
                "type": "image",
                "url": image.get("fullsize") or image.get("thumb") or "",
                "alt": image.get("alt", ""),
            })
        external = embed.get("external")
        if isinstance(external, dict):
            media.append({
                "type": "link",
                "url": external.get("uri", ""),
                "title": external.get("title", ""),
                "alt": external.get("description", ""),
            })
        video = embed.get("video")
        if isinstance(video, dict):
            media.append({
                "type": "video",
                "url": video.get("playlist") or video.get("thumbnail") or "",
                "alt": embed.get("alt", ""),
            })
        record_embed = embed.get("record")
        if isinstance(record_embed, dict):
            quoted = record_embed.get("uri") or (record_embed.get("record") or {}).get("uri")
            if quoted:
                media.append({"type": "quote", "url": quoted, "alt": ""})
        # recordWithMedia nests the media one level deeper.
        nested = embed.get("media")
        if isinstance(nested, dict):
            media.extend(BlueskyCollector.extract_media(nested))
        return media

    @staticmethod
    def extract_facets(record: dict[str, Any]) -> tuple[list[dict[str, str]], list[str], list[str], list[str]]:
        """Facets carry authoritative tags/mentions/links with correct (byte) offsets."""
        facets: list[dict[str, str]] = []
        hashtags: list[str] = []
        mentions: list[str] = []
        links: list[str] = []
        for facet in record.get("facets") or []:
            for feature in facet.get("features") or []:
                kind = feature.get("$type", "")
                if kind.endswith("#mention"):
                    did = feature.get("did", "")
                    if did:
                        mentions.append(did)
                        facets.append({"type": "mention", "value": did})
                elif kind.endswith("#link"):
                    uri = feature.get("uri", "")
                    if uri:
                        links.append(uri)
                        facets.append({"type": "link", "value": uri})
                elif kind.endswith("#tag"):
                    tag = (feature.get("tag") or "").lower()
                    if tag:
                        hashtags.append(tag)
                        facets.append({"type": "tag", "value": tag})
        return facets, sorted(set(hashtags)), sorted(set(mentions)), sorted(set(links))

    def firehose_url(self) -> str:
        collections = split_env("BLUESKY_COLLECTIONS") or [
            "app.bsky.feed.post",
            "app.bsky.feed.like",
            "app.bsky.feed.repost",            # Follows are how we learn a KOL's follower graph; they are cheap and infrequent.
            "app.bsky.graph.follow",        ]
        params = [("wantedCollections", collection) for collection in collections]
        cursor = self.load_cursor()
        if cursor is not None:
            params.append(("cursor", str(cursor)))
        query = "&".join(f"{key}={value}" for key, value in params)
        return f"{os.getenv('BLUESKY_FIREHOSE_URL', 'wss://jetstream1.us-east.bsky.network/subscribe')}?{query}"

    def load_cursor(self) -> int | None:
        path = Path(os.getenv("BLUESKY_CURSOR_FILE", "/data/bluesky-cursor.json"))
        try:
            return int(json.loads(path.read_text(encoding="utf-8"))["cursor"])
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def save_cursor(self, cursor: int) -> None:
        path = Path(os.getenv("BLUESKY_CURSOR_FILE", "/data/bluesky-cursor.json"))
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"cursor": cursor}), encoding="utf-8")
        temporary.replace(path)

    collections = {
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.feed.repost",
        "app.bsky.graph.follow",
    }

    @staticmethod
    def did_of(uri: str | None) -> str | None:
        """DID of the repository that owns an at:// URI (at://<did>/<collection>/<rkey>)."""
        if isinstance(uri, str) and uri.startswith("at://did:"):
            return uri[len("at://"):].split("/", 1)[0]
        return None

    def normalize_firehose_event(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if payload.get("kind") != "commit":
            return None
        did = payload.get("did")
        commit = payload.get("commit", {})
        collection = commit.get("collection", "")
        operation = commit.get("operation")
        rkey = commit.get("rkey", "")
        record = commit.get("record") or {}
        if not did or not rkey or operation not in {"create", "update", "delete"}:
            return None
        if collection not in self.collections:
            return None
        if not isinstance(record, dict):
            raise ValueError("record is not an object")

        content_id = f"at://{did}/{collection}/{rkey}"
        extras: dict[str, Any] = {"cid": commit.get("cid", ""), "rev": commit.get("rev", "")}
        parent_id: str | None = None
        text = ""
        lang: str | None = None

        if operation == "delete":
            event_type = "delete"
        elif collection == "app.bsky.feed.post":
            event_type = "reply" if record.get("reply") else "post"
            reply = record.get("reply") or {}
            parent_id = (reply.get("parent") or {}).get("uri")
            # reply.root is what makes a conversational thread reconstructable; without it every
            # reply is only linked to its immediate parent.
            extras["root_id"] = (reply.get("root") or {}).get("uri")
            text = record.get("text") or ""
            lang = (record.get("langs") or [None])[0]
            facets, tags, mentions, links = self.extract_facets(record)
            extras.update({
                "facets": facets,
                "links": links,
                "media": self.extract_media(record.get("embed")),
            })
            if tags:
                extras["hashtags"] = tags
            if mentions:
                extras["mentions"] = mentions
        elif collection == "app.bsky.feed.like":
            event_type = "like"
        elif collection == "app.bsky.feed.repost":
            event_type = "repost"
        else:
            event_type = "follow"

        if event_type in {"like", "repost"}:
            subject = record.get("subject")
            subject_uri = subject.get("uri") if isinstance(subject, dict) else None
            parent_id = subject_uri
            if subject_uri:
                source = self.resolve_subject(subject_uri)
                text = source.get("text", "")
                lang = source.get("lang") or None
        elif event_type == "follow":
            subject = record.get("subject")
            if isinstance(subject, str):
                parent_id = subject

        # Engagement is scored for the account that RECEIVES it: a like, repost or reply belongs to
        # the author of the post it targets, a follow to the followed account. author.id stays the
        # actor, so who did what is still recorded.
        kol_did = did
        if event_type in {"like", "repost", "reply"}:
            kol_did = self.did_of(parent_id) or did
        elif event_type == "follow" and isinstance(parent_id, str) and parent_id.startswith("did:"):
            kol_did = parent_id

        created_at = record.get("createdAt")
        if not created_at and payload.get("time_us"):
            created_at = datetime.fromtimestamp(
                payload["time_us"] / 1_000_000, timezone.utc
            ).isoformat().replace("+00:00", "Z")

        event = EventBuilder("bluesky", self.name).build(
            event_type=event_type,
            content_id=content_id,
            parent_id=parent_id,
            kol_id=f"bluesky:{kol_did}",
            author_id=did,
            text=text,
            created_at=created_at,
            lang=lang,
        )
        event.update(extras)
        if collection == "app.bsky.feed.post" and operation != "delete" and text:
            max_cache = int(os.getenv("BLUESKY_POST_CACHE_SIZE", "100000"))
            self.post_cache[content_id] = {"text": text, "lang": lang or ""}
            if len(self.post_cache) > max_cache:
                self.post_cache.pop(next(iter(self.post_cache)))
        return event

    def commit_cursor(self, cursor: int | None, errors_before: int) -> bool:
        """Persist the cursor only after Kafka confirmed everything published before it.

        If a delivery failed, the cursor stays at the last confirmed value. On restart the firehose
        replays from there: a few events are duplicated (the DB writer upserts on event_id) instead
        of being lost, which is the failure mode we want.
        """
        if cursor is None:
            return True
        if self.publisher.delivery_errors > errors_before:
            LOG.error(
                "cursor not advanced past %d: %d Kafka delivery error(s) since the last checkpoint",
                cursor, self.publisher.delivery_errors - errors_before,
            )
            return False
        if not self.publisher.flush():
            LOG.error("cursor not advanced past %d: Kafka did not confirm every message", cursor)
            return False
        self.save_cursor(cursor)
        return True

    def run_firehose(self) -> None:
        reconnect_seconds = int(os.getenv("BLUESKY_RECONNECT_SECONDS", "5"))
        # Cursor is written per batch, not per event: at firehose rate a file write per event is
        # pure disk I/O and slows down the very loop that has to keep up.
        flush_every = max(1, int(os.getenv("BLUESKY_FLUSH_EVERY", "500")))
        flush_seconds = max(1.0, float(os.getenv("BLUESKY_FLUSH_SECONDS", "5")))
        # Author enrichment is batched (25 profiles per API call), so events are buffered briefly.
        publish_batch = max(1, int(os.getenv("BLUESKY_PUBLISH_BATCH", "25")))
        while not STOP:
            stream = None
            confirmed_cursor = self.load_cursor()
            pending_cursor = confirmed_cursor
            errors_at_checkpoint = self.publisher.delivery_errors
            since_checkpoint = 0
            last_checkpoint = time.monotonic()
            buffer: list[dict[str, Any]] = []
            try:
                url = self.firehose_url()
                LOG.info("connecting to Bluesky firehose")
                stream = websocket.create_connection(url, timeout=HTTP_TIMEOUT)
                stream.settimeout(HTTP_TIMEOUT)
                while not STOP:
                    try:
                        raw = stream.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not raw:
                        break
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        LOG.error("invalid Bluesky payload: %s", exc)
                        self.publisher.publish_to_dlq(raw, exc, "bluesky-firehose")
                        continue
                    since_checkpoint += 1
                    # One malformed record must never stop collection for the whole network.
                    try:
                        event = self.normalize_firehose_event(payload)
                    except Exception as exc:  # noqa: BLE001
                        LOG.error("could not normalize Bluesky event: %s", exc)
                        self.publisher.publish_to_dlq(payload, exc, "bluesky-firehose")
                        event = None
                    if event:
                        buffer.append(event)
                        if len(buffer) >= publish_batch:
                            self.publish_batch(buffer)
                            buffer.clear()
                    if payload.get("time_us"):
                        pending_cursor = int(payload["time_us"])
                    if (since_checkpoint >= flush_every
                            or time.monotonic() - last_checkpoint >= flush_seconds):
                        # Everything before this cursor must be in Kafka before it is persisted.
                        if buffer:
                            self.publish_batch(buffer)
                            buffer.clear()
                        if pending_cursor != confirmed_cursor and self.commit_cursor(
                                pending_cursor, errors_at_checkpoint):
                            confirmed_cursor = pending_cursor
                            errors_at_checkpoint = self.publisher.delivery_errors
                        since_checkpoint = 0
                        last_checkpoint = time.monotonic()
            except (websocket.WebSocketException, OSError) as exc:
                LOG.error("Bluesky firehose disconnected: %s", exc)
                if not STOP:
                    time.sleep(reconnect_seconds)
            finally:
                if stream is not None:
                    stream.close()
                if buffer:
                    self.publish_batch(buffer)
                    buffer.clear()
                # Persist progress on disconnect/shutdown so a restart resumes from here.
                if pending_cursor != confirmed_cursor:
                    self.commit_cursor(pending_cursor, errors_at_checkpoint)

    def collect(self) -> Iterable[dict[str, Any]]:
        handles = split_env("BLUESKY_HANDLES")
        if not handles:
            LOG.warning("Bluesky disabled: set BLUESKY_HANDLES")
            return
        builder = EventBuilder("bluesky", self.name)
        for handle in handles:
            profile = self.request("GET", f"{self.api}/com.atproto.identity.resolveHandle",
                                   params={"handle": handle})
            did = profile["did"]
            feed = self.request("GET", f"{self.api}/app.bsky.feed.getAuthorFeed",
                                params={"actor": did, "limit": 100}).get("feed", [])
            self.resolve_authors({entry.get("post", {}).get("author", {}).get("did", did)
                                  for entry in feed})
            for entry in feed:
                post = entry.get("post", {})
                record = post.get("record") or {}
                author = post.get("author", {})
                text = record.get("text") or ""
                facets, tags, mentions, links = self.extract_facets(record)
                reply = record.get("reply") or {}
                metrics = {"likes": int(post.get("likeCount", 0)),
                           "shares": int(post.get("repostCount", 0)),
                           "comments": int(post.get("replyCount", 0))}
                event = builder.build(
                    event_type="repost" if entry.get("reason", {}).get("$type", "").endswith("repost") else "post",
                    content_id=post.get("uri", ""), kol_id=f"bluesky:{did}",
                    author_id=author.get("did", did),
                    handle=author.get("handle", handle), display_name=author.get("displayName", ""),
                    text=text, created_at=record.get("createdAt"), metrics=metrics,
                    lang=(record.get("langs") or [None])[0],
                    hashtags=tags or None, mentions=mentions or None,
                    parent_id=(reply.get("parent") or {}).get("uri"),
                )
                event.update({
                    "facets": facets,
                    "links": links,
                    "media": self.extract_media(record.get("embed")),
                    "root_id": (reply.get("root") or {}).get("uri"),
                    "cid": post.get("cid", ""),
                })
                self.apply_author(event)
                yield event


# Collectors live across polling cycles: the YouTube watchlist and the time of the last keyword
# search must survive from one cycle to the next.
_INSTANCES: dict[str, Collector] = {}


def run_once(publisher: KafkaPublisher, requested: set[str] | None = None) -> None:
    if requested is None:
        requested = {name.lower() for name in split_env("COLLECTORS") or ["youtube", "bluesky"]}
    collectors = {"youtube": YouTubeCollector, "bluesky": BlueskyCollector}
    for name in requested:
        collector = collectors.get(name)
        if not collector:
            LOG.warning("Unknown collector: %s", name)
            continue
        instance = _INSTANCES.get(name)
        if instance is None or instance.publisher is not publisher:
            instance = _INSTANCES[name] = collector(publisher)
        try:
            instance.run()
        except requests.RequestException as exc:
            LOG.error("%s request failed: %s", name, exc)
        except (KeyError, ValueError, TypeError) as exc:
            LOG.error("%s response could not be normalized: %s", name, exc)


def main() -> int:
    global STOP
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="poll each enabled platform once and exit")
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    publisher = KafkaPublisher()

    def stop(*_: Any) -> None:
        global STOP
        STOP = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    requested = {name.lower() for name in split_env("COLLECTORS") or ["youtube", "bluesky"]}
    firehose_thread = None
    if "bluesky" in requested and os.getenv("BLUESKY_MODE", "firehose").lower() == "firehose" and not args.once:
        requested.remove("bluesky")
        firehose_thread = threading.Thread(target=BlueskyCollector(publisher).run_firehose, daemon=True)
        firehose_thread.start()
    try:
        while not STOP:
            run_once(publisher, requested)
            if not publisher.flush():
                LOG.error("Kafka did not confirm every queued message")
            if args.once:
                break
            time.sleep(POLL_SECONDS)
    finally:
        STOP = True
        if firehose_thread:
            firehose_thread.join(timeout=2)
        if not publisher.flush():
            LOG.error("Kafka did not confirm every queued message on shutdown")
    LOG.info("published %d event(s)", publisher.published)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
