"""In-memory stateful aggregation for KOL engagement scores and keyword frequencies."""

from __future__ import annotations

from collections import Counter, OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from processor.config import ScoringWeights
from processor.tokenizer import extract_keywords_and_hashtags

REQUIRED_EVENT_FIELDS = (
    "event_id",
    "platform",
    "event_type",
    "content_id",
    "kol_id",
    "author",
    "created_at",
    "ingested_at",
)
VALID_PLATFORMS = frozenset({"bluesky", "youtube"})
VALID_EVENT_TYPES = frozenset({"post", "comment", "reply", "like", "repost", "follow", "delete"})


class EventValidationError(ValueError):
    """Raised when an incoming social event violates the required contract."""


def event_timestamp(event: dict[str, Any], now_ts: float) -> float:
    """When the collector received the event (ingested_at, else created_at), never later than now.

    The sliding window uses this time, not the processing time, so events replayed from Kafka after
    a restart do not all land in the "last 15 minutes".
    """
    for field_name in ("ingested_at", "created_at"):
        value = event.get(field_name)
        if not isinstance(value, str) or not value:
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return min(parsed.timestamp(), now_ts)
    return now_ts


def is_kol_author(event: dict[str, Any]) -> bool:
    """True when the event was made by the KOL itself (Bluesky author ids carry no platform prefix)."""
    author_id = str((event.get("author") or {}).get("id") or "")
    kol_id = str(event.get("kol_id") or "")
    return bool(author_id) and kol_id in (author_id, f"{event.get('platform')}:{author_id}")


def validate_event(event: Any) -> dict[str, Any]:
    """Validate that a parsed JSON payload conforms to social-event.schema.json."""
    if not isinstance(event, dict):
        raise EventValidationError("Event payload must be a JSON object")

    for req in REQUIRED_EVENT_FIELDS:
        if req not in event or event[req] is None or event[req] == "":
            raise EventValidationError(f"Missing required field: '{req}'")

    platform = event["platform"]
    if platform not in VALID_PLATFORMS:
        raise EventValidationError(f"Unsupported platform: '{platform}'")

    event_type = event["event_type"]
    if event_type not in VALID_EVENT_TYPES:
        raise EventValidationError(f"Unsupported event_type: '{event_type}'")

    author = event["author"]
    if not isinstance(author, dict) or not author.get("id"):
        raise EventValidationError("Field 'author' must be an object with non-empty 'id'")

    metrics = event.get("metrics")
    if metrics is not None and not isinstance(metrics, dict):
        raise EventValidationError("Field 'metrics' must be an object when present")

    return event


class DedupCache:
    """Bounded LRU cache of recent event_ids to ensure idempotent processing."""

    def __init__(self, max_size: int = 200_000) -> None:
        self.max_size = max(1, max_size)
        self._store: OrderedDict[str, float] = OrderedDict()

    def seen_or_add(self, event_id: str, now_ts: float) -> bool:
        """Return True if event_id was already seen; otherwise record it and return False."""
        if event_id in self._store:
            self._store.move_to_end(event_id)
            return True
        self._store[event_id] = now_ts
        if len(self._store) > self.max_size:
            self._store.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._store)


@dataclass
class KOLWindowSample:
    """Single event contribution kept inside a KOL's sliding window."""

    timestamp: float
    likes: int
    shares: int
    comments: int
    views: int
    score: float


@dataclass
class KOLRecord:
    """Aggregated state for a single KOL (`key = kol_id`)."""

    kol_id: str
    platform: str
    handle: str | None = None
    followers: int | None = None
    total_events: int = 0
    event_breakdown: dict[str, int] = field(
        default_factory=lambda: {
            "post": 0,
            "comment": 0,
            "reply": 0,
            "like": 0,
            "repost": 0,
            "follow": 0,
            "delete": 0,
        }
    )
    total_likes: int = 0
    total_shares: int = 0
    total_comments: int = 0
    total_views: int = 0
    total_score: float = 0.0
    hashtag_counts: Counter[str] = field(default_factory=Counter)
    window_samples: deque[KOLWindowSample] = field(default_factory=deque)
    window_likes: int = 0
    window_shares: int = 0
    window_comments: int = 0
    window_views: int = 0
    window_score: float = 0.0
    last_event_at: str | None = None
    dirty: bool = False

    def add_event(
        self,
        event: dict[str, Any],
        event_score: float,
        hashtags: list[str],
        now_ts: float,
        window_seconds: float,
        event_ts: float | None = None,
    ) -> None:
        author = event.get("author") or {}
        # Profile data only comes from the KOL's own events: on a like or a comment the author is
        # the fan, and their handle must not become the KOL's name.
        if is_kol_author(event):
            if author.get("handle"):
                self.handle = str(author["handle"])
            if isinstance(author.get("followers"), int) and author["followers"] >= 0:
                self.followers = author["followers"]
        event_ts = now_ts if event_ts is None else event_ts

        event_type = str(event.get("event_type", "post"))
        self.total_events += 1
        self.event_breakdown[event_type] = self.event_breakdown.get(event_type, 0) + 1

        metrics = event.get("metrics") or {}
        likes = max(0, int(metrics.get("likes") or 0))
        shares = max(0, int(metrics.get("shares") or 0))
        comments = max(0, int(metrics.get("comments") or 0))
        views = max(0, int(metrics.get("views") or 0))

        self.total_likes += likes
        self.total_shares += shares
        self.total_comments += comments
        self.total_views += views
        self.total_score = round(self.total_score + event_score, 4)

        for tag in hashtags:
            self.hashtag_counts[tag] += 1

        # Older events (a replay, a backlog) count toward the totals but not toward "trending now".
        if event_ts >= now_ts - window_seconds:
            sample = KOLWindowSample(
                timestamp=event_ts,
                likes=likes,
                shares=shares,
                comments=comments,
                views=views,
                score=event_score,
            )
            self.window_samples.append(sample)
            self.window_likes += likes
            self.window_shares += shares
            self.window_comments += comments
            self.window_views += views
            self.window_score = round(self.window_score + event_score, 4)

        self.last_event_at = str(event.get("created_at") or "")
        self.evict_expired(now_ts, window_seconds)
        self.dirty = True

    def evict_expired(self, now_ts: float, window_seconds: float) -> bool:
        """Remove samples older than `now_ts - window_seconds`. Return True if anything was evicted."""
        cutoff = now_ts - window_seconds
        evicted = False
        while self.window_samples and self.window_samples[0].timestamp < cutoff:
            old = self.window_samples.popleft()
            self.window_likes = max(0, self.window_likes - old.likes)
            self.window_shares = max(0, self.window_shares - old.shares)
            self.window_comments = max(0, self.window_comments - old.comments)
            self.window_views = max(0, self.window_views - old.views)
            self.window_score = max(0.0, round(self.window_score - old.score, 4))
            evicted = True
        if not self.window_samples:
            self.window_likes = 0
            self.window_shares = 0
            self.window_comments = 0
            self.window_views = 0
            self.window_score = 0.0
        if evicted:
            self.dirty = True
        return evicted

    def to_payload(self, window_minutes: int, updated_at_iso: str) -> dict[str, Any]:
        top_tags = [tag for tag, _ in self.hashtag_counts.most_common(5)]
        velocity = round(self.window_score / max(1, window_minutes), 4)
        return {
            "kol_id": self.kol_id,
            "platform": self.platform,
            "handle": self.handle,
            "followers": self.followers,
            "total_events": self.total_events,
            "window_events": len(self.window_samples),
            "window_minutes": window_minutes,
            "event_breakdown": dict(self.event_breakdown),
            "metrics_total": {
                "likes": self.total_likes,
                "shares": self.total_shares,
                "comments": self.total_comments,
                "views": self.total_views,
            },
            "metrics_window": {
                "likes": self.window_likes,
                "shares": self.window_shares,
                "comments": self.window_comments,
                "views": self.window_views,
            },
            "engagement_score": round(self.total_score, 2),
            "window_score": round(self.window_score, 2),
            "velocity_per_min": round(velocity, 2),
            "top_hashtags": top_tags,
            "last_event_at": self.last_event_at,
            "updated_at": updated_at_iso,
        }


@dataclass
class KeywordOccurrence:
    timestamp: float
    platform: str
    kol_id: str


@dataclass
class KeywordRecord:
    """Sliding-window state for a single keyword or hashtag."""

    keyword: str
    is_hashtag: bool = False
    total_count: int = 0
    occurrences: deque[KeywordOccurrence] = field(default_factory=deque)
    platform_counts: dict[str, int] = field(
        default_factory=lambda: {"bluesky": 0, "youtube": 0}
    )
    kol_counts: Counter[str] = field(default_factory=Counter)
    dirty: bool = False
    # Once published, later updates (including the count decaying to 0) keep being published
    # even if the keyword falls under the minimum number of KOLs.
    published: bool = False

    def add_occurrence(
        self,
        is_hashtag: bool,
        platform: str,
        kol_id: str,
        now_ts: float,
        window_seconds: float,
        event_ts: float | None = None,
    ) -> None:
        if is_hashtag:
            self.is_hashtag = True
        self.total_count += 1
        event_ts = now_ts if event_ts is None else event_ts
        if event_ts >= now_ts - window_seconds:
            self.occurrences.append(KeywordOccurrence(timestamp=event_ts, platform=platform, kol_id=kol_id))
            if platform in self.platform_counts:
                self.platform_counts[platform] += 1
            else:
                self.platform_counts[platform] = 1
            self.kol_counts[kol_id] += 1
        self.evict_expired(now_ts, window_seconds)
        self.dirty = True

    def evict_expired(self, now_ts: float, window_seconds: float) -> bool:
        cutoff = now_ts - window_seconds
        evicted = False
        while self.occurrences and self.occurrences[0].timestamp < cutoff:
            old = self.occurrences.popleft()
            if old.platform in self.platform_counts:
                self.platform_counts[old.platform] = max(0, self.platform_counts[old.platform] - 1)
            if old.kol_id in self.kol_counts:
                self.kol_counts[old.kol_id] -= 1
                if self.kol_counts[old.kol_id] <= 0:
                    del self.kol_counts[old.kol_id]
            evicted = True
        if evicted:
            self.dirty = True
        return evicted

    @property
    def window_count(self) -> int:
        return len(self.occurrences)

    def to_payload(self, window_minutes: int, updated_at_iso: str) -> dict[str, Any]:
        w_count = self.window_count
        return {
            "keyword": self.keyword,
            "is_hashtag": self.is_hashtag,
            "count": w_count,
            "total_count": self.total_count,
            "window_minutes": window_minutes,
            "velocity_per_min": round(w_count / max(1, window_minutes), 2),
            "platforms": {
                "bluesky": self.platform_counts.get("bluesky", 0),
                "youtube": self.platform_counts.get("youtube", 0),
            },
            "unique_kols": len(self.kol_counts),
            "updated_at": updated_at_iso,
        }


class StreamState:
    """Holds all in-memory state for the stream processor instance."""

    def __init__(
        self,
        window_minutes: int = 15,
        dedup_cache_size: int = 200_000,
        extract_text_keywords: bool = True,
        weights: ScoringWeights | None = None,
        max_active_kols: int = 0,
        min_keyword_kols: int = 1,
    ) -> None:
        self.window_minutes = max(1, window_minutes)
        self.window_seconds = float(self.window_minutes * 60)
        self.extract_text_keywords = extract_text_keywords
        self.weights = weights or ScoringWeights()
        # 0 = unlimited. The Bluesky firehose covers the whole network, so without a limit every
        # account ever seen would stay in RAM until the pod is OOM-killed.
        self.max_active_kols = max(0, max_active_kols)
        self.min_keyword_kols = max(1, min_keyword_kols)

        self.dedup = DedupCache(max_size=dedup_cache_size)
        # Ordered by last activity (oldest first), which is the eviction order.
        self.kols: OrderedDict[str, KOLRecord] = OrderedDict()
        self.keywords: dict[str, KeywordRecord] = {}

    def seed_kol(self, kol_id: str, saved: dict[str, Any]) -> bool:
        """Restore a KOL's cumulative totals saved in PostgreSQL (kol.kol_scores).

        Called before the first event of a KOL that is not in RAM: after a restart, a rebalance or
        an eviction. Returns False when the KOL is already tracked.
        """
        if kol_id in self.kols:
            return False
        record = KOLRecord(kol_id=kol_id, platform=str(saved.get("platform") or kol_id.split(":", 1)[0]))
        record.handle = saved.get("handle") or None
        followers = saved.get("followers")
        record.followers = followers if isinstance(followers, int) else None
        record.total_events = int(saved.get("total_events") or 0)
        breakdown = saved.get("event_breakdown")
        if isinstance(breakdown, dict):
            for key, value in breakdown.items():
                record.event_breakdown[str(key)] = int(value or 0)
        totals = saved.get("metrics_total") if isinstance(saved.get("metrics_total"), dict) else {}
        record.total_likes = int(totals.get("likes") or 0)
        record.total_shares = int(totals.get("shares") or 0)
        record.total_comments = int(totals.get("comments") or 0)
        record.total_views = int(totals.get("views") or 0)
        record.total_score = float(saved.get("engagement_score") or 0.0)
        # Only the top hashtags are saved, so their counts restart from a small baseline.
        for tag in saved.get("top_hashtags") or []:
            record.hashtag_counts[str(tag)] += 1
        record.last_event_at = saved.get("last_event_at")
        self.kols[kol_id] = record
        return True

    def evict_inactive(self) -> int:
        """Drop the least recently active KOLs that were already published, down to max_active_kols.

        Their totals live on in kol.metrics.kol-scores / PostgreSQL and are restored by seed_kol
        when the KOL shows up again.
        """
        if not self.max_active_kols or len(self.kols) <= self.max_active_kols:
            return 0
        evicted = 0
        for kol_id in list(self.kols):
            if len(self.kols) <= self.max_active_kols:
                break
            if not self.kols[kol_id].dirty:
                del self.kols[kol_id]
                evicted += 1
        return evicted

    def process_event(self, raw_event: Any, now_ts: float) -> bool:
        """Validate, deduplicate, and aggregate a single event.

        Returns True if the event was newly processed, False if skipped as a duplicate.
        Raises EventValidationError if the event is malformed.
        """
        event = validate_event(raw_event)
        event_id = str(event["event_id"])

        if self.dedup.seen_or_add(event_id, now_ts):
            return False

        kol_id = str(event["kol_id"])
        platform = str(event["platform"])
        event_type = str(event["event_type"])
        metrics = event.get("metrics") or {}

        event_score = self.weights.calculate_event_score(event_type, metrics)
        event_ts = event_timestamp(event, now_ts)
        extracted = extract_keywords_and_hashtags(
            event, extract_from_text=self.extract_text_keywords
        )
        hashtags_only = [kw for kw, is_tag in extracted if is_tag]

        kol_record = self.kols.get(kol_id)
        if kol_record is None:
            kol_record = KOLRecord(kol_id=kol_id, platform=platform)
            self.kols[kol_id] = kol_record
        else:
            self.kols.move_to_end(kol_id)

        kol_record.add_event(
            event=event,
            event_score=event_score,
            hashtags=hashtags_only,
            now_ts=now_ts,
            window_seconds=self.window_seconds,
            event_ts=event_ts,
        )

        for kw, is_tag in extracted:
            kw_record = self.keywords.get(kw)
            if kw_record is None:
                kw_record = KeywordRecord(keyword=kw, is_hashtag=is_tag)
                self.keywords[kw] = kw_record
            kw_record.add_occurrence(
                is_hashtag=is_tag,
                platform=platform,
                kol_id=kol_id,
                now_ts=now_ts,
                window_seconds=self.window_seconds,
                event_ts=event_ts,
            )

        return True

    def collect_dirty_outputs(
        self,
        now_ts: float,
        top_keywords_limit: int = 100,
        updated_at_iso: str | None = None,
    ) -> tuple[list[tuple[str, dict[str, Any]]], list[tuple[str, dict[str, Any]]]]:
        """Evict expired window items and return `(kol_score_messages, keyword_messages)` to publish."""
        if updated_at_iso is None:
            updated_at_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

        kol_outputs: list[tuple[str, dict[str, Any]]] = []
        for kol_id, record in self.kols.items():
            record.evict_expired(now_ts, self.window_seconds)
            if record.dirty:
                kol_outputs.append((kol_id, record.to_payload(self.window_minutes, updated_at_iso)))
                record.dirty = False

        # Evict expired occurrences from keywords and prune completely inactive keywords
        dirty_keywords: list[KeywordRecord] = []
        empty_keys: list[str] = []
        for kw, record in self.keywords.items():
            record.evict_expired(now_ts, self.window_seconds)
            if record.window_count == 0 and not record.dirty:
                empty_keys.append(kw)
                continue
            if record.dirty:
                # A word used by a single account is noise, not a trend.
                if record.published or len(record.kol_counts) >= self.min_keyword_kols:
                    dirty_keywords.append(record)
                else:
                    record.dirty = False

        for kw in empty_keys:
            del self.keywords[kw]

        # Sort dirty keywords by current window count (descending), then total_count
        dirty_keywords.sort(key=lambda r: (r.window_count, r.total_count), reverse=True)
        selected_keywords = dirty_keywords[: max(1, top_keywords_limit)]

        keyword_outputs: list[tuple[str, dict[str, Any]]] = []
        for record in selected_keywords:
            keyword_outputs.append(
                (record.keyword, record.to_payload(self.window_minutes, updated_at_iso))
            )
            record.dirty = False
            record.published = True

        # Clear dirty flag on remaining low-rank keywords so they don't accumulate forever
        for record in dirty_keywords[max(1, top_keywords_limit) :]:
            record.dirty = False

        return kol_outputs, keyword_outputs
