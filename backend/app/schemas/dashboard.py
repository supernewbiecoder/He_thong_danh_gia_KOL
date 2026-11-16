from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class PlatformSummary(BaseModel):
    platform: str
    kols: int
    events: int


class SummaryResponse(BaseModel):
    period_minutes: int
    platform: str
    kols: int
    events: int
    window_events: int
    engagement_score: float
    velocity_per_min: float
    platforms: list[PlatformSummary]


class KOLScore(BaseModel):
    model_config = ConfigDict(extra="allow")

    kol_id: str
    platform: str
    handle: str | None = None
    followers: int | None = None
    total_events: int
    window_events: int
    window_minutes: int
    event_breakdown: dict[str, Any]
    metrics_total: dict[str, Any]
    metrics_window: dict[str, Any]
    engagement_score: float
    window_score: float
    velocity_per_min: float
    top_hashtags: list[str]
    last_event_at: datetime | None = None
    updated_at: datetime


class KOLListResponse(BaseModel):
    period_minutes: int
    platform: str
    total: int
    items: list[KOLScore]


class ScoreHistoryPoint(BaseModel):
    bucket: datetime
    engagement_score: float
    window_score: float
    velocity_per_min: float
    total_events: int


class KOLHistoryResponse(BaseModel):
    kol_id: str
    days: int
    items: list[ScoreHistoryPoint]


class KeywordMetric(BaseModel):
    keyword: str
    is_hashtag: bool
    count: int
    total_count: int
    window_minutes: int
    velocity_per_min: float
    platforms: dict[str, Any]
    unique_kols: int
    updated_at: datetime


class KeywordListResponse(BaseModel):
    period_minutes: int
    total: int
    items: list[KeywordMetric]


class PlatformBreakdown(BaseModel):
    platform: str
    kols: int
    events: int
    engagement_score: float
