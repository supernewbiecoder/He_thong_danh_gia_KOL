from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ....core.database import get_connection
from ....repositories.dashboard import DashboardRepository
from ....schemas.dashboard import (
    KOLHistoryResponse,
    KOLListResponse,
    KOLScore,
    KeywordListResponse,
    PlatformBreakdown,
    SummaryResponse,
)
from ....services.dashboard import serialize_rows

router = APIRouter()


def platform_value(platform: str) -> str | None:
    if platform not in ("all", "bluesky", "youtube"):
        raise HTTPException(status_code=400, detail="platform must be all, bluesky, or youtube")
    return None if platform == "all" else platform


def period_value(period: int) -> int:
    if period != 15:
        raise HTTPException(status_code=400, detail="only the existing 15-minute window is available")
    return period


@router.get("/summary", response_model=SummaryResponse)
def summary(period: int = Query(15), platform: str = Query("all")) -> SummaryResponse:
    period_value(period)
    with get_connection() as connection:
        score, platforms = DashboardRepository(connection).summary(platform_value(platform))
    return SummaryResponse(
        period_minutes=period, platform=platform, kols=int(score["kols"]),
        events=int(score["events"]), window_events=int(score["window_events"]),
        engagement_score=float(score["engagement_score"]),
        velocity_per_min=float(score["velocity_per_min"]), platforms=platforms,
    )


@router.get("/kols", response_model=KOLListResponse)
def kols(
    period: int = Query(15), platform: str = Query("all"), search: str = Query("", max_length=100),
    sort: str = Query("engagement_score"), limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0),
) -> KOLListResponse:
    period_value(period)
    sort_columns = {
        "engagement_score": "engagement_score", "window_score": "window_score",
        "velocity_per_min": "velocity_per_min", "total_events": "total_events", "followers": "followers",
    }
    order = sort_columns.get(sort)
    if order is None:
        raise HTTPException(status_code=400, detail=f"unsupported sort: {sort}")
    with get_connection() as connection:
        rows, total = DashboardRepository(connection).list_kols(
            platform_value(platform), search, order, limit, offset
        )
    return KOLListResponse(period_minutes=period, platform=platform, total=total, items=serialize_rows(rows))


@router.get("/kols/{kol_id}/history", response_model=KOLHistoryResponse)
def kol_history(kol_id: str, days: int = Query(7, ge=1, le=30)) -> KOLHistoryResponse:
    with get_connection() as connection:
        rows = DashboardRepository(connection).history(kol_id, days)
    return KOLHistoryResponse(kol_id=kol_id, days=days, items=serialize_rows(rows))


@router.get("/kols/{kol_id}", response_model=KOLScore)
def kol_detail(kol_id: str) -> KOLScore:
    with get_connection() as connection:
        row = DashboardRepository(connection).get_kol(kol_id)
    if row is None:
        raise HTTPException(status_code=404, detail="KOL not found")
    return KOLScore.model_validate(row)


@router.get("/keywords", response_model=KeywordListResponse)
def keywords(
    period: int = Query(15), limit: int = Query(20, ge=1, le=100), search: str = Query("", max_length=100),
) -> KeywordListResponse:
    period_value(period)
    with get_connection() as connection:
        rows = DashboardRepository(connection).keywords(search, limit)
    return KeywordListResponse(period_minutes=period, total=len(rows), items=serialize_rows(rows))


@router.get("/platform-breakdown", response_model=list[PlatformBreakdown])
def platform_breakdown() -> list[PlatformBreakdown]:
    with get_connection() as connection:
        rows = DashboardRepository(connection).platform_breakdown()
    return serialize_rows(rows)
