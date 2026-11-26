from __future__ import annotations

from typing import Any

import psycopg


class DashboardRepository:
    """Read-only SQL queries for the dashboard's existing tables."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self.connection = connection

    def summary(self, platform: str | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        score = self.connection.execute(
            """
            SELECT COUNT(*) AS kols, COALESCE(SUM(total_events), 0) AS events,
                   COALESCE(SUM(engagement_score), 0) AS engagement_score,
                   COALESCE(SUM(window_events), 0) AS window_events,
                   COALESCE(SUM(velocity_per_min), 0) AS velocity_per_min
            FROM kol.kol_scores WHERE (%s IS NULL OR platform = %s)
            """, (platform, platform),
        ).fetchone()
        platforms = self.connection.execute(
            """
            SELECT platform, COUNT(*) AS kols, COALESCE(SUM(total_events), 0) AS events
            FROM kol.kol_scores WHERE (%s IS NULL OR platform = %s)
            GROUP BY platform ORDER BY platform
            """, (platform, platform),
        ).fetchall()
        return score, platforms

    def list_kols(
        self, platform: str | None, search: str, order: str, limit: int, offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        pattern = f"%{search.strip()}%"
        rows = self.connection.execute(
            f"""
            SELECT kol_id, platform, handle, followers, total_events, window_events, window_minutes,
                   event_breakdown, metrics_total, metrics_window, engagement_score, window_score,
                   velocity_per_min, top_hashtags, last_event_at, updated_at
            FROM kol.kol_scores
            WHERE (%s IS NULL OR platform = %s)
              AND (%s = '' OR kol_id ILIKE %s OR COALESCE(handle, '') ILIKE %s)
            ORDER BY {order} DESC NULLS LAST, kol_id LIMIT %s OFFSET %s
            """, (platform, platform, search.strip(), pattern, pattern, limit, offset),
        ).fetchall()
        total = self.connection.execute(
            """
            SELECT COUNT(*) AS total FROM kol.kol_scores
            WHERE (%s IS NULL OR platform = %s)
              AND (%s = '' OR kol_id ILIKE %s OR COALESCE(handle, '') ILIKE %s)
            """, (platform, platform, search.strip(), pattern, pattern),
        ).fetchone()["total"]
        return rows, int(total)

    def get_kol(self, kol_id: str) -> dict[str, Any] | None:
        return self.connection.execute(
            "SELECT * FROM kol.kol_scores WHERE kol_id = %s", (kol_id,)
        ).fetchone()

    def history(self, kol_id: str, days: int) -> list[dict[str, Any]]:
        return self.connection.execute(
            """
            SELECT bucket, engagement_score, window_score, velocity_per_min, total_events
            FROM kol.kol_score_history
            WHERE kol_id = %s AND bucket >= now() - (%s * interval '1 day')
            ORDER BY bucket
            """, (kol_id, days),
        ).fetchall()

    def keywords(self, search: str, limit: int) -> list[dict[str, Any]]:
        pattern = f"%{search.strip()}%"
        return self.connection.execute(
            """
            SELECT keyword, is_hashtag, count, total_count, window_minutes, velocity_per_min,
                   platforms, unique_kols, updated_at FROM kol.keyword_metrics
            WHERE (%s = '' OR keyword ILIKE %s) ORDER BY count DESC, keyword LIMIT %s
            """, (search.strip(), pattern, limit),
        ).fetchall()

    def platform_breakdown(self) -> list[dict[str, Any]]:
        return self.connection.execute(
            """
            SELECT platform, COUNT(*) AS kols, COALESCE(SUM(total_events), 0) AS events,
                   COALESCE(SUM(engagement_score), 0) AS engagement_score
            FROM kol.kol_scores GROUP BY platform ORDER BY platform
            """
        ).fetchall()
