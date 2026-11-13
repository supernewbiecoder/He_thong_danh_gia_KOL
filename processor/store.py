"""Read the last saved totals of KOLs from PostgreSQL (kol.kol_scores, written by metrics-writer).

Module 3 keeps its state in RAM. Without this, a restart or a rebalance would start every KOL's
cumulative score from zero again. The store is optional: if PostgreSQL is unreachable the processor
keeps running and simply does not restore (it retries a little later).
"""

from __future__ import annotations

import sys
import time
from typing import Any

LOAD_SQL = """
SELECT kol_id, platform, handle, followers, total_events, event_breakdown, metrics_total,
       engagement_score, top_hashtags, last_event_at::text AS last_event_at
FROM kol.kol_scores
WHERE kol_id = ANY(%s)
"""


class ScoreStore:
    def __init__(self, dsn: str, retry_after_seconds: float = 30.0) -> None:
        self.dsn = dsn
        self.retry_after_seconds = retry_after_seconds
        self._conn: Any = None
        self._paused_until = 0.0

    def load(self, kol_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not kol_ids or time.monotonic() < self._paused_until:
            return {}
        try:
            import psycopg
            from psycopg.rows import dict_row

            if self._conn is None or self._conn.closed:
                self._conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5,
                                             row_factory=dict_row)
            rows = self._conn.execute(LOAD_SQL, (kol_ids,)).fetchall()
        except Exception as exc:  # noqa: BLE001 - restore is best effort, never stop the stream
            print(f"[processor] cannot restore KOL totals from PostgreSQL ({exc}); "
                  f"retrying in {self.retry_after_seconds:.0f}s", file=sys.stderr)
            self.close()
            self._paused_until = time.monotonic() + self.retry_after_seconds
            return {}
        return {row["kol_id"]: row for row in rows}

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
        self._conn = None
