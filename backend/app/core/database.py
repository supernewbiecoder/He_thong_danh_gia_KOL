from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row

from .config import settings


@contextmanager
def get_connection() -> Iterator[psycopg.Connection[Any]]:
    """Provide a short-lived read-only connection for one repository operation."""
    try:
        with psycopg.connect(settings.database_url, row_factory=dict_row) as connection:
            yield connection
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
