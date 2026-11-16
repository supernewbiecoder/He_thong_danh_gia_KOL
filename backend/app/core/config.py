from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://kol_readonly:change-me@127.0.0.1:5432/kol",
    )
    cors_allow_origins: tuple[str, ...] = tuple(
        origin.strip()
        for origin in os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:8501").split(",")
        if origin.strip()
    )
    api_prefix: str = "/api/v1"


settings = Settings()
