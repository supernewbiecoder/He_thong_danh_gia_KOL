"""Compatibility entrypoint; the application lives in ``backend.app.main``."""

from .app.main import app

__all__ = ["app"]
