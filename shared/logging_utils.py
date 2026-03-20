from __future__ import annotations

import logging
import os
import sys
from typing import Iterable

_PROJECT_LOGGER = "media_tracks"
_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _resolve_level(default: int = logging.INFO) -> int:
    raw = str(os.environ.get("MEDIA_TRACKS_LOG_LEVEL", "")).strip().upper()
    if not raw:
        return default
    return getattr(logging, raw, default)


def configure_project_logging(*, level: int | None = None) -> logging.Logger:
    logger = logging.getLogger(_PROJECT_LOGGER)
    if not getattr(logger, "_media_tracks_configured", False):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.propagate = False
        logger._media_tracks_configured = True  # type: ignore[attr-defined]
    logger.setLevel(_resolve_level(level or logging.INFO))
    return logger


def get_project_logger(name: str) -> logging.Logger:
    root = configure_project_logging()
    normalized = str(name or "").strip()
    if not normalized:
        return root
    if normalized == _PROJECT_LOGGER or normalized.startswith(f"{_PROJECT_LOGGER}."):
        return logging.getLogger(normalized)
    return logging.getLogger(f"{_PROJECT_LOGGER}.{normalized}")


def summarize_libraries(libraries: Iterable[str] | None) -> str:
    names = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    return ", ".join(names) if names else "(default selection)"
