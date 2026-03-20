"""Shared yt-dlp utilities for Media Tracks.

Used by both web/integrations.py and script/media_tracks.py to ensure
consistent yt-dlp flag construction across the worker and web layer.
"""

from __future__ import annotations

from pathlib import Path


def yt_dlp_base_flags(cookies_file: str | None = None, *, quiet: bool = True) -> list[str]:
    """Return the base yt-dlp command flags used by all invocations."""
    flags = ["yt-dlp", "--no-warnings"]
    if quiet:
        flags.append("--quiet")
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    return flags
