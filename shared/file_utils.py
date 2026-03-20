"""Shared file utilities for Media Tracks.

Used by both web/logic.py and script/media_tracks.py to ensure consistent
atomic file operations and audio validation across the worker and web layer.
"""

from __future__ import annotations

import secrets
import subprocess
from pathlib import Path

from storage import ffprobe_duration


def sibling_temp_path(target_path: str | Path, *, prefix: str) -> Path:
    target = Path(target_path)
    token = secrets.token_hex(6)
    return target.with_name(f"{prefix}{token}{target.suffix}")


def atomic_replace_file(prepared_path: str | Path, destination_path: str | Path) -> bool:
    """Atomically replace destination_path with prepared_path using a backup swap.

    Both files must be in the same directory. Returns True if an existing file
    was replaced, False if destination did not previously exist.
    """
    prepared = Path(prepared_path)
    destination = Path(destination_path)
    if prepared.parent != destination.parent:
        raise RuntimeError("Prepared file must be in the same folder as the destination")

    backup_path = None
    replaced_existing = destination.exists()
    if replaced_existing:
        backup_path = sibling_temp_path(destination, prefix=f"{destination.stem}.bak.")
        destination.replace(backup_path)

    try:
        prepared.replace(destination)
    except Exception:
        if backup_path and backup_path.exists():
            backup_path.replace(destination)
        raise
    else:
        if backup_path:
            backup_path.unlink(missing_ok=True)
    return replaced_existing


def validate_audio_file(filepath: str | Path) -> tuple[bool, str]:
    """Validate an audio file with strict checks (size, codec, duration).

    Returns (True, description) on success or (False, error_message) on failure.
    The file is deleted if validation fails to avoid leaving corrupt files on disk.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        return False, "File missing after download"
    size = filepath.stat().st_size
    if size < 10_000:
        filepath.unlink(missing_ok=True)
        return False, f"File too small ({size} bytes)"
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "a:0",
         "-show_entries", "stream=codec_type",
         "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 or "audio" not in result.stdout:
        filepath.unlink(missing_ok=True)
        return False, f"ffprobe rejected: {result.stderr.strip()[:120]}"
    dur = ffprobe_duration(filepath)
    return True, f"{size / 1024:.1f} KB, {dur:.1f}s"
