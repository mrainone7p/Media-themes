#!/usr/bin/env python3
"""Shared storage helpers for Media Tracks.

SQLite-only runtime. CSV import is one-shot migration only (first access per library).
Shared by both the web app (app.py) and worker (media_tracks.py).
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import re
import sqlite3
import subprocess
import threading
import time

_log = logging.getLogger("media-tracks.storage")
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple

import requests
import yaml

TMDB_GUID_RE = re.compile(r"(?:themoviedb://|tmdb://)(\d+)", re.IGNORECASE)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
LOGS_DIR = Path("/app/logs")
DB_DEFAULT_PATH = str(LOGS_DIR / "media_tracks.db")

LEDGER_HEADERS = [
    "title",
    "year",
    "status",
    "url",
    "start_offset",
    "golden_source_url",
    "golden_source_offset",
    "local_source_url",
    "local_source_offset",
    "local_source_origin",
    "local_source_method",
    "local_source_recorded_at",
    "end_offset",
    "plex_title",
    "folder",
    "rating_key",
    "tmdb_id",
    "last_updated",
    "notes",
    "source_origin",
    "theme_exists",
    "theme_duration",
    "theme_size",
    "theme_mtime",
]

_csv_lock = threading.Lock()
_db_lock = threading.Lock()

STATUS_ORDER = ("UNMONITORED", "MISSING", "STAGED", "APPROVED", "AVAILABLE", "FAILED")
VALID_STATUSES = frozenset(STATUS_ORDER)
NON_PERSISTED_STATUSES = frozenset({"REMOVED"})
LEGACY_STATUS_MAP = {
    "PENDING": "MISSING",
    "DOWNLOADED": "AVAILABLE",
    "IGNORED": "UNMONITORED",
    "REJECTED": "UNMONITORED",
    "NO_PLAYLIST": "MISSING",
}
MANUAL_STATUS_TRANSITIONS = {
    "UNMONITORED": frozenset({"MISSING"}),
    "MISSING": frozenset({"STAGED", "FAILED"}),
    "STAGED": frozenset({"APPROVED", "MISSING", "FAILED"}),
    "APPROVED": frozenset({"AVAILABLE", "MISSING", "FAILED"}),
    "AVAILABLE": frozenset({"MISSING"}),
    "FAILED": frozenset({"MISSING", "STAGED"}),
}


def normalize_status(status: str | None, default: str = "MISSING") -> str:
    normalized = str(status or "").strip().upper()
    return normalized or default


def migrate_legacy_status(status: str | None) -> tuple[str, str | None]:
    normalized = normalize_status(status)
    mapped = LEGACY_STATUS_MAP.get(normalized)
    if mapped:
        return mapped, f"[migrated from {normalized}]"
    return normalized, None


def valid_manual_targets(current_status: str | None) -> tuple[str, ...]:
    current = normalize_status(current_status)
    targets = set(MANUAL_STATUS_TRANSITIONS.get(current, frozenset()))
    targets.add("UNMONITORED")
    return tuple(status for status in STATUS_ORDER if status in targets)


def validate_manual_status_transition(
    current_status: str | None,
    attempted_status: str | None,
    *,
    has_url: bool = False,
    has_theme: bool = False,
) -> dict | None:
    attempted = normalize_status(attempted_status, default="")
    current = normalize_status(current_status, default="")
    if not attempted or attempted not in VALID_STATUSES:
        return {
            "error": "Invalid target status",
            "reason_code": "INVALID_STATUS",
            "current_status": current,
            "attempted_status": attempted,
        }
    if attempted == current:
        return None

    supported_targets = list(valid_manual_targets(current))
    if attempted == "UNMONITORED":
        return None
    if attempted == "STAGED" and not has_url:
        return {
            "error": "Cannot set status to STAGED without a source URL",
            "reason_code": "MISSING_URL",
            "current_status": current,
            "attempted_status": attempted,
            "supported_targets": supported_targets,
        }
    if attempted == "AVAILABLE" and not has_theme:
        return {
            "error": "Cannot set status to AVAILABLE when the local theme is missing",
            "reason_code": "MISSING_LOCAL_THEME",
            "current_status": current,
            "attempted_status": attempted,
            "supported_targets": supported_targets,
        }
    if attempted == "APPROVED" and current != "STAGED":
        return {
            "error": "Only STAGED items can be approved",
            "reason_code": "APPROVAL_REQUIRES_STAGED",
            "current_status": current,
            "attempted_status": attempted,
            "supported_targets": supported_targets,
        }

    allowed = MANUAL_STATUS_TRANSITIONS.get(current, frozenset())
    if attempted not in allowed:
        return {
            "error": (
                f"Unsupported manual status transition: {current} -> {attempted}. "
                f"Allowed manual targets from {current}: {', '.join(supported_targets)}"
            ),
            "reason_code": "INVALID_STATUS_TRANSITION",
            "current_status": current,
            "attempted_status": attempted,
            "supported_targets": supported_targets,
        }
    return None


def status_after_clearing_source(current_status: str | None, *, has_theme: bool = False) -> tuple[str, str]:
    current = normalize_status(current_status, default="")
    if has_theme:
        return "AVAILABLE", "preserved_available"
    if current == "UNMONITORED":
        return "UNMONITORED", "preserved_unmonitored"
    if current == "FAILED":
        return "FAILED", "preserved_failed"
    return "MISSING", "reset_missing"


def should_persist_status(status: str | None) -> bool:
    normalized = normalize_status(status, default="")
    return bool(normalized) and normalized not in NON_PERSISTED_STATUSES


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

now_str = _now_str  # public alias used by app.py and media_tracks.py


def load_config() -> dict:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def get_db_path() -> str:
    cfg = load_config()
    return str(cfg.get("db_path") or DB_DEFAULT_PATH)


def ledger_path_for(name: str, logs_dir: str | Path = LOGS_DIR) -> str:
    safe = re.sub(r"[^a-z0-9]+", "_", str(name).lower().strip()).strip("_") or "default"
    return str(Path(logs_dir) / f"tracks_{safe}.csv")


def normalize_golden_source_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    match = re.match(r"https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)", url)
    if match:
        owner, repo, branch, path = match.groups()
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    return url


def read_golden_source_text(
    url: str,
    *,
    cache_dir: str | Path,
    force_refresh: bool = False,
    cache_ttl_sec: int = 1800,
    allow_local_file: bool = False,
    cache_prefix: str = "",
) -> tuple[str, str, float, str]:
    """Return (normalized_url, csv_text, fetch_ms, fetch_mode)."""
    normalized = normalize_golden_source_url(url)
    if not normalized:
        raise ValueError("Golden Source URL is not configured")

    cache_root = Path(cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()[:16]
    cache_path = cache_root / f"{cache_prefix}{digest}.csv"

    if allow_local_file:
        local_path = Path(normalized)
        if local_path.exists() and local_path.is_file():
            return (
                normalized,
                local_path.read_text(encoding="utf-8-sig", errors="replace"),
                0.0,
                "local-file",
            )

    now = time.time()
    if not force_refresh and cache_path.exists():
        age = now - cache_path.stat().st_mtime
        if age <= max(0, int(cache_ttl_sec or 0)):
            return (
                normalized,
                cache_path.read_text(encoding="utf-8-sig", errors="replace"),
                0.0,
                "local-cache",
            )

    started_at = time.perf_counter()
    response = requests.get(normalized, timeout=20)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig", errors="replace")
    cache_path.write_text(text, encoding="utf-8")
    fetch_ms = round((time.perf_counter() - started_at) * 1000, 1)
    return normalized, text, fetch_ms, "remote-fetch"


def _slug_from_path(path: str) -> str:
    stem = Path(path).stem
    if stem.startswith("tracks_"):
        stem = stem[7:]
    return re.sub(r"[^a-z0-9]+", "_", stem.lower().strip()).strip("_") or "default"


def _name_from_slug(slug: str) -> str:
    return slug.replace("_", " ").title()


def _normalize_row(row: dict) -> dict:
    # Build output dict, converting None → "" for all string fields.
    # dict.get(key, default) returns None (not the default) when the value IS None,
    # so we must explicitly handle None to avoid null rating_key leaking to the frontend.
    out: dict = {}
    for k in LEDGER_HEADERS:
        v = row.get(k)
        out[k] = "" if v is None else v

    migrated_status, migration_note = migrate_legacy_status(out.get("status"))
    out["status"] = migrated_status
    if migration_note:
        notes = (out.get("notes", "") or "").strip()
        out["notes"] = f"{notes} {migration_note}".strip()

    # Guarantee rating_key is always a non-None string — critical for delete/patch lookups
    out["rating_key"] = str(out.get("rating_key") or "").strip()

    out["end_offset"]   = str(out.get("end_offset", "0") or "0")
    out["start_offset"] = str(out.get("start_offset", "0") or "0")
    out["golden_source_offset"] = str(out.get("golden_source_offset", "0") or "0")
    out["local_source_offset"] = str(out.get("local_source_offset", "0") or "0")
    out["tmdb_id"]      = str(out.get("tmdb_id", "") or "").strip()
    out["source_origin"] = str(out.get("source_origin", "") or "unknown").strip() or "unknown"
    out["local_source_url"] = str(out.get("local_source_url", "") or "").strip()
    out["local_source_origin"] = str(out.get("local_source_origin", "") or "").strip()
    out["local_source_method"] = str(out.get("local_source_method", "") or "").strip()
    out["local_source_recorded_at"] = str(out.get("local_source_recorded_at", "") or "").strip()
    out["folder"]       = str(out.get("folder", "") or "").strip()

    out["theme_exists"] = 1 if str(out.get("theme_exists", "0") or "0").strip() in {"1", "true", "True"} else 0
    try:
        out["theme_duration"] = float(out.get("theme_duration", 0) or 0)
    except Exception:
        out["theme_duration"] = 0.0
    try:
        out["theme_size"] = int(float(out.get("theme_size", 0) or 0))
    except Exception:
        out["theme_size"] = 0
    try:
        out["theme_mtime"] = float(out.get("theme_mtime", 0) or 0)
    except Exception:
        out["theme_mtime"] = 0.0

    if not out.get("last_updated"):
        out["last_updated"] = _now_str()
    return out


def _csv_load_rows(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    rows = []
    with open(p, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("rating_key"):
                normalized = _normalize_row(row)
                if normalized["status"] in NON_PERSISTED_STATUSES:
                    continue
                rows.append(normalized)
    rows.sort(key=lambda r: (str(r.get("title", "")).lower(), str(r.get("rating_key", ""))))
    return rows


def _connect() -> sqlite3.Connection:
    db_path = Path(get_db_path())
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    _init_db(conn)
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _init_db(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS libraries (
            slug TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_path TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            library_slug TEXT NOT NULL,
            rating_key TEXT NOT NULL,
            title TEXT,
            year TEXT,
            status TEXT,
            url TEXT,
            start_offset TEXT,
            golden_source_url TEXT,
            golden_source_offset TEXT DEFAULT '0',
            local_source_url TEXT,
            local_source_offset TEXT DEFAULT '0',
            local_source_origin TEXT,
            local_source_method TEXT,
            local_source_recorded_at TEXT,
            end_offset TEXT,
            plex_title TEXT,
            folder TEXT,
            tmdb_id TEXT,
            last_updated TEXT,
            notes TEXT,
            source_origin TEXT DEFAULT 'unknown',
            theme_exists INTEGER DEFAULT 0,
            theme_duration REAL DEFAULT 0,
            theme_size INTEGER DEFAULT 0,
            theme_mtime REAL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (library_slug, rating_key),
            FOREIGN KEY (library_slug) REFERENCES libraries(slug) ON DELETE CASCADE
        )
        """
    )
    cols = _column_names(conn, "items")
    wanted = {
        "theme_exists": "INTEGER DEFAULT 0",
        "theme_duration": "REAL DEFAULT 0",
        "theme_size": "INTEGER DEFAULT 0",
        "theme_mtime": "REAL DEFAULT 0",
        "tmdb_id": "TEXT",
        "source_origin": "TEXT DEFAULT 'unknown'",
        "golden_source_url": "TEXT",
        "golden_source_offset": "TEXT DEFAULT '0'",
        "local_source_url": "TEXT",
        "local_source_offset": "TEXT DEFAULT '0'",
        "local_source_origin": "TEXT",
        "local_source_method": "TEXT",
        "local_source_recorded_at": "TEXT",
    }
    for name, ddl in wanted.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE items ADD COLUMN {name} {ddl}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_library_status ON items(library_slug, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_folder ON items(folder)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_updated ON items(updated_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_tmdb ON items(tmdb_id)")
    conn.commit()


def _ensure_library(conn: sqlite3.Connection, slug: str, source_path: str | None = None):
    now = _now_str()
    name = _name_from_slug(slug)
    cur = conn.execute("SELECT slug FROM libraries WHERE slug=?", (slug,))
    if cur.fetchone() is None:
        conn.execute(
            "INSERT INTO libraries(slug,name,source_path,created_at,updated_at) VALUES(?,?,?,?,?)",
            (slug, name, source_path or "", now, now),
        )
    else:
        conn.execute(
            "UPDATE libraries SET source_path=COALESCE(NULLIF(?,''),source_path), updated_at=? WHERE slug=?",
            (source_path or "", now, slug),
        )


def _import_csv_if_needed(conn: sqlite3.Connection, path: str, slug: str):
    count = conn.execute("SELECT COUNT(*) FROM items WHERE library_slug=?", (slug,)).fetchone()[0]
    if count:
        return
    rows = _csv_load_rows(path)
    if not rows:
        return
    _log.warning(
        "DEPRECATED: Importing %d rows from legacy CSV file '%s' for library '%s'. "
        "This one-shot migration path will be removed in a future version. "
        "If you see this repeatedly, please report it at https://github.com/mrainone7p/Media-themes/issues",
        len(rows), path, slug,
    )
    _ensure_library(conn, slug, path)
    now = _now_str()
    for row in rows:
        row = _normalize_row(row)
        if not row["rating_key"]:
            continue  # skip rows without a key — they can't be primary-key'd
        conn.execute(
            """
            INSERT OR REPLACE INTO items(
                library_slug,rating_key,title,year,status,url,start_offset,golden_source_url,golden_source_offset,
                local_source_url,local_source_offset,local_source_origin,local_source_method,local_source_recorded_at,end_offset,
                plex_title,folder,tmdb_id,last_updated,notes,source_origin,theme_exists,theme_duration,
                theme_size,theme_mtime,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                slug, row["rating_key"], row["title"], row["year"], row["status"], row["url"],
                row["start_offset"], row["golden_source_url"], row["golden_source_offset"],
                row["local_source_url"], row["local_source_offset"], row["local_source_origin"],
                row["local_source_method"], row["local_source_recorded_at"],
                row["end_offset"], row["plex_title"], row["folder"],
                row["tmdb_id"], row["last_updated"], row["notes"], row.get("source_origin", "unknown"),
                int(row["theme_exists"]), float(row["theme_duration"]),
                int(row["theme_size"]), float(row["theme_mtime"]), now, now,
            ),
        )
    conn.commit()


def _sqlite_load_rows(path: str) -> list[dict]:
    slug = _slug_from_path(path)
    with _db_lock:
        conn = _connect()
        try:
            _ensure_library(conn, slug, path)
            _import_csv_if_needed(conn, path, slug)
            cur = conn.execute(
                """
                SELECT title,year,status,url,start_offset,golden_source_url,golden_source_offset,
                       local_source_url,local_source_offset,local_source_origin,local_source_method,local_source_recorded_at,end_offset,plex_title,folder,
                       rating_key,tmdb_id,last_updated,notes,source_origin,theme_exists,theme_duration,
                       theme_size,theme_mtime
                FROM items
                WHERE library_slug=?
                ORDER BY LOWER(COALESCE(title,'')), rating_key
                """,
                (slug,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            normalized_rows = []
            for row in rows:
                normalized = _normalize_row(row)
                if not should_persist_status(normalized["status"]):
                    continue
                normalized_rows.append(normalized)
            return normalized_rows
        finally:
            conn.close()


def _sqlite_save_rows(path: str, rows: Iterable[dict]):
    slug = _slug_from_path(path)
    # Only save rows that have a non-empty rating_key
    normalized_rows = []
    for row in rows:
        if not (row.get("rating_key") or "").strip():
            continue
        normalized = _normalize_row(row)
        if not should_persist_status(normalized["status"]):
            continue
        normalized_rows.append(normalized)
    rows = normalized_rows
    now = _now_str()
    with _db_lock:
        conn = _connect()
        try:
            _ensure_library(conn, slug, path)
            existing = {
                r[0] for r in conn.execute(
                    "SELECT rating_key FROM items WHERE library_slug=?", (slug,)
                ).fetchall()
            }
            incoming = {r["rating_key"] for r in rows}
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO items(
                        library_slug,rating_key,title,year,status,url,start_offset,golden_source_url,golden_source_offset,
                        local_source_url,local_source_offset,local_source_origin,local_source_method,local_source_recorded_at,end_offset,
                        plex_title,folder,tmdb_id,last_updated,notes,source_origin,theme_exists,theme_duration,
                        theme_size,theme_mtime,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(library_slug,rating_key) DO UPDATE SET
                        title=excluded.title,
                        year=excluded.year,
                        status=excluded.status,
                        url=excluded.url,
                        start_offset=excluded.start_offset,
                        golden_source_url=excluded.golden_source_url,
                        golden_source_offset=excluded.golden_source_offset,
                        local_source_url=excluded.local_source_url,
                        local_source_offset=excluded.local_source_offset,
                        local_source_origin=excluded.local_source_origin,
                        local_source_method=excluded.local_source_method,
                        local_source_recorded_at=excluded.local_source_recorded_at,
                        end_offset=excluded.end_offset,
                        plex_title=excluded.plex_title,
                        folder=excluded.folder,
                        tmdb_id=excluded.tmdb_id,
                        last_updated=excluded.last_updated,
                        notes=excluded.notes,
                        source_origin=excluded.source_origin,
                        theme_exists=excluded.theme_exists,
                        theme_duration=excluded.theme_duration,
                        theme_size=excluded.theme_size,
                        theme_mtime=excluded.theme_mtime,
                        updated_at=excluded.updated_at
                    """,
                    (
                        slug, row["rating_key"], row["title"], row["year"], row["status"], row["url"],
                        row["start_offset"], row["golden_source_url"], row["golden_source_offset"],
                        row["local_source_url"], row["local_source_offset"], row["local_source_origin"],
                        row["local_source_method"], row["local_source_recorded_at"], row["end_offset"], row["plex_title"], row["folder"],
                        row["tmdb_id"], row["last_updated"], row["notes"], row.get("source_origin", "unknown"),
                        int(row["theme_exists"]), float(row["theme_duration"]),
                        int(row["theme_size"]), float(row["theme_mtime"]), now, now,
                    ),
                )
            for stale in existing - incoming:
                conn.execute("DELETE FROM items WHERE library_slug=? AND rating_key=?", (slug, stale))
            conn.commit()
        finally:
            conn.close()


# ── Public API ────────────────────────────────────────────────────────────────

def load_ledger_rows(path: str) -> list[dict]:
    return _sqlite_load_rows(path)


def save_ledger_rows(path: str, rows: Iterable[dict]):
    return _sqlite_save_rows(path, rows)


# Aliases used by media_tracks.py worker
def load_ledger_map(path: str) -> dict[str, dict]:
    """Return rows as {rating_key: row_dict}."""
    rows = load_ledger_rows(path)
    return {r["rating_key"]: r for r in rows if r.get("rating_key")}


def save_ledger_map(path: str, row_map: dict[str, dict]):
    """Save from {rating_key: row_dict}."""
    save_ledger_rows(path, list(row_map.values()))


def clear_local_source_provenance(row: dict) -> dict:
    row["local_source_url"] = ""
    row["local_source_offset"] = "0"
    row["local_source_origin"] = ""
    row["local_source_method"] = ""
    row["local_source_recorded_at"] = ""
    return row


def stamp_local_source_provenance(
    row: dict,
    *,
    recorded_at: str | None = None,
    method: str = "",
) -> dict:
    timestamp = str(recorded_at or _now_str()).strip()
    row["local_source_url"] = str(row.get("url", "") or "").strip()
    row["local_source_offset"] = str(row.get("start_offset", "0") or "0")
    row["local_source_origin"] = str(row.get("source_origin", "") or "unknown").strip() or "unknown"
    row["local_source_method"] = str(method or "").strip()
    row["local_source_recorded_at"] = timestamp
    return row


# ── Theme cache helpers ────────────────────────────────────────────────────────

def ffprobe_duration(filepath: str | Path) -> float:
    try:
        r = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", str(filepath),
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        return float((r.stdout or "").strip() or 0)
    except Exception:
        return 0.0


def sync_theme_cache(row: dict, theme_filename: str, probe_duration: bool = False) -> Tuple[dict, bool]:
    """Update cached theme metadata on a row. Returns (updated_row, changed)."""
    row = _normalize_row(dict(row))
    folder = row.get("folder", "")
    path = Path(folder) / theme_filename if folder else None
    changed = False

    if not path or not path.exists():
        for k, v in {"theme_exists": 0, "theme_duration": 0.0, "theme_size": 0, "theme_mtime": 0.0}.items():
            if row.get(k) != v:
                row[k] = v
                changed = True
        had_local_provenance = any(
            str(row.get(key, "") or "").strip()
            for key in ("local_source_url", "local_source_origin", "local_source_method", "local_source_recorded_at")
        ) or str(row.get("local_source_offset", "0") or "0").strip() not in {"", "0"}
        if had_local_provenance:
            clear_local_source_provenance(row)
            changed = True
        return row, changed

    stat = path.stat()
    size = int(stat.st_size)
    mtime = float(stat.st_mtime)
    prev_size = int(row.get("theme_size", 0) or 0)
    prev_mtime = float(row.get("theme_mtime", 0) or 0)
    if int(row.get("theme_exists", 0) or 0) != 1:
        row["theme_exists"] = 1
        changed = True
    if prev_size != size:
        row["theme_size"] = size
        changed = True
    if abs(prev_mtime - mtime) > 1e-6:
        row["theme_mtime"] = mtime
        changed = True
    if probe_duration:
        cached_sig_matches = prev_size == size and abs(prev_mtime - mtime) <= 1e-6
        dur = float(row.get("theme_duration", 0) or 0)
        if dur <= 0 or not cached_sig_matches:
            new_dur = ffprobe_duration(path)
            if abs(float(row.get("theme_duration", 0) or 0) - new_dur) > 1e-6:
                row["theme_duration"] = new_dur
                changed = True
    return row, changed
