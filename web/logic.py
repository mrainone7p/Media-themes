#!/usr/bin/env python3
"""Business logic and orchestration for Media Tracks.

This module centralizes "what should happen" decisions so `web.app` can stay
focused on Flask wiring. The persistence layer remains in `shared.storage`, and
API/subprocess integrations live in `web.integrations`.
"""

from __future__ import annotations

import atexit
import csv
import json
import os
import queue
import re
import secrets
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable

import yaml

WEB_DIR = Path(__file__).resolve().parent
SHARED_DIR = WEB_DIR.parent / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

import integrations
from storage import (
    CONFIG_PATH,
    LEDGER_HEADERS,
    MANUAL_STATUS_TRANSITIONS,
    STATUS_ORDER,
    ffprobe_duration,
    get_db_path,
    ledger_path_for,
    load_ledger_rows as load_ledger,
    normalize_golden_source_url,
    now_str,
    read_golden_source_text,
    save_ledger_rows as save_ledger,
    status_after_clearing_source,
    sync_theme_cache,
    validate_manual_status_transition,
)

# ── Runtime paths and constants ──────────────────────────────────────────────

UI_TERMINOLOGY_PATH = os.environ.get("UI_TERMINOLOGY_PATH", "/app/web/ui_terminology.yaml")
LOGS_DIR = Path("/app/logs")
RUNS_DIR = LOGS_DIR / "runs"
TASKS_FILE = LOGS_DIR / "task_history.jsonl"
EXPORTS_DIR = LOGS_DIR / "exports"
SCRIPT_PATH = "/app/script/media_tracks.py"
GOLDEN_CACHE_DIR = LOGS_DIR / "golden_source_cache"
TEMPLATE_PATH = Path("/app/web/template.html")
_HEALTH_CACHE_TTL = 30
_health_cache: dict[str, object] = {"ts": 0.0, "key": None, "payload": None}
CRON_FILE_PATH = Path(os.environ.get("MEDIA_TRACKS_CRON_FILE", "/etc/cron.d/media-tracks"))
CRON_COMMAND = "python3 /app/script/media_tracks.py >> /proc/1/fd/1 2>> /proc/1/fd/2"
SCHEDULER_AUTHORITY = os.environ.get("MEDIA_TRACKS_SCHEDULER_AUTHORITY", "cron").strip().lower() or "cron"

for path in (RUNS_DIR, EXPORTS_DIR, GOLDEN_CACHE_DIR):
    path.mkdir(parents=True, exist_ok=True)

EDITABLE_LEDGER_FIELDS = set(LEDGER_HEADERS) - {"folder", "rating_key"}


def _sibling_temp_path(target_path: str | Path, *, prefix: str) -> Path:
    target = Path(target_path)
    token = secrets.token_hex(6)
    return target.with_name(f"{prefix}{token}{target.suffix}")


def _validate_audio_ready(audio_path: str | Path) -> float:
    path = Path(audio_path)
    if not path.exists():
        raise RuntimeError(f"Prepared audio file missing: {path.name}")
    duration = ffprobe_duration(path)
    if duration <= 0:
        raise RuntimeError(f"Prepared audio file is not valid audio: {path.name}")
    return duration


def _atomic_replace_theme_file(prepared_path: str | Path, theme_path: str | Path) -> bool:
    prepared = Path(prepared_path)
    destination = Path(theme_path)
    if prepared.parent != destination.parent:
        raise RuntimeError("Prepared audio must be in the same folder as the destination")

    backup_path = None
    replaced_existing = destination.exists()
    if replaced_existing:
        backup_path = _sibling_temp_path(destination, prefix=f"{destination.stem}.bak.")
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

CONFIG_DEFAULTS = {
    "plex_url": "",
    "plex_token": "",
    "tmdb_api_key": "",
    "ui_token": "",
    "media_roots": ["/media"],
    "libraries": [],
    "audio_format": "mp3",
    "quality_profile": "high",
    "theme_filename": "theme.mp3",
    "max_theme_duration": 45,
    "mode": "manual",
    "golden_source_url": "",
    "golden_source_cache_ttl_sec": 1800,
    "golden_source_resolve_tmdb": False,
    "cron_schedule": "0 3 * * *",
    "schedule_enabled": False,
    "schedule_libraries": [],
    "schedule_step1": True,
    "schedule_step2": True,
    "schedule_step3": True,
    "schedule_test_limit": 0,
    "auto_approve": False,
    "auto_approve_manual": False,
    "search_mode": "playlist",
    "search_query_playlist": "{title} {year} soundtrack playlist",
    "search_query_direct": "{title} {year} theme song",
    "search_fallback": True,
    "search_fuzzy": False,
    "search_only_golden": False,
    "refresh_golden_source_each_run": True,
    "cookies_file": "",
    "max_retries": 3,
    "download_delay_seconds": 5,
    "test_limit": 0,
    "resolve_retry_limit": 3,
}

CONFIG_BOOL_FIELDS = {
    "schedule_enabled",
    "schedule_step1",
    "schedule_step2",
    "schedule_step3",
    "auto_approve",
    "auto_approve_manual",
    "search_fallback",
    "search_fuzzy",
    "search_only_golden",
    "refresh_golden_source_each_run",
    "golden_source_resolve_tmdb",
}

CONFIG_ENUM_FIELDS = {
    "audio_format": {"mp3", "m4a", "flac", "opus"},
    "quality_profile": {"high", "balanced", "small", "smallest"},
    "search_mode": {"playlist", "direct"},
    "mode": {"manual", "auto", "cron"},
}

CONFIG_NUMERIC_FIELDS = {
    "max_theme_duration": {"type": "int", "min": 0, "max": 3600},
    "golden_source_cache_ttl_sec": {"type": "int", "min": 0, "max": 604800},
    "max_retries": {"type": "int", "min": 0, "max": 20},
    "download_delay_seconds": {"type": "float", "min": 0, "max": 3600},
    "test_limit": {"type": "int", "min": 0, "max": 100000},
    "schedule_test_limit": {"type": "int", "min": 0, "max": 100000},
    "resolve_retry_limit": {"type": "int", "min": 0, "max": 1000},
}

LIBRARY_TYPE_ALIASES = {
    "movie": "movie",
    "movies": "movie",
    "film": "movie",
    "films": "movie",
    "show": "show",
    "shows": "show",
    "tv": "show",
    "series": "show",
    "tvshow": "show",
    "tvshows": "show",
    "": "",
}

_CRON_FIELD_RE = re.compile(r"^[^\s]+$")
_FILENAME_INVALID_CHARS_RE = re.compile(r"[\\/]")
_CRON_ENTRY_RE = re.compile(r"^(?P<cron>\S+\s+\S+\s+\S+\s+\S+\s+\S+)\s+(?P<command>.+)$")


# ── Config and UI helpers ────────────────────────────────────────────────────

def config_error(field, code, message, value=None):
    error = {"field": field, "code": code, "message": message}
    if value is not None:
        error["value"] = value
    return error


def _coerce_config_bool(field, value, errors):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    errors.append(config_error(field, "invalid_boolean", "Expected a boolean value.", value))
    return None


def _coerce_config_number(field, value, spec, errors):
    if isinstance(value, bool):
        errors.append(config_error(field, "invalid_number", "Expected a numeric value.", value))
        return None
    raw = value.strip() if isinstance(value, str) else value
    if raw in ("", None):
        raw = 0 if spec["min"] == 0 else None
    try:
        number = int(raw) if spec["type"] == "int" else float(raw)
    except (TypeError, ValueError):
        errors.append(config_error(field, "invalid_number", "Expected a numeric value.", value))
        return None
    if number < spec["min"] or number > spec["max"]:
        errors.append(config_error(field, "out_of_range", f"Expected a value between {spec['min']} and {spec['max']}.", value))
        return None
    return int(number) if spec["type"] == "int" else number


def _normalize_config_enum(field, value, errors):
    normalized = str(value or "").strip().lower()
    if field == "quality_profile":
        normalized = {"best": "high", "hq": "high", "standard": "balanced", "medium": "balanced", "low": "small", "lq": "smallest"}.get(normalized, normalized)
    elif field == "search_mode":
        normalized = {"youtube": "playlist", "soundtrack": "playlist", "song": "direct", "theme": "direct"}.get(normalized, normalized)
    elif field == "mode":
        normalized = {"scheduled": "cron"}.get(normalized, normalized)
    if normalized in CONFIG_ENUM_FIELDS[field]:
        return normalized
    errors.append(config_error(field, "invalid_choice", f"Expected one of: {', '.join(sorted(CONFIG_ENUM_FIELDS[field]))}.", value))
    return None


def _normalize_media_roots(raw_cfg, errors):
    roots = raw_cfg.get("media_roots")
    if roots is None and raw_cfg.get("media_root") is not None:
        roots = [raw_cfg.get("media_root")]
    if roots is None:
        return list(CONFIG_DEFAULTS["media_roots"])
    if not isinstance(roots, list):
        errors.append(config_error("media_roots", "invalid_type", "Expected media_roots to be an array.", roots))
        return list(CONFIG_DEFAULTS["media_roots"])
    normalized = []
    for idx, root in enumerate(roots):
        text = str(root or "").strip()
        if not text:
            continue
        if text not in normalized:
            normalized.append(text)
        elif root not in ("", None):
            errors.append(config_error(f"media_roots[{idx}]", "duplicate", "Duplicate media root.", root))
    return normalized or list(CONFIG_DEFAULTS["media_roots"])


def _normalize_library_type(value, field, errors):
    normalized = LIBRARY_TYPE_ALIASES.get(str(value or "").strip().lower())
    if normalized is not None:
        return normalized
    errors.append(config_error(field, "invalid_choice", "Library type must be movie, show, or blank.", value))
    return ""


def _normalize_libraries(raw_cfg, errors):
    libraries = raw_cfg.get("libraries")
    if libraries is None:
        legacy_name = str(raw_cfg.get("plex_library_name", "") or "").strip()
        return [{"name": legacy_name, "enabled": True, "type": ""}] if legacy_name else []
    if not isinstance(libraries, list):
        errors.append(config_error("libraries", "invalid_type", "Expected libraries to be an array.", libraries))
        return []

    normalized = []
    seen = set()
    for idx, item in enumerate(libraries):
        if isinstance(item, str):
            item = {"name": item.strip(), "enabled": True}
        elif not isinstance(item, dict):
            errors.append(config_error(f"libraries[{idx}]", "invalid_type", "Each library must be an object.", item))
            continue
        name = str(item.get("name", "") or "").strip()
        if not name:
            errors.append(config_error(f"libraries[{idx}].name", "required", "Library name is required.", item.get("name")))
            continue
        enabled = _coerce_config_bool(f"libraries[{idx}].enabled", item.get("enabled", True), errors)
        if enabled is None:
            enabled = True
        lib_type = _normalize_library_type(item.get("type", ""), f"libraries[{idx}].type", errors)
        if name.casefold() in seen:
            errors.append(config_error(f"libraries[{idx}].name", "duplicate", "Duplicate library name.", name))
            continue
        seen.add(name.casefold())
        normalized.append({"name": name, "enabled": enabled, "type": lib_type})
    return normalized


def _normalize_schedule_libraries(value, normalized_cfg, errors):
    if value is None:
        return [lib["name"] for lib in normalized_cfg["libraries"] if lib.get("enabled", True)]
    if not isinstance(value, list):
        errors.append(config_error("schedule_libraries", "invalid_type", "Expected schedule_libraries to be an array.", value))
        return []
    available = {lib["name"] for lib in normalized_cfg["libraries"] if lib.get("enabled", True)}
    selected = []
    seen = set()
    for idx, item in enumerate(value):
        name = str(item or "").strip()
        if not name or name.casefold() in seen:
            continue
        seen.add(name.casefold())
        if name not in available:
            errors.append(config_error(f"schedule_libraries[{idx}]", "unknown_library", "Scheduled libraries must reference enabled configured libraries.", name))
            continue
        selected.append(name)
    return selected


def _normalize_cron_schedule(value, errors):
    cron = str(value or "").strip()
    if not cron:
        return CONFIG_DEFAULTS["cron_schedule"]
    parts = cron.split()
    if len(parts) != 5 or any(not _CRON_FIELD_RE.match(part) for part in parts):
        errors.append(config_error("cron_schedule", "invalid_cron", "Cron schedule must have five space-separated fields.", value))
        return CONFIG_DEFAULTS["cron_schedule"]
    return " ".join(parts)


def _normalize_theme_filename(value, errors):
    filename = str(value or "").strip() or CONFIG_DEFAULTS["theme_filename"]
    if _FILENAME_INVALID_CHARS_RE.search(filename):
        errors.append(config_error("theme_filename", "invalid_filename", "Theme filename must not include path separators.", value))
        return CONFIG_DEFAULTS["theme_filename"]
    return filename


def normalize_config(raw_cfg, *, for_save=False):
    source = raw_cfg if isinstance(raw_cfg, dict) else {}
    errors = []
    normalized = dict(CONFIG_DEFAULTS)

    for field, default in CONFIG_DEFAULTS.items():
        if field in {"media_roots", "libraries", "schedule_libraries", "cron_schedule", "theme_filename"}:
            continue
        if field in CONFIG_BOOL_FIELDS:
            coerced = _coerce_config_bool(field, source.get(field, default), errors)
            normalized[field] = default if coerced is None else coerced
        elif field in CONFIG_ENUM_FIELDS:
            coerced = _normalize_config_enum(field, source.get(field, default), errors)
            normalized[field] = default if coerced is None else coerced
        elif field in CONFIG_NUMERIC_FIELDS:
            coerced = _coerce_config_number(field, source.get(field, default), CONFIG_NUMERIC_FIELDS[field], errors)
            normalized[field] = default if coerced is None else coerced
        else:
            normalized[field] = str(source.get(field, default) or "").strip()

    if "search_query" in source and "search_query_playlist" not in source:
        normalized["search_query_playlist"] = str(source.get("search_query") or "").strip() or CONFIG_DEFAULTS["search_query_playlist"]

    normalized["golden_source_url"] = normalize_golden_source_url(normalized["golden_source_url"])
    normalized["media_roots"] = _normalize_media_roots(source, errors)
    normalized["libraries"] = _normalize_libraries(source, errors)
    normalized["schedule_libraries"] = _normalize_schedule_libraries(source.get("schedule_libraries"), normalized, errors)
    normalized["cron_schedule"] = _normalize_cron_schedule(source.get("cron_schedule", CONFIG_DEFAULTS["cron_schedule"]), errors)
    normalized["theme_filename"] = _normalize_theme_filename(source.get("theme_filename", CONFIG_DEFAULTS["theme_filename"]), errors)

    if normalized["search_fuzzy"]:
        normalized["search_fallback"] = False
    return normalized, errors if for_save else []


def load_raw_config() -> dict:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except Exception:
        return {}


def load_config() -> dict:
    cfg, _ = normalize_config(load_raw_config(), for_save=False)
    return cfg


def save_config(data: dict) -> dict:
    global _health_cache
    normalized, errors = normalize_config(data, for_save=True)
    if errors:
        raise ValueError(errors)
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        yaml.dump(normalized, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    _health_cache = {"ts": 0.0, "key": None, "payload": None}
    return normalized


def scheduler_managed_via_cron() -> bool:
    return SCHEDULER_AUTHORITY == "cron"


def _render_cron_file(schedule_enabled: bool, cron_schedule: str) -> str:
    lines = [
        "# Managed by Media Tracks. Manual edits will be overwritten.",
        "SHELL=/bin/sh",
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    ]
    if schedule_enabled:
        lines.append(f"{cron_schedule} {CRON_COMMAND}")
    else:
        lines.append("# Scheduler disabled in config.yaml")
    return "\n".join(lines) + "\n"


def _extract_cron_schedule_from_text(text: str) -> str | None:
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _CRON_ENTRY_RE.match(line)
        if not match:
            continue
        command = match.group("command").strip()
        if command == CRON_COMMAND:
            return " ".join(match.group("cron").split())
    return None


def active_scheduler_source() -> dict:
    configured_cron = (load_config().get("cron_schedule") or CONFIG_DEFAULTS["cron_schedule"]).strip()
    details = {
        "authority": SCHEDULER_AUTHORITY,
        "configured_cron": configured_cron,
        "cron_file": str(CRON_FILE_PATH),
        "active_cron": None,
        "schedule_enabled": False,
        "detail": "",
        "error": None,
    }
    if not scheduler_managed_via_cron():
        details["detail"] = "Schedule is managed inside the Flask process."
        return details
    if not CRON_FILE_PATH.exists():
        details["detail"] = f"Cron file missing at {CRON_FILE_PATH}."
        return details
    try:
        cron_text = CRON_FILE_PATH.read_text(encoding="utf-8")
    except Exception as exc:
        details["detail"] = f"Unable to read cron file at {CRON_FILE_PATH}."
        details["error"] = str(exc)
        return details
    active_cron = _extract_cron_schedule_from_text(cron_text)
    details["active_cron"] = active_cron
    details["schedule_enabled"] = bool(active_cron)
    if active_cron:
        details["detail"] = f"Active schedule comes from {CRON_FILE_PATH}."
    else:
        details["detail"] = f"No active Media Tracks cron entry is installed in {CRON_FILE_PATH}."
    return details


def refresh_scheduler(config: dict | None = None) -> dict:
    global _health_cache
    cfg = config if isinstance(config, dict) else load_config()
    cron_schedule = _normalize_cron_schedule(cfg.get("cron_schedule", CONFIG_DEFAULTS["cron_schedule"]), [])
    schedule_enabled = bool(cfg.get("schedule_enabled", False))
    details = {
        "ok": True,
        "authority": SCHEDULER_AUTHORITY,
        "configured_cron": cron_schedule,
        "schedule_enabled": schedule_enabled,
        "cron_file": str(CRON_FILE_PATH),
        "active_cron": None,
        "detail": "",
        "error": None,
    }
    if not scheduler_managed_via_cron():
        details["detail"] = "Scheduler is managed inside the Flask process; no cron refresh needed."
        _health_cache = {"ts": 0.0, "key": None, "payload": None}
        return details
    contents = _render_cron_file(schedule_enabled, cron_schedule)
    try:
        CRON_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(CRON_FILE_PATH.parent), delete=False) as tmp:
            tmp.write(contents)
            temp_path = Path(tmp.name)
        os.chmod(temp_path, 0o644)
        os.replace(temp_path, CRON_FILE_PATH)
        completed = subprocess.run(["crontab", str(CRON_FILE_PATH)], check=True, capture_output=True, text=True)
        active = active_scheduler_source()
        details["active_cron"] = active.get("active_cron")
        details["schedule_enabled"] = bool(active.get("schedule_enabled"))
        details["detail"] = active.get("detail") or ("Cron schedule reloaded." if schedule_enabled else "Cron schedule cleared.")
        stderr = (completed.stderr or "").strip()
        if stderr:
            details["detail"] = f"{details['detail']} ({stderr})"
        _health_cache = {"ts": 0.0, "key": None, "payload": None}
        return details
    except subprocess.CalledProcessError as exc:
        details["ok"] = False
        details["error"] = (exc.stderr or exc.stdout or str(exc)).strip() or str(exc)
        details["detail"] = "Failed to reload cron schedule."
    except Exception as exc:
        details["ok"] = False
        details["error"] = str(exc)
        details["detail"] = "Failed to rewrite cron schedule."
    finally:
        temp_candidate = locals().get("temp_path")
        if isinstance(temp_candidate, Path) and temp_candidate.exists():
            try:
                temp_candidate.unlink()
            except OSError:
                pass
        _health_cache = {"ts": 0.0, "key": None, "payload": None}
    return details


def get_ui_token() -> str:
    env_token = os.environ.get("UI_TOKEN", "").strip()
    return env_token or str(load_config().get("ui_token", "") or "").strip()


def is_authorized_api_request(path: str, headers: dict, args: dict) -> bool:
    if not path.startswith("/api/"):
        return True
    token = get_ui_token()
    if not token:
        return True
    provided = headers.get("X-UI-Token") or args.get("token", "")
    return provided == token


_template_cache: str | None = None

def load_template() -> str:
    global _template_cache
    if _template_cache is not None:
        return _template_cache
    if TEMPLATE_PATH.exists():
        _template_cache = TEMPLATE_PATH.read_text(encoding="utf-8")
    elif (WEB_DIR / "template.html").exists():
        _template_cache = (WEB_DIR / "template.html").read_text(encoding="utf-8")
    else:
        return "<h1>Template not found</h1>"
    return _template_cache


def load_ui_terminology() -> dict:
    try:
        with open(UI_TERMINOLOGY_PATH, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except Exception:
        local = WEB_DIR / "ui_terminology.yaml"
        if local.exists():
            try:
                return yaml.safe_load(local.read_text(encoding="utf-8")) or {}
            except Exception:
                return {}
    return {}


def get_libraries(cfg: dict) -> list[dict]:
    libraries = cfg.get("libraries")
    if libraries and isinstance(libraries, list):
        return libraries
    return [{"name": cfg.get("plex_library_name", "Movies"), "enabled": True}]


def boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


# ── Ledger and row mutation helpers ──────────────────────────────────────────

def row_has_theme(row: dict) -> bool:
    return str(row.get("theme_exists", "") or "") == "1"


def status_validation_error(
    row: dict,
    attempted_status: str,
    *,
    current_status: str | None = None,
    has_url: bool | None = None,
    has_theme: bool | None = None,
):
    return validate_manual_status_transition(
        current_status if current_status is not None else row.get("status", ""),
        attempted_status,
        has_url=bool(str(row.get("url", "") or "").strip()) if has_url is None else has_url,
        has_theme=row_has_theme(row) if has_theme is None else has_theme,
    )


def ledger_row_response(row: dict) -> dict:
    return {header: str(row.get(header, "") or "") for header in LEDGER_HEADERS}


def save_ledger_row_updates(row: dict, updates: dict, *, default_notes: str | None = None):
    attempted_status = None
    original_status = str(row.get("status", "") or "")
    candidate_row = dict(row)
    for key, value in updates.items():
        if key not in EDITABLE_LEDGER_FIELDS:
            continue
        normalized = str(value or "")
        if key == "status":
            attempted_status = normalized.upper()
            continue
        candidate_row[key] = normalized

    candidate_has_url = bool(str(candidate_row.get("url", "") or "").strip())
    candidate_has_theme = row_has_theme(candidate_row)

    if attempted_status:
        error = status_validation_error(
            candidate_row,
            attempted_status,
            current_status=original_status,
            has_url=candidate_has_url,
            has_theme=candidate_has_theme,
        )
        if error:
            error["rating_key"] = str(row.get("rating_key", "") or "")
            error["title"] = row.get("title") or row.get("plex_title") or ""
            return None, error

    for key, value in updates.items():
        if key not in EDITABLE_LEDGER_FIELDS:
            continue
        normalized = str(value or "")
        row[key] = normalized
    if attempted_status:
        row["status"] = attempted_status

    if "url" in updates:
        row["source_origin"] = "manual" if str(updates.get("url") or "").strip() else "unknown"
    row["last_updated"] = now_str()
    if "notes" not in updates and default_notes is not None:
        row["notes"] = default_notes
    return row, None


def clear_source_urls_for_rows(rows: list[dict], *, keys=None, note: str, now: str):
    key_filter = set(str(key) for key in (keys or []))
    summary = {
        "requested": len(key_filter) if keys is not None else len(rows),
        "matched": 0,
        "cleared": 0,
        "updated": 0,
        "preserved_available": 0,
        "reset_missing": 0,
        "preserved_failed": 0,
        "preserved_unmonitored": 0,
        "skipped_without_url": 0,
    }
    for row in rows:
        rating_key = str(row.get("rating_key", "") or "")
        if key_filter and rating_key not in key_filter:
            continue
        summary["matched"] += 1
        had_url = bool(str(row.get("url", "") or "").strip())
        if not had_url:
            summary["skipped_without_url"] += 1
            continue
        next_status, bucket = status_after_clearing_source(row.get("status", ""), has_theme=row_has_theme(row))
        previous_status = str(row.get("status", "") or "").upper()
        row["url"] = ""
        row["source_origin"] = "unknown"
        row["status"] = next_status
        row["last_updated"] = now
        row["notes"] = note
        summary["cleared"] += 1
        summary[bucket] += 1
        if previous_status != next_status:
            summary["updated"] += 1
    return summary


def find_row_by_identity(rows: list[dict], rating_key: str = "", folder: str = "", tmdb_id: str = ""):
    rating_key = str(rating_key or "").strip()
    folder = str(folder or "").strip()
    tmdb_id = str(tmdb_id or "").strip()
    if rating_key:
        row = next((row for row in rows if str(row.get("rating_key", "") or "").strip() == rating_key), None)
        if row:
            return row, "rating_key"
    if folder:
        row = next((row for row in rows if str(row.get("folder", "") or "").strip() == folder), None)
        if row:
            return row, "folder"
    if tmdb_id:
        row = next((row for row in rows if str(row.get("tmdb_id", "") or "").strip() == tmdb_id), None)
        if row:
            return row, "tmdb_id"
    return None, ""


def get_media_roots(cfg: dict) -> list[str]:
    roots = cfg.get("media_roots")
    if isinstance(roots, list) and roots:
        return [str(root) for root in roots if str(root).strip()]
    single = cfg.get("media_root")
    return [str(single)] if single else ["/media"]


def is_allowed_folder(folder: str, roots: list[str]) -> bool:
    if not folder:
        return False
    try:
        folder_path = Path(folder).resolve()
    except Exception:
        return False
    for root in roots:
        try:
            root_path = Path(root).resolve()
            if folder_path == root_path or folder_path.is_relative_to(root_path):
                return True
        except Exception:
            continue
    return False


def library_type_for_name(cfg: dict, library_name: str) -> str:
    for library in cfg.get("libraries", []):
        if str(library.get("name", "")).strip() == str(library_name or "").strip():
            return str(library.get("type", "") or "").strip()
    return ""


def theme_target_folder(row: dict, *, library_type: str = "") -> str:
    """Return the destination folder for a theme file.

    Movies keep the theme in the media folder. Shows currently also use the show
    base folder recorded in the ledger, which keeps the current workflow intact
    while making the media-type decision explicit for future TV improvements.
    """
    _ = library_type or "movie"
    return str(row.get("folder", "") or "")


def theme_file_path(row: dict, cfg: dict, *, library_type: str = "") -> Path:
    return Path(theme_target_folder(row, library_type=library_type)) / cfg.get("theme_filename", "theme.mp3")


# ── Golden Source helpers ────────────────────────────────────────────────────

def import_csv_reader(text: str):
    return csv.DictReader(text.splitlines())


def parse_golden_source_csv(text: str) -> list[dict]:
    reader = import_csv_reader(text)
    if not reader.fieldnames:
        raise ValueError("Golden Source CSV has no header row")
    rows = []
    for row in reader:
        clean = {str(key or "").strip().lower(): str(value or "").strip() for key, value in row.items()}
        tmdb_id = clean.get("tmdb_id", "")
        if not tmdb_id:
            continue
        clean["start_offset"] = clean.get("start_offset", "0") or "0"
        clean["end_offset"] = clean.get("end_offset", "0") or "0"
        clean.pop("verified", None)
        rows.append(clean)
    return rows


def fetch_golden_source_catalog(url: str, *, force_refresh: bool = False, cache_ttl_sec: int = 1800):
    normalized, text, fetch_ms, fetch_mode = read_golden_source_text(
        url,
        cache_dir=GOLDEN_CACHE_DIR,
        force_refresh=force_refresh,
        cache_ttl_sec=cache_ttl_sec,
        allow_local_file=True,
        cache_prefix="catalog_",
    )
    return normalized, parse_golden_source_csv(text), fetch_ms, fetch_mode


def resolve_row_tmdb_id(row: dict, cfg: dict) -> str:
    tmdb_id = str(row.get("tmdb_id", "") or "").strip()
    if tmdb_id:
        return tmdb_id
    tmdb_key = cfg.get("tmdb_api_key", "")
    if not tmdb_key:
        return ""
    data = integrations.tmdb_lookup(row.get("title", "") or row.get("plex_title", ""), row.get("year", ""), tmdb_key)
    if not data or str(data.get("media_type", "")) != "movie":
        return ""
    return str(data.get("id", "") or "").strip()


def golden_source_import_summary(data: dict):
    library = data.get("library", "")
    if not library:
        return {"ok": False, "error": "Missing library"}, 400
    cfg = load_config()
    source_url = data.get("url") or cfg.get("golden_source_url", "")
    overwrite = boolish(data.get("overwrite_existing", False))
    auto_approve = boolish(data.get("auto_approve", False))
    force_refresh = boolish(data.get("force_refresh", cfg.get("refresh_golden_source_each_run", True)))
    cache_ttl_sec = int(cfg.get("golden_source_cache_ttl_sec", 1800) or 1800)
    resolve_missing_tmdb = boolish(data.get("resolve_missing_tmdb", cfg.get("golden_source_resolve_tmdb", False)))
    started = time.perf_counter()

    try:
        normalized_url, catalog_rows, fetch_ms, fetch_mode = fetch_golden_source_catalog(
            source_url,
            force_refresh=force_refresh,
            cache_ttl_sec=cache_ttl_sec,
        )
    except Exception as exc:
        return {"ok": False, "error": f"Golden Source fetch failed: {str(exc)[:180]}"}, 400

    catalog = {str(row.get("tmdb_id", "")).strip(): row for row in catalog_rows if str(row.get("tmdb_id", "")).strip()}
    if not catalog:
        return {"ok": False, "error": "Golden Source CSV had no usable rows"}, 400

    def norm_title(value):
        value = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
        return re.sub(r"\s+", " ", value).strip()

    catalog_title_year = {}
    for row in catalog_rows:
        title_key = norm_title(row.get("title", ""))
        year_key = str(row.get("year", "") or "").strip()
        if title_key and year_key:
            catalog_title_year[f"{title_key}|{year_key}"] = row

    path = ledger_path_for(library)
    rows = load_ledger(path)
    now = now_str()
    imported = skipped_existing = missing_tmdb = no_match = 0
    tmdb_cache = {}

    for row in rows:
        current_status = str(row.get("status", "") or "").upper()
        match = None
        tmdb_id = str(row.get("tmdb_id", "") or "").strip()
        if tmdb_id:
            match = catalog.get(tmdb_id)
        if not match and catalog_title_year:
            title_key = norm_title(row.get("title", "") or row.get("plex_title", ""))
            year_key = str(row.get("year", "") or "").strip()
            if title_key and year_key:
                match = catalog_title_year.get(f"{title_key}|{year_key}")
                if match and not tmdb_id:
                    tmdb_id = str(match.get("tmdb_id", "") or "").strip()
        if not match and resolve_missing_tmdb:
            cache_key = str(row.get("rating_key", "") or "")
            cached = tmdb_cache.get(cache_key)
            if cached is None:
                cached = resolve_row_tmdb_id(row, cfg)
                tmdb_cache[cache_key] = cached
            tmdb_id = tmdb_id or cached
            if tmdb_id:
                match = catalog.get(tmdb_id)
        if not match:
            if tmdb_id:
                no_match += 1
            else:
                missing_tmdb += 1
            continue

        existing_url = str(row.get("url", "") or "").strip()
        if existing_url and not overwrite:
            skipped_existing += 1
            if tmdb_id:
                row["tmdb_id"] = tmdb_id
            continue

        incoming_url = str(match.get("source_url", "") or "").strip()
        row["tmdb_id"] = tmdb_id or str(match.get("tmdb_id", "") or "").strip()
        row["url"] = incoming_url
        row["golden_source_url"] = incoming_url
        row["golden_source_offset"] = match.get("start_offset", "0") or "0"
        row["end_offset"] = match.get("end_offset", "0") or "0"
        row["source_origin"] = "golden_source" if incoming_url else "unknown"
        if current_status == "UNMONITORED":
            pass
        elif not incoming_url and current_status != "AVAILABLE":
            row["status"] = "MISSING"
        elif current_status != "AVAILABLE":
            row["status"] = "APPROVED" if auto_approve else "STAGED"
        row["last_updated"] = now
        row["notes"] = (
            f"Imported from Golden Source ({Path(normalized_url).name})"
            if incoming_url else f"Golden Source cleared source URL ({Path(normalized_url).name})"
        )
        imported += 1

    save_ledger(path, rows)
    return {
        "ok": True,
        "source_url": normalized_url,
        "catalog_rows": len(catalog_rows),
        "matched": imported + skipped_existing,
        "imported": imported,
        "skipped_existing": skipped_existing,
        "missing_tmdb": missing_tmdb,
        "no_match": no_match,
        "fetch_ms": fetch_ms,
        "fetch_mode": fetch_mode,
        "cache_ttl_sec": cache_ttl_sec,
        "resolve_missing_tmdb": resolve_missing_tmdb,
        "total_ms": round((time.perf_counter() - started) * 1000, 1),
    }, 200


# ── Theme / media operations ─────────────────────────────────────────────────

def media_payload(library: str, show: str, *, nocache: bool = False):
    cfg = load_config()
    filename = cfg.get("theme_filename", "theme.mp3")
    path = ledger_path_for(library) if library else str(LOGS_DIR / "theme_log.csv")
    rows = load_ledger(path)
    media = []
    for row in rows:
        has_theme = int(row.get("theme_exists", 0) or 0) == 1
        duration = float(row.get("theme_duration", 0) or 0)
        if show == "with_theme" and not has_theme:
            continue
        if show == "without_theme" and has_theme:
            continue
        media.append({
            "rating_key": row.get("rating_key", ""),
            "title": row.get("title", ""),
            "plex_title": row.get("plex_title", ""),
            "year": row.get("year", ""),
            "folder": row.get("folder", ""),
            "url": row.get("url", ""),
            "start_offset": row.get("start_offset", "0"),
            "end_offset": row.get("end_offset", "0"),
            "duration": duration,
            "has_theme": has_theme,
            "status": row.get("status", ""),
            "last_updated": row.get("last_updated", ""),
        })
    media.sort(key=lambda row: row.get("title", "").lower())
    return media


def movie_bio_payload(rating_key: str, library: str) -> dict:
    if not rating_key:
        return {"summary": ""}
    cfg = load_config()
    tmdb_key = cfg.get("tmdb_api_key", "")
    plex_url = cfg.get("plex_url", "").rstrip("/")
    plex_token = cfg.get("plex_token", "")
    cache_key = f"{library}:{rating_key}"

    if tmdb_key:
        try:
            path = ledger_path_for(library) if library else str(LOGS_DIR / "theme_log.csv")
            rows = load_ledger(path)
            row = next((row for row in rows if row.get("rating_key") == rating_key), None)
            title = (row or {}).get("title") or (row or {}).get("plex_title", "")
            year = (row or {}).get("year", "")
            if title:
                summary = integrations.fetch_tmdb_overview(cache_key, title, year, tmdb_key)
                if summary:
                    return {"summary": summary}
        except Exception:
            pass

    if plex_url and plex_token:
        try:
            summary = integrations.fetch_plex_summary(rating_key, plex_url, plex_token)
            if summary:
                return {"summary": summary}
        except Exception:
            pass
    return {"summary": ""}


def theme_info_payload(folder: str):
    cfg = load_config()
    roots = get_media_roots(cfg)
    if not is_allowed_folder(folder, roots):
        return {"error": "forbidden"}, 403
    path = Path(folder) / cfg.get("theme_filename", "theme.mp3")
    if not path.exists():
        return {"error": "not found"}, 404
    size = path.stat().st_size
    return {
        "duration": ffprobe_duration(path),
        "size": size,
        "size_kb": round(size / 1024, 1),
        "folder": folder,
        "filename": cfg.get("theme_filename", "theme.mp3"),
        "path": str(path),
    }, 200


def trim_theme_payload(data: dict):
    library = data.get("library", "")
    rating_key = data.get("rating_key", "")
    try:
        start_offset = int(data.get("start_offset", 0))
        end_offset = int(data.get("end_offset", 0))
    except (ValueError, TypeError):
        return {"ok": False, "error": "start_offset and end_offset must be numbers"}, 400
    cfg = load_config()
    roots = get_media_roots(cfg)
    filename = cfg.get("theme_filename", "theme.mp3")
    audio_format = cfg.get("audio_format", "mp3")
    max_duration = int(cfg.get("max_theme_duration", 0))
    path = ledger_path_for(library) if library else str(LOGS_DIR / "theme_log.csv")
    rows = load_ledger(path)
    row = next((row for row in rows if str(row.get("rating_key", "")) == str(rating_key)), None)
    if not row:
        return {"ok": False, "error": f"Not found in ledger — key={rating_key}"}, 404
    folder = row.get("folder", "")
    if not is_allowed_folder(folder, roots):
        return {"ok": False, "error": "Folder not allowed"}, 403
    theme_path = Path(folder) / filename
    if not theme_path.exists():
        return {"ok": False, "error": f"Theme file not on disk: {theme_path}"}, 404
    try:
        duration = ffprobe_duration(theme_path)
        if duration <= 0:
            return {"ok": False, "error": "Could not read audio duration"}, 400
        start = max(0, start_offset)
        end = duration - max(0, end_offset) if end_offset > 0 else duration
        if max_duration > 0 and (end - start) > max_duration:
            end = start + max_duration
        if start >= duration:
            return {"ok": False, "error": f"Start offset ({start_offset}s) exceeds file duration ({duration:.1f}s)"}, 400
        if end <= start:
            return {"ok": False, "error": "Nothing left after trimming"}, 400
        if start <= 0 and end >= duration:
            row["start_offset"] = str(start_offset)
            row["end_offset"] = str(end_offset)
            row["last_updated"] = now_str()
            row["notes"] = f"No trim needed ({duration:.1f}s)"
            save_ledger(path, rows)
            return {"ok": True, "message": f"No trim needed — {duration:.1f}s", "duration": duration}, 200
        tmp = _sibling_temp_path(theme_path, prefix=f"{theme_path.stem}.trim.")
        integrations.trim_audio_copy(theme_path, tmp, start=start, end=end, timeout=60)
        new_duration = _validate_audio_ready(tmp)
        _atomic_replace_theme_file(tmp, theme_path)
        row["start_offset"] = str(start_offset)
        row["end_offset"] = str(end_offset)
        row["last_updated"] = now_str()
        row["notes"] = f"Trimmed: {duration:.1f}s → {new_duration:.1f}s"
        row, _ = sync_theme_cache(row, filename, probe_duration=True)
        save_ledger(path, rows)
        return {"ok": True, "message": f"Trimmed {duration:.1f}s → {new_duration:.1f}s", "duration": new_duration}, 200
    except Exception as exc:
        if 'tmp' in locals():
            Path(tmp).unlink(missing_ok=True)
        return {"ok": False, "error": str(exc)[:200]}, 500


def delete_theme_payload(data: dict):
    library = (data.get("library", "") or "").strip()
    rating_key = str(data.get("rating_key", "") or "").strip()
    folder_hint = str(data.get("folder", "") or "").strip()
    tmdb_id = str(data.get("tmdb_id", "") or "").strip()

    cfg = load_config()
    filename = cfg.get("theme_filename", "theme.mp3")
    roots = get_media_roots(cfg)
    if filename not in {"theme.mp3", "theme.m4a", "theme.flac", "theme.opus"}:
        return {"ok": False, "error": f"Unexpected theme filename: {filename}"}, 400

    path = ledger_path_for(library) if library else str(LOGS_DIR / "theme_log.csv")
    rows = load_ledger(path)
    row, matched_by = find_row_by_identity(rows, rating_key, folder_hint, tmdb_id)
    if not row:
        for lib_entry in load_config().get("libraries", []):
            lib_name = lib_entry.get("name", "")
            if not lib_name or lib_name == library:
                continue
            alt_path = ledger_path_for(lib_name)
            alt_rows = load_ledger(alt_path)
            row, matched_by = find_row_by_identity(alt_rows, rating_key, folder_hint, tmdb_id)
            if row:
                path = alt_path
                rows = alt_rows
                break

    folder = (row.get("folder", "") if row else None) or folder_hint
    if not folder:
        detail = f"key={rating_key!r}, lib={library!r}, folder_hint={folder_hint!r}, ledger_size={len(rows)}"
        return {"ok": False, "error": f"Media item not found in ledger and no folder provided. ({detail})"}, 404
    if not is_allowed_folder(folder, roots):
        return {
            "ok": False,
            "error": (
                f"Folder is not inside an allowed media root. folder={folder!r} — roots={roots}. "
                f"Check that media_roots in config.yaml matches the path Plex uses for this library inside the container."
            ),
        }, 403

    theme_path = Path(folder) / filename
    if theme_path.name != filename:
        return {"ok": False, "error": "Path mismatch — refusing to delete"}, 400

    def clear_theme_metadata(item):
        item["status"] = "MISSING"
        item["theme_exists"] = 0
        item["theme_duration"] = 0.0
        item["theme_size"] = 0
        item["theme_mtime"] = 0.0
        item["last_updated"] = now_str()

    if not theme_path.exists():
        if row:
            clear_theme_metadata(row)
            row["notes"] = "Theme file already missing — status reset"
            save_ledger(path, rows)
        return {"ok": True, "message": "File already gone — status reset to Missing", "matched_by": matched_by or "folder_hint"}, 200

    try:
        theme_path.unlink()
        if row:
            clear_theme_metadata(row)
            row["notes"] = "Theme deleted via Theme manager"
            save_ledger(path, rows)
            return {"ok": True, "message": f"Deleted {filename} — status reset to Missing", "matched_by": matched_by or "folder_hint"}, 200
        return {"ok": True, "message": f"Deleted {filename} (ledger row not found — status not updated)", "matched_by": matched_by or "folder_hint"}, 200
    except PermissionError:
        return {"ok": False, "error": f"Permission denied deleting {theme_path}. Check file ownership."}, 500
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:200]}, 500


def download_now_payload(data: dict):
    library = (data.get("library", "") or "").strip()
    rating_key = str(data.get("rating_key", "") or "").strip()
    folder_hint = str(data.get("folder", "") or "").strip()
    tmdb_id = str(data.get("tmdb_id", "") or "").strip()
    if not rating_key and not folder_hint:
        return {"ok": False, "error": "Missing identity: provide rating_key or folder"}, 400

    cfg = load_config()
    roots = get_media_roots(cfg)
    path = ledger_path_for(library) if library else str(LOGS_DIR / "theme_log.csv")
    rows = load_ledger(path)
    row, matched_by = find_row_by_identity(rows, rating_key, folder_hint, tmdb_id)
    if not row:
        return {"ok": False, "error": f"Not found in ledger for library '{library}'"}, 404

    url = (row.get("url", "") or "").strip()
    if not url:
        return {"ok": False, "error": "No source URL on this row — add a source first"}, 400
    folder = theme_target_folder(row, library_type=library_type_for_name(cfg, library))
    if not is_allowed_folder(folder, roots):
        return {"ok": False, "error": f"Folder not allowed: {folder!r} — check media_roots in config.yaml"}, 403

    theme_path = theme_file_path(row, cfg, library_type=library_type_for_name(cfg, library))
    audio_format = cfg.get("audio_format", "mp3") if cfg.get("audio_format", "mp3") in {"mp3", "m4a", "flac", "opus"} else "mp3"
    quality_profile = cfg.get("quality_profile", "high")
    slug = re.sub(r"[^a-z0-9]", "", rating_key.lower())[:8] or "dl"
    replaced_existing = False
    prepared_duration = 0.0

    try:
        downloaded = integrations.download_audio(
            url,
            folder,
            slug,
            audio_format=audio_format,
            quality_profile=quality_profile,
            cookies_file=cfg.get("cookies_file", "") or None,
        )
        prepared_duration = _validate_audio_ready(downloaded)
        start_offset = int(row.get("start_offset", 0) or 0)
        end_offset = int(row.get("end_offset", 0) or 0)
        max_duration = int(cfg.get("max_theme_duration", 0) or 0)
        if prepared_duration > 0 and (start_offset > 0 or end_offset > 0 or (max_duration > 0 and prepared_duration > max_duration)):
            start = max(0, start_offset)
            end = prepared_duration - max(0, end_offset) if end_offset > 0 else prepared_duration
            if max_duration > 0 and (end - start) > max_duration:
                end = start + max_duration
            if 0 <= start < prepared_duration and end > start and (start > 0 or end < prepared_duration):
                tmp_trim = _sibling_temp_path(theme_path, prefix=f"{theme_path.stem}.trim.")
                integrations.trim_audio_copy(downloaded, tmp_trim, start=start, end=end, timeout=60)
                downloaded.unlink(missing_ok=True)
                downloaded = tmp_trim
                prepared_duration = _validate_audio_ready(downloaded)
        replaced_existing = _atomic_replace_theme_file(downloaded, theme_path)
        row["status"] = "AVAILABLE"
        row["last_updated"] = now_str()
        row["notes"] = "Downloaded via manual download (replaced existing local theme)" if replaced_existing else "Downloaded via manual download"
        row, _ = sync_theme_cache(row, cfg.get("theme_filename", "theme.mp3"), probe_duration=True)
        save_ledger(path, rows)
        message = f"Downloaded and replaced existing {cfg.get('theme_filename', 'theme.mp3')}" if replaced_existing else f"Downloaded and saved as {cfg.get('theme_filename', 'theme.mp3')}"
        return {"ok": True, "message": message, "matched_by": matched_by or "rating_key", "replaced_existing": replaced_existing}, 200
    except subprocess.TimeoutExpired:
        integrations.cleanup_temp_downloads(folder, slug)
        return {"ok": False, "error": "Download timed out (180s)"}, 500
    except Exception as exc:
        integrations.cleanup_temp_downloads(folder, slug)
        if 'downloaded' in locals():
            Path(downloaded).unlink(missing_ok=True)
        if 'tmp_trim' in locals():
            Path(tmp_trim).unlink(missing_ok=True)
        return {"ok": False, "error": str(exc)[:200]}, 500


def sync_library_themes_payload(library: str):
    if not library:
        return {"ok": False, "error": "Missing library"}, 400
    cfg = load_config()
    filename = cfg.get("theme_filename", "theme.mp3")
    path = ledger_path_for(library)
    rows = load_ledger(path)
    updated = found = missing = promoted = 0
    now = now_str()
    for row in rows:
        new_row, changed = sync_theme_cache(row, filename, probe_duration=False)
        if changed:
            row.update(new_row)
            updated += 1
        exists = int(row.get("theme_exists", 0) or 0)
        if exists:
            found += 1
            if str(row.get("status", "") or "").upper() != "AVAILABLE":
                row["status"] = "AVAILABLE"
                row["last_updated"] = now
                row["notes"] = "Promoted to Available — theme file detected on disk"
                updated += 1
                promoted += 1
        else:
            missing += 1
            if str(row.get("status", "") or "").upper() == "AVAILABLE":
                row["status"] = "MISSING"
                row["last_updated"] = now
                row["notes"] = "Reset to Missing — theme file no longer on disk"
                updated += 1
    if updated:
        save_ledger(path, rows)
    return {
        "ok": True,
        "library": library,
        "total": len(rows),
        "updated": updated,
        "themes_found": found,
        "themes_missing": missing,
        "promoted": promoted,
    }, 200


# ── Health / scheduling / task helpers ───────────────────────────────────────

def next_cron_run(cron_expr: str) -> str | None:
    try:
        minute_s, hour_s, dom_s, month_s, dow_s = cron_expr.strip().split()
    except ValueError:
        return None

    def parse_field(expr, lo, hi):
        values = set()
        for chunk in expr.split(","):
            if chunk == "*":
                values.update(range(lo, hi + 1))
            elif "/" in chunk:
                base, step = chunk.split("/", 1)
                start = lo if base == "*" else int(base)
                values.update(range(start, hi + 1, int(step)))
            elif "-" in chunk:
                start, end = chunk.split("-", 1)
                values.update(range(int(start), int(end) + 1))
            else:
                values.add(int(chunk))
        return sorted(value for value in values if lo <= value <= hi)

    try:
        minutes = parse_field(minute_s, 0, 59)
        hours = parse_field(hour_s, 0, 23)
        months = parse_field(month_s, 1, 12)
        dom = parse_field(dom_s, 1, 31) if dom_s != "*" else None
        dow = parse_field(dow_s, 0, 6) if dow_s != "*" else None
    except Exception:
        return None

    candidate = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(400):  # at most ~400 day-level iterations to cover a year
        if candidate.month not in months:
            # skip to first day of next month
            if candidate.month == 12:
                candidate = candidate.replace(year=candidate.year + 1, month=1, day=1, hour=0, minute=0)
            else:
                candidate = candidate.replace(month=candidate.month + 1, day=1, hour=0, minute=0)
            continue

        weekday = candidate.weekday() % 7
        day_ok = True
        if dom is not None and dow is not None:
            day_ok = candidate.day in dom or weekday in dow
        elif dom is not None:
            day_ok = candidate.day in dom
        elif dow is not None:
            day_ok = weekday in dow
        if not day_ok:
            candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
            continue

        if candidate.hour not in hours:
            # skip to next valid hour today
            later = [h for h in hours if h > candidate.hour]
            if later:
                candidate = candidate.replace(hour=later[0], minute=minutes[0])
                continue
            # no valid hour left today, move to next day
            candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
            continue

        if candidate.minute not in minutes:
            later = [m for m in minutes if m > candidate.minute]
            if later:
                candidate = candidate.replace(minute=later[0])
                continue
            # no valid minute left this hour, advance to next hour
            candidate = (candidate + timedelta(hours=1)).replace(minute=0)
            continue

        return candidate.isoformat()
    return None


def api_health_payload() -> dict:
    cfg = load_config()
    scheduler_source = active_scheduler_source()
    cache_key = json.dumps(
        {
            "plex_url": (cfg.get("plex_url") or "").strip(),
            "plex_token": bool((cfg.get("plex_token") or "").strip()),
            "tmdb_key": bool((cfg.get("tmdb_api_key") or "").strip()),
            "golden_source_url": (cfg.get("golden_source_url") or "").strip(),
            "media_roots": list(get_media_roots(cfg)),
            "libraries": [
                {
                    "name": lib.get("name"),
                    "type": lib.get("type"),
                    "enabled": lib.get("enabled", True),
                }
                for lib in (cfg.get("libraries") or [])
            ],
            "schedule_enabled": bool(cfg.get("schedule_enabled", False)),
            "schedule_libraries": list(cfg.get("schedule_libraries") or []),
            "cron_schedule": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "scheduler_source": scheduler_source,
        },
        sort_keys=True,
    )
    now = time.time()
    if _health_cache["payload"] is not None and _health_cache["key"] == cache_key and now - float(_health_cache["ts"] or 0.0) < _HEALTH_CACHE_TTL:
        return dict(_health_cache["payload"])
    result = {}

    plex_url = (cfg.get("plex_url") or "").strip().rstrip("/")
    plex_token = (cfg.get("plex_token") or "").strip()
    if not plex_url or not plex_token:
        result["plex"] = {"state": "off", "label": "Not configured"}
    else:
        try:
            libraries = integrations.plex_sections(plex_url, plex_token)
            result["plex"] = {"state": "ok", "label": "Connected", "detail": f"{len(libraries)} libraries"}
        except Exception as exc:
            result["plex"] = {"state": "error", "label": "Connection failed", "detail": str(exc)[:100]}

    tmdb_key = (cfg.get("tmdb_api_key") or "").strip()
    if not tmdb_key:
        result["tmdb"] = {"state": "off", "label": "Not configured"}
    else:
        try:
            tmdb_result = integrations.test_tmdb_key(tmdb_key)
            result["tmdb"] = {"state": "ok", "label": "Connected"} if tmdb_result.get("ok") else {"state": "error", "label": "Invalid key"}
        except Exception as exc:
            result["tmdb"] = {"state": "error", "label": "API error", "detail": str(exc)[:100]}

    golden_source_url = (cfg.get("golden_source_url") or "").strip()
    if not golden_source_url:
        result["golden_source"] = {"state": "off", "label": "Not configured"}
    else:
        try:
            _, rows, _, _ = fetch_golden_source_catalog(golden_source_url)
            result["golden_source"] = {"state": "ok", "label": f"Loaded: {len(rows):,} rows"} if rows else {"state": "warning", "label": "Loaded: 0 rows", "detail": "No usable rows found"}
        except Exception as exc:
            result["golden_source"] = {"state": "error", "label": "Load failed", "detail": str(exc)[:100]}

    result["toolchain"] = integrations.toolchain_status()

    roots = get_media_roots(cfg)
    missing_paths = []
    readonly_paths = []
    for root in roots:
        path = Path(root)
        if not path.exists():
            missing_paths.append(root)
        elif not os.access(str(path), os.W_OK):
            readonly_paths.append(root)
    if missing_paths:
        result["storage"] = {"state": "error", "label": "Missing path", "detail": "; ".join(missing_paths[:2])}
    elif readonly_paths:
        result["storage"] = {"state": "warning", "label": "Read-only path", "detail": "; ".join(readonly_paths[:2])}
    else:
        result["storage"] = {"state": "ok", "label": "Writable", "detail": f"{len(roots)} path{'s' if len(roots) != 1 else ''} writable"}

    try:
        db_path = Path(get_db_path())
        if not db_path.exists():
            result["database"] = {"state": "warning", "label": "Not initialized"}
        else:
            conn = sqlite3.connect(str(db_path), timeout=5)
            conn.execute("SELECT 1")
            conn.close()
            result["database"] = {"state": "ok", "label": "Healthy"}
    except Exception as exc:
        result["database"] = {"state": "error", "label": "DB error", "detail": str(exc)[:100]}

    all_libraries = [lib for lib in (cfg.get("libraries") or []) if not lib.get("type") or lib.get("type") in ("movie", "show")]
    enabled = [lib for lib in all_libraries if lib.get("enabled") is not False]
    scheduled_names = set(cfg.get("schedule_libraries") or [lib["name"] for lib in enabled])
    scheduled = [lib for lib in enabled if lib.get("name") in scheduled_names]
    if not enabled:
        result["libraries"] = {"state": "warning", "label": "No enabled libraries", "enabled": 0, "scheduled": 0}
    else:
        result["libraries"] = {"state": "ok", "label": f"{len(enabled)} enabled", "detail": f"{len(scheduled)} in scheduler", "enabled": len(enabled), "scheduled": len(scheduled)}

    cron_expr = str(scheduler_source.get("active_cron") or cfg.get("cron_schedule") or "0 3 * * *").strip()
    cron_valid = len(cron_expr.split()) == 5
    next_run = next_cron_run(cron_expr) if cron_valid else None
    schedule_enabled = bool(scheduler_source.get("schedule_enabled")) if scheduler_managed_via_cron() else bool(cfg.get("schedule_enabled", False))
    detail_suffix = scheduler_source.get("detail") or ""
    if not cron_valid and schedule_enabled:
        result["schedule"] = {
            "state": "error",
            "label": "Invalid cron",
            "cron": cron_expr,
            "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "next_run": None,
            "libraries": len(scheduled),
            "source": scheduler_source,
            "detail": detail_suffix,
        }
    elif not schedule_enabled:
        result["schedule"] = {
            "state": "off",
            "label": "Disabled",
            "cron": cron_expr,
            "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "next_run": None,
            "libraries": len(scheduled),
            "source": scheduler_source,
            "detail": detail_suffix,
        }
    elif not scheduled:
        result["schedule"] = {
            "state": "warning",
            "label": "No libraries selected",
            "cron": cron_expr,
            "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "next_run": next_run,
            "libraries": 0,
            "source": scheduler_source,
            "detail": detail_suffix,
        }
    else:
        result["schedule"] = {
            "state": "ok",
            "label": "Enabled",
            "cron": cron_expr,
            "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "next_run": next_run,
            "libraries": len(scheduled),
            "source": scheduler_source,
            "detail": detail_suffix,
        }

    _health_cache["ts"] = now
    _health_cache["key"] = cache_key
    _health_cache["payload"] = dict(result)
    return result


def record_task(task_name, status="success", scope="", summary="", details=None, duration_seconds=None):
    entry = {
        "time": now_str(),
        "task": str(task_name or "Task"),
        "status": str(status or "success"),
        "outcome": str(status or "success"),
        "scope": str(scope or ""),
        "summary": str(summary or ""),
        "details": details or {},
        "duration_seconds": float(duration_seconds or 0),
    }
    try:
        with open(TASKS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def load_task_entries(limit=250):
    entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    run_entries = []
    for run_file in sorted(RUNS_DIR.glob("*.json")):
        try:
            run = json.loads(run_file.read_text(encoding="utf-8"))
            run_status = str(run.get("status") or run.get("outcome") or "success")
            run_entries.append({
                "time": run.get("time", ""),
                "task": {1: "Scan Libraries", 2: "Find Sources", 3: "Download Themes"}.get(run.get("pass"), "Pipeline Run"),
                "status": run_status,
                "outcome": run_status,
                "scope": "",
                "summary": run.get("summary", ""),
                "details": {"pass": run.get("pass", 0), "stats": run.get("stats", {}), "return_code": run.get("return_code"), "stop_requested": bool(run.get("stop_requested"))},
                "duration_seconds": run.get("duration_seconds") or 0,
                "is_run_history": True,
            })
        except Exception:
            continue
    return sorted(entries + run_entries, key=lambda entry: entry.get("time", ""), reverse=True)[: max(1, int(limit or 250))]


def parse_run_stats(lines: Iterable[str]) -> dict:
    stats = {}
    for line in lines:
        if "Pass 1 complete" in line:
            match = re.search(r"Total:\s*(\d+)\s*Have theme:\s*(\d+)\s*Missing:\s*(\d+)\s*Staged:\s*(\d+)\s*Approved:\s*(\d+)\s*New:\s*(\d+)\s*Removed:\s*(\d+)", line)
            if match:
                stats["pass1"] = {"total": int(match.group(1)), "has_theme": int(match.group(2)), "missing": int(match.group(3)), "staged": int(match.group(4)), "approved": int(match.group(5)), "new": int(match.group(6)), "removed": int(match.group(7))}
        if "Pass 2 complete" in line:
            match = re.search(r"Staged:\s*(\d+)\s*Missing:\s*(\d+)\s*Failed:\s*(\d+)", line)
            if match:
                stats["pass2"] = {"staged": int(match.group(1)), "missing": int(match.group(2)), "failed": int(match.group(3))}
        if "Pass 3 complete" in line:
            match = re.search(r"Available:\s*(\d+)\s*Failed:\s*(\d+)\s*Skipped:\s*(\d+)", line)
            if match:
                stats["pass3"] = {"available": int(match.group(1)), "failed": int(match.group(2)), "skipped": int(match.group(3))}
    return stats


# ── Run orchestration ────────────────────────────────────────────────────────

@dataclass
class RunManager:
    lock: threading.Lock = field(default_factory=threading.Lock)
    active: bool = False
    clients: list = field(default_factory=list)
    proc: subprocess.Popen | None = None
    stop_requested: bool = False
    started_at: float | None = None
    last_line: str = ""
    current_pass: int = 0
    scope_label: str = ""
    libraries: list[str] = field(default_factory=list)

    def broadcast(self, message: str):
        dead = []
        for client in self.clients:
            try:
                try:
                    client.put_nowait(message)
                except Exception:
                    try:
                        client.get_nowait()
                        client.put_nowait(message)
                    except Exception:
                        dead.append(client)
                        continue
            except Exception:
                dead.append(client)
        for client in dead:
            try:
                self.clients.remove(client)
            except Exception:
                pass

    def start(self, *, force_pass: int = 0, explicit_libraries=None, scope_label: str = "", allow_schedule_disabled: bool = False):
        explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
        with self.lock:
            if self.active:
                return False
            self.active = True
        thread = threading.Thread(
            target=self._do_run,
            kwargs={
                "force_pass": force_pass,
                "explicit_libraries": explicit_libraries,
                "scope_label": scope_label,
                "allow_schedule_disabled": allow_schedule_disabled,
            },
            daemon=True,
        )
        thread.start()
        return True

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.stop_requested = True
            try:
                self.proc.send_signal(signal.SIGTERM)
                self.broadcast("[STOP] Graceful stop requested…")
            except Exception:
                self.proc.kill()
            return True
        return False

    def event_stream(self):
        client = queue.Queue(maxsize=8000)
        self.clients.append(client)
        try:
            while True:
                try:
                    message = client.get(timeout=30)
                except Exception:
                    yield "data: __DONE__\n\n"
                    break
                yield f"data: {message}\n\n"
                if message == "__DONE__":
                    break
        finally:
            try:
                self.clients.remove(client)
            except Exception:
                pass

    def history(self):
        runs = []
        for run_file in sorted(RUNS_DIR.glob("*.json")):
            try:
                runs.append(json.loads(run_file.read_text()))
            except Exception:
                pass
        return runs

    def status(self):
        return {"active": self.active, "started_at": self.started_at, "pass": self.current_pass, "last_line": self.last_line, "scope": self.scope_label, "libraries": self.libraries}

    def _do_run(self, *, force_pass=0, explicit_libraries=None, scope_label="", allow_schedule_disabled=False):
        run_log = []
        timestamp = now_str()
        run_pass = force_pass or 0
        proc = None
        return_code = None
        explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
        resolved_scope = scope_label or (
            explicit_libraries[0] if len(explicit_libraries) == 1 else f"{len(explicit_libraries)} selected libraries" if explicit_libraries else "scheduled libraries"
        )
        try:
            self.started_at = time.time()
            self.last_line = ""
            self.current_pass = run_pass
            self.stop_requested = False
            self.scope_label = resolved_scope
            self.libraries = list(explicit_libraries)
            env = {**os.environ, "CONFIG_PATH": CONFIG_PATH}
            if force_pass:
                env["FORCE_PASS"] = str(force_pass)
            else:
                env.pop("FORCE_PASS", None)
            if allow_schedule_disabled:
                env["RUN_SCHEDULE_NOW"] = "1"
            else:
                env.pop("RUN_SCHEDULE_NOW", None)
            if explicit_libraries:
                env["RUN_LIBRARIES"] = json.dumps(explicit_libraries)
            else:
                env.pop("RUN_LIBRARIES", None)
            proc = subprocess.Popen([sys.executable, SCRIPT_PATH], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
            self.proc = proc
            for line in proc.stdout:
                line = line.rstrip()
                run_log.append(line)
                self.broadcast(line)
                self.last_line = line
                if "Pass 1" in line:
                    run_pass = max(run_pass, 1)
                if "Pass 2" in line:
                    run_pass = max(run_pass, 2)
                if "Pass 3" in line:
                    run_pass = max(run_pass, 3)
                self.current_pass = run_pass
            return_code = proc.wait()
        except Exception as exc:
            message = f"[ERROR] {exc}"
            run_log.append(message)
            self.broadcast(message)
        finally:
            stop_requested = self.stop_requested
            if return_code == 0 and not stop_requested:
                outcome = "success"
            elif stop_requested:
                outcome = "stopped"
            else:
                outcome = "error"
            self.proc = None
            self.stop_requested = False
            self.broadcast("__DONE__")
            self.active = False
            duration = time.time() - self.started_at if self.started_at else None
            summary = next((line for line in reversed(run_log) if any(marker in line for marker in ["complete", "caught up", "nothing to do", "processed", "STOP", "ERROR"])), "No output")
            stats = parse_run_stats(run_log)
            run_file = RUNS_DIR / (timestamp.replace(":", "-").replace(" ", "_") + ".json")
            run_file.write_text(json.dumps({
                "time": timestamp,
                "pass": run_pass,
                "scope": resolved_scope,
                "libraries": explicit_libraries,
                "summary": summary,
                "status": outcome,
                "outcome": outcome,
                "log": "\n".join(run_log),
                "duration_seconds": duration,
                "stats": stats,
                "return_code": return_code,
                "stop_requested": stop_requested,
            }))
            record_task(
                {1: "Run Scan Now", 2: "Run Find Sources Now", 3: "Run Download Themes Now"}.get(run_pass, "Run Pipeline"),
                outcome,
                resolved_scope,
                summary,
                {"pass": run_pass, "stats": stats, "return_code": return_code, "stop_requested": stop_requested, "libraries": explicit_libraries},
                duration,
            )
            self.started_at = None
            self.scope_label = ""
            self.libraries = []

    def cleanup(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()


RUN_MANAGER = RunManager()
atexit.register(RUN_MANAGER.cleanup)


def export_golden_source_csv_payload(data: dict):
    started = time.perf_counter()
    library = (data.get("library", "") or "").strip()
    cfg = load_config()
    target_libraries = [library] if library else [lib.get("name", "").strip() for lib in cfg.get("libraries", []) if lib.get("name")]
    if not target_libraries:
        return {"ok": False, "error": "No libraries configured"}, 400
    output_rows = []
    current_time = now_str()
    for target in target_libraries:
        rows = load_ledger(ledger_path_for(target))
        for row in rows:
            url = str(row.get("url", "") or "").strip()
            if not url:
                continue
            output_rows.append({
                "tmdb_id": str(row.get("tmdb_id", "") or "").strip(),
                "title": str(row.get("title", "") or row.get("plex_title", "") or "").strip(),
                "year": str(row.get("year", "") or "").strip(),
                "source_url": url,
                "start_offset": str(row.get("start_offset", "0") or "0").strip() or "0",
                "updated_at": str(row.get("last_updated", "") or "").strip() or current_time,
                "notes": str(row.get("notes", "") or "").strip(),
            })
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_name = library or "all_libraries"
    filename = f"golden_source_export_{re.sub(r'[^a-z0-9]+', '_', scope_name.lower()).strip('_') or 'library'}_{stamp}.csv"
    path = EXPORTS_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["tmdb_id", "title", "year", "source_url", "start_offset", "updated_at", "notes"])
        writer.writeheader()
        writer.writerows(output_rows)
    record_task("Export Golden Source CSV", "success", library or "all libraries", f"Exported {len(output_rows)} rows", {"library": library or "", "libraries_exported": len(target_libraries), "rows_exported": len(output_rows), "file": filename}, time.perf_counter() - started)
    return {"ok": True, "rows_exported": len(output_rows), "file": filename, "download_url": f"/api/tasks/download/{filename}"}, 200


def export_candidate_csv_payload(data: dict):
    started = time.perf_counter()
    library = (data.get("library", "") or "").strip()
    cfg = load_config()
    target_libraries = [library] if library else [lib.get("name", "").strip() for lib in cfg.get("libraries", []) if lib.get("name")]
    if not target_libraries:
        return {"ok": False, "error": "No libraries configured"}, 400
    output_rows = []
    for target in target_libraries:
        rows = load_ledger(ledger_path_for(target))
        for row in rows:
            url = str(row.get("url", "") or "").strip()
            tmdb_id = str(row.get("tmdb_id", "") or "").strip()
            if not url or not tmdb_id or str(row.get("golden_source_url", "") or "").strip():
                continue
            output_rows.append({"tmdb_id": tmdb_id, "source_url": url, "start_offset": str(row.get("start_offset", "0") or "0").strip() or "0"})
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    scope_name = library or "all_libraries"
    filename = f"candidate_export_{re.sub(r'[^a-z0-9]+', '_', scope_name.lower()).strip('_') or 'library'}_{stamp}.csv"
    path = EXPORTS_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["tmdb_id", "source_url", "start_offset"])
        writer.writeheader()
        writer.writerows(output_rows)
    record_task("Export Candidate CSV", "success", library or "all libraries", f"Exported {len(output_rows)} candidate rows", {"library": library or "", "libraries_exported": len(target_libraries), "rows_exported": len(output_rows), "file": filename}, time.perf_counter() - started)
    return {"ok": True, "rows_exported": len(output_rows), "file": filename, "download_url": f"/api/tasks/download/{filename}"}, 200


def cleanup_logs_payload(data: dict):
    keep_days = int(data.get("keep_days", 14) or 14)
    cutoff = time.time() - (keep_days * 86400)
    deleted = 0
    for log_file in LOGS_DIR.glob("*.log"):
        try:
            if log_file.stat().st_mtime < cutoff:
                log_file.unlink(missing_ok=True)
                deleted += 1
        except Exception:
            pass
    record_task("Clean Up Logs", "success", "", f"Removed {deleted} log files", {"keep_days": keep_days, "deleted": deleted})
    return {"ok": True, "deleted": deleted, "keep_days": keep_days}, 200


def prune_task_history_payload(data: dict):
    keep_runs = int(data.get("keep_runs", 100) or 100)
    removed_runs = 0
    for run_file in sorted(RUNS_DIR.glob("*.json"))[:-max(1, keep_runs)]:
        try:
            run_file.unlink(missing_ok=True)
            removed_runs += 1
        except Exception:
            pass
    task_entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                task_entries.append(json.loads(line))
            except Exception:
                pass
    kept_entries = task_entries[-max(1, keep_runs):]
    with open(TASKS_FILE, "w", encoding="utf-8") as fh:
        for entry in kept_entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    record_task("Prune Task History", "success", "", f"Removed {removed_runs} run entries", {"removed_runs": removed_runs, "kept_entries": len(kept_entries)})
    return {"ok": True, "removed_runs": removed_runs, "kept_task_entries": len(kept_entries)}, 200


def sqlite_maintenance_payload(data: dict):
    do_backup = bool(data.get("backup", True))
    do_vacuum = bool(data.get("vacuum", True))
    db_path = Path(get_db_path())
    if not db_path.exists():
        return {"ok": False, "error": f"Database not found: {db_path}"}, 404
    backup_file = ""
    if do_backup:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"media_tracks_backup_{stamp}.db"
        (EXPORTS_DIR / backup_file).write_bytes(db_path.read_bytes())
    if do_vacuum:
        conn = sqlite3.connect(str(db_path), timeout=60)
        try:
            conn.execute("VACUUM")
            conn.commit()
        finally:
            conn.close()
    record_task("SQLite Maintenance", "success", "", "Backup/Vacuum completed", {"backup_file": backup_file, "vacuum": do_vacuum})
    return {"ok": True, "backup_file": backup_file, "download_url": f"/api/tasks/download/{backup_file}" if backup_file else ""}, 200


def clear_all_source_urls_payload(data: dict):
    library = (data.get("library", "") or "").strip()
    cfg = load_config()
    target_libraries = [library] if library else [lib.get("name", "").strip() for lib in cfg.get("libraries", []) if lib.get("name")]
    if not target_libraries:
        return {"ok": False, "error": "No libraries configured"}, 400
    now = now_str()
    total_summary = {"requested": 0, "matched": 0, "cleared": 0, "updated": 0, "preserved_available": 0, "reset_missing": 0, "preserved_failed": 0, "preserved_unmonitored": 0, "skipped_without_url": 0}
    changed_libraries = 0
    for target in target_libraries:
        path = ledger_path_for(target)
        rows = load_ledger(path)
        summary = clear_source_urls_for_rows(rows, note="Source URL cleared via Tasks maintenance", now=now)
        for key, value in summary.items():
            total_summary[key] += value
        if summary["cleared"]:
            save_ledger(path, rows)
            changed_libraries += 1
    record_task("Clear All Source URLs", "success", library or "all libraries", f"Cleared {total_summary['cleared']} URLs", {"library": library or "", "libraries_cleared": changed_libraries, **total_summary})
    return {"ok": True, "library": library, "libraries_cleared": changed_libraries, **total_summary}, 200
