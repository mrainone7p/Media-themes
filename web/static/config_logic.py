"""Configuration validation and normalization for Media Tracks.

Extracted from web/logic.py to keep logic.py focused on orchestration.
This module owns: config constants, coercion helpers, normalize_config,
load_raw_config, and load_config.

save_config remains in logic.py because it must invalidate the health
check cache (_health_cache), which is owned by the orchestration layer.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

WEB_DIR = Path(__file__).resolve().parent
SHARED_DIR = WEB_DIR.parent / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from storage import CONFIG_PATH, normalize_golden_source_url


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


# ── Config and UI helpers ─────────────────────────────────────────────────────

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
