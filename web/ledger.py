"""Ledger and source-catalog domain logic."""

from __future__ import annotations

import re
import time
from pathlib import Path

from shared.golden_source_csv import parse_golden_source_csv_rows
from shared.storage import (
    LEDGER_HEADERS,
    clear_golden_source_import_record,
    clear_selected_source_record,
    ffprobe_duration,
    infer_selected_source_contract,
    ledger_path_for,
    load_ledger_rows as load_ledger,
    now_str,
    read_golden_source_text,
    save_ledger_rows as save_ledger,
    stamp_golden_source_import_record,
    stamp_selected_source_record,
    set_selected_source_contract,
    status_after_clearing_source,
    validate_manual_status_transition,
)
import web.integrations as integrations
from web.services import load_config

LOGS_DIR = Path("/app/logs")
GOLDEN_CACHE_DIR = LOGS_DIR / "golden_source_cache"
EDITABLE_LEDGER_FIELDS = set(LEDGER_HEADERS) - {"folder", "rating_key"}

for path in (GOLDEN_CACHE_DIR,):
    path.mkdir(parents=True, exist_ok=True)


def require_library_name(library: str) -> str:
    library_name = str(library or "").strip()
    if not library_name:
        raise ValueError("Missing library")
    return library_name


def legacy_theme_log_path() -> str:
    return str(LOGS_DIR / "theme_log.csv")


def _parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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
    original_url = str(row.get("url", "") or "").strip()
    original_start_offset = str(row.get("start_offset", "0") or "0")
    original_end_offset = str(row.get("end_offset", "0") or "0")
    original_selected_kind = str(row.get("selected_source_kind", "") or "")
    original_selected_method = str(row.get("selected_source_method", "") or "")
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
        row[key] = str(value or "")
    if attempted_status:
        row["status"] = attempted_status

    if "url" in updates:
        updated_url = str(updates.get("url") or "").strip()
        golden_url = str(row.get("golden_source_url", "") or "").strip()
        selected_method = str(updates.get("selected_source_method", "") or "").strip().lower()
        if updated_url and golden_url and updated_url == golden_url:
            row["source_origin"] = "golden_source"
        elif updated_url and selected_method in {"playlist", "direct"}:
            row["source_origin"] = f"youtube_{selected_method}"
        else:
            row["source_origin"] = "manual" if updated_url else "unknown"
        set_selected_source_contract(
            row,
            kind=str(updates.get("selected_source_kind", "") or ""),
            method=str(updates.get("selected_source_method", "") or ""),
        )
        selected_changed = any(
            (
                updated_url != original_url,
                str(row.get("start_offset", "0") or "0") != original_start_offset,
                str(row.get("end_offset", "0") or "0") != original_end_offset,
                str(row.get("selected_source_kind", "") or "") != original_selected_kind,
                str(row.get("selected_source_method", "") or "") != original_selected_method,
            )
        )
        if updated_url:
            if selected_changed or not str(row.get("selected_source_recorded_at", "") or "").strip():
                stamp_selected_source_record(row)
        else:
            clear_selected_source_record(row)
    elif "selected_source_kind" in updates or "selected_source_method" in updates:
        selected_kind, selected_method = infer_selected_source_contract(candidate_row)
        set_selected_source_contract(
            row,
            kind=str(updates.get("selected_source_kind", "") or selected_kind),
            method=str(updates.get("selected_source_method", "") or selected_method),
        )
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
        row["start_offset"] = "0"
        row["source_origin"] = "unknown"
        row["selected_source_kind"] = ""
        row["selected_source_method"] = ""
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
    _ = library_type or "movie"
    return str(row.get("folder", "") or "")


def theme_file_path(row: dict, cfg: dict, *, library_type: str = "") -> Path:
    return Path(theme_target_folder(row, library_type=library_type)) / cfg.get("theme_filename", "theme.mp3")


def parse_golden_source_csv(text: str) -> list[dict]:
    return parse_golden_source_csv_rows(text)


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
    overwrite = _parse_bool(data.get("overwrite_existing", False))
    auto_approve = _parse_bool(data.get("auto_approve", False))
    force_refresh = _parse_bool(data.get("force_refresh", cfg.get("refresh_golden_source_each_run", True)))
    cache_ttl_sec = int(cfg.get("golden_source_cache_ttl_sec", 1800) or 1800)
    resolve_missing_tmdb = _parse_bool(data.get("resolve_missing_tmdb", cfg.get("golden_source_resolve_tmdb", False)))
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
        incoming_url = str(match.get("source_url", "") or "").strip()
        incoming_offset = str(match.get("start_offset", "0") or "0")
        row["tmdb_id"] = tmdb_id or str(match.get("tmdb_id", "") or "").strip()
        row["golden_source_url"] = incoming_url
        row["golden_source_offset"] = incoming_offset
        row["end_offset"] = match.get("end_offset", "0") or "0"
        if incoming_url:
            stamp_golden_source_import_record(row, imported_at=now)
        else:
            clear_golden_source_import_record(row)
        if existing_url and not overwrite:
            skipped_existing += 1
            continue

        if overwrite or not existing_url:
            row["url"] = incoming_url
            row["start_offset"] = incoming_offset
            row["source_origin"] = "golden_source" if incoming_url else "unknown"
            set_selected_source_contract(row, kind="golden" if incoming_url else "", method="golden_source" if incoming_url else "")
            if incoming_url:
                stamp_selected_source_record(row, recorded_at=now)
            else:
                clear_selected_source_record(row)
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
