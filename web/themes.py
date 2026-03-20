"""Theme and media domain logic."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from shared.file_utils import atomic_replace_file, sibling_temp_path, validate_audio_file
from shared.storage import ffprobe_duration, ledger_path_for, load_ledger_rows as load_ledger, now_str, save_ledger_rows as save_ledger, sync_theme_cache
from web import integrations
from web.ledger import (
    find_row_by_identity,
    get_media_roots,
    is_allowed_folder,
    legacy_theme_log_path,
    library_type_for_name,
    require_library_name,
    theme_file_path,
    theme_target_folder,
)
from web.services import load_config


def _validate_audio_ready(audio_path: str | Path) -> float:
    ok, msg = validate_audio_file(audio_path)
    if not ok:
        raise RuntimeError(f"Prepared audio file is not valid: {msg}")
    return ffprobe_duration(Path(audio_path))


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
            path = ledger_path_for(library) if library else legacy_theme_log_path()
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


def trim_theme_payload(data: dict):
    try:
        library = require_library_name(data.get("library", ""))
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    rating_key = data.get("rating_key", "")
    try:
        start_offset = int(data.get("start_offset", 0))
        end_offset = int(data.get("end_offset", 0))
    except (ValueError, TypeError):
        return {"ok": False, "error": "start_offset and end_offset must be numbers"}, 400
    cfg = load_config()
    roots = get_media_roots(cfg)
    filename = cfg.get("theme_filename", "theme.mp3")
    max_duration = int(cfg.get("max_theme_duration", 0))
    path = ledger_path_for(library)
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
        tmp = sibling_temp_path(theme_path, prefix=f"{theme_path.stem}.trim.")
        integrations.trim_audio_copy(theme_path, tmp, start=start, end=end, timeout=60)
        new_duration = _validate_audio_ready(tmp)
        atomic_replace_file(tmp, theme_path)
        row["start_offset"] = str(start_offset)
        row["end_offset"] = str(end_offset)
        row["last_updated"] = now_str()
        row["notes"] = f"Trimmed: {duration:.1f}s → {new_duration:.1f}s"
        row, _ = sync_theme_cache(row, filename, probe_duration=True)
        save_ledger(path, rows)
        return {"ok": True, "message": f"Trimmed {duration:.1f}s → {new_duration:.1f}s", "duration": new_duration}, 200
    except Exception as exc:
        if "tmp" in locals():
            Path(tmp).unlink(missing_ok=True)
        return {"ok": False, "error": str(exc)[:200]}, 500


def delete_theme_payload(data: dict):
    try:
        library = require_library_name(data.get("library", ""))
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    rating_key = str(data.get("rating_key", "") or "").strip()
    folder_hint = str(data.get("folder", "") or "").strip()
    tmdb_id = str(data.get("tmdb_id", "") or "").strip()

    cfg = load_config()
    filename = cfg.get("theme_filename", "theme.mp3")
    roots = get_media_roots(cfg)
    if filename not in {"theme.mp3", "theme.m4a", "theme.flac", "theme.opus"}:
        return {"ok": False, "error": f"Unexpected theme filename: {filename}"}, 400

    path = ledger_path_for(library)
    rows = load_ledger(path)
    row, matched_by = find_row_by_identity(rows, rating_key, folder_hint, tmdb_id)

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
    try:
        library = require_library_name(data.get("library", ""))
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 400
    rating_key = str(data.get("rating_key", "") or "").strip()
    folder_hint = str(data.get("folder", "") or "").strip()
    tmdb_id = str(data.get("tmdb_id", "") or "").strip()
    if not rating_key and not folder_hint:
        return {"ok": False, "error": "Missing identity: provide rating_key or folder"}, 400

    cfg = load_config()
    roots = get_media_roots(cfg)
    path = ledger_path_for(library)
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
                tmp_trim = sibling_temp_path(theme_path, prefix=f"{theme_path.stem}.trim.")
                integrations.trim_audio_copy(downloaded, tmp_trim, start=start, end=end, timeout=60)
                downloaded.unlink(missing_ok=True)
                downloaded = tmp_trim
                _validate_audio_ready(downloaded)
        replaced_existing = atomic_replace_file(downloaded, theme_path)
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
        if "downloaded" in locals():
            Path(downloaded).unlink(missing_ok=True)
        if "tmp_trim" in locals():
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
