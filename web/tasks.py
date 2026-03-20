"""Scheduler, health, UI, and maintenance task domain logic."""

from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from shared.storage import CONFIG_PATH, get_db_path, ledger_path_for, load_ledger_rows as load_ledger, now_str, save_ledger_rows as save_ledger
import web.integrations as integrations
from web.ledger import clear_source_urls_for_rows, fetch_golden_source_catalog, get_media_roots
from web.services import (
    CONFIG_DEFAULTS,
    RUNS_DIR,
    TASKS_FILE,
    TASK_ACTIVITY_SUMMARY_FILE,
    _CRON_ENTRY_RE,
    _normalize_cron_schedule,
    _normalize_task_entry,
    _trim_summary_entries,
    load_config,
    normalize_config,
    record_task,
)

UI_TERMINOLOGY_PATH = os.environ.get("UI_TERMINOLOGY_PATH", "/app/web/ui_terminology.yaml")
WEB_DIR = Path(__file__).resolve().parent
LOGS_DIR = Path("/app/logs")
EXPORTS_DIR = LOGS_DIR / "exports"
_HEALTH_CACHE_TTL = 30
_HEALTH_CACHE_EMPTY = {"ts": 0.0, "key": None, "payload": None}
_health_cache: dict[str, dict[str, object]] = {
    "lite": dict(_HEALTH_CACHE_EMPTY),
    "full": dict(_HEALTH_CACHE_EMPTY),
}
CRON_FILE_PATH = Path(os.environ.get("MEDIA_TRACKS_CRON_FILE", "/etc/cron.d/media-tracks"))
CRON_COMMAND = "python3 -m script.media_tracks >> /proc/1/fd/1 2>> /proc/1/fd/2"
SCHEDULER_AUTHORITY = os.environ.get("MEDIA_TRACKS_SCHEDULER_AUTHORITY", "cron").strip().lower() or "cron"
_template_cache: str | None = None

for path in (RUNS_DIR, EXPORTS_DIR):
    path.mkdir(parents=True, exist_ok=True)


def save_config(data: dict) -> dict:
    global _health_cache
    normalized, errors = normalize_config(data, for_save=True)
    if errors:
        raise ValueError(errors)
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        yaml.dump(normalized, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)
    _health_cache = {"lite": dict(_HEALTH_CACHE_EMPTY), "full": dict(_HEALTH_CACHE_EMPTY)}
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
        _health_cache = {"lite": dict(_HEALTH_CACHE_EMPTY), "full": dict(_HEALTH_CACHE_EMPTY)}
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
        _health_cache = {"lite": dict(_HEALTH_CACHE_EMPTY), "full": dict(_HEALTH_CACHE_EMPTY)}
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
        _health_cache = {"lite": dict(_HEALTH_CACHE_EMPTY), "full": dict(_HEALTH_CACHE_EMPTY)}
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


def load_template() -> str:
    global _template_cache
    if _template_cache is not None:
        return _template_cache
    template_path = WEB_DIR / "template.html"
    if template_path.exists():
        _template_cache = template_path.read_text(encoding="utf-8")
        return _template_cache
    return "<h1>Template not found</h1>"


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
    for _ in range(400):
        if candidate.month not in months:
            if candidate.month == 12:
                candidate = candidate.replace(year=candidate.year + 1, month=1, day=1, hour=0, minute=0)
            else:
                candidate = candidate.replace(month=candidate.month + 1, day=1, hour=0, minute=0)
            continue
        weekday = candidate.weekday() % 7
        if dom is not None and dow is not None:
            day_ok = candidate.day in dom or weekday in dow
        elif dom is not None:
            day_ok = candidate.day in dom
        elif dow is not None:
            day_ok = weekday in dow
        else:
            day_ok = True
        if not day_ok:
            candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
            continue
        if candidate.hour not in hours:
            later = [h for h in hours if h > candidate.hour]
            if later:
                candidate = candidate.replace(hour=later[0], minute=minutes[0])
                continue
            candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
            continue
        if candidate.minute not in minutes:
            later = [m for m in minutes if m > candidate.minute]
            if later:
                candidate = candidate.replace(minute=later[0])
                continue
            candidate = (candidate + timedelta(hours=1)).replace(minute=0)
            continue
        return candidate.isoformat()
    return None


def _health_validation_state(label: str, detail: str) -> dict:
    return {"state": "unknown", "label": label, "detail": detail}


def api_health_payload(mode: str = "lite") -> dict:
    cfg = load_config()
    resolved_mode = "full" if str(mode or "").strip().lower() == "full" else "lite"
    scheduler_source = active_scheduler_source()
    cache_key = json.dumps(
        {
            "mode": resolved_mode,
            "plex_url": (cfg.get("plex_url") or "").strip(),
            "plex_token": bool((cfg.get("plex_token") or "").strip()),
            "tmdb_key": bool((cfg.get("tmdb_api_key") or "").strip()),
            "golden_source_url": (cfg.get("golden_source_url") or "").strip(),
            "media_roots": list(get_media_roots(cfg)),
            "libraries": [{"name": lib.get("name"), "type": lib.get("type"), "enabled": lib.get("enabled", True)} for lib in (cfg.get("libraries") or [])],
            "schedule_enabled": bool(cfg.get("schedule_enabled", False)),
            "schedule_libraries": list(cfg.get("schedule_libraries") or []),
            "cron_schedule": (cfg.get("cron_schedule") or "0 3 * * *").strip(),
            "scheduler_source": scheduler_source,
        },
        sort_keys=True,
    )
    now = time.time()
    cache_bucket = _health_cache.setdefault(resolved_mode, dict(_HEALTH_CACHE_EMPTY))
    if cache_bucket["payload"] is not None and cache_bucket["key"] == cache_key and now - float(cache_bucket["ts"] or 0.0) < _HEALTH_CACHE_TTL:
        return dict(cache_bucket["payload"])

    result = {}
    plex_url = (cfg.get("plex_url") or "").strip().rstrip("/")
    plex_token = (cfg.get("plex_token") or "").strip()
    if not plex_url or not plex_token:
        result["plex"] = {"state": "off", "label": "Not configured"}
    elif resolved_mode != "full":
        result["plex"] = _health_validation_state("Ready to validate", "Click Validate to test the Plex connection.")
    else:
        try:
            libraries = integrations.plex_sections(plex_url, plex_token)
            result["plex"] = {"state": "ok", "label": "Connected", "detail": f"{len(libraries)} libraries"}
        except Exception as exc:
            result["plex"] = {"state": "error", "label": "Connection failed", "detail": str(exc)[:100]}

    tmdb_key = (cfg.get("tmdb_api_key") or "").strip()
    if not tmdb_key:
        result["tmdb"] = {"state": "off", "label": "Not configured"}
    elif resolved_mode != "full":
        result["tmdb"] = _health_validation_state("Ready to validate", "Click Validate to test the TMDB API key.")
    else:
        try:
            tmdb_result = integrations.test_tmdb_key(tmdb_key)
            result["tmdb"] = {"state": "ok", "label": "Connected"} if tmdb_result.get("ok") else {"state": "error", "label": "Invalid key"}
        except Exception as exc:
            result["tmdb"] = {"state": "error", "label": "API error", "detail": str(exc)[:100]}

    golden_source_url = (cfg.get("golden_source_url") or "").strip()
    if not golden_source_url:
        result["golden_source"] = {"state": "off", "label": "Not configured"}
    elif resolved_mode != "full":
        result["golden_source"] = _health_validation_state("Ready to validate", "Click Validate to check the Golden Source feed.")
    else:
        try:
            _, rows, _, _ = fetch_golden_source_catalog(golden_source_url)
            result["golden_source"] = {"state": "ok", "label": f"Loaded: {len(rows):,} rows"} if rows else {"state": "warning", "label": "Loaded: 0 rows", "detail": "No usable rows found"}
        except Exception as exc:
            result["golden_source"] = {"state": "error", "label": "Load failed", "detail": str(exc)[:100]}

    result["toolchain"] = (
        integrations.toolchain_status()
        if resolved_mode == "full"
        else _health_validation_state("Ready to validate", "Click Validate to check yt-dlp and ffmpeg.")
    )

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
        result["schedule"] = {"state": "error", "label": "Invalid cron", "cron": cron_expr, "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(), "next_run": None, "libraries": len(scheduled), "source": scheduler_source, "detail": detail_suffix}
    elif not schedule_enabled:
        result["schedule"] = {"state": "off", "label": "Disabled", "cron": cron_expr, "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(), "next_run": None, "libraries": len(scheduled), "source": scheduler_source, "detail": detail_suffix}
    elif not scheduled:
        result["schedule"] = {"state": "warning", "label": "No libraries selected", "cron": cron_expr, "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(), "next_run": next_run, "libraries": 0, "source": scheduler_source, "detail": detail_suffix}
    else:
        result["schedule"] = {"state": "ok", "label": "Enabled", "cron": cron_expr, "configured_cron": (cfg.get("cron_schedule") or "0 3 * * *").strip(), "next_run": next_run, "libraries": len(scheduled), "source": scheduler_source, "detail": detail_suffix}

    result["validation"] = {
        "mode": resolved_mode,
        "full": resolved_mode == "full",
        "label": "Validated now" if resolved_mode == "full" else "Quick status",
        "detail": "External integrations are checked only when you run Validate."
        if resolved_mode != "full"
        else "All dashboard integrations were checked live.",
    }

    cache_bucket["ts"] = now
    cache_bucket["key"] = cache_key
    cache_bucket["payload"] = dict(result)
    return result


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
    summary_entries = []
    if TASK_ACTIVITY_SUMMARY_FILE.exists():
        for line in TASK_ACTIVITY_SUMMARY_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            summary_entries.append(_normalize_task_entry(entry, is_run_history=bool(entry.get("is_run_history"))))
    kept_summary_entries = _trim_summary_entries(summary_entries, max_entries=keep_runs)
    with open(TASK_ACTIVITY_SUMMARY_FILE, "w", encoding="utf-8") as fh:
        for entry in kept_summary_entries:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    record_task("Prune Task History", "success", "", f"Removed {removed_runs} run entries", {"removed_runs": removed_runs, "kept_entries": len(kept_entries)})
    return {"ok": True, "removed_runs": removed_runs, "kept_task_entries": len(kept_entries), "kept_summary_entries": len(kept_summary_entries)}, 200


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
