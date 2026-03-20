"""Application services for the Media Tracks web backend.

This module centralizes app-facing service helpers so ``web.app`` can stay focused
on Flask route wiring. It also owns configuration normalization and run
orchestration, replacing the previous ``web.config_logic`` and
``web.run_logic`` modules.
"""

from __future__ import annotations

import atexit
import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml

from shared.golden_source_csv import GOLDEN_SOURCE_OPTIONAL_COLUMNS, GOLDEN_SOURCE_REQUIRED_COLUMNS
from shared.logging_utils import get_project_logger, summarize_libraries
from shared.storage import (
    CONFIG_PATH,
    MANUAL_STATUS_TRANSITIONS,
    STATUS_ORDER,
    ledger_path_for,
    load_ledger_rows as load_ledger,
    normalize_golden_source_url,
    now_str,
    save_ledger_rows as save_ledger,
)
import web.integrations as integrations

SERVICE_LOG = get_project_logger("web.services")

# ── Configuration helpers ────────────────────────────────────────────────────

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
        errors.append(config_error("theme_filename", "invalid_filename", "Theme filename cannot include path separators.", value))
        return CONFIG_DEFAULTS["theme_filename"]
    return filename


def normalize_config(raw_cfg, *, for_save=False):
    raw_cfg = raw_cfg or {}
    errors = []
    normalized = dict(CONFIG_DEFAULTS)
    normalized["media_roots"] = _normalize_media_roots(raw_cfg, errors)
    normalized["libraries"] = _normalize_libraries(raw_cfg, errors)

    for field in CONFIG_BOOL_FIELDS:
        if field in raw_cfg:
            coerced = _coerce_config_bool(field, raw_cfg.get(field), errors)
            if coerced is not None:
                normalized[field] = coerced
    for field in CONFIG_ENUM_FIELDS:
        if field in raw_cfg:
            coerced = _normalize_config_enum(field, raw_cfg.get(field), errors)
            if coerced is not None:
                normalized[field] = coerced
    for field, spec in CONFIG_NUMERIC_FIELDS.items():
        if field in raw_cfg:
            coerced = _coerce_config_number(field, raw_cfg.get(field), spec, errors)
            if coerced is not None:
                normalized[field] = coerced

    for field in {"plex_url", "plex_token", "tmdb_api_key", "ui_token", "cookies_file", "search_query_playlist", "search_query_direct"}:
        if field in raw_cfg:
            normalized[field] = str(raw_cfg.get(field, "") or "").strip()
    normalized["golden_source_url"] = normalize_golden_source_url(raw_cfg.get("golden_source_url", normalized["golden_source_url"]))
    normalized["theme_filename"] = _normalize_theme_filename(raw_cfg.get("theme_filename", normalized["theme_filename"]), errors)
    normalized["schedule_libraries"] = _normalize_schedule_libraries(raw_cfg.get("schedule_libraries"), normalized, errors)
    normalized["cron_schedule"] = _normalize_cron_schedule(raw_cfg.get("cron_schedule", normalized["cron_schedule"]), errors)

    if raw_cfg.get("media_root") and not raw_cfg.get("media_roots"):
        normalized["media_root"] = str(raw_cfg.get("media_root") or "").strip()
    if for_save:
        normalized.pop("media_root", None)
    return normalized, errors


def load_raw_config() -> dict:
    if not Path(CONFIG_PATH).exists():
        return dict(CONFIG_DEFAULTS)
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


def load_config() -> dict:
    normalized, _ = normalize_config(load_raw_config(), for_save=False)
    return normalized


# ── Run orchestration ────────────────────────────────────────────────────────

LOGS_DIR = Path("/app/logs")
RUNS_DIR = LOGS_DIR / "runs"
TASKS_FILE = LOGS_DIR / "task_history.jsonl"
TASK_ACTIVITY_SUMMARY_FILE = LOGS_DIR / "task_activity_summary.jsonl"
SCRIPT_MODULE = "script.media_tracks"
TASK_ACTIVITY_MAX_ENTRIES = 1000
_TASK_ACTIVITY_LOCK = threading.Lock()

for path in (RUNS_DIR,):
    path.mkdir(parents=True, exist_ok=True)


def _normalize_task_entry(entry: dict, *, is_run_history: bool = False) -> dict:
    details = entry.get("details")
    normalized_details = details if isinstance(details, dict) else {}
    normalized = {
        "time": now_str(),
        "task": "Task",
        "status": "success",
        "outcome": "success",
        "scope": "",
        "summary": "",
        "details": normalized_details,
        "duration_seconds": 0.0,
    }
    normalized.update(entry or {})
    normalized["task"] = str(normalized.get("task") or "Task")
    normalized["status"] = str(normalized.get("status") or "success")
    normalized["outcome"] = str(normalized.get("outcome") or normalized["status"])
    normalized["scope"] = str(normalized.get("scope") or "")
    normalized["summary"] = str(normalized.get("summary") or "")
    normalized["details"] = normalized_details
    try:
        normalized["duration_seconds"] = float(normalized.get("duration_seconds") or 0)
    except Exception:
        normalized["duration_seconds"] = 0.0
    if is_run_history:
        normalized["is_run_history"] = True
    else:
        normalized.pop("is_run_history", None)
    return normalized


def _trim_summary_entries(entries: list[dict], max_entries: int = TASK_ACTIVITY_MAX_ENTRIES) -> list[dict]:
    capped = max(1, int(max_entries or TASK_ACTIVITY_MAX_ENTRIES))
    return entries[-capped:]


def _append_task_activity_summary(entry: dict):
    normalized = _normalize_task_entry(entry, is_run_history=bool(entry.get("is_run_history")))
    with _TASK_ACTIVITY_LOCK:
        summary_entries = []
        if TASK_ACTIVITY_SUMMARY_FILE.exists():
            for line in TASK_ACTIVITY_SUMMARY_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    summary_entries.append(json.loads(line))
                except Exception:
                    continue
        summary_entries.append(normalized)
        trimmed_entries = _trim_summary_entries(summary_entries)
        with open(TASK_ACTIVITY_SUMMARY_FILE, "w", encoding="utf-8") as fh:
            for item in trimmed_entries:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")


def record_task(task_name, status="success", scope="", summary="", details=None, duration_seconds=None):
    entry = _normalize_task_entry({
        "time": now_str(),
        "task": task_name,
        "status": status,
        "outcome": status,
        "scope": scope,
        "summary": summary,
        "details": details,
        "duration_seconds": duration_seconds,
    })
    try:
        with open(TASKS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass
    try:
        _append_task_activity_summary(entry)
    except Exception:
        pass


RUN_TASK_NAMES = {
    "Run Pipeline",
    "Run Scan Now",
    "Run Find Sources Now",
    "Run Download Themes Now",
}


def _task_name_for_pass(run_pass: int) -> str:
    return {
        1: "Scan Libraries",
        2: "Find Sources",
        3: "Download Themes",
    }.get(run_pass, "Pipeline Run")


def _run_history_entry(run: dict) -> dict:
    run_pass = int(run.get("pass") or 0)
    run_status = str(run.get("status") or run.get("outcome") or "success")
    libraries = [str(name).strip() for name in (run.get("libraries") or []) if str(name).strip()]
    return _normalize_task_entry({
        "time": run.get("time", ""),
        "task": _task_name_for_pass(run_pass),
        "status": run_status,
        "outcome": run_status,
        "scope": str(run.get("scope") or ""),
        "summary": run.get("summary", ""),
        "details": {
            "pass": run_pass,
            "stats": run.get("stats", {}),
            "return_code": run.get("return_code"),
            "stop_requested": bool(run.get("stop_requested")),
            "libraries": libraries,
        },
        "duration_seconds": run.get("duration_seconds") or 0,
        "is_run_history": True,
    }, is_run_history=True)


def _is_legacy_run_task_entry(entry: dict) -> bool:
    details = entry.get("details")
    return (
        isinstance(details, dict)
        and int(details.get("pass") or 0) > 0
        and str(entry.get("task") or "") in RUN_TASK_NAMES
    )


def _read_summary_entries() -> list[dict]:
    entries = []
    if not TASK_ACTIVITY_SUMMARY_FILE.exists():
        return entries
    for line in TASK_ACTIVITY_SUMMARY_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            entries.append(_normalize_task_entry(entry, is_run_history=bool(entry.get("is_run_history"))))
        except Exception:
            continue
    return entries


def _rebuild_task_activity_summary():
    entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            if _is_legacy_run_task_entry(entry):
                continue
            entries.append(_normalize_task_entry(entry))
    for run_file in sorted(RUNS_DIR.glob("*.json")):
        try:
            run = json.loads(run_file.read_text(encoding="utf-8"))
            entries.append(_run_history_entry(run))
        except Exception:
            continue
    trimmed_entries = _trim_summary_entries(sorted(entries, key=lambda entry: entry.get("time", "")))
    with _TASK_ACTIVITY_LOCK:
        with open(TASK_ACTIVITY_SUMMARY_FILE, "w", encoding="utf-8") as fh:
            for entry in trimmed_entries:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def load_task_entries(limit=250):
    if not TASK_ACTIVITY_SUMMARY_FILE.exists():
        _rebuild_task_activity_summary()
    entries = _read_summary_entries()
    if not entries and (TASKS_FILE.exists() or any(RUNS_DIR.glob("*.json"))):
        _rebuild_task_activity_summary()
        entries = _read_summary_entries()
    return sorted(entries, key=lambda entry: entry.get("time", ""), reverse=True)[: max(1, int(limit or 250))]


def _normalize_caller_surface(value: object, *, default: str) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"dashboard", "scheduler", "tasks"} else default


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
    last_outcome: str | None = None
    last_return_code: int | None = None
    last_summary: str = ""
    completed_at: float | None = None

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

    def start(self, *, force_pass: int = 0, explicit_libraries=None, scope_label: str = "", allow_schedule_disabled: bool = False, caller_surface: str = "tasks"):
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
                "caller_surface": caller_surface,
            },
            daemon=True,
        )
        thread.start()
        return True

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.stop_requested = True
            SERVICE_LOG.info("Run stop requested: pid=%s scope=%s libraries=%s", getattr(self.proc, "pid", None), self.scope_label or "(unspecified)", summarize_libraries(self.libraries))
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
                    message = client.get(timeout=5)
                except queue.Empty:
                    if self.active:
                        yield ": heartbeat\n\n"
                        continue
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
        return {
            "active": self.active,
            "started_at": self.started_at,
            "pass": self.current_pass,
            "last_line": self.last_line,
            "scope": self.scope_label,
            "libraries": self.libraries,
            "outcome": self.last_outcome,
            "return_code": self.last_return_code,
            "summary": self.last_summary,
            "completed_at": self.completed_at,
        }

    def _do_run(self, *, force_pass=0, explicit_libraries=None, scope_label="", allow_schedule_disabled=False, caller_surface="tasks"):
        run_log = []
        timestamp = now_str()
        run_pass = force_pass or 0
        return_code = None
        explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
        resolved_scope = scope_label or (
            explicit_libraries[0] if len(explicit_libraries) == 1 else f"{len(explicit_libraries)} selected libraries" if explicit_libraries else "scheduled libraries"
        )
        try:
            self.started_at = time.time()
            self.completed_at = None
            self.last_line = ""
            self.current_pass = run_pass
            self.stop_requested = False
            self.scope_label = resolved_scope
            self.libraries = list(explicit_libraries)
            self.last_outcome = None
            self.last_return_code = None
            self.last_summary = ""
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
            env["RUN_CALLER_SURFACE"] = caller_surface
            env["RUN_SCOPE_LABEL"] = resolved_scope
            SERVICE_LOG.info("Run subprocess start: pass=%s caller_surface=%s scope_label=%s libraries=%s command=%s", force_pass or 0, caller_surface, resolved_scope, summarize_libraries(explicit_libraries), f"{sys.executable} -m {SCRIPT_MODULE}")
            proc = subprocess.Popen([sys.executable, "-m", SCRIPT_MODULE], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
            self.proc = proc
            SERVICE_LOG.info("Run subprocess pid: pid=%s pass=%s caller_surface=%s scope_label=%s", proc.pid, force_pass or 0, caller_surface, resolved_scope)
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
            SERVICE_LOG.info("Run subprocess exit: pid=%s return_code=%s", proc.pid, return_code)
        except Exception as exc:
            message = f"[ERROR] {exc}"
            run_log.append(message)
            self.broadcast(message)
            SERVICE_LOG.error("Run subprocess failed before completion: pass=%s caller_surface=%s scope_label=%s error=%s", force_pass or 0, caller_surface, resolved_scope, exc)
        finally:
            stop_requested = self.stop_requested
            if return_code == 0 and not stop_requested:
                outcome = "success"
            elif stop_requested:
                outcome = "stopped"
            else:
                outcome = "error"
            pid = getattr(self.proc, "pid", None)
            self.proc = None
            duration = time.time() - self.started_at if self.started_at else None
            summary = next((line for line in reversed(run_log) if any(marker in line for marker in ["complete", "caught up", "nothing to do", "processed", "STOP", "ERROR"])), "No output")
            self.last_outcome = outcome
            self.last_return_code = return_code
            self.last_summary = summary
            self.completed_at = time.time()
            self.stop_requested = False
            SERVICE_LOG.info("Run subprocess outcome: pid=%s pass=%s caller_surface=%s scope_label=%s return_code=%s duration_sec=%s outcome=%s", pid, force_pass or 0, caller_surface, resolved_scope, return_code, f"{duration:.3f}" if duration is not None else "n/a", outcome)
            self.broadcast("__DONE__")
            self.active = False
            stats = parse_run_stats(run_log)
            run_file = RUNS_DIR / (timestamp.replace(":", "-").replace(" ", "_") + ".json")
            run_record = {
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
            }
            run_file.write_text(json.dumps(run_record))
            try:
                _append_task_activity_summary(_run_history_entry(run_record))
            except Exception:
                pass
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


# ── Ledger/theme operations and app-facing service helpers ──────────────────


def _logic_modules():
    import web.ledger as ledger
    import web.tasks as tasks
    import web.themes as themes

    return ledger, tasks, themes


def is_authorized_api_request(path: str, headers: dict, args: dict) -> bool:
    _, tasks, _ = _logic_modules()
    return tasks.is_authorized_api_request(path, headers, args)


def load_template() -> str:
    _, tasks, _ = _logic_modules()
    return tasks.load_template()


def load_ui_terminology() -> dict:
    _, tasks, _ = _logic_modules()
    return tasks.load_ui_terminology()


def movie_bio_payload(rating_key: str, library: str) -> dict:
    _, _, themes = _logic_modules()
    return themes.movie_bio_payload(rating_key, library)


def require_library_name(library: str) -> str:
    ledger, _, _ = _logic_modules()
    return ledger.require_library_name(library)


def get_config_payload():
    cfg = load_config()
    cfg.pop("ui_token", None)
    return cfg


def post_config_payload(incoming):
    if not isinstance(incoming, dict):
        return {"ok": False, "error": "validation_failed", "errors": [config_error("body", "invalid_type", "Expected a JSON object.")]}, 400
    _, tasks, _ = _logic_modules()
    existing = load_config()
    candidate = dict(existing)
    candidate.update(incoming)
    normalized, errors = normalize_config(candidate, for_save=True)
    if errors:
        return {"ok": False, "error": "validation_failed", "errors": errors}, 400
    tasks.save_config(normalized)
    scheduler_fields = {
        "cron_schedule",
        "schedule_enabled",
        "schedule_libraries",
        "schedule_step1",
        "schedule_step2",
        "schedule_step3",
        "schedule_test_limit",
        "auto_approve",
        "search_only_golden",
    }
    scheduler_result = None
    if tasks.scheduler_managed_via_cron() and any(existing.get(field) != normalized.get(field) for field in scheduler_fields):
        scheduler_result = tasks.refresh_scheduler(normalized)
        if not scheduler_result.get("ok", False):
            return {
                "ok": False,
                "error": "scheduler_refresh_failed",
                "message": scheduler_result.get("error") or scheduler_result.get("detail") or "Failed to refresh scheduler.",
                "config_saved": True,
                "scheduler": scheduler_result,
                "config": {key: value for key, value in normalized.items() if key != "ui_token"},
            }, 500
    return {
        "ok": True,
        "config": {key: value for key, value in normalized.items() if key != "ui_token"},
        "scheduler": scheduler_result or tasks.active_scheduler_source(),
    }, 200


def status_model_payload():
    return {
        "statuses": list(STATUS_ORDER),
        "manual_transitions": {status: list(MANUAL_STATUS_TRANSITIONS.get(status, ())) for status in STATUS_ORDER},
        "manual_any": ["UNMONITORED"],
    }


def test_plex_payload(data):
    try:
        return integrations.test_plex((data.get("url", "") or "").rstrip("/"), data.get("token", ""))
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:120]}


def plex_libraries_payload(data):
    try:
        return {"ok": True, "libraries": integrations.list_plex_libraries((data.get("url", "") or "").rstrip("/"), data.get("token", ""))}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:120]}


def test_tmdb_payload(data):
    try:
        return integrations.test_tmdb_key((data or {}).get("key", ""))
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:120]}


def test_golden_source_payload(data):
    ledger, _, _ = _logic_modules()
    try:
        url = data.get("url") or load_config().get("golden_source_url", "")
        normalized_url, rows, fetch_ms, fetch_mode = ledger.fetch_golden_source_catalog(url)
        if not rows:
            return {"ok": False, "error": "CSV loaded but no usable rows found (need tmdb_id column)"}
        return {
            "ok": True,
            "source_url": normalized_url,
            "rows": len(rows),
            "fetch_ms": fetch_ms,
            "fetch_mode": fetch_mode,
            "required_columns": list(GOLDEN_SOURCE_REQUIRED_COLUMNS),
            "optional_columns": list(GOLDEN_SOURCE_OPTIONAL_COLUMNS),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:180]}


def list_cookies_payload():
    config_dir = Path("/app/config")
    files = [str(path) for path in config_dir.glob("*.txt")] if config_dir.exists() else []
    return {"files": files}


def api_health_payload(mode: str = "lite"):
    _, tasks, _ = _logic_modules()
    return tasks.api_health_payload(mode=mode)


def get_ledger_payload(library: str):
    ledger, _, _ = _logic_modules()
    library_name = ledger.require_library_name(library)
    return load_ledger(ledger_path_for(library_name))


def patch_ledger_payload(key: str, library: str, updates: dict):
    ledger, _, _ = _logic_modules()
    library_name = ledger.require_library_name(library)
    path = ledger_path_for(library_name)
    rows = load_ledger(path)
    for row in rows:
        if row.get("rating_key") == key:
            saved_row, error = ledger.save_ledger_row_updates(row, updates, default_notes="Edited via web UI")
            if error:
                return error, 400
            save_ledger(path, rows)
            return {"ok": True, "row": ledger.ledger_row_response(saved_row)}, 200
    return {"error": "not found"}, 404


def save_manual_source_payload(data: dict):
    ledger, _, _ = _logic_modules()
    key = str(data.get("rating_key", "") or "").strip()
    library = str(data.get("library", "") or "").strip()
    url = str(data.get("url", "") or "").strip()
    target_status = str(data.get("target_status", data.get("status", "")) or "").strip().upper()
    if not key:
        return {"ok": False, "error": "Missing rating_key"}, 400
    if not library:
        return {"ok": False, "error": "Missing library"}, 400
    if not url:
        return {"ok": False, "error": "Missing url"}, 400
    if not target_status:
        return {"ok": False, "error": "Missing target_status"}, 400

    path = ledger_path_for(library)
    rows = load_ledger(path)
    row = next((item for item in rows if str(item.get("rating_key", "") or "").strip() == key), None)
    if not row:
        return {"ok": False, "error": "not found"}, 404

    saved_row, error = ledger.save_ledger_row_updates(row, {
        "url": url,
        "start_offset": data.get("start_offset", "0"),
        "notes": data.get("notes", ""),
        "status": target_status,
    })
    if error:
        error["library"] = library
        return error, 400
    save_ledger(path, rows)
    return {"ok": True, "row": ledger.ledger_row_response(saved_row)}, 200


def bulk_ledger_payload(library: str, data: dict):
    ledger, _, _ = _logic_modules()
    library_name = ledger.require_library_name(library)
    path = ledger_path_for(library_name)
    keys = set(data.get("keys", []))
    status = str(data.get("status", "") or "").upper()
    rows = load_ledger(path)
    count = 0
    for row in rows:
        if row.get("rating_key") in keys:
            current = str(row.get("status", "")).upper()
            error = ledger.status_validation_error(row, status)
            if error:
                error["rating_key"] = row.get("rating_key", "")
                error["title"] = row.get("title") or row.get("plex_title") or ""
                error["requested_keys"] = len(keys)
                return error, 400
            row["status"] = status
            row["last_updated"] = now_str()
            row["notes"] = f"Bulk {current}->{status} via web UI"
            count += 1
    save_ledger(path, rows)
    return {"ok": True, "updated": count, "skipped": len(keys) - count}, 200


def clear_selected_sources_payload(library: str, data: dict):
    ledger, _, _ = _logic_modules()
    library_name = ledger.require_library_name(library or data.get("library", ""))
    path = ledger_path_for(library_name)
    keys = [str(key) for key in (data.get("keys", []) or []) if str(key).strip()]
    if not keys:
        return {"ok": False, "error": "No ledger rows selected"}, 400
    rows = load_ledger(path)
    summary = ledger.clear_source_urls_for_rows(rows, keys=keys, note="Source URL cleared via Theme Manager", now=now_str())
    missing_keys = sorted(set(keys) - {str(row.get("rating_key", "") or "") for row in rows})
    if summary["cleared"]:
        save_ledger(path, rows)
    summary["missing_keys"] = missing_keys
    summary["library"] = library_name
    return {"ok": True, "summary": summary}, 200


def import_golden_source_payload(data: dict):
    ledger, _, _ = _logic_modules()
    return ledger.golden_source_import_summary(data)


def trim_theme_payload(data: dict):
    _, _, themes = _logic_modules()
    return themes.trim_theme_payload(data)


def delete_theme_payload(data: dict):
    _, _, themes = _logic_modules()
    return themes.delete_theme_payload(data)


def download_now_payload(data: dict):
    _, _, themes = _logic_modules()
    return themes.download_now_payload(data)


def sync_library_themes_payload(library: str):
    _, _, themes = _logic_modules()
    return themes.sync_library_themes_payload(library)


def theme_file_path_for(folder: str):
    ledger, _, _ = _logic_modules()
    cfg = load_config()
    if not ledger.is_allowed_folder(folder, ledger.get_media_roots(cfg)):
        return None, {"error": "forbidden"}, 403
    path = Path(folder) / cfg.get("theme_filename", "theme.mp3")
    if not path.exists():
        return None, {"error": "not found"}, 404
    return path, None, 200


def plex_poster_payload(rating_key: str):
    cfg = load_config()
    plex_url = cfg.get("plex_url", "").rstrip("/")
    plex_token = cfg.get("plex_token", "")
    if not plex_url or not plex_token:
        return None
    try:
        return integrations.fetch_plex_poster(rating_key, plex_url, plex_token)
    except Exception:
        return None


def tmdb_poster_payload(title: str, year: str, size: str):
    if not title:
        return None
    tmdb_key = load_config().get("tmdb_api_key", "")
    if not tmdb_key:
        return None
    return integrations.tmdb_poster_url(title, year, tmdb_key, size=size)


def tmdb_lookup_payload(title: str, year: str):
    title = (title or "").strip()
    year = (year or "").strip()
    if not title:
        return {"ok": False, "error": "missing title"}, 400
    tmdb_key = load_config().get("tmdb_api_key", "")
    if not tmdb_key:
        return {"ok": False, "error": "missing tmdb key"}, 400
    data = integrations.tmdb_lookup(title, year, tmdb_key)
    if not data:
        return {"ok": False, "error": "not found"}, 404
    return {"ok": True, **data}, 200


def youtube_search_payload(data: dict):
    query = data.get("query", "")
    if not query:
        return {"ok": False, "error": "No query"}
    try:
        cfg = load_config()
        return {"ok": True, "results": integrations.youtube_search(query, cfg.get("cookies_file", "") or None)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:150]}


def preview_url_payload(data: dict):
    url = data.get("url", "")
    if not url:
        return {"ok": False, "error": "No URL provided"}
    try:
        cfg = load_config()
        stream_url = integrations.preview_stream_url(url, cfg.get("cookies_file", "") or None)
        key = integrations.cache_preview_stream(url, stream_url)
        return {"ok": True, "audio_url": f"/api/preview/proxy/{key}"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "URL extraction timed out"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:150]}


def proxy_preview_payload(key: str, range_header: str):
    stream_url = integrations.get_cached_preview_stream(key)
    if not stream_url:
        return None, {"error": "no stream"}, 404
    try:
        response = integrations.stream_remote_audio(stream_url, range_header)
        headers = {"Content-Type": response.headers.get("Content-Type", "audio/webm"), "Accept-Ranges": "bytes"}
        if "Content-Length" in response.headers:
            headers["Content-Length"] = response.headers["Content-Length"]
        if "Content-Range" in response.headers:
            headers["Content-Range"] = response.headers["Content-Range"]
        return response, headers, response.status_code
    except Exception as exc:
        return None, {"error": str(exc)[:150]}, 500


def trigger_pass_payload(pass_num: int, data: dict):
    if pass_num not in (1, 2, 3):
        return {"error": "pass must be 1, 2, or 3"}, 400
    libraries = data.get("libraries")
    if libraries is not None and not isinstance(libraries, list):
        return {"error": "libraries must be an array"}, 400
    explicit_libraries = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    library = str(data.get("library") or "").strip()
    if library:
        explicit_libraries = [library]
    caller_surface = _normalize_caller_surface(data.get("caller_surface"), default="tasks")
    scope_label = str(data.get("scope_label") or "").strip()
    SERVICE_LOG.info("Run request received: pass=%s libraries=%s scope_label=%s caller_surface=%s", pass_num, summarize_libraries(explicit_libraries), scope_label or "(auto)", caller_surface)
    if not RUN_MANAGER.start(force_pass=pass_num, explicit_libraries=explicit_libraries, scope_label=scope_label, caller_surface=caller_surface):
        return {"error": "run in progress"}, 409
    return {"ok": True}, 200


def trigger_schedule_now_payload(data: dict):
    libraries = data.get("libraries")
    if libraries is not None and not isinstance(libraries, list):
        return {"error": "libraries must be an array"}, 400

    cfg = load_config()
    enabled_libraries = []
    for lib in cfg.get("libraries", []):
        name = str(lib.get("name") or "").strip()
        if name and lib.get("enabled", True) and name not in enabled_libraries:
            enabled_libraries.append(name)

    requested_libraries = []
    for name in (libraries or []):
        normalized = str(name or "").strip()
        if normalized and normalized not in requested_libraries:
            requested_libraries.append(normalized)

    invalid_libraries = [name for name in requested_libraries if name not in enabled_libraries]
    if invalid_libraries:
        return {"error": "Requested libraries must reference enabled configured libraries", "invalid_libraries": invalid_libraries}, 400

    configured = []
    for name in (cfg.get("schedule_libraries") or []):
        normalized = str(name or "").strip()
        if normalized and normalized not in configured:
            configured.append(normalized)

    explicit_libraries = list(requested_libraries)
    if not explicit_libraries:
        explicit_libraries = [name for name in configured if name in enabled_libraries] or list(enabled_libraries)
    if not explicit_libraries:
        return {"error": "No enabled libraries available for the scheduler", "libraries": []}, 400

    scope_label = str(data.get("scope_label") or "").strip() or (
        explicit_libraries[0] if len(explicit_libraries) == 1 else f"{len(explicit_libraries)} scheduled libraries"
    )
    caller_surface = _normalize_caller_surface(data.get("caller_surface"), default="scheduler")
    SERVICE_LOG.info("Run request received: pass=%s libraries=%s scope_label=%s caller_surface=%s", 0, summarize_libraries(explicit_libraries), scope_label, caller_surface)
    if not RUN_MANAGER.start(force_pass=0, explicit_libraries=explicit_libraries, scope_label=scope_label, allow_schedule_disabled=True, caller_surface=caller_surface):
        return {"error": "run in progress"}, 409
    return {"ok": True, "libraries": explicit_libraries, "scope_label": scope_label}, 200


def stop_run_payload():
    if RUN_MANAGER.stop():
        return {"ok": True}, 200
    return {"ok": False, "error": "No run in progress"}, 200


def history_payload():
    return RUN_MANAGER.history()


def tasks_history_payload(limit: int):
    return load_task_entries(limit=limit)


def export_golden_source_csv_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.export_golden_source_csv_payload(data)


def export_candidate_csv_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.export_candidate_csv_payload(data)


def task_download_path(filename: str):
    _, tasks, _ = _logic_modules()
    safe = Path(filename).name
    path = tasks.EXPORTS_DIR / safe
    if not path.exists():
        return None
    return path


def cleanup_logs_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.cleanup_logs_payload(data)


def prune_task_history_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.prune_task_history_payload(data)


def tasks_refresh_themes_payload(data: dict):
    _, _, themes = _logic_modules()
    library = data.get("library", "")
    payload, status = themes.sync_library_themes_payload(library)
    record_task("Refresh Local Theme Detection", "success" if payload.get("ok") else "error", library, f"Updated {payload.get('updated', 0)} rows", {"library": library, **payload})
    return payload, status


def sqlite_maintenance_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.sqlite_maintenance_payload(data)


def clear_all_source_urls_payload(data: dict):
    _, tasks, _ = _logic_modules()
    return tasks.clear_all_source_urls_payload(data)


def run_status_payload():
    return RUN_MANAGER.status()
