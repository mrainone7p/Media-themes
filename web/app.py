#!/usr/bin/env python3
"""Media Tracks web application."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from collections.abc import Mapping
from flask import Flask, Response, abort, g, jsonify, request, send_file, stream_with_context

from shared.logging_utils import get_project_logger
import web.services as services
import web.tasks as tasks
import web.themes as themes
import web.ledger as ledger_mod

app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
APP_LOG = get_project_logger("web.app")
REQUEST_LOG = get_project_logger("web.request")
WEB_PORT = int(os.environ.get("WEB_PORT", "8182"))


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "debug"}


REQUEST_DEBUG_LOGGING_ENABLED = _env_flag("WEB_DEBUG_REQUEST_LOGGING")


_QUIET_REQUEST_RULES = {
    "/api/run/status": {"success_ms": None, "label": "Run status poll"},
    "/api/health": {"success_ms": None, "label": "Health check"},
    "/api/dashboard/summary": {"success_ms": None, "label": "Dashboard summary refresh"},
    "/api/tasks/history": {"success_ms": None, "label": "Task history refresh"},
    "/api/ledger": {"success_ms": None, "label": "Ledger refresh"},
    "/api/cookies": {"success_ms": None, "label": "Cookie inventory"},
    "/api/movie/bio": {"success_ms": None, "label": "Movie bio lookup"},
    "/api/poster": {"success_ms": None, "label": "Poster fetch"},
    "/api/youtube/search": {"success_ms": 15000, "label": "YouTube search"},
    "/api/preview": {"success_ms": 15000, "label": "Preview extraction"},
}


_REQUEST_DEBUG_PATHS = {
    "/api/ledger",
    "/api/tasks/history",
    "/api/health",
    "/api/run/status",
}


def _request_log_rule(path: str) -> dict | None:
    for prefix, rule in _QUIET_REQUEST_RULES.items():
        if path == prefix or path.startswith(f"{prefix}/"):
            return rule
    return None


def _response_size_bytes(response: Response) -> int | None:
    content_length = response.calculate_content_length()
    if content_length is not None:
        return int(content_length)
    header_value = response.headers.get("Content-Length")
    if header_value is None:
        return None
    try:
        return int(header_value)
    except (TypeError, ValueError):
        return None


def _set_request_debug_stats(**stats):
    current = dict(getattr(g, "_request_debug_stats", {}) or {})
    current.update({key: value for key, value in stats.items() if value is not None})
    g._request_debug_stats = current


def _infer_row_count(payload) -> int | None:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, Mapping):
        for key in ("rows", "items", "history", "entries", "tasks"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
    return None


@app.before_request
def _auth_guard():
    g._request_started_at = time.perf_counter()
    if not tasks.is_authorized_api_request(request.path, request.headers, request.args):
        return jsonify({"error": "unauthorized"}), 401


@app.after_request
def _log_request(response: Response):
    started_at = getattr(g, "_request_started_at", None)
    elapsed_ms = (time.perf_counter() - started_at) * 1000 if started_at is not None else 0.0
    path = request.path or "/"
    response_size = _response_size_bytes(response)
    debug_stats = dict(getattr(g, "_request_debug_stats", {}) or {})
    if response_size is not None:
        debug_stats.setdefault("response_bytes", response_size)

    if REQUEST_DEBUG_LOGGING_ENABLED and path in _REQUEST_DEBUG_PATHS:
        stat_parts = []
        row_count = debug_stats.get("row_count")
        if row_count is not None:
            stat_parts.append(f"rows={row_count}")
        if response_size is not None:
            stat_parts.append(f"bytes={response_size}")
        if debug_stats.get("component_count") is not None:
            stat_parts.append(f"components={debug_stats['component_count']}")
        if debug_stats.get("library_count") is not None:
            stat_parts.append(f"libraries={debug_stats['library_count']}")
        if debug_stats.get("mode"):
            stat_parts.append(f"mode={debug_stats['mode']}")
        if debug_stats.get("limit") is not None:
            stat_parts.append(f"limit={debug_stats['limit']}")
        if debug_stats.get("active") is not None:
            stat_parts.append(f"active={str(bool(debug_stats['active'])).lower()}")
        stats_suffix = f" [{' '.join(stat_parts)}]" if stat_parts else ""
        REQUEST_LOG.info("Request debug %s %s -> %s in %.1fms%s", request.method, path, response.status_code, elapsed_ms, stats_suffix)
        return response

    rule = _request_log_rule(path)
    is_mutating = request.method not in {"GET", "HEAD", "OPTIONS"}
    if response.status_code >= 400:
        should_log = True
    elif rule is not None:
        success_ms = rule.get("success_ms")
        should_log = bool(success_ms is not None and elapsed_ms >= success_ms)
    else:
        should_log = elapsed_ms >= 2000 or is_mutating
    if should_log:
        level = logging.WARNING if response.status_code >= 400 else logging.INFO
        if rule is not None and rule.get("label"):
            REQUEST_LOG.log(level, "%s -> %s in %.1fs [%s %s]", rule["label"], response.status_code, elapsed_ms / 1000.0, request.method, path)
        else:
            REQUEST_LOG.log(level, "%s %s -> %s in %.0fms", request.method, path, response.status_code, elapsed_ms)
    return response


@app.route("/")
def index():
    return tasks.load_template()


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(services.get_config_payload())


@app.route("/api/config", methods=["POST"])
def post_config():
    payload, status = services.post_config_payload(request.get_json(silent=True))
    return jsonify(payload), status


@app.route("/api/ui-terminology", methods=["GET"])
def get_ui_terminology():
    return jsonify(tasks.load_ui_terminology())


@app.route("/api/status-model", methods=["GET"])
def get_status_model():
    return jsonify(services.status_model_payload())


@app.route("/api/test/plex", methods=["POST"])
def test_plex():
    return jsonify(services.test_plex_payload(request.get_json(silent=True) or {}))


@app.route("/api/plex/libraries", methods=["POST"])
def plex_libraries():
    return jsonify(services.plex_libraries_payload(request.get_json(silent=True) or {}))


@app.route("/api/movie/bio")
def movie_bio():
    return jsonify(themes.movie_bio_payload(request.args.get("key", ""), request.args.get("library", "")))


@app.route("/api/test/tmdb", methods=["POST"])
def test_tmdb():
    return jsonify(services.test_tmdb_payload(request.get_json(silent=True) or {}))


@app.route("/api/test/golden-source", methods=["POST"])
def test_golden_source():
    return jsonify(services.test_golden_source_payload(request.get_json(silent=True) or {}))


@app.route("/api/cookies")
def list_cookies():
    return jsonify(services.list_cookies_payload())


@app.route("/api/health", methods=["GET"])
def api_health():
    mode = request.args.get("mode", "lite")
    payload = tasks.api_health_payload(mode)
    if REQUEST_DEBUG_LOGGING_ENABLED:
        _set_request_debug_stats(component_count=len(payload), mode=mode)
    return jsonify(payload)


@app.route("/api/dashboard/summary", methods=["GET"])
def dashboard_summary():
    return jsonify(services.dashboard_summary_payload())


def _required_library_arg(value: str):
    try:
        return ledger_mod.require_library_name(value), None
    except ValueError as exc:
        return "", (jsonify({"ok": False, "error": str(exc)}), 400)


@app.route("/api/ledger", methods=["GET"])
def get_ledger():
    library, error_response = _required_library_arg(request.args.get("library", ""))
    if error_response:
        return error_response
    payload = services.get_ledger_payload(library)
    if REQUEST_DEBUG_LOGGING_ENABLED:
        _set_request_debug_stats(row_count=_infer_row_count(payload))
    return jsonify(payload)


@app.route("/api/ledger/<key>", methods=["PATCH"])
def patch_ledger(key):
    library, error_response = _required_library_arg(request.args.get("library", ""))
    if error_response:
        return error_response
    payload, status = services.patch_ledger_payload(key, library, request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/ledger/manual-source", methods=["POST"])
def save_manual_source():
    payload, status = services.save_manual_source_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/ledger/bulk", methods=["POST"])
def bulk_ledger():
    library, error_response = _required_library_arg(request.args.get("library", ""))
    if error_response:
        return error_response
    payload, status = services.bulk_ledger_payload(library, request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/ledger/clear-sources", methods=["POST"])
def clear_selected_sources():
    data = request.get_json(silent=True) or {}
    library, error_response = _required_library_arg(request.args.get("library", "") or data.get("library", ""))
    if error_response:
        return error_response
    payload, status = services.clear_selected_sources_payload(library, data)
    return jsonify(payload), status


@app.route("/api/golden-source/import", methods=["POST"])
def import_golden_source():
    payload, status = ledger_mod.golden_source_import_summary(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/trim", methods=["POST"])
def trim_theme():
    payload, status = themes.trim_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/delete", methods=["POST"])
def delete_theme():
    payload, status = themes.delete_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/download-now", methods=["POST"])
def download_now():
    payload, status = themes.download_now_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/library/sync-themes", methods=["POST"])
def sync_library_themes():
    payload, status = themes.sync_library_themes_payload((request.get_json(silent=True) or {}).get("library", ""))
    return jsonify(payload), status


@app.route("/api/theme")
def serve_theme():
    path, payload, status = services.theme_file_path_for(request.args.get("folder", ""))
    if payload is not None:
        return jsonify(payload), status
    return send_file(str(path), mimetype="audio/mpeg", conditional=True)


@app.route("/api/poster")
def serve_poster():
    poster = services.plex_poster_payload(request.args.get("key", ""))
    if not poster:
        return "", 404
    data, content_type = poster
    return Response(data, mimetype=content_type)


@app.route("/api/poster/tmdb")
def tmdb_poster():
    url = services.tmdb_poster_payload((request.args.get("title", "") or "").strip(), (request.args.get("year", "") or "").strip(), (request.args.get("size", "") or "w342").strip())
    if not url:
        return "", 404
    return "", 302, {"Location": url, "Cache-Control": "public, max-age=86400"}


@app.route("/api/tmdb/lookup")
def tmdb_lookup():
    payload, status = services.tmdb_lookup_payload(request.args.get("title", ""), request.args.get("year", ""))
    return jsonify(payload), status


@app.route("/api/youtube/search", methods=["POST"])
def youtube_search():
    return jsonify(services.youtube_search_payload(request.get_json(silent=True) or {}))


@app.route("/api/preview", methods=["POST"])
def preview_url():
    return jsonify(services.preview_url_payload(request.get_json(silent=True) or {}))


@app.route("/api/preview/proxy/<key>")
def proxy_preview(key):
    response, payload, status = services.proxy_preview_payload(key, request.headers.get("Range", ""))
    if response is None:
        return jsonify(payload), status
    return Response(response.iter_content(chunk_size=8192), status=status, headers=payload)


@app.route("/api/run/pass/<int:pass_num>", methods=["POST"])
def trigger_pass(pass_num):
    payload, status = services.trigger_pass_payload(pass_num, request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/run/schedule-now", methods=["POST"])
def trigger_schedule_now():
    payload, status = services.trigger_schedule_now_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/run/scan", methods=["POST"])
def trigger_scan():
    # Legacy compatibility alias for older clients that still invoke scan directly.
    return trigger_pass(1)


@app.route("/api/run/stop", methods=["POST"])
def stop_run():
    payload, status = services.stop_run_payload()
    return jsonify(payload), status


@app.route("/api/run/stream")
def run_stream():
    return Response(stream_with_context(services.RUN_MANAGER.event_stream()), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/history")
def get_history():
    return jsonify(services.history_payload())


@app.route("/api/tasks/history")
def tasks_history():
    limit = int(request.args.get("limit", 250) or 250)
    payload = services.tasks_history_payload(limit=limit)
    if REQUEST_DEBUG_LOGGING_ENABLED:
        _set_request_debug_stats(row_count=_infer_row_count(payload), limit=limit)
    return jsonify(payload)


@app.route("/api/tasks/export-golden-source", methods=["POST"])
def export_golden_source_csv():
    payload, status = tasks.export_golden_source_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/export-candidate-csv", methods=["POST"])
def export_candidate_csv():
    payload, status = tasks.export_candidate_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/download/<path:filename>")
def download_task_file(filename):
    path = services.task_download_path(filename)
    if path is None:
        abort(404)
    return send_file(str(path), as_attachment=True)


@app.route("/api/tasks/cleanup-logs", methods=["POST"])
def cleanup_logs():
    payload, status = tasks.cleanup_logs_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/prune-history", methods=["POST"])
def prune_task_history():
    payload, status = tasks.prune_task_history_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/refresh-themes", methods=["POST"])
def tasks_refresh_themes():
    payload, status = services.tasks_refresh_themes_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/sqlite-maintenance", methods=["POST"])
def sqlite_maintenance():
    payload, status = tasks.sqlite_maintenance_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/clear-source-urls", methods=["POST"])
def clear_all_source_urls():
    payload, status = tasks.clear_all_source_urls_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/run/status")
def run_status():
    payload = services.run_status_payload()
    if REQUEST_DEBUG_LOGGING_ENABLED:
        _set_request_debug_stats(active=payload.get("active"), library_count=len(payload.get("libraries") or []))
    return jsonify(payload)


def _sig_handler(sig, frame):
    services.RUN_MANAGER.cleanup()
    sys.exit(0)


signal.signal(signal.SIGTERM, _sig_handler)

if __name__ == "__main__":
    from waitress import serve

    APP_LOG.info("Container/web startup: config_path=%s web_port=%s startup_scan_disabled=%s request_debug_logging=%s", services.CONFIG_PATH, WEB_PORT, True, REQUEST_DEBUG_LOGGING_ENABLED)
    APP_LOG.info("HTTP logging tuned for signal over noise: chatty status, preview, and search endpoints now log only on failure or unusually slow responses.")
    if REQUEST_DEBUG_LOGGING_ENABLED:
        APP_LOG.info("Per-request debug logging enabled for /api/ledger, /api/tasks/history, /api/health, and /api/run/status via WEB_DEBUG_REQUEST_LOGGING.")
    serve(app, host="0.0.0.0", port=WEB_PORT, threads=4)
