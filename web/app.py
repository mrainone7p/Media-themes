#!/usr/bin/env python3
"""Media Tracks web application."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from flask import Flask, Response, abort, g, jsonify, request, send_file, stream_with_context

from web import services

app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
REQUEST_LOG = logging.getLogger("media_tracks.web")
WEB_PORT = int(os.environ.get("WEB_PORT", "8182"))

if not REQUEST_LOG.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    REQUEST_LOG.addHandler(handler)
REQUEST_LOG.setLevel(logging.INFO)
REQUEST_LOG.propagate = False

_QUIET_REQUEST_RULES = {
    "/api/run/status": {"success_ms": None, "label": "Run status poll"},
    "/api/health": {"success_ms": None, "label": "Health check"},
    "/api/tasks/history": {"success_ms": None, "label": "Task history refresh"},
    "/api/ledger": {"success_ms": None, "label": "Ledger refresh"},
    "/api/cookies": {"success_ms": None, "label": "Cookie inventory"},
    "/api/movie/bio": {"success_ms": None, "label": "Movie bio lookup"},
    "/api/poster": {"success_ms": None, "label": "Poster fetch"},
    "/api/youtube/search": {"success_ms": 15000, "label": "YouTube search"},
    "/api/preview": {"success_ms": 15000, "label": "Preview extraction"},
}


def _request_log_rule(path: str) -> dict | None:
    for prefix, rule in _QUIET_REQUEST_RULES.items():
        if path == prefix or path.startswith(f"{prefix}/"):
            return rule
    return None


@app.before_request
def _auth_guard():
    g._request_started_at = time.perf_counter()
    if not services.is_authorized_api_request(request.path, request.headers, request.args):
        return jsonify({"error": "unauthorized"}), 401


@app.after_request
def _log_request(response: Response):
    started_at = getattr(g, "_request_started_at", None)
    elapsed_ms = (time.perf_counter() - started_at) * 1000 if started_at is not None else 0.0
    path = request.path or "/"
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
    return services.load_template()


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify(services.get_config_payload())


@app.route("/api/config", methods=["POST"])
def post_config():
    payload, status = services.post_config_payload(request.get_json(silent=True))
    return jsonify(payload), status


@app.route("/api/ui-terminology", methods=["GET"])
def get_ui_terminology():
    return jsonify(services.load_ui_terminology())


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
    return jsonify(services.movie_bio_payload(request.args.get("key", ""), request.args.get("library", "")))


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
    return jsonify(services.api_health_payload())


def _required_library_arg(value: str):
    try:
        return services.require_library_name(value), None
    except ValueError as exc:
        return "", (jsonify({"ok": False, "error": str(exc)}), 400)


@app.route("/api/ledger", methods=["GET"])
def get_ledger():
    library, error_response = _required_library_arg(request.args.get("library", ""))
    if error_response:
        return error_response
    return jsonify(services.get_ledger_payload(library))


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
    payload, status = services.import_golden_source_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/info")
def theme_info():
    payload, status = services.theme_info_payload(request.args.get("folder", ""))
    return jsonify(payload), status


@app.route("/api/theme/trim", methods=["POST"])
def trim_theme():
    payload, status = services.trim_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/delete", methods=["POST"])
def delete_theme():
    payload, status = services.delete_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/download-now", methods=["POST"])
def download_now():
    payload, status = services.download_now_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/library/sync-themes", methods=["POST"])
def sync_library_themes():
    payload, status = services.sync_library_themes_payload((request.get_json(silent=True) or {}).get("library", ""))
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


@app.route("/api/media")
def get_media():
    return jsonify(services.media_payload(request.args.get("library", ""), request.args.get("show", "with_theme"), nocache=bool(request.args.get("nocache", ""))))


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
    payload, status = services.trigger_scan_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


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
    return jsonify(services.tasks_history_payload(limit=int(request.args.get("limit", 250) or 250)))


@app.route("/api/tasks/export-golden-source", methods=["POST"])
def export_golden_source_csv():
    payload, status = services.export_golden_source_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/export-candidate-csv", methods=["POST"])
def export_candidate_csv():
    payload, status = services.export_candidate_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/download/<path:filename>")
def download_task_file(filename):
    path = services.task_download_path(filename)
    if path is None:
        abort(404)
    return send_file(str(path), as_attachment=True)


@app.route("/api/tasks/cleanup-logs", methods=["POST"])
def cleanup_logs():
    payload, status = services.cleanup_logs_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/prune-history", methods=["POST"])
def prune_task_history():
    payload, status = services.prune_task_history_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/refresh-themes", methods=["POST"])
def tasks_refresh_themes():
    payload, status = services.tasks_refresh_themes_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/sqlite-maintenance", methods=["POST"])
def sqlite_maintenance():
    payload, status = services.sqlite_maintenance_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/clear-source-urls", methods=["POST"])
def clear_all_source_urls():
    payload, status = services.clear_all_source_urls_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/run/status")
def run_status():
    return jsonify(services.run_status_payload())


def _sig_handler(sig, frame):
    services.RUN_MANAGER.cleanup()
    sys.exit(0)


signal.signal(signal.SIGTERM, _sig_handler)

if __name__ == "__main__":
    REQUEST_LOG.info("HTTP logging tuned for signal over noise: chatty status, preview, and search endpoints now log only on failure or unusually slow responses.")
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
