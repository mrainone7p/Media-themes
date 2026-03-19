#!/usr/bin/env python3
"""Media Tracks web application."""

from __future__ import annotations

import signal
import subprocess
import sys
from pathlib import Path

from flask import Flask, Response, abort, jsonify, request, send_file, stream_with_context

import integrations
import logic
from logic import RUN_MANAGER
from storage import (
    MANUAL_STATUS_TRANSITIONS,
    STATUS_ORDER,
    ledger_path_for,
    load_ledger_rows as load_ledger,
    now_str,
    save_ledger_rows as save_ledger,
)

app = Flask(__name__)


@app.before_request
def _auth_guard():
    if not logic.is_authorized_api_request(request.path, request.headers, request.args):
        return jsonify({"error": "unauthorized"}), 401


@app.route("/")
def index():
    return logic.load_template()


@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = logic.load_config()
    cfg.pop("ui_token", None)
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def post_config():
    incoming = request.get_json(silent=True)
    if not isinstance(incoming, dict):
        return jsonify({"ok": False, "error": "validation_failed", "errors": [logic.config_error("body", "invalid_type", "Expected a JSON object.")]}), 400
    current = logic.load_raw_config()
    current.update(incoming)
    normalized, errors = logic.normalize_config(current, for_save=True)
    if errors:
        return jsonify({"ok": False, "error": "validation_failed", "errors": errors}), 400
    logic.save_config(normalized)
    return jsonify({"ok": True, "config": {key: value for key, value in normalized.items() if key != "ui_token"}})


@app.route("/api/ui-terminology", methods=["GET"])
def get_ui_terminology():
    return jsonify(logic.load_ui_terminology())


@app.route("/api/status-model", methods=["GET"])
def get_status_model():
    return jsonify({
        "statuses": list(STATUS_ORDER),
        "manual_transitions": {status: list(MANUAL_STATUS_TRANSITIONS.get(status, ())) for status in STATUS_ORDER},
        "manual_any": ["UNMONITORED"],
    })


@app.route("/api/test/plex", methods=["POST"])
def test_plex():
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(integrations.test_plex((data.get("url", "") or "").rstrip("/"), data.get("token", "")))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:120]})


@app.route("/api/plex/libraries", methods=["POST"])
def plex_libraries():
    data = request.get_json(silent=True) or {}
    try:
        return jsonify({"ok": True, "libraries": integrations.list_plex_libraries((data.get("url", "") or "").rstrip("/"), data.get("token", ""))})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:120]})


@app.route("/api/movie/bio")
def movie_bio():
    return jsonify(logic.movie_bio_payload(request.args.get("key", ""), request.args.get("library", "")))


@app.route("/api/test/tmdb", methods=["POST"])
def test_tmdb():
    try:
        return jsonify(integrations.test_tmdb_key((request.get_json(silent=True) or {}).get("key", "")))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:120]})


@app.route("/api/test/golden-source", methods=["POST"])
def test_golden_source():
    data = request.get_json(silent=True) or {}
    try:
        url = data.get("url") or logic.load_config().get("golden_source_url", "")
        normalized_url, rows, fetch_ms, fetch_mode = logic.fetch_golden_source_catalog(url)
        if not rows:
            return jsonify({"ok": False, "error": "CSV loaded but no usable rows found (need tmdb_id column)"})
        return jsonify({
            "ok": True,
            "source_url": normalized_url,
            "rows": len(rows),
            "fetch_ms": fetch_ms,
            "fetch_mode": fetch_mode,
            "required_columns": ["tmdb_id", "source_url"],
            "optional_columns": ["title", "year", "start_offset", "updated_at", "notes"],
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:180]})


@app.route("/api/cookies")
def list_cookies():
    config_dir = Path("/app/config")
    files = [str(path) for path in config_dir.glob("*.txt")] if config_dir.exists() else []
    return jsonify({"files": files})


@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify(logic.api_health_payload())


@app.route("/api/ledger", methods=["GET"])
def get_ledger():
    library = request.args.get("library", "")
    path = ledger_path_for(library) if library else str(logic.LOGS_DIR / "theme_log.csv")
    return jsonify(load_ledger(path))


@app.route("/api/ledger/<key>", methods=["PATCH"])
def patch_ledger(key):
    library = request.args.get("library", "")
    path = ledger_path_for(library) if library else str(logic.LOGS_DIR / "theme_log.csv")
    rows = load_ledger(path)
    for row in rows:
        if row.get("rating_key") == key:
            saved_row, error = logic.save_ledger_row_updates(row, request.get_json(silent=True) or {}, default_notes="Edited via web UI")
            if error:
                return jsonify(error), 400
            save_ledger(path, rows)
            return jsonify({"ok": True, "row": logic.ledger_row_response(saved_row)})
    return jsonify({"error": "not found"}), 404


@app.route("/api/ledger/manual-source", methods=["POST"])
def save_manual_source():
    data = request.get_json(silent=True) or {}
    key = str(data.get("rating_key", "") or "").strip()
    library = str(data.get("library", "") or "").strip()
    url = str(data.get("url", "") or "").strip()
    target_status = str(data.get("target_status", data.get("status", "")) or "").strip().upper()
    if not key:
        return jsonify({"ok": False, "error": "Missing rating_key"}), 400
    if not library:
        return jsonify({"ok": False, "error": "Missing library"}), 400
    if not url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    if not target_status:
        return jsonify({"ok": False, "error": "Missing target_status"}), 400

    path = ledger_path_for(library)
    rows = load_ledger(path)
    row = next((item for item in rows if str(item.get("rating_key", "") or "").strip() == key), None)
    if not row:
        return jsonify({"ok": False, "error": "not found"}), 404

    saved_row, error = logic.save_ledger_row_updates(row, {
        "url": url,
        "start_offset": data.get("start_offset", "0"),
        "notes": data.get("notes", ""),
        "status": target_status,
    })
    if error:
        error["library"] = library
        return jsonify(error), 400
    save_ledger(path, rows)
    return jsonify({"ok": True, "row": logic.ledger_row_response(saved_row)})


@app.route("/api/ledger/bulk", methods=["POST"])
def bulk_ledger():
    library = request.args.get("library", "")
    path = ledger_path_for(library) if library else str(logic.LOGS_DIR / "theme_log.csv")
    data = request.get_json(silent=True) or {}
    keys = set(data.get("keys", []))
    status = str(data.get("status", "") or "").upper()
    rows = load_ledger(path)
    count = 0
    for row in rows:
        if row.get("rating_key") in keys:
            current = str(row.get("status", "")).upper()
            error = logic.status_validation_error(row, status)
            if error:
                error["rating_key"] = row.get("rating_key", "")
                error["title"] = row.get("title") or row.get("plex_title") or ""
                error["requested_keys"] = len(keys)
                return jsonify(error), 400
            row["status"] = status
            row["last_updated"] = now_str()
            row["notes"] = f"Bulk {current}->{status} via web UI"
            count += 1
    save_ledger(path, rows)
    return jsonify({"ok": True, "updated": count, "skipped": len(keys) - count})


@app.route("/api/ledger/clear-sources", methods=["POST"])
def clear_selected_sources():
    data = request.get_json(silent=True) or {}
    library = (request.args.get("library", "") or data.get("library", "") or "").strip()
    path = ledger_path_for(library) if library else str(logic.LOGS_DIR / "theme_log.csv")
    keys = [str(key) for key in (data.get("keys", []) or []) if str(key).strip()]
    if not keys:
        return jsonify({"ok": False, "error": "No ledger rows selected"}), 400
    rows = load_ledger(path)
    summary = logic.clear_source_urls_for_rows(rows, keys=keys, note="Source URL cleared via Theme Manager", now=now_str())
    missing_keys = sorted(set(keys) - {str(row.get("rating_key", "") or "") for row in rows})
    if summary["cleared"]:
        save_ledger(path, rows)
    summary["missing_keys"] = missing_keys
    summary["library"] = library
    return jsonify({"ok": True, "summary": summary})


@app.route("/api/golden-source/import", methods=["POST"])
def import_golden_source():
    payload, status = logic.golden_source_import_summary(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/info")
def theme_info():
    payload, status = logic.theme_info_payload(request.args.get("folder", ""))
    return jsonify(payload), status


@app.route("/api/theme/trim", methods=["POST"])
def trim_theme():
    payload, status = logic.trim_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/delete", methods=["POST"])
def delete_theme():
    payload, status = logic.delete_theme_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/theme/download-now", methods=["POST"])
def download_now():
    payload, status = logic.download_now_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/library/sync-themes", methods=["POST"])
def sync_library_themes():
    payload, status = logic.sync_library_themes_payload((request.get_json(silent=True) or {}).get("library", ""))
    return jsonify(payload), status


@app.route("/api/theme")
def serve_theme():
    folder = request.args.get("folder", "")
    cfg = logic.load_config()
    if not logic.is_allowed_folder(folder, logic.get_media_roots(cfg)):
        return jsonify({"error": "forbidden"}), 403
    path = Path(folder) / cfg.get("theme_filename", "theme.mp3")
    if not path.exists():
        return jsonify({"error": "not found"}), 404
    return send_file(str(path), mimetype="audio/mpeg", conditional=True)


@app.route("/api/poster")
def serve_poster():
    rating_key = request.args.get("key", "")
    cfg = logic.load_config()
    plex_url = cfg.get("plex_url", "").rstrip("/")
    plex_token = cfg.get("plex_token", "")
    if not plex_url or not plex_token:
        return "", 404
    try:
        poster = integrations.fetch_plex_poster(rating_key, plex_url, plex_token)
        if not poster:
            return "", 404
        data, content_type = poster
        return Response(data, mimetype=content_type)
    except Exception:
        return "", 404


@app.route("/api/poster/tmdb")
def tmdb_poster():
    title = (request.args.get("title", "") or "").strip()
    year = (request.args.get("year", "") or "").strip()
    size = (request.args.get("size", "") or "w342").strip()
    if not title:
        return "", 404
    tmdb_key = logic.load_config().get("tmdb_api_key", "")
    if not tmdb_key:
        return "", 404
    url = integrations.tmdb_poster_url(title, year, tmdb_key, size=size)
    if not url:
        return "", 404
    return "", 302, {"Location": url, "Cache-Control": "public, max-age=86400"}


@app.route("/api/tmdb/lookup")
def tmdb_lookup():
    title = (request.args.get("title", "") or "").strip()
    year = (request.args.get("year", "") or "").strip()
    if not title:
        return jsonify({"ok": False, "error": "missing title"}), 400
    tmdb_key = logic.load_config().get("tmdb_api_key", "")
    if not tmdb_key:
        return jsonify({"ok": False, "error": "missing tmdb key"}), 400
    data = integrations.tmdb_lookup(title, year, tmdb_key)
    if not data:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, **data})


@app.route("/api/media")
def get_media():
    return jsonify(logic.media_payload(request.args.get("library", ""), request.args.get("show", "with_theme"), nocache=bool(request.args.get("nocache", ""))))


@app.route("/api/youtube/search", methods=["POST"])
def youtube_search():
    data = request.get_json(silent=True) or {}
    query = data.get("query", "")
    if not query:
        return jsonify({"ok": False, "error": "No query"})
    try:
        cfg = logic.load_config()
        return jsonify({"ok": True, "results": integrations.youtube_search(query, cfg.get("cookies_file", "") or None)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:150]})


@app.route("/api/preview", methods=["POST"])
def preview_url():
    data = request.get_json(silent=True) or {}
    url = data.get("url", "")
    if not url:
        return jsonify({"ok": False, "error": "No URL provided"})
    try:
        cfg = logic.load_config()
        stream_url = integrations.preview_stream_url(url, cfg.get("cookies_file", "") or None)
        key = integrations.cache_preview_stream(url, stream_url)
        return jsonify({"ok": True, "audio_url": f"/api/preview/proxy/{key}"})
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "URL extraction timed out"})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:150]})


@app.route("/api/preview/proxy/<key>")
def proxy_preview(key):
    stream_url = integrations.get_cached_preview_stream(key)
    if not stream_url:
        return jsonify({"error": "no stream"}), 404
    try:
        response = integrations.stream_remote_audio(stream_url, request.headers.get("Range", ""))
        headers = {"Content-Type": response.headers.get("Content-Type", "audio/webm"), "Accept-Ranges": "bytes"}
        if "Content-Length" in response.headers:
            headers["Content-Length"] = response.headers["Content-Length"]
        if "Content-Range" in response.headers:
            headers["Content-Range"] = response.headers["Content-Range"]
        return Response(response.iter_content(chunk_size=8192), status=response.status_code, headers=headers)
    except Exception as exc:
        return jsonify({"error": str(exc)[:150]}), 500


@app.route("/api/run/pass/<int:pass_num>", methods=["POST"])
def trigger_pass(pass_num):
    if pass_num not in (1, 2, 3):
        return jsonify({"error": "pass must be 1, 2, or 3"}), 400
    data = request.get_json(silent=True) or {}
    libraries = data.get("libraries")
    if libraries is not None and not isinstance(libraries, list):
        return jsonify({"error": "libraries must be an array"}), 400
    explicit_libraries = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    library = str(data.get("library") or "").strip()
    if library:
        explicit_libraries = [library]
    if not RUN_MANAGER.start(force_pass=pass_num, explicit_libraries=explicit_libraries, scope_label=str(data.get("scope_label") or "").strip()):
        return jsonify({"error": "run in progress"}), 409
    return jsonify({"ok": True})


@app.route("/api/run/schedule-now", methods=["POST"])
def trigger_schedule_now():
    data = request.get_json(silent=True) or {}
    cfg = logic.load_config()
    enabled_libraries = [str(lib.get("name") or "").strip() for lib in cfg.get("libraries", []) if str(lib.get("name") or "").strip() and lib.get("enabled", True)]
    configured = [str(name).strip() for name in (cfg.get("schedule_libraries") or []) if str(name).strip()]
    explicit_libraries = [name for name in configured if name in enabled_libraries] or enabled_libraries
    scope_label = str(data.get("scope_label") or "").strip() or (explicit_libraries[0] if len(explicit_libraries) == 1 else f"{len(explicit_libraries)} scheduled libraries" if explicit_libraries else "scheduled libraries")
    if not RUN_MANAGER.start(force_pass=0, explicit_libraries=explicit_libraries, scope_label=scope_label, allow_schedule_disabled=True):
        return jsonify({"error": "run in progress"}), 409
    return jsonify({"ok": True, "libraries": explicit_libraries})


@app.route("/api/run/scan", methods=["POST"])
def trigger_scan():
    data = request.get_json(silent=True) or {}
    libraries = data.get("libraries")
    if libraries is not None and not isinstance(libraries, list):
        return jsonify({"error": "libraries must be an array"}), 400
    explicit_libraries = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    library = str(data.get("library") or "").strip()
    if library:
        explicit_libraries = [library]
    if not RUN_MANAGER.start(force_pass=1, explicit_libraries=explicit_libraries, scope_label=str(data.get("scope_label") or "").strip()):
        return jsonify({"error": "run in progress"}), 409
    return jsonify({"ok": True})


@app.route("/api/run/stop", methods=["POST"])
def stop_run():
    if RUN_MANAGER.stop():
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "No run in progress"})


@app.route("/api/run/stream")
def run_stream():
    return Response(stream_with_context(RUN_MANAGER.event_stream()), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/history")
def get_history():
    return jsonify(RUN_MANAGER.history())


@app.route("/api/tasks/history")
def tasks_history():
    return jsonify(logic.load_task_entries(limit=int(request.args.get("limit", 250) or 250)))


@app.route("/api/tasks/export-golden-source", methods=["POST"])
def export_golden_source_csv():
    payload, status = logic.export_golden_source_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/export-candidate-csv", methods=["POST"])
def export_candidate_csv():
    payload, status = logic.export_candidate_csv_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/download/<path:filename>")
def download_task_file(filename):
    safe = Path(filename).name
    path = logic.EXPORTS_DIR / safe
    if not path.exists():
        abort(404)
    return send_file(str(path), as_attachment=True)


@app.route("/api/tasks/cleanup-logs", methods=["POST"])
def cleanup_logs():
    payload, status = logic.cleanup_logs_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/prune-history", methods=["POST"])
def prune_task_history():
    payload, status = logic.prune_task_history_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/refresh-themes", methods=["POST"])
def tasks_refresh_themes():
    library = (request.get_json(silent=True) or {}).get("library", "")
    payload, status = logic.sync_library_themes_payload(library)
    logic.record_task("Refresh Local Theme Detection", "success" if payload.get("ok") else "error", library, f"Updated {payload.get('updated', 0)} rows", {"library": library, **payload})
    return jsonify(payload), status


@app.route("/api/tasks/sqlite-maintenance", methods=["POST"])
def sqlite_maintenance():
    payload, status = logic.sqlite_maintenance_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/tasks/clear-source-urls", methods=["POST"])
def clear_all_source_urls():
    payload, status = logic.clear_all_source_urls_payload(request.get_json(silent=True) or {})
    return jsonify(payload), status


@app.route("/api/run/status")
def run_status():
    return jsonify(RUN_MANAGER.status())


def _sig_handler(sig, frame):
    RUN_MANAGER.cleanup()
    sys.exit(0)


signal.signal(signal.SIGTERM, _sig_handler)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
