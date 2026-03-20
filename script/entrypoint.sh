#!/bin/sh
# entrypoint.sh — Media Tracks
# Starts the Flask web UI and the cron daemon without kicking off a startup scan.

set -e

CONFIG_FILE="${CONFIG_PATH:-/app/config/config.yaml}"
WEB_PORT="${WEB_PORT:-8182}"
SCHEDULER_AUTHORITY="${MEDIA_TRACKS_SCHEDULER_AUTHORITY:-cron}"
STARTUP_SCAN_DISABLED="true"

log_info() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [INFO] media_tracks.entrypoint: $*"
}

log_warn() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [WARNING] media_tracks.entrypoint: $*"
}

log_error() {
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [ERROR] media_tracks.entrypoint: $*"
}

# RUN_ONCE mode — used for one-shot Docker runs / testing
if [ "${RUN_ONCE}" = "true" ]; then
    log_info "RUN_ONCE=true — running once then exiting"
    python3 -m script.media_tracks
    exit 0
fi

log_info "Container/web startup: config_path=${CONFIG_FILE} web_port=${WEB_PORT} scheduler_authority=${SCHEDULER_AUTHORITY} startup_scan_disabled=${STARTUP_SCAN_DISABLED}"
log_info "Scheduler bootstrap begin: authority=${SCHEDULER_AUTHORITY} cron_file=${MEDIA_TRACKS_CRON_FILE:-/etc/cron.d/media-tracks}"

BOOTSTRAP_PAYLOAD_FILE=$(mktemp)
cleanup_bootstrap_payload() {
    rm -f "$BOOTSTRAP_PAYLOAD_FILE"
}
trap cleanup_bootstrap_payload EXIT

PYTHONPATH="/app${PYTHONPATH:+:$PYTHONPATH}" BOOTSTRAP_PAYLOAD_FILE="$BOOTSTRAP_PAYLOAD_FILE" python3 - <<'PY'
import json
import os
import web.logic as logic

cfg = logic.load_config()
result = logic.refresh_scheduler(cfg)
payload = {
    "ok": bool(result.get("ok")),
    "cron": result.get("active_cron") or result.get("configured_cron") or cfg.get("cron_schedule", "0 3 * * *"),
    "detail": result.get("detail", ""),
    "error": result.get("error", ""),
    "authority": result.get("authority", "cron"),
}
with open(os.environ["BOOTSTRAP_PAYLOAD_FILE"], "w", encoding="utf-8") as fh:
    json.dump(payload, fh)
PY

CRON_SCHEDULE=$(BOOTSTRAP_PAYLOAD_FILE="$BOOTSTRAP_PAYLOAD_FILE" python3 -c 'import json, os; print((json.load(open(os.environ["BOOTSTRAP_PAYLOAD_FILE"], encoding="utf-8")).get("cron") or "0 3 * * *"))')
BOOTSTRAP_OK=$(BOOTSTRAP_PAYLOAD_FILE="$BOOTSTRAP_PAYLOAD_FILE" python3 -c 'import json, os; print("1" if json.load(open(os.environ["BOOTSTRAP_PAYLOAD_FILE"], encoding="utf-8")).get("ok") else "0")')
BOOTSTRAP_DETAIL=$(BOOTSTRAP_PAYLOAD_FILE="$BOOTSTRAP_PAYLOAD_FILE" python3 -c 'import json, os; print(json.load(open(os.environ["BOOTSTRAP_PAYLOAD_FILE"], encoding="utf-8")).get("detail", ""))')
BOOTSTRAP_ERROR=$(BOOTSTRAP_PAYLOAD_FILE="$BOOTSTRAP_PAYLOAD_FILE" python3 -c 'import json, os; print(json.load(open(os.environ["BOOTSTRAP_PAYLOAD_FILE"], encoding="utf-8")).get("error", ""))')

echo "============================================="
echo "  Media Tracks"
echo "============================================="
echo "  Config   : $CONFIG_FILE"
echo "  Schedule : $CRON_SCHEDULE"
echo "  Web UI   : http://localhost:${WEB_PORT}"
echo "============================================="

# Start Flask web UI in background (log to container stdout)
log_info "Starting web UI on port ${WEB_PORT}"
PYTHONPATH="/app${PYTHONPATH:+:$PYTHONPATH}" python3 -m web.app >> /proc/1/fd/1 2>> /proc/1/fd/2 &

if [ "$BOOTSTRAP_OK" = "1" ]; then
    log_info "Scheduler bootstrap: $BOOTSTRAP_DETAIL"
else
    log_error "Scheduler bootstrap failed: $BOOTSTRAP_ERROR"
    exit 1
fi

log_info "Startup scan disabled — schedule changes are applied by the web backend when configuration is saved."

log_info "Entering cron daemon mode"
cron -f
