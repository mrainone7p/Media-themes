#!/bin/sh
# entrypoint.sh — Media Tracks
# Starts the Flask web UI and the cron daemon without kicking off a startup scan.

set -e

CONFIG_FILE="${CONFIG_PATH:-/app/config/config.yaml}"
WEB_PORT="${WEB_PORT:-8182}"

# RUN_ONCE mode — used for one-shot Docker runs / testing
if [ "${RUN_ONCE}" = "true" ]; then
    echo "[INFO] RUN_ONCE=true — running once then exiting"
    python3 /app/script/media_tracks.py
    exit 0
fi

SCHEDULER_BOOTSTRAP=$(PYTHONPATH="/app/web:/app/shared${PYTHONPATH:+:$PYTHONPATH}" python3 - <<'PY'
import json
import logic

cfg = logic.load_config()
result = logic.refresh_scheduler(cfg)
print(json.dumps({
    "ok": bool(result.get("ok")),
    "cron": result.get("active_cron") or result.get("configured_cron") or cfg.get("cron_schedule", "0 3 * * *"),
    "detail": result.get("detail", ""),
    "error": result.get("error", ""),
    "authority": result.get("authority", "cron"),
}))
PY
)

CRON_SCHEDULE=$(printf '%s' "$SCHEDULER_BOOTSTRAP" | python3 -c "import json,sys; print((json.load(sys.stdin).get('cron') or '0 3 * * *'))")
BOOTSTRAP_OK=$(printf '%s' "$SCHEDULER_BOOTSTRAP" | python3 -c "import json,sys; print('1' if json.load(sys.stdin).get('ok') else '0')")
BOOTSTRAP_DETAIL=$(printf '%s' "$SCHEDULER_BOOTSTRAP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('detail',''))")
BOOTSTRAP_ERROR=$(printf '%s' "$SCHEDULER_BOOTSTRAP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('error',''))")

echo "============================================="
echo "  Media Tracks"
echo "============================================="
echo "  Config   : $CONFIG_FILE"
echo "  Schedule : $CRON_SCHEDULE"
echo "  Web UI   : http://localhost:${WEB_PORT}"
echo "============================================="

# Start Flask web UI in background (log to container stdout)
echo "[INFO] Starting web UI on port ${WEB_PORT}..."
python3 /app/web/app.py >> /proc/1/fd/1 2>> /proc/1/fd/2 &

if [ "$BOOTSTRAP_OK" = "1" ]; then
    echo "[INFO] Scheduler bootstrap complete: $BOOTSTRAP_DETAIL"
else
    echo "[ERROR] Scheduler bootstrap failed: $BOOTSTRAP_ERROR"
    exit 1
fi

echo "[INFO] Startup scan disabled — schedule changes are applied by the web backend when configuration is saved."

echo "[INFO] Entering cron daemon mode..."
cron -f
