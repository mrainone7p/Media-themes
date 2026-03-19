#!/bin/sh
# entrypoint.sh — Media Tracks
# Starts the Flask web UI and the cron daemon without kicking off a startup scan.

set -e

CONFIG_FILE="${CONFIG_PATH:-/app/config/config.yaml}"

# Read cron schedule from config
CRON_SCHEDULE=$(python3 -c "
import yaml, sys
try:
    with open('$CONFIG_FILE') as f:
        cfg = yaml.safe_load(f)
    print(cfg.get('cron_schedule', '0 3 * * *'))
except Exception:
    print('0 3 * * *')
")

echo "============================================="
echo "  Media Tracks"
echo "============================================="
echo "  Config   : $CONFIG_FILE"
echo "  Schedule : $CRON_SCHEDULE"
echo "  Web UI   : http://localhost:8080"
echo "============================================="

# RUN_ONCE mode — used for one-shot Docker runs / testing
if [ "${RUN_ONCE}" = "true" ]; then
    echo "[INFO] RUN_ONCE=true — running once then exiting"
    python3 /app/script/media_tracks.py
    exit 0
fi

# Start Flask web UI in background (log to container stdout)
echo "[INFO] Starting web UI on port 8080..."
python3 /app/web/app.py >> /proc/1/fd/1 2>> /proc/1/fd/2 &

# Register cron job
CRON_JOB="$CRON_SCHEDULE python3 /app/script/media_tracks.py >> /proc/1/fd/1 2>> /proc/1/fd/2"
echo "$CRON_JOB" > /etc/cron.d/media-tracks
chmod 0644 /etc/cron.d/media-tracks
crontab /etc/cron.d/media-tracks
echo "[INFO] Cron job registered: $CRON_SCHEDULE"

echo "[INFO] Startup scan disabled — waiting for scheduled or manual runs..."

echo "[INFO] Entering cron daemon mode..."
cron -f
