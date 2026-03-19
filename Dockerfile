# ──────────────────────────────────────────────────────────────────────────────
#  Media Tracks — Dockerfile
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# System deps: ffmpeg for audio conversion, cron for scheduling, curl for yt-dlp
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        cron \
        curl \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp — latest release binary (more up-to-date than pip package)
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp \
        -o /usr/local/bin/yt-dlp \
    && chmod +x /usr/local/bin/yt-dlp

# Python packages
RUN pip install --no-cache-dir pyyaml requests flask

# App directories
RUN mkdir -p /app/script /app/config /app/logs /app/web /app/shared /app/logs/runs

# App files
COPY script/media_tracks.py  /app/script/media_tracks.py
COPY script/entrypoint.sh    /app/script/entrypoint.sh
COPY web/app.py              /app/web/app.py
COPY web/template.html       /app/web/template.html
COPY web/ui_terminology.yaml /app/web/ui_terminology.yaml
COPY shared/storage.py       /app/shared/storage.py
RUN chmod +x /app/script/entrypoint.sh

ENV CONFIG_PATH=/app/config/config.yaml

EXPOSE 8080

ENTRYPOINT ["/app/script/entrypoint.sh"]
