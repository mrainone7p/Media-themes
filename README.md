# Media Tracks

A self-hosted Plex theme manager. Scans your Plex libraries, finds theme sources, lets you review and approve them, then downloads and saves `theme.mp3` files directly into your media folders.

> UI note: newer builds replace the legacy **History** page with a unified **Tasks** page (run tasks, exports, maintenance, and task history).

## Quick start

### 1. Pull the image

```bash
docker pull ghcr.io/mrainone7p/media-tracks:beta
```

### 2. Create your config directory

```bash
mkdir -p /your/path/media-tracks/config
mkdir -p /your/path/media-tracks/logs
```

### 3. Create `config/config.yaml`

```yaml
plex_url: http://192.168.1.x:32400
plex_token: your-plex-token
tmdb_api_key: your-tmdb-key
ui_token: ''            # optional — set to lock the UI behind a token

media_roots:
  - /media

libraries:
  - name: Movies
    type: movie
    enabled: true

audio_format: mp3
quality_profile: high
theme_filename: theme.mp3
max_theme_duration: 60
mode: manual

golden_source_url: 
# Optional Golden Source performance tuning
# golden_source_cache_ttl_sec: 1800
# golden_source_resolve_tmdb: false
cron_schedule: 0 3 * * *
schedule_enabled: false
```

### 4. Create `docker-compose.yml`

```yaml
services:
  media-tracks:
    image: ghcr.io/mrainone7p/media-tracks:beta
    container_name: media-tracks
    restart: unless-stopped

    ports:
      - "8182:8080"

    environment:
      CONFIG_PATH: /app/config/config.yaml

    volumes:
      - /your/path/media-tracks/config:/app/config
      - /your/path/media-tracks/logs:/app/logs
      - /your/media/library:/media   # must match media_roots in config

    mem_limit: 512m
    cpus: "1.0"
```

### 5. Run

```bash
docker compose up -d
```

Open `http://your-host:8182` in a browser.

---

## Configuration

| Key | Description |
|-----|-------------|
| `plex_url` | Full URL to your Plex server including port |
| `plex_token` | Your Plex authentication token |
| `tmdb_api_key` | Free API key from [themoviedb.org](https://www.themoviedb.org/settings/api) |
| `ui_token` | Optional token to protect the web UI |
| `media_roots` | List of host paths where your media lives (inside the container) |
| `libraries` | List of Plex library names to manage |
| `theme_filename` | Output filename — default `theme.mp3` |
| `max_theme_duration` | Trim downloads to this many seconds (0 = no limit) |
| `golden_source_url` | URL (or local file path inside container) to a curated CSV of known-good theme sources |
| `golden_source_cache_ttl_sec` | Seconds to reuse cached Golden Source CSV before re-fetching (default `1800`) |
| `golden_source_resolve_tmdb` | When `true`, import may call TMDB for rows missing `tmdb_id` (slower but can increase matches) |
| `cron_schedule` | When automated runs fire — standard cron syntax |
| `schedule_enabled` | `true` to enable automated runs |

## Golden Source CSV format

The curated source CSV must have these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `tmdb_id` | ✓ | TMDB movie ID |
| `source_url` | ✓ | Direct audio source URL |
| `title` | | Movie title (used for fallback matching) |
| `year` | | Release year |
| `start_offset` | | Trim start in seconds |
| `verified` | | Whether source has been verified |
| `updated_at` | | Last update date |
| `notes` | | Any notes |

## Building locally

```bash
git clone https://github.com/mrainone7p/Media-themes.git -b beta
cd Media-themes
docker build -t media-tracks:local .
```

## Project structure

```
├── Dockerfile
├── script/
│   ├── media_tracks.py    # three-pass pipeline worker
│   └── entrypoint.sh      # container startup
├── web/
│   ├── app.py             # Flask API + web server
│   └── template.html      # single-file frontend
└── shared/
    └── storage.py         # shared SQLite storage layer
```
