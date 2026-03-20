# Media Tracks

A self-hosted Plex theme manager. Scans your Plex libraries, finds theme sources, lets you review and approve them, then downloads and saves `theme.mp3` files directly into your media folders.

> UI note: current navigation uses **Configuration**, **Library**, **Schedule**, and **Tasks**.

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
      - "8182:8182"

    environment:
      CONFIG_PATH: /app/config/config.yaml
      WEB_PORT: 8182

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

The container listens on `WEB_PORT`, which defaults to `8182`. Keep the right-hand side of the port mapping aligned with that value. For example, if you change `WEB_PORT` to `9000`, update the mapping to `"8182:9000"` (or choose another host port if preferred) and open `http://your-host:9000`.

---


## UI terminology

User-facing labels are centralized in `web/ui_terminology.yaml`.

- Navigation: `Configuration`, `Library`, `Schedule`, `Tasks`
- Status labels: `Unmonitored`, `Missing`, `Staged`, `Approved`, `Available`, `Failed`
- Shared action/toast wording is also defined there so bulk actions, button labels, and success messages stay consistent with page copy.

## Status key and pipeline semantics

The pipeline uses a single status model everywhere in the worker logs, schedule page, library table, and task history.

- `Unmonitored`: title is excluded from automation until you re-enable it.
- `Missing`: no local theme file is available yet, so the title is queued for source discovery.
- `Staged`: a source URL has been saved and is waiting for approval.
- `Approved`: the staged source is approved for the next download run.
- `Available`: a local `theme.mp3` file exists and the title is complete.
- `Failed`: source discovery or download hit an error and needs review before retrying.

There is **no persisted `REMOVED` status**. If a title disappears from Plex, the next scan removes that row from the library ledger instead of keeping a hidden terminal state.

The three pipeline steps follow that same vocabulary:

1. **Scan Libraries** updates titles to `Missing` or `Available` based on what already exists on disk.
2. **Find Theme Sources** searches only `Missing` titles and moves successful matches to `Staged`.
3. **Download Themes** downloads `Approved` titles and marks successful results as `Available`.

When Step 2 cannot find a usable source, the title stays `Missing`. When Step 2 or Step 3 encounters a non-retryable problem, the title can move to `Failed`. Clearing a saved source URL moves the title back to `Missing`, except `Unmonitored` and `Failed` items keep those states, and titles with a local theme remain `Available`.

## Mobile & responsive UI behavior

The web UI now uses a single responsive layout tuned for phones and tablets (no separate mobile app/view to maintain):

- On small screens, the global page navigation sidebar is repositioned to a **fixed bottom navigation bar**.
- Page spacing and card padding are reduced on mobile to avoid cramped content and excessive whitespace.
- Popups/modals switch to a mobile-friendly bottom-sheet style with full-width layout and safer height limits.
- Toast notifications move above the bottom nav so they do not overlap navigation controls.

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
| `source_url` | ✓ | Direct audio source URL (maps to DB `golden_source_url`) |
| `title` | | Movie title (used for fallback matching) |
| `year` | | Release year |
| `start_offset` | | Trim start in seconds (maps to DB `golden_source_offset`) |
| `updated_at` | | Last update date |
| `notes` | | Any notes |

Notes:
- Golden Source CSV fields are imported into Golden Source DB fields (`golden_source_url`, `golden_source_offset`).
- User-selected `url` and `start_offset` values are not imported from the Golden Source CSV.

## Building locally

```bash
git clone https://github.com/mrainone7p/Media-themes.git -b beta
cd Media-themes
docker build -t media-tracks:local .
```

## Python entrypoints

Use package-style startup from the repository root so local runs, tests, and Docker all resolve imports the same way:

```bash
python -m web.app
python -m script.media_tracks
```

## Project structure

```
├── Dockerfile             # canonical container build file
├── script/
│   ├── __init__.py        # worker package marker for python -m
│   ├── entrypoint.sh      # container startup
│   └── media_tracks.py    # worker pipeline: scan, source discovery, downloads
├── shared/
│   ├── __init__.py        # shared package marker
│   ├── file_utils.py      # filesystem helpers for atomic writes/replaces
│   ├── golden_source_csv.py # Golden Source CSV parsing/validation helpers
│   ├── storage.py         # SQLite/database access, ledger persistence, status rules
│   └── yt_dlp_utils.py    # yt-dlp command helpers shared by worker/web code
├── web/
│   ├── __init__.py        # web package marker
│   ├── app.py             # Flask app and canonical API route definitions
│   ├── services.py        # service layer for config, run orchestration, and API payloads
│   ├── tasks.py           # scheduler, maintenance, exports, health, and UI helper services
│   ├── ledger.py          # library ledger/theme state operations and Golden Source helpers
│   ├── integrations.py    # Plex, TMDB, and external integration helpers
│   ├── themes.py          # theme file operations such as trim/delete/download-now helpers
│   ├── logic.py           # legacy compatibility wrapper around consolidated services
│   ├── config_logic.py    # compatibility wrapper for older config imports
│   ├── run_logic.py       # compatibility wrapper for older run imports
│   ├── template.html      # single HTML shell for the frontend pages/navigation
│   ├── ui_terminology.yaml # centralized user-facing labels/copy
│   └── static/js/
│       ├── app.js         # shared frontend bootstrapping and Configuration page behavior
│       ├── library.js     # Library page interactions
│       ├── schedule.js    # Schedule/Run page interactions and progress UI
│       └── tasks.js       # Tasks page interactions, exports, and maintenance UI
├── tests/
│   └── ...                # regression coverage for storage, worker, and run behavior
└── docs/
    └── ...                # workflow/status design notes
```

Ownership after the simplification is:

- **Routes:** `web/app.py`
- **Services / orchestration:** `web/services.py`
- **Worker logic:** `script/media_tracks.py`
- **Storage / ledger persistence:** `shared/storage.py`
- **Frontend pages:** `web/template.html` with page-specific behavior in `web/static/js/app.js`, `web/static/js/library.js`, `web/static/js/schedule.js`, and `web/static/js/tasks.js`
