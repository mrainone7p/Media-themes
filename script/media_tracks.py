#!/usr/bin/env python3
"""
media_tracks.py — Media Tracks
─────────────────────────────────────────────────────────────────────────────
Downloads the first track of a YouTube soundtrack playlist as a theme.mp3
sidecar file for every movie in your media library.

─── Three-pass pipeline ──────────────────────────────────────────────────────

  PASS 1 — Scan  (fast, no external calls)
    • Connects to Plex, pulls full movie list
    • Checks every folder for an existing theme file
    • Creates/updates every row in the ledger
    • Marks removed movies, re-queues missing themes

  PASS 2 — Resolve  (TMDB + YouTube calls)
    • Looks up official title via TMDB for each PENDING movie
    • Searches YouTube for a soundtrack playlist, saves track 1 URL
    • Sets status → STAGED and waits for manual approval
    • Can run from existing ledger — no Plex connection required

  PASS 3 — Download  (yt-dlp)
    • Downloads theme audio for every APPROVED movie
    • Respects start_offset / end_offset to trim audio
    • Caps duration at max_theme_duration from config
    • Can run from existing ledger — no Plex connection required

─── Status flow ──────────────────────────────────────────────────────────────

  PENDING → STAGED → APPROVED → DOWNLOADED
                              → FAILED
  PENDING may move to FAILED when strict source rules do not find a valid match
  Any     → REJECTED     (permanent skip — never retried)
  Any     → IGNORED      (skip in future runs — can be reset to PENDING)
  Any     → REMOVED      (no longer in library)

─── FORCE_PASS environment variable ─────────────────────────────────────────

  Set FORCE_PASS=1  to run Pass 1 only (index/scan)
  Set FORCE_PASS=2  to run Pass 2 only (resolve URLs from existing ledger)
  Set FORCE_PASS=3  to run Pass 3 only (download APPROVED from existing ledger)
  Unset / 0         to run all passes sequentially (cron mode)

─── Structured progress ─────────────────────────────────────────────────────

  Lines prefixed with @@PROGRESS@@ are JSON objects for the GUI:
  {"pass":1,"current":5,"total":100,"title":"Movie Name","action":"scanning"}
"""

import csv
import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import yaml

# ─── Shared storage ───────────────────────────────────────────────────────────

SHARED_DIR = Path(__file__).resolve().parents[1] / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from storage import (
    LEDGER_HEADERS,
    ffprobe_duration,
    ledger_path_for as shared_ledger_path_for,
    load_ledger_map,
    save_ledger_map,
    sync_theme_cache,
)

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("media-tracks")

# Simple in-memory caches to reduce repeated calls per run
_tmdb_cache: dict = {}
_yt_cache: dict = {}


def _retry_sleep(attempt: int, base: float = 1.0):
    time.sleep(base * (2 ** attempt))


# ─── Progress ─────────────────────────────────────────────────────────────────

def emit_progress(pass_num: int, current: int, total: int, title: str, action: str, **extra):
    """Emit a structured progress event for the GUI."""
    data = {"pass": pass_num, "current": current, "total": total,
            "title": title, "action": action, **extra}
    print(f"@@PROGRESS@@{json.dumps(data)}", flush=True)


# ─── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
LOCK_PATH   = os.environ.get("LOCK_PATH",   "/app/logs/media_tracks.lock")


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    if os.environ.get("DRY_RUN", "").lower() == "true":
        cfg["dry_run"] = True
    log.info(f"Config loaded — mode: {cfg.get('mode', 'manual')}")
    return cfg


def acquire_lock(path: str):
    """Best-effort cross-process lock (Linux containers)."""
    try:
        import fcntl
    except Exception:
        return None
    lock_file = open(path, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except BlockingIOError:
        lock_file.close()
        return None


# ─── Status constants ─────────────────────────────────────────────────────────

ST_PENDING    = "PENDING"
ST_STAGED     = "STAGED"
ST_APPROVED   = "APPROVED"
ST_DOWNLOADED = "DOWNLOADED"
ST_FAILED     = "FAILED"
ST_REJECTED   = "REJECTED"
ST_REMOVED    = "REMOVED"
ST_IGNORED    = "IGNORED"


# ─── Ledger helpers ───────────────────────────────────────────────────────────

def _append_note(notes: str, addition: str) -> str:
    notes = str(notes or "").strip()
    return f"{notes} | {addition}" if notes else addition


def _coerce_status_for_row(row: dict, theme_filename: str) -> tuple[dict, bool]:
    """Apply worker-side status policy guardrails using URL + local theme presence."""
    row, _ = sync_theme_cache(row, theme_filename, probe_duration=False)

    status = row.get("status", ST_PENDING)
    has_url = bool(str(row.get("url", "")).strip())
    has_theme = bool(int(row.get("theme_exists", 0) or 0))
    coerced_reason = None

    if status in (ST_STAGED, ST_APPROVED):
        if not has_url:
            row["status"] = ST_PENDING
            coerced_reason = f"Status policy: {status} requires URL; reset to PENDING"
        elif has_theme:
            row["status"] = ST_DOWNLOADED
            coerced_reason = f"Status policy: {status} requires no local theme; promoted to DOWNLOADED"
    elif status == ST_DOWNLOADED and not has_theme:
        row["status"] = ST_PENDING
        coerced_reason = "Status policy: DOWNLOADED requires local theme; reset to PENDING"

    if coerced_reason:
        row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row["notes"] = _append_note(row.get("notes", ""), coerced_reason)
        return row, True
    return row, False

def load_ledger(path: str) -> dict:
    return load_ledger_map(path)


def save_ledger(path: str, ledger: dict):
    return save_ledger_map(path, ledger)


def ledger_path_for(library_name: str) -> str:
    return shared_ledger_path_for(library_name)


def ledger_upsert(ledger: dict, rating_key: str, plex_title: str, title: str,
                  year: str, folder: str, status: str, url: str = "",
                  start_offset=0, end_offset=0, notes: str = "", tmdb_id: str = ""):
    """Insert or update a row. Never overwrites a user-supplied URL."""
    existing = ledger.get(rating_key, {})
    ledger[rating_key] = {
        "title":          title or existing.get("title", plex_title),
        "year":           year,
        "status":         status,
        "url":            url if url else existing.get("url", ""),
        "start_offset":   str(start_offset) if start_offset else existing.get("start_offset", "0"),
        "end_offset":     str(end_offset) if end_offset else existing.get("end_offset", "0"),
        "plex_title":     plex_title,
        "folder":         folder,
        "rating_key":     rating_key,
        "tmdb_id":        str(tmdb_id) if tmdb_id else existing.get("tmdb_id", ""),
        "last_updated":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "notes":          notes,
        # Preserve cached theme metadata
        "theme_exists":   existing.get("theme_exists", 0),
        "theme_duration": existing.get("theme_duration", 0.0),
        "theme_size":     existing.get("theme_size", 0),
        "theme_mtime":    existing.get("theme_mtime", 0.0),
    }


# ─── Plex API ─────────────────────────────────────────────────────────────────

_TMDB_GUID_RE = re.compile(r"(?:themoviedb://|tmdb://)(\d+)", re.IGNORECASE)


def _extract_tmdb_id(item: dict) -> Optional[str]:
    for g in item.get("Guid", []):
        m = _TMDB_GUID_RE.search(g.get("id", ""))
        if m:
            return m.group(1)
    m = _TMDB_GUID_RE.search(item.get("guid", ""))
    return m.group(1) if m else None


def get_plex_movies(plex_url: str, token: str, library_name: str) -> list:
    base    = plex_url.rstrip("/")
    headers = {"X-Plex-Token": token, "Accept": "application/json"}

    try:
        resp = requests.get(f"{base}/library/sections", headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Cannot reach Plex at {base}: {e}") from e

    sections   = resp.json().get("MediaContainer", {}).get("Directory", [])
    section_id = next(
        (s["key"] for s in sections if s.get("title", "").lower() == library_name.lower()),
        None,
    )
    if section_id is None:
        raise RuntimeError(
            f"Library '{library_name}' not found in Plex. "
            f"Available: {[s.get('title') for s in sections]}"
        )

    try:
        resp = requests.get(
            f"{base}/library/sections/{section_id}/all",
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch movie list: {e}") from e

    movies = []
    for item in resp.json().get("MediaContainer", {}).get("Metadata", []):
        try:
            file_path = item["Media"][0]["Part"][0]["file"]
        except (KeyError, IndexError):
            log.warning(f"No file path for '{item.get('title')}' — skipping")
            continue
        movies.append({
            "rating_key": str(item["ratingKey"]),
            "plex_title": item.get("title", "Unknown"),
            "plex_year":  str(item.get("year", "")),
            "folder":     str(Path(file_path).parent),
            "tmdb_id":    _extract_tmdb_id(item),
        })

    log.info(f"Plex: {len(movies)} movies in '{library_name}'")
    return movies


# ─── TMDB API ─────────────────────────────────────────────────────────────────

TMDB_BASE = "https://api.themoviedb.org/3"

_TITLE_CLEAN_RE  = re.compile(r"[\(\[\{].*?[\)\]\}]")
_TITLE_SUFFIX_RE = re.compile(
    r"\b(director'?s cut|extended|ultimate|remastered|unrated|edition|cut)\b",
    re.IGNORECASE,
)


def normalize_title(title: str) -> str:
    t = _TITLE_CLEAN_RE.sub("", title or "")
    t = _TITLE_SUFFIX_RE.sub("", t)
    return re.sub(r"\s{2,}", " ", t).strip()


def get_tmdb_metadata(tmdb_id, plex_title: str, plex_year: str, api_key: str) -> dict:
    def _get(path, params=None):
        for attempt in range(3):
            try:
                r = requests.get(
                    f"{TMDB_BASE}{path}",
                    params={"api_key": api_key, **(params or {})},
                    timeout=10,
                )
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                if attempt < 2:
                    _retry_sleep(attempt)
                    continue
                return None

    cache_key = f"id:{tmdb_id}" if tmdb_id else f"search:{normalize_title(plex_title)}:{plex_year}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    data = None
    if tmdb_id:
        data = _get(f"/movie/{tmdb_id}")
        if data and "title" not in data:
            data = None

    if data is None:
        search = _get("/search/movie", {"query": normalize_title(plex_title), "year": plex_year})
        if search and search.get("results"):
            data = _get(f"/movie/{search['results'][0]['id']}")

    if data is None:
        result = {"title": plex_title, "year": plex_year, "tmdb_id": str(tmdb_id or "")}
        _tmdb_cache[cache_key] = result
        return result

    result = {
        "title":   data.get("title", plex_title),
        "year":    str(data.get("release_date", plex_year))[:4],
        "tmdb_id": str(data.get("id", tmdb_id or "") or ""),
    }
    _tmdb_cache[cache_key] = result
    return result


# ─── YouTube search ───────────────────────────────────────────────────────────

_SEARCH_STOPWORDS   = {"the", "a", "an", "and", "of", "for", "official", "video", "movie"}
_DIRECT_HINT_WORDS  = {"theme", "song", "score", "ost", "soundtrack"}
_PLAYLIST_HINT_WORDS = {"playlist", "soundtrack", "score", "ost", "theme"}


def _run_yt_dlp(cmd: list) -> Optional[str]:
    for attempt in range(3):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return result.stdout
            log.debug(f"yt-dlp error (attempt {attempt+1}): {result.stderr[:120]}")
        except Exception as e:
            log.debug(f"yt-dlp error (attempt {attempt+1}): {e}")
        if attempt < 2:
            _retry_sleep(attempt, base=1.5)
    return None


def _normalize_match_text(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", (text or "").lower()))


def _title_terms(title: str) -> list:
    return [
        t for t in re.findall(r"[a-z0-9]+", normalize_title(title).lower())
        if t not in _SEARCH_STOPWORDS and (len(t) > 2 or t.isdigit())
    ]


def _template_is_strict_safe(template: str, mode: str) -> bool:
    tpl = (template or "").lower()
    if "{title}" not in tpl or "{year}" not in tpl:
        return False
    needed = _PLAYLIST_HINT_WORDS if mode == "playlist" else _DIRECT_HINT_WORDS
    return any(word in tpl for word in needed)


def _candidate_matches(candidate_title: str, search_title: str, year: str, mode: str) -> bool:
    hay = _normalize_match_text(candidate_title)
    if not hay:
        return False
    for term in _title_terms(search_title):
        if term not in hay:
            return False
    if year and str(year) not in hay:
        return False
    needed = _PLAYLIST_HINT_WORDS if mode == "playlist" else _DIRECT_HINT_WORDS
    return any(word in hay for word in needed)


def _normalize_result_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return "https://www.youtube.com" + url
    if url.startswith("watch?") or url.startswith("playlist?"):
        return "https://www.youtube.com/" + url
    if "youtube.com" in url:
        return url if url.startswith("http") else "https://" + url.lstrip("/")
    return url


def _search_youtube_candidates(query: str, mode: str, cookies_file: Optional[str],
                                max_results: int = 10) -> list:
    flags = ["yt-dlp", "--no-warnings", "--quiet", "--flat-playlist"]
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    if mode == "playlist":
        target = ("https://www.youtube.com/results?search_query="
                  + urllib.parse.quote(query) + "&sp=EgIQAw%3D%3D")
        cmd = flags + ["--print", "%(title)s\t%(url)s", target]
    else:
        target = f"ytsearch{max_results}:{query}"
        cmd = flags + ["--print", "%(title)s\t%(webpage_url)s", target]

    out = _run_yt_dlp(cmd)
    if out is None:
        return []

    rows = []
    for raw in out.strip().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        if "\t" in raw:
            title, url = raw.split("\t", 1)
        else:
            title, url = "", raw
        url = _normalize_result_url(url)
        if mode == "playlist" and "list=" not in url:
            continue
        if mode == "direct" and "watch" not in url:
            continue
        rows.append({"title": title.strip(), "url": url.strip()})
        if len(rows) >= max_results:
            break
    return rows


def _resolve_playlist_first_track(playlist_url: str, cookies_file: Optional[str]) -> Optional[str]:
    flags = ["yt-dlp", "--no-warnings", "--quiet"]
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    out = _run_yt_dlp(
        flags + ["--playlist-items", "1", "--print", "webpage_url",
                 "--yes-playlist", playlist_url]
    )
    if out is None:
        return None
    for line in out.strip().splitlines():
        line = line.strip()
        if line.startswith("https://"):
            return line
    return None


def _pick_youtube_source(query: str, search_title: str, year: str, mode: str,
                         cookies_file: Optional[str], fuzzy: bool) -> tuple:
    candidates = _search_youtube_candidates(query, mode, cookies_file)
    if not candidates:
        return None, "No search results"

    selected = None
    if fuzzy:
        selected = candidates[0]
    else:
        for cand in candidates:
            if _candidate_matches(cand.get("title", ""), search_title, str(year or ""), mode):
                selected = cand
                break
        if selected is None:
            return None, "No strict title match"

    if mode == "playlist":
        track_url = _resolve_playlist_first_track(selected.get("url", ""), cookies_file)
        if not track_url:
            return None, "Playlist found but track 1 could not be resolved"
        return track_url, selected.get("title", "")

    return selected.get("url", ""), selected.get("title", "")


# ─── Golden Source ────────────────────────────────────────────────────────────

GOLDEN_SOURCE_EXPECTED_COLUMNS = [
    "tmdb_id", "title", "year", "source_url",
    "start_offset", "verified", "updated_at", "notes",
]
GOLDEN_SOURCE_REQUIRED_COLUMNS = ["tmdb_id", "source_url"]


def _normalize_golden_source_url(url: str) -> str:
    url = (url or "").strip()
    m = re.match(r"https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)", url)
    if m:
        owner, repo, branch, path = m.groups()
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    return url


def _fetch_golden_source_catalog(url: str) -> tuple:
    """Fetch Golden Source CSV. Returns (normalized_url, {tmdb_id: row_dict})."""
    normalized = _normalize_golden_source_url(url)
    if not normalized:
        raise ValueError("Golden Source URL is not configured")
    response = requests.get(normalized, timeout=20)
    response.raise_for_status()
    text = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames:
        raise ValueError("Golden Source CSV has no header row")
    fieldnames = [str(f or "").strip() for f in reader.fieldnames]
    missing = [c for c in GOLDEN_SOURCE_REQUIRED_COLUMNS if c not in fieldnames]
    if missing:
        raise ValueError(
            "Golden Source CSV is missing required column(s): " + ", ".join(missing)
        )
    rows = {}
    for row in reader:
        clean = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
        tmdb_id    = clean.get("tmdb_id", "")
        source_url = clean.get("source_url", "")
        if not tmdb_id or not source_url:
            continue
        rows[str(tmdb_id)] = {
            "source_url":   source_url,
            "start_offset": clean.get("start_offset", "0") or "0",
            "verified":     clean.get("verified", ""),
            "updated_at":   clean.get("updated_at", ""),
            "notes":        clean.get("notes", ""),
            "title":        clean.get("title", ""),
            "year":         clean.get("year", ""),
        }
    return normalized, rows


# ─── Audio helpers ────────────────────────────────────────────────────────────

def get_audio_duration(filepath: Path) -> float:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)],
            capture_output=True, text=True, timeout=15,
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def validate_audio_file(filepath: Path) -> tuple:
    if not filepath.exists():
        return False, "File missing after download"
    size = filepath.stat().st_size
    if size < 10_000:
        filepath.unlink(missing_ok=True)
        return False, f"File too small ({size} bytes)"
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "a:0",
         "-show_entries", "stream=codec_type",
         "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0 or "audio" not in result.stdout:
        filepath.unlink(missing_ok=True)
        return False, f"ffprobe rejected: {result.stderr.strip()[:120]}"
    dur = get_audio_duration(filepath)
    return True, f"{size / 1024:.1f} KB, {dur:.1f}s"


def trim_audio(filepath: Path, start_offset: int, end_offset: int,
               max_duration: int, audio_format: str) -> tuple:
    if start_offset <= 0 and end_offset <= 0 and max_duration <= 0:
        return True, "No trimming needed"
    dur = get_audio_duration(filepath)
    if dur <= 0:
        return True, "Could not determine duration, skipping trim"
    start = max(0, start_offset)
    end   = dur - max(0, end_offset) if end_offset > 0 else dur
    if max_duration > 0 and (end - start) > max_duration:
        end = start + max_duration
    if start <= 0 and end >= dur:
        return True, "No trimming needed"
    if dur <= (end - start) and start <= 0:
        return True, f"File already {dur:.1f}s, no trim needed"
    tmp = filepath.with_suffix(f".trim.{audio_format}")
    cmd = ["ffmpeg", "-y", "-i", str(filepath),
           "-ss", str(start), "-to", str(end), "-c", "copy", str(tmp)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            tmp.unlink(missing_ok=True)
            return False, f"ffmpeg trim failed: {result.stderr[:120]}"
        tmp.replace(filepath)
        new_dur = get_audio_duration(filepath)
        return True, f"Trimmed to {new_dur:.1f}s (was {dur:.1f}s)"
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return False, f"Trim error: {e}"


def download_track(url: str, output_path: str, audio_format: str, max_retries: int,
                   cookies_file: Optional[str], start_offset: int = 0,
                   end_offset: int = 0, max_duration: int = 0,
                   quality_profile: str = "high") -> tuple:
    flags = ["yt-dlp", "--no-warnings", "--quiet"]
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]

    Path(output_path).unlink(missing_ok=True)
    quality_map   = {"high": "0", "balanced": "3", "small": "5", "smallest": "7"}
    audio_quality = quality_map.get(str(quality_profile or "high").lower(), "0")

    cmd = flags + [
        "--extract-audio",
        "--audio-format",   audio_format,
        "--audio-quality",  audio_quality,
        "--playlist-items", "1",
        "--yes-playlist",
        "--output",         output_path,
        "--retries",        str(max_retries),
        url,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return False, "Download timed out"
    except Exception as e:
        return False, str(e)

    if result.returncode != 0:
        return False, (result.stderr or "yt-dlp error").strip()[:150]

    ok, msg = validate_audio_file(Path(output_path))
    if not ok:
        return False, msg

    if start_offset > 0 or end_offset > 0 or max_duration > 0:
        trim_ok, trim_msg = trim_audio(
            Path(output_path), start_offset, end_offset, max_duration, audio_format,
        )
        if not trim_ok:
            return False, trim_msg
        log.info(f"  {trim_msg}")
        return validate_audio_file(Path(output_path))

    return ok, msg


# ─── Pass 1: Scan / Index ─────────────────────────────────────────────────────

def pass1_scan(ledger: dict, plex_movies: list, theme_filename: str) -> tuple:
    """Sync ledger to current Plex state. Returns (pending_movies, stats)."""
    plex_keys = {m["rating_key"] for m in plex_movies}
    stats = {
        "total": len(plex_movies), "has_theme": 0, "pending": 0,
        "staged": 0, "approved": 0, "removed": 0, "skipped": 0, "new": 0,
    }

    # Mark removed
    for key, row in ledger.items():
        if key not in plex_keys and row["status"] != ST_REMOVED:
            row["status"]       = ST_REMOVED
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "No longer in Plex library"
            stats["removed"]   += 1
            log.info(f"[REMOVED]  {row.get('title') or row.get('plex_title')} ({row.get('year')})")

    pending = []

    for i, movie in enumerate(plex_movies, 1):
        key        = movie["rating_key"]
        plex_title = movie["plex_title"]
        plex_year  = movie["plex_year"]
        folder     = movie["folder"]
        has_theme  = (Path(folder) / theme_filename).exists()
        row        = ledger.get(key)

        emit_progress(1, i, len(plex_movies), plex_title, "scanning")

        if row is None:
            status = ST_DOWNLOADED if has_theme else ST_PENDING
            ledger_upsert(
                ledger, key, plex_title, plex_title, plex_year, folder, status,
                notes="Theme already present" if has_theme else "New — added to queue",
                tmdb_id=movie.get("tmdb_id") or "",
            )
            ledger[key], _ = sync_theme_cache(ledger[key], theme_filename, probe_duration=False)
            stats["new"] += 1
            if has_theme:
                stats["has_theme"] += 1
            else:
                stats["pending"] += 1
                pending.append(movie)
            continue

        # Update mutable fields that may have changed in Plex
        row["folder"]     = folder
        row["plex_title"] = plex_title
        if movie.get("tmdb_id"):
            row["tmdb_id"] = movie["tmdb_id"]
        row, _ = sync_theme_cache(row, theme_filename, probe_duration=False)
        ledger[key] = row
        current = row["status"]

        if current in (ST_REJECTED, ST_IGNORED):
            stats["skipped"] += 1
            continue

        if current == ST_REMOVED:
            row["status"]       = ST_DOWNLOADED if has_theme else ST_PENDING
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "Re-appeared in Plex"
            log.info(f"[RETURNED] {plex_title} ({plex_year})")
        elif has_theme and current != ST_DOWNLOADED:
            row["status"]       = ST_DOWNLOADED
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "Theme detected on disk — auto-promoted"
            log.info(f"[PROMOTED] {plex_title} ({plex_year})")
        elif not has_theme and current == ST_DOWNLOADED:
            row["status"]       = ST_PENDING
            row["url"]          = ""
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "Theme file missing — re-queued"
            log.info(f"[RESET]    {plex_title} ({plex_year})")

        row, coerced = _coerce_status_for_row(row, theme_filename)
        if coerced:
            log.info(f"[POLICY]   {plex_title} ({plex_year}) — {row['notes']}")
        ledger[key] = row

        current = ledger[key]["status"]
        if current == ST_DOWNLOADED:   stats["has_theme"] += 1
        elif current == ST_PENDING:    stats["pending"]   += 1; pending.append(movie)
        elif current == ST_STAGED:     stats["staged"]    += 1
        elif current == ST_APPROVED:   stats["approved"]  += 1

    return pending, stats


# ─── Pass 2: Resolve source URLs ──────────────────────────────────────────────

def pass2_resolve(ledger: dict, pending_movies: list, cfg: dict) -> dict:
    """Find source URLs for pending movies. Sets status → STAGED or FAILED."""
    tmdb_api_key  = cfg.get("tmdb_api_key", "")
    cookies_file  = cfg.get("cookies_file", "") or None
    search_mode   = cfg.get("search_mode", "playlist")
    query_playlist = cfg.get("search_query_playlist", "{title} {year} soundtrack playlist")
    query_direct  = cfg.get("search_query_direct",   "{title} {year} theme song")
    fallback      = bool(cfg.get("search_fallback", True))
    fuzzy         = bool(cfg.get("search_fuzzy", False))
    golden_only   = bool(cfg.get("search_only_golden", False))
    source_url    = cfg.get("golden_source_url", "")

    if fuzzy:
        fallback = False

    # Backward-compat: old single-key config
    if "search_query" in cfg and "search_query_playlist" not in cfg:
        query_playlist = cfg["search_query"]

    stats = {"staged": 0, "no_playlist": 0, "failed": 0, "golden_matched": 0}

    # ── Golden Source only mode ───────────────────────────────────────────────
    golden_catalog: dict = {}
    if golden_only:
        try:
            _, golden_catalog = _fetch_golden_source_catalog(source_url)
            log.info(f"Golden Source only mode enabled — loaded {len(golden_catalog)} rows")
        except Exception as e:
            log.error(f"Golden Source fetch failed: {e}")
            for movie in pending_movies:
                ledger_upsert(
                    ledger, movie["rating_key"], movie["plex_title"], movie["plex_title"],
                    movie["plex_year"], movie["folder"], ST_FAILED,
                    notes=f"Golden Source fetch failed: {str(e)[:120]}",
                    tmdb_id=movie.get("tmdb_id") or "",
                )
                row, _ = _coerce_status_for_row(ledger[movie["rating_key"]], cfg.get("theme_filename", "theme.mp3"))
                ledger[movie["rating_key"]] = row
                stats["failed"] += 1
            return stats

    # ── Per-movie resolution ──────────────────────────────────────────────────
    for i, movie in enumerate(pending_movies, 1):
        key        = movie["rating_key"]
        plex_title = movie["plex_title"]
        plex_year  = movie["plex_year"]
        folder     = movie["folder"]

        emit_progress(2, i, len(pending_movies), plex_title, "resolving")

        # TMDB lookup
        if tmdb_api_key:
            tmdb = get_tmdb_metadata(movie.get("tmdb_id"), plex_title, plex_year, tmdb_api_key)
        else:
            tmdb = {"title": plex_title, "year": plex_year,
                    "tmdb_id": movie.get("tmdb_id") or ""}

        title    = tmdb.get("title", plex_title)
        year     = tmdb.get("year", plex_year)
        tmdb_id  = str(tmdb.get("tmdb_id") or movie.get("tmdb_id") or "")
        search_title = normalize_title(title) or title

        if title != plex_title:
            log.info(f"  TMDB: '{plex_title}' → '{title}'")

        # Golden Source only path
        if golden_only:
            match = golden_catalog.get(tmdb_id)
            if not match:
                log.info("  Golden Source: no TMDB match — marking FAILED")
                ledger_upsert(
                    ledger, key, plex_title, title, year, folder, ST_FAILED,
                    notes="Golden Source only mode — no TMDB match found", tmdb_id=tmdb_id,
                )
                row, coerced = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
                if coerced:
                    log.info(f"[POLICY]   {plex_title} ({year}) — {row['notes']}")
                ledger[key] = row
                stats["failed"]      += 1
                stats["no_playlist"] += 1
                continue
            ledger_upsert(
                ledger, key, plex_title, title, year, folder, ST_STAGED,
                url=match.get("source_url", ""),
                start_offset=match.get("start_offset", 0),
                end_offset=match.get("end_offset", 0),
                notes="Matched from Golden Source", tmdb_id=tmdb_id,
            )
            row, coerced = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
            if coerced:
                log.info(f"[POLICY]   {plex_title} ({year}) — {row['notes']}")
            ledger[key] = row
            stats["staged"]         += 1
            stats["golden_matched"] += 1
            continue

        # Strict template validation
        if not fuzzy:
            if search_mode == "playlist" and not _template_is_strict_safe(query_playlist, "playlist"):
                ledger_upsert(
                    ledger, key, plex_title, title, year, folder, ST_FAILED,
                    notes="Invalid playlist query template for strict mode", tmdb_id=tmdb_id,
                )
                row, _ = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
                ledger[key] = row
                stats["failed"] += 1
                continue
            if search_mode == "direct" and not _template_is_strict_safe(query_direct, "direct"):
                ledger_upsert(
                    ledger, key, plex_title, title, year, folder, ST_FAILED,
                    notes="Invalid direct query template for strict mode", tmdb_id=tmdb_id,
                )
                row, _ = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
                ledger[key] = row
                stats["failed"] += 1
                continue

        # Build attempt list
        attempts = []
        if search_mode == "playlist":
            attempts.append(("playlist", query_playlist.format(title=search_title, year=year)))
            if fallback and not fuzzy:
                attempts.append(("direct", query_direct.format(title=search_title, year=year)))
        else:
            attempts.append(("direct", query_direct.format(title=search_title, year=year)))
            if fallback and not fuzzy:
                attempts.append(("playlist", query_playlist.format(title=search_title, year=year)))

        url         = None
        method_used = ""
        last_reason = "No source found"
        for idx, (mode, query) in enumerate(attempts):
            log.info(f"[RESOLVE]  {title} ({year})  →  {mode}: {query!r}")
            candidate_url, detail = _pick_youtube_source(
                query, search_title, str(year or ""), mode, cookies_file, fuzzy=fuzzy
            )
            if candidate_url:
                url         = candidate_url
                method_used = mode if idx == 0 else f"{mode} (fallback)"
                last_reason = detail or "Matched"
                break
            last_reason = detail or last_reason
            if idx < len(attempts) - 1:
                log.info(f"  {mode.title()} not accepted — trying fallback")

        if not url:
            log.info(f"  No valid source found — marking FAILED ({last_reason})")
            ledger_upsert(
                ledger, key, plex_title, title, year, folder, ST_FAILED,
                notes=f"No valid source found — {last_reason}", tmdb_id=tmdb_id,
            )
            row, _ = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
            ledger[key] = row
            stats["failed"]      += 1
            stats["no_playlist"] += 1
            continue

        log.info(f"  Found via {method_used}: {url}")
        ledger_upsert(
            ledger, key, plex_title, title, year, folder, ST_STAGED,
            url=url, notes=f"Found via {method_used} — awaiting approval", tmdb_id=tmdb_id,
        )
        row, coerced = _coerce_status_for_row(ledger[key], cfg.get("theme_filename", "theme.mp3"))
        if coerced:
            log.info(f"[POLICY]   {plex_title} ({year}) — {row['notes']}")
        ledger[key] = row
        stats["staged"] += 1

    return stats


# ─── Pass 3: Download approved ────────────────────────────────────────────────

def pass3_download(ledger: dict, cfg: dict) -> dict:
    """Download all APPROVED rows."""
    theme_filename  = cfg.get("theme_filename", "theme.mp3")
    audio_format    = cfg.get("audio_format", "mp3").lower()
    max_retries     = int(cfg.get("max_retries", 3))
    delay_secs      = float(cfg.get("download_delay_seconds", 5))
    cookies_file    = cfg.get("cookies_file", "") or None
    max_duration    = int(cfg.get("max_theme_duration", 0))
    quality_profile = str(cfg.get("quality_profile", "high") or "high").lower()
    dry_run         = bool(cfg.get("dry_run", False))

    approved   = [(rk, r) for rk, r in ledger.items() if r["status"] == ST_APPROVED]
    stats      = {"downloaded": 0, "failed": 0, "skipped": 0}
    need_delay = False

    if not approved:
        log.info("No APPROVED movies to download.")
        return stats

    log.info(f"─── Pass 3: downloading {len(approved)} approved movies ───")
    if dry_run:
        log.info("DRY_RUN enabled — skipping downloads and leaving ledger unchanged.")
        stats["skipped"] = len(approved)
        return stats

    for i, (rk, row) in enumerate(approved, 1):
        title  = row["title"]
        year   = row["year"]
        folder = row["folder"]
        url    = row.get("url", "")

        emit_progress(3, i, len(approved), title, "downloading")

        row, coerced = _coerce_status_for_row(row, theme_filename)
        if coerced:
            ledger[rk] = row
            log.info(f"[POLICY]   {title} ({year}) — {row['notes']}")
        if row["status"] != ST_APPROVED:
            stats["skipped"] += 1
            continue

        if not url:
            log.warning(f"[APPROVED] {title} ({year}) — no URL, resetting to PENDING")
            row["status"]       = ST_PENDING
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "APPROVED but no URL — reset to PENDING"
            continue

        if need_delay and delay_secs > 0:
            log.info(f"  Waiting {delay_secs}s...")
            time.sleep(delay_secs)
        need_delay = True

        log.info(f"[DOWNLOAD] {title} ({year}) — {url}")
        s_offset = int(row.get("start_offset", 0) or 0)
        e_offset = int(row.get("end_offset",   0) or 0)

        success, message = download_track(
            url, str(Path(folder) / theme_filename),
            audio_format, max_retries, cookies_file,
            s_offset, e_offset, max_duration, quality_profile,
        )

        if success:
            log.info(f"[OK]       {title} ({year}) — {message}")
            row["status"]       = ST_DOWNLOADED
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = message
            row, _               = sync_theme_cache(row, theme_filename, probe_duration=True)
            row, coerced         = _coerce_status_for_row(row, theme_filename)
            if coerced:
                log.info(f"[POLICY]   {title} ({year}) — {row['notes']}")
            ledger[rk]           = row  # write back with updated theme cache
            stats["downloaded"] += 1
        else:
            log.warning(f"[FAILED]   {title} ({year}) — {message}")
            row["status"]       = ST_FAILED
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = message
            row, coerced         = _coerce_status_for_row(row, theme_filename)
            if coerced:
                log.info(f"[POLICY]   {title} ({year}) — {row['notes']}")
            ledger[rk]           = row
            stats["failed"]    += 1

    return stats


# ─── Library helpers ──────────────────────────────────────────────────────────

def get_libraries(cfg: dict) -> list:
    """Return libraries selected for scheduled/manual batch runs."""
    libs = cfg.get("libraries")
    if libs and isinstance(libs, list):
        available      = [l for l in libs if l.get("enabled", True)]
        selected_names = set(cfg.get("schedule_libraries") or [])
        if selected_names:
            chosen = [l for l in available if l.get("name") in selected_names]
            if chosen:
                return chosen
        return available
    name = cfg.get("plex_library_name", "Movies")
    return [{"name": name, "enabled": True}]


def pending_from_ledger(ledger: dict) -> list:
    """Reconstruct pending_movies list from ledger rows (for FORCE_PASS=2)."""
    return [
        {
            "rating_key": r["rating_key"],
            "plex_title": r.get("plex_title") or r.get("title", "Unknown"),
            "plex_year":  r.get("year", ""),
            "folder":     r.get("folder", ""),
            "tmdb_id":    r.get("tmdb_id") or None,
        }
        for r in ledger.values()
        if r["status"] == ST_PENDING
    ]


# ─── Scan single library ──────────────────────────────────────────────────────

def scan_library(cfg: dict, library_name: str, log_file_path: str, force_pass: int = 0):
    """
    Run passes for a single library.
    force_pass=0  → auto/cron mode: runs steps enabled in schedule config
    force_pass=1  → Pass 1 only (scan/index)
    force_pass=2  → Pass 2 only (resolve from existing ledger)
    force_pass=3  → Pass 3 only (download from existing ledger)
    """
    plex_url     = cfg.get("plex_url", "").rstrip("/")
    plex_token   = cfg.get("plex_token", "")
    theme_file   = cfg.get("theme_filename", "theme.mp3")
    test_limit   = int(cfg.get("test_limit", 0))
    auto_approve = cfg.get("auto_approve", False)

    step1_enabled = cfg.get("schedule_step1", True)
    step2_enabled = cfg.get("schedule_step2", True)
    step3_enabled = cfg.get("schedule_step3", True)

    log.info(f"═══ Library: {library_name} ═══")

    # ── Pass 2 only ───────────────────────────────────────────────────────────
    if force_pass == 2:
        ledger  = load_ledger(log_file_path)
        pending = pending_from_ledger(ledger)
        if not pending:
            log.info(f"'{library_name}' — no PENDING movies to resolve")
            return
        if test_limit > 0:
            log.info(f"RESOLVE BATCH SIZE {test_limit} applied")
            pending = pending[:test_limit]
        log.info(f"─── Pass 2: resolving URLs for {len(pending)} pending movies ───")
        t0    = time.time()
        stats = pass2_resolve(ledger, pending, cfg)
        save_ledger(log_file_path, ledger)
        log.info(
            f"Pass 2 complete — Staged: {stats['staged']}  "
            f"No playlist: {stats['no_playlist']} ({time.time()-t0:.1f}s)"
        )
        log.info("→ Review the Database tab, set status to APPROVED, then run Download")
        return

    # ── Pass 3 only ───────────────────────────────────────────────────────────
    if force_pass == 3:
        ledger = load_ledger(log_file_path)
        if auto_approve:
            _auto_approve_staged(ledger)
            save_ledger(log_file_path, ledger)
        if not any(r["status"] == ST_APPROVED for r in ledger.values()):
            log.info(f"'{library_name}' — no APPROVED movies to download")
            return
        t0    = time.time()
        stats = pass3_download(ledger, cfg)
        save_ledger(log_file_path, ledger)
        log.info(
            f"Pass 3 complete — Downloaded: {stats['downloaded']}  "
            f"Failed: {stats['failed']}  Skipped: {stats.get('skipped', 0)} ({time.time()-t0:.1f}s)"
        )
        return

    # ── Pass 1 ────────────────────────────────────────────────────────────────
    if force_pass == 1 or step1_enabled:
        try:
            plex_movies = get_plex_movies(plex_url, plex_token, library_name)
        except RuntimeError as e:
            log.error(f"Plex error for '{library_name}': {e}")
            emit_progress(1, 0, 0, "Plex error", "error", message=str(e))
            return

        if not plex_movies:
            log.warning(f"No movies found in '{library_name}' — skipping")
            return

        log.info(f"─── Pass 1: scanning {len(plex_movies)} movies ───")
        t0     = time.time()
        ledger = load_ledger(log_file_path)
        pending, idx_stats = pass1_scan(ledger, plex_movies, theme_file)
        log.info(
            f"Scan — Total: {idx_stats['total']}  "
            f"Have theme: {idx_stats['has_theme']}  "
            f"Pending: {idx_stats['pending']}  "
            f"Staged: {idx_stats['staged']}  "
            f"Approved: {idx_stats['approved']}  "
            f"New: {idx_stats['new']}  "
            f"Removed: {idx_stats['removed']}"
        )
        save_ledger(log_file_path, ledger)
        log.info(f"Pass 1 complete — ledger saved: {log_file_path} ({time.time()-t0:.1f}s)")

        if force_pass == 1:
            return
    else:
        log.info("Step 1 (Scan) disabled in schedule — skipping")
        ledger  = load_ledger(log_file_path)
        pending = pending_from_ledger(ledger)

    # ── Pass 2 (auto mode) ────────────────────────────────────────────────────
    if step2_enabled and pending:
        to_resolve = pending[:test_limit] if test_limit > 0 else pending
        if test_limit > 0:
            log.info(f"RESOLVE BATCH SIZE {test_limit} — resolving {len(to_resolve)} of {len(pending)}")
        log.info(f"─── Pass 2: resolving URLs for {len(to_resolve)} pending movies ───")
        t0    = time.time()
        stats = pass2_resolve(ledger, to_resolve, cfg)
        save_ledger(log_file_path, ledger)
        log.info(
            f"Pass 2 complete — Staged: {stats['staged']}  "
            f"No playlist: {stats['no_playlist']} ({time.time()-t0:.1f}s)"
        )
    elif not step2_enabled:
        log.info("Step 2 (Find Sources) disabled in schedule — skipping")

    # ── Auto-approve ─────────────────────────────────────────────────────────
    if auto_approve:
        _auto_approve_staged(ledger)
        save_ledger(log_file_path, ledger)

    # ── Pass 3 (auto mode) ────────────────────────────────────────────────────
    if step3_enabled:
        if any(r["status"] == ST_APPROVED for r in ledger.values()):
            t0    = time.time()
            stats = pass3_download(ledger, cfg)
            save_ledger(log_file_path, ledger)
            log.info(
                f"Pass 3 complete — Downloaded: {stats['downloaded']}  "
                f"Failed: {stats['failed']}  Skipped: {stats.get('skipped', 0)} ({time.time()-t0:.1f}s)"
            )
            return
    else:
        log.info("Step 3 (Download) disabled in schedule — skipping")

    log.info(f"'{library_name}' — all caught up, nothing to do this run.")


def _auto_approve_staged(ledger: dict):
    count = 0
    for row in ledger.values():
        if row["status"] == ST_STAGED:
            row["status"]       = ST_APPROVED
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"]        = "Auto-approved"
            count += 1
    if count:
        log.info(f"[AUTO-APPROVE] {count} movies auto-approved")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    cfg  = load_config()
    lock = acquire_lock(LOCK_PATH)
    if lock is None:
        log.warning("Another run is already in progress — exiting.")
        return

    if not cfg.get("plex_token"):
        log.error("plex_token not set in config.yaml — aborting")
        sys.exit(1)

    force_pass = int(os.environ.get("FORCE_PASS", "0"))
    if force_pass:
        log.info(f"FORCE_PASS={force_pass} — running Pass {force_pass} only")

    schedule_enabled = cfg.get("schedule_enabled", True)
    if force_pass == 0 and not schedule_enabled:
        log.info("Automated schedule is disabled — exiting without running pipeline")
        return

    libraries = get_libraries(cfg)
    if not libraries:
        log.error("No libraries configured or all disabled — check config.yaml")
        sys.exit(1)

    log.info("Media Tracks starting up")
    log.info(f"Libraries: {[l['name'] for l in libraries]}")
    if force_pass in (2, 3):
        log.info("Skipping Plex connection — running from existing ledger")
    else:
        log.info(f"Connecting to Plex at {cfg.get('plex_url', '')}")

    for lib in libraries:
        name  = lib["name"]
        lpath = lib.get("ledger") or ledger_path_for(name)
        scan_library(cfg, name, lpath, force_pass=force_pass)

    log.info("All libraries processed.")


if __name__ == "__main__":
    main()
