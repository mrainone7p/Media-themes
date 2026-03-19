#!/usr/bin/env python3
"""External integrations for Media Tracks.

This module owns API calls, subprocess wrappers, and short-lived integration
caches. It intentionally avoids business-rule decisions so higher-level code in
web.logic can decide what should happen with the results.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
import sys
from pathlib import Path

import requests as http_requests

WEB_DIR = Path(__file__).resolve().parent
SHARED_DIR = WEB_DIR.parent / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from storage import TMDB_GUID_RE as _TMDB_GUID_RE

# ── TMDB / Plex caches ───────────────────────────────────────────────────────

_poster_cache: dict[str, tuple[bytes, str]] = {}
_tmdb_poster_cache: dict[str, tuple[float, str]] = {}
_tmdb_lookup_cache: dict[str, tuple[float, dict]] = {}
_bio_cache: dict[str, tuple[float, str]] = {}

_TMDB_POSTER_TTL = 86400
_TMDB_LOOKUP_TTL = 86400
_BIO_TTL = 86400

# ── Preview stream cache ─────────────────────────────────────────────────────

_stream_cache: dict[str, tuple[float, str]] = {}
_preview_url_cache: dict[str, tuple[float, str]] = {}
_STREAM_TTL = 600
_STREAM_MAX = 200
_PREVIEW_URL_TTL = 600
_PREVIEW_URL_MAX = 200

_youtube_search_cache: dict[str, tuple[float, list[dict]]] = {}
_YOUTUBE_SEARCH_TTL = 300
_toolchain_cache: tuple[float, dict] | None = None
_TOOLCHAIN_STATUS_TTL = 300
_cache_lock = threading.Lock()


# ── Generic external helpers ─────────────────────────────────────────────────

def run_command(cmd: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def get_json(url: str, *, headers: dict | None = None, params: dict | None = None, timeout: int = 8) -> dict:
    response = http_requests.get(url, headers=headers, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _yt_dlp_base_flags(cookies_file: str | None = None, *, quiet: bool = True) -> list[str]:
    flags = ["yt-dlp", "--no-warnings"]
    if quiet:
        flags.append("--quiet")
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    return flags


# ── Plex helpers ─────────────────────────────────────────────────────────────

def plex_headers(token: str) -> dict[str, str]:
    return {"X-Plex-Token": token, "Accept": "application/json"}


def plex_sections(plex_url: str, token: str) -> list[dict]:
    payload = get_json(f"{plex_url.rstrip('/')}/library/sections", headers=plex_headers(token), timeout=15)
    return payload.get("MediaContainer", {}).get("Directory", [])


def test_plex(plex_url: str, token: str) -> dict:
    libraries = plex_sections(plex_url, token)
    return {"ok": True, "libraries": len(libraries)}


def list_plex_libraries(plex_url: str, token: str) -> list[dict]:
    libraries = plex_sections(plex_url, token)
    return [
        {"name": item["title"], "type": item.get("type", "")}
        for item in libraries
        if item.get("type") in ("movie", "show")
    ]


def fetch_plex_poster(rating_key: str, plex_url: str, plex_token: str) -> tuple[bytes, str] | None:
    if rating_key in _poster_cache:
        return _poster_cache[rating_key]
    response = http_requests.get(
        f"{plex_url.rstrip('/')}/library/metadata/{rating_key}/thumb",
        headers={"X-Plex-Token": plex_token},
        timeout=8,
    )
    if response.status_code != 200:
        return None
    content_type = response.headers.get("Content-Type", "image/jpeg")
    if len(_poster_cache) > 500:
        _poster_cache.clear()
    _poster_cache[rating_key] = (response.content, content_type)
    return _poster_cache[rating_key]


def fetch_plex_summary(rating_key: str, plex_url: str, plex_token: str) -> str:
    response = http_requests.get(
        f"{plex_url.rstrip('/')}/library/metadata/{rating_key}",
        headers=plex_headers(plex_token),
        timeout=8,
    )
    if response.status_code != 200:
        return ""
    metadata = response.json().get("MediaContainer", {}).get("Metadata", [{}])
    return metadata[0].get("summary", "") if metadata else ""


def extract_tmdb_id(item: dict) -> str | None:
    for guid in item.get("Guid", []):
        match = _TMDB_GUID_RE.search(guid.get("id", ""))
        if match:
            return match.group(1)
    match = _TMDB_GUID_RE.search(item.get("guid", ""))
    return match.group(1) if match else None


# ── TMDB helpers ─────────────────────────────────────────────────────────────

def _tmdb_search(endpoint: str, *, title: str, year: str, tmdb_key: str) -> list[dict]:
    params = {"api_key": tmdb_key, "query": title, "language": "en-US"}
    if endpoint == "movie" and year:
        params["year"] = year
    response = http_requests.get(f"https://api.themoviedb.org/3/search/{endpoint}", params=params, timeout=8)
    if response.status_code != 200:
        return []
    return response.json().get("results", [])


def tmdb_lookup(title: str, year: str, tmdb_key: str) -> dict | None:
    if not title or not tmdb_key:
        return None
    key = f"{str(title).strip().lower()}|{str(year).strip()}"
    now = time.time()
    cached = _tmdb_lookup_cache.get(key)
    if cached and now - cached[0] < _TMDB_LOOKUP_TTL:
        return cached[1]

    movie_results = _tmdb_search("movie", title=title, year=year, tmdb_key=tmdb_key)
    if movie_results:
        movie_id = movie_results[0].get("id")
        if movie_id:
            data = {"id": movie_id, "media_type": "movie", "url": f"https://www.themoviedb.org/movie/{movie_id}"}
            _tmdb_lookup_cache[key] = (now, data)
            return data

    tv_results = _tmdb_search("tv", title=title, year="", tmdb_key=tmdb_key)
    if tv_results:
        tv_id = tv_results[0].get("id")
        if tv_id:
            data = {"id": tv_id, "media_type": "tv", "url": f"https://www.themoviedb.org/tv/{tv_id}"}
            _tmdb_lookup_cache[key] = (now, data)
            return data
    return None


def tmdb_poster_url(title: str, year: str, tmdb_key: str, *, size: str = "w342") -> str | None:
    if not title or not tmdb_key:
        return None
    size = size if size in {"w92", "w154", "w185", "w342", "w500", "original"} else "w342"
    key = f"{str(title).strip().lower()}|{str(year).strip()}|{size}"
    now = time.time()
    cached = _tmdb_poster_cache.get(key)
    if cached and now - cached[0] < _TMDB_POSTER_TTL:
        return cached[1]

    results = _tmdb_search("movie", title=title, year=year, tmdb_key=tmdb_key)
    if not results:
        results = _tmdb_search("tv", title=title, year="", tmdb_key=tmdb_key)
    poster_path = results[0].get("poster_path") if results else None
    if not poster_path:
        return None
    url = f"https://image.tmdb.org/t/p/{size}{poster_path}"
    _tmdb_poster_cache[key] = (now, url)
    return url


def fetch_tmdb_overview(cache_key: str, title: str, year: str, tmdb_key: str) -> str:
    now = time.time()
    cached = _bio_cache.get(cache_key)
    if cached and now - cached[0] < _BIO_TTL:
        return cached[1]

    movie_results = _tmdb_search("movie", title=title, year=year, tmdb_key=tmdb_key)
    if not movie_results:
        movie_results = _tmdb_search("tv", title=title, year="", tmdb_key=tmdb_key)
    overview = movie_results[0].get("overview", "") if movie_results else ""
    if overview:
        _bio_cache[cache_key] = (now, overview)
    return overview


def test_tmdb_key(key: str) -> dict:
    response = http_requests.get("https://api.themoviedb.org/3/configuration", params={"api_key": key}, timeout=8)
    if response.status_code == 401:
        return {"ok": False, "error": "Invalid API key"}
    response.raise_for_status()
    return {"ok": True}


# ── yt-dlp / ffmpeg helpers ──────────────────────────────────────────────────

def youtube_search(query: str, cookies_file: str | None = None) -> list[dict]:
    cache_key = f"{str(query or '').strip().lower()}|{str(cookies_file or '').strip()}"
    now = time.time()
    with _cache_lock:
        cached = _youtube_search_cache.get(cache_key)
        if cached and now - cached[0] < _YOUTUBE_SEARCH_TTL:
            return [dict(row) for row in cached[1]]

    search_url = "https://www.youtube.com/results?search_query=" + urllib.parse.quote(query)
    result = run_command(
        _yt_dlp_base_flags(cookies_file) + [
            "--flat-playlist",
            "--print",
            "%(title)s\t%(url)s\t%(duration_string)s",
            "--playlist-items",
            "1:10",
            search_url,
        ],
        timeout=30,
    )
    rows = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[1].startswith("https://"):
            rows.append({"title": parts[0], "url": parts[1], "duration": parts[2] if len(parts) > 2 else ""})
    with _cache_lock:
        _youtube_search_cache[cache_key] = (now, [dict(row) for row in rows])
    return rows


def _preview_cache_key(url: str, cookies_file: str | None = None) -> str:
    return f"{str(url or '').strip()}|{str(cookies_file or '').strip()}"


def prune_preview_url_cache(now: float | None = None):
    now = now or time.time()
    expired = [key for key, (ts, _) in _preview_url_cache.items() if now - ts > _PREVIEW_URL_TTL]
    for key in expired:
        _preview_url_cache.pop(key, None)
    if len(_preview_url_cache) > _PREVIEW_URL_MAX:
        oldest = sorted(_preview_url_cache.items(), key=lambda kv: kv[1][0])
        for key, _ in oldest[: len(_preview_url_cache) - _PREVIEW_URL_MAX]:
            _preview_url_cache.pop(key, None)


def preview_stream_url(url: str, cookies_file: str | None = None) -> str:
    cache_key = _preview_cache_key(url, cookies_file)
    now = time.time()
    with _cache_lock:
        prune_preview_url_cache(now)
        cached = _preview_url_cache.get(cache_key)
        if cached and now - cached[0] < _PREVIEW_URL_TTL:
            return cached[1]

    result = run_command(
        _yt_dlp_base_flags(cookies_file) + [
            "--format",
            "bestaudio",
            "--get-url",
            "--playlist-items",
            "1",
            "--yes-playlist",
            url,
        ],
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or "yt-dlp error")[:150])
    stream_url = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
    if not stream_url:
        raise RuntimeError("Could not extract stream URL")
    with _cache_lock:
        _preview_url_cache[cache_key] = (now, stream_url)
    return stream_url


def cache_preview_stream(source_url: str, stream_url: str) -> str:
    key = hashlib.md5(source_url.encode()).hexdigest()[:12]
    _stream_cache[key] = (time.time(), stream_url)
    prune_stream_cache()
    return key


def prune_stream_cache(now: float | None = None):
    now = now or time.time()
    expired = [key for key, (ts, _) in _stream_cache.items() if now - ts > _STREAM_TTL]
    for key in expired:
        _stream_cache.pop(key, None)
    if len(_stream_cache) > _STREAM_MAX:
        oldest = sorted(_stream_cache.items(), key=lambda kv: kv[1][0])
        for key, _ in oldest[: len(_stream_cache) - _STREAM_MAX]:
            _stream_cache.pop(key, None)


def get_cached_preview_stream(key: str) -> str | None:
    prune_stream_cache()
    entry = _stream_cache.get(key)
    return entry[1] if entry else None


def stream_remote_audio(url: str, range_header: str = ""):
    headers = {"Range": range_header} if range_header else {}
    return http_requests.get(url, stream=True, timeout=30, headers=headers)


def download_audio(url: str, folder: str | Path, slug: str, *, audio_format: str, quality_profile: str, cookies_file: str | None = None) -> Path:
    quality_map = {
        "high": "bestaudio",
        "balanced": "bestaudio[abr<=192]/bestaudio",
        "small": "bestaudio[abr<=128]/bestaudio",
        "smallest": "bestaudio[abr<=96]/bestaudio",
    }
    output_template = str(Path(folder) / f"mt_tmp_{slug}.%(ext)s")
    cmd = _yt_dlp_base_flags(cookies_file, quiet=False) + [
        "-x",
        "--audio-format",
        audio_format,
        "--audio-quality",
        "0",
        "-f",
        quality_map.get(quality_profile, "bestaudio"),
        "-o",
        output_template,
        "--playlist-items",
        "1",
        url,
    ]
    result = run_command(cmd, timeout=180)
    if result.returncode != 0:
        raise RuntimeError(f"Download failed: {(result.stderr or result.stdout or 'yt-dlp error').strip()[:300]}")
    downloaded = next(Path(folder).glob(f"mt_tmp_{slug}.*"), None)
    if not downloaded or not downloaded.exists():
        raise RuntimeError("yt-dlp succeeded but output file not found")
    return downloaded


def trim_audio_copy(source_path: str | Path, output_path: str | Path, *, start: float, end: float, timeout: int = 60) -> None:
    result = run_command(
        ["ffmpeg", "-y", "-i", str(source_path), "-ss", str(start), "-to", str(end), "-c", "copy", str(output_path)],
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg error: {result.stderr[:150]}")


def cleanup_temp_downloads(folder: str | Path, slug: str):
    for path in Path(folder).glob(f"mt_tmp_{slug}.*"):
        path.unlink(missing_ok=True)
    for path in Path(folder).glob(f"mt_trim_{slug}.*"):
        path.unlink(missing_ok=True)


# ── Dependency / toolchain helpers ───────────────────────────────────────────

def toolchain_status() -> dict:
    global _toolchain_cache
    now = time.time()
    with _cache_lock:
        if _toolchain_cache and now - _toolchain_cache[0] < _TOOLCHAIN_STATUS_TTL:
            return dict(_toolchain_cache[1])

    ytdlp_bin = shutil.which("yt-dlp")
    ffmpeg_bin = shutil.which("ffmpeg")
    ytdlp_ver = None
    ffmpeg_ver = None
    if ytdlp_bin:
        try:
            ytdlp_ver = run_command(["yt-dlp", "--version"], timeout=5).stdout.strip()
        except Exception:
            pass
    if ffmpeg_bin:
        try:
            first = run_command(["ffmpeg", "-version"], timeout=5).stdout.splitlines()[0]
            match = re.search(r"ffmpeg version (\S+)", first)
            ffmpeg_ver = match.group(1) if match else "unknown"
        except Exception:
            pass

    if ytdlp_bin and ffmpeg_bin:
        detail = " · ".join(part for part in [f"yt-dlp {ytdlp_ver}" if ytdlp_ver else "", f"ffmpeg {ffmpeg_ver}" if ffmpeg_ver else ""] if part)
        payload = {"state": "ok", "label": "Ready", "detail": detail, "ytdlp_version": ytdlp_ver, "ffmpeg_version": ffmpeg_ver}
    elif not ytdlp_bin and not ffmpeg_bin:
        payload = {"state": "error", "label": "Dependency issue", "detail": "yt-dlp and ffmpeg not found"}
    elif not ytdlp_bin:
        payload = {"state": "error", "label": "yt-dlp missing", "detail": f"ffmpeg {ffmpeg_ver}" if ffmpeg_ver else "ffmpeg present"}
    else:
        payload = {"state": "error", "label": "ffmpeg missing", "detail": f"yt-dlp {ytdlp_ver}" if ytdlp_ver else "yt-dlp present"}

    with _cache_lock:
        _toolchain_cache = (now, dict(payload))
    return payload
