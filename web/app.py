#!/usr/bin/env python3
"""Media Tracks — Web GUI v3.2"""

import atexit, hashlib, json, os, queue, re, signal, subprocess, sys, threading, time as _time
import csv
from datetime import datetime
from pathlib import Path

import requests as http_requests
import yaml
from flask import (Flask, Response, jsonify, render_template_string,
                   request, stream_with_context, send_file, abort)

app = Flask(__name__)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
UI_TERMINOLOGY_PATH = os.environ.get("UI_TERMINOLOGY_PATH", "/app/web/ui_terminology.yaml")
LOGS_DIR    = Path("/app/logs")
RUNS_DIR    = LOGS_DIR / "runs"
TASKS_FILE  = LOGS_DIR / "task_history.jsonl"
EXPORTS_DIR = LOGS_DIR / "exports"
SCRIPT_PATH = "/app/script/media_tracks.py"
RUNS_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Shared storage (SQLite) ───────────────────────────────────────────────────
_SHARED = "/app/shared"
if _SHARED not in sys.path:
    sys.path.insert(0, _SHARED)
from storage import (
    LEDGER_HEADERS,
    ffprobe_duration,
    get_db_path,
    ledger_path_for,
    load_ledger_rows as load_ledger,
    MANUAL_STATUS_TRANSITIONS,
    save_ledger_rows as save_ledger,
    STATUS_ORDER,
    status_after_clearing_source,
    sync_theme_cache,       # FIX: now imported so download-now and trim update theme metadata
    validate_manual_status_transition,
)

EDITABLE_LEDGER_FIELDS = set(LEDGER_HEADERS) - {"folder", "rating_key"}


def _row_has_theme(row):
    return str(row.get("theme_exists", "") or "") == "1"


def _status_after_clearing_source(row):
    return status_after_clearing_source(
        row.get("status", ""),
        has_theme=_row_has_theme(row),
    )


def _clear_source_urls_for_rows(rows, *, keys=None, note, now):
    key_filter = set(str(k) for k in (keys or []))
    summary = {
        "requested": len(key_filter) if keys is not None else len(rows),
        "matched": 0,
        "cleared": 0,
        "updated": 0,
        "preserved_available": 0,
        "reset_missing": 0,
        "preserved_failed": 0,
        "preserved_unmonitored": 0,
        "skipped_without_url": 0,
    }
    for row in rows:
        rating_key = str(row.get("rating_key", "") or "")
        if key_filter and rating_key not in key_filter:
            continue
        summary["matched"] += 1
        had_url = bool(str(row.get("url", "") or "").strip())
        if not had_url:
            summary["skipped_without_url"] += 1
            continue
        next_status, bucket = _status_after_clearing_source(row)
        previous_status = str(row.get("status", "") or "").upper()
        row["url"] = ""
        row["source_origin"] = "unknown"
        row["status"] = next_status
        row["last_updated"] = now
        row["notes"] = note
        summary["cleared"] += 1
        summary[bucket] += 1
        if previous_status != next_status:
            summary["updated"] += 1
    return summary


def _status_validation_error(row, attempted_status):
    return validate_manual_status_transition(
        row.get("status", ""),
        attempted_status,
        has_url=bool(str(row.get("url", "") or "").strip()),
        has_theme=_row_has_theme(row),
    )


def _ledger_row_response(row):
    return {header: str(row.get(header, "") or "") for header in LEDGER_HEADERS}


def _save_ledger_row_updates(row, updates, *, default_notes=None):
    attempted_status = None
    candidate_row = dict(row)
    for key, value in updates.items():
        if key not in EDITABLE_LEDGER_FIELDS:
            continue
        normalized = str(value or "")
        if key == "status":
            attempted_status = normalized.upper()
            candidate_row[key] = attempted_status
        else:
            candidate_row[key] = normalized

    if attempted_status:
        err = _status_validation_error(candidate_row, attempted_status)
        if err:
            err["rating_key"] = str(row.get("rating_key", "") or "")
            err["title"] = row.get("title") or row.get("plex_title") or ""
            return None, err

    for key, value in updates.items():
        if key not in EDITABLE_LEDGER_FIELDS:
            continue
        normalized = str(value or "")
        row[key] = attempted_status if key == "status" and attempted_status else normalized

    if "url" in updates:
        row["source_origin"] = "manual" if str(updates.get("url") or "").strip() else "unknown"
    row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "notes" not in updates and default_notes is not None:
        row["notes"] = default_notes
    return row, None

_run_lock    = threading.Lock()
_run_active  = False
_run_clients = []
_run_proc    = None
_run_stop_requested = False
_run_started_at = None
_run_last_line = ""
_run_pass = 0
_run_scope_label = ""
_run_libraries = []
_poster_cache  = {}
_media_cache   = {}
_stream_cache  = {}
_STREAM_TTL    = 600
_STREAM_MAX    = 200
_tmdb_poster_cache = {}
_TMDB_POSTER_TTL   = 86400
_tmdb_lookup_cache = {}
_TMDB_LOOKUP_TTL   = 86400
_bio_cache = {}
_BIO_TTL   = 86400
_GOLDEN_CACHE_DIR = LOGS_DIR / "golden_source_cache"
_GOLDEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _broadcast(msg):
    is_progress = msg.startswith("@@PROGRESS@@")
    dead = []
    for q in _run_clients:
        try:
            q.put_nowait(msg)
        except Exception:
            if is_progress:
                try:
                    q.get_nowait()
                    q.put_nowait(msg)
                except Exception:
                    pass
            else:
                try:
                    q.get_nowait()
                    q.put_nowait(msg)
                except Exception:
                    dead.append(q)
    for q in dead:
        try: _run_clients.remove(q)
        except: pass

def _parse_run_stats(lines):
    stats = {}
    for line in lines:
        if "Pass 1 complete" in line:
            m = re.search(r"Total:\s*(\d+)\s*Have theme:\s*(\d+)\s*Missing:\s*(\d+)\s*Staged:\s*(\d+)\s*Approved:\s*(\d+)\s*New:\s*(\d+)\s*Removed:\s*(\d+)", line)
            if m:
                stats["pass1"] = {"total":int(m.group(1)),"has_theme":int(m.group(2)),"missing":int(m.group(3)),
                                   "staged":int(m.group(4)),"approved":int(m.group(5)),"new":int(m.group(6)),"removed":int(m.group(7))}
        if "Pass 2 complete" in line:
            m = re.search(r"Staged:\s*(\d+)\s*Missing:\s*(\d+)\s*Failed:\s*(\d+)", line)
            if m: stats["pass2"] = {"staged":int(m.group(1)),"missing":int(m.group(2)),"failed":int(m.group(3))}
        if "Pass 3 complete" in line:
            m = re.search(r"Available:\s*(\d+)\s*Failed:\s*(\d+)\s*Skipped:\s*(\d+)", line)
            if m: stats["pass3"] = {"available":int(m.group(1)),"failed":int(m.group(2)),"skipped":int(m.group(3))}
    return stats

def _record_task(task_name, status="success", scope="", summary="", details=None, duration_seconds=None):
    normalized_status = str(status or "success")
    entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "task": str(task_name or "Task"),
        "status": normalized_status,
        "outcome": normalized_status,
        "scope": str(scope or ""),
        "summary": str(summary or ""),
        "details": details or {},
        "duration_seconds": float(duration_seconds or 0),
    }
    try:
        with open(TASKS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _load_task_entries(limit=250):
    entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    run_entries = []
    for f in sorted(RUNS_DIR.glob("*.json")):
        try:
            run = json.loads(f.read_text(encoding="utf-8"))
            run_status = str(run.get("status") or run.get("outcome") or "success")
            run_entries.append({
                "time": run.get("time", ""),
                "task": {1: "Scan Libraries", 2: "Find Sources", 3: "Download Themes"}.get(run.get("pass"), "Pipeline Run"),
                "status": run_status,
                "outcome": run_status,
                "scope": "",
                "summary": run.get("summary", ""),
                "details": {
                    "pass": run.get("pass", 0),
                    "stats": run.get("stats", {}),
                    "return_code": run.get("return_code"),
                    "stop_requested": bool(run.get("stop_requested")),
                },
                "duration_seconds": run.get("duration_seconds") or 0,
                "is_run_history": True,
            })
        except Exception:
            continue
    all_entries = sorted(entries + run_entries, key=lambda e: e.get("time", ""), reverse=True)
    return all_entries[:max(1, int(limit or 250))]

def load_config():
    try:
        with open(CONFIG_PATH) as f: return yaml.safe_load(f) or {}
    except: return {}

def save_config(data):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

def get_ui_token():
    env_token = os.environ.get("UI_TOKEN", "").strip()
    if env_token: return env_token
    return str(load_config().get("ui_token", "") or "").strip()

@app.before_request
def _auth_guard():
    if not request.path.startswith("/api/"): return
    token = get_ui_token()
    if not token: return
    provided = request.headers.get("X-UI-Token") or request.args.get("token", "")
    if provided != token:
        return jsonify({"error": "unauthorized"}), 401

def get_media_roots(cfg):
    roots = cfg.get("media_roots")
    if isinstance(roots, list) and roots:
        return [str(r) for r in roots if str(r).strip()]
    single = cfg.get("media_root")
    if single: return [str(single)]
    return ["/media"]

def is_allowed_folder(folder, roots):
    if not folder: return False
    try:
        fpath = Path(folder).resolve()
    except Exception:
        return False
    for root in roots:
        try:
            rpath = Path(root).resolve()
            if fpath == rpath or fpath.is_relative_to(rpath): return True
        except Exception:
            continue
    return False


def _find_row_by_identity(rows, rating_key="", folder="", tmdb_id=""):
    """Find a ledger row using stable identity, preferring rating_key then folder."""
    rk = str(rating_key or "").strip()
    fd = str(folder or "").strip()
    tid = str(tmdb_id or "").strip()
    if rk:
        row = next((r for r in rows if str(r.get("rating_key", "") or "").strip() == rk), None)
        if row:
            return row, "rating_key"
    if fd:
        row = next((r for r in rows if str(r.get("folder", "") or "").strip() == fd), None)
        if row:
            return row, "folder"
    if tid:
        row = next((r for r in rows if str(r.get("tmdb_id", "") or "").strip() == tid), None)
        if row:
            return row, "tmdb_id"
    return None, ""

def _prune_stream_cache(now=None):
    now = now or _time.time()
    expired = [k for k,(ts,_) in _stream_cache.items() if now-ts > _STREAM_TTL]
    for k in expired: _stream_cache.pop(k, None)
    if len(_stream_cache) > _STREAM_MAX:
        oldest = sorted(_stream_cache.items(), key=lambda kv: kv[1][0])
        for k,_ in oldest[:len(_stream_cache)-_STREAM_MAX]:
            _stream_cache.pop(k, None)

def _tmdb_poster_url(title, year, tmdb_key, size="w342"):
    if not title or not tmdb_key: return None
    size = size if size in {"w92","w154","w185","w342","w500","original"} else "w342"
    key = f"{str(title).strip().lower()}|{str(year).strip()}|{size}"
    now = _time.time()
    cached = _tmdb_poster_cache.get(key)
    if cached and now - cached[0] < _TMDB_POSTER_TTL: return cached[1]
    params = {"api_key": tmdb_key, "query": title, "language": "en-US"}
    if year: params["year"] = year
    try:
        sr = http_requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=8)
        results = sr.json().get("results", []) if sr.status_code == 200 else []
        if not results:
            sr = http_requests.get("https://api.themoviedb.org/3/search/tv",
                                   params={"api_key":tmdb_key,"query":title,"language":"en-US"}, timeout=8)
            results = sr.json().get("results", []) if sr.status_code == 200 else []
        poster_path = results[0].get("poster_path") if results else None
        if not poster_path: return None
        url = f"https://image.tmdb.org/t/p/{size}{poster_path}"
        _tmdb_poster_cache[key] = (now, url)
        return url
    except Exception: return None

def _tmdb_lookup(title, year, tmdb_key):
    if not title or not tmdb_key: return None
    key = f"{str(title).strip().lower()}|{str(year).strip()}"
    now = _time.time()
    cached = _tmdb_lookup_cache.get(key)
    if cached and now - cached[0] < _TMDB_LOOKUP_TTL: return cached[1]
    params = {"api_key": tmdb_key, "query": title, "language": "en-US"}
    if year: params["year"] = year
    try:
        sr = http_requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=8)
        results = sr.json().get("results", []) if sr.status_code == 200 else []
        if results:
            mid = results[0].get("id")
            if mid:
                data = {"id": mid, "media_type": "movie", "url": f"https://www.themoviedb.org/movie/{mid}"}
                _tmdb_lookup_cache[key] = (now, data); return data
        sr = http_requests.get("https://api.themoviedb.org/3/search/tv",
                               params={"api_key":tmdb_key,"query":title,"language":"en-US"}, timeout=8)
        results = sr.json().get("results", []) if sr.status_code == 200 else []
        if results:
            tid = results[0].get("id")
            if tid:
                data = {"id": tid, "media_type": "tv", "url": f"https://www.themoviedb.org/tv/{tid}"}
                _tmdb_lookup_cache[key] = (now, data); return data
    except Exception: return None
    return None

def get_libraries(cfg):
    libs = cfg.get("libraries")
    if libs and isinstance(libs, list): return libs
    return [{"name": cfg.get("plex_library_name","Movies"), "enabled": True}]

def get_audio_duration(filepath):
    try:
        r = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1",str(filepath)],
            capture_output=True, text=True, timeout=15)
        return float(r.stdout.strip())
    except: return 0.0

def _boolish(v):
    if isinstance(v, bool): return v
    return str(v).strip().lower() in {"1","true","yes","on"}

def _normalize_golden_source_url(url):
    url = (url or "").strip()
    if not url: return ""
    m = re.match(r"https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)", url)
    if m:
        owner, repo, branch, path = m.groups()
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
    return url

def _golden_cache_path(normalized_url):
    digest = hashlib.sha256(normalized_url.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return _GOLDEN_CACHE_DIR / f"catalog_{digest}.csv"


def _parse_golden_source_csv(text):
    reader = import_csv_reader(text)
    if not reader.fieldnames:
        raise ValueError("Golden Source CSV has no header row")
    rows = []
    for row in reader:
        clean = {
            str(k or "").strip().lower(): str(v or "").strip()
            for k, v in row.items()
        }
        tmdb_id = clean.get("tmdb_id", "")
        source_url = clean.get("source_url", "")
        if not tmdb_id:
            continue
        clean["tmdb_id"] = tmdb_id
        clean["source_url"] = source_url
        clean["start_offset"] = clean.get("start_offset", "0") or "0"
        clean["end_offset"] = clean.get("end_offset", "0") or "0"
        # Backward compatibility: tolerate legacy Golden Source columns we no longer use.
        clean.pop("verified", None)
        rows.append(clean)
    return rows


def _fetch_golden_source_catalog(url, force_refresh=False, cache_ttl_sec=1800):
    normalized = _normalize_golden_source_url(url)
    if not normalized:
        raise ValueError("Golden Source URL is not configured")

    # Allow local file path as source for truly local matching.
    local_path = Path(normalized)
    if local_path.exists() and local_path.is_file():
        text = local_path.read_text(encoding="utf-8-sig", errors="replace")
        rows = _parse_golden_source_csv(text)
        return normalized, rows, 0.0, "local-file"

    cache_path = _golden_cache_path(normalized)
    now = _time.time()
    if not force_refresh and cache_path.exists():
        age = now - cache_path.stat().st_mtime
        if age <= max(0, int(cache_ttl_sec or 0)):
            text = cache_path.read_text(encoding="utf-8-sig", errors="replace")
            rows = _parse_golden_source_csv(text)
            return normalized, rows, 0.0, "local-cache"

    t0 = _time.perf_counter()
    r = http_requests.get(normalized, timeout=20)
    r.raise_for_status()
    fetch_ms = round((_time.perf_counter() - t0) * 1000, 1)
    text = r.content.decode("utf-8-sig", errors="replace")
    rows = _parse_golden_source_csv(text)
    cache_path.write_text(text, encoding="utf-8")
    return normalized, rows, fetch_ms, "remote-fetch"

def import_csv_reader(text):
    import csv as _csv
    return _csv.DictReader(text.splitlines())

def _resolve_row_tmdb_id(row, cfg):
    tmdb_id = str(row.get("tmdb_id", "") or "").strip()
    if tmdb_id: return tmdb_id
    tmdb_key = cfg.get("tmdb_api_key", "")
    if not tmdb_key: return ""
    data = _tmdb_lookup(row.get("title","") or row.get("plex_title",""), row.get("year",""), tmdb_key)
    if not data: return ""
    if str(data.get("media_type","")) != "movie": return ""
    return str(data.get("id","") or "").strip()

# ─── Template ─────────────────────────────────────────────────────────────────
_tpl_path = Path("/app/web/template.html")
def _load_template():
    if _tpl_path.exists(): return _tpl_path.read_text(encoding="utf-8")
    local = Path(__file__).parent / "template.html"
    if local.exists(): return local.read_text(encoding="utf-8")
    return "<h1>Template not found</h1>"

def load_ui_terminology():
    try:
        with open(UI_TERMINOLOGY_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        local = Path(__file__).parent / "ui_terminology.yaml"
        if local.exists():
            try:
                return yaml.safe_load(local.read_text(encoding="utf-8")) or {}
            except Exception:
                return {}
    return {}

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index(): return _load_template()

@app.route("/api/config", methods=["GET"])
def get_config():
    cfg = load_config(); cfg.pop("ui_token", None); return jsonify(cfg)

@app.route("/api/config", methods=["POST"])
def post_config():
    cfg = load_config(); cfg.update(request.json); save_config(cfg); return jsonify({"ok": True})

@app.route("/api/ui-terminology", methods=["GET"])
def get_ui_terminology():
    return jsonify(load_ui_terminology())


@app.route("/api/status-model", methods=["GET"])
def get_status_model():
    return jsonify(
        {
            "statuses": list(STATUS_ORDER),
            "manual_transitions": {
                status: list(MANUAL_STATUS_TRANSITIONS.get(status, ()))
                for status in STATUS_ORDER
            },
            "manual_any": ["UNMONITORED"],
        }
    )

@app.route("/api/test/plex", methods=["POST"])
def test_plex():
    data = request.json or {}
    url = data.get("url","").rstrip("/"); token = data.get("token","")
    try:
        r = http_requests.get(f"{url}/library/sections",
                              headers={"X-Plex-Token":token,"Accept":"application/json"}, timeout=8)
        r.raise_for_status()
        libs = r.json().get("MediaContainer",{}).get("Directory",[])
        return jsonify({"ok":True,"libraries":len(libs)})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:120]})

@app.route("/api/plex/libraries", methods=["POST"])
def plex_libraries():
    data = request.json or {}
    url = data.get("url","").rstrip("/"); token = data.get("token","")
    try:
        r = http_requests.get(f"{url}/library/sections",
                              headers={"X-Plex-Token":token,"Accept":"application/json"}, timeout=8)
        r.raise_for_status()
        all_libs = r.json().get("MediaContainer",{}).get("Directory",[])
        libs = [{"name":d["title"],"type":d.get("type","")} for d in all_libs if d.get("type") in ("movie","show")]
        return jsonify({"ok":True,"libraries":libs})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:120]})

@app.route("/api/movie/bio")
def movie_bio():
    rating_key = request.args.get("key","")
    lib = request.args.get("library","")
    if not rating_key: return jsonify({"summary":""})
    cache_key = f"{lib}:{rating_key}"
    now = _time.time()
    cached = _bio_cache.get(cache_key)
    if cached and now - cached[0] < _BIO_TTL: return jsonify({"summary":cached[1]})
    cfg = load_config()
    tmdb_key = cfg.get("tmdb_api_key","")
    plex_url = cfg.get("plex_url","").rstrip("/")
    plex_token = cfg.get("plex_token","")
    if tmdb_key:
        try:
            lpath = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
            rows = load_ledger(lpath)
            row = next((r for r in rows if r.get("rating_key") == rating_key), None)
            title = (row or {}).get("title") or (row or {}).get("plex_title","")
            year = (row or {}).get("year","")
            if title:
                params = {"api_key":tmdb_key,"query":title,"language":"en-US"}
                if year: params["year"] = year
                sr = http_requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=8)
                if sr.status_code == 200:
                    results = sr.json().get("results",[])
                    if not results:
                        sr2 = http_requests.get("https://api.themoviedb.org/3/search/tv",
                                                params={"api_key":tmdb_key,"query":title,"language":"en-US"}, timeout=8)
                        if sr2.status_code == 200: results = sr2.json().get("results",[])
                    if results:
                        overview = results[0].get("overview","")
                        if overview:
                            _bio_cache[cache_key] = (now, overview)
                            return jsonify({"summary": overview})
        except Exception: pass
    if plex_url and plex_token:
        try:
            r = http_requests.get(f"{plex_url}/library/metadata/{rating_key}",
                headers={"X-Plex-Token":plex_token,"Accept":"application/json"}, timeout=8)
            if r.status_code == 200:
                meta = r.json().get("MediaContainer",{}).get("Metadata",[{}])
                summary = meta[0].get("summary","") if meta else ""
                if summary:
                    _bio_cache[cache_key] = (now, summary)
                    return jsonify({"summary": summary})
        except Exception: pass
    return jsonify({"summary":""})

@app.route("/api/test/tmdb", methods=["POST"])
def test_tmdb():
    key = (request.json or {}).get("key","")
    try:
        r = http_requests.get("https://api.themoviedb.org/3/configuration", params={"api_key":key}, timeout=8)
        if r.status_code == 401: return jsonify({"ok":False,"error":"Invalid API key"})
        r.raise_for_status(); return jsonify({"ok":True})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:120]})

@app.route("/api/test/golden-source", methods=["POST"])
def test_golden_source():
    data = request.json or {}
    url = data.get("url") or load_config().get("golden_source_url","")
    try:
        normalized_url, rows, fetch_ms, fetch_mode = _fetch_golden_source_catalog(url)
        if not rows:
            return jsonify({"ok":False,"error":"CSV loaded but no usable rows found (need tmdb_id column)"})
        return jsonify({
            "ok": True,
            "source_url": normalized_url,
            "rows": len(rows),
            "fetch_ms": fetch_ms,
            "fetch_mode": fetch_mode,
            "required_columns": ["tmdb_id", "source_url"],
            "optional_columns": ["title", "year", "start_offset", "updated_at", "notes"],
        })
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:180]})

@app.route("/api/cookies")
def list_cookies():
    config_dir = Path("/app/config")
    files = [str(f) for f in config_dir.glob("*.txt")] if config_dir.exists() else []
    return jsonify({"files": files})

@app.route("/api/ledger", methods=["GET"])
def get_ledger():
    lib = request.args.get("library","")
    path = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    return jsonify(load_ledger(path))

@app.route("/api/ledger/<key>", methods=["PATCH"])
def patch_ledger(key):
    lib = request.args.get("library","")
    path = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    rows = load_ledger(path)
    for row in rows:
        if row.get("rating_key") == key:
            req = request.json or {}
            saved_row, err = _save_ledger_row_updates(row, req, default_notes="Edited via web UI")
            if err:
                return jsonify(err), 400
            save_ledger(path, rows)
            return jsonify({"ok":True, "row": _ledger_row_response(saved_row)})
    return jsonify({"error":"not found"}), 404

@app.route("/api/ledger/manual-source", methods=["POST"])
def save_manual_source():
    data = request.json or {}
    key = str(data.get("rating_key", "") or "").strip()
    lib = str(data.get("library", "") or "").strip()
    if not key:
        return jsonify({"ok": False, "error": "Missing rating_key"}), 400
    if not lib:
        return jsonify({"ok": False, "error": "Missing library"}), 400
    url = str(data.get("url", "") or "").strip()
    target_status = str(data.get("target_status", data.get("status", "")) or "").strip().upper()
    if not url:
        return jsonify({"ok": False, "error": "Missing url"}), 400
    if not target_status:
        return jsonify({"ok": False, "error": "Missing target_status"}), 400

    path = ledger_path_for(lib)
    rows = load_ledger(path)
    row = next((r for r in rows if str(r.get("rating_key", "") or "").strip() == key), None)
    if not row:
        return jsonify({"ok": False, "error": "not found"}), 404

    updates = {
        "url": url,
        "start_offset": data.get("start_offset", "0"),
        "notes": data.get("notes", ""),
        "status": target_status,
    }
    saved_row, err = _save_ledger_row_updates(row, updates)
    if err:
        err["library"] = lib
        return jsonify(err), 400

    save_ledger(path, rows)
    return jsonify({"ok": True, "row": _ledger_row_response(saved_row)})

@app.route("/api/ledger/bulk", methods=["POST"])
def bulk_ledger():
    lib = request.args.get("library","")
    path = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    data = request.json
    keys = set(data.get("keys",[]))
    status = str(data.get("status","") or "").upper()
    rows = load_ledger(path); now = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); count=0; skipped=0
    for row in rows:
        if row.get("rating_key") in keys:
            cur = str(row.get("status","")).upper()
            err = _status_validation_error(row, status)
            if err:
                err["rating_key"] = row.get("rating_key", "")
                err["title"] = row.get("title") or row.get("plex_title") or ""
                err["requested_keys"] = len(keys)
                return jsonify(err), 400
            row["status"] = status; row["last_updated"] = now
            row["notes"] = f"Bulk {cur}->{status} via web UI"; count += 1
    save_ledger(path, rows)
    return jsonify({"ok":True,"updated":count,"skipped":skipped})

@app.route("/api/ledger/clear-sources", methods=["POST"])
def clear_selected_sources():
    data = request.json or {}
    lib = (request.args.get("library", "") or data.get("library", "") or "").strip()
    path = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    keys = [str(k) for k in (data.get("keys", []) or []) if str(k).strip()]
    if not keys:
        return jsonify({"ok": False, "error": "No ledger rows selected"}), 400
    rows = load_ledger(path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = _clear_source_urls_for_rows(
        rows,
        keys=keys,
        note="Source URL cleared via Library",
        now=now,
    )
    missing_keys = sorted(set(keys) - {str(row.get("rating_key", "") or "") for row in rows})
    if summary["cleared"]:
        save_ledger(path, rows)
    summary["missing_keys"] = missing_keys
    summary["library"] = lib
    return jsonify({"ok": True, "summary": summary})

@app.route("/api/golden-source/import", methods=["POST"])
def import_golden_source():
    data = request.json or {}
    lib = data.get("library","")
    if not lib: return jsonify({"ok":False,"error":"Missing library"}), 400
    cfg = load_config()
    source_url = data.get("url") or cfg.get("golden_source_url","")
    overwrite = _boolish(data.get("overwrite_existing", False))
    auto_approve = _boolish(data.get("auto_approve", False))
    force_refresh = _boolish(data.get("force_refresh", cfg.get("refresh_golden_source_each_run", True)))
    cache_ttl_sec = int(cfg.get("golden_source_cache_ttl_sec", 1800) or 1800)
    resolve_missing_tmdb = _boolish(data.get("resolve_missing_tmdb", cfg.get("golden_source_resolve_tmdb", False)))
    t0_total = _time.perf_counter()
    try:
        normalized_url, catalog_rows, fetch_ms, fetch_mode = _fetch_golden_source_catalog(
            source_url,
            force_refresh=force_refresh,
            cache_ttl_sec=cache_ttl_sec,
        )
    except Exception as e:
        return jsonify({"ok":False,"error":f"Golden Source fetch failed: {str(e)[:180]}"}), 400
    catalog = {str(r.get("tmdb_id","")).strip(): r for r in catalog_rows if str(r.get("tmdb_id","")).strip()}
    if not catalog:
        return jsonify({"ok":False,"error":"Golden Source CSV had no usable rows"}), 400

    def _norm_title(v):
        v = re.sub(r"[^a-z0-9]+"," ",str(v or "").lower())
        return re.sub(r"\s+"," ",v).strip()

    catalog_title_year = {}
    for r in catalog_rows:
        title = _norm_title(r.get("title",""))
        year = str(r.get("year","") or "").strip()
        if title and year: catalog_title_year[f"{title}|{year}"] = r

    path = ledger_path_for(lib)
    rows = load_ledger(path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    imported = skipped_existing = missing_tmdb = no_match = 0
    tmdb_cache = {}
    for row in rows:
        cur = str(row.get("status","") or "").upper()
        match = None
        tmdb_id = str(row.get("tmdb_id","") or "").strip()
        if tmdb_id: match = catalog.get(tmdb_id)
        if not match and catalog_title_year:
            title_key = _norm_title(row.get("title","") or row.get("plex_title",""))
            year_key = str(row.get("year","") or "").strip()
            if title_key and year_key:
                match = catalog_title_year.get(f"{title_key}|{year_key}")
                if match and not tmdb_id:
                    tmdb_id = str(match.get("tmdb_id","") or "").strip()
        if not match and resolve_missing_tmdb:
            key = str(row.get("rating_key","") or "")
            cached = tmdb_cache.get(key)
            if cached is None:
                cached = _resolve_row_tmdb_id(row, cfg); tmdb_cache[key] = cached
            tmdb_id = tmdb_id or cached
            if tmdb_id:
                match = catalog.get(str(tmdb_id))

        if not match:
            if tmdb_id:
                no_match += 1
            else:
                missing_tmdb += 1
            continue
        existing_url = str(row.get("url","") or "").strip()
        if existing_url and not overwrite:
            skipped_existing += 1
            if tmdb_id: row["tmdb_id"] = tmdb_id
            continue
        row["tmdb_id"] = tmdb_id or str(match.get("tmdb_id","") or "").strip()
        incoming_url = str(match.get("source_url", "") or "").strip()
        row["golden_source_url"] = incoming_url
        row["golden_source_offset"] = match.get("start_offset","0") or "0"
        row["end_offset"] = match.get("end_offset","0") or "0"
        row["source_origin"] = "golden_source" if incoming_url else "unknown"
        if cur == "UNMONITORED":
            pass
        elif not incoming_url and cur != "AVAILABLE":
            row["status"] = "MISSING"
        elif cur != "AVAILABLE":
            row["status"] = "APPROVED" if auto_approve else "STAGED"
        row["last_updated"] = now
        if incoming_url:
            row["notes"] = f"Imported from Golden Source ({Path(normalized_url).name})"
        else:
            row["notes"] = f"Golden Source cleared source URL ({Path(normalized_url).name})"
        imported += 1
    save_ledger(path, rows)
    total_ms = round((_time.perf_counter()-t0_total)*1000, 1)
    return jsonify({
        "ok": True,
        "source_url": normalized_url,
        "catalog_rows": len(catalog_rows),
        "matched": imported + skipped_existing,
        "imported": imported,
        "skipped_existing": skipped_existing,
        "missing_tmdb": missing_tmdb,
        "no_match": no_match,
        "fetch_ms": fetch_ms,
        "fetch_mode": fetch_mode,
        "cache_ttl_sec": cache_ttl_sec,
        "resolve_missing_tmdb": resolve_missing_tmdb,
        "total_ms": total_ms,
    })

@app.route("/api/theme/info")
def theme_info():
    folder = request.args.get("folder","")
    cfg = load_config(); filename = cfg.get("theme_filename","theme.mp3"); roots = get_media_roots(cfg)
    if not is_allowed_folder(folder, roots): return jsonify({"error":"forbidden"}), 403
    path = Path(folder) / filename
    if not path.exists(): return jsonify({"error":"not found"}), 404
    dur = get_audio_duration(path); size = path.stat().st_size
    return jsonify({
        "duration": dur,
        "size": size,
        "size_kb": round(size/1024, 1),
        "folder": folder,
        "filename": filename,
        "path": str(path),
    })

@app.route("/api/theme/trim", methods=["POST"])
def trim_theme():
    data = request.json or {}
    lib = data.get("library",""); key = data.get("rating_key","")
    s_off = int(data.get("start_offset",0)); e_off = int(data.get("end_offset",0))
    cfg = load_config(); filename = cfg.get("theme_filename","theme.mp3")
    roots = get_media_roots(cfg); audio_fmt = cfg.get("audio_format","mp3")
    max_dur = int(cfg.get("max_theme_duration",0))
    path_ledger = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    rows = load_ledger(path_ledger)
    row = next((r for r in rows if str(r.get("rating_key","")) == str(key)), None)
    if not row:
        return jsonify({"ok":False,"error":f"Not found in ledger — key={key}"})
    folder = row.get("folder","")
    if not is_allowed_folder(folder, roots): return jsonify({"ok":False,"error":"Folder not allowed"}), 403
    theme_path = Path(folder) / filename
    if not theme_path.exists(): return jsonify({"ok":False,"error":f"Theme file not on disk: {theme_path}"})
    try:
        dur = get_audio_duration(theme_path)
        if dur <= 0: return jsonify({"ok":False,"error":"Could not read audio duration"})
        start = max(0,s_off); end = dur - max(0,e_off) if e_off > 0 else dur
        if max_dur > 0 and (end-start) > max_dur: end = start + max_dur
        if start >= dur: return jsonify({"ok":False,"error":f"Start offset ({s_off}s) exceeds file duration ({dur:.1f}s)"})
        if end <= start: return jsonify({"ok":False,"error":f"Nothing left after trimming"})
        if start <= 0 and end >= dur:
            row["start_offset"] = str(s_off); row["end_offset"] = str(e_off)
            row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row["notes"] = f"No trim needed ({dur:.1f}s)"
            save_ledger(path_ledger, rows)
            return jsonify({"ok":True,"message":f"No trim needed — {dur:.1f}s","duration":dur})
        tmp = theme_path.with_suffix(f".trim.{audio_fmt}")
        trim_cmd = ["ffmpeg","-y","-i",str(theme_path),"-ss",str(start),"-to",str(end),"-c","copy",str(tmp)]
        trim_result = subprocess.run(trim_cmd, capture_output=True, text=True, timeout=60)
        if trim_result.returncode != 0:
            tmp.unlink(missing_ok=True)
            return jsonify({"ok":False,"error":f"ffmpeg error: {trim_result.stderr[:150]}"})
        tmp.replace(theme_path); new_dur = get_audio_duration(theme_path)
        row["start_offset"] = str(s_off); row["end_offset"] = str(e_off)
        row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row["notes"] = f"Trimmed: {dur:.1f}s → {new_dur:.1f}s"
        row, _ = sync_theme_cache(row, filename, probe_duration=True)
        save_ledger(path_ledger, rows)
        return jsonify({"ok":True,"message":f"Trimmed {dur:.1f}s → {new_dur:.1f}s","duration":new_dur})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:200]})

# ── /api/theme/delete — BUG FIXES:
#    1. Clears theme_exists/size/duration/mtime after deletion (was only clearing status).
#    2. Much more informative error message when folder is not in allowed roots.
#    3. Handles the case where the file is already gone cleanly.
@app.route("/api/theme/delete", methods=["POST"])
def delete_theme():
    data = request.json or {}
    lib = (data.get("library","") or "").strip()
    key = str(data.get("rating_key","") or "").strip()
    folder_hint = str(data.get("folder","") or "").strip()

    cfg = load_config()
    filename = cfg.get("theme_filename","theme.mp3")
    roots = get_media_roots(cfg)
    allowed_names = {"theme.mp3","theme.m4a","theme.flac","theme.opus"}
    if filename not in allowed_names:
        return jsonify({"ok":False,"error":f"Unexpected theme filename: {filename}"})

    # ── Find the row ──────────────────────────────────────────────────────────
    path_ledger = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    rows = load_ledger(path_ledger)
    tmdb_id = str(data.get("tmdb_id", "") or "").strip()
    row, matched_by = _find_row_by_identity(rows, key, folder_hint, tmdb_id)

    # Cross-library fallback
    if not row:
        for lib_entry in load_config().get("libraries", []):
            lib_name = lib_entry.get("name","")
            if not lib_name or lib_name == lib: continue
            alt_path = ledger_path_for(lib_name)
            alt_rows = load_ledger(alt_path)
            row, matched_by = _find_row_by_identity(alt_rows, key, folder_hint, tmdb_id)
            if row:
                path_ledger = alt_path
                rows = alt_rows
                break

    # ── Resolve folder ────────────────────────────────────────────────────────
    folder = (row.get("folder","") if row else None) or folder_hint

    if not folder:
        detail = f"key={key!r}, lib={lib!r}, folder_hint={folder_hint!r}, ledger_size={len(rows)}"
        return jsonify({"ok":False,"error":f"Media item not found in ledger and no folder provided. ({detail})"})

    if not is_allowed_folder(folder, roots):
        return jsonify({
            "ok": False,
            "error": (
                f"Folder is not inside an allowed media root. "
                f"folder={folder!r} — roots={roots}. "
                f"Check that media_roots in config.yaml matches the path Plex uses for this library inside the container."
            )
        }), 403

    theme_path = Path(folder) / filename
    if theme_path.name != filename:
        return jsonify({"ok":False,"error":"Path mismatch — refusing to delete"})

    # ── Clear theme metadata helper ───────────────────────────────────────────
    def _clear_theme_metadata(r):
        r["status"]         = "MISSING"
        r["theme_exists"]   = 0
        r["theme_duration"] = 0.0
        r["theme_size"]     = 0
        r["theme_mtime"]    = 0.0
        r["last_updated"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not theme_path.exists():
        if row:
            _clear_theme_metadata(row)
            row["notes"] = "Theme file already missing — status reset"
            save_ledger(path_ledger, rows)
        return jsonify({"ok":True,"message":"File already gone — status reset to Missing","matched_by":matched_by or "folder_hint"})

    try:
        theme_path.unlink()
        if row:
            _clear_theme_metadata(row)
            row["notes"] = "Theme deleted via Theme manager"
            save_ledger(path_ledger, rows)
            return jsonify({"ok":True,"message":f"Deleted {filename} — status reset to Missing","matched_by":matched_by or "folder_hint"})
        return jsonify({"ok":True,"message":f"Deleted {filename} (ledger row not found — status not updated)","matched_by":matched_by or "folder_hint"})
    except PermissionError:
        return jsonify({"ok":False,"error":f"Permission denied deleting {theme_path}. Check file ownership."})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:200]})

@app.route("/api/theme/download-now", methods=["POST"])
def download_now():
    """Immediately download the source URL for a single row."""
    data = request.json or {}
    lib = (data.get("library","") or "").strip()
    key = str(data.get("rating_key","") or "").strip()
    folder_hint = str(data.get("folder","") or "").strip()
    tmdb_id = str(data.get("tmdb_id","") or "").strip()
    if not key and not folder_hint:
        return jsonify({"ok":False,"error":"Missing identity: provide rating_key or folder"}), 400
    cfg = load_config()
    filename = cfg.get("theme_filename","theme.mp3")
    roots = get_media_roots(cfg)
    cookies_file = cfg.get("cookies_file","") or None
    audio_format = cfg.get("audio_format","mp3")
    quality_profile = cfg.get("quality_profile","high")
    max_dur = int(cfg.get("max_theme_duration",0) or 0)
    path_ledger = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    rows = load_ledger(path_ledger)
    row, matched_by = _find_row_by_identity(rows, key, folder_hint, tmdb_id)
    if not row:
        return jsonify({"ok":False,"error":f"Not found in ledger for library '{lib}'"}), 404
    url = (row.get("url","") or "").strip()
    if not url: return jsonify({"ok":False,"error":"No source URL on this row — add a source first"}), 400
    folder = row.get("folder","")
    if not is_allowed_folder(folder, roots):
        return jsonify({"ok":False,"error":f"Folder not allowed: {folder!r} — check media_roots in config.yaml"}), 403
    theme_path = Path(folder) / filename
    replaced_existing = False
    if theme_path.exists():
        try:
            theme_path.unlink(missing_ok=True)
            replaced_existing = True
        except Exception as e:
            return jsonify({"ok":False,"error":f"Failed to replace existing theme file: {str(e)[:160]}"}), 500
    quality_map = {"high":"bestaudio","balanced":"bestaudio[abr<=192]/bestaudio",
                   "small":"bestaudio[abr<=128]/bestaudio","smallest":"bestaudio[abr<=96]/bestaudio"}
    fmt_str = quality_map.get(quality_profile,"bestaudio")
    ext = audio_format if audio_format in {"mp3","m4a","flac","opus"} else "mp3"
    slug = re.sub(r"[^a-z0-9]","",key.lower())[:8] or "dl"
    tmp_template = str(Path(folder) / f"mt_tmp_{slug}.%(ext)s")
    cmd = ["yt-dlp","--no-warnings","-x","--audio-format",ext,"--audio-quality","0",
           "-f",fmt_str,"-o",tmp_template,"--playlist-items","1"]
    if cookies_file and Path(cookies_file).exists():
        cmd += ["--cookies", cookies_file]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "yt-dlp error").strip()[:300]
            return jsonify({"ok":False,"error":f"Download failed: {err}"})
        downloaded = next(Path(folder).glob(f"mt_tmp_{slug}.*"), None)
        if not downloaded or not downloaded.exists():
            return jsonify({"ok":False,"error":"yt-dlp succeeded but output file not found"})
        start_offset = int(row.get("start_offset",0) or 0)
        end_offset = int(row.get("end_offset",0) or 0)
        dur = get_audio_duration(downloaded)
        if dur > 0 and (start_offset > 0 or end_offset > 0 or (max_dur > 0 and dur > max_dur)):
            start = max(0, start_offset)
            end = dur - max(0, end_offset) if end_offset > 0 else dur
            if max_dur > 0 and (end - start) > max_dur: end = start + max_dur
            if 0 < start < dur and end > start:
                tmp_trim = Path(folder) / f"mt_trim_{slug}.{ext}"
                trim_cmd = ["ffmpeg","-y","-i",str(downloaded),"-ss",str(start),"-to",str(end),"-c","copy",str(tmp_trim)]
                trim_result = subprocess.run(trim_cmd, capture_output=True, text=True, timeout=60)
                if trim_result.returncode == 0:
                    downloaded.unlink(missing_ok=True); downloaded = tmp_trim
        downloaded.rename(theme_path)
        row["status"] = "AVAILABLE"
        row["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row["notes"] = "Downloaded via manual download (replaced existing local theme)" if replaced_existing else "Downloaded via manual download"
        # FIX: sync theme cache so theme_exists / duration / size / mtime are accurate
        row, _ = sync_theme_cache(row, filename, probe_duration=True)
        save_ledger(path_ledger, rows)
        msg = f"Downloaded and replaced existing {filename}" if replaced_existing else f"Downloaded and saved as {filename}"
        return jsonify({"ok":True,"message":msg,"matched_by":matched_by or "rating_key","replaced_existing":replaced_existing})
    except subprocess.TimeoutExpired:
        for f in Path(folder).glob(f"mt_tmp_{slug}.*"): f.unlink(missing_ok=True)
        return jsonify({"ok":False,"error":"Download timed out (180s)"})
    except Exception as e:
        for f in Path(folder).glob(f"mt_tmp_{slug}.*"): f.unlink(missing_ok=True)
        return jsonify({"ok":False,"error":str(e)[:200]})

# ── NEW: sync theme file metadata for a library without a full Plex scan ──────
@app.route("/api/library/sync-themes", methods=["POST"])
def sync_library_themes():
    """
    Refreshes theme_exists / theme_duration / theme_size / theme_mtime for every
    row without needing a full Plex scan (Pass 1).
    Useful after manual file operations or to reconcile stale status.
    """
    data = request.json or {}
    lib = (data.get("library","") or "").strip()
    if not lib:
        return jsonify({"ok": False, "error": "Missing library"}), 400
    cfg = load_config()
    filename = cfg.get("theme_filename", "theme.mp3")
    path = ledger_path_for(lib)
    rows = load_ledger(path)
    updated = found = missing = promoted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in rows:
        new_row, changed = sync_theme_cache(row, filename, probe_duration=False)
        if changed:
            row.update(new_row)
            updated += 1
        new_exists = int(row.get("theme_exists", 0) or 0)
        if new_exists:
            found += 1
            # Promote status if file is present but status doesn't reflect it.
            if str(row.get("status", "") or "").upper() != "AVAILABLE":
                row["status"] = "AVAILABLE"
                row["last_updated"] = now
                row["notes"] = "Promoted to Available — theme file detected on disk"
                updated += 1
                promoted += 1
        else:
            missing += 1
            # Reset status if file is gone but status still claims availability.
            if str(row.get("status", "") or "").upper() == "AVAILABLE":
                row["status"] = "MISSING"
                row["last_updated"] = now
                row["notes"] = "Reset to Missing — theme file no longer on disk"
                updated += 1
    if updated:
        save_ledger(path, rows)
    return jsonify({
        "ok": True,
        "library": lib,
        "total": len(rows),
        "updated": updated,
        "themes_found": found,
        "themes_missing": missing,
        "promoted": promoted,
    })

@app.route("/api/theme")
def serve_theme():
    folder = request.args.get("folder","")
    cfg = load_config(); filename = cfg.get("theme_filename","theme.mp3"); roots = get_media_roots(cfg)
    if not is_allowed_folder(folder, roots): return jsonify({"error":"forbidden"}), 403
    path = Path(folder) / filename
    if not path.exists(): return jsonify({"error":"not found"}), 404
    return send_file(str(path), mimetype="audio/mpeg", conditional=True)

@app.route("/api/poster")
def serve_poster():
    rk = request.args.get("key","")
    if rk in _poster_cache:
        data, ct = _poster_cache[rk]; return Response(data, mimetype=ct)
    cfg = load_config(); plex_url = cfg.get("plex_url","").rstrip("/"); plex_token = cfg.get("plex_token","")
    if not plex_url or not plex_token: return "",404
    try:
        r = http_requests.get(f"{plex_url}/library/metadata/{rk}/thumb",
                              headers={"X-Plex-Token":plex_token}, timeout=8)
        if r.status_code == 200:
            ct = r.headers.get("Content-Type","image/jpeg")
            if len(_poster_cache) > 500: _poster_cache.clear()
            _poster_cache[rk] = (r.content, ct)
            return Response(r.content, mimetype=ct)
    except Exception: pass
    return "",404

@app.route("/api/poster/tmdb")
def tmdb_poster():
    title = (request.args.get("title","") or "").strip()
    year = (request.args.get("year","") or "").strip()
    size = (request.args.get("size","") or "w342").strip()
    if not title: return "",404
    cfg = load_config(); tmdb_key = cfg.get("tmdb_api_key","")
    if not tmdb_key: return "",404
    url = _tmdb_poster_url(title, year, tmdb_key, size=size)
    if not url: return "",404
    return "",302,{"Location":url,"Cache-Control":"public, max-age=86400"}

@app.route("/api/tmdb/lookup")
def tmdb_lookup():
    title = (request.args.get("title","") or "").strip()
    year = (request.args.get("year","") or "").strip()
    if not title: return jsonify({"ok":False,"error":"missing title"}), 400
    cfg = load_config(); tmdb_key = cfg.get("tmdb_api_key","")
    if not tmdb_key: return jsonify({"ok":False,"error":"missing tmdb key"}), 400
    data = _tmdb_lookup(title, year, tmdb_key)
    if not data: return jsonify({"ok":False,"error":"not found"}), 404
    return jsonify({"ok":True,**data})

@app.route("/api/media")
def get_media():
    lib = request.args.get("library",""); show = request.args.get("show","with_theme")
    nocache = request.args.get("nocache","")
    cache_key = f"{lib}:{show}"
    if not nocache and cache_key in _media_cache:
        ts, cached = _media_cache[cache_key]
        if _time.time() - ts < 30: return jsonify(cached)
    cfg = load_config(); filename = cfg.get("theme_filename","theme.mp3")
    path = ledger_path_for(lib) if lib else str(LOGS_DIR/"theme_log.csv")
    rows = load_ledger(path); media = []
    for row in rows:
        status = row.get("status","")
        theme_path = Path(row.get("folder","")) / filename
        has_theme = theme_path.exists()
        dur = get_audio_duration(theme_path) if has_theme else 0
        if show == "with_theme" and not has_theme: continue
        if show == "without_theme" and has_theme: continue
        media.append({"rating_key":row.get("rating_key",""),"title":row.get("title",""),
                      "plex_title":row.get("plex_title",""),"year":row.get("year",""),
                      "folder":row.get("folder",""),"url":row.get("url",""),
                      "start_offset":row.get("start_offset","0"),"end_offset":row.get("end_offset","0"),
                      "duration":dur,"has_theme":has_theme,"status":status,
                      "last_updated":row.get("last_updated","")})
    media.sort(key=lambda r: r.get("title","").lower())
    _media_cache[cache_key] = (_time.time(), media)
    return jsonify(media)

@app.route("/api/youtube/search", methods=["POST"])
def youtube_search():
    data = request.json or {}; query = data.get("query","")
    if not query: return jsonify({"ok":False,"error":"No query"})
    cfg = load_config(); cookies_file = cfg.get("cookies_file","") or None
    flags = ["yt-dlp","--no-warnings","--quiet"]
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    import urllib.parse
    search_url = "https://www.youtube.com/results?search_query=" + urllib.parse.quote(query)
    try:
        result = subprocess.run(
            flags + ["--flat-playlist","--print","%(title)s\t%(url)s\t%(duration_string)s",
                     "--playlist-items","1:10", search_url],
            capture_output=True, text=True, timeout=30)
        results = []
        for line in result.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[1].startswith("https://"):
                results.append({"title":parts[0],"url":parts[1],"duration":parts[2] if len(parts)>2 else ""})
        return jsonify({"ok":True,"results":results})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:150]})

@app.route("/api/preview", methods=["POST"])
def preview_url():
    data = request.json or {}; url = data.get("url","")
    if not url: return jsonify({"ok":False,"error":"No URL provided"})
    cfg = load_config(); cookies_file = cfg.get("cookies_file","") or None
    flags = ["yt-dlp","--no-warnings","--quiet"]
    if cookies_file and Path(cookies_file).exists():
        flags += ["--cookies", cookies_file]
    cmd = flags + ["--format","bestaudio","--get-url","--playlist-items","1","--yes-playlist", url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return jsonify({"ok":False,"error":(result.stderr or "yt-dlp error")[:150]})
        stream_url = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
        if not stream_url: return jsonify({"ok":False,"error":"Could not extract stream URL"})
        key = hashlib.md5(url.encode()).hexdigest()[:12]
        _stream_cache[key] = (_time.time(), stream_url); _prune_stream_cache()
        return jsonify({"ok":True,"audio_url":f"/api/preview/proxy/{key}"})
    except subprocess.TimeoutExpired:
        return jsonify({"ok":False,"error":"URL extraction timed out"})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)[:150]})

@app.route("/api/preview/proxy/<key>")
def proxy_preview(key):
    _prune_stream_cache()
    if key not in _stream_cache: return jsonify({"error":"no stream"}), 404
    stream_url = _stream_cache[key][1]
    try:
        r = http_requests.get(stream_url, stream=True, timeout=30,
                              headers={"Range": request.headers.get("Range","")})
        headers = {"Content-Type":r.headers.get("Content-Type","audio/webm"),"Accept-Ranges":"bytes"}
        if "Content-Length" in r.headers: headers["Content-Length"] = r.headers["Content-Length"]
        if "Content-Range" in r.headers: headers["Content-Range"] = r.headers["Content-Range"]
        return Response(r.iter_content(chunk_size=8192), status=r.status_code, headers=headers)
    except Exception as e:
        return jsonify({"error":str(e)[:150]}), 500

@app.route("/api/run/pass/<int:pass_num>", methods=["POST"])
def trigger_pass(pass_num):
    global _run_active
    data = request.get_json(silent=True) or {}
    libraries = data.get("libraries")
    library = str(data.get("library") or "").strip()
    if libraries is not None and not isinstance(libraries, list):
        return jsonify({"error": "libraries must be an array"}), 400
    explicit_libraries = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    if library:
        explicit_libraries = [library]
    scope_label = str(data.get("scope_label") or "").strip()
    with _run_lock:
        if _run_active: return jsonify({"error":"run in progress"}), 409
        _run_active = True
    threading.Thread(target=_do_run, args=(pass_num, explicit_libraries, scope_label), daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/run/scan", methods=["POST"])
def trigger_scan():
    global _run_active
    data = request.get_json(silent=True) or {}
    libraries = data.get("libraries")
    library = str(data.get("library") or "").strip()
    if libraries is not None and not isinstance(libraries, list):
        return jsonify({"error": "libraries must be an array"}), 400
    explicit_libraries = [str(name).strip() for name in (libraries or []) if str(name).strip()]
    if library:
        explicit_libraries = [library]
    scope_label = str(data.get("scope_label") or "").strip()
    with _run_lock:
        if _run_active: return jsonify({"error":"run in progress"}), 409
        _run_active = True
    threading.Thread(target=_do_run, args=(1, explicit_libraries, scope_label), daemon=True).start()
    return jsonify({"ok":True})

@app.route("/api/run/stop", methods=["POST"])
def stop_run():
    global _run_proc, _run_stop_requested
    if _run_proc and _run_proc.poll() is None:
        _run_stop_requested = True
        try: _run_proc.send_signal(signal.SIGTERM); _broadcast("[STOP] Graceful stop requested…")
        except Exception: _run_proc.kill()
        return jsonify({"ok":True})
    return jsonify({"ok":False,"error":"No run in progress"})

def _do_run(force_pass=0, explicit_libraries=None, scope_label=""):
    global _run_active, _run_proc, _run_stop_requested, _run_started_at, _run_last_line, _run_pass, _run_scope_label, _run_libraries
    run_log = []; timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); run_pass = force_pass or 0
    proc = None
    return_code = None
    stop_requested = False
    outcome = "error"
    explicit_libraries = [str(name).strip() for name in (explicit_libraries or []) if str(name).strip()]
    try:
        resolved_scope = scope_label or (
            explicit_libraries[0] if len(explicit_libraries) == 1
            else f"{len(explicit_libraries)} selected libraries" if explicit_libraries
            else "scheduled libraries"
        )
        _run_started_at = _time.time(); _run_last_line = ""; _run_pass = run_pass; _run_stop_requested = False
        _run_scope_label = resolved_scope
        _run_libraries = list(explicit_libraries)
        env = {**os.environ, "CONFIG_PATH": CONFIG_PATH}
        if force_pass: env["FORCE_PASS"] = str(force_pass)
        else: env.pop("FORCE_PASS", None)
        if explicit_libraries:
            env["RUN_LIBRARIES"] = json.dumps(explicit_libraries)
        else:
            env.pop("RUN_LIBRARIES", None)
        proc = subprocess.Popen([sys.executable, SCRIPT_PATH],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
        _run_proc = proc
        for line in proc.stdout:
            line = line.rstrip(); run_log.append(line); _broadcast(line)
            _run_last_line = line
            if "Pass 1" in line: run_pass = max(run_pass, 1)
            if "Pass 2" in line: run_pass = max(run_pass, 2)
            if "Pass 3" in line: run_pass = max(run_pass, 3)
            _run_pass = run_pass
        return_code = proc.wait()
    except Exception as e:
        msg = f"[ERROR] {e}"; run_log.append(msg); _broadcast(msg)
    finally:
        stop_requested = _run_stop_requested
        if return_code == 0 and not stop_requested:
            outcome = "success"
        elif stop_requested:
            outcome = "stopped"
        elif return_code is not None:
            outcome = "error"
        elif any("[ERROR]" in line for line in run_log):
            outcome = "error"
        _run_proc = None; _run_stop_requested = False; _broadcast("__DONE__"); _run_active = False
        duration = _time.time() - _run_started_at if _run_started_at else None
        summary = next((l for l in reversed(run_log) if any(k in l for k in
            ["complete","caught up","nothing to do","processed","STOP","ERROR"])),"No output")
        stats = _parse_run_stats(run_log)
        fname = timestamp.replace(":","-").replace(" ","_")+".json"
        with open(RUNS_DIR/fname,"w") as f:
            json.dump({
                "time": timestamp,
                "pass": run_pass,
                "scope": resolved_scope,
                "libraries": explicit_libraries,
                "summary": summary,
                "status": outcome,
                "outcome": outcome,
                "log": "\n".join(run_log),
                "duration_seconds": duration,
                "stats": stats,
                "return_code": return_code,
                "stop_requested": stop_requested,
            }, f)
        _record_task(
            {1:'Run Scan Now',2:'Run Find Sources Now',3:'Run Download Themes Now'}.get(run_pass, 'Run Pipeline'),
            outcome,
            resolved_scope,
            summary,
            {'pass': run_pass, 'stats': stats, 'return_code': return_code, 'stop_requested': stop_requested, 'libraries': explicit_libraries},
            duration,
        )
        _run_started_at = None
        _run_scope_label = ""
        _run_libraries = []

@app.route("/api/run/stream")
def run_stream():
    q = queue.Queue(maxsize=8000); _run_clients.append(q)
    def gen():
        try:
            while True:
                msg = q.get(timeout=30); yield f"data: {msg}\n\n"
                if msg == "__DONE__": break
        except: yield "data: __DONE__\n\n"
        finally:
            try: _run_clients.remove(q)
            except: pass
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.route("/api/history")
def get_history():
    runs = []
    for f in sorted(RUNS_DIR.glob("*.json")):
        try:
            with open(f) as fh: runs.append(json.load(fh))
        except: pass
    return jsonify(runs)

@app.route('/api/tasks/history')
def tasks_history():
    limit = int(request.args.get('limit', 250) or 250)
    return jsonify(_load_task_entries(limit=limit))

@app.route('/api/tasks/export-golden-source', methods=['POST'])
def export_golden_source_csv():
    t0 = _time.perf_counter()
    data = request.json or {}
    lib = (data.get('library', '') or '').strip()
    cfg = load_config()
    libs = [l.get('name','').strip() for l in cfg.get('libraries',[]) if l.get('name')]
    target_libs = [lib] if lib else libs
    if not target_libs:
        return jsonify({'ok': False, 'error': 'No libraries configured'}), 400
    out_rows = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for target_lib in target_libs:
        rows = load_ledger(ledger_path_for(target_lib))
        for r in rows:
            url = str(r.get('url', '') or '').strip()
            if not url:
                continue
            updated = str(r.get('last_updated', '') or '').strip() or now
            out_rows.append({
                'tmdb_id': str(r.get('tmdb_id', '') or '').strip(),
                'title': str(r.get('title', '') or r.get('plex_title', '') or '').strip(),
                'year': str(r.get('year', '') or '').strip(),
                'source_url': url,
                'start_offset': str(r.get('start_offset', '0') or '0').strip() or '0',
                'updated_at': updated,
                'notes': str(r.get('notes', '') or '').strip(),
            })
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    scope_name = lib or 'all_libraries'
    fname = f'golden_source_export_{re.sub(r"[^a-z0-9]+", "_", scope_name.lower()).strip("_") or "library"}_{stamp}.csv'
    fpath = EXPORTS_DIR / fname
    with open(fpath, 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=['tmdb_id','title','year','source_url','start_offset','updated_at','notes'])
        w.writeheader()
        w.writerows(out_rows)
    _record_task('Export Golden Source CSV', 'success', lib or 'all libraries', f'Exported {len(out_rows)} rows',
                 {'library': lib or '', 'libraries_exported': len(target_libs), 'rows_exported': len(out_rows), 'file': fname}, _time.perf_counter()-t0)
    return jsonify({'ok': True, 'rows_exported': len(out_rows), 'file': fname, 'download_url': f'/api/tasks/download/{fname}'})

@app.route('/api/tasks/download/<path:filename>')
def download_task_file(filename):
    safe = Path(filename).name
    fpath = EXPORTS_DIR / safe
    if not fpath.exists():
        abort(404)
    return send_file(str(fpath), as_attachment=True)

@app.route('/api/tasks/cleanup-logs', methods=['POST'])
def cleanup_logs():
    data = request.json or {}
    keep_days = int(data.get('keep_days', 14) or 14)
    cutoff = _time.time() - (keep_days * 86400)
    deleted = 0
    for f in LOGS_DIR.glob('*.log'):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
                deleted += 1
        except Exception:
            pass
    _record_task('Clean Up Logs', 'success', '', f'Removed {deleted} log files', {'keep_days': keep_days, 'deleted': deleted})
    return jsonify({'ok': True, 'deleted': deleted, 'keep_days': keep_days})

@app.route('/api/tasks/prune-history', methods=['POST'])
def prune_task_history():
    data = request.json or {}
    keep_runs = int(data.get('keep_runs', 100) or 100)
    run_files = sorted(RUNS_DIR.glob('*.json'))
    removed_runs = 0
    for f in run_files[:-max(1, keep_runs)]:
        try:
            f.unlink(missing_ok=True)
            removed_runs += 1
        except Exception:
            pass
    task_entries = []
    if TASKS_FILE.exists():
        for line in TASKS_FILE.read_text(encoding='utf-8', errors='ignore').splitlines():
            try:
                task_entries.append(json.loads(line))
            except Exception:
                pass
    kept = task_entries[-max(1, keep_runs):]
    with open(TASKS_FILE, 'w', encoding='utf-8') as fh:
        for entry in kept:
            fh.write(json.dumps(entry, ensure_ascii=False) + '\n')
    _record_task('Prune Task History', 'success', '', f'Removed {removed_runs} run entries', {'removed_runs': removed_runs, 'kept_entries': len(kept)})
    return jsonify({'ok': True, 'removed_runs': removed_runs, 'kept_task_entries': len(kept)})

@app.route('/api/tasks/refresh-themes', methods=['POST'])
def tasks_refresh_themes():
    data = request.json or {}
    lib = (data.get('library', '') or '').strip()
    if not lib:
        return jsonify({'ok': False, 'error': 'Missing library'}), 400
    result = sync_library_themes()
    payload = result.get_json() if hasattr(result, 'get_json') else {'ok': False}
    _record_task('Refresh Local Theme Detection', 'success' if payload.get('ok') else 'error', lib,
                 f"Updated {payload.get('updated', 0)} rows",
                 {'library': lib, **payload})
    return result

@app.route('/api/tasks/sqlite-maintenance', methods=['POST'])
def sqlite_maintenance():
    data = request.json or {}
    do_backup = bool(data.get('backup', True))
    do_vacuum = bool(data.get('vacuum', True))
    db_path = Path(get_db_path())
    if not db_path.exists():
        return jsonify({'ok': False, 'error': f'Database not found: {db_path}'}), 404
    backup_file = ''
    if do_backup:
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f'media_tracks_backup_{stamp}.db'
        (EXPORTS_DIR / backup_file).write_bytes(db_path.read_bytes())
    if do_vacuum:
        import sqlite3
        conn = sqlite3.connect(str(db_path), timeout=60)
        try:
            conn.execute('VACUUM')
            conn.commit()
        finally:
            conn.close()
    _record_task('SQLite Maintenance', 'success', '', 'Backup/Vacuum completed', {'backup_file': backup_file, 'vacuum': do_vacuum})
    return jsonify({'ok': True, 'backup_file': backup_file, 'download_url': f'/api/tasks/download/{backup_file}' if backup_file else ''})

@app.route('/api/tasks/clear-source-urls', methods=['POST'])
def clear_all_source_urls():
    data = request.json or {}
    lib = (data.get('library', '') or '').strip()
    cfg = load_config()
    libs = [l.get('name','').strip() for l in cfg.get('libraries',[]) if l.get('name')]
    target_libs = [lib] if lib else libs
    if not target_libs:
        return jsonify({'ok': False, 'error': 'No libraries configured'}), 400
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    total_summary = {
        'requested': 0,
        'matched': 0,
        'cleared': 0,
        'updated': 0,
        'preserved_available': 0,
        'reset_missing': 0,
        'preserved_failed': 0,
        'preserved_unmonitored': 0,
        'skipped_without_url': 0,
    }
    changed_libs = 0
    for target_lib in target_libs:
        rows = load_ledger(ledger_path_for(target_lib))
        summary = _clear_source_urls_for_rows(
            rows,
            note='Source URL cleared via Tasks maintenance',
            now=now,
        )
        for key, value in summary.items():
            total_summary[key] += value
        if summary['cleared']:
            save_ledger(ledger_path_for(target_lib), rows)
            changed_libs += 1
    _record_task(
        'Clear All Source URLs',
        'success',
        lib or 'all libraries',
        f"Cleared {total_summary['cleared']} URLs",
        {
            'library': lib or '',
            'libraries_cleared': changed_libs,
            **total_summary,
        },
    )
    return jsonify({'ok': True, 'library': lib, 'libraries_cleared': changed_libs, **total_summary})

@app.route("/api/run/status")
def run_status():
    return jsonify({"active":_run_active,"started_at":_run_started_at,
                    "pass":_run_pass,"last_line":_run_last_line,
                    "scope":_run_scope_label,"libraries":_run_libraries})

def _cleanup():
    global _run_proc
    if _run_proc and _run_proc.poll() is None:
        _run_proc.terminate()
        try: _run_proc.wait(timeout=5)
        except: _run_proc.kill()

atexit.register(_cleanup)
def _sig_handler(sig, frame):
    _cleanup(); sys.exit(0)
signal.signal(signal.SIGTERM, _sig_handler)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
