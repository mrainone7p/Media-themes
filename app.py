from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "instance" / "theme_library.db"
DB_PATH = Path(os.environ.get("THEME_LIBRARY_DB_PATH", DEFAULT_DB_PATH))
WEB_HOST = os.environ.get("FLASK_RUN_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("PORT", "8182"))
DEBUG = os.environ.get("FLASK_DEBUG", "0") == "1"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS theme_library (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    year INTEGER,
    status TEXT NOT NULL,
    action TEXT NOT NULL,
    golden_source_url TEXT,
    golden_source_offset INTEGER DEFAULT 0,
    source_url TEXT,
    source_offset INTEGER DEFAULT 0,
    updated_at TEXT,
    notes TEXT,
    folder_path TEXT NOT NULL,
    tmdb_id INTEGER
);
"""

SEED_ROWS: list[tuple[Any, ...]] = [
    (
        "10 Cloverfield Lane",
        2016,
        "Missing",
        "Manager",
        "",
        0,
        "",
        0,
        "2026-03-20 20:14",
        "No approved source attached yet.",
        "/media/Movies/10 Cloverfield Lane (2016)",
        333371,
    ),
    (
        "Interstellar",
        2014,
        "Available",
        "Manager",
        "https://www.youtube.com/watch?v=UDVtMYqUAyw",
        94,
        "https://www.youtube.com/watch?v=4y33h81phKU",
        58,
        "2026-03-19 09:02",
        "Theme already downloaded as sidecar.",
        "/media/Movies/Interstellar (2014)",
        157336,
    ),
    (
        "Blade Runner 2049",
        2017,
        "Staged",
        "Manager",
        "https://www.youtube.com/watch?v=5m4ZkEqQrn0",
        102,
        "https://www.youtube.com/watch?v=2RcpadSZkvY",
        77,
        "2026-03-18 16:47",
        "Candidate source saved for approval review.",
        "/media/Movies/Blade Runner 2049 (2017)",
        335984,
    ),
    (
        "The Last of Us",
        2023,
        "Failed",
        "Manager",
        "",
        0,
        "https://www.youtube.com/watch?v=pfA5UqEU_80",
        31,
        "2026-03-17 13:11",
        "Source download failed: HTTP 403 from upstream.",
        "/media/Shows/The Last of Us (2023)",
        100088,
    ),
    (
        "Dune: Part Two",
        2024,
        "Approved",
        "Manager",
        "https://www.youtube.com/watch?v=F5cZ4g0HvHo",
        88,
        "https://www.youtube.com/watch?v=fW4HzAqPyxE",
        46,
        "2026-03-21 07:30",
        "Ready for next download pipeline run.",
        "/media/Movies/Dune Part Two (2024)",
        693134,
    ),
    (
        "The Batman",
        2022,
        "Missing",
        "Manager",
        "https://www.youtube.com/watch?v=dwHbq6YC7hY",
        75,
        "",
        0,
        "2026-03-21 08:42",
        "Golden source exists but local source still needs review.",
        "/media/Movies/The Batman (2022)",
        414906,
    ),
]


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["DATABASE"] = str(DB_PATH)
    init_db()

    @app.template_filter("mmss")
    def mmss_filter(value: int | None) -> str:
        total_seconds = int(value or 0)
        minutes, seconds = divmod(total_seconds, 60)
        return f"{minutes:02d}:{seconds:02d}"

    @app.route("/")
    def index() -> str:
        return render_template("theme_library.html", themes=get_themes())

    @app.route("/theme-library")
    def theme_library() -> str:
        return render_template("theme_library.html", themes=get_themes())

    @app.route("/dashboard")
    def dashboard() -> str:
        return render_template("dashboard.html")

    @app.route("/configuration")
    def configuration() -> str:
        return render_template("configuration.html")

    @app.get("/health")
    def health() -> tuple[dict[str, str], int]:
        return {"status": "ok"}, 200

    @app.get("/api/dashboard/summary")
    def api_dashboard_summary():
        library = request.args.get("library", "All")
        range_qty = int(request.args.get("range_qty", "30"))
        range_freq = request.args.get("range_freq", "Days")

        lib_filter = ""
        lib_params: list[Any] = []
        if library and library != "All":
            lib_filter = " AND folder_path LIKE ?"
            lib_params = [f"/media/{library}/%"]

        with get_connection() as conn:
            # KPIs
            kpi_rows = conn.execute(
                f"SELECT status, COUNT(*) as cnt FROM theme_library WHERE 1=1{lib_filter} GROUP BY status",
                lib_params,
            ).fetchall()
            kpis: dict[str, int] = {"total": 0}
            for row in kpi_rows:
                kpis[row["status"].lower()] = row["cnt"]
                kpis["total"] += row["cnt"]

            # Libraries
            lib_rows = conn.execute(
                "SELECT DISTINCT folder_path FROM theme_library"
            ).fetchall()
            libs = sorted(
                {p["folder_path"].split("/")[2] for p in lib_rows if len(p["folder_path"].split("/")) > 2}
            )

            # Activity timeline
            freq_map = {
                "Days": ("DATE(updated_at)", "day"),
                "Weeks": ("strftime('%Y-W%W', updated_at)", "day"),
                "Months": ("strftime('%Y-%m', updated_at)", "day"),
                "Years": ("strftime('%Y', updated_at)", "day"),
            }
            bucket_expr = freq_map.get(range_freq, freq_map["Days"])[0]

            cutoff_days = {
                "Days": range_qty,
                "Weeks": range_qty * 7,
                "Months": range_qty * 30,
                "Years": range_qty * 365,
            }.get(range_freq, range_qty)
            cutoff_date = (datetime.now() - timedelta(days=cutoff_days)).strftime("%Y-%m-%d")

            activity_rows = conn.execute(
                f"SELECT {bucket_expr} as period, status, COUNT(*) as cnt "
                f"FROM theme_library WHERE updated_at >= ?{lib_filter} "
                f"GROUP BY period, status ORDER BY period ASC",
                [cutoff_date] + lib_params,
            ).fetchall()

        activity: dict[str, dict[str, int]] = {}
        for row in activity_rows:
            period = row["period"] or "Unknown"
            if period not in activity:
                activity[period] = {}
            activity[period][row["status"].lower()] = row["cnt"]

        activity_list = [{"period": k, **v} for k, v in activity.items()]

        return jsonify({
            "kpis": kpis,
            "activity": activity_list,
            "libraries": libs,
            "last_refreshed": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    @app.get("/api/themes")
    def api_themes():
        status = request.args.get("status")
        search = request.args.get("search", "").strip().lower()
        sort_by = request.args.get("sort", "updated_at")
        direction = request.args.get("direction", "desc")

        valid_sorts = {
            "title": "title",
            "year": "year",
            "status": "status",
            "updated_at": "updated_at",
            "tmdb_id": "tmdb_id",
        }
        order_column = valid_sorts.get(sort_by, "updated_at")
        order_direction = "ASC" if direction == "asc" else "DESC"

        query = "SELECT * FROM theme_library WHERE 1=1"
        params: list[Any] = []
        if status and status != "All":
            query += " AND status = ?"
            params.append(status)
        if search:
            query += " AND (LOWER(title) LIKE ? OR LOWER(folder_path) LIKE ? OR CAST(tmdb_id AS TEXT) LIKE ?)"
            wildcard = f"%{search}%"
            params.extend([wildcard, wildcard, wildcard])
        query += f" ORDER BY {order_column} {order_direction}, title ASC"

        with get_connection() as connection:
            rows = connection.execute(query, params).fetchall()

        return jsonify([row_to_dict(row) for row in rows])

    return app



def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection



def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        connection.executescript(SCHEMA_SQL)
        count = connection.execute("SELECT COUNT(*) FROM theme_library").fetchone()[0]
        if count == 0:
            connection.executemany(
                """
                INSERT INTO theme_library (
                    title, year, status, action, golden_source_url, golden_source_offset,
                    source_url, source_offset, updated_at, notes, folder_path, tmdb_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                SEED_ROWS,
            )
            connection.commit()



def get_themes() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT * FROM theme_library ORDER BY updated_at DESC, title ASC"
        ).fetchall()
    return [row_to_dict(row) for row in rows]



def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    for key in ("golden_source_offset", "source_offset"):
        value = int(data.get(key) or 0)
        minutes, seconds = divmod(value, 60)
        data[f"{key}_display"] = f"{minutes:02d}:{seconds:02d}"
    return data


app = create_app()


if __name__ == "__main__":
    app.run(debug=DEBUG, host=WEB_HOST, port=WEB_PORT)
