from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "instance" / "theme_library.db"

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


def get_db_path() -> Path:
    configured_path = os.environ.get("THEME_DB_PATH", "").strip()
    return Path(configured_path) if configured_path else DEFAULT_DB_PATH


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["DATABASE"] = str(get_db_path())
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

    @app.route("/configuration")
    def configuration() -> str:
        return render_template("configuration.html")

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
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
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
    port = int(os.environ.get("WEB_PORT", "8182"))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug, host="0.0.0.0", port=port)
