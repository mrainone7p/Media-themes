from __future__ import annotations

import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))
sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, dump=lambda *args, **kwargs: ""))

import script.media_tracks as media_tracks
import shared.storage as storage
import web.ledger as ledger
import web.services as services
import web.themes as themes


class StorageLocalProvenanceTests(unittest.TestCase):
    def test_sqlite_migration_and_round_trip_include_local_provenance_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "media_tracks.db"
            ledger_path = Path(tmpdir) / "tracks_movies.csv"

            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE libraries (
                    slug TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_path TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE items (
                    library_slug TEXT NOT NULL,
                    rating_key TEXT NOT NULL,
                    title TEXT,
                    year TEXT,
                    status TEXT,
                    url TEXT,
                    start_offset TEXT,
                    golden_source_url TEXT,
                    golden_source_offset TEXT DEFAULT '0',
                    end_offset TEXT,
                    plex_title TEXT,
                    folder TEXT,
                    tmdb_id TEXT,
                    last_updated TEXT,
                    notes TEXT,
                    source_origin TEXT DEFAULT 'unknown',
                    theme_exists INTEGER DEFAULT 0,
                    theme_duration REAL DEFAULT 0,
                    theme_size INTEGER DEFAULT 0,
                    theme_mtime REAL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (library_slug, rating_key)
                )
                """
            )
            conn.commit()
            conn.close()

            row = {
                "rating_key": "1",
                "title": "Example",
                "year": "1999",
                "status": "AVAILABLE",
                "url": "https://example.test/selected",
                "start_offset": "12",
                "selected_source_kind": "custom",
                "selected_source_method": "playlist",
                "golden_source_url": "https://example.test/golden",
                "golden_source_offset": "8",
                "local_source_url": "https://example.test/local",
                "local_source_offset": "12",
                "local_source_origin": "manual",
                "local_source_kind": "custom",
                "local_source_method": "playlist",
                "local_source_recorded_at": "2026-03-23 10:00:00",
                "end_offset": "0",
                "plex_title": "Example",
                "folder": "/tmp/example",
                "tmdb_id": "123",
                "last_updated": "2026-03-23 10:00:00",
                "notes": "Saved",
                "source_origin": "manual",
                "theme_exists": 1,
                "theme_duration": 12.5,
                "theme_size": 2048,
                "theme_mtime": 123.4,
            }

            with patch("shared.storage.get_db_path", return_value=str(db_path)):
                storage.save_ledger_rows(str(ledger_path), [row])
                loaded = storage.load_ledger_rows(str(ledger_path))

            self.assertEqual(1, len(loaded))
            self.assertEqual("https://example.test/local", loaded[0]["local_source_url"])
            self.assertEqual("12", loaded[0]["local_source_offset"])
            self.assertEqual("manual", loaded[0]["local_source_origin"])
            self.assertEqual("custom", loaded[0]["selected_source_kind"])
            self.assertEqual("playlist", loaded[0]["selected_source_method"])
            self.assertEqual("custom", loaded[0]["local_source_kind"])
            self.assertEqual("playlist", loaded[0]["local_source_method"])
            self.assertEqual("2026-03-23 10:00:00", loaded[0]["local_source_recorded_at"])


class SourceFlowLayeringTests(unittest.TestCase):
    def test_golden_import_updates_curated_fields_without_overwriting_selected_source(self):
        row = {
            "rating_key": "1",
            "title": "Example",
            "year": "1999",
            "status": "STAGED",
            "url": "https://example.test/manual-selected",
            "start_offset": "15",
            "selected_source_kind": "custom",
            "selected_source_method": "manual",
            "golden_source_url": "",
            "golden_source_offset": "0",
            "local_source_url": "https://example.test/local-theme",
            "local_source_offset": "15",
            "local_source_origin": "manual",
            "local_source_kind": "custom",
            "local_source_method": "manual",
            "local_source_recorded_at": "2026-03-22 10:00:00",
            "end_offset": "0",
            "plex_title": "Example",
            "folder": "/media/Example",
            "tmdb_id": "123",
            "last_updated": "",
            "notes": "",
            "source_origin": "manual",
            "theme_exists": "1",
        }
        rows = [row]

        with (
            patch("web.ledger.load_config", return_value={}),
            patch(
                "web.ledger.fetch_golden_source_catalog",
                return_value=(
                    "https://example.test/golden.csv",
                    [{
                        "tmdb_id": "123",
                        "title": "Example",
                        "year": "1999",
                        "source_url": "https://example.test/golden-selected",
                        "start_offset": "4",
                        "end_offset": "9",
                    }],
                    0.5,
                    "remote-fetch",
                ),
            ),
            patch("web.ledger.load_ledger", return_value=rows),
            patch("web.ledger.save_ledger"),
        ):
            payload, status = ledger.golden_source_import_summary({
                "library": "Movies",
                "overwrite_existing": False,
            })

        self.assertEqual(200, status)
        self.assertTrue(payload["ok"])
        self.assertEqual("https://example.test/manual-selected", row["url"])
        self.assertEqual("15", row["start_offset"])
        self.assertEqual("manual", row["source_origin"])
        self.assertEqual("custom", row["selected_source_kind"])
        self.assertEqual("manual", row["selected_source_method"])
        self.assertEqual("https://example.test/golden-selected", row["golden_source_url"])
        self.assertEqual("4", row["golden_source_offset"])
        self.assertEqual("https://example.test/local-theme", row["local_source_url"])
        self.assertEqual("manual", row["local_source_method"])

    def test_manual_save_updates_selected_source_only(self):
        row = {
            "rating_key": "1",
            "title": "Example",
            "status": "STAGED",
            "url": "https://example.test/old-selected",
            "start_offset": "3",
            "selected_source_kind": "custom",
            "selected_source_method": "direct",
            "golden_source_url": "https://example.test/golden",
            "golden_source_offset": "1",
            "local_source_url": "https://example.test/local-theme",
            "local_source_offset": "3",
            "local_source_origin": "golden_source",
            "local_source_kind": "golden",
            "local_source_method": "golden_source",
            "local_source_recorded_at": "2026-03-22 09:00:00",
            "end_offset": "0",
            "notes": "",
            "source_origin": "golden_source",
            "theme_exists": "0",
        }

        with (
            patch("web.services.load_ledger", return_value=[row]),
            patch("web.services.save_ledger"),
        ):
            payload, status = services.save_manual_source_payload({
                "library": "Movies",
                "rating_key": "1",
                "url": "https://example.test/new-selected",
                "start_offset": "11",
                "selected_source_kind": "custom",
                "selected_source_method": "playlist",
                "target_status": "APPROVED",
                "notes": "Manual override",
            })

        self.assertEqual(200, status)
        self.assertTrue(payload["ok"])
        self.assertEqual("https://example.test/new-selected", row["url"])
        self.assertEqual("11", row["start_offset"])
        self.assertEqual("youtube_playlist", row["source_origin"])
        self.assertEqual("custom", row["selected_source_kind"])
        self.assertEqual("playlist", row["selected_source_method"])
        self.assertEqual("https://example.test/golden", row["golden_source_url"])
        self.assertEqual("https://example.test/local-theme", row["local_source_url"])
        self.assertEqual("golden_source", row["local_source_method"])

    def test_manual_download_stamps_local_provenance_from_selected_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            downloaded = folder / "downloaded.mp3"
            downloaded.write_bytes(b"audio")
            row = {
                "rating_key": "1",
                "title": "Example",
                "year": "1999",
                "status": "APPROVED",
                "url": "https://example.test/selected",
                "start_offset": "7",
                "selected_source_kind": "custom",
                "selected_source_method": "playlist",
                "source_origin": "manual",
                "folder": str(folder),
                "theme_exists": "0",
            }

            with (
                patch("web.themes.load_config", return_value={"theme_filename": "theme.mp3", "audio_format": "mp3", "quality_profile": "high", "max_theme_duration": 0}),
                patch("web.themes.get_media_roots", return_value=[str(folder.parent)]),
                patch("web.themes.load_ledger", return_value=[row]),
                patch("web.themes.integrations.download_audio", return_value=downloaded),
                patch("web.themes._validate_audio_ready", return_value=10.0),
                patch("web.themes.sibling_temp_path", return_value=folder / "trimmed.mp3"),
                patch("web.themes.integrations.trim_audio_copy", side_effect=lambda _src, dst, **_kwargs: Path(dst).write_bytes(b"trimmed")),
                patch("web.themes.atomic_replace_file", return_value=False),
                patch("web.themes.sync_theme_cache", side_effect=lambda current_row, *_args, **_kwargs: (current_row, True)),
                patch("web.themes.save_ledger"),
                patch("web.themes.now_str", return_value="2026-03-23 11:00:00"),
            ):
                payload, status = themes.download_now_payload({"library": "Movies", "rating_key": "1"})

        self.assertEqual(200, status)
        self.assertTrue(payload["ok"])
        self.assertEqual("https://example.test/selected", row["local_source_url"])
        self.assertEqual("7", row["local_source_offset"])
        self.assertEqual("manual", row["local_source_origin"])
        self.assertEqual("custom", row["local_source_kind"])
        self.assertEqual("playlist", row["local_source_method"])
        self.assertEqual("2026-03-23 11:00:00", row["local_source_recorded_at"])

    def test_worker_download_stamps_local_provenance_from_selected_source(self):
        row = {
            "title": "Example",
            "year": "1999",
            "status": media_tracks.ST_APPROVED,
            "url": "https://example.test/selected",
            "start_offset": "5",
            "selected_source_kind": "golden",
            "selected_source_method": "golden_source",
            "source_origin": "golden_source",
            "folder": "/tmp/example",
            "theme_exists": 0,
        }
        ledger_rows = {"1": row}

        with (
            patch("script.media_tracks.download_track", return_value=(True, "Downloaded")),
            patch("script.media_tracks.sync_theme_cache", side_effect=lambda current_row, *_args, **_kwargs: (current_row, True)),
            patch("script.media_tracks.now_str", return_value="2026-03-23 12:00:00"),
        ):
            stats = media_tracks.pass3_download(ledger_rows, {
                "theme_filename": "theme.mp3",
                "audio_format": "mp3",
                "max_retries": 1,
                "download_delay_seconds": 0,
                "max_theme_duration": 0,
                "quality_profile": "high",
                "dry_run": False,
            })

        self.assertEqual(1, stats["downloaded"])
        self.assertEqual("https://example.test/selected", row["local_source_url"])
        self.assertEqual("5", row["local_source_offset"])
        self.assertEqual("golden_source", row["local_source_origin"])
        self.assertEqual("golden", row["local_source_kind"])
        self.assertEqual("golden_source", row["local_source_method"])
        self.assertEqual("2026-03-23 12:00:00", row["local_source_recorded_at"])


if __name__ == "__main__":
    unittest.main()
