from __future__ import annotations

import sys
import types
import unittest
from unittest import mock

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import web.services as services


class DashboardSummaryPayloadTests(unittest.TestCase):
    def setUp(self):
        services._DASHBOARD_SUMMARY_CACHE["key"] = None
        services._DASHBOARD_SUMMARY_CACHE["payload"] = None

    def test_dashboard_summary_aggregates_enabled_libraries_and_recent_activity(self):
        config = {
            "libraries": [
                {"name": "Movies", "type": "movie", "enabled": True},
                {"name": "Shows", "type": "show", "enabled": True},
                {"name": "Disabled", "type": "movie", "enabled": False},
                {"name": "Music", "type": "artist", "enabled": True},
            ],
            "schedule_libraries": ["Shows"],
        }
        ledger_rows = {
            "Movies": [
                {"status": "MISSING"},
                {"status": "APPROVED"},
                {"status": "AVAILABLE"},
            ],
            "Shows": [
                {"status": "FAILED"},
                {"status": "AVAILABLE"},
                {"status": "UNMONITORED"},
            ],
        }
        entries = [
            {"task": "Download Themes", "status": "success", "time": "2026-03-20 12:00:00", "details": {"pass": 3}, "summary": "Downloaded themes"},
            {"task": "Find Sources", "status": "success", "time": "2026-03-20 11:00:00", "details": {"pass": 2}, "summary": "Found sources"},
            {"task": "Scan Libraries", "status": "success", "time": "2026-03-20 10:00:00", "details": {"pass": 1}, "summary": "Scanned libraries"},
            {"task": "SQLite Maintenance", "status": "success", "time": "2026-03-20 09:00:00", "details": {}, "summary": "Vacuum complete"},
        ]

        with (
            mock.patch("web.services.load_config", return_value=config),
            mock.patch("web.services.ledger_path_for", side_effect=lambda library: library),
            mock.patch("web.services.load_ledger", side_effect=lambda path: ledger_rows[path]),
            mock.patch("web.services.load_task_entries", return_value=entries),
        ):
            payload = services.dashboard_summary_payload()

        self.assertEqual(2, payload["libraries"]["enabled_count"])
        self.assertEqual(1, payload["libraries"]["scheduled_count"])
        self.assertEqual(["Movies", "Shows"], payload["libraries"]["enabled"])
        self.assertEqual(["Shows"], payload["libraries"]["scheduled"])
        self.assertEqual(
            {
                "MISSING": 1,
                "STAGED": 0,
                "APPROVED": 1,
                "AVAILABLE": 2,
                "FAILED": 1,
                "UNMONITORED": 1,
            },
            payload["counts_by_status"],
        )
        self.assertEqual(1, payload["counts_by_library"]["Movies"]["APPROVED"])
        self.assertEqual(1, payload["counts_by_library"]["Shows"]["FAILED"])
        self.assertEqual("Scan Libraries", payload["recent_activity"]["scan"]["task"])
        self.assertEqual("Find Sources", payload["recent_activity"]["discover"]["task"])
        self.assertEqual("Download Themes", payload["recent_activity"]["download"]["task"])
        self.assertEqual("Download Themes", payload["recent_activity"]["task"]["task"])

    def test_dashboard_summary_reuses_cached_payload_until_inputs_change(self):
        config = {
            "libraries": [
                {"name": "Movies", "type": "movie", "enabled": True},
            ],
            "schedule_libraries": ["Movies"],
        }
        entries = [{"task": "Scan Libraries", "status": "success", "time": "2026-03-20 10:00:00", "details": {"pass": 1}, "summary": "Scanned libraries"}]

        stat_sequence = iter([
            mock.Mock(st_mtime_ns=100),
            mock.Mock(st_mtime_ns=200),
            mock.Mock(st_mtime_ns=100),
            mock.Mock(st_mtime_ns=200),
            mock.Mock(st_mtime_ns=100),
            mock.Mock(st_mtime_ns=201),
        ])

        with (
            mock.patch("web.services.load_config", return_value=config),
            mock.patch("web.services.ledger_path_for", return_value="/tmp/movies.sqlite"),
            mock.patch("web.services.load_ledger", side_effect=[[{"status": "AVAILABLE"}], [{"status": "AVAILABLE"}], [{"status": "FAILED"}], [{"status": "FAILED"}]]) as load_ledger,
            mock.patch("web.services.load_task_entries", return_value=entries) as load_task_entries,
            mock.patch("pathlib.Path.stat", side_effect=lambda: next(stat_sequence)),
        ):
            first = services.dashboard_summary_payload()
            second = services.dashboard_summary_payload()
            third = services.dashboard_summary_payload()

        self.assertEqual(4, load_ledger.call_count)
        self.assertEqual(2, load_task_entries.call_count)
        self.assertEqual(1, first["counts_by_status"]["AVAILABLE"])
        self.assertEqual(1, second["counts_by_status"]["AVAILABLE"])
        self.assertEqual(1, third["counts_by_status"]["FAILED"])


if __name__ == "__main__":
    unittest.main()
