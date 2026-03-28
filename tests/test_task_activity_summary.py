from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import web.services as services
import web.tasks as tasks


class TaskActivitySummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.logs_dir = Path(self.temp_dir.name)
        self.runs_dir = self.logs_dir / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.tasks_file = self.logs_dir / "task_history.jsonl"
        self.summary_file = self.logs_dir / "task_activity_summary.jsonl"

        self.original_services_paths = (
            services.LOGS_DIR,
            services.RUNS_DIR,
            services.TASKS_FILE,
            services.TASK_ACTIVITY_SUMMARY_FILE,
        )
        self.original_tasks_paths = (
            tasks.RUNS_DIR,
            tasks.TASKS_FILE,
            tasks.TASK_ACTIVITY_SUMMARY_FILE,
        )

        services.LOGS_DIR = self.logs_dir
        services.RUNS_DIR = self.runs_dir
        services.TASKS_FILE = self.tasks_file
        services.TASK_ACTIVITY_SUMMARY_FILE = self.summary_file

        tasks.RUNS_DIR = self.runs_dir
        tasks.TASKS_FILE = self.tasks_file
        tasks.TASK_ACTIVITY_SUMMARY_FILE = self.summary_file

    def tearDown(self):
        services.LOGS_DIR, services.RUNS_DIR, services.TASKS_FILE, services.TASK_ACTIVITY_SUMMARY_FILE = self.original_services_paths
        tasks.RUNS_DIR, tasks.TASKS_FILE, tasks.TASK_ACTIVITY_SUMMARY_FILE = self.original_tasks_paths
        self.temp_dir.cleanup()

    def _write_jsonl(self, path: Path, entries: list[dict]):
        with open(path, "w", encoding="utf-8") as fh:
            for entry in entries:
                fh.write(json.dumps(entry) + "\n")

    def test_load_task_entries_reads_summary_file_instead_of_detailed_sources(self):
        self._write_jsonl(
            self.summary_file,
            [
                {
                    "time": "2026-03-20 10:00:00",
                    "task": "Summary Entry",
                    "status": "success",
                    "outcome": "success",
                    "summary": "Served from summary",
                    "details": {},
                }
            ],
        )
        self._write_jsonl(
            self.tasks_file,
            [
                {
                    "time": "2026-03-20 11:00:00",
                    "task": "Detailed Task Entry",
                    "status": "success",
                    "outcome": "success",
                    "summary": "Should not be re-read",
                    "details": {},
                }
            ],
        )
        (self.runs_dir / "run.json").write_text(
            json.dumps(
                {
                    "time": "2026-03-20 12:00:00",
                    "pass": 1,
                    "status": "success",
                    "summary": "Detailed run entry",
                    "scope": "library",
                    "libraries": ["Example"],
                }
            ),
            encoding="utf-8",
        )

        entries = services.load_task_entries(limit=10)

        self.assertEqual(1, len(entries))
        self.assertEqual("Summary Entry", entries[0]["task"])
        self.assertEqual("Served from summary", entries[0]["summary"])


    def test_api_health_payload_uses_cache_within_ttl(self):
        config = {
            "plex_url": "",
            "plex_token": "",
            "tmdb_api_key": "",
            "curated_source_url": "",
            "media_roots": [str(self.logs_dir)],
            "libraries": [],
            "schedule_enabled": False,
            "schedule_libraries": [],
            "cron_schedule": "0 3 * * *",
        }
        tasks._health_cache = {"lite": dict(tasks._HEALTH_CACHE_EMPTY), "full": dict(tasks._HEALTH_CACHE_EMPTY)}

        with (
            mock.patch("web.tasks.load_config", return_value=config),
            mock.patch("web.tasks.active_scheduler_source", return_value={"authority": "cron", "detail": "ok"}) as scheduler_source,
            mock.patch("web.tasks.get_media_roots", return_value=[str(self.logs_dir)]),
            mock.patch("web.tasks.get_db_path", return_value=str(self.logs_dir / "missing.sqlite")),
            mock.patch("web.tasks.os.access", return_value=True) as access_mock,
            mock.patch("web.tasks.time.time", side_effect=[1000.0] * 20 + [1005.0] * 20),
        ):
            first = tasks.api_health_payload("lite")
            second = tasks.api_health_payload("lite")

        self.assertEqual(first, second)
        self.assertEqual(2, scheduler_source.call_count)
        self.assertEqual(1, access_mock.call_count)

    def test_record_task_updates_summary_file(self):
        services.record_task("SQLite Maintenance", "success", "", "Backup complete", {"backup": True}, 3)

        summary_entries = [json.loads(line) for line in self.summary_file.read_text(encoding="utf-8").splitlines() if line.strip()]

        self.assertEqual(1, len(summary_entries))
        self.assertEqual("SQLite Maintenance", summary_entries[0]["task"])
        self.assertEqual("Backup complete", summary_entries[0]["summary"])
        self.assertEqual(3.0, summary_entries[0]["duration_seconds"])

    def test_prune_task_history_trims_summary_file(self):
        summary_entries = [
            {
                "time": f"2026-03-20 10:0{i}:00",
                "task": f"Task {i}",
                "status": "success",
                "outcome": "success",
                "summary": f"Summary {i}",
                "details": {},
            }
            for i in range(3)
        ]
        self._write_jsonl(self.summary_file, summary_entries)
        self._write_jsonl(self.tasks_file, summary_entries)

        payload, status = tasks.prune_task_history_payload({"keep_runs": 2})

        self.assertEqual(200, status)
        self.assertEqual(2, payload["kept_summary_entries"])
        remaining = [json.loads(line) for line in self.summary_file.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(["Task 1", "Task 2"], [entry["task"] for entry in remaining[:2]])
        self.assertEqual("Prune Task History", remaining[-1]["task"])


if __name__ == "__main__":
    unittest.main()
