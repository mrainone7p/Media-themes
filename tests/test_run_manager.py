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

import web.logic as logic
import web.services as services


class _FakeQueue:
    def __init__(self, events):
        self._events = list(events)

    def get(self, timeout=None):
        if not self._events:
            raise AssertionError("No queued event available")
        event = self._events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return event

    def put_nowait(self, message):
        self._events.append(message)

    def get_nowait(self):
        return self.get(timeout=0)


class RunManagerTests(unittest.TestCase):
    def test_event_stream_emits_heartbeat_while_run_is_active(self):
        manager = logic.RunManager(active=True)
        fake_queue = _FakeQueue([services.queue.Empty(), "__DONE__"])

        with mock.patch.object(services.queue, "Queue", return_value=fake_queue):
            stream = manager.event_stream()
            self.assertEqual(": heartbeat\n\n", next(stream))
            self.assertEqual("data: __DONE__\n\n", next(stream))
            with self.assertRaises(StopIteration):
                next(stream)

    def test_event_stream_stops_after_timeout_once_run_is_inactive(self):
        manager = logic.RunManager(active=False)
        fake_queue = _FakeQueue([services.queue.Empty()])

        with mock.patch.object(services.queue, "Queue", return_value=fake_queue):
            stream = manager.event_stream()
            with self.assertRaises(StopIteration):
                next(stream)

    def test_history_returns_full_run_payloads_from_run_files(self):
        manager = logic.RunManager()
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir)
            first_run = {
                "time": "2026-03-20 10:00:00",
                "summary": "First run",
                "log": ["line 1"],
                "stats": {"pass1": {"total": 1}},
            }
            second_run = {
                "time": "2026-03-20 11:00:00",
                "summary": "Second run",
                "log": ["line 2"],
                "stats": {"pass1": {"total": 2}},
            }
            (runs_dir / "20260320-100000.json").write_text(json.dumps(first_run), encoding="utf-8")
            (runs_dir / "20260320-110000.json").write_text(json.dumps(second_run), encoding="utf-8")

            with mock.patch.object(services, "RUNS_DIR", runs_dir):
                payload = manager.history(include_log=True, limit=10, offset=0)

        self.assertEqual(2, payload["total"])
        self.assertEqual(2, len(payload["runs"]))
        self.assertEqual("20260320-110000.json", payload["runs"][0]["id"])
        self.assertEqual(["line 2"], payload["runs"][0]["log"])
        self.assertEqual({"pass1": {"total": 2}}, payload["runs"][0]["stats"])
        self.assertEqual(["line 1"], payload["runs"][1]["log"])

    def test_status_reports_last_outcome_details(self):
        manager = logic.RunManager(
            active=False,
            started_at=12.5,
            current_pass=3,
            last_line="downloaded",
            scope_label='library "Sci-Fi"',
            libraries=["Sci-Fi"],
            last_outcome="error",
            last_return_code=1,
            last_summary="[ERROR] boom",
            completed_at=33.0,
        )

        self.assertEqual(
            {
                "active": False,
                "started_at": 12.5,
                "pass": 3,
                "last_line": "downloaded",
                "scope": 'library "Sci-Fi"',
                "libraries": ["Sci-Fi"],
                "outcome": "error",
                "return_code": 1,
                "summary": "[ERROR] boom",
                "completed_at": 33.0,
                "client_count": 0,
            },
            manager.status(),
        )


if __name__ == "__main__":
    unittest.main()
