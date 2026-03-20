from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "web") not in sys.path:
    sys.path.insert(0, str(ROOT / "web"))
if str(ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(ROOT / "shared"))

import logic
import run_logic


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
        fake_queue = _FakeQueue([run_logic.queue.Empty(), "__DONE__"])

        with mock.patch.object(run_logic.queue, "Queue", return_value=fake_queue):
            stream = manager.event_stream()
            self.assertEqual(": heartbeat\n\n", next(stream))
            self.assertEqual("data: __DONE__\n\n", next(stream))
            with self.assertRaises(StopIteration):
                next(stream)

    def test_event_stream_stops_after_timeout_once_run_is_inactive(self):
        manager = logic.RunManager(active=False)
        fake_queue = _FakeQueue([run_logic.queue.Empty()])

        with mock.patch.object(run_logic.queue, "Queue", return_value=fake_queue):
            stream = manager.event_stream()
            with self.assertRaises(StopIteration):
                next(stream)

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
            },
            manager.status(),
        )


if __name__ == "__main__":
    unittest.main()
