from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import web.logic as logic


class LibraryRequirementLogicTests(unittest.TestCase):
    def test_require_library_name_rejects_blank_values(self):
        with self.assertRaisesRegex(ValueError, "Missing library"):
            logic.require_library_name("  ")

    def test_trim_theme_payload_requires_library(self):
        payload, status = logic.trim_theme_payload({"rating_key": "1", "start_offset": 0, "end_offset": 0})

        self.assertEqual(400, status)
        self.assertEqual("Missing library", payload["error"])

    def test_delete_theme_payload_requires_library(self):
        payload, status = logic.delete_theme_payload({"rating_key": "1", "folder": "/tmp/example"})

        self.assertEqual(400, status)
        self.assertEqual("Missing library", payload["error"])

    def test_download_now_payload_requires_library(self):
        payload, status = logic.download_now_payload({"rating_key": "1"})

        self.assertEqual(400, status)
        self.assertEqual("Missing library", payload["error"])


class LibraryRequirementAppSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "web" / "app.py").read_text(encoding="utf-8")
        cls.services_source = (ROOT / "web" / "services.py").read_text(encoding="utf-8")

    def test_ledger_routes_use_required_library_helper(self):
        for snippet in (
            'def get_ledger():\n    library, error_response = _required_library_arg(',
            'def patch_ledger(key):\n    library, error_response = _required_library_arg(',
            'def bulk_ledger():\n    library, error_response = _required_library_arg(',
            'def clear_selected_sources():\n    data = request.get_json(silent=True) or {}\n    library, error_response = _required_library_arg(',
        ):
            self.assertIn(snippet, self.app_source)

    def test_web_app_no_longer_falls_back_to_theme_log_for_ledger_routes(self):
        self.assertNotIn('path = ledger_path_for(library) if library else str(logic.LOGS_DIR / "theme_log.csv")', self.app_source)

    def test_scan_route_delegates_to_canonical_pass_handler(self):
        self.assertIn('@app.route("/api/run/scan", methods=["POST"])\ndef trigger_scan():\n    return trigger_pass(1)', self.app_source)
        self.assertNotIn("def trigger_scan_payload(", self.services_source)

    def test_legacy_theme_routes_removed_from_app(self):
        self.assertNotIn('@app.route("/api/theme/info")', self.app_source)
        self.assertNotIn('@app.route("/api/media")', self.app_source)


if __name__ == "__main__":
    unittest.main()
