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
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")

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
        self.assertIn('@app.route("/api/run/scan", methods=["POST"])', self.app_source)
        self.assertIn('def trigger_scan():\n    # Legacy compatibility alias for older clients that still invoke scan directly.\n    return trigger_pass(1)', self.app_source)
        self.assertNotIn("def trigger_scan_payload(", self.services_source)

    def test_manual_source_service_persists_end_offset(self):
        self.assertIn('"end_offset": data.get("end_offset", "0"),', self.services_source)

    def test_legacy_theme_routes_removed_from_app(self):
        self.assertNotIn('@app.route("/api/theme/info")', self.app_source)
        self.assertNotIn('@app.route("/api/media")', self.app_source)

    def test_removed_legacy_theme_routes_have_no_repo_consumers(self):
        for legacy_path in ("/api/theme/info", "/api/media"):
            self.assertNotIn(legacy_path, self.app_source)
            self.assertNotIn(legacy_path, self.services_source)

    def test_search_modal_step_rail_uses_semantic_buttons(self):
        self.assertIn('id="search-step-rail" role="navigation" aria-label="Search modal steps"', self.template_source)
        self.assertIn('<button type="button" class="srail-step active" id="srail-1"', self.template_source)
        self.assertIn('aria-current="step"', self.template_source)
        self.assertIn('aria-controls="search-step-2"', self.template_source)
        self.assertIn('aria-controls="search-step-3"', self.template_source)

    def test_search_method_picker_uses_radiogroup_markup(self):
        self.assertIn('class="search-method-grid" role="radiogroup"', self.template_source)
        for snippet in (
            'type="radio" class="sr-only search-method-radio" id="sm-radio-curated_source"',
            'type="radio" class="sr-only search-method-radio" id="sm-radio-playlist"',
            'type="radio" class="sr-only search-method-radio" id="sm-radio-direct"',
            'type="radio" class="sr-only search-method-radio" id="sm-radio-custom"',
            'type="radio" class="sr-only search-method-radio" id="sm-radio-paste"',
        ):
            self.assertIn(snippet, self.template_source)

    def test_review_results_step_is_shown_via_flex_when_activated(self):
        library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")
        self.assertIn("el.style.display=active?'flex':'none';", library_source)


    def test_favicon_route_serves_root_favicon_path(self):
        self.assertIn('@app.route("/favicon.ico")', self.app_source)
        self.assertIn('return send_file(favicon_path, mimetype="image/png", max_age=0)', self.app_source)
        self.assertIn('<link rel="icon" type="image/x-icon" href="/favicon.ico">', self.template_source)

    def test_curated_source_card_stays_first_and_uses_minimal_unavailable_copy(self):
        library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")
        self.assertIn("wrap.prepend(curatedCard);", library_source)
        self.assertIn("'No curated source available yet.'", library_source)
        self.assertNotIn("choose another method to search alternatives", library_source)


if __name__ == "__main__":
    unittest.main()
