from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ThemeManagerSourceColumnsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")
        cls.library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")

    def test_table_headers_use_golden_custom_local_columns(self):
        self.assertIn("sortTable('golden_state')", self.template_source)
        self.assertIn("sortTable('custom_state')", self.template_source)
        self.assertIn("sortTable('local_state')", self.template_source)
        self.assertNotIn("Golden Source URL ↕", self.template_source)
        self.assertNotIn("Source URL ↕", self.template_source)
        self.assertNotIn("Start Offset (mm:ss) ↕", self.template_source)

    def test_source_filter_options_match_new_states(self):
        for option in (
            'value="CUSTOM">With Custom Source',
            'value="NO_CUSTOM">Without Custom Source',
            'value="LOCAL">Downloaded Locally',
            'value="NO_LOCAL">Not Downloaded Locally',
        ):
            self.assertIn(option, self.template_source)

    def test_library_js_renders_new_source_state_cells(self):
        for snippet in (
            "function _goldenSourceState(row={})",
            "function _customSourceState(row={})",
            "function _localSourceState(row={})",
            "_renderSourceStateCell('Golden'",
            "_renderSourceStateCell('Custom'",
            "_renderSourceStateCell('Local'",
        ):
            self.assertIn(snippet, self.library_source)


if __name__ == "__main__":
    unittest.main()
