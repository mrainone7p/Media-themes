from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ThemeModalSourceManagementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")
        cls.library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")

    def test_source_details_card_owns_preview_and_clip_summary(self):
        for snippet in (
            'id="theme-modal-source-preview"',
            'onclick="themeModalPreviewSource()"',
            'id="theme-modal-source-clip"',
            'id="theme-modal-source-clip-main"',
            'Open Preview to inspect the kept portion before downloading.',
        ):
            self.assertIn(snippet, self.template_source)

    def test_local_theme_actions_keep_player_and_trim_only(self):
        local_card_split = self.template_source.split('id="theme-local-card"', 1)[1].split('id="theme-source-card"', 1)[0]
        self.assertIn('id="theme-local-player"', local_card_split)
        self.assertNotIn('id="theme-modal-inline-play"', local_card_split)
        local_actions_split = self.template_source.split('id="theme-local-actions"', 1)[1].split('</div>', 1)[0]
        self.assertIn('id="theme-modal-trim-btn"', local_actions_split)
        self.assertNotIn('id="theme-modal-delete-btn"', local_actions_split)
        self.assertNotIn('id="theme-replace-btn"', local_actions_split)

    def test_footer_owns_replace_and_delete_actions(self):
        theme_modal_split = self.template_source.split('<div class="modal-overlay" id="theme-modal">', 1)[1]
        footer_split = theme_modal_split.split('<div class="modal-footer">', 1)[1].split('</div>', 2)[0]
        self.assertIn('id="theme-replace-btn"', footer_split)
        self.assertIn('id="theme-modal-delete-btn"', footer_split)

    def test_theme_modal_supports_saved_source_preview_and_replace_without_local_theme(self):
        for snippet in (
            "function _themeModalUpdateSourceClipSummary(row={})",
            "sourcePreviewBtn.disabled=!sourceUrl;",
            "sourcePreviewBtn.style.display=hasStoredSource?'':'none';",
            "if(replaceBtn) replaceBtn.style.display=hasStoredSource?'':'none';",
            "if(localDeleteBtn) localDeleteBtn.style.display=hasLocalTheme?'':'none';",
            "async function themeModalPreviewSource(){",
            "await openSearchModal(c.rk||'', c.title||'', c.year||'', encodeURIComponent(c.library||_activeLib||''));",
            "goToStep3(url,{",
            "entryMode:'existing'",
        ):
            self.assertIn(snippet, self.library_source)


if __name__ == "__main__":
    unittest.main()
