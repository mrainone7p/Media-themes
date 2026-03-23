from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ThemeModalSourceManagementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")
        cls.library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")

    def test_source_details_put_copy_and_open_inline_with_source_url(self):
        source_card_split = self.template_source.split('id="theme-source-card"', 1)[1].split('id="theme-modal-source-clip"', 1)[0]
        self.assertIn('id="theme-modal-source-url"', source_card_split)
        self.assertIn('id="theme-modal-source-controls"', source_card_split)
        self.assertIn('id="theme-modal-source-copy"', source_card_split)
        self.assertIn('id="theme-modal-source-open"', source_card_split)
        self.assertNotIn('id="theme-modal-source-preview"', source_card_split)
        self.assertNotIn('>Controls<', source_card_split)

    def test_local_theme_section_hosts_saved_source_preview_ui(self):
        local_card_split = self.template_source.split('id="theme-local-card"', 1)[1].split('id="theme-source-card"', 1)[0]
        for snippet in (
            'id="theme-local-player"',
            'id="theme-local-preview-empty"',
            'id="theme-modal-inline-play"',
            'id="theme-modal-inline-play-main"',
            'Load a preview to confirm the kept portion.',
        ):
            self.assertIn(snippet, local_card_split)
        local_actions_split = self.template_source.split('id="theme-local-actions"', 1)[1].split('</div>', 1)[0]
        self.assertIn('id="theme-modal-trim-btn"', local_actions_split)
        self.assertNotIn('theme-modal-delete-btn', local_actions_split)
        self.assertNotIn('theme-modal-delete-local-btn', local_actions_split)
        self.assertNotIn('theme-replace-btn', local_actions_split)

    def test_footer_splits_saved_source_and_local_theme_delete_actions(self):
        theme_modal_split = self.template_source.split('<div class="modal-overlay" id="theme-modal">', 1)[1]
        footer_split = theme_modal_split.split('<div class="modal-footer">', 1)[1].split('</div>', 2)[0]
        self.assertIn('id="theme-replace-btn"', footer_split)
        self.assertIn('Replace Source', footer_split)
        self.assertIn('id="theme-modal-delete-btn"', footer_split)
        self.assertIn('Delete Source', footer_split)
        self.assertIn('id="theme-modal-delete-local-btn"', footer_split)
        self.assertIn('Delete Local Theme', footer_split)

    def test_theme_modal_supports_inline_saved_source_preview_and_split_actions(self):
        for snippet in (
            'function _themeModalUpdateSourceClipSummary(row={})',
            'function _themeModalUpdateInlinePreviewSummary(row={}, duration=0){',
            'async function _themeModalLoadSavedSourcePreview(row={}){',
            'const showStoredSourcePreviewInLocalTheme=isSourceOnly;',
            "document.getElementById('theme-modal-local-status').textContent=hasLocalTheme",
            "sourceControls.style.display=hasStoredSource?'':'none';",
            "_setHidden(localPlayer, !(hasLocalTheme || showStoredSourcePreviewInLocalTheme), (hasLocalTheme || showStoredSourcePreviewInLocalTheme)?'block':'');",
            "if(replaceBtn) replaceBtn.style.display=canReplaceStoredSource?'':'none';",
            "if(deleteSourceBtn) deleteSourceBtn.style.display=canDeleteStoredSource?'':'none';",
            "if(deleteLocalBtn) deleteLocalBtn.style.display=canDeleteLocalTheme?'':'none';",
            "if(showStoredSourcePreviewInLocalTheme) await _themeModalLoadSavedSourcePreview(row);",
            'async function themeModalDeleteSource(){',
            "Delete Saved Source",
            "async function openThemeModal(rk,title,year,folder,row={},library=''){",
            'function themeModalDeleteLocalTheme(){',
            "openDeleteModal(c.rk,c.title||'',c.library||_activeLib||'',c.folder||'');",
        ):
            self.assertIn(snippet, self.library_source)


if __name__ == "__main__":
    unittest.main()
