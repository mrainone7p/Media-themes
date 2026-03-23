from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ThemeModalSourceManagementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")
        cls.library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")

    def test_local_theme_section_contains_only_local_playback_metadata_and_actions(self):
        local_card_split = self.template_source.split('id="theme-local-card"', 1)[1].split('id="theme-workflow-card"', 1)[0]
        for snippet in (
            'id="theme-local-toggle"',
            'aria-controls="theme-local-body"',
            'id="theme-local-player"',
            'id="theme-modal-audio"',
            'id="theme-modal-local-state"',
            'id="theme-modal-file"',
            'id="theme-local-clip"',
            'class="ui-meta-row review-meta-item clip-summary-row hidden"',
            'id="theme-local-actions"',
            'id="theme-modal-trim-btn"',
            'id="theme-modal-local-rematch-btn"',
            'id="theme-modal-local-delete-btn"',
            '>Rematch<',
            '>Delete<',
        ):
            self.assertIn(snippet, local_card_split)
        for snippet in (
            'theme-workflow-copy',
            'theme-workflow-open',
            'themeModalDeleteSource',
            '>Clear Source<',
            '>Approve<',
            '>Download<',
        ):
            self.assertNotIn(snippet, local_card_split)

    def test_selected_source_section_contains_its_own_player_metadata_and_actions(self):
        source_card_split = self.template_source.split('id="theme-workflow-card"', 1)[1].split('<div class="modal-footer">', 1)[0]
        for snippet in (
            'id="theme-workflow-toggle"',
            'aria-controls="theme-workflow-body"',
            'id="theme-workflow-player"',
            'id="theme-workflow-audio"',
            'Workflow State',
            'id="theme-workflow-state"',
            'Source Type',
            'id="theme-workflow-origin"',
            'id="theme-workflow-url"',
            'id="theme-workflow-added"',
            'id="theme-workflow-clip"',
            'class="ui-meta-row review-meta-item clip-summary-row hidden"',
            'id="theme-workflow-actions"',
            'id="theme-workflow-copy"',
            'id="theme-workflow-open"',
        ):
            self.assertIn(snippet, source_card_split)
        for snippet in (
            'theme-modal-local-rematch-btn',
            'theme-modal-local-delete-btn',
            'theme-modal-file',
            'theme-modal-local-state',
        ):
            self.assertNotIn(snippet, source_card_split)



    def test_theme_modal_cards_use_collapsible_details_markup(self):
        for snippet in (
            '<details class="review-card theme-section-card theme-source-details theme-modal-card-details" id="theme-local-card">',
            '<details class="review-card theme-section-card theme-source-details theme-modal-card-details" id="theme-workflow-card">',
            'id="theme-local-toggle"',
            'id="theme-workflow-toggle"',
        ):
            self.assertIn(snippet, self.template_source)

    def test_template_contains_updated_trim_source_label(self):
        self.assertIn('>Trim Source<', self.template_source)

    def test_footer_only_contains_close_action(self):
        footer_split = self.template_source.split('<div class="modal-footer">', 1)[1].split('</div>', 2)[0]
        self.assertIn('>Close<', footer_split)
        self.assertNotIn('Delete Source', footer_split)
        self.assertNotIn('Delete Local Theme', footer_split)
        self.assertNotIn('Replace Source', footer_split)

    def test_library_js_defines_separate_local_and_selected_source_update_paths(self):
        for snippet in (
            'function _themeModalUpdateLocalClipSummary(row={}, duration=0){',
            'function _themeModalUpdateSelectedSourceClipSummary(row={}, duration=0){',
            'async function _themeModalLoadSelectedSourcePreview(row={}){',
            'function _themeModalUpdateLocalCard(row={}){',
            'function _themeModalUpdateWorkflowCard(row={}){',
            "_themeModalAudio.setHandlers({",
            "onloadedmetadata:(audio)=>_themeModalUpdateLocalClipSummary(_themeModalContext?.row||row, audio.duration||0)",
            "if(hasStoredSource) await _themeModalLoadSelectedSourcePreview(row);",
            "const _themeModalSourceAudio=bindModalAudio({audioId:'theme-workflow-audio'",
            'function themeModalSourceToggle(){ _themeModalSourceAudio.toggle(); }',
        ):
            self.assertIn(snippet, self.library_source)


    def test_theme_modal_workflow_metadata_separates_state_from_type_without_duplicate_detail(self):
        for snippet in (
            "return _selectedSourceLabel(row);",
            "return _selectedSourceStateText(row);",
            "? _renderSourceStatePill(_themeModalSourceState(row), _themeModalSourceOriginClass(row), _themeModalSourceState(row))",
            "if(originEl) originEl.innerHTML=hasSelected ? _themeModalSourceOriginMarkup(row) : '—';",
        ):
            self.assertIn(snippet, self.library_source)
        self.assertNotIn('theme-workflow-detail', self.template_source)

    def test_theme_modal_cards_persist_collapsed_state_with_sensible_defaults(self):
        for snippet in (
            "const _THEME_MODAL_CARD_STORAGE_KEY='mt-theme-modal-card-state';",
            "function _themeModalCardDefaultOpen(cardId, row={}){",
            "if(cardId==='theme-local-card') return hasLocal;",
            "if(cardId==='theme-workflow-card') return hasSelected || !hasLocal;",
            "function _themeModalPersistCardState(cardId, isOpen){",
            "card.addEventListener('toggle', ()=>_themeModalPersistCardState(cardId, card.open));",
            "_themeModalBindCardToggles();",
            "_themeModalApplyCardState(row);",
        ):
            self.assertIn(snippet, self.library_source)

    def test_source_actions_follow_state_driven_trim_clear_then_stage_approve_download(self):
        for snippet in (
            'function _themeModalWorkflowActions(row={}){',
            "push('find-source','Find Source','btn btn-amber is-primary','themeModalOpenManualSearch');",
            "push('trim-source','Trim Source','btn btn-ghost','themeModalPreviewSourceTrim');",
            "push('clear-source','Clear Source','btn btn-ghost','themeModalDeleteSource');",
            "if(status==='APPROVED')",
            "push('download-now','Download','btn btn-green is-primary','themeModalDownloadApproved');",
            "if(status==='STAGED')",
            "push('approve','Approve','btn btn-amber is-primary','themeModalApproveSource');",
            "push('stage','Stage','btn btn-amber is-primary','themeModalStageSource');",
            'function themeModalStageSource(){',
            "themeModalSetStatus('STAGED');",
        ):
            self.assertIn(snippet, self.library_source)


    def test_theme_modal_secondary_modals_track_return_context_for_cancel_reopen(self):
        for snippet in (
            'const _themeModalReturnContext={deleteModal:null,trimModal:null,ytModal:null};',
            'function _themeModalSnapshotContext(){',
            "function _themeModalSetReturnContext(modalKey, shouldReturn=false, context={}){",
            "function _themeModalClearReturnContext(modalKey){",
            "function _themeModalReopenFromReturnContext(modalKey){",
            "_themeModalSetReturnContext('deleteModal', true, _themeModalSnapshotContext());",
            "_themeModalSetReturnContext('trimModal', true, _themeModalSnapshotContext());",
            "_themeModalSetReturnContext('ytModal', true, _themeModalSnapshotContext());",
            "_themeModalReopenFromReturnContext('deleteModal');",
            "_themeModalReopenFromReturnContext('trimModal');",
            "_themeModalReopenFromReturnContext('ytModal');",
            "_themeModalClearReturnContext('deleteModal');",
            "_themeModalClearReturnContext('trimModal');",
            "_themeModalClearReturnContext('ytModal');",
        ):
            self.assertIn(snippet, self.library_source)

    def test_local_empty_state_remains_minimal_while_source_empty_state_guides_search(self):
        for snippet in (
            "if(statusEl) statusEl.textContent=hasLocal ? 'On disk and ready to play or trim.' : 'No local theme file on disk yet.';",
            "if(empty) empty.textContent=hasLocal ? '' : 'No local theme file is on disk yet. Review the selected source below or find one to continue.';",
            "if(subtitle) subtitle.textContent=hasSelected",
            "'No selected source yet. Find a source to continue.'",
        ):
            self.assertIn(snippet, self.library_source)

    def test_theme_modal_next_action_normalizes_stale_available_before_exposing_actions(self):
        for snippet in (
            'function _effectiveRowStatus(row={}){',
            "if(status==='AVAILABLE' && !hasTheme) return hasStoredSource ? 'STAGED' : 'MISSING';",
            "const status=_effectiveRowStatus(row);",
            "if(status==='APPROVED'){",
            "if(status==='STAGED'){",
            "push('stage','Stage','btn btn-amber is-primary','themeModalStageSource');",
        ):
            self.assertIn(snippet, self.library_source)

    def test_search_modal_approve_then_download_path_still_saves_staged_first(self):
        for snippet in (
            "const status='STAGED';",
            'await saveSourceEditor(true);',
            "await updateRow(key,'status','APPROVED');",
            'await approveSourceEditor(true);',
            "toast('Approved source — downloading now…','info');",
        ):
            self.assertIn(snippet, self.library_source)


if __name__ == "__main__":
    unittest.main()
