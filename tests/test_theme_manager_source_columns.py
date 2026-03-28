from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ThemeManagerSourceColumnsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (ROOT / "web" / "template.html").read_text(encoding="utf-8")
        cls.library_source = (ROOT / "web" / "static" / "js" / "library.js").read_text(encoding="utf-8")

    def test_table_headers_use_curated_and_selected_columns(self):
        self.assertIn("{id:'curated_state',label:'Curated Source'", self.library_source)
        self.assertIn("{id:'custom_state',label:'Selected Source'", self.library_source)
        self.assertIn("onclick=\"sortTable('${col.id}')\"", self.library_source)
        self.assertNotIn("{id:'local_state'", self.library_source)
        self.assertNotIn("Curated Source URL ↕", self.template_source)
        self.assertNotIn("Source URL ↕", self.template_source)
        self.assertNotIn("Start Offset (mm:ss) ↕", self.template_source)

    def test_source_filter_options_match_method_aware_states(self):
        for option in (
            'value="PLAYLIST">Playlist-selected',
            'value="DIRECT">Direct-selected',
            'value="PASTE">Pasted URL-selected',
            'value="MANUAL">Manual-selected',
            'value="LOCAL">Downloaded Locally',
            'value="NO_LOCAL">Not Downloaded Locally',
        ):
            self.assertIn(option, self.template_source)
        self.assertNotIn('value="CUSTOM">With Custom Source', self.template_source)
        self.assertNotIn('value="NO_CUSTOM">Without Custom Source', self.template_source)


    def test_library_js_uses_selected_source_contract_for_playlist_labels_and_filtering(self):
        start = self.library_source.index("function formatStatusLabel(value)")
        end = self.library_source.index("function _localSourceState(row={})")
        helper_block = self.library_source[start:end]
        node_script = f"""
{helper_block}
function uiTerm(_key, fallback) {{ return fallback; }}
const _defaultStatusDisplay = {{}};
function _effectiveRowStatus(row={{}}) {{
  return String(row?.status || '').toUpperCase();
}}
const playlistRow = {{
  url:'https://example.test/playlist',
  curated_source_url:'https://example.test/curated',
  status:'STAGED',
  source_origin:'manual',
  selected_source_kind:'custom',
  selected_source_method:'playlist',
}};
const manualFallbackRow = {{
  url:'https://example.test/manual',
  status:'APPROVED',
  source_origin:'manual',
}};
const curatedRow = {{
  url:'https://example.test/curated',
  curated_source_url:'https://example.test/curated',
  status:'MISSING',
  selected_source_kind:'curated',
  selected_source_method:'curated_source',
}};
process.stdout.write(JSON.stringify({{
  playlistLabel:_selectedSourceLabel(playlistRow),
  playlistFilterKey:_selectedSourceFilterKey(playlistRow),
  playlistState:_customSourceState(playlistRow),
  manualFallbackLabel:_selectedSourceLabel(manualFallbackRow),
  manualFallbackFilterKey:_selectedSourceFilterKey(manualFallbackRow),
  manualFallbackState:_customSourceState(manualFallbackRow),
  curatedLabel:_selectedSourceLabel(curatedRow),
  curatedState:_customSourceState(curatedRow),
  curatedSourceState:_curatedSourceState(curatedRow),
}}));
"""
        result = subprocess.run(
            ["node", "-e", node_script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual("Playlist", payload["playlistLabel"])
        self.assertEqual("playlist", payload["playlistFilterKey"])
        self.assertEqual("playlist", payload["playlistState"]["key"])
        self.assertEqual("Playlist - Staged", payload["playlistState"]["pillLabel"])
        self.assertEqual("Custom", payload["manualFallbackLabel"])
        self.assertEqual("manual", payload["manualFallbackFilterKey"])
        self.assertEqual("Custom - Approved", payload["manualFallbackState"]["pillLabel"])
        self.assertEqual("Curated Source", payload["curatedLabel"])
        self.assertEqual("Curated Source - Saved", payload["curatedState"]["pillLabel"])
        self.assertEqual("Identified", payload["curatedSourceState"]["label"])
        self.assertEqual("Curated source identified", payload["curatedSourceState"]["detail"])

    def test_library_js_renders_new_source_state_cells(self):
        for snippet in (
            "function _curatedSourceState(row={})",
            "function _customSourceState(row={})",
            "function _localSourceState(row={})",
            "function _selectedSourceStateSummary(row={}, opts={})",
            "function _renderConfirmSelectedSourceState(targetId,row={},opts={})",
            "function _renderSourceStateStack(targetId,row={},opts={})",
            "String(row?.curated_source_imported_at||'').trim() || String(row?.last_updated||'').trim()",
            "String(row?.selected_source_recorded_at||'').trim() || String(row?.last_updated||'').trim()",
            "const curatedState=_curatedSourceState(row);",
            "stateLabel:_sourceStatePillLabel('Curated', curatedState.label)",
            "note:curatedState.detail",
            "_renderSourceStateCell('', _renderSourceStatePill(curatedState.label, curatedState.className, curatedState.detail), '', curatedState.chips)",
            "_renderSourceStateCell('', _renderSourceStatePill(customState.pillLabel, customState.className, customState.detail || customState.pillLabel), '', customState.chips)",
            "pillLabel:_sourceStatePillLabel(typeLabel, statusLabel)",
            "_sourceStatePillLabel(_selectedSourceLabel(previewRow), _selectedSourceStateText(previewRow))",
            "_themeModalSourceOriginMarkup(row)",
        ):
            self.assertIn(snippet, self.library_source)

    def test_library_js_uses_distinct_offsets_for_curated_selected_and_local_layers(self):
        for snippet in (
            "offset:_normalizedOffsetValue(sourceRow?.curated_source_offset||'0')",
            "const curatedOffsetValue=_normalizedOffsetValue(existingRow?.curated_source_offset||'0');",
            "if(layer==='curated_source') return row?.curated_source_offset||0;",
            "if(layer==='local_theme') return row?.local_source_offset ?? row?.start_offset ?? 0;",
            "return row?.start_offset||0;",
            "_themeModalOffsetLabel(row, true, 'local_theme')",
            "_setMethodQuickPick('curated_source', curated.url ? {title:'Curated Source URL', url:curated.url, start_offset:curated.offset} : false);",
        ):
            self.assertIn(snippet, self.library_source)

    def test_library_js_does_not_reuse_selected_source_offset_for_curated_source_ui(self):
        self.assertNotIn("offset:_normalizedOffsetValue(sourceRow?.start_offset||'0')", self.library_source)
        self.assertNotIn("const curatedOffsetValue=_normalizedOffsetValue(existingRow?.start_offset||'0');", self.library_source)

    def test_library_js_no_longer_sorts_or_renders_local_table_column(self):
        self.assertNotIn("if(col==='local_state')", self.library_source)
        self.assertNotIn("_sortCol==='local_state'", self.library_source)
        self.assertNotIn("_renderSourceStateCell('Local'", self.library_source)
        self.assertNotIn("label:'Curated Source'", self.library_source)
        self.assertNotIn("label:'Saved Source'", self.library_source)

    def test_library_js_status_cell_no_longer_renders_inline_status_editor(self):
        self.assertIn("function renderStatusCell(row)", self.library_source)
        self.assertIn('<span class="badge s-${status}" title="${statusDesc(status)}">', self.library_source)
        self.assertNotIn('select class="st-sel"', self.library_source)
        self.assertNotIn("onchange=\"updateRow('${rk}','status',this.value)\"", self.library_source)

    def test_library_js_reserves_ready_for_download_readiness_not_curated_source_presence(self):
        self.assertIn("label:'Identified'", self.library_source)
        self.assertIn("detail:'Curated source identified'", self.library_source)
        self.assertIn("stateLabel:_sourceStatePillLabel('Curated', curatedState.label)", self.library_source)
        self.assertNotIn("_sourceStatePillLabel('Curated', _rowHasCuratedSource(row) ? 'Ready' : 'Not Available')", self.library_source)
        self.assertNotIn("label:'Ready', className:'is-curated'", self.library_source)

    def test_confirm_step_selected_source_summary_uses_single_source_renderer(self):
        start = self.library_source.index("function _renderSelectedSourceSummary(url, title)")
        end = self.library_source.index("function _manualSaveTargetStatus()")
        selected_summary_block = self.library_source[start:end]
        self.assertIn("_renderConfirmSelectedSourceState('se-source-state-stack', row, {draft:{selectedUrl:cleanUrl}});", selected_summary_block)
        self.assertNotIn("_renderSourceStateStack('se-source-state-stack'", selected_summary_block)
        self.assertIn("Review the currently selected source before saving.", self.template_source)
        self.assertIn("Current Selection", self.template_source)


if __name__ == "__main__":
    unittest.main()
