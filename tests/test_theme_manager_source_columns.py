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

    def test_table_headers_use_golden_and_selected_columns(self):
        self.assertIn("sortTable('golden_state')", self.template_source)
        self.assertIn("sortTable('custom_state')", self.template_source)
        self.assertNotIn("sortTable('local_state')", self.template_source)
        self.assertIn("Selected Source ↕", self.template_source)
        self.assertNotIn("Golden Source URL ↕", self.template_source)
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
const playlistRow = {{
  url:'https://example.test/playlist',
  golden_source_url:'https://example.test/golden',
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
process.stdout.write(JSON.stringify({{
  playlistLabel:_selectedSourceLabel(playlistRow),
  playlistFilterKey:_selectedSourceFilterKey(playlistRow),
  playlistState:_customSourceState(playlistRow),
  manualFallbackLabel:_selectedSourceLabel(manualFallbackRow),
  manualFallbackFilterKey:_selectedSourceFilterKey(manualFallbackRow),
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
        self.assertEqual("Playlist · Staged", payload["playlistState"]["pillLabel"])
        self.assertEqual("Manual", payload["manualFallbackLabel"])
        self.assertEqual("manual", payload["manualFallbackFilterKey"])

    def test_library_js_renders_new_source_state_cells(self):
        for snippet in (
            "function _goldenSourceState(row={})",
            "function _customSourceState(row={})",
            "function _localSourceState(row={})",
            "function _renderSourceStateStack(targetId,row={},opts={})",
            "String(row?.golden_source_imported_at||'').trim() || String(row?.last_updated||'').trim()",
            "String(row?.selected_source_recorded_at||'').trim() || String(row?.last_updated||'').trim()",
            "_renderSourceStateCell('Golden Source'",
            "_renderSourceStateCell('Selected Source'",
            "_sourceStatePillLabel(_selectedSourceLabel(previewRow), _selectedSourceStateText(previewRow))",
            "_themeModalSourceOriginMarkup(row)",
        ):
            self.assertIn(snippet, self.library_source)

    def test_library_js_uses_distinct_offsets_for_golden_selected_and_local_layers(self):
        for snippet in (
            "offset:_normalizedOffsetValue(sourceRow?.golden_source_offset||'0')",
            "const goldenOffsetValue=_normalizedOffsetValue(existingRow?.golden_source_offset||'0');",
            "if(layer==='golden_source') return row?.golden_source_offset||0;",
            "if(layer==='local_theme') return row?.local_source_offset ?? row?.start_offset ?? 0;",
            "return row?.start_offset||0;",
            "_themeModalOffsetLabel(row, true, 'local_theme')",
            "_setMethodQuickPick('golden_source', golden.url ? {title:'Golden Source URL', url:golden.url, start_offset:golden.offset} : false);",
        ):
            self.assertIn(snippet, self.library_source)

    def test_library_js_does_not_reuse_selected_source_offset_for_golden_source_ui(self):
        self.assertNotIn("offset:_normalizedOffsetValue(sourceRow?.start_offset||'0')", self.library_source)
        self.assertNotIn("const goldenOffsetValue=_normalizedOffsetValue(existingRow?.start_offset||'0');", self.library_source)

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


if __name__ == "__main__":
    unittest.main()
