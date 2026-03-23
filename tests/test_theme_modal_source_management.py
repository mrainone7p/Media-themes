from __future__ import annotations

import json
import subprocess
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
            "const localProbe=await _themeModalVerifyLocalPlayback(row, resolvedFolder);",
            "function _themeModalProbeLocalAudio(folder=''){",
            "function _themeModalApplyVerifiedLocalAvailability(row={}, exists=false, metadata={}){",
            "const nextRow=_themeModalContext?.row||row;",
            "_themeModalApplyVerifiedLocalAvailability(nextRow, true, {duration:audio.duration||0});",
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

    def test_stale_local_theme_probe_promotes_local_playback_before_rendering(self):
        start = self.library_source.index("function _themeModalHasVerifiedLocal(row={}){")
        end = self.library_source.index("async function themeModalDownloadApproved(){")
        modal_block = self.library_source[start:end]
        node_script = f"""
const clipSummaryDurations=[];
const loadedAudioSrcs=[];
const elements=new Map();
function makeElement(id) {{
  return {{
    id,
    textContent:'',
    innerHTML:'',
    value:id==='cfg-theme_filename' ? 'theme.mp3' : '',
    src:'',
    href:'#',
    disabled:false,
    hidden:false,
    open:false,
    dataset:{{}},
    style:{{display:''}},
    className:'',
    onclick:null,
    classList:{{toggle(){{}}, add(){{}}, remove(){{}}}},
    addEventListener(){{}},
    removeAttribute(name){{ if(name==='src') this.src=''; }},
    setAttribute(){{}},
  }};
}}
const document={{
  getElementById(id){{
    if(!elements.has(id)) elements.set(id, makeElement(id));
    return elements.get(id);
  }}
}};
let _themeModalContext=null;
let _rows=[];
let _filtered=[];
let _rowMap={{}};
let _activeLib='Movies';
let _curKey='';
let _maxDur=45;
function _currentLib(){{ return 'Movies'; }}
function stopAllAudio(){{}}
function stopCurrentAudio(){{}}
function setBio(){{}}
function openModal(){{}}
function displayStatus(status){{ return status; }}
function apiUrl(url){{ return url; }}
function fmt(value){{ return String(Number(value||0)); }}
function parseTrim(value){{ return Number(value||0); }}
function _normalizedOffsetValue(value){{ return String(value ?? '0'); }}
function _setHidden(el, hidden, display=''){{ if(!el) return; el.hidden=!!hidden; el.style.display=display; }}
function _renderSourceStatePill(label){{ return label; }}
function _themeModalSourceOriginClass(){{ return 'is-custom'; }}
function _themeModalSourceOriginMarkup(){{ return 'origin'; }}
function themeModalCopyWorkflowSource(){{}}
function themeModalOpenWorkflowSource(){{}}
function _themeModalRenderWorkflowActions(){{ return ''; }}
function _themeModalWorkflowActions(){{ return []; }}
async function _themeModalLoadSelectedSourcePreview(){{ return true; }}
function _themeModalSetLinkRow(){{}}
function _setClipSummary(_summaryId, _mainId, _subId, _warningId, duration){{ clipSummaryDurations.push(Number(duration||0)); }}
function _themeModalUpdateLocalClipSummary(_row={{}}, duration=0){{ clipSummaryDurations.push(Number(duration||0)); }}
function _themeModalUpdateLocalCard(row={{}}){{ document.getElementById('theme-modal-local-status').textContent=_themeModalHasVerifiedLocal(row) ? 'On disk and ready to play or trim.' : 'No local theme file on disk yet.'; }}
function _themeModalUpdateWorkflowCard(){{}}
function _themeModalBindCardToggles(){{}}
function _themeModalApplyCardState(){{}}
function _themeModalUpdateStatusFlow(){{}}
const localStorage={{ getItem(){{ return null; }}, setItem(){{}} }};
function _normalizeSourceKind(value){{ const normalized=String(value||'').trim().toLowerCase(); return normalized==='golden' || normalized==='custom' ? normalized : ''; }}
function _normalizeSourceMethod(value){{ return String(value||'').trim().toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,''); }}
function _legacySourceMethodFromOrigin(origin=''){{ const normalized=String(origin||'').trim().toLowerCase(); if(normalized.startsWith('golden_source')) return 'golden_source'; if(normalized.includes('playlist')) return 'playlist'; if(normalized.includes('direct')) return 'direct'; if(normalized==='manual' || normalized==='custom') return 'manual'; return ''; }}
function _rowUsesGoldenSource(row){{ const sourceUrl=String(row?.url||'').trim(); const goldenUrl=String(row?.golden_source_url||'').trim(); return !!sourceUrl && !!goldenUrl && sourceUrl===goldenUrl; }}
function _selectedSourceContract(row={{}}){{ const url=String(row?.url||'').trim(); if(!url) return {{kind:'', method:'', url:''}}; let kind=_normalizeSourceKind(row?.selected_source_kind); let method=_normalizeSourceMethod(row?.selected_source_method); if(!kind) kind=method==='golden_source' || _rowUsesGoldenSource(row) ? 'golden' : 'custom'; if(!method) method=kind==='golden' ? 'golden_source' : 'manual'; return {{kind, method, url}}; }}
function _themeHasLocal(row){{ return String(row?.theme_exists||'')==='1'; }}
function _effectiveRowStatus(row={{}}){{ const status=String(row?.status||'MISSING').toUpperCase(); const hasTheme=_themeHasLocal(row); const hasStoredSource=!!String(_selectedSourceContract(row).url||'').trim(); if(status==='AVAILABLE' && !hasTheme) return hasStoredSource ? 'STAGED' : 'MISSING'; return status; }}
function _themeModalSourceState(){{ return 'Saved'; }}
function _themeModalSourceAdded(row={{}}){{ return row.selected_source_recorded_at || '—'; }}
function _themeModalSourceUrl(row={{}}){{ return String(row?.url||'').trim(); }}
function _themeModalOffsetValue(row={{}}, layer='selected_source'){{ if(layer==='local_theme') return row?.local_source_offset ?? row?.start_offset ?? 0; return row?.start_offset || 0; }}
function _clipLengthOffsetLabel(duration=0, offset=0){{ return `Length ${{duration}} · Offset ${{offset}}`; }}
const _themeModalAudio={{
  handlers:{{}},
  audio:{{
    src:'',
    duration:12.5,
    load(){{ loadedAudioSrcs.push(this.src||''); if(this.src && _themeModalAudio.handlers.onloadedmetadata) _themeModalAudio.handlers.onloadedmetadata(this); }},
    removeAttribute(name){{ if(name==='src') this.src=''; }},
  }},
  cleanup(){{}},
  setHandlers(handlers){{ this.handlers=handlers||{{}}; }}
}};
const _themeModalSourceAudio={{ audio:{{ removeAttribute(){{}}, load(){{}} }}, cleanup(){{}}, setHandlers(){{}} }};
class Audio {{
  constructor(){{ this.src=''; this.duration=12.5; this.preload=''; this.onloadedmetadata=null; this.oncanplaythrough=null; this.onerror=null; }}
  pause(){{}}
  removeAttribute(name){{ if(name==='src') this.src=''; }}
  load(){{ if(this.onloadedmetadata) this.onloadedmetadata(); }}
}}
{modal_block}
const row={{
  rating_key:'1',
  title:'Example',
  year:'1999',
  folder:'/media/example',
  status:'AVAILABLE',
  theme_exists:'0',
  theme_duration:0,
  url:'https://example.test/theme',
  start_offset:'7',
  source_origin:'manual',
  selected_source_kind:'custom',
  selected_source_method:'playlist',
  selected_source_recorded_at:'2026-03-23 10:00:00',
}};
(async()=>{{
  await openThemeModal('1', 'Example', '1999', '/media/example', row, 'Movies');
  process.stdout.write(JSON.stringify({{
    themeExists:row.theme_exists,
    themeDuration:row.theme_duration,
    localSourceUrl:row.local_source_url,
    localSourceMethod:row.local_source_method,
    loadedAudioSrcs,
    clipSummaryDurations,
    durMeta:document.getElementById('theme-modal-dur-meta').textContent,
    localStatus:document.getElementById('theme-modal-local-status').textContent,
  }}));
}})().catch((error)=>{{ console.error(error); process.exit(1); }});
"""
        result = subprocess.run(
            ["node", "-e", node_script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(1, payload["themeExists"])
        self.assertEqual(12.5, payload["themeDuration"])
        self.assertEqual("https://example.test/theme", payload["localSourceUrl"])
        self.assertEqual("playlist", payload["localSourceMethod"])
        self.assertIn("/api/theme?folder=%2Fmedia%2Fexample", payload["loadedAudioSrcs"])
        self.assertIn(12.5, payload["clipSummaryDurations"])
        self.assertEqual("Theme on disk", payload["durMeta"])
        self.assertEqual("On disk and ready to play or trim.", payload["localStatus"])



if __name__ == "__main__":
    unittest.main()
