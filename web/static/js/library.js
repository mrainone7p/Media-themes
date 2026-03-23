// Consolidated library/theme manager + search modal behavior.

// ── SECTION: STATUS / TABLE LOGIC ──────────────────────────────────────────────
// Shared table helpers
function _tmdbLink(title,year){ return 'https://www.themoviedb.org/search?query='+encodeURIComponent(((title||'')+' '+(year||'')).trim()); }
const _tmdbLookupCache = {};
async function openTmdb(title, year, evt){
  if(evt) evt.preventDefault();
  const key=((title||'')+'|'+(year||'')).toLowerCase();
  if(_tmdbLookupCache[key]){ window.open(_tmdbLookupCache[key], '_blank'); return false; }
  try{
    const r=await fetch(apiUrl('/api/tmdb/lookup?title='+encodeURIComponent(title||'')+'&year='+encodeURIComponent(year||'')));
    const data=await r.json();
    if(data.ok && data.url){
      _tmdbLookupCache[key]=data.url;
      window.open(data.url,'_blank');
      return false;
    }
  }catch(e){}
  window.open(_tmdbLink(title,year),'_blank');
  return false;
}
function _tmdbPill(title,year){ return `<a class="modal-link-pill tmdb-pill" href="${_tmdbLink(title,year)}" target="_blank" rel="noopener">🎬 TMDB</a>`; }
function _ytLink(url){ return url?`<a class="modal-link-pill yt-pill" href="${url.replace(/"/g,'&quot;')}" target="_blank" rel="noopener">▶ YouTube ↗</a>`:''; }
function _escapeHtml(value){
  return String(value ?? '')
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;')
    .replace(/'/g,'&#39;');
}
function _escapeAttr(value){
  return _escapeHtml(value);
}

let _rows=[],_filtered=[],_sortCol='title',_sortDir=1,_page=0,_activeLib=null;
let _rowMap={}; // rating_key → full row object, avoids onclick escaping bugs
let _selectedKeys=new Set();
let _lastDbCheckIndex=null;
const PAGE=80;
// === STATE / STATUS MAPPING ===
const SC={'UNMONITORED':'#94a3b8','MISSING':'#f26d78','STAGED':'#b06aff','APPROVED':'#f5a623','AVAILABLE':'#2dd4a0','FAILED':'#f05252'};
function statusDesc(status){
  return uiTerm(`statuses.${status}.description`, _defaultStatusDesc[status] || '');
}
function formatStatusLabel(value){
  return String(value||'')
    .replace(/[_-]+/g,' ')
    .toLowerCase()
    .replace(/\b\w/g, chr=>chr.toUpperCase())
    .trim() || 'Unknown';
}
function displayStatus(status){
  const normalized=String(status||'').toUpperCase();
  return formatStatusLabel(uiTerm(`statuses.${normalized}.label`, _defaultStatusDisplay[normalized] || normalized));
}

function _hasLocalTheme(row){
  return String(row?.theme_exists||'')==='1';
}

function _hasSourceUrl(row){
  return !!String(row?.url||'').trim();
}

function _rowHasGoldenSource(row){
  return !!String(row?.golden_source_url||'').trim();
}

function _rowUsesGoldenSource(row){
  const sourceUrl=String(row?.url||'').trim();
  const goldenUrl=String(row?.golden_source_url||'').trim();
  return !!sourceUrl && !!goldenUrl && sourceUrl===goldenUrl;
}

function _normalizeSourceKind(value){
  const normalized=String(value||'').trim().toLowerCase();
  return normalized==='golden' || normalized==='custom' ? normalized : '';
}

function _normalizeSourceMethod(value){
  return String(value||'').trim().toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
}

function _legacySourceMethodFromOrigin(origin=''){
  const normalized=String(origin||'').trim().toLowerCase();
  if(!normalized) return '';
  if(normalized.startsWith('golden_source')) return 'golden_source';
  if(normalized.includes('playlist')) return 'playlist';
  if(normalized.includes('direct')) return 'direct';
  if(normalized==='manual' || normalized==='custom') return 'manual';
  return '';
}

function _sourceKindLabel(kind=''){
  return kind==='golden' ? 'Golden' : kind==='custom' ? 'Custom' : 'Unknown';
}

function _sourceMethodLabel(method=''){
  const map={
    golden_source:'Curated',
    playlist:'Playlist',
    direct:'Direct',
    custom:'Custom',
    paste:'Paste',
    manual:'Manual',
    existing:'Existing',
  };
  return map[method] || formatStatusLabel(method||'unknown');
}

function _sourceKindClass(kind=''){
  if(kind==='golden') return 'is-golden';
  if(kind==='custom') return 'is-custom';
  return 'is-unknown';
}

function _selectedSourceContract(row={}){
  const url=String(row?.url||'').trim();
  if(!url) return {kind:'', method:'', url:''};
  let kind=_normalizeSourceKind(row?.selected_source_kind);
  let method=_normalizeSourceMethod(row?.selected_source_method);
  if(!method) method=_legacySourceMethodFromOrigin(row?.source_origin);
  if(!kind){
    if(method==='golden_source' || _rowUsesGoldenSource(row)) kind='golden';
    else kind='custom';
  }
  if(!method) method=kind==='golden' ? 'golden_source' : 'manual';
  return {kind, method, url};
}

function _localSourceContract(row={}){
  const url=String(row?.local_source_url||'').trim();
  if(!url) return {kind:'', method:'', url:''};
  let kind=_normalizeSourceKind(row?.local_source_kind);
  let method=_normalizeSourceMethod(row?.local_source_method);
  if(!method || method==='manual_download' || method==='pass3_download'){
    const selected=_selectedSourceContract(row);
    method=selected.method || _legacySourceMethodFromOrigin(row?.local_source_origin) || 'manual';
  }
  if(!kind){
    const selected=_selectedSourceContract(row);
    kind=selected.kind || (method==='golden_source' ? 'golden' : 'custom');
  }
  return {kind, method, url};
}

function _goldenSourceState(row={}){
  const hasGolden=_rowHasGoldenSource(row);
  if(!hasGolden) return {key:'not_available', label:'Not Available', className:'is-unknown', detail:'No curated source', chips:[]};
  return {key:'available', label:'Ready', className:'is-golden', detail:'Curated source available', chips:['Curated']};
}

function _selectedSourceLabel(row={}){
  const sourceUrl=String(row?.url||'').trim();
  const goldenUrl=String(row?.golden_source_url||'').trim();
  const selected=_selectedSourceContract(row);
  const method=selected.method || _legacySourceMethodFromOrigin(row?.source_origin);
  if((sourceUrl && goldenUrl && sourceUrl===goldenUrl) || selected.kind==='golden' || method==='golden_source') return 'Golden Source';
  if(method==='playlist') return 'Playlist';
  if(method==='direct') return 'Direct';
  if(sourceUrl) return 'Custom';
  return '—';
}

function _selectedSourceStateText(row={}){
  const selectedUrl=String(row?.url||'').trim();
  if(!selectedUrl) return 'Not Selected';
  if(_hasLocalTheme(row)) return 'Downloaded';
  const status=String(row?.status||'').toUpperCase();
  if(status==='APPROVED') return 'Approved';
  if(status==='STAGED') return 'Staged';
  if(status==='FAILED') return 'Failed';
  if(status==='UNMONITORED') return 'Unmonitored';
  return 'Identified';
}

function _customSourceState(row={}){
  const selected=_selectedSourceContract(row);
  const typeLabel=_selectedSourceLabel(row);
  const statusLabel=_selectedSourceStateText(row);
  if(!selected.url){
    return {key:'none', typeLabel:typeLabel==='—'?'—':typeLabel, statusLabel, className:'is-unknown', detail:'', chips:[]};
  }
  return {
    key:typeLabel==='Golden Source' ? 'golden' : (selected.method||'custom'),
    typeLabel,
    statusLabel,
    className:_sourceKindClass(selected.kind),
    detail:'',
    chips:[],
  };
}

function _localSourceState(row={}){
  const hasLocal=_hasLocalTheme(row);
  if(hasLocal){
    const local=_localSourceContract(row);
    return {
      key:'downloaded',
      label:'On disk',
      className:_sourceKindClass(local.kind)||'is-direct',
      detail:local.url || 'Local theme recorded',
      chips:local.url ? [_sourceKindLabel(local.kind), _sourceMethodLabel(local.method)] : ['Local'],
    };
  }
  return {
    key:String(row?.status||'MISSING').toLowerCase(),
    label:'Missing',
    className:'is-unknown',
    detail:_hasSourceUrl(row) ? 'Awaiting download' : 'No local theme',
    chips:[],
  };
}

function _renderSourceStatePill(label, className='', title=''){
  const safeTitle=title ? ` title="${_escapeAttr(title)}"` : '';
  return `<span class="ui-pill db-source-pill ${className}"${safeTitle}>${_escapeHtml(label)}</span>`;
}

function _renderSourceStateChips(chips=[]){
  if(!Array.isArray(chips) || !chips.length) return '';
  return `<div class="db-source-meta">${chips.map(chip=>`<span class="ui-pill muted-chip db-source-chip">${_escapeHtml(chip)}</span>`).join('')}</div>`;
}

function _renderSourceStateCell(title, primary, secondary='', chips=[]){
  return `<div class="db-source-state">
    <span class="db-source-title">${_escapeHtml(title)}</span>
    ${primary}
    ${_renderSourceStateChips(chips)}
    ${secondary?`<div class="db-source-detail" title="${_escapeAttr(secondary)}">${_escapeHtml(secondary)}</div>`:''}
  </div>`;
}

function _statusValidation(row, attemptedStatus){
  const attempted=String(attemptedStatus||'').toUpperCase();
  const current=String(row?.status||'').toUpperCase();
  if(!STATUSES.includes(attempted)){
    return {ok:false, reason:'That status is no longer available.'};
  }
  if(current===attempted) return {ok:true};
  if(STATUS_MANUAL_ANY.includes(attempted)) return {ok:true};
  if(attempted==='STAGED' && !_hasSourceUrl(row)){
    return {ok:false, reason:'Add a source URL before moving this item to Staged.'};
  }
  if(attempted==='AVAILABLE' && !_hasLocalTheme(row)){
    return {ok:false, reason:'Local theme file is missing. Download it first.'};
  }
  if(attempted==='APPROVED' && current!=='STAGED'){
    return {ok:false, reason:'Only Staged items can be approved.'};
  }
  if(!STATUS_TRANSITIONS[current] || !STATUS_TRANSITIONS[current].includes(attempted)){
    return {ok:false, reason:`Cannot move ${displayStatus(current)} to ${displayStatus(attempted)}.`};
  }
  return {ok:true};
}

function allowedStatuses(row){
  const current=String(row?.status||'').toUpperCase();
  return STATUSES.filter(status=>status===current || _statusValidation(row,status).ok);
}

function renderStatusCell(row){
  const rk=row.rating_key;
  const status=row.status;
  const opts=allowedStatuses(row).map(s=>`<option value="${s}"${status===s?' selected':''}>${displayStatus(s)}</option>`).join('');
  return `<span class="badge s-${status}" title="${statusDesc(status)}" style="cursor:pointer;position:relative"><span class="si"></span><select class="st-sel" style="position:absolute;inset:0;opacity:0;cursor:pointer;width:100%" onchange="updateRow('${rk}','status',this.value)">${opts}</select><span class="status-label">${displayStatus(status)}</span></span>`;
}

function renderStatusKey(){
  const el=document.getElementById('status-key');
  if(!el) return;
  el.innerHTML=STATUSES.map(s=>`<span class="status-key-item"><span class="badge s-${s}" style="font-size:10px;padding:2px 7px"><span class="si"></span>${displayStatus(s)}</span><span class="status-key-desc">${statusDesc(s)}</span></span>`).join('');
}

async function loadDatabase(syncPeer=true){
  const cfg=await loadConfig();
  const allLibs=cfg.libraries||[{name:cfg.plex_library_name||'Movies',enabled:true}];
  const libs=allLibs.filter(l=>(!l.type||l.type==='movie'||l.type==='show') && l.enabled);
  const tabs=document.getElementById('lib-tabs');
  if(!libs.length){
    tabs.innerHTML='<div class="empty empty-inline">No enabled libraries — toggle one on in Configuration.</div>';
    document.getElementById('db-tbody').innerHTML='';
    document.getElementById('db-empty').style.display='block';
    document.getElementById('db-empty').textContent='No enabled libraries.';
    _refreshScopedRunLabels();
    removeItemDetailsPanel();
    return;
  }
  if(!_activeLib || !libs.some(l=>l.name===_activeLib)) _activeLib=libs[0].name;
  tabs.innerHTML=libs.map((lib,i)=>`
    <div class="tab ${i===0&&!_activeLib||_activeLib===lib.name?'active':''}"
      data-lib="${lib.name}"
      onclick="switchLib('${lib.name.replace(/'/g,"\\'")}')">
      ${lib.name}
    </div>`).join('');
  if(!_activeLib) _activeLib=libs[0]?.name;
  _refreshScopedRunLabels();
  await loadLibRows(_activeLib);
  removeItemDetailsPanel();
}

async function switchLib(name){
  _activeLib=name;
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.textContent.trim().replace(' (off)','')===name));
  _refreshScopedRunLabels();
  await loadLibRows(name);
  removeItemDetailsPanel();
}

async function loadLibRows(name){
  const {data}=await requestJson('/api/ledger?library='+encodeURIComponent(name));
  _rows=Array.isArray(data)?data:[];
  // Build map for safe data lookup from action buttons (avoids onclick escaping)
  _rowMap={};
  _rows.forEach(row=>{ if(row.rating_key) _rowMap[row.rating_key]=row; });
  renderChips(); filterTable();
}

function upsertLedgerRow(savedRow){
  if(!savedRow || !savedRow.rating_key) return null;
  const rk=String(savedRow.rating_key);
  const existingIndex=_rows.findIndex(r=>String(r.rating_key)===rk);
  const merged=existingIndex>=0 ? {..._rows[existingIndex], ...savedRow} : {...savedRow};
  if(existingIndex>=0) _rows.splice(existingIndex,1,merged);
  else _rows.push(merged);
  _rowMap[rk]=merged;
  return merged;
}

function renderChips(){
  const counts={};
  _rows.forEach(r=>{
    counts[r.status]=(counts[r.status]||0)+1;
  });
  const filters=[
    {label:'Total', count:_rows.length, active:!document.getElementById('db-search').value.trim() && !document.getElementById('db-filter').value && !document.getElementById('db-source-filter').value && !document.getElementById('db-action-filter').value, color:'', handler:"clearDbFilter()"},
    {label:'MISSING', count:counts.MISSING||0, active:document.getElementById('db-filter').value==='MISSING', color:SC.MISSING, handler:"filterByStatus('MISSING')"},
    {label:'AVAILABLE', count:counts.AVAILABLE||0, active:document.getElementById('db-filter').value==='AVAILABLE', color:SC.AVAILABLE, handler:"filterByStatus('AVAILABLE')"},
    {label:'STAGED', count:counts.STAGED||0, active:document.getElementById('db-filter').value==='STAGED', color:SC.STAGED, handler:"filterByStatus('STAGED')"},
    {label:'APPROVED', count:counts.APPROVED||0, active:document.getElementById('db-filter').value==='APPROVED', color:SC.APPROVED, handler:"filterByStatus('APPROVED')"},
    {label:'FAILED', count:counts.FAILED||0, active:document.getElementById('db-filter').value==='FAILED', color:SC.FAILED, handler:"filterByStatus('FAILED')"}
  ];
  document.getElementById('db-chips').innerHTML=filters.map(f=>`
    <button type="button" class="chip ${f.active?'active':''}" onclick="${f.handler}">
      <span class="chip-head">${f.count}</span>
      <span class="chip-body">${f.color?`<span class="chip-dot" style="background:${f.color}"></span>`:''}<span class="status-label"${f.color?` style="color:${f.color}"`:''}>${formatStatusLabel(f.label)}</span></span>
    </button>`).join('');
  updateDbActionHints(counts);
}


function updateDbActionHints(counts){
  const pending=counts.MISSING||0;
  const staged=counts.STAGED||0;
  const approved=counts.APPROVED||0;
  const btnResolve=document.getElementById('db-btn-resolve');
  const btnDownload=document.getElementById('db-btn-download');
  const btnApproveAll=document.getElementById('db-approve-all');
  if(btnResolve){
    btnResolve.disabled=pending===0;
    btnResolve.title=pending?`Find sources for ${pending} missing items`:'No missing items to resolve';
  }
  if(btnDownload){
    btnDownload.disabled=approved===0;
    btnDownload.title=approved?`Download ${approved} Approved item${approved===1?'':'s'}`:'No Approved items to download';
  }
  if(btnApproveAll){
    btnApproveAll.disabled=staged===0;
    btnApproveAll.title=staged?`Set ${staged} Staged item${staged===1?'':'s'} to Approved`:'No Staged items to approve';
  }
}

function filterByStatus(s){ document.getElementById('db-filter').value=s; filterTable(); }
function filterBySourceState(value){ document.getElementById('db-source-filter').value=value; filterTable(); }
function filterByAction(value){ document.getElementById('db-action-filter').value=value; filterTable(); }
function clearDbFilter(opts={}){
  document.getElementById('db-filter').value='';
  document.getElementById('db-source-filter').value='';
  document.getElementById('db-action-filter').value='';
  if(!opts.keepSearch) document.getElementById('db-search').value='';
  filterTable();
}

function rowActionType(row){
  const status=String(row?.status||'').toUpperCase();
  if(status==='STAGED') return 'APPROVE';
  if(status==='APPROVED') return 'DOWNLOAD';
  if(status==='MISSING' && !_hasSourceUrl(row)) return 'FIND_SOURCE';
  return 'MANAGE';
}

function _sortVal(row,col){
  if(col==='title') return (row.title||row.plex_title||'').toString().toLowerCase();
  if(col==='year') return Number(row.year||0);
  if(col==='start_offset') return parseTrim(row.start_offset||'0');
  if(col==='last_updated') return Date.parse((row.last_updated||'').replace(' ','T'))||0;
  if(col==='golden_source_url'){
    const url=String(row.golden_source_url||'').trim().toLowerCase();
    return [url?1:0,url];
  }
  if(col==='golden_state'){
    const state=_goldenSourceState(row);
    const rank={not_available:0,available:1,downloaded:2};
    return [rank[state.key] ?? 0, state.label.toLowerCase()];
  }
  if(col==='custom_state'){
    const state=_customSourceState(row);
    const rank={none:0,custom:1,direct:2,playlist:3,golden:4};
    return [rank[state.key] ?? 0, `${state.typeLabel} ${state.statusLabel}`.toLowerCase()];
  }
  if(col==='source_origin') return (row.source_origin||'').toString().toLowerCase();
  if(col==='current_theme') return (row.status==='AVAILABLE'?1:0);
  return (row[col]||'').toString().toLowerCase();
}

function filterTable(){
  const q=(document.getElementById('db-search').value||'').toLowerCase().trim();
  const st=document.getElementById('db-filter').value;
  const sourceFilter=document.getElementById('db-source-filter').value;
  const actionFilter=document.getElementById('db-action-filter').value;
  const clearBtn=document.getElementById('db-clear-filters');
  if(clearBtn) clearBtn.style.display=(q||st||sourceFilter||actionFilter)?'':'none';
  renderChips();
  _filtered=_rows.filter(r=>{
    if(st&&r.status!==st) return false;
    const hasGolden=!!String(r.golden_source_url||'').trim();
    const hasSource=!!String(r.url||'').trim();
    const hasCustom=hasSource && !_rowUsesGoldenSource(r);
    const hasLocal=_hasLocalTheme(r);
    if(sourceFilter==='GOLDEN' && !hasGolden) return false;
    if(sourceFilter==='NO_GOLDEN' && hasGolden) return false;
    if(sourceFilter==='CUSTOM' && !hasCustom) return false;
    if(sourceFilter==='NO_CUSTOM' && hasCustom) return false;
    if(sourceFilter==='LOCAL' && !hasLocal) return false;
    if(sourceFilter==='NO_LOCAL' && hasLocal) return false;
    if(actionFilter && rowActionType(r)!==actionFilter) return false;
    const haystack=[r.title||'',r.plex_title||'',r.year||'',r.url||'',r.golden_source_url||'',r.notes||'',r.status||''].join(' ').toLowerCase();
    if(q&&!haystack.includes(q)) return false;
    return true;
  });
  _filtered.sort((a,b)=>{
    if(_sortCol==='golden_source_url' || _sortCol==='golden_state' || _sortCol==='custom_state'){
      const [ap,au]=_sortVal(a,_sortCol);
      const [bp,bu]=_sortVal(b,_sortCol);
      if(ap!==bp) return (ap-bp)*_sortDir;
      return au<bu?-_sortDir:au>bu?_sortDir:0;
    }
    const av=_sortVal(a,_sortCol), bv=_sortVal(b,_sortCol);
    return av<bv?-_sortDir:av>bv?_sortDir:0;
  });
  _page=0; renderTable();
}

let _dbFilterTimer=null;
function debouncedFilterTable(){
  clearTimeout(_dbFilterTimer);
  _dbFilterTimer=setTimeout(()=>filterTable(),150);
}

function sortTable(col){ if(_sortCol===col)_sortDir*=-1; else{_sortCol=col;_sortDir=1;} filterTable(); }

// ── Audio player ─────────────────────────────────────────────────────────────
let _curAudio=null, _curKey=null;
const _audioEl=()=>document.getElementById('global-audio');

function _htmlAttr(s){
  if(s==null||s===undefined) return '';
  return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;');
}

function _setHidden(el, hidden=true, displayValue=''){
  if(!el) return;
  el.classList.toggle('hidden', !!hidden);
  el.style.display=hidden?'none':displayValue;
}

function _truncateSourceText(value, {fallback='—', max=64, middle=false}={}){
  const raw=String(value||'').trim();
  if(!raw) return fallback;
  if(raw.length<=max) return raw;
  if(!middle) return raw.slice(0, Math.max(1, max-1))+'…';
  const head=Math.max(18, Math.floor((max-1)/2));
  const tail=Math.max(12, max-head-1);
  return `${raw.slice(0,head)}…${raw.slice(-tail)}`;
}

function _applyTruncatedText(el, value, options={}){
  if(!el) return;
  const raw=String(value||'').trim();
  el.textContent=_truncateSourceText(raw, options);
  el.title=raw||options.fallback||'';
}

function _applyAutoScrollText(el, value, {fallback='—'}={}){
  if(!el) return;
  const raw=String(value||'').trim();
  const text=raw || fallback;
  el.innerHTML=`<span>${text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</span>`;
  el.title=raw || fallback;
  el.classList.add('auto-scroll-text');
  requestAnimationFrame(()=>{
    const span=el.querySelector('span');
    const scrollDistance=Math.max(0, (span?.scrollWidth||0) - (el.clientWidth||0));
    el.style.setProperty('--scroll-distance', `${scrollDistance}px`);
    el.classList.toggle('auto-scroll-active', !!raw && scrollDistance > 12);
  });
}

function _renderSelectedSourceSummary(url, title){
  const subtitleEl=document.getElementById('se-source-summary-subtitle');
  const copyBtn=document.getElementById('se-copy-btn');
  const openBtn=document.getElementById('se-open-btn');
  const cleanUrl=String(url||'').trim();
  const row=_rowMap[_seKey||_searchKey]||_rows.find(r=>String(r.rating_key)===String(_seKey||_searchKey))||{};
  _renderSourceStateStack('se-source-state-stack', row, {compact:true, draft:{selectedUrl:cleanUrl}});
  if(subtitleEl){
    subtitleEl.textContent=cleanUrl
      ? `${String(title||'').trim()||_sourceTitleFromUrl(cleanUrl)}`
      : 'Review the saved source and local theme file before saving.';
  }
  if(copyBtn) copyBtn.disabled=!cleanUrl;
  if(openBtn) openBtn.disabled=!cleanUrl;
}

function _manualSaveTargetStatus(){
  return 'STAGED';
}

function _recommendedAction(row){
  if(row.rating_key) return {label:'Manage', cls:'btn btn-blue btn-xs row-primary-action', fn:'_playTheme'};
  return {label:'Manage', cls:'btn btn-ghost btn-xs row-primary-action', fn:'_manualSearch'};
}

function _rowSecondaryActions(row){
  const hasTheme=(row.status==='AVAILABLE');
  const hasSource=_hasSourceUrl(row);
  const actions=[];
  if(hasSource) actions.push({label:'Preview source', fn:'_previewSource'});
  if(row.status!=='STAGED' && hasSource) actions.push({label:'Set Staged', fn:'_stageRow'});
  if(row.status!=='APPROVED' && row.status==='STAGED') actions.push({label:'Approve to Approved', fn:'_approveRow'});
  if(row.rating_key) actions.push({label:'Open manage', fn:'_playTheme'});
  if(hasTheme && row.rating_key) actions.push({label:'Delete theme', fn:'_deleteTheme'});
  actions.push({label:'Manual search', fn:'_manualSearch'});
  return actions;
}

function renderRowActionCell(row){
  const rk=_htmlAttr(row.rating_key);
  const url=_htmlAttr(row.url||'');
  const folder=_htmlAttr(row.folder||'');
  const title=_htmlAttr(row.title||row.plex_title||'');
  const year=_htmlAttr(row.year||'');
  const primary=_recommendedAction(row);
  const secondaries=_rowSecondaryActions(row)
    .filter(a=>a.fn!==primary.fn)
    .map(a=>`<button class="btn btn-ghost btn-xs row-action-item" data-rk="${rk}" data-url="${url}" data-folder="${folder}" data-title="${title}" data-year="${year}" onclick="_rowActionInvoke('${a.fn}',this)">${a.label}</button>`)
    .join('');
  return `<div class="row-action-wrap">
    <button class="${primary.cls}" data-rk="${rk}" data-url="${url}" data-folder="${folder}" data-title="${title}" data-year="${year}" onclick="${primary.fn}(this)">${primary.label}</button>
    <button class="btn btn-ghost btn-xs row-action-more" onclick="toggleRowMenu(this,event)" title="More actions">⋯</button>
    <div class="row-action-menu">${secondaries||'<span class="bulk-hint" style="padding:6px">No secondary actions</span>'}</div>
  </div>`;
}

function _closeAllRowMenus(){
  document.querySelectorAll('.row-action-menu.open').forEach(m=>{
    m.classList.remove('open');
    m.style.position='';m.style.top='';m.style.left='';
  });
}
function toggleRowMenu(btn, evt){
  if(evt) evt.stopPropagation();
  const wrap=btn.closest('.row-action-wrap');
  if(!wrap) return;
  const menu=wrap.querySelector('.row-action-menu');
  const willOpen=!menu.classList.contains('open');
  _closeAllRowMenus();
  if(willOpen){
    menu.classList.add('open');
    const r=btn.getBoundingClientRect();
    menu.style.position='fixed';
    menu.style.left=Math.max(0,r.right-170)+'px';
    const menuH=menu.offsetHeight||160;
    if(r.top>menuH+8) menu.style.top=(r.top-menuH-6)+'px';
    else menu.style.top=(r.bottom+6)+'px';
  }
}

function _rowActionInvoke(fnName, btn){
  _closeAllRowMenus();
  if(typeof window[fnName]==='function') window[fnName](btn);
}

function _stageRow(btn){
  const rk=btn.dataset.rk;
  if(!rk) return;
  updateRow(rk,'status','STAGED');
}

function _approveRow(btn){
  const rk=btn.dataset.rk;
  if(!rk) return;
  updateRow(rk,'status','APPROVED');
}

// Safe action helpers — read data from DOM attributes, _rowMap is secondary fallback
function _playTheme(btn){
  const rk=btn.dataset.rk;
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  const title=row?(row.title||row.plex_title||''):btn.dataset.title||'';
  const year=row?row.year||'':btn.dataset.year||'';
  const folder=row?row.folder||'':btn.dataset.folder||'';
  openThemeModal(rk, title, year, folder, row||{}, _activeLib||row?.library||'');
}
function _deleteTheme(btn){
  const rk=(btn.dataset.rk||'').trim();
  const folder=(btn.dataset.folder||'').trim();
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  const title=row?(row.title||row.plex_title||''):btn.dataset.title||rk||'Unknown';
  const resolvedFolder=(row?.folder||'').trim()||folder;
  if(!rk){
    toast('This item is missing its Plex ID — run ⟳ Scan Library first','err');
    return;
  }
  if(!resolvedFolder){
    toast('No folder path for this item — run ⟳ Scan Library to fix, then try again','err');
    return;
  }
  openDeleteModal(rk, title, _activeLib||'', resolvedFolder);
}
function _openTmdbRow(rk, evt){
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  if(!row) return true;
  return openTmdb(row.title||row.plex_title||'', row.year||'', evt);
}

function _previewSource(btn){
  if(btn.disabled) return;
  const rk=btn.dataset.rk;
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  const url=(row?.url)||btn.dataset.url||'';
  if(!url) return;
  const title=row?(row.title||row.plex_title||''):'';
  const year=row?row.year||'':'';
  openYtModal(rk, title, year, url, encodeURIComponent(_activeLib||row?.library||''));
}
function _downloadNow(btn){
  if(btn.disabled) return;
  const rk=btn.dataset.rk;
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  if(!row) return;
  downloadNow(rk, row.title||row.plex_title||'', _activeLib||'');
}
function _manualSearch(btn){
  const rk=btn.dataset.rk;
  const row=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  if(!row) return;
  openSearchModal(rk, row.title||row.plex_title||'', row.year||'', encodeURIComponent(_activeLib||row?.library||''));
}

function renderUrlInput(row){
  let html='';
  if(row.url){
    html+=`<a href="${row.url}" target="_blank" rel="noopener" class="db-link-icon" title="Open in YouTube">↗</a>`;
  }
  html+=`<input class="inline-ed db-inline-url wide" value="${(row.url||'').replace(/"/g,'&quot;')}" placeholder="paste URL…" onblur="updateRowAndRefresh('${row.rating_key}','url',this.value)" onkeydown="if(event.key==='Enter')this.blur()">`;
  return html;
}

function togglePlay(key,src,btn){
  const audio=_audioEl();
  if(_curKey===key){
    if(audio.paused){ audio.play(); btn.innerHTML='<svg viewBox="0 0 24 24" fill="#000"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>'; }
    else { audio.pause(); btn.innerHTML='<svg viewBox="0 0 24 24" fill="#000"><polygon points="5 3 19 12 5 21 5 3"/></svg>'; }
    return;
  }
  stopAllAudio();
  _curKey=key;
  audio.src=apiUrl(src);
  audio.play();
  btn.innerHTML='<svg viewBox="0 0 24 24" fill="#000"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>';
  audio.ontimeupdate=()=>{
    const pct=audio.duration?(audio.currentTime/audio.duration*100):0;
    const fill=document.getElementById('fill-'+key);
    const timeEl=document.getElementById('time-'+key);
    if(fill) fill.style.width=pct+'%';
    if(timeEl) timeEl.textContent=fmt(audio.currentTime)+(audio.duration?' / '+fmt(audio.duration):'');
  };
  audio.onended=()=>{ stopCurrentAudio(); };
}

function stopCurrentAudio(){
  if(!_curKey) return;
  const audio=_audioEl(); audio.pause(); audio.ontimeupdate=null; audio.onended=null;
  // Reset ALL play buttons to play icon (fixes visual glitch across pages)
  document.querySelectorAll('.play-btn').forEach(btn=>{
    btn.innerHTML='<svg viewBox="0 0 24 24" fill="#000"><polygon points="5 3 19 12 5 21 5 3"/></svg>';
  });
  const oldFill=document.getElementById('fill-'+_curKey);
  if(oldFill) oldFill.style.width='0%';
  const oldTime=document.getElementById('time-'+_curKey);
  if(oldTime) oldTime.textContent='0:00';
  _curKey=null;
}

function stopAllAudio(keepId){
  stopCurrentAudio();
  const ga=_audioEl();
  if(ga){ ga.pause(); }
  const keep = new Set(Array.isArray(keepId) ? keepId : (keepId ? [keepId] : []));
  ['yt-modal-audio','theme-modal-audio','trim-modal-audio','se-audio'].forEach(id=>{
    if(keep.has(id)) return;
    const a=document.getElementById(id);
    if(a){ a.pause(); a.onloadedmetadata=null; a.ontimeupdate=null; a.onended=null; a.src=''; }
  });
  resetActivePreviewBtn();
}

document.addEventListener('visibilitychange',()=>{
  if(document.hidden) stopAllAudio();
});
window.addEventListener('blur',()=>{ stopAllAudio(); });

function skipAudio(key, secs){
  if(_curKey!==key) return;
  const audio=_audioEl();
  if(!audio.duration) return;
  audio.currentTime = Math.max(0, Math.min(audio.duration, audio.currentTime + secs));
}

function seekAudio(e,bar,key){
  if(_curKey!==key) return;
  const audio=_audioEl(); if(!audio.duration) return;
  const rect=bar.getBoundingClientRect();
  audio.currentTime=(e.clientX-rect.left)/rect.width*audio.duration;
}

function fmt(s){ const m=Math.floor(s/60),sec=Math.floor(s%60); return m+':'+(sec<10?'0':'')+sec; }
function parseTrim(val){
  // Accepts: "80" (seconds), "1:20" (1m 20s = 80s), "02:30" (2m 30s = 150s)
  if(typeof val==='number') return val;
  val=String(val).trim();
  if(val.includes(':')){
    const parts=val.split(':');
    return (parseInt(parts[0])||0)*60+(parseInt(parts[1])||0);
  }
  return parseInt(val)||0;
}

function normalizeOffsetInput(el){
  if(!el) return 0;
  const s=parseTrim(el.value||'0');
  el.value=fmt(Math.max(0,s));
  return s;
}

function normalizeOptionalOffsetInput(el){
  if(!el) return 0;
  const raw=String(el.value||'').trim();
  if(!raw){
    el.value='';
    return 0;
  }
  const s=parseTrim(raw);
  el.value=fmt(Math.max(0,s));
  return s;
}

function adjustOffset(el, delta){
  if(!el) return;
  const cur=parseTrim(el.value||0);
  const next=Math.max(0, cur + (delta||0));
  el.value=fmt(next);
  el.dispatchEvent(new Event('input',{bubbles:true}));
}

function bindOffsetWheel(){
  if(window._offsetWheelBound) return;
  window._offsetWheelBound=true;
  document.addEventListener('wheel', (e)=>{
    const el=e.target;
    if(el && el.classList && el.classList.contains('offset-input')){
      e.preventDefault();
      adjustOffset(el, e.deltaY<0 ? 5 : -5);
    }
  }, {passive:false});
}

async function previewUrl(key, encodedUrl){
  const url = decodeURIComponent(encodedUrl);
  toast('Fetching preview…','info');
  try {
    const r = await fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
    if(!r.ok){ toast('Preview failed','err'); return; }
    const data = await r.json();
    if(!data.ok){ toast(data.error||'Preview failed','err'); return; }
    // Play the preview audio
    const audio = _audioEl();
    stopAllAudio();
    _curKey = key;
    audio.src = apiUrl(data.audio_url);
    audio.play();
    toast('Playing preview','ok');
    // Update time display if player exists
    audio.ontimeupdate=()=>{
      const timeEl=document.getElementById('time-'+key);
      if(timeEl) timeEl.textContent=fmt(audio.currentTime)+(audio.duration?' / '+fmt(audio.duration):'');
    };
    audio.onended=()=>{ _curKey=null; };
  } catch(e){ toast('Preview error','err'); }
}

document.addEventListener('click',(e)=>{
  if(!e.target.closest('.row-action-wrap')) _closeAllRowMenus();
});
document.querySelector('.tbl-wrap')?.addEventListener('scroll',()=>_closeAllRowMenus());

// === TABLE / ROW RENDERING ===
// ── Table render ─────────────────────────────────────────────────────────────
function renderTable(){
  const start=_page*PAGE, slice=_filtered.slice(start,start+PAGE);
  const tbody=document.getElementById('db-tbody');
  const empty=document.getElementById('db-empty');
  if(!_filtered.length){
    tbody.innerHTML=''; 
    const q=(document.getElementById('db-search').value||'').trim();
    const st=document.getElementById('db-filter').value;
    const hasFilter=!!(q||st||document.getElementById('db-source-filter').value||document.getElementById('db-action-filter').value);
    empty.innerHTML=hasFilter
      ? `No matches for the current filters. <button class="btn btn-ghost btn-xs" onclick="clearDbFilter()">Clear filters</button>`
      : `No items yet. <button class="btn btn-ghost btn-xs" onclick="dbRunPass(1)">⟳ Scan Library</button>`;
    empty.style.display='block'; 
    document.getElementById('db-pag').innerHTML='';
    return;
  }
  empty.style.display='none';
  tbody.innerHTML=slice.map(row=>{
    const rk=row.rating_key;
    const checked=_selectedKeys.has(rk)?'checked':'';
    const rawTitle=(row.title||row.plex_title||'');
    const titleAttr=rawTitle.replace(/"/g,'&quot;');
    const tmdbHref=_tmdbLink(row.title||row.plex_title,row.year);
    const goldenState=_goldenSourceState(row);
    const customState=_customSourceState(row);
    return `<tr>
      <td><input type="checkbox" class="row-cb" ${checked} onclick="toggleRowSelect('${rk}',this,event)"></td>
      <td class="db-cell-title" title="${titleAttr}">
        <a class="media-title-link" href="${tmdbHref}" onclick="return _openTmdbRow('${rk}',event)" target="_blank" rel="noopener">${rawTitle}</a>
      </td>
      <td class="db-cell-subtle">${row.year||''}</td>
      <td>
        ${renderStatusCell(row)}
      </td>
      <td>${renderRowActionCell(row)}</td>
      <td>${_renderSourceStateCell('Golden', _renderSourceStatePill(goldenState.label, goldenState.className, goldenState.detail), '', goldenState.chips)}</td>
      <td>${_renderSourceStateCell('Selected', _renderSourceStatePill(customState.typeLabel, customState.className, customState.statusLabel), customState.statusLabel, customState.chips)}</td>
      <td class="db-cell-mono">${(row.last_updated||'').slice(5,16)}</td>
      <td class="db-cell-notes" title="${(row.notes||'').replace(/"/g,'&quot;')}">${row.notes||'—'}</td>
    </tr>`;
  }).join('');

  const tot=Math.ceil(_filtered.length/PAGE);
  document.getElementById('db-pag').innerHTML=tot<=1
    ?`<span>${_filtered.length} movies</span>`
    :`<button class="pag-btn" onclick="goPage(${_page-1})" ${_page===0?'disabled':''}>← Prev</button>
      <span>Page ${_page+1} of ${tot} · ${_filtered.length} movies</span>
      <button class="pag-btn" onclick="goPage(${_page+1})" ${_page>=tot-1?'disabled':''}>Next →</button>`;
}

function goPage(p){ _page=p; renderTable(); window.scrollTo(0,0); }

function requireLibraryContext(lib, actionLabel='perform this action'){
  const library=String(lib||'').trim();
  if(library) return library;
  toast(`Select a library before you ${actionLabel}`,'info');
  return '';
}

async function updateRow(key,field,value,selEl){
  const library=requireLibraryContext(_activeLib,'save changes');
  if(!library){
    renderTable();
    return;
  }
  const row=_rows.find(r=>r.rating_key===key);
  const prevValue=row?row[field]:undefined;
  if(field==='status' && row){
    const localCheck=_statusValidation(row,value);
    if(!localCheck.ok){
      toast(localCheck.reason,'info');
      renderTable();
      return;
    }
  }
  if(row){
    row[field]=value;
    if(_rowMap[key]) _rowMap[key][field]=value; // keep map in sync
  }
  const {ok,data}=await patchJson('/api/ledger/'+key+'?library='+encodeURIComponent(library),{[field]:value});
  if(ok){
    toast('Saved','ok');
    renderChips();
    renderTable();
  } else {
    if(row){
      row[field]=prevValue;
      if(_rowMap[key]) _rowMap[key][field]=prevValue;
    }
    const msg=[data.error,data.reason_code?`(${data.reason_code})`:'' ,data.current_status&&data.attempted_status?`[${data.current_status} → ${data.attempted_status}]`:'' ].filter(Boolean).join(' ').trim();
    toast(msg||'Save failed','err');
    await loadDatabase(false);
  }
}

async function updateRowAndRefresh(key,field,value){
  await updateRow(key,field,value);
  renderChips();
  filterTable();
}

async function saveOffsetInput(key, el){
  const s=normalizeOffsetInput(el);
  await updateRow(key,'start_offset',s);
}

async function bulkApprove(){
  const library=requireLibraryContext(_activeLib,'run bulk approve');
  if(!library) return;
  const staged=_rows.filter(r=>r.status==='STAGED');
  if(!staged.length){ toast('No Staged items','info'); return; }
  const {ok,data}=await postJson('/api/ledger/bulk?library='+encodeURIComponent(library),{keys:staged.map(r=>r.rating_key),status:'APPROVED'});
  if(ok){
    staged.forEach(row=>{ row.status='APPROVED'; if(_rowMap[row.rating_key]) _rowMap[row.rating_key].status='APPROVED'; });
    const updated=data.updated ?? staged.length;
    const skipped=data.skipped ?? 0;
    toast(`Set ${updated} Staged item${updated===1?'':'s'} to Approved${skipped?` · ${skipped} skipped`:''}`,'ok');
    renderChips();
    filterTable();
  } else toast('Failed','err');
}

async function dbDownloadApproved(){
  const approved=_rows.filter(r=>r.status==='APPROVED');
  if(!approved.length){ toast('No Approved items','info'); return; }
  dbRunPass(3);
}


async function importGoldenSource(){
  if(!_activeLib){ toast('Select a library first','info'); return; }
  openGoldenSourceModal(_activeLib);
}

async function downloadNow(rk, title, lib){
  const library=requireLibraryContext(lib||_activeLib,'download a theme');
  if(!library) return;
  const row = _rows.find(r=>r.rating_key===rk);
  if(!row){ toast('Row not found','err'); return; }
  if(!row.url){ toast('No source URL on this row — add a source first','info'); return; }
  const ok=await openConfirmModal('Manual Download', {
    text:`Download theme now for ${title}?`,
    fields:[{label:'Source URL', value:row.url, type:'url', mono:true}]
  }, 'Download Now');
  if(!ok) return;
  toast('Downloading…','info');
  dbProgressStart('Downloading selected title…');
  dbProgressSet(35,'Preparing download…');
  const r = await fetch('/api/theme/download-now',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({rating_key:rk, library, folder:row.folder||'', tmdb_id:row.tmdb_id||''})
  });
  const data = await r.json();
  if(data.ok){
    const how=data.matched_by?` (${data.matched_by})`:'';
    dbProgressSet(90,'Finalizing download…');
    toast((data.message||'Downloaded')+how,'ok');
    dbProgressDone('Download complete');
    loadDatabase();
  }
  else {
    dbProgressFail(data.error||'Download failed');
    toast(data.error||'Download failed','err');
  }
}


function toggleRowSelect(key,cb,evt){
  const idx=_filtered.findIndex(r=>r.rating_key===key);
  if(evt && evt.shiftKey && _lastDbCheckIndex!==null && idx!==-1){
    const start=Math.min(_lastDbCheckIndex, idx);
    const end=Math.max(_lastDbCheckIndex, idx);
    for(let i=start;i<=end;i++){
      const rk=_filtered[i].rating_key;
      if(cb.checked) _selectedKeys.add(rk); else _selectedKeys.delete(rk);
    }
  }else{
    if(cb.checked) _selectedKeys.add(key); else _selectedKeys.delete(key);
  }
  _lastDbCheckIndex=idx;
  updateBulkBar();
  renderTable();
}

function toggleAllSelect(masterCb){
  _filtered.forEach(row=>{ if(masterCb.checked) _selectedKeys.add(row.rating_key); else _selectedKeys.delete(row.rating_key); });
  _lastDbCheckIndex=null;
  renderTable(); updateBulkBar();
}

function selectAllVisible(){
  const start=_page*PAGE, slice=_filtered.slice(start,start+PAGE);
  slice.forEach(row=>_selectedKeys.add(row.rating_key));
  updateBulkBar(); renderTable();
}

function updateBulkBar(){
  const bar=document.getElementById('db-bulk-bar');
  const countEl=document.getElementById('db-bulk-count');
  if(_selectedKeys.size>0){ bar.classList.add('active'); countEl.textContent=_selectedKeys.size+' selected'; }
  else bar.classList.remove('active');
  const approveBtn=document.getElementById('db-bulk-approve');
  const clearSel=document.getElementById('db-clear-sel');
  const selectedWithUrl=[..._selectedKeys].some(k=>{
    const row=_rows.find(r=>r.rating_key===k);
    return row && row.url;
  });
  const selectedStaged=[..._selectedKeys].some(k=>{
    const row=_rows.find(r=>r.rating_key===k);
    return row && row.status==='STAGED';
  });
  if(approveBtn){
    approveBtn.disabled=!selectedStaged;
    approveBtn.title=selectedStaged?'Set selected Staged items to Approved':'Only Staged items can be approved';
  }
  if(clearSel) clearSel.disabled=!selectedWithUrl;
  if(clearSel) clearSel.title=selectedWithUrl?'Clear source URLs for selected items':'Select items with source URLs to clear';
  const master=document.getElementById('db-select-all');
  if(master && _filtered.length){
    const allSelected=_filtered.every(r=>_selectedKeys.has(r.rating_key));
    const someSelected=_filtered.some(r=>_selectedKeys.has(r.rating_key));
    master.checked=allSelected; master.indeterminate=someSelected&&!allSelected;
  }
}

function clearSelection(){ _selectedKeys.clear(); _lastDbCheckIndex=null; renderTable(); updateBulkBar(); }

async function bulkSetStatus2(status){
  const library=requireLibraryContext(_activeLib,'run a bulk status update');
  if(!library) return;
  if(!_selectedKeys.size){ toast('Nothing selected','info'); return; }
  const keys=[..._selectedKeys];
  const {ok,data:d}=await postJson('/api/ledger/bulk?library='+encodeURIComponent(library),{keys,status});
  if(ok){
    keys.forEach(key=>{
      const row=_rows.find(r=>r.rating_key===key);
      if(row) row.status=status;
      if(_rowMap[key]) _rowMap[key].status=status;
    });
    const updated=d.updated ?? keys.length;
    const skipped=d.skipped ?? 0;
    toast(`${updated} items → ${displayStatus(status)}${skipped?` · ${skipped} skipped`:''}`,'ok');
    clearSelection();
    renderChips();
    filterTable();
  }
  else{
    const msg=[d.error,d.reason_code?`(${d.reason_code})`:'' ,d.current_status&&d.attempted_status?`[${displayStatus(d.current_status)} → ${displayStatus(d.attempted_status)}]`:'' ].filter(Boolean).join(' ').trim();
    toast(msg||'Bulk update failed','err');
    await loadDatabase(false);
  }
}

async function clearSourcesSelected(){
  const library=requireLibraryContext(_activeLib,'clear source URLs');
  if(!library) return;
  const keys=[..._selectedKeys];
  const rows=_rows.filter(r=>keys.includes(r.rating_key));
  if(!rows.length){ toast('Nothing selected','info'); return; }
  const confirmed=await openConfirmModal('Clear Source URLs', {
    text:`Clear source URLs for ${rows.length} selected items?`
  }, 'Clear URLs');
  if(!confirmed) return;
  const {ok,data:d}=await postJson(apiUrl('/api/ledger/clear-sources?library='+encodeURIComponent(library)),{keys});
  if(!ok){
    toast(d.error||'Failed to clear source URLs','err');
    await loadDatabase(false);
    return;
  }
  rows.forEach(row=>{
    row.url='';
    if(String(row.status||'').toUpperCase()!=='AVAILABLE') row.status='MISSING';
    if(_rowMap[row.rating_key]){
      _rowMap[row.rating_key].url='';
      if(String(_rowMap[row.rating_key].status||'').toUpperCase()!=='AVAILABLE') _rowMap[row.rating_key].status='MISSING';
    }
  });
  const s=d.summary||{};
  const parts=[`${s.cleared||0} URLs cleared`];
  if(s.preserved_available) parts.push(`${s.preserved_available} kept ${displayStatus('AVAILABLE')}`);
  if(s.reset_missing) parts.push(`${s.reset_missing} reset to ${displayStatus('MISSING')}`);
  if(s.skipped_without_url) parts.push(`${s.skipped_without_url} already empty`);
  toast(parts.join(' · '),'ok');
  clearSelection();
  renderChips();
  filterTable();
}



function _currentLib(){
  return document.getElementById('lib-tabs')?.querySelector('.tab.active')?.dataset?.lib||_activeLib||'';
}
const _bioCache={};
function _bioCacheKey(rk, library=''){
  return `${String(library||'').trim() || '__current__'}::${String(rk||'')}`;
}
async function fetchBio(rk, library=''){
  const lib=String(library||'').trim()||_currentLib();
  const cacheKey=_bioCacheKey(rk, lib);
  if(_bioCache[cacheKey]!==undefined) return _bioCache[cacheKey];
  try{
    const qs='key='+encodeURIComponent(rk)+(lib?'&library='+encodeURIComponent(lib):'');
    const r=await fetch('/api/movie/bio?'+qs);
    const d=await r.json();
    _bioCache[cacheKey]=d.summary||'';
    return _bioCache[cacheKey];
  }catch{
    _bioCache[cacheKey]='';
    return '';
  }
}
async function setBio(elId,rk,library=''){
  const el=document.getElementById(elId);
  const wrap=document.getElementById(`${elId}-wrap`);
  const toggle=document.getElementById(`${elId}-toggle`);
  if(!el) return;
  if(!rk){
    _setHidden(el, true);
    _setHidden(wrap, true);
    _setHidden(toggle, true);
    return;
  }
  const bio=await fetchBio(rk, library);
  if(bio){
    el.textContent=bio;
    el.classList.add('is-clamped');
    el.dataset.expanded='false';
    _setHidden(el, false);
    _setHidden(wrap, false, 'flex');
    setTimeout(()=>{
      const needsToggle=(el.scrollHeight - el.clientHeight) > 2;
      if(toggle){
        _setHidden(toggle, !needsToggle, needsToggle?'inline-flex':'');
        toggle.textContent='More';
        toggle.setAttribute('aria-expanded','false');
      }
    }, 60);
  }else{
    _setHidden(el, true);
    _setHidden(wrap, true);
    _setHidden(toggle, true);
  }
}

function toggleBio(elId){
  const el=document.getElementById(elId);
  const toggle=document.getElementById(`${elId}-toggle`);
  if(!el) return;
  const expanded=el.dataset.expanded==='true';
  el.dataset.expanded=expanded?'false':'true';
  el.classList.toggle('is-clamped', expanded);
  if(toggle){
    toggle.textContent=expanded?'More':'Less';
    toggle.setAttribute('aria-expanded', expanded?'false':'true');
  }
}

// ── Media page ───────────────────────────────────────────────────────────────
let _mediaRows=[],_mediaFiltered=[],_mediaPage=0,_mediaSortCol='title',_mediaSortDir=1;
let _deleteKey=null,_deleteLib='',_deleteFolder='';

function openDeleteModal(rk,title,lib,folder=''){
  _deleteKey=rk;
  _deleteLib=lib||_activeLib||'';
  _deleteFolder=folder||'';
  document.getElementById('delete-modal-name').textContent=title;
  const pathEl=document.getElementById('delete-modal-path');
  if(pathEl) pathEl.textContent=folder||'(folder unknown)';
  openModal('delete-modal');
}
function closeDeleteModal(){
  closeModal('delete-modal');
  _deleteKey=null; _deleteLib=''; _deleteFolder='';
  runModalMediaCleanup();
}
async function confirmDelete(){
  if(!_deleteKey && !_deleteFolder){ closeDeleteModal(); return; }
  // Capture values BEFORE closeDeleteModal() zeroes them out
  const key    = _deleteKey    || '';
  const folder = _deleteFolder || '';
  const lib    = requireLibraryContext(_deleteLib || _activeLib,'delete a theme');
  if(!lib) return;
  closeDeleteModal();
  toast('Deleting…','info');
  const r=await fetch('/api/theme/delete',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({rating_key:key, library:lib, folder:folder})
  });
  const data=await r.json();
  if(data.ok){
    const how=data.matched_by?` (${data.matched_by})`:'';
    toast((data.message||'Deleted')+how,'ok');
    stopCurrentAudio();
    loadDatabase();
  }
  else toast(data.error||'Failed','err');
}


// === MODAL HANDLING ===
// ── Golden Source Modal ───────────────────────────────────────────────────────
function openGoldenSourceModal(lib){
  document.getElementById('gs-modal-lib-label').textContent = `Library: ${lib}`;
  document.getElementById('gs-overwrite').checked = false;
  document.getElementById('gs-approve').checked = false;
  const res = document.getElementById('gs-result');
  res.style.display = 'none'; res.textContent = '';
  document.getElementById('gs-confirm-btn').disabled = false;
  document.getElementById('gs-confirm-btn').textContent = '★ Import Now';
  openModal('gs-modal');
}
function closeGoldenSourceModal(){
  closeModal('gs-modal');
}
async function confirmGoldenSourceImport(){
  if(!_activeLib) return;
  const overwrite = document.getElementById('gs-overwrite').checked;
  const autoApprove = document.getElementById('gs-approve').checked;
  const btn = document.getElementById('gs-confirm-btn');
  const dbBtn = document.getElementById('db-btn-golden');
  btn.disabled = true; btn.textContent = 'Importing…';
  if(dbBtn){ dbBtn.disabled = true; dbBtn.textContent = 'Importing…'; }
  try{
    const r = await fetch('/api/golden-source/import',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({library:_activeLib, overwrite_existing:overwrite, auto_approve:autoApprove})
    });
    const data = await r.json().catch(()=>({ok:false,error:'Invalid response'}));
    const res = document.getElementById('gs-result');
    if(!r.ok || !data.ok){
      res.style.cssText = 'display:block;padding:10px 12px;border-radius:8px;font-size:12px;font-family:var(--mono);margin-bottom:12px;background:var(--red-soft);color:var(--red);border:1px solid var(--red)';
      res.textContent = '✗ ' + (data.error || 'Import failed');
      btn.disabled = false; btn.textContent = '★ Retry';
      return;
    }
    res.style.cssText = 'display:block;padding:10px 12px;border-radius:8px;font-size:12px;font-family:var(--mono);margin-bottom:12px;background:var(--green-soft);color:var(--green);border:1px solid var(--green)';
    res.textContent = `✓ Imported ${data.imported} · Kept ${data.skipped_existing} existing · ${data.no_match} unmatched · ${data.fetch_ms||0}ms fetch`;
    btn.textContent = '✓ Done'; btn.disabled = true;
    toast(`Golden Source: ${data.imported} imported · ${data.skipped_existing} kept · ${data.no_match} unmatched`,'ok');
    loadDatabase();
    setTimeout(()=>closeGoldenSourceModal(), 1800);
  } finally {
    if(dbBtn){ dbBtn.disabled = false; dbBtn.textContent = '★ Import Golden Source'; }
  }
}

// ── 3-Step Search Modal ───────────────────────────────────────────────────────
let _searchKey=null,_searchTitle='',_searchYear='',_searchLib='';
let _searchMethod='playlist',_searchCustomQuery='';
let _searchResults=[];
let _searchCurrentStep=1;
let _autoApproveManual=false;
let _seKey=null,_seLib='';
// Persistent state so Back works
let _lastSearchResults=[];
let _lastSearchResultsKey=null;
let _lastSearchMethod='playlist';
let _previewCache={};
let _lastPreviewedUrl='';
let _step3PreparedUrl='';
let _searchModalStateKey=null;
let _activeQuickPickBtn=null;
let _activeQuickPickUrl='';
let _selectedSourceTitle='';
let _sePreviewLoadSeq=0;
let _step3EntryMode='';
let _searchDefaultMethod='playlist';


// ── SECTION: INIT ──────────────────────────────────────────────────────────────
// Sidebar / UI chrome
function toggleNav(){
  const nav=document.querySelector('nav');
  nav.classList.toggle('collapsed');
  document.body.classList.toggle('nav-collapsed');
  document.getElementById('nav-collapse-btn').textContent=nav.classList.contains('collapsed')?'»':'«';
  try{localStorage.setItem('mt-nav-collapsed',nav.classList.contains('collapsed')?'1':'0');}catch(e){}
}
(function(){try{if(localStorage.getItem('mt-nav-collapsed')==='1'){document.querySelector('nav').classList.add('collapsed');document.body.classList.add('nav-collapsed');setTimeout(()=>{const b=document.getElementById('nav-collapse-btn');if(b)b.textContent='»';},0);}}catch(e){}})();

// ── Theme preview modal ──────────────────────────────────────────────────────
let _themeModalContext={};
function _themeHasLocal(row){
  if(!row) return false;
  return String(row.theme_exists||'')==='1';
}
function _themeModalPrimarySourceUrl(row={}){
  return String(_selectedSourceContract(row).url||'').trim();
}
function _themeModalImportedAt(row={}){
  const raw=String(row?.last_updated||'').trim();
  if(!raw) return 'Not imported yet';
  const iso=raw.replace(' ','T')+'Z';
  const dt=new Date(iso);
  if(Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleString(undefined,{year:'numeric',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
}
function _themeModalSourceOriginLabel(row={}){
  return _selectedSourceLabel(row);
}
function _themeModalSourceState(row={}){
  return _selectedSourceStateText(row);
}
function _themeModalSourceUrl(row={}){
  return String(_selectedSourceContract(row).url||'').trim();
}
function _themeModalSourceOffset(row={}){
  const selected=_selectedSourceContract(row);
  if(!selected.url) return '—';
  return _themeModalOffsetLabel({...row, url:selected.url}, _themeHasLocal(row), selected.kind==='golden' ? 'golden_source' : 'selected_source');
}
function _themeModalSourceAdded(row={}){
  if(!String(_selectedSourceContract(row).url||'').trim()) return '—';
  return _themeModalImportedAt(row);
}

function _renderSourceStateCard(summary, opts={}){
  const compact=opts.compact===true;
  const jsUrl=JSON.stringify(String(summary.url||''));
  const jsCopied=JSON.stringify('Source URL copied');
  const actions=summary.url
    ? `<div class="source-state-actions">
        <button class="btn btn-ghost btn-xs" type="button" onclick='event.stopPropagation();_copyTextValue(${jsUrl},${jsCopied})'>Copy</button>
        <button class="btn btn-ghost btn-xs" type="button" onclick='event.stopPropagation();window.open(${jsUrl},\"_blank\",\"noopener\")'>Open</button>
      </div>`
    : '';
  return `<div class="source-state-card ${summary.url?'':'is-empty'}">
    <div class="source-state-main">
      <div class="source-state-head">
        <span class="source-state-label">${_escapeHtml(summary.label)}</span>
        <span class="ui-pill review-source-pill ${summary.className}">${_escapeHtml(summary.stateLabel)}</span>
      </div>
      ${summary.chips?.length?`<div class="source-state-meta">${summary.chips.map(chip=>`<span class="ui-pill muted-chip">${_escapeHtml(chip)}</span>`).join('')}</div>`:''}
      <div class="source-state-url" title="${_escapeAttr(summary.url||summary.note)}">${_escapeHtml(summary.url||summary.note)}</div>
      ${compact?'':`<div class="source-state-note">${_escapeHtml(`Offset ${summary.offset} · ${summary.timestamp}`)}</div>`}
    </div>
    ${actions}
  </div>`;
}

function _copyTextValue(value='', successMessage='Copied'){
  const text=String(value||'').trim();
  if(!text) return toast('Nothing to copy','info');
  const write=navigator.clipboard?.writeText
    ? navigator.clipboard.writeText(text)
    : Promise.reject(new Error('clipboard unavailable'));
  return write.then(()=>toast(successMessage,'ok')).catch(()=>{
    const tmp=document.createElement('textarea');
    tmp.value=text;
    tmp.setAttribute('readonly','readonly');
    tmp.style.position='absolute';
    tmp.style.left='-9999px';
    document.body.appendChild(tmp);
    tmp.select();
    document.execCommand('copy');
    document.body.removeChild(tmp);
    toast(successMessage,'ok');
  });
}

function _renderSourceStateStack(targetId,row={},opts={}){
  const el=document.getElementById(targetId);
  if(!el) return;
  const draft=opts.draft||{};
  const previewRow={...row};
  if(Object.prototype.hasOwnProperty.call(draft,'selectedUrl')) previewRow.url=draft.selectedUrl;
  const selected=_selectedSourceContract(previewRow);
  const local=_localSourceContract(row);
  const summaries=[
    {
      label:'Curated Source',
      stateLabel:_rowHasGoldenSource(row) ? 'Available' : 'None',
      className:_rowHasGoldenSource(row) ? 'is-golden' : 'is-unknown',
      chips:_rowHasGoldenSource(row) ? ['Curated'] : [],
      url:String(row?.golden_source_url||'').trim(),
      offset:_rowHasGoldenSource(row) ? _themeModalOffsetLabel(row, _themeHasLocal(row), 'golden_source') : '—',
      timestamp:_themeModalImportedAt(row),
      note:_rowHasGoldenSource(row) ? 'Curated source available' : 'No curated source saved',
    },
    {
      label:'Saved Source',
      stateLabel:selected.url ? _selectedSourceStateText(previewRow) : 'None',
      className:selected.url ? _sourceKindClass(selected.kind) : 'is-unknown',
      chips:selected.url ? [_selectedSourceLabel(previewRow)] : [],
      url:selected.url,
      offset:selected.url ? _themeModalOffsetLabel(previewRow, _themeHasLocal(row), selected.kind==='golden' ? 'golden_source' : 'selected_source') : '—',
      timestamp:selected.url ? _themeModalImportedAt(row) : '—',
      note:selected.url ? 'Saved for approval or download' : 'No selected source saved',
    },
    {
      label:'Local Theme',
      stateLabel:_themeHasLocal(row) ? 'On disk' : 'Missing',
      className:_themeHasLocal(row) ? _sourceKindClass(local.kind) : 'is-unknown',
      chips:_themeHasLocal(row) && local.url ? [_sourceKindLabel(local.kind), _sourceMethodLabel(local.method)] : [],
      url:local.url,
      offset:_themeHasLocal(row) ? _themeModalOffsetLabel(row, true, 'local_theme') : '—',
      timestamp:String(row?.local_source_recorded_at||'').trim() || '—',
      note:_themeHasLocal(row) ? 'Local theme file found' : 'No local theme file found',
    }
  ];
  el.innerHTML=summaries.map(layer=>_renderSourceStateCard(layer,{compact:opts.compact===true})).join('');
}
function _clipWindowMeta(duration=0, offset=0, maxDur=0){
  const total=Math.max(0, Number(duration)||0);
  const rawOffset=Math.max(0, parseTrim(offset||0));
  const start=total>0?Math.min(rawOffset,total):rawOffset;
  const cap=Math.max(0, Number(maxDur)||0);
  const end=total>0?(cap>0?Math.min(total,start+cap):total):0;
  const length=total>0?Math.max(0,end-start):0;
  const exceeds=total>0 && rawOffset>total;
  const zero=total>0 && length<=0.01;
  const short=total>0 && !zero && length<3;
  return {total,rawOffset,start,end,length,exceeds,zero,short};
}
function _clipLengthOffsetLabel(duration=0, offset=0, maxDur=0){
  const meta=_clipWindowMeta(duration, offset, maxDur);
  return `Length ${meta.total>0?fmt(meta.length):'—'} · Offset ${fmt(meta.rawOffset)}`;
}
function _clipWindowRangeLabel(duration=0, offset=0, maxDur=0, scopeLabel='preview'){
  const meta=_clipWindowMeta(duration, offset, maxDur);
  if(meta.total<=0) return `Load a ${scopeLabel} to confirm the kept portion.`;
  return `Keeps ${fmt(meta.start)} → ${fmt(meta.end)} of ${fmt(meta.total)} ${scopeLabel}`;
}
function _clipWarningText(duration=0, offset=0, maxDur=0, scopeLabel='preview'){
  const meta=_clipWindowMeta(duration, offset, maxDur);
  if(meta.total<=0) return '';
  if(meta.exceeds) return `Offset ${fmt(meta.rawOffset)} exceeds ${scopeLabel} duration ${fmt(meta.total)}. Reduce the offset to keep audio.`;
  if(meta.zero) return `Zero-length result — offset ${fmt(meta.rawOffset)} reaches the end of the ${scopeLabel}.`;
  if(meta.short) return `Very short result — only ${fmt(meta.length)} will be kept.`;
  return '';
}
function _setClipSummary(summaryId, mainId, subId, warningId, duration=0, offset=0, maxDur=0, scopeLabel='preview'){
  const summary=document.getElementById(summaryId);
  const main=document.getElementById(mainId);
  const sub=document.getElementById(subId);
  const warning=document.getElementById(warningId);
  const meta=_clipWindowMeta(duration, offset, maxDur);
  if(main) main.textContent=_clipLengthOffsetLabel(duration, offset, maxDur);
  if(sub) sub.textContent=_clipWindowRangeLabel(duration, offset, maxDur, scopeLabel);
  if(warning) warning.textContent=_clipWarningText(duration, offset, maxDur, scopeLabel);
  if(summary){
    summary.classList.toggle('is-short', !!meta.short && !meta.exceeds && !meta.zero);
    summary.classList.toggle('is-zero', !!meta.zero && !meta.exceeds);
    summary.classList.toggle('is-warning', !!meta.exceeds);
  }
}
function _themeModalOffsetValue(row={}, layer='selected_source'){
  if(layer==='golden_source') return row?.golden_source_offset||0;
  if(layer==='local_theme') return row?.local_source_offset ?? row?.start_offset ?? 0;
  return row?.start_offset||0;
}
function _themeModalOffsetLabel(row={}, hasTheme=false, layer='selected_source'){
  const duration=hasTheme && Number(row?.theme_duration||0)>0?parseFloat(row.theme_duration||0):0;
  return _clipLengthOffsetLabel(duration, _themeModalOffsetValue(row, layer), 0);
}
function _themeModalNextAction(row={}, hasTheme=false, hasStoredSource=false){
  const status=String(row?.status||'').toUpperCase();
  if(hasTheme) return null;
  if(hasStoredSource){
    if(status==='STAGED') return {label:'Approve Theme',className:'btn btn-amber',handler:'approve'};
    return {label:'Download Theme',className:'btn btn-green',handler:'download'};
  }
  return {label:'Find Theme',className:'btn btn-amber',handler:'find'};
}
const THEME_MODAL_STATUS_HELPERS={
  MISSING:{
    default:'No selected source is saved and no local theme file was found.',
    sourceOnly:'A selected source is saved, but the local theme file is still missing.'
  },
  STAGED:{
    default:'A selected source is waiting for approval, and the local theme file is still missing.',
    local:'A selected source is saved and the local theme file is already on disk.'
  },
  APPROVED:{
    default:'A selected source is approved and ready to download because the local theme file is missing.',
    local:'A selected source is approved and the local theme file is already on disk.'
  },
  AVAILABLE:{
    default:'This item is marked Available, but the local theme file is missing.',
    local:'The local theme file is on disk and ready for playback.'
  },
  FAILED:{
    default:'Source or download needs attention because the local theme file is still missing.',
    local:'The last update failed, but an older local theme file is still on disk.'
  }
};
function _themeModalUpdateStatusFlow(status='MISSING', state={}){
  const normalized=String(status||'MISSING').toUpperCase();
  const helper=document.getElementById('theme-modal-status-helper');
  const helperSet=THEME_MODAL_STATUS_HELPERS[normalized];
  let message='Review the current theme state.';
  if(typeof helperSet==='string') message=helperSet;
  else if(helperSet){
    if(state?.hasTheme && helperSet.local) message=helperSet.local;
    else if(state?.hasStoredSource && !state?.hasTheme && helperSet.sourceOnly) message=helperSet.sourceOnly;
    else if(helperSet.default) message=helperSet.default;
  }
  if(helper) helper.textContent=message;
}
function openThemeModal(rk,title,year,folder,row={},library=''){
  stopAllAudio();
  const resolvedLibrary=String(library||row?.library||'').trim()||_activeLib||_currentLib();
  _themeModalContext={rk,title,year,folder,row,library:resolvedLibrary};
  const hasTheme=_themeHasLocal(row);
  const status=String(row?.status||'MISSING').toUpperCase();
  const hasStoredSource=!!String(_selectedSourceContract(row).url||'').trim();
  const hasLocalTheme=hasTheme;
  const isDownloadable=!hasLocalTheme && hasStoredSource && status==='APPROVED';
  const isSourceOnly=hasStoredSource && !hasLocalTheme;
  const themeFilename=(document.getElementById('cfg-theme_filename')?.value||'theme.mp3').trim()||'theme.mp3';
  const themeFile=folder?`${folder}/${themeFilename}`:'Unknown folder';

  document.getElementById('theme-modal-title').textContent=title;
  document.getElementById('theme-modal-year').textContent=year;
  document.getElementById('theme-modal-dur-meta').textContent=hasLocalTheme
    ?'Theme on disk'
    :(isSourceOnly?'Source saved only':'Theme missing');
  const statusBadge=document.getElementById('theme-modal-status-badge');
  if(statusBadge){
    statusBadge.className=`badge s-${status}`;
    statusBadge.innerHTML=`<span class="si"></span>${displayStatus(status)}`;
  }
  _themeModalUpdateStatusFlow(status, {hasTheme:hasLocalTheme, hasStoredSource, isDownloadable});
  document.getElementById('theme-modal-local-status').textContent=hasLocalTheme
    ?'Available locally'
    :(isSourceOnly?'Local file missing':'No local theme file found');
  document.getElementById('theme-modal-file').textContent=`File path: ${themeFile}`;
  document.getElementById('theme-modal-local-dur').textContent=hasLocalTheme
    ? _themeModalOffsetLabel(row, true, 'local_theme')
    : 'Duration: —';
  const sourceOriginLabel=_themeModalSourceOriginLabel(row);
  const sourceStateLabel=_themeModalSourceState(row);
  const sourceUrl=_themeModalSourceUrl(row);
  const sourceOffset=_themeModalSourceOffset(row);
  const sourceAdded=_themeModalSourceAdded(row);
  document.getElementById('theme-modal-source-summary').textContent=hasStoredSource
    ? sourceStateLabel
    : 'No selected source is saved yet.';
  document.getElementById('theme-modal-source-origin').textContent=sourceOriginLabel;
  document.getElementById('theme-modal-source-url').textContent=sourceUrl || '—';
  document.getElementById('theme-modal-source-offset').textContent=sourceOffset;
  document.getElementById('theme-modal-source-added').textContent=sourceAdded;
  const sourceCopyBtn=document.getElementById('theme-modal-source-copy');
  const sourceOpenBtn=document.getElementById('theme-modal-source-open');
  if(sourceCopyBtn){
    sourceCopyBtn.disabled=!sourceUrl;
    sourceCopyBtn.onclick=sourceUrl?themeModalCopySource:null;
  }
  if(sourceOpenBtn){
    sourceOpenBtn.disabled=!sourceUrl;
    sourceOpenBtn.onclick=sourceUrl?themeModalOpenSource:null;
  }

  const sourceEmpty=document.getElementById('theme-source-empty');
  const sourceMetaList=document.getElementById('theme-source-meta-list');
  _setHidden(sourceEmpty, hasStoredSource, hasStoredSource?'':'block');
  if(sourceMetaList) sourceMetaList.style.display=hasStoredSource?'flex':'none';

  const localCard=document.getElementById('theme-local-card');
  const localMeta=document.getElementById('theme-local-meta');
  const localTrimBtn=document.getElementById('theme-modal-trim-btn');
  const localDeleteBtn=document.getElementById('theme-modal-delete-btn');
  if(localCard) localCard.classList.toggle('compact', !hasLocalTheme);
  if(localMeta) localMeta.style.display='block';
  if(localTrimBtn) localTrimBtn.style.display=hasLocalTheme?'':'none';
  if(localDeleteBtn) localDeleteBtn.style.display=hasLocalTheme?'':'none';
  const localInlinePlay=document.getElementById('theme-modal-inline-play');
  _setHidden(localInlinePlay, !hasLocalTheme, hasLocalTheme?'inline-flex':'');
  const sourceCard=document.getElementById('theme-source-card');
  if(sourceCard) sourceCard.classList.toggle('compact', !hasStoredSource);

  const replaceBtn=document.getElementById('theme-replace-btn');
  const findBtn=document.getElementById('theme-find-btn');
  const nextStepBtn=document.getElementById('theme-next-step-btn');
  if(replaceBtn) replaceBtn.style.display=hasLocalTheme?'':'none';
  _setHidden(findBtn, hasLocalTheme || hasStoredSource);
  const nextAction=_themeModalNextAction(row, hasLocalTheme, hasStoredSource);
  if(nextStepBtn){
    if(nextAction && nextAction.handler!=='find'){
      _setHidden(nextStepBtn, false, '');
      nextStepBtn.textContent=nextAction.label;
      nextStepBtn.className=nextAction.className;
      nextStepBtn.dataset.action=nextAction.handler;
    }else{
      _setHidden(nextStepBtn, true);
      nextStepBtn.dataset.action='';
      nextStepBtn.className='btn btn-ghost';
      nextStepBtn.textContent='Next Step';
    }
  }

  _setHidden(document.getElementById('theme-local-player'), !hasLocalTheme, hasLocalTheme?'block':'');

  document.getElementById('theme-modal-poster').src=apiUrl('/api/poster?key='+rk);
  document.getElementById('theme-modal-poster').style.display='';
  // TMDB link
  const tmdbUrl='https://www.themoviedb.org/search/movie?query='+encodeURIComponent(title+' '+year);
  const tmdbLink=row?.tmdb_id?`https://www.themoviedb.org/movie/${encodeURIComponent(row.tmdb_id)}`:tmdbUrl;
  document.getElementById('theme-modal-links').innerHTML=
    `<a class="modal-link-pill tmdb-pill" href="${tmdbLink}" target="_blank" rel="noopener">TMDB</a>`;
  setBio('theme-modal-bio', rk, resolvedLibrary);
  _themeModalAudio.cleanup({clearSrc:false});
  if(hasLocalTheme) _themeModalAudio.audio.src=apiUrl('/api/theme?folder='+encodeURIComponent(folder));
  else _themeModalAudio.audio.src='';
  openModal('theme-modal');
  _themeModalAudio.setHandlers();
  if(hasLocalTheme) _themeModalAudio.audio.load();
  if(_curKey) stopCurrentAudio();
}
function themeModalRunNextStep(){
  const btn=document.getElementById('theme-next-step-btn');
  const action=String(btn?.dataset?.action||'').trim();
  const rk=_themeModalContext?.rk;
  if(action==='approve' && rk){
    updateRow(rk,'status','APPROVED');
    closeThemeModal();
    return;
  }
  if(action==='download'){
    closeThemeModal();
    themeModalDownloadApproved();
    return;
  }
  if(action==='find'){
    themeModalOpenManualSearch();
  }
}
async function themeModalDownloadApproved(){
  const library=requireLibraryContext(_themeModalContext?.library||_activeLib,'download a theme');
  if(!library) return;
  const row=_themeModalContext?.row||{};
  if(!String(_selectedSourceContract(row).url||'').trim()) return toast('No selected source URL on this item','info');
  const {ok,data}=await postJson('/api/theme/download-now',{library,rating_key:_themeModalContext?.rk||'',folder:_themeModalContext?.folder||'',tmdb_id:_themeModalContext?.row?.tmdb_id||''});
  if(!ok){
    toast(data.error||'Download failed','err');
    return;
  }
  toast('Download started','ok');
  closeThemeModal();
}

function themeModalOpenSource(){
  const url=_themeModalPrimarySourceUrl(_themeModalContext?.row||{});
  if(!url) return toast('No source URL on this row','info');
  window.open(url,'_blank');
}
async function themeModalCopySource(){
  const url=_themeModalPrimarySourceUrl(_themeModalContext?.row||{});
  if(!url) return toast('No source URL on this row','info');
  try{
    if(navigator.clipboard?.writeText){
      await navigator.clipboard.writeText(url);
    }else{
      const tmp=document.createElement('textarea');
      tmp.value=url;
      tmp.setAttribute('readonly','readonly');
      tmp.style.position='absolute';
      tmp.style.left='-9999px';
      document.body.appendChild(tmp);
      tmp.select();
      document.execCommand('copy');
      document.body.removeChild(tmp);
    }
    toast('Source URL copied','ok');
  }catch{
    toast('Could not copy source URL','err');
  }
}
function themeModalSetStatus(status){
  const key=_themeModalContext?.rk;
  if(!key) return;
  updateRow(key,'status',status);
  closeThemeModal();
}
function themeModalDelete(){
  const c=_themeModalContext||{};
  if(!c.rk) return;
  closeThemeModal();
  openDeleteModal(c.rk,c.title||'',c.library||_activeLib||'',c.folder||'');
}
function themeModalEditTrim(){
  const c=_themeModalContext||{};
  if(!c.rk) return;
  closeThemeModal();
  _trimRk=c.rk;
  _trimLib=String(c.library||'').trim();
  document.getElementById('trim-modal-title').textContent=c.title||'Edit Trim';
  document.getElementById('trim-modal-meta').textContent=[c.year||'',`Duration: ${fmt(parseFloat(c.row?.theme_duration||0)||0)}`].filter(Boolean).join(' · ');
  document.getElementById('trim-modal-poster').src=apiUrl('/api/poster?key='+encodeURIComponent(c.rk));
  document.getElementById('trim-modal-links').innerHTML=`<a class="modal-link-pill tmdb-pill" href="${_tmdbLink(c.title||'',c.year||'')}" target="_blank" rel="noopener">TMDB</a>`;
  document.getElementById('trim-modal-info').textContent='Trim local theme start offset and preview the resulting clip.';
  setBio('trim-modal-bio', c.rk, _trimLib);
  document.getElementById('trim-modal-offset').value=String(c.row?.start_offset||'0');
  trimModalUpdateResult();
  _trimModalAudio.cleanup({clearSrc:false});
  _trimModalAudio.audio.src=apiUrl('/api/theme?folder='+encodeURIComponent(c.folder||''));
  openModal('trim-modal');
  _trimModalAudio.setHandlers();
  _trimModalAudio.audio.load();
}
async function themeModalSyncFromDisk(){
  if(!_activeLib) return toast('No active library selected','err');
  const {ok,data:d}=await postJson('/api/library/sync-themes',{library:_activeLib});
  if(!ok) return toast(d.error||'Sync failed','err');
  toast(`Synced local theme metadata (${d.updated||0} updated)`,'ok');
  await loadDatabase();
}
function themeModalOpenManualSearch(){
  const rk=_themeModalContext.rk;
  if(!rk) return;
  closeThemeModal();
  openSearchModal(rk,_themeModalContext.title||'',_themeModalContext.year||'',encodeURIComponent(_themeModalContext.library||''));
}
function closeThemeModal(){
  closeModal('theme-modal');
  runModalMediaCleanup(()=>_themeModalAudio.cleanup());
}
function themeModalToggle(){ _themeModalAudio.toggle(); }
function themeModalSkip(s){ _themeModalAudio.skip(s); }
function themeModalSeek(val){ _themeModalAudio.seek(val); }

// ── Column resize ────────────────────────────────────────────────────────────
function initResizableCols(tableSelector){
  document.querySelectorAll(tableSelector+' th').forEach(th=>{
    if(th.querySelector('.col-resize')) return;
    th.classList.add('resizable');
    const handle=document.createElement('div');
    handle.className='col-resize';
    th.appendChild(handle);
    handle.addEventListener('mousedown',function(e){
      e.preventDefault();
      handle.classList.add('dragging');
      const startX=e.pageX, startW=th.offsetWidth;
      function onMove(ev){
        const next=Math.max(72,startW+ev.pageX-startX);
        th.style.width=next+'px';
        th.style.minWidth=next+'px';
      }
      function onUp(){handle.classList.remove('dragging');document.removeEventListener('mousemove',onMove);document.removeEventListener('mouseup',onUp);}
      document.addEventListener('mousemove',onMove);
      document.addEventListener('mouseup',onUp);
    });
    handle.addEventListener('dblclick',function(e){
      e.preventDefault();
      th.style.width='';
      th.style.minWidth='';
    });
  });
}
// Init on first load of each page
let _dbColsInit=false;

// ── Theme toggle ─────────────────────────────────────────────────────────────
function toggleTheme(){
  const html=document.documentElement;
  const isDark=html.getAttribute('data-theme')!=='light';
  html.setAttribute('data-theme',isDark?'light':'dark');
  document.getElementById('theme-toggle-btn').textContent=isDark?'☾':'☀';
  try{localStorage.setItem('mt-theme',isDark?'light':'dark');}catch(e){}
}
(function(){try{const t=localStorage.getItem('mt-theme');if(t==='light'){document.documentElement.setAttribute('data-theme','light');setTimeout(()=>{const b=document.getElementById('theme-toggle-btn');if(b)b.textContent='☾';},0);}}catch(e){}})();

function _searchStateKey(key){
  return key==null ? null : String(key);
}

function _searchStateMatches(key){
  const normalized=_searchStateKey(key);
  return normalized!==null && normalized===_searchModalStateKey;
}

function _setOwnedSearchResults(key, results){
  _lastSearchResults=Array.isArray(results) ? results : [];
  _lastSearchResultsKey=_lastSearchResults.length ? _searchStateKey(key) : null;
}

function _searchPreviewReset(){
  resetActivePreviewBtn();
  const audio=_searchPreviewAudio?.audio;
  if(audio){
    audio.pause();
    audio.src='';
    audio.load();
  }
  const slider=document.getElementById('sm-preview-slider');
  const cur=document.getElementById('sm-preview-cur');
  const dur=document.getElementById('sm-preview-dur');
  if(slider) slider.value=0;
  if(cur) cur.textContent='0:00';
  if(dur) dur.textContent='—';
}

function _hideGoldenValidationError(){
  const panel=document.getElementById('se-golden-error');
  const msg=document.getElementById('se-golden-error-msg');
  if(panel) panel.classList.add('hidden');
  if(msg) msg.textContent='';
}

function _focusSourceEditorUrl(){
  const urlEl=document.getElementById('se-url');
  if(!urlEl) return;
  urlEl.focus();
  urlEl.select();
}

function _returnToSearchMethodChoice(){
  _hideGoldenValidationError();
  goToSearchStep(1);
}

function _renderGoldenValidationError(errorMessage){
  const notice=errorMessage||'Golden Source preview could not be loaded.';
  const infoEl=document.getElementById('se-info');
  const panel=document.getElementById('se-golden-error');
  const msg=document.getElementById('se-golden-error-msg');
  const countEl=document.getElementById('sm-results-count');
  _setOwnedSearchResults(null, []);
  _renderSearchResultsState('Curated source preview failed. Choose another method to search alternatives.', 'error');
  if(countEl) countEl.textContent='';
  if(infoEl) infoEl.textContent='Curated source unavailable — retry preview, paste/edit a replacement URL, or choose another method.';
  if(msg) msg.textContent=notice;
  if(panel) panel.classList.remove('hidden');
}

function _resetSourceEditorDraft(){
  _sePreviewLoadSeq++;
  _step3PreparedUrl='';
  _selectedSourceTitle='';
  const urlEl=document.getElementById('se-url');
  const offsetEl=document.getElementById('se-offset');
  const loadBtn=document.getElementById('se-load-btn');
  if(urlEl) urlEl.value='';
  if(offsetEl) offsetEl.value='0:00';
  _renderSelectedSourceSummary('','');
  if(loadBtn){ loadBtn.textContent='↺ Refresh'; loadBtn.disabled=false; }
  _sourceEditorAudio.cleanup();
  _step3EntryMode='';
  _hideGoldenValidationError();
  const trimWindow=document.getElementById('se-trim-window');
  const startLabel=document.getElementById('se-trim-start-label');
  const endLabel=document.getElementById('se-trim-end-label');
  if(trimWindow) trimWindow.style.display='none';
  if(startLabel) startLabel.textContent='Start 0:00';
  if(endLabel) endLabel.textContent='End —';
  const infoEl=document.getElementById('se-info');
  if(infoEl) infoEl.textContent='Select a result or paste a URL to preview';
}

function _resetSearchModalState(){
  _setOwnedSearchResults(null, []);
  const resultsEl=document.getElementById('search-results');
  const countEl=document.getElementById('sm-results-count');
  const pasteEl=document.getElementById('search-paste-url');
  if(resultsEl) resultsEl.innerHTML='';
  if(countEl) countEl.textContent='';
  if(pasteEl) pasteEl.value='';
  _searchPreviewReset();
  _resetSourceEditorDraft();
}

function _normalizedOffsetValue(val){
  return fmt(parseTrim(val||'0'));
}

function _goldenSourceMeta(row){
  const sourceRow=row||_rowMap[_searchKey]||_rows.find(r=>String(r.rating_key)===String(_searchKey))||{};
  return {
    url:String(sourceRow?.golden_source_url||'').trim(),
    offset:_normalizedOffsetValue(sourceRow?.golden_source_offset||'0')
  };
}

function _searchMethodCardId(method){
  return method==='golden_source' ? 'sm-card-golden' : 'sm-card-'+method;
}

function _setSearchMethodOrder(hasGolden){
  const wrap=document.getElementById('sm-primary-methods');
  const goldenCard=document.getElementById('sm-card-golden');
  if(!wrap || !goldenCard) return;
  if(hasGolden) wrap.prepend(goldenCard);
  else wrap.appendChild(goldenCard);
}

function _applyAudioOffset(audio, offsetValue){
  const start=Math.max(0, parseTrim(offsetValue||'0'));
  if(!audio || !start) return;
  const setOffset=()=>{
    try{ audio.currentTime=Math.min(start, audio.duration||start); }catch(_e){}
  };
  if(audio.readyState >= 1) setOffset();
  else audio.addEventListener('loadedmetadata', setOffset, {once:true});
}

function _setSearchPreviewPlayIcon(isPlaying){
  const pbtn=document.getElementById('sm-preview-play');
  if(pbtn) pbtn.innerHTML=isPlaying?PAUSE_ICON_SVG:PLAY_ICON_SVG;
}

function _bindSearchPreviewAudio(){
  const audio=_audioEl();
  return {
    audio,
    setHandlers(){
      audio.onloadedmetadata=()=>{
        const d=document.getElementById('sm-preview-dur');
        if(d) d.textContent=audio.duration?fmt(audio.duration):'—';
      };
      audio.ontimeupdate=()=>{
        const dur=audio.duration||0;
        const cur=audio.currentTime||0;
        const s=document.getElementById('sm-preview-slider');
        const c=document.getElementById('sm-preview-cur');
        const d=document.getElementById('sm-preview-dur');
        if(d) d.textContent=dur?fmt(dur):'—';
        if(c) c.textContent=fmt(cur);
        if(s && dur) s.value=(cur/dur*100);
      };
      audio.onended=()=>{ resetActivePreviewBtn(); };
      audio.onerror=()=>{ _setSearchPreviewPlayIcon(false); };
    },
    playFrom(src){
      stopAllAudio('global-audio');
      audio.src=src;
      this.setHandlers();
      return audio.play().then(()=>_setSearchPreviewPlayIcon(true));
    },
    skip(seconds){
      if(!audio.duration) return;
      audio.currentTime=Math.max(0, Math.min(audio.duration, (audio.currentTime||0)+(Number(seconds)||0)));
    },
    toggle(){
      if(!audio.src || audio.src===window.location.href) return;
      if(audio.paused){
        stopAllAudio('global-audio');
        audio.play().then(()=>_setSearchPreviewPlayIcon(true)).catch(()=>{});
      }else{
        audio.pause();
        _setSearchPreviewPlayIcon(false);
      }
    },
    seek(val){ if(audio.duration) audio.currentTime=(Number(val)||0)/100*audio.duration; },
    pauseAndReset(){
      audio.pause();
      _setSearchPreviewPlayIcon(false);
    }
  };
}

const _searchPreviewAudio=_bindSearchPreviewAudio();

function _sourceTitleFromUrl(url){
  try{
    const u=new URL(url);
    const id=u.searchParams.get('v')||u.searchParams.get('list')||u.pathname.split('/').filter(Boolean).pop()||u.hostname;
    return `${u.hostname} · ${id}`;
  }catch(_e){ return url||'—'; }
}

function buildSearchQuery(method,title,year){
  const base=((title||'')+' '+(year||'')).trim();
  if(method==='playlist') return (base+' soundtrack playlist').trim();
  if(method==='direct') return (base+' theme song').trim();
  if(method==='golden_source') return '';
  if(method==='paste') return '';
  return _searchCustomQuery || base;
}

function _updateQueryDisplay(){
  const inp = document.getElementById('sm-custom-input');
  if(inp) inp.value=_searchCustomQuery||buildSearchQuery('custom',_searchTitle,_searchYear);
}

function _searchMethodMeta(method){
  const normalized=String(method||'').trim()||'unknown';
  const map={
    golden_source:{label:'Golden Source', className:'is-golden'},
    playlist:{label:'Playlist', className:'is-playlist'},
    direct:{label:'Direct', className:'is-direct'},
    custom:{label:'Custom', className:'is-custom'},
    paste:{label:'Paste URL', className:'is-custom'},
    unknown:{label:'Unknown', className:'is-unknown'}
  };
  return map[normalized] || map.unknown;
}

function _renderSearchMethodChosen(method){
  const el=document.getElementById('sm-results-method');
  if(!el) return;
  const meta=_searchMethodMeta(method);
  el.textContent=meta.label;
  el.className=`ui-pill review-source-pill ${meta.className}`;
}

function selectSearchMethod(method){
  const goldenCard=document.getElementById('sm-card-golden');
  if(method==='golden_source' && goldenCard?.classList.contains('disabled')) return;
  _searchMethod=method;
  if(method==='golden_source'){
    _setOwnedSearchResults(null, []);
    _renderSearchResultsState('Golden Source stays in curated review unless you choose another method to search alternatives.');
    const countEl=document.getElementById('sm-results-count');
    if(countEl) countEl.textContent='';
  }
  ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
    const card=document.getElementById(_searchMethodCardId(m));
    if(card) card.classList.toggle('active',m===method);
  });
  _updateQueryDisplay();
  _renderSearchMethodChosen(method);
  _setSearchFooter(1);
}

function pickSearchMethod(method, event){
  if(event) event.stopPropagation();
  selectSearchMethod(method);
  if(method==='custom') document.getElementById('sm-custom-input')?.focus();
  if(method==='paste') document.getElementById('search-paste-url')?.focus();
}

function _srailClick(step){
  if(step===1) goToSearchStep(1);
  else if(step===2 && _lastSearchResults.length) goToSearchStep(2);
  else if(step===3 && document.getElementById('se-url')?.value?.trim()) goToSearchStep(3);
}

function _setStepRail(step){
  [1,2,3].forEach(n=>{
    const el=document.getElementById('srail-'+n);
    if(!el) return;
    el.classList.remove('active','done');
    if(n<step) el.classList.add('done');
    else if(n===step) el.classList.add('active');
  });
}

function _setSearchFooter(step){
  const back = document.getElementById('search-modal-back');
  const primary = document.getElementById('search-modal-primary');
  const dlnow = document.getElementById('search-modal-download-now');
  if(!back || !primary || !dlnow) return;
  if(step===1){
    back.style.display='none';
    primary.style.display='';
    primary.textContent='🔍 Search';
    primary.className='btn btn-amber';
    primary.onclick=doSearch;
    _setHidden(dlnow, true);
  } else if(step===2){
    back.style.display='';
    primary.style.display='none';
    _setHidden(dlnow, true);
  } else if(step===3){
    back.style.display='';
    primary.style.display='';
    primary.textContent='Approve';
    primary.className='btn btn-amber';
    primary.onclick=approveSourceEditor;
    _setHidden(dlnow, false, '');
  }
}

function goToSearchStep(step){
  _searchCurrentStep=step;
  [1,2,3].forEach(n=>{
    const el=document.getElementById('search-step-'+n);
    if(el) el.style.display=(n===step)?'':'none';
  });
  _setStepRail(step);
  _setSearchFooter(step);
}

async function openSearchModal(rk,title,year,lib){
  stopAllAudio();
  const sameItem=_searchStateMatches(rk);
  if(!sameItem) _resetSearchModalState();
  _searchKey=rk; _searchTitle=title; _searchYear=year;
  _searchLib=lib?decodeURIComponent(lib):'';
  _seKey=rk; _seLib=_searchLib||_activeLib||'';
  _searchModalStateKey=_searchStateKey(rk);
  const existingRow=_rowMap[rk]||_rows.find(r=>String(r.rating_key)===String(rk));
  const goldenUrl=String(existingRow?.golden_source_url||'').trim();
  const hasGolden=!!goldenUrl;
  const cfg=await(await fetch('/api/config')).json();
  _autoApproveManual = !!cfg.auto_approve_manual;
  const defaultMethod=(cfg.search_mode||'playlist')==='playlist'?'playlist':'direct';
  _searchDefaultMethod=defaultMethod;
  if(!sameItem) _searchMethod=hasGolden?'golden_source':(_lastSearchMethod||defaultMethod);
  if(!_searchCustomQuery) _searchCustomQuery=buildSearchQuery('direct',title,year);
  // Header
  document.getElementById('search-modal-title').textContent=title||'Find Theme Source';
  document.getElementById('search-modal-year-meta').textContent=year||'';
  const posterEl=document.getElementById('search-modal-poster');
  if(title){
    posterEl.src=apiUrl('/api/poster/tmdb?title='+encodeURIComponent(title)+'&year='+encodeURIComponent(year||'')+'&size=w342');
    _setHidden(posterEl, false, '');
  }
  const tmdbUrl='https://www.themoviedb.org/search/movie?query='+encodeURIComponent(title+' '+year);
  document.getElementById('search-modal-links').innerHTML=`<a class="modal-link-pill tmdb-pill" href="${tmdbUrl}" target="_blank" rel="noopener">🎬 TMDB</a>`;
  setBio('search-modal-bio',rk,_searchLib);
  // Restore or reset method cards
  ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
    const card=document.getElementById(_searchMethodCardId(m));
    if(card) card.classList.toggle('active',m===_searchMethod);
  });
  _renderSearchMethodChosen(_searchMethod);
  _updateQueryDisplay();
  const existingUrl=(existingRow?.url||'').trim();
  const existingOffset=fmt(parseTrim(existingRow?.start_offset||'0:00'));
  const urlEl=document.getElementById('se-url');
  const offsetEl=document.getElementById('se-offset');
  const pasteEl=document.getElementById('search-paste-url');
  const infoEl=document.getElementById('se-info');
  const hasSameItemDraft=sameItem && !!((urlEl?.value||'').trim() || _step3PreparedUrl || (pasteEl?.value||'').trim());
  if(!hasSameItemDraft){
    if(pasteEl) pasteEl.value='';
    if(urlEl) urlEl.value=existingUrl;
    if(offsetEl) offsetEl.value=existingOffset;
    _step3PreparedUrl=existingUrl||'';
    _selectedSourceTitle=_sourceTitleFromUrl(existingUrl)||'';
    _renderSelectedSourceSummary(existingUrl,_selectedSourceTitle);
    if(infoEl) infoEl.textContent=existingUrl?'Existing source loaded — click Refresh to preview':'Select a result or paste a URL to preview';
  } else {
    _step3PreparedUrl=(urlEl?.value||'').trim()||_step3PreparedUrl||existingUrl||'';
    if((urlEl?.value||'').trim()) seUrlChanged();
  }
  const goldenCard=document.getElementById('sm-card-golden');
  const goldenDesc=document.getElementById('sm-golden-desc');
  const goldenOffset=document.getElementById('sm-golden-offset');
  const goldenMeta=document.getElementById('sm-golden-meta');
  const goldenOffsetValue=_normalizedOffsetValue(existingRow?.golden_source_offset||'0');
  if(goldenCard) goldenCard.classList.toggle('disabled',!hasGolden);
  if(goldenCard) goldenCard.classList.toggle('recommended',hasGolden);
  if(goldenDesc) goldenDesc.textContent=hasGolden?'Use the curated source already linked to this item. If it fails, stay here to retry or replace the URL.':'No curated source is available for this item yet — choose another method to search alternatives.';
  if(goldenOffset) goldenOffset.textContent=hasGolden?`Offset ${goldenOffsetValue}`:'Offset —';
  if(goldenMeta) goldenMeta.textContent=hasGolden?`Curated match • starts at ${goldenOffsetValue}`:'Choose another method if no curated source is available';
  _setSearchMethodOrder(hasGolden);
  _initMethodQuickPicks(existingRow);
  if(_searchMethod==='golden_source' && !hasGolden){
    _searchMethod='playlist';
    ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
      const card=document.getElementById(_searchMethodCardId(m));
      if(card) card.classList.toggle('active',m===_searchMethod);
    });
    _renderSearchMethodChosen(_searchMethod);
  }
  const canRestoreSearchResults=_searchMethod!=='golden_source';
  const hasOwnedResults=canRestoreSearchResults && _lastSearchResults.length && _lastSearchResultsKey===_searchStateKey(rk);
  const shouldRestoreStep3=sameItem && _searchCurrentStep===3 && hasSameItemDraft;
  if(hasOwnedResults) _renderResults(_lastSearchResults);
  else if(_searchMethod==='golden_source'){
    _setOwnedSearchResults(null, []);
    _renderSearchResultsState('Golden Source stays in curated review unless you choose another method to search alternatives.');
  }
  // Decide starting step
  if(shouldRestoreStep3){
    goToSearchStep(3);
  } else if(hasOwnedResults){
    goToSearchStep(2);
  } else {
    _setOwnedSearchResults(null, []);
    goToSearchStep(1);
  }
  openModal('search-modal');
}

function closeSearchModal(){
  closeModal('search-modal');
  runModalMediaCleanup(); _searchLib='';
}

function searchModalBack(){
  if(_searchCurrentStep===3){
    if(_searchMethod==='golden_source') goToSearchStep(1);
    else goToSearchStep(2);
  }
  else if(_searchCurrentStep===2) goToSearchStep(1);
  else closeSearchModal();
}

function searchModalPrimary(){
  if(_searchCurrentStep===1) doSearch();
  else if(_searchCurrentStep===3) approveSourceEditor();
}

async function _searchByMethod(method, showStep=true){
  const q=buildSearchQuery(method,_searchTitle,_searchYear);
  if(!q){ toast('No query','info'); return null; }
  _lastSearchMethod=method;
  if(showStep){
    stopAllAudio(); resetActivePreviewBtn();
    goToSearchStep(2);
    document.getElementById('search-results').innerHTML='<div class="search-results-state">Searching…</div>';
  }
  const r=await fetch('/api/youtube/search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({query:q})});
  const data=await r.json();
  if(!data.ok){
    if(showStep){
      document.getElementById('search-results').innerHTML=`<div class="search-results-state error">${data.error}</div>`;
      _setSearchFooter(2);
    }
    return null;
  }
  _setOwnedSearchResults(_searchKey, data.results||[]);
  if(showStep){
    _renderResults(_lastSearchResults);
    _setSearchFooter(2);
  }
  return _lastSearchResults;
}

async function doSearch(){
  _searchMethod=_searchMethod||'playlist';
  if(_searchMethod==='paste') return goToStep3FromPaste();
  if(_searchMethod==='golden_source'){
    const golden=_goldenSourceMeta();
    const url=golden.url;
    if(!url) return toast('Golden Source URL not currently available for this item','info');
    _setOwnedSearchResults(null, []);
    _renderSearchResultsState('Golden Source stays in curated review unless you choose another method to search alternatives.');
    return goToStep3(url,{skipPreview:false,sourceTitle:'Golden Source URL',startOffset:golden.offset,entryMode:'golden_fast_path'});
  }
  await _searchByMethod(_searchMethod, true);
}


function _renderSearchResultsState(message, tone='info'){
  const el=document.getElementById('search-results');
  const countEl=document.getElementById('sm-results-count');
  if(!el) return;
  if(countEl) countEl.textContent='';
  el.innerHTML=`<div class="search-results-state ${tone==='error'?'error':''}">${message}</div>`;
}

async function _fallbackFromGoldenValidation(errorMessage){
  _sourceEditorAudio.cleanup();
  _renderGoldenValidationError(errorMessage||'Golden Source preview could not be loaded.');
}

function _renderResults(results){
  const el=document.getElementById('search-results');
  const countEl=document.getElementById('sm-results-count');
  if(countEl) countEl.textContent=results.length ? `${results.length} results` : '';
  if(!results.length){
    el.innerHTML='<div class="search-results-state">No results found.</div>';
    return;
  }
  el.innerHTML=results.map((r,i)=>{
    const safeHref=String(r.url||'').replace(/"/g,'&quot;');
    const safeUrl=String(r.url||'').replace(/'/g,"\\'");
    const safeTitle=String(r.title||'').replace(/</g,'&lt;');
    const safeTitleAttr=safeTitle.replace(/"/g,'&quot;');
    const safeTitleJs=String(r.title||'').replace(/'/g,"\\'").replace(/</g,'&lt;').replace(/"/g,'&quot;');
    return `
    <div class="search-result-card ${i===0?'recommended':''}">
      <div class="result-idx">${i+1}.</div>
      <div class="search-result-main">
        ${i===0?'<span class="search-result-inline-badge">Top</span>':''}
        <a href="${safeHref}" target="_blank" rel="noopener" class="search-result-title" title="${safeTitleAttr}"><span class="search-result-title-text">${safeTitle}</span><span class="search-result-link-icon">↗</span></a>
      </div>
      <div class="search-result-actions">
        <span class="search-result-duration">${r.duration||'—'}</span>
        <button class="btn btn-ghost btn-xs" onclick="previewSearchResult('${safeUrl}',this)">▶ Preview</button>
        <button class="btn btn-amber btn-xs" onclick="goToStep3('${safeUrl}',{skipPreview:false,sourceTitle:'${safeTitleJs}'})">Pick</button>
      </div>
    </div>`;
  }).join('');
  _renderSearchMethodChosen(_searchMethod);
  if(results[0]?.url) setTimeout(()=>previewSearchResult(results[0].url),80);
}



function _renderMethodQuickPick(method, result, opts={}){
  if(!result) return '<span class="sm-quickpick-title">No quick match available</span>';
  const safeMethod=String(method||'').replace(/'/g,"\\'");
  const safeUrl=String(result.url||'').replace(/'/g,"\\'");
  const safeHref=String(result.url||'').replace(/"/g,'&quot;');
  const rawTitle=String(opts.title || result.title || '1st result').trim();
  const rawDisplayTitle=String(opts.displayTitle || rawTitle || result.url || '1st result').trim();
  const safeTitleAttr=rawDisplayTitle.replace(/</g,'&lt;').replace(/"/g,'&quot;');
  const safeTitleJs=rawTitle.replace(/'/g,"\\'").replace(/</g,'&lt;').replace(/"/g,'&quot;');
  const safeOffset=String(result.start_offset||'0').replace(/'/g,"\\'");
  const label=opts.label || 'First match';
  const showOpen=opts.showOpen===true;
  const titleText=opts.scrollTitle===true
    ? rawDisplayTitle
    : _truncateSourceText(rawDisplayTitle, {fallback:'1st result', max:52, middle:!!opts.truncateMiddle});
  const titleMarkup=opts.linkTitle===false
    ? `<span class="sm-quickpick-title" title="${safeTitleAttr}">${titleText}</span>`
    : `<a class="sm-quickpick-link sm-quickpick-title" href="${safeHref}" target="_blank" rel="noopener" title="${safeTitleAttr}">${titleText}<span>↗</span></a>`;
  const offsetMarkup=result.start_offset
    ? `<span class="ui-pill muted-chip" style="margin:4px 0 0">Offset ${_normalizedOffsetValue(result.start_offset)}</span>`
    : '';
  const openMarkup=showOpen
    ? `<a class="btn btn-ghost btn-xs" href="${safeHref}" target="_blank" rel="noopener" onclick="event.stopPropagation()">↗ Open</a>`
    : '';
  const selectLabel=opts.selectLabel || 'Pick';
  return `<span class="sm-quickpick-label">${label}</span>${titleMarkup}${offsetMarkup}<div class="sm-quickpick-buttons"><button class="btn btn-ghost btn-xs" onclick="event.stopPropagation();previewQuickPick('${safeMethod}','${safeUrl}',this)">▶ Preview</button><button class="btn btn-amber btn-xs" onclick="event.stopPropagation();quickPickSelect('${safeMethod}','${safeUrl}','${safeTitleJs}','${safeOffset}')">${selectLabel}</button>${openMarkup}</div>`;
}

function _setMethodQuickPick(method, result){
  const el=document.getElementById('sm-first-'+method);
  if(!el) return;
  if(result===null){ el.innerHTML='<span class="sm-quickpick-loading">Loading quick pick…</span>'; return; }
  const quickPickOpts=method==='golden_source'
    ? {label:'Quick pick', title:'Golden Source URL', displayTitle:(result&&result.url)||'Golden Source URL', showOpen:true, linkTitle:false, selectLabel:'Pick', scrollTitle:true}
    : {label:'Quick pick'};
  el.innerHTML=_renderMethodQuickPick(method, result, quickPickOpts);
}


async function previewQuickPick(method,url,btn){
  if(!url || !btn) return;
  const audio=_searchPreviewAudio.audio;
  if(_activeQuickPickBtn===btn && _activeQuickPickUrl===url && !audio.paused){
    resetActivePreviewBtn();
    return;
  }
  _searchMethod=method||_searchMethod;
  _lastSearchMethod=_searchMethod;
  const startOffset=method==='golden_source' ? _goldenSourceMeta().offset : '0:00';
  btn.disabled=true; btn.textContent='Loading…';
  try{
    const cached=_previewCache[url];
    let data=null;
    if(cached){
      data={ok:true,audio_url:cached.audio_url};
    }else{
      const r=await fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
      data=await r.json();
      if(data?.ok) _previewCache[url]={audio_url:data.audio_url};
    }
    if(!data?.ok){ toast(data?.error||'Preview failed','err'); btn.textContent='▶ Preview'; return; }
    _lastPreviewedUrl=url;
    resetActivePreviewBtn();
    await _searchPreviewAudio.playFrom(apiUrl(data.audio_url)).catch(()=>{});
    _applyAudioOffset(audio, startOffset);
    window._activePreviewBtn=btn;
    _activeQuickPickBtn=btn;
    _activeQuickPickUrl=url;
    btn.textContent='■ Stop';
  }finally{
    btn.disabled=false;
  }
}

async function quickPickSelect(method,url,title='',startOffset='0:00'){
  if(!url) return;
  _searchMethod=method||_searchMethod;
  if(_searchMethod!=='golden_source') await _searchByMethod(_searchMethod,false);
  else {
    _setOwnedSearchResults(null, []);
    _renderSearchResultsState('Golden Source stays in curated review unless you choose another method to search alternatives.');
  }
  if(_searchMethod!=='golden_source' && _lastSearchResults.length) _renderResults(_lastSearchResults);
  goToStep3(url,{skipPreview:false,sourceTitle:title,startOffset,entryMode:method==='golden_source'?'golden_manual_select':'search_result'});
}

async function _prefetchMethodFirstResult(method){
  if(method==='custom') return;
  const q=buildSearchQuery(method,_searchTitle,_searchYear);
  if(!q) return;
  try{
    const r=await fetch('/api/youtube/search',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({query:q})});
    const data=await r.json();
    _setMethodQuickPick(method, (data.ok && data.results && data.results.length)?data.results[0]:false);
  }catch(_e){ _setMethodQuickPick(method,false); }
}

function _initMethodQuickPicks(existingRow){
  _setMethodQuickPick('playlist',null);
  _setMethodQuickPick('direct',null);
  const row=existingRow||_rowMap[_searchKey]||_rows.find(r=>String(r.rating_key)===String(_searchKey));
  const golden=_goldenSourceMeta(row);
  _setMethodQuickPick('golden_source', golden.url ? {title:'Golden Source URL', url:golden.url, start_offset:golden.offset} : false);
  _prefetchMethodFirstResult('playlist');
  _prefetchMethodFirstResult('direct');
}

function searchPreviewSeek(val){ _searchPreviewAudio.seek(val); }
function searchPreviewSkip(seconds){ _searchPreviewAudio.skip(seconds); }
function searchPreviewToggle(){ _searchPreviewAudio.toggle(); }

function _sourceEditorTrimMeta(){
  const audio=_sourceEditorAudio.audio;
  const total=Math.max(0, Number(audio?.duration)||0);
  const offsetValue=document.getElementById('se-offset')?.value || '0';
  const rawOffset=Math.max(0, parseTrim(offsetValue||'0'));
  const start=total>0?Math.min(rawOffset,total):rawOffset;
  const maxDuration=Math.max(0, Number(_maxDur)||0);
  const end=total>0?(maxDuration>0?Math.min(total,start+maxDuration):total):0;
  const length=total>0?Math.max(0,end-start):0;
  const endOffset=total>0?Math.max(0, Math.round(total-end)):0;
  const exceeds=total>0 && rawOffset>total;
  const short=total>0 && length>0 && length<3;
  return {total,rawOffset,start,end,length,endOffset,exceeds,short};
}

function seSyncOffsetInputs(){
  const offsetEl=document.getElementById('se-offset');
  if(!offsetEl) return;
  normalizeOffsetInput(offsetEl);
  seUpdateTrim();
}

function seSnapOffsetToCurrent(){
  const a=document.getElementById('se-audio');
  const offsetEl=document.getElementById('se-offset');
  if(!a || !offsetEl) return;
  offsetEl.value=fmt(a.currentTime||0);
  seUpdateTrim();
}

async function searchModalDownloadNow(){
  const key=_seKey||_searchKey;
  const lib=_seLib||_searchLib||_activeLib||'';
  if(!key){ toast('No row selected','err'); return; }
  try{
    await approveSourceEditor(true);
  }catch(_e){
    return;
  }
  closeSearchModal();
  toast('Approved source — downloading now…','info');
  dbProgressStart('Downloading selected title…');
  dbProgressSet(35,'Preparing download…');
  const row=_rows.find(r=>r.rating_key===key);
  const folder=row?row.folder||'':'';
  const tmdb=row?row.tmdb_id||'':'';
  const r=await fetch('/api/theme/download-now',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({rating_key:key,library:lib,folder,tmdb_id:tmdb})
  });
  const data=await r.json().catch(()=>({ok:false,error:'Download failed'}));
  if(data.ok){
    dbProgressSet(90,'Finalizing download…');
    dbProgressDone('Download complete');
    toast(data.message||'Download started','ok');
  }
  else {
    dbProgressFail(data.error||'Download failed');
    toast(data.error||'Download failed','err');
  }
  loadDatabase();
}

function goToStep3(url, opts={}){
  _step3EntryMode=opts.entryMode||'';
  stopAllAudio();
  _hideGoldenValidationError();
  if(_searchMethod!=='golden_source' && _lastSearchResults.length) _renderResults(_lastSearchResults);
  document.getElementById('se-url').value=url||'';
  document.getElementById('se-offset').value=_normalizedOffsetValue(opts.startOffset||'0');
  document.getElementById('se-cur').textContent='0:00';
  document.getElementById('se-dur').textContent='—';
  document.getElementById('se-slider').value=0;
  document.getElementById('se-trim-start-label').textContent='Start 0:00';
  document.getElementById('se-trim-end-label').textContent='End —';
  const trimWindow=document.getElementById('se-trim-window');
  if(trimWindow) trimWindow.style.display='none';
  document.getElementById('se-info').textContent='Loading preview…';
  _step3PreparedUrl=url||'';
  _selectedSourceTitle=(opts.sourceTitle||_selectedSourceTitle||_sourceTitleFromUrl(url||''));
  _renderSelectedSourceSummary(url,_selectedSourceTitle);
  _sourceEditorAudio.setPlaying(false);
  seUpdateTrim();
  goToSearchStep(3);
  if(url && opts.skipPreview!==true) setTimeout(()=>seLoadPreview(),80);
}


function goToStep3FromPaste(){
  const url=(document.getElementById('search-paste-url').value||'').trim();
  if(!url){ toast('Paste a URL first','info'); return; }
  goToStep3(url,{skipPreview:false,sourceTitle:_sourceTitleFromUrl(url),entryMode:'paste'});
}

// ── Source editor (Step 3) functions ────────────────────────────────────────
function openSourceEditor(url, previewOnly){
  if(!url){ toast('No URL provided','info'); return; }
  _seKey=_searchKey; _seLib=_searchLib||_activeLib||'';
  goToStep3(url,{skipPreview:false,sourceTitle:_sourceTitleFromUrl(url),entryMode:'existing'});
}

function closeSourceEditor(){
  closeSearchModal();
}

function backToSearch(){
  goToSearchStep(2);
}

let _seAutoLoadTimer=null;
function seUrlChanged(){
  _hideGoldenValidationError();
  const u=(document.getElementById('se-url').value||'').trim();
  if(u) _selectedSourceTitle=_sourceTitleFromUrl(u);
  _renderSelectedSourceSummary(u, u?_selectedSourceTitle:(_selectedSourceTitle||'—'));
  if(_seAutoLoadTimer) clearTimeout(_seAutoLoadTimer);
  if(!u) return;
  _seAutoLoadTimer=setTimeout(()=>{
    if((document.getElementById('se-url')?.value||'').trim()===u) seLoadPreview();
  }, 350);
}

async function seCopyUrl(){
  const url=(document.getElementById('se-url')?.value||'').trim();
  if(!url) return;
  try{
    await navigator.clipboard.writeText(url);
    toast('Source URL copied','ok');
  }catch(_e){
    toast('Copy failed','err');
  }
}

function seOpenUrl(){
  const url=(document.getElementById('se-url')?.value||'').trim();
  if(!url) return;
  window.open(url,'_blank','noopener');
}

async function seLoadPreview(){
  const url=(document.getElementById('se-url').value||'').trim();
  if(!url){ toast('Enter a URL first','info'); return; }
  const loadSeq=++_sePreviewLoadSeq;
  const cached=_previewCache[url];
  try{
    let data=null;
    if(cached){
      data={ok:true,audio_url:cached.audio_url};
      document.getElementById('se-info').textContent='Loaded cached preview';
    }else{
      document.getElementById('se-info').textContent='Resolving stream…';
      const r=await fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
      data=await r.json();
      if(data?.ok) _previewCache[url]={audio_url:data.audio_url};
    }
    if(loadSeq!==_sePreviewLoadSeq) return;
    if(!data.ok){
      const previewError=data.error||'Failed';
      document.getElementById('se-info').textContent='Error: '+previewError;
      if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select'){
        await _fallbackFromGoldenValidation(previewError);
        return;
      }
      return;
    }
    _hideGoldenValidationError();
    const audio=_sourceEditorAudio.audio;
    stopAllAudio('se-audio');
    audio.src=apiUrl(data.audio_url);
    _lastPreviewedUrl=url;
    const startedAt=Date.now();
    let handledPlaybackError=false;
    _sourceEditorAudio.setHandlers({
      onloadedmetadata:(loaded)=>{
        if(loadSeq!==_sePreviewLoadSeq) return;
        _applyAudioOffset(loaded, document.getElementById('se-offset')?.value || '0');
        document.getElementById('se-info').textContent=`Duration: ${fmt(loaded.duration)}`;
        seUpdateTrim();
        _sourceEditorAudio.play().catch(()=>{});
      },
      onerror:()=>{
        if(loadSeq!==_sePreviewLoadSeq || handledPlaybackError) return;
        if(Date.now()-startedAt<700) return;
        handledPlaybackError=true;
        const playbackError='Playback error — review another source below';
        document.getElementById('se-info').textContent=playbackError;
        _sourceEditorAudio.cleanup();
        if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select'){
          void _fallbackFromGoldenValidation('Golden Source preview failed');
          return;
        }
      }
    });
    audio.load();
  }catch(e){
    const previewError=String(e&&e.message?e.message:e||'Preview failed');
    if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select'){
      document.getElementById('se-info').textContent='Error: '+previewError;
      await _fallbackFromGoldenValidation(previewError);
      return;
    }
    toast('Error: '+previewError,'err');
  }
}

function seToggle(){
  const audio=_sourceEditorAudio.audio;
  if(!audio.src||audio.src===window.location.href){ seLoadPreview(); return; }
  _sourceEditorAudio.toggle();
}

function seSeek(val){ _sourceEditorAudio.seek(val); }
function seSkip(s){ _sourceEditorAudio.skip(s); }

function seUpdateTrim(){
  const audio=_sourceEditorAudio.audio;
  const trimWindow=document.getElementById('se-trim-window');
  const startShade=document.getElementById('se-trim-left');
  const endShade=document.getElementById('se-trim-right');
  const startMarker=document.getElementById('se-trim-start');
  const endMarker=document.getElementById('se-trim-end');
  const startLabel=document.getElementById('se-trim-start-label');
  const endLabel=document.getElementById('se-trim-end-label');
  const offsetValue=document.getElementById('se-offset')?.value || '0';
  const duration=audio?.duration||0;
  const infoEl=document.getElementById('se-info');
  if(!trimWindow || !audio || !audio.duration){
    if(trimWindow) trimWindow.style.display='none';
    if(startLabel) startLabel.textContent='Start 0:00';
    if(endLabel) endLabel.textContent='End —';
    const summary=document.getElementById('se-clip-summary');
    const main=document.getElementById('se-clip-summary-main');
    const sub=document.getElementById('se-clip-summary-sub');
    const warning=document.getElementById('se-clip-summary-warning');
    const offsetFmt=_normalizedOffsetValue(offsetValue);
    if(main) main.textContent=`Offset ${offsetFmt} · Keep — · End —`;
    if(sub) sub.textContent='Load a preview to confirm the kept portion.';
    if(warning) warning.textContent='';
    if(summary){
      summary.classList.remove('is-short','is-zero','is-warning');
    }
    if(infoEl) infoEl.textContent=`Offset ${offsetFmt} — load a preview to confirm the kept portion`;
    return;
  }
  const meta=_sourceEditorTrimMeta();
  const startPct=(meta.start/duration)*100;
  const endPct=(meta.end/duration)*100;
  trimWindow.style.display='block';
  startShade.style.width=`${startPct}%`;
  endShade.style.left=`${endPct}%`;
  startMarker.style.left=`${startPct}%`;
  endMarker.style.left=`${endPct}%`;
  if(startLabel) startLabel.textContent=`Start ${fmt(meta.start)}`;
  if(endLabel) endLabel.textContent=`End ${fmt(meta.end)}`;
  const summary=document.getElementById('se-clip-summary');
  const main=document.getElementById('se-clip-summary-main');
  const sub=document.getElementById('se-clip-summary-sub');
  const warning=document.getElementById('se-clip-summary-warning');
  if(main) main.textContent=`Offset ${fmt(meta.start)} · Keep ${fmt(meta.length)} · End ${fmt(meta.end)}`;
  if(sub) sub.textContent=`Preview ${fmt(meta.total)} total · keeping ${fmt(meta.start)} → ${fmt(meta.end)}`;
  if(warning) warning.textContent=meta.exceeds
    ?`Offset ${fmt(meta.rawOffset)} exceeds preview duration ${fmt(meta.total)}.`
    :(meta.short ? `Very short result — only ${fmt(meta.length)} will be kept.` : '');
  if(summary){
    summary.classList.toggle('is-short', !!meta.short && !meta.exceeds);
    summary.classList.remove('is-zero');
    summary.classList.toggle('is-warning', !!meta.exceeds);
  }
  if(infoEl){
    const offsetFmt=_normalizedOffsetValue(offsetValue);
    infoEl.textContent=`Offset ${offsetFmt} · keep ${fmt(meta.length)} · ends at ${fmt(meta.end)}`;
  }
}

function sePreviewFromOffset(){
  const audio=_sourceEditorAudio.audio;
  const s=parseTrim(document.getElementById('se-offset')?.value || '0');
  if(!audio.src||audio.src===window.location.href){ seLoadPreview(); return; }
  stopAllAudio('se-audio');
  audio.currentTime=s;
  _sourceEditorAudio.play().catch(()=>{});
}

function _draftSelectedSourceContract(url=''){
  const cleanUrl=String(url||'').trim();
  if(!cleanUrl) return {kind:'', method:''};
  const row=_rowMap[_seKey||_searchKey]||_rows.find(r=>String(r.rating_key)===String(_seKey||_searchKey))||{};
  const goldenUrl=String(row?.golden_source_url||'').trim();
  if(cleanUrl && goldenUrl && cleanUrl===goldenUrl) return {kind:'golden', method:'golden_source'};
  if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select') return {kind:'golden', method:'golden_source'};
  if(_step3EntryMode==='search_result'){
    if(_searchMethod==='playlist' || _searchMethod==='direct') return {kind:'custom', method:_searchMethod};
    if(_searchMethod==='custom') return {kind:'custom', method:'custom'};
  }
  if(_step3EntryMode==='paste') return {kind:'custom', method:'paste'};
  if(_step3EntryMode==='existing'){
    const existing=_selectedSourceContract({...row, url:cleanUrl});
    if(existing.url) return {kind:existing.kind, method:existing.method};
  }
  return {kind:'custom', method:'manual'};
}

async function saveSourceEditor(skipClose=false){
  const key=_seKey||_searchKey; if(!key) return;
  const url=(document.getElementById('se-url').value||'').trim();
  const trimMeta=_sourceEditorTrimMeta();
  const offset=trimMeta.rawOffset;
  const endOffset=trimMeta.endOffset;
  const sourceContract=_draftSelectedSourceContract(url);
  if(!url){ toast('Please enter a URL','info'); return; }
  const lib=_seLib||_searchLib||_activeLib||'';
  const status='STAGED';
  const notes='URL approved via manual review';

  const saveResp=await fetch('/api/ledger/manual-source',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      rating_key:key,
      library:lib,
      url,
      start_offset:offset,
      end_offset:endOffset,
      selected_source_kind:sourceContract.kind,
      selected_source_method:sourceContract.method,
      notes,
      target_status:status
    })
  });
  const savePayload=await saveResp.json().catch(()=>({}));
  if(!saveResp.ok || savePayload?.ok===false){
    toast(savePayload.error||'Failed to save manual source','err');
    throw new Error(savePayload.error||'save_manual_source_failed');
  }

  const savedRow=upsertLedgerRow(savePayload.row);
  if(savedRow){
    renderChips();
    filterTable();
  }
  if(!skipClose){
    closeSearchModal();
    toast('Source saved','ok');
  }
  return true;
}

async function approveSourceEditor(skipClose=false){
  const key=_seKey||_searchKey;
  if(!key) return false;
  await saveSourceEditor(true);
  await updateRow(key,'status','APPROVED');
  if(!skipClose){
    closeSearchModal();
    toast('Source approved','ok');
  }
  return true;
}


function previewSearchResult(url, btn){
  if(!url) return;
  if(btn){
    btn.dataset.previewUrl = url;
    btn.textContent = 'Loading…'; btn.disabled = true;
  }
  fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})})
    .then(r=>r.json()).then(async data=>{
      if(btn){ btn.textContent = '▶ Preview'; btn.disabled = false; }
      if(!data.ok){ toast(data.error||'Failed','err'); return; }
      _previewCache[url]={audio_url:data.audio_url};
      _lastPreviewedUrl=url;
      resetActivePreviewBtn();
      await _searchPreviewAudio.playFrom(apiUrl(data.audio_url)).catch(()=>{});
      window._activePreviewBtn = btn||null;
      if(btn) btn.textContent = '■ Stop';
      _activeQuickPickBtn = null;
      _activeQuickPickUrl = '';
    }).catch(()=>{ if(btn) resetPreviewButton(btn); toast('Preview failed','err'); });
}

function resetPreviewButton(btn){
  if(!btn) return;
  btn.textContent = '▶ Preview';
  btn.disabled = false;
  const url = btn.dataset.previewUrl;
  if(url) btn.onclick = ()=>previewSearchResult(url, btn);
}

function resetActivePreviewBtn(){
  if(window._activePreviewBtn){
    resetPreviewButton(window._activePreviewBtn);
    window._activePreviewBtn = null;
  }
  if(_activeQuickPickBtn){
    _activeQuickPickBtn.textContent='▶ Preview';
    _activeQuickPickBtn=null;
    _activeQuickPickUrl='';
  }
  _searchPreviewAudio.pauseAndReset();
}

// ── Trim modal preview ────────────────────────────────────────────────────────
let _trimRk='',_trimLib='';
function trimModalUpdateResult(){
  const row=_mediaRows.find(r=>r.rating_key===_trimRk); if(!row) return;
  const offsetValue=document.getElementById('trim-modal-offset').value||'0';
  const duration=Number(_trimModalAudio.audio?.duration)||Number(row.theme_duration)||Number(row.duration)||0;
  document.getElementById('trim-modal-result').textContent=_clipLengthOffsetLabel(duration, offsetValue, 0);
  _setClipSummary('trim-modal-summary','trim-modal-summary-main','trim-modal-summary-sub','trim-modal-summary-warning',duration,offsetValue,0,'preview');
}

function trimModalPreview(fromOffset){
  const row=_mediaRows.find(r=>r.rating_key===_trimRk); if(!row) return;
  _trimModalAudio.cleanup({clearSrc:false});
  _trimModalAudio.audio.src=apiUrl(`/api/theme?folder=${encodeURIComponent(row.folder)}`);
  const offset=fromOffset?parseTrim(document.getElementById('trim-modal-offset').value):0;
  _trimModalAudio.setHandlers({
    onloadedmetadata:(audio)=>{ audio.currentTime=offset; _trimModalAudio.play().catch(()=>{}); }
  });
  _trimModalAudio.audio.load();
  toast(fromOffset?`Previewing from ${fmt(offset)}`:'Playing from start','info');
}
function trimModalTogglePlay(){ _trimModalAudio.toggle(); }
function trimModalSeek(val){ _trimModalAudio.seek(val); }
function trimModalSkip(s){ _trimModalAudio.skip(s); }
function closeTrimModal(){
  closeModal('trim-modal');
  _trimLib='';
  runModalMediaCleanup(()=>_trimModalAudio.cleanup());
}
async function applyTrimFromModal(){
  if(!_trimRk) return;
  const library=requireLibraryContext(_trimLib||_activeLib,'trim a theme');
  if(!library) return;
  const start=parseTrim(document.getElementById('trim-modal-offset').value||'0');
  const r=await fetch('/api/theme/trim',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({library,rating_key:_trimRk,start_offset:start,end_offset:0})});
  const d=await r.json().catch(()=>({ok:false,error:'Trim failed'}));
  if(!r.ok || d.ok===false) return toast(d.error||'Trim failed','err');
  toast(d.message||'Trim applied','ok');
  closeTrimModal();
  await loadDatabase();
}

// ── YouTube Modal (Database page) ────────────────────────────────────────────
let _ytModalKey=null,_ytModalLib='';

const _ytModalAudio=bindModalAudio({audioId:'yt-modal-audio',playBtnId:'yt-modal-play',sliderId:'yt-modal-slider',curId:'yt-modal-cur',durId:'yt-modal-dur',statusId:'yt-modal-status'});
const _themeModalAudio=bindModalAudio({audioId:'theme-modal-audio',playBtnId:'theme-modal-play',sliderId:'theme-modal-slider',curId:'theme-modal-cur',durId:'theme-modal-dur'});
const _trimModalAudio=bindModalAudio({audioId:'trim-modal-audio',playBtnId:'trim-modal-play',sliderId:'trim-modal-slider',curId:'trim-modal-cur',durId:'trim-modal-dur'});
const _sourceEditorAudio=bindModalAudio({audioId:'se-audio',playBtnId:'se-play-btn',sliderId:'se-slider',curId:'se-cur',durId:'se-dur',statusId:'se-info'});

async function mediaDownloadApproved(){
  const lib=_activeLib||'';
  const approved=_mediaRows.filter(r=>r.status==='APPROVED').length;
  if(!approved){toast('No approved items in '+lib,'info');return;}
  const btn=document.getElementById('media-dl-btn');
  btn.innerHTML='<span class="spinner"></span>Downloading…'; btn.disabled=true;
  toast(`Downloading ${approved} approved items from ${lib}…`,'info');
  await startPipelineRun(3,'db',{libraries:lib?[lib]:[],scopeLabel:lib?`library "${lib}"`:'selected library'});
  // Re-enable after a delay (the actual process runs async)
  setTimeout(()=>{btn.textContent='↓ Download Approved Items';btn.disabled=false;},3000);
}
function openYtModal(rk,title,year,url,lib){
  stopAllAudio();
  _ytModalKey=rk;_ytModalLib=decodeURIComponent(lib);
  // Poster
  const posterEl=document.getElementById('yt-modal-poster');
  posterEl.src=apiUrl('/api/poster?key='+rk);
  _setHidden(posterEl, false, '');
  // Header info
  document.getElementById('yt-modal-title').textContent=title;
  document.getElementById('yt-modal-year-meta').textContent=year;
  document.getElementById('yt-modal-dur-badge').style.display='none';
  // Links: TMDB + YouTube
  const tmdbUrl='https://www.themoviedb.org/search/movie?query='+encodeURIComponent(title+' '+year);
  const isYt=url.includes('youtube.com')||url.includes('youtu.be');
  document.getElementById('yt-modal-links').innerHTML=
    `<a class="modal-link-pill tmdb-pill" href="${tmdbUrl}" target="_blank" rel="noopener">TMDB</a>`+
    (isYt?`<a class="modal-link-pill yt-pill" href="${url.replace(/"/g,'&quot;')}" target="_blank" rel="noopener">▶ YouTube ↗</a>`:`<a class="modal-link-pill" href="${url.replace(/"/g,'&quot;')}" target="_blank" rel="noopener">↗ Source</a>`);
  // Reset player
  document.getElementById('yt-modal-start').value='0:00';
  document.getElementById('yt-modal-trim-info').textContent='';
  _ytModalAudio.setStatus('Resolving stream…');
  setBio('yt-modal-bio', rk, _ytModalLib);
  _setHidden(document.getElementById('yt-modal-save'), false, 'inline-flex');
  _ytModalAudio.cleanup({clearSrc:false});
  document.getElementById('yt-modal-dur-val').textContent='—';
  openModal('yt-modal');
  // Fetch stream
  fetch('/api/preview',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})})
    .then(r=>r.json()).then(data=>{
      if(!data.ok){_ytModalAudio.setStatus('Error: '+(data.error||'Failed'));return;}
      _ytModalAudio.audio.src=apiUrl(data.audio_url);
      _ytModalAudio.setHandlers({
        onloadedmetadata:(audio)=>{
          const d=fmt(audio.duration);
          document.getElementById('yt-modal-dur-val').textContent=d;
          document.getElementById('yt-modal-dur-badge').style.display='';
          _ytModalAudio.setStatus('Streaming · '+d);
          ytModalUpdateTrim();
          _ytModalAudio.play().catch(()=>{_ytModalAudio.setStatus('Click ▶ to play');});
        },
        onerror:()=>{_ytModalAudio.setStatus('Playback error — try again');}
      });
      _ytModalAudio.audio.load();
    }).catch(e=>{_ytModalAudio.setStatus('Error: '+e);});
}
function closeYtModal(){
  closeModal('yt-modal');
  runModalMediaCleanup(()=>_ytModalAudio.cleanup());
  _ytModalKey=null;
}
function ytModalToggle(){ _ytModalAudio.toggle(); }
function ytModalSeek(val){ _ytModalAudio.seek(val); }
function ytModalSkip(s){ _ytModalAudio.skip(s); }
function ytModalUpdateTrim(){
  const audio=document.getElementById('yt-modal-audio'),dur=audio.duration||0;
  const s=parseTrim(document.getElementById('yt-modal-start').value);
  const res=dur>0?Math.max(0,dur-s):0;
  document.getElementById('yt-modal-trim-info').textContent=dur>0?`→ ${fmt(res)} remaining (from ${fmt(dur)})`:'';
}
function ytModalPreviewTrim(){
  const s=parseTrim(document.getElementById('yt-modal-start').value);
  _ytModalAudio.audio.currentTime=s;
  _ytModalAudio.play().catch(()=>{});
}
async function ytModalSave(){
  if(!_ytModalKey) return;
  const s=parseTrim(document.getElementById('yt-modal-start').value);
  await fetch('/api/ledger/'+_ytModalKey+'?library='+encodeURIComponent(_ytModalLib),{
    method:'PATCH',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({start_offset:s,notes:'Offset set via YouTube preview'})
  });
  closeYtModal(); toast('Offset saved','ok');
  if(document.getElementById('page-theme-manager').classList.contains('active')) loadDatabase();
}

// ── SECTION: RUN / SCHEDULE LOGIC ──────────────────────────────────────────────
