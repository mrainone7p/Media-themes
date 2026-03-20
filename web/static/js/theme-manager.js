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
}

async function switchLib(name){
  _activeLib=name;
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.textContent.trim().replace(' (off)','')===name));
  _refreshScopedRunLabels();
  await loadLibRows(name);
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
  let golden=0, noGolden=0;
  let manage=0;
  _rows.forEach(r=>{
    counts[r.status]=(counts[r.status]||0)+1;
    if(String(r.golden_source_url||'').trim()) golden+=1;
    else noGolden+=1;
    if(rowActionType(r)==='MANAGE') manage+=1;
  });
  const filters=[
    {label:'Total', count:_rows.length, active:!document.getElementById('db-search').value.trim() && !document.getElementById('db-filter').value && !document.getElementById('db-source-filter').value && !document.getElementById('db-action-filter').value, color:'', handler:"clearDbFilter()"},
    {label:'MISSING', count:counts.MISSING||0, active:document.getElementById('db-filter').value==='MISSING', color:SC.MISSING, handler:"filterByStatus('MISSING')"},
    {label:'AVAILABLE', count:counts.AVAILABLE||0, active:document.getElementById('db-filter').value==='AVAILABLE', color:SC.AVAILABLE, handler:"filterByStatus('AVAILABLE')"},
    {label:'GOLDEN SOURCE', count:golden, active:document.getElementById('db-source-filter').value==='GOLDEN', color:'var(--amber)', handler:"filterBySourceState('GOLDEN')"},
    {label:'NO GOLDEN SOURCE', count:noGolden, active:document.getElementById('db-source-filter').value==='NO_GOLDEN', color:'var(--text3)', handler:"filterBySourceState('NO_GOLDEN')"},
    {label:'MANAGE', count:manage, active:document.getElementById('db-action-filter').value==='MANAGE', color:'var(--blue)', handler:"filterByAction('MANAGE')"},
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
  if(col==='source_origin') return (row.source_origin||'').toString().toLowerCase();
  if(col==='current_theme') return (row.status==='AVAILABLE'?1:0);
  return (row[col]||'').toString().toLowerCase();
}

function filterTable(){
  const q=(document.getElementById('db-search').value||'').toLowerCase().trim();
  const st=document.getElementById('db-filter').value;
  const sourceFilter=document.getElementById('db-source-filter').value;
  const actionFilter=document.getElementById('db-action-filter').value;
  renderChips();
  _filtered=_rows.filter(r=>{
    if(st&&r.status!==st) return false;
    const hasGolden=!!String(r.golden_source_url||'').trim();
    const hasSource=!!String(r.url||'').trim();
    if(sourceFilter==='GOLDEN' && !hasGolden) return false;
    if(sourceFilter==='NO_GOLDEN' && hasGolden) return false;
    if(sourceFilter==='SOURCE_URL' && !hasSource) return false;
    if(sourceFilter==='NO_SOURCE_URL' && hasSource) return false;
    if(actionFilter && rowActionType(r)!==actionFilter) return false;
    const haystack=[r.title||'',r.plex_title||'',r.year||'',r.url||'',r.golden_source_url||'',r.notes||'',r.status||''].join(' ').toLowerCase();
    if(q&&!haystack.includes(q)) return false;
    return true;
  });
  _filtered.sort((a,b)=>{
    if(_sortCol==='golden_source_url'){
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

function _renderSelectedSourceSummary(url, title){
  const sourceEl=document.getElementById('se-source-title');
  const summaryEl=document.getElementById('se-source-url-summary');
  const copyBtn=document.getElementById('se-copy-btn');
  const openBtn=document.getElementById('se-open-btn');
  const cleanUrl=String(url||'').trim();
  const cleanTitle=String(title||'').trim()||_sourceTitleFromUrl(cleanUrl)||'—';
  _applyTruncatedText(sourceEl, cleanTitle, {fallback:'—', max:62});
  _applyTruncatedText(summaryEl, cleanUrl, {fallback:'No source URL selected', max:64, middle:true});
  if(copyBtn) copyBtn.disabled=!cleanUrl;
  if(openBtn) openBtn.disabled=!cleanUrl;
}

function _manualSaveTargetStatus(){
  const key=_seKey||_searchKey;
  const row=_rowMap[key]||_rows.find(r=>String(r.rating_key)===String(key));
  const isAvailable=row && ['AVAILABLE'].includes(String(row.status||'').toUpperCase());
  return (isAvailable || !_autoApproveManual) ? 'STAGED' : 'APPROVED';
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

function toggleRowMenu(btn, evt){
  if(evt) evt.stopPropagation();
  const wrap=btn.closest('.row-action-wrap');
  if(!wrap) return;
  const menu=wrap.querySelector('.row-action-menu');
  const willOpen=!menu.classList.contains('open');
  document.querySelectorAll('.row-action-menu.open').forEach(m=>m.classList.remove('open'));
  if(willOpen) menu.classList.add('open');
}

function _rowActionInvoke(fnName, btn){
  document.querySelectorAll('.row-action-menu.open').forEach(m=>m.classList.remove('open'));
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
  openThemeModal(rk, title, year, folder, row||{});
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
  openYtModal(rk, title, year, url, encodeURIComponent(_activeLib||''));
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
  openSearchModal(rk, row.title||row.plex_title||'', row.year||'', encodeURIComponent(_activeLib||''));
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
  if(!e.target.closest('.row-action-wrap')) document.querySelectorAll('.row-action-menu.open').forEach(m=>m.classList.remove('open'));
});

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
    const safeUrlAttr=(row.url||'').replace(/"/g,'&quot;');
    const off=fmt(parseTrim(row.start_offset||0));
    const tmdbHref=_tmdbLink(row.title||row.plex_title,row.year);
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
      <td style="white-space:nowrap">${row.golden_source_url?`<a class="golden-link-pill" href="${String(row.golden_source_url).replace(/"/g,'&quot;')}" target="_blank" rel="noopener" title="${String(row.golden_source_url).replace(/"/g,'&quot;')}">★ Golden Source ↗</a>`:'Not available'}</td>
      <td style="white-space:nowrap">${row.url?`<a href="${safeUrlAttr}" target="_blank" rel="noopener" class="db-link-icon" title="${safeUrlAttr}">↗</a>`:''}
        <input class="inline-ed db-inline-url" value="${safeUrlAttr}" placeholder="paste URL…" onblur="updateRowAndRefresh('${rk}','url',this.value)" onkeydown="if(event.key==='Enter')this.blur()"></td>
      <td><input class="inline-ed offset-input" type="text" value="${off}" style="width:74px;font-size:11px"
        onblur="saveOffsetInput('${rk}',this)"
        onkeydown="if(event.key==='Enter')this.blur()"></td>
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
async function fetchBio(rk){
  if(_bioCache[rk]!==undefined) return _bioCache[rk];
  try{
    const lib=_currentLib();
    const qs='key='+encodeURIComponent(rk)+(lib?'&library='+encodeURIComponent(lib):'');
    const r=await fetch('/api/movie/bio?'+qs);
    const d=await r.json();
    _bioCache[rk]=d.summary||'';
    return _bioCache[rk];
  }catch{ _bioCache[rk]=''; return ''; }
}
async function setBio(elId,rk){
  const el=document.getElementById(elId);
  if(!el) return;
  if(!rk){el.style.display='none';return;}
  const bio=await fetchBio(rk);
  if(bio){el.textContent=bio;el.style.display='';}
  else{el.style.display='none';}
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
  return String(row.status||'').toUpperCase()==='AVAILABLE' || String(row.theme_exists||'')==='1';
}
function _themeSourceMeta(row={}){
  const notes=String(row?.notes||'');
  const sourceOrigin=String(row?.source_origin||'').toLowerCase();
  const sourceUrl=String(row?.url||'').trim();
  const goldenUrl=String(row?.golden_source_url||'').trim();
  let type='unknown';
  let method='Unknown';
  if(sourceOrigin==='golden_source' || sourceOrigin==='golden_source_verified'){
    type='golden';
    method='Golden Source import';
  }
  if(/found via playlist/i.test(notes) || /playlist/i.test(sourceUrl) || /[?&]list=/.test(sourceUrl)){
    type='playlist';
    method='Playlist search pull';
  }else if(/found via direct/i.test(notes)){
    type='direct';
    method='Direct track pull';
  }else if(sourceOrigin==='manual'){
    type='custom';
    method='Custom / manual source';
  }else if(!sourceUrl && goldenUrl){
    type='golden';
    method='Golden Source import';
  }else if(sourceUrl){
    type='direct';
    method='Direct source link';
  }
  const labelMap={golden:'Golden Source',playlist:'Playlist',direct:'Direct',custom:'Custom',unknown:'Unknown'};
  const classMap={golden:'is-golden',playlist:'is-playlist',direct:'is-direct',custom:'is-custom',unknown:'is-unknown'};
  return {type,label:labelMap[type]||'Unknown',method,className:classMap[type]||'is-unknown'};
}
function _themeModalPrimarySourceUrl(row={}){
  return String(row?.url||'').trim() || String(row?.golden_source_url||'').trim();
}
function _themeModalImportedAt(row={}){
  const raw=String(row?.last_updated||'').trim();
  if(!raw) return 'Not imported yet';
  const iso=raw.replace(' ','T')+'Z';
  const dt=new Date(iso);
  if(Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleString(undefined,{year:'numeric',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
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
function _themeModalOffsetLabel(row={}, hasTheme=false){
  const duration=hasTheme && Number(row?.theme_duration||0)>0?parseFloat(row.theme_duration||0):0;
  return _clipLengthOffsetLabel(duration, row?.start_offset||0, 0);
}
function _themeModalNextAction(row={}, hasTheme=false){
  const status=String(row?.status||'').toUpperCase();
  if(status==='STAGED') return {label:'Approve Theme',className:'btn btn-amber',handler:'approve'};
  if(status==='APPROVED') return {label:'Download Theme',className:'btn btn-green',handler:'download'};
  if(!hasTheme) return {label:'Find Theme',className:'btn btn-amber',handler:'find'};
  return null;
}
const THEME_MODAL_STATUS_HELPERS={
  MISSING:'No source attached yet.',
  STAGED:'Source selected, awaiting approval.',
  APPROVED:'Ready to download.',
  AVAILABLE:'Local theme saved.',
  FAILED:'Needs attention before retrying.'
};
function _themeModalUpdateStatusFlow(status='MISSING'){
  const normalized=String(status||'MISSING').toUpperCase();
  const helper=document.getElementById('theme-modal-status-helper');
  if(helper) helper.textContent=THEME_MODAL_STATUS_HELPERS[normalized]||'Review the current theme state.';
}
function openThemeModal(rk,title,year,folder,row={}){
  stopAllAudio();
  _themeModalContext={rk,title,year,folder,row};
  const hasTheme=_themeHasLocal(row);
  const status=String(row?.status||'MISSING').toUpperCase();
  const storedPrimarySourceUrl=_themeModalPrimarySourceUrl(row);
  const hasSource=status!=='MISSING' && !!storedPrimarySourceUrl;
  const primarySourceUrl=hasSource?storedPrimarySourceUrl:'';
  const sourceMeta=hasSource?_themeSourceMeta(row):{label:'None',className:'is-unknown',method:'—'};
  const themeFilename=(document.getElementById('cfg-theme_filename')?.value||'theme.mp3').trim()||'theme.mp3';
  const themeFile=folder?`${folder}/${themeFilename}`:'Unknown folder';

  document.getElementById('theme-modal-title').textContent=title;
  document.getElementById('theme-modal-year').textContent=year;
  document.getElementById('theme-modal-dur-meta').textContent=hasTheme?'Theme present':'Theme missing';
  const statusBadge=document.getElementById('theme-modal-status-badge');
  if(statusBadge){
    statusBadge.className=`badge s-${status}`;
    statusBadge.innerHTML=`<span class="si"></span>${displayStatus(status)}`;
  }
  _themeModalUpdateStatusFlow(status);
  document.getElementById('theme-modal-local-status').textContent=hasTheme
    ?'Available locally'
    :'Missing locally';
  document.getElementById('theme-modal-file').textContent=`File path: ${themeFile}`;
  document.getElementById('theme-modal-local-dur').textContent=`Duration: ${hasTheme&&row?.theme_duration?fmt(parseFloat(row.theme_duration||0)):'—'}`;
  const sourceUrlEl=document.getElementById('theme-modal-source-url');
  if(sourceUrlEl){
    _applyTruncatedText(sourceUrlEl, primarySourceUrl, {fallback:'—', max:56, middle:true});
  }
  document.getElementById('theme-modal-origin').textContent=hasSource?sourceMeta.method:'—';
  document.getElementById('theme-modal-imported').textContent=hasSource?_themeModalImportedAt(row):'—';
  document.getElementById('theme-modal-offset').textContent=hasSource?_themeModalOffsetLabel(row, hasTheme):'—';
  document.getElementById('theme-modal-source-notes').textContent=hasSource?(String(row?.notes||'').trim()||'No source notes recorded.'):'—';
  document.getElementById('theme-modal-source-summary').textContent=hasSource
    ?'Theme used for download and the details applied to it.'
    :'No source is attached right now.';
  const sourcePill=document.getElementById('theme-modal-source-pill');
  sourcePill.textContent=sourceMeta.label;
  sourcePill.className=`review-source-pill ${sourceMeta.className}`;
  sourcePill.style.display=hasSource?'inline-flex':'none';

  const sourceEmpty=document.getElementById('theme-source-empty');
  const sourceMetaList=document.getElementById('theme-source-meta-list');
  const sourceDetails=document.getElementById('theme-source-details');
  const openSourceBtn=document.getElementById('theme-open-source-btn');
  if(sourceEmpty) sourceEmpty.style.display=hasSource?'none':'block';
  if(sourceMetaList) sourceMetaList.style.display=hasSource?'flex':'none';
  if(openSourceBtn) openSourceBtn.style.display=hasSource?'':'none';
  const copySourceBtn=document.getElementById('theme-copy-source-btn');
  if(copySourceBtn) copySourceBtn.style.display=hasSource?'':'none';
  if(sourceDetails){
    sourceDetails.style.display=hasSource?'block':'none';
    sourceDetails.open=false;
  }

  const localCard=document.getElementById('theme-local-card');
  const localMeta=document.getElementById('theme-local-meta');
  const localMissing=document.getElementById('theme-local-missing');
  const localTrimBtn=document.getElementById('theme-modal-trim-btn');
  const localDeleteBtn=document.getElementById('theme-modal-delete-btn');
  if(localCard) localCard.classList.toggle('compact', !hasTheme);
  if(localMeta) localMeta.style.display='block';
  if(localMissing) localMissing.style.display='none';
  if(localTrimBtn) localTrimBtn.style.display=hasTheme?'':'none';
  if(localDeleteBtn) localDeleteBtn.style.display=hasTheme?'':'none';
  const sourceCard=document.getElementById('theme-source-card');
  if(sourceCard) sourceCard.classList.toggle('compact', !hasSource);

  const replaceBtn=document.getElementById('theme-replace-btn');
  const findBtn=document.getElementById('theme-find-btn');
  const nextStepBtn=document.getElementById('theme-next-step-btn');
  if(replaceBtn) replaceBtn.style.display='';
  if(findBtn) findBtn.style.display=hasTheme?'none':'';
  const nextAction=_themeModalNextAction(row, hasTheme);
  if(nextStepBtn){
    if(nextAction && nextAction.handler!=='find'){
      nextStepBtn.style.display='';
      nextStepBtn.textContent=nextAction.label;
      nextStepBtn.className=nextAction.className;
      nextStepBtn.dataset.action=nextAction.handler;
    }else{
      nextStepBtn.style.display='none';
      nextStepBtn.dataset.action='';
      nextStepBtn.className='btn btn-ghost';
      nextStepBtn.textContent='Next Step';
    }
  }

  document.getElementById('theme-local-player').style.display=hasTheme?'block':'none';
  document.getElementById('theme-local-missing').style.display='none';

  document.getElementById('theme-modal-poster').src=apiUrl('/api/poster?key='+rk);
  document.getElementById('theme-modal-poster').style.display='';
  // TMDB link
  const tmdbUrl='https://www.themoviedb.org/search/movie?query='+encodeURIComponent(title+' '+year);
  const tmdbLink=row?.tmdb_id?`https://www.themoviedb.org/movie/${encodeURIComponent(row.tmdb_id)}`:tmdbUrl;
  document.getElementById('theme-modal-links').innerHTML=
    `<a class="modal-link-pill tmdb-pill" href="${tmdbLink}" target="_blank" rel="noopener">TMDB</a>`;
  setBio('theme-modal-bio', rk);
  document.getElementById('theme-modal-bio')?.classList.add('is-clamped');
  _themeModalAudio.cleanup({clearSrc:false});
  if(hasTheme) _themeModalAudio.audio.src=apiUrl('/api/theme?folder='+encodeURIComponent(folder));
  else _themeModalAudio.audio.src='';
  openModal('theme-modal');
  _themeModalAudio.setHandlers();
  if(hasTheme) _themeModalAudio.audio.load();
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
  const library=requireLibraryContext(_activeLib,'download a theme');
  if(!library) return;
  const row=_themeModalContext?.row||{};
  if(String(row?.status||'').toUpperCase()!=='APPROVED') return;
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
  openDeleteModal(c.rk,c.title||'',_activeLib||'',c.folder||'');
}
function themeModalEditTrim(){
  const c=_themeModalContext||{};
  if(!c.rk) return;
  closeThemeModal();
  _trimRk=c.rk;
  document.getElementById('trim-modal-title').textContent=c.title||'Edit Trim';
  document.getElementById('trim-modal-meta').textContent=[c.year||'',`Duration: ${fmt(parseFloat(c.row?.theme_duration||0)||0)}`].filter(Boolean).join(' · ');
  document.getElementById('trim-modal-poster').src=apiUrl('/api/poster?key='+encodeURIComponent(c.rk));
  document.getElementById('trim-modal-links').innerHTML=`<a class="modal-link-pill tmdb-pill" href="${_tmdbLink(c.title||'',c.year||'')}" target="_blank" rel="noopener">TMDB</a>`;
  document.getElementById('trim-modal-info').textContent='Trim local theme start offset and preview the resulting clip.';
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
  openSearchModal(rk,_themeModalContext.title||'',_themeModalContext.year||'',encodeURIComponent(_activeLib||''));
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
