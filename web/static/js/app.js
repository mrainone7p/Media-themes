// Consolidated app bootstrap + shared helpers.


// ── Utils ────────────────────────────────────────────────────────────────────
const PLAY_ICON_SVG='<svg class="icon-play" viewBox="0 0 24 24"><polygon points="5 3 19 12 5 21 5 3"/></svg>';
const PAUSE_ICON_SVG='<svg class="icon-pause" viewBox="0 0 24 24" fill="#000"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>';

// ── SECTION: MODAL / AUDIO HELPERS ───────────────────────────────────────────

/**
 * Bind a set of DOM elements into a reusable audio player controller.
 * Used by all modal audio players (YouTube search, theme preview, trim, source editor).
 */
function bindModalAudio({audioId, playBtnId, sliderId, curId, durId, statusId}){
  const audio=document.getElementById(audioId);
  const playBtn=document.getElementById(playBtnId);
  const slider=document.getElementById(sliderId);
  const curEl=document.getElementById(curId);
  const durEl=document.getElementById(durId);
  const statusEl=statusId?document.getElementById(statusId):null;
  const setPlaying=(isPlaying)=>{ if(playBtn) playBtn.innerHTML=isPlaying?PAUSE_ICON_SVG:PLAY_ICON_SVG; };
  const clamp=(n,min,max)=>Math.max(min,Math.min(max,n));
  const syncDuration=()=>{ if(durEl) durEl.textContent=audio.duration?fmt(audio.duration):'—'; };
  const syncProgress=()=>{
    const dur=audio.duration||0;
    const cur=audio.currentTime||0;
    if(slider && dur) slider.value=(cur/dur*100);
    if(curEl) curEl.textContent=fmt(cur);
  };
  return {
    audio,
    setHandlers({onloadedmetadata, ontimeupdate, onended, onerror}={}){
      audio.onloadedmetadata=()=>{ syncDuration(); if(typeof onloadedmetadata==='function') onloadedmetadata(audio); };
      audio.ontimeupdate=()=>{ syncProgress(); if(typeof ontimeupdate==='function') ontimeupdate(audio); };
      audio.onended=()=>{ setPlaying(false); if(typeof onended==='function') onended(audio); };
      audio.onerror=()=>{ setPlaying(false); if(typeof onerror==='function') onerror(audio); };
    },
    toggle(){
      if(audio.paused){
        stopAllAudio(audioId);
        audio.play().then(()=>setPlaying(true)).catch(()=>{});
      }else{
        audio.pause();
        setPlaying(false);
      }
    },
    play(){
      stopAllAudio(audioId);
      return audio.play().then(()=>setPlaying(true));
    },
    seek(val){
      if(!audio.duration) return;
      const pct=clamp(Number(val)||0,0,100);
      audio.currentTime=pct/100*audio.duration;
    },
    skip(seconds){
      if(!audio.duration) return;
      audio.currentTime=clamp(audio.currentTime+seconds,0,audio.duration);
    },
    cleanup({clearSrc=true}={}){
      audio.pause();
      if(clearSrc) audio.src='';
      audio.onloadedmetadata=null;
      audio.ontimeupdate=null;
      audio.onended=null;
      audio.onerror=null;
      if(slider) slider.value=0;
      if(curEl) curEl.textContent='0:00';
      if(durEl) durEl.textContent='—';
      if(statusEl) statusEl.textContent='';
      setPlaying(false);
    },
    syncDuration,
    syncProgress,
    setStatus(msg){ if(statusEl) statusEl.textContent=msg; },
    setPlaying
  };
}

let _confirmResolver=null;
const _modalLifecycles={};

function registerModalLifecycle(id, callbacks={}){
  _modalLifecycles[id]={...(_modalLifecycles[id]||{}), ...callbacks};
  const overlay=document.getElementById(id);
  if(!overlay || overlay.dataset.modalLifecycleBound==='1') return;
  overlay.addEventListener('click',(event)=>{
    if(event.target!==overlay) return;
    const lifecycle=_modalLifecycles[id]||{};
    if(typeof lifecycle.requestClose==='function') lifecycle.requestClose();
    else closeModal(id);
  });
  overlay.dataset.modalLifecycleBound='1';
}

function openModal(id){
  const modal=document.getElementById(id);
  if(!modal) return;
  modal.classList.add('open');
  const lifecycle=_modalLifecycles[id]||{};
  if(typeof lifecycle.onOpen==='function') lifecycle.onOpen(modal);
}

function closeModal(id){
  const modal=document.getElementById(id);
  if(!modal) return;
  modal.classList.remove('open');
  const lifecycle=_modalLifecycles[id]||{};
  if(typeof lifecycle.onClose==='function') lifecycle.onClose(modal);
}

function topOpenModalId(){
  const overlays=[...document.querySelectorAll('.modal-overlay.open')];
  if(!overlays.length) return '';
  return overlays[overlays.length-1].id||'';
}

document.addEventListener('keydown',(event)=>{
  if(event.key!=='Escape') return;
  const id=topOpenModalId();
  if(!id) return;
  const lifecycle=_modalLifecycles[id]||{};
  event.preventDefault();
  if(typeof lifecycle.requestClose==='function') lifecycle.requestClose();
  else closeModal(id);
});

function runModalMediaCleanup(...cleanupFns){
  cleanupFns.forEach(fn=>{
    if(typeof fn!=='function') return;
    try{ fn(); }catch(_err){}
  });
  stopAllAudio();
}

function toSafeConfirmUrl(rawUrl){
  try{
    const parsed=new URL(String(rawUrl||''), window.location.origin);
    if(['http:','https:','mailto:'].includes(parsed.protocol)) return parsed.href;
  }catch(_err){
    return '';
  }
  return '';
}

function sanitizeConfirmHtml(input){
  const parser=new DOMParser();
  const doc=parser.parseFromString(String(input||''),'text/html');
  const allowedTags=new Set(['BR','A','STRONG','EM','CODE']);

  function sanitizeNode(node){
    if(node.nodeType===Node.TEXT_NODE) return document.createTextNode(node.textContent||'');
    if(node.nodeType!==Node.ELEMENT_NODE) return document.createDocumentFragment();

    const tag=node.tagName.toUpperCase();
    const frag=document.createDocumentFragment();
    Array.from(node.childNodes).forEach(child=>frag.appendChild(sanitizeNode(child)));

    if(!allowedTags.has(tag)) return frag;
    if(tag==='BR') return document.createElement('br');

    const safeEl=document.createElement(tag.toLowerCase());
    if(tag==='A'){
      const safeHref=toSafeConfirmUrl(node.getAttribute('href')||'');
      if(safeHref){
        safeEl.setAttribute('href',safeHref);
        safeEl.setAttribute('target','_blank');
        safeEl.setAttribute('rel','noopener noreferrer');
      }
    }

    safeEl.appendChild(frag);
    return safeEl;
  }

  const out=document.createDocumentFragment();
  Array.from(doc.body.childNodes).forEach(node=>out.appendChild(sanitizeNode(node)));
  return out;
}

function renderConfirmText(parent,text){
  const content=String(text||'');
  if(!content) return;
  const parts=content.split('\n');
  parts.forEach((part,idx)=>{
    if(idx>0) parent.appendChild(document.createElement('br'));
    parent.appendChild(document.createTextNode(part));
  });
}

function renderConfirmMessage(msgEl,message){
  if(!msgEl) return;
  msgEl.replaceChildren();

  const payload=(message&&typeof message==='object'&&!Array.isArray(message))?message:{text:message||''};
  if(payload.text){
    const textBlock=document.createElement('div');
    renderConfirmText(textBlock,payload.text);
    msgEl.appendChild(textBlock);
  }

  if(Array.isArray(payload.fields)){
    payload.fields.forEach(field=>{
      if(!field||(!field.value&&field.value!==0)) return;
      const row=document.createElement('div');
      row.style.marginTop='10px';

      if(field.label){
        const label=document.createElement('div');
        label.style.fontSize='11px';
        label.style.color='var(--text3)';
        label.textContent=String(field.label);
        row.appendChild(label);
      }

      if(field.type==='url'){
        const safeHref=toSafeConfirmUrl(field.value);
        const link=document.createElement('a');
        link.style.wordBreak='break-all';
        if(safeHref){
          link.href=safeHref;
          link.target='_blank';
          link.rel='noopener noreferrer';
          link.textContent=String(field.value);
        }else{
          link.textContent=String(field.value);
        }
        row.appendChild(link);
      }else{
        const value=document.createElement('div');
        value.textContent=String(field.value);
        if(field.mono){
          value.style.fontFamily='var(--mono)';
          value.style.fontSize='11px';
          value.style.wordBreak='break-all';
        }
        row.appendChild(value);
      }
      msgEl.appendChild(row);
    });
  }

  if(payload.html){
    const htmlBlock=document.createElement('div');
    htmlBlock.style.marginTop='10px';
    htmlBlock.appendChild(sanitizeConfirmHtml(payload.html));
    msgEl.appendChild(htmlBlock);
  }
}

function openConfirmModal(title, message, okLabel='Confirm'){
  const titleEl=document.getElementById('confirm-modal-title');
  const msgEl=document.getElementById('confirm-modal-message');
  const okBtn=document.getElementById('confirm-modal-ok');
  if(titleEl) titleEl.textContent=title||'Confirm Action';
  renderConfirmMessage(msgEl,message);
  if(okBtn) okBtn.textContent=okLabel||'Confirm';
  openModal('confirm-modal');
  return new Promise(resolve=>{
    _confirmResolver=resolve;
    okBtn.onclick=()=>{ closeModal('confirm-modal'); _confirmResolver=null; resolve(true); };
  });
}
function closeConfirmModal(){
  closeModal('confirm-modal');
  if(_confirmResolver){ const r=_confirmResolver; _confirmResolver=null; r(false); }
}

// ── SECTION: CONFIG / API HELPERS ─────────────────────────────────────────────
const _rawFetch = window.fetch.bind(window);
let _authPrompted = false;
let _configCache=null;

function getApiToken(){ return localStorage.getItem('mt-api-token') || ''; }
function apiUrl(url){
  const token = getApiToken();
  if(!token || typeof url!=='string' || !url.startsWith('/api/')) return url;
  const sep = url.includes('?') ? '&' : '?';
  return url + sep + 'token=' + encodeURIComponent(token);
}
function jsonRequestOptions(method, body, init={}){
  const headers=new Headers(init.headers||{});
  if(body!==undefined && !headers.has('Content-Type')) headers.set('Content-Type','application/json');
  return {
    ...init,
    method,
    headers,
    body: body===undefined ? init.body : JSON.stringify(body)
  };
}
async function requestJson(url, init={}){
  const response=await fetch(url, init);
  let data={};
  try{ data=await response.json(); }catch(_err){ data={}; }
  return {ok:response.ok && data?.ok!==false, response, data};
}
async function postJson(url, body, init={}){
  return requestJson(url, jsonRequestOptions('POST', body, init));
}
async function patchJson(url, body, init={}){
  return requestJson(url, jsonRequestOptions('PATCH', body, init));
}
async function loadConfig(force=false){
  if(!force && _configCache) return _configCache;
  const {data}=await requestJson('/api/config');
  _configCache=(data && typeof data==='object')?data:{};
  return _configCache;
}
function rememberConfigPatch(patch={}){
  _configCache={...(_configCache||{}), ...patch};
  return _configCache;
}
let _dashboardLibraries={enabled:[],scheduled:[]};
function dashboardQuickLibraries(){
  const scheduled=(_dashboardLibraries.scheduled||[]).filter(Boolean);
  const enabled=(_dashboardLibraries.enabled||[]).filter(Boolean);
  return scheduled.length ? scheduled : enabled;
}
function dashboardBadgeClass(state){
  return {ok:'ok',warning:'warn',warn:'warn',off:'off',error:'fail',fail:'fail',unknown:'unknown'}[state] || 'off';
}
function dashboardStatusBadge(label,state='off'){
  return `<span class="dashboard-badge ${dashboardBadgeClass(state)}"><span class="status-label">${formatStatusLabel(label)}</span></span>`;
}
function formatDashboardTime(value){
  if(!value) return null;
  const safe=String(value).replace(' ','T');
  const dt=new Date(safe);
  if(Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString();
}
function _fmtCountdown(ms){
  if(ms<=0) return 'Now';
  const totalSec=Math.floor(ms/1000);
  const h=Math.floor(totalSec/3600);
  const m=Math.floor((totalSec%3600)/60);
  const s=totalSec%60;
  const pad=n=>String(n).padStart(2,'0');
  if(h>0) return `${h}h ${pad(m)}m ${pad(s)}s`;
  if(m>0) return `${pad(m)}m ${pad(s)}s`;
  return `${pad(s)}s`;
}
function formatNextRun(isoStr){
  if(!isoStr) return null;
  const dt=new Date(isoStr);
  if(Number.isNaN(dt.getTime())) return isoStr;
  return _fmtCountdown(dt-new Date());
}
function formatNextRunFull(isoStr){
  if(!isoStr) return null;
  const dt=new Date(isoStr);
  if(Number.isNaN(dt.getTime())) return null;
  const rel=formatNextRun(isoStr)||'Now';
  const abs=dt.toLocaleString([],{weekday:'short',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',timeZoneName:'short'});
  return {rel,abs,dt};
}
// Live countdown wiring
let _countdownInterval=null;
let _countdownTargetDt=null;
let _globalRunStatusTimer=null;
let _globalRunStatusInFlight=false;
const _idleRunStatusPollMs=30000;
const _activeRunStatusPollMs=4000;
const _liveRunStatusPages=new Set(['dashboard','theme-manager','tasks','scheduler']);
const _countdownPages=new Set(['dashboard','scheduler']);
function _activePageName(){
  const active=document.querySelector('.page.active');
  return active ? String(active.id||'').replace(/^page-/,'') : '';
}
function _pageNeedsLiveRunState(page=_activePageName()){
  return _liveRunStatusPages.has(page);
}
function _pageNeedsCountdown(page=_activePageName()){
  return _countdownPages.has(page);
}
function _syncCountdownTimer(){
  // Static display only — no countdown interval
  if(!_countdownTargetDt){
    const schedWrap=document.getElementById('sched-cron-next');
    if(schedWrap && _activePageName()!=='scheduler') schedWrap.style.display='none';
    return;
  }
  _updateStaticNextRun();
}
function _startCountdowns(isoStr){
  if(!isoStr){_countdownTargetDt=null;_stopCountdownDisplay();return;}
  const dt=new Date(isoStr);
  if(Number.isNaN(dt.getTime())){_countdownTargetDt=null;_stopCountdownDisplay();return;}
  _countdownTargetDt=dt;
  _syncCountdownTimer();
}
function _updateStaticNextRun(){
  if(!_countdownTargetDt) return;
  const absStr=_countdownTargetDt.toLocaleString([],{weekday:'short',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',timeZoneName:'short'});
  // Scheduler static next run
  const schedAbs=document.getElementById('sched-cron-next-abs');
  if(schedAbs) schedAbs.textContent=absStr;
  const schedWrap=document.getElementById('sched-cron-next');
  if(schedWrap) schedWrap.style.display='';
}
function _tickCountdowns(){
  // Kept for compatibility — now just calls static update
  _updateStaticNextRun();
}
function _stopCountdownDisplay(){
  const schedWrap=document.getElementById('sched-cron-next');
  if(schedWrap) schedWrap.style.display='none';
  if(_countdownInterval){clearInterval(_countdownInterval);_countdownInterval=null;}
}
function _syncGlobalRunStatusPolling({immediate=false}={}){
  const shouldPoll=_pageNeedsLiveRunState() || _wasRunning;
  if(!shouldPoll){
    if(_globalRunStatusTimer){clearTimeout(_globalRunStatusTimer);_globalRunStatusTimer=null;}
    return;
  }
  const nextDelay=_wasRunning ? _activeRunStatusPollMs : _idleRunStatusPollMs;
  if(immediate){
    if(_globalRunStatusTimer){clearTimeout(_globalRunStatusTimer);_globalRunStatusTimer=null;}
    updateGlobalRunStatus();
    return;
  }
  if(_globalRunStatusTimer) return;
  _globalRunStatusTimer=setTimeout(()=>{
    _globalRunStatusTimer=null;
    updateGlobalRunStatus();
  }, nextDelay);
}
function openThemeManagerFiltered(status=''){
  showPage('theme-manager');
  setTimeout(()=>{
    clearDbFilter();
    if(status) filterByStatus(status);
  }, 300);
}
function scrollToSection(sectionId){
  if(!sectionId) return;
  const el=document.getElementById(sectionId);
  if(!el) return;
  setTimeout(()=>{
    el.scrollIntoView({behavior:'smooth',block:'start'});
    el.classList.remove('section-flash');
    void el.offsetWidth; // reflow to restart animation
    el.classList.add('section-flash');
    setTimeout(()=>el.classList.remove('section-flash'),2000);
  },220);
}
function navigateTo(page,section=''){
  showPage(page);
  if(section) scrollToSection(section);
}
function dashboardOpenTarget(target={}){
  if(target.page==='configuration') return navigateTo('configuration',target.section||'');
  if(target.page==='tasks') return showPage('tasks');
  if(target.page==='scheduler') return navigateTo('scheduler',target.section||'');
  if(target.page==='schedule') return navigateTo('scheduler',target.section||'');
  if(target.page==='theme-manager' || target.page==='database') return openThemeManagerFiltered(target.filter||'');
  return showPage('dashboard');
}
async function dashboardImportGoldenSource(){
  showPage('theme-manager');
  setTimeout(async()=>{
    if(!_activeLib) await loadDatabase();
    importGoldenSource();
  }, 350);
}
async function dashboardRunQuick(passNum){
  const libraries=dashboardQuickLibraries();
  if(!libraries.length) return toast('Enable at least one library first','err');
  return startPipelineRun(passNum,'run',{libraries,scopeLabel:_formatScopeLabel(libraries,'enabled libraries'),callerSurface:'dashboard'});
}
function dashboardShowPipelineHelp(){
  document.getElementById('pipeline-help-modal').classList.add('open');
}
function renderDashboardPipelineOverview(counts){
  const items=[
    {label:'Missing', sub:'Needs source', status:'MISSING', value:counts.MISSING||0, color:'var(--red)'},
    {label:'Staged', sub:'Review queue', status:'STAGED', value:counts.STAGED||0, color:'var(--purple)'},
    {label:'Approved', sub:'Ready to download', status:'APPROVED', value:counts.APPROVED||0, color:'var(--yellow)'},
    {label:'Available', sub:'Already local', status:'AVAILABLE', value:counts.AVAILABLE||0, color:'var(--green)'},
    {label:'Failed', sub:'Needs review', status:'FAILED', value:counts.FAILED||0, color:'var(--red)'},
  ];
  const el=document.getElementById('dashboard-pipeline-overview');
  if(!el) return;
  el.innerHTML=items.map(item=>`
    <button class="dashboard-stat" onclick="openThemeManagerFiltered('${item.status}')">
      <span class="dashboard-stat-count" style="color:${item.color}">${item.value}</span>
      <span class="dashboard-stat-label"><span class="dashboard-stat-dot" style="background:${item.color}"></span>${displayStatus(item.status)}</span>
      <span class="dashboard-stat-sub">${item.sub}</span>
    </button>`).join('');
}
function renderDashboardSystemHealth(health){
  const el=document.getElementById('dashboard-system-health');
  if(!el) return;
  const rows=[
    {label:'Plex', key:'plex'},
    {label:'TMDB API', key:'tmdb'},
    {label:'Golden Source', key:'golden_source'},
    {label:'Download Toolchain', key:'toolchain'},
    {label:'Storage', key:'storage'},
    {label:'Database', key:'database'},
    {label:'Libraries', key:'libraries'},
  ];
  el.innerHTML='<div style="display:grid;gap:6px">'+rows.map(row=>{
    const h=health[row.key]||{state:'unknown',label:'Unknown'};
    const badge=dashboardStatusBadge(h.label, h.state);
    const detail=h.detail||'';
    return `<div class="dash-health-row">
      <span class="dash-health-label">${row.label}</span>
      ${detail ? `<span class="dash-health-detail" title="${detail.replace(/"/g,'&quot;')}">${detail}</span>` : ''}
      ${badge}
    </div>`;
  }).join('')+'</div>';
  const noteEl=document.getElementById('dashboard-health-note');
  const validation=health?.validation||{};
  if(noteEl){
    noteEl.textContent=validation.detail || (_dashboardHealthMode==='full'
      ? 'All dashboard integrations were checked live.'
      : 'Cached or placeholder status is shown until you run Refresh.');
  }
  const refreshBtn=document.getElementById('dashboard-health-refresh');
  if(refreshBtn){
    refreshBtn.textContent=_dashboardHealthLoading ? 'Refreshing…' : 'Refresh';
    refreshBtn.disabled=_dashboardHealthLoading;
  }
}
function renderDashboardSchedule(health){
  // Schedule section replaced by Action Station — delegate to it
  renderDashboardActionStation(health);
}
function renderDashboardDeferredPlaceholders(cfg, enabledLibs, scheduledLibs){
  renderDashboardPipelineOverview({MISSING:0,STAGED:0,APPROVED:0,AVAILABLE:0,FAILED:0});
  renderDashboardNeedsAttention(cfg, {MISSING:0,STAGED:0,APPROVED:0,AVAILABLE:0,FAILED:0}, enabledLibs);
  renderDashboardRecentActivity([]);
  renderDashboardLibraryOverview(cfg, enabledLibs, scheduledLibs);
  renderDashLibTabs(enabledLibs);
  renderHistStatusFilters();
  renderBarDaysInput();
  renderBarChart();
  renderPieChart();
  const healthCache=readDashboardHealthCache();
  if(healthCache){
    _dashboardHealthMode=healthCache?.validation?.mode || 'lite';
    renderDashboardSystemHealth(healthCache);
    renderDashboardSchedule(healthCache);
  }else{
    _dashboardHealthMode='lite';
    const healthEl=document.getElementById('dashboard-system-health');
    if(healthEl){
      healthEl.innerHTML='<div class="dashboard-empty">No live health check has run yet. Use Validate to fetch the latest system health.</div>';
    }
    const noteEl=document.getElementById('dashboard-health-note');
    if(noteEl) noteEl.textContent='Cached or placeholder status is shown until you run Refresh.';
    const refreshBtn=document.getElementById('dashboard-health-refresh');
    if(refreshBtn){
      refreshBtn.disabled=false;
      refreshBtn.textContent='Validate';
    }
  }
}
function renderDashboardNeedsAttention(cfg, counts, enabledLibs){
  const items=[];
  const plexOk=(cfg.plex_url||'').trim() && (cfg.plex_token||'').trim();
  const tmdbOk=(cfg.tmdb_api_key||'').trim();
  if(!plexOk) items.push({title:'Plex not configured', detail:'Connect Plex to enable library scanning and indexing.', cta:'Configure', target:{page:'configuration',section:'config-section-connections'}});
  if(!tmdbOk) items.push({title:'TMDB API not configured', detail:'Add your TMDB API key to enable metadata matching.', cta:'Configure', target:{page:'configuration',section:'config-section-connections'}});
  if(!enabledLibs.length) items.push({title:'No libraries enabled', detail:'Enable at least one movie or TV library to start processing.', cta:'Add Libraries', target:{page:'configuration',section:'config-section-libraries'}});
  if((counts.MISSING||0)>0) items.push({title:`${counts.MISSING} missing title${counts.MISSING===1?'':'s'} need source discovery`, detail:'Run Find Sources to search for theme matches.', cta:'Open Missing', target:{page:'theme-manager',filter:'MISSING'}});
  if((counts.FAILED||0)>0) items.push({title:`${counts.FAILED} failed title${counts.FAILED===1?'':'s'} need review`, detail:'Inspect failures and update sources or retry.', cta:'Review Failed', target:{page:'theme-manager',filter:'FAILED'}});
  const el=document.getElementById('dashboard-needs-attention');
  if(!el) return;
  if(!items.length){
    el.innerHTML='<div class="dashboard-empty">Nothing needs attention right now.</div>';
    return;
  }
  const SHOW_MAX=3;
  const hasMore=items.length>SHOW_MAX;
  const itemsHtml=items.map((item,i)=>`
    <div class="dashboard-attention-item${i>=SHOW_MAX?' dash-attention-extra hidden':''}">
      <div class="dashboard-attention-copy">
        <div class="dashboard-main-copy">${item.title}</div>
        <div class="dashboard-sub-copy">${item.detail}</div>
      </div>
      <button class="btn btn-amber btn-sm" style="flex-shrink:0" onclick='dashboardOpenTarget(${JSON.stringify(item.target)})'>${item.cta}</button>
    </div>`).join('');
  const toggleHtml=hasMore?`
    <button class="btn btn-ghost btn-xs dash-attention-toggle" onclick="toggleDashboardAttention(this,${items.length})">View all ${items.length} issues</button>`:'';
  el.innerHTML=`<div style="display:grid;gap:6px">${itemsHtml}</div>${toggleHtml}`;
}
function toggleDashboardAttention(btn,total){
  const container=btn.closest('#dashboard-needs-attention');
  const extras=container.querySelectorAll('.dash-attention-extra');
  const expanded=btn.dataset.expanded==='1';
  extras.forEach(el=>el.classList.toggle('hidden',expanded));
  btn.dataset.expanded=expanded?'0':'1';
  btn.textContent=expanded?`View all ${total} issues`:'Show less';
}
function renderDashboardActionStation(health){
  const el=document.getElementById('dashboard-action-station');
  if(!el) return;
  const s=health.schedule||{state:'unknown',label:'Unknown',next_run:null};
  const scheduleActive=s.state==='ok'||s.state==='warning'||s.state==='warn';
  const scheduleDetail=s.detail?`<div style="font-size:11px;color:var(--text3);margin-top:6px">${s.detail}</div>`:'';
  // Next run block (static display, no countdown)
  let nextRunHtml='';
  const nrFull=formatNextRunFull(s.next_run);
  if(nrFull){
    nextRunHtml=`<div class="dash-station-next-run">
      <div class="dash-station-next-label">Next Scheduled Run</div>
      <div class="dash-station-next-rel">${nrFull.abs}</div>
      ${scheduleDetail}
    </div>`;
  } else if(scheduleActive){
    nextRunHtml=`<div class="dash-station-next-run">
      <div class="dash-station-next-label">Automation</div>
      <div style="margin-top:3px">${dashboardStatusBadge(s.label,s.state)}</div>
      ${scheduleDetail}
    </div>`;
  } else {
    nextRunHtml=`<div class="dash-station-next-run" style="border-style:dashed">
      <div class="dash-station-next-label">Automation</div>
      <div style="font-size:11px;color:var(--text3);margin-top:2px">No schedule configured</div>
      ${scheduleDetail}
    </div>`;
  }
  const setupBtn=`<button class="btn btn-ghost btn-sm btn-action-setup" onclick="navigateTo('scheduler','scheduler-config-section')">Setup Schedule</button>`;
  const runOrStopBtn=_dashRunActive
    ?`<button class="btn btn-red btn-sm btn-action-stop" id="dash-run-btn" onclick="stopRun('run')">Stop</button>`
    :`<button class="btn btn-green btn-sm btn-action-run" id="dash-run-btn" onclick="startScheduledRun('dashboard')">Run Schedule</button>`;
  const themesBtn=`<button class="btn btn-ghost btn-sm btn-action-themes" onclick="showPage('theme-manager')">Manage Themes</button>`;
  el.innerHTML=nextRunHtml+`<div class="dash-action-buttons">${setupBtn}${runOrStopBtn}${themesBtn}</div>`;
}
function renderDashboardActionStationFromConfig(cfg, scheduledLibs){
  renderDashboardActionStation({
    schedule:{
      state:cfg.schedule_enabled ? (scheduledLibs.length ? 'ok' : 'warning') : 'off',
      label:cfg.schedule_enabled ? (scheduledLibs.length ? 'Configured' : 'No libraries selected') : 'Disabled',
      next_run:null,
      detail:cfg.schedule_enabled
        ? `Scheduled for ${scheduledLibs.length} librar${scheduledLibs.length===1?'y':'ies'}. Quick status will calculate the next run shortly.`
        : 'Enable automation in Scheduler to run this pipeline automatically.',
    }
  });
}
function _updateDashRunButton(){
  const btn=document.getElementById('dash-run-btn');
  if(!btn) return;
  if(_dashRunActive){
    btn.textContent='Stop';
    btn.className='btn btn-red btn-sm btn-action-stop';
    btn.style.cssText='';
    btn.onclick=function(){stopRun('run');};
  } else {
    btn.textContent='Run Schedule';
    btn.className='btn btn-green btn-sm btn-action-run';
    btn.style.cssText='';
    btn.onclick=function(){startScheduledRun('dashboard');};
  }
}
function dashboardLatestTask(entries, matcher){
  return (entries||[]).find(entry=>matcher((entry.task||''), entry)) || null;
}
function formatRunSummary(entry){
  if(!entry) return 'Not run yet';
  const pass=(entry.details||{}).pass;
  const stats=(entry.details||{}).stats||{};
  if(pass===1 && stats.pass1){
    const s=stats.pass1; const parts=[];
    if(s.total!=null) parts.push(`${s.total} total`);
    if(s.has_theme!=null) parts.push(`${s.has_theme} available`);
    if(s.missing!=null) parts.push(`${s.missing} missing`);
    if(parts.length) return parts.join(' · ');
  }
  if(pass===2 && stats.pass2){
    const s=stats.pass2; const parts=[];
    if(s.staged!=null) parts.push(`${s.staged} staged`);
    if(s.missing!=null) parts.push(`${s.missing} missing`);
    if(s.failed!=null&&s.failed>0) parts.push(`${s.failed} failed`);
    if(parts.length) return parts.join(' · ');
  }
  if(pass===3 && stats.pass3){
    const s=stats.pass3; const parts=[];
    if(s.available!=null) parts.push(`${s.available} downloaded`);
    if(s.skipped!=null&&s.skipped>0) parts.push(`${s.skipped} skipped`);
    if(s.failed!=null&&s.failed>0) parts.push(`${s.failed} failed`);
    if(parts.length) return parts.join(' · ');
  }
  return entry.summary||entry.task||'Completed';
}
function renderDashboardRecentActivity(activity){
  const recent=Array.isArray(activity)
    ? {
      scan: dashboardLatestTask(activity, (task,entry)=>/scan/i.test(task)||entry?.details?.pass===1),
      discover: dashboardLatestTask(activity, (task,entry)=>/find sources|source discovery/i.test(task)||entry?.details?.pass===2),
      download: dashboardLatestTask(activity, (task,entry)=>/download/i.test(task)||entry?.details?.pass===3),
      task: dashboardLatestTask(activity, ()=>true),
    }
    : {
      scan: activity?.scan||null,
      discover: activity?.discover||null,
      download: activity?.download||null,
      task: activity?.task||null,
    };
  const items=[
    {label:'Last scan', entry:recent.scan},
    {label:'Source discovery', entry:recent.discover},
    {label:'Download run', entry:recent.download},
    {label:'Task run', entry:recent.task},
  ];
  const el=document.getElementById('dashboard-recent-activity');
  if(!el) return;
  el.innerHTML=`<div style="display:grid;gap:6px">`+items.map(item=>{
    const entry=item.entry;
    const statusState=entry?(entry.status==='success'?'ok':entry.status==='stopped'?'off':'fail'):'off';
    const statusLabel=entry?formatStatusLabel(entry.status||'success'):'None';
    const badge=dashboardStatusBadge(statusLabel, statusState);
    const timeStr=entry?formatDashboardTime(entry.time):null;
    const summary=formatRunSummary(entry);
    const hasEntry=!!entry;
    return `<div class="dash-activity-row${hasEntry?' clickable':''}"${hasEntry?' tabindex="0" role="button" aria-label="View '+item.label+' details" onclick="navigateToActivity(\''+item.label+'\')" onkeydown="if(event.key===\'Enter\'||event.key===\' \'){event.preventDefault();navigateToActivity(\''+item.label+'\')}"':''}>
      ${badge}
      <span class="dash-activity-label">${item.label}</span>
      <span class="dash-activity-time">${timeStr||'—'}</span>
      <span class="dash-activity-summary">${summary}</span>
    </div>`;
  }).join('')+`</div>`;
}
function navigateToActivity(label){
  showPage('tasks');
  setTimeout(()=>{
    const keyword={'Last scan':'scan','Source discovery':'find sources','Download run':'download','Task run':''}[label]||'';
    if(!keyword) return;
    const cards=document.querySelectorAll('.task-activity-card');
    for(const card of cards){
      if(card.textContent.toLowerCase().includes(keyword)){
        card.scrollIntoView({behavior:'smooth',block:'center'});
        card.classList.remove('section-flash');
        void card.offsetWidth;
        card.classList.add('section-flash');
        setTimeout(()=>card.classList.remove('section-flash'),2000);
        break;
      }
    }
  },500);
}
// ── Dashboard chart tooltip ───────────────────────────────────────────────────
let _dashTooltipEl=null;
function _ensureDashTooltip(){
  if(_dashTooltipEl) return _dashTooltipEl;
  _dashTooltipEl=document.createElement('div');
  _dashTooltipEl.className='dash-chart-tooltip';
  document.body.appendChild(_dashTooltipEl);
  return _dashTooltipEl;
}
function _showDashTooltip(html,evt){
  const el=_ensureDashTooltip();
  el.innerHTML=html;
  el.classList.add('visible');
  const rect=el.getBoundingClientRect();
  let x=evt.clientX+12, y=evt.clientY-10;
  if(x+rect.width>window.innerWidth-8) x=evt.clientX-rect.width-12;
  if(y+rect.height>window.innerHeight-8) y=evt.clientY-rect.height-10;
  if(y<4) y=4;
  el.style.left=x+'px';
  el.style.top=y+'px';
}
function _hideDashTooltip(){
  if(_dashTooltipEl) _dashTooltipEl.classList.remove('visible');
}
// ── Dashboard library section + processing history ────────────────────────────
let _dashSummaryByLib={};
let _dashSummaryOverall={MISSING:0,STAGED:0,APPROVED:0,AVAILABLE:0,FAILED:0,UNMONITORED:0};
let _dashSelectedLib='all';
let _dashHistStatusFilter='all';
let _dashboardLoadSeq=0;
let _dashboardHealthRequestSeq=0;
let _dashboardHealthMode='lite';
let _dashboardHealthLoading=false;
const _dashboardHealthStorageKey='dashboard.health.cache';
function readDashboardHealthCache(){
  try{
    const raw=window.localStorage ? window.localStorage.getItem(_dashboardHealthStorageKey) : null;
    if(!raw) return null;
    const parsed=JSON.parse(raw);
    return parsed && typeof parsed==='object' ? parsed : null;
  }catch(_err){
    return null;
  }
}
function writeDashboardHealthCache(health){
  try{
    if(window.localStorage && health && typeof health==='object'){
      window.localStorage.setItem(_dashboardHealthStorageKey, JSON.stringify(health));
    }
  }catch(_err){}
}

function emptyDashboardCounts(){
  return {MISSING:0,STAGED:0,APPROVED:0,AVAILABLE:0,FAILED:0,UNMONITORED:0};
}
function dashboardCountsForLibrary(name){
  if(name==='all') return {...emptyDashboardCounts(), ...(_dashSummaryOverall||{})};
  return {...emptyDashboardCounts(), ...(_dashSummaryByLib[name]||{})};
}

// How it Works? panel — always starts collapsed, ephemeral state
let _howItWorksOpen=false;
function toggleHowItWorks(){
  _howItWorksOpen=!_howItWorksOpen;
  _applyHowItWorksState();
  if(_howItWorksOpen){
    setTimeout(()=>document.addEventListener('click',_howItWorksOutsideClick,true),0);
    document.addEventListener('keydown',_howItWorksEscape);
  }
}
function _applyHowItWorksState(){
  const content=document.getElementById('dash-how-content');
  const chev=document.getElementById('dash-pipeline-help-chev');
  const btn=document.getElementById('dash-how-toggle');
  if(content) content.style.display=_howItWorksOpen?'':'none';
  if(chev) chev.style.transform=_howItWorksOpen?'':'rotate(180deg)';
  if(btn) btn.setAttribute('aria-expanded',String(_howItWorksOpen));
}
function closeHowItWorks(){
  if(!_howItWorksOpen) return;
  _howItWorksOpen=false;
  _applyHowItWorksState();
  document.removeEventListener('click',_howItWorksOutsideClick,true);
  document.removeEventListener('keydown',_howItWorksEscape);
}
function _howItWorksOutsideClick(e){
  const panel=document.getElementById('dash-pipeline-help');
  if(panel && !panel.contains(e.target)) closeHowItWorks();
}
function _howItWorksEscape(e){
  if(e.key==='Escape') closeHowItWorks();
}
function _resetHowItWorks(){
  _howItWorksOpen=false;
  _applyHowItWorksState();
  document.removeEventListener('click',_howItWorksOutsideClick,true);
  document.removeEventListener('keydown',_howItWorksEscape);
}
// Toggle secret field visibility (API keys)
function toggleSecretField(btn){
  const wrap=btn.closest('.field-secret-wrap');
  const input=wrap?.querySelector('input');
  if(!input) return;
  const isPassword=input.type==='password';
  input.type=isPassword?'text':'password';
  const eyeOn=btn.querySelector('.eye-icon');
  const eyeOff=btn.querySelector('.eye-off-icon');
  if(eyeOn) eyeOn.style.display=isPassword?'none':'';
  if(eyeOff) eyeOff.style.display=isPassword?'':'none';
}

function renderDashLibTabs(enabledLibs){
  const el=document.getElementById('dash-lib-tabs');
  if(!el) return;
  const tabs=[{name:'all',label:'All'},...(enabledLibs||[]).map(l=>({name:l.name||l,label:l.name||l}))];
  el.innerHTML=tabs.map(t=>`<button type="button" class="dash-lib-tab${t.name===_dashSelectedLib?' active':''}" onclick="switchDashLib(${JSON.stringify(t.name)})">${t.label}</button>`).join('');
}

function renderHistStatusFilters(){
  const el=document.getElementById('dash-hist-status-filters');
  if(!el) return;
  const filters=[
    {k:'all',label:'All',c:'var(--text2)'},
    {k:'AVAILABLE',label:'Available',c:'var(--green)'},
    {k:'STAGED',label:'Staged',c:'var(--purple)'},
    {k:'APPROVED',label:'Approved',c:'var(--yellow)'},
    {k:'FAILED',label:'Failed',c:'var(--red)'},
    {k:'MISSING',label:'Missing',c:'var(--red)'},
  ];
  el.innerHTML=filters.map(f=>{
    const active=f.k===_dashHistStatusFilter;
    return `<button type="button" class="dash-lib-tab${active?' active':''}" style="${active?'border-color:'+f.c+';color:'+f.c+';background:transparent':''}" onclick="_setHistFilter(${JSON.stringify(f.k)})">${f.label}</button>`;
  }).join('');
}

function _setHistFilter(k){_dashHistStatusFilter=k;renderHistStatusFilters();renderBarChart();renderPieChart();}

function switchDashLib(name){
  _dashSelectedLib=name;
  renderDashLibTabs(Object.keys(_dashSummaryByLib).map(n=>({name:n})));
  renderBarChart();
  renderPieChart();
}

function renderDashLibKpi(){
  const el=document.getElementById('dash-lib-kpi');
  if(!el) return;
  const counts=dashboardCountsForLibrary(_dashSelectedLib);
  const items=[
    {label:'Missing',sub:'Needs source',status:'MISSING',value:counts.MISSING||0,color:'var(--red)'},
    {label:'Staged',sub:'Review queue',status:'STAGED',value:counts.STAGED||0,color:'var(--purple)'},
    {label:'Approved',sub:'Ready to download',status:'APPROVED',value:counts.APPROVED||0,color:'var(--yellow)'},
    {label:'Available',sub:'Already local',status:'AVAILABLE',value:counts.AVAILABLE||0,color:'var(--green)'},
    {label:'Failed',sub:'Needs review',status:'FAILED',value:counts.FAILED||0,color:'var(--red)'},
  ];
  el.innerHTML=items.map(item=>`
    <button class="dashboard-stat" onclick="openThemeManagerFiltered('${item.status}')">
      <span class="dashboard-stat-count" style="color:${item.color}">${item.value}</span>
      <span class="dashboard-stat-label status-label status-tone-${item.status}" style="color:${item.color}">${displayStatus(item.status)}</span>
      <span class="dashboard-stat-sub">${item.sub}</span>
    </button>`).join('');
}

// ── Bar chart — activity over time ────────────────────────────────────────────
let _dashTimelineData={};
let _dashBarDaysInput=30;
const BAR_STATUSES=['AVAILABLE','APPROVED','STAGED','MISSING','FAILED'];

function renderBarDaysInput(){
  const el=document.getElementById('dash-bar-timegroup');
  if(!el) return;
  el.innerHTML=`<div class="dash-bar-days-wrap"><span>Last</span><input type="number" min="1" max="365" value="${_dashBarDaysInput}" class="dash-bar-days-input" onchange="_setBarDays(this.value)" onkeydown="if(event.key==='Enter'){this.blur()}"><span>days</span></div>`;
}

function _setBarDays(v){
  const n=Math.max(1,Math.min(365,parseInt(v,10)||30));
  _dashBarDaysInput=n;
  renderBarChart();
}

function _aggregateTimeline(timeline, group, statusFilter){
  const buckets={};
  for(const [day, statuses] of Object.entries(timeline)){
    let key=day;
    if(group==='week'){
      const d=new Date(day+'T00:00:00');
      const jan1=new Date(d.getFullYear(),0,1);
      const weekNum=Math.ceil(((d-jan1)/86400000+jan1.getDay()+1)/7);
      key=d.getFullYear()+'-W'+String(weekNum).padStart(2,'0');
    } else if(group==='month'){
      key=day.slice(0,7);
    } else if(group==='year'){
      key=day.slice(0,4);
    }
    if(!buckets[key]) buckets[key]={};
    for(const [st,count] of Object.entries(statuses)){
      if(statusFilter!=='all' && st!==statusFilter) continue;
      if(!BAR_STATUSES.includes(st)) continue;
      buckets[key][st]=(buckets[key][st]||0)+count;
    }
  }
  return buckets;
}

function _formatBarLabel(key, group){
  if(group==='day') return key.slice(5);
  if(group==='week') return key;
  if(group==='month'){
    const parts=key.split('-');
    const months=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return months[parseInt(parts[1],10)-1]+' '+parts[0].slice(2);
  }
  return key;
}

function renderBarChart(){
  const el=document.getElementById('dash-bar-chart');
  if(!el) return;
  const timeline=_dashTimelineData||{};
  const N=_dashBarDaysInput||30;
  // Generate all day keys for the last N days (fill empty days)
  const today=new Date();
  const allDayKeys=[];
  for(let i=N-1;i>=0;i--){
    const d=new Date(today);
    d.setDate(d.getDate()-i);
    allDayKeys.push(d.toISOString().slice(0,10));
  }
  // Determine aggregation: daily, weekly, or monthly
  let group='day';
  if(N>180) group='month';
  else if(N>60) group='week';
  // Aggregate timeline data filtered to our date range
  const cutoff=allDayKeys[0];
  const filteredTimeline={};
  for(const [day, statuses] of Object.entries(timeline)){
    if(day>=cutoff) filteredTimeline[day]=statuses;
  }
  // Fill in empty days so chart is continuous
  allDayKeys.forEach(d=>{ if(!filteredTimeline[d]) filteredTimeline[d]={}; });
  const buckets=_aggregateTimeline(filteredTimeline, group, _dashHistStatusFilter);
  const keys=Object.keys(buckets).sort();
  if(!keys.length){
    el.innerHTML='<div class="dash-bar-empty">No data for the selected period.</div>';
    return;
  }
  // Calculate totals per status for legend
  const legendTotals={};
  BAR_STATUSES.forEach(st=>{ legendTotals[st]=0; });
  keys.forEach(k=>{
    BAR_STATUSES.forEach(st=>{ legendTotals[st]+=(buckets[k][st]||0); });
  });
  // Calculate max stacked height
  let maxTotal=0;
  keys.forEach(k=>{
    let t=0;
    BAR_STATUSES.forEach(st=>{ t+=(buckets[k][st]||0); });
    if(t>maxTotal) maxTotal=t;
  });
  if(!maxTotal){ el.innerHTML='<div class="dash-bar-empty">No activity in this period.</div>'; return; }
  // SVG dimensions
  const chartW=el.clientWidth||400;
  const chartH=200;
  const padL=40, padR=10, padT=10, padB=30;
  const plotW=chartW-padL-padR;
  const plotH=chartH-padT-padB;
  const barW=Math.max(4,Math.min(40,Math.floor(plotW/keys.length)-2));
  const gap=Math.max(1,(plotW-barW*keys.length)/(keys.length||1));
  // Y-axis scale
  const yScale=plotH/maxTotal;
  // Build bars
  let bars='';
  let labels='';
  keys.forEach((key,i)=>{
    const x=padL+i*(barW+gap)+gap/2;
    let y=padT+plotH;
    BAR_STATUSES.forEach(st=>{
      const val=buckets[key][st]||0;
      if(!val) return;
      const h=val*yScale;
      y-=h;
      bars+=`<rect x="${x}" y="${y}" width="${barW}" height="${h}" fill="${PIE_COLORS[st]}" opacity="0.85" rx="2" data-bar-key="${key}" data-bar-st="${st}" style="cursor:pointer"/>`;
    });
    // X label — show smartly based on bar count
    const lbl=_formatBarLabel(key,group);
    const maxLabels=Math.min(12,keys.length);
    const showLabel=keys.length<=maxLabels||(i%Math.ceil(keys.length/maxLabels)===0)||(i===keys.length-1);
    if(showLabel){
      labels+=`<text x="${x+barW/2}" y="${chartH-4}" text-anchor="middle" fill="var(--text3)" font-size="9" font-family="var(--mono)">${lbl}</text>`;
    }
  });
  // Y-axis gridlines
  let yLines='';
  const ySteps=4;
  for(let i=0;i<=ySteps;i++){
    const yVal=Math.round(maxTotal/ySteps*i);
    const yPos=padT+plotH-yVal*yScale;
    yLines+=`<line x1="${padL}" y1="${yPos}" x2="${chartW-padR}" y2="${yPos}" stroke="var(--border)" stroke-width="0.5"/>`;
    yLines+=`<text x="${padL-6}" y="${yPos+3}" text-anchor="end" fill="var(--text3)" font-size="9" font-family="var(--mono)">${yVal}</text>`;
  }
  // Legend with status totals
  const legendItems=BAR_STATUSES.filter(st=>legendTotals[st]>0).map(st=>{
    const label=st[0]+st.slice(1).toLowerCase();
    return `<div class="dash-pie-legend-item"><span class="dash-pie-legend-dot" style="background:${PIE_COLORS[st]}"></span>${label}: ${legendTotals[st]}</div>`;
  }).join('');
  el.innerHTML=`<svg width="100%" height="${chartH}" viewBox="0 0 ${chartW} ${chartH}" class="dash-bar-chart-svg">${yLines}${bars}${labels}</svg>${legendItems?'<div class="dash-pie-legend" style="margin-top:10px">'+legendItems+'</div>':''}`;
  // Tooltips
  el.querySelectorAll('[data-bar-key]').forEach(rect=>{
    rect.addEventListener('mousemove',function(e){
      const key=this.dataset.barKey;
      const bucket=buckets[key]||{};
      let total=0;
      BAR_STATUSES.forEach(st=>{ total+=(bucket[st]||0); });
      let rows='';
      BAR_STATUSES.forEach(st=>{
        const val=bucket[st]||0;
        if(!val) return;
        const label=st[0]+st.slice(1).toLowerCase();
        rows+=`<div class="dash-chart-tooltip-row"><span class="dash-chart-tooltip-dot" style="background:${PIE_COLORS[st]}"></span>${label}<span class="dash-chart-tooltip-val">${val}</span></div>`;
      });
      const html=`<div class="dash-chart-tooltip-title">${_formatBarLabel(key,group)}</div>${rows}<div class="dash-chart-tooltip-row" style="border-top:1px solid var(--border);margin-top:4px;padding-top:4px;color:var(--text3)">Total: ${total}</div>`;
      _showDashTooltip(html,e);
    });
    rect.addEventListener('mouseleave',_hideDashTooltip);
  });
}
// Pie chart — shows composition of selected library
const PIE_COLORS={MISSING:'#f26d78',STAGED:'#bb86ff',APPROVED:'#f4b43f',AVAILABLE:'#2dd4a0',FAILED:'#f05252',UNMONITORED:'#94a3b8'};
function renderPieChart(){
  const el=document.getElementById('dash-pie-chart');
  if(!el) return;
  const counts=dashboardCountsForLibrary(_dashSelectedLib);
  const PIE_STATUSES=['AVAILABLE','APPROVED','STAGED','MISSING','FAILED','UNMONITORED'];
  let total=0;
  PIE_STATUSES.forEach(st=>{ total+=(counts[st]||0); });
  if(!total){
    el.innerHTML='<div class="dash-pie-empty">No summary data</div>';
    return;
  }
  const r=90, cx=110, cy=110;
  let slices='', startAngle=-Math.PI/2;
  const legendItems=[];
  const _pieCounts=counts;
  const _pieTotal=total;
  PIE_STATUSES.forEach(st=>{
    if(!(counts[st]||0)) return;
    const pct=(counts[st]||0)/total;
    const angle=pct*2*Math.PI;
    const endAngle=startAngle+angle;
    const largeArc=angle>Math.PI?1:0;
    const x1=cx+r*Math.cos(startAngle), y1=cy+r*Math.sin(startAngle);
    const x2=cx+r*Math.cos(endAngle), y2=cy+r*Math.sin(endAngle);
    if(pct>=0.999){
      slices+=`<circle cx="${cx}" cy="${cy}" r="${r}" fill="${PIE_COLORS[st]}" opacity="0.85" data-pie-st="${st}" class="dash-pie-slice" style="cursor:pointer"/>`;
    } else {
      slices+=`<path d="M${cx},${cy} L${x1},${y1} A${r},${r} 0 ${largeArc} 1 ${x2},${y2} Z" fill="${PIE_COLORS[st]}" opacity="0.85" data-pie-st="${st}" class="dash-pie-slice" style="cursor:pointer"/>`;
    }
    startAngle=endAngle;
    legendItems.push(`<div class="dash-pie-legend-item"><span class="dash-pie-legend-dot" style="background:${PIE_COLORS[st]}"></span>${st[0]+st.slice(1).toLowerCase()}: ${counts[st]}</div>`);
  });
  el.innerHTML=`<svg width="220" height="220" viewBox="0 0 220 220">${slices}</svg><div class="dash-pie-legend">${legendItems.join('')}</div>`;
  // Full tooltip on hover — show ALL statuses
  el.querySelectorAll('[data-pie-st]').forEach(shape=>{
    shape.addEventListener('mousemove',function(e){
      const hoveredSt=this.dataset.pieSt;
      let rows='';
      PIE_STATUSES.forEach(st=>{
        const count=_pieCounts[st]||0;
        if(!count) return;
        const pct=Math.round(count/_pieTotal*100);
        const label=st[0]+st.slice(1).toLowerCase();
        const highlight=st===hoveredSt?'font-weight:700;color:var(--text)':'';
        rows+=`<div class="dash-chart-tooltip-row" style="${highlight}"><span class="dash-chart-tooltip-dot" style="background:${PIE_COLORS[st]}"></span>${label}<span class="dash-chart-tooltip-val">${count} (${pct}%)</span></div>`;
      });
      const html=`<div class="dash-chart-tooltip-title">Status Distribution</div>${rows}<div class="dash-chart-tooltip-row" style="border-top:1px solid var(--border);margin-top:4px;padding-top:4px;color:var(--text3)">Total: ${_pieTotal} items</div>`;
      _showDashTooltip(html,e);
    });
    shape.addEventListener('mouseleave',_hideDashTooltip);
  });
}

function renderDashboardLibraryOverview(cfg, enabledLibs, scheduledLibs){
  const allLibs=(cfg.libraries||[]).filter(lib=>(!lib.type||lib.type==='movie'||lib.type==='show'));
  const summaryEl=document.getElementById('dash-library-summary');
  const el=document.getElementById('dashboard-library-overview');
  if(summaryEl){
    const summaryParts=[`${enabledLibs.length} enabled`];
    if(scheduledLibs.length) summaryParts.push(`${scheduledLibs.length} in scheduler`);
    summaryEl.innerHTML=`
      <span class="dash-schedule-key">Libraries</span>
      <span style="font-size:12px;color:var(--text2)">${summaryParts.join(' · ')}</span>
      ${dashboardStatusBadge(enabledLibs.length?'Ready':'Needs setup', enabledLibs.length?'ok':'warn')}`;
  }
  if(!el) return;
  if(!allLibs.length){
    el.innerHTML='<div class="dashboard-empty">No libraries configured. Add libraries in Configuration to get started.</div>';
    return;
  }
  el.innerHTML=`<div class="card-title" style="margin-bottom:8px;margin-top:4px">Media Libraries</div><div style="display:grid;gap:6px">`+allLibs.map(lib=>{
    const scheduled=scheduledLibs.some(n=>n===lib.name);
    const type=lib.type==='show'?'TV':'Movie';
    const disabled=lib.enabled===false;
    return `<div class="dash-health-row">
      <span class="dash-health-label">${lib.name}</span>
      <span class="dash-health-detail">${type}${scheduled?' · In Scheduler':''}</span>
      ${dashboardStatusBadge(disabled?'Disabled':(scheduled?'In Scheduler':'Enabled'), disabled?'off':(scheduled?'ok':'warn'))}
    </div>`;
  }).join('')+`</div>`;
}
async function loadDashboardDeferredData(seq, cfg, enabledLibs){
  const {data}=await requestJson('/api/dashboard/summary');
  if(seq!==_dashboardLoadSeq) return;
  const summary=(data&&typeof data==='object')?data:{};
  _dashSummaryByLib=(summary.counts_by_library&&typeof summary.counts_by_library==='object')?summary.counts_by_library:{};
  _dashSummaryOverall={...emptyDashboardCounts(), ...((summary.counts_by_status&&typeof summary.counts_by_status==='object')?summary.counts_by_status:{})};
  if(summary.libraries && typeof summary.libraries==='object'){
    _dashboardLibraries={
      enabled:Array.isArray(summary.libraries.enabled)?summary.libraries.enabled:enabledLibs.map(lib=>lib.name),
      scheduled:Array.isArray(summary.libraries.scheduled)?summary.libraries.scheduled:(_dashboardLibraries.scheduled||[]),
    };
  }
  if(_dashSelectedLib!=='all' && !_dashSummaryByLib[_dashSelectedLib]) _dashSelectedLib='all';
  _dashTimelineData=(summary.status_timeline&&typeof summary.status_timeline==='object')?summary.status_timeline:{};

  renderDashboardPipelineOverview(_dashSummaryOverall);
  renderDashboardNeedsAttention(cfg, _dashSummaryOverall, enabledLibs);
  renderDashboardRecentActivity(summary.recent_activity||{});
  renderDashLibTabs(enabledLibs);
  renderHistStatusFilters();
  renderBarDaysInput();
  renderBarChart();
  renderPieChart();
}
async function dashboardRefreshHealth(full=false){
  const requestSeq=++_dashboardHealthRequestSeq;
  _dashboardHealthLoading=true;
  const refreshBtn=document.getElementById('dashboard-health-refresh');
  if(refreshBtn){
    refreshBtn.disabled=true;
    refreshBtn.textContent='Refreshing…';
  }
  try{
    const mode=full ? 'full' : 'lite';
    const {data:health}=await requestJson('/api/health?mode='+encodeURIComponent(mode));
    if(requestSeq!==_dashboardHealthRequestSeq) return;
    if(health && typeof health==='object'){
      _dashboardHealthMode=health?.validation?.mode || mode;
      writeDashboardHealthCache(health);
      renderDashboardSystemHealth(health);
      renderDashboardSchedule(health);
    }
  }finally{
    if(requestSeq===_dashboardHealthRequestSeq){
      _dashboardHealthLoading=false;
      const currentBtn=document.getElementById('dashboard-health-refresh');
      if(currentBtn){
        currentBtn.disabled=false;
        currentBtn.textContent='Refresh';
      }
    }
  }
}
async function loadDashboard(force=false){
  const seq=++_dashboardLoadSeq;
  const tsEl=document.getElementById('dash-last-updated');
  if(tsEl) tsEl.textContent='Updated '+new Date().toLocaleTimeString();
  const cfg=await loadConfig(force);
  if(seq!==_dashboardLoadSeq) return;
  const allLibs=(cfg.libraries||[]).filter(lib=>(!lib.type||lib.type==='movie'||lib.type==='show'));
  const enabledLibs=allLibs.filter(lib=>lib.enabled!==false);
  const selectedNames=new Set((cfg.schedule_libraries&&cfg.schedule_libraries.length)?cfg.schedule_libraries:enabledLibs.map(lib=>lib.name));
  const scheduledLibs=enabledLibs.filter(lib=>selectedNames.has(lib.name)).map(lib=>lib.name);
  _dashboardLibraries={enabled:enabledLibs.map(lib=>lib.name), scheduled:scheduledLibs};
  _dashSummaryByLib={};
  _dashSummaryOverall=emptyDashboardCounts();
  _dashTimelineData={};
  if(_dashSelectedLib!=='all' && !_dashSummaryByLib[_dashSelectedLib]) _dashSelectedLib='all';
  _resetHowItWorks();
  renderDashboardDeferredPlaceholders(cfg, enabledLibs, scheduledLibs);
  renderDashboardActionStationFromConfig(cfg, scheduledLibs);
  loadDashboardDeferredData(seq, cfg, enabledLibs).catch(err=>console.warn('Dashboard deferred load failed', err));
}
function setPendingTestBadge(elId, label='Working…'){
  const el=document.getElementById(elId);
  if(el) el.innerHTML=`<span class="test-result test-pending">${label}</span>`;
}
window.fetch = function(input, init){
  const token = getApiToken();
  let opts = init || {};
  if(token){
    const headers = new Headers(opts.headers || {});
    headers.set('X-UI-Token', token);
    opts = {...opts, headers};
  }
  let req = input;
  if(typeof input === 'string'){ req = apiUrl(input); }
  return _rawFetch(req, opts).then(r=>{
    if(r.status === 401 && !_authPrompted){
      _authPrompted = true;
      const t = prompt('Enter API token');
      if(t){
        localStorage.setItem('mt-api-token', t.trim());
        location.reload();
      }
    }
    return r;
  });
};

function showPage(name) {
  stopAllAudio();
  const aliases={'database':'theme-manager','schedule':'scheduler'};
  const resolved=aliases[String(name||'').trim()] || String(name||'').trim() || 'dashboard';
  const pageEl=document.getElementById('page-'+resolved);
  const navEl=document.getElementById('nav-'+resolved);
  if(!pageEl || !navEl){ console.warn('Missing page or nav for', name); return; }
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  pageEl.classList.add('active');
  navEl.classList.add('active');
  if(history && history.replaceState){ history.replaceState(null,'','#'+resolved); }
  if(resolved==='dashboard') loadDashboard();
  if(resolved==='theme-manager'){ loadDatabase(); setTimeout(()=>{if(!_dbColsInit){initResizableCols('#page-theme-manager table');_dbColsInit=true;}},500); }
  if(resolved==='tasks') loadTasksPage();
  if(resolved==='configuration') loadConfiguration();
  if(resolved==='scheduler') loadRunPage();
  removeItemDetailsPanel();
  _syncCountdownTimer();
  _syncGlobalRunStatusPolling({immediate:true});
}

function removeItemDetailsPanel(){
  try{
    const page=document.getElementById('page-theme-manager');
    if(!page) return;
    page.querySelectorAll(':is([data-item-details], .item-details, .details-panel, .right-panel, .side-panel, .panel, .card)').forEach(panel=>{
      const heading=panel.querySelector(':scope > :is(h1,h2,h3,h4,.card-title,.panel-title)');
      if(heading && heading.textContent.trim()==='Item details') panel.remove();
    });
  }catch(e){}
}

let _wasRunning=false;
let _dashRunActive=false;
async function updateGlobalRunStatus(){
  if(_globalRunStatusInFlight) return;
  _globalRunStatusInFlight=true;
  try{
    const r=await fetch('/api/run/status');
    const d=await r.json();
    const bar=document.getElementById('global-run');
    const dot=document.getElementById('global-run-dot');
    const text=document.getElementById('global-run-text');
    const sub=document.getElementById('global-run-sub');
    const dbPill=document.getElementById('db-run-pill');
    if(d.active){
      bar.classList.remove('hidden');
      dot.className='run-dot active';
      text.textContent='Running';
      sub.textContent=d.scope ? `${d.scope} · ${d.last_line||'Working…'}` : (d.last_line||'Working…');
      if(dbPill){ dbPill.classList.add('active'); dbPill.innerHTML='<span class="run-dot active"></span><span>Running</span>'; }
      _wasRunning=true;
      if(!_dashRunActive){_dashRunActive=true;_updateDashRunButton();}
      return;
    }
    dot.className='run-dot done';
    text.textContent='Idle';
    sub.textContent='';
    bar.classList.add('hidden');
    if(dbPill){ dbPill.classList.remove('active'); dbPill.innerHTML='<span class="run-dot"></span><span>Idle</span>'; }
    if(_dashRunActive){_dashRunActive=false;_updateDashRunButton();}
    if(_wasRunning){
      _wasRunning=false;
      loadDashboard(); loadDatabase(); loadTasksPage(); loadRunPage();
    }
  }catch(e){}
  finally{
    _globalRunStatusInFlight=false;
    _syncGlobalRunStatusPolling();
  }
}


function initSharedProgress(){
  ['db','run','tasks'].forEach(scope=>{
    const el=_progressEls(scope);
    if(!el.wrap) return;
    el.wrap.classList.remove('active');
    el.wrap.dataset.state='queued';
    if(el.fill) el.fill.style.width='0%';
    if(el.label) el.label.textContent='queued';
    if(el.meta) el.meta.textContent='';
    if(el.state) el.state.textContent='Queued';
    if(el.stop) el.stop.style.display='none';
    if(el.retry) el.retry.style.display='none';
  });
}

function initSidebarNav(){
  document.querySelectorAll('.nav-item[data-page]').forEach(item=>{
    item.addEventListener('click', (e)=>{
      e.preventDefault();
      e.stopPropagation();
      const page=item.dataset.page;
      if(page) showPage(page);
    });
  });
}


let _toastT;
function toast(msg,type='info'){
  const t=document.getElementById('toast');
  t.textContent=msg; t.className='show '+type;
  clearTimeout(_toastT); _toastT=setTimeout(()=>t.className='',3000);
}
function testBadge(elId, ok, msg){
  const el=document.getElementById(elId);
  el.className='test-result '+(ok?'test-ok':'test-fail');
  el.textContent=(ok?'✓ ':'✗ ')+msg;
}

let _libs=[], _maxDur=45;
let _uiTerminology={};
const _defaultStatusDisplay={'UNMONITORED':'Unmonitored','MISSING':'Missing','STAGED':'Staged','APPROVED':'Approved','AVAILABLE':'Available','FAILED':'Failed'};
const _defaultStatusDesc={
  UNMONITORED:'Hidden from automation until you re-enable this title.',
  MISSING:'Needs a source because no local theme file is available yet.',
  STAGED:'Source saved and waiting for approval before download.',
  APPROVED:'Approved for download on the next run.',
  AVAILABLE:'Theme file is present locally and ready to use.',
  FAILED:'Source discovery or download needs attention before retrying.'
};
let STATUSES=['UNMONITORED','MISSING','STAGED','APPROVED','AVAILABLE','FAILED'];
let STATUS_TRANSITIONS={
  UNMONITORED:['MISSING'],
  MISSING:['STAGED','FAILED'],
  STAGED:['APPROVED','MISSING','FAILED'],
  APPROVED:['AVAILABLE','MISSING','FAILED'],
  AVAILABLE:['MISSING'],
  FAILED:['MISSING','STAGED']
};
let STATUS_MANUAL_ANY=['UNMONITORED'];

function uiTerm(path, fallback=''){
  const parts=String(path||'').split('.');
  let cur=_uiTerminology;
  for(const part of parts){
    if(!cur || typeof cur!=='object' || !(part in cur)) return fallback;
    cur=cur[part];
  }
  return (typeof cur==='string' || typeof cur==='number') ? String(cur) : fallback;
}

function applyUiTerminology(){
  document.querySelectorAll('[data-ui]').forEach(el=>{
    const key=el.dataset.ui;
    const val=uiTerm(key,'');
    if(val) el.textContent=val;
  });
  emphasizeStatusCopy();
}

async function loadUiTerminology(){
  try{
    const r=await fetch('/api/ui-terminology');
    const data=await r.json();
    _uiTerminology=(data && typeof data==='object')?data:{};
  }catch(e){
    _uiTerminology={};
  }
  applyUiTerminology();
}

async function loadStatusModel(){
  try{
    const r=await fetch('/api/status-model');
    const data=await r.json();
    if(Array.isArray(data?.statuses) && data.statuses.length){
      STATUSES=data.statuses.map(s=>String(s||'').toUpperCase()).filter(Boolean);
    }
    if(data?.manual_transitions && typeof data.manual_transitions==='object'){
      const next={};
      Object.entries(data.manual_transitions).forEach(([status, targets])=>{
        next[String(status||'').toUpperCase()]=Array.isArray(targets)
          ? targets.map(t=>String(t||'').toUpperCase()).filter(Boolean)
          : [];
      });
      STATUS_TRANSITIONS=next;
    }
    if(Array.isArray(data?.manual_any)){
      STATUS_MANUAL_ANY=data.manual_any.map(s=>String(s||'').toUpperCase()).filter(Boolean);
    }
  }catch(e){}
}

function emphasizeStatusCopy(){
  const terms=STATUSES.slice();
  const rx=new RegExp(`\\b(${terms.join('|')})\\b`,'gi');
  document.querySelectorAll('.status-copy').forEach(el=>{
    const src=el.textContent||'';
    el.innerHTML=src.replace(rx,(_,term)=>{
      const normalized=String(term||'').toUpperCase();
      return `<strong class="status-term status-tone-${normalized}">${displayStatus(normalized)}</strong>`;
    });
  });
}

async function loadConfiguration(force=false){
  const cfg=await loadConfig(force);
  ['plex_url','plex_token','tmdb_api_key',
   'search_mode','search_query_playlist','search_query_direct','golden_source_url',
   'audio_format','quality_profile','theme_filename','max_retries','download_delay_seconds','test_limit']
    .forEach(k=>{ const el=document.getElementById('cfg-'+k); if(el) el.value=cfg[k]??''; });

  const fbEl = document.getElementById('cfg-search_fallback');
  if(fbEl) fbEl.checked = cfg.search_fallback !== false;
  const matchingModeEl = document.getElementById('cfg-search_matching_mode');
  if(matchingModeEl) matchingModeEl.value = cfg.search_fuzzy ? 'fuzzy' : 'strict';
  const goldenOnlyEl = document.getElementById('cfg-search_only_golden');
  if(goldenOnlyEl) goldenOnlyEl.checked = !!cfg.search_only_golden;
  const goldenRefreshEl = document.getElementById('cfg-refresh_golden_source_each_run');
  if(goldenRefreshEl) goldenRefreshEl.checked = cfg.refresh_golden_source_each_run !== false;
  syncMatchingModeSelect();
  const manAuto=document.getElementById('cfg-auto_approve_manual');
  if(manAuto) manAuto.checked = !!cfg.auto_approve_manual;

  _libs=(cfg.libraries||[{name:cfg.plex_library_name||'Movies',enabled:true}]).map(l=>({...l}));
  renderLibs();
  _fillTaskLibSelects();
  if(!_runLibs.length){ _runLibs=(cfg.libraries||[]).filter(l=>l.enabled!==false && (!l.type||l.type==='movie'||l.type==='show')).map(l=>({...l, scheduled:true})); }
  _refreshScopedRunLabels();
  removeItemDetailsPanel();

  // Max duration toggle
  _maxDur = cfg.max_theme_duration ?? 45;
  setDuration(_maxDur, true);

  // Cookies
  await loadCookieOptions(cfg.cookies_file || '');
}

function onSearchModeChange(){
  syncMatchingModeSelect();
}

function syncMatchingModeSelect(){
  const fb=document.getElementById('cfg-search_fallback');
  const mode=document.getElementById('cfg-search_matching_mode');
  const help=document.getElementById('search-matching-help');
  if(!fb || !mode) return;
  const fuzzy = mode.value === 'fuzzy';
  if(fuzzy) fb.checked=false;
  fb.disabled = fuzzy;
  const fbLabel=document.getElementById('lbl-search-fallback');
  if(fbLabel) fbLabel.style.opacity = fuzzy ? '.45' : '1';
  if(help){
    help.innerHTML = fuzzy
      ? '<div class="field-help-line"><strong>Strict Matching</strong> checks that the source includes the full query.</div><div class="field-help-line"><strong>Fuzzy Matching</strong> uses the query and accepts the first result. It also turns off fallback search.</div>'
      : '<div class="field-help-line"><strong>Strict Matching</strong> checks that the source includes the full query.</div><div class="field-help-line"><strong>Fuzzy Matching</strong> uses the query and accepts the first result.</div>';
  }
}

function setDuration(val, noSave){
  _maxDur = val;
  document.querySelectorAll('.dur-opt').forEach(el=>{
    el.classList.toggle('active', parseInt(el.dataset.val)===val);
  });
  const customInput=document.getElementById('cfg-custom-dur');
  if(customInput && ![30,45,60,0].includes(val)) customInput.value=val;
  else if(customInput) customInput.value='';
}

async function loadCookieOptions(current) {
  try {
    const {data}=await requestJson('/api/cookies');
    const sel = document.getElementById('cfg-cookies_file');
    sel.innerHTML = '<option value="">No cookies file</option>';
    (data.files || []).forEach(f => {
      const opt = document.createElement('option');
      opt.value = f; opt.textContent = f.split('/').pop();
      if (f === current) opt.selected = true;
      sel.appendChild(opt);
    });
    updateCookieIndicator();
  } catch(e) { console.error(e); }
}

function updateCookieIndicator() {
  const sel = document.getElementById('cfg-cookies_file');
  const ind = document.getElementById('cookie-indicator');
  if (sel.value) {
    ind.className = 'cookie-indicator cookie-ok';
    ind.textContent = '✓ ' + sel.value.split('/').pop();
  } else {
    ind.className = 'cookie-indicator cookie-none';
    ind.textContent = 'No file selected';
  }
}

function slugify(s){ return s.toLowerCase().trim().replace(/[^a-z0-9]+/g,'_').replace(/^_|_$/g,'')||'library'; }

function renderLibs(){
  const container=document.getElementById('lib-list');
  container.innerHTML=_libs.map((lib,i)=>{
    const typeLabel=lib.type==='movie'?'Movie':lib.type==='show'?'TV':'';
    const typePillClass=lib.type==='show'?'type-pill tv':'type-pill';
    const typeBadge=typeLabel?`<span class="${typePillClass}">${typeLabel}</span>`:'';
    const stateBadge=`<span class="lib-state ${lib.enabled?'on':'off'}">${lib.enabled?'Enabled':'Disabled'}</span>`;
    return `<div class="lib-row" draggable="true" data-lib-index="${i}"
        ondragstart="_libDragStart(event,${i})"
        ondragover="_libDragOver(event)"
        ondrop="_libDrop(event,${i})"
        ondragend="_libDragEnd(event)">
      <span class="drag-handle" title="Drag to reorder">⠿</span>
      <label class="toggle">
        <input type="checkbox" ${lib.enabled?'checked':''} onchange="_libs[${i}].enabled=this.checked;renderLibs()">
        <span class="toggle-track"></span>
      </label>
      <input type="text" value="${lib.name||''}" placeholder="Library name (exact match in Plex)"
        oninput="_libs[${i}].name=this.value" style="flex:1;min-width:0;background:transparent;border:none;color:var(--text);font-family:var(--ui);font-size:13px;font-weight:600;outline:none;padding:0">
      ${typeBadge}
      ${stateBadge}
      <button class="btn btn-ghost btn-xs" onclick="_libs.splice(${i},1);renderLibs()" style="flex-shrink:0;margin-left:4px">✕</button>
    </div>`;
  }).join('');
}
let _libDragSrc=null;
function _libDragStart(e,i){_libDragSrc=i;e.currentTarget.classList.add('dragging');e.dataTransfer.effectAllowed='move';}
function _libDragOver(e){e.preventDefault();e.dataTransfer.dropEffect='move';e.currentTarget.classList.add('drag-over');}
function _libDrop(e,i){
  e.preventDefault();
  e.currentTarget.classList.remove('drag-over');
  if(_libDragSrc===null||_libDragSrc===i) return;
  const moved=_libs.splice(_libDragSrc,1)[0];
  _libs.splice(i,0,moved);
  _libDragSrc=null;
  renderLibs();
  rememberConfigPatch({libraries:_libs.filter(l=>String(l.name||'').trim())});
}
function _libDragEnd(e){e.currentTarget.classList.remove('dragging');document.querySelectorAll('.lib-row').forEach(r=>r.classList.remove('drag-over'));_libDragSrc=null;}

function addLibrary(){ _libs.push({name:'',enabled:false}); renderLibs(); }

async function testPlex(){
  const url=document.getElementById('cfg-plex_url').value;
  const token=document.getElementById('cfg-plex_token').value;
  setPendingTestBadge('plex-test-result','Testing…');
  const {data}=await postJson('/api/test/plex',{url,token});
  testBadge('plex-test-result', !!data.ok, data.ok ? `Connected · ${data.libraries} libraries` : data.error);
}

async function importLibraries(){
  const url=document.getElementById('cfg-plex_url').value;
  const token=document.getElementById('cfg-plex_token').value;
  setPendingTestBadge('lib-import-result','Fetching…');
  const {ok,data}=await postJson('/api/plex/libraries',{url,token});
  if(ok){
    const existing=new Set(_libs.map(l=>l.name));
    let added=0;
    (data.libraries||[]).forEach(lib=>{ if(!existing.has(lib.name)){ _libs.push({name:lib.name,type:lib.type,enabled:false}); added++; } });
    renderLibs();
    rememberConfigPatch({libraries:_libs.filter(l=>String(l.name||'').trim())});
    document.getElementById('lib-import-result').innerHTML=`<span class="test-result test-ok">✓ ${added} added (${(data.libraries||[]).length} total · movie &amp; TV only)</span>`;
  } else {
    testBadge('lib-import-result', false, data.error);
  }
}

async function testTmdb(){
  const key=document.getElementById('cfg-tmdb_api_key').value;
  setPendingTestBadge('tmdb-test-result','Testing…');
  const {data}=await postJson('/api/test/tmdb',{key});
  testBadge('tmdb-test-result', !!data.ok, data.ok ? 'API key valid' : data.error);
}

async function testGoldenSource(){
  const url=(document.getElementById('cfg-golden_source_url').value||'').trim();
  const badge=document.getElementById('golden-test-result');
  const btn=document.getElementById('golden-test-btn');
  if(!url){ testBadge('golden-test-result', false, 'Enter a Golden Source URL first'); return; }
  if(btn){ btn.disabled=true; btn.textContent='Testing…'; }
  if(badge) badge.innerHTML='<span class="test-result test-pending">Testing…</span>';
  try{
    const {ok,data}=await postJson('/api/test/golden-source',{url});
    testBadge('golden-test-result', ok, ok ? `Ready · ${data.rows} rows · ${data.fetch_ms} ms` : (data.error||'Test failed'));
  }catch(e){
    testBadge('golden-test-result', false, 'Test failed');
  }finally{
    if(btn){ btn.disabled=false; btn.textContent='⚡ Test Golden Source'; }
  }
}

async function saveConfiguration(){
  const strFields=['plex_url','plex_token','tmdb_api_key',
    'search_mode','search_query_playlist','search_query_direct','golden_source_url',
    'cookies_file','audio_format','quality_profile','theme_filename'];
  const numFields=['max_retries','download_delay_seconds','test_limit'];
  const cfg={};
  strFields.forEach(k=>{ const el=document.getElementById('cfg-'+k); if(el) cfg[k]=el.value; });
  numFields.forEach(k=>{ const el=document.getElementById('cfg-'+k); if(el) cfg[k]=Number(el.value); });
  cfg.libraries=_libs.filter(l=>l.name.trim());
  cfg.max_theme_duration = _maxDur;
  cfg.search_fallback = document.getElementById('cfg-search_fallback').checked;
  cfg.search_fuzzy = (document.getElementById('cfg-search_matching_mode')?.value || 'strict') === 'fuzzy';
  cfg.search_only_golden = document.getElementById('cfg-search_only_golden').checked;
  cfg.refresh_golden_source_each_run = document.getElementById('cfg-refresh_golden_source_each_run').checked;
  if(cfg.search_fuzzy) cfg.search_fallback = false;
  cfg.auto_approve_manual = document.getElementById('cfg-auto_approve_manual').checked;
  const {ok,data}=await postJson('/api/config',cfg);
  if(ok){
    rememberConfigPatch(cfg);
    toast('Configuration saved','ok');
    renderLibs();
    _fillTaskLibSelects();
    _refreshScopedRunLabels();
  } else toast(data?.message || data?.error || 'Save failed','err');
}

// App bootstrap.

try{
  document.addEventListener('DOMContentLoaded', async ()=>{
    await loadUiTerminology();
    await loadStatusModel();
    initSidebarNav();
    initSharedProgress();
    removeItemDetailsPanel();
    updateGlobalRunStatus();
    _syncGlobalRunStatusPolling();
    const initial=((location.hash||'').replace(/^#/,'').trim());
    if(['dashboard','configuration','database','theme-manager','schedule','scheduler','tasks'].includes(initial)){
      showPage(initial);
    }else{
      showPage('dashboard');
    }
  });
}catch(e){}

registerModalLifecycle('confirm-modal',{requestClose:closeConfirmModal});
registerModalLifecycle('delete-modal',{requestClose:closeDeleteModal});
registerModalLifecycle('gs-modal',{requestClose:closeGoldenSourceModal});
registerModalLifecycle('search-modal',{requestClose:closeSearchModal});
registerModalLifecycle('trim-modal',{requestClose:closeTrimModal});
registerModalLifecycle('yt-modal',{requestClose:closeYtModal});
registerModalLifecycle('theme-modal',{requestClose:closeThemeModal});

// ── Init ─────────────────────────────────────────────────────────────────────
renderTaskCards();
bindOffsetWheel();
