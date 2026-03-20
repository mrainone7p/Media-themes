// Consolidated scheduler/run behavior.

function _progressEls(scope){
  return {
    wrap:document.getElementById(scope+'-progress'),
    fill:document.getElementById(scope+'-progress-fill'),
    label:document.getElementById(scope+'-progress-label'),
    meta:document.getElementById(scope+'-progress-meta'),
    state:document.getElementById(scope+'-progress-state'),
    stop:document.getElementById(scope+'-progress-stop'),
    retry:document.getElementById(scope+'-progress-retry')
  };
}

function setProgressState(scope,state,{copy='',meta='',pct=0,showStop=false,showRetry=false}={}){
  const el=_progressEls(scope);
  if(!el.wrap) return;
  const stateLabel={queued:'Queued',running:'Running',stopped:'Stopped',success:'Success',error:'Error'};
  el.wrap.classList.add('active');
  el.wrap.dataset.state=state;
  if(el.fill) el.fill.style.width=Math.max(0,Math.min(100,pct))+'%';
  if(el.label && copy) el.label.textContent=copy;
  if(el.meta) el.meta.textContent=meta||'';
  if(el.state) el.state.textContent=stateLabel[state]||state;
  if(el.stop) el.stop.style.display=showStop?'inline-flex':'none';
  if(el.retry) el.retry.style.display=showRetry?'inline-flex':'none';
}

function dbProgressStart(label='Working…'){
  setProgressState('db','running',{copy:label,pct:12,showStop:true,meta:'Running pass…'});
  const pill=document.getElementById('db-run-pill');
  if(pill) pill.classList.add('active');
}
function dbProgressSet(pct,label){ setProgressState('db','running',{copy:label||'Running…',pct,showStop:true}); }
function dbProgressDone(label='Done'){
  setProgressState('db','success',{copy:label,pct:100,meta:'Run completed successfully.',showRetry:true});
  const pill=document.getElementById('db-run-pill'); if(pill) pill.classList.remove('active');
}
function dbProgressFail(label='Failed'){
  setProgressState('db','error',{copy:label,pct:100,meta:'Run ended with an error.',showRetry:true});
  const pill=document.getElementById('db-run-pill'); if(pill) pill.classList.remove('active');
}

let _dbEvtSrc=null, _dbPollTimer=null, _dbLastProgress=0;
let _activeRunContext=null;
let _lastRunRequest={db:null,run:null,tasks:null};

function _dbStopPoll(){ clearInterval(_dbPollTimer); _dbPollTimer=null; }

function _passLabel(pass){
  return {0:'Automated Schedule',1:'Scan',2:'Find Sources',3:'Download'}[pass]||'Run';
}

function _shortScopeLabel(scopeLabel=''){
  const text=String(scopeLabel||'').trim();
  return text.length>56 ? text.slice(0,53)+'…' : text;
}

function _statusCopy(pass, scopeLabel=''){
  const base={0:'Running automated schedule',1:'Running pass 1/3',2:'Running pass 2/3',3:'Running pass 3/3'}[pass]||'Running';
  const suffix=_shortScopeLabel(scopeLabel);
  return suffix ? `${base} · ${suffix}` : base;
}

function _formatScopeLabel(libraries=[], fallback='scheduled libraries'){
  const libs=(libraries||[]).map(v=>String(v||'').trim()).filter(Boolean);
  if(!libs.length) return fallback;
  if(libs.length===1) return `library "${libs[0]}"`;
  if(libs.length===2) return `libraries "${libs[0]}" and "${libs[1]}"`;
  if(libs.length===3) return `libraries "${libs[0]}", "${libs[1]}", and "${libs[2]}"`;
  return `${libs.length} libraries selected`;
}

function _selectedRunLibraries(){
  return (_runLibs||[]).filter(l=>l.scheduled).map(l=>l.name);
}

function _refreshScopedRunLabels(){
  const activeLabel=_activeLib?`"${_activeLib}"`:'selected library';
  const dbLabels={
    'db-btn-scan':`⟳ Scan ${activeLabel}`,
    'db-btn-resolve':`⚡ Find Sources in ${activeLabel}`,
    'db-btn-download':`↓ Download Approved in ${activeLabel}`
  };
  Object.entries(dbLabels).forEach(([id,label])=>{
    const el=document.getElementById(id);
    if(el) el.textContent=label;
  });

  const scheduledLibs=_selectedRunLibraries();
  const scheduleScope=_formatScopeLabel(scheduledLibs, 'scheduled libraries');
  const summaryEl=document.getElementById('run-schedule-summary');
  if(summaryEl){
    const steps=[];
    if(document.getElementById('sched-step1')?.checked) steps.push('Scan Libraries');
    if(document.getElementById('sched-step2')?.checked) steps.push('Find Theme Sources');
    if(document.getElementById('sched-step3')?.checked) steps.push('Download Themes');
    summaryEl.textContent=`Uses ${scheduleScope} · Steps: ${steps.length?steps.join(', '):'none enabled'}`;
  }
  const runBtn=document.getElementById('btn-schedule-run');
  if(runBtn) runBtn.title=`Run the configured automated schedule for ${scheduleScope}`;
}

function _setRunUiActive(pass){
  const runBtn=document.getElementById('btn-schedule-run');
  const stopBtn=document.getElementById('btn-schedule-stop');
  if(runBtn){runBtn.disabled=true;runBtn.style.display='none';}
  if(stopBtn) stopBtn.style.display='';
  const dot=document.getElementById('run-dot');
  const status=document.getElementById('run-status');
  if(dot) dot.className='run-dot active';
  if(status) status.textContent=_statusCopy(pass, _activeRunContext?.scopeLabel||_formatScopeLabel(_selectedRunLibraries(),'scheduled libraries'));
}

function _setRunUiIdle(text='Idle — run the configured automated schedule when ready',done=false){
  const runBtn=document.getElementById('btn-schedule-run');
  const stopBtn=document.getElementById('btn-schedule-stop');
  if(runBtn){runBtn.disabled=false;runBtn.style.display='';}
  if(stopBtn) stopBtn.style.display='none';
  const dot=document.getElementById('run-dot');
  const status=document.getElementById('run-status');
  if(dot) dot.className=done?'run-dot done':'run-dot';
  if(status) status.textContent=text;
}

function _setDbButtons(disabled){
  ['db-btn-scan','db-btn-resolve','db-btn-download'].forEach(id=>{ const el=document.getElementById(id); if(el) el.disabled=disabled; });
}

function _applyContextStart(ctx){
  const copy=_statusCopy(ctx.pass, ctx.scopeLabel);
  const meta=ctx.scopeLabel ? `Connecting to runner for ${ctx.scopeLabel}…` : 'Connecting to runner…';
  setProgressState(ctx.scope,'running',{copy,pct:0,showStop:true,showRetry:false,meta});
  if(ctx.scope==='db') _setDbButtons(true);
  if(ctx.scope==='run') _setRunUiActive(ctx.pass);
  if(ctx.scope==='tasks') setProgressState('tasks','running',{copy,pct:0,showStop:true,meta:ctx.scopeLabel?`Running ${ctx.scopeLabel}…`:'Running selected libraries…'});
}

function _applyContextFinish(ctx,state,meta,pct=100){
  const statusLabel=state==='success'?'Success':state==='stopped'?'Stopped':'Error';
  const copy=ctx.scopeLabel ? `${statusLabel} · ${_shortScopeLabel(ctx.scopeLabel)}` : statusLabel;
  setProgressState(ctx.scope,state,{copy,pct,showRetry:true,meta,showStop:false});
  if(ctx.scope==='db') _setDbButtons(false);
  if(ctx.scope==='run') _setRunUiIdle(state==='success' ? 'Done · '+new Date().toLocaleTimeString() : (state==='stopped' ? 'Stopped by user' : 'Error — review logs'), state==='success');
  if(ctx.scope==='tasks'){}
}

function _normalizeRunOutcome(outcome=''){
  const normalized=String(outcome||'').trim().toLowerCase();
  return ['success','stopped','error'].includes(normalized) ? normalized : '';
}

function _runOutcomeMeta(state,payload={}){
  const summary=String(payload.summary||payload.last_line||'').trim();
  if(summary) return summary;
  if(state==='success') return 'Run completed.';
  if(state==='stopped') return 'Run stopped.';
  return 'Run failed.';
}

async function _finishRunContext(ctx,payload=null){
  if(!ctx || ctx.finishing) return;
  ctx.finishing=true;
  let d=payload;
  if(!d){
    try{
      const r=await fetch('/api/run/status');
      d=await r.json();
    }catch(e){
      d={};
    }
  }
  if(d?.scope) ctx.scopeLabel=d.scope;
  const state=_normalizeRunOutcome(d?.outcome) || (ctx.stoppedByUser ? 'stopped' : 'error');
  const meta=_runOutcomeMeta(state,d||{});
  _applyContextFinish(ctx,state,meta);
  toast(meta,state==='success'?'ok':state==='stopped'?'info':'err');
  loadDatabase(); loadTasksPage(); loadRunPage();
  _activeRunContext=null;
}

function _handleProgressData(ctx,p){
  const pct=p.total>0?Math.round(p.current/p.total*100):0;
  const copy=_statusCopy(ctx.pass, ctx.scopeLabel);
  const detail=p.action==='error'?(p.message||'Error'):`${p.current}/${p.total} ${p.action} "${p.title}"`;
  const meta=ctx.scopeLabel ? `${ctx.scopeLabel} · ${detail}` : detail;
  const state=p.action==='error'?'error':'running';
  setProgressState(ctx.scope,state,{copy,meta,pct,showStop:state==='running',showRetry:state!=='running'});
  if(ctx.scope==='run') _setRunUiActive(ctx.pass);
}

function _connectRunStream(ctx){
  if(_dbEvtSrc){ _dbEvtSrc.close(); }
  _dbEvtSrc=new EventSource(apiUrl('/api/run/stream'));
  _dbEvtSrc.onmessage=async e=>{
    _dbLastProgress=Date.now();
    if(!_activeRunContext) return;
    if(e.data==='__DONE__'){
      if(_dbEvtSrc){ _dbEvtSrc.close(); _dbEvtSrc=null; }
      _dbStopPoll();
      await _finishRunContext(_activeRunContext);
      return;
    }
    if(e.data.startsWith('@@PROGRESS@@')){
      try{ _handleProgressData(_activeRunContext,JSON.parse(e.data.slice(12))); }catch(ex){}
      return;
    }
    const fallbackMeta=_activeRunContext.scopeLabel ? `${_activeRunContext.scopeLabel} · ${e.data.slice(0,140)}` : e.data.slice(0,140);
    setProgressState(_activeRunContext.scope,'running',{copy:_statusCopy(_activeRunContext.pass,_activeRunContext.scopeLabel),meta:fallbackMeta,pct:Math.min(98,parseInt(_progressEls(_activeRunContext.scope).fill?.style.width||'0',10)||0),showStop:true});
  };
  _dbEvtSrc.onerror=()=>{ if(_dbEvtSrc){ _dbEvtSrc.close(); _dbEvtSrc=null; } };
}

function _dbStartPoll(){
  _dbStopPoll();
  _dbPollTimer=setInterval(async()=>{
    try{
      const d=await(await fetch('/api/run/status')).json();
      if(!_activeRunContext) return;
      if(d.scope) _activeRunContext.scopeLabel=d.scope;
      if(!d.active){
        _dbStopPoll();
        if(_dbEvtSrc){ _dbEvtSrc.close(); _dbEvtSrc=null; }
        await _finishRunContext(_activeRunContext,d);
        return;
      }
      if(d.last_line && Date.now()-_dbLastProgress > 2500){
        const meta=_activeRunContext.scopeLabel ? `${_activeRunContext.scopeLabel} · ${d.last_line.slice(0,140)}` : d.last_line.slice(0,140);
        setProgressState(_activeRunContext.scope,'running',{copy:_statusCopy(_activeRunContext.pass,_activeRunContext.scopeLabel),meta,showStop:true,pct:Math.min(98,parseInt(_progressEls(_activeRunContext.scope).fill?.style.width||'0',10)||10)});
      }
    }catch(e){}
  }, 2000);
}

function _runCallerSurface(scope, options={}){
  const explicit=String(options.callerSurface||'').trim().toLowerCase();
  if(['dashboard','scheduler','tasks'].includes(explicit)) return explicit;
  if(typeof _activePageName==='function'){
    const active=String(_activePageName()||'').trim().toLowerCase();
    if(active==='dashboard') return 'dashboard';
    if(active==='scheduler') return 'scheduler';
  }
  return scope==='tasks' ? 'tasks' : 'tasks';
}

async function startPipelineRun(passNum,scope='db',options={}){
  const libraries=(options.libraries||[]).map(v=>String(v||'').trim()).filter(Boolean);
  const scopeLabel=String(options.scopeLabel||'').trim() || _formatScopeLabel(libraries, scope==='db'?'selected library':'scheduled libraries');
  const callerSurface=_runCallerSurface(scope, options);
  _lastRunRequest[scope]={mode:'pass',pass:passNum,libraries:[...libraries],scopeLabel};
  const ctx={scope,pass:passNum,stoppedByUser:false,libraries,scopeLabel};
  _activeRunContext=ctx;
  _dbLastProgress=Date.now();
  _applyContextStart(ctx);
  const body={scope_label:scopeLabel,caller_surface:callerSurface};
  if(libraries.length===1) body.library=libraries[0];
  else if(libraries.length>1) body.libraries=libraries;
  const r=await fetch('/api/run/pass/'+passNum,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(!r.ok){
    const d=await r.json().catch(()=>({}));
    _applyContextFinish(ctx,'error',d.error||'Unable to start run.');
    _activeRunContext=null;
    return;
  }
  _connectRunStream(ctx);
  _dbStartPoll();
}

function dbRunPass(passNum){
  if(!_activeLib){ toast('Select a library first','info'); return; }
  return startPipelineRun(passNum,'db',{libraries:[_activeLib],scopeLabel:`library "${_activeLib}"`});
}
async function startScheduledRun(callerSurface='scheduler'){
  const terminal=document.getElementById('terminal');
  if(terminal) terminal.innerHTML='';
  const libraries=_selectedRunLibraries();
  if(!libraries.length){ toast('Select at least one scheduled library','err'); return; }
  const requestedScopeLabel=_formatScopeLabel(libraries,'scheduled libraries');
  _lastRunRequest.run={mode:'schedule',pass:0,libraries:[...libraries],scopeLabel:requestedScopeLabel};
  const ctx={scope:'run',pass:0,stoppedByUser:false,libraries:[...libraries],scopeLabel:requestedScopeLabel};
  _activeRunContext=ctx;
  _dbLastProgress=Date.now();
  _applyContextStart(ctx);
  const r=await fetch('/api/run/schedule-now',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({libraries,scope_label:requestedScopeLabel,caller_surface:callerSurface})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){
    _applyContextFinish(ctx,'error',d.error||'Unable to start scheduled run.');
    _activeRunContext=null;
    return;
  }
  const actualLibraries=(d.libraries||[]).map(v=>String(v||'').trim()).filter(Boolean);
  if(actualLibraries.length) ctx.libraries=[...actualLibraries];
  ctx.scopeLabel=String(d.scope_label||'').trim() || _formatScopeLabel(ctx.libraries,'scheduled libraries');
  _lastRunRequest.run={mode:'schedule',pass:0,libraries:[...ctx.libraries],scopeLabel:ctx.scopeLabel};
  _applyContextStart(ctx);
  _connectRunStream(ctx);
  _dbStartPoll();
}

function retryRun(scope){
  const last=_lastRunRequest[scope]||{mode:'pass',pass:1,libraries:[],scopeLabel:''};
  if(last.mode==='schedule') return startScheduledRun();
  return startPipelineRun(last.pass||1,scope,{libraries:last.libraries||[],scopeLabel:last.scopeLabel||''});
}
let _mediaEvtSrc=null;

async function loadRunPage(){
  const cfg=await loadConfig();
  document.getElementById('run-cron').value=cfg.cron_schedule||'0 3 * * *';
  // Wire up live countdown for scheduler page
  try{
    const {data:health}=await requestJson('/api/health');
    const nextRun=(health&&health.schedule&&health.schedule.next_run)||null;
    _startCountdowns(nextRun);
  }catch(e){}
  document.getElementById('run-schedule-limit').value=cfg.schedule_test_limit ?? cfg.test_limit ?? 0;
  document.getElementById('sched-enabled').checked = cfg.schedule_enabled !== false;
  const enabledLibs=(cfg.libraries||[]).filter(l=>l.enabled!==false && (!l.type||l.type==='movie'||l.type==='show'));
  const selected=new Set((cfg.schedule_libraries&&cfg.schedule_libraries.length)?cfg.schedule_libraries:enabledLibs.map(l=>l.name));
  _runLibs=enabledLibs.map(l=>({...l, scheduled:selected.has(l.name)}));
  renderRunLibs();
  document.getElementById('sched-step1').checked = cfg.schedule_step1 !== false;
  document.getElementById('sched-step2').checked = cfg.schedule_step2 !== false;
  document.getElementById('sched-step3').checked = cfg.schedule_step3 !== false;
  document.getElementById('sched-autoapprove').checked = !!cfg.auto_approve;
  document.getElementById('sched-only-golden').checked = !!cfg.search_only_golden;
  applyScheduleEnabledState();
  _refreshScopedRunLabels();
  loadSchedulerHistory();
}

function applyScheduleEnabledState(){
  const enabled=document.getElementById('sched-enabled').checked;
  const wrap=document.getElementById('sched-config-wrap');
  if(!wrap) return;
  wrap.classList.toggle('is-disabled', !enabled);
  wrap.querySelectorAll('input, select, textarea, button').forEach(el=>{
    // Always keep the enable toggle and run button interactive
    if(el.id && ['btn-schedule-run','btn-schedule-stop','sched-enabled'].includes(el.id)) return;
    el.disabled = !enabled;
  });
}

let _runLibs=[];
function renderRunLibs(){
  document.getElementById('run-lib-toggles').innerHTML=_runLibs.map((lib,i)=>`
    <div class="run-lib-pill">
      <label class="toggle" style="transform:scale(.85)">
        <input type="checkbox" ${lib.scheduled?'checked':''} onchange="_runLibs[${i}].scheduled=this.checked;saveRunSchedule()">
        <span class="toggle-track"></span>
      </label>
      <span style="font-size:12px;font-family:var(--mono);color:${lib.scheduled?'var(--text)':'var(--text3)'}">${lib.name}</span>
    </div>`).join('');
  _refreshScopedRunLabels();
}

let _schedDebounce=null;
async function saveRunSchedule(){
  clearTimeout(_schedDebounce);
  _schedDebounce=setTimeout(async()=>{
    const cron=document.getElementById('run-cron').value;
    const enabled=document.getElementById('sched-enabled').checked;
    const cfg={...(await loadConfig())};
    cfg.schedule_enabled=enabled;
    cfg.cron_schedule=cron;
    cfg.schedule_libraries=_runLibs.filter(l=>l.scheduled).map(l=>l.name);
    cfg.schedule_step1=document.getElementById('sched-step1').checked;
    cfg.schedule_step2=document.getElementById('sched-step2').checked;
    cfg.schedule_step3=document.getElementById('sched-step3').checked;
    cfg.schedule_test_limit=parseInt(document.getElementById('run-schedule-limit').value||'0',10)||0;
    cfg.auto_approve=document.getElementById('sched-autoapprove').checked;
    cfg.search_only_golden=document.getElementById('sched-only-golden').checked;
    const {ok,data}=await postJson('/api/config',cfg);
    if(ok){
      rememberConfigPatch(cfg);
      toast(enabled ? 'Scheduler saved' : 'Automation disabled','ok');
      renderRunLibs();
      applyScheduleEnabledState();
    }else{
      toast(data?.message || data?.error || 'Scheduler save failed','err');
    }
  },500);
}

function appendLine(line){
  const t=document.getElementById('terminal');
  if(!t) return;
  const d=document.createElement('div');
  d.className='ll '+(line.includes('[OK]')?'ll-ok':line.includes('[FAILED]')?'ll-err':line.includes('[RESOLVE]')?'ll-resolve':line.includes('WARNING')||line.includes('WARN')?'ll-warn':line.includes('ERROR')?'ll-err':line.startsWith('═')||line.startsWith('─')||line.startsWith('=')?'ll-head':line.includes('Library:')||line.includes('═══')?'ll-lib':'ll-info');
  d.textContent=line; t.appendChild(d); t.scrollTop=t.scrollHeight;
}

function clearTerminal(){ const t=document.getElementById('terminal'); if(t) t.innerHTML='<div class="ll ll-info">Cleared.</div>'; }
async function stopRun(scope='run'){
  toast('Stopping run…','info');
  if(_activeRunContext) _activeRunContext.stoppedByUser=true;
  await fetch('/api/run/stop',{method:'POST'});
  const activeScope=_activeRunContext?.scope||scope;
  const stopCopy=_activeRunContext?.scopeLabel ? `Stopped · ${_shortScopeLabel(_activeRunContext.scopeLabel)}` : 'Stopped';
  setProgressState(activeScope,'stopped',{copy:stopCopy,meta:'Stop requested. Waiting for process shutdown…',showRetry:true,pct:Math.min(99,parseInt(_progressEls(activeScope).fill?.style.width||'0',10)||0)});
  if(activeScope==='run') _setRunUiIdle('Stopping…',false);
}

// ── SECTION: TASKS LOGIC ───────────────────────────────────────────────────────

function openRunResults(pass){
  const map={1:'MISSING',2:'STAGED',3:'AVAILABLE'};
  const st=map[pass]||'';
  showPage('theme-manager');
  setTimeout(()=>{
    if(st) filterByStatus(st);
    const search=document.getElementById('db-search');
    if(search) search.value='';
    toast(st?`Showing ${displayStatus(st)} rows from completed pass ${pass}.`:'Showing run rows.','info');
  },350);
}

function _activityMetricChips(stats={}, cls='hcard-chip metric'){
  const s1=stats?.pass1||null;
  const s2=stats?.pass2||null;
  const s3=stats?.pass3||null;
  const chips=[];
  const add=(label, value)=>{
    if(value===undefined || value===null || value==='' || Number.isNaN(Number(value))) return;
    chips.push(`<span class="${cls}">${label} · ${value}</span>`);
  };
  if(s1){
    add('Added', s1.new);
    add('Missing', s1.missing);
    add('Already available', s1.has_theme);
    if(!s2 && s1.staged) add('Staged', s1.staged);
    if(!s3 && s1.approved) add('Approved', s1.approved);
  }
  if(s2){
    add('Staged', s2.staged);
    add('Still missing', s2.missing);
    add('Failed', s2.failed);
  }
  if(s3){
    add('Downloaded', s3.available);
    add('Failed', s3.failed);
    add('Skipped', s3.skipped);
  }
  return chips;
}

async function loadSchedulerHistory(){
  const el=document.getElementById('scheduler-run-history');
  if(!el) return;
  const r=await fetch('/api/history');
  const runs=await r.json();
  if(!runs.length){
    el.innerHTML='<div class="empty">No Scheduler run history yet.</div>';
    return;
  }
  const PASS_LABELS={0:'Automated Scheduler Run',1:'Step 1 — Scan Libraries',2:'Step 2 — Find Theme Sources',3:'Step 3 — Download Themes'};
  const OUTCOME_BADGES={success:'pb-3 Success',error:'pb-e Error',stopped:'pb-2 Stopped'};
  el.innerHTML=runs.reverse().map((run,i)=>{
    const outcome=(run.outcome||run.status||'success').toLowerCase();
    const bc=OUTCOME_BADGES[outcome] || 'pb-e Error';
    const [cls,...parts]=bc.split(' ');
    const passLabel=PASS_LABELS[run.pass]||'Pipeline Run';
    const summary=(run.summary||'No summary recorded.').replace(/</g,'&lt;');
    const scope=(run.scope||'').replace(/</g,'&lt;');
    const metricChips=_activityMetricChips(run.stats||{});
    const meta=[
      scope?`<span class="hcard-chip">Scope · ${scope}</span>`:'',
      run.duration_seconds?`<span class="hcard-chip">Duration · ${Math.round(run.duration_seconds)}s</span>`:'',
      ...metricChips
    ].filter(Boolean).join('');
    return `<div class="hcard">
      <div class="hcard-head" onclick="document.getElementById('scheduler-log-${i}').classList.toggle('open')">
        <div>
          <div class="hcard-time">${(run.time||'').replace(/</g,'&lt;')}</div>
          <div class="hcard-sum">${passLabel} · ${summary}</div>
          ${meta?`<div class="hcard-meta">${meta}</div>`:''}
          <div style="margin-top:8px"><button class="btn btn-ghost btn-xs" onclick="event.stopPropagation();openRunResults(${run.pass||0})">Open filtered rows</button></div>
        </div>
        <span class="pass-badge ${cls}"><span class="status-label">${parts.join(' ')}</span></span>
      </div>
      <div class="hcard-body" id="scheduler-log-${i}">${(run.log||'').replace(/</g,'&lt;')}</div>
    </div>`;
  }).join('');
}
