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
