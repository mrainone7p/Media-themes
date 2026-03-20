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

const TASK_CARD_CONFIG={
  runPipeline:[
    {
      title:'Step 1 — Scan Libraries',
      description:'Checks the selected libraries in Plex, sees which items already have a local theme file, and updates each item to Missing or Available.',
      controlType:'button',
      buttonLabel:'Run Scan for Selected Libraries',
      buttonClass:'btn btn-blue btn-sm',
      handler:'taskRunPass(1)'
    },
    {
      title:'Step 2 — Find Theme Sources',
      description:'Looks for source matches for Missing items using your current discovery settings. Matches that look usable are saved as Staged for review.',
      controlType:'button',
      buttonLabel:'Run Find Sources for Selected Libraries',
      buttonClass:'btn btn-purple btn-sm',
      handler:'taskRunPass(2)'
    },
    {
      title:'Step 3 — Download Themes',
      description:'Downloads themes for Approved items, saves them into the media folders, and marks completed items as Available.',
      controlType:'button',
      buttonLabel:'Run Download for Selected Libraries',
      buttonClass:'btn btn-green btn-sm',
      handler:'taskRunPass(3)'
    }
  ],
  exportData:[
    {
      title:'★ Export Golden Source CSV',
      description:'Exports rows with source URLs only in Golden Source format. Tooltips explain each field.',
      controlType:'selectAndButton',
      selectId:'task-lib-export',
      selectClass:'filter-sel',
      selectTitle:'Library scope for this export',
      buttonLabel:'Export',
      buttonClass:'btn btn-amber btn-sm',
      buttonTitle:'Download a curated Golden Source-format CSV',
      handler:'taskExportGoldenSource()',
      cardClass:'golden-emphasis task-wide',
      extraHtml:'<div class="field-help compact" title="tmdb_id,title,year,source_url,start_offset,updated_at,notes">Fields: tmdb_id · title · year · source_url · start_offset · updated_at · notes</div>'
    },
    {
      title:'Community Candidate Export',
      description:'Export themes not yet in the official Golden Source list for community submission.',
      controlType:'selectAndTwoButtons',
      selectId:'task-lib-candidate',
      selectClass:'filter-sel',
      selectTitle:'Library scope for candidate export',
      buttonLabel:'Export Candidate CSV',
      buttonClass:'btn btn-amber btn-sm',
      handler:'taskExportCandidateCSV()',
      secondButtonLabel:'Submit for Review',
      secondButtonClass:'btn btn-purple btn-sm btn-outline',
      secondHandler:'openModal(\'submission-modal\')',
      cardClass:'task-wide',
      extraHtml:'<div class="field-help compact" style="margin-top:6px">Community submissions are reviewed before they are added to the official curated catalog.</div><div class="field-help compact" style="margin-top:4px;color:var(--text3)">Includes: tmdb_id · source_url · start_offset</div>'
    }
  ],
  cleanup:[
    {
      title:'Clean Up Logs',
      description:'Delete old .log files while keeping the most recent number of days.',
      controlType:'inputAndButton',
      inputId:'task-keep-days',
      inputClass:'filter-sel',
      inputValue:'14',
      inputStyle:'max-width:90px',
      buttonLabel:'Run',
      buttonClass:'btn btn-ghost btn-sm',
      handler:'taskCleanupLogs()'
    },
    {
      title:'Prune Task History',
      description:'Trim stored task and run history so only the newest entries remain.',
      controlType:'inputAndButton',
      inputId:'task-keep-runs',
      inputClass:'filter-sel',
      inputValue:'100',
      inputStyle:'max-width:90px',
      buttonLabel:'Run',
      buttonClass:'btn btn-ghost btn-sm',
      handler:'taskPruneHistory()'
    },
    {
      title:'Refresh Local Theme Detection',
      description:'Rescan one library so the app matches database state to the theme files currently on disk.',
      controlType:'selectAndButton',
      selectId:'task-lib-refresh',
      selectClass:'filter-sel',
      buttonLabel:'Run',
      buttonClass:'btn btn-ghost btn-sm',
      handler:'taskRefreshThemes()'
    }
  ],
  maintenance:[
    {
      title:'SQLite Backup + Optimize/Vacuum',
      description:'Creates a timestamped SQLite DB backup, then runs VACUUM to compact and optimize the database file for faster reads and lower disk usage.',
      controlType:'button',
      buttonLabel:'Run',
      buttonClass:'btn btn-ghost btn-sm',
      handler:'taskSqliteMaintenance()'
    },
    {
      title:'Clear All Source URLs',
      description:'Clears stored source URLs for the selected library. <strong class="danger-note">Irreversible.</strong>',
      controlType:'selectAndButton',
      selectId:'task-lib-clear-sources',
      selectClass:'filter-sel',
      buttonLabel:'Clear URLs',
      buttonClass:'btn btn-red btn-sm',
      handler:'taskClearAllSources()',
      titleStyle:'color:var(--red)'
    }
  ]
};

function renderTaskCards(){
  const renderRowControls=(card)=>{
    if(card.controlType==='button') return `<button class="${card.buttonClass}" onclick="${card.handler}">${card.buttonLabel}</button>`;
    if(card.controlType==='selectAndButton') return `<select class="${card.selectClass||'filter-sel'}" id="${card.selectId}"${card.selectTitle?` title="${card.selectTitle}"`:''}></select><button class="${card.buttonClass}" onclick="${card.handler}"${card.buttonTitle?` title="${card.buttonTitle}"`:''}>${card.buttonLabel}</button>`;
    if(card.controlType==='selectAndTwoButtons') return `<select class="${card.selectClass||'filter-sel'}" id="${card.selectId}"${card.selectTitle?` title="${card.selectTitle}"`:''}></select><div class="task-btn-group"><button class="${card.buttonClass}" onclick="${card.handler}">${card.buttonLabel}</button><button class="${card.secondButtonClass}" onclick="${card.secondHandler}">${card.secondButtonLabel}</button></div>`;
    if(card.controlType==='inputAndButton') return `<input class="${card.inputClass||'filter-sel'}" id="${card.inputId}" value="${card.inputValue||''}"${card.inputStyle?` style="${card.inputStyle}"`:''}><button class="${card.buttonClass}" onclick="${card.handler}">${card.buttonLabel}</button>`;
    return '';
  };
  const renderCards=(cards)=>cards.map(card=>`<div class="task-card${card.cardClass?` ${card.cardClass}`:''}"><div class="task-title"${card.titleStyle?` style="${card.titleStyle}"`:''}>${card.title}</div><div class="task-desc">${card.description}</div><div class="task-row">${renderRowControls(card)}</div>${card.extraHtml||''}</div>`).join('');

  const runGrid=document.getElementById('task-grid-run-pipeline');
  const exportGrid=document.getElementById('task-grid-export');
  const cleanupGrid=document.getElementById('task-grid-cleanup');
  if(runGrid) runGrid.innerHTML=renderCards(TASK_CARD_CONFIG.runPipeline);
  if(exportGrid) exportGrid.innerHTML=renderCards(TASK_CARD_CONFIG.exportData);
  if(cleanupGrid) cleanupGrid.innerHTML=renderCards([...TASK_CARD_CONFIG.cleanup, ...TASK_CARD_CONFIG.maintenance]);
}

async function loadTasksPage(){
  await loadTaskHistoryList();
}

async function loadTaskHistoryList(){
  const r=await fetch('/api/tasks/history?limit=200');
  const items=await r.json();
  const el=document.getElementById('task-history-list');
  if(!el) return;
  if(!items.length){ el.innerHTML='<div class="empty task-empty">No activity yet.</div>'; return; }
  const statusClass=(status)=>{
    const normalized=String(status||'info').toLowerCase();
    return ['success','error','stopped','info'].includes(normalized) ? normalized : 'info';
  };
  el.innerHTML=items.map(it=>{
    const chips=[
      it.scope?`<span class="task-activity-chip">Scope · ${(it.scope||'').replace(/</g,'&lt;')}</span>`:'',
      it.duration_seconds?`<span class="task-activity-chip">Duration · ${Math.round(it.duration_seconds)}s</span>`:'',
      ..._activityMetricChips(it.details?.stats||{}, 'task-activity-chip')
    ].filter(Boolean).join('');
    const actions=it.details?.pass?`<div class="task-activity-actions"><button class="btn btn-ghost btn-xs" onclick="openRunResults(${it.details.pass||0})">Open filtered rows</button></div>`:'';
    return `<div class="task-activity-card">
      <div class="task-activity-top">
        <div class="task-activity-main">
          <div class="task-activity-title">${(it.task||'Task').replace(/</g,'&lt;')}</div>
          <div class="task-activity-sub">${(it.time||'').replace(/</g,'&lt;')}${it.details?.pass?` · Step ${it.details.pass}`:''}</div>
        </div>
        <span class="task-status ${statusClass(it.outcome||it.status)}"><span class="status-label">${formatStatusLabel(it.outcome||it.status||'info')}</span></span>
      </div>
      <div class="task-activity-summary">${(it.summary||'No summary recorded.').replace(/</g,'&lt;')}</div>
      ${chips?`<div class="task-activity-meta">${chips}</div>`:''}
      ${actions}
    </div>`;
  }).join('');
}

function taskLibValue(id){ const el=document.getElementById(id); return el?el.value:''; }
let _taskLibSelection=new Set();
function _fillTaskLibSelects(){
  const libraryOpts = (_libs||[]).map(l=>`<option value="${l.name}">${l.name}</option>`).join('');
  const scopedOpts = '<option value="__all__">All libraries</option>' + libraryOpts;
  const exportEl=document.getElementById('task-lib-export'); if(exportEl) exportEl.innerHTML=scopedOpts;
  const candidateEl=document.getElementById('task-lib-candidate'); if(candidateEl) candidateEl.innerHTML=scopedOpts;
  const clearEl=document.getElementById('task-lib-clear-sources'); if(clearEl) clearEl.innerHTML=scopedOpts;
  const refreshEl=document.getElementById('task-lib-refresh'); if(refreshEl) refreshEl.innerHTML=libraryOpts;
  if(!_taskLibSelection.size){ (_libs||[]).forEach(l=>_taskLibSelection.add(l.name)); }
  const pills=document.getElementById('task-lib-pills');
  if(pills){
    pills.innerHTML=(_libs||[]).map(l=>{
      const active=_taskLibSelection.has(l.name);
      const safe=encodeURIComponent(String(l.name||''));
      return `<button class="task-lib-pill ${active?'active':''}" onclick="toggleTaskLib(decodeURIComponent('${safe}'))" title="Include ${l.name} in pipeline runs">${l.name}</button>`;
    }).join('');
  }
}

function toggleTaskLib(name){
  if(_taskLibSelection.has(name)) _taskLibSelection.delete(name);
  else _taskLibSelection.add(name);
  _fillTaskLibSelects();
}
function taskSelectAllLibs(v){
  _taskLibSelection=new Set(v?(_libs||[]).map(l=>l.name):[]);
  _fillTaskLibSelects();
}

async function taskRunPass(passNum){
  const libs=[..._taskLibSelection];
  if(!libs.length) return toast('Select at least one library','err');
  await startPipelineRun(passNum,'tasks',{libraries:libs,scopeLabel:_formatScopeLabel(libs,'selected libraries')});
  setTimeout(loadTasksPage,1200);
}
async function runTaskAction({
  url,
  body,
  successMessage,
  failureFallback='Task failed',
  onSuccess,
  reloadDatabase=false,
  reloadTasksPage=true
}){
  let response;
  try{
    response=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  }catch(e){
    toast(failureFallback,'err');
    return {ok:false,data:{},response:null};
  }

  let data={};
  try{
    data=await response.json();
  }catch(e){
    data={};
  }

  const ok=response.ok && data.ok!==false;
  if(!ok){
    toast(data.error||failureFallback,'err');
    return {ok:false,data,response};
  }

  const msg=(typeof successMessage==='function')?successMessage(data):successMessage;
  if(msg) toast(msg,'ok');
  if(typeof onSuccess==='function') await onSuccess(data);
  if(reloadDatabase) await loadDatabase();
  if(reloadTasksPage) await loadTasksPage();
  return {ok:true,data,response};
}
async function taskExportGoldenSource(){
  const lib=taskLibValue('task-lib-export');
  if(!lib) return toast('Select a library first','err');
  await runTaskAction({
    url:'/api/tasks/export-golden-source',
    body:{library:lib==='__all__'?'':lib},
    successMessage:(d)=>`Exported ${d.rows_exported} rows`,
    failureFallback:'Export failed',
    onSuccess:(d)=>{ if(d.download_url) window.open(apiUrl(d.download_url),'_blank'); }
  });
}
async function taskExportCandidateCSV(){
  const lib=taskLibValue('task-lib-candidate');
  if(!lib) return toast('Select a library first','err');
  await runTaskAction({
    url:'/api/tasks/export-candidate-csv',
    body:{library:lib==='__all__'?'':lib},
    successMessage:(d)=>`Exported ${d.rows_exported} candidate rows`,
    failureFallback:'Candidate export failed',
    onSuccess:(d)=>{ if(d.download_url) window.open(apiUrl(d.download_url),'_blank'); }
  });
}
async function taskCleanupLogs(){
  const keep=parseInt(document.getElementById('task-keep-days')?.value||'14',10);
  await runTaskAction({
    url:'/api/tasks/cleanup-logs',
    body:{keep_days:keep},
    successMessage:(d)=>`Removed ${d.deleted} logs`,
    failureFallback:'Cleanup failed'
  });
}
async function taskPruneHistory(){
  const keep=parseInt(document.getElementById('task-keep-runs')?.value||'100',10);
  await runTaskAction({
    url:'/api/tasks/prune-history',
    body:{keep_runs:keep},
    successMessage:(d)=>`Removed ${d.removed_runs} run entries`,
    failureFallback:'Prune failed'
  });
}
async function taskRefreshThemes(){
  const lib=taskLibValue('task-lib-refresh');
  if(!lib) return toast('Select a library first','err');
  await runTaskAction({
    url:'/api/tasks/refresh-themes',
    body:{library:lib},
    successMessage:(d)=>`Updated ${d.updated} rows`,
    failureFallback:'Refresh failed',
    reloadDatabase:true
  });
}
async function taskSqliteMaintenance(){
  await runTaskAction({
    url:'/api/tasks/sqlite-maintenance',
    body:{backup:true,vacuum:true},
    successMessage:'SQLite backup/vacuum complete',
    failureFallback:'Maintenance failed',
    onSuccess:(d)=>{ if(d.download_url) window.open(apiUrl(d.download_url),'_blank'); }
  });
}
async function taskClearAllSources(){
  const lib=taskLibValue('task-lib-clear-sources');
  if(!lib) return toast('Select a library first','err');
  const scopeLabel = lib==='__all__' ? 'all libraries' : lib;
  const ok=await openConfirmModal('Clear all source URLs',{
    detail:`This will clear all source URLs in ${scopeLabel}. This action is irreversible.`,
    okLabel:'Clear URLs',
    tone:'danger'
  });
  if(!ok) return;
  await runTaskAction({
    url:'/api/tasks/clear-source-urls',
    body:{library:lib==='__all__'?'':lib},
    successMessage:(d)=>{
      const parts=[`Cleared ${d.cleared} URLs${d.libraries_cleared?` across ${d.libraries_cleared} libraries`:''}`];
      if(d.preserved_available) parts.push(`${d.preserved_available} kept ${displayStatus('AVAILABLE')}`);
      if(d.reset_missing) parts.push(`${d.reset_missing} reset to ${displayStatus('MISSING')}`);
      return parts.join(' · ');
    },
    failureFallback:'Failed',
    reloadDatabase:true
  });
}

