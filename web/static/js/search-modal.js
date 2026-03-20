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
    offset:_normalizedOffsetValue(sourceRow?.start_offset||'0')
  };
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

function selectSearchMethod(method){
  const goldenCard=document.getElementById('sm-card-golden');
  if(method==='golden_source' && goldenCard?.classList.contains('disabled')) return;
  _searchMethod=method;
  ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
    const card=document.getElementById('sm-card-'+m);
    if(card) card.classList.toggle('active',m===method);
  });
  _updateQueryDisplay();
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
    dlnow.style.display='none';
  } else if(step===2){
    back.style.display='';
    primary.style.display='none';
    dlnow.style.display='none';
  } else if(step===3){
    back.style.display='';
    primary.style.display='';
    primary.textContent=_manualSaveTargetStatus()==='APPROVED'?'Approve + Save':'Save for Review';
    primary.className='btn btn-amber';
    primary.onclick=saveSourceEditor;
    dlnow.style.display='';
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
  if(title){ posterEl.src=apiUrl('/api/poster/tmdb?title='+encodeURIComponent(title)+'&year='+encodeURIComponent(year||'')+'&size=w342'); posterEl.style.display=''; }
  const tmdbUrl='https://www.themoviedb.org/search/movie?query='+encodeURIComponent(title+' '+year);
  document.getElementById('search-modal-links').innerHTML=`<a class="modal-link-pill tmdb-pill" href="${tmdbUrl}" target="_blank" rel="noopener">🎬 TMDB</a>`;
  setBio('search-modal-bio',rk);
  // Restore or reset method cards
  ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
    const card=document.getElementById('sm-card-'+m);
    if(card) card.classList.toggle('active',m===_searchMethod);
  });
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
  const goldenOffsetValue=_normalizedOffsetValue(existingRow?.start_offset||'0');
  if(goldenCard) goldenCard.classList.toggle('disabled',!hasGolden);
  if(goldenCard) goldenCard.classList.toggle('recommended',hasGolden);
  if(goldenDesc) goldenDesc.textContent=hasGolden?'Use the curated source already linked to this item.':'No curated source is available for this item yet.';
  if(goldenOffset) goldenOffset.textContent=hasGolden?`Offset ${goldenOffsetValue}`:'Offset —';
  if(goldenMeta) goldenMeta.textContent=hasGolden?`Curated match • starts at ${goldenOffsetValue}`:'Manual search fallback';
  _setSearchMethodOrder(hasGolden);
  _initMethodQuickPicks(existingRow);
  if(_searchMethod==='golden_source' && !hasGolden){
    _searchMethod='playlist';
    ['playlist','direct','custom','paste','golden_source'].forEach(m=>{
      const card=document.getElementById('sm-card-'+m);
      if(card) card.classList.toggle('active',m===_searchMethod);
    });
  }
  const hasOwnedResults=_lastSearchResults.length && _lastSearchResultsKey===_searchStateKey(rk);
  const shouldRestoreStep3=sameItem && _searchCurrentStep===3 && hasSameItemDraft;
  if(hasOwnedResults) _renderResults(_lastSearchResults);
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
  if(_searchCurrentStep===3) goToSearchStep(2);
  else if(_searchCurrentStep===2) goToSearchStep(1);
  else closeSearchModal();
}

function searchModalPrimary(){
  if(_searchCurrentStep===1) doSearch();
  else if(_searchCurrentStep===3) saveSourceEditor();
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
    return goToStep3(url,{skipPreview:false,sourceTitle:'Golden Source URL',startOffset:golden.offset,entryMode:'golden_fast_path'});
  }
  await _searchByMethod(_searchMethod, true);
}


function _renderSearchResultsState(message, tone='info'){
  const el=document.getElementById('search-results');
  if(!el) return;
  el.innerHTML=`<div class="search-results-state ${tone==='error'?'error':''}">${message}</div>`;
}

async function _fallbackFromGoldenValidation(errorMessage){
  const notice=errorMessage||'Golden Source preview could not be loaded.';
  const fallbackMethod=_searchDefaultMethod||'playlist';
  _searchMethod=fallbackMethod;
  toast(`${notice} Showing ${fallbackMethod==='direct'?'Direct':'Playlist'} results instead.`, 'info');
  goToSearchStep(2);
  _renderSearchResultsState('Curated source could not be previewed. Searching for reviewable matches…');
  const results=await _searchByMethod(fallbackMethod, true);
  if(results && results.length){
    const el=document.getElementById('search-results');
    if(el){
      el.insertAdjacentHTML('afterbegin', `<div class="search-results-state" style="margin-bottom:10px">Curated source could not be previewed (${notice}). Review the fallback matches below, or go back and paste a replacement URL.</div>`);
    }
    return;
  }
  _renderSearchResultsState(`Curated source could not be previewed (${notice}). Go back to choose another method, or paste a replacement URL in Step 1.`,'error');
  const countEl=document.getElementById('sm-results-count');
  if(countEl) countEl.textContent='';
}

function _renderResults(results){
  const el=document.getElementById('search-results');
  const countEl=document.getElementById('sm-results-count');
  if(countEl) countEl.textContent=results.length ? `${results.length} results` : '';
  if(!results.length){
    el.innerHTML='<div class="search-results-state">No results found.</div>';
    return;
  }
  el.innerHTML=results.map((r,i)=>`
    <div class="search-result-card ${i===0?'recommended':''}">
      <div class="result-idx">${i+1}.</div>
      <div class="search-result-main">
        ${i===0?`<span class="ui-badge src-badge recommended result-chip">Best match</span>`:''}
        <a href="${r.url}" target="_blank" rel="noopener" class="search-result-title">${r.title.replace(/</g,'&lt;')} <span style="font-size:11px">↗</span></a>
      </div>
      <div class="search-result-actions">
        <span class="search-result-duration">${r.duration||'—'}</span>
        <button class="btn btn-ghost btn-xs" onclick="previewSearchResult('${r.url.replace(/'/g,"\\'")}',this)">▶ Preview</button>
        <button class="btn btn-amber btn-xs" onclick="goToStep3('${r.url.replace(/'/g,"\\'")}',{skipPreview:false,sourceTitle:'${r.title.replace(/'/g,"\\'").replace(/</g,'&lt;')}'})">Pick</button>
      </div>
    </div>`).join('');
  if(results[0]?.url) setTimeout(()=>previewSearchResult(results[0].url),80);
}


function _renderMethodQuickPick(method, result, opts={}){
  if(!result) return '<span class="sm-quickpick-title">No quick match available</span>';
  const safeMethod=String(method||'').replace(/'/g,"\\'");
  const safeUrl=String(result.url||'').replace(/'/g,"\\'");
  const safeHref=String(result.url||'').replace(/"/g,'&quot;');
  const rawTitle=String(opts.title || result.title || '1st result').trim();
  const safeTitleAttr=rawTitle.replace(/</g,'&lt;').replace(/"/g,'&quot;');
  const safeTitleJs=rawTitle.replace(/'/g,"\\'").replace(/</g,'&lt;').replace(/"/g,'&quot;');
  const safeOffset=String(result.start_offset||'0').replace(/'/g,"\\'");
  const label=opts.label || 'First match';
  const showOpen=opts.showOpen===true;
  const titleText=_truncateSourceText(rawTitle, {fallback:'1st result', max:52, middle:!!opts.truncateMiddle});
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
    ? {label:'Curated match', title:'Curated Golden Source URL', showOpen:true, linkTitle:false, selectLabel:'Select'}
    : {label:'First match'};
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
  if(_lastSearchResults.length) _renderResults(_lastSearchResults);
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
function searchPreviewToggle(){ _searchPreviewAudio.toggle(); }

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
    await saveSourceEditor(true);
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
  if(_lastSearchResults.length) _renderResults(_lastSearchResults);
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
  const loadBtn=document.getElementById('se-load-btn');
  if(loadBtn){ loadBtn.textContent='Loading…'; loadBtn.disabled=true; }
  _sourceEditorAudio.setPlaying(false);
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

function seUrlChanged(){
  const u=(document.getElementById('se-url').value||'').trim();
  if(u) _selectedSourceTitle=_sourceTitleFromUrl(u);
  _renderSelectedSourceSummary(u, u?_selectedSourceTitle:(_selectedSourceTitle||'—'));
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
  const btn=document.getElementById('se-load-btn');
  if(btn){ btn.textContent='Loading…'; btn.disabled=true; }
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
    if(btn){ btn.textContent='↺ Refresh'; btn.disabled=false; }
    if(!data.ok){
      const previewError=data.error||'Failed';
      document.getElementById('se-info').textContent='Error: '+previewError;
      if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select'){
        await _fallbackFromGoldenValidation(previewError);
      }
      return;
    }
    const audio=_sourceEditorAudio.audio;
    stopAllAudio('se-audio');
    audio.src=apiUrl(data.audio_url);
    _lastPreviewedUrl=url;
    const startedAt=Date.now();
    _sourceEditorAudio.setHandlers({
      onloadedmetadata:(loaded)=>{
        if(loadSeq!==_sePreviewLoadSeq) return;
        _applyAudioOffset(loaded, document.getElementById('se-offset').value||'0');
        document.getElementById('se-info').textContent=`Duration: ${fmt(loaded.duration)}`;
        seUpdateTrim();
        _sourceEditorAudio.play().catch(()=>{});
      },
      onerror:()=>{
        if(loadSeq!==_sePreviewLoadSeq) return;
        if(Date.now()-startedAt<700) return;
        const playbackError='Playback error — click Refresh to retry';
        document.getElementById('se-info').textContent=playbackError;
        if(_step3EntryMode==='golden_fast_path' || _step3EntryMode==='golden_manual_select'){
          void _fallbackFromGoldenValidation('Playback error');
        }
      }
    });
    audio.load();
  }catch(e){
    if(btn){ btn.textContent='↺ Refresh'; btn.disabled=false; }
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
  const offsetValue=document.getElementById('se-offset')?.value||'0';
  const duration=audio?.duration||0;
  const infoEl=document.getElementById('se-info');
  if(!trimWindow || !audio || !audio.duration){
    if(trimWindow) trimWindow.style.display='none';
    if(startLabel) startLabel.textContent='Start 0:00';
    if(endLabel) endLabel.textContent='End —';
    if(infoEl){
      const offsetFmt=_normalizedOffsetValue(offsetValue);
      infoEl.textContent=`Offset ${offsetFmt} — load a preview to confirm the kept portion`;
    }
    return;
  }
  const meta=_clipWindowMeta(duration, offsetValue, _maxDur);
  const startPct=(meta.start/duration)*100;
  const endPct=(meta.end/duration)*100;
  trimWindow.style.display='block';
  startShade.style.width=`${startPct}%`;
  endShade.style.left=`${endPct}%`;
  startMarker.style.left=`${startPct}%`;
  endMarker.style.left=`${endPct}%`;
  if(startLabel) startLabel.textContent=`Start ${fmt(meta.start)}`;
  if(endLabel) endLabel.textContent=`End ${fmt(meta.end)}`;
  if(infoEl){
    const clipLen=Math.max(0, meta.end-meta.start);
    const offsetFmt=_normalizedOffsetValue(offsetValue);
    infoEl.textContent=`Preview ${fmt(duration)} total · keeping ${fmt(clipLen)} from ${offsetFmt}`;
  }
}

function sePreviewFromOffset(){
  const audio=_sourceEditorAudio.audio;
  const s=parseTrim(document.getElementById('se-offset').value);
  if(!audio.src||audio.src===window.location.href){ seLoadPreview(); return; }
  stopAllAudio('se-audio');
  audio.currentTime=s;
  _sourceEditorAudio.play().catch(()=>{});
}

async function saveSourceEditor(skipClose=false){
  const key=_seKey||_searchKey; if(!key) return;
  const url=(document.getElementById('se-url').value||'').trim();
  const offset=parseTrim(document.getElementById('se-offset').value||'0');
  if(!url){ toast('Please enter a URL','info'); return; }
  const lib=_seLib||_searchLib||_activeLib||'';
  const status=_manualSaveTargetStatus();
  const notes=status==='APPROVED'?'Manual source auto-approved':'URL set via manual search — moved to Staged for approval';

  const saveResp=await fetch('/api/ledger/manual-source',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      rating_key:key,
      library:lib,
      url,
      start_offset:offset,
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
    const finalStatus=String(savedRow?.status||status).toUpperCase();
    toast(finalStatus==='APPROVED'
      ?uiTerm('actions.toasts.manual_saved_auto_approved','Saved — auto-approved')
      :uiTerm('actions.toasts.manual_saved_staged','Saved — staged for approval'),'ok');
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
let _trimRk='';
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
  runModalMediaCleanup(()=>_trimModalAudio.cleanup());
}
async function applyTrimFromModal(){
  if(!_trimRk) return;
  const library=requireLibraryContext(_activeLib,'trim a theme');
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
  posterEl.src=apiUrl('/api/poster?key='+rk); posterEl.style.display='';
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
  setBio('yt-modal-bio', rk);
  document.getElementById('yt-modal-save').style.display='inline-flex';
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
