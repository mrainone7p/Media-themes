try{
  document.addEventListener('DOMContentLoaded', async ()=>{
    await loadUiTerminology();
    await loadStatusModel();
    initSidebarNav();
    initSharedProgress();
    removeItemDetailsPanel();
    updateGlobalRunStatus();
    setInterval(updateGlobalRunStatus, 4000);
    setInterval(removeItemDetailsPanel, 2000);
    const initial=((location.hash||'').replace(/^#/,'').trim());
    if(['dashboard','configuration','database','theme-manager','schedule','scheduler','tasks'].includes(initial)){
      showPage(initial);
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
loadDashboard();
bindOffsetWheel();
