const rowsBody = document.querySelector('#themeRows');
const managerModal = document.querySelector('#managerModal');
const workflowModal = document.querySelector('#workflowModal');
const searchInput = document.querySelector('#searchInput');
const statusFilter = document.querySelector('#statusFilter');
const sortBy = document.querySelector('#sortBy');
const sortDirection = document.querySelector('#sortDirection');

const sampleResults = [
  { title: '10 Cloverfield Lane (Complete)', duration: 'NA', url: 'https://www.youtube.com/playlist?list=PLwvekjMc3144vdqg155f0e17T2DfdbmCH', best: true, offset: '00:45' },
  { title: '10 Cloverfield Lane 2016 Soundtrack', duration: 'NA', url: 'https://www.youtube.com/watch?v=2RcpadSZkvY', best: false, offset: '00:30' },
  { title: '10 Cloverfield Lane Soundtrack Music Suite', duration: '29:57', url: 'https://www.youtube.com/watch?v=5m4ZkEqQrn0', best: false, offset: '01:17' },
];

async function loadThemes() {
  if (!rowsBody) return;
  const params = new URLSearchParams({
    search: searchInput?.value || '',
    status: statusFilter?.value || 'All',
    sort: sortBy?.value || 'updated_at',
    direction: sortDirection?.value || 'desc',
  });
  const response = await fetch(`/api/themes?${params.toString()}`);
  const themes = await response.json();
  renderRows(themes);
}

function renderRows(themes) {
  rowsBody.innerHTML = '';
  themes.forEach((theme) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><input type="checkbox" aria-label="Select ${theme.title}"></td>
      <td>
        <div class="title-cell">
          <strong>${theme.title}</strong>
          <small>${theme.folder_path}</small>
        </div>
      </td>
      <td>${theme.year ?? ''}</td>
      <td><span class="status-pill ${theme.status.toLowerCase()}">${theme.status}</span></td>
      <td><button class="table-action" data-theme-id="${theme.id}">Manager</button></td>
      <td>${theme.golden_source_url || '—'}</td>
      <td>${theme.golden_source_offset_display}</td>
      <td>${theme.source_url || '—'}</td>
      <td>${theme.source_offset_display}</td>
      <td>${theme.updated_at || '—'}</td>
      <td>${theme.notes || '—'}</td>
      <td>${theme.folder_path}</td>
      <td>${theme.tmdb_id || '—'}</td>
    `;
    tr.querySelector('.table-action').addEventListener('click', () => openManager(theme));
    rowsBody.appendChild(tr);
  });
}

function openManager(theme) {
  document.querySelector('#managerTitle').textContent = theme.title;
  document.querySelector('#managerMeta').textContent = `${theme.year} · TMDB ${theme.tmdb_id}`;
  const statusButton = document.querySelector('#managerStatus');
  statusButton.textContent = theme.status;
  statusButton.className = `pill status-pill ${theme.status.toLowerCase()}`;
  document.querySelector('#localThemeSummary').textContent = theme.status === 'Available' ? 'Theme file present locally' : 'Missing locally';
  document.querySelector('#localThemePath').textContent = `File path: ${theme.folder_path}/theme.mp3`;
  document.querySelector('#localThemeDuration').textContent = theme.status === 'Available' ? 'Duration: 00:58' : 'Duration: —';
  document.querySelector('#sourceDetailsText').textContent = theme.source_url ? `${theme.source_url} · start ${theme.source_offset_display}` : 'No source is attached right now.';
  document.querySelector('#openWorkflowButton').onclick = () => openWorkflow(theme);
  toggleModal(managerModal, true);
}

function openWorkflow(theme) {
  document.querySelector('#workflowTitle').textContent = theme.title;
  document.querySelector('#workflowYear').textContent = theme.year;
  document.querySelector('#customQuery').value = `${theme.title} ${theme.year} theme song`;
  renderResults(theme);
  toggleModal(workflowModal, true);
}

function renderResults(theme) {
  const list = document.querySelector('#resultsList');
  list.innerHTML = '';
  sampleResults.forEach((result, index) => {
    const row = document.createElement('div');
    row.className = `result-row ${result.best ? 'best' : ''}`;
    row.innerHTML = `
      <span class="result-index">${index + 1}.</span>
      <div class="result-copy">
        <strong>${result.title}</strong>
        <small>${result.url}</small>
      </div>
      <span class="result-duration">${result.duration}</span>
      <button class="ghost-button preview-button">Preview</button>
      <button class="primary-button pick-button">Pick</button>
    `;
    row.querySelector('.pick-button').addEventListener('click', () => {
      document.querySelector('#previewTitle').textContent = result.title;
      document.querySelector('#previewUrl').textContent = result.url;
      document.querySelector('#previewOffset').textContent = result.offset;
    });
    list.appendChild(row);
  });
}

function toggleModal(modal, visible) {
  if (!modal) return;
  modal.classList.toggle('hidden', !visible);
  modal.setAttribute('aria-hidden', String(!visible));
}

document.querySelectorAll('[data-close-modal]').forEach((button) => {
  button.addEventListener('click', () => {
    toggleModal(document.querySelector(`#${button.dataset.closeModal}`), false);
  });
});

[searchInput, statusFilter, sortBy, sortDirection].forEach((element) => {
  element?.addEventListener('input', loadThemes);
  element?.addEventListener('change', loadThemes);
});

loadThemes();

/* ── Eye toggle (Configuration page) ── */
document.querySelectorAll('.eye-toggle').forEach(btn => {
  btn.addEventListener('click', () => {
    const input = document.querySelector('#' + btn.dataset.target);
    if (!input) return;
    const isPassword = input.type === 'password';
    input.type = isPassword ? 'text' : 'password';
    btn.querySelector('.eye-icon-open').classList.toggle('hidden', !isPassword);
    btn.querySelector('.eye-icon-closed').classList.toggle('hidden', isPassword);
  });
});

/* ── Dashboard ── */
const activityChart = document.querySelector('#activityChart');
if (activityChart) {
  const STATUS_COLORS = {
    missing: '#ff7f88',
    staged: '#8f68ff',
    approved: '#2190c7',
    available: '#39c58d',
    failed: '#f6b23c',
  };
  const ALL_STATUSES = Object.keys(STATUS_COLORS);
  let _dashLib = 'All';

  async function initDashboard() {
    const qty = document.querySelector('#rangeQty')?.value || '30';
    const freq = document.querySelector('#rangeFreq')?.value || 'Days';
    const params = new URLSearchParams({ library: _dashLib, range_qty: qty, range_freq: freq });
    const res = await fetch('/api/dashboard/summary?' + params.toString());
    const data = await res.json();
    renderKPIs(data.kpis);
    renderLibraryTabs(data.libraries);
    renderActivityChart(data.activity);
    renderChartLegend();
    document.querySelector('#lastRefreshed').textContent = data.last_refreshed;
  }

  function renderKPIs(kpis) {
    document.querySelector('#kpiTotal').textContent = kpis.total || 0;
    document.querySelector('#kpiMissing').textContent = kpis.missing || 0;
    document.querySelector('#kpiStaged').textContent = kpis.staged || 0;
    document.querySelector('#kpiApproved').textContent = kpis.approved || 0;
    document.querySelector('#kpiAvailable').textContent = kpis.available || 0;
    document.querySelector('#kpiFailed').textContent = kpis.failed || 0;
  }

  function renderLibraryTabs(libraries) {
    const container = document.querySelector('#libraryFilter');
    if (!container) return;
    const tabs = ['All', ...libraries];
    container.innerHTML = tabs.map(lib =>
      `<button class="ghost-button${lib === _dashLib ? ' active-pill' : ''}" data-lib="${lib}">${lib}</button>`
    ).join('');
    container.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', () => {
        _dashLib = btn.dataset.lib;
        initDashboard();
      });
    });
  }

  function renderActivityChart(activity) {
    const svg = activityChart;
    const W = 700, H = 240, PAD_L = 40, PAD_B = 50, PAD_T = 10, PAD_R = 10;
    const chartW = W - PAD_L - PAD_R;
    const chartH = H - PAD_T - PAD_B;

    if (!activity.length) {
      svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
      svg.innerHTML = `<text x="${W / 2}" y="${H / 2}" text-anchor="middle" fill="#7b7f98" font-size="14">No activity data for selected range</text>`;
      return;
    }

    // Compute max total per period
    let maxTotal = 0;
    activity.forEach(d => {
      let total = 0;
      ALL_STATUSES.forEach(s => { total += (d[s] || 0); });
      if (total > maxTotal) maxTotal = total;
    });
    if (maxTotal === 0) maxTotal = 1;

    const barW = Math.max(8, Math.min(40, (chartW / activity.length) - 4));
    const gap = (chartW - barW * activity.length) / (activity.length + 1);

    let svgContent = '';
    // Y-axis gridlines
    const yTicks = 4;
    for (let i = 0; i <= yTicks; i++) {
      const y = PAD_T + chartH - (chartH * i / yTicks);
      const val = Math.round(maxTotal * i / yTicks);
      svgContent += `<line x1="${PAD_L}" y1="${y}" x2="${W - PAD_R}" y2="${y}" stroke="#ece9f7" stroke-width="1"/>`;
      svgContent += `<text x="${PAD_L - 6}" y="${y + 4}" text-anchor="end" fill="#7b7f98" font-size="11">${val}</text>`;
    }

    // Bars
    activity.forEach((d, i) => {
      const x = PAD_L + gap + i * (barW + gap);
      let yOffset = 0;

      ALL_STATUSES.forEach(status => {
        const count = d[status] || 0;
        if (count === 0) return;
        const barH = (count / maxTotal) * chartH;
        const y = PAD_T + chartH - yOffset - barH;
        svgContent += `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" fill="${STATUS_COLORS[status]}" rx="3">`;
        svgContent += `<title>${status}: ${count}</title></rect>`;
        yOffset += barH;
      });

      // X-axis label
      const labelSkip = Math.max(1, Math.floor(activity.length / 12));
      if (i % labelSkip === 0) {
        const label = d.period.length > 10 ? d.period.slice(5) : d.period;
        svgContent += `<text x="${x + barW / 2}" y="${PAD_T + chartH + 18}" text-anchor="middle" fill="#7b7f98" font-size="10" transform="rotate(-35, ${x + barW / 2}, ${PAD_T + chartH + 18})">${label}</text>`;
      }
    });

    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    svg.innerHTML = svgContent;
  }

  function renderChartLegend() {
    const legend = document.querySelector('#chartLegend');
    if (!legend) return;
    legend.innerHTML = ALL_STATUSES.map(s =>
      `<span class="legend-item"><span class="legend-dot" style="background:${STATUS_COLORS[s]}"></span>${s}</span>`
    ).join('');
  }

  // Event listeners
  document.querySelector('#refreshDashboard')?.addEventListener('click', initDashboard);
  document.querySelector('#rangeQty')?.addEventListener('change', initDashboard);
  document.querySelector('#rangeFreq')?.addEventListener('change', initDashboard);

  initDashboard();
}
