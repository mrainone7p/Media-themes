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
