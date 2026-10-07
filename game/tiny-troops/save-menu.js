/* Saved-run browser. Persistence and profile credentials stay in the game adapter. */
(function () {
  'use strict';
  const files = window.TinyTroopsSaveFiles, $ = id => document.getElementById(id);
  const esc = text => String(text ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  if (!files) return;
  const panel = document.createElement('section'); panel.id = 'ttSaveFiles'; panel.className = 'tt-save-files';
  panel.setAttribute('aria-label', 'Available saved runs');
  panel.innerHTML = '<div class="tt-save-heading"><h3>Saved runs on this device</h3><button id="ttRefreshSaves" class="secondary tiny" type="button">Refresh</button></div><p class="tt-save-help">Choose a run to load it. For a save from another device, enter its name and code above.</p><div id="ttSaveList"></div>';
  $('menuStatus').after(panel);
  const status = document.createElement('div'); status.id = 'ttSaveState'; status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
  status.className = 'tt-save-state'; status.textContent = 'Progress saves automatically.'; $('manualSave').closest('footer').appendChild(status);
  const resolveButton = document.createElement('button'); resolveButton.id = 'ttResolveSave'; resolveButton.className = 'secondary'; resolveButton.hidden = true; resolveButton.textContent = 'Resolve cloud save'; document.body.appendChild(resolveButton);
  resolveButton.onclick = () => { openMainMenu(); conflictPanel.scrollIntoView({ block: 'nearest' }); };
  const conflictPanel = document.createElement('section'); conflictPanel.id = 'ttSaveConflict'; conflictPanel.className = 'tt-save-conflict'; conflictPanel.hidden = true; conflictPanel.setAttribute('role', 'alert'); panel.before(conflictPanel);
  let resolving = false;
  function renderConflict() {
    const conflict = files.conflict(); resolveButton.hidden = !conflict || $('mainMenu').classList.contains('open'); conflictPanel.hidden = !conflict;
    if (!conflict) { conflictPanel.replaceChildren(); return; }
    conflictPanel.innerHTML = `<b>Cloud save: ${esc(conflict.name)}</b><p>${esc(conflict.message)} ${conflict.type === 'save/code-conflict' ? 'Enter the original save code above to load the cloud run.' : 'Loading the cloud run first keeps a separate device copy of your current progress.'}</p><div class="tt-save-conflict-actions"><button data-tt-save-resolve="load" ${resolving ? 'disabled' : ''}>Load cloud run</button><button class="secondary" data-tt-save-resolve="copy" ${resolving ? 'disabled' : ''}>Save this run separately</button><button class="secondary" data-tt-save-resolve="continue" ${resolving ? 'disabled' : ''}>Keep playing on this device</button></div>`;
  }
  conflictPanel.addEventListener('click', async event => {
    const button = event.target.closest('[data-tt-save-resolve]'); if (!button || button.disabled || resolving) return;
    resolving = true; renderConflict();
    try {
      const result = await files.resolve(button.dataset.ttSaveResolve);
      if (result && $('ttResume') && !$('ttResume').hidden && $('mainMenu').classList.contains('open')) $('ttResume').click();
    } finally { resolving = false; renderConflict(); renderList(); }
  });
  function renderList() {
    const entries = files.list();
    $('ttSaveList').innerHTML = entries.length ? entries.map(entry => {
      const date = entry.updatedAtMs ? new Date(entry.updatedAtMs).toLocaleString() : 'Older checkpoint';
      return `<article class="tt-save-row"><div class="tt-save-copy"><strong>${esc(entry.name)}</strong><span>${entry.readable ? `Wave ${entry.wave} · ${entry.size} troops · ${entry.coins} coins${entry.completed ? ' · Completed' : ''}` : 'This save could not be read.'}</span><small>${esc(date)}${entry.recovered ? ' · Recovery backup' : ''}</small><span class="tt-save-army" aria-label="Saved army">${esc(entry.army)}</span></div><button type="button" class="secondary" data-tt-load-save="${esc(entry.clean)}" aria-label="Load ${esc(entry.name)}" ${entry.readable ? '' : 'disabled'}>${entry.recovered ? 'Recover' : 'Load'}</button></article>`;
    }).join('') : `<p class="tt-save-empty">${files.available() ? 'No saved runs yet. Start a run with a name and code; its checkpoint will appear here.' : 'Browser storage is unavailable. Enter a save name and code above to load from the cloud.'}</p>`;
  }
  $('ttRefreshSaves').onclick = renderList;
  panel.addEventListener('click', async event => {
    const button = event.target.closest('[data-tt-load-save]'); if (!button || button.disabled) return;
    button.disabled = true;
    try { if (files.select(button.dataset.ttLoadSave)) await files.load(button.dataset.ttLoadSave); }
    finally { renderList(); }
  });
  window.addEventListener('tt-saves-changed', () => { renderList(); renderConflict(); });
  window.addEventListener('storage', event => { if (!event.key || event.key.startsWith(TinyTroopsSaves.PREFIX) || event.key.startsWith(TinyTroopsSaves.BACKUP_PREFIX)) renderList(); });
  window.addEventListener('tt-save-status', event => {
    const saved = event.detail; status.textContent = saved.text;
    status.dataset.state = saved.phase === 'failed' ? saved.local ? 'warning' : 'error' : saved.phase === 'syncing' ? 'pending' : 'saved';
    status.title = saved.payload ? 'Last checkpoint: ' + new Date(saved.payload.updatedAtMs).toLocaleString() : '';
    renderConflict();
  });
  function checkpoint() { if (S.tt?.started && files.active()) saveProfile('page checkpoint'); }
  // These synchronous local writes are independent of combat timer cancellation.
  window.addEventListener('pagehide', checkpoint);
  document.addEventListener('visibilitychange', () => { if (document.hidden) checkpoint(); });
  window.addEventListener('online', () => { if (S.tt?.started && files.active() && files.connection()) saveProfile('connection restored'); });
  renderList(); renderConflict();
})();
