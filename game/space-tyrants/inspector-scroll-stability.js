/* Space Tyrants — stable selected-planet inspector scrolling and controls.
   Loaded last so every existing renderPlanet wrapper runs inside this guard. */

const STX_INSPECTOR_SCROLL_CANDIDATES = [
  '.project-row',
  '.resource-row',
  '.facility-row',
  '.order-row',
  '.owner-row',
  '.metric',
  '.section-label',
  '.stx-resource-specialization',
  '.stx-inspector-disclosure',
  '[class*="-card"]'
].join(',');

const stxInspectorShipConstructionOpen = new Map();
let stxInspectorLastPlanetId = state.selected?.id || null;
let stxInspectorPointerActive = false;
let stxInspectorInteractionGuardInstalled = false;

function stxInspectorNormalizeKey(text){
  return String(text || '')
    .toLowerCase()
    .replace(/\d+(?:[.,]\d+)*/g, '#')
    .replace(/\s+/g, ' ')
    .trim();
}

function stxInspectorSectionKey(node, body){
  let cursor = node;
  while(cursor && cursor !== body){
    let prev = cursor.previousElementSibling;
    while(prev){
      if(prev.classList?.contains('section-label')) return stxInspectorNormalizeKey(prev.textContent);
      prev = prev.previousElementSibling;
    }
    cursor = cursor.parentElement;
  }
  return '';
}

function stxInspectorAnchorKey(node, body){
  if(!node) return '';
  const section = stxInspectorSectionKey(node, body);
  const labelNode = node.matches?.('.section-label')
    ? node
    : node.querySelector?.('.project-head strong,.card-title strong,.stx-resource-title b,.stx-inspector-disclosure-toggle span,strong,label,span');
  const label = stxInspectorNormalizeKey(labelNode?.textContent || node.textContent);
  const classes = [...(node.classList || [])]
    .filter(name => /(?:row|card|metric|section-label|specialization|disclosure)/.test(name))
    .sort()
    .join('.');
  return `${section}|${classes}|${label}`;
}

function stxInspectorCandidates(body){
  if(!body) return [];
  const bodyRect = body.getBoundingClientRect();
  return [...body.querySelectorAll(STX_INSPECTOR_SCROLL_CANDIDATES)]
    .filter(node => {
      const rect = node.getBoundingClientRect();
      return rect.height >= 6 && rect.bottom > bodyRect.top + 1 && rect.top < bodyRect.bottom - 1;
    })
    .sort((a,b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
}

function stxInspectorSnapshot(body){
  if(!body) return null;
  const bodyRect = body.getBoundingClientRect();
  const all = [...body.querySelectorAll(STX_INSPECTOR_SCROLL_CANDIDATES)];
  const seen = new Map();
  const occurrenceByNode = new Map();

  all.forEach(node => {
    const key = stxInspectorAnchorKey(node, body);
    const occurrence = seen.get(key) || 0;
    occurrenceByNode.set(node, occurrence);
    seen.set(key, occurrence + 1);
  });

  const anchors = stxInspectorCandidates(body).slice(0, 8).map(node => ({
    key: stxInspectorAnchorKey(node, body),
    occurrence: occurrenceByNode.get(node) || 0,
    offset: node.getBoundingClientRect().top - bodyRect.top
  }));

  return {
    scrollTop: body.scrollTop,
    anchors
  };
}

function stxInspectorRestore(body, snapshot){
  if(!body || !snapshot) return;
  const nodes = [...body.querySelectorAll(STX_INSPECTOR_SCROLL_CANDIDATES)];
  const byKey = new Map();

  nodes.forEach(node => {
    const key = stxInspectorAnchorKey(node, body);
    if(!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(node);
  });

  const bodyRect = body.getBoundingClientRect();
  for(const anchor of snapshot.anchors){
    const node = byKey.get(anchor.key)?.[anchor.occurrence];
    if(!node) continue;
    const currentOffset = node.getBoundingClientRect().top - bodyRect.top;
    body.scrollTop += currentOffset - anchor.offset;
    return;
  }

  const maxScroll = Math.max(0, body.scrollHeight - body.clientHeight);
  body.scrollTop = Math.min(snapshot.scrollTop, maxScroll);
}

function stxInspectorFindSection(body, label){
  if(!body) return null;
  const target = stxInspectorNormalizeKey(label);
  return [...body.querySelectorAll('.section-label')]
    .find(node => stxInspectorNormalizeKey(node.textContent) === target) || null;
}

function stxInspectorShipConstructionRows(body){
  const label = stxInspectorFindSection(body, 'Mandates & Active Projects');
  const list = label?.nextElementSibling;
  if(!list?.classList?.contains('project-list')) return {label, list:null, rows:[]};
  const rows = [...list.children].filter(row => {
    if(!row.matches?.('.project-row')) return false;
    const title = row.querySelector?.('.project-head strong')?.textContent || '';
    return /\bunder construction\b/i.test(title);
  });
  return {label, list, rows};
}

function stxInspectorShipConstructionCount(planet, rows){
  const queueCount = Array.isArray(planet?.buildQueue) ? planet.buildQueue.length : 0;
  return Math.max(queueCount, rows.length);
}

function stxInspectorDecorateShipConstruction(){
  const planet = state.selected;
  const body = document.getElementById('planetBody');
  if(!planet || !body || body.querySelector('.stx-ship-construction-disclosure')) return;

  const {label, list, rows} = stxInspectorShipConstructionRows(body);
  const count = stxInspectorShipConstructionCount(planet, rows);
  const hasShipyard = Number(planet.infra?.shipyard || 0) > 0;
  if(!label || !list || (!hasShipyard && count === 0 && rows.length === 0)) return;

  const open = stxInspectorShipConstructionOpen.get(planet.id) === true;
  const section = document.createElement('section');
  section.className = `stx-inspector-disclosure stx-ship-construction-disclosure${open ? ' open' : ''}`;

  const toggle = document.createElement('button');
  toggle.type = 'button';
  toggle.className = 'stx-inspector-disclosure-toggle';
  toggle.setAttribute('aria-expanded', String(open));
  toggle.innerHTML = `<span>SHIPS BEING CONSTRUCTED</span><span class="stx-inspector-disclosure-summary"><b>${count}</b><small>${count === 1 ? 'ship' : 'ships'}</small><i aria-hidden="true">${open ? '▴' : '▾'}</i></span>`;

  const details = document.createElement('div');
  details.className = 'stx-inspector-disclosure-body project-list';
  details.hidden = !open;
  rows.forEach(row => details.appendChild(row));
  if(!rows.length){
    details.innerHTML = '<div class="project-empty">No ships are currently being produced at this world.</div>';
  }

  toggle.onclick = event => {
    event.preventDefault();
    event.stopPropagation();
    const next = toggle.getAttribute('aria-expanded') !== 'true';
    stxInspectorShipConstructionOpen.set(planet.id, next);
    toggle.setAttribute('aria-expanded', String(next));
    details.hidden = !next;
    section.classList.toggle('open', next);
    const chevron = toggle.querySelector('i');
    if(chevron) chevron.textContent = next ? '▴' : '▾';
  };

  section.append(toggle, details);
  list.insertAdjacentElement('afterend', section);

  if(!list.querySelector('.project-row,.project-empty')){
    const empty = document.createElement('div');
    empty.className = 'project-empty';
    empty.textContent = 'No active non-ship projects.';
    list.appendChild(empty);
  }
}

function stxInspectorInstallStyles(){
  if(document.getElementById('stxInspectorStabilityStyles')) return;
  const style = document.createElement('style');
  style.id = 'stxInspectorStabilityStyles';
  style.textContent = `
.stx-inspector-disclosure{margin:9px 0 2px;border:1px solid rgba(116,155,233,.16);border-radius:11px;background:linear-gradient(135deg,rgba(65,91,160,.11),rgba(18,29,62,.16));overflow:hidden}
.stx-inspector-disclosure-toggle{width:100%;min-height:42px;border:0;background:transparent;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 10px;cursor:pointer;text-align:left;color:#b9c7e2;font-size:.61rem;font-weight:900;letter-spacing:.11em}
.stx-inspector-disclosure-toggle:hover{background:rgba(78,231,255,.07)}
.stx-inspector-disclosure-summary{display:flex;align-items:center;gap:5px;white-space:nowrap;letter-spacing:0;color:#dce9ff}
.stx-inspector-disclosure-summary b{min-width:18px;text-align:right;font-size:.72rem;font-variant-numeric:tabular-nums;color:#fff}
.stx-inspector-disclosure-summary small{font-size:.56rem;font-weight:700;color:#8298bd}
.stx-inspector-disclosure-summary i{width:14px;text-align:center;font-size:.68rem;font-style:normal;color:#4ee7ff}
.stx-inspector-disclosure-body{padding:0 7px 7px}
.stx-inspector-disclosure-body .project-row:first-child{margin-top:0}
`;
  document.head.appendChild(style);
}

function stxInspectorDisableNativeAnchoring(){
  const body = document.getElementById('planetBody');
  if(body) body.style.overflowAnchor = 'none';
}

function stxInspectorInstallInteractionGuard(){
  if(stxInspectorInteractionGuardInstalled) return;
  stxInspectorInteractionGuardInstalled = true;
  document.addEventListener('pointerdown', event => {
    const body = document.getElementById('planetBody');
    stxInspectorPointerActive = !!body && body.contains(event.target);
  }, true);
  const release = () => { stxInspectorPointerActive = false; };
  document.addEventListener('pointerup', release, true);
  document.addEventListener('pointercancel', release, true);
  window.addEventListener('blur', release);
}

stxInspectorInstallStyles();
stxInspectorInstallInteractionGuard();
stxInspectorDisableNativeAnchoring();

const STX_stableInspectorRenderPlanet = renderPlanet;
renderPlanet = function(){
  /* Do not replace the inspector DOM between pointer-down and pointer-up. That
     small interaction lock prevents a live simulation refresh from moving a
     button out from under the cursor before the click can complete. */
  if(stxInspectorPointerActive) return;

  const beforeBody = document.getElementById('planetBody');
  const currentPlanetId = state.selected?.id || null;
  const preserve = !!beforeBody && currentPlanetId !== null && currentPlanetId === stxInspectorLastPlanetId;
  const snapshot = preserve ? stxInspectorSnapshot(beforeBody) : null;

  STX_stableInspectorRenderPlanet();

  const afterBody = document.getElementById('planetBody');
  if(afterBody){
    afterBody.style.overflowAnchor = 'none';
    stxInspectorDecorateShipConstruction();
    if(preserve) stxInspectorRestore(afterBody, snapshot);
  }
  stxInspectorLastPlanetId = currentPlanetId;
};
