/* Space Tyrants — stable selected-planet inspector scrolling.
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
  '[class*="-card"]'
].join(',');

let stxInspectorLastPlanetId = state.selected?.id || null;

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
    : node.querySelector?.('.project-head strong,.card-title strong,.stx-resource-title b,strong,label,span');
  const label = stxInspectorNormalizeKey(labelNode?.textContent || node.textContent);
  const classes = [...(node.classList || [])]
    .filter(name => /(?:row|card|metric|section-label|specialization)/.test(name))
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

function stxInspectorDisableNativeAnchoring(){
  const body = document.getElementById('planetBody');
  if(body) body.style.overflowAnchor = 'none';
}

stxInspectorDisableNativeAnchoring();

const STX_stableInspectorRenderPlanet = renderPlanet;
renderPlanet = function(){
  const beforeBody = document.getElementById('planetBody');
  const currentPlanetId = state.selected?.id || null;
  const preserve = !!beforeBody && currentPlanetId !== null && currentPlanetId === stxInspectorLastPlanetId;
  const snapshot = preserve ? stxInspectorSnapshot(beforeBody) : null;

  STX_stableInspectorRenderPlanet();

  const afterBody = document.getElementById('planetBody');
  if(afterBody){
    afterBody.style.overflowAnchor = 'none';
    if(preserve) stxInspectorRestore(afterBody, snapshot);
  }
  stxInspectorLastPlanetId = currentPlanetId;
};
