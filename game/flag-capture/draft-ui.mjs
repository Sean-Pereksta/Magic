import {FACTIONS, draftCost, armyCost} from './engine.mjs';

const ROLES = ['leader', 'scout', 'brute', 'mystic'];
const FACTION_KEYS = ['bloom', 'chaos', 'choir'];
const DEFAULT_ROLES = ['leader', 'scout', 'scout', 'brute', 'mystic'];
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[char]));
const validFaction = value => FACTION_KEYS.includes(value);
const moneyValue = value => Number.isFinite(Number(value)) ? Math.max(12, Math.min(40, Math.round(Number(value)))) : 24;
const integer = value => typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
const homeRows = draft => draft.side === 'ai' ? [0, 1] : [draft.height - 2, draft.height - 1];
const pointKey = point => `${point.x},${point.y}`;
const validPoint = (draft, point) => point && Number.isInteger(point.x) && point.x >= 0 && point.x < draft.width && homeRows(draft).includes(point.y);
const cellName = (draft, point) => `${String.fromCharCode(65 + point.x)}${draft.height - point.y}`;

function affordableRoles(requested, money) {
  const army = ['leader'];
  for (const role of Array.isArray(requested) ? requested : DEFAULT_ROLES) {
    if (role === 'leader' || !ROLES.includes(role) || army.length >= 8) continue;
    if (armyCost([...army, role]) <= money) army.push(role);
  }
  return army;
}

function repairPositions(draft) {
  const used = new Set();
  const positions = draft.army.map((_, index) => {
    const point = draft.positions[index];
    if (!validPoint(draft, point) || used.has(pointKey(point))) return null;
    used.add(pointKey(point));
    return {x: point.x, y: point.y};
  });
  const columns = Array.from({length: draft.width}, (_, x) => x).sort((a, b) => Math.abs(a - (draft.width - 1) / 2) - Math.abs(b - (draft.width - 1) / 2) || a - b);
  const rows = draft.side === 'ai' ? [0, 1] : [draft.height - 1, draft.height - 2];
  const cells = rows.flatMap(y => columns.map(x => ({x, y})));
  draft.positions = positions.map(point => {
    if (point) return point;
    const free = cells.find(cell => !used.has(pointKey(cell)));
    used.add(pointKey(free));
    return {...free};
  });
}

/** Mutable, UI-owned purchase and deployment state; no battle state is changed. */
export function createDraft({faction = 'bloom', side = 'player', width = 9, height = 7, money = 24, army} = {}) {
  const draft = {
    faction: validFaction(faction) ? faction : 'bloom', side: side === 'ai' ? 'ai' : 'player',
    width: [7, 9, 11].includes(width) ? width : 9, height: [5, 7, 9].includes(height) ? height : 7,
    money: moneyValue(money), army: [], positions: [], selectedIndex: 0
  };
  draft.army = affordableRoles(army, draft.money);
  repairPositions(draft);
  return draft;
}

/** Preserve purchases and valid placements; repair only what the new setup invalidates. */
export function resizeDraft(draft, {faction, width, height, money} = {}) {
  if (validFaction(faction)) draft.faction = faction;
  if ([7, 9, 11].includes(width)) draft.width = width;
  if ([5, 7, 9].includes(height)) draft.height = height;
  if (money !== undefined) draft.money = moneyValue(money);
  while (armyCost(draft.army) > draft.money && draft.army.length > 1) {
    const index = draft.army.findLastIndex(role => role !== 'leader');
    draft.army.splice(index, 1);
    draft.positions.splice(index, 1);
    if (draft.selectedIndex > index) draft.selectedIndex--;
  }
  draft.selectedIndex = Math.max(0, Math.min(draft.army.length - 1, draft.selectedIndex));
  repairPositions(draft);
  return draft;
}

export function applyDraftAction(draft, action = {}) {
  const fail = error => ({ok: false, error});
  if (!draft || !Array.isArray(draft.army) || !Array.isArray(draft.positions)) return fail('Choose a warband first.');
  if (!action || typeof action !== 'object') return fail('Choose a warband command.');
  const index = integer(action.index);
  if (action.type === 'buy') {
    if (!['scout', 'brute', 'mystic'].includes(action.role)) return fail('Choose a scout, brute, or mystic. Your commander is already included.');
    if (draft.army.length >= 8) return fail('Your warband is full: eight creatures.');
    if (armyCost(draft.army) + draftCost(action.role) > draft.money) return fail('You need more starting money for that creature.');
    draft.army.push(action.role);
    draft.selectedIndex = draft.army.length - 1;
    repairPositions(draft);
    return {ok: true};
  }
  if (action.type === 'remove' || action.type === 'select') {
    if (!Number.isInteger(index) || index < 0 || index >= draft.army.length) return fail('Select a creature in your warband.');
    if (action.type === 'select') { draft.selectedIndex = index; return {ok: true}; }
    if (draft.army[index] === 'leader') return fail('Your commander must stay in the warband.');
    draft.army.splice(index, 1);
    draft.positions.splice(index, 1);
    if (draft.selectedIndex > index) draft.selectedIndex--;
    draft.selectedIndex = Math.max(0, Math.min(draft.army.length - 1, draft.selectedIndex));
    return {ok: true};
  }
  if (action.type === 'place') {
    const point = {x: integer(action.x), y: integer(action.y)};
    if (!validPoint(draft, point)) return fail('Place creatures inside your two deployment rows.');
    if (!Number.isInteger(draft.selectedIndex) || !draft.army[draft.selectedIndex]) return fail('Select a creature before placing it.');
    const occupied = draft.positions.findIndex(position => position.x === point.x && position.y === point.y);
    const previous = {...draft.positions[draft.selectedIndex]};
    draft.positions[draft.selectedIndex] = point;
    if (occupied >= 0 && occupied !== draft.selectedIndex) draft.positions[occupied] = previous;
    return {ok: true};
  }
  return fail('Choose a warband command.');
}

function sprite(draft, role) {
  return `<span class="draft-sprite" style="background-position:${ROLES.indexOf(role) * 100 / 3}% ${FACTION_KEYS.indexOf(draft.faction) * 50}%" aria-hidden="true"></span>`;
}

/** Delegated buttons use only data-draft-action, data-side, role/index/x/y. */
export function renderDraft(draft) {
  const faction = FACTIONS[draft.faction], spent = armyCost(draft.army), remaining = draft.money - spent;
  const selected = faction.units[draft.army[draft.selectedIndex]];
  const attrs = `data-side="${draft.side}"`;
  const roster = draft.army.map((role, index) => {
    const def = faction.units[role], position = cellName(draft, draft.positions[index]);
    return `<div class="draft-troop ${index === draft.selectedIndex ? 'is-selected' : ''}" role="listitem"><button type="button" class="draft-select" data-draft-action="select" ${attrs} data-index="${index}" aria-pressed="${index === draft.selectedIndex}" aria-label="Select ${esc(def.name)}, ${role === 'leader' ? 'commander' : role}, ${position}">${sprite(draft, role)}<span><strong>${esc(def.name)}</strong><small>${role === 'leader' ? 'Commander' : role} · ${draftCost(role)} coins · ${position}</small></span></button><button type="button" class="draft-remove" data-draft-action="remove" ${attrs} data-index="${index}" aria-label="${role === 'leader' ? 'Commander is required' : `Remove ${esc(def.name)} and refund ${draftCost(role)} coins`}" ${role === 'leader' ? 'disabled' : ''}>${role === 'leader' ? '★' : '×'}</button></div>`;
  }).join('');
  const shop = ['scout', 'brute', 'mystic'].map(role => {
    const def = faction.units[role], cost = draftCost(role);
    return `<button type="button" class="draft-buy" data-draft-action="buy" ${attrs} data-role="${role}" ${remaining < cost || draft.army.length >= 8 ? 'disabled' : ''} aria-label="Buy ${esc(def.name)} for ${cost} coins">${sprite(draft, role)}<span><strong>+ ${esc(def.name)}</strong><small>${role} · ${cost} coins</small></span></button>`;
  }).join('');
  const cells = homeRows(draft).flatMap(y => Array.from({length: draft.width}, (_, x) => {
    const index = draft.positions.findIndex(point => point.x === x && point.y === y), role = draft.army[index], name = role ? faction.units[role].name : 'empty';
    const label = `${cellName(draft, {x, y})}, ${name}. ${index >= 0 && index !== draft.selectedIndex ? 'Swap with' : 'Place'} ${selected.name}.`;
    return `<button type="button" class="draft-cell ${index === draft.selectedIndex ? 'is-selected' : ''} ${role ? 'is-occupied' : ''}" data-draft-action="place" ${attrs} data-x="${x}" data-y="${y}" aria-label="${esc(label)}" aria-pressed="${index === draft.selectedIndex}"><small>${cellName(draft, {x, y})}</small>${role ? sprite(draft, role) : '<span class="draft-cell-dot" aria-hidden="true">·</span>'}</button>`;
  })).join('');
  return `<section class="draft-builder" data-side="${draft.side}" style="--draft-faction:${faction.color};--draft-columns:${draft.width}" aria-labelledby="draft-title-${draft.side}"><header class="draft-heading"><div><h3 id="draft-title-${draft.side}">${draft.side === 'ai' ? 'Player 2 warband' : 'Starting warband'}</h3><p>${esc(faction.name)} · ${draft.army.length}/8 creatures · one commander</p></div><div class="draft-money" role="status" aria-live="polite"><strong>${remaining}<small> coins left</small></strong><span>${spent} / ${draft.money} spent</span></div></header><div class="draft-roster" role="list" aria-label="Purchased creatures">${roster}</div><div class="draft-shop" role="group" aria-label="Buy starting creatures">${shop}</div><div class="draft-deployment-heading"><strong>Arrange your starting positions</strong><span>Select a creature, then a cell. Occupied cells swap.</span></div><div class="draft-deployment" role="group" aria-label="${draft.side === 'ai' ? 'Player 2' : 'Player 1'} deployment, two home rows">${cells}</div></section>`;
}
