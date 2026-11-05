import { LABELS } from './diplomacy.mjs';
import { RESOURCES } from './data.mjs';
import { alive, atWar } from './core.mjs';
import { councilActive } from './council-state.mjs';
import { tradeBlocked } from './living.mjs';

const aliases = {
  ACCESS: 'passage travel cross crossing borders access',
  PEACE: 'peace ceasefire truce end war',
  WAR: 'declare declaration attack war',
  BETRAY: 'declare declaration break treaties betray war',
  PLEDGE_WAR: 'declare declaration promise enter join war',
  AID: 'gift donate aid give help',
  EXCHANGE: 'buy purchase sell swap resource exchange trade',
  RECURRING: 'buy purchase recurring deliveries supply shipment trade',
  PROMISE: 'promise payment later',
  NON_AGGRESSION: 'peace non aggression pact treaty',
  PLEDGE_PEACE: 'peace promise not attack',
};
const requestTypes = new Set(['ACCESS','JOINT_WAR','DEFEND','POSITION','BUILD_DEFENSES','WITHDRAW','TERRITORY','VASSALAGE','EMBARGO']);
const peacetimeTypes = new Set(['ALLIANCE','TRADE','EXCHANGE','DEFEND','POSITION','BUILD_DEFENSES','PROMISE','RECURRING','LOAN','NON_AGGRESSION','ACCESS','EMBARGO','GUARANTEE','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND','PLEDGE_WITHDRAW','PLEDGE_BUILD','PLEDGE_PEACE']);
const commercialTypes = new Set(['TRADE','EXCHANGE','RECURRING','LOAN']);

export function diplomaticActions(state, { actor, ruler, councilId = null }) {
  const council = councilId && state.allianceCouncils?.find(c => c.id === councilId);
  if (councilId && (!council || !council.participants.includes(actor) || !councilActive(state, council))) return [];
  const members = (council ? council.participants.filter(h => h !== actor) : [ruler]).filter(h => h && alive(state, h));
  if (!members.length || state.outcome || state.phase === 'founding') return [];
  const name = id => state.kingdoms.find(h => h.id === id)?.name || id;
  const audience = members.map(name).join(', ');
  const actions = [];
  function add(type, label, { intent = {}, direction = requestTypes.has(type) ? 'request' : 'offer', houses = members, terms = false, keywords = '' } = {}) {
    // Availability uses public diplomatic restrictions only. Resource amounts
    // and negotiable terms remain editable in the existing review form.
    houses = houses.filter(h => !(peacetimeTypes.has(type) && atWar(state, actor, h)) && !(commercialTypes.has(type) && tradeBlocked(state, actor, h)));
    if (!houses.length) return;
    actions.push({ id: `${type}-${actions.length}`, label, detail: houses.map(name).join(', '), keywords: `${label} ${aliases[type] || ''} ${keywords}`, type, direction, intent: { type, duration: 10, ...intent }, requestedHouses: houses, councilId, ruler: houses[0], terms });
  }
  for (const [type, label] of Object.entries(LABELS)) {
    if (council && ['WAR','BETRAY','VASSALAGE','MARRIAGE','INTELLIGENCE'].includes(type)) continue;
    if (type === 'PEACE') {
      const enemies = members.filter(h => atWar(state, actor, h));
      if (enemies.length) add(type, `Peace treaty · ${name(actor)} and ${enemies.map(name).join(', ')}`, { houses: enemies });
      continue;
    }
    if (['WAR','BETRAY'].includes(type) && atWar(state, actor, ruler)) continue;
    add(type, label, { terms: ['MARRIAGE','INTELLIGENCE'].includes(type) });
  }
  for (const resource of RESOURCES) {
    add('EXCHANGE', `Buy ${resource} · resource exchange`, { direction: 'request', intent: { giveResource: resource, giveAmount: 10, receiveResource: 'gold', receiveAmount: 0 }, keywords: `request receive ${resource}` });
    add('RECURRING', `Request recurring ${resource} deliveries`, { direction: 'request', intent: { giveResource: resource, giveAmount: 10, receiveResource: 'gold', receiveAmount: 0 }, keywords: `buy ${resource}` });
    add('AID', `Gift ${resource} · offer aid`, { intent: { giveResource: resource, giveAmount: 10 }, keywords: `give offer ${resource}` });
  }
  for (const target of state.kingdoms.filter(h => h.id !== actor && (council || !members.includes(h.id)) && alive(state, h.id))) {
    const enemies = members.filter(h => h !== target.id && atWar(state, h, target.id));
    if (enemies.length) add('PEACE', `Request peace · ${enemies.map(name).join(', ')} and ${target.name}`, { direction: 'request', houses: enemies, intent: { targetId: target.id } });
  }
  return actions.map(action => ({ ...action, detail: action.detail || audience }));
}

export function searchDiplomaticActions(actions, query) {
  const words = String(query).toLowerCase().trim().split(/\s+/).filter(Boolean);
  if (!words.length) return [];
  return actions.filter(action => words.every(word => action.keywords.toLowerCase().includes(word)))
    .sort((a, b) => Number(b.label.toLowerCase().startsWith(words.join(' '))) - Number(a.label.toLowerCase().startsWith(words.join(' '))));
}

export function installDiplomaticActionSearch(details, { getActions, select }) {
  const doc = details.ownerDocument, panel = details.querySelector('.chat-options-panel');
  const section = doc.createElement('section');
  section.className = 'diplomatic-action-search';
  const label = doc.createElement('label');
  label.textContent = 'Search diplomatic actions';
  const input = doc.createElement('input');
  input.type = 'search'; input.placeholder = 'Search diplomatic actions…'; input.autocomplete = 'off';
  input.setAttribute('aria-label', 'Search diplomatic actions');
  const results = doc.createElement('div');
  results.className = 'diplomatic-search-results';
  const status = doc.createElement('p'); status.className = 'fine'; status.setAttribute('role', 'status');
  label.append(input); section.append(label, results, status); panel.prepend(section);
  const siblings = [...panel.children].filter(node => node !== section);
  const previousHidden = new Map();
  let entries = [];
  function render() {
    entries = searchDiplomaticActions(getActions(), input.value);
    const searching = !!input.value.trim();
    for (const node of siblings) {
      if (searching && !previousHidden.has(node)) previousHidden.set(node, node.hidden);
      if (searching) node.hidden = true;
      else if (previousHidden.has(node)) { node.hidden = previousHidden.get(node); previousHidden.delete(node); }
    }
    results.replaceChildren();
    for (const entry of entries) {
      const button = doc.createElement('button'); button.type = 'button';
      const title = doc.createElement('strong'); title.textContent = entry.label;
      const detail = doc.createElement('small'); detail.textContent = entry.detail;
      button.append(title, detail);
      button.addEventListener('click', () => {
        // Re-resolve against the current conversation before opening a form.
        const current = getActions().find(action => action.id === entry.id && action.label === entry.label && action.detail === entry.detail);
        if (current) select(current);
      });
      results.append(button);
    }
    status.textContent = searching ? entries.length ? `${entries.length} actions. Select one to review the terms.` : 'No matching actions in this conversation.' : 'Search offers, requests, declarations, treaties and promises.';
  }
  input.addEventListener('input', render);
  input.addEventListener('keydown', event => {
    if (event.key === 'Enter') { event.preventDefault(); results.querySelector('button')?.click(); }
    if (event.key === 'ArrowDown') { event.preventDefault(); results.querySelector('button')?.focus(); }
  });
  details.addEventListener('toggle', () => { if (details.open) { render(); input.focus({ preventScroll: true }); } });
  render();
  return { input, render };
}
