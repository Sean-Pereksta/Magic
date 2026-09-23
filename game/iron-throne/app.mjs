import { BUILDINGS, HOUSES, RESOURCE_ICONS, RESOURCES, TERRAINS, UNITS } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, build, buildCheck, commandLimit, createGame, economyProjection, kingdom, mergeArmies, orderArmy, parseSave, recruit, settlements, sizeOf, splitArmy, strength, treaty } from './core.mjs';
import { LABELS, commitDeal, deliverPledge, describeIntent, endTurn, evaluateDeal, validateIntent } from './diplomacy.mjs';
import { DiplomacyClient } from './chat.mjs';
import { WorldMap } from './map.mjs';

const $ = id => document.getElementById(id), SAVE_KEY = 'catnmice.iron-throne.v1';
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const costText = cost => Object.entries(cost).map(([r, n]) => `${n} ${r}`).join(' · ');
let state = createGame(), selected = '5,6', selectedArmy = null, tab = 'land', orderMode = null, activeRuler = 'wintermere', proposals = [], epoch = 0, toastTimer, outcomeShown = false;
let restored = false, config = {}, client = new DiplomacyClient(), turnstileWidget = null, challengeToken = '';
try {
  const saved = localStorage.getItem(SAVE_KEY);
  if (saved) { state = parseSave(saved); restored = true; }
} catch (error) { $('load-warning').hidden = false; $('load-warning').textContent = `Your saved campaign could not be loaded: ${error.message} Starting a new reign will replace it.`; }
const map = new WorldMap($('map'), { getState: () => state, onSelect: selectTile });

function toast(message) { $('toast').textContent = message; $('toast').hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => { $('toast').hidden = true; }, 4200); }
function save() {
  try { localStorage.setItem(SAVE_KEY, JSON.stringify(state)); $('save-status').textContent = `Saved on this device · turn ${state.turn}`; restored = true; return true; }
  catch { $('save-status').textContent = 'Save unavailable · export a copy'; return false; }
}
function changed() { epoch++; proposals = []; save(); render(); }
function result(action) { if (!action.ok) toast(action.error); else changed(); return action.ok; }
function selectTile(id) {
  if (orderMode && selectedArmy) {
    const ok = result(orderArmy(state, PLAYER, selectedArmy, id, orderMode));
    if (ok) { toast('Marching orders issued. Armies move when you end the turn.'); orderMode = null; }
  }
  selected = id; map.selected = id;
  const army = state.armies.find(a => a.tile === id && a.owner === PLAYER);
  if (!orderMode && army) selectedArmy = army.id;
  tab = 'land'; render();
}
function goTo(id, armyId) { selected = id; selectedArmy = armyId || state.armies.find(a => a.tile === id && a.owner === PLAYER)?.id || null; tab = 'land'; map.selected = id; map.center(id); render(); }
function render() {
  const k = kingdom(state, PLAYER), { income } = economyProjection(state, PLAYER);
  $('turn').textContent = `Turn ${state.turn}`;
  $('season').textContent = `${['SPRING', 'SUMMER', 'AUTUMN', 'WINTER'][(state.turn - 1) % 4]} · YEAR ${Math.floor((state.turn - 1) / 4) + 1}`;
  $('resources').innerHTML = RESOURCES.map(r => `<div class="resource" title="${escape(r)}: ${income[r] >= 0 ? '+' : ''}${income[r]} next turn"><span class="resource-icon">${RESOURCE_ICONS[r]}</span><div><small>${r}</small><b>${k.resources[r]}</b><span class="income ${income[r] < 0 ? 'negative' : ''}">${income[r] >= 0 ? '+' : ''}${income[r]} / turn</span></div></div>`).join('') + `<div class="resource"><span class="resource-icon">♟</span><div><small>Population</small><b>${k.population}</b><span class="income">${k.happiness}% content</span></div></div>`;
  const rivals = state.kingdoms.filter(h => h.id !== PLAYER && alive(state, h.id)), allies = rivals.filter(h => treaty(state, PLAYER, h.id, 'alliance') || treaty(state, PLAYER, h.id, 'vassalage'));
  $('objective').textContent = `${settlements(state, PLAYER).length}/${Math.ceil(settlements(state).length * .6)} settlements · ${allies.length}/${Math.floor(rivals.length / 2) + 1} allies · Accord ${state.diplomaticTurns}/3 turns`;
  $('end-turn').disabled = !!state.outcome;
  document.querySelectorAll('[data-tab]').forEach(b => { b.classList.toggle('active', b.dataset.tab === tab); b.setAttribute('aria-current', b.dataset.tab === tab ? 'page' : 'false'); });
  const scroll = $('panel').scrollTop;
  $('panel').innerHTML = tab === 'land' ? landPanel() : tab === 'realm' ? realmPanel() : tab === 'council' ? councilPanel() : ledgerPanel();
  $('panel').scrollTop = scroll;
  $('latest-events').innerHTML = state.events.slice(0, 3).map(e => `<div class="event"><b>T${e.turn}</b>${escape(e.message)}</div>`).join('');
  const t = state.tiles[selected]; $('coordinates').textContent = `${t.name || TERRAINS[t.terrain].name} · ${t.id}`;
  $('order-hint').hidden = !orderMode; $('order-hint').textContent = 'Select a destination · Esc cancels';
  map.armyId = selectedArmy; map.selected = selected; map.draw();
  if ($('diplomacy').open) renderDiplomacy();
  if (state.outcome && !outcomeShown) {
    outcomeShown = true; $('outcome-title').textContent = state.outcome.won ? 'The crown is yours.' : 'A dynasty falls.';
    $('outcome-reason').textContent = state.outcome.reason;
    if (!$('welcome').open) $('outcome').showModal();
  }
}
function landPanel() {
  const t = state.tiles[selected], k = kingdom(state, PLAYER), owner = kingdom(state, t.owner), armies = state.armies.filter(a => a.tile === selected);
  let html = `<span class="eyebrow">${escape(owner?.name || 'THE UNCLAIMED MARCHES')}</span><div class="selection-title"><h2>${escape(t.name || TERRAINS[t.terrain].name)}</h2><span class="badge">${escape(t.id)}</span></div><div class="tile-meta">${TERRAINS[t.terrain].name}${t.resource ? ` · ${t.resource} deposits` : ''}${t.river ? ' · River crossing' : ''}${t.road ? ' · Road' : ''}</div>`;
  if (t.building) html += `<div class="realm-card"><strong>${BUILDINGS[t.building]?.name || 'City'}</strong><div class="fine">${t.walls ? `Walls ${t.walls}/60 · ` : ''}${t.market ? 'Market · ' : ''}${t.workshop ? 'Workshop · ' : ''}Defense ×${(TERRAINS[t.terrain].defense * (t.building === 'fort' ? 1.6 : 1) * (t.walls ? 1.6 : 1)).toFixed(1)}</div></div>`;
  if (t.project) html += `<div class="realm-card"><strong>${BUILDINGS[t.project.type].name} underway</strong><div class="fine">${t.project.remaining} turn${t.project.remaining === 1 ? '' : 's'} until completion</div><div class="progress"><span style="width:${100 * (1 - t.project.remaining / (BUILDINGS[t.project.type].turns + 1))}%"></span></div></div>`;
  html += armies.map(a => `<div class="army-card ${a.id === selectedArmy ? 'selected' : ''}"><div class="army-name"><strong>${escape(kingdom(state, a.owner).name)}</strong><span class="badge">${sizeOf(a)} troops</span></div><div class="army-stats">${Object.entries(a.units).filter(([, n]) => n > 0).map(([u, n]) => `<span>${UNITS[u].icon} ${n} ${UNITS[u].name}</span>`).join('')}</div><p class="fine">Strength ${Math.round(strength(a))} · ${a.path.length ? `Marching toward ${escape(a.target)} (${a.path.length} hexes)` : 'Holding position'}</p>${a.owner === PLAYER ? `<div class="button-row"><button data-order="${a.id}">March</button><button data-hold="${a.id}">Hold</button><button data-split="${a.id}">Split</button></div>${armies.filter(x => x.owner === PLAYER).length > 1 ? '<button class="full" data-merge="true">Combine armies here</button>' : ''}` : `<button class="full" data-talk="${a.owner}">Speak to ruler</button>`}</div>`).join('');
  if (t.owner === PLAYER && ['city', 'town', 'fort'].includes(t.building)) {
    html += `<div class="section-label">MUSTER TROOPS · ${k.commands} ORDERS LEFT</div><div class="build-list">${Object.entries(UNITS).map(([id, u]) => `<button class="build-card" data-recruit="${id}" ${state.outcome || !k.commands ? 'disabled' : ''}><span class="build-title">${u.icon} ${u.count} ${u.name}</span><span class="build-cost">${costText(u.cost)}</span><span class="build-desc">${u.description}</span></button>`).join('')}</div>`;
  }
  if (t.owner === PLAYER || !t.owner) {
    const relevant = Object.keys(BUILDINGS).filter(type => {
      if (['wall', 'market', 'workshop'].includes(type)) return ['city', 'town'].includes(t.building);
      if (type === 'city') return t.building === 'town';
      if (type === 'road') return !t.road;
      if (t.building) return false;
      const b = BUILDINGS[type]; return !b.terrain || b.terrain.includes(t.terrain);
    });
    html += `<div class="section-label">CONSTRUCTION · ${k.commands}/${commandLimit(state, PLAYER)} ORDERS</div><div class="build-list">${relevant.map(type => { const b = BUILDINGS[type], why = buildCheck(state, PLAYER, selected, type); return `<button class="build-card" data-build="${type}" ${why ? 'disabled' : ''} title="${escape(why || b.description)}"><span class="build-title">${b.icon} ${b.name} <small>· ${b.turns}t</small></span><span class="build-cost">${costText(b.cost)}</span><span class="build-desc">${escape(why || b.description)}</span></button>`; }).join('')}</div>`;
  } else html += `<div class="realm-card"><p class="fine">${atWar(state, PLAYER, t.owner) ? 'Enemy territory. March an army here to invade.' : 'Neutral borders. Negotiate an alliance for military access, or declare war.'}</p><button class="full" data-talk="${t.owner}">Visit ${escape(owner.name)}</button></div>`;
  return html;
}
function realmPanel() {
  const k = kingdom(state, PLAYER), { income, routes } = economyProjection(state, PLAYER);
  return `<span class="eyebrow">HOUSE ASHEN</span><h2>Your realm</h2><div class="stat-grid"><span>Construction orders</span><b>${k.commands}/${commandLimit(state, PLAYER)}</b><span>Population happiness</span><b>${k.happiness}%</b><span>Connected trade routes</span><b>${routes}</b><span>Settlements</span><b>${settlements(state, PLAYER).length}</b></div><div class="section-label">TAX POLICY</div><label>Balance income and growth<select id="tax">${['low', 'medium', 'high'].map(t => `<option ${k.tax === t ? 'selected' : ''}>${t}</option>`).join('')}</select></label><p class="fine">Low taxes grow population and happiness. High taxes produce gold but reduce happiness.</p><div class="section-label">NET CHANGE NEXT TURN</div><div class="stat-grid">${RESOURCES.map(r => `<span>${r}</span><b class="${income[r] < 0 ? 'negative' : ''}">${income[r] >= 0 ? '+' : ''}${income[r]}</b>`).join('')}</div><div class="section-label">YOUR SETTLEMENTS</div>${settlements(state, PLAYER).map(t => `<button class="full" data-goto="${t.id}">♜ ${escape(t.name)} · ${t.id}</button>`).join('')}<div class="section-label">YOUR ARMIES</div>${armiesOf(state, PLAYER).map(a => `<button class="full" data-goto="${a.tile}" data-army="${a.id}">⚑ ${sizeOf(a)} troops · ${a.tile}</button>`).join('')}<p class="fine">Roads must connect every hex between settlements to earn trade income. Trade agreements permit economic routes through your partners' land; alliances permit army passage.</p>`;
}
function councilPanel() {
  return `<span class="eyebrow">FIVE RULERS. FIVE AMBITIONS.</span><h2>The great houses</h2><p class="fine">An agreement becomes binding only after you review and ratify its terms.</p>${state.kingdoms.filter(k => k.id !== PLAYER).map(k => { const r = k.relations[PLAYER], status = !alive(state, k.id) ? 'Fallen' : atWar(state, PLAYER, k.id) ? 'At war' : treaty(state, PLAYER, k.id, 'alliance') ? 'Allied' : 'At peace'; return `<article class="house-card" style="--house:${k.color}"><span class="house-sigil">${k.sigil}</span><h3>${escape(k.name)}</h3><p>${escape(k.ruler)} · ${status}</p><p>Opinion ${r.opinion > 0 ? '+' : ''}${r.opinion} · Trust ${r.trust > 0 ? '+' : ''}${r.trust}<br>${k.honor > .8 ? 'Honorable' : k.honor < .4 ? 'Unpredictable' : 'Pragmatic'} · ${k.aggression > .7 ? 'Warlike' : k.greed > .8 ? 'Mercantile' : 'Watchful'}</p><button data-talk="${k.id}" ${!alive(state, k.id) ? 'disabled' : ''}>Enter the council chamber →</button></article>`; }).join('')}`;
}
function ledgerPanel() {
  const pledges = state.pledges.filter(p => [p.debtor, p.creditor].includes(PLAYER));
  const treaties = state.treaties.filter(t => t.parties.includes(PLAYER));
  return `<span class="eyebrow">A WORD IS A DEBT</span><h2>Oaths & treaties</h2><p class="fine">Allies must travel, gather troops and finish construction. Delays can break a promise. Defenders must stay on station for two turns.</p>${treaties.map(t => `<div class="pledge-card"><strong>${escape(t.type)} · ${escape(kingdom(state, t.parties.find(p => p !== PLAYER)).name)}</strong><p>Expires on turn ${t.expires} (${t.expires - state.turn} left)</p></div>`).join('')}${pledges.length ? pledges.slice().reverse().map(p => `<div class="pledge-card"><strong>${escape(LABELS[p.intent.type])}</strong><p>${escape(kingdom(state, p.debtor).name)} → ${escape(kingdom(state, p.creditor).name)}</p><p>${escape(describeIntent(p.intent))}</p><p class="${p.status}">${escape(p.status.toUpperCase())} · Due turn ${p.deadline}${p.intent.type === 'DEFEND' ? ` · On station ${p.held}/2` : ''}</p>${p.status === 'pending' && p.debtor === PLAYER ? `<button class="full" data-deliver="${p.id}">Deliver promised resources</button>` : ''}</div>`).join('') : '<p class="empty">No military or payment pledges yet. Negotiate one with a ruler.</p>'}`;
}

$('panel').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b || b.disabled) return;
  const d = b.dataset;
  if (d.build) result(build(state, PLAYER, selected, d.build));
  if (d.recruit) { const r = recruit(state, PLAYER, selected, d.recruit); if (r.ok) selectedArmy = r.armyId; result(r); }
  if (d.order) { selectedArmy = d.order; orderMode = 'move'; map.armyId = selectedArmy; render(); toast('Select a destination on the map.'); }
  if (d.hold) { orderMode = null; result(orderArmy(state, PLAYER, d.hold, selected, 'hold')); }
  if (d.split) result(splitArmy(state, PLAYER, d.split));
  if (d.merge) result(mergeArmies(state, PLAYER, selected));
  if (d.goto) goTo(d.goto, d.army);
  if (d.talk) openDiplomacy(d.talk);
  if (d.deliver) result(deliverPledge(state, d.deliver));
});
$('panel').addEventListener('change', e => { if (e.target.id === 'tax' && !state.outcome) { kingdom(state, PLAYER).tax = e.target.value; changed(); } });
document.querySelectorAll('[data-tab]').forEach(b => b.addEventListener('click', () => { tab = b.dataset.tab; orderMode = null; $('panel').scrollTop = 0; render(); }));
$('end-turn').addEventListener('click', () => { orderMode = null; endTurn(state); changed(); });
$('zoom-in').onclick = () => map.setZoom(map.zoom * 1.25);
$('zoom-out').onclick = () => map.setZoom(map.zoom / 1.25);
$('home').onclick = () => map.home(); $('fit-map').onclick = () => map.fit();
document.addEventListener('keydown', e => { if (e.key === 'Escape') { orderMode = null; render(); } });
document.querySelectorAll('[data-close]').forEach(b => b.addEventListener('click', () => $(b.dataset.close).close()));
$('menu-button').onclick = () => $('menu').showModal();
$('help').onclick = () => $('help-dialog').showModal();
$('save-now').onclick = () => toast(save() ? 'Campaign saved on this device.' : 'Local storage is unavailable. Export a save file.');
function showNewCampaign() {
  document.querySelectorAll('dialog[open]').forEach(d => d.close());
  $('resume').hidden = !restored; $('start-game').textContent = restored ? 'Replace save & begin a new reign' : 'Begin your reign'; $('welcome').showModal();
}
$('new-campaign').onclick = showNewCampaign; $('play-again').onclick = showNewCampaign;
$('new-game-form').addEventListener('submit', e => {
  e.preventDefault(); client.cancel(); state = createGame(Number($('seed').value), $('preset').value); epoch++;
  selected = '5,6'; selectedArmy = state.armies[0].id; tab = 'land'; orderMode = null; outcomeShown = false;
  $('welcome').close(); $('load-warning').hidden = true; map.home(); changed(); toast('Your reign begins. Select land to build, or visit Houses to negotiate.');
});
$('resume').onclick = () => { $('welcome').close(); map.home(); render(); if (state.outcome) $('outcome').showModal(); };
function downloadSave() {
  const blob = new Blob([JSON.stringify(state, null, 2)], { type: 'application/json' }), url = URL.createObjectURL(blob), a = document.createElement('a');
  a.href = url; a.download = `iron-throne-turn-${state.turn}.json`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}
$('export-save').onclick = downloadSave;
$('import-save').addEventListener('change', async e => {
  const file = e.target.files[0]; if (!file) return;
  try {
    if (file.size > 2000000) throw new Error('Save files must be smaller than 2 MB.');
    const imported = parseSave(await file.text());
    if (!confirm('Replace your current campaign with this imported save? Export your current save first if you want to keep it.')) return;
    client.cancel(); state = imported; selected = settlements(state, PLAYER)[0]?.id || '5,6'; selectedArmy = null; outcomeShown = false; epoch++; $('menu').close(); map.home(); changed(); toast('Campaign imported.');
  } catch (error) { toast(`Import failed: ${error.message}`); }
  finally { e.target.value = ''; }
});

function openDiplomacy(id) {
  activeRuler = id; proposals = []; state.conversations[id] ||= [];
  $('give-amount').value = '60'; $('receive-amount').value = '0'; $('offer-type').value = atWar(state, PLAYER, id) ? 'PEACE' : 'ALLIANCE';
  updateOfferFields(); renderDiplomacy(); $('diplomacy').showModal();
}
function renderDiplomacy() {
  const k = kingdom(state, activeRuler), r = k.relations[PLAYER];
  $('diplomacy').style.setProperty('--house', k.color);
  $('ruler-mark').textContent = k.sigil; $('ruler-house').textContent = k.name; $('ruler-name').textContent = k.ruler; $('ruler-motto').textContent = `“${k.motto}”`;
  $('ruler-relation').textContent = `Opinion ${r.opinion} · Trust ${r.trust} · ${atWar(state, PLAYER, k.id) ? 'At war' : 'At peace'}`;
  $('messages').innerHTML = (state.conversations[activeRuler] || []).map(m => `<div class="message ${escape(m.role)}"><small>${m.role === 'player' ? 'YOU · HOUSE ASHEN' : m.role === 'council' ? 'COUNCIL RULING' : escape(k.ruler.toUpperCase())}</small>${escape(m.text)}</div>`).join('') || `<div class="message"><small>${escape(k.ruler.toUpperCase())}</small>You have my attention, Regent. What brings your envoy to my court?</div>`;
  $('messages').scrollTop = $('messages').scrollHeight;
  renderProposals();
}
function renderProposals() {
  $('proposals').innerHTML = proposals.map((i, index) => {
    const v = evaluateDeal(state, activeRuler, i);
    return `<div class="proposal ${v.status}"><h4>${escape(LABELS[i.type])} · ${v.status.toUpperCase()}</h4><p>${escape(describeIntent(i))}</p><p>${escape(v.reason)}</p>${v.status === 'accept' ? `<button data-ratify="${index}" class="${['WAR', 'BETRAY'].includes(i.type) ? 'danger' : 'primary'}">${['WAR', 'BETRAY'].includes(i.type) ? 'Declare war with these consequences' : 'Ratify these terms'}</button>` : v.status === 'counter' ? `<button data-counter="${index}">Review counteroffer (${v.counter.giveAmount} ${v.counter.giveResource})</button>` : ''}</div>`;
  }).join('');
}
$('proposals').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b) return;
  if (b.dataset.counter !== undefined) {
    const index = Number(b.dataset.counter), v = evaluateDeal(state, activeRuler, proposals[index]);
    if (v.counter) proposals[index] = v.counter; renderProposals();
  }
  if (b.dataset.ratify !== undefined) {
    const index = Number(b.dataset.ratify), proposal = proposals[index];
    const r = commitDeal(state, activeRuler, proposal);
    if (r.ok) { appendMessage(activeRuler, 'council', `${describeIntent(proposal)} — ratified on turn ${state.turn}.`); toast('Agreement ratified. Check Pledges for any orders or promises.'); }
    result(r);
  }
});
function appendMessage(rulerId, role, text) {
  state.conversations[rulerId] ||= []; state.conversations[rulerId].push({ role, text: text.slice(0, 2000) }); state.conversations[rulerId] = state.conversations[rulerId].slice(-50);
}
$('offer-type').innerHTML = Object.entries(LABELS).map(([id, label]) => `<option value="${id}">${label}</option>`).join('');
for (const id of ['give-resource', 'receive-resource']) $(id).innerHTML = RESOURCES.map(r => `<option>${r}</option>`).join('');
$('give-resource').value = 'gold';
function updateOfferFields() {
  const type = $('offer-type').value;
  $('receive-fields').hidden = !['EXCHANGE', 'TRIBUTE'].includes(type);
  if ($('receive-fields').hidden) $('receive-amount').value = '0';
  if (['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE'].includes(type)) $('give-amount').value = '0';
  $('give-amount').disabled = ['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE'].includes(type);
  $('target-label').hidden = !['JOINT_WAR', 'DEFEND', 'POSITION', 'BUILD_DEFENSES', 'TERRITORY'].includes(type);
  let choices = [];
  if (type === 'JOINT_WAR') choices = state.kingdoms.filter(k => ![PLAYER, activeRuler].includes(k.id) && alive(state, k.id)).map(k => [k.id, k.name]);
  if (type === 'DEFEND') choices = Object.values(state.tiles).filter(t => t.owner === PLAYER && ['city', 'town', 'fort'].includes(t.building)).map(t => [t.id, `${t.name || 'Fort'} (${t.id})`]);
  if (type === 'POSITION') choices = [[selected, `Selected map tile (${selected})`], ...settlements(state).filter(t => [PLAYER, activeRuler].includes(t.owner) && t.id !== selected).map(t => [t.id, `${t.name} (${t.id})`])];
  if (type === 'BUILD_DEFENSES') choices = Object.values(state.tiles).filter(t => t.owner === activeRuler && !t.building && Number.isFinite(TERRAINS[t.terrain].cost)).slice(0, 60).map(t => [t.id, `${TERRAINS[t.terrain].name} (${t.id})`]);
  if (type === 'TERRITORY') choices = Object.values(state.tiles).filter(t => t.owner === activeRuler && ['city', 'town', 'fort'].includes(t.building) && !t.capital).map(t => [t.id, `${t.name || 'Fort'} (${t.id})`]);
  $('offer-target').innerHTML = choices.map(([id, label]) => `<option value="${escape(id)}">${escape(label)}</option>`).join('');
}
$('offer-type').onchange = updateOfferFields;
$('offer-form').addEventListener('submit', e => {
  e.preventDefault(); const i = validateIntent({ type: $('offer-type').value, giveResource: $('give-resource').value, giveAmount: Number($('give-amount').value), receiveResource: $('receive-resource').value, receiveAmount: Number($('receive-amount').value), targetId: $('target-label').hidden ? '' : $('offer-target').value, duration: Number($('duration').value) });
  if (!i) { toast('Use whole resource amounts and a duration between 2 and 20 turns.'); return; }
  proposals = [i]; renderProposals(); $('proposals').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
});
$('chat-form').addEventListener('submit', async e => {
  e.preventDefault(); const message = $('chat-message').value.trim();
  if (!message || client.busy || state.outcome) return;
  const rulerId = activeRuler, requestEpoch = epoch, campaign = state;
  const pending = client.send(campaign, rulerId, message, challengeToken, $('use-gemini').checked);
  appendMessage(rulerId, 'player', message); $('chat-message').value = ''; $('send-chat').disabled = true; $('send-chat').textContent = 'Envoy travelling…'; renderDiplomacy();
  try {
    const response = await pending;
    if (epoch !== requestEpoch || campaign !== state) { toast('The world changed while your envoy travelled. Send a new message for current terms.'); return; }
    appendMessage(rulerId, 'ruler', response.reply); save();
    if (rulerId === activeRuler) { proposals = response.intents; $('chat-notice').textContent = response.notice; renderDiplomacy(); }
    $('ai-status').textContent = response.source === 'gemini' ? 'Gemini council connected' : 'Scripted council ready';
  } finally {
    $('send-chat').disabled = false; $('send-chat').textContent = 'Send envoy →';
    challengeToken = ''; if (turnstileWidget !== null) globalThis.turnstile?.reset(turnstileWidget);
  }
});
async function enableGemini() {
  $('privacy').hidden = !$('use-gemini').checked;
  if (!$('use-gemini').checked) { $('turnstile').hidden = true; return; }
  $('turnstile').hidden = false;
  if (!client.endpoint || !config.turnstileSiteKey) { $('chat-notice').textContent = 'Gemini is not connected for this site yet. Use the scripted council and treaty desk.'; $('use-gemini').checked = false; $('privacy').hidden = true; return; }
  try {
    if (!globalThis.turnstile) await new Promise((resolve, reject) => {
      let script = document.getElementById('turnstile-script');
      if (script) script.remove();
      script = document.createElement('script'); script.id = 'turnstile-script'; script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit'; script.async = true; script.onload = resolve; script.onerror = reject; document.head.append(script);
    });
    if (turnstileWidget === null) turnstileWidget = globalThis.turnstile.render($('turnstile'), { sitekey: config.turnstileSiteKey, action: 'iron-throne', theme: 'dark', callback: token => { challengeToken = token; }, 'expired-callback': () => { challengeToken = ''; }, 'error-callback': () => { challengeToken = ''; $('chat-notice').textContent = 'Verification unavailable. Scripted diplomacy still works.'; } });
    $('chat-notice').textContent = 'Gemini conversations use a limited shared allowance. The treaty desk is always available.';
  } catch { $('chat-notice').textContent = 'Verification could not load. Scripted diplomacy still works.'; $('use-gemini').checked = false; }
}
$('use-gemini').onchange = enableGemini;
fetch('./config.json', { cache: 'no-store' }).then(r => r.ok ? r.json() : {}).then(data => { config = data; client = new DiplomacyClient({ endpoint: config.diplomacyEndpoint }); if (client.endpoint && config.turnstileSiteKey) $('ai-status').textContent = 'Gemini available · opt in at the council'; }).catch(() => {});
$('resume').hidden = !restored;
if (restored) $('start-game').textContent = 'Replace save & begin a new reign';
render(); $('welcome').showModal();
