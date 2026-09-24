import { ART, preloadAllArt, installArtFallbacks } from './asset-manifest.mjs';
import { buildingLevel } from './economy.mjs';
import { setFormation } from './warfare.mjs';
import { art, battleReports, buildingInspection, commercialConnections, constructionBrowser, economySummary, foreignEconomy, formationControl, musterBrowser, rivalTurnReports, systemTitle, tradePanel } from './expansion-ui.mjs';
import { BUILDINGS, HOUSES, RESOURCE_ICONS, RESOURCES, TERRAINS, UNITS } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, build, buildHighway, buildCheck, commandLimit, createGame, economyProjection, kingdom, mergeArmies, orderArmy, parseSave, recruit, settlements, sizeOf, splitArmy, strength, treaty } from './core.mjs';
import { appendConversation, applySpeech, ambassadorCapacity, ambassadorIncident, assignAmbassador, consumeMessage, diplomaticCapacity, economicRelationship, markRead, messageAllowance, recruitAmbassador, relationDescriptions } from './living.mjs';
import { isPlayerPromise } from './promises.mjs';
import { acceptRulerMemories, LABELS, commitDeal, deliverPledge, describeIntent, endTurn, evaluateDeal, validateIntent } from './diplomacy.mjs';
import { DiplomacyClient } from './chat.mjs';
import { CHECK_NAMES, CHECK_LABELS, diagnosticDetails, diagnosticReport } from './diagnostics.mjs';
import { WorldMap } from './map.mjs';

const $ = id => document.getElementById(id), SAVE_KEY = 'catnmice.iron-throne.v1';
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const costText = cost => Object.entries(cost).map(([r, n]) => `${n} ${r}`).join(' · ');
let state = createGame(), selected = '5,6', selectedArmy = null, tab = 'land', orderMode = null, activeRuler = 'wintermere', proposals = [], epoch = 0, toastTimer, outcomeShown = false;
let restored = false, config = {}, client = new DiplomacyClient(), turnstileWidget = null, challengeToken = '';
let sending = false, compactCouncil = false, reviewedTrade = null;
let configReady = false, verificationLoad = null, geminiChoiceMade = false;
let geminiAttempted = false, configurationFailure = false;
try {
  const saved = localStorage.getItem(SAVE_KEY);
  if (saved) { state = parseSave(saved); restored = true; }
} catch (error) { $('load-warning').hidden = false; $('load-warning').textContent = `Your saved campaign could not be loaded: ${error.message} Starting a new reign will replace it.`; }
installArtFallbacks(document);
$('resume').hidden = !restored;
$('start-game').disabled = $('resume').disabled = true;
$('welcome').addEventListener('cancel', event => { if ($('start-game').disabled) event.preventDefault(); });
$('welcome').showModal();
const artResult = await preloadAllArt(({completed,total,failed}) => {
  $('art-progress').max = total; $('art-progress').value = completed;
  $('art-status').textContent = `Loading artwork ${completed} / ${total}${failed ? ` · ${failed} unavailable` : ''}`;
});
$('art-status').textContent = artResult.failed ? `Artwork ready · ${artResult.failed} unavailable images will use fallback art.` : 'All artwork ready';
$('start-game').disabled = $('resume').disabled = false;
const map = new WorldMap($('map'), { getState: () => state, onSelect: selectTile });

function toast(message) { $('toast').textContent = message; $('toast').hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => { $('toast').hidden = true; }, 4200); }
function save() {
  try { localStorage.setItem(SAVE_KEY, JSON.stringify(state)); $('save-status').textContent = `Saved on this device · turn ${state.turn}`; restored = true; return true; }
  catch { $('save-status').textContent = 'Save unavailable · export a copy'; return false; }
}
function changed() { epoch++; proposals = []; state.diplomacy.offers = {}; save(); render(); }
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
  $('resources').innerHTML = RESOURCES.map(r => `<div class="resource" title="${escape(r)}: ${income[r] >= 0 ? '+' : ''}${income[r]} next turn"><span class="resource-icon">${art(ART.resources[r],r,'resource-art')}</span><div><small>${r}</small><b>${k.resources[r]}</b><span class="income ${income[r] < 0 ? 'negative' : ''}">${income[r] >= 0 ? '+' : ''}${income[r]} / turn</span></div></div>`).join('') + `<div class="resource"><span class="resource-icon">♟</span><div><small>Population</small><b>${k.population}</b><span class="income">${k.happiness}% content</span></div></div>`;
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
  map.armyId = selectedArmy; map.selected = selected; map.reducedEffects = !!state.presentation?.reducedEffects; map.draw();
  renderDispatches();
  if ($('diplomacy').open) renderDiplomacy();
  if (state.outcome && !outcomeShown) {
    outcomeShown = true; $('outcome-title').textContent = state.outcome.won ? 'The crown is yours.' : 'A dynasty falls.';
    $('outcome-reason').textContent = state.outcome.reason;
    if (!$('welcome').open) $('outcome').showModal();
  }
}
function landPanel() {
  const t = state.tiles[selected], k = kingdom(state, PLAYER), owner = kingdom(state, t.owner), armies = state.armies.filter(a => a.tile === selected);
  let html = `<span class="eyebrow">${escape(owner?.name || 'THE UNCLAIMED MARCHES')}</span><div class="selection-title"><h2>${escape(t.name || TERRAINS[t.terrain].name)}</h2><span class="badge">${escape(t.id)}</span></div><div class="tile-meta">${TERRAINS[t.terrain].name}${t.resource ? ` · ${t.quality} ${t.resource} deposit` : ''}${t.river ? ' · River crossing' : ''}${t.road ? ' · Road' : ''}</div>`;
  html += buildingInspection(state,t);
  html += armies.map(a => `<div class="army-card ${a.id === selectedArmy ? 'selected' : ''}"><div class="army-name"><strong>${escape(kingdom(state, a.owner).name)}</strong><span class="badge">${sizeOf(a)} troops</span></div><div class="army-stats">${Object.entries(a.units).filter(([, n]) => n > 0).map(([u, n]) => `<span>${UNITS[u].icon} ${n} ${UNITS[u].name}</span>`).join('')}</div><p class="fine">Strength ${Math.round(strength(a))} · ${a.path.length ? `Marching toward ${escape(a.target)} (${a.path.length} hexes)` : 'Holding position'}</p>${a.owner === PLAYER ? `${formationControl(a)}<div class="button-row"><button data-order="${a.id}">March</button><button data-hold="${a.id}">Hold</button><button data-split="${a.id}">Split</button></div>${armies.filter(x => x.owner === PLAYER).length > 1 ? '<button class="full" data-merge="true">Combine armies here</button>' : ''}` : `<button class="full" data-talk="${a.owner}">Speak to ruler</button>`}</div>`).join('');
  if (t.owner === PLAYER && ['city', 'town', 'fort'].includes(t.building)) {
    html += musterBrowser(state,t);
  }
  if (t.owner === PLAYER || !t.owner) {
    html += constructionBrowser(state,t);
  } else html += `<div class="realm-card"><p class="fine">${atWar(state, PLAYER, t.owner) ? 'Enemy territory. March an army here to invade.' : 'Neutral borders. Negotiate an alliance for military access, or declare war.'}</p><button class="full" data-talk="${t.owner}">Visit ${escape(owner.name)}</button></div>`;
  if(t.owner===PLAYER && (['city','town','tradeOutpost'].includes(t.building)||t.market))html+=tradePanel(state,t)+commercialConnections(state,t);
  html += ambassadorCards(state.ambassadors.filter(a => a.tile === selected && a.status !== 'dead'));
  return html;
}
function realmPanel() {
  const k = kingdom(state, PLAYER), { income, routes } = economyProjection(state, PLAYER);
  return `${systemTitle('kingdom')}<span class="eyebrow">HOUSE ASHEN</span><h2>Your realm</h2><div class="stat-grid"><span>Construction orders</span><b>${k.commands}/${commandLimit(state, PLAYER)}</b><span>Population happiness</span><b>${k.happiness}%</b><span>Connected trade routes</span><b>${routes}</b><span>Settlements</span><b>${settlements(state, PLAYER).length}</b></div><div class="section-label">TAX POLICY</div><label>Balance income and growth<select id="tax">${['low', 'medium', 'high'].map(t => `<option ${k.tax === t ? 'selected' : ''}>${t}</option>`).join('')}</select></label><p class="fine">Low taxes grow population and happiness. High taxes produce gold but reduce happiness.</p><div class="section-label">NET CHANGE NEXT TURN</div><div class="stat-grid">${RESOURCES.map(r => `<span>${r}</span><b class="${income[r] < 0 ? 'negative' : ''}">${income[r] >= 0 ? '+' : ''}${income[r]}</b>`).join('')}</div><div class="section-label">YOUR SETTLEMENTS</div>${settlements(state, PLAYER).map(t => `<button class="full" data-goto="${t.id}">♜ ${escape(t.name)} · ${t.id}</button>`).join('')}<div class="section-label">YOUR ARMIES</div>${armiesOf(state, PLAYER).map(a => `<button class="full" data-goto="${a.tile}" data-army="${a.id}">⚑ ${sizeOf(a)} troops · ${a.tile}</button>`).join('')}${economySummary(state)}${rivalTurnReports(state)}${tradePanel(state)}${battleReports(state)}${ambassadorPanel()}<p class="fine">Roads must connect every hex between settlements to earn trade income. Trade agreements permit economic routes through your partners' land; alliances permit army passage.</p>`;
}
function councilPanel() {
  return `${systemTitle('diplomacy')}${tradePanel(state)}<span class="eyebrow">FIVE RULERS. FIVE AMBITIONS.</span><h2>The great houses</h2><p class="fine">An agreement becomes binding only after you review and ratify its terms.</p>${state.kingdoms.filter(k => k.id !== PLAYER).map(k => { const r = k.relations[PLAYER], status = !alive(state, k.id) ? 'Fallen' : atWar(state, PLAYER, k.id) ? 'At war' : treaty(state, PLAYER, k.id, 'alliance') ? 'Allied' : 'At peace'; return `<article class="house-card" style="--house:${k.color}"><span class="house-sigil">${k.sigil}</span><h3>${escape(k.name)}</h3><p>${escape(k.ruler)} · ${status}</p><p>Opinion ${r.opinion > 0 ? '+' : ''}${r.opinion} · Trust ${r.trust > 0 ? '+' : ''}${r.trust}<br>${k.honor > .8 ? 'Honorable' : k.honor < .4 ? 'Unpredictable' : 'Pragmatic'} · ${k.aggression > .7 ? 'Warlike' : k.greed > .8 ? 'Mercantile' : 'Watchful'}</p>${foreignEconomy(state,k.id)}<button data-talk="${k.id}" ${!alive(state, k.id) ? 'disabled' : ''}>Enter the council chamber →</button></article>`; }).join('')}`;
}
function ledgerPanel(rulerId = null) {
  const pledges = state.pledges.filter(p => [p.debtor, p.creditor].includes(PLAYER) && (!rulerId || [p.debtor, p.creditor].includes(rulerId)));
  const treaties = state.treaties.filter(t => t.parties.includes(PLAYER) && (!rulerId || t.parties.includes(rulerId)));
  return `<span class="eyebrow">A WORD IS A DEBT</span><h2>Oaths & treaties</h2><p class="fine">Allies must travel, gather troops and finish construction. Delays can break a promise. Defenders must stay on station for two turns.</p>${treaties.map(t => `<div class="pledge-card"><strong>${escape(t.type)} · ${escape(kingdom(state, t.parties.find(p => p !== PLAYER)).name)}</strong><p>Expires on turn ${t.expires} (${t.expires - state.turn} left)</p></div>`).join('')}${pledges.length ? pledges.slice().reverse().map(p => `<div class="pledge-card"><strong>${escape(LABELS[p.intent.type])}</strong><p>${escape(kingdom(state, p.debtor).name)} → ${escape(kingdom(state, p.creditor).name)}</p><p>${escape(describeIntent(p.intent))}</p><p class="${p.status}">${escape(p.status.toUpperCase())} · Due turn ${p.deadline}${['DEFEND', 'PLEDGE_DEFEND'].includes(p.intent.type) ? ` · On station ${p.held}/2` : ''}</p>${p.status === 'pending' && p.debtor === PLAYER && p.intent.type === 'PROMISE' ? `<button class="full" data-deliver="${p.id}">Deliver promised resources</button>` : ''}</div>`).join('') : '<p class="empty">No military or payment pledges yet. Negotiate one with a ruler.</p>'}`;
}

$('panel').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b || b.disabled) return;
  const d = b.dataset;
  if(d.tradeDecline){const offer=state.commerce.offers.find(o=>o.id===Number(d.tradeDecline));if(offer)offer.status='declined';changed();}
  if(d.tradeReview||d.tradeCounter){const offer=state.commerce.offers.find(o=>o.id===Number(d.tradeReview||d.tradeCounter)&&o.expires>=state.turn&&o.status==='pending');if(offer){openDiplomacy(offer.from,false);reviewedTrade=offer.id;proposals=[offer.intent];storeOffers();save();renderProposals();if(d.tradeCounter)loadOffer(offer.intent);}}

  if(d.highwayFrom){const copy=structuredClone(state),preview=buildHighway(copy,PLAYER,d.highwayFrom,d.highwayTo);if(!preview.ok)toast(preview.error);else if(confirm(`Upgrade ${preview.tiles} road hexes to Royal Highway? Cost: ${costText(preview.cost)}. Construction takes up to 5 turns.`))result(buildHighway(state,PLAYER,d.highwayFrom,d.highwayTo));}
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
$('panel').addEventListener('change', e => { if(e.target.dataset.formation){result(setFormation(state,PLAYER,e.target.dataset.formation,e.target.value));return;} if (e.target.id === 'tax' && !state.outcome) { kingdom(state, PLAYER).tax = e.target.value; changed(); } });
document.querySelectorAll('[data-tab]').forEach(b => b.addEventListener('click', () => { tab = b.dataset.tab; orderMode = null; $('panel').scrollTop = 0; render(); }));
$('end-turn').addEventListener('click', () => { orderMode = null; endTurn(state); changed(); voiceNextDispatch(); });
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

function openDiplomacy(id, compact = false) {
  if (!alive(state, id)) return;
  reviewedTrade=null; activeRuler = id; proposals = state.diplomacy.offers?.[id] || []; state.conversations[id] ||= [];
  const incoming=state.commerce.offers.find(o=>o.from===id&&o.status==='pending'&&o.expires>=state.turn);if(incoming&&!proposals.length){proposals=[incoming.intent];reviewedTrade=incoming.id;}
  markRead(state, id); compactCouncil = compact;
  $('give-amount').value = '60'; $('receive-amount').value = '0'; $('offer-type').value = atWar(state, PLAYER, id) ? 'PEACE' : 'ALLIANCE';
  updateOfferFields(); setCouncilMode(compact); renderDiplomacy(); renderDispatches(); save();
  if (configReady) enableGemini();
}
function renderDiplomacy() {
  const k = kingdom(state, activeRuler), r = k.relations[PLAYER];
  $('diplomacy').style.setProperty('--house', k.color);
  $('ruler-mark').textContent = k.sigil; $('ruler-house').textContent = k.name; $('ruler-name').textContent = k.ruler; $('ruler-motto').textContent = `“${k.motto}”`;
  $('ruler-relation').textContent = `Opinion ${r.opinion} · Trust ${r.trust} · ${atWar(state, PLAYER, k.id) ? 'At war' : 'At peace'}`;
  $('messages').innerHTML = (state.conversations[activeRuler] || []).map(m => `<div class="message ${escape(m.role)}"><small>${m.role === 'player' ? 'YOU · HOUSE ASHEN' : m.role === 'council' ? 'COUNCIL RULING' : escape(k.ruler.toUpperCase())}</small>${escape(m.text)}</div>`).join('') || `<div class="message"><small>${escape(k.ruler.toUpperCase())}</small>You have my attention, Regent. What brings your envoy to my court?</div>`;
  $('messages').scrollTop = $('messages').scrollHeight;
  if ($('diplomacy').open) markRead(state, activeRuler);
  $('relations-summary').innerHTML = relationDescriptions(state, activeRuler).map(([label, text]) => `<div><b>${escape(label)}</b><span>${escape(text)}</span></div>`).join('');
  const economics = economicRelationship(state, activeRuler, PLAYER);
  $('council-records-body').innerHTML = ledgerPanel(activeRuler) + '<h3>Recent shipments</h3>' + (economics.recent.map(t => `<p class="fine">Turn ${t.turn} · ${escape(kingdom(state, t.from).name)} → ${escape(kingdom(state, t.to).name)} · ${t.amount} ${escape(t.resource)}</p>`).join('') || '<p class="fine">No recent resource shipments.</p>') + ambassadorPanel();
  renderProposals(); updateChatControls();
}
function updateChatControls() {
  const a = messageAllowance(state, activeRuler);
  $('message-allowance').textContent = a.hosted ? `Ambassador: ${a.remaining}/10 · shared dispatches: ${a.regularRemaining}/${diplomaticCapacity(state)}` : `Shared dispatches: ${a.remaining}/${a.limit} this turn`;
  $('send-chat').disabled = sending || !!state.outcome || a.remaining === 0;
  $('send-chat').textContent = sending ? 'Envoy travelling…' : 'Send envoy →';
  $('offer-form').querySelector('button[type="submit"]').disabled = sending || !!state.outcome || a.remaining === 0;
  updateDiagnostics();
}
function updateDiagnostics() {
  $('gemini-diagnostics').hidden = !geminiAttempted || !client.lastDiagnostic;
  if ($('gemini-diagnostics-dialog').open && client.lastDiagnostic) fillDiagnostics();
}
function fillDiagnostics() {
  const record = client.lastDiagnostic; if (!record) return;
  const info = diagnosticDetails(record);
  $('diagnostics-stage').textContent = info.stage;
  $('diagnostics-reason').textContent = info.reason;
  $('diagnostics-action').textContent = info.action;
  for (const name of CHECK_NAMES) {
    const cell = $(`diagnostics-${name}`);
    cell.textContent = CHECK_LABELS[record.checks[name]]; cell.dataset.state = record.checks[name];
  }
  $('diagnostics-report').value = diagnosticReport(record, client.endpoint, location.origin, { endpoint: !!client.endpoint, siteKey: !!config.turnstileSiteKey });
  $('diagnostics-copy-status').textContent = '';
}
$('gemini-diagnostics').onclick = () => {
  if (!client.lastDiagnostic) return;
  fillDiagnostics(); $('gemini-diagnostics-dialog').showModal();
};
$('copy-diagnostics').onclick = async () => {
  const report = $('diagnostics-report'); let copied = false;
  try { await navigator.clipboard.writeText(report.value); copied = true; }
  catch {
    report.focus(); report.select();
    try { copied = document.execCommand('copy'); } catch { /* Leave the report selected for manual copy. */ }
  }
  $('diagnostics-copy-status').textContent = copied ? 'Report copied.' : 'Copy unavailable. The report is selected; use your device’s Copy command.';
};
function renderProposals() {
  $('proposals').innerHTML = proposals.map((i, index) => {
    const v = evaluateDeal(state, activeRuler, i), promise = isPlayerPromise(i);
    return `<div class="proposal ${v.status}"><h4>${promise ? 'PROPOSED PROMISE' : escape(LABELS[i.type])} · ${v.status.toUpperCase()}</h4><p>${escape(describeIntent(i))}</p>${promise ? `<p><strong>Deadline: Turn ${state.turn + i.duration}</strong></p>` : ''}<p>${escape(v.reason)}</p>${v.status === 'accept' ? `<button data-ratify="${index}" class="${['WAR', 'BETRAY'].includes(i.type) ? 'danger' : 'primary'}">${promise ? 'Give My Word' : ['WAR', 'BETRAY'].includes(i.type) ? 'Declare war with these consequences' : 'Accept & Ratify'}</button>` : v.status === 'counter' ? `<div class="counter-terms"><strong>${escape(kingdom(state, activeRuler).name)} counteroffer</strong><p>${escape(describeIntent(v.counter))}</p><button class="primary" data-ratify-counter="${index}">Accept & Ratify</button><button data-counter="${index}">Review counteroffer</button></div>` : ''}<div class="button-row"><button data-modify="${index}">${promise ? 'Clarify' : 'Modify Offer'}</button><button data-reply="${index}">${promise ? 'I Make No Such Promise' : 'Reply'}</button></div></div>`;
  }).join('');
}
function storeOffers() { state.diplomacy.offers ||= {}; state.diplomacy.offers[activeRuler] = proposals.slice(0, 4); }
function loadOffer(i) {
  setCouncilMode(false); $('offer-type').value = i.type; updateOfferFields();
  $('give-resource').value = i.giveResource; $('give-amount').value = i.giveAmount;
  $('receive-resource').value = i.receiveResource; $('receive-amount').value = i.receiveAmount;
  $('trade-kind').value=i.tradeKind||'immediate';
  $('duration').value = i.duration; $('offer-target').value = i.targetId;
  if (i.conditionHouseId) $('condition-target').value = i.conditionHouseId;
  $('offer-form').scrollIntoView({ block: 'nearest' });
}
$('proposals').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b) return;
  if (b.dataset.counter !== undefined) {
    const index = Number(b.dataset.counter), v = evaluateDeal(state, activeRuler, proposals[index]);
    if (v.counter) proposals[index] = v.counter; storeOffers(); save(); renderProposals();
  }
  if (b.dataset.modify !== undefined) loadOffer(proposals[Number(b.dataset.modify)]);
  if (b.dataset.reply !== undefined) {
    const i = proposals[Number(b.dataset.reply)];
    if (isPlayerPromise(i)) { sendDiplomatic('I make no such promise. Let us clarify what you need.'); }
    else { $('chat-message').value = 'Let us discuss those terms. '; $('chat-message').focus(); }
  }
  if (b.dataset.ratify !== undefined || b.dataset.ratifyCounter !== undefined) {
    const index = Number(b.dataset.ratify ?? b.dataset.ratifyCounter);
    const proposal = b.dataset.ratifyCounter !== undefined ? evaluateDeal(state, activeRuler, proposals[index]).counter : proposals[index];
    const r = commitDeal(state, activeRuler, proposal);
    if (r.ok) { if(reviewedTrade){const offer=state.commerce.offers.find(o=>o.id===reviewedTrade);if(offer)offer.status='accepted';reviewedTrade=null;} appendMessage(activeRuler, 'council', `${describeIntent(proposal)} — ratified on turn ${state.turn}.`); toast('Your word is recorded. The ledger tracks what happens next.'); }
    result(r);
  }
});
function appendMessage(rulerId, role, text) { return appendConversation(state, rulerId, role, text); }
$('offer-type').innerHTML = Object.entries(LABELS).map(([id, label]) => `<option value="${id}">${label}</option>`).join('');
for (const id of ['give-resource', 'receive-resource']) $(id).innerHTML = RESOURCES.map(r => `<option>${r}</option>`).join('');
$('give-resource').value = 'gold';
function updateOfferFields() {
  const type = $('offer-type').value;
  $('trade-kind-label').hidden=!['EXCHANGE','RECURRING'].includes(type);
  $('receive-fields').hidden = !['EXCHANGE', 'TRIBUTE', 'RECURRING', 'LOAN'].includes(type);
  if ($('receive-fields').hidden) $('receive-amount').value = '0';
  if ((['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE'].includes(type) || (isPlayerPromise({type}) && type !== 'PROMISE'))) $('give-amount').value = '0';
  $('give-amount').disabled = (['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE'].includes(type) || (isPlayerPromise({type}) && type !== 'PROMISE'));
  $('target-label').hidden = !['JOINT_WAR', 'DEFEND', 'POSITION', 'BUILD_DEFENSES', 'TERRITORY', 'EMBARGO', 'GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_ATTACK', 'PLEDGE_DEFEND', 'PLEDGE_BUILD', 'PLEDGE_PEACE'].includes(type);
  let choices = [];
  if (['JOINT_WAR', 'EMBARGO', 'GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_PEACE'].includes(type)) choices = state.kingdoms.filter(k => ![PLAYER, activeRuler].includes(k.id) && alive(state, k.id)).map(k => [k.id, k.name]);
  if (type === 'DEFEND') choices = Object.values(state.tiles).filter(t => t.owner === PLAYER && ['city', 'town', 'fort'].includes(t.building)).map(t => [t.id, `${t.name || 'Fort'} (${t.id})`]);
  if (type === 'POSITION') choices = [[selected, `Selected map tile (${selected})`], ...settlements(state).filter(t => [PLAYER, activeRuler].includes(t.owner) && t.id !== selected).map(t => [t.id, `${t.name} (${t.id})`])];
  if (type === 'BUILD_DEFENSES') choices = Object.values(state.tiles).filter(t => t.owner === activeRuler && !t.building && Number.isFinite(TERRAINS[t.terrain].cost)).slice(0, 60).map(t => [t.id, `${TERRAINS[t.terrain].name} (${t.id})`]);
  if (type === 'TERRITORY') choices = Object.values(state.tiles).filter(t => t.owner === activeRuler && ['city', 'town', 'fort'].includes(t.building) && !t.capital).map(t => [t.id, `${t.name || 'Fort'} (${t.id})`]);
  if (type === 'PLEDGE_DEFEND') choices = Object.values(state.tiles).filter(t => t.owner === activeRuler && ['city', 'town', 'fort'].includes(t.building)).map(t => [t.id, `${t.name || 'Fort'} (${t.id})`]);
  if (type === 'PLEDGE_BUILD') choices = Object.values(state.tiles).filter(t => t.owner === PLAYER && !t.building && !t.project && Number.isFinite(TERRAINS[t.terrain].cost)).slice(0, 60).map(t => [t.id, `Fort at ${t.id}`]);
  if (type === 'PLEDGE_ATTACK') choices = [...state.armies.filter(a => ![PLAYER, activeRuler].includes(a.owner)).map(a => [a.id, `${kingdom(state, a.owner).name} army at ${a.tile}`]), ...settlements(state).filter(t => ![PLAYER, activeRuler].includes(t.owner)).map(t => [t.id, `${t.name} (${t.id})`])];
  $('condition-label').hidden = type !== 'PLEDGE_WAR';
  $('condition-target').innerHTML = '<option value="">Unconditional commitment</option>' + state.kingdoms.filter(k => ![PLAYER, activeRuler].includes(k.id) && alive(state, k.id)).map(k => `<option value="${k.id}">Only if ${escape(k.name)} attacks this House</option>`).join('');
  $('offer-target').innerHTML = choices.map(([id, label]) => `<option value="${escape(id)}">${escape(label)}</option>`).join('');
}
$('offer-type').onchange = updateOfferFields;
$('trade-kind').onchange=()=>{const kind=$('trade-kind').value;$('offer-type').value=['recurring','strategic','preferential'].includes(kind)?'RECURRING':'EXCHANGE';if(kind==='purchase')$('give-resource').value='gold';updateOfferFields();};
$('offer-form').addEventListener('submit', e => {
  e.preventDefault();
  const raw = { type: $('offer-type').value, giveResource: $('give-resource').value, giveAmount: Number($('give-amount').value), receiveResource: $('receive-resource').value, receiveAmount: Number($('receive-amount').value), targetId: $('target-label').hidden ? '' : $('offer-target').value, duration: Number($('duration').value) };
  if(!$('trade-kind-label').hidden) raw.tradeKind=$('trade-kind').value;
  if (!$('condition-label').hidden && $('condition-target').value) raw.conditionHouseId = $('condition-target').value;
  const i = validateIntent(raw);
  if (!i) { toast('Use whole resource amounts and valid terms (1–20 turns for a promise, 2–20 for agreements).'); return; }
  sendDiplomatic(describeIntent(i), i);
});
$('chat-form').addEventListener('submit', e => { e.preventDefault(); sendDiplomatic($('chat-message').value.trim()); });
async function sendDiplomatic(message, proposal = null) {
  if (!message || sending || state.outcome) return;
  const spent = consumeMessage(state, activeRuler); if (!spent.ok) { toast(spent.error); return; }
  const rulerId = activeRuler, requestEpoch = epoch, campaign = state;
  if (!proposal) applySpeech(campaign, rulerId, message);
  sending = true;
  geminiAttempted ||= $('use-gemini').checked || configurationFailure;
  const pending = client.send(campaign, rulerId, message, challengeToken, $('use-gemini').checked, { proposal });
  appendMessage(rulerId, 'player', message); $('chat-message').value = ''; save(); renderDiplomacy(); renderDispatches();
  try {
    const response = await pending;
    if (epoch !== requestEpoch || campaign !== state) {
      if (campaign === state) { appendMessage(rulerId, 'council', 'Circumstances changed while the envoy travelled. These terms require a fresh discussion.'); save(); }
      return;
    }
    appendMessage(rulerId, 'ruler', response.reply);
    if (response.source === 'gemini') acceptRulerMemories(state, rulerId, response);
    const candidates = [proposal, ...response.intents, response.proposal, response.counterProposal, response.promiseDetected].filter(Boolean);
    const unique = [...new Map(candidates.map(i => [JSON.stringify(i), i])).values()].slice(0, 4);
    state.diplomacy.offers ||= {}; state.diplomacy.offers[rulerId] = unique;
    if (rulerId === activeRuler) { proposals = unique; $('chat-notice').textContent = response.notice + (configurationFailure ? ' Open Diagnostics for connection details.' : ''); }
    if (rulerId !== activeRuler || !$('diplomacy').open) kingdom(state, rulerId).relations[PLAYER].unread = Math.min(99, kingdom(state, rulerId).relations[PLAYER].unread + 1);
    $('ai-status').textContent = response.source === 'gemini' ? 'Gemini council connected' : 'Local council ready';
    save();
  } finally {
    sending = false; challengeToken = '';
    if ($('diplomacy').open) renderDiplomacy(); renderDispatches();
    // Preserve the captured failure. Reopening the council retries verification;
    // a failed message must not immediately reset the widget and hide its cause.
  }
}
function loadVerification() {
  if (typeof globalThis.turnstile?.render === 'function') return Promise.resolve();
  if (verificationLoad) return verificationLoad;
  verificationLoad = new Promise((resolve, reject) => {
    document.getElementById('turnstile-script')?.remove();
    const script = document.createElement('script');
    script.id = 'turnstile-script'; script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit'; script.async = true;
    const timeout = setTimeout(() => { script.remove(); reject(new Error('Verification timed out')); }, 12000);
    script.onload = () => { clearTimeout(timeout); resolve(); };
    script.onerror = () => { clearTimeout(timeout); script.remove(); reject(new Error('Verification unavailable')); };
    document.head.append(script);
  }).finally(() => { verificationLoad = null; });
  return verificationLoad;
}
async function enableGemini() {
  const enabled = $('use-gemini').checked && !!client.endpoint && !!config.turnstileSiteKey;
  $('privacy').hidden = !enabled; $('turnstile').hidden = !enabled || client.hasSession();
  $('ai-status').textContent = enabled ? 'Gemini council enabled' : 'Scripted council ready';
  if (!enabled) {
    challengeToken = '';
    $('chat-notice').textContent = 'Local diplomacy is ready. Your terms still need ratification.';
    return;
  }
  // Verification starts when a visible council opens, not behind the welcome dialog.
  // No Gemini request is made until the player sends a message.
  if (!$('diplomacy').open) return;
  if (client.hasSession()) { $('turnstile').hidden = true; $('chat-notice').textContent = 'Gemini ready. Your diplomacy session is active.'; return; }
  $('chat-notice').textContent = client.now() < client.cooldownUntil ? 'Gemini is resting after a rate limit or connection error. Scripted diplomacy is available.' : challengeToken ? 'Gemini ready. Send your envoy to begin.' : 'Gemini is enabled. Preparing verification…';
  try {
    await loadVerification();
    if (!$('use-gemini').checked || !$('diplomacy').open) return;
    if (turnstileWidget === null) turnstileWidget = globalThis.turnstile.render($('turnstile'), {
      sitekey: config.turnstileSiteKey, action: 'iron-throne', theme: 'dark',
      callback: async token => {
        challengeToken = token;
        const ready = await client.openSession(token);
        challengeToken = '';
        $('turnstile').hidden = ready || !$('use-gemini').checked;
        $('ai-status').textContent = ready ? 'Gemini council connected' : 'Local council ready';
        $('chat-notice').textContent = ready ? 'Gemini ready. Your diplomacy session is active.' : `${diagnosticDetails(client.lastDiagnostic).reason} Local diplomacy is available; reopen the council to retry.`;
        updateDiagnostics();
      },
      'expired-callback': () => {
        challengeToken = '';
        if ($('use-gemini').checked && $('diplomacy').open && !client.hasSession()) {
          $('chat-notice').textContent = 'Verification expired. Preparing a new challenge…';
          globalThis.turnstile?.reset(turnstileWidget);
        }
      },
      'error-callback': () => {
        challengeToken = ''; client.recordFailure('TURNSTILE_WIDGET_FAILED', {}, '/verification');
        $('chat-notice').textContent = 'Verification unavailable. Local diplomacy remains available.'; updateDiagnostics();
      }
    });
    else if (!challengeToken) globalThis.turnstile.reset(turnstileWidget);
  } catch {
    if (!$('use-gemini').checked || !$('diplomacy').open) return;
    client.recordFailure('TURNSTILE_LOAD_FAILED', {}, '/verification'); updateDiagnostics();
    $('chat-notice').textContent = 'Verification could not load. Local diplomacy remains available; reopen the council to retry.';
    $('ai-status').textContent = 'Gemini verification unavailable';
  }
}
$('use-gemini').onchange = () => { geminiChoiceMade = true; enableGemini(); };
let configStatus;
fetch('./config.json', { cache: 'no-store' }).then(r => { configStatus = r.status; if (!r.ok) throw new Error('config'); return r.json(); }).catch(() => { configurationFailure = true; return {}; }).then(data => {
  config = data || {}; client = new DiplomacyClient({ endpoint: config.diplomacyEndpoint }); configReady = true;
  const available = !!client.endpoint && !!config.turnstileSiteKey;
  if (configurationFailure) client.recordFailure('CONFIG_LOAD_FAILED', { httpStatus: configStatus }, '/config.json');
  else if (!available && (config.diplomacyEndpoint || config.turnstileSiteKey)) { configurationFailure = true; client.recordFailure('CLIENT_CONFIG', {}, '/config.json'); }
  $('use-gemini').disabled = !available;
  if (!geminiChoiceMade) $('use-gemini').checked = available;
  enableGemini();
});
function renderDispatches() {
  $('dispatch-bar').innerHTML = state.kingdoms.filter(k => k.id !== PLAYER && alive(state, k.id)).map(k => {
    const r = k.relations[PLAYER], messages = state.conversations[k.id] || [], last = messages.filter(m => m.role === 'ruler').at(-1);
    const urgent = state.pledges.some(p => p.creditor === k.id && p.debtor === PLAYER && p.status === 'pending' && p.deadline - state.turn <= 2);
    return `<button class="dispatch-house ${urgent ? 'urgent' : ''}" data-dispatch="${k.id}" style="--house:${k.color}" aria-label="Open ${escape(k.name)} conversation${r.unread ? `, ${r.unread} unread` : ''}"><span class="dispatch-sigil">${k.sigil}</span><span><b>${escape(k.name.replace('House ', ''))}<i class="relation-dot ${r.trust < 0 ? 'distrust' : r.opinion > 25 ? 'friendly' : ''}"></i>${r.unread ? `<em>${r.unread}</em>` : ''}${urgent ? ' ⏳' : ''}${state.commerce.offers.some(o=>o.from===k.id&&o.status==='pending'&&o.expires>=state.turn)?' ⚖':''}</b><small>${escape(last?.text.slice(0, 64) || k.ruler)}</small></span></button>`;
  }).join('');
}
function setCouncilMode(compact) {
  compactCouncil = compact;
  const dialog = $('diplomacy'); if (dialog.open) dialog.close();
  dialog.classList.toggle('compact', compact); $('expand-council').textContent = compact ? 'Expand' : 'Compact';
  if (compact) dialog.show(); else dialog.showModal();
}
$('dispatch-bar').addEventListener('click', e => { const b = e.target.closest('[data-dispatch]'); if (b) openDiplomacy(b.dataset.dispatch, true); });
$('expand-council').onclick = () => { setCouncilMode(!compactCouncil); renderDiplomacy(); };
$('quick-offer').onclick = () => { setCouncilMode(false); $('offer-type').value = 'EXCHANGE'; updateOfferFields(); $('offer-form').scrollIntoView({ block: 'nearest' }); };
$('quick-request').onclick = () => { setCouncilMode(false); $('offer-type').value = 'DEFEND'; updateOfferFields(); $('offer-form').scrollIntoView({ block: 'nearest' }); };
$('quick-promises').onclick = () => { setCouncilMode(false); $('council-records').open = true; $('council-records').scrollIntoView({ block: 'start' }); };
$('diplomacy').addEventListener('close', () => { save(); renderDispatches(); });
$('council-records-body').addEventListener('click', e => { const b = e.target.closest('[data-deliver]'); if (b) result(deliverPledge(state, b.dataset.deliver)); });

function ambassadorCards(envoys) {
  return envoys.map(a => `<article class="envoy-card"><strong>⚜ ${escape(kingdom(state, a.owner).name)} ambassador</strong><p class="fine">${escape(a.status)} · ${escape(a.tile)}${a.host ? ` · Assigned to ${escape(kingdom(state, a.host).name)}` : ''}${a.status === 'detained' ? ` · Held by ${escape(kingdom(state, a.detainedBy)?.name)}` : ''}</p>${a.owner === PLAYER ? `<div class="button-row"><button data-envoy-goto="${a.tile}">Locate</button><button data-envoy-recall="${a.id}" ${a.status === 'detained' ? 'disabled' : ''}>Recall</button></div><label>Assign to a capital<select data-envoy-destination="${a.id}">${state.kingdoms.filter(k => k.id !== PLAYER && alive(state, k.id)).map(k => `<option value="${k.id}" ${a.host === k.id ? 'selected' : ''}>${escape(k.name)}</option>`).join('')}</select></label><button class="full" data-envoy-assign="${a.id}" ${a.status === 'detained' ? 'disabled' : ''}>Send ambassador</button>` : `<div class="button-row">${['passage', 'turn-away', 'expel', 'detain', 'release', 'execute'].map(action => `<button class="${action === 'execute' ? 'danger' : ''}" data-envoy-incident="${a.id}" data-incident="${action}">${{passage:'Allow passage','turn-away':'Turn away',expel:'Expel',detain:'Detain',release:'Release',execute:'Execute…'}[action]}</button>`).join('')}</div>`}</article>`).join('');
}
function ambassadorPanel() {
  const envoys = state.ambassadors.filter(a => a.status !== 'dead' && (a.owner === PLAYER || state.tiles[a.tile]?.owner === PLAYER));
  return `<div class="section-label">AMBASSADORS · ${state.ambassadors.filter(a => a.owner === PLAYER && a.status !== 'dead').length}/${ambassadorCapacity(state)}</div><p class="fine">${diplomaticCapacity(state)} shared dispatches per turn. A resident ambassador grants 10 separate messages with their host.</p><button class="full" data-envoy-recruit="true" ${!ambassadorCapacity(state) || state.outcome ? 'disabled' : ''}>Recruit ambassador · 35 gold, 15 wood, 1 order</button>${ambassadorCards(envoys)}`;
}
document.addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b || b.disabled) return;
  if (b.dataset.envoyRecruit) result(recruitAmbassador(state));
  if (b.dataset.envoyGoto) { if ($('diplomacy').open) $('diplomacy').close(); goTo(b.dataset.envoyGoto); }
  if (b.dataset.envoyRecall) result(assignAmbassador(state, PLAYER, b.dataset.envoyRecall));
  if (b.dataset.envoyAssign) result(assignAmbassador(state, PLAYER, b.dataset.envoyAssign, b.closest('.envoy-card').querySelector('select').value));
  if (b.dataset.envoyIncident) {
    const action = b.dataset.incident;
    const confirmed = action !== 'execute' || confirm('Execute this ambassador? Their House will declare war, cancel its treaties and suffer enormous losses of trust. Every other House will remember this violation of diplomatic immunity.');
    if (confirmed) result(ambassadorIncident(state, PLAYER, b.dataset.envoyIncident, action, action === 'execute'));
  }
});
$('reduced-effects').checked = !!state.presentation?.reducedEffects;
$('reduced-effects').onchange = () => { state.presentation = { reducedEffects: $('reduced-effects').checked }; map.reducedEffects = state.presentation.reducedEffects; save(); map.draw(); };

async function voiceNextDispatch() {
  if (!$('use-gemini').checked || !client.hasSession() || client.now() < client.cooldownUntil || state.diplomacy.voicedTurn === state.turn) return;
  const chosen = state.kingdoms.slice(1).map(k => ({ id: k.id, entry: state.conversations[k.id]?.findLast(m => m.role === 'ruler' && m.kind && m.turn >= state.turn - 1 && !m.voiced) })).find(c => c.entry);
  if (!chosen) return;
  // One optional incoming voice request per turn. The local dispatch already
  // exists, so rendering and turn resolution never wait on the model.
  const campaign = state, requestEpoch = epoch;
  state.diplomacy.voicedTurn = state.turn; chosen.entry.voiced = true;
  const voice = new DiplomacyClient({ endpoint: client.endpoint }); voice.session = client.session;
  const response = await voice.send(campaign, chosen.id, 'Deliver the supplied diplomatic dispatch in your own voice.', '', true, { event: chosen.entry.text });
  client.cooldownUntil = Math.max(client.cooldownUntil, voice.cooldownUntil);
  if (voice.hasSession()) client.session = voice.session;
  if (campaign !== state || epoch !== requestEpoch || response.source !== 'gemini') return;
  chosen.entry.fact = chosen.entry.text; chosen.entry.text = response.reply;
  save(); renderDispatches(); if ($('diplomacy').open && activeRuler === chosen.id) renderDiplomacy();
}
let lastSessionActivity = 0;
document.addEventListener('pointerdown', () => {
  if ($('use-gemini').checked && client.hasSession() && Date.now() - lastSessionActivity > 60000) {
    lastSessionActivity = Date.now(); client.openSession();
  }
}, { passive: true });

$('resume').hidden = !restored;
if (restored) $('start-game').textContent = 'Replace save & begin a new reign';
render(); $('welcome').showModal();
