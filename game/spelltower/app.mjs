import { COLS, ROWS, TOWER_ROW, SPELLS, RECIPES, INTERACTIONS, ENEMIES, createGame, applyAction, previewSpell, getActivePlayer, getRecipe } from './engine.mjs';
import { icon, drawEnemy, drawTower } from './art.mjs';
import { SpellEffects } from './effects.mjs';
import { saveRun, loadRun, makeLocalNames, INACTIVE_TURN_MS } from './session.mjs';

const $ = id => document.getElementById(id);
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const params = new URLSearchParams(location.search);
const storage = { getItem(key) { try { return localStorage.getItem(key); } catch { return null; } }, setItem(key, value) { localStorage.setItem(key, value); }, removeItem(key) { localStorage.removeItem(key); } };
const onlineIdentity = (params.get('username') || storage.getItem('gh.username') || '').trim();
const explicitLocal = ['solo', 'hotseat'].includes(params.get('mode')) || params.get('solo') === '1';
const roomId = explicitLocal ? '' : (params.get('gameId') || params.get('lobbyId') || '');
const isPack = location.pathname.includes('/packs/') || location.hostname === '127.0.0.1' || location.hostname === 'localhost' && params.get('offline') === '1';
const lobbyURL = new URL('/lobby/lobby.html', isPack ? 'https://catnmice.com' : location.origin);
if (onlineIdentity && !isPack) lobbyURL.searchParams.set('username', onlineIdentity);
$('lobby-link').href = lobbyURL.href;

let game = null, menuMode = params.get('mode') === 'hotseat' ? 'hotseat' : 'solo', selectedId = null;
let fuseMode = false, fuseFirst = null, busy = false, handoffPending = false, lastPoint = null;
let session = null, roomMeta = {}, connection = '', lastRevision = -1, shownResult = false, recentLog = [];
let saveWarningShown = false, toastTimer, bannerTimer, bootGeneration = 0, connecting = false;
const effects = new SpellEffects($('effects'), $('board'));
const terrainLabels = { wet: 'Puddle', vines: 'Vines', burn: 'Flames', ice: 'Ice', mud: 'Mud', stone: 'Barricade', steam: 'Steam', charged: 'Charged', poison: 'Spores' };
const statusLabels = { wet: 'Wet', stun: 'Stunned', frozen: 'Frozen', rooted: 'Rooted', burn: 'Burning', poison: 'Poisoned', slow: 'Slowed', blinded: 'Blinded', vulnerable: 'Exposed' };
const patternLabels = { cell: 'One tile', cross: 'Cross', chain: 'Connected targets', area: '3 × 3 area', column: 'Whole lane', row: 'Whole row', twoColumns: 'Two lanes', largeArea: 'Wide area' };

for (const element of document.querySelectorAll('[data-icon]')) element.innerHTML = icon(element.dataset.icon);
for (const id of ['menu-tower', 'brand-icon', 'tower-art']) $(id).innerHTML = drawTower();
$('selected-art').innerHTML = icon('water');

function toast(message) {
  $('toast').textContent = message; $('toast').classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => $('toast').classList.remove('show'), 3500);
}
function closeDialogs() { for (const dialog of document.querySelectorAll('dialog[open]')) dialog.close(); }
function showDialog(id) { if (!$(id).open) $(id).showModal(); }
function actor() { return game?.mode === 'online' ? onlineIdentity : game ? getActivePlayer(game) : ''; }
function isTurn() { return !!game && game.status === 'playing' && getActivePlayer(game) === actor() && !handoffPending && !busy; }
function hand() { return game?.hands[actor()] || []; }
function selected() { return hand().find(card => card.id === selectedId); }
function canCast(card) { return isTurn() && card && card.usedTurn !== game.turn && card.charges > 0 && SPELLS[card.key].cost <= game.mana; }
function cardArt(key) {
  const spell = SPELLS[key], parts = spell.parts;
  return `<span class="card-art">${icon(parts[0])}${parts.length > 1 ? `<span class="fusion-secondary">${icon(parts[1])}</span>` : spell.tier ? '<span class="fusion-secondary upgrade-star" aria-hidden="true">✦</span>' : ''}</span>`;
}

function chooseMode(mode) {
  if (connecting && mode !== 'online') { bootGeneration++; connecting = false; $('start-btn').disabled = false; $('menu-message').textContent = ''; }
  menuMode = mode;
  for (const button of document.querySelectorAll('[data-mode]')) { const active = button.dataset.mode === mode; button.classList.toggle('selected', active); button.setAttribute('aria-pressed', String(active)); }
  $('local-options').hidden = mode === 'online'; $('seats-label').hidden = mode !== 'hotseat'; $('seat-names').hidden = mode !== 'hotseat';
  $('mode-description').textContent = mode === 'solo' ? 'Play at your own pace. Your run saves after every move.' : mode === 'hotseat' ? 'Cooperate on one screen. Each mage has a hand; creatures move after everyone takes a turn.' : isPack ? 'Return to the launcher and choose Online on Spell Tower to create or join a party.' : 'Create or join a Spell Tower room in the arcade. Everyone uses their own screen and shares one tower.';
  $('start-btn').innerHTML = mode === 'online' ? 'Open multiplayer lobby <span aria-hidden="true">→</span>' : 'Enter the glade <span aria-hidden="true">→</span>';
  const saved = loadRun(storage); $('resume-btn').hidden = !saved || mode === 'online' || saved.mode !== mode;
  if (saved && saved.mode === mode) $('resume-btn').textContent = `Continue · wave ${saved.wave}`;
  if (mode === 'hotseat') renderSeatNames();
}
function renderSeatNames() {
  const previous = [...$('seat-names').querySelectorAll('input')].map(el => el.value);
  $('seat-names').innerHTML = Array.from({ length: Number($('seats').value) }, (_, i) => `<label>Mage ${i + 1}<input aria-label="Mage ${i + 1} name" maxlength="26" value="${escape(previous[i] || `Mage ${i + 1}`)}" autocomplete="off"></label>`).join('');
}
function localPlayers() { return menuMode === 'hotseat' ? makeLocalNames(Number($('seats').value), [...$('seat-names').querySelectorAll('input')].map(el => el.value)) : makeLocalNames(1, [onlineIdentity || 'You']); }
function prepareRun() {
  selectedId = null; fuseFirst = null; fuseMode = false; handoffPending = false; lastPoint = null;
  lastRevision = -1; recentLog = []; shownResult = false; effects.clear(); closeDialogs();
  $('menu').hidden = true; $('game').hidden = false; $('handoff').hidden = true;
  document.documentElement.dataset.catnmicePlaying = 'true';
  window.dispatchEvent(new Event('catnmice-game-started'));
  requestAnimationFrame(() => effects.resize());
}
function startLocal(saved = null) {
  bootGeneration++; session?.close(); session = null; connection = ''; roomMeta = {};
  const next = saved || createGame({ players: localPlayers(), mode: menuMode, difficulty: $('difficulty').value, seed: crypto.getRandomValues(new Uint32Array(1))[0] });
  prepareRun(); acceptState(next, {}, true);
  const url = new URL(location.href); url.searchParams.delete('gameId'); url.searchParams.delete('lobbyId'); url.searchParams.set('mode', next.mode); history.replaceState({}, '', url);
}

async function startOnline() {
  const generation = ++bootGeneration;
  chooseMode('online'); connecting = true; $('menu-message').textContent = 'Opening your Spell Tower room…'; $('start-btn').disabled = true;
  try {
    if (!onlineIdentity) throw new Error('Open the game from your signed-in arcade lobby.');
    const { openOnlineRoom } = await import('./network.mjs');
    const opened = await openOnlineRoom({ gameId: roomId, username: onlineIdentity,
      onState(next, meta) { if (generation !== bootGeneration) return; if ($('game').hidden) prepareRun(); acceptState(next, meta); },
      onConnection(message) { if (generation !== bootGeneration) return; connection = message; $('connection-label').textContent = message; },
      onError(error) { if (generation === bootGeneration) { toast(error.message || 'The room connection was interrupted.'); connection = 'Connection interrupted'; $('connection-label').textContent = connection; } }
    });
    if (generation !== bootGeneration) { opened.close(); return; }
    session = opened; renderControls();
  } catch (error) { if (generation === bootGeneration) { $('menu-message').textContent = error.message || 'The room could not connect. Open it again from the arcade.'; toast($('menu-message').textContent); } }
  finally { if (generation === bootGeneration) { connecting = false; $('start-btn').disabled = false; } }
}

function persist() {
  if (game?.mode === 'online') return;
  if (!saveRun(storage, game) && !saveWarningShown) { saveWarningShown = true; toast('This browser could not save your run. Keep this tab open to continue playing.'); }
}
function acceptState(next, meta = {}, initial = false) {
  const old = game, changed = initial || next.revision !== lastRevision;
  const restarted = old && ((roomMeta.startedAt && meta.startedAt && roomMeta.startedAt !== meta.startedAt) || old.status !== 'playing' && next.status === 'playing');
  if (restarted) {
    closeDialogs(); shownResult = false; recentLog = []; selectedId = null; fuseFirst = null; fuseMode = false;
    handoffPending = false; $('handoff').hidden = true; effects.clear(); lastPoint = null;
  }
  game = next; roomMeta = meta;
  if (old && old.turn !== next.turn) { selectedId = null; fuseMode = false; fuseFirst = null; lastPoint = null; }
  if (!hand().some(card => card.id === selectedId)) selectedId = null;
  if (!hand().some(card => card.id === fuseFirst)) fuseFirst = null;
  if (changed && !initial && old && old.turn !== next.turn && next.mode === 'hotseat' && next.status === 'playing') {
    handoffPending = true; $('handoff-name').textContent = `${getActivePlayer(next)}'s turn`; $('handoff').hidden = false;
    requestAnimationFrame(() => $('ready-btn').focus());
  }
  if (changed) {
    for (const event of next.events || []) if (['combo', 'wave', 'turn', 'combine', 'gift', 'tower', 'victory', 'defeat', 'join', 'leave'].includes(event.type)) recentLog.unshift(event);
    recentLog = recentLog.slice(0, 6);
  }
  render();
  if (changed && !initial) {
    effects.play(next.events);
    const reactions = [...new Set((next.events || []).filter(e => e.type === 'combo').map(e => e.text))];
    const wave = (next.events || []).find(e => e.type === 'wave');
    if (reactions.length || wave) {
      $('combo-banner').textContent = reactions.length ? `${reactions[0]}${reactions.length > 1 ? ` · +${reactions.length - 1} reactions` : ''}` : wave.text;
      $('combo-banner').classList.add('show'); clearTimeout(bannerTimer); bannerTimer = setTimeout(() => $('combo-banner').classList.remove('show'), 1800);
    }
  }
  lastRevision = next.revision; persist();
  if (next.status !== 'playing' && !shownResult) { shownResult = true; showResult(); }
}

function terrainArt(tile) {
  let paths = '';
  if (tile.wet > 0) paths += '<path d="M9 33Q14 22 30 27T55 32Q57 45 35 46T9 33" fill="#58c5df" fill-opacity=".55" stroke="#b8eff1" stroke-width="1.2"/><path d="M17 33q8-4 15 0m2 6q9 3 15-1" fill="none" stroke="#d7ffff" stroke-width="1.5"/>';
  if (tile.mud > 0) paths += '<path d="M8 38Q12 27 25 33T55 37Q58 50 28 49T8 38" fill="#735c40" stroke="#c0a278" stroke-width="1.5"/><path d="M18 37h9m12 6h8" stroke="#d3b786"/>';
  if (tile.vines > 0) paths += '<path d="M5 55Q16 38 9 21T28 4M58 56Q45 37 55 20T39 3M9 49Q26 55 39 49T58 48" fill="none" stroke="#294f2b" stroke-width="5"/><path d="M5 55Q16 38 9 21T28 4M58 56Q45 37 55 20T39 3" fill="none" stroke="#8bd36c" stroke-width="2.5"/><path d="M9 35q-10-10-4-15 11 2 10 11M52 35q14-9 7-15-9 2-10 12" fill="#a2dc77" stroke="#355830"/>';
  if (tile.ice > 0) paths += '<path d="M4 39 18 23 25 39 36 18 48 35 59 28 55 53 13 54Z" fill="#b9edfa" fill-opacity=".5" stroke="#e0ffff" stroke-width="1.5"/><path d="m18 23 3 22m15-27 2 31m10-14-8 10" stroke="#f6ffff" stroke-width="1"/>';
  if (tile.stone > 0) paths += '<path d="M4 46 7 34 23 31 34 37 37 49ZM29 44 35 28 52 25 60 36 57 49Z" fill="#829081" stroke="#354e43" stroke-width="2"/><path d="m8 34 12 4 3-7m12-3 10 10 15-2m-15 2-2 10" fill="none" stroke="#c4caaa" stroke-width="2"/>';
  if (tile.burn > 0) paths += '<path d="M6 55Q0 40 13 32q-4 12 4 11 4-13 10-19-2 18 7 19 3-15 14-18-1 12 6 17 8 10 1 16Z" fill="#f1743b" stroke="#93402e" stroke-width="1.5"/><path d="M13 55q-1-10 7-13 1 9 7 9 0-12 9-15-2 13 6 15 3-9 8-8l-1 12Z" fill="#ffd083"/>';
  if (tile.steam > 0) paths += '<path d="M15 48q-10-8 2-15t-1-14m16 29q-10-9 0-17t0-16m16 31q-8-9 1-16t-1-13" fill="none" stroke="#e8f9ef" stroke-opacity=".55" stroke-width="5" stroke-linecap="round"/>';
  if (tile.poison > 0) paths += '<g fill="#c6e879" stroke="#5d8449"><circle cx="12" cy="40" r="4"/><circle cx="43" cy="31" r="5"/><circle cx="51" cy="46" r="3"/><circle cx="22" cy="23" r="3"/></g>';
  if (tile.charged > 0) paths += '<path d="m8 19 9 4-5 10 11-2m18-7 11 2-9 12 12-3" fill="none" stroke="#ffec8e" stroke-width="2"/>';
  return paths ? `<span class="terrain-art" aria-hidden="true"><svg viewBox="0 0 64 64" preserveAspectRatio="none">${paths}</svg></span>` : '';
}

function buildBoard() {
  for (let y = 0; y < ROWS; y++) for (let x = 0; x < COLS; x++) {
    const cell = document.createElement('button'); cell.type = 'button'; cell.className = 'cell'; cell.dataset.x = x; cell.dataset.y = y;
    if (y === TOWER_ROW) { cell.classList.add('tower-cell'); cell.disabled = true; cell.setAttribute('aria-label', 'The tower'); }
    else {
      cell.addEventListener('pointerenter', event => { if (event.pointerType !== 'touch') inspectCell(x, y); });
      cell.addEventListener('focus', () => inspectCell(x, y));
      cell.addEventListener('click', () => clickCell(x, y));
    }
    $('board').appendChild(cell);
  }
}
function renderBoard() {
  for (const cell of $('board').children) {
    const x = Number(cell.dataset.x), y = Number(cell.dataset.y), tile = game.terrain[`${x},${y}`] || {};
    const enemies = game.enemies.filter(e => e.x === x && e.y === y);
    cell.className = `cell${y === TOWER_ROW ? ' tower-cell' : ''}`;
    for (const key of Object.keys(terrainLabels)) if (tile[key] > 0) cell.classList.add(key);
    const enemy = enemies[0];
    cell.innerHTML = terrainArt(tile) + (enemy ? `<span class="cell-content"><span class="enemy-art">${drawEnemy(enemy.key === 'baby' ? 'slime_baby' : enemy.key)}</span></span><span class="enemy-hp-number">${enemy.hp}</span><span class="enemy-health"><i style="width:${Math.max(0, Math.min(100, enemy.hp / enemy.maxHP * 100))}%"></i></span>${enemies.length > 1 ? `<span class="enemy-count">×${enemies.length}</span>` : ''}<span class="enemy-status">${Object.entries(statusLabels).filter(([key]) => enemy[key] > 0).slice(0, 2).map(([key, label]) => `<span title="${label}: ${enemy[key]} rounds">${label}</span>`).join('')}</span>` : '');
    const details = Object.entries(terrainLabels).filter(([key]) => tile[key] > 0).map(([key, label]) => `${label} ${tile[key]} rounds`).join(', ');
    const description = `Lane ${x + 1}, row ${y + 1}${enemy ? `; ${ENEMIES[enemy.key]?.name || 'Creature'}, ${enemy.hp} health` : '; empty'}${details ? `; ${details}` : ''}`;
    if (y !== TOWER_ROW) cell.setAttribute('aria-label', description);
    cell.title = y === TOWER_ROW ? 'Defend the tower' : description;
  }
}
function renderHand() {
  const cards = hand(), focusId = document.activeElement?.dataset?.cardId, selectedCard = selected(), partner = cards.find(c => c.id === fuseFirst);
  $('hand').hidden = handoffPending;
  $('hand').innerHTML = cards.map((card, i) => {
    const spell = SPELLS[card.key], fusable = fuseMode && partner && card.id !== partner.id && getRecipe(partner.key, card.key);
    const spent = card.usedTurn === game.turn;
    return `<button class="spell-card${card.id === selectedId ? ' selected' : ''}${fusable ? ' fusable' : ''}${spell.tier ? ' fused' : ''}${spent ? ' spent' : ''}" data-card-id="${escape(card.id)}" aria-pressed="${card.id === selectedId}" title="${escape(`${spell.name} · ${spell.cost} mana · ${card.charges} charges. ${spell.description}${spent ? ' Already cast this turn.' : ''}`)}"><span class="card-cost" title="Mana cost">${spell.cost}</span>${cardArt(card.key)}<strong>${escape(spell.name)}</strong><span class="card-footer"><span class="charges" aria-label="${card.charges} charges">${Array.from({ length: 3 }, (_, n) => `<i class="${n >= card.charges ? 'empty' : ''}"></i>`).join('')}</span><span class="key-hint">${spent ? 'Used' : i < 9 ? i + 1 : ''}</span></span></button>`;
  }).join('');
  for (const button of $('hand').children) button.onclick = () => chooseCard(button.dataset.cardId);
  if (focusId) $('hand').querySelector(`[data-card-id="${CSS.escape(focusId)}"]`)?.focus({ preventScroll: true });
  $('selected-art').innerHTML = selectedCard ? cardArt(selectedCard.key) : icon('water');
  $('selected-name').textContent = selectedCard ? SPELLS[selectedCard.key].name : 'Make a little magic';
  $('selected-description').textContent = selectedCard ? SPELLS[selectedCard.key].description : 'Choose a spell below, then choose a tile. Combine terrain to make each cast count.';
  $('selected-tags').innerHTML = selectedCard ? [patternLabels[SPELLS[selectedCard.key].pattern], `${SPELLS[selectedCard.key].cost} mana`, `${selectedCard.charges} charges`, 'Once per turn'].map(label => `<span>${label}</span>`).join('') : '';
}
function renderControls() {
  if (!game) return;
  $('endTurnBtn').disabled = !isTurn() || (game.mode === 'online' && !session);
  $('endTurnBtn').innerHTML = game.mode === 'solo' ? 'End turn <span aria-hidden="true">→</span>' : 'Pass turn <span aria-hidden="true">→</span>';
  $('fuse-btn').disabled = !isTurn(); $('fuse-btn').classList.toggle('active', fuseMode); $('fuse-btn').textContent = fuseMode ? 'Cancel fusion' : 'Fuse spells';
  $('gift-btn').hidden = game.players.length < 2; $('gift-btn').disabled = !isTurn() || !selected();
  $('turn-name').textContent = handoffPending ? 'Pass the screen' : `${getActivePlayer(game)}${isTurn() ? "'s turn" : ' is casting'}`;
  $('turn-badge').textContent = isTurn() ? 'YOUR TURN' : 'WAITING'; $('turn-badge').classList.toggle('waiting', !isTurn());
  $('connection-label').textContent = connection;
  const card = selected(); let hint = 'Choose a spell, then tap the battlefield. F fuses · E ends turn.';
  if (handoffPending) hint = 'Pass the screen, then press “I’m ready”.';
  else if (busy) hint = 'Resolving your magic…';
  else if (!isTurn()) hint = `${getActivePlayer(game)} is casting. Inspect your cards while you wait.`;
  else if (fuseMode) hint = fuseFirst ? 'Choose a highlighted partner. Two cards become one advanced spell; fusion costs no mana.' : 'Choose the first basic spell to fuse.';
  else if (card) hint = card.usedTurn === game.turn ? `${SPELLS[card.key].name} was used this turn. Choose another card, or end your turn.` : game.mana < SPELLS[card.key].cost ? 'Not enough mana. End your turn to refill.' : `${SPELLS[card.key].name} · ${SPELLS[card.key].cost} mana · ${SPELLS[card.key].description}`;
  $('target-hint').textContent = hint; $('target-hint').title = hint; $('target-hint').classList.remove('combo');
}
function render() {
  $('mode-label').textContent = game.mode === 'solo' ? `Solo defense${saveWarningShown ? '' : ' · autosaved'}` : game.mode === 'hotseat' ? `${game.players.length} mages · hot-seat co-op` : `${game.players.length} / 8 mages · online co-op`;
  $('hp-text').textContent = `${game.hp} / ${game.maxHP}`; $('hp-fill').style.width = `${Math.max(0, game.hp / game.maxHP * 100)}%`;
  $('mana-text').textContent = `${game.mana} / ${game.maxMana}`; $('mana-fill').style.width = `${Math.max(0, game.mana / game.maxMana * 100)}%`;
  $('wave-text').innerHTML = `${game.wave} <span>/ 10</span>`;
  $('round-label').textContent = `${[5, 10].includes(game.wave) ? 'Boss wave' : 'The enchanted glade'} · Round ${game.round} · ${game.enemies.length} approaching`;
  renderBoard(); renderHand(); renderControls();
  $('battle-log').innerHTML = recentLog.map(event => `<li class="${event.type === 'combo' ? 'combo' : ''}">${escape(event.text)}</li>`).join('');
  if (lastPoint) inspectCell(lastPoint.x, lastPoint.y);
}

function inspectCell(x, y) {
  if (!game) return;
  lastPoint = { x, y };
  for (const cell of $('board').children) cell.classList.remove('targeted', 'combo-target');
  const card = selected(), enemies = game.enemies.filter(e => e.x === x && e.y === y), tile = game.terrain[`${x},${y}`] || {};
  $('field-title').textContent = enemies.length ? ENEMIES[enemies[0].key].name : `Lane ${x + 1} · tile ${y + 1}`;
  $('field-description').textContent = enemies.length ? `${enemies[0].hp} / ${enemies[0].maxHP} health · ${ENEMIES[enemies[0].key].speed} movement · ${ENEMIES[enemies[0].key].damage} tower damage` : 'Shape this ground before the next creatures arrive.';
  const lines = [];
  for (const enemy of enemies) {
    const spec = ENEMIES[enemy.key], statuses = Object.entries(statusLabels).filter(([key]) => enemy[key] > 0).map(([key, label]) => `${label} ${enemy[key]}`).join(' · ');
    lines.push(`<div class="field-detail"><strong>${escape(spec.name)}</strong>Weak: ${spec.weak.join(', ') || 'none'}<br>Resists: ${spec.resist.join(', ') || 'none'}${statuses ? `<br>${statuses}` : ''}${spec.immovable ? '<br>Too heavy to push.' : ''}${spec.rootImmune ? '<br>Walks through roots.' : ''}</div>`);
  }
  for (const [key, label] of Object.entries(terrainLabels)) if (tile[key] > 0) lines.push(`<div class="field-detail"><strong>${label} · ${tile[key]} round${tile[key] === 1 ? '' : 's'}</strong>${key === 'stone' ? `${tile.strength || 0} barricade health` : key === 'burn' ? `${tile.burnPower || 2} fire damage per round` : 'Mix another element here to trigger a reaction.'}</div>`);
  $('field-details').innerHTML = lines.join('');
  if (card && !fuseMode && canCast(card)) {
    const preview = previewSpell(game, card.key, x, y), combo = preview.combos.length > 0;
    for (const p of preview.cells) { const cell = $('board').children[p.y * COLS + p.x]; cell?.classList.add('targeted'); if (combo) cell?.classList.add('combo-target'); }
    const hint = preview.combos.length ? `${preview.combos.join(' · ')} — tap to cast ${SPELLS[card.key].name}` : preview.text;
    $('target-hint').textContent = hint; $('target-hint').title = hint; $('target-hint').classList.toggle('combo', combo);
  }
}
async function clickCell(x, y) {
  inspectCell(x, y);
  const card = selected();
  if (!card || fuseMode) {
    if (matchMedia('(max-width: 1050px)').matches) toast(`${$('field-title').textContent}. ${$('field-description').textContent}`);
    return;
  }
  if (!canCast(card)) { toast(card.usedTurn === game.turn ? 'That card has already been cast this turn.' : !isTurn() ? 'Wait for your turn.' : 'Not enough mana. End your turn to refill.'); return; }
  await dispatch({ type: 'cast', cardId: card.id, x, y });
}
async function chooseCard(cardId) {
  if (handoffPending || busy) return;
  const card = hand().find(c => c.id === cardId); if (!card) return;
  if (fuseMode && isTurn()) {
    if (!fuseFirst) { if (SPELLS[card.key].tier) { toast('Choose a basic element; advanced spells are already complete.'); return; } fuseFirst = cardId; selectedId = cardId; }
    else if (fuseFirst === cardId) { fuseFirst = null; selectedId = null; }
    else {
      const first = hand().find(c => c.id === fuseFirst), recipe = first && getRecipe(first.key, card.key);
      if (!recipe) { toast('Choose a highlighted basic element to complete the fusion.'); return; }
      const ok = await dispatch({ type: 'combine', cardIds: [fuseFirst, cardId] });
      if (ok) { fuseFirst = null; fuseMode = false; selectedId = hand().findLast(c => c.key === recipe)?.id || null; toast(`${SPELLS[recipe].name} is ready. Select a tile to cast it.`); }
    }
  } else selectedId = selectedId === cardId ? null : cardId;
  renderHand(); renderControls(); if (lastPoint) inspectCell(lastPoint.x, lastPoint.y);
}
async function dispatch(action) {
  if (busy || !game) return false;
  busy = true; renderControls(); const revision = game.revision;
  try {
    const next = game.mode === 'online' ? await session.dispatch(action, revision) : applyAction(game, actor(), action);
    // Online snapshots and the transaction result can arrive in either order. Render each revision once.
    if (next.revision >= game.revision) acceptState(next, roomMeta);
    return true;
  } catch (error) { toast(error.message || 'That spell could not be cast. Try again.'); return false; }
  finally { busy = false; renderControls(); if (lastPoint) inspectCell(lastPoint.x, lastPoint.y); }
}

function showGuide(tab = 'basics') {
  for (const button of document.querySelectorAll('[data-tab]')) button.classList.toggle('active', button.dataset.tab === tab);
  if (tab === 'basics') $('guide-content').innerHTML = `<h3>Defend one tower, together.</h3><ol><li><strong>Choose a spell, then choose a path tile.</strong> The highlighted tiles show its reach. A blue glow and reaction name reveal a combination.</li><li><strong>Set up, then follow through.</strong> Rainfall leaves puddles. Spark conducts through them for extra damage and stun. Vines make Firebolt burn harder. Terrain lasts several rounds.</li><li><strong>Spend your mana thoughtfully.</strong> Basic spells cost 1 mana; fused spells cost 2. Each card has 3 charges and can be cast once per turn. Mana refills for the next caster.</li><li><strong>Fuse two basic cards.</strong> Press Fuse spells, then choose two cards. Both are replaced with one stronger spell; mixing costs no mana. Every pair of basic elements has a recipe.</li><li><strong>End your turn when ready.</strong> In co-op, each mage acts before creatures advance. Your hand replenishes as the adventure continues. Keep all four lanes covered.</li><li><strong>Survive ten waves.</strong> Clear each wave to recover a little tower health. The Thorncrown Ogre arrives on wave 5; the Ashwing awaits on wave 10.</li></ol><h3>Ways to play</h3><p><strong>Solo:</strong> a complete offline game with automatic saving. <strong>Hot seat:</strong> 2–4 players pass one screen and cooperate. <strong>Online:</strong> up to 8 friends, separate hands, one shared tower. The host can pass a stalled player's turn after 90 seconds without a move.</p><p><strong>Keyboard:</strong> 1–9 selects a card · F toggles fusion · E ends your turn · Escape cancels selection. Tab and Enter work on every card and tile. Tap works everywhere.</p>`;
  else if (tab === 'interactions') $('guide-content').innerHTML = `<p>Order matters. Prepare the ground with one spell, then trigger it with another. These reactions use the same rules in every mode.</p><div class="guide-grid">${INTERACTIONS.map(item => `<article class="guide-entry"><h3>${escape(item.name)}</h3><p><strong>${escape(item.when)}</strong></p><p>${escape(item.description)}</p></article>`).join('')}</div>`;
  else $('guide-content').innerHTML = `<p>Two basic cards become one advanced card with three charges. Fusions perform their elements in a useful order. Double-element upgrades need two separate cards of the same element.</p><div class="guide-grid">${RECIPES.map(recipe => `<article class="guide-entry"><div class="recipe-art"><span>${icon(recipe.a)}</span><b>+</b><span>${icon(recipe.b)}</span><b>→ 2 mana</b></div><h3>${escape(recipe.name)}</h3><p>${escape(SPELLS[recipe.key].description)}</p></article>`).join('')}</div>`;
  showDialog('guide-dialog');
}
function openPause() {
  const online = game?.mode === 'online', host = roomMeta.host === onlineIdentity;
  $('pause-copy').textContent = online ? 'Your online party can keep playing while this menu is open. Leave removes your seat and passes hosting to another mage.' : saveWarningShown ? 'Browser saving is unavailable. Keep this tab open to preserve the current run.' : 'Your run saves after every move. Nothing advances until you end your turn.';
  $('leave-btn').textContent = online ? 'Leave online party' : 'Save & main menu';
  $('restart-btn').hidden = online && !host;
  $('skip-btn').hidden = !(online && host && getActivePlayer(game) !== onlineIdentity && Date.now() - (roomMeta.changedAt || Date.now()) >= INACTIVE_TURN_MS);
  showDialog('pause-dialog');
}
async function freshRun() {
  if (busy) return;
  if (game.mode === 'online') {
    try { await session.restart(); closeDialogs(); shownResult = false; } catch (error) { toast(error.message); }
  } else {
    menuMode = game.mode;
    const next = createGame({ players: game.players, mode: game.mode, difficulty: game.difficulty, seed: crypto.getRandomValues(new Uint32Array(1))[0] });
    prepareRun(); acceptState(next, {}, true);
  }
}
async function returnToMenu() {
  if (busy) return;
  busy = true;
  try {
    if (game?.mode === 'online') await session?.leave(); else persist();
    bootGeneration++; session?.close(); session = null; game = null; effects.clear(); closeDialogs();
    $('game').hidden = true; $('menu').hidden = false; $('menu-message').textContent = ''; $('start-btn').disabled = false;
    delete document.documentElement.dataset.catnmicePlaying;
    const url = new URL(location.href); url.searchParams.delete('gameId'); url.searchParams.delete('lobbyId'); url.searchParams.set('mode', menuMode === 'online' ? 'solo' : menuMode); history.replaceState({}, '', url);
    chooseMode(menuMode === 'online' ? 'solo' : menuMode);
  } catch (error) { toast(`Could not leave the room yet: ${error.message}`); }
  finally { busy = false; }
}
function showResult() {
  closeDialogs(); $('handoff').hidden = true; handoffPending = false;
  const won = game.status === 'won'; $('result-art').innerHTML = won ? drawTower() : icon('nature');
  $('result-kicker').textContent = won ? 'Ten waves. One beautiful defense.' : 'Every attempt teaches a new spell.';
  $('result-title').textContent = won ? 'The glade is yours.' : 'The tower needs another tale.';
  $('result-description').textContent = `${won ? 'The Ashwing has fallen, and your tower still stands.' : `Your defense reached wave ${game.wave} of 10.`} ${game.kills} creatures defeated · ${game.combos} reactions triggered · ${game.round} rounds.`;
  $('play-again-btn').hidden = game.mode === 'online' && roomMeta.host !== onlineIdentity;
  $('result-menu-btn').textContent = game.mode === 'online' ? 'Leave party' : 'Main menu'; showDialog('result-dialog');
}

for (const button of document.querySelectorAll('[data-mode]')) button.onclick = () => chooseMode(button.dataset.mode);
for (const button of document.querySelectorAll('[data-close]')) button.onclick = () => $(button.dataset.close).close();
for (const button of document.querySelectorAll('[data-tab]')) button.onclick = () => showGuide(button.dataset.tab);
$('seats').onchange = renderSeatNames;
const requestedSeats = Number(params.get('players')); if (requestedSeats >= 2 && requestedSeats <= 4) $('seats').value = String(Math.floor(requestedSeats));
$('start-btn').onclick = () => {
  if (menuMode !== 'online') { startLocal(); return; }
  if (isPack) { $('menu-message').textContent = 'Return to the launcher and choose Online on Spell Tower. Your local run stays saved here.'; toast('Choose Online on Spell Tower in the launcher.'); }
  else location.assign(lobbyURL.href);
};
$('lobby-link').onclick = event => { if (isPack) { event.preventDefault(); toast('Close this game window to return to your launcher.'); } };
$('resume-btn').onclick = () => { const saved = loadRun(storage); if (saved) startLocal(saved); else { toast('No saved run is available. Start a new adventure.'); chooseMode(menuMode); } };
$('menu-guide').onclick = () => showGuide(); $('guide-btn').onclick = () => showGuide(); $('all-combos-btn').onclick = () => showGuide('interactions');
$('endTurnBtn').onclick = () => dispatch({ type: 'endTurn' });
$('fuse-btn').onclick = () => { if (!isTurn()) return; fuseMode = !fuseMode; fuseFirst = null; selectedId = null; render(); };
$('ready-btn').onclick = () => { handoffPending = false; $('handoff').hidden = true; render(); $('hand').querySelector('button')?.focus(); };
$('pause-btn').onclick = openPause; $('resume-game-btn').onclick = () => $('pause-dialog').close();
$('restart-btn').onclick = freshRun; $('play-again-btn').onclick = freshRun;
$('leave-btn').onclick = returnToMenu; $('result-menu-btn').onclick = returnToMenu;
$('skip-btn').onclick = async () => { try { await session.skipInactive(); $('pause-dialog').close(); } catch (error) { toast(error.message); } };
$('gift-btn').onclick = () => {
  if (!isTurn() || !selected()) return;
  $('gift-options').innerHTML = game.players.filter(name => name !== actor()).map(name => `<button class="button" data-recipient="${escape(name)}">Give ${escape(SPELLS[selected().key].name)} to ${escape(name)}</button>`).join('');
  for (const button of $('gift-options').children) button.onclick = async () => { if (await dispatch({ type: 'gift', cardId: selectedId, to: button.dataset.recipient })) $('gift-dialog').close(); };
  showDialog('gift-dialog');
};
$('sound-btn').onclick = () => {
  effects.enableSound(!effects.sound); $('sound-btn').setAttribute('aria-pressed', String(effects.sound));
  $('sound-btn').title = effects.sound ? 'Turn sound off' : 'Turn sound on'; $('sound-btn').setAttribute('aria-label', $('sound-btn').title);
  toast(effects.sound ? 'Spell sounds on' : 'Spell sounds off');
};
document.addEventListener('keydown', event => {
  if (!game || $('game').hidden || document.querySelector('dialog[open]') || event.target.closest('input,select,textarea') || event.ctrlKey || event.metaKey || event.altKey || event.repeat) return;
  if (event.key === 'Escape') { selectedId = null; fuseFirst = null; fuseMode = false; lastPoint = null; render(); }
  else if (/^[1-9]$/.test(event.key)) { const card = hand()[Number(event.key) - 1]; if (card) { event.preventDefault(); chooseCard(card.id); } }
  else if (event.key.toLowerCase() === 'f' && isTurn()) { event.preventDefault(); $('fuse-btn').click(); }
  else if (event.key.toLowerCase() === 'e' && isTurn()) { event.preventDefault(); dispatch({ type: 'endTurn' }); }
});
window.addEventListener('offline', () => { if (game?.mode === 'online') { connection = 'Offline · reconnecting'; $('connection-label').textContent = connection; toast('Connection lost. Your committed moves are saved; reconnect to continue.'); } });
window.addEventListener('pagehide', () => { persist(); session?.close(); });
// A back/forward-cache restore does not re-run modules, but pagehide closed the
// subscriptions. Reload the shared snapshot instead of reviving a closed room.
window.addEventListener('pageshow', event => { if (event.persisted && game?.mode === 'online') location.reload(); });
buildBoard(); chooseMode(menuMode);
if (roomId) startOnline();
