import {firebase, getProfile, rankFor, rankEmblem} from './profile.mjs';
import {createLobbyStore} from './lobby-store.mjs';
import {assetUrl, loadManifest} from './assets.mjs';

const $ = id => document.getElementById(id);
const params = new URLSearchParams(location.search);
const FACTIONS = {frontier: 'Frontier Directorate', ascendant: 'Ascendant Conclave', brood: 'Brood Dominion'};
const STRENGTHS = {frontier: 'Ranged firepower · Repairs · Bunkers · Siege', ascendant: 'Shields · Elite range · Battlefield control', brood: 'Rapid reinforcement · Creep regeneration · Flanking'};
const RANKED_NOTICE = 'Ranked matches are currently unavailable. Custom multiplayer works normally and your existing saved rank remains visible.';
let identity = null, store = null, room = null, unsubscribe = null, busy = false, launched = false;
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

function status(message, error = false) {
  $('connection').textContent = message;
  $('connection').classList.toggle('error', error);
}
async function action(fn) {
  if (busy) return;
  busy = true;
  try { await fn(); } catch (error) { status(error.message, true); }
  finally { busy = false; }
}
function displayName() { return identity?.username || localStorage.getItem('gh.username') || identity?.displayName || 'Commander'; }
function panels(name) {
  for (const [id, key] of [['browserPanel', 'browse'], ['roomPanel', 'room'], ['rankPanel', 'rank']]) $(id).hidden = name !== key;
}
function factionCards(host, select, disabled = false) {
  host.innerHTML = '';
  for (const [id, name] of Object.entries(FACTIONS)) {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'faction-card'; button.disabled = disabled;
    button.setAttribute('aria-pressed', String(select.value === id));
    button.innerHTML = `<img src="./turncraft/assets/factions/${id}.webp" alt="" loading="lazy"><b>${escape(name)}</b><small>${escape(STRENGTHS[id])}</small>`;
    button.onclick = () => {select.value = id; for (const card of host.children) card.setAttribute('aria-pressed', String(card === button)); select.onchange?.();};
    host.append(button);
  }
}
function gameLink() {
  const url = new URL('./turncraft.html', location.href);
  url.searchParams.set('gameId', room.id);
  url.searchParams.set('username', room.members[identity.uid]);
  url.searchParams.set('turncraftLobby', '1');
  url.searchParams.set('returnTo', '/game/turncraft-lobby.html?mode=browse');
  return url.href;
}
function launch() {
  if (launched || !room?.members[identity.uid] || room.status !== 'started') return;
  launched = true;
  $('loadingPlayers').innerHTML = room.turncraftSeats.map(uid => {
    const player = room.turncraftPlayers[uid];
    return `<article class="launch-card"><img class="factionSplash" src="./turncraft/assets/factions/${player.factionId}.webp" alt="${escape(FACTIONS[player.factionId])}"><img src="${escape(rankEmblem(player))}" alt="${escape(rankFor(player.mmr, player.rankedMatches))}"><h3>${escape(player.name)}</h3><p>${escape(FACTIONS[player.factionId])}</p><small>${escape(rankFor(player.mmr, player.rankedMatches))}${player.rankedMatches ? ' · '+Math.round(player.mmr)+' MMR' : ''}</small></article>`;
  }).join('');
  $('loadingStatus').textContent = `${$('map').selectedOptions[0]?.textContent || room.settings.mapId} · Custom multiplayer · Connecting to the shared battlefield…`;
  $('loadingDialog').showModal();
  setTimeout(() => location.assign(gameLink()), 700);
}
function renderRoom() {
  if (!room) return;
  const mine = room.turncraftPlayers[identity.uid];
  if (!mine) {status('Your seat is no longer in this room.', true); return;}
  panels('room');
  const host = room.hostUid === identity.uid, started = room.status === 'started', finished = room.status === 'finished', waiting = room.status === 'waiting';
  $('roomName').textContent = room.name;
  $('roomVisibility').textContent = `${room.visibility} lobby · Custom multiplayer`;
  $('inviteCode').textContent = room.inviteCode || 'Loading…';
  $('players').innerHTML = room.turncraftSeats.map(uid => {
    const p = room.turncraftPlayers[uid], rank = rankFor(p.mmr, p.rankedMatches);
    return `<div class="row"><div class="commander"><img class="emblem" src="${escape(rankEmblem(p))}" alt="${escape(rank)}">${p.avatar && /^https:\/\//.test(p.avatar) ? `<img class="avatar" src="${escape(p.avatar)}" alt="">` : ''}<div><div class="name">${escape(p.name)} ${uid === room.hostUid ? '<span class="badge">HOST</span>' : ''}</div><small>${escape(FACTIONS[p.factionId])} · ${escape(rank)}${p.rankedMatches ? ' · '+Math.round(p.mmr)+' MMR' : ''}</small></div></div><strong class="${p.ready ? 'ready' : 'muted'}">${finished ? 'Finished' : p.ready ? 'Ready' : 'Preparing'}</strong></div>`;
  }).join('');
  $('faction').value = mine.factionId; $('faction').disabled = !waiting;
  factionCards($('roomFactionCards'), $('faction'), !waiting);
  $('ready').hidden = finished; $('ready').textContent = mine.ready ? 'Cancel Ready' : 'Ready'; $('ready').disabled = !waiting;
  $('start').hidden = !host; $('start').textContent = finished ? 'Rematch' : 'Start Match';
  $('start').disabled = started || waiting && (room.turncraftSeats.length < 2 || !room.turncraftSeats.every(uid => room.turncraftPlayers[uid].ready));
  $('resume').hidden = !started; $('leave').disabled = started;
  $('roomNote').textContent = finished ? 'Battle complete. The host can open a fresh rematch room; every commander readies again.' : started ? 'This match has started. Reconnect with the same CatnMice browser identity.' : host ? 'Launch when every commander is ready. At least two human players are required.' : 'Waiting for the host to launch after everyone is ready.';
  for (const id of ['map', 'gameMode', 'maxPlayers', 'visibility']) $(id).disabled = !host || !waiting;
  $('map').value = room.settings.mapId; $('gameMode').value = room.settings.gameMode;
  $('maxPlayers').value = String(room.settings.maxPlayers); $('visibility').value = room.visibility;
  let report = $('roomResult');
  if (!report) {report = document.createElement('div'); report.id = 'roomResult'; $('players').after(report);}
  report.innerHTML = '';
  if (finished) {
    const result = room.turncraftResult || {}, players = result.players || room.players.map(name => ({name})), winner = result.winnerIdx == null ? 'Draw' : players[result.winnerIdx]?.name || 'Commander';
    report.innerHTML = `<div class="notice"><h3>${escape(winner)}${result.winnerIdx == null ? '' : ' wins'}</h3><p>${escape(result.reason || 'The battlefield is secure.')}</p><small>${Math.max(0, Math.floor(result.durationSeconds || 0))} seconds · Custom match · Saved rank unchanged</small></div>`;
  }
  status(finished ? 'Battle complete. Your result is saved.' : started ? 'Your match is ready. Reconnecting to the battlefield…' : 'Lobby synchronized. Choose your faction, then mark yourself ready.');
  if (started) launch();
}
async function openRoom(data) {
  if (data.nextLobbyId) return openRoom(await store.get(data.nextLobbyId));
  unsubscribe?.(); launched = false; room = data;
  sessionStorage.setItem('turncraft.lastLobby', data.id);
  history.replaceState({}, '', `?lobbyId=${encodeURIComponent(data.id)}`);
  renderRoom();
  const {db, storeApi} = await firebase();
  let generation = 0;
  unsubscribe = storeApi.onSnapshot(storeApi.doc(db, 'lobbies', data.id), async snapshot => {
    const observed = ++generation;
    if (room?.id !== data.id) return;
    if (snapshot.exists() && snapshot.data().nextLobbyId) {
      unsubscribe?.(); void store.get(snapshot.data().nextLobbyId).then(openRoom).catch(error => status(error.message, true)); return;
    }
    if (!snapshot.exists() || snapshot.data().status === 'closed') {unsubscribe?.(); room = null; await browse(); status('This command room has closed.'); return;}
    const row = snapshot.data();
    if (!row.members?.[identity.uid]) {unsubscribe?.(); room = null; await browse(); status('You are no longer a member of this room.'); return;}
    const decorated = await store.decorate({id: data.id, ...row, inviteCode: room.inviteCode || data.inviteCode});
    if (observed !== generation || room?.id !== data.id) return;
    room = decorated; renderRoom();
  }, () => status('Connection interrupted. Firestore will reconnect automatically; your seat is reserved.', true));
}
async function browse(quick = false) {
  panels('browse');
  const response = await store.browse(), rows = response.lobbies.filter(row => !row.members?.[identity.uid]);
  if (quick && rows.length) {await openRoom(await store.join({lobbyId: rows[0].id})); return;}
  $('lobbies').innerHTML = '';
  for (const row of response.lobbies) {
    const div = document.createElement('div'); div.className = 'row';
    const text = document.createElement('div');
    text.innerHTML = `<div class="room-name">${escape(row.name)}</div><small>${escape(row.host)} · ${row.players.length}/${row.settings.maxPlayers} commanders · ${escape(row.settings.mapId)}</small>`;
    const button = document.createElement('button'); button.textContent = row.members?.[identity.uid] ? 'Reconnect' : 'Join Match';
    button.onclick = () => action(async () => openRoom(row.members?.[identity.uid] ? await store.get(row.id) : await store.join({lobbyId: row.id})));
    div.append(text, button); $('lobbies').append(div);
  }
  if (!response.lobbies.length) $('lobbies').innerHTML = '<p class="muted">No open lobbies yet. Create one and invite a friend.</p>';
  status(quick ? 'No compatible lobby is open yet. Create a room or browse again.' : 'Public custom lobbies are up to date.');
}
async function rankings() {
  panels('rank');
  const result = await store.leaderboard();
  $('rankNotice').textContent = 'Existing saved TurnCraft ranks are shown below. Custom matches do not change ratings.';
  $('leaderboard').innerHTML = result.players.length ? `<table class="rank-table"><thead><tr><th>Rank</th><th>Commander</th><th>MMR</th><th>Wins / Losses</th></tr></thead><tbody>${result.players.map((p, i) => `<tr><td>${i+1}</td><td><img class="emblem" src="${escape(rankEmblem(p))}" alt="${escape(rankFor(p.mmr, p.rankedMatches))}"> ${p.username === displayName() ? '<a href="./turncraft.html?view=profile">'+escape(p.username)+'</a>' : escape(p.username)}</td><td>${Math.round(p.mmr)}</td><td>${p.rankedWins || 0} / ${p.rankedLosses || 0}</td></tr>`).join('')}</tbody></table>` : '<p class="muted">No saved TurnCraft ranked profiles yet.</p>';
}
function ranked() {
  panels('rank'); $('rankNotice').textContent = RANKED_NOTICE;
  $('leaderboard').innerHTML = '<p class="muted">Campaign, survival and custom games never change MMR.</p><button id="browseCustom" class="primary">Browse Custom Matches</button>';
  $('browseCustom').onclick = () => action(() => browse());
  status('Custom multiplayer is ready to play. Ranked results are not enabled.');
}
async function configure() {
  if (!room) return;
  const result = await store.configure(room.id, {settings: {mapId: $('map').value, gameMode: $('gameMode').value, maxPlayers: Number($('maxPlayers').value), startingResources: 'standard'}, visibility: $('visibility').value});
  room = {...room, ...result}; renderRoom();
}
$('quick').onclick = () => action(() => browse(true));
$('browse').onclick = () => action(() => browse());
$('create').onclick = () => $('createDialog').showModal();
$('code').onclick = () => $('codeDialog').showModal();
$('cancelCreate').onclick = () => $('createDialog').close();
$('cancelCode').onclick = () => $('codeDialog').close();
$('rankings').onclick = () => action(rankings); $('ranked').onclick = ranked;
$('createForm').onsubmit = event => {
  event.preventDefault(); void action(async () => {
    const requestId = sessionStorage.getItem('turncraft.pendingLobbyRequest') || crypto.randomUUID();
    sessionStorage.setItem('turncraft.pendingLobbyRequest', requestId);
    const row = await store.create({requestId, name: $('newName').value, visibility: $('newVisibility').value, factionId: 'frontier'});
    sessionStorage.removeItem('turncraft.pendingLobbyRequest'); $('createDialog').close(); await openRoom(row);
  });
};
$('codeForm').onsubmit = event => {
  event.preventDefault(); void action(async () => {const row = await store.join({code: $('joinCode').value.trim().toUpperCase()}); $('codeDialog').close(); await openRoom(row);});
};
$('faction').onchange = () => action(() => store.player(room.id, {factionId: $('faction').value}));
$('ready').onclick = () => action(() => store.player(room.id, {ready: !room.turncraftPlayers[identity.uid].ready}));
$('start').onclick = () => action(async () => {
  const rematching = room.status === 'finished', result = rematching ? await store.rematch(room.id) : await store.start(room.id);
  if (rematching) await openRoom(result);
});
$('resume').onclick = launch;
$('leave').onclick = () => action(async () => {
  await store.leave(room.id); unsubscribe?.(); room = null; sessionStorage.removeItem('turncraft.lastLobby');
  history.replaceState({}, '', '?mode=browse'); await browse();
});
for (const id of ['map', 'gameMode', 'maxPlayers', 'visibility']) $(id).onchange = () => action(configure);
$('copyInvite').onclick = () => action(async () => {const url = new URL('./turncraft-lobby.html', location.href); url.searchParams.set('code', room.inviteCode); await navigator.clipboard.writeText(url.href); status('Invite link copied. Share it with another commander.');});
async function boot() {
  await loadManifest(); document.body.style.backgroundImage = `linear-gradient(120deg,#08111be8,#111f34dc),url("${assetUrl('menu.background', 'ui/main_menu/battlefield.webp')}")`;
  const response = await getProfile(); identity = response.identity;
  const {db, auth, storeApi} = await firebase();
  store = createLobbyStore({db, auth, api: storeApi, getIdentity: () => ({...identity, username: displayName()})});
  const value = params.get('lobbyId') || params.get('gameId'), code = params.get('code');
  if (code) {$('joinCode').value = code; $('codeDialog').showModal(); status('Invitation found. Press Join Lobby to claim your seat.'); return;}
  if (value) {
    try {const row = await store.get(value); await openRoom(row.members?.[identity.uid] ? row : await store.join({lobbyId: value})); return;}
    catch (error) {if (error.code !== 'INVITE_REQUIRED') throw error; $('codeDialog').showModal(); status('Enter the private invite code to claim your seat.'); return;}
  }
  const mode = params.get('mode');
  if (mode === 'create') $('createDialog').showModal();
  if (mode === 'code') $('codeDialog').showModal();
  if (mode === 'ranked') {ranked(); return;}
  if (mode === 'leaderboard') {await rankings(); return;}
  await browse(mode === 'quick');

}
document.addEventListener('error', event => {if (event.target instanceof HTMLImageElement) event.target.hidden = true;}, true);
void action(boot);
addEventListener('beforeunload', () => {unsubscribe?.();});
