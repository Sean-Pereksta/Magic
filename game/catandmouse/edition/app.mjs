import { createSimulation, getPlacementError } from './simulation.mjs';
import { DIFFICULTIES, isCompleteLoadout, MAPS, STARTER_LOADOUT, STEP_MS, TOWERS } from './battlefield.mjs';
import { CAT_DIRECTORY_URL, PROTOCOL_VERSION } from './directory-config.mjs';
import { Battlefield, loadPaintings, prepareBattlefield, towerCell } from './board.mjs';
import { isWall } from './walls.mjs';

const $ = id => document.getElementById(id);
const screen = $('screen');
const overlay = $('overlay');
const canvas = $('field');
const board = new Battlefield(canvas, $('minimap'));
const mobileLayout = matchMedia('(pointer: coarse), (max-width: 570px)');
const mill = MAPS['flour-mill'];
const arcade = 'https://www.catnmice.com/lobby/lobby.html#live-lobbies';
const escape = value => String(value ?? '').replace(/[&<>"']/g, character => (
  { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[character]
));
const controlParams = new URLSearchParams(location.hash.slice(1));
const localHost = ['127.0.0.1', 'localhost'].includes(location.hostname);
const controlPort = Number(controlParams.get('control'));
const desktop = localHost && controlPort > 0 && controlPort < 65536 && controlParams.get('desktop');
const expectedRoom = controlParams.get('room');
const inviteCode = controlParams.get('invite');
const browserName = 'Mouse';
let view = '';
let network, lobby, host, hostTimer, solo, state, playerId, previousFrame, battleTime, selected, cursor, audio;
let touchAim = false;
let aimTile = null;
let pendingBuild = null;
let placementMessage = '';
let placementMessageUntil = 0;
let fieldTouch = null;
let hostPolling = false;
let lastHud = 0;
let paused = false;
let resultsOpen = false;
let preserveStatus = false;
let directoryPoll;
let profile = read('cat-mouse-profile-v1', { matches: {} });
let settings = { volume: .4, quality: 'high', reduced: matchMedia('(prefers-reduced-motion: reduce)').matches, follow: true, touch: true, ...read('cat-mouse-settings-v1', {}) };
let soloDraft = { difficulty: 'normal', kit: [...STARTER_LOADOUT] };
let lobbyKit = [...STARTER_LOADOUT];
let draftRoom = { roomName: 'The Flour Mill', hostName: read('cat-mouse-name-v1', browserName), difficulty: 'normal', capacity: 3, visibility: 'public' };
const keys = new Set();

function read(key, fallback, storage = localStorage) {
  try { return JSON.parse(storage.getItem(key)) ?? fallback; } catch { return fallback; }
}

function write(key, value, storage = localStorage) {
  try { storage.setItem(key, JSON.stringify(value)); } catch { /* Storage can be disabled. */ }
}

function returnToArcade() {
  const value = controlParams.get('returnUrl');
  if (!value) return arcade;
  try {
    const url = new URL(value);
    const allowed = new Map([
      ['https://www.catnmice.com', '/lobby/lobby.html'],
      ['https://catnmice.com', '/lobby/lobby.html'],
      ['https://sean-pereksta.github.io', '/Magic/lobby/lobby.html'],
    ]);
    if (allowed.get(url.origin) === url.pathname || (localHost && ['localhost', '127.0.0.1'].includes(url.hostname)
      && url.protocol === 'http:' && url.pathname.endsWith('/lobby/lobby.html'))) {
      url.hash = 'live-lobbies';
      return url.href;
    }
  } catch { /* Use the canonical Arcade. */ }
  return arcade;
}

$('arcade-link').href = returnToArcade();
if (desktop) $('edition-label').textContent = 'LOCAL EDITION';

const button = (label, action, kind = 'secondary', disabled = false) => `<button type="button" data-action="${action}" class="${kind}" ${disabled ? 'disabled' : ''}>${label}</button>`;
const link = (label, url, kind = 'secondary') => `<a class="button ${kind}" href="${escape(url)}">${label}</a>`;
const panel = (title, content, back = 'title') => `<section class="panel">${back ? button('← Back', back, 'quiet') : ''}<h1>${title}</h1>${content}</section>`;
const options = (entries, value) => entries.map(([key, label]) => `<option value="${escape(key)}" ${String(value) === key ? 'selected' : ''}>${escape(label)}</option>`).join('');
const difficultyOptions = value => options(Object.entries(DIFFICULTIES).map(([id, difficulty]) => [id, difficulty.name]), value);
const difficultyNote = {
  easy: 'More starting cheese and lighter enemy damage. Watch for the Cat’s marked pounce.',
  normal: 'Late waves send Sappers to attack clustered defenses and Menders to heal the swarm.',
  hard: 'Spread out your defenses and give yourself room to dodge the Cat’s tail sweep.',
  nightmare: 'Watch the ground for the Cat’s sweep and charge. Keep an escape route open.',
};
const mapOptions = () => options(Object.entries(MAPS).map(([id, map]) => [id, map.name]), mill.id);
const mount = (id, html) => {
  if (view === 'browse' && id !== 'browse') clearTimeout(directoryPoll);
  view = id;
  screen.innerHTML = html;
};

function recordSummary() {
  const matches = Object.values(profile.matches || {});
  $('record').textContent = matches.length ? `${matches.filter(match => match.result === 'won').length} WINS · ${matches.length} PLAYED` : '';
}

function title() {
  recordSummary();
  mount('title', `<section class="title-view"><span class="eyebrow">THE FLOUR MILL</span><h1>Cat <em>&amp;</em><br>Mouse</h1>
    <p class="subtitle">Gather cheese, build defenses, and stay one step ahead of the Cat.</p>
    <div class="title-actions">${button('Singleplayer <span>→</span>', 'solo-setup', '')}
      ${button('Multiplayer <span>→</span>', 'multiplayer')}<a href="https://www.catnmice.com/hub/ranked.html?game=catandmouse" target="_top">Ranked Survival Challenge</a>${button('Settings <span>→</span>', 'settings')}</div>
  </section>`);
}

function cardIcon(type) {
  const cell = towerCell(type);
  return `<span class="kit-art" aria-hidden="true" style="background-position:${cell % 4 * 100 / 3}% ${Math.floor(cell / 4) * 50}%"></span>`;
}

function kitForm(kit, disable = false) {
  const catalog = Object.entries(TOWERS).map(([id, item]) => [id, item.name]);
  return `<div class="kit">${kit.map((type, index) => `<label title="${escape(TOWERS[type]?.description || 'Choose a tower')}">
    ${cardIcon(type)} <span>Slot ${index + 1}</span><select data-slot="${index}" ${disable ? 'disabled' : ''}>
    ${options([['', 'Choose…'], ...catalog], type)}</select></label>`).join('')}</div>`;
}

function soloSetup() {
  mount('solo-setup', panel('Singleplayer', `<div class="form-grid">
    <label>Map<select id="solo-map">${mapOptions()}</select></label>
    <label>Difficulty<select id="solo-difficulty">${difficultyOptions(soloDraft.difficulty)}</select></label>
    </div><p class="difficulty-note" id="solo-difficulty-note">${difficultyNote[soloDraft.difficulty]}</p>
    <div class="map-card">${mill.name}<small>48 × 48 · four rat entrances</small></div>
    <h2>Eight-slot loadout</h2>${kitForm(soloDraft.kit)}
    <div class="actions">${button('Start Game', 'start-solo', '', !isCompleteLoadout(soloDraft.kit))}
      ${button('Use Starter Kit', 'starter')}</div><p id="view-status" role="status"></p>`));
}

function multiplayer() {
  mount('multiplayer', panel('Multiplayer', `<div class="stack">
    ${button('Browse Lobbies <span>→</span>', 'browse', '')}
    ${button('Join by Invite <span>→</span>', 'invite')}
    ${button('Host Lobby <span>→</span>', 'host')}
    </div><p id="view-status" role="status" hidden></p>
    <div id="host-download" class="actions" hidden>${link('Download Local Edition', './download.html', '')}</div>`));
}

function joinByInvite() {
  mount('invite', panel('Join by Invite', `<label>Guest invitation link<input id="invite-link" placeholder="Paste the link from your host" enterkeyhint="go"></label>
    <div class="actions">${button('Open Waiting Room', 'open-invite', '')}</div><p id="view-status" role="status"></p>`, 'multiplayer'));
}

function allowedJoin(url) {
  const info = new URLSearchParams(url.hash.slice(1));
  const room = info.get('room');
  return !url.username && !url.password && url.pathname === '/' && /^[a-zA-Z0-9_-]{8,100}$/.test(room || '')
    && (url.protocol === 'https:' || (localHost && url.protocol === 'http:' && ['127.0.0.1', 'localhost'].includes(url.hostname)));
}

function message(text, isError = false) {
  const status = $('view-status');
  if (status) {
    status.hidden = false;
    status.textContent = text;
    status.classList.toggle('error', isError);
  }
}

async function browse() {
  mount('browse', panel('Live Lobbies', `<p id="view-status" role="status">Looking for rooms…</p><ul class="room-list" id="room-list"></ul>
    <div class="actions">${button('Refresh', 'browse')}${link('All Arcade Lobbies', returnToArcade())}</div>`, 'multiplayer'));
  if (!CAT_DIRECTORY_URL) return message('Cat & Mouse discovery is unavailable. You can still join with an invitation.', true);
  const list = $('room-list');
  const refresh = async () => {
    if (view !== 'browse' || list !== $('room-list')) return;
    clearTimeout(directoryPoll);
    if (document.hidden) {
      directoryPoll = setTimeout(refresh, 4000);
      return;
    }
    try {
      const response = await fetch(new URL('/rooms', CAT_DIRECTORY_URL), { cache: 'no-store', signal: AbortSignal.timeout(6000) });
      if (!response.ok) throw new Error('Directory unavailable');
      const { rooms } = await response.json();
      if (list !== $('room-list')) return;
      const items = [];
      for (const room of rooms) {
        if (String(room.version) !== String(PROTOCOL_VERSION) || !(room.openSeats > 0) || !(Date.parse(room.expiresAt) > Date.now())) continue;
        try {
          const url = new URL('/#room=' + encodeURIComponent(room.id), room.serverUrl);
          if (!allowedJoin(url)) continue;
          items.push(`<li><div><b>${escape(room.roomName)}</b><span>${escape(room.hostName)} · ${escape(MAPS[room.map]?.name || room.map)} · ${escape(DIFFICULTIES[room.difficulty]?.name || room.difficulty)} · ${escape(room.openSeats)} open</span></div>${link('Join', url.href, '')}</li>`);
        } catch { /* Skip corrupt origins. */ }
      }
      list.innerHTML = items.join('');
      message(items.length ? '' : 'No public rooms are open.');
    } catch {
      if (list !== $('room-list')) return;
      list.replaceChildren();
      message('Cat & Mouse discovery is unavailable. You can still join with an invitation.', true);
    }
    directoryPoll = setTimeout(refresh, 10000);
  };
  await refresh();
}

function settingsMarkup() {
  return `<label>Sound volume <input id="volume" type="range" min="0" max="1" step="0.05" value="${settings.volume}"></label>
    <label>Graphics quality<select id="quality">${options([['high', 'High'], ['low', 'Low']], settings.quality)}</select></label>
    <label class="check"><input type="checkbox" id="reduced" ${settings.reduced ? 'checked' : ''}> Reduced motion</label>
    <label class="check"><input type="checkbox" id="follow" ${settings.follow ? 'checked' : ''}> Follow my mouse with the camera</label>
    <label class="check"><input type="checkbox" id="touch" ${settings.touch ? 'checked' : ''}> Touch controls</label>
    <p>Tap the floor to walk. On touch, open Build, choose a structure, tap a tile, then tap Place. With a keyboard and mouse, use WASD or arrows to walk and 1–8 to build. You can walk through your own structures.</p>`;
}

function renderSettings() {
  mount('settings', panel('Settings', settingsMarkup() + `<p><a href="https://www.catnmice.com/game/catandmouse.html">Classic edition and account history</a></p>`));
}

function hostSetup() {
  mount('host', panel('Host Lobby', `<div class="form-grid">
    <label>Room name<input id="room-name" maxlength="60" value="${escape(draftRoom.roomName)}"></label>
    <label>Host name<input id="host-name" maxlength="30" value="${escape(draftRoom.hostName)}"></label>
    <label>Map<select id="room-map">${mapOptions()}</select></label>
    <label>Difficulty<select id="room-difficulty">${difficultyOptions(draftRoom.difficulty)}</select></label>
    <label>Seats<select id="capacity">${options([['1', '1 seat'], ['2', '2 seats'], ['3', '3 seats'], ['4', '4 seats']], draftRoom.capacity)}</select></label>
    <label>Visibility<select id="visibility">${options([['public', 'Public'], ['invite', 'Invite only']], draftRoom.visibility)}</select></label>
    </div><p class="difficulty-note" id="room-difficulty-note">${difficultyNote[draftRoom.difficulty]}</p>
    <div class="actions">${button('Start Server', 'host-start', '')}</div><p id="view-status" role="status"></p>`, 'multiplayer'));
}

async function control(method = 'GET', body, suffix = '') {
  const response = await fetch(`http://127.0.0.1:${controlPort}/host${suffix}`, {
    method, headers: { Authorization: 'Bearer ' + desktop, ...(body ? { 'Content-Type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(4500),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || result.error || 'The host controls are unavailable.');
  return result;
}

async function refreshHost() {
  clearTimeout(hostTimer);
  if (!hostPolling) return;
  try {
    const status = await control();
    if (!hostPolling) return;
    host = status;
    if (host.serverUrl && host.hostToken && host.lobbyId && !network) {
      connect(host.serverUrl, host.lobbyId, { name: draftRoom.hostName, hostToken: host.hostToken });
    }
    showHostStatus();
  } catch (error) { message(error.message, true); }
  if (hostPolling) hostTimer = setTimeout(refreshHost, 1500);
}

function showHostStatus() {
  const line = $('host-status');
  if (!line || !host) return;
  line.textContent = [host.message, host.directoryStatus].filter(Boolean).join(' · ');
  $('copy-invite').disabled = !host.inviteUrl;
  $('retry-tunnel').hidden = host.stage !== 'failed';
}

function hostingStatus() {
  mount('hosting', panel('Opening the lobby', `<p id="host-status" role="status">Starting the match server…</p><p id="view-status" role="status"></p>
    <div class="actions">${button('Cancel', 'host-stop')}${button('Retry Tunnel', 'host-retry')}${button('Copy Invitation', 'copy-invite', 'secondary', true)}</div>`, null));
  screen.querySelector('[data-action="host-retry"]').id = 'retry-tunnel';
  screen.querySelector('[data-action="copy-invite"]').id = 'copy-invite';
  showHostStatus();
}

function joinPrompt() {
  const name = read('cat-mouse-name-v1', browserName);
  mount('join', panel('Enter the waiting room', `<label>Display name<input id="guest-name" value="${escape(name)}" maxlength="30" enterkeyhint="go"></label>
    <div class="actions">${button('Join Room', 'join-room', '')}${link('Return to Live Lobbies', returnToArcade())}</div><p id="view-status" role="status"></p>`, null));
}

function updateLobby() {
  const self = lobby?.players.find(entry => entry.id === playerId);
  if (!self || !$('battle').hidden) return;
  const inWaiting = lobby.phase === 'waiting';
  const invite = self.isHost ? `<p id="host-status" role="status"></p><div class="actions">${button('Copy Invitation', 'copy-invite', 'secondary', !host?.inviteUrl)}
    ${button('Retry Tunnel', 'host-retry')}${button('Stop Hosting', 'host-stop')}</div>` : '';
  const start = self.isHost ? `${button('Start', 'normal-start', '', !lobby.canStart)}
    ${button('Force Start', 'force-start', 'secondary', !inWaiting)}` : '';
  if (view !== 'waiting') {
    mount('waiting', panel(escape(lobby.roomName), `<p id="room-summary"></p><ul class="roster" id="roster"></ul>
      <h2>Your loadout</h2><div id="lobby-kit"></div><div class="actions" id="ready-actions"></div>
      ${invite}<p id="view-status" role="status"></p>`, null));
    if (self.isHost) {
      screen.querySelector('[data-action="host-retry"]').id = 'retry-tunnel';
      screen.querySelector('[data-action="copy-invite"]').id = 'copy-invite';
    }
  }
  $('room-summary').textContent = `${MAPS[lobby.map]?.name || lobby.map} · ${DIFFICULTIES[lobby.difficulty]?.name || lobby.difficulty} · ${lobby.players.length}/${lobby.capacity} seats`;
  $('roster').innerHTML = lobby.players.map(entry => `<li><b>${escape(entry.name)}${entry.isHost ? ' · Host' : ''}</b>
    <span>${entry.connected ? 'Connected' : 'Disconnected'} · ${entry.ready ? 'Ready' : 'Choosing loadout'}${entry.loaded ? ' · Battlefield loaded' : ''}
    · ${escape((entry.loadout || []).filter(type => TOWERS[type]).map(type => TOWERS[type].name).join(', '))}</span></li>`).join('');
  const key = JSON.stringify([lobbyKit, self.ready, inWaiting]);
  if ($('lobby-kit').dataset.key !== key) {
    $('lobby-kit').dataset.key = key;
    $('lobby-kit').innerHTML = kitForm(lobbyKit, self.ready || !inWaiting);
  }
  const buttons = `${button(self.ready ? 'Unready' : 'Ready', 'ready', self.ready ? 'secondary' : '', !inWaiting || !isCompleteLoadout(lobbyKit))}
    ${start}${self.isHost ? '' : button('Leave Room', 'leave-room')}`;
  if ($('ready-actions').dataset.key !== buttons) {
    $('ready-actions').dataset.key = buttons;
    $('ready-actions').innerHTML = buttons;
  }
  showHostStatus();
  if (lobby.phase === 'loading') message('Loading the battlefield for everyone…');
  else if (lobby.phase === 'countdown') message('Battle starts in a few seconds.');
  else if (lobby.phase === 'waiting' && !preserveStatus) message('');
}

function transmit(payload) {
  if (network?.socket?.readyState !== WebSocket.OPEN) return false;
  network.socket.send(JSON.stringify(payload));
  return true;
}

function connect(origin, room, identity) {
  disconnect();
  const tokenKey = 'cat-mouse-seat:' + new URL(origin).origin + ':' + room;
  const connection = { origin, room, identity, tokenKey, token: read(tokenKey, null, sessionStorage), attempt: 0, stopped: false };
  network = connection;
  openSocket(connection);
}

function openSocket(connection) {
  const url = new URL('/socket', connection.origin);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  const socket = new WebSocket(url);
  connection.socket = socket;
  const watchdog = setTimeout(() => socket.close(), 9000);
  socket.addEventListener('open', () => {
    if (network !== connection) return socket.close();
    transmit({ type: 'join', roomId: connection.room, version: PROTOCOL_VERSION, ...connection.identity, token: connection.token });
  });
  socket.addEventListener('message', async event => {
    if (network !== connection || connection.socket !== socket) return;
    let data;
    try { data = JSON.parse(event.data); } catch { return; }
    if (data.type === 'joined') {
      clearTimeout(watchdog);
      connection.token = data.token;
      connection.joinedSocket = socket;
      connection.attempt = 0;
      playerId = data.playerId;
      write(connection.tokenKey, data.token, sessionStorage);
      connection.priorKit = true;
      networkNotice('');
    } else if (data.type === 'lobby') {
      lobby = data.lobby;
      if (connection.priorKit) {
        const self = lobby.players.find(entry => entry.id === playerId);
        if (self?.loadout?.length === STARTER_LOADOUT.length) lobbyKit = self.loadout.slice();
        connection.priorKit = false;
      }
      updateLobby();
    } else if (data.type === 'prepare') {
      try {
        await loadPaintings();
        if (network === connection && connection.socket === socket && MAPS[data.map]) {
          prepareBattlefield(data.map);
          transmit({ type: 'loaded', matchId: data.matchId });
        }
      } catch { message('The painted battlefield did not load. Try rejoining the room.', true); }
    } else if (data.type === 'snapshot') {
      try {
        await loadPaintings();
        if (network === connection && connection.socket === socket) receiveState(data.state, data.events);
      } catch { networkNotice('The painted battlefield could not load.'); }
    } else if (data.type === 'delta') {
      if (state?.tick !== data.baseTick) transmit({ type: 'resync' });
      else receiveState({ ...state, ...data.changes }, data.events);
    } else if (data.type === 'action-result' && !data.ok) {
      reportCommandError(data.code);
    } else if (data.type === 'error') {
      if (!socketHasSeat(connection, socket) || ['room-changed', 'started', 'load-timeout', 'seat-expired'].includes(data.code)) {
        if (data.code === 'seat-expired') clearSeat(connection);
        connection.stopped = true;
        clearTimeout(watchdog);
        terminal(data.message || 'This room is unavailable.');
      } else if (lobby?.phase === 'running' || !$('battle').hidden) {
        reportCommandError(data.code, data.message);
      } else {
        preserveStatus = true;
        message(data.message || 'The room declined that request.', true);
      }
    }
  });
  socket.addEventListener('close', () => {
    clearTimeout(watchdog);
    if (network !== connection || connection.stopped) return;
    if (!connection.token || connection.attempt >= 12) {
      terminal(connection.token ? 'Your connection to the host ended.' : 'The host could not be reached. The room may have closed.');
      return;
    }
    connection.attempt++;
    if (pendingBuild) reportCommandError('connection-lost');
    networkNotice('Reconnecting to the host…');
    message('Reconnecting to the host…');
    connection.retry = setTimeout(() => openSocket(connection), Math.min(5000, 400 + connection.attempt * 460));
  });
}

function socketHasSeat(connection, socket) {
  return playerId && connection.joinedSocket === socket;
}

function clearSeat(connection) {
  try { sessionStorage.removeItem(connection.tokenKey); } catch { /* Storage can be disabled. */ }
}

function disconnect() {
  if (network) {
    network.stopped = true;
    clearTimeout(network.retry);
    network.socket?.close();
  }
  network = null;
  lobby = null;
  playerId = null;
  preserveStatus = false;
  lobbyKit = [...STARTER_LOADOUT];
}

function terminal(reason) {
  if (overlay.open) overlay.close();
  leaveBattle();
  disconnect();
  mount('unavailable', panel('Room unavailable', `<p class="error" role="status">${escape(reason)}</p>
    <div class="actions">${link('Return to Live Lobbies', returnToArcade(), '')}${button('Main Menu', 'title')}</div>`, null));
}

function networkNotice(text) {
  $('network-status').hidden = !text;
  $('network-status').textContent = text;
}

function sound(kind) {
  if (!audio || settings.volume <= 0) return;
  const oscillator = audio.createOscillator();
  const gain = audio.createGain();
  const time = audio.currentTime;
  oscillator.type = kind === 'impact' ? 'triangle' : 'sine';
  oscillator.frequency.setValueAtTime(kind === 'impact' ? 140 : 530, time);
  oscillator.frequency.exponentialRampToValueAtTime(kind === 'impact' ? 65 : 770, time + .09);
  gain.gain.setValueAtTime(Math.max(.0002, settings.volume * .065), time);
  gain.gain.exponentialRampToValueAtTime(.0001, time + .13);
  oscillator.connect(gain).connect(audio.destination);
  oscillator.start();
  oscillator.stop(time + .14);
}

function unlockAudio() {
  if (typeof AudioContext !== 'function' || settings.volume <= 0) return;
  audio ||= new AudioContext();
  if (audio.state === 'suspended') audio.resume().catch(() => {});
}

async function startSolo() {
  if (!isCompleteLoadout(soloDraft.kit)) return;
  message('Loading the painted battlefield…');
  const screenId = view;
  try {
    await loadPaintings();
    if (view !== screenId || solo) return;
    prepareBattlefield(mill.id);
    playerId = 'solo-mouse';
    solo = createSimulation({
      matchId: crypto.randomUUID(), mapId: mill.id, difficulty: soloDraft.difficulty,
      players: [{ id: playerId, name: 'Mouse', loadout: soloDraft.kit }],
    });
    battleTime = 0;
    receiveState(solo.snapshot());
  } catch (error) { message(`The battlefield could not start. ${error.message}`, true); }
}

function receiveState(next, effects = next.events) {
  if (!state || state.matchId !== next.matchId) {
    document.documentElement.dataset.catnmicePlaying='true';window.dispatchEvent(new Event('catnmice-game-started'));if(location.protocol==='https:'&&location.hostname.endsWith('.trycloudflare.com')){void import('https://www.catnmice.com/hub/community-activity.mjs').then(m=>m.recordCommunity('cat')).catch(()=>{});}
    board.reset(next.mapId);
    board.camera.follow = settings.follow;
    resetPlacement();
    syncBattleLayout();
    resultsOpen = false;
    $('menu').hidden = true;
    $('battle').hidden = false;
    renderBuild(next.players.find(entry => entry.id === playerId)?.loadout || STARTER_LOADOUT);
    canvas.focus({ preventScroll: true });
  }
  state = next;
  const recent = board.push(next, effects, playerId);
  if (pendingBuild) {
    const matchesBuild = entry => entry.ownerId === playerId && entry.x === pendingBuild.x && entry.y === pendingBuild.y;
    if (next.structures.some(entry => entry.type === pendingBuild.type && matchesBuild(entry))
      || recent.some(event => event.type === 'build' && event.kind === pendingBuild.type && matchesBuild(event))) {
      finishPlacement();
    }
  }
  const active = recent.filter(event => event.time >= next.time - 175);
  if (active.some(event => event.type === 'impact')) sound('impact');
  if (active.some(event => event.type === 'collect' && event.entityId === playerId)) sound('pickup');
  if (next.status === 'won' || next.status === 'lost') finish(next);
}

function finish(next) {
  if (resultsOpen) return;
  resultsOpen = true;
  profile.matches = { ...profile.matches, ...read('cat-mouse-profile-v1', { matches: {} }).matches };
  if (!Object.hasOwn(profile.matches, next.matchId)) {
    profile.matches[next.matchId] = { result: next.status, map: next.mapId, date: new Date().toISOString(), wave: next.wave };
    write('cat-mouse-profile-v1', profile);
  }
  resetPlacement();
  showOverlay(next.status === 'won' ? 'You survived!' : 'The mice were caught',
    `<p>${next.status === 'won' ? 'You outlasted every wave and escaped the Cat.' : 'The Cat and his swarm caught every mouse.'}</p>
    <div class="actions">${button('Main Menu', 'exit-battle', '')}</div>`);
}

function leaveBattle() {
  solo = null;
  state = null;
  paused = false;
  resultsOpen = false;
  resetPlacement();
  keys.clear();
  networkNotice('');
  $('battle').hidden = true;
  $('menu').hidden = false;
}

function renderBuild(kit) {
  $('build-bar').innerHTML = kit.map((type, index) => {
    const item = TOWERS[type];
    return `<button type="button" data-build="${index}" aria-pressed="false" title="${escape(item.description)}"><span class="hotkey">${index + 1}</span>
      ${cardIcon(type)}<b>${escape(item.name)}</b><small>${item.cost} cheese</small></button>`;
  }).join('');
  updateSelected();
}

function setBuildTray(open) {
  $('build-toggle').setAttribute('aria-expanded', String(open));
}

function syncBattleLayout() {
  $('battle-overview').open = !mobileLayout.matches;
  setBuildTray(false);
}

function resetPlacement() {
  selected = null;
  cursor = null;
  aimTile = null;
  touchAim = false;
  pendingBuild = null;
  placementMessage = '';
  placementMessageUntil = 0;
  fieldTouch = null;
  setBuildTray(false);
  updateSelected();
}

function finishPlacement() {
  resetPlacement();
  if (mobileLayout.matches) $('build-toggle').focus({ preventScroll: true });
  else canvas.focus({ preventScroll: true });
}

function selectSlot(slot, useTouch) {
  if (pendingBuild) return;
  if (slot === selected && touchAim === useTouch) return resetPlacement();
  selected = slot;
  touchAim = useTouch;
  aimTile = null;
  placementMessage = '';
  setBuildTray(false);
  updateSelected();
  if (touchAim) $('build-cancel').focus({ preventScroll: true });
  else canvas.focus({ preventScroll: true });
}

function updateSelected() {
  $('build-bar').querySelectorAll('[data-build]').forEach(button => button.setAttribute('aria-pressed', String(Number(button.dataset.build) === selected)));
  $('battle').classList.toggle('touch-aim', touchAim);
  updatePlacement();
}

function updatePlacement() {
  const type = selectedTower();
  const error = type && aimTile && getPlacementError(state, playerId, type, aimTile.x, aimTile.y);
  const prompt = !type ? 'WASD or arrow keys to move · tap the floor to walk'
    : isWall(type) ? 'Place beside a wall to extend it or form a corner.'
      : `${TOWERS[type].description} Select a tile to build.`;
  let status = placementMessage || prompt;
  if (touchAim && type) {
    if (pendingBuild) status = 'Placing…';
    else if (!aimTile) status = `Tap a tile for ${TOWERS[type].name}.`;
    else status = placementMessage || (error ? commandErrors[error] || 'Choose another tile.' : `${TOWERS[type].name} · ${TOWERS[type].cost} cheese`);
  }
  if ($('build-info').textContent !== status) $('build-info').textContent = status;
  $('battle').classList.toggle('action-status', !touchAim && Boolean(placementMessage));
  $('build-place').disabled = !touchAim || !aimTile || Boolean(error || pendingBuild);
  $('build-cancel').disabled = Boolean(pendingBuild);
}

function selectedTower() {
  const player = state?.players.find(entry => entry.id === playerId);
  return selected === null ? null : (player?.loadout || STARTER_LOADOUT)[selected];
}

const commandErrors = {
  occupied: 'This tile is occupied.', 'out-of-reach': 'Move closer to build here.', 'not-enough-cheese': 'You need more cheese.',
  blocked: 'This terrain cannot be built on.', 'spawn-protected': 'Keep the entrance clear.', 'invalid-cell': 'Choose a tile inside the map.',
  'structure-limit': 'The battlefield is at its building limit.', 'economy-limit': 'You’ve reached the limit for this building.',
  'mouse-caught': 'Your mouse has been caught.', 'match-ended': 'The match has ended.', 'under-construction': 'Wait for this building to finish.',
  'connection-lost': 'Connection lost. Try again when you reconnect.',
};

function reportCommandError(code, detail) {
  pendingBuild = null;
  placementMessage = detail || commandErrors[code] || 'That action could not be completed.';
  placementMessageUntil = performance.now() + 3200;
  updatePlacement();
}

function sendCommand(command) {
  if (!state || paused || state.status !== 'active') return false;
  if (!solo) {
    const sent = transmit({ type: 'input', command });
    if (!sent && command.type !== 'move') reportCommandError('connection-lost');
    return sent;
  }
  const response = solo.command(playerId, command);
  if (response && !response.ok && command.type !== 'move') {
    reportCommandError(response.code);
  }
  return Boolean(response?.ok);
}

function updateHud() {
  const player = state.players.find(entry => entry.id === playerId);
  if (!player) return;
  $('cheese').textContent = player.cheese;
  $('health').textContent = `${Math.ceil(player.hp)}/${player.maxHp}`;
  const cat = state.enemies.find(entry => entry.type === 'cat');
  const warning = cat?.windupUntil > state.time ? {
    pounce: 'Pounce incoming', 'tail-sweep': 'Tail sweep incoming', 'cat-charge': 'Charge incoming',
  }[cat.attackKind] : null;
  $('wave-title').textContent = state.wave ? `Wave ${state.wave}` : 'Wave 1 ahead';
  const until = Math.max(0, Math.ceil((state.nextWaveAt - state.time) / 1000));
  $('wave-clock').textContent = warning || (Number.isFinite(until) && state.nextWaveAt
    ? until ? `Next wave in ${until}s` : 'Wave incoming'
    : state.status === 'active' ? 'Survive the chase' : '');
  $('objective').textContent = !player.hp ? 'Mouse caught' : cat ? 'Cat on the hunt' : 'Stay alive';
  const activeEnemies = state.enemies;
  const hint = activeEnemies.some(enemy => enemy.type === 'sapper' && enemy.windupUntil > state.time)
    ? 'Sapper blast · spread out' : activeEnemies.some(enemy => enemy.type === 'mender')
      ? 'Menders heal nearby rats' : activeEnemies.some(enemy => enemy.type === 'sapper')
        ? 'Sappers attack clustered towers' : '';
  $('threat-hint').hidden = !hint;
  $('threat-hint').textContent = hint;
  $('teammates').innerHTML = state.players.filter(entry => entry.id !== playerId).map(entry => {
    const connection = lobby?.players.find(player => player.id === entry.id);
    return `<div>${escape(entry.name)} · ${connection?.connected === false ? 'Offline' : entry.hp > 0 ? Math.ceil(entry.hp) + ' HP' : 'Caught'}</div>`;
  }).join('');
  $('build-bar').querySelectorAll('[data-build]').forEach(button => {
    const item = TOWERS[player.loadout[Number(button.dataset.build)]];
    button.classList.toggle('unaffordable', player.cheese < item.cost);
  });
  updatePlacement();
}

function frame(now) {
  if (state) {
    if (solo && !paused && state.status === 'active') {
      battleTime += Math.min(now - (previousFrame || now), 180);
      while (battleTime >= STEP_MS && state.status === 'active') {
        solo.step();
        receiveState(solo.snapshot());
        battleTime -= STEP_MS;
      }
    }
    if (now - lastHud >= 125) {
      if (pendingBuild && now > pendingBuild.deadline) reportCommandError(null, 'Placement wasn’t confirmed. Check the tile and try again.');
      if (!touchAim && now > placementMessageUntil) placementMessage = '';
      updateHud();
      lastHud = now;
    }
    const type = selectedTower();
    const tile = touchAim ? aimTile : cursor;
    const target = type && tile ? { ...tile, type, fixed: touchAim, available: !getPlacementError(state, playerId, type, tile.x, tile.y) } : null;
    board.draw(now, settings, target);
  } else if (view === 'waiting' && lobby?.phase === 'countdown') {
    const seconds = Math.max(1, Math.ceil((lobby.countdownUntil - Date.now()) / 1000));
    const label = `Battle starts in ${seconds}…`;
    if ($('view-status').textContent !== label) message(label);
  }
  previousFrame = now;
  requestAnimationFrame(frame);
}

function showOverlay(title, content) {
  $('overlay-content').innerHTML = `<h2>${title}</h2>${content}`;
  if (!overlay.open) overlay.showModal();
}

function battleMenu() {
  if (resultsOpen) return;
  stopWalking();
  paused = Boolean(solo);
  showOverlay(solo ? 'Paused' : 'Match Menu', `${settingsMarkup()}
    <div class="actions">${button('Resume', 'close-overlay', '')}${button('Leave Match', 'exit-battle')}</div>`);
}

async function action(id) {
  unlockAudio();
  if (id === 'title') title();
  if (id === 'solo-setup') soloSetup();
  if (id === 'multiplayer') multiplayer();
  if (id === 'invite') joinByInvite();
  if (id === 'browse') await browse();
  if (id === 'settings') renderSettings();
  if (id === 'host') {
    if (desktop) hostSetup();
    else {
      message('To host online, download and launch Cat & Mouse Local Edition.', true);
      $('host-download').hidden = false;
    }
  }
  if (id === 'starter') { soloDraft.kit = [...STARTER_LOADOUT]; soloSetup(); }
  if (id === 'start-solo') await startSolo();
  if (id === 'open-invite') {
    try {
      const url = new URL($('invite-link').value.trim());
      if (!allowedJoin(url)) throw new Error();
      location.assign(url.href);
    } catch { message('Use the complete guest invitation from your host.', true); }
  }
  if (id === 'join-room') {
    const name = $('guest-name').value.trim() || browserName;
    write('cat-mouse-name-v1', name);
    $('guest-name').disabled = true;
    message('Contacting the match server…');
    if (!network) connect(location.origin, expectedRoom, { name, invite: inviteCode || undefined });
  }
  if (id === 'host-start') {
    draftRoom = { roomName: $('room-name').value.trim(), hostName: $('host-name').value.trim(), map: $('room-map').value,
      difficulty: $('room-difficulty').value, capacity: Number($('capacity').value), visibility: $('visibility').value };
    if (!draftRoom.roomName || !draftRoom.hostName) return message('Enter a room name and host name.', true);
    write('cat-mouse-name-v1', draftRoom.hostName);
    hostingStatus();
    try {
      host = await control('POST', draftRoom);
      hostPolling = true;
      showHostStatus();
      await refreshHost();
    } catch (error) { message(error.message, true); }
  }
  if (id === 'host-retry') {
    try { host = await control('POST', null, '/retry'); showHostStatus(); } catch (error) { message(error.message, true); }
  }
  if (id === 'host-stop') {
    try {
      hostPolling = false;
      clearTimeout(hostTimer);
      if (network) clearSeat(network);
      disconnect();
      host = await control('DELETE');
      multiplayer();
    } catch (error) { message(error.message, true); }
  }
  if (id === 'copy-invite' && host?.inviteUrl) {
    try { await navigator.clipboard.writeText(host.inviteUrl); message('Invitation copied.'); }
    catch { showOverlay('Guest Invitation', `<label>Copy this link<input value="${escape(host.inviteUrl)}" readonly></label><div class="actions">${button('Done', 'close-overlay', '')}</div>`); }
  }
  if (id === 'ready') {
    preserveStatus = false;
    const ready = lobby.players.find(entry => entry.id === playerId)?.ready;
    transmit({ type: 'ready', ready: !ready, loadout: lobbyKit });
  }
  if (id === 'normal-start') {
    preserveStatus = false;
    transmit({ type: 'start', force: false });
  }
  if (id === 'force-start') {
    const entrants = lobby.players.filter(entry => entry.connected).map(entry => `<li>${escape(entry.name)} · ${entry.ready ? 'Ready' : 'Joining with current choices and starter slots'}</li>`).join('');
    showOverlay('Force Start', `<p>These connected players will enter if they load the battlefield in time.</p><ul>${entrants}</ul>
      <div class="actions">${button('Confirm Force Start', 'confirm-force', '')}${button('Cancel', 'close-overlay')}</div>`);
  }
  if (id === 'confirm-force') {
    overlay.close();
    preserveStatus = false;
    transmit({ type: 'start', force: true });
  }
  if (id === 'leave-room' || id === 'exit-battle') {
    if (overlay.open) overlay.close();
    transmit({ type: 'leave' });
    if (network) clearSeat(network);
    disconnect();
    leaveBattle();
    if (desktop && host?.serverUrl) {
      hostPolling = false;
      try { host = await control('DELETE'); } catch { /* The launcher reports shutdown state on next open. */ }
      clearTimeout(hostTimer);
    }
    if (id === 'leave-room' && expectedRoom) joinPrompt(); else title();
  }
  if (id === 'close-overlay') overlay.close();
}

document.addEventListener('click', event => {
  const target = event.target.closest('button');
  if (!target || target.disabled) return;
  if (target.dataset.action) action(target.dataset.action).catch(error => message(error.message, true));
  if (target.dataset.build !== undefined) {
    selectSlot(Number(target.dataset.build), mobileLayout.matches || event.pointerType === 'touch');
  }
});

$('build-toggle').onclick = () => {
  if (!state || overlay.open) return;
  const open = $('build-toggle').getAttribute('aria-expanded') !== 'true';
  resetPlacement();
  setBuildTray(open);
};
$('build-cancel').onclick = finishPlacement;
$('build-place').onclick = () => {
  if (!touchAim || !aimTile || pendingBuild || overlay.open) return;
  const type = selectedTower();
  const { x, y } = aimTile;
  const error = getPlacementError(state, playerId, type, x, y);
  if (error) return reportCommandError(error);
  const command = { type: 'build', slot: selected, x, y };
  if (sendCommand(command)) {
    if (solo) finishPlacement();
    else pendingBuild = { x, y, type, deadline: performance.now() + 5000 };
    updatePlacement();
  }
};
mobileLayout.addEventListener('change', syncBattleLayout);

document.addEventListener('change', event => {
  const element = event.target;
  if (element.id === 'solo-difficulty') {
    soloDraft.difficulty = element.value;
    $('solo-difficulty-note').textContent = difficultyNote[element.value];
  }
  if (element.id === 'room-difficulty') $('room-difficulty-note').textContent = difficultyNote[element.value];
  if (element.dataset.slot !== undefined) {
    const kit = view === 'waiting' ? lobbyKit : soloDraft.kit;
    const updated = Number(element.dataset.slot);
    const previous = kit[updated];
    const duplicate = element.value ? kit.indexOf(element.value) : -1;
    if (duplicate >= 0 && duplicate !== updated) kit[duplicate] = previous;
    kit[updated] = element.value;
    if (view === 'waiting') { transmit({ type: 'ready', ready: false, loadout: kit }); updateLobby(); }
    else soloSetup();
    document.querySelector(`[data-slot="${updated}"]`)?.focus({ preventScroll: true });
  }
  if (!['volume', 'quality', 'reduced', 'follow', 'touch'].includes(element.id)) return;
  settings[element.id] = element.type === 'checkbox' ? element.checked : element.id === 'volume' ? Number(element.value) : element.value;
  if (element.id === 'follow') board.camera.follow = settings.follow;
  write('cat-mouse-settings-v1', settings);
});

const directionKeys = {
  w: [0, -1], arrowup: [0, -1], s: [0, 1], arrowdown: [0, 1],
  a: [-1, 0], arrowleft: [-1, 0], d: [1, 0], arrowright: [1, 0],
};

function walk() {
  const vectors = [...new Set([...keys].map(key => directionKeys[key]))];
  const right = Math.sign(vectors.reduce((sum, vector) => sum + vector[0], 0));
  const down = Math.sign(vectors.reduce((sum, vector) => sum + vector[1], 0));
  sendCommand({ type: 'move', dx: Math.sign(right + down), dy: Math.sign(down - right) });
}

function stopWalking() {
  if (!keys.size) return;
  keys.clear();
  walk();
}

window.addEventListener('keydown', event => {
  if (!state || overlay.open || event.target.matches('input,select,textarea')) return;
  const key = event.key.toLowerCase();
  if (Object.hasOwn(directionKeys, key)) {
    event.preventDefault();
    if (!keys.has(key)) { keys.add(key); walk(); }
  } else if (/^[1-8]$/.test(key)) {
    const slot = Number(key) - 1;
    if (slot !== selected || touchAim) selectSlot(slot, false);
  } else if (key === 'escape') {
    if (pendingBuild) return;
    if (selected !== null || $('build-toggle').getAttribute('aria-expanded') === 'true') finishPlacement();
    else battleMenu();
  }
});
window.addEventListener('keyup', event => { if (keys.delete(event.key.toLowerCase())) walk(); });
window.addEventListener('blur', stopWalking);
canvas.addEventListener('pointermove', event => {
  if (event.pointerType === 'touch') {
    if (fieldTouch?.id === event.pointerId && Math.hypot(event.clientX - fieldTouch.x, event.clientY - fieldTouch.y) > 12) fieldTouch = null;
    return;
  }
  if (!state || !board.tileX) return;
  const point = board.coordinates(event.offsetX, event.offsetY);
  cursor = { x: Math.round(point.x), y: Math.round(point.y) };
});
canvas.addEventListener('pointerleave', () => { cursor = null; });

function useBattlefield(event) {
  if (!state || !board.tileX || overlay.open || pendingBuild) return;
  unlockAudio();
  if ($('build-toggle').getAttribute('aria-expanded') === 'true') {
    setBuildTray(false);
    return;
  }
  const point = board.coordinates(event.offsetX, event.offsetY);
  const x = Math.round(point.x);
  const y = Math.round(point.y);
  placementMessage = '';
  if (selected !== null) {
    if (touchAim || event.pointerType === 'touch') {
      touchAim = true;
      aimTile = { x, y };
      updateSelected();
      return;
    }
    return sendCommand({ type: 'build', x, y, slot: selected });
  }
  const tower = state.structures.find(entry => entry.x === x && entry.y === y && entry.ownerId === playerId && !entry.branch && TOWERS[entry.type]?.upgrades);
  if (tower) {
    stopWalking();
    const choices = Object.entries(TOWERS[tower.type].upgrades).map(([branch, details]) => `<p>${escape(details.description)}</p>${button(`${escape(details.name)} · ${details.cost} cheese`, `upgrade/${x}/${y}/${branch}`, '')}`).join('');
    showOverlay('Upgrade ' + escape(TOWERS[tower.type].name), `${choices}<div class="actions">${button('Cancel', 'close-overlay')}</div>`);
  } else {
    keys.clear();
    sendCommand({ type: 'move', x, y });
  }
}

canvas.addEventListener('pointerdown', event => {
  if (event.button !== 0) return;
  if (event.pointerType !== 'touch') return useBattlefield(event);
  if (!settings.touch) return;
  fieldTouch = event.isPrimary ? { id: event.pointerId, x: event.clientX, y: event.clientY } : null;
});
canvas.addEventListener('pointerup', event => {
  if (event.pointerType !== 'touch' || fieldTouch?.id !== event.pointerId) return;
  const { x, y } = fieldTouch;
  fieldTouch = null;
  if (Math.hypot(event.clientX - x, event.clientY - y) <= 12) useBattlefield(event);
});
canvas.addEventListener('pointercancel', () => { fieldTouch = null; });
document.addEventListener('click', event => {
  const value = event.target.closest('button')?.dataset.action;
  if (!value?.startsWith('upgrade/')) return;
  const [, x, y, branch] = value.split('/');
  sendCommand({ type: 'upgrade', x: Number(x), y: Number(y), branch });
  overlay.close();
});
canvas.addEventListener('wheel', event => {
  event.preventDefault();
  board.scale(event.deltaY < 0 ? 1 : -1);
}, { passive: false });
$('zoom-in').onclick = () => board.scale(1);
$('zoom-out').onclick = () => board.scale(-1);
$('camera').onclick = () => { board.camera.follow = !board.camera.follow; };
$('minimap').onclick = event => {
  if (!state) return;
  board.pan(event.offsetX / $('minimap').clientWidth * state.size, event.offsetY / $('minimap').clientHeight * state.size);
};
$('battle-menu').onclick = battleMenu;
overlay.addEventListener('close', () => { paused = false; });
overlay.addEventListener('cancel', event => { if (resultsOpen) event.preventDefault(); });
window.addEventListener('pointerdown', unlockAudio, { once: true });

requestAnimationFrame(frame);
if (expectedRoom) {
  joinPrompt();
  if (read('cat-mouse-seat:' + location.origin + ':' + expectedRoom, null, sessionStorage)) $('guest-name') && action('join-room');
} else if (desktop) {
  title();
  control().then(status => {
    if (status.serverUrl && status.lobbyId) {
      host = status;
      hostPolling = true;
      hostingStatus();
      refreshHost();
    }
  }).catch(() => {});
} else title();
