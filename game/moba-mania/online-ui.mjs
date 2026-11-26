import { HEROES } from './data.mjs';
import { drawPortrait } from './renderer.mjs';

const $ = id => document.getElementById(id);
const teams = ['Azure', 'Rose'];
const roleOrder = ['Tank', 'Damage', 'Support', 'Control'];
const query = new URLSearchParams(location.search);
const hosted = query.get('hosted') === '1';
const sessionKey = 'moba-mania.online-seat';
const launchName = query.get('name');

function readSession() {
  try { return JSON.parse(sessionStorage.getItem(sessionKey) || 'null'); } catch { return null; }
}

function writeSession(value) {
  try {
    if (value) sessionStorage.setItem(sessionKey, JSON.stringify(value));
    else sessionStorage.removeItem(sessionKey);
  } catch { /* Joining works without storage. */ }
}

async function openDirectory(live) {
  const status = $('online-menu-status');
  status.textContent = 'Opening server browser…';
  try {
    if (!globalThis.CatnmiceServers) {
      const path = /\/game\/moba-mania\//.test(new URL(import.meta.url).pathname)
        ? '../../shared/managed-servers.js?v=magic-servers-2'
        : './managed-servers.js';
      await import(path);
    }
    globalThis.CatnmiceServers.open('mobamania', { live });
    status.textContent = '';
  } catch {
    status.textContent = 'Could not open the server browser. Try again.';
  }
}

export function createOnlineLobby({ getHero, onSnapshot, onLobby, onStatus, onExit }) {
  const dialog = $('online-dialog'), name = $('online-name'), pick = $('online-hero');
  const room = $('online-room'), picks = $('online-picks'), specialty = $('online-specialty');
  const seats = $('online-teams'), feedback = $('online-feedback'), retry = $('online-retry'), start = $('online-start');
  let socket, state, identity, connectTimer, retryTimer, retries = 0, displayed = false, ready = false;
  let draftHero;

  for (const role of roleOrder) {
    const group = document.createElement('optgroup');
    group.label = role;
    for (const hero of Object.values(HEROES).filter(hero => hero.specialty?.label === role)) {
      const option = document.createElement('option');
      option.value = hero.id;
      option.textContent = hero.name;
      group.append(option);
    }
    if (group.children.length) pick.append(group);
  }

  const player = () => state?.players.find(entry => entry.playerId === identity?.playerId);
  function send(message) {
    if (socket?.readyState !== WebSocket.OPEN) return false;
    socket.send(JSON.stringify(message));
    return true;
  }
  function message(text) { feedback.textContent = text; }
  function show() {
    if (!dialog.open) dialog.showModal();
  }
  function chooseSeat(team, slot) {
    if (player()) {
      send({ type: 'select', team, slot, hero: draftHero });
    } else {
      send({ type: 'join', name: name.value.trim(), team, slot, hero: draftHero });
    }
  }

  function render() {
    const me = player();
    const waiting = state?.phase === 'waiting';
    const live = state?.phase === 'playing';
    room.textContent = state
      ? state.name + ' · ' + state.players.filter(entry => entry.connected).length + '/6 players · ' + (waiting ? 'Lobby' : live ? 'In match' : 'Match finished')
      : 'Connecting to the host…';
    picks.hidden = !state || (!waiting && !live);
    name.disabled = Boolean(me);
    pick.disabled = !waiting;
    pick.value = me?.hero || draftHero;
    const hero = HEROES[pick.value];
    specialty.hidden = !waiting || !hero?.specialty;
    if (!specialty.hidden) {
      specialty.textContent = [hero.role, hero.specialty.description, hero.specialty.effect].filter(Boolean).join(' · ');
      specialty.style.setProperty('--role-color', hero.specialty.color);
    }
    const focusedSeat = document.activeElement?.dataset.seat;
    seats.replaceChildren();
    for (const [team, roster] of (state?.teams || []).entries()) {
      const panel = document.createElement('section'), heading = document.createElement('h3');
      panel.className = 'online-team';
      panel.dataset.team = String(team);
      heading.textContent = teams[team];
      panel.append(heading);
      for (const seat of roster) {
        const row = document.createElement('div'), info = document.createElement('div');
        const title = document.createElement('strong'), detail = document.createElement('small');
        const occupant = HEROES[seat.hero];
        const isMe = Boolean(identity && seat.playerId === identity.playerId);
        row.className = 'online-seat';
        if (occupant) {
          const portrait = document.createElement('canvas');
          portrait.setAttribute('aria-hidden', 'true');
          drawPortrait(portrait, occupant.id, 34);
          row.append(portrait);
        }
        title.textContent = isMe ? 'You' : seat.bot ? (live ? 'Bot' : 'Bot on start') : seat.name;
        detail.textContent = occupant ? occupant.name + ' · ' + occupant.role : seat.bot ? 'Open seat' : 'Choosing a hero';
        info.append(title, detail);
        row.append(info);
        if (seat.bot && !seat.playerId && (waiting || (live && !me))) {
          const button = document.createElement('button');
          button.type = 'button';
          button.textContent = me ? 'Switch' : live ? 'Take over' : 'Join';
          button.dataset.seat = team + '-' + seat.slot;
          button.setAttribute('aria-label', button.textContent + ' ' + teams[team] + ' seat ' + (seat.slot + 1));
          button.onclick = () => chooseSeat(team, seat.slot);
          row.append(button);
        }
        panel.append(row);
      }
      seats.append(panel);
    }
    if (focusedSeat) seats.querySelector('[data-seat="' + focusedSeat + '"]')?.focus({ preventScroll:true });
    start.hidden = !waiting || !me || state.hostId !== me.playerId;
    if (waiting) {
      if (me) message(start.hidden ? 'Waiting for the host to start. You can change your hero before the match.' : 'Start whenever you’re ready. Bots will take the remaining seats.');
      else message(state.joinable ? 'Pick a hero and an open seat.' : 'All six seats are occupied.');
    } else if (live) {
      if (me) message('Entering the match…');
      else message(state.joinable ? 'Take over a bot to join with its hero and progress.' : 'All six seats are occupied.');
    } else if (state) {
      message('The next lobby will open shortly.');
    }
  }

  function connect() {
    clearTimeout(retryTimer);
    clearTimeout(connectTimer);
    retry.hidden = true;
    ready = false;
    if (socket) socket.close();
    const endpoint = new URL('/api/moba/online', location.href);
    endpoint.protocol = endpoint.protocol === 'https:' ? 'wss:' : 'ws:';
    const next = new WebSocket(endpoint);
    socket = next;
    connectTimer = setTimeout(() => next.close(), 7000);
    next.onopen = () => {
      if (socket !== next) return;
      if (identity) send({ type:'join', playerId:identity.playerId, token:identity.token, name:name.value });
    };
    next.onmessage = event => {
      if (socket !== next) return;
      let packet;
      try { packet = JSON.parse(event.data); } catch { return; }
      switch (packet.type) {
        case 'lobby':
          clearTimeout(connectTimer);
          state = packet.state;
          if (state.phase !== 'playing' || !player()) {
            if (onLobby(state) !== false) show();
            render();
          }
          break;
        case 'welcome':
          clearTimeout(connectTimer);
          identity = { playerId:packet.playerId, token:packet.token };
          writeSession(identity);
          ready = true;
          retries = 0;
          onStatus('');
          break;
        case 'snapshot':
          if (!ready) break;
          if (dialog.open) dialog.close();
          onSnapshot(packet.state);
          break;
        case 'error':
          if (packet.code === 'resume-invalid' || (packet.code === 'round-ended' && identity)) {
            identity = null;
            writeSession(null);
            onExit();
            show();
            render();
          }
          if (dialog.open) message(packet.message);
          else onStatus(packet.message, true);
          break;
      }
    };
    next.onclose = event => {
      if (socket !== next) return;
      clearTimeout(connectTimer);
      ready = false;
      if (event.code === 4001) {
        identity = null;
        writeSession(null);
        onExit();
        show();
        message('This seat was resumed in another tab. Reconnect to choose an open seat.');
        retry.hidden = false;
        return;
      }
      const reconnecting = identity && retries < 4;
      if (dialog.open) {
        seats.replaceChildren();
        start.hidden = true;
        message(reconnecting ? 'Connection lost. Reconnecting…' : 'Could not reach the host.');
        retry.hidden = Boolean(reconnecting);
      } else {
        onStatus(reconnecting ? 'Reconnecting to the host…' : 'Connection lost. Open match options to reconnect.');
      }
      if (reconnecting) retryTimer = setTimeout(() => { retries++; connect(); }, 1200);
    };
  }

  function open() {
    displayed = true;
    if (identity === undefined) identity = readSession();
    draftHero = getHero();
    state = null;
    seats.replaceChildren();
    room.textContent = 'Connecting to the host…';
    picks.hidden = true;
    specialty.hidden = true;
    start.hidden = true;
    message('');
    show();
    connect();
  }
  function leave() {
    send({ type:'leave' });
    const old = socket;
    socket = undefined;
    old?.close();
    clearTimeout(connectTimer);
    clearTimeout(retryTimer);
    identity = null;
    ready = false;
    retries = 0;
    state = null;
    writeSession(null);
    displayed = false;
    if (dialog.open) dialog.close();
    onExit();
  }

  name.value = launchName || '';
  $('online-lobbies').onclick = () => hosted ? open() : void openDirectory(false);
  $('online-live').onclick = () => hosted ? open() : void openDirectory(true);
  $('online-close').onclick = leave;
  dialog.addEventListener('cancel', event => { event.preventDefault(); leave(); });
  name.addEventListener('change', () => { name.value = name.value.trim().slice(0, 22); });
  pick.addEventListener('change', () => {
    draftHero = pick.value;
    const me = player();
    if (me && draftHero !== me.hero) send({ type:'select', team:me.team, hero:draftHero });
    else render();
  });
  retry.onclick = () => { retries = 0; connect(); };
  start.onclick = () => send({ type:'start' });

  return {
    launch() {
      if (hosted && query.get('online') === '1') open();
      else if (query.get('online') === '1') void openDirectory(query.get('live') === '1');
    },
    command(action) { return ready && send({ type:'command', action }); },
    get connected() { return ready; },
    returnToLobby() { if (displayed) { show(); render(); } },
    leave,
    reconnect() { retries = 0; connect(); }
  };
}
