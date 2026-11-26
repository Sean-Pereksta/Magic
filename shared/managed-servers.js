'use strict';
(() => {
  if (typeof window !== 'undefined' && window.CatnmiceServers) return;
  const titles = Object.freeze({
    acornrally: 'Acorn Rally', bannerlands: 'Bannerlands', trashtactics: 'Trash Tactics',
    cat: 'Cat & Mouse', lastsignal: 'Last Signal'
  });
  const projectId = 'magic-servers-1ca0c';
  const documents = 'projects/' + projectId + '/databases/(default)/documents';
  const firestore = 'https://firestore.googleapis.com/v1/' + documents;
  const phases = Object.freeze({ waiting: 'In lobby', playing: 'In match', racing: 'Racing', finished: 'Match finished' });
  const allowedOrigin = /^https:\/\/[a-z0-9]+(?:-[a-z0-9]+)*\.trycloudflare\.com$/;
  let active;

  async function getJson(url, options, timeout) {
    const signal = new AbortController();
    const timer = setTimeout(() => signal.abort(), timeout);
    try {
      const response = await fetch(url, {
        ...options, signal: signal.signal, credentials: 'omit', cache: 'no-store',
        redirect: 'error', referrerPolicy: 'no-referrer'
      });
      if (!response.ok) throw Object.assign(new Error('Request failed'), { status: response.status });
      return await response.json();
    } finally { clearTimeout(timer); }
  }

  function countsValid(game, players, capacity, queued) {
    const acorn = game === 'acornrally';
    return Number.isInteger(capacity) && capacity >= (acorn ? 2 : 1) && capacity <= (acorn ? 8 : 256) &&
      Number.isInteger(players) && players >= 0 && players <= capacity &&
      Number.isInteger(queued) && queued >= 0 && queued <= 1000;
  }

  function collection(game) {
    if (!Object.hasOwn(titles, game)) throw Error('Unknown game');
    return game === 'acornrally' ? '/acornRallyLobbies' : '/managedGameDirectories/' + game + '/lobbies';
  }

  function phaseValid(game, phase) {
    return phase === 'waiting' || phase === 'finished' || phase === (game === 'acornrally' ? 'racing' : 'playing');
  }

  function parseListing(game, doc) {
    const prefix = documents + collection(game) + '/';
    if (!doc || typeof doc.name !== 'string' || !doc.name.startsWith(prefix)) return null;
    const code = doc.name.slice(prefix.length), f = doc.fields;
    if (!/^\d{4}$/.test(code) || !f) return null;
    const name = f.name?.stringValue, visibility = f.visibility?.stringValue, origin = f.origin?.stringValue;
    const acorn = game === 'acornrally';
    const url = f.joinUrl?.stringValue, phase = f.phase?.stringValue;
    const hostId = acorn ? f.ownerUid?.stringValue : f.hostId?.stringValue;
    const expires = Date.parse(f.expiresAt?.timestampValue), updated = Date.parse(f.updatedAt?.timestampValue);
    const now = Date.now();
    const players = Number(f.players?.integerValue), capacity = Number(f.capacity?.integerValue), queued = Number(f.queued?.integerValue);
    if (typeof name !== 'string' || !name.trim() || name.length > 40 ||
        !['code', 'public'].includes(visibility) || typeof origin !== 'string' || !allowedOrigin.test(origin) ||
        url !== origin + (acorn ? '/?room=join' : '/__catnmice/join') ||
        !hostId || typeof hostId !== 'string' || hostId.length > 128 ||
        !phaseValid(game, phase) || !countsValid(game, players, capacity, queued) ||
        !Number.isFinite(updated) || !Number.isFinite(expires) || expires <= now || updated > now + 30000 ||
        now - updated > (acorn ? 120000 : 180000) || expires <= updated || expires > now + 180000) return null;
    return { game, name, visibility, origin, url, hostId, code, phase, players, capacity, queued, expires,
      joinable: acorn ? queued < 128 : f.joinable?.booleanValue === true };
  }

  function rank(a, b) {
    return Number(b.joinable) - Number(a.joinable) || Number(b.phase === 'waiting') - Number(a.phase === 'waiting') ||
      (b.capacity - b.players - b.queued) - (a.capacity - a.players - a.queued) ||
      a.queued - b.queued || a.name.localeCompare(b.name) || a.code.localeCompare(b.code);
  }

  async function publicServers(game) {
    const listing = collection(game);
    const parent = listing.slice(0, listing.lastIndexOf('/'));
    const collectionId = listing.slice(listing.lastIndexOf('/') + 1);
    let cursor;
    const result = [], deadline = Date.now() + 10000;
    do {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw Error('Directory timed out');
      const query = { from: [{ collectionId }], where: { fieldFilter: {
        field: { fieldPath: 'visibility' }, op: 'EQUAL', value: { stringValue: 'public' }
      } }, orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }], limit: 25 };
      if (cursor) query.startAt = { values: [{ referenceValue: cursor }], before: false };
      const rows = await getJson(firestore + parent + ':runQuery', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ structuredQuery: query })
      }, remaining);
      if (!Array.isArray(rows)) throw Error('Invalid directory response');
      const docs = rows.map(row => row.document).filter(Boolean);
      for (const doc of docs) {
        const server = parseListing(game, doc);
        if (server?.visibility === 'public') result.push(server);
      }
      if (docs.length < 25) return result.sort(rank);
      const last = docs[docs.length - 1].name;
      if (typeof last !== 'string' || (cursor && last <= cursor)) throw Error('Invalid directory page');
      cursor = last;
    } while (true);
  }

  async function byCode(game, code) {
    if (typeof code !== 'string' || !/^\d{4}$/.test(code)) return null;
    try {
      const doc = await getJson(firestore + collection(game) + '/' + code, {}, 5000);
      const server = parseListing(game, doc);
      return server?.code === code ? server : null;
    } catch (error) {
      if (error.status === 403 || error.status === 404) return null;
      throw error;
    }
  }

  async function probe(server, timeout = 3500) {
    if (server.expires <= Date.now()) return null;
    const acorn = server.game === 'acornrally';
    try {
      const live = await getJson(server.origin + (acorn ? '/api/rooms/status' : '/__catnmice/status'), {}, timeout);
      const queued = acorn ? live.waiting : live.queued;
      if (live.visibility !== server.visibility || !phaseValid(server.game, live.phase) ||
          !countsValid(server.game, live.players, live.capacity, queued)) return null;
      if (acorn) {
        if (live.game !== 'acorn-rally' || live.queueEnabled !== true || queued >= 128) return null;
      } else if (live.game !== server.game || live.hostId !== server.hostId ||
          live.joinable !== true || live.players >= live.capacity) return null;
      if (server.expires <= Date.now()) return null;
      return { ...server, phase: live.phase, players: live.players, capacity: live.capacity, queued, joinable: true };
    } catch { return null; }
  }

  function joinUrl(server, name = '') {
    const dest = new URL(server.url);
    const displayName = name.replace(/[\x00-\x1f\x7f]/g, '').trim().slice(0, 22);
    const handoff = new URLSearchParams();
    if (server.game === 'acornrally') {
      handoff.set('join', '1');
      if (displayName) handoff.set('name', displayName);
    } else if (displayName) {
      dest.searchParams.set('name', displayName);
    }
    if (server.visibility === 'code') handoff.set('code', server.code);
    dest.hash = handoff.toString();
    return dest.href;
  }

  function summary(server) {
    const players = server.players + '/' + server.capacity + ' players';
    return phases[server.phase] + ' · ' + players + (server.queued ? ' · ' + server.queued + ' queued' : '');
  }

  async function best(servers, current) {
    const deadline = Date.now() + 9000, verified = [];
    for (let i = 0; i < servers.length && current() && Date.now() < deadline; i += 6) {
      const wait = Math.min(2400, Math.max(1, deadline - Date.now()));
      const batch = await Promise.all(servers.slice(i, i + 6).map(server => probe(server, wait)));
      verified.push(...batch.filter(Boolean));
      verified.sort(rank);
      if (verified[0]?.phase === 'waiting') break;
    }
    return verified[0];
  }

  function open(game, { live = false, onJoin } = {}) {
    if (!Object.hasOwn(titles, game)) throw Error('Unknown game');
    active?.close();
    const dialog = document.createElement('dialog');
    dialog.setAttribute('aria-label', titles[game] + ' multiplayer servers');
    // Isolate the picker from individual games' button, canvas and menu styles.
    const container = document.createElement('div');
    dialog.append(container);
    const root = container.attachShadow ? container.attachShadow({ mode: 'open' }) : container;
    dialog.style.cssText = 'position:fixed;inset:0;margin:auto;width:min(31rem,calc(100% - 2rem));max-height:85vh;box-sizing:border-box;overflow:auto;color:#f5f3ed;background:#17212b;border:1px solid #627070;border-radius:12px;padding:0;z-index:2147483647;';
    root.innerHTML = '<style>*{box-sizing:border-box}section{padding:20px;color:#f5f3ed;font:16px/1.45 system-ui,sans-serif}h2,h3,p{margin:0}h2{font-size:1.35rem}h3{font-size:1rem}header,.tools,li,form{display:flex;align-items:center;gap:10px}header,.tools,li{justify-content:space-between}.tools,form,p{margin-top:16px}button,input{font:inherit;color:inherit;background:#253542;border:1px solid #86928f;border-radius:6px;min-height:40px;padding:5px 12px}button{cursor:pointer}button:focus-visible,input:focus-visible{outline:3px solid #f5cb73}button.live{background:#f1be64;color:#202124;font-weight:650}ul{list-style:none;padding:0;margin:8px 0;max-height:35vh;overflow:auto}li{border-bottom:1px solid #46545d;padding:8px 0}li small{display:block;color:#c9d5d1}li strong{overflow-wrap:anywhere}form{flex-wrap:wrap}label{display:grid;gap:4px}input{width:7em}p:empty{display:none}</style>' +
      '<section><header><h2></h2><button type="button" data-close aria-label="Close server browser">Close</button></header>' +
      '<label style="margin-top:12px">Player name (optional)<input name="player" maxlength="22" autocomplete="nickname" style="width:min(100%,18rem)"></label>' +
      '<div class="tools"><button class="live" type="button" data-live>Live Join</button><button type="button" data-refresh>Refresh</button></div>' +
      '<div class="tools"><h3>Public servers</h3></div><ul></ul><form><label>Private server code<input name="code" maxlength="4" minlength="4" pattern="[0-9]{4}" inputmode="numeric" autocomplete="off" placeholder="0000" required></label>' +
      '<button type="submit">Join by code</button></form><p role="status" aria-live="polite"></p></section>';
    const el = selector => root.querySelector(selector);
    el('h2').textContent = titles[game] + ' servers';
    const player = el('input[name=player]');
    try { player.value = (sessionStorage.getItem('catnmice-multiplayer-name') || '').slice(0, 22); } catch {}
    player.oninput = () => {
      try { sessionStorage.setItem('catnmice-multiplayer-name', player.value.slice(0, 22)); } catch {}
    };
    (document.fullscreenElement || document.body).append(dialog);
    const opener = document.activeElement;
    let request = 0;
    function close() {
      ++request;
      if (dialog.open && dialog.close) dialog.close();
      dialog.remove();
      if (active?.close === close) active = null;
      if (opener?.isConnected) opener.focus?.();
    }
    active = { close };
    if (dialog.showModal) dialog.showModal();
    else dialog.setAttribute('open', '');
    dialog.addEventListener('cancel', event => { event.preventDefault(); close(); });
    dialog.addEventListener('keydown', event => event.stopPropagation());
    dialog.addEventListener('keyup', event => event.stopPropagation());
    el('[data-close]').onclick = close;
    const current = id => id === request && dialog.isConnected;
    const feedback = message => { el('[role="status"]').textContent = message; };
    async function enter(server, id) {
      if (!current(id)) return;
      const destination = joinUrl(server, player.value);
      if (onJoin) { await onJoin({ ...server, url: destination }); if (current(id)) close(); }
      else window.location.assign(destination);
    }
    async function join(server, id = ++request) {
      feedback('Checking ' + server.name + '…');
      const available = await probe(server, 3500);
      if (!current(id)) return;
      if (available) await enter(available, id);
      else feedback('This server has no open seat or has gone offline. Try another server.');
    }
    function draw(servers) {
      const list = el('ul');
      list.replaceChildren();
      for (const server of servers) {
        const row = document.createElement('li'), info = document.createElement('div');
        const name = document.createElement('strong'), detail = document.createElement('small');
        name.textContent = server.name;
        detail.textContent = summary(server);
        const button = document.createElement('button');
        button.type = 'button'; button.textContent = server.joinable ? 'Join' : 'Check for seat';
        button.setAttribute('aria-label', button.textContent + ' · ' + server.name);
        button.onclick = () => { void join(server).catch(() => feedback('Could not open this server.')); };
        info.append(name, detail); row.append(info, button); list.append(row);
      }
    }
    async function load(autoJoin) {
      const id = ++request;
      feedback(autoJoin ? 'Looking for an open seat…' : 'Loading public servers…');
      try {
        const servers = await publicServers(game);
        if (!current(id)) return;
        draw(servers);
        if (!servers.length) { feedback('No public servers are online.'); return; }
        if (!autoJoin) { feedback(''); return; }
        const available = await best(servers, () => current(id));
        if (!current(id)) return;
        if (available) await enter(available, id);
        else feedback('No public server is accepting players right now.');
      } catch { if (current(id)) feedback('Could not load public servers. Try Refresh.'); }
    }
    el('[data-refresh]').onclick = () => void load(false);
    el('[data-live]').onclick = () => void load(true);
    el('form').onsubmit = async event => {
      event.preventDefault();
      const code = el('input[name=code]').value.trim();
      if (!/^\d{4}$/.test(code)) { feedback('Enter a four-digit code.'); return; }
      const id = ++request;
      feedback('Looking up the server…');
      try {
        const server = await byCode(game, code);
        if (!current(id)) return;
        if (server) await join(server, id);
        else feedback('No active server has that code.');
      } catch {
        if (current(id)) feedback('Could not look up that code.');
      }
    };
    void load(live);
    return { close };
  }
  const api = Object.freeze({ projectId, titles, publicServers, byCode, probe, joinUrl, summary, open });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else window.CatnmiceServers = api;
})();
