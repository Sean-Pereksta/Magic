'use strict';

const $ = (selector) => document.querySelector(selector);
const host = window.hub;
const sections = {Library:'library','Live Lobbies':'lobbies',Rankings:'rankings',Downloads:'downloads',Settings:'settings'};
const spotlights = ['bannerlands','trashtactics','cat'];
const shortTitles = {bannerlands:'Bannerlands'};
const filters = ['All games','Installed','Offline Ready','Multiplayer','Favorites'];
const rankingIds = ['trashtactics','bannerlands','cat'];
const localLobbyGames = ['trashtactics','bannerlands','cat','lastsignal'];
const view = $('#view');
const dialog = $('#dialog');
let state = {catalog:{games:[]},settings:{}};
let page = 'Library', detailId = '', filter = 'All games', query = '', feature = 0, rankGame = 'trashtactics';
let sequence = 0, rankSequence = 0, statusTimeout, snapshotKey = '';
const inProgress = new Set();
const matches = (id) => games().find(game => game.id === id);
const games = () => Array.isArray(state.catalog?.games) ? state.catalog.games : [];
const esc = (value) => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const attr = esc;
const fmt = (value) => Number(value).toLocaleString();
const title = (game) => shortTitles[game.id] || game.title;
const ids = (list) => Array.isArray(list) ? list.map(item => typeof item === 'string' ? item : item.id || item.key || item.gameId) : [];
const installed = (id) => ids(state.installed).includes(id);
const favorite = (id) => ids(state.favorites).includes(id);
const guest = () => !state.profile || state.profile.guest === true || state.profile.isGuest === true || state.profile.id === 'guest' || String(state.profile.id || '').startsWith('guest:');
const multiplayer = (game) => (game.modes || []).includes('multi') || (game.actions || []).some(a => ['lobby','live','online'].includes(a.type));
const packs = () => games().filter(game => game.offline);
const supported = (g) => g.offline && (installed(g.id) || g.offline.available !== false);
const playsLocally = (game) => localLobbyGames.includes(game.id) || game.offline?.type === 'native-windows';
const normalize = (value) => String(value || '').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase();

function mediaPath(path) {
  if (typeof path !== 'string' || !/^[a-z0-9/_\-. %()]+$/i.test(path) || path.includes('..') || path.startsWith('/') || path.includes('%')) return '';
  return '../content/' + path;
}
function imageFor(game, size = 'card') {
  if (spotlights.includes(game.id)) return 'art/' + game.id + '-' + size + '.webp';
  return mediaPath(size === 'hero' ? game.hero || game.art : game.art || game.hero);
}
function accentFor(id) {
  const features = {bannerlands:0,trashtactics:2,cat:4};
  return features[id] ?? Array.from(id).reduce((n,c)=>(n*31+c.charCodeAt(0))%12,0);
}
function btn(label, action, data = {}, style = '', symbol = '') {
  const attrs = Object.entries(data).map(([name,value]) => 'data-' + name + '="' + attr(value) + '"').join(' ');
  return `<button type="button" class="button ${style}" data-action="${action}" ${attrs}>${symbol ? icon(symbol) : ''}${esc(label)}</button>`;
}
function titleLine(label, count) {
  return `<h2>${esc(label)}${count === undefined ? '' : '<span class="total-tag">' + fmt(count) + '</span>'}</h2>`;
}
function message(text) {
  const status = $('#status');
  status.textContent = text;
  status.classList.toggle('active', Boolean(text));
  clearTimeout(statusTimeout);
  if (text) statusTimeout = setTimeout(() => {status.classList.remove('active');},6500);
}
function safeAvatar(value) {
  if (!value || typeof value !== 'string') return '';
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || /^data:image\/(?:png|jpeg|webp|gif);base64,/i.test(value) ? url.href : '';
  } catch {return '';}
}
function profileName(profile) {return profile?.name || profile?.displayName || profile?.username || 'Guest';}
function chrome() {
  $('#navigation').innerHTML = Object.entries(sections).map(([name,symbol]) =>
    `<button class="nav-link" data-go="${name}" aria-label="${name}" title="${name}" ${name === page ? 'aria-current="page"' : ''}>${icon(symbol)}<span>${name}</span></button>`).join('');
  $('#page-title').textContent = page;
  document.title = (detailId && matches(detailId) ? title(matches(detailId)) : page) + ' · Catnmice Game Hub';
  const online = typeof state.online === 'boolean' ? state.online : navigator.onLine;
  const connection = $('#connection');
  connection.dataset.status = online ? 'online' : 'offline';
  connection.textContent = online ? 'Online' : 'Offline';
  $('#username').textContent = profileName(state.profile);
  $('#profile-type').textContent = guest() ? 'Guest profile' : 'Catnmice profile';
  $('#rail-login').hidden = !guest();
  const avatar = safeAvatar(state.profile?.avatar || state.profile?.photoURL);
  $('#rail-avatar').innerHTML = avatar ? `<img alt="" src="${attr(avatar)}">` : guest() ? icon('user') : esc(profileName(state.profile).slice(0,1).toUpperCase());
  document.body.classList.toggle('reduced', Boolean(state.settings?.reducedMotion));
  $('#main').setAttribute('aria-label',page);
}
async function refresh(redraw = true) {
  const request = ++sequence;
  let next;
  if (host) next = await host.invoke('state',{});
  else {
    const response = await fetch('../content/catalog.json');
    if (!response.ok) throw new Error('The bundled catalog could not be loaded.');
    next = {catalog:await response.json(), settings:{}, online:navigator.onLine};
  }
  if (request !== sequence) return;
  const key = JSON.stringify(next);
  if (key === snapshotKey) return;
  snapshotKey = key;
  state = {...next,catalog:next.catalog || {games:[]},settings:next.settings || {}};
  if (redraw) render();
  else chrome();
}
function navigate(to,id = '') {
  if (!(to in sections)) return;
  page = to; detailId = id; query = ''; $('#search').value = '';
  if (dialog.open) dialog.close();
  render();window.scrollTo(0,0);$('#main').focus({preventScroll:true});
}
function onlinePlay(game) {
  const actions = game.actions || [];
  return actions.find(a => a.type !== 'single') || actions[0] || {};
}
function hostLobby(game) {
  return playsLocally(game) || game.hostRoute ? btn('Host Lobby','play',{id:game.id,mode:'host'},'glass','lobbies') : '';
}
function launchButton(game, compact = false) {
  if (playsLocally(game)) return btn('Play','play',{id:game.id,mode:'offline'},'gold','play');
  const offline = state.online === false && installed(game.id);
  const mode = offline ? 'offline' : 'online';
  const action = (game.actions || []).find(a => a.type === 'single') || onlinePlay(game);
  return btn(compact ? 'Play' : offline ? 'Play Offline' : 'Play', 'play', {id:game.id,mode,kind:action.type || ''}, 'gold','play');
}
function hero(game, isDetail = false) {
  const rendered = spotlights.filter(id => matches(id));
  const subtitle = game.id === 'bannerlands' ? '<p class="hero-subtitle">The War of Crowns</p>' : '';
  const name = title(game);
  return `<article class="hero tone-${accentFor(game.id)}">
    <img class="hero-art" src="${attr(imageFor(game,'hero'))}" alt="">
    <div class="hero-body"><p class="eyebrow">${icon(isDetail ? 'gamepad' : 'flag')}${isDetail ? esc(game.category || 'Game') : 'Featured game'}</p>
    <h2 class="hero-title">${esc(name)}</h2>${subtitle}<p class="hero-desc">${esc(game.description)}</p>
    <div class="controls">${isDetail ? gameControls(game) : launchButton(game) + btn('Game details','detail',{id:game.id},'glass','info')}</div></div>
    ${!isDetail && rendered.length > 1 ? `<div class="hero-nav" aria-label="Featured games"><div class="dots">${rendered.map((id,i) => `<button class="dot" data-action="slide" data-index="${i}" aria-label="Feature ${attr(title(matches(id)))}" aria-pressed="${id===game.id}"></button>`).join('')}</div><button class="icon-button" data-action="previous" aria-label="Previous featured game">${icon('left')}</button><button class="icon-button" data-action="next" aria-label="Next featured game">${icon('right')}</button></div>` : ''}
  </article>`;
}
function gameControls(game) {
  if (playsLocally(game)) {
    const main = installed(game.id) ? launchButton(game) : btn('Install game','install',{id:game.id},'gold','downloads');
    return main + btn('Multiplayer','play',{id:game.id,mode:'online'},'glass','play') + hostLobby(game);
  }
  const out = [];
  const actions = game.actions || [];
  if (supported(game)) out.push(installed(game.id) ? btn('Play Offline','play',{id:game.id,mode:'offline'},'gold','disc') : btn('Install offline pack','install',{id:game.id},'gold','downloads'));
  const onlineAction = onlinePlay(game);
  if (onlineAction.type) {
    const label = (game.offline || onlineAction.type !== 'single') ? 'Play Online' : 'Play';
    out.push(btn(label,'play',{id:game.id,mode:'online',kind:onlineAction.type},out.length ? 'glass':'gold','play'));
    for (const action of actions) {
      if (action.type === onlineAction.type) continue;
      const label = action.type === 'single' ? 'Play solo online' : action.label;
      out.push(btn(label,'play',{id:game.id,mode:'online',kind:action.type},'glass'));
    }
  } else out.push(launchButton(game));
  out.push(hostLobby(game));
  if (state.online && ids(state.saveSyncGames).includes(game.id)) out.push(btn('Sync game saves','play',{id:game.id,mode:'save-sync'},'glass','disc'));
  return out.join('');
}
function card(game) {
  const subtitle = game.category || 'Game';
  const mode = installed(game.id) ? 'Installed' : game.offline ? 'Offline Ready' : 'Online only';
  return `<article class="game-card tone-${accentFor(game.id)}"><button class="art-button" data-action="detail" data-id="${attr(game.id)}" aria-label="${attr(title(game))} details"><img loading="lazy" src="${attr(imageFor(game))}" alt=""><span class="card-body"><span class="card-title">${esc(title(game))}</span><span class="card-meta"><span class="category">${esc(subtitle)}</span><span class="separator">·</span>${mode}</span></span></button><button class="card-play" data-action="quickplay" data-id="${attr(game.id)}" title="Play ${attr(title(game))}" aria-label="Play ${attr(title(game))}">${icon('play')}</button></article>`;
}
function continueRow() {
  const recent = [...new Set(ids(state.recent))].map(matches).filter(Boolean).slice(0,4);
  if (!recent.length) return `<section class="continue-empty"><span class="clock-icon">${icon('clock')}</span><div><h2>Continue Playing</h2><p>Your recent games will appear here.</p></div><button class="text-button" data-action="browse">Browse games ${icon('arrow')}</button></section>`;
  return `<section class="section"><div class="section-title">${titleLine('Continue Playing')}</div><div class="recent-grid">${recent.map(g => `<button class="recent-item" data-action="replay" data-id="${attr(g.id)}"><img src="${attr(imageFor(g))}" alt=""><strong>${esc(title(g))}</strong>${icon('play')}</button>`).join('')}</div></section>`;
}
function filteredGames() {
  const index = (g) => {
    const featureIndex = spotlights.indexOf(g.id);
    return featureIndex >= 0 ? featureIndex : g.featured || ['flagship','showcase'].includes(g.tier) ? 4 : 5;
  };
  return games().filter(game => {
    const valid = filter === 'All games' || (filter === 'Installed' && installed(game.id)) || (filter === 'Offline Ready' && supported(game)) || (filter === 'Multiplayer' && multiplayer(game)) || (filter === 'Favorites' && favorite(game.id));
    return valid && normalize([game.title, game.description,game.category,game.id,game.tier].join(' ')).includes(normalize(query.trim()));
  }).sort((a,b) => index(a)-index(b) || a.title.localeCompare(b.title));
}
function lifetime(record, asOf, scope) {
  const number = typeof record === 'number' ? record : record?.total;
  const available = number !== undefined && number !== null && Number.isFinite(Number(number));
  const synced = asOf || record?.asOf;
  const date = synced && new Date(synced);
  let meta = available ? `<strong>${fmt(number)}</strong> lifetime players${scope ? ' '+esc(scope) : ''}` : 'Lifetime players · Awaiting first sync';
  if (available && record && typeof record === 'object' && Number.isFinite(record.guests)) meta += ` · ${fmt(record.guests)} estimated guests`;
  if (available && date && !isNaN(date)) meta += ` · Synced ${esc(date.toLocaleDateString())}`;
  return `<p class="count-copy">${meta}</p>`;
}
function listing() {
  const list = filteredGames();
  return `<section class="section" id="library-list"><div class="section-title">${titleLine(query ? 'Search results' : 'All games',list.length)}
    <div class="tools" aria-label="Filter games">${filters.map(label => `<button class="chip" data-action="filter" data-value="${label}" aria-pressed="${filter === label}">${label}</button>`).join('')}</div></div>
    <div class="grid">${list.length ? list.map(card).join('') : `<div class="gallery-empty"><p>${query ? 'No games match your search.' : 'No games match this filter.'}</p><button class="text-button" data-action="reset">Show all games ${icon('arrow')}</button></div>`}</div>
    ${lifetime(state.counts?.hub,state.counts?.asOf,'across the hub')}</section>`;
}
function library() {
  if (!games().length) return `<div class="notice"><p>The game catalog is unavailable.</p>${btn('Try again','reload',{},'','refresh')}</div>`;
  const featured = spotlights.map(matches).filter(Boolean);
  const game = featured[feature % (featured.length || 1)] || games()[0];
  return (query ? '' : hero(game) + continueRow()) + listing();
}
function details(game) {
  const shots = (game.screenshots || []).map(item => typeof item === 'string' ? {path:item} : item).filter(item => mediaPath(item.path || item.src));
  const tags = [game.category, multiplayer(game) ? 'Multiplayer' : null, game.modes?.includes('single') ? 'Single player' : null, game.offline ? installed(game.id) ? 'Installed' : 'Offline Ready' : 'Online only', /prototype/i.test(game.tier) ? 'Prototype' : null].filter(Boolean);
  return `<button class="text-button back-line" data-go="Library">${icon('back')} Back to library</button>${hero(game,true)}
    <div class="detail-grid"><article class="detail-card"><div class="section-title"><h2>About the game</h2><button class="chip" data-action="favorite" data-id="${attr(game.id)}" aria-pressed="${favorite(game.id)}">${icon('star')} ${favorite(game.id)?'Favorited':'Favorite'}</button></div>
    <div class="tags">${tags.map(label => `<span class="tag">${esc(label)}</span>`).join('')}</div>${game.requiresUser || game.requiresAccount || game.accountRequired ? '<p>A Catnmice account is required to play.</p>':''}
    ${playsLocally(game) ? '<p>Play from the installed game. Open Multiplayer to join a room, or choose Host Lobby to host from this PC.</p>' : multiplayer(game) ? '<p>Join the official game or lobby to see available rooms and invites.</p>' : ''}
    ${game.offline && !playsLocally(game) ? '<p>Install the local game pack to play without a connection.</p>' : ''}</article>
    <article class="detail-card"><h2>Lifetime players</h2>${lifetime(state.counts?.games?.[game.id],state.counts?.asOf)}
    ${rankingIds.includes(game.id) ? btn('View rankings','show-rank',{id:game.id},'','rankings') : ''}</article></div>
    ${shots.length ? `<section class="section"><div class="section-title">${titleLine('Gameplay',shots.length)}</div><div class="gallery">${shots.map((item,i) => `<button class="shot" data-action="shot" data-src="${attr(mediaPath(item.path || item.src))}" data-title="${attr(title(game))} gameplay screenshot ${i+1}" aria-label="Enlarge gameplay screenshot ${i+1}"><img loading="lazy" src="${attr(mediaPath(item.path || item.src))}" alt="${attr(item.alt || title(game)+' gameplay, screenshot '+(i+1))}"></button>`).join('')}</div></section>` : ''}
  `;
}
function lobbies() {
  const lobbyGames = localLobbyGames.map(matches).filter(Boolean);
  return `<section class="intro-art"><img src="art/trashtactics-hero.webp" alt=""><h2>Live Lobbies</h2><p>Browse public rooms, join with an invite, or host a lobby from this PC.</p><div class="controls">${btn('Browse official lobbies','community',{section:'lobbies'},'gold','lobbies')}</div></section>
    <section class="panel"><h2>Join with an invite</h2><form id="invite-form" class="form-row"><input class="field" name="invite" type="url" required placeholder="Paste a Catnmice or game invite link" aria-label="Invite link"><button class="button" type="submit">${icon('link')} Open invite</button></form></section>
    ${matches('lastsignal')?`<section class="panel"><div class="section-title"><h2>Last Signal public arenas</h2>${btn('Refresh','arena-refresh',{},'','refresh')}</div><div id="arena-rooms" role="status">Checking for open arenas…</div></section>`:''}
    <section class="panel"><h2>Game lobbies</h2><div class="line-list">${lobbyGames.map(g => `<div class="list-item"><img src="${attr(imageFor(g))}" alt=""><div class="item-main"><strong>${esc(title(g))}</strong><small>${installed(g.id) ? 'Installed' : 'Available to install'}</small></div><div class="controls">${btn('Multiplayer','play',{id:g.id,mode:'online'},'','arrow')}${hostLobby(g)}</div></div>`).join('')}</div></section>`;
}
async function loadPublicArenas(){
  const box=$('#arena-rooms');if(!box||!host)return;
  try{
    const rooms=(await host.invoke('publicArenas')).filter(r=>r.expires>Date.now());
    if(!box.isConnected)return;
    box.innerHTML=rooms.length?rooms.map(r=>`<div class="list-item"><div class="item-main"><strong>${esc(r.name)}</strong><small>${esc(r.arena)} · ${r.count}/8 players</small></div>${btn('Join','arena-join',{invite:r.invite,expires:r.expires},'glass','arrow')}</div>`).join(''):'No public arenas are waiting.';
  }catch{if(box.isConnected)box.textContent='The public arena directory is unavailable. You can still paste a host invitation.';}
}
function rankings() {
  return `<section class="panel"><div class="tools" aria-label="Select a ranked game">${rankingIds.filter(matches).map(id => `<button class="chip" data-action="rank" data-id="${id}" aria-pressed="${rankGame===id}">${esc(title(matches(id)))}</button>`).join('')}</div><div id="rank-result" role="status" aria-busy="true"><p>Loading leaderboard…</p></div></section>`;
}
async function loadRankings() {
  const number = ++rankSequence, id = rankGame, box = $('#rank-result');
  if (!box) return;
  try {
    if (!host) throw new Error('Open the desktop app to load rankings.');
    const result = await host.invoke('rankings',{id});
    if (number !== rankSequence || !box.isConnected) return;
    if (!result || result.available === false || !Array.isArray(result.players)) throw new Error(result?.message || 'The official leaderboard is unavailable.');
    const table = result.players.length ? `<table class="ranking-table"><thead><tr><th scope="col">Rank</th><th scope="col">Player</th><th scope="col">Elo</th><th scope="col">Matches</th>${id !== 'trashtactics' ? '<th scope="col">Tier</th>':''}</tr></thead><tbody>${result.players.map((p,i) => `<tr><td>${i+1}</td><td>${esc(p.displayName || p.username)}</td><td>${esc(p.rating)}${Number.isFinite(p.change) ? ` (${p.change>0?'+':''}${p.change})`:''}</td><td>${esc(p.games ?? '—')}</td>${id==='trashtactics'?'':`<td>${p.placementsRemaining>0?esc(p.placementsRemaining)+' placements left':esc(p.tier || '—')}</td>`}</tr>`).join('')}</tbody></table>` : '<p>No ranked matches have been recorded for this leaderboard.</p>';
    box.innerHTML = `<div class="controls"><h2>${id==='cat'?'Ranked Survival Challenge':id==='bannerlands'?'1v1 ranked ladder':'1v1 leaderboard'}</h2></div>${result.season?'<p>Season '+esc(result.season)+'</p>':''}${table}<div class="controls">${btn('Open ranked play','community',{section:'ranked',id},'gold','rankings')}</div>`;
  } catch(error) {
    if (number === rankSequence && box.isConnected) box.innerHTML = `<p>${esc(cleanError(error))}</p><div class="controls">${btn('Refresh leaderboard','refresh-rank',{},'','refresh')}</div>`;
  } finally {box.setAttribute('aria-busy','false');}
}
function downloads() {
  const downloadState = Array.isArray(state.downloads) ? state.downloads : [];
  const buildInfo = state.hubBuild > 0 ? 'Running build '+esc(state.hubBuild)+'. ' :
    state.version ? 'Installed version '+esc(state.version)+'. ' : '';
  const updateInfo = state.publisherUpdates === false
    ? 'Install the latest Windows launcher from catnmice.com/hub/download.html to enable updates.'
    : 'Check for published updates from Magic and Truth for the launcher, game catalog and your installed games.';
  const updateControls = state.publisherUpdates === false ? '' :
    btn('Check for updates','update',{},state.restartPending?'glass':'gold','refresh');
  const restart = state.restartPending ? btn('Restart launcher','restart',{},'gold','refresh') : '';
  return `<section class="intro-art"><img src="art/bannerlands-hero.webp" alt=""><h2>Launcher updates</h2><p>${buildInfo}${state.restartPending ? 'The update is ready to apply.' : updateInfo}</p>${restart || updateControls ? `<div class="controls">${restart}${updateControls}</div>` : ''}</section>
    ${downloadState.length ? `<section class="panel"><h2>Download activity</h2><div class="line-list">${downloadState.map(d=>`<div class="list-item"><div class="item-main"><strong>${esc(d.title || (d.id==='hub'?'Game Hub':d.id==='catalog'?'Game catalog':title(matches(d.id) || {id:d.id,title:d.id})) || 'Update')}</strong><small>${esc([d.status,d.message].filter(Boolean).join(' · ') || 'In progress')}${Number.isFinite(d.progress)?' · '+Math.round(d.progress)+'%':''}</small></div></div>`).join('')}</div></section>` : ''}
    <section class="panel"><div class="section-title">${titleLine('Game packs',packs().length)}</div><div class="line-list">${packs().length ? packs().map(g=>`<div class="list-item"><img src="${attr(imageFor(g))}" alt=""><div class="item-main"><strong>${esc(title(g))}</strong><small>${installed(g.id)?'Installed':'Available to install'}${g.offline.bytes ? ' · '+fmt(Math.ceil(g.offline.bytes/1048576))+' MB':''}</small></div>${installed(g.id)?btn(playsLocally(g) ? 'Play' : 'Play Offline','play',{id:g.id,mode:'offline'},'','play'):btn('Install','install',{id:g.id},'','downloads')}${ids(state.rollbackAvailable).includes(g.id) ? btn('Restore previous version','rollback',{id:g.id}) : ''}</div>`).join(''):'<p>No game packs are available in this catalog.</p>'}</div></section>`;
}
function settings() {
  return `<section class="panel"><h2>Player profile</h2><p>Playing as <strong>${esc(profileName(state.profile))}</strong>.</p><div class="controls">${btn('Manage profiles','accounts',{},'gold','user')}${guest()?'':btn('View website profile','community',{section:'profile'},'','external')+btn('Link guest activity','ask-link',{},'','link')}</div></section>
    <section class="panel"><h2>Launcher preferences</h2><label class="preference"><span><strong>Automatic updates</strong><small>${state.publisherUpdates === false ? 'Install the latest Windows launcher to enable updates.' : 'Check GitHub for launcher and installed-game updates when connected.'}</small></span><input type="checkbox" data-setting="autoUpdate" ${state.publisherUpdates === false ? 'disabled' : state.settings.autoUpdate !== false ? 'checked' : ''}></label>
    <label class="preference"><span><strong>Reduce motion</strong><small>Use static transitions throughout the launcher.</small></span><input type="checkbox" data-setting="reducedMotion" ${state.settings.reducedMotion?'checked':''}></label></section>`;
}
function render() {
  const inviteValue = $('#invite-form input')?.value || '';
  const editingInvite = document.activeElement?.matches('#invite-form input');
  chrome();
  const active = document.activeElement;
  let key = '';
  if (active && view.contains(active)) {
    key = active.dataset.action ? Object.keys(active.dataset).map(k => '[data-'+k.replace(/[A-Z]/g,m=>'-'+m.toLowerCase())+'="'+CSS.escape(active.dataset[k])+'"]').join('') : '';
  }
  if (detailId && matches(detailId)) view.innerHTML = details(matches(detailId));
  else if (page === 'Library') view.innerHTML = library();
  else if (page === 'Live Lobbies') {view.innerHTML=lobbies();loadPublicArenas();}
  else if (page === 'Rankings') {view.innerHTML=rankings();loadRankings();}
  else if (page === 'Downloads') view.innerHTML=downloads();
  else if (page === 'Settings') view.innerHTML=settings();
  view.setAttribute('aria-busy','false');
  const invite = $('#invite-form input');
  if(invite && inviteValue) invite.value=inviteValue;
  if(invite && editingInvite) invite.focus({preventScroll:true});
  if (key) view.querySelector(key)?.focus({preventScroll:true});
}
function cleanError(error) {
  return String(error?.message || error || 'The action could not be completed.').replace(/^Error invoking remote method ['"][^'"]+['"]:\s*(Error:\s*)?/, '').slice(0,500);
}
function updateSummary(result) {
  if (result?.restartPending) return 'The launcher update is ready. Select Restart launcher to apply it.';
  if (result?.changed === 0) return 'No new updates to apply.';
  if (Number.isInteger(result?.changed) && result.changed > 0) {
    return result.changed + (result.changed === 1 ? ' update found.' : ' updates found.') + ' See Download activity for details.';
  }
  return 'Update check completed.';
}
async function dispatch(action, payload, success) {
  if (!host) {message('Open the Windows launcher to use this action.');return;}
  const key = JSON.stringify([action,payload]);
  if (inProgress.has(key)) {message('This action is already in progress.');return;}
  inProgress.add(key);
  try {
    const result = await host.invoke(action,payload);
    if (result && (result.ok === false || result.error)) throw new Error(result.error || result.message || 'The action could not be completed.');
    if (success || result?.message) message(result?.message || (typeof success === 'function' ? success(result) : success));
    await refresh();
  } catch(error) {message(cleanError(error));}
  finally {inProgress.delete(key);}
}
function showDialog(label,content) {
  $('#dialog-body').innerHTML = `<div class="dialog-top"><h2>${esc(label)}</h2><button class="icon-button" aria-label="Close dialog" data-action="dismiss">${icon('close')}</button></div>${content}`;
  if (!dialog.open) dialog.showModal();
}
function accounts() {
  const profiles = Array.isArray(state.profiles) ? state.profiles : [];
  const profilesList = profiles.length ? `<div class="profile-choices"><h3>Local profiles</h3><div class="choice-list">${profiles.map(p=>btn(profileName(p),'account',{mode:'switch',id:p.id},'',p.id===state.profile?.id ? 'check' : 'user')).join('')}</div></div>` : '';
  showDialog('Player profiles',`<p class="dialog-copy">Use your Catnmice account to bring your profile and rankings into the hub.</p><div class="choice-list">${btn('Log In','account',{mode:'login'},'gold','user')}${btn('Create Profile','account',{mode:'register'},'','external')}${btn('Continue as Guest','account',{mode:'guest'},'','gamepad')}</div>${profilesList}
    <div class="profile-choices"><h3>Verified rankings</h3><p class="dialog-copy" id="verified-profile-note">Open the official Catnmice site in your default browser to authorize sharing your existing verified ranking identity with the launcher.</p><div class="choice-list"><button class="button" data-action="account" data-mode="verified" aria-describedby="verified-profile-note">${icon('external')} Use verified browser profile</button></div></div>`);
}
function browse() {
  if (detailId || page !== 'Library') navigate('Library');
  const target = $('#library-list');
  target?.scrollIntoView({block:'start'});
  target?.querySelector('.chip')?.focus({preventScroll:true});
}
function onAction(el) {
  const {action,id,mode,kind,section} = el.dataset;
  switch(action) {
    case 'arena-refresh':loadPublicArenas();break;
    case 'arena-join':
      if(Number(el.dataset.expires)<=Date.now()){loadPublicArenas();message('This listing expired. Choose an open arena.');}
      else dispatch('invite',{url:el.dataset.invite});
      break;
    case 'detail': navigate('Library',id);break;
    case 'quickplay': case 'replay': {
      const game = matches(id);
      if (!game) break;
      if (playsLocally(game)) {
        dispatch('play',{id,mode:'offline'});
        break;
      }
      const last = action==='replay' && state.recent?.find?.(e=>typeof e==='object' && (e.id || e.key)===id);
      if (last?.mode==='offline' && installed(id)) dispatch('play',{id,mode:'offline'});
      else if (installed(id) && state.online===false) dispatch('play',{id,mode:'offline'});
      else dispatch('play',{id,mode:'online',action:(game.actions || [])[0]?.type || ''});
      break;
    }
    case 'play': dispatch('play',{id,mode,action:kind});break;
    case 'install': dispatch('install',{id},'Game pack installation completed.');break;
    case 'update': dispatch('update',id ? {id} : {},updateSummary);break;
    case 'restart': dispatch('restart',{});break;
    case 'rollback': dispatch('rollback',{id},'Previous game version restored.');break;
    case 'previous': case 'next': case 'slide': {
      const total = spotlights.filter(matches).length;
      feature = action==='slide' ? Number(el.dataset.index) : (feature+total+(action==='next'?1:-1))%total;
      render();break;
    }
    case 'browse': browse();break;
    case 'filter': filter = el.dataset.value;render();break;
    case 'reset': filter='All games';query='';$('#search').value='';render();break;
    case 'reload': refresh().catch(e=>message(cleanError(e)));break;
    case 'favorite': if(guest()) accounts();else dispatch('favorite',{id});break;
    case 'accounts': accounts();break;
    case 'ask-link': showDialog('Link guest activity',`<p class="dialog-copy">Count this device’s guest play under ${esc(profileName(state.profile))} when lifetime player totals sync.</p><div class="controls">${btn('Link activity','link',{},'gold','link')}${btn('Cancel','dismiss')}</div>`);break;
    case 'link': dialog.close();dispatch('linkGuest',{},'Guest activity linked.');break;
    case 'account': dialog.close();dispatch('account',id ? {mode,id} : {mode});break;
    case 'community': dispatch('community',{section,id});break;
    case 'rank': rankGame=id;render();break;
    case 'show-rank': rankGame=id;navigate('Rankings');break;
    case 'refresh-rank': loadRankings();break;
    case 'dismiss': dialog.close();break;
    case 'shot': showDialog(el.dataset.title,`<figure class="dialog-figure"><img src="${attr(el.dataset.src)}" alt="${attr(el.dataset.title)}"></figure>`);break;
  }
}
document.addEventListener('click',event => {
  const el = event.target.closest('button');
  if (!el) {if(event.target===dialog) dialog.close();return;}
  if (el.dataset.go) {filter='All games';navigate(el.dataset.go);}
  else if(el.dataset.action) onAction(el);
});
document.addEventListener('submit',event => {
  if(event.target.id!=='invite-form') return;
  event.preventDefault();dispatch('invite',{url:event.target.elements.invite.value.trim()});
});
document.addEventListener('change',event => {
  const name = event.target.dataset.setting;
  if(name) dispatch('settings',{[name]:event.target.checked},'Preference saved.');
});
$('#search').addEventListener('input',event => {
  if(page!=='Library' || detailId) filter='All games';
  page='Library';detailId='';query=event.target.value;render();window.scrollTo(0,0);
});
document.addEventListener('keydown',event => {
  if(dialog.open || event.ctrlKey || event.metaKey || event.altKey) return;
  if(event.key==='/' && !event.target.matches('input,textarea,select')) {event.preventDefault();$('#search').focus();}
  if(event.key==='Escape' && document.activeElement===$('#search')) {$('#search').value='';query='';render();$('#main').focus();}
});
document.querySelectorAll('[data-icon]').forEach(el => {el.innerHTML=icon(el.dataset.icon);});
const updateChrome = () => refresh().catch(e=>message(cleanError(e)));
window.addEventListener('offline',updateChrome);
window.addEventListener('online',updateChrome);
window.addEventListener('focus',() => {if(host && !dialog.open) refresh().catch(e=>message(cleanError(e)));});
if(typeof host?.onChange === 'function') host.onChange(() => refresh().catch(e=>message(cleanError(e))));
refresh().catch(error => {view.innerHTML=`<div class="notice"><p>${esc(cleanError(error))}</p>${btn('Try again','reload')}</div>`;view.removeAttribute('aria-busy');});
