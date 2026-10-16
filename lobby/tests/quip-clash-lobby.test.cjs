const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');
const core = fs.readFileSync(path.join(__dirname, '../lobby-core.html'), 'utf8').replace(/\r\n/g, '\n');
const registry = vm.runInNewContext(core.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]);
const quip = registry.find(game => game.key === 'quipclash');

function functionSource(name) {
  const match = core.match(new RegExp(`    (?:async )?function ${name}\\([^]*?\\n    \\}`));
  assert.ok(match, `missing ${name}`);
  return match[0];
}

function harness({username = 'Laugh & Lines', lobby = {gameType: 'quipclash', status: 'waiting'}, currentLobbyId = null} = {}) {
  const launches = [], modals = [], mutations = [], controls = new Map(), saved = new Map();
  let left = 0;
  const snapshot = {id: 'AB12CD', exists: () => Boolean(lobby), data: () => lobby};
  const unexpectedMutation = type => () => {mutations.push(type); throw new Error(`unexpected ${type}`);};
  const context = vm.createContext({
    username, currentLobbyId, currentLobbyData: null, pendingAction: null, launchingGame: false,
    startingLobbyGame: false, isHost: true, selectedVisibility: 'private', linkedLobbyId: null, db: {},
    auth: {currentUser: {uid: 'own-uid'}}, SK: {lastGame: 'lastGame', lastLobby: 'lastLobby'},
    MULTIPLAYER_LAUNCH_MODES: {LOBBY: 'lobby', REALM: 'realm', MATCHMAKING: 'matchmaking'},
    getLaunchMode: game => game?.launchMode || 'lobby', getGame: key => registry.find(game => game.key === key),
    getMaxPlayers: key => registry.find(game => game.key === key)?.maxPlayers || Infinity,
    gameColor: () => '#9af0ca', gameVisual: () => '', gameTitle: key => registry.find(game => game.key === key)?.title,
    escapeHtml: value => String(value || ''), setMessage() {},
    getDoc: async () => snapshot, doc: (_db, collection, id) => ({collection, id}),
    addDoc: unexpectedMutation('addDoc'), runTransaction: unexpectedMutation('runTransaction'),
    deleteDoc: unexpectedMutation('deleteDoc'), setDoc: unexpectedMutation('setDoc'),
    leaveLobby: async () => {left++;context.currentLobbyId = null;},
    openModal: id => modals.push(id), launchGameUrl: (url, reason) => launches.push({url, reason}),
    save: (key, value) => saved.set(key, value), stopCurrentLobbySafetyPolling() {},
    setBusy() {}, renderCurrentLobby() {}, toast() {}, console: {warn() {}},
    $: id => {if (!controls.has(id)) controls.set(id, {value: ''});return controls.get(id);},
    URLSearchParams, encodeURIComponent
  });
  for (const name of ['requireUser','requestJoinLobby','joinOnlineGame','joinLobby','openCreateLobby','createLobby','startGame','redirectStartedGame','showLinkedLobbyPrompt']) {
    vm.runInContext(functionSource(name), context);
  }
  return {context, launches, modals, mutations, controls, saved, left: () => left};
}

test('Quip Clash is an account-only hosted party game with its dedicated room page', () => {
  assert.ok(quip);
  assert.equal(quip.route, '/game/quip-clash.html');
  assert.equal(quip.multiplayerRoute, quip.route);
  assert.equal(quip.imagePath, '/game/quip-clash/assets/quip-clash.svg');
  assert.equal(quip.requiresUser, true);
  assert.equal(quip.launchMode, 'lobby');
  assert.equal(quip.minPlayers, 4);
  assert.equal(quip.maxPlayers, 12);
  assert.deepEqual(Array.from(quip.modes), ['multi']);
  assert.deepEqual(library.getActions(quip), [{type: 'online', label: 'Play with Friends'}]);
});

test('effective lobby loader registers Quip Clash exactly once, including an older core', async () => {
  const loader = fs.readFileSync(path.join(__dirname, '../lobby.html'), 'utf8');
  const code = loader.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
  for (const input of [core, core.replace(/^      \{key:"quipclash"[^\n]*\n/m, '')]) {
    let output = '';
    await vm.runInNewContext(code, {
      GameHubLibrary: library, fetch: async () => ({ok: true, text: async () => input}),
      document: {open() {}, write: html => {output = html;}, close() {}}, console: {warn() {}, error() {}}
    });
    const games = vm.runInNewContext(output.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]);
    assert.equal(games.filter(game => game.key === 'quipclash').length, 1);
    assert.equal(games.find(game => game.key === 'quipclash').multiplayerRoute, quip.route);
  }
});

test('browse action requires an account and an explicit player action', async () => {
  const guest = harness({username: ''});
  await guest.context.joinOnlineGame(quip, true);
  assert.deepEqual(guest.modals, ['accountGateModal']);
  assert.equal(guest.context.pendingAction.type, 'online');
  assert.equal(guest.context.pendingAction.gameKey, 'quipclash');
  assert.deepEqual(guest.launches, []);
  const member = harness();
  await member.context.joinOnlineGame(quip);
  assert.deepEqual(member.launches, []);
  await library.runAction(quip, 'online', {joinOnlineGame: member.context.joinOnlineGame});
  assert.deepEqual(member.launches, [{url: '/game/quip-clash.html?mode=browse&username=Laugh%20%26%20Lines', reason: 'matchmaking-join'}]);
  assert.deepEqual(member.mutations, []);
});

test('Create Lobby opens the dedicated creator after the existing account gate', () => {
  const guest = harness({username: ''});
  guest.context.openCreateLobby('quipclash');
  assert.deepEqual(guest.modals, ['accountGateModal']);
  assert.equal(guest.context.pendingAction.type, 'create');
  const member = harness();
  member.context.openCreateLobby('quipclash');
  assert.deepEqual(member.launches, [{url: '/game/quip-clash.html?mode=create&username=Laugh%20%26%20Lines', reason: 'matchmaking-join'}]);
  assert.deepEqual(member.mutations, []);
});

test('choosing Quip Clash in the existing creator carries the room settings without an incomplete Firestore write', async () => {
  const h = harness();
  h.context.$('newLobbyGame').value = 'quipclash';
  h.context.$('newLobbyName').value = ' Best & Funniest ';
  await h.context.createLobby();
  const url = new URL(h.launches[0].url, 'https://example.test');
  assert.equal(url.pathname, quip.route);
  assert.equal(url.searchParams.get('name'), 'Best & Funniest');
  assert.equal(url.searchParams.get('visibility'), 'private');
  assert.equal(url.searchParams.get('username'), 'Laugh & Lines');
  assert.equal(url.searchParams.get('mode'), 'create');
  assert.deepEqual(h.mutations, []);
});

test('join and invite actions reach the dedicated transactional join, preserve the invite, and leave a previous room', async () => {
  const guest = harness({username: ''});
  await guest.context.joinLobby('AB12CD', 'AB12CD__laugh%20%26%20lines');
  assert.deepEqual(guest.modals, ['accountGateModal']);
  assert.equal(guest.context.pendingAction.inviteId, 'AB12CD__laugh%20%26%20lines');
  const h = harness({currentLobbyId: 'old-room'});
  await h.context.joinLobby('AB12CD', 'AB12CD__laugh%20%26%20lines');
  const url = new URL(h.launches[0].url, 'https://example.test');
  assert.equal(url.searchParams.get('lobbyId'), 'AB12CD');
  assert.equal(url.searchParams.get('username'), 'Laugh & Lines');
  assert.equal(url.searchParams.get('inviteId'), 'AB12CD__laugh%20%26%20lines');
  assert.equal(h.left(), 1);
  assert.deepEqual(h.mutations, []);
});

test('existing members reopen running and completed rooms through the dedicated page', async () => {
  for (const status of ['started','finished']) {
    const h = harness({lobby: {gameType: 'quipclash', status}});
    await h.context.joinLobby('AB12CD');
    assert.equal(new URL(h.launches[0].url, 'https://example.test').searchParams.get('lobbyId'), 'AB12CD');
    assert.deepEqual(h.mutations, []);
  }
});

test('shared links preview waiting rooms and permit only current members to reopen a running Quip Clash', async () => {
  for (const status of ['waiting','started','finished']) {
    const h = harness({lobby: {gameType: 'quipclash', status, players: ['Laugh & Lines'], members: {'own-uid': 'Laugh & Lines'}}});
    await h.context.showLinkedLobbyPrompt('AB12CD');
    assert.deepEqual(h.modals, ['linkLobbyModal']);
    assert.equal(h.context.linkedLobbyId, 'AB12CD');
    assert.deepEqual(h.launches, [], 'preview must not automatically enter the game');
    assert.deepEqual(h.mutations, []);
  }
  const other = harness({lobby: {gameType: 'quipclash', status: 'started', players: ['Other'], members: {other: 'Other'}}});
  await other.context.showLinkedLobbyPrompt('AB12CD');
  assert.deepEqual(other.modals, []);
  assert.equal(other.context.linkedLobbyId, null);
});

test('hub host Start action delegates setup and scoring to Quip Clash instead of marking a partial game started', async () => {
  const h = harness({currentLobbyId: 'AB12CD'});
  await h.context.startGame();
  assert.deepEqual(h.launches, [{url: '/game/quip-clash.html?lobbyId=AB12CD&username=Laugh%20%26%20Lines', reason: 'matchmaking-join'}]);
  assert.deepEqual(h.mutations, []);
});

test('started-lobby navigation preserves the game lobby id and existing membership guards', () => {
  const h = harness({currentLobbyId: 'AB12CD'});
  const data = {gameType: 'quipclash', status: 'started', players: ['Laugh & Lines']};
  h.context.redirectStartedGame('other-room', data);
  h.context.redirectStartedGame('AB12CD', {...data, players: ['Other']});
  assert.deepEqual(h.launches, []);
  h.context.redirectStartedGame('AB12CD', data);
  assert.deepEqual(h.launches, [{url: '/game/quip-clash.html?lobbyId=AB12CD&username=Laugh%20%26%20Lines', reason: 'lobby-started'}]);
  assert.equal(h.saved.get('lastLobby'), 'AB12CD');
});

test('TurnCraft retains its own dedicated join route', async () => {
  const h = harness({lobby: {gameType: 'turncraft', turncraftVersion: 1, status: 'waiting'}});
  await h.context.joinLobby('turncraft-room');
  assert.deepEqual(h.launches, [{url: '/game/turncraft-lobby.html?lobbyId=turncraft-room&username=Laugh%20%26%20Lines', reason: 'matchmaking-join'}]);
  assert.deepEqual(h.mutations, []);
});
