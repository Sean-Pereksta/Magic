const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');
const core = library.upgradeHtml(fs.readFileSync(process.env.LOBBY_CORE_PATH || path.join(__dirname, '../lobby-core.html'), 'utf8').replace(/\r\n/g, '\n'));
const registry = vm.runInNewContext(core.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]);
const apes = registry.find(game => game.key === 'apestogetherstrong');
const forge = registry.find(game => game.key === 'crowncouncil');
const turncraft = registry.find(game => game.key === 'turncraft');

function harness({username = '', write = () => new Promise(() => {}), rememberedLobby = null} = {}) {
  const saved = new Map(), launches = [], modals = [], warnings = [], lobbyCalls = [];
  if (rememberedLobby) saved.set('lastLobby', rememberedLobby.id);
  const unexpectedLobbyCall = name => (...args) => {lobbyCalls.push({name, args}); throw new Error('Solo launch attempted ' + name);};
  let renders = 0;
  const context = vm.createContext({
    username, launchingGame: false, pendingAction: null, recentGames: [], currentUserData: {},
    currentLobbyId: rememberedLobby?.id || null, currentLobbyData: rememberedLobby,
    gamePlayCounts: {}, baseProfileCache: new Map(), profileBundleCache: new Map(),
    SK: {recent: 'recent', lastGame: 'lastGame', lastLobby: 'lastLobby'}, db: {},
    doc: (_db, collection, key) => ({collection, key}), setDoc: write,
    addDoc: unexpectedLobbyCall('addDoc'), getDoc: unexpectedLobbyCall('getDoc'),
    onSnapshot: unexpectedLobbyCall('onSnapshot'), runTransaction: unexpectedLobbyCall('runTransaction'),
    increment: value => value, serverTimestamp: () => 123,
    save: (key, value) => saved.set(key, value), read: key => saved.get(key), normalize: value => value.toLowerCase(),
    renderAllGames: () => renders++, renderProfile: () => renders++,
    toast() {}, console: {warn: (...args) => warnings.push(args)},
    openModal: id => modals.push(id),
    launchGameUrl: (url, reason) => {context.launchingGame = true; launches.push({url, reason});}
  });
  for (const name of ['logGamePlay', 'addRecentGame', 'playSingle']) {
    const start = core.indexOf('    async function ' + name + '(');
    assert.ok(start >= 0);
    const end = core.indexOf('\n    }', start) + '\n    }'.length;
    vm.runInContext(core.slice(start, end), context);
  }
  return {context, saved, launches, modals, warnings, lobbyCalls, renders: () => renders};
}

for (const username of ['', 'Test Ape']) {
  test('Apes Play launches immediately with stalled cloud writes: ' + (username || 'guest'), async () => {
    const writes = [];
    const h = harness({username, write: ref => {writes.push(ref); return new Promise(() => {});}});
    const result = library.runAction(apes, 'single', {playSingle: h.context.playSingle});
    assert.deepEqual(h.launches, [{url: '/game/apes-together-strong.html', reason: 'single-play'}]);
    assert.equal(h.saved.get('lastGame'), apes.key);
    assert.equal(JSON.parse(h.saved.get('recent'))[0].key, apes.key);
    assert.ok(writes.some(ref => ref.collection === 'gameStats'));
    if (username) assert.ok(writes.some(ref => ref.collection === 'users'));
    await result;
    await h.context.playSingle(apes, true);
    assert.equal(h.launches.length, 1, 'repeated clicks must not launch or count twice');
    assert.equal(writes.filter(ref => ref.collection === 'gameStats').length, 1);
  });
}

test('rejected cloud writes do not prevent launch or local recent history', async () => {
  const h = harness({username: 'Ape', write: async () => {throw new Error('offline');}});
  await h.context.playSingle(apes, true);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.launches.length, 1);
  assert.equal(JSON.parse(h.saved.get('recent'))[0].key, apes.key);
  assert.ok(h.warnings.length);
});

test('solo launch still requires a user action and honors account gates', async () => {
  const h = harness();
  await h.context.playSingle(apes);
  assert.equal(h.launches.length, 0);
  await h.context.playSingle({...apes, requiresUser: true}, true);
  assert.deepEqual(h.modals, ['accountGateModal']);
  assert.equal(h.context.pendingAction.gameKey, apes.key);
  assert.equal(h.saved.size, 0);
});

test('solo route parameters and cloud stats still work when online', async () => {
  const h = harness({username: 'Ape & King', write: async () => {}});
  await h.context.playSingle({...apes, singleRoute: '/game/apes-together-strong.html?mode=solo', autoUsername: true}, true);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(h.launches[0].url, '/game/apes-together-strong.html?mode=solo&username=Ape%20%26%20King');
  assert.equal(h.context.gamePlayCounts[apes.key], 1);
  assert.equal(h.context.currentUserData.gamePlayCounts[apes.key], 1);
  assert.equal(h.renders(), 2);
});

test('TurnCraft solo launch carries the exact selected profile without creating a lobby', async () => {
  const h = harness({username: 'Forge & Commander'});
  await h.context.playSingle(turncraft, true);
  assert.deepEqual(h.launches, [{url: '/game/turncraft.html?username=Forge%20%26%20Commander', reason: 'single-play'}]);
  assert.deepEqual(h.lobbyCalls, []);
});

for (const [username, status] of [['', 'waiting'], ['Forge & Commander', 'started']]) {
  test('Faction Forge solo action ignores a remembered ' + status + ' lobby for ' + (username || 'guest'), async () => {
    const writes = [], actionCalls = [];
    const h = harness({username, rememberedLobby: {id: 'old-forge-room', gameType: 'crowncouncil', status, players: [username]}, write: ref => {writes.push(ref); return new Promise(() => {});}});
    const result = library.runAction(forge, 'single', {
      playSingle: h.context.playSingle,
      openCreateLobby: key => actionCalls.push({type: 'lobby', key}),
      joinOnlineGame: game => actionCalls.push({type: 'online', key: game.key})
    });
    assert.deepEqual(h.launches, [{url: '/game/crowncouncil.html?mode=solo' + (username ? '&username=Forge%20%26%20Commander' : ''), reason: 'single-play'}]);
    assert.deepEqual(actionCalls, []);
    assert.deepEqual(h.lobbyCalls, []);
    assert.ok(writes.every(ref => ref.collection !== 'lobbies'), 'solo analytics must not create or update a lobby');
    assert.equal(h.saved.get('lastLobby'), 'old-forge-room');
    assert.equal(h.saved.get('lastGame'), 'crowncouncil');
    await result;
  });
}

test('Faction Forge multiplayer action opens the lobby creator without launching solo', async () => {
  const calls = [];
  await library.runAction(forge, 'lobby', {
    playSingle: () => calls.push('single'),
    openCreateLobby: key => calls.push(key),
    joinOnlineGame: () => calls.push('online')
  });
  assert.deepEqual(calls, ['crowncouncil']);
});


