const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');
const core = library.upgradeHtml(fs.readFileSync(process.env.LOBBY_CORE_PATH || path.join(__dirname, '../lobby-core.html'), 'utf8').replace(/\r\n/g, '\n'));
const registry = vm.runInNewContext(core.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]);
const apes = registry.find(game => game.key === 'apestogetherstrong');

function harness({username = '', write = () => new Promise(() => {})} = {}) {
  const saved = new Map(), launches = [], modals = [], warnings = [];
  let renders = 0;
  const context = vm.createContext({
    username, launchingGame: false, pendingAction: null, recentGames: [], currentUserData: {},
    gamePlayCounts: {}, baseProfileCache: new Map(), profileBundleCache: new Map(),
    SK: {recent: 'recent', lastGame: 'lastGame'}, db: {},
    doc: (_db, collection, key) => ({collection, key}), setDoc: write,
    increment: value => value, serverTimestamp: () => 123,
    save: (key, value) => saved.set(key, value), normalize: value => value.toLowerCase(),
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
  return {context, saved, launches, modals, warnings, renders: () => renders};
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
