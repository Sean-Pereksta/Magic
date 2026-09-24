const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');

// CI always uses the actual hub core. A local source fixture can be supplied
// when running an isolated renderer/browser harness without Firebase.
const core = fs.readFileSync(process.env.LOBBY_CORE_PATH || path.join(__dirname, '../lobby-core.html'), 'utf8');
const upgraded = library.upgradeHtml(core);
const registry = html => {
  const match = html.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/);
  assert.ok(match, 'the existing game registry must remain intact');
  return JSON.parse(JSON.stringify(vm.runInNewContext(`(${match[1]})`)));
};
const games = registry(core);
const byKey = Object.fromEntries(games.map(game => [game.key, game]));

for (const key of ['receiverwindowqb', 'arcanewilds']) {
  test(`${key}: singleplayer offers only Play`, () => {
    assert.deepEqual(library.getActions(byKey[key]), [{ type: 'single', label: 'Play' }]);
  });
}
for (const key of ['cat', 'chesswarlord', 'stoneyrelic']) {
  test(`${key}: hosted multiplayer offers only Create Lobby`, () => {
    assert.deepEqual(library.getActions(byKey[key]), [{ type: 'lobby', label: 'Create Lobby' }]);
  });
}
for (const key of ['warrealms', 'biblegame', 'bibleroguelike', 'turncraft', 'ironthrone']) {
  test(`${key}: both Play Offline and Create Lobby are available`, () => {
    assert.deepEqual(library.getActions(byKey[key]), [
      { type: 'single', label: 'Play Offline' }, { type: 'lobby', label: 'Create Lobby' }
    ]);
  });
}

test('shared worlds and matchmaking retain working direct entry actions', () => {
  assert.deepEqual(library.getActions(byKey.gridbound), [{ type: 'online', label: 'Enter Realm' }]);
  assert.deepEqual(library.getActions(byKey.cardrealmsmulti), [{ type: 'online', label: 'Find Match' }]);
  assert.deepEqual(library.getActions({ modes: ['multi'], launchMode: 'realm' }), [{ type: 'online', label: 'Enter World' }]);
});

test('unknown games and modes do not gain launch actions', () => {
  for (const game of [null, {}, { modes: [] }, { modes: ['unknown'] }]) assert.deepEqual(library.getActions(game), []);
});

test('actions reuse existing user-initiated launch and account-gated lobby paths', async () => {
  const calls = [];
  const hooks = {
    playSingle: (...args) => calls.push(['single', ...args]),
    openCreateLobby: (...args) => calls.push(['lobby', ...args]),
    joinOnlineGame: (...args) => calls.push(['online', ...args])
  };
  await library.runAction(byKey.warrealms, 'single', hooks);
  await library.runAction(byKey.biblegame, 'lobby', hooks);
  await library.runAction(byKey.gridbound, 'online', hooks);
  assert.deepEqual(calls, [
    ['single', byKey.warrealms, true], ['lobby', 'biblegame'], ['online', byKey.gridbound, true]
  ]);
  assert.throws(() => library.runAction(byKey.cat, 'single', hooks), /does not support/);
  assert.throws(() => library.runAction(byKey.gridbound, 'lobby', hooks), /does not support/);
  assert.equal(calls.length, 3);
});

test('navigation changes on desktop and mobile without invalidating saved view ids', () => {
  const buttons = [...upgraded.matchAll(/data-view="singleplayer"[^>]*>([\s\S]*?)<\/button>/g)];
  assert.equal(buttons.length, 2);
  buttons.forEach(match => { assert.match(match[1], /Library/); assert.doesNotMatch(match[1], /Singleplayer/); });
  assert.match(upgraded, /id="view-singleplayer"/);
  assert.match(upgraded, />Game Library<\/div>/);
  assert.match(upgraded, /id="gameLibraryStyles"/);
});

test('the entire registry and all routes remain unchanged by the library upgrade', () => {
  assert.deepEqual(registry(upgraded), games);
  assert.equal(byKey.warrealms.singleRoute, '/game/warrealms.html?mode=singleplayer');
  assert.equal(byKey.bibleroguelike.singleRoute, '/game/biblegame.html?mode=roguelike');
  for (const game of games) {
    const types = library.getActions(game).map(action => action.type);
    assert.equal(types.includes('single'), game.modes.includes('single'), game.key);
    assert.equal(types.includes('lobby') || types.includes('online'), game.modes.includes('multi'), game.key);
  }
});

function render({ search = '', category = 'All', sort = 'title', favorites = [], recent = [] } = {}) {
  const elements = new Map();
  const $ = id => {
    if (!elements.has(id)) elements.set(id, {
      value: id === 'gameSearch' ? search : id === 'gameSort' ? sort : '', hidden: false,
      children: [], markup: '', set innerHTML(value) { this.markup = value; this.children = []; },
      get innerHTML() { return this.markup; }, appendChild(child) { this.children.push(child); }
    });
    return elements.get(id);
  };
  const match = upgraded.match(/    function renderSingleLibrary\(\)\{[\s\S]*?\n    \}\n    \$\("gameSearch"\)/);
  assert.ok(match, 'the shared library renderer must still exist');
  const source = match[0].slice(0, match[0].lastIndexOf('\n    $("gameSearch")'));
  vm.runInNewContext(`${source}\nrenderSingleLibrary();`, {
    $, GAMES: games, SINGLE_GAMES: games.filter(game => game.modes.includes('single')), GAME_BY_KEY: byKey,
    selectedCategory: category, gamePlayCounts: {}, recentGames: recent, favorites: new Set(favorites),
    getGame: key => byKey[key] || { key, modes: [] }, gameCard: game => game.key,
    normalize: value => String(value || '').trim().toLowerCase()
  });
  return elements;
}

test('All includes every game, including online-only games and prototypes', () => {
  assert.deepEqual([...render().get('singleGameGrid').children].sort(), games.map(game => game.key).sort());
});

test('search and category filters include multiplayer games', () => {
  assert.deepEqual(render({ search: 'Cat & Mouse' }).get('singleGameGrid').children, ['cat']);
  const strategy = render({ category: 'Strategy' }).get('singleGameGrid').children;
  assert.ok(strategy.includes('chesswarlord'));
  assert.ok(strategy.includes('arcanewilds') === false);
  assert.match(render({ search: 'no-such-game-7654321' }).get('singleGameGrid').innerHTML, /No games match/);
});

test('online favorites and recents appear; deleted and inherited keys are ignored', () => {
  const elements = render({
    favorites: ['cat', 'warrealms', 'gridbound', 'deleted-game', '__proto__'],
    recent: [{ key: 'gridbound' }, 'cat', { key: 'warrealms' }, 'deleted-game', '__proto__']
  });
  assert.deepEqual(elements.get('favoritesStrip').children, ['cat', 'warrealms', 'gridbound']);
  assert.deepEqual(elements.get('continueStrip').children, ['gridbound', 'cat', 'warrealms']);
  assert.equal(elements.get('favoritesSection').hidden, false);
  assert.equal(elements.get('continueSection').hidden, false);
});

test('recent sorting works for online-only games', () => {
  const result = render({ sort: 'recent', recent: ['cat', 'chesswarlord'] }).get('singleGameGrid').children;
  assert.deepEqual(result.slice(0, 2), ['cat', 'chesswarlord']);
});

test('upgrade is idempotent and refuses missing source anchors', () => {
  assert.equal(library.upgradeHtml(upgraded), upgraded);
  assert.throws(() => library.upgradeHtml(core.replace('let games=SINGLE_GAMES.filter(', 'let games=OTHER.filter(')), /catalog filter/);
  assert.throws(() => library.upgradeHtml(core.replace('data-view="singleplayer"', 'data-view="missing"')), /navigation/);
});

test('unchanged launch/account handlers are preserved verbatim', () => {
  for (const signature of ['    async function playSingle(', '    async function joinOnlineGame(']) {
    const start = core.indexOf(signature);
    assert.ok(start >= 0, signature);
    const end = core.indexOf('\n    }', start) + '\n    }'.length;
    assert.ok(upgraded.includes(core.slice(start, end)), signature);
  }
});

test('catalog organization keeps the Game Library heading after its deferred boot', () => {
  const organization = fs.readFileSync(path.join(__dirname, '../lobby-catalog-organization.js'), 'utf8');
  assert.match(organization, /singleTitle\.textContent="Game Library"/);
  assert.doesNotMatch(organization, /singleTitle\.textContent="Singleplayer Library"/);
});

test('the real loader injects extra games before upgrading and preserves core module syntax', async () => {
  const loader = fs.readFileSync(path.join(__dirname, '../lobby.html'), 'utf8');
  assert.match(loader, /<script src="\.\/lobby-library\.js"><\/script>/);
  const script = loader.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
  let written = '';
  const errors = [];
  await vm.runInNewContext(script, {
    fetch: async () => ({ ok: true, text: async () => core }), GameHubLibrary: library,
    document: { open() {}, write(value) { written = value; }, close() {}, body: {} },
    console: { error: (...args) => errors.push(args) }
  });
  assert.equal(errors.length, 0, JSON.stringify(errors));
  const loadedGames = registry(written);
  for (const key of ['circuitbound', 'mtgcatalog', 'fantasyfootball', 'receiverwindowqb']) {
    assert.equal(loadedGames.filter(game => game.key === key).length, 1, key);
  }
  assert.match(written, /let games=GAMES\.filter\(/);
  assert.match(written, /lobby-catalog-organization\.js/);
  const modules = [...written.matchAll(/<script\b[^>]*type="module"[^>]*>([\s\S]*?)<\/script>/g)];
  assert.ok(modules.length > 0);
  assert.equal(typeof vm.SourceTextModule, 'function', 'Run with node --experimental-vm-modules --test');
  for (const match of modules) assert.doesNotThrow(() => new vm.SourceTextModule(match[1]));
});
