const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');

// CI always uses the actual hub core. A local source fixture can be supplied
// when running an isolated renderer/browser harness without Firebase.
const core = fs.readFileSync(process.env.LOBBY_CORE_PATH || path.join(__dirname, '../lobby-core.html'), 'utf8').replace(/\r\n/g, '\n');
const upgraded = library.upgradeHtml(core);
const registry = html => {
  const match = html.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/);
  assert.ok(match, 'the existing game registry must remain intact');
  return JSON.parse(JSON.stringify(vm.runInNewContext(`(${match[1]})`)));
};
const games = registry(core);
const byKey = Object.fromEntries(games.map(game => [game.key, game]));

for (const key of ['receiverwindowqb', 'arcanewilds', 'apestogetherstrong']) {
  test(`${key}: singleplayer offers only Play`, () => {
    assert.deepEqual(library.getActions(byKey[key]), [{ type: 'single', label: 'Play' }]);
  });
}
for (const key of ['cat', 'chesswarlord', 'stoneyrelic']) {
  test(`${key}: hosted multiplayer offers only Create Lobby`, () => {
    assert.deepEqual(library.getActions(byKey[key]), [{ type: 'lobby', label: 'Create Lobby' }]);
  });
}
for (const key of ['warrealms', 'biblegame', 'bibleroguelike', 'ironthrone']) {
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

test('Apes Together Strong uses the installed solo route and never creates a lobby', async () => {
   const game=byKey.apestogetherstrong;
   assert.equal(game.route, '/game/apes-together-strong.html');
   assert.deepEqual(game.modes,['single']);
   assert.ok(fs.existsSync(path.join(__dirname,'../..',game.route)));
   let launched=false;
   await library.runAction(game,'single',{playSingle:(selected,userInitiated)=>{assert.equal(selected.key,'apestogetherstrong');assert.equal(userInitiated,true);launched=true;}});
   assert.equal(launched,true);
   assert.throws(()=>library.runAction(game,'lobby',{}),/does not support/);
 });

// Small event fixture for the card's asynchronous artwork and existing hooks.
// Browser CI below exercises the actual HTML and responsive CSS.
function cardHarness() {
  class Element {
    constructor() {
      this.listeners = new Map(); this.dataset = {}; this.hidden = false; this.disabled = false;
      this.style = { setProperty() {} };
      this.classes = new Set(); this.classList = { add: value => this.classes.add(value) };
    }
    addEventListener(type, callback) {
      if (!this.listeners.has(type)) this.listeners.set(type, []);
      this.listeners.get(type).push(callback);
    }
    async emit(type, event = {}) { await Promise.all((this.listeners.get(type) || []).map(callback => callback(event))); }
    set innerHTML(value) {
      this.markup = value;
      this.favorite = new Element(); this.cover = new Element(); this.cover.hidden = true;
      this.portraits = [new Element(), new Element(), new Element()];
      this.actions = [...value.matchAll(/data-game-action="([^"]+)"/g)].map(match => {
        const button = new Element(); button.dataset.gameAction = match[1]; return button;
      });
    }
    get innerHTML() { return this.markup; }
    querySelector(selector) { return selector === '.favorite-btn' ? this.favorite : this.cover; }
    querySelectorAll(selector) { return selector === '.tc-portrait img' ? this.portraits : this.actions; }
  }
  const probes = [], calls = [];
  const document = { createElement: tag => { const node = new Element(); if (tag === 'img') probes.push(node); return node; } };
  const hooks = {
    document, escapeHtml: value => String(value), gameColor: () => '#456', gameVisual: () => '<span>Original art</span>',
    favorites: new Set(['turncraft']), gamePlayCounts: {turncraft: 7},
    toggleFavorite: key => calls.push(['favorite', key]),
    playSingle: (...args) => calls.push(['single', ...args]),
    openCreateLobby: key => calls.push(['lobby', key]), joinOnlineGame: (...args) => calls.push(['online', ...args]), toast: (...args) => calls.push(['toast', ...args])
  };
  return { hooks, probes, calls, create: game => library.createGameCard(game || byKey.turncraft, hooks) };
}

test('TurnCraft keeps its existing launch configuration and card hooks', async () => {
  const game = byKey.turncraft;
  assert.equal(game.route, '/game/turncraft.html');
  assert.deepEqual(game.modes, ['single', 'multi']);
  assert.equal(game.minPlayers, 2); assert.equal(game.maxPlayers, 3); assert.equal(game.multiplayerRoute, '/game/turncraft-lobby.html'); assert.equal(game.autoRedirect, true);
  const harness = cardHarness(), card = harness.create();
  assert.equal(card.dataset.gameKey, 'turncraft');
  assert.match(card.className, /\btc-tile\b/);
  assert.match(card.innerHTML, /aria-pressed="true"/);
  assert.match(card.innerHTML, /7 plays/);
  let propagationStopped = false;
  await card.favorite.emit('click', {stopPropagation() { propagationStopped = true; }});
  await card.actions[0].emit('click'); await card.actions[1].emit('click');
  assert.equal(propagationStopped, true);
  assert.deepEqual(harness.calls, [['favorite', 'turncraft'], ['single', game, true], ['online', game, true]]);
  assert.match(upgraded, /<link rel="stylesheet" href="\.\/turncraft-tile\.css\?v=1">/);
});

test('TurnCraft uses tracked faction artwork immediately and caches failed remote uploads', async () => {
  const harness = cardHarness(), first = harness.create(), second = harness.create();
  for (const faction of ['frontier', 'ascendant', 'brood']) {
    assert.match(first.innerHTML, new RegExp(`/game/turncraft/assets/factions/${faction}\\.webp`));
  }
  assert.equal(first.cover.hidden, true); assert.equal(second.cover.hidden, true);
  assert.equal(harness.probes.length, 1, 'multiple cards share a single remote probe');
  assert.match(harness.probes[0].src, /\/effects\/ui\/turncraft-lobby\.webp$/);
  await harness.probes[0].emit('error');
  const later = harness.create(); await Promise.resolve();
  assert.equal(harness.probes.length, 1, 'favorite/search rerenders must not retry a missing upload');
  assert.equal(later.cover.src, undefined); assert.equal(later.cover.hidden, true);
  await later.portraits[0].emit('error'); assert.equal(later.portraits[0].hidden, true);
  const other = harness.create(byKey.warrealms);
  assert.equal(other.className, 'game-card'); assert.match(other.innerHTML, /Original art/);
  assert.equal(harness.probes.length, 1, 'other games never request TurnCraft artwork');
});

test('TurnCraft reveals its new cover only after the actual card image loads', async () => {
  const harness = cardHarness(), card = harness.create();
  await harness.probes[0].emit('load'); await Promise.resolve();
  assert.equal(card.cover.src, harness.probes[0].src);
  assert.equal(card.cover.hidden, true, 'the fallback remains visible while the card image decodes');
  await card.cover.emit('load');
  assert.equal(card.cover.hidden, false); assert.equal(card.classes.has('tc-cover-ready'), true);
  const second = harness.create(); await Promise.resolve();
  assert.equal(harness.probes.length, 1); assert.equal(second.cover.src, card.cover.src);
});

test('TurnCraft suppresses duplicate launches and restores both actions after failure', async () => {
  const harness = cardHarness(); let rejectLaunch;
  harness.hooks.playSingle = () => new Promise((resolve, reject) => { rejectLaunch = reject; });
  const card = harness.create(), launching = card.actions[0].emit('click');
  assert.ok(card.actions.every(button => button.disabled));
  await card.actions[1].emit('click'); assert.deepEqual(harness.calls, []);
  const originalError = console.error; console.error = () => {};
  try { rejectLaunch(new Error('Fixture launch failure')); await launching; }
  finally { console.error = originalError; }
  assert.ok(card.actions.every(button => !button.disabled));
  assert.deepEqual(harness.calls, [['toast', 'Could not open that game. Please try again.', 'bad']]);
  await card.actions[1].emit('click'); assert.deepEqual(harness.calls.at(-1), ['online', byKey.turncraft, true]);
});
