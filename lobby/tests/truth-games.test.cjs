const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');
const flagships = require('../lobby-flagships.js');
const arcade = require('../lobby-arcade.js');
const root = path.join(__dirname, '..');
const core = fs.readFileSync(path.join(root, 'lobby-core.html'), 'utf8').replace(/\r\n/g, '\n');
const loader = fs.readFileSync(path.join(root, 'lobby.html'), 'utf8').match(/<script>\s*([\s\S]*?)<\/script>/)[1];
const expected = {
  warofthegoldhouses: 'war-of-the-gold-houses',
  critteratlas: 'critter-atlas',
  acornrally: 'acorn-rally',
  bannerlands: 'bannerlands',
  monstermallexpanded: 'monster-mall-expanded',
  pacmanblackout: 'pacman',
  pirates: 'pirates',
  academystandards: 'academy-standards',
  operationwhiskerfall: 'operation-whiskerfall'
};

async function load(input = core) {
  let written = '';
  await vm.runInNewContext(loader, {
    fetch: async () => ({ok: true, text: async () => input}),
    GameHubLibrary: library, GameHubFlagships: flagships, GameHubArcade: arcade,
    document: {open() {}, write(html) {written = html;}, close() {}, body: {}},
    console: {warn() {}, error(error) {throw error;}}
  });
  assert.ok(written, 'the lobby must render successfully');
  return written;
}

function registry(html) {
  return vm.runInNewContext(html.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]);
}

test('Truth games join the shared catalog exactly once', async () => {
  const html = await load();
  for (const output of [html, await load(html)]) {
    const games = registry(output);
    for (const [key, slug] of Object.entries(expected)) {
      const matches = games.filter(game => game.key === key);
      assert.equal(matches.length, 1, key);
      assert.equal(matches[0].route, `https://sean-pereksta.github.io/Truth/games/${slug}/`);
      const actions = key === 'acornrally' ? [
        {type: 'single', label: 'Play in Browser'},
        {type: 'online', label: 'Multiplayer'}
      ] : key === 'bannerlands' ? [
        {type: 'single', label: 'Play in Browser'},
        {type: 'live', label: 'Live Multiplayer', href: '#live-lobbies'}
      ] : [{type: 'single', label: 'Play'}];
      assert.deepEqual(library.getActions(matches[0]), actions);
      assert.ok(!matches[0].requiresUser, `${key} remains playable as a guest`);
    }
    assert.match(output, /let games=GAMES\.filter\(/);
  }
});

test('the three Windows host editions preserve their guest routes and open hosting directly', async () => {
  const html = await load();
  const games = registry(html);
  const cat = games.find(game => game.key === 'cat');
  const banner = games.find(game => game.key === 'bannerlands');
  const trash = games.find(game => game.key === 'trashtactics');
  assert.equal(cat.singleRoute, '/game/catandmouse/edition/index.html');
  assert.equal(cat.route, '/game/catandmouse.html');
  assert.match(cat.paintedTile, /\/game\/catandmouse\/edition\/art\/.*\.webp$/);
  assert.equal(cat.localEditionRoute, '/game/catandmouse/edition/download.html');
  assert.equal(banner.localEditionRoute, 'https://sean-pereksta.github.io/Truth/games/bannerlands/download.html');
  assert.equal(trash.localEditionRoute, '/game/trash-tactics/download.html');
  assert.equal(trash.multiplayerRoute, '/game/trash-tactics/?online=1');
  assert.equal(cat.hostRoute, '/game/catandmouse/edition/?host=1');
  assert.equal(banner.hostRoute, 'https://sean-pereksta.github.io/Truth/games/bannerlands/?host=1');
  assert.equal(trash.hostRoute, '/game/trash-tactics/?online=1&host=1');
  for (const output of [html, await load(html)]) {
    assert.equal(output.match(/id="live-lobbies"/g).length, 1);
    assert.equal(output.match(/src="\.\/live-lobbies\.mjs\?v=\d+"/g).length, 1);
    assert.match(output, /data-live-filter="all"/);
    for (const id of ['acornrally', 'bannerlands', 'trashtactics', 'lastsignal', 'cat']) {
      assert.ok(output.includes(`data-live-filter="${id}"`));
      assert.ok(output.includes(`data-live-section="${id}"`));
    }
  }
});

test('a user Play action navigates directly to every Truth game URL', async () => {
  const games = registry(await load());
  const functionSource = name => core.match(new RegExp(`    (?:async )?function ${name}\\([^]*?\\n    \\}`))[0];
  const launches = [];
  const context = vm.createContext({
    username: '', launchingGame: false, lobbyBrowserRefreshTimer: null,
    ALLOWED_GAME_LAUNCH_REASONS: new Set(['single-play']),
    stopCurrentLobbySafetyPolling() {}, clearInterval() {}, save() {},
    logGamePlay: async () => {}, SK: {lastGame: 'lastGame'},
    location: {assign(url) {launches.push(url);}}, console,
    toast(message) {assert.fail(message);}
  });
  vm.runInContext(functionSource('launchGameUrl') + '\n' + functionSource('playSingle'), context);
  for (const key of Object.keys(expected)) {
    context.launchingGame = false;
    const game = games.find(item => item.key === key);
    await context.playSingle(game, true);
    assert.equal(launches.at(-1), game.route);
  }
  assert.equal(launches.length, Object.keys(expected).length);
});

test('Acorn Rally opens multiplayer in-game and offers the Windows host download', async () => {
  const html = await load();
  const game = registry(html).find(item => item.key === 'acornrally');
  const launches = [];
  const sandbox = vm.createContext({
    launchingGame: false, logGamePlay: async () => {}, save() {}, SK: {lastGame: 'lastGame'}, console,
    launchGameUrl(url, reason) {launches.push({url, reason});},
    toast(message) {assert.fail(message);}
  });
  vm.runInContext(core.match(/    async function joinOnlineGame\([^]*?\n    \}/)[0], sandbox);
  await sandbox.joinOnlineGame(game, true);
  assert.deepEqual(launches, [{url: 'https://sean-pereksta.github.io/Truth/games/acorn-rally/?room=join', reason: 'matchmaking-join'}]);
  assert.equal(game.category, 'Racing');
  assert.equal(game.joinLabel, 'Multiplayer');
  assert.equal(game.localEditionRoute, 'https://sean-pereksta.github.io/Truth/games/acorn-rally/?download=pc');
  assert.equal(game.localEditionLabel, 'PC Host Download');
  const pcButton = library.localEditionButton(game, String);
  assert.equal(pcButton, '<a class="btn secondary small" href="https://sean-pereksta.github.io/Truth/games/acorn-rally/?download=pc" aria-label="PC Host Download: Acorn Rally">PC Host Download</a>');
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'assets/game-artwork.json'), 'utf8'));
  const artwork = {};
  vm.runInNewContext(fs.readFileSync(path.join(root, 'assets/game-artwork.js'), 'utf8'), artwork);
  for (const definition of [manifest.games.acornrally, artwork.GameHubArtworkManifest.games.acornrally]) {
    assert.equal(definition.localEditionRoute, game.localEditionRoute);
    assert.equal(definition.localEditionLabel, game.localEditionLabel);
    assert.equal(definition.joinLabel, game.joinLabel);
  }
});

test('each Truth game has its own generated WebP tile, thumbnail and hero', async () => {
  const games = registry(await load());
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'assets/game-artwork.json'), 'utf8'));
  const prompts = JSON.parse(fs.readFileSync(path.join(root, 'assets/truth-game-art-prompts.json'), 'utf8'));
  const sources = new Set();
  for (const key of Object.keys(expected)) {
    const game = games.find(item => item.key === key);
    const art = manifest.games[key];
    assert.match(game.imagePath, /^\/lobby\/assets\/game-tiles\/[a-z-]+-v2\.webp$/);
    sources.add(game.imagePath);
    assert.equal(art.imagePath, game.imagePath);
    assert.equal(art.provenance, 'original-built-in-imagegen');
    assert.ok(prompts.images.some(image => image.key === key && image.prompt.length > 100));
    for (const variant of ['tile', 'thumb', 'hero']) {
      const image = fs.readFileSync(path.join(root, 'assets', art[variant]));
      assert.equal(image.toString('ascii', 0, 4), 'RIFF', `${key} ${variant} has a RIFF header`);
      assert.equal(image.toString('ascii', 8, 12), 'WEBP', `${key} ${variant} is WebP`);
    }
  }
  assert.equal(sources.size, Object.keys(expected).length);
});
