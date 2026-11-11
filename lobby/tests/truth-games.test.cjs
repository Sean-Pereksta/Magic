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

test('six Truth games join the shared catalog exactly once and retain only Play', async () => {
  const html = await load();
  for (const output of [html, await load(html)]) {
    const games = registry(output);
    for (const [key, slug] of Object.entries(expected)) {
      const matches = games.filter(game => game.key === key);
      assert.equal(matches.length, 1, key);
      assert.equal(matches[0].route, `https://sean-pereksta.github.io/Truth/games/${slug}/`);
      assert.deepEqual(library.getActions(matches[0]), [{type: 'single', label: 'Play'}]);
      assert.ok(!matches[0].requiresUser, `${key} remains playable as a guest`);
    }
    assert.match(output, /let games=GAMES\.filter\(/);
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
  assert.equal(launches.length, 6);
});

test('each Truth tile uses its own local game artwork', async () => {
  const games = registry(await load());
  const sources = new Set();
  for (const key of Object.keys(expected)) {
    const game = games.find(item => item.key === key);
    assert.match(game.imagePath, /^\/lobby\/assets\/game-tiles\/[a-z-]+\.jpg$/);
    sources.add(game.imagePath);
    const image = fs.readFileSync(path.join(root, game.imagePath.replace('/lobby/', '')));
    assert.equal(image.readUInt16BE(0), 0xffd8, `${key} has a valid JPEG header`);
  }
  assert.equal(sources.size, 6);
});
