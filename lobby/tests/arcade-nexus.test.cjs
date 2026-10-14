const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {execFileSync} = require('node:child_process');
const root = path.resolve(__dirname, '../..');
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const library = require('../lobby-library.js');
const config = require('../lobby-showcase.js');

async function catalog() {
  let output;
  const script = read('lobby/lobby.html').match(/<script>\s*([\s\S]*?)<\/script>/)[1];
  await vm.runInNewContext(script, {
    fetch: async () => ({ok:true, text:async () => read('lobby/lobby-core.html')}),
    GameHubLibrary:library,
    document:{open(){}, write(html){output=html;}, close(){}}, console
  });
  return vm.runInNewContext('('+output.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]+')');
}

test('every effective catalog route and alternate action points to a tracked file', async () => {
  const games = await catalog();
  const tracked = new Set(execFileSync('git', ['ls-tree','-r','--name-only','HEAD'], {cwd:root,encoding:'utf8'}).trim().split('\n'));
  for (const game of games) for (const field of ['route','singleRoute','multiplayerRoute','dmRoute','downloadRoute']) {
    if (!game[field]) continue;
    const name = decodeURIComponent(game[field].split('?')[0]).replace(/^\//, '');
    assert.ok(tracked.has(name) || tracked.has(name+'index.html'), `${game.key}: missing ${field} ${name}`);
  }
  assert.equal(new Set(games.map(game => game.key)).size, games.length);
  for (const key of ['circuitbound','fantasyfootball','mtgcatalog']) assert.ok(games.some(game => game.key===key));
});

test('editorial configuration references the registry without duplicating gameplay fields', async () => {
  const games = await catalog();
  const keys = new Set(games.map(game => game.key));
  const picks = [...config.flagships,...config.premium,...config.builtOut];
  assert.equal(new Set(picks.map(pick => pick.key)).size,picks.length);
  for (const pick of picks) {
    assert.ok(keys.has(pick.key), `unknown featured key ${pick.key}`);
    assert.doesNotMatch(games.find(game => game.key===pick.key).title,/\(Incomplete\)/i);
    for (const field of ['route','modes','requiresUser','launchMode']) assert.equal(pick[field],undefined);
  }
});

test('every registered game has ready, nonempty tile and thumbnail assets', async () => {
  const manifest = JSON.parse(read('lobby/assets/game-artwork.json'));
  for (const game of await catalog()) {
    assert.ok(Object.hasOwn(manifest.games,game.key), `missing artwork entry ${game.key}`);
    const art = manifest.games[game.key];
    assert.equal(art.ready,true, `unfinished artwork ${game.key}`);
    for (const variant of ['tile','thumb','hero']) {
      const name = path.join(root,'lobby/assets',art[variant]);
      assert.ok(fs.existsSync(name), `missing ${game.key} ${variant}`);
      assert.ok(fs.statSync(name).size>1000, `empty ${game.key} ${variant}`);
    }
  }
  assert.ok(fs.existsSync(path.join(root,'lobby/assets',manifest.fallback)));
});

test('artwork uses CDN configuration, responsive sizes, escaping and an original fallback', () => {
  const sandbox = {GameHubShowcase:{assetBase:'https://assets.example.test/arcade/'},GameHubArtworkManifest:{fallback:'game-tiles/arcade-fallback.webp',games:{known:{tile:'game-tiles/known.webp',thumb:'game-thumbnails/known.webp',hero:'game-heroes/known.webp'}}}};
  vm.runInNewContext(read('lobby/lobby-artwork.js'),sandbox);
  const api = sandbox.GameHubArtwork;
  assert.equal(api.image({key:'known'},'hero'),'https://assets.example.test/arcade/game-heroes/known.webp');
  assert.match(api.visual({key:'known'},'art'),/srcset=.*240w.*640w.*1280w/);
  assert.match(api.visual({key:'known'},'tiny'),/height="135"/);
  assert.match(api.visual({key:'new-game'},'art'),/https:\/\/assets.example.test\/arcade\/game-tiles\/arcade-fallback.webp/);
  assert.doesNotMatch(api.visual({key:'new-game',imagePath:'/safe" onload="bad'},'art'),/src="\/safe" onload="bad/);
  sandbox.GameHubShowcase.artworkOverrides={known:{hero:'campaign/chapter-two.webp'}};
  assert.equal(api.image({key:'known'},'hero'),'https://assets.example.test/arcade/campaign/chapter-two.webp');
});
