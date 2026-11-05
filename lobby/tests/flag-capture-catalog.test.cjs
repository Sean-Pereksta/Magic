const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const library = require('../lobby-library.js');
const showcase = require('../lobby-showcase.js');
const read = file => fs.readFileSync(path.join(__dirname, '..', file), 'utf8');

async function effectiveRegistry() {
  let output;
  const loader = read('lobby.html').match(/<script>\s*([\s\S]*?)<\/script>/)[1];
  await vm.runInNewContext(loader, {
    fetch: async () => ({ok: true, text: async () => read('lobby-core.html')}),
    GameHubLibrary: library,
    document: {open() {}, write(html) {output = html;}, close() {}}, console
  });
  return vm.runInNewContext('(' + output.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1] + ')');
}

test('Flag Capture uses its existing standalone Play route for AI and same-screen battles', async () => {
  const flags = (await effectiveRegistry()).filter(game => game.key === 'flag');
  assert.equal(flags.length, 1);
  const game = flags[0];
  assert.equal(game.title, 'Flag Capture: Riftbound');
  assert.equal(game.category, 'Strategy');
  assert.match(game.desc, /cooldown/i);
  assert.match(game.desc, /AI/);
  assert.match(game.desc, /same.screen|one screen|local.{0,12}2/i);
  assert.equal(game.route, '/game/Flag.html');
  assert.deepEqual(Array.from(game.modes), ['single']);
  assert.equal(game.requiresUser, undefined);
  const actions = library.getActions(game);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].type, 'single');
  // Local two-player shares this page; it does not add an online lobby action.
  assert.throws(() => library.runAction(game, 'lobby', {}), /does not support/);
  const launches = [];
  await library.runAction(game, 'single', {playSingle: (selected, userInitiated) => launches.push({route: selected.route, userInitiated})});
  assert.deepEqual(launches, [{route: '/game/Flag.html', userInitiated: true}]);
});

function artworkHarness() {
  const sandbox = {GameHubShowcase: showcase};
  vm.createContext(sandbox);
  vm.runInContext(read('assets/game-artwork.js'), sandbox);
  vm.runInContext(read('lobby-artwork.js'), sandbox);
  return {manifest: sandbox.GameHubArtworkManifest, artwork: sandbox.GameHubArtwork};
}

function cardHarness(artwork) {
  class Element {
    constructor() {
      this.dataset = {}; this.disabled = false; this.listeners = new Map();
      this.style = {setProperty() {}};
      this.classList = {add() {}, remove() {}, toggle() {}};
    }
    addEventListener(type, listener) {this.listeners.set(type, listener);}
    async emit(type, event = {}) {await this.listeners.get(type)?.(event);}
    set innerHTML(value) {
      this.markup = value; this.favorite = new Element();
      this.actions = [...value.matchAll(/data-game-action="([^"]+)"/g)].map(match => {
        const button = new Element(); button.dataset.gameAction = match[1]; return button;
      });
    }
    get innerHTML() {return this.markup;}
    querySelector(selector) {return selector === '.favorite-btn' ? this.favorite : null;}
    querySelectorAll(selector) {return selector === '[data-game-action]' ? this.actions : [];}
  }
  const calls = [];
  const hooks = {
    document: {createElement: () => new Element()},
    escapeHtml: value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    gameColor: () => '#55dfbd', gameVisual: (game, variant) => artwork.visual(game, variant),
    favorites: new Set(['flag']), gamePlayCounts: {flag: 4},
    toggleFavorite: key => calls.push(['favorite', key]),
    playSingle: (game, userInitiated) => calls.push(['play', game.key, game.route, userInitiated]),
    openCreateLobby: key => calls.push(['lobby', key]),
    joinOnlineGame: game => calls.push(['online', game.key]), toast: message => calls.push(['toast', message])
  };
  return {calls, create: game => library.createGameCard(game, hooks)};
}

test('Flag Capture artwork metadata describes the current tactical game and agrees across both manifests', async () => {
  const {manifest} = artworkHarness(), json = JSON.parse(read('assets/game-artwork.json'));
  assert.deepEqual(JSON.parse(JSON.stringify(manifest)), json);
  const game = (await effectiveRegistry()).find(game => game.key === 'flag'), art = json.games.flag;
  assert.equal(art.title, game.title); assert.equal(art.category, 'Strategy'); assert.equal(art.route, game.route);
  assert.deepEqual(art.modes, ['single']); assert.equal(art.ready, true);
  assert.doesNotMatch(art.title + ' ' + art.desc, /Incomplete|compact action game/i);
  for (const text of [game.desc, art.desc]) {
    assert.match(text, /21\s*troops/i); assert.match(text, /formation/i); assert.match(text, /ground/i);
  }
  assert.match(game.imagePath, /flag-riftbound-v2\.(?:webp|png)$/);
});

test('tile, thumbnail, hero and responsive sources all resolve to the new cartoon cover', async () => {
  const {artwork, manifest} = artworkHarness(), game = (await effectiveRegistry()).find(game => game.key === 'flag');
  const variants = {tile:'game-tiles', thumb:'game-thumbnails', hero:'game-heroes'};
  for (const [variant, directory] of Object.entries(variants)) {
    assert.equal(manifest.games.flag[variant], `${directory}/flag-riftbound-v2.webp`);
    assert.equal(artwork.image(game, variant), `/lobby/assets/${directory}/flag-riftbound-v2.webp`);
  }
  const cardArt = artwork.visual(game, 'art');
  assert.match(cardArt, /game-thumbnails\/flag-riftbound-v2\.webp 240w/);
  assert.match(cardArt, /game-tiles\/flag-riftbound-v2\.webp 640w/);
  assert.match(cardArt, /game-heroes\/flag-riftbound-v2\.webp 1280w/);
  assert.doesNotMatch(cardArt, /(?:game-tiles|game-thumbnails)\/flag\.webp/);
  assert.match(artwork.visual(game, 'tiny'), /src="\/lobby\/assets\/game-thumbnails\/flag-riftbound-v2\.webp"/);
});

test('the rebuilt tactical card keeps one Play action and its existing favorite identity', async () => {
  const games = await effectiveRegistry(), game = games.find(game => game.key === 'flag');
  const {artwork} = artworkHarness(), harness = cardHarness(artwork), card = harness.create(game);
  assert.equal(card.dataset.gameKey, 'flag'); assert.match(card.className, /\bfc-tile\b/);
  for (const copy of [/COOLDOWN TACTICS/, /Lumen\s*Brood/i, /Crooked\s*Carnival/i, /Aureate\s*Choir/i, /21\s*troops/i, /formation/i, /ground/i, /campaign/i]) assert.match(card.innerHTML, copy);
  assert.match(card.innerHTML, /AI\s*\/\s*local\s*2P/i); assert.match(card.innerHTML, /aria-pressed="true"/);
  assert.match(card.innerHTML, /sizes="auto, \(max-width:600px\) 92vw/);
  assert.deepEqual(card.actions.map(button => button.dataset.gameAction), ['single']);
  let stopped = false; await card.favorite.emit('click', {stopPropagation() {stopped = true;}});
  await card.actions[0].emit('click');
  assert.equal(stopped, true);
  assert.deepEqual(harness.calls, [['favorite','flag'], ['play','flag','/game/Flag.html',true]]);
  const neighbor = harness.create(games.find(game => game.key === 'warrealms'));
  assert.equal(neighbor.className, 'game-card');
  assert.doesNotMatch(neighbor.innerHTML, /fc-factions|fc-feature|COOLDOWN TACTICS|Aureate\s*Choir/);
  assert.deepEqual(neighbor.actions.map(button => button.dataset.gameAction), ['single','lobby']);
});

function webpDimensions(buffer) {
  assert.equal(buffer.toString('ascii', 0, 4), 'RIFF'); assert.equal(buffer.toString('ascii', 8, 12), 'WEBP');
  for (let offset = 12; offset + 8 <= buffer.length;) {
    const type = buffer.toString('ascii', offset, offset + 4), length = buffer.readUInt32LE(offset + 4), start = offset + 8;
    if (type === 'VP8X') return [buffer.readUIntLE(start + 4, 3) + 1, buffer.readUIntLE(start + 7, 3) + 1];
    if (type === 'VP8L') {const bits = buffer.readUInt32LE(start + 1); return [(bits & 0x3fff) + 1, ((bits >>> 14) & 0x3fff) + 1];}
    if (type === 'VP8 ') return [buffer.readUInt16LE(start + 6) & 0x3fff, buffer.readUInt16LE(start + 8) & 0x3fff];
    offset = start + length + (length & 1);
  }
  assert.fail('The cover must contain a supported WebP image frame.');
}

test('new cover variants exist at the lobby responsive sizes and retain a widescreen composition', () => {
  const {manifest} = artworkHarness();
  for (const [variant, width, height] of [['thumb',240,135], ['tile',640,360], ['hero',1280,720]]) {
    const buffer = fs.readFileSync(path.join(__dirname, '..', 'assets', manifest.games.flag[variant]));
    assert.deepEqual(webpDimensions(buffer), [width,height], variant);
  }
});

test('the new title does not silently change existing editorial placement', () => {
  const sandbox = {GameHubShowcase: showcase, module: {exports: {}}};
  vm.runInNewContext(read('lobby-catalog-organization.js'), sandbox);
  assert.equal(sandbox.module.exports.statusForTitle('Flag Capture: Riftbound', 'flag'), 'prototype');
  assert.ok(![...showcase.flagships, ...showcase.premium, ...showcase.builtOut].some(game => game.key === 'flag'));
});
