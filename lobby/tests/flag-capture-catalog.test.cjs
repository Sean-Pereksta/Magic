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
  assert.match(game.desc, /cooldown-based.*AI/);
  assert.match(game.desc, /friend on one screen/);
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

test('the new title does not silently change existing editorial placement', () => {
  const sandbox = {GameHubShowcase: showcase, module: {exports: {}}};
  vm.runInNewContext(read('lobby-catalog-organization.js'), sandbox);
  assert.equal(sandbox.module.exports.statusForTitle('Flag Capture: Riftbound', 'flag'), 'prototype');
  assert.ok(![...showcase.flagships, ...showcase.premium, ...showcase.builtOut].some(game => game.key === 'flag'));
});
