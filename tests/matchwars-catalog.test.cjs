const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
function catalog(withConfig = true) {
  const context = vm.createContext({});
  if (withConfig) vm.runInContext(read('lobby/lobby-showcase.js'), context);
  context.module = { exports: {} };
  vm.runInContext(read('lobby/lobby-catalog-organization.js'), context);
  return { config: context.GameHubShowcase, api: context.module.exports };
}

test('Match Wars is promoted to the Complete game shelf with live showcase config loaded', () => {
  const { api, config } = catalog();
  const entries = [...config.flagships, ...config.premium, ...config.builtOut].filter(item => item.key === 'matchwars');
  assert.equal(entries.length, 1);
  assert.equal(api.statusForTitle('Match Wars', 'matchwars'), 'showcase');
  assert.equal(api.statusMeta.showcase.label, 'Complete');
});

test('fallback catalog also recognizes Match Wars as a complete game', () => {
  assert.equal(catalog(false).api.statusForTitle('Match Wars', 'matchwars'), 'showcase');
});

test('the promoted listing preserves its existing route, category, and single-player mode', () => {
  const source = read('lobby/lobby-core.html');
  const entrySource = source.match(/\{key:"matchwars",[^\n]+\}/)?.[0];
  assert.ok(entrySource, 'Match Wars listing must exist');
  const entry = vm.runInNewContext(`(${entrySource})`);
  assert.equal(entry.key, 'matchwars');
  assert.equal(entry.title, 'Match Wars');
  assert.equal(entry.route, '/game/matchwars.html');
  assert.equal(entry.category, 'Arcade');
  assert.deepEqual(Array.from(entry.modes), ['single']);
  assert.match(entry.desc, /elemental/);
  assert.match(entry.desc, /animated/);
});
