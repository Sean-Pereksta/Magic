const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../game/neon-arena/neon-assets.js'), 'utf8');
function assets(config = {}) {
  const sandbox = {NEON_ASSET_CONFIG: config, URL, console: {warn(){}}, module:{exports:{}}};
  vm.runInNewContext(source, sandbox);
  return sandbox.NeonAssets;
}
test('missing hosting keeps the procedural fallback and causes no relative asset requests', () => {
  const api = assets();
  assert.equal(api.baseURL, '');
  assert.equal(api.url('weapons/assault.glb'), '');
});
test('one configured root preserves nested folder paths and versions all assets', () => {
  const api = assets({ASSET_BASE_URL:'https://assets.example.test/neon-breach/', version:'2 beta'});
  assert.equal(api.url(api.weapons.assault), 'https://assets.example.test/neon-breach/weapons/assault.glb?v=2%20beta');
  assert.equal(api.url(api.characters.alien_rammerBoss), 'https://assets.example.test/neon-breach/characters/alien_rammerBoss.glb?v=2%20beta');
  assert.equal(Object.keys(api.weapons).length, 14);
  assert.equal(Object.keys(api.characters).length, 21);
});
test('malformed roots and paths are rejected without a download', () => {
  for (const root of ['javascript:alert(1)', 'https://user:secret@example.test/', 'https://example.test/?x=1', 'https://example.test/#hash']) assert.equal(assets({ASSET_BASE_URL:root}).baseURL, '');
  const api = assets({ASSET_BASE_URL:'https://example.test'});
  for (const path of ['ui/../secrets', 'ui/./logo.png', '/ui/logo.png', 'https://other.test/a', 'ui/logo.png?override=1']) assert.equal(api.url(path), '');
});
