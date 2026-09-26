import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { houseFromLabel, installCorrespondence } from '../correspondence-ui.mjs';

const ids = ['ashen','wintermere','thornwall','sunspire','vesper','redharbor','stormholt','goldmere','ravenfell','oakwarden','dawnreach','saltwynd'];
const houses = ids.map(id => ({id, name: `House ${id}`}));

test('all twelve rendered local House labels resolve by exact identity', () => {
  for (const house of houses) assert.equal(houseFromLabel(`YOU · ${house.name.toUpperCase()}`, houses), house);
});
test('foreign labels and IDs resolve without guessing the player House', () => {
  assert.equal(houseFromLabel(' House Wintermere ', houses)?.id, 'wintermere');
  assert.equal(houseFromLabel('vesper', houses)?.id, 'vesper');
  assert.equal(houseFromLabel('Unknown ruler', houses), null);
  assert.equal(houseFromLabel('', houses), null);
  assert.equal(houseFromLabel('House Ashen and Vesper', houses), null);
});
test('pages without the council are left untouched', () => {
  assert.equal(installCorrespondence({getElementById: () => null}), undefined);
});
test('optional portrait registry uses the shared R2 path resolver and cannot delay startup', async () => {
  const code = await readFile(new URL('../asset-manifest.mjs', import.meta.url), 'utf8');
  assert.match(code, /portraits: Object\.fromEntries\(CAMPAIGN_HOUSES\.map/);
  assert.match(code, /`portraits\/\$\{h\.id\}\.png`/);
  assert.match(code, /Object\.entries\(IRON_THRONES_ART\.portraits\).*ironThronesAsset/);
  assert.match(code, /startupPaths=ALL_ART_PATHS\.filter\(path=>!path\.startsWith\('portraits\/'\)\)/);
});
test('entry points enhance the existing app instead of replacing it', async () => {
  const html = await readFile(new URL('../index.html', import.meta.url), 'utf8');
  assert.equal((html.match(/src="\.\/correspondence\.mjs"/g) || []).length, 1);
  assert.equal((html.match(/src="\.\/app\.mjs"/g) || []).length, 1);
  for (const id of ['messages','proposals','offer-form','council-records','gemini-diagnostics']) {
    assert.equal((html.match(new RegExp(`id="${id}"`, 'g')) || []).length, 1);
  }
});
test('presentation adapter has no engine, network, storage, or ratification calls', async () => {
  const code = await readFile(new URL('../correspondence-ui.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(code, /\b(?:commitDeal|evaluateDeal|fetch|localStorage|sessionStorage)\s*\(/);
  assert.doesNotMatch(code, /(?:data-ratify|data-human-accept).*\.click\(/);
});
