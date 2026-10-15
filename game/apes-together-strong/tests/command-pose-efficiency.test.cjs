'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');

test('one authored pose resolves its command once and reacts to changes at the same simulation time', () => {
  const c = vm.createContext({ console }); c.window = c; c.ATSRenderer = class {};
  for (const name of ['command-animation', 'character-art']) vm.runInContext(fs.readFileSync(path.join(__dirname, '..', name + '.js'), 'utf8'), c);
  let reads = 0, assigned = null;
  const a = { id: 'ape-efficiency', species: 'gorilla', state: 'follow', hp: 100, maxHp: 100, moving: true, age: 240 };
  Object.defineProperty(a, 'armyOrder', { get() { reads++; return assigned; } });
  const g = { time: 1, siege: { groups: { gorilla: { id: 'active-group', orders: [{ type: 'hold' }] } } } };
  let pose = c.ATSCharacterArt.resolve(a, 1, { game: g });
  assert.equal(reads, 1, 'tempo and response reuse the already resolved follow command');
  assert.equal(pose.commandOrder, 'follow');
  reads = 0; assigned = { group: 'active-group', index: 0 }; a.moving = false;
  pose = c.ATSCharacterArt.resolve(a, 1, { game: g });
  assert.equal(reads, 3, 'one army lookup reads assignment, group and index once');
  assert.equal(pose.commandGuard, true);
  g.siege.groups.gorilla.orders[0].type = 'charge'; a.moving = true; reads = 0;
  pose = c.ATSCharacterArt.resolve(a, 1, { game: g });
  assert.equal(reads, 3); assert.equal(pose.commandOrder, 'charge'); assert.equal(pose.state, 'sprint');
});
