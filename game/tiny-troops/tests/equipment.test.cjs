const test = require('node:test');
const assert = require('node:assert/strict');
const E = require('../equipment-rules.js');
const R = require('../core.js');
function unit(n = 'Archer', t = ['Blade'], star = 1) { return { n, e: '🏹', t, star, team: 'ally', baseAtk: 20, atk: 20, baseMaxHp: 100, maxHp: 100, hp: 100, baseSpd: 1, spd: 1, focus: 'near' }; }
function fixture() { const state = { tt: R.newRun({}, 10), round: 1, phase: 'recruit', squad: [unit(), unit('Mortar Tortoise', ['Machine', 'Stone'])], bench: [unit('Medic', ['Healer'])] }; E.migrate(state); return state; }
test('the full 18-item catalog has exact baseline odds and compatible native defender roles', () => {
  assert.equal(E.ITEMS.length, 18); assert.equal(new Set(E.ITEMS.map(item => item.id)).size, 18);
  assert.deepEqual(E.RARITY_WEIGHTS, [52, 28, 14, 5, 1]); assert.deepEqual(E.DROP_CHANCES, { normal: .12, elite: .35, boss: 1 });
  assert.equal(E.compatible('watchers-lens', unit()), true);
  assert.equal(E.compatible('guardian-charm', unit()), false);
  assert.equal(E.compatible('guardian-charm', unit('Medic', ['Healer'])), true);
  assert.equal(E.compatible('blizzard-core', unit('Frost Archer', ['Frost'])), true);
  assert.equal(E.compatible('blizzard-core', unit('Squire', ['Guard'])), false);
  assert.equal(E.compatible('flame-oil', { n: 'Enemy', team: 'enemy' }), false);
});
test('unique copies equip, replace, unequip, and cannot occupy two defenders', () => {
  const state = fixture(), [first, second] = state.squad;
  const bolts = E.addItem(state, 'sharpened-bolts'), speed = E.addItem(state, 'reinforced-gears');
  assert.ok(E.equip(state, first.ttId, 0, bolts.id).ok);
  assert.equal(E.equip(state, second.ttId, 0, bolts.id).ok, false);
  const replacement = E.equip(state, first.ttId, 0, speed.id);
  assert.equal(replacement.replaced, bolts); assert.equal(bolts.towerId, null);
  assert.equal(E.equipped(state, first).length, 1);
  assert.ok(E.unequip(state, first.ttId, 0).ok); assert.equal(speed.towerId, null);
  assert.equal(E.unequip(state, first.ttId, 0).ok, false);
  assert.equal(state.tt.equipment.items.length, 2);
});
test('locked slots, incompatible items, and combat actions leave inventory unchanged', () => {
  const state = fixture(), tower = state.squad[0], charm = E.addItem(state, 'guardian-charm'), bolts = E.addItem(state, 'sharpened-bolts');
  const before = JSON.stringify(state.tt.equipment);
  assert.equal(E.equip(state, tower.ttId, 1, bolts.id).ok, false);
  assert.equal(E.equip(state, tower.ttId, 0, charm.id).ok, false);
  state.phase = 'battle'; assert.equal(E.equip(state, tower.ttId, 0, bolts.id).ok, false);
  assert.equal(JSON.stringify(state.tt.equipment), before);
  state.phase = 'recruit'; tower.star = 5; assert.equal(E.slotsFor(tower), 2); assert.ok(E.equip(state, tower.ttId, 1, bolts.id).ok);
});
test('sale and permanent removal recover gear; native KO and bench movement retain gear', () => {
  const state = fixture(), tower = state.squad[0], bolts = E.addItem(state, 'sharpened-bolts'); E.equip(state, tower.ttId, 0, bolts.id);
  tower.dead = true; tower.hp = 0; E.reconcile(state); assert.equal(bolts.towerId, tower.ttId);
  state.squad[0] = null; state.bench.push(tower); E.reconcile(state); assert.equal(bolts.towerId, tower.ttId);
  state.bench.pop(); assert.deepEqual(E.reconcile(state), [bolts.id]); assert.equal(bolts.towerId, null); assert.equal(state.tt.equipment.items.length, 1);
});
test('migration preserves existing roster/progress, repairs duplicate copies, and frees invalid assignments', () => {
  const state = fixture(), roster = JSON.stringify(state.squad), id = state.squad[0].ttId;
  state.tt.equipment = { items: [
    { id: 'duplicate', item: 'sharpened-bolts', towerId: id, slot: 0 },
    { id: 'duplicate', item: 'reinforced-gears', towerId: id, slot: 0 },
    { id: 'missing', item: 'stone-plating', towerId: 'removed', slot: 0 }
  ] };
  E.migrate(state);
  assert.equal(JSON.stringify(state.squad), roster); assert.equal(state.tt.equipment.items.length, 3);
  assert.equal(new Set(state.tt.equipment.items.map(item => item.id)).size, 3);
  assert.equal(state.tt.equipment.items.filter(item => item.towerId).length, 1);
  assert.equal(state.tt.equipment.items[2].towerId, null);
  E.migrate(state); assert.equal(state.tt.equipment.items.length, 3);
});
test('first victory guarantees introduction and repeated callbacks cannot grant duplicate loot', () => {
  const state = fixture(); let calls = 0;
  const first = E.reward(state, { wave: 1 }, () => { calls++; return .999; });
  assert.ok(first.awarded); assert.ok(first.introductory); assert.equal(first.instance.item, 'sharpened-bolts'); assert.equal(calls, 0);
  const same = E.reward(state, { wave: 1, activity: 'boss' }, () => 0);
  assert.ok(same.duplicate); assert.equal(state.tt.equipment.items.length, 1);
  const serialized = JSON.parse(JSON.stringify(state)); E.migrate(serialized);
  assert.ok(E.reward(serialized, { wave: 1 }, () => 0).duplicate); assert.equal(serialized.tt.equipment.items.length, 1);
});
test('12/35/100 percent thresholds and seven-miss bad-luck protection', () => {
  for (const [activity, chance] of Object.entries(E.DROP_CHANCES)) {
    const state = fixture(); state.tt.equipment.introClaimed = true;
    assert.equal(E.reward(state, { wave: 1, activity }, () => chance === 1 ? .999999 : chance - .00001).awarded, true);
    if (chance < 1) { const other = fixture(); other.tt.equipment.introClaimed = true; assert.equal(E.reward(other, { wave: 1, activity }, () => chance).awarded, false); }
  }
  const state = fixture(); E.reward(state, { wave: 1 }, () => .99);
  for (let wave = 2; wave <= 8; wave++) assert.equal(E.reward(state, { wave }, () => .99).awarded, false);
  const protectedDrop = E.reward(state, { wave: 9 }, () => .99);
  assert.equal(protectedDrop.awarded, true); assert.equal(protectedDrop.protectedDrop, true); assert.equal(state.tt.equipment.misses, 0);
  assert.equal(E.reward(state, { wave: 10, activity: 'boss' }, () => 1).awarded, true, 'boss loot is guaranteed even with a boundary-valued RNG');
});
test('rarity distribution boundaries are deterministic, all named categories remain inspectable', () => {
  for (const [roll, rarity] of [[0, 'common'], [.519999, 'common'], [.52, 'uncommon'], [.8, 'rare'], [.94, 'epic'], [.99, 'legendary']]) {
    let call = 0; assert.equal(E.rollItem(() => call++ === 0 ? roll : .3).rarity, rarity);
  }
  const state = fixture(); E.ITEMS.forEach(item => E.addItem(state, item.id));
  assert.equal(E.filterItems(state, { rarity: 'legendary' }).length, 2);
  assert.equal(E.filterItems(state, { category: 'control' }).length, 2);
  assert.ok(E.filterItems(state, { compatibility: true }, state.squad[0]).length < 18);
});
test('preview is read-only and recovery restores both loot transaction ledger and inventory together', () => {
  const state = fixture(), tower = state.squad[0], speed = E.addItem(state, 'swift-trigger');
  const before = JSON.stringify(state), preview = E.preview(state, tower, { slot: 0, item: speed.item });
  assert.equal(preview.speed, 1.12); assert.equal(JSON.stringify(state), before);
  const checkpoint = JSON.parse(before); E.reward(state, { wave: 1 }, () => .9);
  const recovered = JSON.parse(JSON.stringify(checkpoint)); E.migrate(recovered);
  assert.equal(recovered.tt.equipment.items.length, 1);
  E.reward(recovered, { wave: 1 }, () => .9); E.reward(recovered, { wave: 1 }, () => 0);
  assert.equal(recovered.tt.equipment.items.length, 2);
});
