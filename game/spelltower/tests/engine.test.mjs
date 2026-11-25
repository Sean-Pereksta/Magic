import test from 'node:test';
import assert from 'node:assert/strict';
import {
  COLS, ROWS, TOWER_ROW, SPELLS, BASE_ELEMENTS, RECIPES, INTERACTIONS,
  createGame, applyAction, previewSpell, getActivePlayer, getRecipe
} from '../engine.mjs';

const fresh = (players = ['Ada'], mode = players.length > 1 ? 'hotseat' : 'solo') => createGame({ players, mode, seed: 9217 });
const enemy = (x, y, key = 'rat', hp = 100) => ({ id: `test-${key}-${x}-${y}`, key, x, y, hp, maxHP: hp, speed: 1, damage: 2,
  wet: 0, stun: 0, frozen: 0, rooted: 0, burn: 0, burnPower: 0, poison: 0, slow: 0, blinded: 0, vulnerable: 0 });
function cast(d, key, x, y, actor = getActivePlayer(d)) {
  const card = d.hands[actor].find(c => c.key === key);
  assert.ok(card, `Missing ${key}`);
  return applyAction(d, actor, { type: 'cast', cardId: card.id, x, y });
}
const end = d => applyAction(d, getActivePlayer(d), { type: 'endTurn' });
function inject(d, key, actor = getActivePlayer(d)) {
  d.hands[actor].push({ id: `test-card-${key}`, key, charges: 3, usedTurn: -1 });
  return d;
}
function assertFirestore(value, parentArray = false) {
  if (Array.isArray(value)) {
    assert.equal(parentArray, false, 'Firestore forbids arrays directly inside arrays');
    for (const item of value) assertFirestore(item, true);
  } else if (value && typeof value === 'object') {
    for (const item of Object.values(value)) assertFirestore(item, false);
  } else {
    assert.notEqual(typeof value, 'undefined');
    assert.notEqual(typeof value, 'function');
    if (typeof value === 'number') assert.ok(Number.isFinite(value));
  }
}

test('seeded runs and retried actions are deterministic and immutable', () => {
  const d = fresh(), before = structuredClone(d);
  assert.deepEqual(d, fresh());
  assert.deepEqual(end(d), end(d));
  assert.deepEqual(d, before);
  assert.deepEqual(createGame({ seed: 'a' }), createGame({ seed: 'a' }));
  assert.notDeepEqual(createGame({ seed: 'a' }), createGame({ seed: 'b' }));
});

test('all seven elements have symmetric, unambiguous mixed and self recipes', () => {
  assert.equal(COLS, 4); assert.equal(ROWS, 10); assert.equal(TOWER_ROW, 9);
  assert.equal(Object.keys(SPELLS).length, 35);
  assert.equal(RECIPES.length, 28);
  assert.equal(RECIPES.filter(r => r.a !== r.b).length, 21);
  assert.equal(new Set(RECIPES.map(r => [r.a, r.b].sort().join('+'))).size, 28);
  assert.ok(INTERACTIONS.length >= 15);
  for (const a of BASE_ELEMENTS) for (const b of BASE_ELEMENTS) {
    assert.ok(SPELLS[getRecipe(a, b)]);
    assert.equal(getRecipe(a, b), getRecipe(b, a));
  }
});

test('everyone starts with the elements needed to discover setup and detonation', () => {
  const d = fresh(['Ada', 'Bo', 'Cy']);
  for (const player of d.players) assert.deepEqual(d.hands[player].map(c => c.key), BASE_ELEMENTS);
  assert.equal(d.mana, 6);
  assert.equal(new Set(Object.values(d.hands).flat().map(c => c.id)).size, 21);
});

test('player names and seat counts are safe for JSON maps and match the available modes', () => {
  for (const name of ['__proto__', 'constructor', 'prototype', 'Ada\nBo', 'x'.repeat(33), '   ']) {
    assert.throws(() => createGame({ players: [name] }), /names/i);
    assert.throws(() => applyAction(fresh(['Ada'], 'online'), name, { type: 'join', player: name }), /valid player/i);
  }
  assert.throws(() => createGame({ players: ['A', 'B'], mode: 'solo' }), /one player/);
  assert.throws(() => createGame({ players: ['A', 'B', 'C', 'D', 'E'], mode: 'hotseat' }), /four players/);
  assert.equal(createGame({ players: ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], mode: 'online' }).players.length, 8);
});

test('cast guards work on turn zero, after reload, and against a different player', () => {
  const d = fresh(['Ada', 'Bo']);
  const cardId = d.hands.Ada.find(c => c.key === 'water').id;
  assert.throws(() => applyAction(d, 'Bo', { type: 'cast', cardId, x: 1, y: 4 }), /turn/i);
  const after = cast(d, 'water', 1, 4);
  assert.equal(after.mana, 5);
  const reloaded = JSON.parse(JSON.stringify(after));
  assert.throws(() => applyAction(reloaded, 'Ada', { type: 'cast', cardId, x: 1, y: 4 }), /once per turn/i);
  assert.throws(() => applyAction(d, 'Bo', { type: 'combine', cardIds: d.hands.Bo.slice(0, 2).map(c => c.id) }), /turn/i);
  assert.equal(d.mana, 6);
});

test('invalid targets, exhausted mana and mismatched hands leave state unchanged', () => {
  const d = fresh(), before = structuredClone(d), cardId = d.hands.Ada[0].id;
  for (const [x, y] of [[-1, 2], [4, 2], [1, 9], [1.5, 2], [1, NaN]]) {
    assert.throws(() => applyAction(d, 'Ada', { type: 'cast', cardId, x, y }), /path tile/i);
  }
  assert.throws(() => applyAction(d, 'Ada', { type: 'cast', cardId: 'missing', x: 1, y: 2 }), /hand/i);
  const spent = structuredClone(d); spent.mana = 0;
  assert.throws(() => cast(spent, 'water', 1, 2), /mana/i);
  assert.deepEqual(d, before);
});

test('lightning crosses empty connected puddles, adds damage and stuns every wet target', () => {
  let d = fresh(); d.enemies = [enemy(0, 4), enemy(2, 4), enemy(3, 7)];
  d = cast(d, 'water', 1, 4);
  const afterWater = d.enemies.map(e => e.hp);
  const preview = previewSpell(d, 'lightning', 0, 4);
  assert.ok(preview.cells.some(p => p.x === 2 && p.y === 4));
  assert.ok(preview.combos.some(t => t.includes('Conducted')));
  d = cast(d, 'lightning', 0, 4);
  assert.equal(d.enemies[0].hp, afterWater[0] - 7);
  assert.equal(d.enemies[1].hp, afterWater[1] - 7);
  assert.equal(d.enemies[0].stun, 1);
  assert.equal(d.enemies[1].stun, 1);
  assert.equal(d.enemies[2].hp, 100);
});

test('fire consumes surrounding vines for extra impact and a stronger persistent burn', () => {
  let d = fresh(); d.enemies = [enemy(1, 4)];
  d = cast(d, 'nature', 1, 4);
  const rootedHP = d.enemies[0].hp;
  d = cast(d, 'fire', 1, 4);
  assert.equal(rootedHP - d.enemies[0].hp, Math.round(8 * 1.4));
  assert.equal(d.terrain['1,4'].burnPower, 3);
  assert.equal(d.terrain['1,4'].burn, 3);
  assert.equal(d.terrain['1,4'].vines, undefined);
  assert.equal(d.enemies[0].rooted, 0);
  assert.ok(d.events.some(e => e.type === 'combo' && e.text.includes('Bramble')));
  const beforeTick = d.enemies[0].hp;
  d = end(d);
  assert.equal(beforeTick - d.enemies.find(e => e.id === 'test-rat-1-4').hp, Math.round(3 * 1.4));
});

test('wet ice freezes and earth shatters, clears control, and damages neighbors', () => {
  let d = fresh(); d.enemies = [enemy(1, 4), enemy(2, 4)];
  d = cast(d, 'water', 1, 4); d = cast(d, 'ice', 1, 4);
  assert.equal(d.enemies[0].frozen, 2);
  const firstHP = d.enemies[0].hp, secondHP = d.enemies[1].hp;
  d = cast(d, 'earth', 1, 4);
  assert.equal(firstHP - d.enemies[0].hp, 9);
  assert.equal(secondHP - d.enemies[1].hp, 2);
  assert.equal(d.enemies[0].frozen, 0);
  assert.equal(d.terrain['1,4'].ice, undefined);
});

test('rain extinguishes burning and cleanses poison in terrain and on enemies', () => {
  let d = fresh(); d.enemies = [{ ...enemy(1, 4), burn: 3, burnPower: 3, poison: 3 }];
  d.terrain['1,4'] = { burn: 3, burnPower: 3, poison: 3 };
  d = cast(d, 'water', 1, 4);
  assert.equal(d.enemies[0].burn, 0); assert.equal(d.enemies[0].poison, 0);
  assert.equal(d.terrain['1,4'].burn, undefined); assert.equal(d.terrain['1,4'].poison, undefined);
  assert.equal(d.terrain['1,4'].wet, 3); assert.equal(d.terrain['1,4'].steam, 2);
});

test('gust spreads original terrain once, pushes once, and never moves a golem', () => {
  let d = fresh(); d.enemies = [enemy(1, 5), enemy(2, 5, 'golem')];
  d.terrain['1,5'] = { burn: 3, burnPower: 3 };
  d = cast(d, 'wind', 1, 5);
  assert.equal(d.enemies.find(e => e.key === 'rat').y, 4);
  assert.equal(d.terrain['0,5'].burn, 2);
  assert.equal(d.terrain['2,5'].burn, 2);
  assert.equal(d.terrain['1,7'], undefined, 'Newly spread fire should not spread again during the same gust');
  let g = fresh(); g.enemies = [enemy(1, 5, 'golem')]; g = cast(g, 'wind', 1, 5);
  assert.equal(g.enemies[0].y, 5);
});

test('enemies and terrain tick once per completed cooperative round', () => {
  let d = fresh(['Ada', 'Bo', 'Cy']); d.enemies = [enemy(1, 4)]; d.terrain['3,7'] = { wet: 3 };
  d = end(d); assert.equal(d.enemies[0].y, 4); assert.equal(d.round, 1); assert.equal(d.terrain['3,7'].wet, 3);
  d = end(d); assert.equal(d.enemies[0].y, 4); assert.equal(d.round, 1);
  d = end(d); assert.equal(d.enemies.find(e => e.id === 'test-rat-1-4').y, 5); assert.equal(d.round, 2);
  assert.equal(d.terrain['3,7'].wet, 2); assert.equal(getActivePlayer(d), 'Ada');
});

test('movement checks every crossed tile so fast foes cannot skip vines or walls', () => {
  let d = fresh(); d.enemies = [{ ...enemy(1, 3), speed: 2 }]; d.terrain['1,4'] = { vines: 2 };
  d = end(d); assert.equal(d.enemies.find(e => e.id === 'test-rat-1-3').y, 4);
  let w = fresh(); w.enemies = [{ ...enemy(1, 3), speed: 2 }]; w.terrain['1,4'] = { stone: 3, strength: 8 };
  w = end(w); assert.equal(w.enemies.find(e => e.id === 'test-rat-1-3').y, 3); assert.equal(w.terrain['1,4'].strength, 6);
});

test('blocked entrances queue reinforcements instead of deleting a wave', () => {
  let d = fresh(); d.enemies = [0, 1, 2, 3].map(x => ({ ...enemy(x, 0), frozen: 2 }));
  d = end(d);
  assert.equal(d.enemies.length, 4); assert.equal(d.spawnQueue.length, 2);
  d = end(d);
  assert.equal(d.spawnQueue.length, 4); assert.equal(d.roundsInWave, 3);
  d.enemies = [];
  d = end(d);
  assert.equal(d.wave, 1, 'Pending reinforcements must be fought before the next wave');
  assert.equal(d.enemies.length, 4); assert.equal(d.spawnQueue.length, 0);
});

test('finite terrain expires and mana is restored for the next caster', () => {
  let d = fresh(); d.enemies = []; d.terrain['1,4'] = { wet: 1, vines: 1, ice: 1, burn: 1, burnPower: 3, stone: 1, strength: 4 };
  d.mana = 0; d = end(d);
  assert.equal(d.terrain['1,4'], undefined); assert.equal(d.mana, d.maxMana);
});

test('forging consumes two actual cards, rejects a repeated ID, and supports self upgrades in solo', () => {
  let d = fresh(); const ids = d.hands.Ada.slice(0, 2).map(c => c.id);
  assert.throws(() => applyAction(d, 'Ada', { type: 'combine', cardIds: [ids[0], ids[0]] }), /different/);
  d = applyAction(d, 'Ada', { type: 'combine', cardIds: ids });
  assert.ok(d.hands.Ada.some(c => c.key === 'steam'));
  assert.equal(d.hands.Ada.length, 6);
  assert.equal(d.mana, 6);
  d = end(d);
  const fires = d.hands.Ada.filter(c => c.key === 'fire');
  assert.equal(fires.length, 2);
  d = applyAction(d, 'Ada', { type: 'combine', cardIds: fires.map(c => c.id) });
  assert.ok(d.hands.Ada.some(c => c.key === 'inferno'));
});

test('a used card cannot reset its once-per-turn limit by being forged', () => {
  let d = cast(fresh(), 'water', 1, 4);
  const ids = ['water', 'lightning'].map(key => d.hands.Ada.find(c => c.key === key).id);
  assert.throws(() => applyAction(d, 'Ada', { type: 'combine', cardIds: ids }), /Fuse before casting/);
  d = end(d);
  d = applyAction(d, 'Ada', { type: 'combine', cardIds: ids });
  assert.ok(d.hands.Ada.some(c => c.key === 'tempest'));
});

test('gifting preserves charges and turn marker and requires the active player', () => {
  let d = fresh(['Ada', 'Bo']);
  d = cast(d, 'water', 1, 4);
  const card = d.hands.Ada.find(c => c.key === 'water');
  d = applyAction(d, 'Ada', { type: 'gift', cardId: card.id, to: 'Bo' });
  assert.ok(!d.hands.Ada.some(c => c.id === card.id));
  assert.deepEqual(d.hands.Bo.find(c => c.id === card.id), card);
  assert.throws(() => applyAction(d, 'Bo', { type: 'gift', cardId: card.id, to: 'Ada' }), /turn/);
});

test('late joins wait for the next round and last pending player leaving ticks exactly once', () => {
  let d = fresh(['Ada', 'Bo'], 'online'); d.enemies = [enemy(1, 4)];
  d = end(d); assert.equal(getActivePlayer(d), 'Bo');
  d = applyAction(d, 'Cy', { type: 'join', player: 'Cy' });
  assert.deepEqual(d.pendingPlayers, ['Bo']);
  assert.equal(d.round, 1);
  d = applyAction(d, 'Bo', { type: 'leave', player: 'Bo' });
  assert.equal(d.round, 2); assert.equal(d.enemies.find(e => e.id === 'test-rat-1-4').y, 5);
  assert.deepEqual(d.pendingPlayers, ['Ada', 'Cy']);
  d = end(d); assert.equal(d.round, 2); assert.equal(getActivePlayer(d), 'Cy');
  d = end(d); assert.equal(d.round, 3);
});

test('active and already-finished departures neither stall nor prematurely tick the round', () => {
  let d = fresh(['Ada', 'Bo', 'Cy'], 'online'); d.enemies = [enemy(1, 4)];
  d = applyAction(d, 'Ada', { type: 'leave', player: 'Ada' });
  assert.equal(getActivePlayer(d), 'Bo'); assert.equal(d.round, 1);
  d = end(d); assert.equal(getActivePlayer(d), 'Cy');
  d = applyAction(d, 'Bo', { type: 'leave', player: 'Bo' });
  assert.equal(getActivePlayer(d), 'Cy'); assert.equal(d.round, 1);
  d = end(d); assert.equal(d.round, 2); assert.equal(getActivePlayer(d), 'Cy');
  assert.throws(() => applyAction(d, 'Cy', { type: 'leave', player: 'Ada' }), /own seat/);
});

test('two-lane spells affect distinct columns at the right edge', () => {
  let d = inject(fresh(), 'flood'); d.enemies = [enemy(3, 4), enemy(2, 4)];
  const p = previewSpell(d, 'flood', 3, 4);
  assert.equal(p.cells.length, 18); assert.deepEqual([...new Set(p.cells.map(c => c.x))].sort(), [2, 3]);
  d = cast(d, 'flood', 3, 4);
  assert.equal(d.enemies[0].hp, 96); assert.equal(d.enemies[1].hp, 96);
  assert.equal(d.enemies[0].y, 3); assert.equal(d.enemies[1].y, 3);
});

test('all spells produce finite serializable state and previews without mutating input', () => {
  for (const key of Object.keys(SPELLS)) {
    let d = fresh(); d.enemies = [enemy(1, 4), enemy(2, 4, 'golem')];
    if (!d.hands.Ada.some(c => c.key === key)) inject(d, key);
    const before = structuredClone(d), p = previewSpell(d, key, 1, 4);
    assert.ok(p.cells.length > 0, key); assert.deepEqual(d, before, `${key} preview mutated state`);
    d = cast(d, key, 1, 4);
    assertFirestore(d); assertFirestore(p);
    assert.deepEqual(JSON.parse(JSON.stringify(d)), d);
    assert.ok(d.events.length <= 72);
  }
});

test('all ten waves end in a retained victory, and lethal arrivals end in retained defeat', () => {
  let d = fresh(); d.wave = 10; d.roundsInWave = 3; d.enemies = [];
  d = end(d); assert.equal(d.status, 'won'); assert.ok(d.events.some(e => e.type === 'victory'));
  assert.throws(() => end(d), /ended/);
  let lost = fresh(); lost.hp = 1; lost.enemies = [enemy(1, 8)];
  lost = end(lost); assert.equal(lost.status, 'lost'); assert.equal(lost.hp, 0);
  assert.ok(lost.events.some(e => e.type === 'defeat'));
});

test('long unplayed runs terminate rather than accumulating unbounded histories', () => {
  let d = fresh(['Ada', 'Bo'], 'hotseat');
  for (let i = 0; i < 300 && d.status === 'playing'; i++) {
    d = end(d);
    assertFirestore(d);
    assert.ok(d.events.length <= 72);
    for (const hand of Object.values(d.hands)) assert.ok(hand.length <= 14);
  }
  assert.equal(d.status, 'lost');
});
