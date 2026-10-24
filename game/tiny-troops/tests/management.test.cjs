const test = require('node:test');
const assert = require('node:assert/strict');
const M = require('../management-rules.js');
const troop = (id, role = 'MELEE') => ({ ttId: id, n: id, role, hp: 40, maxHp: 40, atk: 8, spd: 1, baseMaxHp: 40, baseAtk: 8, baseSpd: 1, shield: 0 });
const state = () => ({ tt: { mode: 'PLAYING' }, phase: 'recruit', squad: [troop('a'), troop('b'), ...Array(14).fill(null)], bench: [troop('c'), null, null] });
test('preparation board swaps preserve objects and all slot identities', () => {
  const s = state(), a = s.squad[0], b = s.squad[1];
  const move = M.move(s, { where: 'board', index: 0, unit: a }, { where: 'board', index: 1 });
  assert.equal(move.ok, true); assert.equal(move.squad[0], b); assert.equal(move.squad[1], a);
  assert.equal(s.squad[0], a); assert.equal(move.bossSwap, false);
  assert.equal(new Set([...move.squad, ...move.bench].filter(Boolean)).size, 3);
});
test('empty reserve and empty board moves are free; occupied reserve switch requires boss reward', () => {
  const s = state(), a = s.squad[0];
  assert.equal(M.move(s, { where: 'board', index: 0 }, { where: 'bench', index: 1 }).ok, true);
  assert.equal(M.move(s, { where: 'bench', index: 0 }, { where: 'board', index: 2 }).ok, true);
  assert.equal(M.move(s, { where: 'bench', index: 0 }, { where: 'board', index: 0 }).ok, false);
  assert.equal(M.move(s, { where: 'board', index: 0 }, { where: 'bench', index: 0 }).ok, false);
  s.bossSwapAvailable = true;
  const result = M.move(s, { where: 'bench', index: 0 }, { where: 'board', index: 0 });
  assert.equal(result.ok && result.bossSwap, true); assert.equal(result.bench[0], a);
});
test('invalid drops, stale selection, pending cards, and running or paused battles cannot mutate slots', () => {
  for (const phase of ['battle', 'evolve', 'dead', 'relic', 'tt-event']) {
    const s = state(); s.phase = phase;
    assert.equal(M.move(s, { where: 'board', index: 0 }, { where: 'board', index: 2 }).ok, false);
  }
  const s = state(), before = JSON.stringify(s);
  for (const index of [-1, 16, 1.2, NaN]) assert.equal(M.move(s, { where: 'board', index: 0 }, { where: 'board', index }).ok, false);
  assert.equal(M.move(s, { where: 'board', index: 0, unit: troop('stale') }, { where: 'board', index: 2 }).ok, false);
  assert.equal(JSON.stringify(s), before);
  s.placing = troop('new'); assert.match(M.preparationReason(s), /selected card/);
});
test('repeated bench transfers never lose or duplicate a troop or invent additional reserve slots', () => {
  const s = state(), original = new Set([...s.squad, ...s.bench].filter(Boolean));
  for (let i = 0; i < 100; i++) {
    let result = M.move(s, { where: 'board', index: 0 }, { where: 'bench', index: 2 }); assert.equal(result.ok, true);
    Object.assign(s, { squad: result.squad, bench: result.bench });
    result = M.move(s, { where: 'bench', index: 2 }, { where: 'board', index: 0 }); assert.equal(result.ok, true);
    Object.assign(s, { squad: result.squad, bench: result.bench });
    assert.equal(s.bench.length, 3); assert.deepEqual(new Set([...s.squad, ...s.bench].filter(Boolean)), original);
  }
});
test('presets migrate malformed records, duplicate IDs, and oversized input into exactly three bounded slots', () => {
  const presets = M.sanitizePresets([{ name: 'x'.repeat(90), board: ['a', 'a', {}, 'b', ...Array(100).fill('c')] }, null, 'bad', { board: ['d'] }]);
  assert.equal(presets.length, 3); assert.equal(presets[0].name.length, 40); assert.equal(presets[0].board.length, 20);
  assert.deepEqual(presets[0].board.slice(0, 5), ['a', null, null, 'b', 'c']); assert.equal(presets[1], null); assert.equal(presets[2], null);
});
test('formation recall keeps new troops and cannot bypass reserve switch or locked rows', () => {
  const s = state(), preset = M.capture(s.squad, 'Test');
  [s.squad[0], s.squad[1]] = [s.squad[1], s.squad[0]]; s.squad[3] = troop('new');
  const result = M.restore(s, preset); assert.equal(result.ok, true); assert.equal(result.squad[0].ttId, 'a'); assert.equal(result.squad[1].ttId, 'b'); assert.ok(result.squad.some(u => u?.ttId === 'new'));
  preset.board[0] = 'c'; assert.match(M.restore(s, preset).reason, /reserve or missing/);
  preset.board[0] = 'a'; preset.board[19] = 'a'; assert.match(M.restore(s, preset).reason, /additional board row/);
});
test('auto arrange favors right-side defenders and left-side ranged without changing troops', () => {
  const squad = [troop('tank', 'ARMORED'), troop('archer', 'RANGED'), troop('medic', 'SUPPORT'), troop('melee', 'MELEE'), ...Array(12).fill(null)];
  const result = M.autoArrange(squad, u => u.role);
  assert.equal(result.indexOf(squad[0]) % 4, 3); assert.equal(result.indexOf(squad[1]) % 4, 0); assert.equal(result.indexOf(squad[2]) % 4, 1); assert.equal(result.indexOf(squad[3]) % 4, 2);
  assert.deepEqual(new Set(result.filter(Boolean)), new Set(squad.filter(Boolean)));
});
test('evolution previews run real native apply and boost on copies without spending or mutating', () => {
  const u = troop('a'), before = JSON.stringify(u);
  const result = M.previewEvolution(u, { apply: next => { next.maxHp += 20; next.baseMaxHp += 20; next.evo = 'guardian'; next.evoEmoji = '🛡️'; } }, 5, next => { next.atk *= 1.12; next.baseAtk *= 1.12; });
  assert.equal(result.ok, true); assert.equal(result.unit.maxHp, 60); assert.equal(result.unit.evo, 'guardian'); assert.equal(result.unit.shield, 35); assert.equal(JSON.stringify(u), before);
  assert.equal(M.previewEvolution(u, { apply: next => { next.atk = NaN; } }, 5).ok, false);
  assert.equal(M.previewEvolution(u, { apply: () => { throw Error('bad evolution'); } }, 5).ok, false); assert.equal(JSON.stringify(u), before);
});
