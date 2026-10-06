const test = require('node:test');
const assert = require('node:assert/strict');
const R = require('../core.js');
const troop = (tags, extra = {}) => ({ n: 'Test troop', e: '⚔️', t: ['Blade'], tags, team: 'ally', hp: 40, maxHp: 40, atk: 8, spd: 1, focus: 'near', ...extra });

test('every run is endless, including migration from obsolete finite settings', () => {
  for (const limit of [20, 60, 300, 10000]) {
    assert.equal('limit' in R.newRun({ limit }), false);
    const save = R.sanitizeSnapshot({ round: 601, squad: [], tt: { limit } });
    assert.equal('limit' in save.tt, false); assert.equal(save.round, 601); assert.equal(save.runEnded, false);
  }
});
test('native roles distinguish protection, support, magic, and attack tempo', () => {
  assert.equal(R.role(troop([], { n: 'Knight', t: ['Guard', 'Blade'] })), 'ARMORED');
  assert.equal(R.role(troop([], { n: 'Archer' })), 'RANGED');
  assert.equal(R.role(troop([], { n: 'Wolf', t: ['Beast'], spd: 1.5 })), 'FAST');
  assert.equal(R.role(troop([], { n: 'Medic', t: ['Healer'], focus: 'support' })), 'SUPPORT');
  assert.equal(R.role(troop([], { n: 'Wizard', t: ['Mage'] })), 'MAGIC');
  assert.ok(R.has(troop([], { n: 'Lancer' }), 'MOUNTED'));
});
test('enemy armor identity does not change just because endless HP scales', () => {
  assert.equal(R.has({ team: 'enemy', n: 'Rat', kind: 'melee', hp: 1e6, spd: 1 }, 'ARMORED'), false);
  assert.equal(R.has({ team: 'enemy', n: 'Iron Sentinel', kind: 'melee', spd: .8 }, 'ARMORED'), true);
});
test('tiered synergies activate, advance, and deactivate when troops fall', () => {
  const army = Array.from({ length: 6 }, () => troop(['RANGED'], { n: 'Archer' }));
  const volley = () => R.synergies(army, true).find(s => s.id === 'volley');
  assert.equal(volley().tier, 2); assert.equal(volley().mods.spdMult, .14); assert.equal(volley().next, 9);
  army[0].dead = true; army[0].hp = 0; assert.equal(volley().tier, 1);
  army.slice(1, 5).forEach(u => u.dead = true); assert.equal(volley().tier, 0);
});
test('all role combinations require their full formation', () => {
  for (const s of R.SYNERGIES.filter(s => s.req)) {
    const army = Object.entries(s.req).flatMap(([t, n]) => Array.from({ length: n }, () => troop([t])));
    if (s.distinctRoles) army.push(troop(['RANGED'], { n: 'Archer' }), troop(['ARMORED'], { t: ['Guard'] }));
    assert.equal(R.synergies(army).find(a => a.id === s.id).tier, 1, s.id);
    assert.equal(R.synergies([]).find(a => a.id === s.id).tier, 0, s.id);
  }
});
test('protected archers require a living defender farther right in the same row', () => {
  const army = Array(16).fill(null); army[0] = troop(['RANGED'], { n: 'Archer' }); army[3] = troop(['ARMORED']); army[7] = troop(['ARMORED']);
  assert.equal(R.protectedBy(army, 0, true).length, 1);
  army[3].dead = true; assert.equal(R.protectedBy(army, 0, true).length, 0);
  army[3].dead = false; assert.equal(R.protectedBy(army, 3).length, 0);
});
test('soft counters never disable a troop or recursively amplify damage over time', () => {
  const fast = troop(['FAST']), ranged = troop(['RANGED']), armored = troop(['ARMORED']);
  assert.equal(R.counterMultiplier(fast, ranged, 'hit'), 1.15);
  assert.equal(R.counterMultiplier(fast, armored, 'hit'), .9);
  assert.equal(R.counterMultiplier(fast, ranged, 'burn'), 1);
  assert.equal(R.counterMultiplier(null, ranged, 'hit'), 1);
});
test('each role has three distinct specialization choices', () => {
  const ids = new Set();
  for (const p of R.PATHS) { assert.equal(ids.has(p.id), false); ids.add(p.id); assert.ok(p.desc); }
  for (const role of Object.keys(R.ROLE_INFO)) {
    const choices = R.pathsFor(troop([role])); assert.equal(choices.length, 3, role); assert.equal(new Set(choices.map(p => p.id)).size, 3);
  }
  const archer = troop(['RANGED'], { ttPath: 'flaming' }); assert.ok(R.has(archer, 'FIRE')); assert.equal(R.pathMods(archer).burn, 2);
});
test('recruit cards explain activation before merely counting progress', () => {
  const archers = [troop(['RANGED'], { n: 'Archer' }), troop(['RANGED'], { n: 'Archer' })];
  assert.match(R.recruitImpact(archers, troop(['RANGED'], { n: 'Archer' })), /Activates Volley/);
});
test('seeded drafts are reproducible without relying on combat randomness', () => {
  const a = R.newRun({}, 1234), b = R.newRun({}, 1234);
  for (let i = 0; i < 100; i++) { const x = R.nextRandom(a); assert.equal(x, R.nextRandom(b)); assert.ok(x >= 0 && x < 1); }
});
test('partially corrupt saves repair numbers and collections without losing stars or progression', () => {
  const s = R.sanitizeSnapshot({ round: '180', coins: -20, boost: null, upgrades: { guardBastion: 'bad' }, squad: [troop([], { star: 13, hp: null, maxHp: 800, atk: 'bad', personalGrowthStacks: { kills: 9 }, path13: 'mythic_warlord' })], tt: { stats: { bad: null }, difficulty: 'bad', steps: 'bad' } });
  assert.equal(s.coins, 0); assert.equal(s.squad[0].star, 13); assert.equal(s.squad[0].path13, 'mythic_warlord'); assert.deepEqual(s.squad[0].personalGrowthStacks, { kills: 9 });
  assert.ok(Number.isFinite(s.squad[0].atk)); assert.ok(s.squad[0].hp > 0); assert.equal(s.upgrades.guardBastion, 0); assert.equal(s.tt.difficulty, 'standard'); assert.deepEqual(s.tt.stats, {}); assert.ok(s.boost.tb);
});
test('legacy saves migrate with the existing campaign and equipment intact', () => {
  const s = R.sanitizeSnapshot({ saveVersion: 'tiny_troops_v1', schema: 2, round: 72, coins: 220, relics: ['thornSeed'], abilities: { meteorCall: 3 }, squad: [troop([], { fx: [{ id: 'storm', rank: 4 }], lateChoices: ['shieldForge'], kills: 40 })] });
  assert.equal(s.schema, 3); assert.equal(s.round, 72); assert.equal(s.coins, 220); assert.equal(s.abilities.meteorCall, 3); assert.equal(s.squad[0].fx[0].rank, 4); assert.equal(s.squad[0].kills, 40);
});
test('battle snapshots rewind income and deaths to the checkpoint', () => {
  const checkpoint = { round: 9, coins: 20, kills: 3, squad: [troop([])], tt: R.newRun({}, 99) };
  const saved = R.sanitizeSnapshot({ phase: 'battle', coins: 200, kills: 90, tt: { checkpoint: { state: checkpoint } } });
  assert.equal(saved.coins, 20); assert.equal(saved.kills, 3); assert.equal(saved.round, 9); assert.equal(saved.phase, 'recruit'); assert.equal(saved.tt.checkpoint, null);
});
test('boss relics and events survive reload until the reward is actually picked', () => {
  assert.equal(R.sanitizeSnapshot({ round: 5, phase: 'relic', shopOpen: true, squad: [] }).phase, 'relic');
  const s = R.sanitizeSnapshot({ round: 4, tt: { event: { id: 'armory' } }, squad: [] }); assert.equal(s.phase, 'tt-event'); assert.equal(s.tt.event.id, 'armory');
});
test('fifth-row migration preserves occupied slots and the native wave-250 unlock', () => {
  assert.equal(R.sanitizeSnapshot({ round: 120, squad: [] }).squad.length, 16);
  assert.equal(R.sanitizeSnapshot({ round: 250, squad: [] }).squad.length, 20);
  const army = Array(20).fill(null); army[19] = troop([]); const saved = R.sanitizeSnapshot({ round: 20, squad: army }); assert.ok(saved.squad[19]); assert.equal(saved.boardRowsUnlocked, true);
});
test('bench checkpoints retain empty slots, troop identities, training, and equipment', () => {
  const bench = [null, troop([], { ttId: 'reserve-2', star: 4, starProg: 2, fx: [{ id: 'shield', rank: 3 }] }), null];
  const saved = R.sanitizeSnapshot({ squad: [], bench });
  assert.equal(saved.bench.length, 3); assert.equal(saved.bench[0], null); assert.equal(saved.bench[2], null);
  assert.equal(saved.bench[1].ttId, 'reserve-2'); assert.equal(saved.bench[1].starProg, 2); assert.equal(saved.bench[1].fx[0].rank, 3);
  assert.deepEqual(R.sanitizeSnapshot({ squad: [], bench: [troop([])] }).bench.map(u => u?.n || null), ['Test troop', null, null]);
});
test('invalid enemies and oversized drafts are rejected safely on load', () => {
  const s = R.sanitizeSnapshot({ squad: [], tt: { wavePlan: { enemies: [{ n: 'Broken', maxHp: null }] }, draft: [{ type: 'unknown' }, ...Array(10).fill({ type: 'upgrade', n: 'Training' })] } });
  assert.equal(s.tt.wavePlan, null); assert.ok(s.tt.draft.length <= 3);
});
