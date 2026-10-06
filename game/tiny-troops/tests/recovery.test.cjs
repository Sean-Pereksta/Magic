const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const R = require('../core.js');
const ally = (changes = {}) => ({ n: 'Squire', e: '🛡️', team: 'ally', t: ['Guard'], star: 2, baseMaxHp: 30, baseAtk: 8, baseSpd: 1, ...changes });
function fixture() {
  return { round: 5, coins: 17, kills: 2, points: 31, squad: [null, null, null, ally()], boost: { shield: 5, tb: { Guard: { hp: 10 } } }, tt: { ...R.newRun({}, 123), wavePlan: { round: 5, enemies: [{ n: 'Ogre Boss', team: 'enemy', boss: true, maxHp: 30, hp: 30, atk: 5, spd: .7, shield: 2, kind: 'melee' }] } } };
}
test('basic recovery fights a detached copy of the exact saved enemies without awarding income itself', () => {
  const s = fixture(), plan = R.copy(s.tt.wavePlan);
  R.initializeBasicBattle(s);
  assert.deepEqual(s.tt.wavePlan, plan); assert.notEqual(s.enemies[0], s.tt.wavePlan.enemies[0]);
  let result, kills = 0;
  for (let i = 0; i < 300; i++) { result = R.stepBasicBattle(s); kills += result.events.filter(e => e.killed && e.target.team === 'enemy').length; if (result.outcome) break; }
  assert.equal(result.outcome, 'victory'); assert.equal(kills, 1);
  assert.equal(s.coins, 17); assert.equal(s.kills, 2); assert.equal(s.points, 31);
  assert.deepEqual(s.tt.wavePlan, plan);
});
test('basic combat honors the front line, shields, and wounded-ally healing', () => {
  const s = fixture(); s.squad[0] = ally({ n: 'Medic', t: ['Healer'], focus: 'support', baseAtk: 1 });
  R.initializeBasicBattle(s); s.squad[3].hp = 10; s.enemies[0].hp = s.enemies[0].maxHp = 1000;
  const events = R.stepBasicBattle(s).events;
  assert.ok(events.some(e => e.heal > 0 && e.target === s.squad[3]));
  const hit = events.find(e => e.source.team === 'enemy'); assert.equal(hit.target, s.squad[3]); assert.ok(hit.blocked > 0);
  assert.equal(s.squad[0].hp, s.squad[0].maxHp);
});
test('basic combat can lose and cannot award a free victory for a missing enemy checkpoint', () => {
  const s = fixture(); s.tt.wavePlan.enemies[0].atk = 1000; R.initializeBasicBattle(s);
  assert.equal(R.stepBasicBattle(s).outcome, 'defeat'); assert.equal(s.squad[3].dead, true);
  s.tt.wavePlan.enemies = []; assert.throws(() => R.initializeBasicBattle(s), /enemy checkpoint/);
});
test('bad combat numbers are repaired and the fallback remains deterministic', () => {
  const s = fixture(); s.squad[3].baseSpd = Infinity; s.squad[3].shield = NaN; s.tt.wavePlan.enemies[0].spd = NaN;
  const a = R.copy(s), b = R.copy(s); R.initializeBasicBattle(a); R.initializeBasicBattle(b);
  for (let i = 0; i < 10; i++) { R.stepBasicBattle(a); R.stepBasicBattle(b); }
  assert.deepEqual(a, b);
  for (const u of [...a.squad.filter(Boolean), ...a.enemies]) assert.ok([u.hp,u.maxHp,u.atk,u.spd,u.shield].every(Number.isFinite));
});
test('the basic-combat recovery setting survives a reload only for its own wave', () => {
  const s = fixture(); s.tt.basicCombatRound = 5;
  assert.equal(R.sanitizeSnapshot(s).tt.basicCombatRound, 5);
  s.round = 6; assert.equal(R.sanitizeSnapshot(s).tt.basicCombatRound, undefined);
});
function timers(handler) {
  const jobs = [], context = vm.createContext({ setTimeout(fn) { jobs.push(fn); return jobs.length; }, clearTimeout() {}, ...(handler ? { TinyTroopsPolish: { handleCombatError: handler } } : {}) });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../lifecycle.js'), 'utf8'), context);
  return { jobs, context };
}
test('combat callback errors enter recovery rather than escaping the scheduler', () => {
  const caught = [], { jobs, context } = timers((error, stage) => { caught.push([error.message, stage]); return true; });
  vm.runInContext("ttTimeout(() => { throw new Error('broken spell'); }, 5)", context); jobs[0]();
  assert.deepEqual(caught, [['broken spell', 'combat callback']]);
});
test('canceled callbacks cannot mutate a recovered run, and unrelated exceptions remain visible', () => {
  const { jobs, context } = timers(); vm.runInContext("ttTimeout(() => { throw new Error('stale'); }); ttClearTimers(); ttTimeout(() => { throw new Error('unrelated'); });", context);
  assert.doesNotThrow(jobs[0]); assert.throws(jobs[1], /unrelated/);
});
