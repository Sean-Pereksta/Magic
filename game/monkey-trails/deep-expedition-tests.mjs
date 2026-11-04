import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as D from './deep-journal.mjs';
import * as S from './social-combat.mjs';
import { REEF, LOOM, GLASS, deepSpecies } from './deep-roster.mjs';
import { splitHealth } from './deep-combat.mjs';
import { questArt } from './deep-art.mjs';
import { deepPanel } from './deep-ui.mjs';
const fresh = () => {
  const s = C.newSave('Deep explorer', 'capuchin', 'practice', 781);
  s.world.totalDistance = 310000;
  C.landProgress(s);
  s.run.nextEncounter = s.run.nextCamp = 1e8;
  return s;
};
function add(s, id) {
  const p = structuredClone(s.run.monkey);
  Object.assign(p, {
    species: id,
    id,
    types: C.SPECIES[id].types,
    learned: [],
    loadout: ['strike', C.SPECIES[id].signature, 'brace', 'breathe'],
  });
  p.hp = C.stats(p).hp;
  p.energy = C.stats(p).maxEnergy;
  s.collection[id] = p;
  return p;
}
test('every illustrated chapter follows its own events, ordered legs, choices and saved outcomes', () => {
  let s = fresh();
  const pictures = new Set();
  for (const q of D.quests) {
    C.selectLand(s, q.land);
    D.pin(s, q.id);
    for (const [i, step] of q.steps.entries()) {
      const art = questArt(q.id, i);
      assert.ok(!pictures.has(art));
      pictures.add(art);
      const phase = step.kind === 'camp' ? 'camp' : 'trail';
      s.run.phase = phase;
      if (step.choices) {
        if (step.answer != null)
          assert.throws(() => D.questChoice(s, (step.answer + 1) % step.choices.length));
        D.questChoice(s, step.answer ?? 0);
      } else if (step.kind === 'pace') {
        D.startPace(s, 0);
        for (let t = 1; t <= 180; t++)
          D.paceSample(s, t <= 90 ? (5 * 5280) / 3600 : (7 * 5280) / 3600, t * 1000);
      } else if (['E', 'W', 'N', 'S', 'carry', 'feet'].includes(step.kind)) {
        const n = step.goal * C.FEET;
        D.deepWalk(
          s,
          n,
          step.kind === 'W' ? -n : step.kind === 'E' ? n : 0,
          step.kind === 'S' ? -n : step.kind === 'N' ? n : 0,
          C.random,
        );
      } else for (let k = 0; k < (step.goal || 1); k++) D.event(s, step.kind);
      assert.equal(D.page(s, q.id).stage, i + 1, q.name + ' · ' + step.text);
      s = JSON.parse(JSON.stringify(s));
      s.run.monkey = s.collection[s.run.activeId];
    }
    s.run.phase = 'trail';
    assert.ok(D.done(s, q.id));
    assert.match(deepPanel(s), new RegExp(q.name));
  }
  assert.equal(D.page(s, 8).choice, 'Western colony');
  assert.equal(D.page(s, 10).choice, 'Nursery');
});
test('compass projection and full pace intervals use accepted samples', () => {
  const s = fresh();
  D.pin(s, 1);
  D.questChoice(s, 0);
  D.deepWalk(s, 100 * C.FEET, 100 * C.FEET, 100 * C.FEET, C.random);
  assert.ok(Math.abs(D.page(s, 1).count - 70.7107) < 0.001);
  D.deepWalk(s, 300 * C.FEET, -300 * C.FEET, 0, C.random);
  assert.ok(Math.abs(D.page(s, 1).count - 70.7107) < 0.001);
  C.selectLand(s, 'thunder-highlands');
  D.pin(s, 0);
  D.startPace(s, 0);
  D.paceSample(s, 1700, 180000);
  assert.equal(s.run.pace, undefined);
  assert.equal(D.page(s, 0).stage, 0);
});
test('all four investigations enforce seals, actual depth, twenty-check pity, and persistent signs', () => {
  for (const row of deepSpecies.filter((r) => r.rarity === 'mythic')) {
    const s = fresh();
    C.selectLand(s, row.region);
    for (const id of [6, 8, 10, 11]) D.page(s, id).stage = D.quests[id].steps.length;
    s.run.deepWins = 2;
    D.journal(s).target = row.id;
    s.run.startingDepth = 1e7;
    D.deepWalk(s, 1999 * C.FEET, 0, 0, () => 1);
    assert.equal(D.hunt(s, row.id).feet, 0);
    s.run.distance = row.minFeet * C.FEET;
    for (let n = 0; n < 20; n++) {
      D.deepWalk(s, 2000 * C.FEET, 0, 0, () => 1);
      assert.equal(D.hunt(s, row.id).sign, n === 19);
    }
    s.run.phase = 'dead';
    C.restart(s);
    assert.equal(D.hunt(s, row.id).sign, true);
    assert.throws(() => C.beginDeepBattle(s, true));
    s.run.deepWins = 2;
    s.run.distance = row.minFeet * C.FEET;
    for (const cue of D.trials[row.id]) D.trial(s, row.id, cue[2]);
    if (D.trialLessons[row.id]) {
      C.beginDeepBattle(s, true);
      C.readyBattle(s);
      const b = s.run.battle;
      assert.equal(b.deepLesson, row.id);
      b.choirInterrupt = b.deepBlocked = b.crownSurvived = true;
      for (const e of b.enemies) e.hp = 0;
      C.turn(s, 'strike');
      assert.ok(D.hunt(s, row.id).proven);
    }
    C.beginDeepBattle(s, true);
    C.readyBattle(s);
    for (const e of s.run.battle.enemies) e.hp = 0;
    C.turn(s, 'strike');
    assert.ok(C.sanctuary(s).some((p) => p.species === row.id));
  }
});
test('paired rounds execute both selected abilities, preserve individual HP and survive saving', () => {
  assert.deepEqual(splitHealth(300, 100, 340, 160, 280), [210, 70]);
  assert.deepEqual(splitHealth(210, 70, 340, 160, 500), [340, 160]);
  const s = fresh(),
    host = add(s, 'nacreon'),
    r = add(s, 'vitreus');
  C.setParty(s, [host.id]);
  D.attach(s, host.id, r.id);
  s.run.nextEncounter = 1;
  C.advance(s, 1);
  C.beginPendingEncounter(s);
  C.readyBattle(s);
  const battle = s.run.battle,
    e = battle.enemy;
  e.hp = 10000;
  e.loadout = ['breathe'];
  battle.enemyAction = { type: 'move', id: 'breathe' };
  battle.intent = 'breathe';
  const before = e.hp;
  C.turn(s, 'KingTide', false, { riderId: 'GlassSun' });
  assert.equal(battle.round, 2);
  assert.equal(host.uses.KingTide, 1);
  assert.equal(r.uses.GlassSun, 1);
  const hits = battle.hitFeedback.filter((h) => h.side === 'player');
  assert.equal(hits.length, 2);
  assert.ok(before - e.hp >= hits[0].damage + hits[1].damage * 6);
  assert.ok(!('pooledMaximum' in host));
  assert.equal(D.rider(C.validate(JSON.parse(JSON.stringify(s))), host).species, 'vitreus');
});
test('friendly copies keep pair builds, and 20 raid abilities consume ten turns with uncapped contribution', () => {
  const s = fresh(),
    host = add(s, 'orrakai'),
    r = add(s, 'vitreus');
  C.setParty(s, [host.id]);
  D.attach(s, host.id, r.id);
  host.level = r.level = 60;
  const before = JSON.stringify(s),
    team = S.snapshotTeam(s),
    attempt = S.newRaidAttempt(team, 1, 42);
  assert.equal(team[0].maxHp, C.stats(host).hp + C.stats(r).hp);
  for (let i = 0; i < 10; i++) S.raidAction(attempt, { type: 'move', id: 'strike', riderId: 'strike' });
  assert.equal(attempt.turns, 10);
  assert.equal(attempt.team[0].rider.uses.strike, 10);
  assert.equal(attempt.team[0].uses.strike, 10);
  assert.ok(attempt.totalDamage > S.bossHealth(1));
  assert.equal(JSON.stringify(s), before);
  const duel = S.newDuel({ id: 'a', team }, { id: 'b', team }, 42);
  S.duelAction(duel, 'a', { type: 'move', id: 'Crownfall', riderId: 'GlassSun' });
  assert.equal(duel.sequence, 1);
  assert.ok(duel.teams[0][0].shield >= Math.round(duel.teams[0][0].maxHp * 0.25));
  assert.equal(JSON.stringify(s), before);
});

test('three distinct pairs switch intact and a hit conserves both individual balances', () => {
  const s = fresh(),
    a = add(s, 'bridgeback'),
    b = add(s, 'orrakai'),
    c = add(s, 'nacreon'),
    r = add(s, 'vitreus'),
    r2 = add(s, 'carilluma'),
    r3 = add(s, 'rimewhistle');
  C.setParty(s, [a.id, b.id, c.id]);
  D.attach(s, a.id, r.id);
  D.attach(s, b.id, r2.id);
  D.attach(s, c.id, r3.id);
  assert.throws(() => D.attach(s, b.id, r.id));
  s.run.nextEncounter = 1;
  C.advance(s, 1);
  C.beginPendingEncounter(s);
  C.readyBattle(s);
  const battle = s.run.battle,
    foe = battle.enemy;
  a.hp = 150;
  r.hp = 50;
  foe.hp = 5000;
  foe.loadout = ['strike'];
  battle.enemyAction = { type: 'move', id: 'strike' };
  battle.intent = 'strike';
  const before = [a.hp, r.hp];
  assert.throws(() => C.turn(s, 'strike', false, { riderId: 'not-an-ability' }));
  assert.deepEqual([a.hp, r.hp], before);
  C.turn(s, 'strike', false, { riderId: 'strike' });
  assert.ok(a.hp < 150);
  assert.ok(Math.abs(a.hp / r.hp - 3) < 1e-7);
  C.switchMonkey(s, b.id);
  assert.equal(D.rider(s, s.run.monkey).id, r2.id);
});
test('a conscious sidekick forages in its own eligible region', () => {
  const s = fresh(),
    h = add(s, 'nacreon'),
    r = add(s, 'carilluma');
  C.setParty(s, [h.id]);
  D.attach(s, h.id, r.id);
  C.selectLand(s, LOOM);
  C.advance(s, 1000 * C.FEET);
  assert.equal(r.deepForage || 0, 0);
  C.selectLand(s, REEF);
  C.advance(s, 1500 * C.FEET);
  assert.ok(s.log.some((line) => line.includes('River Pearl')));
  assert.ok((r.deepForage || 0) < 1e-6);
});
