import test from 'node:test';
import assert from 'node:assert/strict';
import { CLASSES } from '../game/matchwars/classes.mjs?v=4';
import { Battle, ENEMIES, CHAMPION_AFFIXES, UPGRADES, elementMultiplier } from '../game/matchwars/combat.mjs?v=4';
import { Board } from '../game/matchwars/board.mjs';
import { createDaily, seededRng } from '../game/matchwars/daily.mjs?v=4';
import { MASTERY_RANK_XP, MASTERY_MAX_XP, SPECIALIZATIONS, normalizeMastery, masteryRank, masteryProgress, masteryBonuses, specializationFor, calculateMasteryAwards, applyMasteryAwards, selectSpecialization } from '../game/matchwars/mastery.mjs?v=4';

function addEnemy(battle, x = 500, health = 600, key = 'shade') {
  const unit = battle._makeUnit(ENEMIES[key], 'enemy', 1);
  unit.x = x; unit.maxHp = health; unit.hp = health * .4; unit.cooldown = 100; unit.spellCooldown = 100;
  battle.units.push(unit); return unit;
}
function specializationFixture(classId, specialization = null) {
  const def = CLASSES[classId];
  const battle = new Battle({ loadout: { [def.key]: classId }, mastery: { [classId]: { xp: 180, specialization } } });
  const caster = battle.summon(def.key)[0]; caster.x = 400; caster.hp = caster.maxHp * .35; caster.shield = 0;
  for (let index = 0; index < 6; index++) {
    const ally = battle.summon(def.key === 'sun' ? 'grove' : 'sun')[0]; ally.x = 405 + index * 9; ally.hp = ally.maxHp * .35; ally.shield = 0; ally.spellCooldown = 100;
    battle._status(ally, 'poison', 3, 2); battle._status(ally, 'curse', 3, .1);
  }
  const foes = Array.from({ length: 7 }, (_, index) => addEnemy(battle, 490 + index * 10));
  for (const foe of foes) { foe.shield = 45; battle._status(foe, 'freeze', .5, 1); }
  for (let i = 0; i < 8; i++) {
    const body = addEnemy(battle, 425, 10, 'ashling'); battle._hit(null, body, 10000);
  }
  const allies = battle.units.filter(unit => !unit.dead && unit.side === 'player');
  assert.equal(battle._autoCast(caster, allies, foes), true);
  const spellState = JSON.stringify(battle.units.filter(unit => !unit.dead).map(unit => ({ hp: unit.hp, shield: unit.shield, x: unit.x, statuses: unit.statuses, decay: unit.decay, attack: unit.attack })));
  battle._impactAttack(caster, foes[0], 10);
  caster.shield = 0;
  battle._hit(foes[1], caster, 8);
  if (classId === 'phoenix-adept') battle._hit(null, caster, 10000);
  const units = battle.units.filter(unit => !unit.dead).map(unit => ({
    side: unit.side, hp: unit.hp, maxHp: unit.maxHp, shield: unit.shield, x: unit.x, armor: unit.armor,
    attack: unit.attack, interval: unit.interval, range: unit.range, speed: unit.speed, decay: unit.decay,
    statuses: unit.statuses,
  }));
  return { battle, caster, foes, snapshot: { spellState, units } };
}

test('mastery thresholds, progress bars and modest rank bonuses have exact boundaries', () => {
  assert.deepEqual(MASTERY_RANK_XP, [0, 60, 180, 420, 800, 1350]);
  for (let index = 0; index < MASTERY_RANK_XP.length; index++) {
    const xp = MASTERY_RANK_XP[index];
    assert.equal(masteryRank(xp), index + 1);
    if (xp > 0) assert.equal(masteryRank(xp - 1), index);
  }
  assert.equal(masteryRank(NaN), 1);
  assert.equal(masteryRank(Infinity), 1);
  assert.equal(masteryRank(-200), 1);
  assert.equal(masteryProgress(120).progress, .5);
  assert.deepEqual(masteryProgress(1350), { rank: 6, xp: 1350, rankXp: 1350, nextXp: 1350, progress: 1, maxRank: 6 });
  const maximum = masteryBonuses('ember-mage', { xp: MASTERY_MAX_XP });
  assert.equal(maximum.hp, 1.1); assert.equal(maximum.attack, 1.075); assert.equal(maximum.cooldown, .925);
});

test('all eighteen classes have exactly two rank-three branches with implemented gameplay differences', () => {
  assert.equal(Object.keys(SPECIALIZATIONS).length, 18);
  assert.equal(new Set(Object.values(SPECIALIZATIONS).flat().map(branch => branch.id)).size, 36);
  for (const classId of Object.keys(CLASSES)) {
    const branches = SPECIALIZATIONS[classId]; assert.equal(branches.length, 2);
    const baseline = JSON.stringify(specializationFixture(classId).snapshot);
    for (const branch of branches) {
      assert.equal(branch.unlockRank, 3); assert.equal(branch.classId, classId);
      assert.ok(branch.name && branch.description && Object.keys(branch.mods).length);
      assert.equal(specializationFor(classId, branch.id), branch);
      const selected = specializationFixture(classId, branch.id);
      assert.notEqual(JSON.stringify(selected.snapshot), baseline, `${branch.name} changes real combat rather than metadata`);
      assert.equal(selected.caster.specialization, branch.id);
      assert.equal(selected.caster.masteryRank, 3);
    }
  }
});

test('save normalization rejects locked, unknown, cross-class and prototype specialization data', () => {
  const first = SPECIALIZATIONS['ember-mage'][0].id;
  const input = { 'ember-mage': { xp: 179, specialization: first }, necromancer: { xp: 900, specialization: first }, madeup: { xp: 300 } };
  const normalized = normalizeMastery(input);
  assert.equal(Object.keys(normalized).length, 18);
  assert.equal(normalized['ember-mage'].specialization, null);
  assert.equal(normalized.necromancer.specialization, null);
  assert.deepEqual(normalizeMastery(Object.create({ 'ember-mage': { xp: 999 } }))['ember-mage'], { xp: 0, specialization: null });
  assert.equal(specializationFor('constructor', first), null);
  assert.equal(normalizeMastery({ 'ember-mage': { xp: 1e12 } })['ember-mage'].xp, MASTERY_MAX_XP);
  assert.equal(selectSpecialization(input, 'ember-mage', first)['ember-mage'].specialization, null);
  const earned = { 'ember-mage': { xp: 180, specialization: null } };
  const selected = selectSpecialization(earned, 'ember-mage', first);
  assert.equal(selected['ember-mage'].specialization, first); assert.equal(earned['ember-mage'].specialization, null);
  assert.equal(selectSpecialization(selected, 'ember-mage', null)['ember-mage'].specialization, null);
});

test('battle mastery is a frozen snapshot; deployments, casts and direct contributions are public', () => {
  const branch = SPECIALIZATIONS['ember-mage'][1].id;
  const input = { 'ember-mage': { xp: 180, specialization: branch } };
  const battle = new Battle({ mastery: input });
  input['ember-mage'].xp = 0; input['ember-mage'].specialization = null;
  assert.ok(Object.isFrozen(battle.mastery) && Object.isFrozen(battle.mastery['ember-mage']));
  const mage = battle.summon('ember', { count: 5 })[0]; mage.x = 400; mage.spellCooldown = 0;
  assert.equal(battle.classStats['ember-mage'].deployments, 2);
  assert.equal(mage.specialization, branch); assert.equal(mage.masteryRank, 3);
  addEnemy(battle); battle.update(.05);
  assert.equal(battle.classStats['ember-mage'].casts, 1);
  assert.ok(battle.classStats['ember-mage'].damage > 0);
  const victim = addEnemy(battle, 450, 10); battle._hit(mage, victim, 10000);
  assert.equal(battle.classStats['ember-mage'].kills, 1);
  assert.equal(input['ember-mage'].xp, 0, 'combat never writes campaign XP');
});

test('XP is capped participation at a declared result, excludes unused classes and starts fresh after restart', () => {
  assert.deepEqual(calculateMasteryAwards({ 'ember-mage': { deployments: 20, casts: 100 } }), []);
  const stats = { 'ember-mage': { deployments: 5000, casts: 99999 }, necromancer: { deployments: 0, casts: 999 }, bad: { deployments: 999 } };
  const awards = calculateMasteryAwards(stats, { won: true, stage: 134 });
  assert.deepEqual(awards, [{ classId: 'ember-mage', xp: 61 }]);
  assert.ok(calculateMasteryAwards(stats, { won: false, stage: 134 })[0].xp < awards[0].xp);
  const input = { 'ember-mage': { xp: 150, specialization: null } };
  const updated = applyMasteryAwards(input, [...awards, ...awards]);
  assert.equal(updated['ember-mage'].xp, 211, 'duplicate class entries in one result cannot duplicate an award');
  assert.equal(input['ember-mage'].xp, 150);
  const battle = new Battle(); battle.summon('ember');
  const restarted = new Battle();
  assert.deepEqual(calculateMasteryAwards(restarted.classStats, { won: true, stage: 1 }), []);
  assert.equal(masteryRank(updated['ember-mage'].xp), 3);
});

test('necromancer branches preserve army, corpse, raise-budget and lifetime caps', () => {
  for (const branch of SPECIALIZATIONS.necromancer) {
    const { battle, caster } = specializationFixture('necromancer', branch.id);
    for (let index = 0; index < 40; index++) { const victim = addEnemy(battle, 425, 10, 'ashling'); battle._hit(null, victim, 10000); }
    battle.units = battle.units.filter(unit => !unit.dead);
    for (let index = 0; index < 20; index++) battle._raiseGuard(caster);
    assert.ok(caster.raises <= 7); assert.ok(battle.corpses.length <= 32);
    assert.ok(battle.units.length <= 64);
    assert.ok(battle.units.filter(unit => unit.side === 'player').length <= 40);
    assert.ok(battle.units.filter(unit => unit.undead).every(unit => unit.decay <= 27 && unit.noReward && unit.noCorpse));
    assert.equal(battle.classStats.necromancer.deployments, 1);
    assert.equal(battle.classStats.necromancer.raised, caster.raises);
  }
});

test('event reinforcements respect caps, elements and terminal battles without advancing wave progress', () => {
  const battle = new Battle({ stage: 100 });
  const before = { next: battle._next, wave: battle.wave, length: battle._schedule.length };
  const reinforced = battle.reinforce('ember', { lane: 2, affix: 'hexer' });
  assert.ok(reinforced); assert.equal(reinforced.element, 'ember'); assert.equal(reinforced.lane, 2);
  assert.equal(reinforced.affix, 'hexer'); assert.equal(reinforced.tag, 'Hexer Champion');
  assert.deepEqual({ next: battle._next, wave: battle.wave, length: battle._schedule.length }, before);
  assert.equal(battle.reinforce('rootking'), null); assert.equal(battle.reinforce('not-an-element'), null);
  battle.status = 'won'; assert.equal(battle.reinforce('void'), null);
});

test('champion affixes expose cues and execute bounded ward, frenzy, mending and hex abilities', () => {
  assert.equal(Object.keys(CHAMPION_AFFIXES).length, 6);
  for (const affix of Object.keys(CHAMPION_AFFIXES)) {
    const battle = new Battle({ stage: 100 });
    const champion = battle.reinforce('ashling', { affix }); champion.x = 500; champion.affixCooldown = 0;
    const target = battle.summon('sun')[0]; target.x = 420; target.spellCooldown = 100;
    const ally = addEnemy(battle, 510); ally.hp = 100;
    champion.hp = champion.maxHp * .4;
    const hp = ally.hp, shield = champion.shield;
    battle.update(.05);
    assert.ok(champion.affix && champion.tag);
    if (affix === 'warded') { assert.ok(champion.shield > shield); assert.equal(champion.wardRenewed, true); }
    if (affix === 'frenzied') assert.ok(champion.statuses.rally);
    if (affix === 'mender') assert.ok(ally.hp > hp || champion.hp > champion.maxHp * .4);
    if (affix === 'hexer') assert.ok(target.statuses.weakness && target.statuses.curse);
    if (affix === 'siphoner') assert.ok(champion.trait.lifesteal >= .15);
    if (affix === 'ironhide') assert.ok(champion.armor >= .08 && champion.trait.reflect > 0);
    assert.ok(champion.shield <= champion.maxHp * .8 + 50);
  }
});

test('the final non-multiple-of-four encounter is warded, named, scheduled and has two bounded phases', () => {
  const events = [], battle = new Battle({ stage: 134, onEvent: event => events.push(event) });
  assert.equal(battle.stage, 134); assert.equal(battle.stageInfo.boss, true); assert.equal(battle.enemyBarrier, true);
  const entry = battle._schedule.find(entry => ENEMIES[entry.key].boss);
  assert.equal(entry.name, 'The Eternal Unmaker');
  battle._spawnEnemy(entry); const boss = battle.units[0];
  boss.hp = boss.maxHp * .6; battle.update(.05);
  boss.hp = boss.maxHp * .25; battle.update(.05);
  battle.update(.1);
  assert.equal(events.filter(event => event.type === 'boss-phase').length, 2);
  assert.equal(boss.phaseIndex, 2); assert.ok(boss.shield < boss.maxHp);
  boss.hp = boss.maxHp; boss.x = 500; boss.spellCooldown = 0;
  const target = battle.summon('sun')[0]; target.x = 420; target.spellCooldown = 100;
  battle.update(.05); assert.ok(boss.spellCasts > 0); assert.ok(target.statuses.weakness);
  battle._hit(null, boss, 1e8); assert.equal(battle.enemyBarrier, false);
});

/** Legal matches, two seconds of thought plus cascade animation, and real command cooldowns. */
function playStrategically(config, rng) {
  const battle = new Battle(config), board = new Board({ rng, stage: config.stage, events: true });
  let nextMove = 0, charge = 0, overdriveUntil = 0;
  for (let t = 0; t < 360 && battle.status === 'playing'; t += .1) {
    if (battle.time >= nextMove) {
      const lane = [0, 1, 2].map(lane => ({ lane, pressure:
        battle.units.filter(u => u.side === 'enemy' && u.lane === lane).reduce((sum, u) => sum + (1000 - u.x) * u.hp / 100, 0) -
        battle.units.filter(u => u.side === 'player' && u.lane === lane).reduce((sum, u) => sum + u.hp / 100 * 190, 0),
      })).sort((a, b) => b.pressure - a.pressure)[0].lane;
      const front = battle.units.filter(u => u.side === 'enemy' && u.lane === lane).sort((a, b) => a.x - b.x)[0];
      const allies = battle.units.filter(u => u.side === 'player' && u.lane === lane);
      const moves = board.legalMoves(); let move = moves[0], best = -1;
      for (const candidate of moves) {
        board._swap(candidate.from, candidate.to);
        let score = board._matches().reduce((sum, group) => sum + group.cells.length *
          (front ? elementMultiplier(group.key, front.element) : 1) *
          (battle.troopFor(group.key).hp > 120 && !allies.some(u => u.range < 80) ? 1.25 : group.key === 'grove' && allies.length > 2 && !allies.some(u => u.key === 'grove') ? 1.2 : 1), 0);
        if (board.grid[candidate.from.r][candidate.from.c].special || board.grid[candidate.to.r][candidate.to.c].special) score += 9;
        board._swap(candidate.from, candidate.to);
        if (score > best) { best = score; move = candidate; }
      }
      const result = board.trySwap(move.from, move.to); assert.equal(result.valid, true);
      for (const step of result.steps) {
        for (const group of step.groups) battle.summon(group.key, { lane, count: group.count, chain: step.chain, elite: group.elite, buffs: group.buffs, power: battle.time < overdriveUntil ? 1.25 : 1 });
        charge += step.cleared.length;
        if (charge >= 50 && battle.time >= overdriveUntil) { charge -= 50; overdriveUntil = battle.time + 10; }
      }
      for (const event of result.events || []) {
        if (event.action === 'reinforcement') battle.reinforce(event.key, { lane });
        if (event.type === 'supply' && event.action === 'collected') battle.gold += 20;
        if (event.action === 'purified') { overdriveUntil = Math.max(overdriveUntil, battle.time + 5); charge += 8; }
      }
      const enemies = battle.units.filter(u => u.side === 'enemy');
      if (enemies.length) {
        const spellLane = [0, 1, 2].sort((a, b) => enemies.filter(u => u.lane === b).reduce((sum, u) => sum + Math.min(100, u.hp), 0) - enemies.filter(u => u.lane === a).reduce((sum, u) => sum + Math.min(100, u.hp), 0))[0];
        battle.cast('meteor', spellLane);
      }
      if (allies.length >= 2) battle.cast('rally', lane);
      if (battle.playerHp < battle.playerMax - 25 || allies.some(u => u.hp < u.maxHp * .6)) battle.cast('heal', lane);
      nextMove = battle.time + 2 + result.steps.length * .3;
    }
    battle.update(.1);
    assert.ok(battle.units.length <= 64 && battle.projectiles.length <= 100 && battle.corpses.length <= 32 && battle.effects.length <= 120);
  }
  return battle;
}

test('trained commanders can defeat 85, 108, 132 and the actual 134 finale using real boards and rare events', () => {
  const mastery = Object.fromEntries(Object.keys(CLASSES).map(id => [id, { xp: 1350, specialization: SPECIALIZATIONS[id][0].id }]));
  const upgrades = Object.fromEntries(UPGRADES.map(def => [def.id, 3]));
  for (const stage of [85, 108, 132, 134]) for (const initialSeed of [278, 417]) {
    let seed = initialSeed;
    const rng = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
    const battle = playStrategically({ stage, upgrades, mastery }, rng);
    assert.equal(battle.status, 'won', `stage ${stage}, seed ${initialSeed} has a trained strategic path`);
    assert.equal(battle.enemyBarrier, false);
    assert.ok(battle.time < 360);
    assert.ok(Object.values(battle.classStats).reduce((sum, record) => sum + record.casts, 0) > 0);
  }
});

test('fixed daily training is viable for today and a level-36 daily without campaign upgrades', () => {
  const today = '2026-11-04', current = createDaily(today, today);
  const candidates = Array.from({ length: 29 }, (_, index) => createDaily(new Date(Date.UTC(2026, 10, 4 - index)).toISOString().slice(0, 10), today));
  const late = candidates.find(config => config.stage === 36);
  assert.ok(late);
  for (const config of [current, late]) {
    assert.ok(Object.values(config.upgrades).every(rank => rank === 2));
    const battle = playStrategically({ ...config, rng: seededRng(config.combatSeed) }, seededRng(config.boardSeed));
    assert.equal(battle.status, 'won', `daily ${config.date} / ${config.armyName} is viable with its fixed training`);
    assert.equal(battle.enemyBarrier, false);
  }
});
