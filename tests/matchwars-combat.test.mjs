import test from 'node:test';
import assert from 'node:assert/strict';
import { Battle, TROOPS, ENEMIES, UPGRADES, elementMultiplier, upgradeCost, difficultyForStage } from '../game/matchwars/combat.mjs';
import { Board } from '../game/matchwars/board.mjs';
import { TOTAL_STAGES, getStage } from '../game/matchwars/campaign.mjs';

const advance = (battle, seconds, action = () => {}) => { for (let t = 0; t < seconds && battle.status === 'playing'; t += .1) { action(t); battle.update(.1); } };

test('element counters form the promised cycle, with mutual sun/void pressure', () => {
  assert.equal(elementMultiplier('ember', 'grove'), 1.5);
  assert.equal(elementMultiplier('grove', 'ember'), .7);
  assert.equal(elementMultiplier('storm', 'tide'), 1.5);
  assert.equal(elementMultiplier('tide', 'ember'), 1.5);
  assert.equal(elementMultiplier('sun', 'void'), 1.35);
  assert.equal(elementMultiplier('void', 'sun'), 1.35);
  assert.equal(elementMultiplier('tide', 'tide'), 1);
});

test('six troop roles and diverse enemy traits are available', () => {
  assert.equal(Object.keys(TROOPS).length, 6);
  assert.ok(Object.keys(ENEMIES).length >= 18);
  for (const trait of ['healer', 'summoner', 'flying', 'regen', 'rangedResist', 'shieldBreak']) assert.ok(Object.values(ENEMIES).some(e => e[trait]));
  assert.equal(new Set(Object.values(TROOPS).map(t => t.spriteRow)).size, 6);
  assert.ok(UPGRADES.length >= 18);
  assert.ok(upgradeCost('combo', 2) > upgradeCost('combo', 1));
});

test('matching relics, elites, cascades and purchased upgrades strengthen summons', () => {
  const basic = new Battle().summon('tide')[0];
  const battle = new Battle({ upgrades: { relic: 2, combo: 2, 'tide-range': 2 } });
  const elite = battle.summon('tide', { lane: 2, chain: 3, buffs: ['fury', 'aegis', 'haste', 'vitality'], elite: true, count: 5 });
  assert.equal(elite.length, 2);
  assert.equal(elite[0].lane, 2);
  assert.ok(elite[0].attack > basic.attack * 2);
  assert.ok(elite[0].maxHp > basic.maxHp * 2);
  assert.ok(elite[0].speed > basic.speed);
  assert.ok(elite[0].shield > 30);
  assert.ok(elite[0].range > basic.range);
});

test('spells apply once, respect cooldowns, and recharge through elapsed simulation time', () => {
  const battle = new Battle({ upgrades: { spellcraft: 3 } });
  battle.playerHp -= 60;
  assert.equal(battle.cast('heal', 1), true);
  const hp = battle.playerHp;
  assert.equal(battle.cast('heal', 1), false);
  assert.equal(battle.playerHp, hp);
  assert.ok(battle.cooldowns.heal < battle.spellDurations.heal);
  advance(battle, 2);
  assert.ok(battle.cooldowns.heal < 16.8);
  assert.equal(battle.cast('unknown', 1), false);
});

test('ranged projectiles safely expire when their target dies', () => {
  const battle = new Battle();
  const archer = battle.summon('tide')[0];
  const foe = battle._makeUnit(ENEMIES.ashling, 'enemy', 1);
  archer.x = 300; foe.x = 450; foe.hp = 1;
  battle.units.push(foe);
  battle._attack(archer, foe);
  assert.equal(battle.projectiles.length, 1);
  foe.dead = true;
  battle.update(.1);
  assert.equal(battle.projectiles.length, 0);
  assert.ok(battle.units.every(unit => Number.isFinite(unit.hp)));
});

test('a neglected battlefield reaches defeat instead of freezing indefinitely', () => {
  const battle = new Battle({ stage: 1 });
  advance(battle, 180);
  assert.equal(battle.status, 'lost');
  assert.equal(battle.playerHp, 0);
  assert.ok(battle.time < 180);
});

test('defending all lanes can win and awards a bounded single battle reward', () => {
  const battle = new Battle({ stage: 1 });
  let last = -5;
  advance(battle, 160, time => {
    if (time - last >= 2) {
      for (let lane = 0; lane < 3; lane++) battle.summon(lane === 0 ? 'tide' : lane === 1 ? 'sun' : 'ember', { lane, power: 1.3 });
      last = time;
    }
  });
  assert.equal(battle.status, 'won');
  assert.ok(battle.reward() >= 77);
  const time = battle.time;
  battle.update(.5);
  assert.equal(battle.time, time);
});

test('caps limit long battles and bosses are forecast on the fourth stage', () => {
  const battle = new Battle({ stage: 4 });
  assert.ok(battle._schedule.some(entry => ENEMIES[entry.key].boss));
  for (let i = 0; i < 200; i++) battle.summon('sun', { count: 49, lane: i % 3 });
  assert.equal(battle.units.length, 40);
  assert.equal(battle.summon('unknown').length, 0);
  assert.ok(battle.forecast().every(entry => entry.counter && Number.isFinite(entry.time)));
  advance(battle, 5);
  assert.ok(battle.units.length <= 64);
  assert.ok(battle.effects.length <= 120);
});

test('a full friendly army reserves space for the boss and cannot deadlock its wave', () => {
  const battle = new Battle({ stage: 4 });
  for (let i = 0; i < 200; i++) battle.summon('sun', { count: 49, lane: i % 3, power: 4 });
  advance(battle, 100);
  assert.equal(battle.status, 'won');
  assert.ok(battle.time >= 49);
});

test('a boss keeps its fortress warded until the showdown is defeated', () => {
  const battle = new Battle({ stage: 4 });
  battle.enemyHp = 2;
  const attacker = battle.summon('storm')[0];
  attacker.x = 930;
  attacker.cooldown = 0;
  battle.update(.1);
  assert.equal(battle.enemyHp, 1);
  assert.equal(battle.enemyBarrier, true);
  assert.equal(battle.status, 'playing');
  const boss = battle._makeUnit(ENEMIES.rootking, 'enemy', 1);
  battle.units.push(boss);
  battle._hit(attacker, boss, 100000);
  battle.update(.1);
  assert.equal(battle.enemyBarrier, false);
  assert.equal(battle.status, 'won');
});

test('cleared-wave victory still requires the boss ward to be broken', () => {
  const battle = new Battle({ stage: 4 });
  battle._next = battle._schedule.length;
  battle.update(.1);
  assert.equal(battle.status, 'playing');
  battle.enemyBarrier = false;
  battle.update(.1);
  assert.equal(battle.status, 'won');
});

test('the campaign actually schedules every advertised enemy variant and all three bosses', () => {
  const scheduled = new Set();
  for (let stage = 1; stage <= 12; stage++) {
    const battle = new Battle({ stage });
    assert.equal(new Set(battle._schedule.map(entry => entry.lane)).size, 3);
    for (const entry of battle._schedule) scheduled.add(entry.key);
  }
  assert.deepEqual([...scheduled].sort(), Object.keys(ENEMIES).sort());
});

test('real matching boards can defeat introductory and boss encounters without purchased upgrades', () => {
  for (const stage of [1, 4, 8, 12]) {
    let seed = stage * 97;
    const rng = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
    const board = new Board({ rng });
    const battle = new Battle({ stage });
    let nextMove = 0;
    advance(battle, 210, () => {
      if (battle.time < nextMove) return;
      const lane = [0, 1, 2].map(lane => ({ lane, pressure:
        battle.units.filter(u => u.side === 'enemy' && u.lane === lane).reduce((sum, u) => sum + (1000 - u.x) * u.hp / u.maxHp, 0) -
        battle.units.filter(u => u.side === 'player' && u.lane === lane).length * 175,
      })).sort((a, b) => b.pressure - a.pressure)[0].lane;
      const options = board.legalMoves();
      const move = options[Math.floor(rng() * options.length)];
      const result = board.trySwap(move.from, move.to);
      assert.equal(result.valid, true);
      for (const step of result.steps) for (const group of step.groups) battle.summon(group.key, { lane, count: group.count, chain: step.chain, buffs: group.buffs, elite: group.elite });
      if (battle.time > 15) for (const spell of ['meteor', 'rally', 'heal']) battle.cast(spell, lane);
      nextMove = battle.time + 2 + result.steps.length * .3;
    });
    assert.equal(battle.status, 'won', `stage ${stage} should be winnable with normal legal matches`);
    assert.ok(battle.time < 210);
  }
});

test('support abilities and elemental damage change tactical outcomes', () => {
  const battle = new Battle({ upgrades: { 'grove-heal': 3, 'sun-shield': 2 } });
  const tank = battle.summon('sun')[0];
  tank.hp -= 50;
  const healer = battle.summon('grove')[0];
  healer.supportCooldown = 0;
  battle.update(.1);
  assert.ok(tank.hp > tank.maxHp - 50);
  const foe = battle._makeUnit(ENEMIES.thornling, 'enemy', 1);
  battle.units.push(foe);
  const mage = battle.summon('ember')[0];
  const before = foe.hp;
  battle._impactAttack(mage, foe, 10);
  assert.equal(Math.round(before - foe.hp), 15);
  assert.ok(foe.statuses.burn);
});

test('48 campaign encounters expose finite, steadily increasing difficulty metrics', () => {
  assert.equal(TOTAL_STAGES, 48);
  assert.equal(difficultyForStage(-20).stage, 1);
  assert.equal(difficultyForStage(500).stage, 48);
  assert.equal(new Battle({ stage: 500 }).stage, 48);
  let prior;
  for (let stage = 1; stage <= TOTAL_STAGES; stage++) {
    const difficulty = difficultyForStage(stage);
    const battle = new Battle({ stage });
    assert.ok(Object.values(difficulty).every(Number.isFinite));
    assert.equal(battle.totalWaves, stage <= 12 ? 3 : stage < 33 ? 4 : 5);
    assert.equal(battle._schedule.length, difficulty.scheduledUnits);
    assert.equal(Math.max(...battle._schedule.map(entry => entry.wave)), battle.totalWaves);
    assert.ok(battle._schedule.every(entry => entry.lane >= 0 && entry.lane <= 2 && entry.time >= 5 && ENEMIES[entry.key]));
    assert.ok(battle._schedule.every((entry, index, list) => !index || entry.time >= list[index - 1].time));
    if (prior) {
      assert.ok(difficulty.pressure > prior.pressure, `stage ${stage} increases normal incoming pressure`);
      assert.ok(difficulty.healthMultiplier >= prior.healthMultiplier);
      assert.ok(difficulty.attackMultiplier >= prior.attackMultiplier);
      assert.ok(difficulty.keepHealth > prior.keepHealth);
      assert.ok(difficulty.spawnInterval <= prior.spawnInterval);
      assert.ok(difficulty.waveInterval <= prior.waveInterval);
    }
    prior = difficulty;
  }
  assert.ok(prior.healthMultiplier < 2.6);
  assert.ok(prior.attackMultiplier < 2.3);
});

test('the original twelve stages retain their original opening, waves, and enemy strength', () => {
  for (let stage = 1; stage <= 12; stage++) {
    const battle = new Battle({ stage });
    assert.equal(battle.difficulty.totalWaves, 3);
    assert.equal(battle.difficulty.waveInterval, 20);
    assert.equal(battle.difficulty.spawnInterval, 1.65);
    assert.equal(battle.difficulty.healthMultiplier, Number((1 + (stage - 1) * .055).toFixed(3)));
    assert.equal(battle.difficulty.attackMultiplier, battle.difficulty.healthMultiplier);
    assert.equal(battle._schedule[0].time, 5);
    if (stage % 4 === 0) assert.equal(battle._schedule.find(entry => ENEMIES[entry.key].boss).time, 49);
  }
});

test('every chapter has a correctly named, forecast boss and later variants have distinct tactics', () => {
  const names = new Set(), baseTraits = JSON.stringify(ENEMIES);
  for (let stage = 4; stage <= 48; stage += 4) {
    const battle = new Battle({ stage });
    const bossIndex = battle._schedule.findIndex(entry => ENEMIES[entry.key].boss);
    const entry = battle._schedule[bossIndex];
    assert.ok(entry);
    assert.equal(entry.key, getStage(stage).bossKey);
    assert.equal(entry.name, getStage(stage).bossName);
    assert.equal(entry.time, battle.difficulty.bossAt);
    battle._next = bossIndex;
    assert.equal(battle.forecast()[0].name, entry.name);
    assert.equal(battle.forecast()[0].boss, true);
    assert.equal(battle.enemyBarrier, true);
    battle._spawnEnemy(entry);
    const boss = battle.units[0];
    assert.equal(boss.name, entry.name);
    assert.ok(boss.maxHp >= ENEMIES[entry.key].hp);
    assert.ok(boss.attack < 55, 'late bosses do not one-shot a trained army');
    assert.equal(!!boss.trait.secondWind, stage > 12);
    names.add(boss.name);
  }
  assert.equal(names.size, 12);
  assert.equal(JSON.stringify(ENEMIES), baseTraits, 'variant traits never mutate shared definitions');
  const sky = new Battle({ stage: 44 });
  assert.equal(sky._enemyDefinition(sky._schedule.find(entry => ENEMIES[entry.key].boss)).flying, true);
  const finale = new Battle({ stage: 48 });
  const emperor = finale._enemyDefinition(finale._schedule.find(entry => ENEMIES[entry.key].boss));
  assert.equal(emperor.chain, true);
  assert.equal(emperor.summonLimit, 6);
  assert.ok(emperor.shield > 100);
});

test('later veterans and half-health boss phases are visible, bounded, and activate only once', () => {
  const events = [];
  const battle = new Battle({ stage: 48, onEvent: event => events.push(event) });
  const veteranEntry = battle._schedule.find(entry => entry.elite);
  battle._spawnEnemy(veteranEntry);
  const veteran = battle.units[0];
  assert.equal(veteran.elite, true);
  assert.match(veteran.name, /^Veteran /);
  const bossEntry = battle._schedule.find(entry => ENEMIES[entry.key].boss);
  battle._spawnEnemy(bossEntry);
  const boss = battle.units[1];
  boss.hp = boss.maxHp * .45;
  const originalShield = boss.shield;
  battle.update(.1);
  assert.equal(events.filter(event => event.type === 'boss-phase').length, 1);
  assert.ok(boss.shield > originalShield);
  assert.ok(boss.statuses.rally);
  advance(battle, 10);
  assert.equal(events.filter(event => event.type === 'boss-phase').length, 1);
  assert.ok(!boss.statuses.rally);
});

test('a full trained army leaves room for the final boss and resolves the final ward', () => {
  const battle = new Battle({ stage: 48 });
  for (let i = 0; i < 100; i++) battle.summon('sun', { count: 49, lane: i % 3, power: 8 });
  assert.equal(battle.units.length, 40);
  advance(battle, 240);
  assert.equal(battle.status, 'won');
  assert.equal(battle.enemyBarrier, false);
  assert.ok(battle.time >= battle.difficulty.bossAt);
  assert.ok(battle.units.length <= 64);
  assert.ok(battle.projectiles.length <= 100);
  assert.ok(battle.effects.length <= 120);
});

test('new fronts and the finale are winnable through real boards with affordable campaign talents', () => {
  for (const [stage, rank] of [[13, 1], [24, 2], [36, 3], [48, 3]]) {
    const upgrades = Object.fromEntries(UPGRADES.map(def => [def.id, rank]));
    const cost = UPGRADES.reduce((sum, def) => sum + Array.from({ length: rank }, (_, level) => upgradeCost(def.id, level)).reduce((a, b) => a + b, 0), 0);
    const guaranteedGold = 120 + Array.from({ length: stage - 1 }, (_, index) => difficultyForStage(index + 1).rewardBonus + 35).reduce((a, b) => a + b, 0);
    assert.ok(cost <= guaranteedGold, `stage ${stage} loadout is affordable without replaying or kill rewards`);
    for (const initialSeed of [278, 417]) {
      let seed = initialSeed;
      const rng = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
      const board = new Board({ rng });
      const battle = new Battle({ stage, upgrades });
      let nextMove = 0, charge = 0, overdriveUntil = 0;
      advance(battle, 300, () => {
        if (battle.time < nextMove) return;
        const lane = [0, 1, 2].map(lane => ({ lane, pressure:
          battle.units.filter(u => u.side === 'enemy' && u.lane === lane).reduce((sum, u) => sum + (1000 - u.x) * u.hp / 100, 0) -
          battle.units.filter(u => u.side === 'player' && u.lane === lane).reduce((sum, u) => sum + u.hp / 100 * 190, 0),
        })).sort((a, b) => b.pressure - a.pressure)[0].lane;
        const options = board.legalMoves();
        const move = options[Math.floor(rng() * options.length)];
        const result = board.trySwap(move.from, move.to);
        assert.equal(result.valid, true);
        for (const step of result.steps) {
          for (const group of step.groups) battle.summon(group.key, { lane, count: group.count, chain: step.chain, buffs: group.buffs, elite: group.elite, power: battle.time < overdriveUntil ? 1.25 : 1 });
          charge += step.cleared.length;
          if (charge >= 50 && battle.time >= overdriveUntil) { charge -= 50; overdriveUntil = battle.time + 10; }
        }
        if (battle.time > 15) for (const spell of ['meteor', 'rally', 'heal']) battle.cast(spell, lane);
        nextMove = battle.time + 3.5 + result.steps.length * .3;
        assert.ok(battle.units.length <= 64);
      });
      assert.equal(battle.status, 'won', `stage ${stage}, board seed ${initialSeed} has a viable strategic path`);
      assert.ok(battle.time < 300);
      assert.ok(battle.reward() > difficultyForStage(stage).rewardBonus);
      assert.ok(battle.units.every(u => Number.isFinite(u.hp) && u.hp <= u.maxHp));
    }
  }
});
