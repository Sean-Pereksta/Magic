import assert from 'node:assert/strict';
import test from 'node:test';
import { HEROES } from '../game/moba-mania/data.mjs';
import { Game } from '../game/moba-mania/engine.mjs';

const teams = [['arc', 'angel', 'ranger'], ['bulwark', 'vela', 'cadence']];
const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-8, `expected ${expected}, got ${actual}`);
const findHero = (game, id) => game.units.find(unit => unit.heroId === id);

test('each hero exposes a draft role and an individual playstyle', () => {
  assert.deepEqual(new Set(Object.values(HEROES).map(hero => hero.specialty.id)),
    new Set(['tank', 'damage', 'support', 'control']));
  for (const { role, specialty } of Object.values(HEROES)) {
    assert.equal(role, `${specialty.label} · ${specialty.focus}`);
    assert.ok(specialty.color && specialty.description && specialty.effect);
  }
});

test('online tank and damage profiles interact without changing PvE or solo damage', () => {
  const game = new Game({ teams, seed: 17 });
  const marksman = findHero(game, 'ranger');
  const tank = findHero(game, 'bulwark');
  near(game.damage(tank, 100, marksman), 100 * 1.12 * .86);
  near(game.damage(marksman, 100, tank), 92);
  const minion = game.addUnit('footman', 1, 1000, 1000);
  near(game.damage(minion, 100, marksman), 100);
  const solo = new Game({ hero: 'ranger', seed: 17 });
  const soloTank = solo.makeHero('bulwark', 1, 2);
  near(solo.damage(soloTank, 100, solo.player), 100);
});

test('online support protects teammates and control extends crowd control with diminishing returns', () => {
  const game = new Game({ teams, seed: 17 });
  const control = findHero(game, 'arc');
  const support = findHero(game, 'angel');
  const teammate = findHero(game, 'ranger');
  const enemy = findHero(game, 'vela');
  teammate.hp -= 200;
  near(game.heal(teammate, 100, support), 122);
  game.shield(teammate, 100, 3, support);
  near(teammate.shield, 122);
  game.shield(support, 100, 3, support);
  near(support.shield, 100);
  game.applyStatus(enemy, 'slow', 3, { sourceId: control.id, factor: .5 });
  near(enemy.status.slow.remaining, 3.6);
  near(enemy.status.slow.factor, .575);
  game.applyStatus(enemy, 'stun', 1, { sourceId: control.id });
  near(enemy.status.stun.remaining, 1.2);
  game.time += 1.2;
  game.tickStatuses(enemy, 1.2);
  game.applyStatus(enemy, 'stun', 1, { sourceId: control.id });
  near(enemy.status.stun.remaining, .66);
});
