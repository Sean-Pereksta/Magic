import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CAT_DOGPILE_RELEASE_DISTANCE,
  CAT_MIN_NEST_SPAWN_SEPARATION,
  CAT_MIN_PATROL_SEPARATION,
  CAT_MIN_SPAWN_SEPARATION,
  chooseNestSafeSpawnIndex,
  chooseSeparatedPatrolIndex,
  pointSeparation,
} from "./hearthmouse-expansion.mjs";
import {
  catCountForNight,
  catSpeedMultiplierForNight,
  installNightCatPressureGuard,
} from "./hearthmouse-expansion-startup-guard.mjs";

test("multi-cat thresholds are driven by nights four and eight", () => {
  assert.equal(catCountForNight(1), 1);
  assert.equal(catCountForNight(3), 1);
  assert.equal(catCountForNight(4), 2);
  assert.equal(catCountForNight(7), 2);
  assert.equal(catCountForNight(8), 3);
  assert.equal(catCountForNight(12), 3);
});

test("cat speed rises gradually with the night and caps at twenty percent", () => {
  assert.equal(catSpeedMultiplierForNight(1), 1);
  assert.ok(Math.abs(catSpeedMultiplierForNight(4) - 1.06) < 1e-9);
  assert.ok(Math.abs(catSpeedMultiplierForNight(8) - 1.14) < 1e-9);
  assert.equal(catSpeedMultiplierForNight(11), 1.2);
  assert.equal(catSpeedMultiplierForNight(30), 1.2);
});

test("runtime cat roster ignores actual colony size and follows the current night", () => {
  class FakeEngine {}
  Object.defineProperty(FakeEngine.prototype, "__catPressureHotfixInstalled", { value: true });
  FakeEngine.prototype.createCats = function populationFactory() {
    const population = 1 + this.colony.filter((member) => member.alive).length;
    const count = population >= 15 ? 3 : population >= 8 ? 2 : 1;
    this.cats = Array.from({ length: count }, (_, index) => ({ id: index }));
  };
  FakeEngine.prototype.followCatPath = function returnSpeed(_cat, _delta, speed) {
    return speed;
  };

  assert.equal(installNightCatPressureGuard({ Engine: FakeEngine }), true);

  const engine = new FakeEngine();
  const realColony = [{ alive: true }, { alive: true }];
  engine.colony = realColony;
  engine.startOfNightPopulation = 3;
  engine.snapshot = { night: 4, population: 3 };

  engine.createCats();
  assert.equal(engine.cats.length, 2);
  assert.equal(engine.colony, realColony);
  assert.equal(engine.startOfNightPopulation, 3);
  assert.equal(engine.snapshot.population, 3);

  engine.snapshot.night = 8;
  engine.createCats();
  assert.equal(engine.cats.length, 3);

  engine.snapshot.night = 2;
  engine.colony = Array.from({ length: 20 }, () => ({ alive: true }));
  engine.startOfNightPopulation = 21;
  engine.snapshot.population = 21;
  engine.createCats();
  assert.equal(engine.cats.length, 1);

  engine.snapshot.night = 8;
  assert.ok(Math.abs(engine.followCatPath({}, 1 / 60, 1) - 1.14) < 1e-9);
});

test("patrol selection deliberately chooses the least crowded side of the house", () => {
  const points = [
    { x: -9, z: -4 },
    { x: -3, z: 0 },
    { x: 3, z: 0 },
    { x: 9, z: 4 },
  ];
  const occupied = [{ x: -8.5, z: -4.25 }];
  assert.equal(chooseSeparatedPatrolIndex(points, occupied, 0), 3);
  assert.ok(pointSeparation(points[3], occupied[0]) > pointSeparation(points[1], occupied[0]));
  assert.ok(CAT_MIN_SPAWN_SEPARATION > CAT_MIN_PATROL_SEPARATION);
  assert.ok(CAT_DOGPILE_RELEASE_DISTANCE > CAT_MIN_PATROL_SEPARATION);
});

test("secondary cats choose a spawn safely away from the nest and other cats", () => {
  const nest = { x: -6.78, z: 3.74 };
  const mabel = { x: 8.25, z: 4.55 };
  const points = [
    { x: -8.5, z: -4.25 },
    { x: 8.6, z: -4.65 },
    { x: 8.9, z: 4.8 },
    { x: 5.65, z: -2.9 },
  ];

  const choiceIndex = chooseNestSafeSpawnIndex(points, [mabel], nest, 0);
  assert.equal(choiceIndex, 1);
  assert.ok(pointSeparation(points[0], nest) < CAT_MIN_NEST_SPAWN_SEPARATION);
  assert.ok(pointSeparation(points[choiceIndex], nest) >= CAT_MIN_NEST_SPAWN_SEPARATION);
  assert.ok(pointSeparation(points[choiceIndex], mabel) >= CAT_MIN_SPAWN_SEPARATION);
});

test("runtime cat roster validates reachability, spawn separation, and chase de-confliction", () => {
  const source = readFileSync(new URL("./hearthmouse-expansion.mjs", import.meta.url), "utf8");
  const startupGuard = readFileSync(new URL("./hearthmouse-expansion-startup-guard.mjs", import.meta.url), "utf8");
  assert.match(source, /function catCanReachMainPatrol/);
  assert.match(source, /function repairCatSpawnLayout/);
  assert.match(source, /CAT_MIN_SPAWN_SEPARATION/);
  assert.match(source, /CAT_MIN_NEST_SPAWN_SEPARATION/);
  assert.match(source, /chooseNestSafeSpawnIndex/);
  assert.match(source, /function targetAlreadyCovered/);
  assert.match(source, /proto\.updateCatPatrol = function keepPatrolPressureSpread/);
  assert.match(source, /spreadPatrolDestination\(this, cat\)/);
  assert.match(source, /Hearthmouse cat roster\/spawn mismatch/);
  assert.match(startupGuard, /proto\.createCats = function nightDrivenCatRoster/);
  assert.match(startupGuard, /proto\.followCatPath = function progressivelyFasterCats/);
});
