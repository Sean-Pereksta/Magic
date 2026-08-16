import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CAT_DOGPILE_RELEASE_DISTANCE,
  CAT_MIN_PATROL_SEPARATION,
  CAT_MIN_SPAWN_SEPARATION,
  catCountForPopulation,
  chooseSeparatedPatrolIndex,
  pointSeparation,
} from "./hearthmouse-expansion.mjs";

test("multi-cat thresholds remain exactly eight and fifteen mice", () => {
  assert.equal(catCountForPopulation(7), 1);
  assert.equal(catCountForPopulation(8), 2);
  assert.equal(catCountForPopulation(14), 2);
  assert.equal(catCountForPopulation(15), 3);
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

test("runtime cat roster validates reachability, spawn separation, and chase de-confliction", () => {
  const source = readFileSync(new URL("./hearthmouse-expansion.mjs", import.meta.url), "utf8");
  assert.match(source, /function catCanReachMainPatrol/);
  assert.match(source, /function repairCatSpawnLayout/);
  assert.match(source, /CAT_MIN_SPAWN_SEPARATION/);
  assert.match(source, /function targetAlreadyCovered/);
  assert.match(source, /proto\.updateCatPatrol = function keepPatrolPressureSpread/);
  assert.match(source, /spreadPatrolDestination\(this, cat\)/);
  assert.match(source, /Hearthmouse cat roster\/spawn mismatch/);
});
