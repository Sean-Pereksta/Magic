import test from "node:test";
import assert from "node:assert/strict";

import {
  crossesNestBoundary,
  nestGateCoordinates,
  pointInsideBounds,
  shouldTriggerMouseDetour,
} from "./hearthmouse-mouse-pathing-guard.mjs";

const nestBounds = Object.freeze({
  minX: -7.25,
  maxX: -5.82,
  minZ: 3.12,
  maxZ: 4.48,
});

test("nest gate sits outside the real east opening and aligns with the deposit lane", () => {
  const gate = nestGateCoordinates(nestBounds, { z: 3.74 });
  assert.ok(gate.x > nestBounds.maxX);
  assert.equal(gate.x, -5.54);
  assert.equal(gate.z, 3.74);
});

test("nest boundary detection distinguishes leaving and returning routes", () => {
  const inside = { x: -6.78, z: 3.74 };
  const outside = { x: -4.2, z: 2.2 };
  assert.equal(pointInsideBounds(nestBounds, inside), true);
  assert.equal(pointInsideBounds(nestBounds, outside), false);
  assert.equal(crossesNestBoundary(nestBounds, inside, outside), true);
  assert.equal(crossesNestBoundary(nestBounds, inside, { x: -6.2, z: 3.5 }), false);
});

test("wall recovery triggers quickly only for genuinely stalled travel", () => {
  assert.equal(shouldTriggerMouseDetour({
    stalledFor: 0.45,
    moved: 0,
    distanceToGoal: 1.2,
    cooldownElapsed: 0.5,
  }), true);
  assert.equal(shouldTriggerMouseDetour({
    stalledFor: 0.45,
    moved: 0.02,
    distanceToGoal: 1.2,
    cooldownElapsed: 0.5,
  }), false);
  assert.equal(shouldTriggerMouseDetour({
    stalledFor: 0.2,
    moved: 0,
    distanceToGoal: 1.2,
    cooldownElapsed: 0.5,
  }), false);
});
