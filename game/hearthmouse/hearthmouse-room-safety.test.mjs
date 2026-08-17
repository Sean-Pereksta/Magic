import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CAT_AGENT_RADIUS,
  CAT_CONVERGENCE_MAX_DISTANCE,
  CAT_CONVERGENCE_MIN_DISTANCE,
  DOORWAY_CLEARANCE_RADIUS,
  STATIC_DOORWAY_CLEARANCE_RADIUS,
  chooseConvergencePatrolIndex,
  colliderInsideDoorwayClearance,
  planarDistance,
  pointInsideDoorwayClearance,
} from "./hearthmouse-room-safety.mjs";

test("doorway clearance rejects props close enough to pinch an entrance", () => {
  const doorway = { x: 3, z: 6 };
  assert.equal(pointInsideDoorwayClearance({ x: 3.4, z: 6.2 }, doorway), true);
  assert.equal(pointInsideDoorwayClearance({ x: 3 + DOORWAY_CLEARANCE_RADIUS + 0.1, z: 6 }, doorway), false);
});

test("static doorway clearance catches crossing trim and boxes without deleting side walls", () => {
  const doorway = { x: 0.45, z: -11.62 };
  const crossingBaseboard = { minX: -1.3, maxX: 2.1, minZ: -11.85, maxZ: -11.7 };
  const crossingBox = { minX: 0.2, maxX: 0.75, minZ: -11.9, maxZ: -11.35 };
  const legitimateSideWall = { minX: 0.925, maxX: 2.1, minZ: -11.9, maxZ: -11.7 };

  assert.equal(colliderInsideDoorwayClearance(crossingBaseboard, doorway), true);
  assert.equal(colliderInsideDoorwayClearance(crossingBox, doorway), true);
  assert.equal(colliderInsideDoorwayClearance(legitimateSideWall, doorway), false);
  assert.ok(STATIC_DOORWAY_CLEARANCE_RADIUS < 0.475);
});

test("cat navigation uses the full cat-sized pathfinding radius", () => {
  assert.equal(CAT_AGENT_RADIUS, 0.205);
  assert.ok(CAT_AGENT_RADIUS > 0.18);
});

test("convergence patrol chooses overlap without putting cats on top of each other", () => {
  const other = { x: 0, z: 0 };
  const points = [
    { x: 0.3, z: 0 },
    { x: 2.2, z: 0 },
    { x: 4.8, z: 0 },
    { x: 8, z: 0 },
  ];
  const chosen = chooseConvergencePatrolIndex(points, other, 2);
  assert.equal(chosen, 2);
  const distance = planarDistance(points[chosen], other);
  assert.ok(distance >= CAT_CONVERGENCE_MIN_DISTANCE);
  assert.ok(distance <= CAT_CONVERGENCE_MAX_DISTANCE);
});

test("runtime guard validates entrances, clears dynamic and static blockers, and recovers disconnected cats", () => {
  const source = readFileSync(new URL("./hearthmouse-room-safety.mjs", import.meta.url), "utf8");
  assert.match(source, /clearDynamicPropsFromDoorways/);
  assert.match(source, /clearStaticCollidersFromDoorways/);
  assert.match(source, /deactivateStaticDoorwayCollider/);
  assert.match(source, /doorwayConnectivityFailures/);
  assert.match(source, /pathfind\(from, to, CAT_AGENT_RADIUS/);
  assert.match(source, /recoverDisconnectedCat/);
  assert.match(source, /reachablePatrolCount/);
  assert.match(source, /maybeConvergePatrol/);
  assert.match(source, /__roomSafetySetNightWrapped/);
});

test("startup guard loads room safety before runtime installation completes", () => {
  const source = readFileSync(new URL("./hearthmouse-expansion-startup-guard.mjs", import.meta.url), "utf8");
  assert.match(source, /import "\.\/hearthmouse-room-safety\.mjs";/);
});
