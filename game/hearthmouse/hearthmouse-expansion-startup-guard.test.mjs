import assert from "node:assert/strict";
import test from "node:test";

import {
  ensureExpansionMouseState,
  ensureExpansionSight,
  installExpansionSightGuard,
} from "./hearthmouse-expansion-startup-guard.mjs";

class Vector3 {
  constructor(x = 0, y = 0, z = 0) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  clone() {
    return new Vector3(this.x, this.y, this.z);
  }
}

const internals = { Vector3 };

test("initializes sight before the expansion assignment becomes visible", () => {
  const engine = { disposed: false };
  assert.equal(installExpansionSightGuard(engine, internals), true);
  assert.equal(engine.__expansion, undefined);

  const expansion = { spatial: {}, scratch: {} };
  engine.__expansion = expansion;

  assert.ok(expansion.sight.occluderCenter instanceof Vector3);
  assert.ok(expansion.sight.occluderScale instanceof Vector3);
  assert.equal(expansion.sight.probes.length, 5);

  const descriptor = Object.getOwnPropertyDescriptor(engine, "__expansion");
  assert.equal(descriptor.get, undefined);
  assert.equal(descriptor.set, undefined);
  assert.equal(descriptor.value, expansion);
  assert.equal(descriptor.writable, true);
});

test("repairs a partial expansion left behind by a failed startup", () => {
  const expansion = { spatial: {}, scratch: {} };
  const engine = { disposed: false, __expansion: expansion };

  assert.equal(installExpansionSightGuard(engine, internals), true);
  assert.ok(expansion.sight.occluderCenter instanceof Vector3);
  assert.ok(expansion.sight.occluderScale instanceof Vector3);

  const sight = expansion.sight;
  assert.equal(ensureExpansionSight(expansion, internals), true);
  assert.equal(expansion.sight, sight);
});

test("backfills expansion AI scratch state for mice created before the expansion loads", () => {
  const carriedFood = { id: "crumb" };
  const path = [new Vector3(4, 0, 2)];
  const mouse = {
    task: "returning",
    carriedFood,
    path,
    delay: 0.6,
    rig: { root: { position: new Vector3(1, 0.025, 3) } },
  };
  const engine = { disposed: false, mice: [mouse] };

  assert.equal(ensureExpansionMouseState(engine, internals), true);

  assert.deepEqual(mouse.nearestCatResult, { cat: null, distance: Infinity });
  assert.deepEqual(mouse.threatResult, { score: 0, cat: null, urgent: false });
  assert.ok(mouse.lastCatDistances instanceof Map);
  assert.ok(mouse.blockedFoodUntil instanceof Map);
  assert.ok(mouse.progressPosition instanceof Vector3);
  assert.ok(mouse.nestActivityGoalScratch instanceof Vector3);
  assert.equal(mouse.colonyIndex, 0);
  assert.equal(mouse.foragingSector, 0);

  // Startup repair must not reset live gameplay state.
  assert.equal(mouse.task, "returning");
  assert.equal(mouse.carriedFood, carriedFood);
  assert.equal(mouse.path, path);
  assert.equal(mouse.delay, 0.6);
});
