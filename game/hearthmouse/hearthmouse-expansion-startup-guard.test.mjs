import assert from "node:assert/strict";
import test from "node:test";

import {
  ensureExpansionSight,
  installExpansionSightGuard,
} from "./hearthmouse-expansion-startup-guard.mjs";

class Vector3 {
  constructor(x = 0, y = 0, z = 0) {
    this.x = x;
    this.y = y;
    this.z = z;
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
