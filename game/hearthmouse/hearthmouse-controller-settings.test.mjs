import test from "node:test";
import assert from "node:assert/strict";

import {
  DEFAULT_CONTROLLER_SETTINGS,
  applyControllerDeadzone,
  controllerAxisVector,
  normalizeControllerSettings,
} from "./hearthmouse-graphics-quality.mjs";

test("controller settings normalize invalid mappings and preserve valid custom mappings", () => {
  const normalized = normalizeControllerSettings({
    enabled: false,
    deadzone: 0.2,
    lookSensitivity: 1.35,
    bindings: {
      moveAxes: [4, 5],
      lookAxes: [2, 2],
      jump: 11,
      interact: -1,
    },
  });

  assert.equal(normalized.enabled, false);
  assert.equal(normalized.deadzone, 0.2);
  assert.equal(normalized.lookSensitivity, 1.35);
  assert.deepEqual(normalized.bindings.moveAxes, [4, 5]);
  assert.deepEqual(normalized.bindings.lookAxes, DEFAULT_CONTROLLER_SETTINGS.bindings.lookAxes);
  assert.equal(normalized.bindings.jump, 11);
  assert.equal(normalized.bindings.interact, DEFAULT_CONTROLLER_SETTINGS.bindings.interact);
});

test("radial deadzone removes stick drift and rescales useful travel", () => {
  assert.deepEqual(applyControllerDeadzone(0.05, -0.05, 0.16), { x: 0, y: 0, magnitude: 0 });
  const full = applyControllerDeadzone(1, 0, 0.16);
  assert.equal(full.x, 1);
  assert.equal(full.y, 0);
  assert.equal(full.magnitude, 1);

  const partial = applyControllerDeadzone(0.58, 0, 0.16);
  assert.ok(partial.x > 0.49 && partial.x < 0.51);
  assert.equal(partial.y, 0);

  const diagonal = applyControllerDeadzone(1, 1, 0.16);
  assert.ok(Math.abs(Math.hypot(diagonal.x, diagonal.y) - 1) < 1e-9);
});

test("axis vectors honor remapped stick pairs", () => {
  const gamepad = { axes: [0.8, -0.4, -0.7, 0.5, 0.9, -0.2] };
  const vector = controllerAxisVector(gamepad, [4, 5], 0.1);
  assert.ok(vector.x > 0.85);
  assert.ok(vector.y < -0.15);
  assert.ok(vector.magnitude > 0.8);
});
