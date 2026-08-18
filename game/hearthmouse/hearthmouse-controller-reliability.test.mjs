import test from "node:test";
import assert from "node:assert/strict";

import {
  controllerDisplayName,
  movementKeysForVector,
} from "./hearthmouse-controller-reliability.mjs";

test("Xbox controller ids are presented cleanly in the settings menu", () => {
  assert.equal(
    controllerDisplayName("Xbox Wireless Controller (STANDARD GAMEPAD Vendor: 045e Product: 0b13)"),
    "Xbox Wireless Controller",
  );
  assert.equal(
    controllerDisplayName("Xbox 360 Controller (XInput STANDARD GAMEPAD)"),
    "Xbox 360 Controller",
  );
});

test("desktop controller bridge maps left-stick direction to Hearthmouse WASD movement", () => {
  assert.deepEqual(movementKeysForVector(0, -1), {
    KeyW: true,
    KeyS: false,
    KeyA: false,
    KeyD: false,
  });
  assert.deepEqual(movementKeysForVector(-0.8, 0.7), {
    KeyW: false,
    KeyS: true,
    KeyA: true,
    KeyD: false,
  });
  assert.deepEqual(movementKeysForVector(0.1, -0.1), {
    KeyW: false,
    KeyS: false,
    KeyA: false,
    KeyD: false,
  });
});
