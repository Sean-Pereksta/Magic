import test from "node:test";
import assert from "node:assert/strict";

import {
  installHearthmouseGamepadPreflight,
  sanitizeGamepadList,
} from "./hearthmouse-controller-preflight.mjs";

test("sparse Gamepad API results never expose null slots to Hearthmouse", () => {
  const xbox = { index: 0, id: "Xbox Wireless Controller", connected: true };
  const second = { index: 3, id: "Second Controller", connected: true };
  const disconnected = { index: 1, id: "Old Controller", connected: false };

  const sparse = new Array(4);
  sparse[0] = xbox;
  sparse[1] = null;
  sparse[2] = disconnected;
  sparse[3] = second;

  assert.deepEqual(sanitizeGamepadList(sparse), [xbox, second]);
});

test("preflight wraps getGamepads so controller menus receive only live devices", () => {
  const xbox = { index: 2, id: "Xbox Wireless Controller", connected: true };
  const targetNavigator = {
    calls: 0,
    getGamepads() {
      this.calls += 1;
      return [null, xbox, undefined];
    },
  };

  assert.equal(installHearthmouseGamepadPreflight(targetNavigator), true);
  assert.deepEqual(targetNavigator.getGamepads(), [xbox]);
  assert.equal(targetNavigator.calls, 1);
});

test("preflight falls back to legacy webkitGetGamepads when needed", () => {
  const pad = { index: 0, id: "Legacy Controller", connected: true };
  const targetNavigator = {
    webkitGetGamepads() {
      return [pad, null];
    },
  };

  assert.equal(installHearthmouseGamepadPreflight(targetNavigator), true);
  assert.deepEqual(targetNavigator.getGamepads(), [pad]);
});
