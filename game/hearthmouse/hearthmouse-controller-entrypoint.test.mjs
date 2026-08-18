import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  installHearthmouseGamepadPreflight,
  sanitizeGamepadList,
} from "./hearthmouse-controller-preflight.mjs";

test("sanitizeGamepadList removes null and disconnected Gamepad API slots", () => {
  const xbox = { index: 0, id: "Xbox Wireless Controller", connected: true };
  assert.deepEqual(sanitizeGamepadList([xbox, null, undefined, { index: 2, connected: false }]), [xbox]);
});

test("preflight falls back to the Navigator prototype when the instance is non-extensible", () => {
  const xbox = { index: 0, id: "Xbox Wireless Controller", connected: true };
  const prototype = {
    getGamepads() {
      return [xbox, null, null];
    },
  };
  const fakeNavigator = Object.preventExtensions(Object.create(prototype));

  assert.equal(installHearthmouseGamepadPreflight(fakeNavigator, null), true);
  assert.deepEqual(fakeNavigator.getGamepads(), [xbox]);
});

test("graphics quality entrypoint always loads controller preflight before the core menu", async () => {
  const source = await readFile(new URL("./hearthmouse-graphics-quality.mjs", import.meta.url), "utf8");
  const preflightIndex = source.indexOf('import "./hearthmouse-controller-preflight.mjs"');
  const coreIndex = source.indexOf('export * from "./hearthmouse-graphics-quality-core.mjs"');
  assert.ok(preflightIndex >= 0);
  assert.ok(coreIndex > preflightIndex);
});
