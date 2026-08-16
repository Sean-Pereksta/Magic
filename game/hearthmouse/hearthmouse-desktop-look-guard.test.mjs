import assert from "node:assert/strict";
import test from "node:test";

import {
  DESKTOP_MOUSE_LOOK_MAX_DELTA,
  DESKTOP_MOUSE_LOOK_YAW_SENSITIVITY,
  clampDesktopMouseDelta,
} from "./hearthmouse-desktop-look-guard.mjs";

test("normal desktop mouse deltas pass through unchanged", () => {
  assert.deepEqual(clampDesktopMouseDelta(24, -12), { x: 24, y: -12, clamped: false });
});

test("large desktop mouse deltas are direction-preserving and bounded", () => {
  const horizontal = clampDesktopMouseDelta(400, 0);
  assert.equal(horizontal.clamped, true);
  assert.equal(horizontal.x, DESKTOP_MOUSE_LOOK_MAX_DELTA);
  assert.equal(horizontal.y, 0);

  const diagonal = clampDesktopMouseDelta(300, 400);
  assert.equal(diagonal.clamped, true);
  assert.ok(Math.abs(Math.hypot(diagonal.x, diagonal.y) - DESKTOP_MOUSE_LOOK_MAX_DELTA) < 1e-9);
  assert.ok(Math.abs(diagonal.x / diagonal.y - 0.75) < 1e-9);
});

test("one anomalous mouse event cannot snap the camera dozens of degrees", () => {
  const safe = clampDesktopMouseDelta(1000, 0);
  const degrees = safe.x * DESKTOP_MOUSE_LOOK_YAW_SENSITIVITY * 180 / Math.PI;
  assert.ok(degrees < 13);
});

test("non-finite desktop mouse deltas are swallowed safely", () => {
  assert.deepEqual(clampDesktopMouseDelta(Infinity, Number.NaN), { x: 0, y: 0, clamped: true });
});
