import assert from "node:assert/strict";
import test from "node:test";

import { ROOM_LAYOUT_BY_ID } from "./hearthmouse-circulation-layout.mjs";
import {
  MOUSE_ONLY_RUNS,
  ROOM_DESIGN_PROFILES,
  validateMouseOnlyRuns,
} from "./hearthmouse-room-design-pass.mjs";

function channelBrightness(hex) {
  const red = (hex >> 16) & 0xff;
  const green = (hex >> 8) & 0xff;
  const blue = hex & 0xff;
  return (red + green + blue) / (255 * 3);
}

test("every unlockable side room has a strong non-white design profile", () => {
  const roomIds = Object.keys(ROOM_DESIGN_PROFILES);
  assert.equal(roomIds.length, 13);
  assert.equal(roomIds.includes("living"), false);
  assert.equal(roomIds.includes("kitchen"), false);

  const labels = new Set();
  for (const roomId of roomIds) {
    const room = ROOM_LAYOUT_BY_ID.get(roomId);
    const profile = ROOM_DESIGN_PROFILES[roomId];
    assert.ok(room, `${roomId} must exist in the shared room catalogue`);
    assert.ok(room.unlockNight > 1, `${roomId} must be an unlockable room`);
    assert.equal(labels.has(profile.label), false, `${roomId} should have a unique room identity`);
    labels.add(profile.label);

    for (const key of ["wall", "lower", "ceiling", "floor"]) {
      assert.ok(channelBrightness(profile[key]) < 0.72, `${roomId} ${key} should not fall back to a white/pale shell`);
    }
    assert.notEqual(profile.wall, profile.lower, `${roomId} needs a two-tone wall treatment`);
  }
});

test("mouseways are true mouse-only shelter corridors and stay out of circulation", () => {
  assert.ok(MOUSE_ONLY_RUNS.length >= 6);
  assert.deepEqual(validateMouseOnlyRuns(), []);

  const rooms = new Set();
  for (const run of MOUSE_ONLY_RUNS) {
    rooms.add(run.room);
    assert.equal(run.mouseOnly, true, `${run.id} must be mouse-only`);
    assert.equal(run.catOnly, true, `${run.id} roof must block cats`);
    assert.ok(run.h <= 0.2, `${run.id} should remain mouse-scale`);
    assert.ok(ROOM_DESIGN_PROFILES[run.room], `${run.id} should live in a redesigned room`);
    assert.equal(ROOM_LAYOUT_BY_ID.get(run.room)?.unlockNight, run.unlockNight, `${run.id} should unlock with its room`);
  }

  assert.ok(rooms.size >= 6, "mouseways should be distributed across the house instead of clustering in one room");
});

test("the mouseway set visibly continues several existing secret-route themes", () => {
  const routeIds = MOUSE_ONLY_RUNS.map((run) => run.routeId).filter(Boolean);
  assert.ok(routeIds.includes("pantry-cellar-chute"));
  assert.ok(routeIds.includes("bedroom-closet-run"));
  assert.ok(routeIds.includes("toy-baseboard-hole"));
  assert.ok(routeIds.includes("refrigerator-utility-run"));
  assert.ok(routeIds.includes("garage-utility-vent"));
});
