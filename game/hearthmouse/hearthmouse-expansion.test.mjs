import assert from "node:assert/strict";
import test from "node:test";

import {
  POLICY_PROFILES,
  ROOM_DEFINITIONS,
  computePounceWindup,
  forageTarget,
  roomUnlocked,
  scoreFoodCandidate,
  selectNightPlan,
  shouldForagersStop,
} from "./hearthmouse-expansion.mjs";

test("balanced is the stable default policy and only desperate targets surplus", () => {
  assert.equal(POLICY_PROFILES.balanced.id, "balanced");
  assert.equal(forageTarget("cautious", 10), 10);
  assert.equal(forageTarget("balanced", 10), 10);
  assert.equal(forageTarget("desperate", 10), 14);
  assert.equal(shouldForagersStop({ policy: "balanced", delivered: 10, nightlyRequirement: 10 }), true);
  assert.equal(shouldForagersStop({ policy: "desperate", delivered: 10, nightlyRequirement: 10 }), false);
});

test("pounce warning closes gradually but never beats the former maximum timing", () => {
  const hunter = Array.from({ length: 12 }, (_, index) => computePounceWindup(index + 1, "hunter"));
  const kitten = Array.from({ length: 12 }, (_, index) => computePounceWindup(index + 1, "kitten"));
  assert.ok(hunter.every((value, index) => index === 0 || value <= hunter[index - 1]));
  assert.ok(kitten.every((value, index) => index === 0 || value <= kitten[index - 1]));
  assert.equal(hunter.at(-1), 0.48);
  assert.equal(kitten.at(-1), 0.34);
  assert.ok(hunter.every((value) => value >= 0.48));
  assert.ok(kitten.every((value) => value >= 0.34));
});

test("cautious food scoring strongly prefers a safe local crumb", () => {
  const local = { nestDistance: 2, mouseDistance: 1.5, depth: 0, value: 1, exposure: 0 };
  const distant = { nestDistance: 13, mouseDistance: 8, depth: 5, value: 5, exposure: 0.15 };
  assert.ok(scoreFoodCandidate(local, "cautious", 0.8) < scoreFoodCandidate(distant, "cautious", 0.8));
  assert.ok(scoreFoodCandidate(distant, "desperate", 0.2) < scoreFoodCandidate(distant, "cautious", 0.8));
});

test("campaign rooms unlock on schedule or through a temporary open door", () => {
  assert.equal(roomUnlocked("hallway", 1), false);
  assert.equal(roomUnlocked("hallway", 2), true);
  assert.equal(roomUnlocked("garage", 10), false);
  assert.equal(roomUnlocked("garage", 10, "garage"), true);
  assert.equal(roomUnlocked("garage", 11), true);
});

test("the season contains a staged full-house exploration chain", () => {
  const ids = new Set(ROOM_DEFINITIONS.map((room) => room.id));
  for (const expected of ["hallway", "pantry", "dining", "bathroom", "bedroom", "children", "utility", "mudroom", "basement-access", "basement", "garage"]) {
    assert.ok(ids.has(expected), expected);
  }
  assert.ok(ROOM_DEFINITIONS.every((room) => room.unlockNight >= 2 && room.unlockNight <= 11));
});

test("night plans are deterministic, varied, and avoid an immediate repeat", () => {
  const seed = 48151623;
  const first = selectNightPlan(seed, 6);
  assert.deepEqual(selectNightPlan(seed, 6), first);
  assert.notEqual(selectNightPlan(seed, 7, first.event.id).event.id, first.event.id);
  const ids = new Set(Array.from({ length: 12 }, (_, index) => selectNightPlan(seed, index + 1).event.id));
  assert.ok(ids.size >= 6);
});
