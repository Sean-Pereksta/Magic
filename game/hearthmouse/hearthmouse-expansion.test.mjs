import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  POLICY_PROFILES,
  ROOM_DEFINITIONS,
  activeForagerLimit,
  catCountForPopulation,
  computePounceWindup,
  forageTarget,
  initialForagerDelay,
  nextColonyPolicy,
  roomUnlocked,
  scoreFoodCandidate,
  scoreShelterEscape,
  selectNightPlan,
  shouldForagersStop,
  shouldPauseNewAssignments,
  shouldRecoverMouse,
} from "./hearthmouse-expansion.mjs";

test("the compact policy control cycles through every risk mode", () => {
  assert.equal(nextColonyPolicy("cautious"), "balanced");
  assert.equal(nextColonyPolicy("balanced"), "desperate");
  assert.equal(nextColonyPolicy("desperate"), "cautious");
  assert.equal(nextColonyPolicy("unknown"), "desperate");
});

test("balanced is the stable default while only cautious stops at the base need", () => {
  assert.equal(POLICY_PROFILES.balanced.id, "balanced");
  assert.equal(forageTarget("cautious", 10), 10);
  assert.equal(forageTarget("balanced", 10), 13);
  assert.equal(forageTarget("desperate", 10), 18);
  assert.equal(shouldForagersStop({ policy: "balanced", delivered: 10, nightlyRequirement: 10 }), false);
  assert.equal(shouldForagersStop({ policy: "desperate", delivered: 10, nightlyRequirement: 10 }), false);
  assert.equal(shouldPauseNewAssignments({ policy: "cautious", delivered: 8, committed: 2, nightlyRequirement: 10 }), true);
  assert.equal(shouldPauseNewAssignments({ policy: "balanced", delivered: 8, committed: 5, nightlyRequirement: 10 }), false);
  assert.equal(shouldPauseNewAssignments({ policy: "desperate", delivered: 8, committed: 10, nightlyRequirement: 10 }), false);
  assert.equal(shouldPauseNewAssignments({ policy: "balanced", delivered: 13, committed: 0, nightlyRequirement: 10 }), true);
});

test("foragers deploy promptly in bounded command-dependent waves", () => {
  const cautious = Array.from({ length: 24 }, (_, index) => initialForagerDelay("cautious", index, 1));
  const balanced = Array.from({ length: 24 }, (_, index) => initialForagerDelay("balanced", index, 1));
  const desperate = Array.from({ length: 24 }, (_, index) => initialForagerDelay("desperate", index, 1));
  assert.ok(Math.max(...cautious) < 2);
  assert.ok(Math.max(...balanced) < 1.3);
  assert.ok(Math.max(...desperate) < 0.8);
  assert.ok(desperate[0] < balanced[0] && balanced[0] < cautious[0]);
});

test("forager concurrency follows colony orders without allowing mass inactivity", () => {
  const cautious = activeForagerLimit("cautious", 20, 20);
  const balanced = activeForagerLimit("balanced", 20, 20);
  const desperate = activeForagerLimit("desperate", 20, 20);
  assert.ok(cautious >= 10);
  assert.ok(cautious < balanced);
  assert.ok(balanced < desperate);
  assert.equal(desperate, 20);
  assert.ok(activeForagerLimit("balanced", 20, 1) >= 17);
  assert.equal(activeForagerLimit("desperate", 20, 1), 20);
  assert.equal(activeForagerLimit("balanced", 20, 0), 0);
});

test("cat pressure scales at eight and fifteen total mice", () => {
  assert.equal(catCountForPopulation(1), 1);
  assert.equal(catCountForPopulation(7), 1);
  assert.equal(catCountForPopulation(8), 2);
  assert.equal(catCountForPopulation(14), 2);
  assert.equal(catCountForPopulation(15), 3);
  assert.equal(catCountForPopulation(24), 3);
});

test("ally escape and recovery code never drops carried food", () => {
  const source = readFileSync(new URL("./hearthmouse-expansion.mjs", import.meta.url), "utf8");
  assert.doesNotMatch(source, /dropMouseFood/);
  assert.match(source, /mouse\.resumeTask = mouse\.carriedFood \? "returning"/);
  assert.match(source, /Never discard gathered food/);
  assert.match(source, /mouse\.task === "returning" && mouse\.carriedFood/);
});

test("the movement watchdog only recovers genuinely stalled travel tasks", () => {
  assert.equal(shouldRecoverMouse({ task: "waiting", stalledFor: 20, distanceMoved: 0, distanceToGoal: 10 }), false);
  assert.equal(shouldRecoverMouse({ task: "to-food", stalledFor: 1.1, distanceMoved: 0, distanceToGoal: 2 }), true);
  assert.equal(shouldRecoverMouse({ task: "to-food", stalledFor: 1.1, distanceMoved: 0.1, distanceToGoal: 2 }), false);
  assert.equal(shouldRecoverMouse({ task: "returning", stalledFor: 1.3, distanceMoved: 0, distanceToGoal: 0.1 }), false);
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

test("balanced and desperate distribute mice across distant scavenging sectors", () => {
  const ordinary = { nestDistance: 11, mouseDistance: 9, depth: 4, value: 3, exposure: 0.08, sectorMatch: 0 };
  const assignedSector = { ...ordinary, sectorMatch: 1 };
  assert.ok(scoreFoodCandidate(assignedSector, "balanced", 0.5) < scoreFoodCandidate(ordinary, "balanced", 0.5));
  assert.ok(scoreFoodCandidate(assignedSector, "desperate", 0.5) < scoreFoodCandidate(ordinary, "desperate", 0.5));
});

test("escape scoring prefers cat-proof shelter away from the cat's direction", () => {
  const base = { pathDistance: 2, catDistance: 3, mouseEta: 0.7, catEta: 0.9, caution: 0.6 };
  const awayAndProtected = scoreShelterEscape({ ...base, directionDot: -0.9, catReachable: false });
  const towardAndProtected = scoreShelterEscape({ ...base, directionDot: 0.9, catReachable: false });
  const awayButReachable = scoreShelterEscape({ ...base, directionDot: -0.9, catReachable: true });
  assert.ok(awayAndProtected < towardAndProtected);
  assert.ok(awayAndProtected < awayButReachable);
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
