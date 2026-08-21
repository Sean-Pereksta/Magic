import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { NIGHT_EVENTS } from "./hearthmouse-expansion-core.mjs";
import { SECRET_ROUTES } from "./hearthmouse-circulation-layout.mjs";
import {
  PLAYER_TRAP_DURATION_SECONDS,
  TUNNEL_INTERIOR_PROFILES,
  activeTrapCountForNight,
  playerTrapRemaining,
  selectTrapAnchors,
  secretRouteSafetyScore,
  shouldRiskKnownTrap,
  territoryRoomsFor,
  tunnelStalkPlan,
  tunnelTransitPoint,
  tunnelTransitProgress,
  tunnelTraversalDuration,
  trapAvoidanceRadius,
  trapDetourCandidates,
} from "./hearthmouse-living-house.mjs";

test("mouse tunnels cover the requested house routes and remain mouse-only", () => {
  const ids = new Set(SECRET_ROUTES.map((route) => route.id));
  for (const id of [
    "kitchen-pantry-run",
    "bedroom-closet-run",
    "dining-hallway-run",
    "refrigerator-utility-run",
    "bathtub-wall-run",
    "couch-burrow",
  ]) assert.ok(ids.has(id), id);
  assert.ok(SECRET_ROUTES.every((route) => route.mouseOnly && tunnelTraversalDuration(route) >= 0.65));
});

test("tunnel travel is continuous and takes time instead of relocating instantly", () => {
  const source = { x: 0, z: 0, normalX: 0, normalZ: 1 };
  const target = { x: 4, z: 3, normalX: -1, normalZ: 0 };
  assert.equal(tunnelTransitProgress(10, 10, 1), 0);
  assert.equal(tunnelTransitProgress(10, 10.5, 1), 0.5);
  assert.equal(tunnelTransitProgress(10, 11, 1), 1);
  assert.deepEqual(tunnelTransitPoint(source, target, 0), { x: 0, z: 0 });
  const middle = tunnelTransitPoint(source, target, 0.5);
  assert.ok(middle.x > 0 && middle.x < 4);
  assert.ok(middle.z > -0.22 && middle.z < 3);
  assert.deepEqual(tunnelTransitPoint(source, target, 1), { x: 3.76, z: 3 });
});

test("tunnel interiors are enclosed by family and urgent mice prefer the safer exit", () => {
  assert.deepEqual(new Set(SECRET_ROUTES.map((route) => route.family)), new Set(Object.keys(TUNNEL_INTERIOR_PROFILES)));
  const unsafeExit = secretRouteSafetyScore({
    entranceDistance: 0.5,
    directGoalDistance: 4,
    exitGoalDistance: 2,
    sourceCatDistance: 1.1,
    exitCatDistance: 0.45,
    noise: 0.1,
    urgent: true,
  });
  const safeExit = secretRouteSafetyScore({
    entranceDistance: 0.7,
    directGoalDistance: 4,
    exitGoalDistance: 3,
    sourceCatDistance: 1.1,
    exitCatDistance: 5.2,
    noise: 0.16,
    urgent: true,
  });
  assert.ok(safeExit > unsafeExit);
  const source = fs.readFileSync(new URL("./hearthmouse-living-house.mjs", import.meta.url), "utf8");
  assert.match(source, /mouse-tunnel-interior-/);
  assert.match(source, /alcove-floor/);
  assert.match(source, /setActiveTunnelInterior/);
  assert.match(source, /mouse\.rig\.root\.visible = false/);
  assert.match(source, /mouse\.rig\.root\.visible = true/);
});

test("exit stalking has bounded patience and at most one optional exit switch", () => {
  const first = tunnelStalkPlan("mabel", "couch-burrow", 12);
  const second = tunnelStalkPlan("mabel", "couch-burrow", 12);
  assert.deepEqual(first, second);
  assert.ok(first.patience >= 4.8 && first.patience <= 7.5);
  assert.ok(first.switchAfter > 0 && first.switchAfter < first.patience);
  assert.equal(typeof first.willSwitch, "boolean");
});

test("trap population grows by night and trap night raises it sharply", () => {
  assert.equal(activeTrapCountForNight(1), 0);
  assert.equal(activeTrapCountForNight(2), 1);
  assert.ok(activeTrapCountForNight(11) > activeTrapCountForNight(2));
  assert.ok(activeTrapCountForNight(8, { trapMultiplier: 2.4 }) >= activeTrapCountForNight(8) * 2);
  const selected = selectTrapAnchors({ night: 11, event: { id: "mouse-trap-night", trapMultiplier: 2.4 }, seed: 42 });
  assert.ok(selected.length >= 6);
  assert.deepEqual(selected, selectTrapAnchors({ night: 11, event: { id: "mouse-trap-night", trapMultiplier: 2.4 }, seed: 42 }));
});

test("trap immobilization lasts exactly five seconds", () => {
  assert.equal(PLAYER_TRAP_DURATION_SECONDS, 5);
  assert.equal(playerTrapRemaining(10, 10), 5);
  assert.ok(Math.abs(playerTrapRemaining(10, 14.999) - 0.001) < 1e-9);
  assert.equal(playerTrapRemaining(10, 15), 0);
  assert.equal(playerTrapRemaining(10, 18), 0);
});

test("colony policy changes both avoidance distance and willingness to risk a known trap", () => {
  assert.ok(trapAvoidanceRadius("cautious") > trapAvoidanceRadius("balanced"));
  assert.ok(trapAvoidanceRadius("balanced") > trapAvoidanceRadius("desperate"));
  assert.equal(shouldRiskKnownTrap("cautious", 1, 0), false);
  assert.equal(shouldRiskKnownTrap("balanced", 0.4, 0), false);
  assert.equal(shouldRiskKnownTrap("balanced", 1, 0), true);
  assert.equal(shouldRiskKnownTrap("desperate", 0.8, 0.5), true);
  const detours = trapDetourCandidates({ x: 0, z: 0 }, { x: 2, z: 0 }, { x: 1, z: 0 }, 0.5);
  assert.equal(detours.length, 2);
  assert.ok(detours[0].z > 0 && detours[1].z < 0);
});

test("cats have recognizable, distinct territories with deterministic nightly variation", () => {
  const mabel = territoryRoomsFor("mabel", 11);
  const biscuit = territoryRoomsFor("biscuit", 11);
  const pepper = territoryRoomsFor("pepper", 11);
  assert.ok(mabel.includes("kitchen") && mabel.includes("pantry"));
  assert.ok(biscuit.includes("hallway") && biscuit.includes("bedroom"));
  assert.ok(pepper.includes("basement") && pepper.includes("garage"));
  assert.notDeepEqual(territoryRoomsFor("mabel", 11), territoryRoomsFor("mabel", 12));
  assert.equal(new Set([...mabel, ...biscuit, ...pepper]).size >= 9, true);
});

test("major living-house events cover lighting, guests, vacuum, dog, catnip, window, groceries, and traps", () => {
  const ids = new Set(NIGHT_EVENTS.map((event) => event.id));
  for (const id of ["power-outage", "guests", "vacuum-night", "dog-visiting", "lights-left-on", "dropped-grocery-bag", "catnip", "open-window", "mouse-trap-night"]) {
    assert.ok(ids.has(id), id);
  }
  assert.equal(NIGHT_EVENTS.find((event) => event.id === "storm-night").lighting, "storm");
  assert.ok(NIGHT_EVENTS.find((event) => event.id === "power-outage").soundSensitivity > 1);
  const source = fs.readFileSync(new URL("./hearthmouse-living-house.mjs", import.meta.url), "utf8");
  assert.match(source, /discoveredRoutes/);
  assert.match(source, /assignTunnelExitStalk/);
  assert.match(source, /beginTunnelTransit/);
  assert.match(source, /tunnelOccludedTargetVisibility/);
  assert.match(source, /mouse-hole-/);
  assert.match(source, /engine\.emitNoise\?\.\(position, 2\.5\)/);
  assert.match(source, /releaseBlockedKeys/);
  assert.match(source, /applyKnownTrapDetours/);
  assert.match(source, /territoryPatrol/);
  assert.match(source, /secretEntranceSearch/);
});
