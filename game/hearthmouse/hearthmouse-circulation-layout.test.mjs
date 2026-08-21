import test from "node:test";
import assert from "node:assert/strict";
import {
  AGENT_CLEARANCES,
  DOORWAY_CORRIDORS,
  ROOM_LAYOUTS,
  SAFE_DECOR_REGIONS,
  SECRET_ROUTE_ENTRANCES,
  TRAP_ANCHORS,
  TRAVEL_LANES,
  doorwayProtectedBounds,
  validateCirculationPlacement,
  validateTrapAnchors,
} from "./hearthmouse-circulation-layout.mjs";
import { BACKROOM_DECORATIONS, validateDecorLayout } from "./hearthmouse-backroom-decor.mjs";
import { EVENT_PROP_ANCHORS } from "./hearthmouse-living-house.mjs";

test("all authored decor, trap anchors, and event props pass the shared circulation validator", () => {
  assert.deepEqual(validateDecorLayout(), []);
  assert.deepEqual(validateTrapAnchors(), []);
  for (const prop of Object.values(EVENT_PROP_ANCHORS).flat()) {
    assert.deepEqual(validateCirculationPlacement(prop, { padding: 0.1 }), [], prop.name);
  }
});

test("every doorway protects its whole opening plus a deep cat turning funnel", () => {
  assert.equal(DOORWAY_CORRIDORS.length, 15);
  assert.ok(AGENT_CLEARANCES.cat > AGENT_CLEARANCES.mouse);
  for (const doorway of DOORWAY_CORRIDORS) {
    const bounds = doorwayProtectedBounds(doorway);
    const protectedWidth = doorway.width + doorway.catTurnMargin * 2;
    const actualWidth = doorway.travelAxis === "z" ? bounds.maxX - bounds.minX : bounds.maxZ - bounds.minZ;
    const actualDepth = doorway.travelAxis === "z" ? bounds.maxZ - bounds.minZ : bounds.maxX - bounds.minX;
    assert.ok(actualWidth >= protectedWidth - 1e-8, doorway.id);
    assert.ok(actualDepth >= 2.14, doorway.id);
  }
});

test("every side room stays detailed while retaining a continuous authored travel lane", () => {
  const sideRooms = ROOM_LAYOUTS.filter((room) => !["living", "kitchen"].includes(room.id));
  assert.equal(SAFE_DECOR_REGIONS.length, ROOM_LAYOUTS.length);
  for (const room of sideRooms) {
    assert.ok(TRAVEL_LANES.some((lane) => lane.roomId === room.id), `${room.id} lane`);
    assert.ok(BACKROOM_DECORATIONS.filter((spec) => spec.room === room.id).length >= 4, `${room.id} decor`);
  }
  assert.ok(TRAVEL_LANES.some((lane) => lane.kind === "stairs"));
  for (const id of ["hallway-center", "hallway-spine", "hallway-north-branch"]) assert.ok(TRAVEL_LANES.some((lane) => lane.id === id));
});

test("traps cannot overlap doorways, stairs, lanes, or secret entrances", () => {
  for (const trap of TRAP_ANCHORS) {
    const failures = validateCirculationPlacement(trap, { padding: 0.1 });
    assert.equal(failures.length, 0, `${trap.id}: ${JSON.stringify(failures)}`);
  }
  assert.equal(SECRET_ROUTE_ENTRANCES.length, 10);
});

test("the cat-accessible doorway graph connects every authored room", () => {
  const connected = new Set(["living"]);
  for (let changed = true; changed;) {
    changed = false;
    for (const doorway of DOORWAY_CORRIDORS) {
      const [a, b] = doorway.rooms;
      if (connected.has(a) === connected.has(b)) continue;
      connected.add(a);
      connected.add(b);
      changed = true;
    }
  }
  assert.deepEqual([...ROOM_LAYOUTS.map((room) => room.id).filter((id) => !connected.has(id))], []);
});
