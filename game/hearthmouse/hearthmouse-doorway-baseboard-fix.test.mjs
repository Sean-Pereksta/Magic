import assert from "node:assert/strict";
import test from "node:test";

import {
  DOORWAY_FLOORBOARD_CLEARANCE,
  LEGACY_DOORWAY_BASEBOARDS,
  clearExpandedDoorwayFloorboards,
  colliderNearDoorway,
  isLowFloorboardCollider,
} from "./hearthmouse-doorway-baseboard-fix.mjs";

function makeMesh(name, visible = true) {
  return { name, visible };
}

function makeCollider(name, {
  minX,
  maxX,
  minZ,
  maxZ,
  minY = 0,
  maxY = 0.14,
  active = true,
  catOnly = false,
}) {
  return { name, minX, maxX, minZ, maxZ, minY, maxY, active, catOnly };
}

test("floorboard classification catches low doorway trim without treating walls as floorboards", () => {
  const doorway = { x: 3.6, z: 6.18 };
  const crossingBoard = makeCollider("crossing-board", {
    minX: -10.35, maxX: 10.35, minZ: 6.17, maxZ: 6.27,
  });
  const sideBoard = makeCollider("side-board", {
    minX: 7.2, maxX: 8.2, minZ: 6.17, maxZ: 6.27,
  });
  const wall = makeCollider("wall", {
    minX: 3.45, maxX: 3.75, minZ: 6.15, maxZ: 6.35, minY: 0, maxY: 2.95,
  });

  assert.equal(isLowFloorboardCollider(crossingBoard), true);
  assert.equal(colliderNearDoorway(crossingBoard, doorway), true);
  assert.equal(colliderNearDoorway(sideBoard, doorway), false);
  assert.equal(isLowFloorboardCollider(wall), false);
  assert.ok(DOORWAY_FLOORBOARD_CLEARANCE > 0.6);
});

test("repair waits for expansion doorway geometry instead of finishing against the base house too early", () => {
  const world = { root: {}, colliders: [], occluders: [], setNight() {} };
  assert.equal(clearExpandedDoorwayFloorboards({ world, __expansion: { navEdges: [] } }), false);
  assert.equal(world.__doorwayBaseboardsRepaired, undefined);
});

test("repair removes the real low doorway blocker, keeps harmless trim, and reruns after night changes", () => {
  const meshes = [
    ...LEGACY_DOORWAY_BASEBOARDS.map((name) => makeMesh(name)),
    makeMesh("mystery-white-floor-board"),
    makeMesh("harmless-floor-board"),
    makeMesh("mouse-tunnel-roof"),
    makeMesh("full-height-wall"),
    makeMesh("late-floor-board", false),
  ];
  const byName = new Map(meshes.map((mesh) => [mesh.name, mesh]));

  const colliders = [
    makeCollider("south-baseboard", {
      minX: -10.35, maxX: 10.35, minZ: 6.17, maxZ: 6.27,
    }),
    makeCollider("north-baseboard-east", {
      minX: -6.8, maxX: 10.33, minZ: -6.27, maxZ: -6.17,
    }),
    makeCollider("mystery-white-floor-board", {
      minX: 3.15, maxX: 4.05, minZ: 6.15, maxZ: 6.25,
    }),
    makeCollider("harmless-floor-board", {
      minX: 7.5, maxX: 8.5, minZ: 8.2, maxZ: 8.3,
    }),
    makeCollider("mouse-tunnel-roof", {
      minX: -5.1, maxX: -4.6, minZ: -9.2, maxZ: -8.8, catOnly: true,
    }),
    makeCollider("full-height-wall", {
      minX: 3.45, maxX: 3.75, minZ: 6.15, maxZ: 6.35, minY: 0, maxY: 2.95,
    }),
    makeCollider("late-floor-board", {
      minX: -1.75, maxX: -0.85, minZ: -6.25, maxZ: -6.15, active: false,
    }),
  ];
  const lateCollider = colliders.find((collider) => collider.name === "late-floor-board");

  let setNightCalls = 0;
  const world = {
    root: {
      traverse(callback) {
        meshes.forEach(callback);
      },
      getObjectByName(name) {
        return byName.get(name);
      },
    },
    colliders,
    occluders: [...meshes],
    setNight() {
      setNightCalls++;
      lateCollider.active = true;
      byName.get("late-floor-board").visible = true;
    },
  };
  const engine = {
    world,
    __expansion: {
      navEdges: [
        { point: { x: 3.6, z: 6.18 } },
        { point: { x: -1.3, z: -6.18 } },
        { point: { x: -4.86, z: -9 } },
      ],
    },
  };

  assert.equal(clearExpandedDoorwayFloorboards(engine), true);
  assert.equal(colliders.find((c) => c.name === "south-baseboard").active, false);
  assert.equal(colliders.find((c) => c.name === "north-baseboard-east").active, false);
  assert.equal(colliders.find((c) => c.name === "mystery-white-floor-board").active, false);
  assert.equal(byName.get("mystery-white-floor-board").visible, false);

  assert.equal(colliders.find((c) => c.name === "harmless-floor-board").active, true);
  assert.equal(byName.get("harmless-floor-board").visible, true);
  assert.equal(colliders.find((c) => c.name === "mouse-tunnel-roof").active, true);
  assert.equal(colliders.find((c) => c.name === "full-height-wall").active, true);

  world.setNight(5);
  assert.equal(setNightCalls, 1);
  assert.equal(lateCollider.active, false);
  assert.equal(byName.get("late-floor-board").visible, false);
});
