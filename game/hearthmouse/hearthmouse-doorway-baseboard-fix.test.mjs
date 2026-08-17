import assert from "node:assert/strict";
import test from "node:test";

import {
  LEGACY_DOORWAY_BASEBOARDS,
  horizontalSegmentsAroundOpenings,
  repairLegacyDoorwayBaseboards,
} from "./hearthmouse-doorway-baseboard-fix.mjs";

test("dining-room baseboard is split around the full doorway", () => {
  const spec = LEGACY_DOORWAY_BASEBOARDS.find((entry) => entry.name === "south-baseboard");
  const segments = horizontalSegmentsAroundOpenings(spec.minX, spec.maxX, spec.openings);
  assert.equal(segments.length, 2);
  const opening = spec.openings[0];
  const openingMin = opening.center - opening.width / 2;
  const openingMax = opening.center + opening.width / 2;
  assert.ok(segments[0].maxX <= openingMin);
  assert.ok(segments[1].minX >= openingMax);
});

test("hallway and pantry baseboard keeps both doorway gaps open", () => {
  const spec = LEGACY_DOORWAY_BASEBOARDS.find((entry) => entry.name === "north-baseboard-east");
  const segments = horizontalSegmentsAroundOpenings(spec.minX, spec.maxX, spec.openings);
  assert.equal(segments.length, 3);
  for (const opening of spec.openings) {
    const openingMin = opening.center - opening.width / 2;
    const openingMax = opening.center + opening.width / 2;
    assert.equal(segments.some((segment) => segment.minX < openingMax && segment.maxX > openingMin), false);
  }
});

test("runtime repair disables legacy collisions and recreates visual-only trim", () => {
  class BoxGeometry {
    constructor(width, height, depth) {
      this.parameters = { width, height, depth };
    }
  }
  class Mesh {
    constructor(geometry, material) {
      this.geometry = geometry;
      this.material = material;
      this.position = { x: 0, y: 0, z: 0, set: (x, y, z) => Object.assign(this.position, { x, y, z }) };
      this.rotation = { copy() {} };
      this.userData = {};
    }
  }
  const sources = new Map();
  for (const spec of LEGACY_DOORWAY_BASEBOARDS) {
    sources.set(spec.name, {
      name: spec.name,
      visible: true,
      material: {},
      geometry: new BoxGeometry(spec.maxX - spec.minX, 0.14, 0.1),
      position: { y: 0.07, z: spec.name === "south-baseboard" ? 6.22 : -6.22 },
      rotation: {},
      castShadow: false,
      receiveShadow: true,
    });
  }
  const added = [];
  const world = {
    root: {
      getObjectByName: (name) => sources.get(name),
      add: (mesh) => added.push(mesh),
    },
    colliders: [...sources.keys()].map((name) => ({ name, active: true })),
    occluders: [...sources.values()],
  };

  assert.equal(repairLegacyDoorwayBaseboards({ world }, { Mesh, BoxGeometry }), true);
  assert.equal(world.colliders.every((collider) => collider.active === false), true);
  assert.equal([...sources.values()].every((mesh) => mesh.visible === false), true);
  assert.equal(added.length, 5);
  assert.equal(added.every((mesh) => mesh.userData.__doorwaySafeTrim === true), true);
});
