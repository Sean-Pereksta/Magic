import test from "node:test";
import assert from "node:assert/strict";
import {
  BACKROOM_DECORATIONS,
  BACKROOM_DOORWAYS,
  CHILDREN_DOORWAY,
  ROOM_VISUAL_IDENTITIES,
  boxIntersectsDoorwayClearance,
  installBackroomDecor,
  validateDecorLayout,
  validateRoomIdentities,
} from "./hearthmouse-backroom-decor.mjs";
import { ROOM_LAYOUTS } from "./hearthmouse-circulation-layout.mjs";

test("backroom decorations preserve every known doorway clearance", () => {
  assert.deepEqual(validateDecorLayout(), []);
});

test("decor pass never adds furniture to the already-finished living room or kitchen", () => {
  assert.equal(BACKROOM_DECORATIONS.some((item) => item.room === "living" || item.room === "kitchen"), false);
});

test("children room storage is moved away from its entrance", () => {
  const childrenItems = BACKROOM_DECORATIONS.filter((item) => item.room === "children");
  assert.ok(childrenItems.length >= 5);
  assert.equal(childrenItems.some((item) => boxIntersectsDoorwayClearance(item, CHILDREN_DOORWAY)), false);
});

test("basement and basement access both receive multiple storage/furniture pieces", () => {
  assert.ok(BACKROOM_DECORATIONS.filter((item) => item.room === "basement").length >= 5);
  assert.ok(BACKROOM_DECORATIONS.filter((item) => item.room === "basement-access").length >= 4);
});

test("known doorway list includes children and basement transitions", () => {
  const ids = new Set(BACKROOM_DOORWAYS.map((doorway) => doorway.id));
  assert.ok(ids.has("children-door"));
  assert.ok(ids.has("basement-access-door"));
  assert.ok(ids.has("basement-door"));
});

test("every side room has a unique architectural palette, treatment, landmark, and light signature", () => {
  assert.deepEqual(validateRoomIdentities(), []);
  const sideRooms = ROOM_LAYOUTS.filter((room) => !["living", "kitchen"].includes(room.id));
  assert.equal(Object.keys(ROOM_VISUAL_IDENTITIES).length, sideRooms.length);
  for (const room of sideRooms) {
    const visual = ROOM_VISUAL_IDENTITIES[room.id];
    assert.ok(visual, room.id);
    assert.notEqual(visual.wall, 0xffffff, `${room.id} wall`);
    assert.notEqual(visual.lowerWall, 0xffffff, `${room.id} lower wall`);
    assert.ok(visual.light.color && visual.light.intensity > 0, `${room.id} light`);
  }
});

test("each redesigned room has a dominant landmark and denser authored detail", () => {
  for (const roomId of Object.keys(ROOM_VISUAL_IDENTITIES)) {
    const items = BACKROOM_DECORATIONS.filter((item) => item.room === roomId);
    assert.ok(items.length >= 7, `${roomId} detail count`);
    assert.ok(items.some((item) => item.landmark) || ["hallway"].includes(roomId), `${roomId} landmark geometry`);
  }
});

test("runtime room groups unlock together while locked-room colliders stay inactive", () => {
  class Vector3 {
    constructor(x = 0, y = 0, z = 0) { this.x = x; this.y = y; this.z = z; }
    set(x, y, z) { this.x = x; this.y = y; this.z = z; return this; }
  }
  class Group {
    constructor() {
      this.children = [];
      this.userData = {};
      this.position = new Vector3();
      this.rotation = { x: 0, y: 0, z: 0 };
      this.visible = true;
    }
    add(child) { this.children.push(child); child.parent = this; }
    updateMatrix() {}
    getObjectByName(name) {
      if (this.name === name) return this;
      for (const child of this.children) {
        const match = child.name === name ? child : child.getObjectByName?.(name);
        if (match) return match;
      }
      return null;
    }
  }
  class Mesh extends Group {
    constructor(geometry, material) { super(); this.geometry = geometry; this.material = material; }
  }
  class BoxGeometry { constructor(w, h, d) { this.size = [w, h, d]; } }
  class MeshStandardMaterial { constructor(options) { Object.assign(this, options); } }
  const root = new Group();
  const world = {
    root,
    colliders: [],
    occluders: [],
    shelterPoints: [],
    setNight() {},
  };
  const engine = {
    snapshot: { night: 1 },
    world,
    __expansion: {
      spatial: {
        colliders: { insertBounds() {} },
        occluders: { insertBounds() {} },
      },
    },
  };
  const I = { Group, Mesh, BoxGeometry, MeshStandardMaterial, Vector3 };

  assert.equal(installBackroomDecor(engine, I), true);
  const decorRoot = root.getObjectByName("hearthmouse-backroom-decor");
  assert.ok(decorRoot);
  assert.equal(root.getObjectByName("hearthmouse-room-identity-hallway").visible, false);
  assert.equal(root.getObjectByName("hearthmouse-room-identity-garage").visible, false);
  assert.ok(world.colliders.some((collider) => collider.__backroomDecor && collider.active === false));

  world.setNight(11);
  for (const roomId of Object.keys(ROOM_VISUAL_IDENTITIES)) {
    assert.equal(root.getObjectByName(`hearthmouse-room-identity-${roomId}`).visible, true, roomId);
  }
  assert.equal(world.colliders.filter((collider) => collider.__backroomDecor).every((collider) => collider.active), true);
});
