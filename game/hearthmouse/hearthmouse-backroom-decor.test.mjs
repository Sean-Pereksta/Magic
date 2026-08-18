import test from "node:test";
import assert from "node:assert/strict";
import {
  BACKROOM_DECORATIONS,
  BACKROOM_DOORWAYS,
  CHILDREN_DOORWAY,
  boxIntersectsDoorwayClearance,
  validateDecorLayout,
} from "./hearthmouse-backroom-decor.mjs";

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
