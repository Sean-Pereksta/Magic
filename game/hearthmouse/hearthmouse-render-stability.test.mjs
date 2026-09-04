import assert from "node:assert/strict";
import test from "node:test";

import { HearthmousePerformanceGovernor } from "./hearthmouse-performance-governor.mjs";
import {
  HEARTHMOUSE_STABLE_SHADOW_LIMITS,
  applyPersistentStructureVisibility,
  extractPersistentRoomStructures,
  isPersistentStructureName,
  stablePixelRatioCap,
} from "./hearthmouse-render-stability.mjs";

function sceneNode(name, { mesh = false } = {}) {
  return {
    name,
    isMesh: mesh,
    visible: true,
    castShadow: true,
    frustumCulled: false,
    matrixAutoUpdate: true,
    userData: {},
    children: [],
    parent: null,
    add(child) {
      child.parent?.remove?.(child);
      child.parent = this;
      this.children.push(child);
    },
    remove(child) {
      const index = this.children.indexOf(child);
      if (index >= 0) this.children.splice(index, 1);
      if (child.parent === this) child.parent = null;
    },
    attach(child) {
      this.add(child);
    },
    traverse(visitor) {
      visitor(this);
      for (const child of this.children) child.traverse?.(visitor) ?? visitor(child);
    },
    updateMatrix() {},
    updateWorldMatrix() {},
  };
}

test("only static room structure names are promoted out of cullable room groups", () => {
  assert.equal(isPersistentStructureName("kitchen-north-wall-0"), true);
  assert.equal(isPersistentStructureName("garage-baseboard-west"), true);
  assert.equal(isPersistentStructureName("attic-ceiling-panel"), true);
  assert.equal(isPersistentStructureName("living-room-chair"), false);
  assert.equal(isPersistentStructureName("kitchen-door"), false);
});

test("persistent walls survive room-group culling while decor stays cullable", () => {
  const roomRoot = sceneNode("expanded-house-rooms");
  const roomGroup = sceneNode("expanded-house-room-living");
  const wall = sceneNode("living-north-wall-0", { mesh: true });
  const trim = sceneNode("living-baseboard-north", { mesh: true });
  const chair = sceneNode("living-chair", { mesh: true });
  roomRoot.add(roomGroup);
  roomGroup.add(wall);
  roomGroup.add(trim);
  roomGroup.add(chair);

  let unlocked = true;
  const manager = {
    engine: { world: { __hearthmouseRoomGroups: new Map([["living", new Set([roomGroup])]]) } },
    roomUnlocked: () => unlocked,
  };

  assert.equal(extractPersistentRoomStructures(manager), 2);
  assert.equal(wall.parent, roomRoot);
  assert.equal(trim.parent, roomRoot);
  assert.equal(chair.parent, roomGroup);
  assert.equal(wall.frustumCulled, true);
  assert.equal(wall.matrixAutoUpdate, false);
  assert.equal(trim.castShadow, false);

  roomGroup.visible = false;
  applyPersistentStructureVisibility(manager);
  assert.equal(wall.visible, true);
  assert.equal(trim.visible, true);
  assert.equal(chair.parent.visible, false);

  unlocked = false;
  applyPersistentStructureVisibility(manager);
  assert.equal(wall.visible, false);
  assert.equal(trim.visible, false);
});

test("stable resolution caps materially reduce high-DPI pixel work", () => {
  assert.equal(stablePixelRatioCap("high", 3), 1.2);
  assert.equal(stablePixelRatioCap("medium", 3), 1.0);
  assert.equal(stablePixelRatioCap("low", 3), 0.88);
  assert.equal(stablePixelRatioCap("high", 1), 1);
  assert.equal(HEARTHMOUSE_STABLE_SHADOW_LIMITS.high, 512);
  assert.equal(HEARTHMOUSE_STABLE_SHADOW_LIMITS.medium, 384);
});

test("patched governor clamps an existing high-quality renderer without touching gameplay state", () => {
  let pixelRatio = 1.4;
  const fakeGovernor = {
    quality: "high",
    devicePixelRatio: 3,
    effectivePixelRatio: 1.4,
    engine: {
      renderer: {
        getPixelRatio: () => pixelRatio,
        setPixelRatio(value) { pixelRatio = value; },
      },
    },
  };
  HearthmousePerformanceGovernor.prototype.applyResolution.call(fakeGovernor, true);
  assert.equal(pixelRatio, 1.2);
  assert.equal(fakeGovernor.effectivePixelRatio, 1.2);
});
