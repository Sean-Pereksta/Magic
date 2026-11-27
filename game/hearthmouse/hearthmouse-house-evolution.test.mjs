import test from "node:test";
import assert from "node:assert/strict";
import { addNestHabitat } from "./hearthmouse-habitat.mjs";
import { bundledRuntime } from "./hearthmouse-runtime-fixture.mjs";

import {
  NOISE_ZONES,
  dawnProgress,
  findNoiseZone,
  personalityTuning,
} from "./hearthmouse-house-evolution.mjs";

test("dawn arrives only during the final campaign window", () => {
  assert.equal(dawnProgress(90), 0);
  assert.equal(dawnProgress(50), 0);
  assert.ok(dawnProgress(25) > 0.49 && dawnProgress(25) < 0.51);
  assert.equal(dawnProgress(0), 1);
});

test("loud floor sections are localized and leave the nest quiet", () => {
  assert.equal(findNoiseZone(5.45, -0.94)?.id, "kitchen-tray");
  assert.equal(findNoiseZone(20.5, 4)?.id, "garage-clatter");
  assert.equal(findNoiseZone(-6.78, 3.74), null);
  assert.ok(NOISE_ZONES.every((zone) => zone.strength > 1.6));
});

test("cat personalities change idle and search behavior without changing perception", () => {
  const mabel = personalityTuning("mabel");
  const biscuit = personalityTuning("biscuit");
  const pepper = personalityTuning("pepper");
  assert.ok(mabel.searchScale > biscuit.searchScale);
  assert.ok(biscuit.patrolSpeed > mabel.patrolSpeed);
  assert.ok(pepper.investigateScale > biscuit.investigateScale);
  for (const tuning of [mabel, biscuit, pepper]) {
    assert.equal("vision" in tuning, false);
    assert.equal("hearing" in tuning, false);
  }
});

test("nest food stays in four shallow trays and stockpile changes reuse their meshes", () => {
  const runtime = bundledRuntime();
  const { I } = runtime;
  const world = runtime.world(new I.Group());
  const engine = { world, snapshot: { stockpile: 96 } };
  const parent = new I.Group();
  world.root.add(parent);
  world.setStockpile(96);
  const legacy = world.root.getObjectByName("visible-winter-stockpile");
  assert.equal(legacy.children.length, 48);
  const baseColliders = [...world.colliders];

  addNestHabitat(engine, I, parent);
  const rack = parent.getObjectByName("nest-pantry-trays");
  const trays = rack.children.slice(1);
  const displayed = () => trays.filter(tray => tray.visible).reduce((sum, tray) =>
    sum + tray.geometry.drawRange.count * 12 / tray.geometry.attributes.position.count, 0);
  assert.equal(trays.length, 4);
  assert.equal(legacy.children.length, 0);
  assert.equal(displayed(), 48);
  assert.ok(trays.every(tray => !tray.castShadow && tray.geometry.boundingBox.max.y < 0.04));
  assert.deepEqual([...world.colliders], baseColliders);

  world.setStockpile(2);
  assert.equal(displayed(), 1);
  world.setStockpile(0);
  assert.equal(displayed(), 0);
  world.setStockpile(300);
  assert.equal(displayed(), 48);
  for (const [index, tray] of trays.entries()) assert.equal(rack.children[index + 1], tray);
});
