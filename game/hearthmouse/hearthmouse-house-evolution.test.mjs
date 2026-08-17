import test from "node:test";
import assert from "node:assert/strict";

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
