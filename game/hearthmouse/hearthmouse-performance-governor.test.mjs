import assert from "node:assert/strict";
import test from "node:test";

import {
  HEARTHMOUSE_RENDER_BUDGETS,
  cappedPixelRatio,
  nextAdaptivePixelRatio,
  renderBudgetForQuality,
} from "./hearthmouse-performance-governor.mjs";

test("medium and high cap native DPR", () => {
  assert.equal(cappedPixelRatio("medium", 3), 1.22);
  assert.equal(cappedPixelRatio("high", 3), 1.65);
  assert.equal(cappedPixelRatio("high", 1.25), 1.25);
  assert.equal(cappedPixelRatio("low", 2), 1);
});

test("render budgets keep medium materially cheaper than high", () => {
  const medium = renderBudgetForQuality("medium");
  const high = renderBudgetForQuality("high");
  assert.ok(medium.maxPixelRatio < high.maxPixelRatio);
  assert.ok(medium.shadowMapLimit < high.shadowMapLimit);
  assert.ok(medium.maxShadowCastingActors < high.maxShadowCastingActors);
  assert.ok(medium.maxShadowedLights < high.maxShadowedLights);
});

test("adaptive DPR steps down quickly under sustained frame pressure", () => {
  const result = nextAdaptivePixelRatio({
    quality: "high",
    currentPixelRatio: 1.65,
    devicePixelRatio: 3,
    averageFps: 41,
    worstFrameTimeMs: 43,
    healthyWindows: 3,
  });
  assert.equal(result.pixelRatio, 1.58);
  assert.equal(result.healthyWindows, 0);
  assert.equal(result.changed, true);
});

test("adaptive DPR recovers slowly only after four healthy windows", () => {
  const base = {
    quality: "medium",
    currentPixelRatio: 1,
    devicePixelRatio: 2,
    averageFps: 61,
    worstFrameTimeMs: 18,
  };
  for (let healthyWindows = 0; healthyWindows < 3; healthyWindows++) {
    const result = nextAdaptivePixelRatio({ ...base, healthyWindows });
    assert.equal(result.pixelRatio, 1);
    assert.equal(result.changed, false);
  }
  const recovered = nextAdaptivePixelRatio({ ...base, healthyWindows: 3 });
  assert.equal(recovered.pixelRatio, 1.035);
  assert.equal(recovered.changed, true);
  assert.equal(recovered.healthyWindows, 0);
});

test("adaptive DPR respects quality floors", () => {
  const floor = HEARTHMOUSE_RENDER_BUDGETS.medium.minPixelRatio;
  const result = nextAdaptivePixelRatio({
    quality: "medium",
    currentPixelRatio: floor,
    devicePixelRatio: 3,
    averageFps: 20,
    worstFrameTimeMs: 70,
  });
  assert.equal(result.pixelRatio, floor);
});
