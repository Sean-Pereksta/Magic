import assert from "node:assert/strict";
import test from "node:test";

import {
  HEARTHMOUSE_RENDER_BUDGETS,
  HearthmousePerformanceGovernor,
  cappedPixelRatio,
  nextAdaptivePixelRatio,
  renderBudgetForQuality,
} from "./hearthmouse-performance-governor.mjs";
import { installRendererGuard } from "./hearthmouse-renderer-budget-guard.mjs";

test("medium and high use conservative DPR caps", () => {
  assert.equal(cappedPixelRatio("medium", 3), 1.12);
  assert.equal(cappedPixelRatio("high", 3), 1.4);
  assert.equal(cappedPixelRatio("high", 1.25), 1.25);
  assert.equal(cappedPixelRatio("low", 2), 0.95);
});

test("render budgets keep medium materially cheaper than high", () => {
  const medium = renderBudgetForQuality("medium");
  const high = renderBudgetForQuality("high");
  assert.ok(medium.maxPixelRatio < high.maxPixelRatio);
  assert.ok(medium.shadowMapLimit < high.shadowMapLimit);
  assert.ok(medium.maxShadowCastingActors < high.maxShadowCastingActors);
  assert.ok(medium.maxShadowedLights < high.maxShadowedLights);
  assert.ok(medium.shadowRefreshIntervalMs > high.shadowRefreshIntervalMs);
});

test("adaptive DPR drops harder under severe frame pressure", () => {
  const result = nextAdaptivePixelRatio({
    quality: "high",
    currentPixelRatio: 1.4,
    devicePixelRatio: 3,
    averageFps: 35,
    worstFrameTimeMs: 55,
    healthyWindows: 3,
  });
  assert.equal(Number(result.pixelRatio.toFixed(4)), 1.2515);
  assert.equal(result.healthyWindows, 0);
  assert.equal(result.changed, true);
});

test("adaptive DPR recovers only after six healthy windows", () => {
  const base = {
    quality: "medium",
    currentPixelRatio: 0.95,
    devicePixelRatio: 2,
    averageFps: 62,
    worstFrameTimeMs: 18,
  };
  for (let healthyWindows = 0; healthyWindows < 5; healthyWindows++) {
    const result = nextAdaptivePixelRatio({ ...base, healthyWindows });
    assert.equal(result.pixelRatio, 0.95);
    assert.equal(result.changed, false);
  }
  const recovered = nextAdaptivePixelRatio({ ...base, healthyWindows: 5 });
  assert.equal(recovered.pixelRatio, 0.975);
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

test("medium and high throttle shadow-map refreshes", () => {
  assert.equal(HEARTHMOUSE_RENDER_BUDGETS.medium.shadowRefreshIntervalMs, 160);
  assert.equal(HEARTHMOUSE_RENDER_BUDGETS.high.shadowRefreshIntervalMs, 90);
  assert.equal(Number.isFinite(HEARTHMOUSE_RENDER_BUDGETS.low.shadowRefreshIntervalMs), false);
});

test("the governor uses rendered-frame timing even when the simulation step is capped", () => {
  const governor = Object.assign(Object.create(HearthmousePerformanceGovernor.prototype), {
    engine: { hearthmousePerformance: { stats: { averageFps: 60 } } },
    quality: "high",
    devicePixelRatio: 2,
    effectivePixelRatio: 1.2,
    healthyWindows: 0,
    renderWindow: { frames: 0, seconds: 0, worstFrameTimeMs: 0 },
    renderTiming: { averageFps: 0, worstFrameTimeMs: 0 },
  });
  for (let frame = 0; frame < 20; frame++) governor.recordRenderedFrame(0.06);
  governor.sampleAdaptive(1200);
  assert.ok(governor.renderTiming.averageFps < 17);
  assert.equal(governor.renderTiming.worstFrameTimeMs, 60);
  assert.ok(governor.effectivePixelRatio < 1.2);

  governor.recordRenderedFrame(2);
  governor.sampleAdaptive(3200);
  assert.equal(governor.lastAdaptiveSample, 1200);
});

test("capped pixel ratio requests resize the canvas only when the effective ratio changes", () => {
  let pixelRatio = 1.2;
  const changes = [];
  const engine = {
    __hearthmousePerformanceGovernor: { effectivePixelRatio: 1.2 },
    renderer: {
      getPixelRatio: () => pixelRatio,
      setPixelRatio(value) { changes.push(value); pixelRatio = value; },
    },
  };
  assert.equal(installRendererGuard(engine), true);
  engine.renderer.setPixelRatio(2);
  engine.renderer.setPixelRatio(1.2);
  engine.__hearthmousePerformanceGovernor.effectivePixelRatio = 1;
  engine.renderer.setPixelRatio(2);
  engine.renderer.setPixelRatio(2);
  assert.deepEqual(changes, [1]);
});
