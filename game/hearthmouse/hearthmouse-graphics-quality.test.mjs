import test from "node:test";
import assert from "node:assert/strict";

import {
  applyGraphicsQuality,
  normalizeGraphicsQuality,
  targetPixelRatioForQuality,
  targetShadowMapSizeForQuality,
} from "./hearthmouse-graphics-quality.mjs";

test("normalizes graphics quality values", () => {
  assert.equal(normalizeGraphicsQuality("LOW"), "low");
  assert.equal(normalizeGraphicsQuality("medium"), "medium");
  assert.equal(normalizeGraphicsQuality("high"), "high");
  assert.equal(normalizeGraphicsQuality("unknown"), "high");
});

test("low and medium cap render resolution without increasing a lower baseline", () => {
  assert.equal(targetPixelRatioForQuality("low", 2), 1);
  assert.equal(targetPixelRatioForQuality("medium", 2), 1.35);
  assert.equal(targetPixelRatioForQuality("high", 2), 2);
  assert.equal(targetPixelRatioForQuality("medium", 1), 1);
});

test("shadow map limits preserve smaller maps and restore high quality sizes", () => {
  assert.equal(targetShadowMapSizeForQuality("low", 2048), 256);
  assert.equal(targetShadowMapSizeForQuality("medium", 2048), 1024);
  assert.equal(targetShadowMapSizeForQuality("medium", 512), 512);
  assert.equal(targetShadowMapSizeForQuality("high", 2048), 2048);
});

test("applying quality changes renderer work and restores the captured high baseline", () => {
  const shadowMapSize = {
    x: 2048,
    y: 2048,
    set(x, y) {
      this.x = x;
      this.y = y;
    },
  };
  let disposedMaps = 0;
  const light = {
    shadow: {
      mapSize: shadowMapSize,
      map: { dispose() { disposedMaps++; } },
      needsUpdate: false,
    },
  };
  const root = {
    traverse(visitor) {
      visitor(this);
      visitor(light);
    },
  };
  const renderer = {
    ratio: 2,
    getPixelRatio() { return this.ratio; },
    setPixelRatio(value) { this.ratio = value; },
    shadowMap: { enabled: true, autoUpdate: true, needsUpdate: false },
  };
  const engine = { renderer, world: { root } };

  applyGraphicsQuality(engine, "low");
  assert.equal(renderer.ratio, 1);
  assert.equal(renderer.shadowMap.enabled, false);
  assert.equal(renderer.shadowMap.autoUpdate, false);
  assert.equal(shadowMapSize.x, 256);
  assert.equal(shadowMapSize.y, 256);
  assert.equal(disposedMaps, 1);

  applyGraphicsQuality(engine, "medium");
  assert.equal(renderer.ratio, 1.35);
  assert.equal(renderer.shadowMap.enabled, true);
  assert.equal(renderer.shadowMap.autoUpdate, true);
  assert.equal(shadowMapSize.x, 1024);
  assert.equal(shadowMapSize.y, 1024);

  applyGraphicsQuality(engine, "high");
  assert.equal(renderer.ratio, 2);
  assert.equal(renderer.shadowMap.enabled, true);
  assert.equal(renderer.shadowMap.autoUpdate, true);
  assert.equal(shadowMapSize.x, 2048);
  assert.equal(shadowMapSize.y, 2048);
});
