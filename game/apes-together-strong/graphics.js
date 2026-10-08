/* Presentation budgets only: no simulation, collision, sight or save data changes. */
(() => {
  'use strict';
  const presets = Object.freeze({
    low: Object.freeze({ maxDpr: 1, animationHz: 8, particleBudget: 80, atmosphere: 0, shadows: 1, foliage: 0, detailFloor: 3 }),
    medium: Object.freeze({ maxDpr: 1.5, animationHz: 12, particleBudget: 160, atmosphere: 1, shadows: 1, foliage: 1, detailFloor: 1 }),
    high: Object.freeze({ maxDpr: 2, animationHz: 18, particleBudget: 280, atmosphere: 2, shadows: 2, foliage: 2, detailFloor: 0 }),
    ultra: Object.freeze({ maxDpr: 2, animationHz: 24, particleBudget: 420, atmosphere: 3, shadows: 2, foliage: 3, detailFloor: 0 })
  });
  window.ATSGraphics = { presets, normalize: value => Object.hasOwn(presets, value) ? value : 'high' };
  Object.defineProperty(ATSRenderer.prototype, 'graphicsProfile', {
    get() { return presets[this.quality] || presets.high; }
  });
  ATSRenderer.prototype.desiredDpr = function () {
    return this.detailLevel >= 4 ? 1 : Math.min(devicePixelRatio || 1, this.graphicsProfile.maxDpr);
  };
})();
