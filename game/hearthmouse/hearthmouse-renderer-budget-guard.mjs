const PATCH_FLAG = Symbol.for("hearthmouse.rendererBudgetGuard");

function installRendererGuard(engine = globalThis.window?.hearthmouseEngine) {
  const renderer = engine?.renderer;
  if (!renderer?.setPixelRatio || renderer[PATCH_FLAG]) return false;

  const originalSetPixelRatio = renderer.setPixelRatio.bind(renderer);
  renderer.setPixelRatio = function hearthmouseBudgetedSetPixelRatio(requested) {
    const governor = engine.__hearthmousePerformanceGovernor;
    const effective = governor?.effectivePixelRatio;
    const ratio = Number.isFinite(effective) && effective > 0
      ? Math.min(Number.isFinite(requested) && requested > 0 ? requested : effective, effective)
      : requested;
    // Three.js reallocates its drawing buffer on every setPixelRatio call.
    if (renderer.getPixelRatio?.() === ratio) return;
    return originalSetPixelRatio(ratio);
  };
  Object.defineProperty(renderer, PATCH_FLAG, { value: true });
  return true;
}

if (typeof window !== "undefined") {
  installRendererGuard();
  window.setInterval(installRendererGuard, 250);
}

export { installRendererGuard };
