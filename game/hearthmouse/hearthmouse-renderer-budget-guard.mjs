const PATCH_FLAG = Symbol.for("hearthmouse.rendererBudgetGuard");

function installRendererGuard() {
  if (typeof window === "undefined") return false;
  const engine = window.hearthmouseEngine;
  const renderer = engine?.renderer;
  if (!renderer?.setPixelRatio || renderer[PATCH_FLAG]) return false;

  const originalSetPixelRatio = renderer.setPixelRatio.bind(renderer);
  renderer.setPixelRatio = function hearthmouseBudgetedSetPixelRatio(requested) {
    const governor = engine.__hearthmousePerformanceGovernor;
    if (!governor) return originalSetPixelRatio(requested);
    const effective = Number(governor.effectivePixelRatio);
    const safeRequested = Number(requested);
    if (!Number.isFinite(effective) || effective <= 0) return originalSetPixelRatio(safeRequested);
    return originalSetPixelRatio(Math.min(Number.isFinite(safeRequested) && safeRequested > 0 ? safeRequested : effective, effective));
  };
  Object.defineProperty(renderer, PATCH_FLAG, { value: true });
  return true;
}

if (typeof window !== "undefined") {
  installRendererGuard();
  window.setInterval(installRendererGuard, 250);
}

export { installRendererGuard };
