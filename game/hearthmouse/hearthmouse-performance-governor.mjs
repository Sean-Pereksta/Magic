import {
  GRAPHICS_QUALITY_PROFILES,
  normalizeGraphicsQuality,
} from "./hearthmouse-graphics-quality-core.mjs";

const GOVERNOR_KEY = "__hearthmousePerformanceGovernor";
const lightBaselines = new WeakMap();
const meshShadowBaselines = new WeakMap();
const shadowSizeBaselines = new WeakMap();

export const HEARTHMOUSE_RENDER_BUDGETS = Object.freeze({
  low: Object.freeze({
    maxPixelRatio: 1,
    minPixelRatio: 0.82,
    targetFpsDesktop: 58,
    targetFpsTouch: 48,
    shadowMapLimit: 256,
    maxShadowedLights: 0,
    maxShadowCastingActors: 0,
    actorShadowDistance: 0,
    roomLightDepth: 0,
    adjustDownStep: 0.10,
    adjustUpStep: 0.05,
  }),
  medium: Object.freeze({
    maxPixelRatio: 1.22,
    minPixelRatio: 0.92,
    targetFpsDesktop: 57,
    targetFpsTouch: 47,
    shadowMapLimit: 768,
    maxShadowedLights: 2,
    maxShadowCastingActors: 2,
    actorShadowDistance: 7.5,
    roomLightDepth: 1,
    adjustDownStep: 0.08,
    adjustUpStep: 0.035,
  }),
  high: Object.freeze({
    maxPixelRatio: 1.65,
    minPixelRatio: 1.08,
    targetFpsDesktop: 57,
    targetFpsTouch: 48,
    shadowMapLimit: 1024,
    maxShadowedLights: 3,
    maxShadowCastingActors: 5,
    actorShadowDistance: 11.5,
    roomLightDepth: 2,
    adjustDownStep: 0.07,
    adjustUpStep: 0.03,
  }),
});

const clamp = (value, minimum, maximum) => Math.max(minimum, Math.min(maximum, value));

export function renderBudgetForQuality(quality) {
  return HEARTHMOUSE_RENDER_BUDGETS[normalizeGraphicsQuality(quality)];
}

export function cappedPixelRatio(quality, devicePixelRatio = 1) {
  const budget = renderBudgetForQuality(quality);
  const baseline = Number.isFinite(devicePixelRatio) && devicePixelRatio > 0 ? devicePixelRatio : 1;
  return Math.min(baseline, budget.maxPixelRatio);
}

export function nextAdaptivePixelRatio({
  quality,
  currentPixelRatio,
  devicePixelRatio = 1,
  averageFps = 60,
  worstFrameTimeMs = 16.7,
  isTouchDevice = false,
  healthyWindows = 0,
}) {
  const budget = renderBudgetForQuality(quality);
  if (quality === "low") return {
    pixelRatio: Math.min(cappedPixelRatio(quality, devicePixelRatio), Math.max(budget.minPixelRatio, currentPixelRatio || 1)),
    healthyWindows: 0,
    changed: false,
  };

  const cap = cappedPixelRatio(quality, devicePixelRatio);
  const floor = Math.min(cap, budget.minPixelRatio);
  const current = clamp(Number(currentPixelRatio) || cap, floor, cap);
  const target = isTouchDevice ? budget.targetFpsTouch : budget.targetFpsDesktop;
  const overloaded = averageFps > 0 && (averageFps < target - 5 || worstFrameTimeMs > 34);
  const healthy = averageFps >= target + 2 && worstFrameTimeMs < 23;

  if (overloaded) {
    const next = clamp(current - budget.adjustDownStep, floor, cap);
    return { pixelRatio: next, healthyWindows: 0, changed: Math.abs(next - current) > 0.001 };
  }

  const nextHealthyWindows = healthy ? healthyWindows + 1 : 0;
  if (nextHealthyWindows >= 4 && current < cap - 0.001) {
    const next = clamp(current + budget.adjustUpStep, floor, cap);
    return { pixelRatio: next, healthyWindows: 0, changed: Math.abs(next - current) > 0.001 };
  }
  return { pixelRatio: current, healthyWindows: nextHealthyWindows, changed: false };
}

function distanceSquared(a, b) {
  if (!a || !b) return Infinity;
  const dx = (a.x ?? 0) - (b.x ?? 0);
  const dz = (a.z ?? 0) - (b.z ?? 0);
  return dx * dx + dz * dz;
}

function actorRoot(actor) {
  return actor?.rig?.root ?? null;
}

function actorPriority(engine, actor, kind) {
  const root = actorRoot(actor);
  if (!root) return -Infinity;
  const d2 = distanceSquared(root.position, engine.playerPosition);
  let score = -d2;
  if (kind === "cat") {
    if (actor.state === "chase") score += 1000;
    if (actor.pouncePhase === "windup" || actor.pouncePhase === "flight") score += 1500;
    if (actor.targetId === "player") score += 500;
  } else if (actor.task === "escaping") score += 240;
  return score;
}

function rememberMeshShadow(mesh) {
  if (!meshShadowBaselines.has(mesh)) meshShadowBaselines.set(mesh, !!mesh.castShadow);
  return meshShadowBaselines.get(mesh);
}

function setActorShadow(actor, enabled) {
  const root = actorRoot(actor);
  if (!root?.traverse) return;
  root.traverse((object) => {
    if (!object?.isMesh) return;
    const baseline = rememberMeshShadow(object);
    object.castShadow = !!enabled && baseline;
  });
}

export function applyActorShadowBudget(engine, quality) {
  const budget = renderBudgetForQuality(quality);
  const candidates = [];
  const maxDistanceSquared = budget.actorShadowDistance * budget.actorShadowDistance;

  for (const cat of engine?.cats ?? []) {
    if (!actorRoot(cat)) continue;
    setActorShadow(cat, false);
    if (distanceSquared(actorRoot(cat).position, engine.playerPosition) <= maxDistanceSquared || cat.state === "chase") {
      candidates.push({ actor: cat, score: actorPriority(engine, cat, "cat") });
    }
  }
  for (const mouse of engine?.mice ?? []) {
    if (!actorRoot(mouse)) continue;
    setActorShadow(mouse, false);
    if (distanceSquared(actorRoot(mouse).position, engine.playerPosition) <= maxDistanceSquared) {
      candidates.push({ actor: mouse, score: actorPriority(engine, mouse, "mouse") });
    }
  }

  candidates.sort((a, b) => b.score - a.score);
  const selected = candidates.slice(0, budget.maxShadowCastingActors);
  for (const item of selected) setActorShadow(item.actor, true);
  return selected.length;
}

function rememberLight(light) {
  let baseline = lightBaselines.get(light);
  if (!baseline) {
    baseline = { visible: light.visible !== false, castShadow: !!light.castShadow };
    lightBaselines.set(light, baseline);
  }
  return baseline;
}

function collectSceneObjects(engine) {
  const objects = [];
  const seen = new Set();
  const roots = [engine?.scene, engine?.world?.root, ...(engine?.world?.lights ?? []), ...(engine?.lights ?? [])].filter(Boolean);
  const visit = (object) => {
    if (!object || seen.has(object)) return;
    seen.add(object);
    objects.push(object);
  };
  for (const root of roots) {
    if (typeof root.traverse === "function") root.traverse(visit);
    else visit(root);
  }
  return objects;
}

function roomDepth(manager, roomId) {
  if (!roomId || !manager) return 0;
  if (typeof manager.roomDistance === "function") return manager.roomDistance(roomId);
  return manager.roomDistances?.get?.(roomId) ?? Infinity;
}

export function applyLightBudget(engine, quality) {
  const budget = renderBudgetForQuality(quality);
  const manager = engine?.hearthmousePerformance ?? engine?.__expansion?.performanceManager;
  const player = engine?.playerPosition;
  const shadowCandidates = [];
  let activeLights = 0;

  for (const object of collectSceneObjects(engine)) {
    const isLight = object?.isLight || object?.shadow?.mapSize;
    if (!isLight) continue;
    const baseline = rememberLight(object);
    const roomId = object.userData?.roomId ?? object.parent?.userData?.roomId ?? null;
    const depth = roomDepth(manager, roomId);
    const roomActive = !roomId || manager?.isRoomRenderActive?.(roomId) !== false;
    const depthAllowed = !roomId || depth <= budget.roomLightDepth;
    object.visible = baseline.visible && roomActive && depthAllowed;
    if (!object.visible) {
      if ("castShadow" in object) object.castShadow = false;
      continue;
    }
    activeLights++;
    if (baseline.castShadow && budget.maxShadowedLights > 0) {
      shadowCandidates.push({ light: object, distance: distanceSquared(object.position, player), roomDepth: depth });
    } else if ("castShadow" in object) object.castShadow = false;
  }

  shadowCandidates.sort((a, b) => a.roomDepth - b.roomDepth || a.distance - b.distance);
  const allowed = new Set(shadowCandidates.slice(0, budget.maxShadowedLights).map((entry) => entry.light));
  for (const entry of shadowCandidates) entry.light.castShadow = allowed.has(entry.light);
  return { activeLights, activeShadowedLights: allowed.size };
}

function rememberShadowSize(shadow) {
  let baseline = shadowSizeBaselines.get(shadow);
  if (!baseline) {
    baseline = { x: shadow.mapSize?.x ?? 1024, y: shadow.mapSize?.y ?? 1024 };
    shadowSizeBaselines.set(shadow, baseline);
  }
  return baseline;
}

export function enforceShadowMapLimit(engine, quality) {
  const limit = renderBudgetForQuality(quality).shadowMapLimit;
  let resized = 0;
  for (const object of collectSceneObjects(engine)) {
    const shadow = object?.shadow;
    if (!shadow?.mapSize) continue;
    const baseline = rememberShadowSize(shadow);
    const x = Math.min(baseline.x, limit);
    const y = Math.min(baseline.y, limit);
    if (shadow.mapSize.x === x && shadow.mapSize.y === y) continue;
    shadow.mapSize.set?.(x, y);
    if (!shadow.mapSize.set) {
      shadow.mapSize.x = x;
      shadow.mapSize.y = y;
    }
    shadow.map?.dispose?.();
    shadow.map = null;
    shadow.needsUpdate = true;
    resized++;
  }
  return resized;
}

function rendererDiagnostics(engine) {
  const info = engine?.renderer?.info;
  return {
    drawCalls: info?.render?.calls ?? 0,
    triangles: info?.render?.triangles ?? 0,
    lines: info?.render?.lines ?? 0,
    points: info?.render?.points ?? 0,
    geometries: info?.memory?.geometries ?? 0,
    textures: info?.memory?.textures ?? 0,
  };
}

export class HearthmousePerformanceGovernor {
  constructor(engine) {
    this.engine = engine;
    this.quality = normalizeGraphicsQuality(window.HearthmouseGraphicsQuality?.quality ?? "high");
    this.devicePixelRatio = Math.max(1, Number(window.devicePixelRatio) || engine?.renderer?.getPixelRatio?.() || 1);
    this.effectivePixelRatio = cappedPixelRatio(this.quality, this.devicePixelRatio);
    this.healthyWindows = 0;
    this.lastAdaptiveSample = -Infinity;
    this.lastBudgetPass = -Infinity;
    this.activeShadowedLights = 0;
    this.shadowCastingActors = 0;
    this.running = true;
    this.raf = 0;
    this.tick = this.tick.bind(this);
    this.applyResolution(true);
    this.raf = window.requestAnimationFrame(this.tick);
  }

  setQuality(value) {
    const next = normalizeGraphicsQuality(value);
    if (next === this.quality) return;
    this.quality = next;
    this.healthyWindows = 0;
    this.effectivePixelRatio = cappedPixelRatio(next, this.devicePixelRatio);
    this.applyResolution(true);
    this.applyBudgets();
  }

  applyResolution(force = false) {
    const renderer = this.engine?.renderer;
    if (!renderer?.setPixelRatio) return;
    const desired = clamp(
      this.effectivePixelRatio,
      Math.min(renderBudgetForQuality(this.quality).minPixelRatio, cappedPixelRatio(this.quality, this.devicePixelRatio)),
      cappedPixelRatio(this.quality, this.devicePixelRatio),
    );
    if (force || Math.abs((renderer.getPixelRatio?.() ?? 1) - desired) > 0.001) renderer.setPixelRatio(desired);
  }

  sampleAdaptive(now) {
    if (now - this.lastAdaptiveSample < 1100) return;
    this.lastAdaptiveSample = now;
    const manager = this.engine?.hearthmousePerformance ?? this.engine?.__expansion?.performanceManager;
    const stats = manager?.stats ?? {};
    const next = nextAdaptivePixelRatio({
      quality: this.quality,
      currentPixelRatio: this.effectivePixelRatio,
      devicePixelRatio: this.devicePixelRatio,
      averageFps: stats.averageFps || 60,
      worstFrameTimeMs: stats.worstFrameTimeMs || 16.7,
      isTouchDevice: !!manager?.isTouchDevice,
      healthyWindows: this.healthyWindows,
    });
    this.healthyWindows = next.healthyWindows;
    if (next.changed) this.effectivePixelRatio = next.pixelRatio;
  }

  applyBudgets() {
    enforceShadowMapLimit(this.engine, this.quality);
    this.shadowCastingActors = applyActorShadowBudget(this.engine, this.quality);
    const lights = applyLightBudget(this.engine, this.quality);
    this.activeShadowedLights = lights.activeShadowedLights;
  }

  snapshot() {
    const manager = this.engine?.hearthmousePerformance ?? this.engine?.__expansion?.performanceManager;
    return {
      quality: this.quality,
      effectivePixelRatio: Number((this.engine?.renderer?.getPixelRatio?.() ?? this.effectivePixelRatio).toFixed(2)),
      pixelRatioCap: cappedPixelRatio(this.quality, this.devicePixelRatio),
      shadowCastingActors: this.shadowCastingActors,
      activeShadowedLights: this.activeShadowedLights,
      activeRenderRooms: manager?.stats?.activeRenderRooms ?? 0,
      averageFps: manager?.stats?.averageFps ?? 0,
      worstFrameTimeMs: manager?.stats?.worstFrameTimeMs ?? 0,
      ...rendererDiagnostics(this.engine),
    };
  }

  tick(now) {
    if (!this.running || !this.engine || this.engine.disposed) return;
    const selected = normalizeGraphicsQuality(window.HearthmouseGraphicsQuality?.quality ?? this.quality);
    if (selected !== this.quality) this.setQuality(selected);
    this.sampleAdaptive(now);
    this.applyResolution();
    if (now - this.lastBudgetPass >= 250) {
      this.lastBudgetPass = now;
      this.applyBudgets();
    }
    this.raf = window.requestAnimationFrame(this.tick);
  }

  dispose() {
    this.running = false;
    if (this.raf) window.cancelAnimationFrame(this.raf);
  }
}

function installGovernor() {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  if (!engine || engine.disposed) return;
  if (engine[GOVERNOR_KEY] instanceof HearthmousePerformanceGovernor) return;
  engine[GOVERNOR_KEY] = new HearthmousePerformanceGovernor(engine);
  window.hearthmouseRenderPerformance = () => engine[GOVERNOR_KEY]?.snapshot?.() ?? null;
}

if (typeof window !== "undefined") {
  window.addEventListener("hearthmouse:graphics-quality", () => installGovernor());
  window.setInterval(installGovernor, 300);
  if (document?.readyState === "loading") document.addEventListener("DOMContentLoaded", installGovernor, { once: true });
  else installGovernor();
}
