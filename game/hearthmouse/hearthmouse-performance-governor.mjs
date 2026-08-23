import { normalizeGraphicsQuality } from "./hearthmouse-graphics-quality-core.mjs";

const GOVERNOR_KEY = "__hearthmousePerformanceGovernor";
const lightBaselines = new WeakMap();
const meshShadowBaselines = new WeakMap();
const shadowSizeBaselines = new WeakMap();
const actorShadowState = new WeakMap();
const sceneCaches = new WeakMap();

export const HEARTHMOUSE_RENDER_BUDGETS = Object.freeze({
  low: Object.freeze({
    maxPixelRatio: 0.95,
    minPixelRatio: 0.72,
    targetFpsDesktop: 58,
    targetFpsTouch: 48,
    shadowMapLimit: 256,
    maxShadowedLights: 0,
    maxShadowCastingActors: 0,
    actorShadowDistance: 0,
    roomLightDepth: 0,
    shadowRefreshIntervalMs: Infinity,
    budgetIntervalMs: 1000,
    adjustDownStep: 0.10,
    adjustUpStep: 0.03,
  }),
  medium: Object.freeze({
    maxPixelRatio: 1.12,
    minPixelRatio: 0.78,
    targetFpsDesktop: 57,
    targetFpsTouch: 46,
    shadowMapLimit: 512,
    maxShadowedLights: 1,
    maxShadowCastingActors: 1,
    actorShadowDistance: 6.5,
    roomLightDepth: 1,
    shadowRefreshIntervalMs: 160,
    budgetIntervalMs: 700,
    adjustDownStep: 0.10,
    adjustUpStep: 0.025,
  }),
  high: Object.freeze({
    maxPixelRatio: 1.40,
    minPixelRatio: 0.90,
    targetFpsDesktop: 57,
    targetFpsTouch: 47,
    shadowMapLimit: 768,
    maxShadowedLights: 2,
    maxShadowCastingActors: 3,
    actorShadowDistance: 9.5,
    roomLightDepth: 1,
    shadowRefreshIntervalMs: 90,
    budgetIntervalMs: 550,
    adjustDownStep: 0.09,
    adjustUpStep: 0.025,
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
  const cap = cappedPixelRatio(quality, devicePixelRatio);
  const floor = Math.min(cap, budget.minPixelRatio);
  const current = clamp(Number(currentPixelRatio) || cap, floor, cap);
  if (quality === "low") return { pixelRatio: current, healthyWindows: 0, changed: false };

  const target = isTouchDevice ? budget.targetFpsTouch : budget.targetFpsDesktop;
  const overloaded = averageFps > 0 && (averageFps < target - 4 || worstFrameTimeMs > 30);
  const severelyOverloaded = averageFps > 0 && (averageFps < target - 14 || worstFrameTimeMs > 48);
  const healthy = averageFps >= target + 3 && worstFrameTimeMs < 21;

  if (overloaded) {
    const step = budget.adjustDownStep * (severelyOverloaded ? 1.65 : 1);
    const next = clamp(current - step, floor, cap);
    return { pixelRatio: next, healthyWindows: 0, changed: Math.abs(next - current) > 0.001 };
  }

  const nextHealthyWindows = healthy ? healthyWindows + 1 : 0;
  if (nextHealthyWindows >= 6 && current < cap - 0.001) {
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
  if (actorShadowState.get(actor) === enabled) return false;
  const root = actorRoot(actor);
  if (!root?.traverse) return false;
  root.traverse((object) => {
    if (!object?.isMesh) return;
    const baseline = rememberMeshShadow(object);
    object.castShadow = !!enabled && baseline;
  });
  actorShadowState.set(actor, enabled);
  return true;
}

export function applyActorShadowBudget(engine, quality) {
  const budget = renderBudgetForQuality(quality);
  const candidates = [];
  const maxDistanceSquared = budget.actorShadowDistance * budget.actorShadowDistance;

  for (const cat of engine?.cats ?? []) {
    const root = actorRoot(cat);
    if (!root) continue;
    if (distanceSquared(root.position, engine.playerPosition) <= maxDistanceSquared || cat.state === "chase") {
      candidates.push({ actor: cat, score: actorPriority(engine, cat, "cat") });
    }
  }
  for (const mouse of engine?.mice ?? []) {
    const root = actorRoot(mouse);
    if (!root) continue;
    if (distanceSquared(root.position, engine.playerPosition) <= maxDistanceSquared) {
      candidates.push({ actor: mouse, score: actorPriority(engine, mouse, "mouse") });
    }
  }

  candidates.sort((a, b) => b.score - a.score);
  const selected = new Set(candidates.slice(0, budget.maxShadowCastingActors).map((entry) => entry.actor));
  for (const cat of engine?.cats ?? []) setActorShadow(cat, selected.has(cat));
  for (const mouse of engine?.mice ?? []) setActorShadow(mouse, selected.has(mouse));
  return selected.size;
}

function rememberLight(light) {
  let baseline = lightBaselines.get(light);
  if (!baseline) {
    baseline = { visible: light.visible !== false, castShadow: !!light.castShadow };
    lightBaselines.set(light, baseline);
  }
  return baseline;
}

function buildSceneCache(engine) {
  const objects = [];
  const lights = [];
  const shadowSources = [];
  const seen = new Set();
  const roots = [engine?.scene, engine?.world?.root, ...(engine?.world?.lights ?? []), ...(engine?.lights ?? [])].filter(Boolean);
  const visit = (object) => {
    if (!object || seen.has(object)) return;
    seen.add(object);
    objects.push(object);
    if (object.isLight || object.shadow?.mapSize) lights.push(object);
    if (object.shadow?.mapSize) shadowSources.push(object);
  };
  for (const root of roots) {
    if (typeof root.traverse === "function") root.traverse(visit);
    else visit(root);
  }
  const cache = {
    objects,
    lights,
    shadowSources,
    routeRevision: engine?.__expansion?.routeRevision ?? 0,
    builtAt: typeof performance !== "undefined" ? performance.now() : Date.now(),
  };
  sceneCaches.set(engine, cache);
  return cache;
}

function getSceneCache(engine, force = false) {
  let cache = sceneCaches.get(engine);
  const routeRevision = engine?.__expansion?.routeRevision ?? 0;
  if (!cache || force || cache.routeRevision !== routeRevision) cache = buildSceneCache(engine);
  return cache;
}

function roomDepth(manager, roomId) {
  if (!roomId || !manager) return 0;
  if (typeof manager.roomDistance === "function") return manager.roomDistance(roomId);
  return manager.roomDistances?.get?.(roomId) ?? Infinity;
}

export function applyLightBudget(engine, quality, cache = getSceneCache(engine)) {
  const budget = renderBudgetForQuality(quality);
  const manager = engine?.hearthmousePerformance ?? engine?.__expansion?.performanceManager;
  const player = engine?.playerPosition;
  const shadowCandidates = [];
  let activeLights = 0;

  for (const object of cache.lights) {
    const baseline = rememberLight(object);
    const roomId = object.userData?.roomId ?? object.parent?.userData?.roomId ?? null;
    const depth = roomDepth(manager, roomId);
    const roomActive = !roomId || manager?.isRoomRenderActive?.(roomId) !== false;
    const depthAllowed = !roomId || depth <= budget.roomLightDepth;
    const shouldShow = baseline.visible && roomActive && depthAllowed;
    if (object.visible !== shouldShow) object.visible = shouldShow;
    if (!shouldShow) {
      if ("castShadow" in object && object.castShadow) object.castShadow = false;
      continue;
    }
    activeLights++;
    if (baseline.castShadow && budget.maxShadowedLights > 0) {
      shadowCandidates.push({ light: object, distance: distanceSquared(object.position, player), roomDepth: depth });
    } else if ("castShadow" in object && object.castShadow) object.castShadow = false;
  }

  shadowCandidates.sort((a, b) => a.roomDepth - b.roomDepth || a.distance - b.distance);
  const allowed = new Set(shadowCandidates.slice(0, budget.maxShadowedLights).map((entry) => entry.light));
  for (const entry of shadowCandidates) {
    const desired = allowed.has(entry.light);
    if (entry.light.castShadow !== desired) entry.light.castShadow = desired;
  }
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

export function enforceShadowMapLimit(engine, quality, cache = getSceneCache(engine)) {
  const limit = renderBudgetForQuality(quality).shadowMapLimit;
  let resized = 0;
  for (const object of cache.shadowSources) {
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
    this.lastShadowRefresh = -Infinity;
    this.lastRouteRevision = -1;
    this.activeShadowedLights = 0;
    this.shadowCastingActors = 0;
    this.running = true;
    this.raf = 0;
    this.tick = this.tick.bind(this);
    this.sceneCache = getSceneCache(engine, true);
    this.configureShadowRenderer();
    this.applyResolution(true);
    this.applyBudgets(true);
    this.raf = window.requestAnimationFrame(this.tick);
  }

  setQuality(value) {
    const next = normalizeGraphicsQuality(value);
    if (next === this.quality) return;
    this.quality = next;
    this.healthyWindows = 0;
    this.effectivePixelRatio = cappedPixelRatio(next, this.devicePixelRatio);
    this.configureShadowRenderer();
    this.applyResolution(true);
    this.applyBudgets(true);
  }

  configureShadowRenderer() {
    const shadowMap = this.engine?.renderer?.shadowMap;
    if (!shadowMap) return;
    if (this.quality === "low") {
      shadowMap.enabled = false;
      shadowMap.autoUpdate = false;
      shadowMap.needsUpdate = false;
      return;
    }
    shadowMap.enabled = true;
    shadowMap.autoUpdate = false;
    shadowMap.needsUpdate = true;
  }

  refreshShadowMap(now) {
    const shadowMap = this.engine?.renderer?.shadowMap;
    if (!shadowMap?.enabled) return;
    const interval = renderBudgetForQuality(this.quality).shadowRefreshIntervalMs;
    if (!Number.isFinite(interval) || now - this.lastShadowRefresh < interval) return;
    this.lastShadowRefresh = now;
    shadowMap.needsUpdate = true;
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
    if (now - this.lastAdaptiveSample < 1000) return;
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

  applyBudgets(forceSceneRefresh = false) {
    const routeRevision = this.engine?.__expansion?.routeRevision ?? 0;
    const sceneChanged = forceSceneRefresh || routeRevision !== this.lastRouteRevision;
    if (sceneChanged) {
      this.sceneCache = getSceneCache(this.engine, true);
      this.lastRouteRevision = routeRevision;
      enforceShadowMapLimit(this.engine, this.quality, this.sceneCache);
    }
    this.shadowCastingActors = applyActorShadowBudget(this.engine, this.quality);
    const lights = applyLightBudget(this.engine, this.quality, this.sceneCache);
    this.activeShadowedLights = lights.activeShadowedLights;
  }

  snapshot() {
    const manager = this.engine?.hearthmousePerformance ?? this.engine?.__expansion?.performanceManager;
    return {
      quality: this.quality,
      effectivePixelRatio: Number((this.engine?.renderer?.getPixelRatio?.() ?? this.effectivePixelRatio).toFixed(2)),
      pixelRatioCap: cappedPixelRatio(this.quality, this.devicePixelRatio),
      shadowRefreshHz: Number.isFinite(renderBudgetForQuality(this.quality).shadowRefreshIntervalMs)
        ? Math.round(1000 / renderBudgetForQuality(this.quality).shadowRefreshIntervalMs)
        : 0,
      cachedSceneObjects: this.sceneCache?.objects?.length ?? 0,
      cachedLights: this.sceneCache?.lights?.length ?? 0,
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
    this.refreshShadowMap(now);
    const interval = renderBudgetForQuality(this.quality).budgetIntervalMs;
    if (now - this.lastBudgetPass >= interval) {
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
  window.addEventListener("hearthmouse:graphics-quality", installGovernor);
  window.setInterval(installGovernor, 1000);
  if (document?.readyState === "loading") document.addEventListener("DOMContentLoaded", installGovernor, { once: true });
  else installGovernor();
}
