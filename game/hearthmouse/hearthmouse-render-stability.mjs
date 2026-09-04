import { normalizeGraphicsQuality } from "./hearthmouse-graphics-quality-core.mjs";
import { HearthmousePerformanceGovernor } from "./hearthmouse-performance-governor.mjs";
import { HearthmousePerformanceManager } from "./hearthmouse-performance-manager.mjs";

const MANAGER_PATCH_FLAG = Symbol.for("hearthmouse.renderStability.manager");
const GOVERNOR_PATCH_FLAG = Symbol.for("hearthmouse.renderStability.governor");
const persistentStructureState = new WeakMap();

const STRUCTURE_TOKEN = /(?:^|[^a-z])(wall|baseboard|skirting|trim|molding|moulding|floor|ceiling|architrave|crown)(?:[^a-z]|$)/i;
const MICRO_SHADOW_TOKEN = /(?:^|[^a-z])(baseboard|skirting|trim|molding|moulding|architrave|crown)(?:[^a-z]|$)/i;

export const HEARTHMOUSE_STABLE_PIXEL_RATIO_CAPS = Object.freeze({
  low: 0.88,
  medium: 1.0,
  high: 1.2,
});

export const HEARTHMOUSE_STABLE_PIXEL_RATIO_FLOORS = Object.freeze({
  low: 0.70,
  medium: 0.74,
  high: 0.80,
});

export const HEARTHMOUSE_STABLE_SHADOW_LIMITS = Object.freeze({
  low: 256,
  medium: 384,
  high: 512,
});

export const HEARTHMOUSE_STABLE_SHADOW_REFRESH_MS = Object.freeze({
  low: Infinity,
  medium: 240,
  high: 140,
});

const clamp = (value, minimum, maximum) => Math.max(minimum, Math.min(maximum, value));

export function isPersistentStructureName(name) {
  return STRUCTURE_TOKEN.test(String(name ?? ""));
}

export function stablePixelRatioCap(quality, devicePixelRatio = Infinity) {
  const normalized = normalizeGraphicsQuality(quality);
  const requested = Number(devicePixelRatio);
  const deviceCap = Number.isFinite(requested) && requested > 0 ? requested : Infinity;
  return Math.min(HEARTHMOUSE_STABLE_PIXEL_RATIO_CAPS[normalized], deviceCap);
}

function managerState(manager) {
  let state = persistentStructureState.get(manager);
  if (!state) {
    state = {
      processedGroups: new WeakSet(),
      nodesByRoom: new Map(),
      extractedNodes: 0,
      optimizedMeshes: 0,
    };
    persistentStructureState.set(manager, state);
  }
  return state;
}

function collectTopLevelStructures(root, output) {
  for (const child of [...(root?.children ?? [])]) {
    if (!child || child.userData?.__hearthmousePersistentStructure) continue;
    if (isPersistentStructureName(child.name)) {
      output.push(child);
      continue;
    }
    collectTopLevelStructures(child, output);
  }
}

function reparentPreservingTransform(node, targetParent) {
  if (!node || !targetParent || node.parent === targetParent) return false;
  try {
    node.parent?.updateWorldMatrix?.(true, true);
    targetParent.updateWorldMatrix?.(true, true);
    if (typeof targetParent.attach === "function") {
      targetParent.attach(node);
      return true;
    }
    node.parent?.remove?.(node);
    targetParent.add?.(node);
    return node.parent === targetParent;
  } catch (error) {
    console.warn("Hearthmouse could not preserve a structural render node during room extraction.", error);
    return false;
  }
}

export function optimizePersistentStructure(root) {
  if (!root) return 0;
  let optimized = 0;
  const visit = (object) => {
    if (!object) return;
    if ("matrixAutoUpdate" in object) {
      object.updateMatrix?.();
      object.matrixAutoUpdate = false;
    }
    if (!object.isMesh) return;
    object.frustumCulled = true;
    if (MICRO_SHADOW_TOKEN.test(String(object.name ?? root.name ?? ""))) object.castShadow = false;
    optimized++;
  };
  if (typeof root.traverse === "function") root.traverse(visit);
  else visit(root);
  return optimized;
}

export function extractPersistentRoomStructures(manager) {
  const registry = manager?.engine?.world?.__hearthmouseRoomGroups;
  if (!(registry instanceof Map)) return 0;
  const state = managerState(manager);
  let extracted = 0;

  for (const [roomId, groups] of registry) {
    for (const group of groups ?? []) {
      if (!group || state.processedGroups.has(group) || !group.parent) continue;
      const structures = [];
      collectTopLevelStructures(group, structures);
      let roomNodes = state.nodesByRoom.get(roomId);
      if (!roomNodes) {
        roomNodes = new Set();
        state.nodesByRoom.set(roomId, roomNodes);
      }

      for (const node of structures) {
        const baseVisible = node.visible !== false;
        if (!reparentPreservingTransform(node, group.parent)) continue;
        node.userData ??= {};
        node.userData.__hearthmousePersistentStructure = true;
        node.userData.__hearthmousePersistentBaseVisible = baseVisible;
        node.userData.roomId ??= roomId;
        state.optimizedMeshes += optimizePersistentStructure(node);
        roomNodes.add(node);
        extracted++;
      }
      state.processedGroups.add(group);
    }
  }

  state.extractedNodes += extracted;
  return extracted;
}

export function applyPersistentStructureVisibility(manager) {
  const state = persistentStructureState.get(manager);
  if (!state) return 0;
  let visible = 0;
  for (const [roomId, nodes] of state.nodesByRoom) {
    const unlocked = manager?.roomUnlocked?.(roomId) !== false;
    for (const node of [...nodes]) {
      if (!node?.parent) {
        nodes.delete(node);
        continue;
      }
      const shouldShow = unlocked && node.userData?.__hearthmousePersistentBaseVisible !== false;
      if (node.visible !== shouldShow) node.visible = shouldShow;
      if (shouldShow) visible++;
    }
  }
  return visible;
}

function enforceStableShadowMaps(governor) {
  const quality = normalizeGraphicsQuality(governor?.quality ?? "high");
  const limit = HEARTHMOUSE_STABLE_SHADOW_LIMITS[quality];
  let resized = 0;
  for (const source of governor?.sceneCache?.shadowSources ?? []) {
    const shadow = source?.shadow;
    if (!shadow?.mapSize) continue;
    const x = Math.min(Number(shadow.mapSize.x) || limit, limit);
    const y = Math.min(Number(shadow.mapSize.y) || limit, limit);
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

function installManagerPatch() {
  const prototype = HearthmousePerformanceManager?.prototype;
  if (!prototype || prototype[MANAGER_PATCH_FLAG]) return false;
  const originalApplyRoomGroupVisibility = prototype.applyRoomGroupVisibility;
  if (typeof originalApplyRoomGroupVisibility !== "function") return false;

  prototype.applyRoomGroupVisibility = function hearthmouseStableRoomVisibility() {
    extractPersistentRoomStructures(this);
    const result = Reflect.apply(originalApplyRoomGroupVisibility, this, arguments);
    applyPersistentStructureVisibility(this);
    return result;
  };

  Object.defineProperty(prototype, MANAGER_PATCH_FLAG, { value: true });
  return true;
}

function installGovernorPatch() {
  const prototype = HearthmousePerformanceGovernor?.prototype;
  if (!prototype || prototype[GOVERNOR_PATCH_FLAG]) return false;
  const originalApplyBudgets = prototype.applyBudgets;
  const originalRefreshShadowMap = prototype.refreshShadowMap;
  const originalSampleAdaptive = prototype.sampleAdaptive;
  if (typeof originalApplyBudgets !== "function" || typeof originalRefreshShadowMap !== "function") return false;

  prototype.applyResolution = function hearthmouseStableResolution(force = false) {
    const renderer = this.engine?.renderer;
    if (!renderer?.setPixelRatio) return;
    const quality = normalizeGraphicsQuality(this.quality);
    const cap = stablePixelRatioCap(quality, this.devicePixelRatio);
    const floor = Math.min(cap, HEARTHMOUSE_STABLE_PIXEL_RATIO_FLOORS[quality]);
    this.effectivePixelRatio = clamp(Number(this.effectivePixelRatio) || cap, floor, cap);
    const current = renderer.getPixelRatio?.() ?? 1;
    if (force || Math.abs(current - this.effectivePixelRatio) > 0.001) renderer.setPixelRatio(this.effectivePixelRatio);
  };

  if (typeof originalSampleAdaptive === "function") {
    prototype.sampleAdaptive = function hearthmouseStableAdaptiveResolution(now) {
      const previousSample = this.lastAdaptiveSample;
      const result = Reflect.apply(originalSampleAdaptive, this, arguments);
      if (this.lastAdaptiveSample === previousSample) return result;
      const quality = normalizeGraphicsQuality(this.quality);
      const cap = stablePixelRatioCap(quality, this.devicePixelRatio);
      const floor = Math.min(cap, HEARTHMOUSE_STABLE_PIXEL_RATIO_FLOORS[quality]);
      const manager = this.engine?.hearthmousePerformance ?? this.engine?.__expansion?.performanceManager;
      const fps = Number(manager?.stats?.averageFps) || 60;
      const worst = Number(manager?.stats?.worstFrameTimeMs) || 16.7;
      const underPressure = fps < 49 || worst > 38;
      if (underPressure) {
        const severe = fps < 38 || worst > 52;
        this.effectivePixelRatio = clamp(this.effectivePixelRatio - (severe ? 0.12 : 0.07), floor, cap);
        this.healthyWindows = 0;
      } else {
        this.effectivePixelRatio = clamp(this.effectivePixelRatio, floor, cap);
      }
      return result;
    };
  }

  prototype.refreshShadowMap = function hearthmouseStableShadowRefresh(now) {
    const quality = normalizeGraphicsQuality(this.quality);
    const minimumInterval = HEARTHMOUSE_STABLE_SHADOW_REFRESH_MS[quality];
    if (!Number.isFinite(minimumInterval)) return;
    const previous = this.__hearthmouseStableShadowRefresh ?? -Infinity;
    if (now - previous < minimumInterval) return;
    const before = this.lastShadowRefresh;
    const result = Reflect.apply(originalRefreshShadowMap, this, arguments);
    if (this.lastShadowRefresh !== before) this.__hearthmouseStableShadowRefresh = now;
    return result;
  };

  prototype.applyBudgets = function hearthmouseStableBudgets() {
    const result = Reflect.apply(originalApplyBudgets, this, arguments);
    enforceStableShadowMaps(this);
    return result;
  };

  Object.defineProperty(prototype, GOVERNOR_PATCH_FLAG, { value: true });
  return true;
}

export function installHearthmouseRenderStability() {
  return {
    managerPatched: installManagerPatch(),
    governorPatched: installGovernorPatch(),
  };
}

installHearthmouseRenderStability();
