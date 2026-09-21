import { mergeStaticMeshes } from "./hearthmouse-habitat.mjs";
import { normalizeGraphicsQuality } from "./hearthmouse-graphics-quality-core.mjs";
import { HearthmousePerformanceGovernor } from "./hearthmouse-performance-governor.mjs";
import { HearthmousePerformanceManager } from "./hearthmouse-performance-manager.mjs";

const MANAGER_PATCH_FLAG = Symbol.for("hearthmouse.renderStability.manager");
const GOVERNOR_PATCH_FLAG = Symbol.for("hearthmouse.renderStability.governor");
const persistentStructureState = new WeakMap();

const STRUCTURE_TOKEN = /(?:^|[^a-z])(wall|baseboard|skirting|trim|molding|moulding|floor|ceiling|architrave|crown|partition|header)(?:[^a-z]|$)/i;
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
      processedGroups: new WeakMap(),
      nodesByRoom: new Map(),
      extractedNodes: 0,
      optimizedMeshes: 0,
      batches: [],
      batchSignature: "",
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
    // Structural shells are cheap and must not vanish with stale bounds.
    object.frustumCulled = false;
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
      if (!group || !group.parent) continue;
      if (state.processedGroups.get(group) === group.children?.length) continue;
      const structures = [];
      collectTopLevelStructures(group, structures);
      let roomNodes = state.nodesByRoom.get(roomId);
      if (!roomNodes) {
        roomNodes = new Set();
        state.nodesByRoom.set(roomId, roomNodes);
      }

      for (const node of structures) {
        const baseVisible = node.visible !== false;
        if (!reparentPreservingTransform(node, manager.engine.world.root ?? group.parent)) continue;
        node.userData ??= {};
        node.userData.__hearthmousePersistentStructure = true;
        node.userData.__hearthmousePersistentBaseVisible = baseVisible;
        node.userData.roomId ??= roomId;
        state.optimizedMeshes += optimizePersistentStructure(node);
        roomNodes.add(node);
        extracted++;
      }
      state.processedGroups.set(group, group.children?.length);
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
    // Locked rooms still need opaque shells; only decor follows unlock state.
    for (const node of [...nodes]) {
      if (!node?.parent) {
        nodes.delete(node);
        continue;
      }
      const shouldShow = node.userData?.__hearthmousePersistentBaseVisible !== false && !node.userData?.__hearthmouseStructureRemoved;
      const renderSource = shouldShow && !node.userData.__hearthmouseBatchedStructure;
      if (node.visible !== renderSource) node.visible = renderSource;
      if (shouldShow) visible++;
    }
  }
  return visible;
}

// Keep original wall meshes in the collision/LOS indexes. Their visual copies
// share static draw calls; this does not make walls depend on portal visibility.
export function batchPersistentStructures(manager, I = globalThis.window?.HearthmouseInternals) {
  const state = persistentStructureState.get(manager);
  const root = manager?.engine?.world?.root;
  if (!state || !root || !I?.Mesh || !I?.BoxGeometry) return 0;
  const candidates = [];
  let signature = "";
  for (const nodes of state.nodesByRoom.values()) for (const node of nodes) {
    if (!node.isMesh || Array.isArray(node.material) || node.parent !== root ||
      node.userData.__hearthmousePersistentBaseVisible === false || node.userData.__hearthmouseStructureRemoved) continue;
    if (!node.geometry?.attributes?.normal || !node.geometry.attributes.uv) continue;
    candidates.push(node);
    signature += `${node.id}:${node.geometry.uuid}:${node.material.uuid};`;
  }
  if (signature === state.batchSignature) return state.batches.length;
  const groups = new Map();
  for (const node of candidates) {
    const key = `${node.material.uuid}:${node.castShadow}:${node.receiveShadow}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(node);
  }
  const batches = [];
  for (const nodes of groups.values()) {
    if (nodes.length < 2) continue;
    const first = nodes[0];
    const batch = new I.Mesh(mergeStaticMeshes(I, nodes, root), first.material);
    batch.name = "persistent-structure-batch";
    batch.castShadow = first.castShadow; batch.receiveShadow = first.receiveShadow;
    batch.frustumCulled = false; batch.updateMatrix(); batch.matrixAutoUpdate = false;
    batches.push(batch);
  }
  for (const batch of state.batches) { batch.removeFromParent(); batch.geometry.dispose(); }
  for (const nodes of state.nodesByRoom.values()) for (const node of nodes) {
    node.userData.__hearthmouseBatchedStructure = false;
  }
  for (const nodes of groups.values()) if (nodes.length >= 2) for (const node of nodes) {
    node.userData.__hearthmouseBatchedStructure = true; node.visible = false;
  }
  for (const batch of batches) root.add(batch);
  state.batches = batches;
  state.batchSignature = signature;
  applyPersistentStructureVisibility(manager);
  if (manager.stats) manager.stats.structuralDraws = batches.length + [...groups.values()].filter(nodes => nodes.length === 1).length;
  return batches.length;
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
    const state = managerState(this);
    const revision = this.engine.__expansion?.routeRevision ?? 0;
    const needsAudit = !Number.isFinite(state.nextAudit) || this.clock >= state.nextAudit || state.revision !== revision;
    if (needsAudit) { extractPersistentRoomStructures(this); state.nextAudit = this.clock + 0.8; state.revision = revision; }
    const result = Reflect.apply(originalApplyRoomGroupVisibility, this, arguments);
    applyPersistentStructureVisibility(this);
    if (needsAudit) batchPersistentStructures(this);
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

