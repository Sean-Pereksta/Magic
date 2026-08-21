import {
  ROOM_LAYOUT_BY_ID,
  SECRET_ROUTE_ENTRANCES,
  SECRET_ROUTES,
  TRAP_ANCHORS,
  isCirculationPlacementSafe,
  roomForPoint,
} from "./hearthmouse-circulation-layout.mjs";

export const PLAYER_TRAP_DURATION_SECONDS = 5;

export const CAT_TERRITORY_PROFILES = Object.freeze({
  mabel: Object.freeze({
    home: Object.freeze(["living", "kitchen", "pantry"]),
    variations: Object.freeze(["dining", "hallway", "laundry"]),
  }),
  biscuit: Object.freeze({
    home: Object.freeze(["hallway", "study", "bedroom"]),
    variations: Object.freeze(["children", "living", "bathroom"]),
  }),
  pepper: Object.freeze({
    home: Object.freeze(["basement-access", "basement", "garage"]),
    variations: Object.freeze(["utility", "mudroom", "pantry"]),
  }),
});

const ENTRANCE_BY_ID = new Map(SECRET_ROUTE_ENTRANCES.map((entrance) => [entrance.id, entrance]));
const MOVEMENT_KEYS = Object.freeze([
  "KeyW", "KeyA", "KeyS", "KeyD", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight",
  "ShiftLeft", "ShiftRight", "ControlLeft", "ControlRight", "KeyC", "Space",
]);
const MOVEMENT_KEY_SET = new Set(MOVEMENT_KEYS);
const PLAYER_TARGET_ID = "player";
const TUNNEL_ENTRY_RADIUS = 0.18;
const TUNNEL_EXIT_OFFSET = 0.24;
const TUNNEL_CAT_WAIT_OFFSET = 0.5;
export const EVENT_PROP_ANCHORS = Object.freeze({
  guests: Object.freeze([
    Object.freeze({ name: "guest-shoes", room: "living", x: -8.9, z: 5.45, w: 0.62, h: 0.12, d: 0.28, color: 0x4b342b }),
    Object.freeze({ name: "guest-handbag", room: "living", x: -8.65, z: 4.82, w: 0.45, h: 0.42, d: 0.22, color: 0x493047 }),
    Object.freeze({ name: "guest-coat", room: "dining", x: 6.25, z: 7.18, w: 0.26, h: 0.06, d: 0.72, color: 0x293b45 }),
  ]),
  "grocery-bag": Object.freeze([
    Object.freeze({ name: "dropped-grocery-bag", room: "pantry", x: 5.78, z: -10.62, w: 0.72, h: 0.62, d: 0.5, color: 0xb89a69 }),
  ]),
});

const clamp01 = (value) => Math.max(0, Math.min(1, value));
const planarDistance = (a, b) => Math.hypot((a?.x ?? 0) - (b?.x ?? 0), (a?.z ?? 0) - (b?.z ?? 0));

function seededUnit(seed) {
  const value = Math.sin(Number(seed) * 12.9898 + 78.233) * 43758.5453;
  return value - Math.floor(value);
}

function stringSeed(text) {
  let hash = 2166136261;
  const value = String(text ?? "");
  for (let index = 0; index < value.length; index++) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function tunnelTraversalDuration(route) {
  const authored = Number(route?.duration);
  if (Number.isFinite(authored)) return Math.max(0.65, Math.min(1.6, authored));
  const speed = Math.max(0.35, Number(route?.speed) || 1);
  return Math.max(0.65, Math.min(1.6, 0.92 / speed));
}

export function tunnelTransitProgress(startedAt, now, duration) {
  const safeDuration = Math.max(0.001, Number(duration) || 0.001);
  return clamp01((Number(now) - Number(startedAt)) / safeDuration);
}

export function tunnelTransitPoint(source, target, progress) {
  const p = clamp01(progress);
  const sourceInside = {
    x: source.x - (source.normalX ?? 0) * 0.22,
    z: source.z - (source.normalZ ?? 0) * 0.22,
  };
  const targetInside = {
    x: target.x - (target.normalX ?? 0) * 0.22,
    z: target.z - (target.normalZ ?? 0) * 0.22,
  };
  const targetOutside = {
    x: target.x + (target.normalX ?? 0) * TUNNEL_EXIT_OFFSET,
    z: target.z + (target.normalZ ?? 0) * TUNNEL_EXIT_OFFSET,
  };
  if (p <= 0.18) {
    const local = p / 0.18;
    return { x: source.x + (sourceInside.x - source.x) * local, z: source.z + (sourceInside.z - source.z) * local };
  }
  if (p < 0.82) {
    const local = (p - 0.18) / 0.64;
    const eased = local * local * (3 - 2 * local);
    return {
      x: sourceInside.x + (targetInside.x - sourceInside.x) * eased,
      z: sourceInside.z + (targetInside.z - sourceInside.z) * eased,
    };
  }
  const local = (p - 0.82) / 0.18;
  return {
    x: targetInside.x + (targetOutside.x - targetInside.x) * local,
    z: targetInside.z + (targetOutside.z - targetInside.z) * local,
  };
}

export function tunnelStalkPlan(catId, routeId, now = 0) {
  const seed = stringSeed(`${catId}:${routeId}`) + Math.floor((Number(now) || 0) * 7);
  const patience = 4.8 + seededUnit(seed + 17) * 2.7;
  return Object.freeze({
    patience,
    switchAfter: patience * (0.48 + seededUnit(seed + 31) * 0.16),
    willSwitch: seededUnit(seed + 47) < 0.3,
  });
}

export function territoryRoomsFor(catId, night) {
  const profile = CAT_TERRITORY_PROFILES[String(catId ?? "mabel").toLowerCase()] ?? CAT_TERRITORY_PROFILES.mabel;
  const currentNight = Math.max(1, Math.floor(Number(night) || 1));
  const variation = profile.variations[(currentNight + stringSeed(catId)) % profile.variations.length];
  return Object.freeze([...profile.home, variation]);
}

export function activeTrapCountForNight(night, event = null) {
  const currentNight = Math.max(1, Math.floor(Number(night) || 1));
  if (currentNight < 2) return 0;
  const base = Math.min(5, 1 + Math.floor((currentNight - 2) / 3));
  return Math.min(8, Math.ceil(base * (event?.trapMultiplier ?? 1)) + Math.max(0, Math.floor(event?.trapBonus ?? 0)));
}

export function selectTrapAnchors({ night, event = null, seed = 1, anchors = TRAP_ANCHORS } = {}) {
  const count = activeTrapCountForNight(night, event);
  const currentNight = Math.max(1, Math.floor(Number(night) || 1));
  return anchors
    .filter((anchor) => {
      const room = ROOM_LAYOUT_BY_ID.get(anchor.roomId);
      return anchor.minNight <= currentNight && (!room || room.unlockNight <= currentNight) && isCirculationPlacementSafe(anchor, { padding: 0.1 });
    })
    .map((anchor, index) => ({
      anchor,
      rank: seededUnit(seed + currentNight * 997 + index * 53 + stringSeed(event?.id)) / Math.max(
        0.1,
        (anchor.weight ?? 1) * (ROOM_LAYOUT_BY_ID.get(anchor.roomId)?.gameplay?.trapWeight ?? 1),
      ),
    }))
    .sort((a, b) => a.rank - b.rank || a.anchor.id.localeCompare(b.anchor.id))
    .slice(0, count)
    .map(({ anchor }) => anchor);
}

export function trapAvoidanceRadius(policy) {
  if (policy === "cautious") return 0.72;
  if (policy === "desperate") return 0.24;
  return 0.48;
}

export function shouldRiskKnownTrap(policy, urgency = 0, randomValue = 0.5) {
  const need = clamp01(urgency);
  if (policy === "cautious") return false;
  if (policy === "desperate") return need >= 0.35 && randomValue < 0.42 + need * 0.5;
  return need >= 0.82 && randomValue < 0.12 + need * 0.18;
}

export function playerTrapRemaining(triggeredAt, now) {
  return Math.max(0, PLAYER_TRAP_DURATION_SECONDS - Math.max(0, Number(now) - Number(triggeredAt)));
}

function pointToSegmentDistance(point, start, end) {
  const dx = end.x - start.x;
  const dz = end.z - start.z;
  const lengthSquared = dx * dx + dz * dz;
  if (lengthSquared < 1e-8) return planarDistance(point, start);
  const projection = clamp01(((point.x - start.x) * dx + (point.z - start.z) * dz) / lengthSquared);
  return Math.hypot(point.x - (start.x + dx * projection), point.z - (start.z + dz * projection));
}

export function trapDetourCandidates(start, end, trap, radius) {
  const dx = end.x - start.x;
  const dz = end.z - start.z;
  const length = Math.max(0.001, Math.hypot(dx, dz));
  const nx = -dz / length;
  const nz = dx / length;
  const offset = Math.max(0.2, Number(radius) || 0.48) + 0.16;
  return [
    { x: trap.x + nx * offset, z: trap.z + nz * offset },
    { x: trap.x - nx * offset, z: trap.z - nz * offset },
  ];
}

function unlockedRoom(roomId, night) {
  const room = ROOM_LAYOUT_BY_ID.get(roomId);
  return !room || room.unlockNight <= night;
}

function ensureLivingState(engine, I) {
  if (engine.__livingHouse) return engine.__livingHouse;
  const state = {
    I,
    root: null,
    traps: [],
    knownTrapAnchors: new Set(),
    discoveredRoutes: new Set(),
    routeCooldowns: new Map(),
    playerTunnelTransit: null,
    mouseTunnelTransits: new Map(),
    tunnelVeil: null,
    playerTrap: null,
    releaseBlockedKeys: new Set(),
    scanClock: 0,
    eventClock: 0,
    noiseClock: 0,
    lastStormMask: false,
    lastNoise: null,
    vacuum: null,
    dog: null,
    openWindow: null,
    baselineExposure: engine.renderer?.toneMappingExposure,
    baselineFogDensity: engine.scene?.fog?.density,
    catnipCatId: null,
    basePlanMousePath: null,
  };
  engine.__livingHouse = state;
  return state;
}

function disposeRoot(root) {
  if (!root) return;
  root.removeFromParent?.();
  const geometries = new Set();
  const materials = new Set();
  root.traverse?.((object) => {
    if (object.geometry) geometries.add(object.geometry);
    const list = Array.isArray(object.material) ? object.material : object.material ? [object.material] : [];
    for (const material of list) materials.add(material);
  });
  geometries.forEach((geometry) => geometry.dispose?.());
  materials.forEach((material) => material.dispose?.());
}

function restoreLighting(engine, state) {
  if (Number.isFinite(state.baselineExposure) && engine.renderer) engine.renderer.toneMappingExposure = state.baselineExposure;
  if (Number.isFinite(state.baselineFogDensity) && engine.scene?.fog) engine.scene.fog.density = state.baselineFogDensity;
}

function cleanupNight(engine, state) {
  restoreLighting(engine, state);
  disposeRoot(state.root);
  state.root = null;
  state.traps.length = 0;
  state.playerTrap = null;
  state.releaseBlockedKeys.clear();
  state.routeCooldowns.clear();
  state.playerTunnelTransit = null;
  state.mouseTunnelTransits.clear();
  if (state.tunnelVeil) state.tunnelVeil.style.opacity = "0";
  for (const cat of engine.cats ?? []) {
    cat.__tunnelStalk = null;
    cat.__secretEntranceWaitUntil = 0;
    if (cat.leisureMode === "tunnel-stalk") cat.leisureMode = null;
  }
  state.vacuum = null;
  state.dog = null;
  state.openWindow = null;
  state.lastNoise = null;
  state.eventClock = 0;
  state.noiseClock = 0;
  state.lastStormMask = false;
}

function makeNightRoot(engine, state) {
  if (state.root) return state.root;
  const root = new state.I.Group();
  root.name = "hearthmouse-living-house-night";
  engine.world?.root?.add?.(root);
  state.root = root;
  return root;
}

function addBox(engine, state, spec, parent = null) {
  const mesh = new state.I.Mesh(
    new state.I.BoxGeometry(spec.w, spec.h, spec.d),
    new state.I.MeshStandardMaterial({
      color: spec.color ?? 0x695645,
      roughness: spec.roughness ?? 0.88,
      metalness: spec.metalness ?? 0,
      emissive: spec.emissive ?? 0x000000,
      emissiveIntensity: spec.emissiveIntensity ?? 0,
    }),
  );
  mesh.name = spec.name;
  mesh.position.set(spec.x ?? 0, spec.y ?? spec.h / 2, spec.z ?? 0);
  mesh.rotation.y = spec.rotationY ?? 0;
  mesh.castShadow = false;
  mesh.receiveShadow = true;
  (parent ?? makeNightRoot(engine, state)).add(mesh);
  return mesh;
}

function createTrapVisual(engine, state, trap) {
  const { anchor } = trap;
  const group = new state.I.Group();
  group.name = `mouse-trap-${anchor.type}-${anchor.id}`;
  group.position.set(anchor.x, 0, anchor.z);
  makeNightRoot(engine, state).add(group);
  const localBox = (spec) => addBox(engine, state, { ...spec, x: spec.x ?? 0, z: spec.z ?? 0 }, group);

  if (anchor.type === "thin-wire") {
    localBox({ name: "trap-thin-white-wire", x: 0, y: 0.014, z: 0, w: 0.31, h: 0.008, d: 0.009, color: 0xe4e0d5, metalness: 0.18 });
    localBox({ name: "trap-wire-post", x: -0.145, y: 0.025, z: 0, w: 0.012, h: 0.05, d: 0.012, color: 0xb9b6ae });
  } else if (anchor.type === "cheese-box") {
    localBox({ name: "trap-cardboard-box", x: 0, y: 0.045, z: 0.045, w: 0.28, h: 0.09, d: 0.17, color: 0xa88351 });
    localBox({ name: "trap-box-shadow", x: 0, y: 0.018, z: -0.065, w: 0.24, h: 0.018, d: 0.08, color: 0x473a2b });
  } else if (anchor.type === "hidden-clutter") {
    localBox({ name: "trap-clutter-base", x: 0, y: 0.018, z: 0, w: 0.27, h: 0.025, d: 0.19, color: 0x5b4a38 });
    localBox({ name: "trap-clutter-piece-a", x: -0.11, y: 0.055, z: 0.06, w: 0.08, h: 0.09, d: 0.07, color: 0x75634d, rotationY: 0.3 });
    localBox({ name: "trap-clutter-piece-b", x: 0.12, y: 0.04, z: -0.05, w: 0.06, h: 0.065, d: 0.11, color: 0x3e4544, rotationY: -0.4 });
  } else {
    localBox({ name: "traditional-snap-trap-base", x: 0, y: 0.014, z: 0, w: 0.28, h: 0.025, d: 0.19, color: 0x9a7546 });
    trap.snapBar = localBox({ name: "traditional-snap-trap-bar", x: 0, y: 0.043, z: 0.015, w: 0.24, h: 0.014, d: 0.014, color: 0xbec0b8, metalness: 0.72 });
  }
  trap.visual = group;
}

function spawnTraps(engine, state) {
  const night = engine.snapshot?.night ?? 1;
  const event = engine.__expansion?.currentPlan?.event ?? null;
  const anchors = selectTrapAnchors({
    night,
    event,
    seed: engine.__expansion?.campaignSeed ?? 1,
  });
  let nextId = (engine.foods ?? []).reduce((highest, food) => Math.max(highest, Number(food.id) || 0), -1) + 1;
  for (const anchor of anchors) {
    const trap = {
      id: `trap:${night}:${anchor.id}`,
      anchor,
      known: state.knownTrapAnchors.has(anchor.id),
      triggered: false,
      visual: null,
      snapBar: null,
      food: null,
    };
    createTrapVisual(engine, state, trap);
    const mesh = state.I.makeFood("cheese", (engine.__expansion?.campaignSeed ?? 1) + nextId * 131);
    mesh.name = `trap-bait-${anchor.id}`;
    mesh.position.set(anchor.x, 0.018, anchor.z - 0.025);
    mesh.rotation.y = seededUnit(nextId * 97 + night * 31) * Math.PI * 2;
    mesh.scale.setScalar(0.78);
    engine.world.root.add(mesh);
    const food = {
      id: nextId++, kind: "cheese", value: 4, mesh,
      reservedBy: null, carriedBy: null, deposited: false,
      room: anchor.roomId,
      depth: ROOM_LAYOUT_BY_ID.get(anchor.roomId)?.depth ?? 0,
      nestDistance: mesh.position.distanceTo(engine.world.nestCenter),
      trapId: trap.id,
      trapAnchorId: anchor.id,
    };
    trap.food = food;
    state.traps.push(trap);
    engine.foods.push(food);
  }
}

function tunnelStyleColors(style, discovered) {
  const trim = {
    appliance: 0x777b79,
    baseboard: 0x5b4434,
    cabinet: 0x493221,
    closet: 0x4a352b,
    furniture: 0x352529,
    pipe: 0x6d7472,
    vent: 0x626866,
  }[style] ?? 0x4d392d;
  return { opening: discovered ? 0x171b19 : 0x090b0b, trim, floor: style === "vent" || style === "appliance" ? 0x515756 : 0x3d3027 };
}

function createTunnelEntranceVisual(engine, state, route, entrance, discovered) {
  const axisX = entrance.axis === "x";
  const colors = tunnelStyleColors(entrance.style, discovered);
  const frontX = entrance.x + entrance.normalX * 0.012;
  const frontZ = entrance.z + entrance.normalZ * 0.012;
  const opening = addBox(engine, state, {
    name: `mouse-hole-${entrance.id}`,
    x: frontX,
    y: 0.078,
    z: frontZ,
    w: axisX ? 0.026 : 0.28,
    h: 0.156,
    d: axisX ? 0.28 : 0.026,
    color: colors.opening,
    roughness: 1,
    emissive: discovered ? 0x070907 : 0x000000,
    emissiveIntensity: discovered ? 0.16 : 0,
  });
  opening.userData.__mouseTunnelRoute = route.id;
  opening.userData.__mouseTunnelEntrance = entrance.id;

  addBox(engine, state, {
    name: `mouse-hole-top-${entrance.id}`,
    x: frontX,
    y: 0.171,
    z: frontZ,
    w: axisX ? 0.045 : 0.34,
    h: 0.03,
    d: axisX ? 0.34 : 0.045,
    color: colors.trim,
  });
  for (const side of [-1, 1]) {
    addBox(engine, state, {
      name: `mouse-hole-side-${entrance.id}-${side}`,
      x: frontX + (axisX ? 0 : side * 0.155),
      y: 0.085,
      z: frontZ + (axisX ? side * 0.155 : 0),
      w: 0.035,
      h: 0.17,
      d: 0.035,
      color: colors.trim,
    });
  }

  const thresholdX = entrance.x + entrance.normalX * 0.12;
  const thresholdZ = entrance.z + entrance.normalZ * 0.12;
  const alongX = Math.abs(entrance.normalX) > 0;
  addBox(engine, state, {
    name: `mouse-tunnel-floor-${entrance.id}`,
    x: thresholdX,
    y: 0.008,
    z: thresholdZ,
    w: alongX ? 0.3 : 0.22,
    h: 0.016,
    d: alongX ? 0.22 : 0.3,
    color: colors.floor,
    roughness: 1,
  });
  addBox(engine, state, {
    name: `mouse-tunnel-roof-${entrance.id}`,
    x: thresholdX,
    y: 0.184,
    z: thresholdZ,
    w: alongX ? 0.3 : 0.24,
    h: 0.025,
    d: alongX ? 0.24 : 0.3,
    color: colors.trim,
    roughness: 1,
  });

  if (entrance.style === "vent" || entrance.style === "appliance") {
    for (const side of [-1, 0, 1]) {
      addBox(engine, state, {
        name: `mouse-vent-slat-${entrance.id}-${side}`,
        x: frontX + (axisX ? 0.018 * side : 0),
        y: 0.055 + (side + 1) * 0.035,
        z: frontZ + (axisX ? 0 : 0.018 * side),
        w: axisX ? 0.018 : 0.22,
        h: 0.009,
        d: axisX ? 0.22 : 0.018,
        color: 0x8a8e89,
        metalness: 0.45,
      });
    }
  }
}

function ensureTunnelVeil(state) {
  if (typeof document === "undefined") return null;
  if (state.tunnelVeil?.isConnected) return state.tunnelVeil;
  const existing = document.querySelector(".hearthmouse-tunnel-veil");
  if (existing) {
    state.tunnelVeil = existing;
    return existing;
  }
  const veil = document.createElement("div");
  veil.className = "hearthmouse-tunnel-veil";
  veil.setAttribute("aria-hidden", "true");
  document.querySelector(".game-shell")?.appendChild(veil);
  state.tunnelVeil = veil;
  return veil;
}

function createSecretRouteVisuals(engine, state) {
  const night = engine.snapshot?.night ?? 1;
  const rendered = new Set();
  for (const route of SECRET_ROUTES) {
    if (route.discoveryNight > night) continue;
    const discovered = state.discoveredRoutes.has(route.id);
    for (const entranceId of [route.a, route.b]) {
      if (rendered.has(entranceId)) continue;
      const entrance = ENTRANCE_BY_ID.get(entranceId);
      if (!entrance || !unlockedRoom(entrance.roomId, night)) continue;
      rendered.add(entranceId);
      createTunnelEntranceVisual(engine, state, route, entrance, discovered);
    }
  }
  ensureTunnelVeil(state);
}

function addEventProps(engine, state, event) {
  const props = EVENT_PROP_ANCHORS[event?.eventProps] ?? [];
  for (const spec of props) {
    if (!isCirculationPlacementSafe(spec, { padding: 0.1 })) continue;
    addBox(engine, state, { ...spec, y: spec.h / 2 });
  }
}

function createVacuum(engine, state) {
  const root = new state.I.Group();
  root.name = "roaming-vacuum";
  makeNightRoot(engine, state).add(root);
  addBox(engine, state, { name: "vacuum-head", x: 0, y: 0.12, z: 0, w: 0.7, h: 0.24, d: 0.45, color: 0x42494d, metalness: 0.3 }, root);
  addBox(engine, state, { name: "vacuum-body", x: 0, y: 0.42, z: 0.1, w: 0.38, h: 0.62, d: 0.3, color: 0x6c2426, metalness: 0.18 }, root);
  root.position.set(-7.5, 0, -2.8);
  state.vacuum = { root, pathIndex: 0, points: [[-7.5, -2.8], [-7.8, 4.7], [-3.1, 5.4], [2.1, 5.15], [8.6, 2.1], [8.7, -4.9], [2.7, -5.35], [-4.4, -5.25]] };
}

function createDog(engine, state) {
  const root = new state.I.Group();
  root.name = "visiting-dog";
  makeNightRoot(engine, state).add(root);
  addBox(engine, state, { name: "dog-body", x: 0, y: 0.34, z: 0, w: 0.62, h: 0.54, d: 1.02, color: 0x755437 }, root);
  addBox(engine, state, { name: "dog-head", x: 0, y: 0.58, z: -0.63, w: 0.48, h: 0.46, d: 0.48, color: 0x806042 }, root);
  addBox(engine, state, { name: "dog-muzzle", x: 0, y: 0.49, z: -0.92, w: 0.31, h: 0.22, d: 0.24, color: 0x9b7957 }, root);
  for (const x of [-0.22, 0.22]) for (const z of [-0.34, 0.34]) {
    addBox(engine, state, { name: "dog-leg", x, y: 0.15, z, w: 0.14, h: 0.3, d: 0.15, color: 0x65472f }, root);
  }
  root.position.set(-8, 0, -4.8);
  state.dog = { root, patrolIndex: 0, points: [[-8, -4.8], [-8.3, 4.7], [-3, 5.3], [2.4, 5.2], [8.4, 3.1], [8.2, -4.9], [2.7, -5.3]] };
}

function createOpenWindow(engine, state) {
  const spec = { name: "open-window-gap", room: "mudroom", x: 5.95, z: 13.98, w: 0.62, h: 0.72, d: 0.06, color: 0x111820 };
  if (!isCirculationPlacementSafe(spec, { padding: 0.08 })) return;
  const mesh = addBox(engine, state, { ...spec, y: 0.58, emissive: 0x102030, emissiveIntensity: 0.25 });
  state.openWindow = { mesh, position: new state.I.Vector3(spec.x, 0, spec.z) };
}

function applyLighting(engine, state, event) {
  restoreLighting(engine, state);
  if (event?.lighting === "outage") {
    if (engine.renderer) engine.renderer.toneMappingExposure = (state.baselineExposure ?? 0.94) * 0.56;
    if (engine.scene?.fog && Number.isFinite(state.baselineFogDensity)) engine.scene.fog.density = state.baselineFogDensity * 1.24;
  } else if (event?.lighting === "storm") {
    if (engine.renderer) engine.renderer.toneMappingExposure = (state.baselineExposure ?? 0.94) * 0.76;
    if (engine.scene?.fog && Number.isFinite(state.baselineFogDensity)) engine.scene.fog.density = state.baselineFogDensity * 1.1;
  } else if (event?.lighting === "bright") {
    if (engine.renderer) engine.renderer.toneMappingExposure = (state.baselineExposure ?? 0.94) * 1.16;
    const panels = [
      { name: "hall-light-left-on", x: -2.2, z: -8.9 },
      { name: "dining-light-left-on", x: 3.6, z: 9.1 },
    ];
    for (const panel of panels) addBox(engine, state, { ...panel, y: 2.82, w: 0.52, h: 0.025, d: 0.32, color: 0xffefbd, emissive: 0xffd878, emissiveIntensity: 1.4 });
  }
}

function buildNightFeatures(engine, state) {
  disposeRoot(state.root);
  state.root = null;
  state.traps.length = 0;
  state.vacuum = null;
  state.dog = null;
  state.openWindow = null;
  const event = engine.__expansion?.currentPlan?.event ?? null;
  makeNightRoot(engine, state);
  applyLighting(engine, state, event);
  createSecretRouteVisuals(engine, state);
  addEventProps(engine, state, event);
  if (event?.vacuum) createVacuum(engine, state);
  if (event?.dog) createDog(engine, state);
  if (event?.openWindow) createOpenWindow(engine, state);
  spawnTraps(engine, state);
}

function chooseSeparatedIndex(engine, candidateIndices, occupied, seed) {
  if (!candidateIndices.length) return -1;
  let best = candidateIndices[0];
  let bestScore = -Infinity;
  for (const index of candidateIndices) {
    const point = engine.world?.patrolPoints?.[index];
    if (!point) continue;
    let separation = 10;
    for (const other of occupied) separation = Math.min(separation, planarDistance(point, other));
    const score = separation * 2 + seededUnit(seed + index * 41) * 0.8;
    if (score > bestScore) {
      best = index;
      bestScore = score;
    }
  }
  return best;
}

function patrolIndicesForRooms(engine, rooms) {
  const expansion = engine.__expansion;
  const output = [];
  for (const roomId of rooms) {
    for (const index of expansion?.patrolByRoom?.get?.(roomId) ?? []) if (!output.includes(index)) output.push(index);
  }
  return output;
}

function assignCatTerritories(engine, state) {
  const night = engine.snapshot?.night ?? 1;
  const occupied = [];
  const event = engine.__expansion?.currentPlan?.event;
  state.catnipCatId = event?.catnip && engine.cats?.length
    ? engine.cats[(night + stringSeed(event.id)) % engine.cats.length].id
    : null;
  for (const cat of engine.cats ?? []) {
    const rooms = territoryRoomsFor(cat.id, night).filter((roomId) => unlockedRoom(roomId, night));
    cat.__territoryRooms = rooms;
    cat.__territoryPatrolIndices = patrolIndicesForRooms(engine, rooms);
    cat.__territoryTarget = null;
    cat.__catnipAffected = cat.id === state.catnipCatId;
    const index = chooseSeparatedIndex(engine, cat.__territoryPatrolIndices, occupied, night * 101 + stringSeed(cat.id));
    if (index >= 0) {
      cat.patrolIndex = index;
      cat.__territoryTarget = index;
      occupied.push(engine.world.patrolPoints[index]);
    } else if (cat.rig?.root?.position) occupied.push(cat.rig.root.position);
  }
}

function territoryPatrol(engine, state, cat) {
  if (!cat || (cat.path?.length && cat.pathIndex < cat.path.length)) return false;
  const all = engine.world?.patrolPoints ?? [];
  if (!all.length) return false;
  const event = engine.__expansion?.currentPlan?.event;
  const occupied = (engine.cats ?? [])
    .filter((other) => other !== cat)
    .map((other) => Number.isInteger(other.__territoryTarget) ? all[other.__territoryTarget] : other.rig?.root?.position)
    .filter(Boolean);
  const focus = event?.catFocus ? patrolIndicesForRooms(engine, [event.catFocus]) : [];
  const roaming = cat.__catnipAffected || Math.random() < 0.14;
  const candidates = focus.length && Math.random() < 0.62
    ? focus
    : roaming
      ? all.map((_, index) => index)
      : cat.__territoryPatrolIndices ?? [];
  const index = chooseSeparatedIndex(engine, candidates, occupied, (engine.snapshot?.night ?? 1) * 307 + stringSeed(cat.id) + Math.floor((engine.time ?? 0) / 5));
  const point = all[index];
  if (!point) return false;
  engine.planCatPath?.(cat, point);
  if (!cat.path?.length || cat.pathReachable === false) return false;
  cat.patrolIndex = index;
  cat.__territoryTarget = index;
  return true;
}

function findTrap(state, food) {
  if (!food?.trapId) return null;
  return state.traps.find((trap) => trap.id === food.trapId) ?? null;
}

function playTrapSnap(engine) {
  engine.audio?.noise?.(0.09, 0.82, 5200);
  engine.audio?.tone?.(180, 0.09, 0.46, "square", 42);
  if (typeof window !== "undefined") window.setTimeout(() => engine.audio?.tone?.(72, 0.13, 0.26, "triangle", 35), 24);
}

function triggerTrap(engine, state, trap, mouse = null) {
  if (!trap || trap.triggered) return false;
  trap.triggered = true;
  trap.known = true;
  state.knownTrapAnchors.add(trap.anchor.id);
  if (trap.food) {
    trap.food.reservedBy = null;
    trap.food.carriedBy = null;
    trap.food.deposited = true;
    trap.food.mesh.visible = false;
  }
  if (trap.snapBar) trap.snapBar.rotation.z = Math.PI * 0.42;
  playTrapSnap(engine);
  const position = mouse?.rig?.root?.position ?? engine.playerPosition;
  engine.emitNoise?.(position, 2.5);

  if (mouse) {
    if (mouse.targetFood === trap.food) mouse.targetFood = null;
    mouse.path = [];
    mouse.pathIndex = 0;
    mouse.task = "hiding";
    mouse.hideTimer = 1.8;
    mouse.escapeCooldown = 1.2;
    if (planarDistance(mouse.rig.root.position, engine.playerPosition) < 2.2) engine.showMessage?.(`${mouse.member?.name ?? "A scout"} found a trap.`, 1.6);
  } else {
    const startedAt = engine.time ?? 0;
    state.playerTrap = {
      startedAt,
      until: startedAt + PLAYER_TRAP_DURATION_SECONDS,
      anchor: engine.playerPosition.clone(),
    };
    state.releaseBlockedKeys.clear();
    engine.playerVelocity?.set?.(0, 0, 0);
    engine.verticalVelocity = 0;
    engine.jumpQueued = false;
    engine.showMessage?.("SNAP — trapped!", 1.15);
  }
  return true;
}

function urgencyForEngine(engine) {
  const remaining = Math.max(0, (engine.snapshot?.tonightRequirement ?? 0) - (engine.snapshot?.deliveredTonight ?? 0));
  const availableSafe = (engine.foods ?? []).reduce((total, food) => {
    if (food.trapId || food.deposited || food.carriedBy || !food.mesh?.visible) return total;
    return total + (food.value ?? 1);
  }, 0);
  if (!remaining) return 0;
  return clamp01((remaining - availableSafe * 0.72) / Math.max(1, remaining));
}

function knownTrapFoodShouldBeHidden(engine, state, food) {
  const trap = findTrap(state, food);
  if (!trap || !trap.known || trap.triggered) return false;
  return !shouldRiskKnownTrap(engine.colonyPolicy, urgencyForEngine(engine), Math.random());
}

function applyKnownTrapDetours(engine, state, I, mouse) {
  if (!mouse?.path?.length) return;
  const radius = trapAvoidanceRadius(engine.colonyPolicy);
  const known = state.traps.filter((trap) => trap.known && !trap.triggered);
  if (!known.length) return;
  const output = [];
  let start = mouse.rig.root.position;
  for (const end of mouse.path) {
    let detoured = false;
    for (const trap of known) {
      if (pointToSegmentDistance(trap.anchor, start, end) >= radius) continue;
      const candidates = trapDetourCandidates(start, end, trap.anchor, radius);
      for (const candidate of candidates) {
        const waypoint = new I.Vector3(candidate.x, start.y, candidate.z);
        if (!I.lineClear(start, waypoint, 0.055, engine.world.colliders, "mouse")) continue;
        if (!I.lineClear(waypoint, end, 0.055, engine.world.colliders, "mouse")) continue;
        output.push(waypoint);
        start = waypoint;
        detoured = true;
        break;
      }
      if (detoured) break;
    }
    output.push(end);
    start = end;
  }
  mouse.path = output;
}

function mouseMovementTarget(engine, mouse) {
  if (mouse.task === "escaping" && mouse.escapeGoal) return mouse.escapeGoal;
  if (["returning", "home", "nesting-move"].includes(mouse.task)) return mouse.nestActivityGoal ?? engine.world.nestDeposit;
  return mouse.targetFood?.mesh?.position ?? engine.world.nestCenter;
}

function planKnownSecretRoute(engine, state, I, mouse, basePlan) {
  if ((mouse.__secretPlanCooldownUntil ?? 0) > (engine.time ?? 0)) return false;
  const target = mouseMovementTarget(engine, mouse);
  const start = mouse.rig?.root?.position;
  if (!target || !start) return false;
  const startRoom = roomForPoint(start.x, start.z);
  const targetRoom = roomForPoint(target.x, target.z);
  for (const route of SECRET_ROUTES) {
    if (!state.discoveredRoutes.has(route.id) || route.discoveryNight > (engine.snapshot?.night ?? 1)) continue;
    const a = ENTRANCE_BY_ID.get(route.a);
    const b = ENTRANCE_BY_ID.get(route.b);
    if (!a || !b) continue;
    let source = null;
    let exit = null;
    if (a.roomId === startRoom) { source = a; exit = b; }
    else if (b.roomId === startRoom && !route.oneWay) { source = b; exit = a; }
    if (!source || !exit) continue;
    const sourceDistance = Math.hypot(source.x - target.x, source.z - target.z);
    const exitDistance = Math.hypot(exit.x - target.x, exit.z - target.z);
    if (exit.roomId !== targetRoom && exitDistance + 0.55 >= sourceDistance) continue;
    const entrance = new I.Vector3(source.x, start.y, source.z);
    const result = I.pathfind(start, entrance, 0.055, engine.world.colliders, "mouse");
    if (!result?.reachedGoal || !result.path?.length) continue;
    mouse.path = result.path;
    mouse.pathIndex = 0;
    mouse.__secretRoutePlan = { routeId: route.id, sourceId: source.id };
    state.basePlanMousePath = basePlan;
    return true;
  }
  return false;
}

function routePair(route, sourceId) {
  if (route.a === sourceId) return [ENTRANCE_BY_ID.get(route.a), ENTRANCE_BY_ID.get(route.b)];
  if (!route.oneWay && route.b === sourceId) return [ENTRANCE_BY_ID.get(route.b), ENTRANCE_BY_ID.get(route.a)];
  return [null, null];
}

function tunnelWaitPosition(I, entrance, offset = TUNNEL_CAT_WAIT_OFFSET) {
  return new I.Vector3(
    entrance.x + (entrance.normalX ?? 0) * offset,
    0,
    entrance.z + (entrance.normalZ ?? 0) * offset,
  );
}

function tunnelExitPosition(I, entrance) {
  return new I.Vector3(entrance.x, 0.025, entrance.z);
}

function catSawTunnelEntry(engine, cat, actorTargetId, sourcePosition) {
  if (!cat?.rig?.root?.position) return false;
  if (cat.targetId === actorTargetId) return true;
  if (cat.state === "chase" || planarDistance(cat.rig.root.position, sourcePosition) > 6.2) return false;
  return (engine.targetVisibility?.(cat, actorTargetId, sourcePosition) ?? 0) >= 0.22;
}

function assignTunnelExitStalk(engine, state, route, source, target, actorTargetId, sourcePosition) {
  let witness = null;
  let bestScore = -Infinity;
  for (const cat of engine.cats ?? []) {
    if (!catSawTunnelEntry(engine, cat, actorTargetId, sourcePosition)) continue;
    const distance = planarDistance(cat.rig.root.position, sourcePosition);
    const score = (cat.targetId === actorTargetId ? 20 : 0) - distance;
    if (score > bestScore) {
      witness = cat;
      bestScore = score;
    }
  }
  if (!witness) return null;

  const now = engine.time ?? 0;
  const authored = tunnelStalkPlan(witness.id, route.id, now);
  const exitPosition = tunnelExitPosition(state.I, target);
  const alternateExitPosition = tunnelExitPosition(state.I, source);
  const waitPosition = tunnelWaitPosition(state.I, target);
  const alternateWaitPosition = tunnelWaitPosition(state.I, source);
  witness.targetId = null;
  witness.awareness = Math.max(witness.awareness ?? 0, 0.32);
  witness.investigation?.copy?.(exitPosition);
  witness.lastSeen?.copy?.(exitPosition);
  engine.setCatState?.(witness, "search", authored.patience);
  witness.leisureMode = null;
  witness.leisureTimer = 0;
  witness.__secretEntranceWaitUntil = now + authored.patience;
  witness.__tunnelStalk = {
    routeId: route.id,
    sourceId: source.id,
    targetId: target.id,
    exitPosition,
    alternateExitPosition,
    waitPosition,
    alternateWaitPosition,
    until: now + authored.patience,
    switchAt: now + authored.switchAfter,
    willSwitch: authored.willSwitch,
    switched: false,
  };
  engine.planCatPath?.(witness, waitPosition);
  return witness;
}

function actorInTunnel(state, targetId) {
  if (targetId === PLAYER_TARGET_ID) return !!state.playerTunnelTransit;
  return state.mouseTunnelTransits.has(`mouse:${targetId}`);
}

function beginTunnelTransit(engine, state, route, source, target, position, actorId, mouse = null) {
  const now = engine.time ?? 0;
  if ((state.routeCooldowns.get(actorId) ?? 0) > now) return false;
  if (actorId === PLAYER_TARGET_ID ? state.playerTunnelTransit : state.mouseTunnelTransits.has(actorId)) return false;

  const wasKnown = state.discoveredRoutes.has(route.id);
  const duration = tunnelTraversalDuration(route);
  const actorTargetId = mouse?.member?.id ?? PLAYER_TARGET_ID;
  const sourcePosition = tunnelExitPosition(state.I, source);
  assignTunnelExitStalk(engine, state, route, source, target, actorTargetId, sourcePosition);
  const transit = {
    actorId,
    mouse,
    position,
    route,
    source,
    target,
    startedAt: now,
    duration,
    progress: 0,
    previousTask: mouse?.task ?? null,
  };

  state.discoveredRoutes.add(route.id);
  state.routeCooldowns.set(actorId, now + duration + 0.85);
  engine.emitNoise?.(sourcePosition, route.noise);
  if (!wasKnown) engine.showMessage?.(`The colony discovered the ${route.label ?? route.id.replaceAll("-", " ")}.`, 2.8);

  if (mouse) {
    mouse.path = [];
    mouse.pathIndex = 0;
    mouse.__secretRoutePlan = null;
    mouse.__tunnelTransit = transit;
    mouse.task = "tunneling";
    mouse.escapeCooldown = Math.max(mouse.escapeCooldown ?? 0, duration + 0.35);
    state.mouseTunnelTransits.set(actorId, transit);
  } else {
    engine.playerVelocity?.set?.(0, 0, 0);
    state.playerTunnelTransit = transit;
  }
  return true;
}

function finishTunnelTransit(engine, state, transit) {
  const now = engine.time ?? 0;
  const exit = tunnelTransitPoint(transit.source, transit.target, 1);
  transit.position.set(exit.x, transit.position.y, exit.z);
  if (transit.mouse) {
    const mouse = transit.mouse;
    state.mouseTunnelTransits.delete(transit.actorId);
    mouse.__tunnelTransit = null;
    mouse.__secretRoutePlan = null;
    mouse.__secretPlanCooldownUntil = now + 0.72;
    mouse.task = transit.previousTask === "tunneling" ? (mouse.resumeTask ?? "waiting") : (transit.previousTask ?? "waiting");
    mouse.path = [];
    mouse.pathIndex = 0;
    if (mouse.member?.alive && ["escaping", "to-food", "returning", "home", "nesting-move"].includes(mouse.task)) {
      engine.planMousePath?.(mouse);
    }
  } else {
    state.playerTunnelTransit = null;
    engine.playerEyeY = 0.066;
    engine.playerVelocity?.set?.(0, 0, 0);
    if (state.tunnelVeil) state.tunnelVeil.style.opacity = "0";
  }
}

function advanceTunnelTransit(engine, state, transit) {
  if (!transit?.position || transit.mouse && (!transit.mouse.member?.alive || !transit.mouse.rig?.root?.position)) {
    if (transit?.actorId === PLAYER_TARGET_ID) state.playerTunnelTransit = null;
    else if (transit?.actorId) state.mouseTunnelTransits.delete(transit.actorId);
    return 1;
  }
  const progress = tunnelTransitProgress(transit.startedAt, engine.time ?? 0, transit.duration);
  const point = tunnelTransitPoint(transit.source, transit.target, progress);
  const previousX = transit.position.x;
  const previousZ = transit.position.z;
  transit.position.set(point.x, transit.position.y, point.z);
  transit.progress = progress;
  if (transit.mouse?.rig?.root) {
    const dx = point.x - previousX;
    const dz = point.z - previousZ;
    if (dx * dx + dz * dz > 1e-7) transit.mouse.rig.root.rotation.y = Math.atan2(-dx, -dz);
  }
  if (progress >= 1) finishTunnelTransit(engine, state, transit);
  return progress;
}

function updateSecretRoutes(engine, state) {
  if (engine.snapshot?.phase !== "foraging") return;
  const night = engine.snapshot?.night ?? 1;
  for (const transit of state.mouseTunnelTransits.values()) advanceTunnelTransit(engine, state, transit);

  for (const route of SECRET_ROUTES) {
    if (route.discoveryNight > night) continue;
    for (const sourceId of [route.a, route.oneWay ? null : route.b]) {
      if (!sourceId) continue;
      const [source, target] = routePair(route, sourceId);
      if (!source || !target || !unlockedRoom(source.roomId, night) || !unlockedRoom(target.roomId, night)) continue;
      const playerSpeed = engine.playerVelocity?.length?.() ?? 0;
      if (!state.playerTunnelTransit && playerSpeed > 0.035 && planarDistance(engine.playerPosition, source) < TUNNEL_ENTRY_RADIUS) {
        beginTunnelTransit(engine, state, route, source, target, engine.playerPosition, PLAYER_TARGET_ID);
      }
      for (const mouse of engine.mice ?? []) {
        if (!mouse?.member?.id || !mouse?.member?.alive || !mouse?.rig?.root?.position || mouse.task === "dead" || mouse.task === "tunneling") continue;
        const actorId = `mouse:${mouse.member.id}`;
        const plannedEntry = mouse.__secretRoutePlan?.routeId === route.id && mouse.__secretRoutePlan?.sourceId === source.id;
        if ((plannedEntry || mouse.task !== "waiting") && planarDistance(mouse.rig.root.position, source) < 0.13) {
          beginTunnelTransit(engine, state, route, source, target, mouse.rig.root.position, actorId, mouse);
        }
      }
    }
  }
}

function scanMiceForTraps(engine, state, I, delta) {
  state.scanClock -= delta;
  if (state.scanClock > 0) return;
  state.scanClock = 0.22;
  for (const trap of state.traps) {
    if (trap.triggered || trap.known) continue;
    for (const mouse of engine.mice ?? []) {
      if (!mouse.member?.alive || planarDistance(mouse.rig?.root?.position, trap.anchor) > 1.15) continue;
      const target = new I.Vector3(trap.anchor.x, mouse.rig.root.position.y, trap.anchor.z);
      if (!I.lineClear(mouse.rig.root.position, target, 0.055, engine.world.colliders, "mouse")) continue;
      trap.known = true;
      state.knownTrapAnchors.add(trap.anchor.id);
      break;
    }
  }
}

function moveToward(root, targetX, targetZ, delta, speed) {
  const dx = targetX - root.position.x;
  const dz = targetZ - root.position.z;
  const distance = Math.hypot(dx, dz);
  if (distance < 0.001) return true;
  const step = Math.min(distance, speed * delta);
  root.position.x += dx / distance * step;
  root.position.z += dz / distance * step;
  root.rotation.y = Math.atan2(-dx, -dz);
  return distance <= speed * delta + 0.02;
}

function updateVacuum(engine, state, delta) {
  const vacuum = state.vacuum;
  if (!vacuum) return;
  const point = vacuum.points[vacuum.pathIndex % vacuum.points.length];
  if (moveToward(vacuum.root, point[0], point[1], delta, 1.5)) vacuum.pathIndex = (vacuum.pathIndex + 1 + (Math.random() < 0.22 ? 1 : 0)) % vacuum.points.length;
  state.noiseClock -= delta;
  if (state.noiseClock <= 0) {
    state.noiseClock = 0.64;
    engine.emitNoise?.(vacuum.root.position, 1.7);
    for (const mouse of engine.mice ?? []) {
      if (!mouse.member?.alive || planarDistance(mouse.rig?.root?.position, vacuum.root.position) > 1.05) continue;
      mouse.path = [];
      mouse.pathIndex = 0;
      if (!mouse.carriedFood && mouse.task !== "escaping" && mouse.task !== "hiding") mouse.task = "waiting";
      mouse.delay = Math.max(mouse.delay ?? 0, 0.22);
    }
  }
}

function updateDog(engine, state, delta) {
  const dog = state.dog;
  if (!dog) return;
  const recentNoise = state.lastNoise && (engine.time ?? 0) - state.lastNoise.time < 5.2 && state.lastNoise.strength >= 0.55;
  const point = recentNoise
    ? [state.lastNoise.position.x, state.lastNoise.position.z]
    : dog.points[dog.patrolIndex % dog.points.length];
  if (moveToward(dog.root, point[0], point[1], delta, recentNoise ? 1.38 : 0.82) && !recentNoise) dog.patrolIndex = (dog.patrolIndex + 1) % dog.points.length;
  if (Math.floor((engine.time ?? 0) * 2.2) !== dog.lastStep) {
    dog.lastStep = Math.floor((engine.time ?? 0) * 2.2);
    if (planarDistance(dog.root.position, engine.playerPosition) < 5.5) engine.audio?.catPaw?.(planarDistance(dog.root.position, engine.playerPosition), true);
  }
  if (engine.snapshot?.phase === "foraging" && planarDistance(dog.root.position, engine.playerPosition) < 0.29) engine.playerCaught?.({ id: "visiting-dog" });
}

function updateOpenWindow(engine, state, delta) {
  if (!state.openWindow) return;
  state.openWindow.nextNoise = (state.openWindow.nextNoise ?? 0.3) - delta;
  if (state.openWindow.nextNoise > 0) return;
  state.openWindow.nextNoise = 2.2 + Math.random() * 2.6;
  engine.emitNoise?.(state.openWindow.position, 1.18);
}

function updateLivingHouse(engine, state, I, delta) {
  const event = engine.__expansion?.currentPlan?.event;
  const stormMask = event?.maskCycle === "storm" && !!engine.__expansion?.maskActive;
  if (stormMask && !state.lastStormMask) {
    const thunder = new I.Vector3((seededUnit((engine.time ?? 0) * 17) - 0.5) * 18, 0, -16.5);
    engine.emitNoise?.(thunder, 2.2);
  }
  state.lastStormMask = stormMask;
  updateSecretRoutes(engine, state);
  scanMiceForTraps(engine, state, I, delta);
  updateVacuum(engine, state, delta);
  updateDog(engine, state, delta);
  updateOpenWindow(engine, state, delta);
}

function suppressMovementButKeepLook(engine, baseUpdatePlayer, delta, keysToSuppress) {
  const removed = [];
  for (const code of keysToSuppress) {
    if (engine.keys?.has?.(code)) {
      engine.keys.delete(code);
      removed.push(code);
    }
  }
  const moveX = engine.touch?.moveX;
  const moveY = engine.touch?.moveY;
  if (engine.touch) {
    engine.touch.moveX = 0;
    engine.touch.moveY = 0;
  }
  engine.jumpQueued = false;
  baseUpdatePlayer.call(engine, delta);
  for (const code of removed) engine.keys.add(code);
  if (engine.touch) {
    engine.touch.moveX = moveX;
    engine.touch.moveY = moveY;
  }
}

export function installLivingHouse(I = globalThis.window?.HearthmouseInternals) {
  const proto = I?.Engine?.prototype;
  if (!proto?.__colonyExpansionInstalled || !proto?.__roomSafetyGuardInstalled) return false;
  if (proto.__livingHouseInstalled) return true;
  const required = ["beginNight", "restartCampaign", "createCats", "spawnFood", "updateGame", "updatePlayer", "updateFood", "assignFood", "planMousePath", "playerTakeFood", "mouseTakeFood", "updateCatPatrol", "updateCatSearchMotion", "targetPosition", "targetVisibility", "processCatVision", "emitNoise"];
  if (required.some((name) => typeof proto[name] !== "function")) return false;
  Object.defineProperty(proto, "__livingHouseInstalled", { value: true });

  const base = Object.fromEntries([...required, "chooseNightEvent", "restartNight"].map((name) => [name, proto[name]]));

  proto.beginNight = function livingBeginNight(night) {
    const state = ensureLivingState(this, I);
    cleanupNight(this, state);
    return base.beginNight.call(this, night);
  };

  if (typeof base.chooseNightEvent === "function") {
    proto.chooseNightEvent = function livingNightEvent(night) {
      const result = base.chooseNightEvent.call(this, night);
      const state = ensureLivingState(this, I);
      applyLighting(this, state, this.__expansion?.currentPlan?.event);
      return result;
    };
  }

  proto.restartCampaign = function livingRestartCampaign() {
    const state = ensureLivingState(this, I);
    cleanupNight(this, state);
    state.knownTrapAnchors.clear();
    state.discoveredRoutes.clear();
    return base.restartCampaign.call(this);
  };

  if (typeof base.restartNight === "function") {
    proto.restartNight = function livingRestartNight() {
      const result = base.restartNight.call(this);
      const state = ensureLivingState(this, I);
      state.playerTrap = null;
      state.playerTunnelTransit = null;
      state.mouseTunnelTransits.clear();
      state.routeCooldowns.clear();
      if (state.tunnelVeil) state.tunnelVeil.style.opacity = "0";
      state.releaseBlockedKeys.clear();
      for (const trap of state.traps) {
        trap.triggered = false;
        trap.known = state.knownTrapAnchors.has(trap.anchor.id);
        if (trap.food) {
          trap.food.deposited = false;
          trap.food.carriedBy = null;
          trap.food.reservedBy = null;
          trap.food.mesh.visible = true;
        }
        if (trap.snapBar) trap.snapBar.rotation.z = 0;
      }
      assignCatTerritories(this, state);
      return result;
    };
  }

  proto.createCats = function livingCatRoster() {
    const result = base.createCats.call(this);
    assignCatTerritories(this, ensureLivingState(this, I));
    return result;
  };

  proto.spawnFood = function livingFoodAndTraps() {
    const result = base.spawnFood.call(this);
    buildNightFeatures(this, ensureLivingState(this, I));
    return result;
  };

  proto.updateGame = function livingHouseUpdate(delta) {
    const result = base.updateGame.call(this, delta);
    updateLivingHouse(this, ensureLivingState(this, I), I, delta);
    return result;
  };

  proto.updatePlayer = function exactFiveSecondTrap(delta) {
    const state = ensureLivingState(this, I);
    const transit = state.playerTunnelTransit;
    if (transit) {
      this.playerVelocity?.set?.(0, 0, 0);
      this.verticalVelocity = 0;
      this.onGround = true;
      suppressMovementButKeepLook(this, base.updatePlayer, delta, MOVEMENT_KEYS);
      const progress = advanceTunnelTransit(this, state, transit);
      this.playerVelocity?.set?.(0, 0, 0);
      this.verticalVelocity = 0;
      this.onGround = true;
      this.playerEyeY = 0.066 - Math.sin(progress * Math.PI) * 0.02;
      const veil = ensureTunnelVeil(state);
      if (veil) veil.style.opacity = String(Math.pow(Math.sin(progress * Math.PI), 0.62) * 0.96);
      this.updateCamera?.(0.38, false);
      return;
    }
    const trap = state.playerTrap;
    const now = this.time ?? 0;
    if (trap && now < trap.until) {
      suppressMovementButKeepLook(this, base.updatePlayer, delta, MOVEMENT_KEYS);
      this.playerPosition.copy(trap.anchor);
      this.playerVelocity.set(0, 0, 0);
      this.verticalVelocity = 0;
      this.onGround = true;
      this.jumpQueued = false;
      if (this.camera?.position) {
        const struggle = Math.sin(now * 24) * 0.0022;
        this.camera.position.x += struggle;
        this.camera.position.z -= struggle * 0.55;
        this.camera.rotation.z += Math.sin(now * 31) * 0.0016;
      }
      return;
    }
    if (trap) {
      for (const code of this.keys ?? []) if (MOVEMENT_KEY_SET.has(code)) state.releaseBlockedKeys.add(code);
      if (this.touch) {
        this.touch.moveX = 0;
        this.touch.moveY = 0;
      }
      this.playerVelocity.set(0, 0, 0);
      this.verticalVelocity = 0;
      this.jumpQueued = false;
      state.playerTrap = null;
      this.showMessage?.("Free.", 0.72);
    }
    for (const code of [...state.releaseBlockedKeys]) if (!this.keys?.has?.(code)) state.releaseBlockedKeys.delete(code);
    if (state.releaseBlockedKeys.size) return suppressMovementButKeepLook(this, base.updatePlayer, delta, state.releaseBlockedKeys);
    return base.updatePlayer.call(this, delta);
  };

  proto.playerTakeFood = function livingPlayerPickup(food) {
    const state = ensureLivingState(this, I);
    if (state.playerTunnelTransit) return;
    const trap = findTrap(state, food);
    if (trap) return triggerTrap(this, state, trap);
    return base.playerTakeFood.call(this, food);
  };

  proto.updateFood = function tunnelSafeFoodUpdate(delta) {
    if (ensureLivingState(this, I).playerTunnelTransit) return;
    return base.updateFood.call(this, delta);
  };

  proto.mouseTakeFood = function livingMousePickup(mouse, food) {
    const state = ensureLivingState(this, I);
    const trap = findTrap(state, food);
    if (trap) return triggerTrap(this, state, trap, mouse);
    return base.mouseTakeFood.call(this, mouse, food);
  };

  proto.assignFood = function trapAwareFoodAssignment(mouse) {
    const state = ensureLivingState(this, I);
    const hidden = [];
    for (const food of this.foods ?? []) {
      if (!knownTrapFoodShouldBeHidden(this, state, food) || !food.mesh.visible) continue;
      food.mesh.visible = false;
      hidden.push(food);
    }
    try {
      return base.assignFood.call(this, mouse);
    } finally {
      for (const food of hidden) if (!food.deposited && !food.carriedBy) food.mesh.visible = true;
    }
  };

  proto.planMousePath = function learnedMouseRoutes(mouse) {
    const state = ensureLivingState(this, I);
    state.basePlanMousePath = base.planMousePath;
    if (planKnownSecretRoute(this, state, I, mouse, base.planMousePath)) {
      applyKnownTrapDetours(this, state, I, mouse);
      return;
    }
    const result = base.planMousePath.call(this, mouse);
    applyKnownTrapDetours(this, state, I, mouse);
    return result;
  };

  proto.updateCatPatrol = function territorialCatPatrol(cat, delta, speedOverride) {
    const state = ensureLivingState(this, I);
    if (cat.__tunnelStalk && cat.state !== "search") {
      cat.__tunnelStalk = null;
      cat.__secretEntranceWaitUntil = 0;
      if (cat.leisureMode === "tunnel-stalk") cat.leisureMode = null;
    }
    territoryPatrol(this, state, cat);
    return base.updateCatPatrol.call(this, cat, delta, cat.__catnipAffected ? (speedOverride ?? 0.78) * 1.12 : speedOverride);
  };

  proto.updateCatSearchMotion = function secretEntranceSearch(cat, delta, center, speed) {
    const now = this.time ?? 0;
    const stalk = cat.__tunnelStalk;
    if (stalk && cat.state !== "chase" && stalk.until > now) {
      if (stalk.willSwitch && stalk.arrivedAt && !stalk.switched && now >= stalk.switchAt) {
        stalk.switched = true;
        stalk.arrivedAt = 0;
        stalk.waitPosition = stalk.alternateWaitPosition;
        stalk.exitPosition = stalk.alternateExitPosition;
        cat.investigation?.copy?.(stalk.exitPosition);
        cat.lastSeen?.copy?.(stalk.exitPosition);
        cat.path = [];
        cat.pathIndex = 0;
        this.planCatPath?.(cat, stalk.waitPosition);
      }

      const distance = planarDistance(cat.rig?.root?.position, stalk.waitPosition);
      if (distance <= 0.38) {
        if (!stalk.arrivedAt) stalk.arrivedAt = now;
        cat.path = [];
        cat.pathIndex = 0;
        cat.leisureMode = "tunnel-stalk";
        cat.leisureTimer = Math.max(0, stalk.until - now);
        if (cat.rig?.root && stalk.exitPosition) {
          cat.yaw = this.yawToward?.(cat.rig.root.position, stalk.exitPosition) ?? cat.yaw;
          cat.rig.root.rotation.y = cat.yaw;
        }
        return 0;
      }
      if (!cat.path?.length || cat.pathIndex >= cat.path.length) this.planCatPath?.(cat, stalk.waitPosition);
      return this.followCatPath?.(cat, delta, Math.min(speed ?? 0.62, 0.68)) ?? 0;
    }
    cat.__tunnelStalk = null;
    cat.__secretEntranceWaitUntil = 0;
    if (cat.leisureMode === "tunnel-stalk") cat.leisureMode = null;
    return base.updateCatSearchMotion.call(this, cat, delta, center, speed);
  };

  proto.targetVisibility = function tunnelOccludedTargetVisibility(cat, targetId, targetPosition) {
    const state = ensureLivingState(this, I);
    if (actorInTunnel(state, targetId)) return 0;
    return base.targetVisibility.call(this, cat, targetId, targetPosition);
  };

  proto.targetPosition = function tunnelHiddenTargetPosition(targetId) {
    const state = ensureLivingState(this, I);
    if (actorInTunnel(state, targetId)) return null;
    return base.targetPosition.call(this, targetId);
  };

  proto.processCatVision = function roomLightAwareVision(cat, target, interval) {
    const state = ensureLivingState(this, I);
    const visibleTarget = target?.id && actorInTunnel(state, target.id) ? null : target;
    const event = this.__expansion?.currentPlan?.event;
    const targetPosition = visibleTarget?.position;
    const targetRoom = targetPosition ? roomForPoint(targetPosition.x, targetPosition.z) : null;
    const roomLight = ROOM_LAYOUT_BY_ID.get(targetRoom)?.gameplay?.light ?? 0.62;
    const identityMultiplier = 0.72 + roomLight * 0.48;
    const eventMultiplier = event?.lighting === "outage" ? 0.7 : event?.brightRooms?.includes?.(targetRoom) ? 1.38 : 1;
    const multiplier = identityMultiplier * eventMultiplier;
    const result = base.processCatVision.call(this, cat, visibleTarget, interval * multiplier);
    if (cat.state === "chase" && cat.__tunnelStalk) {
      cat.__tunnelStalk = null;
      cat.__secretEntranceWaitUntil = 0;
      if (cat.leisureMode === "tunnel-stalk") cat.leisureMode = null;
    }
    return result;
  };

  proto.emitNoise = function livingNoiseMemory(position, strength) {
    const state = ensureLivingState(this, I);
    const roomId = position ? roomForPoint(position.x, position.z) : null;
    const roomNoise = ROOM_LAYOUT_BY_ID.get(roomId)?.gameplay?.noise ?? 0.5;
    const adjustedStrength = Number.isFinite(strength) ? strength * (0.82 + roomNoise * 0.36) : strength;
    if (position && Number.isFinite(adjustedStrength)) {
      if (!state.lastNoise) state.lastNoise = { position: new I.Vector3(), strength: 0, time: 0 };
      state.lastNoise.position.copy(position);
      state.lastNoise.strength = adjustedStrength;
      state.lastNoise.time = this.time ?? 0;
    }
    return base.emitNoise.call(this, position, adjustedStrength);
  };

  const engine = globalThis.window?.hearthmouseEngine;
  if (engine && !engine.disposed) {
    const state = ensureLivingState(engine, I);
    assignCatTerritories(engine, state);
    if (!state.root) buildNightFeatures(engine, state);
  }
  return true;
}

function installWhenReady() {
  if (typeof window === "undefined") return;
  if (!installLivingHouse(window.HearthmouseInternals)) window.setTimeout(installWhenReady, 16);
}

if (typeof window !== "undefined") installWhenReady();
