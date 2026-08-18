export const CHILDREN_DOORWAY = Object.freeze({ id: "children-door", x: -7.7, z: -10.63, radius: 1.18 });

export const BACKROOM_DOORWAYS = Object.freeze([
  CHILDREN_DOORWAY,
  Object.freeze({ id: "hallway-door", x: -1.3, z: -6.33, radius: 1.0 }),
  Object.freeze({ id: "pantry-hall-door", x: 2.05, z: -8.9, radius: 0.95 }),
  Object.freeze({ id: "pantry-kitchen-door", x: 4.5, z: -6.33, radius: 1.0 }),
  Object.freeze({ id: "dining-door", x: 3.6, z: 6.33, radius: 1.0 }),
  Object.freeze({ id: "bathroom-door", x: -3, z: -11.75, radius: 0.92 }),
  Object.freeze({ id: "bedroom-door", x: 0.45, z: -11.75, radius: 0.92 }),
  Object.freeze({ id: "utility-door", x: 15.05, z: 4.05, radius: 0.95 }),
  Object.freeze({ id: "mudroom-door", x: 3.6, z: 11.55, radius: 0.9 }),
  Object.freeze({ id: "basement-access-door", x: 7.15, z: -8.9, radius: 0.95 }),
  Object.freeze({ id: "basement-door", x: 9.8, z: -11.75, radius: 1.0 }),
  Object.freeze({ id: "garage-door", x: 18.75, z: 4, radius: 0.95 }),
]);

const D = (kind, name, room, x, z, w, d, h, color, extra = {}) => Object.freeze({
  kind, name, room, x, z, w, d, h, color, ...extra,
});

export const BACKROOM_DECORATIONS = Object.freeze([
  // Hallway / study wing: wall-biased pieces keep the central running lane open.
  D("shelf", "hall-wall-shelf", "hallway", -4.1, -8.8, 0.52, 1.55, 1.58, 0x564235),
  D("chair", "hall-reading-chair", "hallway", -3.85, -10.55, 0.62, 0.62, 0.92, 0x6d5748),
  D("rug", "hall-runner", "hallway", -1.1, -9.1, 2.55, 0.74, 0.018, 0x704c42),
  D("shelf", "study-bookcase", "study", -9.82, -8.05, 0.46, 2.15, 1.82, 0x4f3728),
  D("chair", "study-side-chair", "study", -8.25, -7.45, 0.66, 0.66, 0.94, 0x665044),
  D("stack", "study-storage-stack", "study", -9.25, -9.75, 0.72, 0.6, 0.58, 0x786047),

  // Pantry / dining: more household density without touching the kitchen proper.
  D("shelf", "pantry-back-rack", "pantry", 3.0, -10.95, 1.25, 0.46, 1.72, 0x5a422d),
  D("stack", "pantry-dry-goods", "pantry", 5.25, -10.95, 0.82, 0.55, 0.68, 0x8b704e),
  D("chair", "dining-chair-west", "dining", 1.35, 9.1, 0.6, 0.6, 0.9, 0x53392d),
  D("chair", "dining-chair-east", "dining", 4.85, 10.55, 0.6, 0.6, 0.9, 0x53392d),
  D("rug", "dining-rug", "dining", 3.2, 9.0, 3.0, 1.55, 0.018, 0x6f4c43),

  // Bathroom / bedroom.
  D("shelf", "bath-linen-shelf", "bathroom", -4.32, -13.55, 0.42, 1.1, 1.52, 0x718486),
  D("basket", "bath-hamper", "bathroom", -1.82, -15.25, 0.55, 0.55, 0.62, 0x9a8464),
  D("box", "bedroom-nightstand", "bedroom", -0.78, -14.35, 0.56, 0.56, 0.62, 0x554035, { collide: true }),
  D("shelf", "bedroom-wardrobe", "bedroom", -0.82, -15.35, 0.58, 1.0, 1.72, 0x513b37),

  // Children's room: the entry stays deliberately open. Storage is moved to the side/back walls.
  D("basket", "children-side-storage-bin", "children", -9.45, -11.65, 0.72, 0.58, 0.48, 0x8c6945),
  D("shelf", "children-bookcase", "children", -9.72, -14.55, 0.48, 1.48, 1.35, 0x76533b),
  D("chair", "children-small-chair", "children", -6.15, -14.45, 0.52, 0.52, 0.72, 0x7a5b48),
  D("rug", "children-play-rug", "children", -7.62, -13.15, 2.25, 1.45, 0.018, 0x8d6650),
  D("stack", "children-toy-blocks", "children", -6.25, -12.55, 0.6, 0.55, 0.42, 0xa37a4d),

  // Laundry / utility / mudroom.
  D("basket", "laundry-hamper", "laundry", 11.25, 5.55, 0.7, 0.62, 0.72, 0x88745e),
  D("shelf", "laundry-supply-rack", "laundry", 13.9, 5.72, 1.45, 0.46, 1.58, 0x5c5148),
  D("chair", "laundry-stool", "laundry", 12.1, 2.35, 0.5, 0.5, 0.68, 0x615144),
  D("shelf", "utility-north-rack", "utility", 16.25, 2.25, 1.52, 0.44, 1.76, 0x50585a),
  D("shelf", "utility-south-rack", "utility", 17.55, 5.75, 1.3, 0.44, 1.7, 0x51595b),
  D("stack", "utility-paint-cans", "utility", 16.1, 5.45, 0.62, 0.52, 0.48, 0x72787a),
  D("shelf", "mudroom-boot-rack", "mudroom", 1.25, 13.65, 1.18, 0.42, 0.82, 0x55463a),
  D("basket", "mudroom-basket", "mudroom", 4.8, 13.7, 0.64, 0.54, 0.52, 0x856f54),

  // Basement access gets storage along the walls, not the stair/door line.
  D("shelf", "basement-access-east-rack", "basement-access", 11.82, -7.55, 0.42, 1.35, 1.72, 0x403831),
  D("shelf", "basement-access-supply-shelf", "basement-access", 9.6, -6.82, 1.45, 0.42, 1.48, 0x4c4034),
  D("stack", "basement-access-boxes", "basement-access", 8.15, -10.7, 0.72, 0.62, 0.62, 0x69533b),
  D("chair", "basement-access-folding-chair", "basement-access", 10.9, -10.45, 0.55, 0.55, 0.82, 0x4b4741),

  // Basement: denser storage/work area while preserving the center route from the stairs.
  D("shelf", "basement-west-storage", "basement", 7.65, -13.45, 0.46, 1.85, 1.82, 0x45433e),
  D("shelf", "basement-east-storage", "basement", 11.95, -15.55, 0.46, 1.65, 1.82, 0x45433e),
  D("shelf", "basement-back-storage", "basement", 9.65, -16.35, 1.6, 0.42, 1.7, 0x4e4840),
  D("chair", "basement-work-chair", "basement", 10.85, -13.9, 0.58, 0.58, 0.88, 0x4f4b45),
  D("stack", "basement-storage-totes", "basement", 8.45, -16.05, 0.86, 0.62, 0.72, 0x5f6260),

  // Garage: practical clutter concentrated around outer walls.
  D("shelf", "garage-tool-rack", "garage", 23.9, 2.15, 0.46, 2.15, 1.88, 0x474b4d),
  D("shelf", "garage-storage-rack", "garage", 21.65, 8.0, 2.0, 0.44, 1.82, 0x4a4d4f),
  D("chair", "garage-folding-chair", "garage", 20.25, 7.35, 0.58, 0.58, 0.84, 0x555556),
  D("stack", "garage-storage-bins", "garage", 23.55, 0.65, 0.92, 0.72, 0.8, 0x62686a),
]);

export function boxIntersectsDoorwayClearance(spec, doorway, padding = 0.08) {
  const halfW = Math.max(0, Number(spec?.w) || 0) / 2 + padding;
  const halfD = Math.max(0, Number(spec?.d) || 0) / 2 + padding;
  const x = Number(spec?.x) || 0;
  const z = Number(spec?.z) || 0;
  const doorX = Number(doorway?.x) || 0;
  const doorZ = Number(doorway?.z) || 0;
  const nearestX = Math.max(x - halfW, Math.min(x + halfW, doorX));
  const nearestZ = Math.max(z - halfD, Math.min(z + halfD, doorZ));
  return Math.hypot(nearestX - doorX, nearestZ - doorZ) <= (Number(doorway?.radius) || 0);
}

export function validateDecorLayout(specs = BACKROOM_DECORATIONS, doorways = BACKROOM_DOORWAYS) {
  const failures = [];
  for (const spec of specs) {
    if (spec.room === "living" || spec.room === "kitchen") failures.push({ name: spec.name, reason: "protected-main-room" });
    for (const doorway of doorways) {
      if (boxIntersectsDoorwayClearance(spec, doorway)) failures.push({ name: spec.name, doorway: doorway.id, reason: "doorway-clearance" });
    }
  }
  return failures;
}

function freezeStatic(mesh) {
  mesh.updateMatrix?.();
  mesh.matrixAutoUpdate = false;
  mesh.matrixWorldNeedsUpdate = true;
}

function installBackroomDecor(engine, I) {
  const world = engine?.world;
  const expansion = engine?.__expansion;
  if (!world?.root || !expansion?.spatial?.colliders || !I?.Mesh || !I?.BoxGeometry || !I?.MeshStandardMaterial) return false;
  if (world.__backroomDecorInstalled) return true;
  const existing = world.root.getObjectByName?.("hearthmouse-backroom-decor");
  if (existing) {
    world.__backroomDecorInstalled = true;
    return true;
  }

  const layoutFailures = validateDecorLayout();
  if (layoutFailures.length) {
    console.warn("Hearthmouse backroom decor skipped because doorway clearance validation failed", layoutFailures);
    return false;
  }

  const root = new I.Group();
  root.name = "hearthmouse-backroom-decor";
  world.root.add(root);

  const materials = new Map();
  const geometries = new Map();
  const material = (color, roughness = 0.9) => {
    const key = `${color}:${roughness}`;
    let value = materials.get(key);
    if (!value) {
      value = new I.MeshStandardMaterial({ color, roughness, metalness: 0 });
      materials.set(key, value);
    }
    return value;
  };
  const geometry = (w, h, d) => {
    const key = `${w}:${h}:${d}`;
    let value = geometries.get(key);
    if (!value) {
      value = new I.BoxGeometry(w, h, d);
      geometries.set(key, value);
    }
    return value;
  };

  const addOccluder = (mesh, x, z, w, d) => {
    mesh.userData.occluder = true;
    world.occluders.push(mesh);
    expansion.spatial.occluders?.insertBounds?.(mesh, x - w / 2, z - d / 2, x + w / 2, z + d / 2);
  };

  const addPart = (name, x, y, z, w, h, d, color, { occlude = false } = {}) => {
    const mesh = new I.Mesh(geometry(w, h, d), material(color));
    mesh.name = name;
    mesh.position.set(x, y, z);
    mesh.castShadow = false;
    mesh.receiveShadow = true;
    root.add(mesh);
    freezeStatic(mesh);
    if (occlude) addOccluder(mesh, x, z, w, d);
    return mesh;
  };

  const addCollider = (spec, height = spec.h) => {
    const collider = {
      minX: spec.x - spec.w / 2,
      maxX: spec.x + spec.w / 2,
      minZ: spec.z - spec.d / 2,
      maxZ: spec.z + spec.d / 2,
      minY: 0,
      maxY: height,
      name: `${spec.name}-collision`,
      catOnly: false,
      active: true,
    };
    world.colliders.push(collider);
    expansion.spatial.colliders.insertBounds(collider, collider.minX, collider.minZ, collider.maxX, collider.maxZ);
    return collider;
  };

  const addShelf = (spec) => {
    const board = Math.min(0.085, Math.max(0.055, spec.h * 0.045));
    const side = Math.min(0.09, Math.max(0.06, spec.w * 0.12));
    const backDepth = Math.min(0.07, spec.d * 0.18);
    addPart(`${spec.name}-left`, spec.x - spec.w / 2 + side / 2, spec.h / 2, spec.z, side, spec.h, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-right`, spec.x + spec.w / 2 - side / 2, spec.h / 2, spec.z, side, spec.h, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-back`, spec.x, spec.h / 2, spec.z - spec.d / 2 + backDepth / 2, spec.w, spec.h, backDepth, spec.color, { occlude: true });
    for (const fraction of [0.08, 0.36, 0.64, 0.94]) {
      addPart(`${spec.name}-board-${fraction}`, spec.x, spec.h * fraction, spec.z, spec.w, board, spec.d, spec.color);
    }
    addCollider(spec);
  };

  const addChair = (spec) => {
    const seatY = spec.h * 0.46;
    const legH = seatY;
    const leg = Math.min(0.09, spec.w * 0.16);
    addPart(`${spec.name}-seat`, spec.x, seatY, spec.z, spec.w, 0.1, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-back`, spec.x, spec.h * 0.72, spec.z - spec.d * 0.43, spec.w, spec.h * 0.55, 0.09, spec.color, { occlude: true });
    for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
      addPart(`${spec.name}-leg-${sx}-${sz}`, spec.x + sx * spec.w * 0.35, legH / 2, spec.z + sz * spec.d * 0.34, leg, legH, leg, spec.color);
    }
    if (spec.collide !== false) addCollider(spec, spec.h);
  };

  const addStack = (spec) => {
    const lowerH = spec.h * 0.56;
    addPart(`${spec.name}-lower`, spec.x - spec.w * 0.08, lowerH / 2, spec.z, spec.w, lowerH, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-upper`, spec.x + spec.w * 0.16, lowerH + (spec.h - lowerH) / 2, spec.z - spec.d * 0.08, spec.w * 0.72, spec.h - lowerH, spec.d * 0.82, spec.color, { occlude: true });
    if (spec.collide !== false) addCollider(spec, spec.h);
  };

  const addBasket = (spec) => {
    addPart(`${spec.name}-base`, spec.x, 0.045, spec.z, spec.w, 0.09, spec.d, spec.color);
    const rim = 0.06;
    addPart(`${spec.name}-north`, spec.x, spec.h / 2, spec.z - spec.d / 2 + rim / 2, spec.w, spec.h, rim, spec.color, { occlude: true });
    addPart(`${spec.name}-south`, spec.x, spec.h / 2, spec.z + spec.d / 2 - rim / 2, spec.w, spec.h, rim, spec.color, { occlude: true });
    addPart(`${spec.name}-west`, spec.x - spec.w / 2 + rim / 2, spec.h / 2, spec.z, rim, spec.h, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-east`, spec.x + spec.w / 2 - rim / 2, spec.h / 2, spec.z, rim, spec.h, spec.d, spec.color, { occlude: true });
  };

  for (const spec of BACKROOM_DECORATIONS) {
    if (spec.kind === "shelf") addShelf(spec);
    else if (spec.kind === "chair") addChair(spec);
    else if (spec.kind === "stack") addStack(spec);
    else if (spec.kind === "basket") addBasket(spec);
    else if (spec.kind === "rug") addPart(spec.name, spec.x, 0.008, spec.z, spec.w, spec.h, spec.d, spec.color);
    else {
      addPart(spec.name, spec.x, spec.h / 2, spec.z, spec.w, spec.h, spec.d, spec.color, { occlude: spec.h >= 0.45 });
      if (spec.collide) addCollider(spec);
    }
  }

  clearChildrenDoorwayObstructions(engine, expansion);
  world.__backroomDecorInstalled = true;
  return true;
}

const DECORATIVE_OBSTRUCTION = /(box|crate|desk|table|chair|shelf|cabinet|chest|storage|bin|toy)/i;
const STRUCTURAL_OBJECT = /(door|wall|baseboard|trim|floor|ceiling)/i;

function colliderIntersectsDoorway(collider, doorway, radius = 0.62) {
  const nearestX = Math.max(collider.minX, Math.min(collider.maxX, doorway.x));
  const nearestZ = Math.max(collider.minZ, Math.min(collider.maxZ, doorway.z));
  return Math.hypot(nearestX - doorway.x, nearestZ - doorway.z) <= radius;
}

export function clearChildrenDoorwayObstructions(engine, expansion = engine?.__expansion) {
  const world = engine?.world;
  if (!world?.colliders) return 0;
  let cleared = 0;
  for (const collider of world.colliders) {
    if (!collider?.active || !collider.name) continue;
    if (STRUCTURAL_OBJECT.test(collider.name) || !DECORATIVE_OBSTRUCTION.test(collider.name)) continue;
    if (!colliderIntersectsDoorway(collider, CHILDREN_DOORWAY)) continue;
    collider.active = false;
    const mesh = world.root?.getObjectByName?.(collider.name);
    if (mesh) {
      mesh.visible = false;
      const occluderIndex = world.occluders?.indexOf?.(mesh) ?? -1;
      if (occluderIndex >= 0) world.occluders.splice(occluderIndex, 1);
    }
    cleared++;
  }
  return cleared;
}

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  const I = window.HearthmouseInternals;
  if (engine && !engine.disposed && engine.__expansion?.world && I?.Group) {
    installBackroomDecor(engine, I);
    return;
  }
  if (attempt < 400) window.setTimeout(() => installWhenReady(attempt + 1), 25);
}

export { installBackroomDecor };

if (typeof window !== "undefined") installWhenReady();
