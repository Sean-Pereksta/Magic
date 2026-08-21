import {
  DOORWAY_CORRIDORS,
  placementIntersectsDoorway,
  validateCirculationPlacement,
} from "./hearthmouse-circulation-layout.mjs";

export const BACKROOM_DOORWAYS = DOORWAY_CORRIDORS;
export const CHILDREN_DOORWAY = BACKROOM_DOORWAYS.find((doorway) => doorway.id === "children-door");

const D = (kind, name, room, x, z, w, d, h, color, extra = {}) => Object.freeze({
  kind, name, room, x, z, w, d, h, color, ...extra,
});

export const BACKROOM_DECORATIONS = Object.freeze([
  // Hallway / study wing: wall-biased pieces keep the central running lane open.
  D("shelf", "hall-wall-shelf", "hallway", -4.4, -7.45, 0.46, 1.32, 1.58, 0x564235),
  D("chair", "hall-reading-chair", "hallway", -4.34, -10.2, 0.58, 0.58, 0.92, 0x6d5748),
  D("lamp", "hall-reading-lamp", "hallway", -4.34, -9.72, 0.18, 0.18, 1.12, 0xb28a59, { collide: false }),
  D("wall-art", "hall-small-print", "hallway", -4.56, -7.02, 0.04, 0.58, 0.48, 0x8d765f, { collide: false }),
  D("rug", "hall-runner", "hallway", -1.1, -9.1, 2.55, 0.74, 0.018, 0x704c42),
  D("shelf", "study-bookcase", "study", -10.16, -8.7, 0.46, 1.78, 1.82, 0x4f3728),
  D("chair", "study-side-chair", "study", -9.36, -6.92, 0.62, 0.62, 0.94, 0x665044),
  D("stack", "study-storage-stack", "study", -9.68, -10.08, 0.72, 0.6, 0.58, 0x786047),
  D("lamp", "study-reading-lamp", "study", -8.92, -6.86, 0.16, 0.16, 1.08, 0xa98555, { collide: false }),

  // Pantry / dining: more household density without touching the kitchen proper.
  D("shelf", "pantry-back-rack", "pantry", 3.0, -10.95, 1.25, 0.46, 1.72, 0x5a422d),
  D("stack", "pantry-dry-goods", "pantry", 5.25, -10.95, 0.82, 0.55, 0.68, 0x8b704e),
  D("stack", "pantry-paper-bags", "pantry", 6.45, -7.15, 0.58, 0.48, 0.62, 0x9b805a),
  D("prop", "pantry-small-containers", "pantry", 2.6, -7.05, 0.72, 0.22, 0.22, 0x9a744d, { collide: false }),
  D("table", "dining-table", "dining", 1.68, 8.75, 1.42, 2.12, 0.78, 0x5b3b2e, { catOnly: true }),
  D("chair", "dining-chair-north", "dining", 1.68, 7.34, 0.6, 0.6, 0.9, 0x53392d),
  D("chair", "dining-chair-south", "dining", 1.68, 10.16, 0.6, 0.6, 0.9, 0x53392d),
  D("rug", "dining-rug", "dining", 3.2, 9.0, 3.0, 1.55, 0.018, 0x6f4c43),
  D("prop", "dining-dropped-food", "dining", 5.68, 7.05, 0.38, 0.26, 0.06, 0xb28b52, { collide: false }),

  // Bathroom / bedroom.
  D("shelf", "bath-linen-shelf", "bathroom", -4.34, -12.76, 0.42, 0.96, 1.52, 0x718486),
  D("basket", "bath-hamper", "bathroom", -4.24, -15.34, 0.55, 0.55, 0.62, 0x9a8464),
  D("mat", "bath-soft-mat", "bathroom", -2.18, -15.32, 0.72, 0.92, 0.014, 0x6f8588, { collide: false }),
  D("prop", "bath-small-bottles", "bathroom", -4.2, -13.35, 0.36, 0.16, 0.18, 0xa7b5ae, { collide: false }),
  D("box", "bedroom-nightstand", "bedroom", -0.86, -15.55, 0.56, 0.5, 0.62, 0x554035, { collide: true }),
  D("shelf", "bedroom-wardrobe", "bedroom", -0.95, -14.45, 0.58, 0.9, 1.72, 0x513b37),
  D("rug", "bedroom-rug", "bedroom", 0.35, -14.45, 1.45, 1.28, 0.018, 0x80646f),
  D("prop", "bedroom-shoes", "bedroom", 1.62, -15.7, 0.38, 0.28, 0.16, 0x3f342e, { collide: false }),

  // Children's room: the entry stays deliberately open. Storage is moved to the side/back walls.
  D("basket", "children-side-storage-bin", "children", -9.45, -11.65, 0.72, 0.58, 0.48, 0x8c6945),
  D("shelf", "children-bookcase", "children", -9.72, -14.55, 0.48, 1.48, 1.35, 0x76533b),
  D("chair", "children-small-chair", "children", -6.15, -14.45, 0.52, 0.52, 0.72, 0x7a5b48),
  D("rug", "children-play-rug", "children", -7.62, -13.15, 2.25, 1.45, 0.018, 0x8d6650),
  D("stack", "children-toy-blocks", "children", -6.25, -12.55, 0.6, 0.55, 0.42, 0xa37a4d),
  D("prop", "children-scattered-toys", "children", -9.3, -15.12, 0.72, 0.3, 0.18, 0xb17b4d, { collide: false, noisy: true }),
  D("box", "children-mouse-hide-box", "children", -5.56, -11.28, 0.58, 0.42, 0.3, 0x7f5d3f, { collide: false, shelter: true }),

  // Laundry / utility / mudroom.
  D("appliance", "laundry-washer", "laundry", 10.92, 2.24, 0.9, 0.7, 1.05, 0xd5d6d2, { collide: true }),
  D("appliance", "laundry-dryer", "laundry", 12.02, 2.24, 0.9, 0.7, 1.05, 0xc9cbc8, { collide: true }),
  D("basket", "laundry-hamper", "laundry", 11.25, 5.55, 0.7, 0.62, 0.72, 0x88745e),
  D("shelf", "laundry-supply-rack", "laundry", 13.9, 5.72, 1.45, 0.46, 1.58, 0x5c5148),
  D("chair", "laundry-stool", "laundry", 14.42, 2.35, 0.5, 0.5, 0.68, 0x615144),
  D("shelf", "utility-north-rack", "utility", 16.25, 2.25, 1.52, 0.44, 1.76, 0x50585a),
  D("shelf", "utility-south-rack", "utility", 17.55, 5.75, 1.3, 0.44, 1.7, 0x51595b),
  D("stack", "utility-paint-cans", "utility", 16.1, 5.45, 0.62, 0.52, 0.48, 0x72787a),
  D("pipe", "utility-wall-pipes", "utility", 18.48, 5.76, 0.12, 0.3, 1.55, 0x6d7375, { collide: false }),
  D("prop", "utility-tools", "utility", 15.52, 2.22, 0.62, 0.18, 0.24, 0x4a4f50, { collide: false }),
  D("shelf", "mudroom-boot-rack", "mudroom", 1.25, 13.65, 1.18, 0.42, 0.82, 0x55463a),
  D("basket", "mudroom-basket", "mudroom", 4.8, 13.7, 0.64, 0.54, 0.52, 0x856f54),
  D("mat", "mudroom-floor-mat", "mudroom", 3.62, 13.25, 1.22, 0.72, 0.018, 0x4f5149, { collide: false }),
  D("prop", "mudroom-boots", "mudroom", 1.22, 12.65, 0.62, 0.34, 0.28, 0x3d342d, { collide: false }),

  // Basement access gets storage along the walls, not the stair/door line.
  D("shelf", "basement-access-east-rack", "basement-access", 11.82, -7.55, 0.42, 1.35, 1.72, 0x403831),
  D("shelf", "basement-access-supply-shelf", "basement-access", 9.6, -6.82, 1.45, 0.42, 1.48, 0x4c4034),
  D("stack", "basement-access-boxes", "basement-access", 8.12, -7.08, 0.72, 0.62, 0.62, 0x69533b),
  D("chair", "basement-access-folding-chair", "basement-access", 11.55, -10.35, 0.55, 0.55, 0.82, 0x4b4741),
  D("wall-art", "basement-access-coat-hooks", "basement-access", 12.24, -7.2, 0.04, 0.82, 0.38, 0x4a4037, { collide: false }),

  // Basement: denser storage/work area while preserving the center route from the stairs.
  D("shelf", "basement-west-storage", "basement", 7.65, -13.45, 0.46, 1.85, 1.82, 0x45433e),
  D("shelf", "basement-east-storage", "basement", 11.95, -15.55, 0.46, 1.65, 1.82, 0x45433e),
  D("shelf", "basement-back-storage", "basement", 11.22, -16.35, 1.34, 0.42, 1.7, 0x4e4840),
  D("chair", "basement-work-chair", "basement", 10.85, -13.9, 0.58, 0.58, 0.88, 0x4f4b45),
  D("stack", "basement-storage-totes", "basement", 8.45, -16.05, 0.86, 0.62, 0.72, 0x5f6260),
  D("table", "basement-workbench", "basement", 11.58, -12.45, 1.18, 0.56, 0.82, 0x47413a, { catOnly: true }),
  D("pipe", "basement-low-pipes", "basement", 7.38, -12.78, 0.12, 1.02, 1.4, 0x62686a, { collide: false }),

  // Garage: practical clutter concentrated around outer walls.
  D("shelf", "garage-tool-rack", "garage", 23.9, 2.15, 0.46, 2.15, 1.88, 0x474b4d),
  D("shelf", "garage-storage-rack", "garage", 21.65, 8.0, 2.0, 0.44, 1.82, 0x4a4d4f),
  D("chair", "garage-folding-chair", "garage", 20.25, 7.35, 0.58, 0.58, 0.84, 0x555556),
  D("stack", "garage-storage-bins", "garage", 23.55, 0.65, 0.92, 0.72, 0.8, 0x62686a),
  D("tire", "garage-tire-stack", "garage", 19.45, 0.28, 0.72, 0.72, 0.9, 0x2c2d2d, { collide: true }),
  D("prop", "garage-paint-cans", "garage", 23.7, 8.12, 0.72, 0.28, 0.34, 0x73787a, { collide: false }),
  D("wall-art", "garage-tool-wall", "garage", 24.32, 5.62, 0.04, 1.5, 1.12, 0x3f4547, { collide: false }),
]);

export function boxIntersectsDoorwayClearance(spec, doorway, padding = 0.08) {
  return placementIntersectsDoorway(spec, doorway, padding);
}

export function validateDecorLayout(specs = BACKROOM_DECORATIONS, doorways = BACKROOM_DOORWAYS) {
  const failures = [];
  for (const spec of specs) {
    if (spec.room === "living" || spec.room === "kitchen") failures.push({ name: spec.name, reason: "protected-main-room" });
    const cosmetic = ["rug", "mat", "wall-art"].includes(spec.kind) || spec.h <= 0.025;
    for (const failure of validateCirculationPlacement(spec, { cosmetic })) {
      failures.push({ name: spec.name, doorway: failure.reason === "doorway-corridor" ? failure.zone : undefined, ...failure });
    }
    // Keep support for callers that pass a narrowed custom doorway set.
    if (doorways !== BACKROOM_DOORWAYS) {
      for (const doorway of doorways) {
        if (boxIntersectsDoorwayClearance(spec, doorway) && !failures.some((failure) => failure.name === spec.name && failure.zone === doorway.id)) {
          failures.push({ name: spec.name, doorway: doorway.id, zone: doorway.id, reason: "doorway-corridor" });
        }
      }
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
      catOnly: !!spec.catOnly,
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

  const addTable = (spec) => {
    const topThickness = Math.min(0.12, Math.max(0.07, spec.h * 0.14));
    addPart(`${spec.name}-top`, spec.x, spec.h - topThickness / 2, spec.z, spec.w, topThickness, spec.d, spec.color, { occlude: true });
    const leg = Math.min(0.1, Math.max(0.07, spec.w * 0.08));
    for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
      addPart(
        `${spec.name}-leg-${sx}-${sz}`,
        spec.x + sx * (spec.w / 2 - leg * 1.2),
        (spec.h - topThickness) / 2,
        spec.z + sz * (spec.d / 2 - leg * 1.2),
        leg,
        spec.h - topThickness,
        leg,
        spec.color,
      );
    }
    addCollider({ ...spec, catOnly: true });
  };

  const addAppliance = (spec) => {
    addPart(`${spec.name}-body`, spec.x, spec.h / 2, spec.z, spec.w, spec.h, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-door`, spec.x, spec.h * 0.53, spec.z + spec.d / 2 + 0.012, spec.w * 0.58, spec.h * 0.44, 0.022, 0x656a6b);
    addPart(`${spec.name}-controls`, spec.x, spec.h * 0.87, spec.z + spec.d / 2 + 0.018, spec.w * 0.68, 0.08, 0.025, 0x4c5051);
    if (spec.collide !== false) addCollider(spec);
  };

  const addLamp = (spec) => {
    addPart(`${spec.name}-base`, spec.x, 0.035, spec.z, spec.w, 0.07, spec.d, 0x59473a);
    addPart(`${spec.name}-stem`, spec.x, spec.h * 0.48, spec.z, 0.045, spec.h * 0.84, 0.045, 0x665544);
    addPart(`${spec.name}-shade`, spec.x, spec.h * 0.88, spec.z, spec.w * 1.7, spec.h * 0.2, spec.d * 1.7, spec.color);
  };

  const addSmallProp = (spec) => {
    const pieces = spec.name.includes("shoes") || spec.name.includes("boots") ? 2 : 3;
    for (let index = 0; index < pieces; index++) {
      const fraction = pieces === 1 ? 0 : index / (pieces - 1) - 0.5;
      addPart(
        `${spec.name}-${index}`,
        spec.x + fraction * spec.w * 0.72,
        spec.h / 2 + (index % 2) * spec.h * 0.08,
        spec.z + (index % 2 ? spec.d * 0.12 : -spec.d * 0.1),
        spec.w / Math.max(2, pieces),
        spec.h * (0.72 + (index % 2) * 0.18),
        spec.d * 0.72,
        spec.color,
      );
    }
  };

  const addTireStack = (spec) => {
    const layers = 3;
    for (let index = 0; index < layers; index++) {
      const y = (index + 0.5) * spec.h / layers;
      addPart(`${spec.name}-layer-${index}`, spec.x, y, spec.z, spec.w, spec.h / layers * 0.82, spec.d, spec.color, { occlude: true });
      addPart(`${spec.name}-hub-${index}`, spec.x, y, spec.z + spec.d / 2 + 0.008, spec.w * 0.42, spec.h / layers * 0.46, 0.018, 0x4d5050);
    }
    if (spec.collide !== false) addCollider(spec);
  };

  for (const spec of BACKROOM_DECORATIONS) {
    if (spec.kind === "shelf") addShelf(spec);
    else if (spec.kind === "chair") addChair(spec);
    else if (spec.kind === "stack") addStack(spec);
    else if (spec.kind === "basket") addBasket(spec);
    else if (spec.kind === "table") addTable(spec);
    else if (spec.kind === "appliance") addAppliance(spec);
    else if (spec.kind === "lamp") addLamp(spec);
    else if (spec.kind === "prop") addSmallProp(spec);
    else if (spec.kind === "tire") addTireStack(spec);
    else if (spec.kind === "rug" || spec.kind === "mat") addPart(spec.name, spec.x, 0.008, spec.z, spec.w, Math.max(0.008, spec.h), spec.d, spec.color);
    else if (spec.kind === "wall-art") addPart(spec.name, spec.x, 1.46, spec.z, spec.w, spec.h, spec.d, spec.color);
    else {
      addPart(spec.name, spec.x, spec.h / 2, spec.z, spec.w, spec.h, spec.d, spec.color, { occlude: spec.h >= 0.45 });
      if (spec.collide) addCollider(spec);
    }
    if (spec.shelter) {
      world.shelterPoints.push({
        id: spec.name,
        roomId: spec.room,
        catProof: true,
        position: new I.Vector3(spec.x, 0.025, spec.z),
        unlockNight: 1,
      });
    }
  }

  clearDoorwayObstructions(engine, expansion);
  world.__backroomDecorInstalled = true;
  return true;
}

const DECORATIVE_OBSTRUCTION = /(box|crate|desk|table|chair|shelf|cabinet|chest|storage|bin|toy)/i;
const STRUCTURAL_OBJECT = /(door|wall|baseboard|trim|floor|ceiling)/i;

function colliderIntersectsDoorway(collider, doorway) {
  const spec = {
    x: (collider.minX + collider.maxX) / 2,
    z: (collider.minZ + collider.maxZ) / 2,
    w: collider.maxX - collider.minX,
    d: collider.maxZ - collider.minZ,
  };
  return placementIntersectsDoorway(spec, doorway, 0.04);
}

export function clearDoorwayObstructions(engine, expansion = engine?.__expansion, doorways = BACKROOM_DOORWAYS) {
  const world = engine?.world;
  if (!world?.colliders) return 0;
  let cleared = 0;
  for (const collider of world.colliders) {
    if (!collider?.active || !collider.name) continue;
    if (STRUCTURAL_OBJECT.test(collider.name) || !DECORATIVE_OBSTRUCTION.test(collider.name)) continue;
    if (!doorways.some((doorway) => colliderIntersectsDoorway(collider, doorway))) continue;
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

export function clearChildrenDoorwayObstructions(engine, expansion = engine?.__expansion) {
  return clearDoorwayObstructions(engine, expansion, [CHILDREN_DOORWAY]);
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
