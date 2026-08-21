import {
  DOORWAY_CORRIDORS,
  ROOM_LAYOUT_BY_ID,
  SECRET_ROUTE_ENTRANCES,
  validateCirculationPlacement,
} from "./hearthmouse-circulation-layout.mjs";

const freeze = (value) => Object.freeze(value);
const P = (label, wall, lower, ceiling, floor, trim, accent, pattern, beamCount = 0) => freeze({
  label, wall, lower, ceiling, floor, trim, accent, pattern, beamCount,
});

// These palettes deliberately stay away from the pale generic shell colors used by
// the original expansion. Each unlockable space should be recognizable before the
// player reads a label: color, lower-wall treatment, ceiling rhythm, and trim all
// change as the house opens outward.
export const ROOM_DESIGN_PROFILES = freeze({
  hallway: P("sage gallery hall", 0x6e705c, 0x40362d, 0x847b68, 0x55473b, 0x9a825d, 0x6f3f34, "wainscot", 2),
  laundry: P("blue shiplap laundry", 0x607985, 0x3f5053, 0x78878a, 0x5e6968, 0xc4b78f, 0x355d67, "shiplap", 2),
  pantry: P("amber larder", 0x8b6c40, 0x4d3726, 0x79674f, 0x5c4933, 0xb89055, 0x6e3c2b, "pantry", 2),
  dining: P("claret dining room", 0x704544, 0x3e2c27, 0x7d695e, 0x55372f, 0xb08b65, 0x9b684c, "panel", 3),
  study: P("deep green library", 0x3f5c53, 0x2c2924, 0x666b5d, 0x493a31, 0x9b825b, 0x76563a, "library", 3),
  bathroom: P("sea-glass bathroom", 0x71918f, 0x4d6f72, 0x91a09c, 0x728b8b, 0xd4c7a5, 0x3c7775, "tile", 1),
  bedroom: P("dusty plum bedroom", 0x73566a, 0x463640, 0x8a7783, 0x65535f, 0xb69882, 0x9a6a73, "fabric", 2),
  children: P("storybook playroom", 0x92794f, 0x4f6956, 0x9d8c69, 0x6d5c48, 0xd0ae62, 0x8a4d47, "playful", 2),
  utility: P("charcoal service room", 0x505c60, 0x323c3f, 0x626b6d, 0x4f5759, 0xa6aa9f, 0x9b6d3f, "industrial", 3),
  mudroom: P("forest beadboard entry", 0x536b5b, 0x34483a, 0x777b69, 0x515850, 0xb69a6a, 0x7c4f38, "beadboard", 2),
  "basement-access": P("brick stair landing", 0x6c5542, 0x40352c, 0x554a3f, 0x493e34, 0x927255, 0x87533f, "brick", 2),
  basement: P("coal cellar workshop", 0x414d4d, 0x292f30, 0x3c4344, 0x343b3d, 0x777d79, 0x76533a, "brick", 4),
  garage: P("cinderblock garage", 0x596164, 0x343a3c, 0x474d4f, 0x3e4648, 0x8a8e87, 0x823f37, "industrial", 4),
});

const R = (id, room, x, z, w, d, unlockNight, style, routeId = null, colors = {}) => freeze({
  id,
  room,
  x,
  z,
  w,
  d,
  h: 0.19,
  unlockNight,
  style,
  routeId,
  mouseOnly: true,
  catOnly: true,
  roof: colors.roof ?? 0x493b30,
  trim: colors.trim ?? 0x755d45,
  floor: colors.floor ?? 0x302820,
});

// These are actual low covered corridors, not just painted mouse holes. Their
// roofs register as cat-only colliders while the tunnel itself remains open to
// the player and colony mice. Several visually continue an existing secret route;
// the basement run is a local safe lane that gives that room its own mouse scale.
export const MOUSE_ONLY_RUNS = freeze([
  R("pantry-grain-run", "pantry", 6.72, -10.9, 0.34, 0.78, 4, "wood", "pantry-cellar-chute", { roof: 0x4d3826, trim: 0x8c6b43, floor: 0x33281f }),
  R("bedroom-closet-crawl", "bedroom", 1.72, -14.1, 0.34, 1.8, 7, "fabric", "bedroom-closet-run", { roof: 0x513b48, trim: 0x8b6575, floor: 0x362b32 }),
  R("children-toy-tunnel", "children", -5.42, -13.45, 0.34, 1.4, 8, "toy", "toy-baseboard-hole", { roof: 0x66543a, trim: 0xc99b4c, floor: 0x3c3429 }),
  R("utility-service-duct", "utility", 15.45, 5.65, 0.34, 0.75, 8, "metal", "refrigerator-utility-run", { roof: 0x4b5558, trim: 0x858d89, floor: 0x303637 }),
  R("basement-crate-run", "basement", 7.55, -15.25, 0.34, 1.4, 11, "crate", null, { roof: 0x43372c, trim: 0x6f5941, floor: 0x292827 }),
  R("garage-vent-run", "garage", 19.22, 1.72, 0.34, 1.45, 11, "metal", "garage-utility-vent", { roof: 0x454b4d, trim: 0x818783, floor: 0x2d3233 }),
]);

export function validateMouseOnlyRuns(runs = MOUSE_ONLY_RUNS) {
  return runs.flatMap((run) => validateCirculationPlacement(run, {
    padding: 0.05,
    includeSecretEntrances: false,
  }).map((failure) => ({ id: run.id, ...failure })));
}

function freezeStatic(mesh) {
  mesh.updateMatrix?.();
  mesh.matrixAutoUpdate = false;
  mesh.matrixWorldNeedsUpdate = true;
}

function cloneTintMaterial(mesh, color, roughness = 0.9) {
  if (!mesh?.material) return;
  const tint = (material) => {
    if (!material?.clone) return material;
    const cloned = material.clone();
    cloned.color?.setHex?.(color);
    if ("roughness" in cloned) cloned.roughness = roughness;
    cloned.needsUpdate = true;
    return cloned;
  };
  mesh.material = Array.isArray(mesh.material) ? mesh.material.map(tint) : tint(mesh.material);
}

function tintExistingRoomShell(world, roomId, profile) {
  const roomRoot = world?.root?.getObjectByName?.("expanded-house-rooms");
  if (!roomRoot?.traverse) return;
  const wallPrefix = `${roomId}-`;
  roomRoot.traverse((object) => {
    if (!object?.isMesh || typeof object.name !== "string") return;
    if (object.name.startsWith(wallPrefix) && object.name.includes("-wall")) cloneTintMaterial(object, profile.wall, 0.92);
    else if (object.name === `${roomId}-ceiling`) cloneTintMaterial(object, profile.ceiling, 0.94);
    else if (object.name === `${roomId}-floor`) cloneTintMaterial(object, profile.floor, 0.94);
  });

  // A few walls are rebuilt under compatibility names rather than the room id.
  const aliases = {
    laundry: ["rebuilt-laundry-east"],
    study: ["rebuilt-study-north", "rebuilt-study-east"],
  }[roomId] ?? [];
  for (const alias of aliases) {
    roomRoot.traverse((object) => {
      if (object?.isMesh && object.name?.startsWith(alias)) cloneTintMaterial(object, profile.wall, 0.92);
    });
  }
}

function mergeIntervals(intervals, minimum, maximum) {
  const sorted = intervals
    .map(([start, end]) => [Math.max(minimum, start), Math.min(maximum, end)])
    .filter(([start, end]) => end > start)
    .sort((a, b) => a[0] - b[0]);
  const merged = [];
  for (const interval of sorted) {
    const prior = merged[merged.length - 1];
    if (!prior || interval[0] > prior[1] + 0.01) merged.push([...interval]);
    else prior[1] = Math.max(prior[1], interval[1]);
  }
  return merged;
}

function wallOpenings(room, side) {
  const horizontal = side === "north" || side === "south";
  const boundary = side === "north" ? room.minZ : side === "south" ? room.maxZ : side === "west" ? room.minX : room.maxX;
  const minimum = horizontal ? room.minX : room.minZ;
  const maximum = horizontal ? room.maxX : room.maxZ;
  const openings = [];

  for (const doorway of DOORWAY_CORRIDORS) {
    if (!doorway.rooms?.includes(room.id)) continue;
    if (horizontal && doorway.travelAxis === "z" && Math.abs(doorway.z - boundary) < 0.38) {
      openings.push([doorway.x - doorway.width / 2 - 0.12, doorway.x + doorway.width / 2 + 0.12]);
    } else if (!horizontal && doorway.travelAxis === "x" && Math.abs(doorway.x - boundary) < 0.38) {
      openings.push([doorway.z - doorway.width / 2 - 0.12, doorway.z + doorway.width / 2 + 0.12]);
    }
  }

  for (const entrance of SECRET_ROUTE_ENTRANCES) {
    if (entrance.roomId !== room.id) continue;
    const edgeDistance = horizontal ? Math.abs(entrance.z - boundary) : Math.abs(entrance.x - boundary);
    if (edgeDistance >= 0.42) continue;
    const span = horizontal ? Math.max(0.42, entrance.w ?? 0.42) : Math.max(0.42, entrance.d ?? 0.42);
    const center = horizontal ? entrance.x : entrance.z;
    openings.push([center - span / 2 - 0.1, center + span / 2 + 0.1]);
  }

  return mergeIntervals(openings, minimum, maximum);
}

function wallSegments(room, side) {
  const horizontal = side === "north" || side === "south";
  const minimum = horizontal ? room.minX : room.minZ;
  const maximum = horizontal ? room.maxX : room.maxZ;
  const openings = wallOpenings(room, side);
  const segments = [];
  let cursor = minimum;
  for (const [start, end] of openings) {
    if (start > cursor + 0.04) segments.push([cursor, start]);
    cursor = Math.max(cursor, end);
  }
  if (cursor < maximum - 0.04) segments.push([cursor, maximum]);
  return segments;
}

function installRoomDesignPass(engine, I) {
  const world = engine?.world;
  const expansion = engine?.__expansion;
  if (!world?.root || !expansion?.spatial?.colliders || !I?.Group || !I?.Mesh || !I?.BoxGeometry || !I?.MeshStandardMaterial) return false;
  if (world.__roomDesignPassInstalled) return true;
  const existing = world.root.getObjectByName?.("hearthmouse-room-design-pass");
  if (existing) {
    world.__roomDesignPassInstalled = true;
    return true;
  }

  const runFailures = validateMouseOnlyRuns();
  if (runFailures.length) {
    console.warn("Hearthmouse room design pass skipped because mouseway circulation validation failed", runFailures);
    return false;
  }

  const root = new I.Group();
  root.name = "hearthmouse-room-design-pass";
  world.root.add(root);

  const materials = new Map();
  const geometries = new Map();
  const material = (color, roughness = 0.92, metalness = 0) => {
    const key = `${color}:${roughness}:${metalness}`;
    let value = materials.get(key);
    if (!value) {
      value = new I.MeshStandardMaterial({ color, roughness, metalness });
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
  const addPart = ({ name, x, y, z, w, h, d, color, roughness = 0.92, metalness = 0 }) => {
    const mesh = new I.Mesh(geometry(w, h, d), material(color, roughness, metalness));
    mesh.name = name;
    mesh.position.set(x, y, z);
    mesh.castShadow = false;
    mesh.receiveShadow = true;
    root.add(mesh);
    freezeStatic(mesh);
    return mesh;
  };

  const wallPart = (room, side, start, end, y, h, depth, color, suffix, inset = 0) => {
    const horizontal = side === "north" || side === "south";
    const length = end - start;
    if (length <= 0.04) return null;
    if (horizontal) {
      const z = side === "north" ? room.minZ + 0.106 + inset : room.maxZ - 0.106 - inset;
      return addPart({
        name: `${room.id}-${side}-${suffix}`,
        x: (start + end) / 2,
        y,
        z,
        w: length,
        h,
        d: depth,
        color,
      });
    }
    const x = side === "west" ? room.minX + 0.106 + inset : room.maxX - 0.106 - inset;
    return addPart({
      name: `${room.id}-${side}-${suffix}`,
      x,
      y,
      z: (start + end) / 2,
      w: depth,
      h,
      d: length,
      color,
    });
  };

  const addPattern = (room, profile, side, start, end, segmentIndex) => {
    const length = end - start;
    const horizontal = side === "north" || side === "south";
    const at = (along, y, w, h, color, suffix) => {
      const center = start + along;
      if (horizontal) wallPart(room, side, center - w / 2, center + w / 2, y, h, 0.018, color, suffix, 0.018);
      else wallPart(room, side, center - w / 2, center + w / 2, y, h, 0.018, color, suffix, 0.018);
    };

    if (["wainscot", "panel", "library", "beadboard", "fabric"].includes(profile.pattern)) {
      const spacing = profile.pattern === "beadboard" ? 0.62 : 1.05;
      for (let offset = spacing; offset < length - 0.18; offset += spacing) at(offset, 0.43, 0.035, 0.72, profile.trim, `lower-rail-${segmentIndex}-${offset.toFixed(2)}`);
    }
    if (profile.pattern === "tile") {
      for (const y of [0.29, 0.57, 0.85]) wallPart(room, side, start, end, y, 0.018, 0.018, profile.trim, `tile-line-${segmentIndex}-${y}`, 0.018);
    } else if (profile.pattern === "brick") {
      for (const y of [0.38, 0.78, 1.18, 1.58, 1.98, 2.38]) wallPart(room, side, start, end, y, 0.015, 0.018, profile.trim, `mortar-${segmentIndex}-${y}`, 0.018);
    } else if (profile.pattern === "industrial" || profile.pattern === "shiplap") {
      const spacing = profile.pattern === "shiplap" ? 0.46 : 0.9;
      for (let offset = spacing; offset < length - 0.15; offset += spacing) at(offset, 1.47, 0.024, 2.7, profile.trim, `seam-${segmentIndex}-${offset.toFixed(2)}`);
    } else if (profile.pattern === "pantry") {
      wallPart(room, side, start, end, 1.16, 0.06, 0.02, profile.accent, `larder-band-${segmentIndex}`, 0.018);
    } else if (profile.pattern === "playful" && length > 0.85) {
      const patch = Math.min(0.36, length * 0.22);
      at(length * 0.32, 1.42, patch, 0.28, profile.accent, `paint-patch-a-${segmentIndex}`);
      at(length * 0.7, 1.72, patch * 0.72, 0.22, profile.trim, `paint-patch-b-${segmentIndex}`);
    }
  };

  const sides = ["north", "south", "west", "east"];
  for (const [roomId, profile] of Object.entries(ROOM_DESIGN_PROFILES)) {
    const room = ROOM_LAYOUT_BY_ID.get(roomId);
    if (!room) continue;
    tintExistingRoomShell(world, roomId, profile);

    const centerX = (room.minX + room.maxX) / 2;
    const centerZ = (room.minZ + room.maxZ) / 2;
    const width = room.maxX - room.minX;
    const depth = room.maxZ - room.minZ;
    addPart({ name: `${roomId}-design-ceiling`, x: centerX, y: 2.951, z: centerZ, w: width - 0.22, h: 0.018, d: depth - 0.22, color: profile.ceiling });
    addPart({ name: `${roomId}-design-floor`, x: centerX, y: 0.002, z: centerZ, w: width - 0.16, h: 0.004, d: depth - 0.16, color: profile.floor, roughness: 0.96 });

    for (const side of sides) {
      const segments = wallSegments(room, side);
      segments.forEach(([start, end], segmentIndex) => {
        wallPart(room, side, start, end, 1.47, 2.82, 0.024, profile.wall, `liner-${segmentIndex}`);
        wallPart(room, side, start, end, 0.43, 0.82, 0.026, profile.lower, `lower-${segmentIndex}`, 0.012);
        wallPart(room, side, start, end, 0.91, 0.055, 0.03, profile.trim, `chair-rail-${segmentIndex}`, 0.018);
        wallPart(room, side, start, end, 0.075, 0.14, 0.03, profile.trim, `base-trim-${segmentIndex}`, 0.018);
        addPattern(room, profile, side, start, end, segmentIndex);
      });
    }

    if (profile.beamCount > 0) {
      const acrossX = width >= depth;
      for (let index = 1; index <= profile.beamCount; index++) {
        const fraction = index / (profile.beamCount + 1);
        if (acrossX) {
          addPart({
            name: `${roomId}-ceiling-beam-${index}`,
            x: room.minX + width * fraction,
            y: 2.9,
            z: centerZ,
            w: 0.08,
            h: 0.08,
            d: Math.max(0.2, depth - 0.28),
            color: profile.trim,
          });
        } else {
          addPart({
            name: `${roomId}-ceiling-beam-${index}`,
            x: centerX,
            y: 2.9,
            z: room.minZ + depth * fraction,
            w: Math.max(0.2, width - 0.28),
            h: 0.08,
            d: 0.08,
            color: profile.trim,
          });
        }
      }
    }
  }

  const addCatOnlyCollider = (run, y, h) => {
    const collider = {
      minX: run.x - run.w / 2,
      maxX: run.x + run.w / 2,
      minZ: run.z - run.d / 2,
      maxZ: run.z + run.d / 2,
      minY: y - h / 2,
      maxY: y + h / 2,
      name: `mouseway-${run.id}-cat-roof`,
      catOnly: true,
      active: true,
    };
    world.colliders.push(collider);
    expansion.spatial.colliders.insertBounds(collider, collider.minX, collider.minZ, collider.maxX, collider.maxZ);
    return collider;
  };

  for (const run of MOUSE_ONLY_RUNS) {
    const roofY = run.h + 0.028;
    const roofH = 0.056;
    addPart({ name: `mouseway-${run.id}-floor`, x: run.x, y: 0.008, z: run.z, w: run.w, h: 0.016, d: run.d, color: run.floor, roughness: 1 });
    addPart({ name: `mouseway-${run.id}-roof`, x: run.x, y: roofY, z: run.z, w: run.w, h: roofH, d: run.d, color: run.roof, roughness: 0.97, metalness: run.style === "metal" ? 0.18 : 0 });
    addCatOnlyCollider(run, roofY, roofH);

    const longAxisZ = run.d >= run.w;
    const sideThickness = 0.026;
    if (longAxisZ) {
      for (const side of [-1, 1]) addPart({
        name: `mouseway-${run.id}-side-${side}`,
        x: run.x + side * (run.w / 2 - sideThickness / 2),
        y: 0.07,
        z: run.z,
        w: sideThickness,
        h: 0.14,
        d: run.d,
        color: run.trim,
      });
    } else {
      for (const side of [-1, 1]) addPart({
        name: `mouseway-${run.id}-side-${side}`,
        x: run.x,
        y: 0.07,
        z: run.z + side * (run.d / 2 - sideThickness / 2),
        w: run.w,
        h: 0.14,
        d: sideThickness,
        color: run.trim,
      });
    }

    for (const fraction of [0.08, 0.5, 0.92]) {
      if (longAxisZ) {
        addPart({ name: `mouseway-${run.id}-rib-${fraction}`, x: run.x, y: run.h * 0.54, z: run.z - run.d / 2 + run.d * fraction, w: run.w + 0.035, h: run.h, d: 0.025, color: run.trim, metalness: run.style === "metal" ? 0.24 : 0 });
      } else {
        addPart({ name: `mouseway-${run.id}-rib-${fraction}`, x: run.x - run.w / 2 + run.w * fraction, y: run.h * 0.54, z: run.z, w: 0.025, h: run.h, d: run.d + 0.035, color: run.trim, metalness: run.style === "metal" ? 0.24 : 0 });
      }
    }

    if (!world.shelterPoints?.some?.((point) => point.id === `mouseway-${run.id}`)) {
      world.shelterPoints ??= [];
      world.shelterPoints.push({
        id: `mouseway-${run.id}`,
        roomId: run.room,
        catProof: true,
        position: new I.Vector3(run.x, 0.025, run.z),
        unlockNight: run.unlockNight,
      });
    }
  }

  world.__roomDesignPassInstalled = true;
  return true;
}

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  const I = window.HearthmouseInternals;
  if (engine && !engine.disposed && engine.__expansion?.world && I?.Group) {
    installRoomDesignPass(engine, I);
    return;
  }
  if (attempt < 400) window.setTimeout(() => installWhenReady(attempt + 1), 25);
}

export { installRoomDesignPass };

if (typeof window !== "undefined") installWhenReady();
