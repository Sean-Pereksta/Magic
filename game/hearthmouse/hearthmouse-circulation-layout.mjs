const freeze = (value) => Object.freeze(value);

export const AGENT_CLEARANCES = freeze({ mouse: 0.055, cat: 0.205, catTurn: 0.46, stair: 0.58 });

const room = (id, name, unlockNight, minX, maxX, minZ, maxZ, surface, depth, color, gameplay) => freeze({
  id,
  name,
  unlockNight,
  minX,
  maxX,
  minZ,
  maxZ,
  surface,
  depth,
  color,
  gameplay: freeze({ ...gameplay }),
});

// One authoritative room catalogue feeds construction, food weighting, sound,
// territory selection, decor validation, events, traps, and secret routes.
export const HEARTHMOUSE_ROOM_DEFINITIONS = freeze([
  room("hallway", "Hallway", 2, -4.7, 2.1, -11.8, -6.38, "carpet", 2, 0x6b5a4a, {
    foodWeight: 0.55, cover: 0.42, light: 0.58, noise: 0.3, trapWeight: 0.25, identity: "quiet connector and study edge",
  }),
  room("pantry", "Pantry", 4, 2.1, 7.2, -11.8, -6.38, "paper", 3, 0x7d6848, {
    foodWeight: 1.8, cover: 0.52, light: 0.48, noise: 1, trapWeight: 1.8, identity: "rich food and loud packaging",
  }),
  room("dining", "Dining Room", 5, 0.4, 6.8, 6.38, 11.6, "wood", 3, 0x6e4937, {
    foodWeight: 1.25, cover: 0.18, light: 0.72, noise: 0.62, trapWeight: 0.8, identity: "exposed crossings and dropped meals",
  }),
  room("bathroom", "Bathroom", 6, -4.7, -1.3, -16, -11.8, "tile", 4, 0x8ba0a0, {
    foodWeight: 0.2, cover: 1, light: 0.66, noise: 0.7, trapWeight: 0.15, identity: "poor food and excellent escapes",
  }),
  room("bedroom", "Bedroom", 7, -1.3, 2.1, -16, -11.8, "carpet", 4, 0x735a67, {
    foodWeight: 0.62, cover: 0.82, light: 0.42, noise: 0.22, trapWeight: 0.32, identity: "soft cover with perimeter furniture",
  }),
  room("children", "Children's Room", 8, -10.38, -5.02, -15.6, -10.68, "carpet", 5, 0x76654b, {
    foodWeight: 1.05, cover: 1, light: 0.62, noise: 0.78, trapWeight: 0.72, identity: "many hiding places and noisy toys",
  }),
  room("utility", "Utility Room", 8, 15, 18.8, 1.84, 6.26, "metal", 5, 0x646d70, {
    foodWeight: 0.42, cover: 0.52, light: 0.5, noise: 0.88, trapWeight: 0.55, identity: "materials and alternate routing",
  }),
  room("mudroom", "Mudroom", 9, 0.4, 6.8, 11.6, 14.2, "tile", 5, 0x665f52, {
    foodWeight: 0.48, cover: 0.38, light: 0.68, noise: 0.68, trapWeight: 0.45, identity: "low food and a useful junction",
  }),
  room("basement-access", "Basement Access", 10, 7.2, 12.4, -11.8, -6.38, "wood", 6, 0x52473d, {
    foodWeight: 0.52, cover: 0.32, light: 0.34, noise: 0.58, trapWeight: 0.65, identity: "protected stair approach",
  }),
  room("basement", "Basement", 11, 7.2, 12.4, -16.8, -11.8, "tile", 7, 0x3f4648, {
    foodWeight: 0.72, cover: 1, light: 0.16, noise: 0.48, trapWeight: 1.2, identity: "dark cover and rare supplies",
  }),
  room("garage", "Garage", 11, 18.8, 24.5, -0.5, 8.5, "metal", 7, 0x4a4f52, {
    foodWeight: 0.82, cover: 0.12, light: 0.58, noise: 1, trapWeight: 1.45, identity: "long sightlines and valuable resources",
  }),
]);

const baseRooms = [
  room("living", "Living Room", 1, -10.5, 0.1, -6.5, 6.5, "carpet", 0, 0x6b5a4a, {
    foodWeight: 1, cover: 0.72, light: 0.62, noise: 0.3, trapWeight: 0.45, identity: "nest-side shelter",
  }),
  room("kitchen", "Kitchen", 1, -0.1, 10.6, -6.5, 6.5, "tile", 0, 0x82715e, {
    foodWeight: 1.2, cover: 0.35, light: 0.78, noise: 0.72, trapWeight: 1.1, identity: "bright food crossing",
  }),
  room("laundry", "Laundry Room", 3, 10.35, 15.1, 1.7, 6.35, "tile", 2, 0x6d665b, {
    foodWeight: 0.68, cover: 0.68, light: 0.55, noise: 0.72, trapWeight: 0.45, identity: "machine-noise movement windows",
  }),
  room("study", "Study", 6, -10.5, -4.9, -10.8, -6.25, "carpet", 3, 0x5f4d42, {
    foodWeight: 0.5, cover: 0.72, light: 0.42, noise: 0.26, trapWeight: 0.3, identity: "quiet shelves and hidden routes",
  }),
];

export const ROOM_LAYOUTS = freeze([...baseRooms, ...HEARTHMOUSE_ROOM_DEFINITIONS]);
export const ROOM_LAYOUT_BY_ID = new Map(ROOM_LAYOUTS.map((definition) => [definition.id, definition]));

// Safe regions are the usable room envelopes; their listed exclusions are
// subtracted by validateCirculationPlacement before any object is authored.
// This keeps future decorators data-driven without duplicating room geometry.
export const SAFE_DECOR_REGIONS = freeze(ROOM_LAYOUTS.map((definition) => freeze({
  roomId: definition.id,
  minX: definition.minX + 0.02,
  maxX: definition.maxX - 0.02,
  minZ: definition.minZ + 0.02,
  maxZ: definition.maxZ - 0.02,
  excludeDoorways: true,
  excludeTravelLanes: true,
  excludeSecretEntrances: true,
})));
const SAFE_DECOR_REGION_BY_ROOM = new Map(SAFE_DECOR_REGIONS.map((region) => [region.roomId, region]));

const doorway = (id, x, z, width, depth, travelAxis, rooms, extra = {}) => freeze({
  id,
  x,
  z,
  width,
  depth,
  travelAxis,
  rooms: freeze([...rooms]),
  mouseClearance: AGENT_CLEARANCES.mouse,
  catClearance: AGENT_CLEARANCES.cat,
  catTurnMargin: AGENT_CLEARANCES.catTurn,
  ...extra,
});

// depth is the entire protected entrance funnel, deliberately extending well
// into both connected rooms instead of protecting only the door's center point.
export const DOORWAY_CORRIDORS = freeze([
  doorway("main-shortcut", 0, 0, 1.65, 2.2, "x", ["living", "kitchen"]),
  doorway("study-door", -7.65, -6.33, 1.18, 2.2, "z", ["living", "study"]),
  doorway("laundry-door", 10.35, 4.05, 1.2, 2.2, "x", ["kitchen", "laundry"]),
  doorway("hallway-door", -1.3, -6.33, 1.4, 2.25, "z", ["living", "hallway"]),
  doorway("pantry-hall-door", 2.05, -8.9, 1.3, 2.25, "x", ["hallway", "pantry"]),
  doorway("pantry-kitchen-door", 4.5, -6.33, 1.25, 2.25, "z", ["kitchen", "pantry"]),
  doorway("dining-door", 3.6, 6.33, 1.4, 2.3, "z", ["kitchen", "dining"]),
  doorway("bathroom-door", -3, -11.75, 1.05, 2.15, "z", ["hallway", "bathroom"]),
  doorway("bedroom-door", 0.45, -11.75, 1.05, 2.15, "z", ["hallway", "bedroom"]),
  doorway("children-door", -7.7, -10.63, 1.15, 2.2, "z", ["study", "children"]),
  doorway("utility-door", 15.05, 4.05, 1.2, 2.25, "x", ["laundry", "utility"]),
  doorway("mudroom-door", 3.6, 11.55, 1.1, 2.2, "z", ["dining", "mudroom"]),
  doorway("basement-access-door", 7.15, -8.9, 1.1, 2.25, "x", ["pantry", "basement-access"]),
  doorway("basement-door", 9.8, -11.75, 1.05, 2.2, "z", ["basement-access", "basement"]),
  doorway("garage-door", 18.75, 4, 1.2, 2.3, "x", ["utility", "garage"]),
]);

const lane = (id, roomId, minX, maxX, minZ, maxZ, kind = "travel") => freeze({
  id, roomId, minX, maxX, minZ, maxZ, kind,
});

export const TRAVEL_LANES = freeze([
  lane("hallway-center", "hallway", -4.45, 1.72, -9.35, -8.45),
  lane("hallway-spine", "hallway", -1.78, -0.82, -11.78, -6.3),
  lane("hallway-north-branch", "hallway", -3.42, 0.88, -11.48, -10.68),
  lane("study-center", "study", -8.15, -7.15, -10.55, -6.28),
  lane("pantry-through", "pantry", 2.02, 6.92, -9.38, -8.42),
  lane("dining-crossing", "dining", 2.92, 4.28, 6.28, 11.58),
  lane("bathroom-center", "bathroom", -3.48, -2.58, -15.65, -11.62),
  lane("bedroom-center", "bedroom", -0.22, 0.68, -14.72, -11.62),
  lane("children-center", "children", -8.18, -7.22, -15.25, -10.55),
  lane("laundry-through", "laundry", 10.2, 15.15, 3.52, 4.56),
  lane("utility-through", "utility", 14.88, 18.86, 3.5, 4.58),
  lane("mudroom-center", "mudroom", 2.92, 4.28, 11.45, 14.15),
  lane("basement-access-through", "basement-access", 7.02, 10.55, -9.38, -8.42),
  lane("stairs-landing", "basement-access", 9.22, 10.38, -11.82, -8.3, "stairs"),
  lane("basement-aisle", "basement", 9.18, 10.42, -16.72, -11.62),
  lane("garage-entry", "garage", 18.6, 20.2, 3.38, 4.65),
  lane("garage-main-aisle", "garage", 19.35, 24.2, 4.58, 6.08),
]);

const routeEntrance = (id, roomId, x, z, w = 0.72, d = 0.72) => freeze({ id, roomId, x, z, w, d });

export const SECRET_ROUTE_ENTRANCES = freeze([
  routeEntrance("study-hall-west", "study", -5.03, -9, 0.72, 0.86),
  routeEntrance("study-hall-east", "hallway", -4.72, -9, 0.72, 0.86),
  routeEntrance("bath-bedroom-west", "bathroom", -1.48, -13, 0.78, 0.72),
  routeEntrance("bath-bedroom-east", "bedroom", -1.12, -13, 0.78, 0.72),
  routeEntrance("children-bath-west", "children", -5.18, -14.25, 0.78, 0.72),
  routeEntrance("children-bath-east", "bathroom", -4.53, -14.25, 0.78, 0.72),
  routeEntrance("pantry-cellar-west", "pantry", 7.02, -10.55, 0.72, 0.78),
  routeEntrance("pantry-cellar-east", "basement-access", 7.38, -10.55, 0.72, 0.78),
  routeEntrance("garage-utility-west", "utility", 18.62, 2.55, 0.72, 0.78),
  routeEntrance("garage-utility-east", "garage", 18.98, 2.55, 0.72, 0.78),
]);

export const SECRET_ROUTES = freeze([
  freeze({ id: "study-wall-gap", a: "study-hall-west", b: "study-hall-east", discoveryNight: 6, noise: 0.08, speed: 1 }),
  freeze({ id: "bathroom-pipe-gap", a: "bath-bedroom-west", b: "bath-bedroom-east", discoveryNight: 7, noise: 0.12, speed: 0.88 }),
  freeze({ id: "toy-baseboard-hole", a: "children-bath-west", b: "children-bath-east", discoveryNight: 8, noise: 0.34, speed: 0.82 }),
  freeze({ id: "pantry-cellar-chute", a: "pantry-cellar-west", b: "pantry-cellar-east", discoveryNight: 10, noise: 0.45, speed: 0.76, oneWay: false }),
  freeze({ id: "garage-utility-vent", a: "garage-utility-west", b: "garage-utility-east", discoveryNight: 11, noise: 0.6, speed: 0.72 }),
]);

const anchor = (id, roomId, x, z, type, minNight, weight = 1) => freeze({ id, roomId, x, z, type, minNight, weight, w: 0.34, d: 0.3 });

// Human-believable, perimeter-biased anchors. Placement is still validated at
// spawn time because temporary event props can change a night's safe regions.
export const TRAP_ANCHORS = freeze([
  anchor("kitchen-cabinet-wire", "kitchen", 8.95, -5.62, "thin-wire", 2, 0.7),
  anchor("kitchen-wall-snap", "kitchen", 9.82, 5.65, "traditional", 3, 1),
  anchor("pantry-rear-box", "pantry", 3.05, -10.92, "cheese-box", 4, 1.3),
  anchor("pantry-east-clutter", "pantry", 6.08, -10.96, "hidden-clutter", 4, 1.5),
  anchor("pantry-south-wire", "pantry", 2.52, -7.08, "thin-wire", 4, 1.1),
  anchor("dining-sideboard-snap", "dining", 6.12, 10.72, "traditional", 5, 0.8),
  anchor("dining-west-wire", "dining", 0.88, 10.55, "thin-wire", 5, 0.65),
  anchor("children-toy-box", "children", -9.28, -12.05, "hidden-clutter", 8, 0.8),
  anchor("children-back-wire", "children", -6.02, -15.15, "thin-wire", 8, 0.55),
  anchor("utility-paint-clutter", "utility", 16.05, 5.62, "hidden-clutter", 8, 0.7),
  anchor("basement-west-clutter", "basement", 7.62, -15.85, "hidden-clutter", 11, 1.3),
  anchor("basement-east-snap", "basement", 11.92, -13.15, "traditional", 11, 1.1),
  anchor("garage-tool-box", "garage", 23.85, 0.25, "cheese-box", 11, 1.2),
  anchor("garage-back-wire", "garage", 20.08, 7.92, "thin-wire", 11, 1),
  anchor("garage-storage-clutter", "garage", 23.68, 7.75, "hidden-clutter", 11, 1.35),
]);

export function roomForPoint(x, z) {
  for (const definition of ROOM_LAYOUTS) {
    if (x >= definition.minX - 0.12 && x <= definition.maxX + 0.12 && z >= definition.minZ - 0.12 && z <= definition.maxZ + 0.12) return definition.id;
  }
  return null;
}

export function doorwayProtectedBounds(door) {
  const protectedWidth = door.width + (door.catTurnMargin ?? 0.46) * 2;
  const w = door.travelAxis === "x" ? door.depth : protectedWidth;
  const d = door.travelAxis === "z" ? door.depth : protectedWidth;
  return { minX: door.x - w / 2, maxX: door.x + w / 2, minZ: door.z - d / 2, maxZ: door.z + d / 2 };
}

export function placementBounds(spec, padding = 0) {
  const halfW = Math.max(0, Number(spec?.w) || 0) / 2 + padding;
  const halfD = Math.max(0, Number(spec?.d) || 0) / 2 + padding;
  const x = Number(spec?.x) || 0;
  const z = Number(spec?.z) || 0;
  return { minX: x - halfW, maxX: x + halfW, minZ: z - halfD, maxZ: z + halfD };
}

export function boundsOverlap(a, b) {
  return a.minX < b.maxX && a.maxX > b.minX && a.minZ < b.maxZ && a.maxZ > b.minZ;
}

export function placementIntersectsDoorway(spec, door, padding = 0.08) {
  return boundsOverlap(placementBounds(spec, padding), doorwayProtectedBounds(door));
}

export function placementIntersectsTravelLane(spec, travelLane, padding = 0.08) {
  return boundsOverlap(placementBounds(spec, padding), travelLane);
}

export function placementIntersectsSecretEntrance(spec, entrance, padding = 0.08) {
  return boundsOverlap(placementBounds(spec, padding), placementBounds(entrance));
}

export function validateCirculationPlacement(spec, {
  padding = 0.08,
  cosmetic = false,
  includeTravelLanes = true,
  includeSecretEntrances = true,
} = {}) {
  const failures = [];
  for (const door of DOORWAY_CORRIDORS) {
    if (placementIntersectsDoorway(spec, door, padding)) failures.push({ reason: "doorway-corridor", zone: door.id });
  }
  if (!cosmetic && includeTravelLanes) {
    for (const travelLane of TRAVEL_LANES) {
      if (placementIntersectsTravelLane(spec, travelLane, padding)) failures.push({ reason: travelLane.kind === "stairs" ? "stair-clearance" : "travel-lane", zone: travelLane.id });
    }
  }
  if (!cosmetic && includeSecretEntrances) {
    for (const entrance of SECRET_ROUTE_ENTRANCES) {
      if (placementIntersectsSecretEntrance(spec, entrance, padding)) failures.push({ reason: "secret-route", zone: entrance.id });
    }
  }
  const expectedRoom = spec?.room ?? spec?.roomId;
  const bounds = expectedRoom ? SAFE_DECOR_REGION_BY_ROOM.get(expectedRoom) ?? ROOM_LAYOUT_BY_ID.get(expectedRoom) : null;
  if (bounds) {
    const placed = placementBounds(spec);
    if (placed.minX < bounds.minX || placed.maxX > bounds.maxX || placed.minZ < bounds.minZ || placed.maxZ > bounds.maxZ) {
      failures.push({ reason: "outside-room", zone: expectedRoom });
    }
  }
  return failures;
}

export function isCirculationPlacementSafe(spec, options) {
  return validateCirculationPlacement(spec, options).length === 0;
}

export function validateTrapAnchors(anchors = TRAP_ANCHORS) {
  return anchors.flatMap((trap) => validateCirculationPlacement(trap, { padding: 0.1 }).map((failure) => ({ id: trap.id, ...failure })));
}

export function roomGameplay(roomId) {
  return ROOM_LAYOUT_BY_ID.get(roomId)?.gameplay ?? null;
}
