import {
  DOORWAY_CORRIDORS,
  ROOM_LAYOUTS,
  SECRET_ROUTE_ENTRANCES,
  placementIntersectsDoorway,
  validateCirculationPlacement,
} from "./hearthmouse-circulation-layout.mjs";

export const BACKROOM_DOORWAYS = DOORWAY_CORRIDORS;
export const CHILDREN_DOORWAY = BACKROOM_DOORWAYS.find((doorway) => doorway.id === "children-door");

const D = (kind, name, room, x, z, w, d, h, color, extra = {}) => Object.freeze({
  kind, name, room, x, z, w, d, h, color, ...extra,
});

const identity = (wall, lowerWall, trim, floor, accent, treatment, landmark, light, extra = {}) => Object.freeze({
  wall,
  lowerWall,
  trim,
  floor,
  accent,
  treatment,
  landmark,
  light: Object.freeze({ color: light.color, intensity: light.intensity, y: light.y ?? 2.45, point: !!light.point }),
  crown: !!extra.crown,
  lowerHeight: extra.lowerHeight ?? 0.86,
  ambience: extra.ambience ?? "quiet-house",
  exteriorSpill: !!extra.exteriorSpill,
});

// These palettes are deliberately authored as room architecture rather than
// furniture tint.  The rendering pass uses them for full wall faces, lower-wall
// construction, trim, floors, light fixtures, and room-specific pattern work.
export const ROOM_VISUAL_IDENTITIES = Object.freeze({
  hallway: identity(0x726b54, 0x625742, 0x3a2b21, 0x59463b, 0x9b805d, "aged-olive-wallpaper", "gallery runner and console", { color: 0xffd29a, intensity: 0.34 }, { ambience: "old-house-clock" }),
  study: identity(0x29443a, 0x392c22, 0x241c17, 0x3d3028, 0xb8894f, "green-library-paneling", "walnut book wall and writing desk", { color: 0xffbd72, intensity: 0.72, point: true }, { crown: true, lowerHeight: 1.02, ambience: "paper-and-clock" }),
  pantry: identity(0xb6a978, 0x8b7146, 0x60462f, 0x78674b, 0xc49a49, "mustard-plaster-and-shelves", "towering wall of food", { color: 0xffcf86, intensity: 0.56, point: true }, { ambience: "paper-rustle" }),
  dining: identity(0xb7a18a, 0x642e35, 0x3d251d, 0x4b3128, 0x9f6b54, "burgundy-wainscot", "formal table under pendant", { color: 0xffc278, intensity: 0.64, point: true }, { crown: true, lowerHeight: 0.96, ambience: "room-tone" }),
  bathroom: identity(0x87a7a1, 0xb8c7c3, 0x627978, 0x718b8c, 0xd6e0d8, "seafoam-tile-and-pipe", "enormous claw-foot bathtub", { color: 0xcdeeff, intensity: 0.58, point: true }, { lowerHeight: 1.2, ambience: "water-drip" }),
  bedroom: identity(0x765665, 0x947780, 0x44312d, 0x67535b, 0xc29b79, "dusty-plum-plaster", "sheltering bed and hanging quilt", { color: 0xffc487, intensity: 0.48, point: true }, { crown: true, ambience: "soft-room-tone" }),
  children: identity(0x718892, 0xb08a60, 0x5d4635, 0x75604f, 0xd6a75a, "storybook-star-wallpaper", "blanket fort and toy zone", { color: 0xffd48c, intensity: 0.58, point: true }, { ambience: "toy-room-creaks" }),
  laundry: identity(0x8ca6a0, 0xd0c58f, 0x66736d, 0x7d8f8b, 0xe7d982, "aqua-utility-plaster", "washer and dryer machine forest", { color: 0xe7f3db, intensity: 0.52 }, { ambience: "appliance-hum" }),
  utility: identity(0x68736b, 0x4f5954, 0x343b39, 0x565d5c, 0x9b6f46, "exposed-concrete-and-conduit", "furnace and copper pipe maze", { color: 0xd8d2ad, intensity: 0.42 }, { ambience: "furnace-hum" }),
  mudroom: identity(0x526750, 0x7c7159, 0x3f352a, 0x5d625b, 0x9a7c50, "forest-painted-masonry", "coat bench and boot wall", { color: 0xc9e4ec, intensity: 0.5 }, { ambience: "outside-wind", exteriorSpill: true }),
  "basement-access": identity(0x776b58, 0x443a31, 0x2f2822, 0x50433a, 0x9b7d4f, "cracked-plaster-transition", "descending stair and lone bulb", { color: 0xe1b36d, intensity: 0.32, point: true }, { ambience: "stair-creak" }),
  basement: identity(0x4a514f, 0x3e4745, 0x2f3433, 0x3c4444, 0x80684e, "damp-foundation-masonry", "boiler and shadowed workbench", { color: 0xd8ae6d, intensity: 0.28, point: true }, { lowerHeight: 1.32, ambience: "pipe-knock" }),
  garage: identity(0x59646d, 0x40484e, 0x292f33, 0x4a5155, 0x9b493f, "panelled-garage-industrial", "garage door and tool wall", { color: 0xc9def2, intensity: 0.5 }, { ambience: "garage-rumble", exteriorSpill: true }),
});

export function validateRoomIdentities(identities = ROOM_VISUAL_IDENTITIES) {
  const sideRooms = ROOM_LAYOUTS.filter((room) => !["living", "kitchen"].includes(room.id));
  const failures = [];
  const landmarkNames = new Set();
  const colorSignatures = new Set();
  for (const room of sideRooms) {
    const visual = identities[room.id];
    if (!visual) {
      failures.push({ room: room.id, reason: "missing-identity" });
      continue;
    }
    const signature = `${visual.wall}:${visual.lowerWall}:${visual.floor}`;
    if (colorSignatures.has(signature)) failures.push({ room: room.id, reason: "reused-room-palette" });
    colorSignatures.add(signature);
    if (!visual.treatment || !visual.landmark) failures.push({ room: room.id, reason: "weak-identity" });
    if (landmarkNames.has(visual.landmark)) failures.push({ room: room.id, reason: "reused-landmark" });
    landmarkNames.add(visual.landmark);
  }
  return failures;
}

export const BACKROOM_DECORATIONS = Object.freeze([
  // Hallway / study wing: wall-biased pieces keep the central running lane open.
  D("shelf", "hall-wall-shelf", "hallway", -4.4, -7.45, 0.46, 1.32, 1.58, 0x564235),
  D("chair", "hall-reading-chair", "hallway", -4.34, -10.2, 0.58, 0.58, 0.92, 0x6d5748),
  D("lamp", "hall-reading-lamp", "hallway", -4.34, -9.72, 0.18, 0.18, 1.12, 0xb28a59, { collide: false }),
  D("wall-art", "hall-small-print", "hallway", -4.56, -7.02, 0.04, 0.58, 0.48, 0x8d765f, { collide: false }),
  D("wall-art", "hall-family-gallery-a", "hallway", -4.56, -8.05, 0.04, 0.42, 0.36, 0x8a735a, { collide: false }),
  D("wall-art", "hall-family-gallery-b", "hallway", -4.56, -8.62, 0.04, 0.34, 0.46, 0x6f5949, { collide: false }),
  D("wall-art", "hall-low-vent", "hallway", -4.56, -10.82, 0.035, 0.42, 0.22, 0x6f716b, { collide: false }),
  D("rug", "hall-runner", "hallway", -1.1, -9.1, 2.55, 0.74, 0.018, 0x704c42),
  D("shelf", "study-bookcase", "study", -10.16, -8.7, 0.46, 1.78, 1.82, 0x4f3728),
  D("shelf", "study-east-book-wall", "study", -5.27, -7.6, 0.42, 1.2, 2.12, 0x3f2b20, { landmark: true }),
  D("table", "study-writing-desk", "study", -9.55, -10.34, 1.1, 0.52, 0.78, 0x4a3023, { catOnly: true, landmark: true }),
  D("prop", "study-book-piles", "study", -9.55, -10.1, 0.62, 0.28, 0.22, 0x795740, { collide: false }),
  D("chair", "study-side-chair", "study", -9.36, -6.92, 0.62, 0.62, 0.94, 0x665044),
  D("stack", "study-storage-stack", "study", -9.68, -10.08, 0.72, 0.6, 0.58, 0x786047),
  D("lamp", "study-reading-lamp", "study", -8.92, -6.86, 0.16, 0.16, 1.08, 0xa98555, { collide: false }),

  // Pantry / dining: more household density without touching the kitchen proper.
  D("shelf", "pantry-back-rack", "pantry", 3.0, -10.95, 1.25, 0.46, 1.72, 0x5a422d),
  D("shelf", "pantry-food-wall", "pantry", 4.42, -11.48, 1.5, 0.34, 2.05, 0x684b2f, { landmark: true }),
  D("shelf", "pantry-east-shelf", "pantry", 6.82, -7.0, 0.36, 0.88, 1.82, 0x62472f),
  D("stack", "pantry-dry-goods", "pantry", 5.25, -10.95, 0.82, 0.55, 0.68, 0x8b704e),
  D("food-wall", "pantry-cereal-city", "pantry", 4.36, -11.18, 1.28, 0.38, 1.36, 0xc69b58, { collide: false, landmark: true }),
  D("prop", "pantry-jars-and-cans", "pantry", 2.95, -10.78, 0.92, 0.28, 0.3, 0xa68a62, { collide: false }),
  D("stack", "pantry-paper-bags", "pantry", 6.45, -7.15, 0.58, 0.48, 0.62, 0x9b805a),
  D("prop", "pantry-small-containers", "pantry", 2.6, -7.05, 0.72, 0.22, 0.22, 0x9a744d, { collide: false }),
  D("table", "dining-table", "dining", 1.68, 8.75, 1.42, 2.12, 0.78, 0x5b3b2e, { catOnly: true }),
  D("chair", "dining-chair-north", "dining", 1.68, 7.34, 0.6, 0.6, 0.9, 0x53392d),
  D("chair", "dining-chair-south", "dining", 1.68, 10.16, 0.6, 0.6, 0.9, 0x53392d),
  D("chair", "dining-chair-west", "dining", 0.78, 8.75, 0.52, 0.58, 0.88, 0x53392d),
  D("sideboard", "dining-tunnel-sideboard", "dining", 5.98, 9.38, 0.72, 1.5, 1.02, 0x4b3025, { catOnly: true, tunnelFurniture: true, landmark: true }),
  D("hanging-light", "dining-pendant", "dining", 1.68, 8.75, 0.62, 0.62, 2.48, 0xc18d55, { collide: false, landmark: true }),
  D("rug", "dining-rug", "dining", 3.2, 9.0, 3.0, 1.55, 0.018, 0x6f4c43),
  D("prop", "dining-dropped-food", "dining", 5.68, 7.05, 0.38, 0.26, 0.06, 0xb28b52, { collide: false }),

  // Bathroom / bedroom.
  D("shelf", "bath-linen-shelf", "bathroom", -4.34, -12.76, 0.42, 0.96, 1.52, 0x718486),
  D("tub", "bath-claw-foot-tub", "bathroom", -1.72, -14.35, 0.68, 2.22, 0.76, 0xc9d8d4, { catOnly: true, tunnelFurniture: true, shelter: true, landmark: true }),
  D("toilet", "bath-toilet", "bathroom", -4.25, -13.35, 0.56, 0.72, 0.86, 0xd7dfda, { collide: true }),
  D("vanity", "bath-vanity", "bathroom", -4.25, -15.58, 0.62, 0.52, 0.82, 0x5d7774, { collide: true, shelter: true }),
  D("pipe", "bath-exposed-plumbing", "bathroom", -4.47, -15.58, 0.08, 0.44, 0.7, 0x9d795b, { collide: false }),
  D("basket", "bath-hamper", "bathroom", -4.24, -15.34, 0.55, 0.55, 0.62, 0x9a8464),
  D("mat", "bath-soft-mat", "bathroom", -2.18, -15.32, 0.72, 0.92, 0.014, 0x6f8588, { collide: false }),
  D("prop", "bath-small-bottles", "bathroom", -4.2, -13.35, 0.36, 0.16, 0.18, 0xa7b5ae, { collide: false }),
  D("box", "bedroom-nightstand", "bedroom", -0.86, -15.55, 0.56, 0.5, 0.62, 0x554035, { collide: true }),
  D("shelf", "bedroom-wardrobe", "bedroom", -0.95, -14.45, 0.58, 0.9, 1.72, 0x513b37),
  D("bed", "bedroom-shelter-bed", "bedroom", 1.42, -14.38, 0.9, 2.32, 0.82, 0x69505a, { catOnly: true, tunnelFurniture: true, shelter: true, landmark: true }),
  D("lamp", "bedroom-bedside-lamp", "bedroom", -0.86, -15.55, 0.16, 0.16, 1.02, 0xd2a06c, { collide: false }),
  D("fabric", "bedroom-floor-laundry", "bedroom", -0.76, -13.25, 0.52, 0.34, 0.16, 0x897079, { collide: false }),
  D("rug", "bedroom-rug", "bedroom", 0.35, -14.45, 1.45, 1.28, 0.018, 0x80646f),
  D("prop", "bedroom-shoes", "bedroom", 1.62, -15.7, 0.38, 0.28, 0.16, 0x3f342e, { collide: false }),

  // Children's room: the entry stays deliberately open. Storage is moved to the side/back walls.
  D("basket", "children-side-storage-bin", "children", -9.45, -11.65, 0.72, 0.58, 0.48, 0x8c6945),
  D("shelf", "children-bookcase", "children", -9.72, -14.55, 0.48, 1.48, 1.35, 0x76533b),
  D("bed", "children-small-bed", "children", -5.55, -14.25, 0.72, 1.5, 0.66, 0x6f8190, { catOnly: true, tunnelFurniture: true, shelter: true }),
  D("blanket-fort", "children-blanket-fort", "children", -9.2, -12.72, 1.08, 1.08, 0.72, 0xb9855c, { catOnly: true, shelter: true, landmark: true }),
  D("toy-chest", "children-toy-chest", "children", -5.55, -11.35, 0.62, 0.56, 0.54, 0x88543f, { collide: true }),
  D("stuffed", "children-stuffed-bear", "children", -9.18, -15.08, 0.42, 0.36, 0.52, 0xaa7958, { collide: false }),
  D("chair", "children-small-chair", "children", -6.15, -14.45, 0.52, 0.52, 0.72, 0x7a5b48),
  D("rug", "children-play-rug", "children", -7.62, -13.15, 2.25, 1.45, 0.018, 0x8d6650),
  D("stack", "children-toy-blocks", "children", -6.25, -12.55, 0.6, 0.55, 0.42, 0xa37a4d),
  D("prop", "children-scattered-toys", "children", -9.3, -15.12, 0.72, 0.3, 0.18, 0xb17b4d, { collide: false, noisy: true }),
  D("box", "children-mouse-hide-box", "children", -5.56, -11.28, 0.58, 0.42, 0.3, 0x7f5d3f, { collide: false, shelter: true }),

  // Laundry / utility / mudroom.
  D("appliance", "laundry-washer", "laundry", 10.92, 2.24, 0.9, 0.7, 1.05, 0xd5d6d2, { collide: true }),
  D("appliance", "laundry-dryer", "laundry", 12.02, 2.24, 0.9, 0.7, 1.05, 0xc9cbc8, { collide: true }),
  D("fabric-rack", "laundry-hanging-clothes", "laundry", 12.42, 5.98, 1.35, 0.32, 1.62, 0x647978, { collide: false, landmark: true }),
  D("prop", "laundry-detergent-line", "laundry", 13.88, 5.52, 0.82, 0.22, 0.32, 0x7898a0, { collide: false }),
  D("prop", "laundry-mop-broom", "laundry", 14.72, 5.72, 0.24, 0.24, 1.42, 0x745d45, { collide: false }),
  D("basket", "laundry-hamper", "laundry", 11.25, 5.55, 0.7, 0.62, 0.72, 0x88745e),
  D("shelf", "laundry-supply-rack", "laundry", 13.9, 5.72, 1.45, 0.46, 1.58, 0x5c5148),
  D("chair", "laundry-stool", "laundry", 14.42, 2.35, 0.5, 0.5, 0.68, 0x615144),
  D("shelf", "utility-north-rack", "utility", 16.25, 2.25, 1.52, 0.44, 1.76, 0x50585a),
  D("shelf", "utility-south-rack", "utility", 17.55, 5.75, 1.3, 0.44, 1.7, 0x51595b),
  D("water-heater", "utility-water-heater", "utility", 17.45, 2.35, 0.68, 0.68, 1.78, 0x687475, { collide: true, landmark: true }),
  D("furnace", "utility-furnace", "utility", 17.58, 5.72, 1.02, 0.42, 1.42, 0x424a4b, { collide: true, landmark: true }),
  D("wall-art", "utility-electrical-box", "utility", 18.62, 5.55, 0.04, 0.58, 0.72, 0x555d5e, { collide: false }),
  D("stack", "utility-paint-cans", "utility", 16.1, 5.45, 0.62, 0.52, 0.48, 0x72787a),
  D("pipe", "utility-wall-pipes", "utility", 18.48, 5.76, 0.12, 0.3, 1.55, 0x6d7375, { collide: false }),
  D("prop", "utility-tools", "utility", 15.52, 2.22, 0.62, 0.18, 0.24, 0x4a4f50, { collide: false }),
  D("shelf", "mudroom-boot-rack", "mudroom", 1.25, 13.65, 1.18, 0.42, 0.82, 0x55463a),
  D("bench", "mudroom-coat-bench", "mudroom", 1.05, 12.38, 0.82, 0.5, 0.78, 0x4b3b30, { collide: true, landmark: true }),
  D("fabric-rack", "mudroom-hanging-coats", "mudroom", 5.88, 13.72, 1.05, 0.24, 1.72, 0x4e5e4c, { collide: false }),
  D("prop", "mudroom-umbrella", "mudroom", 6.45, 12.55, 0.22, 0.22, 1.12, 0x4b5557, { collide: false }),
  D("basket", "mudroom-basket", "mudroom", 4.8, 13.7, 0.64, 0.54, 0.52, 0x856f54),
  D("mat", "mudroom-floor-mat", "mudroom", 3.62, 13.25, 1.22, 0.72, 0.018, 0x4f5149, { collide: false }),
  D("prop", "mudroom-boots", "mudroom", 1.22, 12.65, 0.62, 0.34, 0.28, 0x3d342d, { collide: false }),

  // Basement access gets storage along the walls, not the stair/door line.
  D("shelf", "basement-access-east-rack", "basement-access", 11.82, -7.55, 0.42, 1.35, 1.72, 0x403831),
  D("shelf", "basement-access-supply-shelf", "basement-access", 9.6, -6.82, 1.45, 0.42, 1.48, 0x4c4034),
  D("stack", "basement-access-boxes", "basement-access", 8.12, -7.08, 0.72, 0.62, 0.62, 0x69533b),
  D("chair", "basement-access-folding-chair", "basement-access", 11.55, -10.35, 0.55, 0.55, 0.82, 0x4b4741),
  D("wall-art", "basement-access-coat-hooks", "basement-access", 12.24, -7.2, 0.04, 0.82, 0.38, 0x4a4037, { collide: false }),
  D("hanging-light", "basement-access-bare-bulb", "basement-access", 11.25, -9.9, 0.16, 0.16, 2.25, 0xc59857, { collide: false, landmark: true }),
  D("pipe", "basement-access-conduit", "basement-access", 12.22, -9.1, 0.06, 1.25, 1.62, 0x59605e, { collide: false }),

  // Basement: denser storage/work area while preserving the center route from the stairs.
  D("shelf", "basement-west-storage", "basement", 7.65, -13.45, 0.46, 1.85, 1.82, 0x45433e),
  D("shelf", "basement-east-storage", "basement", 11.95, -15.55, 0.46, 1.65, 1.82, 0x45433e),
  D("shelf", "basement-back-storage", "basement", 11.22, -16.35, 1.34, 0.42, 1.7, 0x4e4840),
  D("chair", "basement-work-chair", "basement", 10.85, -13.9, 0.58, 0.58, 0.88, 0x4f4b45),
  D("stack", "basement-storage-totes", "basement", 8.45, -16.05, 0.86, 0.62, 0.72, 0x5f6260),
  D("table", "basement-workbench", "basement", 11.58, -12.45, 1.18, 0.56, 0.82, 0x47413a, { catOnly: true }),
  D("water-heater", "basement-old-boiler", "basement", 11.88, -14.35, 0.78, 0.78, 1.72, 0x4f5653, { collide: true, landmark: true }),
  D("stack", "basement-old-furniture", "basement", 7.82, -15.82, 0.72, 0.72, 1.02, 0x51463d),
  D("wall-art", "basement-foundation-cracks", "basement", 7.28, -14.5, 0.035, 1.02, 0.68, 0x2c3433, { collide: false }),
  D("pipe", "basement-low-pipes", "basement", 7.38, -12.78, 0.12, 1.02, 1.4, 0x62686a, { collide: false }),

  // Garage: practical clutter concentrated around outer walls.
  D("shelf", "garage-tool-rack", "garage", 23.9, 2.15, 0.46, 2.15, 1.88, 0x474b4d),
  D("shelf", "garage-storage-rack", "garage", 21.65, 8.0, 2.0, 0.44, 1.82, 0x4a4d4f),
  D("work-cart", "garage-parked-mower", "garage", 20.55, 1.18, 1.05, 1.55, 0.82, 0x39494c, { catOnly: true, landmark: true }),
  D("table", "garage-workbench", "garage", 23.92, 3.55, 0.46, 1.35, 0.88, 0x4f443a, { catOnly: true }),
  D("garage-door", "garage-panel-door", "garage", 22.15, -0.38, 3.65, 0.035, 2.35, 0x66737c, { collide: false, landmark: true }),
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
    const cosmetic = ["rug", "mat", "wall-art", "hanging-light", "fabric", "pipe", "food-wall", "fabric-rack", "garage-door", "stuffed"].includes(spec.kind) || spec.h <= 0.025;
    for (const failure of validateCirculationPlacement(spec, { cosmetic, includeSecretEntrances: !spec.tunnelFurniture })) {
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

  const roomGroups = new Map();
  const roomGroup = (roomId) => {
    let group = roomGroups.get(roomId);
    if (group) return group;
    group = new I.Group();
    group.name = `hearthmouse-room-identity-${roomId}`;
    group.userData.roomId = roomId;
    group.userData.unlockNight = ROOM_LAYOUTS.find((room) => room.id === roomId)?.unlockNight ?? 1;
    root.add(group);
    roomGroups.set(roomId, group);
    return group;
  };

  const materials = new Map();
  const geometries = new Map();
  let activePartParent = null;
  const material = (color, roughness = 0.9, metalness = 0, emissive = 0x000000, emissiveIntensity = 0) => {
    const key = `${color}:${roughness}:${metalness}:${emissive}:${emissiveIntensity}`;
    let value = materials.get(key);
    if (!value) {
      value = new I.MeshStandardMaterial({ color, roughness, metalness, emissive, emissiveIntensity });
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

  const addPart = (name, x, y, z, w, h, d, color, {
    occlude = false,
    parent = activePartParent ?? root,
    roughness = 0.9,
    metalness = 0,
    emissive = 0x000000,
    emissiveIntensity = 0,
    rotationY = 0,
  } = {}) => {
    const mesh = new I.Mesh(geometry(w, h, d), material(color, roughness, metalness, emissive, emissiveIntensity));
    mesh.name = name;
    mesh.position.set(x, y, z);
    mesh.rotation.y = rotationY;
    mesh.castShadow = false;
    mesh.receiveShadow = true;
    parent.add(mesh);
    freezeStatic(mesh);
    if (occlude) addOccluder(mesh, x, z, w, d);
    return mesh;
  };

  const subtractIntervals = (minimum, maximum, cuts) => {
    let segments = [[minimum, maximum]];
    for (const [cutMin, cutMax] of cuts) {
      const next = [];
      for (const [start, end] of segments) {
        if (cutMax <= start || cutMin >= end) next.push([start, end]);
        else {
          if (cutMin > start + 0.04) next.push([start, Math.min(end, cutMin)]);
          if (cutMax < end - 0.04) next.push([Math.max(start, cutMax), end]);
        }
      }
      segments = next;
    }
    return segments.filter(([start, end]) => end - start >= 0.08);
  };

  const wallSegments = (room, side, skipEntrances = false) => {
    const horizontal = side === "north" || side === "south";
    const minimum = horizontal ? room.minX + 0.035 : room.minZ + 0.035;
    const maximum = horizontal ? room.maxX - 0.035 : room.maxZ - 0.035;
    const cuts = [];
    for (const doorway of DOORWAY_CORRIDORS) {
      if (!doorway.rooms.includes(room.id)) continue;
      if (horizontal !== (doorway.travelAxis === "z")) continue;
      const center = horizontal ? doorway.x : doorway.z;
      cuts.push([center - doorway.width / 2 - 0.09, center + doorway.width / 2 + 0.09]);
    }
    if (skipEntrances) {
      const boundary = horizontal ? (side === "north" ? room.minZ : room.maxZ) : (side === "west" ? room.minX : room.maxX);
      for (const entrance of SECRET_ROUTE_ENTRANCES) {
        if (entrance.roomId !== room.id) continue;
        const distance = horizontal ? Math.abs(entrance.z - boundary) : Math.abs(entrance.x - boundary);
        if (distance > 0.42) continue;
        const center = horizontal ? entrance.x : entrance.z;
        const half = Math.max(entrance.w, entrance.d) / 2 + 0.1;
        cuts.push([center - half, center + half]);
      }
    }
    return subtractIntervals(minimum, maximum, cuts);
  };

  const addWallBand = (room, side, y, height, color, { depth = 0.026, skipEntrances = false, suffix = "wall", emissive = 0, emissiveIntensity = 0 } = {}) => {
    const horizontal = side === "north" || side === "south";
    const boundary = horizontal
      ? (side === "north" ? room.minZ + depth / 2 + 0.006 : room.maxZ - depth / 2 - 0.006)
      : (side === "west" ? room.minX + depth / 2 + 0.006 : room.maxX - depth / 2 - 0.006);
    const parent = roomGroup(room.id);
    for (const [start, end] of wallSegments(room, side, skipEntrances)) {
      const center = (start + end) / 2;
      addPart(
        `${room.id}-${side}-${suffix}-${start.toFixed(2)}`,
        horizontal ? center : boundary,
        y,
        horizontal ? boundary : center,
        horizontal ? end - start : depth,
        height,
        horizontal ? depth : end - start,
        color,
        { parent, emissive, emissiveIntensity },
      );
    }
  };

  const addRoomPattern = (room, visual) => {
    const parent = roomGroup(room.id);
    const centerX = (room.minX + room.maxX) / 2;
    const centerZ = (room.minZ + room.maxZ) / 2;
    const width = room.maxX - room.minX;
    const depth = room.maxZ - room.minZ;
    if (/tile|masonry|concrete|garage/.test(visual.treatment)) {
      for (const fraction of [0.28, 0.55, 0.82]) {
        const y = visual.lowerHeight * fraction;
        for (const side of ["north", "south", "west", "east"]) {
          addWallBand(room, side, y, 0.014, visual.trim, { depth: 0.034, skipEntrances: true, suffix: `mortar-${fraction}` });
        }
      }
    } else if (/wallpaper|storybook/.test(visual.treatment)) {
      const count = Math.max(4, Math.min(9, Math.floor(width / 0.62)));
      for (let index = 1; index < count; index++) {
        const x = room.minX + width * index / count;
        if (DOORWAY_CORRIDORS.some((doorway) => doorway.rooms.includes(room.id) && placementIntersectsDoorway({ x, z: room.minZ + 0.025, w: 0.025, d: 0.04 }, doorway, 0.02))) continue;
        addPart(`${room.id}-wallpaper-seam-${index}`, x, 1.54, room.minZ + 0.018, 0.018, 2.05, 0.018, visual.accent, { parent, roughness: 1 });
      }
    } else if (/panel|library|wainscot/.test(visual.treatment)) {
      for (const side of ["north", "south", "west", "east"]) {
        addWallBand(room, side, visual.lowerHeight, 0.055, visual.accent, { depth: 0.038, skipEntrances: true, suffix: "chair-rail" });
      }
    }

    if (room.surface === "wood" || /library|plum|plaster/.test(visual.treatment)) {
      const boardCount = Math.max(4, Math.min(9, Math.floor(width / 0.55)));
      for (let index = 1; index < boardCount; index++) {
        const x = room.minX + width * index / boardCount;
        addPart(`${room.id}-floor-seam-${index}`, x, 0.009, centerZ, 0.012, 0.004, depth - 0.12, visual.trim, { parent, roughness: 1 });
      }
    } else if (room.surface === "tile" || room.surface === "metal") {
      for (const fraction of [0.25, 0.5, 0.75]) {
        addPart(`${room.id}-floor-grid-x-${fraction}`, room.minX + width * fraction, 0.009, centerZ, 0.01, 0.004, depth - 0.12, visual.trim, { parent, roughness: 1 });
        addPart(`${room.id}-floor-grid-z-${fraction}`, centerX, 0.009, room.minZ + depth * fraction, width - 0.12, 0.004, 0.01, visual.trim, { parent, roughness: 1 });
      }
    }
  };

  const addRoomArchitecture = (room, visual) => {
    const parent = roomGroup(room.id);
    const centerX = (room.minX + room.maxX) / 2;
    const centerZ = (room.minZ + room.maxZ) / 2;
    addPart(`${room.id}-identity-floor`, centerX, 0.004, centerZ, room.maxX - room.minX - 0.05, 0.008, room.maxZ - room.minZ - 0.05, visual.floor, { parent, roughness: 0.98 });
    for (const side of ["north", "south", "west", "east"]) {
      addWallBand(room, side, 1.42, 2.72, visual.wall, { suffix: "upper-wall" });
      addWallBand(room, side, visual.lowerHeight / 2, visual.lowerHeight, visual.lowerWall, { depth: 0.034, skipEntrances: true, suffix: "lower-wall" });
      addWallBand(room, side, 0.085, 0.15, visual.trim, { depth: 0.045, skipEntrances: true, suffix: "baseboard" });
      if (visual.crown) addWallBand(room, side, 2.7, 0.095, visual.trim, { depth: 0.045, suffix: "crown" });
    }
    addRoomPattern(room, visual);
    addPart(`${room.id}-light-fixture`, centerX, 2.68, centerZ, 0.28, 0.055, 0.28, visual.accent, {
      parent,
      emissive: visual.light.color,
      emissiveIntensity: Math.max(0.28, visual.light.intensity),
      roughness: 0.55,
    });
    if (visual.exteriorSpill) {
      addPart(`${room.id}-cool-exterior-spill`, room.maxX - 0.2, 1.5, room.maxZ - 0.03, 0.58, 1.36, 0.025, 0xbad7df, {
        parent,
        emissive: 0x7899a8,
        emissiveIntensity: 0.48,
      });
    }
    if (visual.light.point && I.PointLight) {
      const light = new I.PointLight(visual.light.color, visual.light.intensity, Math.min(5.4, Math.max(room.maxX - room.minX, room.maxZ - room.minZ) * 0.82), 2);
      light.name = `${room.id}-localized-room-light`;
      light.position.set(centerX, visual.light.y, centerZ);
      light.castShadow = false;
      parent.add(light);
    }
  };

  for (const room of ROOM_LAYOUTS) {
    const visual = ROOM_VISUAL_IDENTITIES[room.id];
    if (visual) addRoomArchitecture(room, visual);
  }

  const addCollider = (spec, height = spec.h) => {
    const unlockNight = ROOM_LAYOUTS.find((room) => room.id === spec.room)?.unlockNight ?? 1;
    const collider = {
      minX: spec.x - spec.w / 2,
      maxX: spec.x + spec.w / 2,
      minZ: spec.z - spec.d / 2,
      maxZ: spec.z + spec.d / 2,
      minY: 0,
      maxY: height,
      name: `${spec.name}-collision`,
      catOnly: !!spec.catOnly,
      active: unlockNight <= (engine.snapshot?.night ?? 1),
      roomId: spec.room,
      unlockNight,
      __backroomDecor: true,
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

  const addBed = (spec) => {
    const frameY = 0.28;
    addPart(`${spec.name}-frame`, spec.x, frameY, spec.z, spec.w, 0.16, spec.d, 0x49342d, { occlude: true });
    addPart(`${spec.name}-mattress`, spec.x, frameY + 0.16, spec.z, spec.w * 0.96, 0.22, spec.d * 0.96, spec.color, { occlude: true });
    addPart(`${spec.name}-quilt`, spec.x, frameY + 0.285, spec.z + spec.d * 0.08, spec.w * 0.99, 0.045, spec.d * 0.74, 0x9f7a78);
    addPart(`${spec.name}-headboard`, spec.x, spec.h * 0.55, spec.z - spec.d * 0.47, spec.w, spec.h, 0.09, 0x4b352e, { occlude: true });
    for (const side of [-1, 1]) {
      addPart(`${spec.name}-skirt-${side}`, spec.x + side * spec.w * 0.46, 0.2, spec.z + spec.d * 0.08, 0.035, 0.26, spec.d * 0.72, spec.color);
    }
    addCollider({ ...spec, catOnly: true }, Math.max(0.58, spec.h));
  };

  const addTub = (spec) => {
    const rim = 0.095;
    const wallBottom = 0.16;
    const wallHeight = spec.h - wallBottom;
    const wallY = wallBottom + wallHeight / 2;
    addPart(`${spec.name}-floor`, spec.x, 0.19, spec.z, spec.w * 0.82, 0.08, spec.d * 0.82, 0xaebfbc);
    addPart(`${spec.name}-west-rim`, spec.x - spec.w / 2 + rim / 2, wallY, spec.z, rim, wallHeight, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-east-rim`, spec.x + spec.w / 2 - rim / 2, wallY, spec.z, rim, wallHeight, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-north-rim`, spec.x, wallY, spec.z - spec.d / 2 + rim / 2, spec.w, wallHeight, rim, spec.color, { occlude: true });
    addPart(`${spec.name}-south-rim`, spec.x, wallY, spec.z + spec.d / 2 - rim / 2, spec.w, wallHeight, rim, spec.color, { occlude: true });
    addPart(`${spec.name}-water-shadow`, spec.x, spec.h * 0.54, spec.z, spec.w * 0.66, 0.025, spec.d * 0.76, 0x577a7c, { roughness: 0.28, metalness: 0.05 });
    for (const sz of [-1, 1]) for (const sx of [-1, 1]) {
      addPart(`${spec.name}-foot-${sx}-${sz}`, spec.x + sx * spec.w * 0.34, 0.08, spec.z + sz * spec.d * 0.36, 0.1, 0.16, 0.1, 0x766852, { metalness: 0.42 });
    }
    addCollider({ ...spec, catOnly: true });
  };

  const addToilet = (spec) => {
    addPart(`${spec.name}-base`, spec.x, 0.24, spec.z + spec.d * 0.12, spec.w * 0.58, 0.48, spec.d * 0.62, spec.color, { occlude: true });
    addPart(`${spec.name}-bowl`, spec.x, 0.52, spec.z + spec.d * 0.08, spec.w, 0.22, spec.d * 0.78, spec.color, { occlude: true });
    addPart(`${spec.name}-tank`, spec.x, spec.h * 0.68, spec.z - spec.d * 0.32, spec.w * 0.82, spec.h * 0.58, spec.d * 0.32, spec.color, { occlude: true });
    addCollider(spec);
  };

  const addVanity = (spec) => {
    const cabinetBottom = 0.22;
    const cabinetHeight = spec.h - cabinetBottom;
    addPart(`${spec.name}-cabinet`, spec.x, cabinetBottom + cabinetHeight / 2, spec.z, spec.w, cabinetHeight, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-sink`, spec.x, spec.h * 0.9, spec.z, spec.w * 1.04, 0.1, spec.d * 1.02, 0xc6d2ce, { occlude: true });
    addPart(`${spec.name}-pipe`, spec.x, 0.22, spec.z + spec.d * 0.54, 0.055, 0.42, 0.055, 0x8a735b, { metalness: 0.55 });
    addCollider({ ...spec, catOnly: true });
  };

  const addSideboard = (spec) => {
    const legH = 0.24;
    addPart(`${spec.name}-body`, spec.x, legH + (spec.h - legH) / 2, spec.z, spec.w, spec.h - legH, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-top`, spec.x, spec.h + 0.035, spec.z, spec.w * 1.05, 0.07, spec.d * 1.03, 0x3d251d, { occlude: true });
    for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
      addPart(`${spec.name}-leg-${sx}-${sz}`, spec.x + sx * spec.w * 0.38, legH / 2, spec.z + sz * spec.d * 0.38, 0.075, legH, 0.075, spec.color);
    }
    addCollider({ ...spec, catOnly: true });
  };

  const addHangingLight = (spec) => {
    const shadeY = Math.min(2.42, Math.max(1.72, spec.h - 0.28));
    addPart(`${spec.name}-cord`, spec.x, (2.76 + shadeY) / 2, spec.z, 0.025, Math.max(0.12, 2.76 - shadeY), 0.025, 0x302923);
    addPart(`${spec.name}-shade`, spec.x, shadeY, spec.z, spec.w, 0.24, spec.d, spec.color, { emissive: 0x8a541f, emissiveIntensity: 0.32 });
    addPart(`${spec.name}-glow`, spec.x, shadeY - 0.12, spec.z, spec.w * 0.58, 0.035, spec.d * 0.58, 0xffd18a, { emissive: 0xffb65a, emissiveIntensity: 1.05 });
  };

  const addFoodWall = (spec) => {
    const colors = [0xb45f42, 0xd2a452, 0x648073, 0x8b6a9d, 0xc9b06b];
    for (let row = 0; row < 3; row++) for (let column = 0; column < 5; column++) {
      const w = spec.w / 5.7;
      const h = spec.h / 3.45;
      addPart(
        `${spec.name}-${row}-${column}`,
        spec.x - spec.w * 0.39 + column * spec.w * 0.195,
        0.31 + row * h * 1.05,
        spec.z,
        w,
        h,
        spec.d,
        colors[(row * 2 + column) % colors.length],
      );
    }
  };

  const addBlanketFort = (spec) => {
    for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
      addPart(`${spec.name}-support-${sx}-${sz}`, spec.x + sx * spec.w * 0.39, spec.h * 0.42, spec.z + sz * spec.d * 0.39, 0.055, spec.h * 0.84, 0.055, 0x655044);
    }
    addPart(`${spec.name}-blanket-a`, spec.x - spec.w * 0.22, spec.h * 0.78, spec.z, spec.w * 0.58, 0.055, spec.d, spec.color, { rotationY: 0.18, occlude: true });
    addPart(`${spec.name}-blanket-b`, spec.x + spec.w * 0.22, spec.h * 0.72, spec.z, spec.w * 0.58, 0.055, spec.d, 0x8b6f82, { rotationY: -0.18, occlude: true });
    addCollider({ ...spec, catOnly: true });
  };

  const addFabricRack = (spec) => {
    addPart(`${spec.name}-rail`, spec.x, spec.h * 0.9, spec.z, spec.w, 0.05, 0.05, 0x4c514f, { metalness: 0.38 });
    for (const side of [-1, 1]) addPart(`${spec.name}-post-${side}`, spec.x + side * spec.w * 0.45, spec.h / 2, spec.z, 0.05, spec.h, 0.05, 0x4c514f, { metalness: 0.38 });
    const colors = [spec.color, 0x766071, 0x9a805f, 0x506272];
    for (let index = 0; index < 4; index++) {
      addPart(`${spec.name}-fabric-${index}`, spec.x - spec.w * 0.34 + index * spec.w * 0.225, spec.h * 0.58, spec.z, spec.w * 0.18, spec.h * 0.56, spec.d * 0.72, colors[index]);
    }
  };

  const addWaterHeater = (spec) => {
    addPart(`${spec.name}-tank`, spec.x, spec.h / 2, spec.z, spec.w, spec.h, spec.d, spec.color, { occlude: true, metalness: 0.32 });
    for (const y of [0.38, spec.h - 0.38]) addPart(`${spec.name}-band-${y}`, spec.x, y, spec.z + spec.d / 2 + 0.014, spec.w * 1.04, 0.07, 0.03, 0x353d3d, { metalness: 0.55 });
    addPart(`${spec.name}-pipe-a`, spec.x - spec.w * 0.18, spec.h + 0.2, spec.z, 0.055, 0.4, 0.055, 0x9b7652, { metalness: 0.56 });
    addPart(`${spec.name}-pipe-b`, spec.x + spec.w * 0.18, spec.h + 0.16, spec.z, 0.055, 0.32, 0.055, 0x747b78, { metalness: 0.56 });
    addCollider(spec);
  };

  const addFurnace = (spec) => {
    addPart(`${spec.name}-body`, spec.x, spec.h / 2, spec.z, spec.w, spec.h, spec.d, spec.color, { occlude: true, metalness: 0.28 });
    for (let index = -2; index <= 2; index++) {
      addPart(`${spec.name}-vent-${index}`, spec.x + index * spec.w * 0.13, spec.h * 0.62, spec.z + spec.d / 2 + 0.014, spec.w * 0.065, spec.h * 0.32, 0.025, 0x242a2a, { metalness: 0.35 });
    }
    addPart(`${spec.name}-duct`, spec.x, spec.h + 0.22, spec.z, spec.w * 0.62, 0.44, spec.d * 0.72, 0x606968, { metalness: 0.48 });
    addCollider(spec);
  };

  const addBench = (spec) => {
    addPart(`${spec.name}-seat`, spec.x, spec.h * 0.54, spec.z, spec.w, 0.12, spec.d, spec.color, { occlude: true });
    addPart(`${spec.name}-back`, spec.x, spec.h * 0.78, spec.z - spec.d * 0.43, spec.w, spec.h * 0.52, 0.08, spec.color, { occlude: true });
    for (const side of [-1, 1]) addPart(`${spec.name}-leg-${side}`, spec.x + side * spec.w * 0.38, spec.h * 0.27, spec.z, 0.09, spec.h * 0.54, 0.09, spec.color);
    addCollider(spec);
  };

  const addWorkCart = (spec) => {
    addPart(`${spec.name}-body`, spec.x, spec.h * 0.46, spec.z, spec.w, spec.h * 0.66, spec.d * 0.72, spec.color, { occlude: true, metalness: 0.2 });
    addPart(`${spec.name}-handle`, spec.x, spec.h * 0.92, spec.z + spec.d * 0.45, spec.w * 0.7, 0.07, 0.07, 0x343a3b, { metalness: 0.48 });
    for (const sx of [-1, 1]) for (const sz of [-1, 1]) addPart(`${spec.name}-wheel-${sx}-${sz}`, spec.x + sx * spec.w * 0.43, 0.17, spec.z + sz * spec.d * 0.32, 0.16, 0.34, 0.22, 0x232628, { occlude: true });
    addCollider({ ...spec, catOnly: true });
  };

  const addGarageDoor = (spec) => {
    const rows = 4;
    const columns = 5;
    for (let row = 0; row < rows; row++) for (let column = 0; column < columns; column++) {
      const panelW = spec.w / columns - 0.035;
      const panelH = spec.h / rows - 0.035;
      addPart(
        `${spec.name}-panel-${row}-${column}`,
        spec.x - spec.w / 2 + panelW / 2 + 0.02 + column * (spec.w / columns),
        panelH / 2 + 0.02 + row * (spec.h / rows),
        spec.z,
        panelW,
        panelH,
        spec.d,
        row % 2 ? spec.color : 0x59666f,
        { metalness: 0.22 },
      );
    }
  };

  for (const spec of BACKROOM_DECORATIONS) {
    activePartParent = roomGroup(spec.room);
    if (spec.kind === "shelf") addShelf(spec);
    else if (spec.kind === "chair") addChair(spec);
    else if (spec.kind === "stack") addStack(spec);
    else if (spec.kind === "basket") addBasket(spec);
    else if (spec.kind === "table") addTable(spec);
    else if (spec.kind === "appliance") addAppliance(spec);
    else if (spec.kind === "lamp") addLamp(spec);
    else if (spec.kind === "prop") addSmallProp(spec);
    else if (spec.kind === "tire") addTireStack(spec);
    else if (spec.kind === "bed") addBed(spec);
    else if (spec.kind === "tub") addTub(spec);
    else if (spec.kind === "toilet") addToilet(spec);
    else if (spec.kind === "vanity") addVanity(spec);
    else if (spec.kind === "sideboard") addSideboard(spec);
    else if (spec.kind === "hanging-light") addHangingLight(spec);
    else if (spec.kind === "food-wall") addFoodWall(spec);
    else if (spec.kind === "blanket-fort") addBlanketFort(spec);
    else if (spec.kind === "fabric-rack") addFabricRack(spec);
    else if (spec.kind === "water-heater") addWaterHeater(spec);
    else if (spec.kind === "furnace") addFurnace(spec);
    else if (spec.kind === "bench") addBench(spec);
    else if (spec.kind === "work-cart") addWorkCart(spec);
    else if (spec.kind === "garage-door") addGarageDoor(spec);
    else if (spec.kind === "toy-chest") addStack(spec);
    else if (spec.kind === "stuffed" || spec.kind === "fabric") addSmallProp(spec);
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
        unlockNight: ROOM_LAYOUTS.find((room) => room.id === spec.room)?.unlockNight ?? 1,
      });
    }
    activePartParent = null;
  }

  const syncRoomVisibility = (night = engine.snapshot?.night ?? 1) => {
    for (const group of roomGroups.values()) group.visible = (group.userData.unlockNight ?? 1) <= night;
    for (const collider of world.colliders ?? []) {
      if (!collider?.__backroomDecor) continue;
      collider.active = (collider.unlockNight ?? 1) <= night;
    }
  };
  syncRoomVisibility();
  if (!world.__roomIdentitySetNightWrapped && typeof world.setNight === "function") {
    const baseSetNight = world.setNight.bind(world);
    Object.defineProperty(world, "__roomIdentitySetNightWrapped", { value: true, configurable: true });
    world.setNight = (night) => {
      const result = baseSetNight(night);
      syncRoomVisibility(night);
      return result;
    };
  }
  root.userData.roomGroups = roomGroups;

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
