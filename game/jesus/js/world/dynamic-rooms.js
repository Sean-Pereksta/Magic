import { VIEW } from "../config.js";
import { normalizeBiome } from "./regions.js";

const FLOOR = VIEW.floorY;
const WIDTH = 3200;
const HALF_TURN = Math.PI / 2;

function room(id, name, region, difficulty, tags, routeDescription) {
  return {
    id, name, region, biomes: [region], difficulty, width: WIDTH,
    tags: ["authored", "dynamic-route", "movers", ...tags], routeDescription,
    requiresDynamic: true, authoredHazards: true, checkpointX: 100,
    finalArenaSafe: false, finalArenaFallback: "shore-rhythm",
    platforms: [], movers: [], hazards: [], enemySpawns: [], pickupSpawns: [], props: [], switches: [],
    validationWaypoints: [],
  };
}

function shelf(r, x, offsetY, w, extra = {}) {
  const platform = { x, y: FLOOR + offsetY, w, h: 28, kind: "platform", oneWay: true, ...extra };
  r.platforms.push(platform);
  return { surface: "platforms", index: r.platforms.length - 1 };
}
function ground(r, x, w, offsetY = 0, extra = {}) {
  return shelf(r, x, offsetY, w, { h: 500, kind: "ground", oneWay: false, ...extra });
}
function mover(r, baseX, offsetY, w, settings) {
  const p = {
    x: baseX, y: FLOOR + offsetY, baseX, baseY: FLOOR + offsetY,
    w, h: 28, kind: "moving", oneWay: true, route: true, required: true,
    range: 0, axis: "x", phase: 0, speed: 1, ...settings,
  };
  if (p.period) p.speed = Math.PI * 2 / p.period;
  if (p.motion !== "pivot" && p.motion !== "timed") {
    p.x = p.baseX + (p.axis === "x" ? Math.sin(p.phase) * p.range : 0);
    p.y = p.baseY + (p.axis === "y" ? Math.sin(p.phase) * p.range : 0);
  }
  r.movers.push(p);
  return { surface: "movers", index: r.movers.length - 1 };
}
function pivot(r, pivotX, offsetY, settings = {}) {
  const length = settings.length || 380;
  return mover(r, pivotX - length / 2, offsetY, length, {
    motion: "pivot", pivotX, pivotY: FLOOR + offsetY, length,
    baseAngle: 0, angleRange: 0.3, period: 7.4, phase: 0, visualKind: "bridge", ...settings,
  });
}
function recovering(r, x, offsetY, w, settings = {}) {
  return mover(r, x, offsetY, w, {
    motion: "timed", period: 10, warningDuration: 1.2, activeDuration: 7.2,
    retractDuration: 0.4, recoveryDuration: 1.2, phase: 0, recoverable: true, ...settings,
  });
}
function route(r, ...waypoints) {
  r.validationWaypoints = waypoints;
  for (const point of waypoints) r[point.surface][point.index].route = true;
  r.requiredMechanics = [...new Set(r.movers.map(p => p.motion))];
  return r;
}
function combat(r, point, settings = {}) {
  const p = r[point.surface][point.index];
  r.enemySpawns.push({ x: p.x + 82, y: p.y - 52, safeLanding: true, ...settings });
  r.pickupSpawns.push({ x: p.x + 32, y: p.y - 46, type: "bread" });
}
function reward(r, point, { offsetX = 85, rise = 115, type = "holyLight", wings = false } = {}) {
  const p = r[point.surface][point.index];
  const branch = shelf(r, p.x + offsetX, p.y - FLOOR - rise, 160, {
    branch: wings ? "wings" : "reward", secret: true, ...(wings ? { requires: "wings" } : {}),
  });
  const ledge = r.platforms[branch.index];
  r.pickupSpawns.push({ x: ledge.x + 66, y: ledge.y - 46, type, secret: true, ...(wings ? { requires: "wings" } : {}) });
}
function hazard(r, type, point, offsetX, settings = {}) {
  const p = r[point.surface][point.index];
  const x = p.x + offsetX;
  const h = settings.h || (type === "retractSpikes" ? 24 : type === "timedGate" ? 118 : 58);
  const item = {
    type, x, y: p.y - h, w: settings.w || 46, h,
    timed: true, period: 5.8, warningDuration: 1.2, activeDuration: 1.0,
    phase: 0, warning: false, active: false, authored: true,
    safeWaitX: p.x + 28, ...settings,
  };
  if (type === "pendulum") Object.assign(item, {
    pivotX: x + item.w / 2, pivotY: p.y - 205, length: 145, angleRange: 0.65, ballRadius: 22,
  });
  if (type === "rotatingBeam") Object.assign(item, {
    pivotX: x + item.w / 2, pivotY: p.y - 85, length: 160, radius: 10,
  });
  if (type === "crusher") Object.assign(item, { y: p.y - 252, baseY: p.y - 252, travel: 192 });
  if (type === "retractSpikes") Object.assign(item, { y: p.y, baseY: p.y, maxHeight: h });
  r.hazards.push(item);
  if (!r.tags.includes(type)) r.tags.push(type);
  return item;
}
function gust(r, x, offsetY, w, h, force, phase = 0) {
  // Gusts affect only the middle traversal; entry, exit and waiting stations stay calm.
  r.hazards.push({ type: "wind", x, y: FLOOR + offsetY, w, h, force, phase, active: true, authored: true });
  if (!r.tags.includes("wind")) r.tags.push("wind");
}
function seams(r, entryWidth = 460) {
  const entry = ground(r, 0, entryWidth, 0, { entry: true, safe: true });
  const exit = ground(r, 2860, 340, 0, { exit: true, safe: true });
  return { entry, exit };
}

// A long level ferry teaches boarding and waiting before either combat or a hazard.
function reedFerry() {
  const r = room("reed-ferry", "Reed Ferry", "countryside", 1,
    ["river", "ferry", "terraces"], "Board the reed raft, ride the broad river, then climb the olive banks.");
  const { entry, exit } = seams(r);
  const raft = mover(r, 990, 0, 200, { motion: "ferry", range: 610, axis: "x", period: 8, phase: -HALF_TURN, visualKind: "bridge" });
  const bank = ground(r, 1740, 420, -45, { safe: true, arena: true });
  const terrace = shelf(r, 2350, -70, 210);
  const finish = shelf(r, 2610, -35, 190);
  combat(r, bank);
  hazard(r, "retractSpikes", bank, 310, { phase: 1.5 });
  reward(r, bank, { offsetX: 105, type: "fish" });
  r.props.push({ x: 350, y: FLOOR, w: 82, h: 105, artFrame: "fence", routeSign: "Wait for the raft" });
  return route(r, entry, raft, bank, terrace, finish, exit);
}

// The two lifts exchange height; neither is an ornamental upper-route platform.
function millraceRelay() {
  const r = room("millrace-relay", "Millrace Relay", "countryside", 2,
    ["river", "alternating", "stepping"], "Transfer between opposite-phase mill platforms and rest on the far mill bank.");
  const { entry, exit } = seams(r, 440);
  const first = mover(r, 570, -110, 220, { motion: "alternating", axis: "y", range: 110, period: 6.4, phase: HALF_TURN });
  const second = mover(r, 1000, -150, 220, { motion: "alternating", axis: "y", range: 110, period: 6.4, phase: -HALF_TURN });
  const mill = ground(r, 1440, 440, -120, { safe: true, arena: true });
  const step = shelf(r, 2050, -85, 230);
  const garden = shelf(r, 2440, -40, 250);
  combat(r, mill);
  hazard(r, "pendulum", mill, 330, { period: 6.8, phase: 0.8 });
  reward(r, garden, { rise: 110, offsetX: 30, type: "shield" });
  r.props.push({ x: 1470, y: FLOOR - 120, w: 180, h: 190, artFrame: "arch", structure: true });
  return route(r, entry, first, second, mill, step, garden, exit);
}

// A full-height lift is the sole route to the canyon rim; the descent is static.
function canyonCounterweight() {
  const r = room("canyon-counterweight", "Canyon Counterweight", "desert", 2,
    ["canyon", "vertical", "lift"], "Ride a sandstone counterweight above ordinary jump height, then descend the wind-cut rim.");
  const { entry, exit } = seams(r, 470);
  const lift = mover(r, 530, -200, 240, { motion: "lift", axis: "y", range: 200, period: 7.6, phase: HALF_TURN });
  const rim = shelf(r, 830, -380, 400, { safe: true, arena: true });
  const arch = shelf(r, 1370, -310, 220);
  const lower = shelf(r, 1740, -220, 250);
  const oasis = shelf(r, 2160, -125, 300);
  const finish = shelf(r, 2610, -50, 180);
  combat(r, rim);
  hazard(r, "crusher", rim, 310, { w: 70, h: 60, warningDuration: 1.4, activeDuration: 0.8 });
  reward(r, oasis, { rise: 120, offsetX: 65, type: "water" });
  gust(r, 1330, -490, 1150, 600, 75, 0.4);
  return route(r, entry, lift, rim, arch, lower, oasis, finish, exit);
}

// Recovering stepping stones introduce timed visibility from a safe first bank.
function furnaceCaravan() {
  const r = room("furnace-caravan", "Furnace Caravan", "desert", 3,
    ["dunes", "timed", "ferry", "fireJet"], "Cross a returning sandstone plate, wait on the caravan island, and take the furnace shuttle.");
  const { entry, exit } = seams(r, 500);
  const plate = recovering(r, 600, -30, 250);
  const island = ground(r, 1030, 340, -50, { safe: true, arena: true });
  const shuttle = mover(r, 1710, -50, 220, { motion: "ferry", axis: "x", range: 340, period: 7.8, phase: -HALF_TURN });
  const farBank = shelf(r, 2350, -5, 240, { safe: true });
  const lastPlate = recovering(r, 2690, -20, 150, { phase: 0.6 });
  combat(r, island);
  hazard(r, "fireJet", island, 260, { h: 95, w: 42, phase: 0.7, period: 6.2, warningDuration: 1.4 });
  reward(r, farBank, { rise: 115, offsetX: 30, type: "fireRain" });
  return route(r, entry, plate, island, shuttle, farBank, lastPlate, exit);
}

// The hoist and horizontal barge form an L-shaped aqueduct traversal.
function aqueductHoist() {
  const r = room("aqueduct-hoist", "Aqueduct Hoist", "city", 2,
    ["arches", "vertical", "lift", "ferry"], "Take the hoist to the upper aqueduct, then ride a maintenance barge across its missing arches.");
  const { entry, exit } = seams(r, 480);
  const hoist = mover(r, 540, -210, 210, { motion: "lift", axis: "y", range: 210, period: 8, phase: HALF_TURN });
  const landing = shelf(r, 820, -410, 330, { safe: true });
  const barge = mover(r, 1540, -410, 200, { motion: "ferry", axis: "x", range: 390, period: 8.6, phase: -HALF_TURN, visualKind: "bridge" });
  const watch = shelf(r, 2230, -410, 350, { safe: true, arena: true });
  const descent = shelf(r, 2680, -240, 160);
  combat(r, watch, { elite: true });
  hazard(r, "timedGate", watch, 270, { w: 40, h: 118, period: 6.6, phase: 1.2 });
  reward(r, landing, { rise: 115, offsetX: 90, type: "holyLight" });
  r.props.push({ x: 2230, y: FLOOR - 410, w: 210, h: 205, artFrame: "arch", structure: true });
  return route(r, entry, hoist, landing, barge, watch, descent, exit);
}

// A center-pivot bridge changes the launch height before a separate timed roof.
function marketBalance() {
  const r = room("market-balance", "Market Balance", "city", 3,
    ["roofs", "pivot", "timed", "branches"], "Read the tilting market bridge, pause on the bell roof, then cross the returning canopy.");
  const { entry, exit } = seams(r, 500);
  const awning = shelf(r, 650, -70, 220, { safe: true });
  const balance = pivot(r, 1190, -70);
  const bellRoof = shelf(r, 1530, -70, 400, { safe: true, arena: true });
  const canopy = recovering(r, 2080, -90, 250, { period: 10.4, activeDuration: 7.6 });
  const garden = shelf(r, 2480, -50, 280);
  combat(r, bellRoof);
  hazard(r, "rotatingBeam", bellRoof, 300, { period: 6.8, activeDuration: 1.2 });
  reward(r, garden, { rise: 115, offsetX: 45, type: "wind" });
  return route(r, entry, awning, balance, bellRoof, canopy, garden, exit);
}

// A single long cable car leaves a broad safe station at each end of the ravine.
function cedarCableway() {
  const r = room("cedar-cableway", "Cedar Cableway", "mountains", 2,
    ["wind", "ferry", "branches"], "Board the cedar cable car and stay aboard until the far mountain station is in reach.");
  const { entry, exit } = seams(r, 520);
  const cable = mover(r, 1160, -70, 230, { motion: "ferry", axis: "x", range: 660, period: 9.2, phase: -HALF_TURN, visualKind: "bridge" });
  const station = shelf(r, 2110, -140, 430, { safe: true, arena: true });
  const ridge = shelf(r, 2660, -65, 170);
  combat(r, station);
  hazard(r, "pendulum", station, 320, { period: 6.4, phase: 1.3 });
  reward(r, station, { rise: 265, offsetX: 90, type: "fireRain", wings: true });
  gust(r, 650, -360, 1390, 450, 90, 1.1);
  r.props.push({ x: 390, y: FLOOR, w: 108, h: 190, artFrame: "cypress-tree", routeSign: "Ride to the far station" });
  return route(r, entry, cable, station, ridge, exit);
}

// Two independent lifts require a transfer on the sheltered halfway ledge.
function thawingSpire() {
  const r = room("thawing-spire", "Thawing Spire", "mountains", 3,
    ["vertical", "alternating", "rockfall"], "Climb opposite-phase ice lifts with a sheltered transfer, then descend the broken spire.");
  const { entry, exit } = seams(r, 440);
  const lowerLift = mover(r, 510, -170, 190, { motion: "alternating", axis: "y", range: 170, period: 7.2, phase: HALF_TURN });
  const shelter = shelf(r, 780, -330, 230, { safe: true });
  const upperLift = mover(r, 1080, -460, 190, { motion: "alternating", axis: "y", range: 145, period: 7.2, phase: -HALF_TURN });
  const summit = shelf(r, 1350, -585, 360, { safe: true, arena: true });
  const ledge = shelf(r, 1880, -390, 260, { kind: "collapsing", temporary: true, visualKind: "bridge", recoverable: true });
  const lower = shelf(r, 2310, -210, 250);
  const finish = shelf(r, 2710, -70, 150);
  combat(r, summit, { elite: true });
  hazard(r, "fallingRock", summit, 270, { h: 82, w: 42, period: 6.6, warningDuration: 1.5, activeDuration: 0.8 });
  reward(r, shelter, { rise: 115, offsetX: 35, type: "shield" });
  gust(r, 1740, -690, 880, 650, 95, 0.8);
  return route(r, entry, lowerLift, shelter, upperLift, summit, ledge, lower, finish, exit);
}

// A low ferry crosses the buried cistern; the last lift rejoins the shared seam.
function buriedCistern() {
  const r = room("buried-cistern-ferry", "Buried Cistern Ferry", "ruins", 2,
    ["cave", "river", "ferry", "secret"], "Descend into the cistern, ferry beneath the fallen portico, and take the exit lift.");
  const { entry, exit } = seams(r, 460);
  const stairs = shelf(r, 610, 100, 300, { safe: true });
  const ferry = mover(r, 1470, 100, 220, { motion: "ferry", axis: "x", range: 470, period: 8.8, phase: -HALF_TURN });
  const portico = shelf(r, 2230, 40, 340, { safe: true, arena: true });
  const exitLift = mover(r, 2650, -30, 180, { motion: "lift", axis: "y", range: 70, period: 5.6, phase: HALF_TURN });
  combat(r, portico);
  hazard(r, "pendulum", portico, 250, { period: 6.6, phase: 0.8 });
  reward(r, stairs, { rise: 115, offsetX: 85, type: "scroll" });
  return route(r, entry, stairs, ferry, portico, exitLift, exit);
}

// The bridge and two returning slabs have different rhythms, with a refuge between.
function restoredPortico() {
  const r = room("restored-portico", "Restored Portico", "ruins", 3,
    ["arches", "pivot", "timed", "puzzle"], "Balance across a fallen lintel, wait at the sanctuary, then cross two restoring stone slabs.");
  const { entry, exit } = seams(r, 450);
  const lintel = pivot(r, 780, -40, { period: 8, phase: -0.3 });
  const sanctuary = shelf(r, 1130, -25, 350, { safe: true, arena: true });
  const firstSlab = recovering(r, 1640, -80, 250);
  const secondSlab = recovering(r, 2060, -15, 250, { phase: -0.5 });
  const stair = shelf(r, 2470, -10, 300, { safe: true });
  combat(r, sanctuary);
  hazard(r, "retractSpikes", sanctuary, 270, { phase: 0.6, period: 6.2 });
  reward(r, stair, { rise: 120, offsetX: 60, type: "chest" });
  // The plate lies on the safe approach and permanently opens the sanctuary gate.
  // The reward loft previews the plate, gate and next restoring slabs together.
  r.switches.push({ x: 1300, y: FLOOR - 45, w: 52, h: 22, activated: false, pressurePlate: true });
  shelf(r, 1165, -140, 185, { branch: "puzzle", secret: true });
  r.platforms.push({ x: 1425, y: FLOOR - 235, w: 34, h: 210, kind: "gate", oneWay: false, switchLocal: 0, pressureGate: true });
  r.props.push({ x: 1410, y: FLOOR - 25, w: 64, h: 210, artFrame: "gate-closed", structure: true, switchLocal: 0 });
  r.pickupSpawns.push({ x: 1235, y: FLOOR - 185, type: "fireRain", secret: true });
  return route(r, entry, lintel, sanctuary, firstSlab, secondSlab, stair, exit);
}

// Two ferries are separated by a safe lightning tower, so waiting is deliberate.
function thunderRelay() {
  const r = room("thunder-relay", "Thunder Relay", "storm", 2,
    ["lightning", "ferry", "alternating"], "Ride offset storm ferries, pause beneath the lightning tower, and read the warning before leaving shelter.");
  const { entry, exit } = seams(r, 460);
  const first = mover(r, 830, -60, 210, { motion: "ferry", axis: "x", range: 330, period: 6.8, phase: -HALF_TURN });
  const tower = shelf(r, 1430, -100, 340, { safe: true, arena: true });
  const second = mover(r, 2100, -90, 190, { motion: "ferry", axis: "x", range: 320, period: 7.6, phase: HALF_TURN });
  const shelter = shelf(r, 2640, -30, 220, { safe: true });
  combat(r, tower, { elite: true });
  hazard(r, "lightning", tower, 260, { w: 40, h: 110, period: 6.6, warningDuration: 1.5, activeDuration: 0.65, phase: 0.7 });
  reward(r, tower, { rise: 120, offsetX: 70, type: "wind" });
  gust(r, 1810, -320, 780, 430, 120, 0.9);
  return route(r, entry, first, tower, second, shelter, exit);
}

// The final storm composition combines vertical lift, restoring plate, and balance.
function tempestRestoration() {
  const r = room("tempest-restoration", "Tempest Restoration", "storm", 3,
    ["vertical", "lift", "timed", "pivot", "collapse"], "Lift above the storm, wait for the restoring plate, and transfer over a tilting lightning mast.");
  const { entry, exit } = seams(r, 480);
  const lift = mover(r, 550, -190, 220, { motion: "lift", axis: "y", range: 190, period: 7.8, phase: HALF_TURN });
  const refuge = shelf(r, 850, -360, 330, { safe: true, arena: true });
  const plate = recovering(r, 1310, -380, 250, { period: 10.8, activeDuration: 8 });
  const mast = pivot(r, 1890, -360, { period: 7.2, phase: 0.4 });
  const farTower = shelf(r, 2250, -340, 330, { safe: true });
  const descent = shelf(r, 2720, -170, 140);
  combat(r, refuge, { elite: true });
  hazard(r, "rotatingBeam", refuge, 240, { period: 7, warningDuration: 1.5, activeDuration: 1.1 });
  hazard(r, "crusher", farTower, 240, { w: 64, h: 58, phase: 1.6, period: 6.6, warningDuration: 1.4 });
  reward(r, farTower, { rise: 260, offsetX: 60, type: "fireRain", wings: true });
  // A risky optional shortcut has a visible collapse cue and rejoins the descent.
  shelf(r, 2595, -285, 105, { kind: "collapsing", temporary: true, branch: "shortcut", collapseDelay: 0.7 });
  return route(r, entry, lift, refuge, plate, mast, farTower, descent, exit);
}

export const DYNAMIC_ROOM_LIBRARY = [
  reedFerry(), millraceRelay(), canyonCounterweight(), furnaceCaravan(),
  aqueductHoist(), marketBalance(), cedarCableway(), thawingSpire(),
  buriedCistern(), restoredPortico(), thunderRelay(), tempestRestoration(),
];

export function dynamicRoomsFor(region, maxDifficulty = 6) {
  const biome = normalizeBiome(region);
  return DYNAMIC_ROOM_LIBRARY.filter(r => r.region === biome && r.difficulty <= Math.max(2, maxDifficulty));
}


