const ground = (x, w, y = 468) => ({ x, y, w, h: 90, kind: "ground", oneWay: false });
const ledge = (x, y, w, kind = "platform") => ({ x, y, w, h: 18, kind, oneWay: true });
const mover = (x, y, w, axis, range, speed = 1.2) => ({ x, y, w, h: 18, axis, range, speed, kind: "moving", oneWay: true });
const hazard = (type, x, y, w = 44, h = 18) => ({ type, x, y, w, h });

function room(id, region, name, difficulty, platforms, options = {}) {
  platforms[0].entry = true;
  platforms[platforms.length - 1].exit = true;
  return {
    id, region, name, difficulty,
    tags: options.tags || [],
    width: options.width || 1980,
    platforms,
    movers: options.movers || [],
    hazards: options.hazards || [],
    enemySpawns: options.enemySpawns || [],
    pickupSpawns: options.pickupSpawns || [],
    checkpointX: options.checkpointX || 980,
  };
}

export const ROOM_LIBRARY = [
  room("galilee_shore", "galilee", "Galilee Shore", 1, [
    ground(0, 390), ground(470, 420), ground(980, 400), ground(1470, 510),
  ], { tags: ["teaching", "horizontal"], enemySpawns: [{ x: 650 }, { x: 1210 }], pickupSpawns: [{ x: 1090, type: "bread" }] }),
  room("capernaum_roofs", "galilee", "Capernaum Rooftops", 2, [
    ground(0, 330), ledge(390, 415, 190), ledge(650, 350, 210), ledge(930, 405, 220), ledge(1230, 330, 180), ground(1490, 490),
  ], { tags: ["rooftops", "rhythm"], enemySpawns: [{ x: 740 }, { x: 1280 }], checkpointX: 920 }),
  room("sea_crossing", "galilee", "Sea of Galilee", 3, [
    ground(0, 330), ledge(560, 390, 180), ledge(900, 330, 170), ledge(1260, 390, 190), ground(1580, 400),
  ], { tags: ["movingPlatforms", "timing"], movers: [mover(370, 405, 135, "x", 75, 1.15), mover(1110, 370, 130, "y", 55, 1.0)], pickupSpawns: [{ x: 970, type: "shield" }] }),

  room("rome_aqueduct", "rome", "Aqueduct Run", 2, [
    ground(0, 350), ledge(415, 410, 210), ledge(690, 360, 220), ledge(970, 410, 190), ledge(1230, 350, 230), ground(1530, 450),
  ], { tags: ["horizontal", "precision"], enemySpawns: [{ x: 500 }, { x: 1040 }, { x: 1660 }] }),
  room("rome_rooftops", "rome", "Roman Rooftops", 3, [
    ground(0, 280), ledge(350, 410, 160), ledge(570, 335, 190), ledge(825, 390, 170), ledge(1060, 310, 190), ledge(1310, 375, 175), ground(1550, 430),
  ], { tags: ["speed", "rooftops"], enemySpawns: [{ x: 650 }, { x: 1130 }, { x: 1670 }], hazards: [hazard("spikes", 875, 372, 70)] }),
  room("rome_siegeworks", "rome", "Siege Works", 4, [
    ground(0, 280), ledge(390, 405, 140), ledge(650, 345, 150), ledge(930, 285, 150), ledge(1210, 350, 165), ledge(1480, 405, 160), ground(1730, 250),
  ], { tags: ["movingPlatforms", "vertical", "precision"], movers: [mover(285, 400, 110, "x", 55, 1.5), mover(810, 340, 100, "y", 70, 1.25)], enemySpawns: [{ x: 690 }, { x: 1250 }, { x: 1810 }], hazards: [hazard("spikes", 1515, 387, 70)] }),

  room("jerusalem_steps", "jerusalem", "Temple Steps", 2, [
    ground(0, 350), ledge(400, 420, 190), ledge(640, 370, 190), ledge(880, 320, 190), ledge(1120, 370, 190), ledge(1360, 420, 190), ground(1600, 380),
  ], { tags: ["vertical", "stairs"], enemySpawns: [{ x: 710 }, { x: 1190 }, { x: 1710 }] }),
  room("jerusalem_courtyard", "jerusalem", "Courtyard Climb", 3, [
    ground(0, 300), ledge(365, 410, 170), ledge(610, 335, 160), ledge(840, 260, 160), ledge(1080, 335, 170), ledge(1320, 410, 180), ground(1580, 400),
  ], { tags: ["vertical", "precision"], movers: [mover(995, 350, 90, "y", 62, 1.05)], enemySpawns: [{ x: 670 }, { x: 1380 }] }),
  room("jerusalem_high_terrace", "jerusalem", "High Terrace", 5, [
    ground(0, 250), ledge(330, 400, 120), ledge(520, 315, 125), ledge(720, 235, 130), ledge(940, 320, 125), ledge(1160, 405, 130), ledge(1390, 325, 125), ledge(1600, 245, 130), ground(1800, 180),
  ], { tags: ["vertical", "precision", "elite"], movers: [mover(1265, 365, 95, "x", 50, 1.45)], hazards: [hazard("spikes", 1175, 387, 90)], enemySpawns: [{ x: 760 }, { x: 1440 }, { x: 1840 }] }),

  room("hell_lava_bridge", "hell", "Lava Bridge", 3, [
    ground(0, 330), ledge(430, 410, 170, "obsidian"), ledge(680, 350, 175, "obsidian"), ledge(940, 410, 170, "obsidian"), ledge(1210, 340, 190, "obsidian"), ground(1510, 470),
  ], { tags: ["lava", "timing"], hazards: [hazard("lava", 330, 455, 1180, 80), hazard("fireJet", 1010, 392, 35, 18)], enemySpawns: [{ x: 740 }, { x: 1280 }, { x: 1680 }] }),
  room("hell_fire_corridor", "hell", "Fire Jet Corridor", 4, [
    ground(0, 310), ledge(385, 405, 160, "obsidian"), ledge(625, 405, 160, "obsidian"), ledge(865, 405, 160, "obsidian"), ledge(1105, 405, 160, "obsidian"), ledge(1345, 405, 160, "obsidian"), ground(1580, 400),
  ], { tags: ["hazards", "rhythm"], hazards: [hazard("lava", 310, 458, 1270, 75), hazard("fireJet", 455, 387), hazard("fireJet", 935, 387), hazard("fireJet", 1415, 387)], enemySpawns: [{ x: 690 }, { x: 1180 }, { x: 1710 }] }),
  room("hell_abyss", "hell", "Abyss Crossing", 5, [
    ground(0, 250), ledge(370, 395, 125, "obsidian"), ledge(610, 315, 120, "obsidian"), ledge(850, 395, 120, "obsidian"), ledge(1100, 300, 125, "obsidian"), ledge(1360, 390, 120, "obsidian"), ground(1660, 320),
  ], { tags: ["movingPlatforms", "lava", "precision"], movers: [mover(255, 395, 105, "x", 50, 1.4), mover(740, 350, 95, "y", 75, 1.25), mover(1495, 380, 105, "x", 60, 1.5)], hazards: [hazard("lava", 250, 458, 1410, 75)], enemySpawns: [{ x: 650 }, { x: 1140 }, { x: 1760 }] }),

  room("heaven_cloud_stair", "heaven", "Cloud Stairway", 4, [
    ground(0, 270), ledge(350, 400, 150, "cloud"), ledge(570, 330, 150, "cloud"), ledge(790, 255, 150, "cloud"), ledge(1030, 330, 150, "cloud"), ledge(1270, 400, 150, "cloud"), ground(1530, 450),
  ], { tags: ["vertical", "clouds"], pickupSpawns: [{ x: 850, type: "wings" }], enemySpawns: [{ x: 1100 }] }),
  room("heaven_light_bridges", "heaven", "Light Bridges", 5, [
    ground(0, 240), ledge(350, 390, 120, "blessed"), ledge(590, 310, 120, "blessed"), ledge(830, 390, 120, "blessed"), ledge(1070, 300, 120, "blessed"), ledge(1310, 390, 120, "blessed"), ground(1590, 390),
  ], { tags: ["movingPlatforms", "precision"], movers: [mover(245, 390, 95, "x", 50, 1.35), mover(1210, 350, 90, "y", 70, 1.2)], pickupSpawns: [{ x: 1120, type: "holyLight" }] }),
  room("heaven_ascension", "heaven", "Ascension Columns", 6, [
    ground(0, 220), ledge(315, 395, 105, "cloud"), ledge(500, 305, 105, "cloud"), ledge(690, 215, 110, "cloud"), ledge(910, 305, 105, "cloud"), ledge(1110, 395, 105, "cloud"), ledge(1320, 300, 105, "cloud"), ledge(1530, 210, 110, "cloud"), ground(1780, 200),
  ], { tags: ["vertical", "precision", "elite"], movers: [mover(1645, 300, 90, "y", 80, 1.25)], enemySpawns: [{ x: 740 }, { x: 1370 }], pickupSpawns: [{ x: 1570, type: "fireRain" }] }),
];

export const SAFE_FALLBACK_ROOM = room("safe_pilgrim_path", "galilee", "Pilgrim's Path", 1, [
  ground(0, 420), ledge(485, 410, 210), ledge(760, 365, 210), ledge(1035, 410, 210), ledge(1310, 365, 210), ground(1585, 395),
], { tags: ["fallback", "guaranteed"] });

export function roomsFor(region, maxDifficulty = 6) {
  const matches = ROOM_LIBRARY.filter((candidate) => candidate.region === region && candidate.difficulty <= maxDifficulty);
  return matches.length ? matches : ROOM_LIBRARY.filter((candidate) => candidate.region === region);
}

