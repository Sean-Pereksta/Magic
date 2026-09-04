import { seededRandom, uid } from "../common.js";
import { REGIONS } from "./regions.js";
import { roomsFor, SAFE_FALLBACK_ROOM } from "./rooms.js";
import { validateRoom } from "./room-validator.js";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function difficultyTier(depth, journeyDifficulty = null) {
  if (journeyDifficulty) return Math.max(1, Math.min(6, journeyDifficulty));
  return Math.max(1, Math.min(6, 1 + Math.floor(depth / 2)));
}

const REGION_SCENERY = {
  galilee: ["palm", "reeds", "boat", "village"],
  rome: ["arch", "column", "banner", "cypress"],
  jerusalem: ["olive", "arch", "lamp", "stoneHouse"],
  hell: ["basalt", "emberVent", "chain", "ruin"],
  heaven: ["cloudSpire", "lightColumn", "star", "goldArch"],
};

function mirrorRectX(item, baseWidth, offset) {
  return offset + baseWidth - item.x - item.w;
}

function mirrorPointX(x, baseWidth, offset) {
  return offset + baseWidth - x;
}

function expandRoom(room, random) {
  const baseWidth = room.width || 1980;
  const overlap = 150 + Math.floor(random() * 80);
  const offset = baseWidth - overlap;
  const originalPlatforms = room.platforms.map((platform) => ({ ...platform }));
  const originalMovers = room.movers.map((platform) => ({ ...platform }));
  const originalHazards = room.hazards.map((item) => ({ ...item }));
  const originalEnemies = room.enemySpawns.map((spawn) => ({ ...spawn }));

  // The first act keeps its original direction. The second act reverses the same
  // platform language so the player gets a new rhythm instead of empty padding.
  for (const platform of room.platforms) delete platform.exit;
  const reprisePlatforms = originalPlatforms.slice().reverse().map((platform) => ({
    ...platform,
    x: mirrorRectX(platform, baseWidth, offset),
    entry: false,
    exit: false,
  }));
  if (reprisePlatforms.length) reprisePlatforms[reprisePlatforms.length - 1].exit = true;
  room.platforms.push(...reprisePlatforms);

  room.movers.push(...originalMovers.map((platform) => ({
    ...platform,
    x: mirrorRectX(platform, baseWidth, offset),
  })));

  room.hazards.push(...originalHazards.map((item) => ({
    ...item,
    x: mirrorRectX(item, baseWidth, offset),
  })));

  room.enemySpawns.push(...originalEnemies.map((spawn) => ({
    ...spawn,
    x: mirrorPointX(spawn.x, baseWidth, offset),
  })));

  room.width = offset + baseWidth;
  room.checkpointX = Math.floor(offset + overlap * 0.5);
  room.tags = [...new Set([...(room.tags || []), "extended", "two-act", "reprise"])];
}

function buildScenery(room, random) {
  const choices = REGION_SCENERY[room.region] || REGION_SCENERY.galilee;
  const scenery = [];
  let x = 120 + random() * 120;
  while (x < room.width - 100) {
    scenery.push({
      id: uid("scenery"),
      kind: choices[Math.floor(random() * choices.length)],
      x: Math.floor(x),
      scale: 0.72 + random() * 0.72,
      depth: random() > 0.58 ? "near" : "far",
      flip: random() > 0.5,
      phase: random() * Math.PI * 2,
    });
    x += 210 + random() * 300;
  }
  return scenery;
}

function applyModifiers(room, tier, random) {
  const modifiers = [];
  if (tier >= 3 && random() > 0.4) {
    modifiers.push("reinforcements");
    room.enemySpawns.push({
      x: Math.floor(room.width * (0.52 + random() * 0.32)),
      elite: tier >= 5,
    });
  }
  if (tier >= 4 && room.hazards.length && random() > 0.35) {
    modifiers.push("quick-hazards");
    room.hazardSpeed = 1 + Math.min(0.7, tier * 0.08);
  }
  if (tier >= 5 && random() > 0.45) {
    modifiers.push("elite-enemy");
    const spawn = room.enemySpawns[Math.floor(random() * Math.max(1, room.enemySpawns.length))];
    if (spawn) spawn.elite = true;
  }
  return modifiers;
}

export function generateRoom({ region = "galilee", depth = 0, seed = Date.now(), journeyDifficulty = null } = {}) {
  const random = seededRandom(seed);
  const tier = difficultyTier(depth, journeyDifficulty);
  const candidates = roomsFor(region, tier).filter((room) => room.difficulty >= Math.max(1, tier - 2));
  let template = candidates[Math.floor(random() * candidates.length)] || SAFE_FALLBACK_ROOM;
  if (!validateRoom(template).valid) template = SAFE_FALLBACK_ROOM;

  let room = clone(template);
  expandRoom(room, random);
  let validation = validateRoom(room);
  if (!validation.valid) {
    room = clone(SAFE_FALLBACK_ROOM);
    expandRoom(room, random);
    validation = validateRoom(room);
  }

  const modifiers = applyModifiers(room, tier, random);
  room.scenery = buildScenery(room, random);

  room.platforms.forEach((platform) => { platform.id = uid("platform"); });
  room.movers.forEach((platform) => {
    platform.id = uid("mover");
    platform.baseX = platform.x;
    platform.baseY = platform.y;
    platform.phase = random() * Math.PI * 2;
    platform.dx = 0;
    platform.dy = 0;
  });
  room.hazards.forEach((item) => {
    item.id = uid("hazard");
    item.timer = random() * 2;
    item.active = item.type !== "fireJet";
  });

  const enemyPool = REGIONS[region]?.enemyPool || REGIONS.galilee.enemyPool;
  const enemies = room.enemySpawns.map((spawn, index) => ({
    id: uid("enemy"),
    type: enemyPool[index % enemyPool.length],
    x: spawn.x,
    y: 400,
    w: spawn.elite ? 48 : 40,
    h: spawn.elite ? 58 : 50,
    vx: 0,
    patrol: index % 2 ? -1 : 1,
    hp: spawn.elite ? 3 : 1,
    elite: Boolean(spawn.elite),
    dead: false,
  }));

  const pickups = room.pickupSpawns.map((spawn) => ({
    id: uid("pickup"),
    x: spawn.x,
    y: 300,
    w: 28,
    h: 28,
    type: spawn.type,
    collected: false,
  }));

  return {
    ...room,
    tier,
    seed,
    modifiers,
    validation,
    routeValid: validation.valid,
    enemies,
    pickups,
    region,
  };
}
