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

function applyModifiers(room, tier, random) {
  const modifiers = [];
  if (tier >= 3 && random() > 0.4) {
    modifiers.push("reinforcements");
    room.enemySpawns.push({ x: 1050 + Math.floor(random() * 260), elite: tier >= 5 });
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
  let validation = validateRoom(template);
  if (!validation.valid) {
    template = SAFE_FALLBACK_ROOM;
    validation = validateRoom(template);
  }

  const room = clone(template);
  const modifiers = applyModifiers(room, tier, random);
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

