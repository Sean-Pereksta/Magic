import { clamp, overlaps, uid } from "../common.js";
import { damagePlayer } from "../physics.js";
import { VIEW } from "../config.js";

const ATTACK_TIMING = {
  charge: [0.72, 0.78], slam: [0.66, 0.82], targetedLeap: [0.7, 0.86],
  shockwave: [0.78, 0.72], arcVolley: [0.72, 1.2], orbVolley: [0.78, 1.35],
  fireVolley: [0.7, 1.25], sweep: [0.65, 0.9], judgmentLanes: [0.95, 0.72],
  targetCircle: [0.9, 0.7], fireEruption: [0.92, 0.75], fallingDebris: [0.82, 1.25],
  fallingLight: [0.82, 1.25], fallingFire: [0.78, 1.25], radial: [0.72, 1.1],
  alternatingSafeZones: [1.0, 1.0], movingHazard: [0.78, 1.35], dashCombo: [0.62, 1.2],
};

const friendlyNames = {
  charge: "Wallbreaker Charge", slam: "Grounding Slam", targetedLeap: "Targeted Leap",
  shockwave: "Twin Shockwave", arcVolley: "Arcing Volley", orbVolley: "Orb Volley",
  fireVolley: "Cinder Volley", sweep: "Sweeping Light", judgmentLanes: "Judgment Lanes",
  targetCircle: "Marked Ground", fireEruption: "Delayed Eruption", fallingDebris: "Falling Columns",
  fallingLight: "Falling Light", fallingFire: "Falling Fire", radial: "Radiant Ring",
  alternatingSafeZones: "Alternating Sanctuary", movingHazard: "Living Flame", dashCombo: "Threefold Rush",
};

function laneData(arena, player) {
  const laneWidth = (arena.right - arena.left) / 5;
  const playerLane = Math.max(0, Math.min(4, Math.floor((player.x - arena.left) / laneWidth)));
  const safeLane = clamp(playerLane + (playerLane >= 4 ? -1 : 1), 0, 4);
  const dangerous = [0, 1, 2, 3, 4].filter((lane) => lane !== safeLane);
  return { laneWidth, dangerous };
}

export function createAttackData(id, boss, player, arena) {
  const timing = ATTACK_TIMING[id] || [0.7, 0.8];
  const direction = Math.sign(player.x - boss.x) || 1;
  const data = {
    id,
    name: friendlyNames[id] || id,
    tellDuration: timing[0] / boss.phase.speed,
    activeDuration: timing[1] / boss.phase.speed,
    elapsed: 0,
    direction,
    targetX: player.x + player.w / 2,
    targetY: player.y + player.h,
    originX: boss.x + boss.w / 2,
    originY: boss.y + boss.h / 2,
    executed: false,
    spawned: 0,
    hitPlayer: false,
    exposeOnFinish: boss.definition.exposingAttacks.includes(id),
  };
  if (id === "judgmentLanes") Object.assign(data, laneData(arena, player));
  if (id === "alternatingSafeZones") {
    data.safeW = 210;
    const shift = boss.attackCount % 2 ? 180 : -180;
    data.safeX = clamp(player.x + shift, arena.left + 60, arena.right - data.safeW - 60);
  }
  return data;
}

export function telegraphShapes(attack, arena) {
  if (!attack) return [];
  const line = { type: "line", x: attack.targetX, y: 0, w: 6, h: VIEW.floorY, danger: true };
  switch (attack.id) {
    case "charge": return [{ type: "groundLine", x: arena.left, y: VIEW.floorY - 12, w: arena.right - arena.left, h: 12 }];
    case "slam":
    case "targetedLeap":
    case "targetCircle": return [{ type: "circle", x: attack.targetX, y: VIEW.floorY - 8, radius: attack.id === "targetCircle" ? 90 : 120 }];
    case "judgmentLanes": return attack.dangerous.map((lane) => ({ type: "rect", x: arena.left + lane * attack.laneWidth, y: 0, w: attack.laneWidth, h: VIEW.floorY }));
    case "fireEruption": return [-180, 0, 180].map((offset) => ({ type: "circle", x: attack.targetX + offset, y: VIEW.floorY - 8, radius: 62 }));
    case "fallingDebris":
    case "fallingLight":
    case "fallingFire": return [-220, -70, 80, 230].map((offset) => ({ ...line, x: attack.targetX + offset }));
    case "alternatingSafeZones": return [{ type: "safe", x: attack.safeX, y: 0, w: attack.safeW, h: VIEW.floorY }];
    case "radial": return [{ type: "circle", x: attack.originX, y: attack.originY, radius: 150 }];
    case "sweep": return [{ type: "groundLine", x: arena.left, y: attack.targetY - 10, w: arena.right - arena.left, h: 20 }];
    default: return [line];
  }
}

function projectile(world, x, y, vx, vy, type = "orb", damage = 1, life = 3) {
  world.bossHazards.push({ id: uid("boss-hazard"), type, x, y, w: 18, h: 18, vx, vy, damage, life, active: true });
}

function zone(world, x, y, w, h, life = 0.28, type = "attackZone") {
  world.bossHazards.push({ id: uid("boss-zone"), type, x, y, w, h, vx: 0, vy: 0, damage: 1, life, active: true });
}

export function beginActiveAttack(boss, world, player, arena) {
  const attack = boss.currentAttack;
  if (!attack || attack.executed) return;
  attack.executed = true;
  attack.elapsed = 0;
  switch (attack.id) {
    case "charge":
      boss.vx = attack.direction * 850 * boss.phase.speed;
      break;
    case "slam":
    case "targetedLeap":
      boss.x = Math.max(arena.left + 40, Math.min(arena.right - boss.w - 40, attack.targetX - boss.w / 2));
      zone(world, attack.targetX - 125, VIEW.floorY - 34, 250, 34, 0.4, "shockwave");
      break;
    case "judgmentLanes":
      attack.dangerous.forEach((lane) => zone(world, arena.left + lane * attack.laneWidth + 5, 0, attack.laneWidth - 10, VIEW.floorY, 0.48, "judgment"));
      break;
    case "targetCircle":
      zone(world, attack.targetX - 90, VIEW.floorY - 180, 180, 180, 0.42, "eruption");
      break;
    case "fireEruption":
      [-180, 0, 180].forEach((offset) => zone(world, attack.targetX + offset - 58, VIEW.floorY - 125, 116, 125, 0.55, "fire"));
      break;
    case "alternatingSafeZones":
      zone(world, arena.left, 0, Math.max(0, attack.safeX - arena.left), VIEW.floorY, 0.62, "judgment");
      zone(world, attack.safeX + attack.safeW, 0, Math.max(0, arena.right - attack.safeX - attack.safeW), VIEW.floorY, 0.62, "judgment");
      break;
    case "radial":
      for (let index = 0; index < 12; index++) {
        const angle = (Math.PI * 2 * index) / 12;
        projectile(world, boss.x + boss.w / 2, boss.y + boss.h / 2, Math.cos(angle) * 340, Math.sin(angle) * 340, "orb");
      }
      break;
    case "shockwave":
      projectile(world, boss.x, VIEW.floorY - 24, -410, 0, "shockwave");
      projectile(world, boss.x + boss.w, VIEW.floorY - 24, 410, 0, "shockwave");
      break;
    case "sweep":
      projectile(world, boss.x + boss.w / 2, attack.targetY - 18, attack.direction * 620, 0, "sweep");
      break;
    case "movingHazard":
      projectile(world, arena.left + 20, VIEW.floorY - 58, 285 * boss.phase.speed, 0, "fire", 1, 5);
      break;
    default: break;
  }
}

function spawnTimedProjectiles(boss, world) {
  const attack = boss.currentAttack;
  const originX = boss.x + boss.w / 2;
  const originY = boss.y + boss.h * 0.4;
  const volleyTypes = ["arcVolley", "orbVolley", "fireVolley"];
  if (volleyTypes.includes(attack.id)) {
    const due = Math.min(5, Math.floor(attack.elapsed / 0.18) + 1);
    while (attack.spawned < due) {
      const spread = (attack.spawned - 2) * 0.12;
      projectile(world, originX, originY, attack.direction * (360 + Math.abs(spread) * 170), -250 + spread * 520, attack.id === "fireVolley" ? "fire" : "orb");
      attack.spawned++;
    }
  }
  const falling = ["fallingDebris", "fallingLight", "fallingFire"];
  if (falling.includes(attack.id)) {
    const due = Math.min(4, Math.floor(attack.elapsed / 0.2) + 1);
    const type = attack.id === "fallingFire" ? "fire" : attack.id === "fallingLight" ? "light" : "debris";
    while (attack.spawned < due) {
      const offset = [-220, -70, 80, 230][attack.spawned];
      projectile(world, attack.targetX + offset, -40, 0, 520, type, 1, 2);
      attack.spawned++;
    }
  }
  if (attack.id === "dashCombo") {
    const due = Math.min(3, Math.floor(attack.elapsed / 0.32) + 1);
    if (attack.spawned < due) {
      boss.vx = (attack.spawned % 2 ? -1 : 1) * attack.direction * 760;
      attack.spawned = due;
    }
  }
}

export function updateActiveAttack(boss, world, player, arena, dt) {
  const attack = boss.currentAttack;
  if (!attack) return { finished: true, expose: false };
  attack.elapsed += dt;
  spawnTimedProjectiles(boss, world);

  if (attack.id === "charge") {
    boss.x += boss.vx * dt;
    const struckColumn = (world.props || []).find((prop) => !prop.dead && overlaps(boss, prop));
    if (struckColumn) {
      struckColumn.dead = true;
      boss.vx = 0;
      return { finished: true, expose: true, impact: true };
    }
    if (boss.x <= arena.left + 20 || boss.x + boss.w >= arena.right - 20) {
      boss.x = Math.max(arena.left + 20, Math.min(arena.right - boss.w - 20, boss.x));
      boss.vx = 0;
      return { finished: true, expose: true, impact: true };
    }
  }

  if (attack.id === "dashCombo") boss.x = Math.max(arena.left + 20, Math.min(arena.right - boss.w - 20, boss.x + boss.vx * dt));
  if (overlaps(boss, player) && !attack.hitPlayer) {
    attack.hitPlayer = damagePlayer(player, 1, { x: Math.sign(player.x - boss.x) * 480, y: -480 });
  }
  if (attack.elapsed >= attack.activeDuration) return { finished: true, expose: attack.exposeOnFinish };
  return { finished: false, expose: false };
}

export function updateBossHazards(world, player, dt) {
  for (const hazard of world.bossHazards) {
    hazard.life -= dt;
    hazard.x += (hazard.vx || 0) * dt;
    hazard.y += (hazard.vy || 0) * dt;
    if (hazard.type === "orb" || hazard.type === "fire" || hazard.type === "debris" || hazard.type === "light") hazard.vy = (hazard.vy || 0) + 480 * dt;
    if (hazard.active && overlaps(hazard, player)) {
      damagePlayer(player, hazard.damage || 1, { x: Math.sign(player.x - hazard.x) * 260, y: -330 });
      hazard.active = false;
      hazard.life = Math.min(hazard.life, 0.15);
    }
  }
  world.bossHazards = world.bossHazards.filter((hazard) => hazard.life > 0 && hazard.y < VIEW.height + 160);
}
