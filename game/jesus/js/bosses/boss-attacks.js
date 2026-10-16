import { clamp } from "../common.js";
import { spawnCombatProjectile, updateCombatProjectiles, emitCombatEffect } from "../enemies/enemies.js";

const ATTACK_TIMING = {
  charge: [.9, .9], slam: [.9, .8], targetedLeap: [1, .95],
  shockwave: [.9, .65], arcVolley: [.9, 1.1], orbVolley: [.95, 1.15],
  fireVolley: [.95, 1.15], sweep: [.85, .7], judgmentLanes: [1.2, .65],
  targetCircle: [1.1, .65], fireEruption: [1.15, .75], fallingDebris: [1.05, 1.2],
  fallingLight: [1.05, 1.2], fallingFire: [1.05, 1.2], radial: [.95, .8],
  alternatingSafeZones: [1.2, .85], movingHazard: [1, 1.3], dashCombo: [1, 1.4],
};
const names = {
  charge: "Wallbreaker Charge", slam: "Grounding Slam", targetedLeap: "Marked Leap", shockwave: "Twin Shockwave",
  arcVolley: "Arcing Volley", orbVolley: "Tablet Volley", fireVolley: "Cinder Volley", sweep: "Gavel Sweep",
  judgmentLanes: "Judgment Lanes", targetCircle: "Marked Ground", fireEruption: "Delayed Eruption",
  fallingDebris: "Falling Columns", fallingLight: "Falling Light", fallingFire: "Falling Fire", radial: "Radiant Ring",
  alternatingSafeZones: "Sanctuary", movingHazard: "Living Flame", dashCombo: "Twofold Rush",
};
const floorY = (arena) => arena.floorY || 468;

export function createAttackData(id, boss, player, arena) {
  const timing = ATTACK_TIMING[id] || [1, .8];
  const targetX = clamp(player.x + player.w / 2, arena.left + 70, arena.right - 70);
  const data = {
    id, name: names[id] || id,
    // Difficulty accelerates the pattern, never removes the reaction window.
    tellDuration: Math.max(id === "judgmentLanes" || id === "alternatingSafeZones" ? .9 : .67, timing[0] / boss.phase.speed),
    activeDuration: timing[1], elapsed: 0,
    direction: Math.sign(targetX - (boss.x + boss.w / 2)) || -1,
    targetX: id === "slam" ? boss.x + boss.w / 2 : targetX,
    targetY: player.y + player.h,
    originX: boss.x + boss.w / 2, originY: boss.y + boss.h / 2,
    startX: boss.x, startY: boss.y,
    executed: false, spawned: 0, hitPlayer: false, landed: false,
    exposeOnFinish: boss.definition.exposingAttacks.includes(id),
  };
  if (id === "judgmentLanes") {
    data.laneWidth = (arena.right - arena.left) / 5;
    // The occupied lane remains safe. It is visible for the full tell; there
    // is no demand to cross an entire arena before a simultaneous attack.
    data.safeLane = clamp(Math.floor((targetX - arena.left) / data.laneWidth), 0, 4);
    data.dangerous = [0, 1, 2, 3, 4].filter((lane) => lane !== data.safeLane);
  }
  if (id === "alternatingSafeZones") {
    data.safeW = 250;
    const shift = boss.attackCount % 2 ? 75 : -75;
    data.safeX = clamp(targetX - data.safeW / 2 + shift, arena.left + 25, arena.right - data.safeW - 25);
  }
  return data;
}
export function telegraphShapes(attack, arena) {
  if (!attack || !arena) return [];
  const floor = floorY(arena), height = floor - (arena.top || 0);
  const line = { type: "line", x: attack.targetX, y: arena.top || 0, w: 24, h: height, danger: true };
  switch (attack.id) {
    case "charge":
    case "dashCombo": return [{ type: "groundLine", x: arena.left, y: floor - 16, w: arena.right - arena.left, h: 16 }];
    case "slam":
    case "targetedLeap":
    case "targetCircle": return [{ type: "circle", x: attack.targetX, y: floor - 5, radius: attack.id === "targetCircle" ? 88 : 120 }];
    case "judgmentLanes": return attack.dangerous.map((lane) => ({ type: "rect", x: arena.left + lane * attack.laneWidth + 8, y: 0, w: attack.laneWidth - 16, h: height }));
    case "fireEruption": return [-200, 0, 200].map((offset) => ({ type: "circle", x: attack.targetX + offset, y: floor - 5, radius: 52 }));
    case "fallingDebris":
    case "fallingLight":
    case "fallingFire": return [-240, -80, 80, 240].map((offset) => ({ ...line, x: attack.targetX + offset }));
    case "alternatingSafeZones": return [{ type: "safe", x: attack.safeX, y: 0, w: attack.safeW, h: height }];
    case "radial": return [{ type: "circle", x: attack.originX, y: attack.originY, radius: 150 }];
    case "sweep": return [{ type: "groundLine", x: arena.left, y: attack.targetY - 21, w: arena.right - arena.left, h: 21 }];
    default: return [line];
  }
}
function projectile(world, x, y, vx, vy, type = "orb", life = 3, extra = {}) {
  return spawnCombatProjectile(world, { type, x, y, w: 18, h: 18, vx, vy, life, ...extra }, "bossHazards");
}
function zone(world, x, y, w, h, life = .3, type = "attackZone") {
  if (w <= 0) return;
  spawnCombatProjectile(world, { type, x, y, w, h, life, zone: true }, "bossHazards");
}
export function beginActiveAttack(boss, world, player, arena) {
  const attack = boss.currentAttack;
  if (!attack || attack.executed) return;
  attack.executed = true; attack.elapsed = 0;
  attack.startX = boss.x; attack.startY = boss.y;
  const floor = floorY(arena);
  switch (attack.id) {
    case "charge": boss.vx = attack.direction * 620 * Math.min(1.22, boss.phase.speed); break;
    case "slam":
    case "targetedLeap": boss.vx = 0; boss.vy = 0; break;
    case "judgmentLanes":
      attack.dangerous.forEach((lane) => zone(world, arena.left + lane * attack.laneWidth + 8, 0, attack.laneWidth - 16, floor, .42, "judgment"));
      break;
    case "targetCircle": zone(world, attack.targetX - 88, floor - 140, 176, 140, .35, "eruption"); break;
    case "fireEruption":
      [-200, 0, 200].forEach((offset) => zone(world, attack.targetX + offset - 52, floor - 110, 104, 110, .45, "eruption"));
      emitCombatEffect(world, "fx-fire-eruption", attack.targetX, floor, { scale: 1.1 });
      break;
    case "alternatingSafeZones":
      zone(world, arena.left, 0, attack.safeX - arena.left, floor, .5, "judgment");
      zone(world, attack.safeX + attack.safeW, 0, arena.right - attack.safeX - attack.safeW, floor, .5, "judgment");
      break;
    case "radial":
      for (let index = 0; index < 8; index++) {
        const angle = (Math.PI * 2 * index) / 8 + Math.PI / 8;
        projectile(world, attack.originX, attack.originY, Math.cos(angle) * 255, Math.sin(angle) * 255, "orb", 3.2);
      }
      emitCombatEffect(world, "fx-radial-orb", attack.originX, attack.originY, { scale: 1.3 });
      break;
    case "shockwave":
      projectile(world, boss.x, floor - 21, -320, 0, "shockwave", 3.8, { w: 38, h: 21 });
      projectile(world, boss.x + boss.w, floor - 21, 320, 0, "shockwave", 3.8, { w: 38, h: 21 });
      break;
    case "sweep": projectile(world, boss.x + boss.w / 2, attack.targetY - 21, attack.direction * 400, 0, "sweep", 2.6, { w: 42, h: 21 }); break;
    case "movingHazard":
      projectile(world, attack.direction > 0 ? arena.left + 20 : arena.right - 38, floor - 48, attack.direction * 230, 0, "fire", 5, { w: 30, h: 48 });
      break;
    default: break;
  }
}
function spawnTimedProjectiles(boss, world, arena) {
  const attack = boss.currentAttack, x = boss.x + boss.w / 2, y = boss.y + boss.h * .42;
  if (["arcVolley", "orbVolley", "fireVolley"].includes(attack.id)) {
    const due = Math.min(3, Math.floor(attack.elapsed / .28) + 1);
    while (attack.spawned < due) {
      const index = attack.spawned++, dx = attack.targetX - x, dy = attack.targetY - 32 - y;
      const distance = Math.max(1, Math.hypot(dx, dy)), spread = (index - 1) * .15;
      const angle = Math.atan2(dy, dx) + spread;
      const speed = attack.id === "arcVolley" ? 300 : 270;
      projectile(world, x, y, Math.cos(angle) * speed, Math.sin(angle) * speed - (attack.id === "arcVolley" ? 150 : 0), attack.id === "fireVolley" ? "fire" : attack.id === "orbVolley" ? "tablet" : "stone", 3.5, { gravity: attack.id === "arcVolley" ? 190 : 0 });
    }
  }
  if (["fallingDebris", "fallingLight", "fallingFire"].includes(attack.id)) {
    const due = Math.min(4, Math.floor(attack.elapsed / .22) + 1);
    while (attack.spawned < due) {
      const offset = [-240, -80, 80, 240][attack.spawned++];
      const type = attack.id === "fallingFire" ? "fire" : attack.id === "fallingLight" ? "light" : "debris";
      projectile(world, attack.targetX + offset - 12, -35, 0, 360, type, 2.8, { w: 24, h: 28 });
    }
  }
  if (attack.id === "dashCombo") {
    // Two dashes in the SAME committed direction with a 0.32s pause. The
    // player can jump them; the boss cannot reverse directly into a dodge.
    const first = attack.elapsed < .42, second = attack.elapsed > .74 && attack.elapsed < 1.16;
    boss.vx = first || second ? attack.direction * 470 : 0;
  }
}
export function updateActiveAttack(boss, world, player, arena, dt) {
  const attack = boss.currentAttack;
  if (!attack) return { finished: true, expose: true };
  attack.elapsed += dt;
  spawnTimedProjectiles(boss, world, arena);
  let impact = false;
  if (["slam", "targetedLeap"].includes(attack.id)) {
    const duration = attack.id === "targetedLeap" ? .72 : .52;
    const progress = Math.min(1, attack.elapsed / duration);
    const target = clamp(attack.targetX - boss.w / 2, arena.left + 35, arena.right - boss.w - 35);
    boss.x = attack.startX + (target - attack.startX) * progress;
    boss.y = attack.startY + (floorY(arena) - boss.h - attack.startY) * progress - Math.sin(progress * Math.PI) * (attack.id === "targetedLeap" ? 170 : 85);
    if (progress === 1 && !attack.landed) {
      attack.landed = true; impact = true;
      zone(world, attack.targetX - 120, floorY(arena) - 23, 240, 23, .24, "shockwave");
      emitCombatEffect(world, "fx-boss-sweep-slam", attack.targetX, floorY(arena), { scale: 1.2 });
    }
  }
  if (["charge", "dashCombo"].includes(attack.id)) {
    boss.x += boss.vx * dt;
    const column = (world.props || []).find((prop) => !prop.dead && Math.abs((prop.x + prop.w / 2) - (boss.x + boss.w / 2)) < (prop.w + boss.w) / 2 && prop.type === "breakableColumn");
    if (column) {
      column.dead = true; boss.vx = 0;
      emitCombatEffect(world, "fx-rubble-destruction", column.x, column.y + column.h, { scale: 1.2 });
      return { finished: true, expose: true, impact: true };
    }
    if (boss.x <= arena.left + 22 || boss.x + boss.w >= arena.right - 22) {
      boss.x = clamp(boss.x, arena.left + 22, arena.right - boss.w - 22); boss.vx = 0;
      return { finished: true, expose: true, impact: true };
    }
  }
  return { finished: attack.elapsed >= attack.activeDuration, expose: attack.exposeOnFinish, impact };
}
export function updateBossHazards(world, player, dt) { updateCombatProjectiles(world, player, dt, "bossHazards"); }
