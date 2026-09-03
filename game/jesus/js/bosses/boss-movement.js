import { clamp } from "../common.js";
import { VIEW } from "../config.js";

function directionToPlayer(boss, player) {
  return Math.sign((player.x + player.w / 2) - (boss.x + boss.w / 2)) || 1;
}

export function chooseMovement(boss, player, arena) {
  const distance = Math.abs(player.x - boss.x);
  const playerAbove = player.y + player.h < boss.y + 8;
  const close = distance < 150;
  const enraged = boss.health / boss.maxHealth < 0.35;
  const choices = boss.definition.movement;

  if (enraged && choices.includes("enragedChase")) return "enragedChase";
  if (playerAbove && choices.includes("highJump")) return "highJump";
  if (distance > 480 && choices.includes("wallToWall")) return "wallToWall";
  if (distance > 360 && choices.includes("targetedLeap")) return "targetedLeap";
  if (close && choices.includes("retreat")) return "retreat";
  if (choices.includes("platformShift") && boss.moveCount % 3 === 2) return "platformShift";
  if (choices.includes("hover") && boss.moveCount % 2 === 0) return "hover";
  if (choices.includes("pursue")) return "pursue";
  return choices[boss.moveCount % choices.length] || "stationaryCast";
}

export function updateBossMovement(boss, player, arena, dt) {
  boss.moveTimer -= dt;
  if (boss.moveTimer <= 0) {
    boss.movement = chooseMovement(boss, player, arena);
    boss.moveTimer = 0.75 + (boss.movement === "platformShift" ? 0.35 : 0);
    boss.moveCount++;
    if (["targetedLeap", "highJump"].includes(boss.movement) && boss.onGround) {
      boss.vy = boss.movement === "highJump" ? -900 : -720;
      boss.vx = directionToPlayer(boss, player) * (boss.movement === "targetedLeap" ? 390 : 170);
      boss.onGround = false;
    }
  }

  const direction = directionToPlayer(boss, player);
  const speed = 115 * boss.phase.speed;
  switch (boss.movement) {
    case "pursue": boss.vx = direction * speed; break;
    case "enragedChase": boss.vx = direction * speed * 1.65; break;
    case "retreat": boss.vx = -direction * speed * 1.15; break;
    case "shortDash": boss.vx = direction * speed * 2.2; break;
    case "wallToWall": boss.vx = direction * speed * 1.45; break;
    case "hover":
    case "circle":
      boss.vx = direction * speed * 0.65;
      boss.y = 225 + Math.sin(boss.age * 2.2) * 72;
      boss.vy = 0;
      boss.onGround = false;
      break;
    case "platformShift": {
      const targets = [arena.left + 250, (arena.left + arena.right) / 2, arena.right - 300];
      const target = targets[boss.moveCount % targets.length];
      boss.vx = Math.sign(target - boss.x) * speed * 1.4;
      boss.y = 235 + Math.sin(boss.age * 3) * 30;
      boss.vy = 0;
      boss.onGround = false;
      break;
    }
    case "stationaryCast": boss.vx *= 0.82; break;
    default: break;
  }

  if (!["hover", "circle", "platformShift"].includes(boss.movement)) {
    boss.vy += 2100 * dt;
    boss.y += boss.vy * dt;
    const floor = VIEW.floorY - boss.h;
    if (boss.y >= floor) {
      boss.y = floor;
      boss.vy = 0;
      boss.onGround = true;
    }
  }
  boss.x = clamp(boss.x + boss.vx * dt, arena.left + 24, arena.right - boss.w - 24);
}

