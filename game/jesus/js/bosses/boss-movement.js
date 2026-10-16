import { clamp } from "../common.js";

function directionToPlayer(boss, player) { return Math.sign((player.x + player.w / 2) - (boss.x + boss.w / 2)) || boss.facing || -1; }
export function chooseMovement(boss, player, arena) {
  const distance = Math.abs(player.x - boss.x), close = distance < 175;
  const choices = boss.definition.movement;
  if (boss.health / boss.maxHealth < .35 && choices.includes("enragedChase")) return "enragedChase";
  if (close && choices.includes("retreat")) return "retreat";
  if (choices.includes("platformShift") && boss.moveCount % 3 === 2) return "platformShift";
  if (choices.includes("hover")) return boss.moveCount % 2 === 0 ? "hover" : "circle";
  if (distance > 550 && choices.includes("wallToWall")) return "wallToWall";
  if (choices.includes("pursue")) return "pursue";
  return choices[boss.moveCount % choices.length] || "stationaryCast";
}
export function updateBossMovement(boss, player, arena, dt) {
  boss.moveTimer -= dt;
  if (boss.moveTimer <= 0) {
    boss.movement = chooseMovement(boss, player, arena); boss.moveTimer = .9; boss.moveCount++;
  }
  const direction = directionToPlayer(boss, player), speed = 105 * boss.phase.speed;
  boss.facing = direction;
  switch (boss.movement) {
    case "pursue": boss.vx = direction * speed; break;
    case "enragedChase": boss.vx = direction * speed * 1.4; break;
    case "retreat": boss.vx = -direction * speed * .8; break;
    case "wallToWall":
    case "shortDash": boss.vx = direction * speed * 1.3; break;
    case "hover":
    case "circle":
    case "platformShift": {
      const targetX = boss.movement === "platformShift" ? [arena.left + 300, (arena.left + arena.right) / 2, arena.right - 300][boss.moveCount % 3] : player.x + direction * 320;
      boss.vx = clamp((targetX - boss.x) * .7, -speed, speed);
      const targetY = (arena.floorY || 468) - boss.h - 130 + Math.sin(boss.age * 1.8) * 38;
      boss.y += clamp(targetY - boss.y, -130 * dt, 130 * dt); boss.vy = 0; boss.onGround = false;
      break;
    }
    default: boss.vx = direction * speed * .45; break;
  }
  if (!["hover", "circle", "platformShift"].includes(boss.movement)) {
    boss.vy = Math.min(1000, boss.vy + 2100 * dt); boss.y += boss.vy * dt;
    const floor = (arena.floorY || 468) - boss.h;
    if (boss.y >= floor) { boss.y = floor; boss.vy = 0; boss.onGround = true; }
  }
  boss.x = clamp(boss.x + boss.vx * dt, arena.left + 30, arena.right - boss.w - 30);
}
