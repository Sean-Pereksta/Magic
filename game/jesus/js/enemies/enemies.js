import { overlaps } from "../common.js";
import { damagePlayer } from "../physics.js";

function supportingSurface(world, x, currentBottom) {
  let best = null;
  for (const platform of [...world.platforms, ...world.movers]) {
    if (platform.dead || x < platform.x || x > platform.x + platform.w) continue;
    if (platform.y < currentBottom - 24) continue;
    if (!best || platform.y < best.y) best = platform;
  }
  return best;
}

export function updateEnemies(world, player, dt) {
  for (const enemy of world.enemies) {
    if (enemy.dead) continue;
    const speed = (enemy.elite ? 92 : 62) * (enemy.type.includes("hound") ? 1.35 : 1);
    const nextX = enemy.x + enemy.patrol * speed * dt;
    const support = supportingSurface(world, nextX + enemy.w / 2, enemy.y + enemy.h - 10);
    if (!support) enemy.patrol *= -1;
    else {
      enemy.x = nextX;
      enemy.y = support.y - enemy.h;
    }

    if (!overlaps(enemy, player)) continue;
    const stomp = player.vy > 160 && player.y + player.h < enemy.y + enemy.h * 0.48;
    if (stomp) {
      enemy.hp--;
      player.vy = -650;
      if (enemy.hp <= 0) enemy.dead = true;
    } else {
      damagePlayer(player, 1, { x: Math.sign(player.x - enemy.x) * 360, y: -410 });
    }
  }
  world.enemies = world.enemies.filter((enemy) => !enemy.dead || (enemy.fade = (enemy.fade || 0.3) - dt) > 0);
}

export function activeEnemyCount(world, cameraX, viewWidth) {
  return world.enemies.filter((enemy) => !enemy.dead && enemy.x + enemy.w > cameraX - 120 && enemy.x < cameraX + viewWidth + 120).length;
}

