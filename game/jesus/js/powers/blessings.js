export const BLESSINGS = ["shield", "sandals", "bread", "holyLight", "fireRain", "wings"];

export function grantBlessing(player, type) {
  if (!type) return;
  if (type === "bread") {
    player.powers.bread = true;
    player.hearts = Math.min(player.maxHearts, player.hearts + 2);
  } else if (["holyLight", "fireRain"].includes(type)) {
    player.powers[type] = Math.max(player.powers[type] || 0, type === "holyLight" ? 5 : 3);
  } else {
    player.powers[type] = true;
  }
}

export function collectPickups(world, player) {
  const collected = [];
  for (const pickup of world.pickups) {
    if (pickup.collected) continue;
    if (player.x < pickup.x + pickup.w && player.x + player.w > pickup.x &&
        player.y < pickup.y + pickup.h && player.y + player.h > pickup.y) {
      pickup.collected = true;
      grantBlessing(player, pickup.type);
      collected.push(pickup.type);
    }
  }
  return collected;
}

export function useHolyLight(player, bossController) {
  if (!bossController?.boss || (player.powers.holyLight || 0) <= 0) return false;
  const boss = bossController.boss;
  const distance = Math.hypot((boss.x + boss.w / 2) - (player.x + player.w / 2), (boss.y + boss.h / 2) - (player.y + player.h / 2));
  if (distance > 430) return false;
  const hit = bossController.damage(2, "holy-light");
  if (hit) player.powers.holyLight--;
  return hit;
}

export function useFireRain(player, bossController) {
  if (!bossController?.boss || (player.powers.fireRain || 0) <= 0) return false;
  const hit = bossController.damage(3, "fire-rain");
  if (hit) player.powers.fireRain--;
  return hit;
}
