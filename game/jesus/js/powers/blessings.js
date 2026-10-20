import { overlaps } from "../common.js";
import { BOOST_SECONDS } from "./boosts.js";
import { damageEnemy, emitCombatEffect, updateCombatEffects } from "../enemies/enemies.js";

export const BLESSINGS = ["shield", "sandals", "bread", "holyLight", "fireRain", "wings", "water", "wind"];
const center = (entity) => ({ x: entity.x + entity.w / 2, y: entity.y + entity.h / 2 });
const distance = (a, b) => Math.hypot(center(a).x - center(b).x, center(a).y - center(b).y);

function initializeBlessings(player) {
  player.powers ||= {};
  player.boosts ||= {water:0,wind:0};
  if (player.blessingsReady) return;
  player.blessingsReady = true;
  const restoringReserve = Number.isFinite(player.holyLightCapacity);
  player.holyLightCapacity = Math.max(3, Number(player.holyLightCapacity) || 0, Number(player.powers.holyLight) || 0);
  player.powers.holyLight = restoringReserve ? Math.min(player.holyLightCapacity, Number(player.powers.holyLight) || 0) : player.holyLightCapacity;
  player.holyLightRegen = 0;
  player.abilityCooldown = 0;
  player.shieldUnlocked = Boolean(player.shieldUnlocked || player.powers.shield);
  player.shieldRecharge ||= 0;
  player.breadRecovery = 0;
  player.lastBlessingHearts = player.hearts;
}
export function grantBlessing(player, type) {
  initializeBlessings(player);
  if (!BLESSINGS.includes(type)) return;
  if (BOOST_SECONDS[type]) {
    // A fresh water pickup can rescue an airborne player; refreshing an active
    // boost must not refill already-spent jumps in the same flight.
    if(type === "water" && !(player.boosts.water > 0)) player.extraJumps = (player.extraJumps || 0) + 1;
    player.boosts[type] = BOOST_SECONDS[type];
  } else if (type === "bread") {
    player.powers.bread = true;
    player.hearts = Math.min(player.maxHearts, player.hearts + 2);
    player.breadRecovery = 0;
  } else if (type === "holyLight") {
    player.holyLightCapacity = 5;
    player.powers.holyLight = 5;
    player.holyLightRegen = 0;
  } else if (type === "fireRain") player.powers.fireRain = Math.max(Number(player.powers.fireRain) || 0, 3);
  else {
    player.powers[type] = true;
    if (type === "shield") { player.shieldUnlocked = true; player.shieldRecharge = 0; }
  }
}

// Root calls this once per fixed step, including boss fights. Movement benefits
// (sandals and wings) are applied by the player controller.
export function updateBlessings(player, world, dt) {
  initializeBlessings(player);
  for(const key of Object.keys(BOOST_SECONDS)) player.boosts[key] = Math.max(0,(player.boosts[key] || 0)-dt);
  player.abilityCooldown = Math.max(0, player.abilityCooldown - dt);
  if ((player.attackTimer || 0) <= 0) player.abilityState = null;
  const maxLight = player.holyLightCapacity || 3;
  if ((Number(player.powers.holyLight) || 0) < maxLight) {
    player.holyLightRegen += dt;
    if (player.holyLightRegen >= 5.5) {
      player.powers.holyLight = Math.min(maxLight, (Number(player.powers.holyLight) || 0) + 1);
      player.holyLightRegen -= 5.5;
    }
  } else player.holyLightRegen = 0;
  if (player.shieldUnlocked && !player.powers.shield) {
    player.shieldRecharge += dt;
    if (player.shieldRecharge >= 18) {
      player.powers.shield = true; player.shieldRecharge = 0;
      emitCombatEffect(world, "fx-shield-pulse", center(player).x, center(player).y, { scale: .9 });
    }
  } else player.shieldRecharge = 0;
  if (player.hearts < player.lastBlessingHearts) player.breadRecovery = 0;
  if (player.powers.bread && player.hearts > 0 && player.hearts < player.maxHearts) {
    player.breadRecovery += dt;
    if (player.breadRecovery >= 24) {
      player.hearts = Math.min(player.maxHearts, player.hearts + 1); player.breadRecovery = 0;
      emitCombatEffect(world, "fx-bread-healing-aura", center(player).x, player.y + player.h, { duration: 1.1 });
    }
  } else player.breadRecovery = 0;
  player.lastBlessingHearts = player.hearts;
  updateCombatEffects(world, dt);
}
export function collectPickups(world, player) {
  const collected = [];
  for (const pickup of world.pickups || []) {
    if (pickup.collected || pickup.locked || !overlaps(player, pickup)) continue;
    pickup.collected = true;
    grantBlessing(player, pickup.type);
    collected.push(pickup.type);
    const effect=pickup.type==='water'?'fx-water-splash':pickup.type==='bread'?'fx-bread-healing-aura':'fx-pickup-collect';
    emitCombatEffect(world, effect, pickup.x + pickup.w / 2, pickup.y + pickup.h / 2, { duration: .55, scale: .32 });
    world.collectedPickupIds?.add?.(pickup.id);
    world.persistent?.collected?.add?.(pickup.id);
    world._state?.collected?.add?.(pickup.id);
  }
  return collected;
}
function castAnimation(player, state) {
  player.state = state; player.abilityState = state; player.animTime = 0;
  player.attackTimer = .34;
  player.abilityCooldown = .6;
}

// Accept the old (player, bossController) signature as well as the world-aware
// version. An empty cast does not spend a charge: range and exposure matter.
export function useHolyLight(player, worldOrBoss, controller = null) {
  initializeBlessings(player);
  const oldSignature = worldOrBoss?.boss !== undefined && !worldOrBoss?.enemies;
  const world = oldSignature ? worldOrBoss.world : worldOrBoss;
  const bossController = oldSignature ? worldOrBoss : controller;
  if (player.abilityCooldown > 0 || (Number(player.powers.holyLight) || 0) < 1 || player.hearts <= 0) return false;
  const targets = (world?.enemies || []).filter((e) => !e.dead && distance(e, player) <= 300).sort((a, b) => distance(a, player) - distance(b, player)).slice(0, 4);
  let hits = 0;
  for (const target of targets) {
    if (damageEnemy(target, 2, { type: "holy-light", x: center(player).x }, world)) hits++;
  }
  const boss = bossController?.boss;
  if (boss && distance(boss, player) <= 430 && bossController.damage(2, "holy-light")) hits++;
  if (!hits) {
    // Feedback still shows that a guarded target resisted; the reserve stays.
    if (targets.length || (boss && distance(boss, player) <= 430)) emitCombatEffect(world, "fx-shield-pulse", center(player).x, center(player).y, { duration: .3, scale: .65 });
    return false;
  }
  player.powers.holyLight--;
  castAnimation(player, "holy-light");
  emitCombatEffect(world, "fx-holy-light-judgment", center(player).x, player.y + player.h, { duration: .65, scale: 1.2, facing: player.facing });
  return true;
}
export function useFireRain(player, worldOrBoss, controller = null) {
  initializeBlessings(player);
  const oldSignature = worldOrBoss?.boss !== undefined && !worldOrBoss?.enemies;
  const world = oldSignature ? worldOrBoss.world : worldOrBoss;
  const bossController = oldSignature ? worldOrBoss : controller;
  if (player.abilityCooldown > 0 || (Number(player.powers.fireRain) || 0) < 1 || player.hearts <= 0) return false;
  const targets = (world?.enemies || []).filter((e) => !e.dead && distance(e, player) <= 470).slice(0, 8);
  let hits = 0;
  for (const target of targets) {
    if (damageEnemy(target, 3, { type: "fire-rain", x: center(target).x }, world)) {
      hits++;
      emitCombatEffect(world, "fx-fire-meteor", center(target).x, target.y, { duration: .6, scale: .9 });
      emitCombatEffect(world, "fx-fire-eruption", center(target).x, target.y + target.h, { duration: .8 });
    }
  }
  const boss = bossController?.boss;
  if (boss && distance(boss, player) <= 520 && bossController.damage(3, "fire-rain")) {
    hits++;
    emitCombatEffect(world, "fx-fire-eruption", center(boss).x, boss.y + boss.h, { duration: .8, scale: 1.4 });
  }
  if (!hits) return false;
  player.powers.fireRain--;
  castAnimation(player, "fire-rain");
  return true;
}

// Fire pickups immediately change the existing action; light returns once the
// fire reserve is spent. Failed casts keep their charge.
export function useAbility(player, world, bossController, options = {}) {
  if ((Number(player.powers.fireRain) > 0 || options.rain) && useFireRain(player, world, bossController)) return "fireRain";
  return useHolyLight(player, world, bossController) ? "holyLight" : false;
}


