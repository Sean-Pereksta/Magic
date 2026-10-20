import { clamp, overlaps } from "../common.js";
import { damagePlayer, surfaceY } from "../physics.js";

// Every attack commits to its target during a visible tell. Ordinary enemies
// remain slower than the player, allowing a deliberate retreat or jump.
export const ENEMY_TYPES = Object.freeze({
  wanderer: { w: 34, h: 54, hp: 1, speed: 68, detection: 300, range: 57, tell: .64, style: "melee" },
  hound: { w: 48, h: 32, hp: 1, speed: 146, detection: 410, range: 140, tell: .58, style: "pounce", jump: 650 },
  legionary: { w: 38, h: 60, hp: 2, speed: 80, detection: 370, range: 74, tell: .65, style: "melee", armor: true },
  "shield-bearer": { w: 42, h: 62, hp: 2, speed: 63, detection: 350, range: 86, tell: .8, style: "shield", shield: true },
  javelin: { w: 34, h: 56, hp: 1, speed: 86, detection: 550, range: 400, tell: .8, style: "ranged", projectile: "javelin" },
  "temple-guard": { w: 50, h: 72, hp: 3, speed: 55, detection: 420, range: 115, tell: .9, style: "slam", armor: true },
  zealot: { w: 34, h: 52, hp: 1, speed: 150, detection: 240, range: 180, tell: .75, style: "ambush", jump: 680 },
  "stone-guardian": { w: 62, h: 76, hp: 4, speed: 42, detection: 420, range: 145, tell: 1, style: "slam", armor: true },
  imp: { w: 34, h: 38, hp: 1, speed: 108, detection: 370, range: 125, tell: .62, style: "bound", jump: 590 },
  "brim-hound": { w: 54, h: 36, hp: 2, speed: 156, detection: 450, range: 170, tell: .7, style: "pounce", jump: 630, flame: true },
  "fire-skull": { w: 36, h: 40, hp: 2, speed: 76, detection: 570, range: 430, tell: .86, style: "ranged", flying: true, projectile: "fire" },
  wraith: { w: 42, h: 66, hp: 2, speed: 85, detection: 430, range: 155, tell: .85, style: "phase", flying: true },
  "cave-bat": { w: 40, h: 30, hp: 1, speed: 94, detection: 350, range: 235, tell: .7, style: "dive", flying: true },
  vulture: { w: 62, h: 38, hp: 2, speed: 100, detection: 500, range: 320, tell: .95, style: "dive", flying: true },
  "leaping-scorpion": { w: 42, h: 30, hp: 2, speed: 75, detection: 390, range: 195, tell: .78, style: "pounce", jump: 780 },
  "tempest-sentinel": { w: 50, h: 76, hp: 4, speed: 92, detection: 500, range: 145, tell: .85, style: "combo", armor: true },
});
const MAX_PROJECTILES = 64, MAX_EFFECTS = 72;
const cx = (e) => e.x + e.w / 2, cy = (e) => e.y + e.h / 2;

export function emitCombatEffect(world, type, x, y, options = {}) {
  if (!world) return null;
  world.effects ||= [];
  if (world.effects.length >= MAX_EFFECTS) world.effects.shift();
  world.effectSerial = (world.effectSerial || 0) + 1;
  const effect = { id: `effect-${world.effectSerial}`, type, x, y, time: 0, duration: .45, scale: 1, facing: 1, ...options };
  world.effects.push(effect);
  return effect;
}
export function updateCombatEffects(world, dt) {
  const effects = world.effects || [];
  let write = 0;
  for (const effect of effects) {
    effect.time = (effect.time || 0) + dt;
    if (effect.time < (effect.duration || .45)) effects[write++] = effect;
  }
  effects.length = write;
}

// Projectiles are reused with a strict live/spare cap for each combat system.
export function spawnCombatProjectile(world, data, key = "enemyProjectiles") {
  world[key] ||= [];
  if (world[key].length >= MAX_PROJECTILES) return null;
  world.combatPools ||= {};
  const pool = world.combatPools[key] ||= [];
  const projectile = pool.pop() || {};
  for (const field of Object.keys(projectile)) delete projectile[field];
  world.projectileSerial = (world.projectileSerial || 0) + 1;
  Object.assign(projectile, { id: `projectile-${world.projectileSerial}`, type: "orb", w: 16, h: 16, vx: 0, vy: 0, gravity: 0, damage: 1, life: 3, active: true, ...data });
  world[key].push(projectile);
  return projectile;
}
function sweptOverlap(shot, player, previousX, previousY) {
  const dx = shot.x - previousX, dy = shot.y - previousY;
  let enter = 0, leave = 1;
  for (const [position, velocity, minimum, maximum] of [
    [previousX, dx, player.x - shot.w, player.x + player.w],
    [previousY, dy, player.y - shot.h, player.y + player.h],
  ]) {
    if (Math.abs(velocity) < .00001) {
      if (position < minimum || position > maximum) return false;
    } else {
      const a = (minimum - position) / velocity, b = (maximum - position) / velocity;
      enter = Math.max(enter, Math.min(a, b));
      leave = Math.min(leave, Math.max(a, b));
      if (enter > leave) return false;
    }
  }
  return leave >= 0 && enter <= 1;
}
export function updateCombatProjectiles(world, player, dt, key = "enemyProjectiles") {
  const list = world[key] ||= [];
  world.combatPools ||= {};
  const pool = world.combatPools[key] ||= [];
  let write = 0;
  for (const shot of list) {
    const previousX = shot.x, previousY = shot.y;
    shot.life -= dt;
    shot.vy += (shot.gravity || 0) * dt;
    shot.x += shot.vx * dt;
    shot.y += shot.vy * dt;
    if (shot.warning > 0) shot.warning = Math.max(0, shot.warning - dt);
    else if (shot.active && sweptOverlap(shot, player, previousX, previousY)) {
      damagePlayer(player, shot.damage || 1, { x: (Math.sign(cx(player) - shot.x) || 1) * 220, y: -300 });
      shot.active = false;
      shot.life = Math.min(shot.life, .1);
      emitCombatEffect(world, "fx-damage-burst", cx(player), cy(player), { scale: .7 });
    }
    if (shot.terrain && (world.platforms || []).some((s) => !s.dead && !s.open && !s.hidden && !s.oneWay && overlaps(shot, s))) shot.life = 0;
    const limitY = Math.max(world.height || 720, (player.y || 0) + 1100);
    if (shot.life <= 0 || shot.y > limitY || shot.y < -1400 || Math.abs(shot.x - player.x) > 2200) {
      if (pool.length < MAX_PROJECTILES) pool.push(shot);
    } else list[write++] = shot;
  }
  list.length = write;
}
function setState(enemy, state, duration = 0) {
  enemy.aiState = state;
  enemy.stateTime = duration;
  const animation = state === "tell" || state === "attack" ? "attack" : state === "pursue" || state === "patrol" ? "move" : state === "hurt" ? "hurt" : state === "dead" ? "death" : "idle";
  if (enemy.state !== animation) enemy.animTime = 0;
  enemy.state = animation;
}
function initializeEnemy(enemy) {
  if (enemy.combatReady) return;
  const d = ENEMY_TYPES[enemy.type] || ENEMY_TYPES.wanderer;
  enemy.type = ENEMY_TYPES[enemy.type] ? enemy.type : "wanderer";
  const previousWidth = enemy.w || d.w, previousHeight = enemy.h || d.h;
  if (!enemy.customSize) {
    enemy.x += (previousWidth - d.w) / 2;
    enemy.y += previousHeight - d.h;
    enemy.w = d.w; enemy.h = d.h;
  } else { enemy.w ||= d.w; enemy.h ||= d.h; }
  enemy.hp = enemy.customHealth ? (enemy.hp || d.hp) : enemy.guardian
    ? Math.max(enemy.hp || 0, d.hp + 3) : d.hp + (enemy.elite ? 2 : 0);
  enemy.maxHp = enemy.hp;
  enemy.vx ||= 0; enemy.vy ||= 0;
  enemy.facing = enemy.patrol || 1; enemy.patrol ||= 1;
  enemy.homeX = enemy.x; enemy.homeY = enemy.y;
  enemy.age = 0; enemy.animTime = 0; enemy.cooldown = .65;
  enemy.hitCooldown = 0; enemy.attackCount = 0; enemy.combatReady = true;
  setState(enemy, enemy.dead ? "dead" : d.style === "ambush" ? "ambush" : "patrol");
  if (enemy.dead) enemy.fade ??= 0;
}
export function damageEnemy(enemy, amount = 1, source = "attack", world = null) {
  if (!enemy || enemy.dead) return false;
  initializeEnemy(enemy);
  if (enemy.hitCooldown > 0) return false;
  const details = typeof source === "string" ? { type: source } : source;
  const d = ENEMY_TYPES[enemy.type];
  const recovering = ["recover", "hurt"].includes(enemy.aiState);
  const fromFront = Number.isFinite(details.x) && Math.sign(details.x - cx(enemy)) === enemy.facing;
  if (d.shield && fromFront && !recovering && details.type !== "stomp") {
    emitCombatEffect(world, "fx-shield-pulse", cx(enemy), cy(enemy), { scale: .7 });
    return false;
  }
  let damage = amount;
  if (d.armor && !recovering && !["holy-light", "fire-rain", "stomp"].includes(details.type)) damage *= .5;
  if (enemy.type === "wraith" && details.type === "holy-light") damage *= 2;
  enemy.hp = Math.max(0, enemy.hp - damage);
  enemy.hitCooldown = .24; enemy.flash = .16; enemy.telegraph = null;
  enemy.lastDamageSource = details.type;
  emitCombatEffect(world, "fx-gold-impact", cx(enemy), cy(enemy), { scale: .65 });
  if (enemy.hp <= 0) {
    enemy.dead = true; enemy.fade = .6; enemy.vx = 0;
    setState(enemy, "dead", .6);
    emitCombatEffect(world, "fx-damage-burst", cx(enemy), cy(enemy), { scale: .9 });
    world?.deadEnemyIds?.add?.(enemy.id);
    world?.persistent?.dead?.add?.(enemy.id);
    world?._state?.dead?.add?.(enemy.id);
  } else {
    setState(enemy, "hurt", .25);
    enemy.vx = Number.isFinite(details.x) ? (Math.sign(cx(enemy) - details.x) || 1) * 150 : 0;
  }
  return true;
}
function surfaces(world) { return [...(world.platforms || []), ...(world.movers || [])].filter(s => !s.dead && !s.open && !s.hidden); }
function surfaceAt(world, x, bottom, maxDrop = 80) {
  let best = null;
  for (const s of surfaces(world)) {
    const top = surfaceY(s, x);
    if (x < s.x + 2 || x > s.x + s.w - 2 || top < bottom - 24 || top > bottom + maxDrop) continue;
    if (!best || top < surfaceY(best, x)) best = s;
  }
  return best;
}
function lineOfSight(world, enemy, player) {
  const left = Math.min(cx(enemy), cx(player)), right = Math.max(cx(enemy), cx(player));
  const rayY = Math.min(cy(enemy), cy(player));
  return !(world.platforms || []).some((s) => !s.dead && !s.open && !s.hidden && !s.oneWay && s.x > left && s.x + s.w < right && s.y < rayY && s.y + s.h > rayY);
}
function moveGround(world, enemy, dt, chasing) {
  const ridden = enemy.onGround && (world.movers || []).find(s => s.id === enemy.groundId && !s.dead);
  if (ridden) { enemy.x += ridden.dx || 0; enemy.y += ridden.dy || 0; }
  const d = ENEMY_TYPES[enemy.type], previousBottom = enemy.y + enemy.h;
  const previouslyGrounded = enemy.onGround;
  const nextX = enemy.x + enemy.vx * dt, direction = Math.sign(enemy.vx) || enemy.facing;
  const edgeX = nextX + (direction > 0 ? enemy.w + 16 : -16);
  const ahead = surfaceAt(world, edgeX, previousBottom, 90);
  const obstruction = (world.platforms || []).find((s) => !s.dead && !s.open && !s.hidden && !s.oneWay && s.kind !== "slope" && overlaps({ x: nextX, y: enemy.y + 5, w: enemy.w, h: enemy.h - 10 }, s));
  const jumpTarget = chasing && !ahead && surfaces(world).find((s) => {
    const gap = direction > 0 ? s.x - (enemy.x + enemy.w) : enemy.x - (s.x + s.w);
    return !s.dead && gap >= 0 && gap <= 150 && s.y >= previousBottom - 100 && s.y <= previousBottom + 70;
  });
  if (enemy.onGround && (obstruction || jumpTarget) && d.style !== "slam" && d.style !== "shield") {
    enemy.vy = -(d.jump || 630); enemy.onGround = false;
    enemy.vx = direction * Math.max(Math.abs(enemy.vx), 150);
  } else if (enemy.onGround && !ahead && enemy.aiState !== "attack") {
    enemy.vx = 0; enemy.patrol *= -1;
    if (!chasing) enemy.facing = enemy.patrol;
  }
  enemy.x += enemy.vx * dt;
  for (const s of surfaces(world)) {
    if (s.oneWay || s.kind === "slope" || !overlaps(enemy, s)) continue;
    if (previousBottom > s.y + 6) { enemy.x = direction > 0 ? s.x - enemy.w : s.x + s.w; enemy.vx = 0; }
  }
  enemy.vy = Math.min(1100, enemy.vy + 2050 * dt); enemy.y += enemy.vy * dt; enemy.onGround = false; enemy.groundId = null;
  for (const s of surfaces(world)) {
    const top = surfaceY(s, clamp(cx(enemy), s.x, s.x + s.w));
    const slopeSnap = s.kind === "slope" && previouslyGrounded && Math.abs(previousBottom - top) < 24;
    if (enemy.vy < 0 || (!slopeSnap && (previousBottom > top + 7 || enemy.y + enemy.h < top))) continue;
    if (enemy.x + enemy.w > s.x + 2 && enemy.x < s.x + s.w - 2) {
      enemy.y = top - enemy.h; enemy.vy = 0; enemy.onGround = true; enemy.groundId = s.id; break;
    }
  }
}
function beginTell(enemy, player, world) {
  const d = ENEMY_TYPES[enemy.type];
  enemy.facing = Math.sign(cx(player) - cx(enemy)) || enemy.facing;
  enemy.targetX = cx(player); enemy.targetY = cy(player);
  enemy.originX = enemy.x; enemy.originY = enemy.y;
  enemy.vx = 0; enemy.vy = d.flying ? 0 : enemy.vy;
  enemy.attackCount++; enemy.hitPlayer = false;
  setState(enemy, "tell", d.tell); enemy.tellDuration = d.tell;
  const extent = d.style === "slam" ? d.range * 2 : d.range;
  enemy.telegraph = { x: d.style === "slam" ? cx(enemy) - extent / 2 : enemy.facing > 0 ? enemy.x + enemy.w : enemy.x - extent, y: enemy.y + enemy.h - 30, w: extent, h: 30, progress: 0 };
  if (["dive", "phase", "pounce", "ambush", "bound"].includes(d.style)) enemy.telegraph = { x: enemy.targetX - 28, y: enemy.targetY - 28, w: 56, h: 56, progress: 0 };
  emitCombatEffect(world, d.style === "ranged" ? "fx-radial-orb" : "fx-blessing-attack-arc", cx(enemy), cy(enemy), { duration: d.tell, scale: .4, facing: enemy.facing });
}
function startAttack(enemy, world) {
  const d = ENEMY_TYPES[enemy.type];
  enemy.telegraph = null; enemy.attackElapsed = 0;
  setState(enemy, "attack", d.style === "combo" ? .95 : d.style === "dive" ? (enemy.type === "vulture" ? .9 : .64) : .48);
  if (d.style === "ranged") {
    const dx = enemy.targetX - cx(enemy), dy = enemy.targetY - cy(enemy), distance = Math.max(1, Math.hypot(dx, dy));
    const angle=Math.atan2(dy,dx);
    for(const spread of enemy.elite?[-.18,0,.18]:[0])
      spawnCombatProjectile(world, { type: d.projectile, x: cx(enemy), y: cy(enemy), vx:Math.cos(angle+spread)*300, vy:Math.sin(angle+spread)*300, life: 2.6, terrain: true, owner: enemy.id });
  } else if (["pounce", "ambush", "bound"].includes(d.style)) {
    enemy.vx = enemy.facing * (d.style === "ambush" ? 300 : 270); enemy.vy = -(d.jump || 600) * .65; enemy.onGround = false;
  } else if (["dive", "phase"].includes(d.style)) {
    const dx = enemy.targetX - cx(enemy), dy = enemy.targetY - cy(enemy), distance = Math.max(1, Math.hypot(dx, dy));
    enemy.vx = dx / distance * 350; enemy.vy = dy / distance * 350;
  } else if (d.style === "slam") {
    const radius = d.range;
    if (enemy.type === "stone-guardian" || enemy.elite) {
      for (const direction of [-1, 1]) spawnCombatProjectile(world, { type: "shockwave", x: cx(enemy) - 18, y: enemy.y + enemy.h - 21, w: 36, h: 21, vx: direction * 220, life: 1.35, owner: enemy.id });
    } else spawnCombatProjectile(world, { type: "shockwave", x: cx(enemy) - radius, y: enemy.y + enemy.h - 22, w: radius * 2, h: 22, life: .22, owner: enemy.id });
    emitCombatEffect(world, "fx-ground-shockwave", cx(enemy), enemy.y + enemy.h, { scale: radius / 100 });
  } else enemy.vx = enemy.facing * (d.style === "shield" ? 160 : d.style === "combo" ? 190 : 95);
}
function attackHitbox(enemy) {
  const d = ENEMY_TYPES[enemy.type], reach = d.style === "combo" ? 76 : d.style === "shield" ? 38 : 28;
  return { x: enemy.facing > 0 ? enemy.x + enemy.w - 10 : enemy.x - reach, y: enemy.y + 10, w: reach + 10, h: enemy.h - 12 };
}
export function updateEnemies(world, player, dt) {
  dt = Math.min(.05, Math.max(0, dt)); world.enemies ||= [];
  for (const enemy of world.enemies) {
    initializeEnemy(enemy); enemy.animTime += dt;
    enemy.flash = Math.max(0, (enemy.flash || 0) - dt); enemy.hitCooldown = Math.max(0, enemy.hitCooldown - dt);
    if (enemy.dead) { enemy.fade = Math.max(0, (enemy.fade ?? .6) - dt); continue; }
    const d = ENEMY_TYPES[enemy.type], dx = cx(player) - cx(enemy), dy = cy(player) - cy(enemy), distance = Math.hypot(dx, dy);
    if (Math.abs(dx) > 1050 || Math.abs(dy) > 850) continue;
    enemy.age += dt; enemy.cooldown = Math.max(0, enemy.cooldown - dt); enemy.stateTime = Math.max(0, enemy.stateTime - dt);
    const detected = distance < d.detection && lineOfSight(world, enemy, player);
    if (detected) enemy.alertTime = 2.5; else enemy.alertTime = Math.max(0, (enemy.alertTime || 0) - dt);
    const chasing = enemy.alertTime > 0;
    if (enemy.aiState === "ambush") {
      enemy.vx = 0;
      if (detected && Math.abs(dx) < 210) beginTell(enemy, player, world);
    } else if (enemy.aiState === "tell") {
      if (enemy.telegraph) enemy.telegraph.progress = 1 - enemy.stateTime / enemy.tellDuration;
      if (enemy.stateTime <= 0) startAttack(enemy, world);
    } else if (enemy.aiState === "attack") {
      enemy.attackElapsed += dt;
      if (["leaping-scorpion", "zealot"].includes(enemy.type) && enemy.attackElapsed >= .24 && !enemy.stingFired) {
        enemy.stingFired = true;
        spawnCombatProjectile(world, { type: "thorn", x: cx(enemy), y: cy(enemy), vx: enemy.facing * 230, life: 1.25, owner: enemy.id, terrain: true });
      }
      if (d.style === "combo" && enemy.attackElapsed >= .48 && !enemy.comboFired) {
        enemy.comboFired = true; enemy.vx = 0;
        spawnCombatProjectile(world, { type: "light", x: cx(enemy), y: cy(enemy), vx: enemy.facing * 310, life: .9, owner: enemy.id });
        emitCombatEffect(world, "fx-blessing-attack-arc", cx(enemy), cy(enemy), { facing: enemy.facing });
      }
      if (enemy.stateTime <= 0) {
        if (enemy.type === "brim-hound") {
          emitCombatEffect(world, "fx-fire-eruption", enemy.originX + enemy.w / 2, enemy.originY + enemy.h, { scale: .65, duration: .7 });
          spawnCombatProjectile(world, { type: "eruption", x: enemy.originX, y: enemy.originY + enemy.h - 20, w: enemy.w, h: 20, warning: .3, life: .65, owner: enemy.id });
        }
        enemy.vx = 0; enemy.comboFired = false; enemy.stingFired = false;
        setState(enemy, "recover", d.style === "slam" || d.style === "combo" ? 1.25 : .85); enemy.cooldown = enemy.elite ? .95 : 1.15;
      }
    } else if (["recover", "hurt"].includes(enemy.aiState)) {
      enemy.vx *= Math.max(0, 1 - dt * 12);
      if (d.flying) enemy.vy = clamp((enemy.homeY - enemy.y) * 2, -110, 110);
      if (enemy.stateTime <= 0) setState(enemy, chasing ? "pursue" : "patrol");
    } else {
      setState(enemy, chasing ? "pursue" : "patrol");
      const direction = chasing ? Math.sign(dx) || enemy.facing : enemy.patrol;
      enemy.facing = direction;
      const rangedRetreat = d.style === "ranged" && distance < 175;
      enemy.vx = direction * d.speed * (rangedRetreat ? -.9 : 1) * (enemy.elite ? 1.12 : 1);
      if (!chasing && Math.abs(enemy.x - enemy.homeX) > 140) { enemy.patrol *= -1; enemy.homeX = enemy.x - enemy.patrol * 125; }
      const inRange = Math.abs(dx) < d.range && (d.flying || Math.abs(dy) < 95);
      if (detected && inRange && enemy.cooldown <= 0) beginTell(enemy, player, world);
      if (d.flying && enemy.aiState !== "tell") {
        const targetY = chasing ? player.y - (d.style === "dive" ? 125 : 65) : enemy.homeY;
        enemy.vy = clamp((targetY - enemy.y) * 2, -90, 90) + Math.sin(enemy.age * 3) * 18;
      }
    }
    if (d.flying) {
      enemy.x += enemy.vx * dt; enemy.y += enemy.vy * dt;
      const upperBound=world.layoutVersion>=3||enemy.guardian ? (world.bounds?.top??-700)+32 : 32;
      enemy.y = clamp(enemy.y, upperBound, (world.bounds?.bottom??world.height??720) - enemy.h - 60);
    } else moveGround(world, enemy, dt, chasing);
    if (enemy.guardian && enemy.guardBounds) {
      // Keep the encounter on its broad combat shelf, clear of the jump entry.
      const left=enemy.guardBounds.left,right=enemy.guardBounds.right-enemy.w;
      enemy.x=clamp(enemy.x,left,Math.max(left,right));
    }
    if (enemy.y > Math.max(world.height || 720, player.y + 900)) {
      enemy.dead = true; enemy.fade = 0;
      world?.deadEnemyIds?.add?.(enemy.id);world?.persistent?.dead?.add?.(enemy.id);world?._state?.dead?.add?.(enemy.id);
      continue;
    }
    if (!overlaps(enemy, player) && !(enemy.aiState === "attack" && overlaps(attackHitbox(enemy), player))) continue;
    const stomp = player.vy > 100 && player.y + player.h < enemy.y + enemy.h * .62;
    if (stomp) {
      if (damageEnemy(enemy, 1, { type: "stomp", x: cx(player) }, world)) player.vy = -610;
    } else if ((player.dashTimer || 0) > 0 || (player.attackTimer || 0) > 0) {
      damageEnemy(enemy, 1, { type: (player.dashTimer || 0) > 0 ? "dash" : "attack", x: cx(player) }, world);
    } else if (!["recover", "hurt", "ambush"].includes(enemy.aiState) && !enemy.hitPlayer) {
      const hit = damagePlayer(player, 1, { x: (Math.sign(dx) || 1) * 280, y: -380 });
      if (enemy.aiState === "attack") enemy.hitPlayer = hit;
    }
  }
  updateCombatProjectiles(world, player, dt);
}
export function activeEnemyCount(world, cameraX, viewWidth) {
  return (world.enemies || []).filter((e) => !e.dead && e.x + e.w > cameraX - 120 && e.x < cameraX + viewWidth + 120).length;
}



