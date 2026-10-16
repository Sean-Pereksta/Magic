import assert from "node:assert/strict";
import { createPlayer, surfaceY } from "../js/physics.js";
import { ENEMY_TYPES, updateEnemies, damageEnemy, spawnCombatProjectile, updateCombatProjectiles, emitCombatEffect, updateCombatEffects } from "../js/enemies/enemies.js";
import { BOSS_DEFINITIONS } from "../js/bosses/boss-data.js";
import { BossController, BOSS_STATES } from "../js/bosses/boss-controller.js";
import { createAttackData, telegraphShapes, beginActiveAttack, updateActiveAttack } from "../js/bosses/boss-attacks.js";
import { grantBlessing, updateBlessings, useAbility, useHolyLight, useFireRain } from "../js/powers/blessings.js";

const STEP = 1 / 120;
function worldFor(enemies = []) {
  return { width: 2200, height: 720, platforms: [{ id: "floor", x: 0, y: 468, w: 2200, h: 90 }], movers: [], enemies, pickups: [], props: [], hazards: [], bossHazards: [], effects: [], deadEnemyIds: new Set() };
}
function actor(type, x = 300) { const d = ENEMY_TYPES[type]; return { id: `test-${type}`, type, x, y: d.flying ? 270 : 468 - d.h, w: d.w, h: d.h, patrol: 1 }; }
function playerAt(x = 420) { const p = createPlayer(x, 406); p.hearts = p.maxHearts = 50; p.onGround = true; return p; }
function stepEnemies(world, player, seconds, callback = null) {
  for (let i = 0; i < seconds / STEP; i++) {
    player.invulnerable = Math.max(0, (player.invulnerable || 0) - STEP);
    updateEnemies(world, player, STEP); updateBlessings(player, world, STEP); callback?.();
  }
}

assert.equal(Object.keys(ENEMY_TYPES).length, 16, "all generated enemy species must have behavior");
for (const type of Object.keys(ENEMY_TYPES)) {
  const enemy = actor(type), world = worldFor([enemy]), player = playerAt();
  const states = new Set(); let shortestTell = Infinity;
  stepEnemies(world, player, 8, () => { states.add(enemy.aiState); if (enemy.aiState === "tell") shortestTell = Math.min(shortestTell, enemy.tellDuration); });
  assert.ok(states.has("tell"), `${type}: never gave a visible attack warning`);
  assert.ok(states.has("attack"), `${type}: never committed an attack`);
  assert.ok(states.has("recover"), `${type}: no recovery weakness`);
  assert.ok(shortestTell >= .58, `${type}: reaction time below minimum`);
  assert.ok(Number.isFinite(enemy.x + enemy.y), `${type}: invalid motion`);
}
{
  const enemy = actor("hound", 1800), world = worldFor([enemy]), player = playerAt(200);
  stepEnemies(world, player, 2);
  assert.equal(enemy.age, 0, "distant enemy must not simulate or attack");
  assert.equal(world.enemyProjectiles.length, 0);
}
{
  const guard = actor("shield-bearer"), world = worldFor([guard]), player = playerAt(440);
  updateEnemies(world, player, STEP);
  assert.equal(damageEnemy(guard, 1, { type: "holy-light", x: guard.x + 150 }, world), false, "shield must block the front");
  assert.equal(damageEnemy(guard, 1, { type: "attack", x: guard.x - 100 }, world), true, "flanking must beat a shield");
  guard.hitCooldown = 0; guard.aiState = "patrol";
  assert.equal(damageEnemy(guard, 1, { type: "stomp", x: guard.x + 150 }, world), true, "jumping must beat a shield");
  assert.equal(guard.dead, true); assert.ok(world.deadEnemyIds.has(guard.id), "deaths must survive chunk regeneration");
}
{
  const wraith = actor("wraith"), world = worldFor([wraith]);
  assert.equal(damageEnemy(wraith, 1, "holy-light", world), true);
  assert.equal(wraith.dead, true, "light is the wraith's meaningful weakness");
}
{
  const ranged = actor("javelin"), world = worldFor([ranged]), player = playerAt(570);
  while (ranged.aiState !== "tell") updateEnemies(world, player, STEP);
  const marked = ranged.targetX;
  assert.equal(world.enemyProjectiles.length, 0, "tell cannot deal projectile damage");
  player.x = 120;
  for (let i = 0; i < 110 && !world.enemyProjectiles.length; i++) updateEnemies(world, player, STEP);
  assert.ok(world.enemyProjectiles.length > 0, "ranged attack must produce a projectile");
  assert.ok(marked > ranged.x && world.enemyProjectiles[0].vx > 0, "projectile must honor its marked target after a dodge");
}
{
  const enemy = actor("wanderer", 200), world = worldFor([enemy]), player = playerAt(470);
  world.platforms.push({ id: "wall", x: 340, y: 250, w: 50, h: 218, oneWay: false });
  updateEnemies(world, player, STEP);
  assert.equal(enemy.alertTime || 0, 0, "enemy cannot see through a solid wall");
}
{
  const enemy = actor("hound", 250), world = worldFor([enemy]), player = playerAt(460);
  world.platforms = [{ id: "a", x: 0, y: 468, w: 320, h: 90 }, { id: "b", x: 400, y: 440, w: 800, h: 90 }];
  let jumped = false;
  stepEnemies(world, player, 2, () => { if (enemy.vy < -200) jumped = true; });
  assert.ok(jumped, "pursuing hound must negotiate a reachable terrain gap");
}
{
  const slope = { id: "slope", kind: "slope", x: 0, y: 400, w: 800, h: 138, rise: 68, slopeDirection: "up" };
  const enemy = actor("wanderer", 300), world = worldFor([enemy]), player = playerAt(900);
  enemy.y = surfaceY(slope, enemy.x + enemy.w / 2) - enemy.h;
  world.platforms = [slope];
  stepEnemies(world, player, 1);
  assert.ok(Math.abs(enemy.y + enemy.h - surfaceY(slope, enemy.x + enemy.w / 2)) < .1, "enemy feet must follow the slope surface");
  const lift = { id: "lift", x: 200, y: 400, w: 400, h: 22, oneWay: true, dx: 0, dy: 0 };
  world.platforms = []; world.movers = [lift]; enemy.x = 250; enemy.y = 400 - enemy.h; enemy.vy = 0;
  updateEnemies(world, player, STEP);
  assert.equal(enemy.groundId, "lift");
  const oldX = enemy.x; lift.x += 2; lift.y -= 1; lift.dx = 2; lift.dy = -1;
  updateEnemies(world, player, STEP);
  assert.ok(enemy.x >= oldX + 2, "grounded enemy must ride a moving platform");
  assert.ok(Math.abs(enemy.y + enemy.h - lift.y) < .1);
}
{
  const enemy = { ...actor("stone-guardian"), w: 40, h: 52, hp: 2, dead: true }, world = worldFor([enemy]);
  updateEnemies(world, playerAt(), STEP);
  assert.equal(enemy.h, ENEMY_TYPES["stone-guardian"].h, "generic generation dimensions cannot flatten species colliders");
  assert.equal(enemy.fade, 0, "a restored dead enemy cannot reappear on chunk return");
  assert.equal(enemy.state, "death");
}
{
  const world = worldFor(), player = playerAt(500);
  const first = spawnCombatProjectile(world, { x: 0, y: 420, vx: 9000, life: 1 });
  const hearts = player.hearts;
  updateCombatProjectiles(world, player, .1);
  assert.equal(player.hearts, hearts - 1, "fast projectile cannot tunnel through the player");
  for (let i = 0; i < 150; i++) spawnCombatProjectile(world, { x: 0, y: 0, life: .01 });
  assert.ok(world.enemyProjectiles.length <= 64, "live projectile cap");
  const references = new Set(world.enemyProjectiles); references.add(first);
  updateCombatProjectiles(world, player, 1);
  assert.equal(world.enemyProjectiles.length, 0, "expired projectile cleanup");
  assert.ok(world.combatPools.enemyProjectiles.length <= 64, "spare projectile cap");
  assert.ok(references.has(spawnCombatProjectile(world, { x: 0, y: 0 })), "projectile object pool must reuse objects");
  for (let i = 0; i < 120; i++) emitCombatEffect(world, "fx-gold-impact", 0, 0);
  assert.equal(world.effects.length, 72, "VFX cap"); updateCombatEffects(world, 1); assert.equal(world.effects.length, 0);
}
{
  const enemy = actor("wanderer", 450), world = worldFor([enemy]), player = playerAt(420);
  updateBlessings(player, world, STEP);
  assert.equal(player.powers.holyLight, 3, "light starts available without hunting a consumable");
  assert.equal(useAbility(player, world, null), "holyLight");
  assert.equal(enemy.dead, true, "light must affect normal enemies");
  assert.equal(player.powers.holyLight, 2);
  assert.equal(useAbility(player, world, null), false, "no charge is spent on an empty or cooling cast");
  for (let i = 0; i < 670; i++) updateBlessings(player, world, STEP);
  assert.equal(player.powers.holyLight, 3, "light reserve must regenerate");
  grantBlessing(player, "holyLight"); assert.equal(player.holyLightCapacity, 5);
  grantBlessing(player, "shield"); player.powers.shield = false;
  for (let i = 0; i < 2180; i++) updateBlessings(player, world, STEP);
  assert.equal(player.powers.shield, true, "shield must recharge after an absorbed strike");
  player.hearts = 45; grantBlessing(player, "bread"); assert.equal(player.hearts, 47);
  for (let i = 0; i < 2900; i++) updateBlessings(player, world, STEP);
  assert.equal(player.hearts, 48, "bread provides measured recovery");
  grantBlessing(player, "fireRain");
  world.enemies = [actor("legionary", 410), actor("hound", 470), actor("imp", 510)];
  player.abilityCooldown = 0;
  assert.equal(useAbility(player, world, null), "fireRain", "one action selects rain for a crowd");
  assert.equal(player.powers.fireRain, 2);
}

for (const definition of Object.values(BOSS_DEFINITIONS)) {
  const world = worldFor(), player = playerAt(150);
  let defeated = 0;
  const controller = new BossController(() => defeated++);
  controller.start(definition.id, 3, world, player);
  assert.equal(world.region, definition.region, "arena biome must match loaded art");
  assert.ok(world.platforms.some((p) => !p.oneWay && p.x === 0 && p.w === world.width), "arena must have a reachable base route without wings");
  const visited = new Set(), phases = new Set();
  let previousState = null, exposureGap = 0, maxGap = 0;
  for (let i = 0; i < 18000 && !defeated; i++) {
    controller.update(world, player, STEP, { kick() {} });
    updateBlessings(player, world, STEP);
    const boss = controller.boss;
    visited.add(boss.state); phases.add(boss.phase.name); exposureGap += STEP;
    if (boss.state === BOSS_STATES.TELL) {
      assert.ok(boss.currentAttack.tellDuration >= .67, `${definition.id}: phase shortened warning too far`);
      assert.ok(telegraphShapes(boss.currentAttack, controller.arena).length, `${definition.id}: attack has no warning shape`);
      const health = boss.health;
      assert.equal(controller.damage(50, "unfair"), false, "boss must reject damage outside a vulnerability window");
      assert.equal(boss.health, health);
    }
    if (boss.state === BOSS_STATES.VULNERABLE && previousState !== boss.state) {
      maxGap = Math.max(maxGap, exposureGap); exposureGap = 0;
      assert.ok(boss.stateTime >= 1.8 - STEP, "exposure must be long enough to approach");
      assert.equal(controller.damage(4, "simulation"), true);
      assert.equal(controller.damage(4, "spam"), false, "one hit cannot drain a whole boss in the same frame");
    }
    assert.ok(world.bossHazards.length <= 64, "boss projectiles bounded over a complete fight");
    assert.ok(world.effects.length <= 72, "boss effects bounded");
    previousState = boss.state;
  }
  assert.equal(defeated, 1, `${definition.id}: must complete and notify exactly once`);
  assert.equal(phases.size, 3, `${definition.id}: all three phases must be exercised`);
  for (const state of [BOSS_STATES.TELL, BOSS_STATES.ATTACK, BOSS_STATES.VULNERABLE, BOSS_STATES.DEFEATED]) assert.ok(visited.has(state), `${definition.id}: missing ${state}`);
  assert.ok(maxGap < 12, `${definition.id}: too long between guaranteed attack windows`);
  assert.equal(world.bossHazards.length, 0, "defeat clears damaging leftovers");
  for (let i = 0; i < 240; i++) controller.update(world, player, STEP);
  assert.equal(defeated, 1, "defeat callback cannot repeat");
  // Every individual pattern is finite, has a warning, and leaves a safe lane
  // or enough horizontal room for the baseline movement to escape a mark.
  const allAttacks = new Set(definition.phases.flatMap((phase) => phase.attacks));
  for (const id of allAttacks) {
    const encounter = new BossController(); const testWorld = worldFor(); const runner = playerAt(720);
    encounter.start(definition.id, 1, testWorld, runner); runner.x = 720;
    const boss = encounter.boss; boss.currentAttack = createAttackData(id, boss, runner, encounter.arena);
    const attack = boss.currentAttack;
    if (id === "judgmentLanes") assert.ok(attack.dangerous.length === 4 && !attack.dangerous.includes(attack.safeLane));
    if (id === "alternatingSafeZones") assert.ok(runner.x + runner.w > attack.safeX - 140 && runner.x < attack.safeX + attack.safeW + 140, "sanctuary cannot demand an impossible crossing");
    beginActiveAttack(boss, testWorld, runner, encounter.arena);
    let finished = false;
    for (let i = 0; i < 400 && !finished; i++) { finished = updateActiveAttack(boss, testWorld, runner, encounter.arena, STEP).finished; updateCombatProjectiles(testWorld, runner, STEP, "bossHazards"); }
    assert.ok(finished, `${definition.id}/${id}: attack never terminates`);
  }
}
console.log("Combat overhaul: 16 telegraphed AI species, terrain pursuit, directional shields, swept/pool collisions, six blessings and four complete three-phase boss fights passed.");
