import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { PHYSICS } from "../js/config.js";
import { createPlayer, updatePlayer, updateMovers, updateWorldEnvironment, surfaceY } from "../js/physics.js";
import { createLevel, updateWorldStreaming, persistWorldState, restoreWorldState, CHUNK_WIDTH } from "../js/world/generator.js";
import { BIOME_ORDER } from "../js/world/regions.js";
import { updateEnemies } from "../js/enemies/enemies.js";
import { grantBlessing, collectPickups, updateBlessings, useAbility } from "../js/powers/blessings.js";
import { Camera } from "../js/camera.js";

// This is a deterministic endurance harness, not a claim of human completion.
// It runs the shipped fixed-step controllers while scripted relocation visits
// encounters and chunks. Invulnerability prevents death from ending coverage.
const STEP = PHYSICS.fixedStep;
const SECONDS_PER_BIOME = 200;
const STEPS_PER_BIOME = Math.round(SECONDS_PER_BIOME / STEP);
const FIELDS = ["platforms", "movers", "hazards", "enemies", "pickups", "props", "scenery", "switches"];
const started = performance.now();
const reports = [];
const allTypes = new Set(), attackingTypes = new Set();
let simulatedSteps = 0, pooledProjectilesObserved = 0, totalRestores = 0;

function place(player, x, y, groundId = null) {
  player.x = x; player.y = y; player.previousX = x; player.previousY = y;
  player.vx = 0; player.vy = 0; player.onGround = Boolean(groundId); player.groundId = groundId;
  player.wall = 0; player.dashTimer = 0; player.hurtTimer = 0; player.invulnerable = 3600;
}
function finiteEntity(entity, label) {
  for (const key of ["x", "y", "w", "h"]) assert.ok(Number.isFinite(entity[key]), `${label}.${key} became non-finite`);
  for (const key of ["vx", "vy", "animTime"]) if (entity[key] != null) assert.ok(Number.isFinite(entity[key]), `${label}.${key} became non-finite`);
}

for (const [biomeIndex, biome] of BIOME_ORDER.entries()) {
  const options = { biome, seed: 0xC0FFEE + biomeIndex * 7919, difficulty: Math.min(6, biomeIndex + 1), chunkCount: 48, abilities: { sandals: biomeIndex > 0, wings: biomeIndex > 2 } };
  const world = createLevel(options), player = createPlayer(), camera = new Camera();
  assert.equal(world.routeValid, true);
  assert.equal(world.chunkCount, 48);
  if (options.abilities.sandals) grantBlessing(player, "sandals");
  if (options.abilities.wings) grantBlessing(player, "wings");
  grantBlessing(player, "shield"); grantBlessing(player, "bread");
  updateBlessings(player, world, 0);
  const visitedChunks = new Set();
  const collected = new Map(), killed = new Map();
  const peaks = { objects: 0, enemies: 0, projectiles: 0, spareProjectiles: 0, effects: 0, resident: 0 };
  let scheduledChunk = -1, relocations = 0, actualJumps = 0, actualDashes = 0, actualLandings = 0, lastGrounded = false;
  let inputStep = 0;
  const input = {
    horizontal: () => inputStep % 360 < 300 ? 1 : -.35,
    jumpPressed: () => inputStep % 105 === 0,
    jumpHeld: () => inputStep % 105 < 48,
    dashPressed: () => inputStep % 313 === 40,
    downHeld: () => inputStep % 480 > 440,
  };
  for (let step = 0; step < STEPS_PER_BIOME; step++) {
    const seconds = step * STEP;
    const chunkIndex = Math.floor(seconds / 4) % world.chunkCount;
    if (chunkIndex !== scheduledChunk) {
      scheduledChunk = chunkIndex;
      place(player, chunkIndex * CHUNK_WIDTH + 90, world.spawn.y);
      updateWorldStreaming(world, player.x, player.x);
      visitedChunks.add(chunkIndex);
    }
    // Encounter relocation alternates with pickup visits. It intentionally
    // samples AI behavior without pretending the harness solved a platform route.
    if (step % 120 === 0) {
      const enemies = world.enemies.filter(e => e.chunkIndex === chunkIndex && !e.dead);
      const pickups = world.pickups.filter(p => p.chunkIndex === chunkIndex && !p.collected);
      if (step % 480 === 360 && pickups.length) {
        const pickup = pickups[Math.floor(seconds) % pickups.length];
        place(player, pickup.x, pickup.y);
      } else if (enemies.length) {
        const enemy = enemies[Math.floor(seconds) % enemies.length];
        const footX = enemy.x + enemy.w / 2;
        const support = world.platforms.find(s => !s.dead && !s.open && footX >= s.x && footX <= s.x + s.w && Math.abs(surfaceY(s, footX) - (enemy.y + enemy.h)) < 110);
        const x = Math.min(world.width - player.w, enemy.x + 85);
        place(player, x, support ? surfaceY(support, x + player.w / 2) - player.h : enemy.y, support?.id);
      } else place(player, chunkIndex * CHUNK_WIDTH + 90, world.spawn.y);
      relocations++;
    }
    inputStep = step;
    updateWorldStreaming(world, player.x, camera.x);
    updateMovers(world, seconds, STEP);
    updatePlayer(player, world, input, STEP);
    updateWorldEnvironment(world, player, STEP, seconds);
    updateEnemies(world, player, STEP);
    updateBlessings(player, world, STEP);
    if (step % 720 === 320) useAbility(player, world, null);
    collectPickups(world, player);
    camera.update(player, STEP, world);
    simulatedSteps++;
    if (player.vy < -300 && input.jumpPressed()) actualJumps++;
    if (player.dashTimer > 0 && input.dashPressed()) actualDashes++;
    if (player.onGround && !lastGrounded) actualLandings++;
    lastGrounded = player.onGround;
    // Every step checks the hot state; sampled full inventories keep the test
    // fast while asserting strict live/pool bounds throughout the entire run.
    assert.ok(Number.isFinite(player.x + player.y + player.vx + player.vy + camera.x + camera.y), `${biome}: non-finite controller state at step ${step}`);
    assert.ok(player.hearts > 0, `${biome}: safe harness unexpectedly died`);
    assert.ok(world._resident.size <= world.residentLimit, `${biome}: streamed chunks grew without bound`);
    assert.ok(world.enemyProjectiles.length <= 64 && world.bossHazards.length <= 64, `${biome}: live projectiles exceeded cap`);
    assert.ok(world.effects.length <= 96, `${biome}: effects exceeded shared cap`);
    if (step % 30 === 0) {
      let objects = 0;
      for (const field of FIELDS) {
        objects += world[field].length;
        const ids = new Set();
        for (const entity of world[field]) { finiteEntity(entity, `${biome}/${field}/${entity.id}`); assert.ok(!ids.has(entity.id), `duplicate resident ${entity.id}`); ids.add(entity.id); }
      }
      for (const entity of [...world.enemyProjectiles, ...world.bossHazards]) finiteEntity(entity, `${biome}/projectile`);
      for (const enemy of world.enemies) {
        allTypes.add(enemy.type);
        if (enemy.attackCount > 0) attackingTypes.add(enemy.type);
        if (enemy.dead) killed.set(enemy.id, enemy.chunkIndex);
      }
      for (const pickup of world.pickups) if (pickup.collected) collected.set(pickup.id, pickup.chunkIndex);
      const spare = Object.values(world.combatPools || {}).reduce((count, pool) => count + pool.length, 0);
      assert.ok(objects <= 800, `${biome}: resident entity inventory grew past fixed budget (${objects})`);
      assert.ok(spare <= 128, `${biome}: spare projectile pool grew without bound`);
      peaks.objects = Math.max(peaks.objects, objects);
      peaks.enemies = Math.max(peaks.enemies, world.enemies.length);
      peaks.projectiles = Math.max(peaks.projectiles, world.enemyProjectiles.length + world.bossHazards.length);
      peaks.spareProjectiles = Math.max(peaks.spareProjectiles, spare);
      peaks.effects = Math.max(peaks.effects, world.effects.length);
      peaks.resident = Math.max(peaks.resident, world._resident.size);
      pooledProjectilesObserved += spare;
    }
    if (player.y > world.deathY + 100 || player.y < world.bounds.top - 200) place(player, chunkIndex * CHUNK_WIDTH + 90, world.spawn.y);
  }
  assert.equal(visitedChunks.size, world.chunkCount, `${biome}: harness did not stream the whole plan`);
  assert.ok(actualJumps > 10 && actualDashes > 10 && actualLandings > 10, `${biome}: shipped movement was not exercised`);
  assert.ok(collected.size > 0, `${biome}: no pickups reached persistence coverage`);
  assert.ok(killed.size > 0, `${biome}: no AI deaths reached persistence coverage`);
  const snapshot = persistWorldState(world);
  assert.ok(snapshot.dead.length <= world.chunkCount * 16 && snapshot.collected.length <= world.chunkCount * 16, `${biome}: persistent state exceeded finite content budget`);
  const restored = createLevel(options);
  restoreWorldState(restored, JSON.parse(JSON.stringify(snapshot)), player.x);
  assert.deepEqual(persistWorldState(restored), snapshot, `${biome}: checkpoint serialization changed state`);
  for (const [id, index] of [...killed.entries()].slice(0, 3)) {
    updateWorldStreaming(restored, index * CHUNK_WIDTH + 90);
    assert.equal(restored.enemies.find(e => e.id === id)?.dead, true, `${biome}: dead enemy respawned after eviction/restore`);
    totalRestores++;
  }
  for (const [id, index] of [...collected.entries()].slice(0, 3)) {
    updateWorldStreaming(restored, index * CHUNK_WIDTH + 90);
    assert.equal(restored.pickups.find(p => p.id === id)?.collected, true, `${biome}: collected pickup returned after eviction/restore`);
    totalRestores++;
  }
  reports.push({ biome, simulatedSeconds: SECONDS_PER_BIOME, chunksVisited: visitedChunks.size, scriptedRelocations: relocations, jumps: actualJumps, dashes: actualDashes, landings: actualLandings, persistedKills: snapshot.dead.length, persistedPickups: snapshot.collected.length, peaks });
}
assert.equal(simulatedSteps, Math.round(20 * 60 / STEP), "must simulate twenty full minutes at the shipped fixed step");
assert.equal(allTypes.size, 16, "endurance must visit all sixteen generated species");
assert.ok(attackingTypes.size >= 12, "endurance must exercise a broad set of real attack routines");
assert.ok(pooledProjectilesObserved > 0, "endurance must exercise projectile allocation/cleanup/reuse");
console.log(JSON.stringify({ simulatedMinutes: 20, simulatedSteps, fixedStep: STEP, biomes: BIOME_ORDER.length, speciesSeen: allTypes.size, speciesAttacked: attackingTypes.size, persistenceChecks: totalRestores, durationSeconds: Number(((performance.now() - started) / 1000).toFixed(2)), reports }, null, 2));
