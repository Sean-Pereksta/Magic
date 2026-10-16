import { BOSS_ARENAS, BOSS_DEFINITIONS } from "./boss-data.js";
import { createAttackData, beginActiveAttack, updateActiveAttack, updateBossHazards } from "./boss-attacks.js";
import { updateBossMovement } from "./boss-movement.js";
import { clamp, overlaps } from "../common.js";
import { damagePlayer } from "../physics.js";
import { emitCombatEffect } from "../enemies/enemies.js";

export const BOSS_STATES = Object.freeze({ IDLE: "IDLE", TELL: "TELL", ATTACK: "ATTACK", VULNERABLE: "VULNERABLE", RECOVER: "RECOVER", DEFEATED: "DEFEATED" });
export function currentPhase(definition, health, maxHealth) {
  const ratio = health / maxHealth;
  return ratio <= .35 ? definition.phases[2] : ratio <= .7 ? definition.phases[1] : definition.phases[0];
}
function selectAttack(boss, player) {
  const attacks = boss.phase.attacks;
  const distance = Math.abs(player.x - boss.x), playerAbove = player.y + player.h < boss.y;
  const mustExpose = boss.attacksSinceExpose >= 1;
  let candidates = mustExpose ? attacks.filter((id) => boss.definition.exposingAttacks.includes(id)) : [...attacks];
  if (!mustExpose && distance > 430) candidates = candidates.filter((id) => !["slam", "radial"].includes(id));
  if (!mustExpose && playerAbove) candidates = candidates.filter((id) => !["shockwave", "charge"].includes(id));
  if (!candidates.length) candidates = attacks;
  const withoutRepeat = candidates.filter((id) => id !== boss.lastAttack);
  const pool = withoutRepeat.length ? withoutRepeat : candidates;
  return pool[boss.attackCount % pool.length];
}
export function createBossEncounter(kind, difficulty = 1) {
  const definition = BOSS_DEFINITIONS[kind] || BOSS_DEFINITIONS.warBeast;
  const arena = JSON.parse(JSON.stringify(BOSS_ARENAS[definition.arena]));
  arena.floorY ||= 468; arena.top ||= 0; arena.height ||= 650;
  arena.platforms.forEach((platform, index) => { platform.id = `arena-${definition.id}-${index}`; });
  const maxHealth = Math.round(definition.health * (1 + Math.min(5, Math.max(0, difficulty - 1)) * .08));
  const boss = {
    definition, type: definition.id, id: `boss-${definition.id}`,
    x: arena.bossSpawn.x, y: arena.bossSpawn.y, w: definition.size.w, h: definition.size.h,
    vx: 0, vy: 0, facing: -1, onGround: !definition.flying,
    health: maxHealth, maxHealth, state: BOSS_STATES.IDLE, stateTime: 1.25,
    animState: "idle", animTime: 0, currentAttack: null, lastAttack: null,
    attackCount: 0, attacksSinceExpose: 0, movement: "stationaryCast", moveTimer: 0, moveCount: 0,
    age: 0, flash: 0, hitCooldown: 0, defeatedTimer: 0,
  };
  boss.phase = currentPhase(definition, boss.health, boss.maxHealth);
  return { boss, arena };
}
export class BossController {
  constructor(onDefeated = null) { this.boss = null; this.arena = null; this.world = null; this.onDefeated = onDefeated; }
  start(kind, difficulty, world, player) {
    const encounter = createBossEncounter(kind, difficulty);
    this.boss = encounter.boss; this.arena = encounter.arena; this.world = world;
    Object.assign(world, {
      id: `arena-${this.boss.definition.id}`, platforms: this.arena.platforms, movers: [], hazards: this.arena.hazards || [],
      props: this.arena.props || [], enemies: [], pickups: [], bossHazards: [], enemyProjectiles: [], effects: [],
      width: this.arena.width, height: this.arena.height, region: this.arena.region, biome: this.arena.biome || this.arena.region,
      name: this.boss.definition.name, spawn: { ...this.arena.spawn }, bossArena: true, routeValid: true,
    });
    player.x = this.arena.spawn.x; player.y = this.arena.spawn.y;
    player.vx = 0; player.vy = 0; player.invulnerable = Math.max(player.invulnerable || 0, 1.5);
    player.lastSafe = { ...this.arena.spawn };
    return encounter;
  }
  get active() { return Boolean(this.boss && this.boss.state !== BOSS_STATES.DEFEATED); }
  setState(state, duration = 0) {
    const boss = this.boss;
    if (!boss) return;
    boss.state = state; boss.stateTime = duration; boss.animTime = 0;
    const id = boss.currentAttack?.id;
    const secondary = boss.definition.id === "warBeast" ? !["charge", "dashCombo"].includes(id) : boss.definition.id === "templeJudge" ? !["sweep", "slam"].includes(id) : boss.definition.id === "pitLord" ? !["slam", "shockwave"].includes(id) : ["dashCombo", "charge"].includes(id);
    boss.animState = state === BOSS_STATES.DEFEATED ? "death" : [BOSS_STATES.ATTACK, BOSS_STATES.TELL].includes(state) ? (secondary ? "attack2" : "attack") : "idle";
    if (state === BOSS_STATES.DEFEATED && this.world) {
      this.world.bossHazards.length = 0; this.world.enemyProjectiles.length = 0;
      emitCombatEffect(this.world, "fx-gold-impact", boss.x + boss.w / 2, boss.y + boss.h / 2, { duration: 1, scale: 2 });
    }
  }
  damage(amount, source = "attack") {
    const boss = this.boss;
    if (!boss || boss.state !== BOSS_STATES.VULNERABLE || boss.hitCooldown > 0 || amount <= 0) return false;
    boss.health = Math.max(0, boss.health - amount); boss.flash = .2; boss.hitCooldown = .28;
    boss.lastDamageSource = source; boss.phase = currentPhase(boss.definition, boss.health, boss.maxHealth);
    emitCombatEffect(this.world, "fx-gold-impact", boss.x + boss.w / 2, boss.y + boss.h / 2, { scale: 1.1 });
    if (boss.health <= 0) { boss.currentAttack = null; boss.vx = 0; this.setState(BOSS_STATES.DEFEATED, 1.5); }
    return true;
  }
  update(world, player, dt, camera = null) {
    const boss = this.boss;
    if (!boss) return;
    dt = Math.min(.05, Math.max(0, dt));
    this.world = world;
    boss.age += dt; boss.animTime += dt;
    boss.flash = Math.max(0, boss.flash - dt); boss.hitCooldown = Math.max(0, boss.hitCooldown - dt);
    boss.stateTime = Math.max(0, boss.stateTime - dt);
    updateBossHazards(world, player, dt);
    if (boss.state === BOSS_STATES.DEFEATED) {
      if (boss.stateTime <= 0 && !boss.defeatHandled) { boss.defeatHandled = true; this.onDefeated?.(boss); }
      return;
    }
    if (boss.state === BOSS_STATES.IDLE) updateBossMovement(boss, player, this.arena, dt);
    if (boss.state === BOSS_STATES.IDLE && boss.stateTime <= 0) {
      const attackId = selectAttack(boss, player);
      boss.currentAttack = createAttackData(attackId, boss, player, this.arena);
      boss.facing = boss.currentAttack.direction; boss.lastAttack = attackId; boss.attackCount++;
      boss.vx = 0; boss.vy = 0;
      this.setState(BOSS_STATES.TELL, boss.currentAttack.tellDuration);
    } else if (boss.state === BOSS_STATES.TELL && boss.stateTime <= 0) {
      beginActiveAttack(boss, world, player, this.arena);
      this.setState(BOSS_STATES.ATTACK, boss.currentAttack.activeDuration);
    } else if (boss.state === BOSS_STATES.ATTACK) {
      const result = updateActiveAttack(boss, world, player, this.arena, dt);
      if (result.impact) camera?.kick?.(8);
      if (result.finished) {
        boss.vx = 0; boss.vy = 0;
        if (result.expose) {
          boss.attacksSinceExpose = 0;
          this.setState(BOSS_STATES.VULNERABLE, Math.max(1.8, 2.2 / boss.phase.speed));
          // Exposure clears old volleys, giving the player a real attack window.
          for (const hazard of world.bossHazards) hazard.life = Math.min(hazard.life, .18);
        } else {
          boss.attacksSinceExpose++;
          this.setState(BOSS_STATES.RECOVER, .7);
        }
      }
    } else if (boss.state === BOSS_STATES.VULNERABLE) {
      const floor = (this.arena.floorY || 468) - boss.h;
      boss.y = Math.min(floor, boss.y + 320 * dt);
      boss.onGround = boss.y >= floor;
      if (overlaps(boss, player)) {
        const stomp = player.vy > 100 && player.y + player.h < boss.y + boss.h * .62;
        if (stomp && this.damage(2, "stomp")) player.vy = -650;
        else if (((player.dashTimer || 0) > 0 || (player.attackTimer || 0) > 0) && this.damage(1, "dash")) player.vx *= .7;
      }
      if (boss.state === BOSS_STATES.VULNERABLE && boss.stateTime <= 0) this.setState(BOSS_STATES.RECOVER, .8);
    } else if (boss.state === BOSS_STATES.RECOVER && boss.stateTime <= 0) {
      boss.currentAttack = null; this.setState(BOSS_STATES.IDLE, .7);
    }
    boss.x = clamp(boss.x, this.arena.left + 22, this.arena.right - boss.w - 22);
    // The body is damaging only during the committed active attack. Windup,
    // recovery and exposure remain safe to approach for a jump or blessing.
    if (boss.state === BOSS_STATES.ATTACK && overlaps(boss, player) && !boss.currentAttack.hitPlayer) {
      boss.currentAttack.hitPlayer = damagePlayer(player, 1, { x: (Math.sign(player.x - boss.x) || -1) * 330, y: -410 });
    }
  }
}

