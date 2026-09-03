import { BOSS_ARENAS, BOSS_DEFINITIONS } from "./boss-data.js";
import { createAttackData, beginActiveAttack, updateActiveAttack, updateBossHazards } from "./boss-attacks.js";
import { updateBossMovement } from "./boss-movement.js";
import { overlaps } from "../common.js";
import { damagePlayer } from "../physics.js";

export const BOSS_STATES = Object.freeze({
  IDLE: "IDLE",
  TELL: "TELL",
  ATTACK: "ATTACK",
  VULNERABLE: "VULNERABLE",
  RECOVER: "RECOVER",
  DEFEATED: "DEFEATED",
});

function currentPhase(definition, health, maxHealth) {
  const ratio = health / maxHealth;
  if (ratio <= 0.35) return definition.phases[2];
  if (ratio <= 0.7) return definition.phases[1];
  return definition.phases[0];
}

function selectAttack(boss, player) {
  const attacks = boss.phase.attacks;
  const distance = Math.abs(player.x - boss.x);
  const playerAbove = player.y + player.h < boss.y;
  const mustExpose = boss.attacksSinceExpose >= 2;
  let candidates = mustExpose ? attacks.filter((id) => boss.definition.exposingAttacks.includes(id)) : [...attacks];
  if (distance > 430) candidates = candidates.filter((id) => !["slam", "radial"].includes(id));
  if (playerAbove) candidates = candidates.filter((id) => !["shockwave", "charge"].includes(id));
  if (!candidates.length) candidates = attacks;
  const withoutRepeat = candidates.filter((id) => id !== boss.lastAttack);
  const pool = withoutRepeat.length ? withoutRepeat : candidates;
  return pool[boss.attackCount % pool.length];
}

export function createBossEncounter(kind, difficulty = 1) {
  const definition = BOSS_DEFINITIONS[kind] || BOSS_DEFINITIONS.warBeast;
  const arena = JSON.parse(JSON.stringify(BOSS_ARENAS[definition.arena]));
  arena.platforms.forEach((platform, index) => { platform.id = `arena-platform-${index}`; });
  const maxHealth = Math.round(definition.health * (1 + Math.max(0, difficulty - 1) * 0.11));
  const boss = {
    definition,
    x: arena.bossSpawn.x,
    y: arena.bossSpawn.y,
    w: definition.size.w,
    h: definition.size.h,
    vx: 0,
    vy: 0,
    onGround: definition.region !== "heaven" && definition.id !== "templeJudge",
    health: maxHealth,
    maxHealth,
    state: BOSS_STATES.IDLE,
    stateTime: 0,
    currentAttack: null,
    lastAttack: null,
    attackCount: 0,
    attacksSinceExpose: 0,
    movement: "stationaryCast",
    moveTimer: 0,
    moveCount: 0,
    age: 0,
    flash: 0,
    defeatedTimer: 0,
  };
  boss.phase = currentPhase(definition, boss.health, boss.maxHealth);
  return { boss, arena };
}

export class BossController {
  constructor(onDefeated = null) {
    this.boss = null;
    this.arena = null;
    this.onDefeated = onDefeated;
  }

  start(kind, difficulty, world, player) {
    const encounter = createBossEncounter(kind, difficulty);
    this.boss = encounter.boss;
    this.arena = encounter.arena;
    world.platforms = this.arena.platforms;
    world.movers = [];
    world.hazards = [...(this.arena.hazards || [])];
    world.enemies = [];
    world.pickups = [];
    world.bossHazards = [];
    world.width = this.arena.width;
    world.region = this.arena.region;
    world.name = this.boss.definition.name;
    player.x = this.arena.spawn.x;
    player.y = this.arena.spawn.y;
    player.vx = 0;
    player.vy = 0;
    return encounter;
  }

  get active() {
    return Boolean(this.boss && this.boss.state !== BOSS_STATES.DEFEATED);
  }

  setState(state, duration = 0) {
    this.boss.state = state;
    this.boss.stateTime = duration;
  }

  damage(amount, source = "attack") {
    const boss = this.boss;
    if (!boss || boss.state !== BOSS_STATES.VULNERABLE) return false;
    boss.health = Math.max(0, boss.health - amount);
    boss.flash = 0.15;
    boss.lastDamageSource = source;
    boss.phase = currentPhase(boss.definition, boss.health, boss.maxHealth);
    if (boss.health <= 0) {
      boss.currentAttack = null;
      this.setState(BOSS_STATES.DEFEATED, 1.4);
    }
    return true;
  }

  update(world, player, dt, camera) {
    const boss = this.boss;
    if (!boss) return;
    boss.age += dt;
    boss.flash = Math.max(0, boss.flash - dt);
    boss.stateTime = Math.max(0, boss.stateTime - dt);
    updateBossHazards(world, player, dt);

    if (boss.state === BOSS_STATES.DEFEATED) {
      if (boss.stateTime <= 0 && !boss.defeatHandled) {
        boss.defeatHandled = true;
        if (this.onDefeated) this.onDefeated(boss);
      }
      return;
    }

    if ([BOSS_STATES.IDLE, BOSS_STATES.TELL].includes(boss.state)) {
      updateBossMovement(boss, player, this.arena, dt);
    }

    if (boss.state === BOSS_STATES.IDLE && boss.stateTime <= 0) {
      const attackId = selectAttack(boss, player);
      boss.currentAttack = createAttackData(attackId, boss, player, this.arena);
      boss.lastAttack = attackId;
      boss.attackCount++;
      this.setState(BOSS_STATES.TELL, boss.currentAttack.tellDuration);
    } else if (boss.state === BOSS_STATES.TELL && boss.stateTime <= 0) {
      beginActiveAttack(boss, world, player, this.arena);
      this.setState(BOSS_STATES.ATTACK, boss.currentAttack.activeDuration);
    } else if (boss.state === BOSS_STATES.ATTACK) {
      const result = updateActiveAttack(boss, world, player, this.arena, dt);
      if (result.impact) camera.kick(11);
      if (result.finished) {
        if (result.expose) {
          boss.attacksSinceExpose = 0;
          boss.vx = 0;
          this.setState(BOSS_STATES.VULNERABLE, 1.75 / boss.phase.speed);
        } else {
          boss.attacksSinceExpose++;
          this.setState(BOSS_STATES.RECOVER, 0.62 / boss.phase.speed);
        }
      }
    } else if (boss.state === BOSS_STATES.VULNERABLE) {
      boss.vx *= 0.85;
      if (overlaps(boss, player) && player.vy > 160 && player.y + player.h < boss.y + boss.h * 0.58) {
        this.damage(2, "stomp");
        player.vy = -720;
      }
      if (boss.stateTime <= 0) this.setState(BOSS_STATES.RECOVER, 0.7 / boss.phase.speed);
    } else if (boss.state === BOSS_STATES.RECOVER && boss.stateTime <= 0) {
      boss.currentAttack = null;
      this.setState(BOSS_STATES.IDLE, 0.5 / boss.phase.speed);
    }

    if (boss.state !== BOSS_STATES.VULNERABLE && overlaps(boss, player)) {
      damagePlayer(player, 1, { x: Math.sign(player.x - boss.x) * 450, y: -440 });
    }
  }
}
