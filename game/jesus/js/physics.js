import { PHYSICS, VIEW } from "./config.js";
import { approach, clamp, overlaps } from "./common.js";

export function createPlayer(x = 80, y = VIEW.floorY - PHYSICS.playerHeight) {
  return {
    x, y, previousX: x, previousY: y, w: PHYSICS.playerWidth, h: PHYSICS.playerHeight,
    vx: 0, vy: 0, facing: 1, onGround: false, groundId: null, wall: 0,
    coyote: 0, wallCoyote: 0, jumpBuffer: 0, dropTimer: 0, invulnerable: 0,
    dashTimer: 0, dashCooldown: 0, airDashUsed: false, wallJumpLock: 0,
    state: "idle", stateTime: 0, animTime: 0, attackTimer: 0, hurtTimer: 0,
    jumpStateTimer: 0, airAction: "jump", landTimer: 0, accelerationTimer: 0, stopTimer: 0, shieldTimer: 0,
    hearts: 4, maxHearts: 4, extraJumps: 0, lastSafe: { x, y },
    powers: { sandals: false, wings: false, shield: false, bread: false, holyLight: 0, fireRain: 0 },
  };
}

export function surfaceY(surface, x) {
  if (surface.kind !== "slope") return surface.y;
  const u = clamp((x - surface.x) / surface.w, 0, 1);
  return surface.y + (surface.slopeDirection === "up" ? 1 - u : u) * (surface.rise || 48);
}

function enabled(platform) { return !platform.dead && !platform.open && !platform.hidden; }
function emit(world,type,x,y,scale=.4,duration=.4) {
  world.effects ||= [];if(world.effects.length>=96)return;
  world._motionEffectId=(world._motionEffectId||0)+1;
  world.effects.push({id:`${world.id||"world"}:motion:${world._motionEffectId}`,type,x,y,scale,time:0,duration});
}
function setState(player, state, dt) {
  if (player.state !== state) { player.state = state; player.stateTime = 0; player.animTime = 0; }
  else { player.stateTime = (player.stateTime || 0) + dt; player.animTime = (player.animTime || 0) + dt; }
}

export function updateMovers(world, time, dt) {
  for (const mover of world.movers || []) {
    const oldX = mover.x, oldY = mover.y;
    const phase = time * (mover.speed || 1.2) + (mover.phase || 0);
    mover.x = (mover.baseX ?? mover.x) + Math.sin(phase) * (mover.axis === "x" ? mover.range : 0);
    mover.y = (mover.baseY ?? mover.y) + Math.sin(phase) * (mover.axis === "y" ? mover.range : 0);
    mover.dx = mover.x - oldX; mover.dy = mover.y - oldY;
    mover.vx = mover.dx / Math.max(dt, 0.0001); mover.vy = mover.dy / Math.max(dt, 0.0001);
  }
}

// Fixed-step movement uses swept edges, so even a dash cannot tunnel through a wall.
export function updatePlayer(player, world, input, dt) {
  if (!(dt > 0)) return;
  const platforms = world.platforms || [], movers = world.movers || [];
  const surfaces = platforms.concat(movers);
  const grounded = player.onGround;
  const oldGround = player.groundId;
  player.previousX = player.x; player.previousY = player.y;
  const axis = clamp(input.horizontal?.() || 0, -1, 1);
  const oldSpeed=Math.abs(player.vx);
  const jumpPressed = Boolean(input.jumpPressed?.());
  const jumpHeld = Boolean(input.jumpHeld?.());
  const dashPressed = Boolean(input.dashPressed?.());
  for (const timer of ["jumpBuffer", "dropTimer", "invulnerable", "dashCooldown", "wallJumpLock", "hurtTimer", "attackTimer", "jumpStateTimer", "landTimer", "accelerationTimer", "stopTimer", "shieldTimer"]) player[timer] = Math.max(0, (player[timer] || 0) - dt);
  if(grounded&&axis&&oldSpeed<20)player.accelerationTimer=.14;
  if(grounded&&!axis&&oldSpeed>100&&player.stopTimer<=0)player.stopTimer=.14;
  player.coyote = grounded ? PHYSICS.coyoteTime : Math.max(0, (player.coyote || 0) - dt);
  player.wallCoyote = player.wall ? 0.1 : Math.max(0, (player.wallCoyote || 0) - dt);
  if (jumpPressed) player.jumpBuffer = PHYSICS.jumpBuffer;
  if (grounded) { player.airDashUsed = false; player.extraJumps = player.powers.wings ? 1 : 0; }

  const ridden = grounded && oldGround ? movers.find(mover => mover.id === oldGround) : null;
  if (ridden && enabled(ridden)) { player.x += ridden.dx || 0; player.y += ridden.dy || 0; }

  if (input.downHeld?.() && grounded && jumpPressed && surfaces.find(p => p.id === oldGround)?.oneWay) {
    player.dropTimer = PHYSICS.dropThroughTime; player.onGround = false;
    player.coyote = 0; player.jumpBuffer = 0; player.y += 5;
  } else if (player.jumpBuffer > 0 && (player.coyote > 0 || player.wallCoyote > 0 || player.extraJumps > 0)) {
    if (player.coyote <= 0 && player.wallCoyote > 0 && player.wall) {
      player.vx = -player.wall * PHYSICS.wallJumpX;
      player.vy = -PHYSICS.wallJumpY; player.facing = -player.wall;
      player.wallJumpLock = PHYSICS.wallJumpLock;
      player.airAction="wallJump";
    } else {
      player.airAction=player.coyote<=0?"doubleJump":"jump";
      if (player.coyote <= 0) player.extraJumps--; player.vy = -PHYSICS.jumpVelocity;
    }
    player.jumpStateTimer=.24;
    emit(world,player.airAction==="doubleJump"?"fx-wings-flight-aura":"fx-jump-dust",player.x+player.w/2,player.y+player.h,.28,.35);
    player.onGround = false; player.coyote = 0; player.wallCoyote = 0; player.jumpBuffer = 0; player.dashTimer = 0;
    player.groundId = null;
  }

  if (dashPressed && player.dashCooldown <= 0 && (grounded || !player.airDashUsed)) {
    player.dashTimer = PHYSICS.dashDuration; player.dashCooldown = PHYSICS.dashCooldown;
    player.airDashUsed = !grounded; player.vx = (axis ? Math.sign(axis) : player.facing) * PHYSICS.dashSpeed;
    player.facing = Math.sign(player.vx); player.vy = 0;
  }
  const dashing = player.dashTimer > 0;
  if (dashing) { player.dashTimer = Math.max(0, player.dashTimer - dt); player.vy = 0; }
  else {
    const speed = player.powers.sandals ? PHYSICS.sandalsSpeed : PHYSICS.runSpeed;
    if (player.wallJumpLock <= 0) {
      player.vx = Math.abs(axis) > 0.02
        ? approach(player.vx, axis * speed, (grounded ? PHYSICS.groundAcceleration : PHYSICS.airAcceleration) * dt)
        : approach(player.vx, 0, (grounded ? PHYSICS.groundFriction : PHYSICS.airFriction) * dt);
      if (axis) player.facing = Math.sign(axis);
    }
    if (!jumpHeld && player.vy < -PHYSICS.jumpCutVelocity) player.vy = -PHYSICS.jumpCutVelocity;
    player.vy = Math.min(PHYSICS.maxFall, player.vy + PHYSICS.gravity * dt);
    if(player.powers.wings&&jumpHeld&&player.vy>330)player.vy=330;
  }

  const oldX = player.x;
  let nextX = oldX + player.vx * dt;
  player.wall = 0;
  for (const platform of surfaces) {
    if (!enabled(platform) || platform.oneWay || platform.kind === "slope") continue;
    if (player.y + player.h <= platform.y + 0.1 || player.y >= platform.y + platform.h - 0.1) continue;
    if (player.vx > 0 && oldX + player.w <= platform.x + 0.5 && nextX + player.w >= platform.x) {
      nextX = Math.min(nextX, platform.x - player.w); player.wall = 1;
    } else if (player.vx < 0 && oldX >= platform.x + platform.w - 0.5 && nextX <= platform.x + platform.w) {
      nextX = Math.max(nextX, platform.x + platform.w); player.wall = -1;
    }
  }
  player.x = clamp(nextX, world.bounds?.left ?? 0, Math.max(0, (world.width || Infinity) - player.w));
  if (player.wall) { player.vx = 0; player.dashTimer = 0; player.wallCoyote = 0.1; }
  // Contact persists at rest and while sliding instead of requiring horizontal velocity.
  if (!player.wall) for (const p of surfaces) {
    if (!enabled(p) || p.oneWay || p.kind === "slope" || player.y + player.h <= p.y + 3 || player.y >= p.y + p.h - 3) continue;
    if (Math.abs(player.x + player.w - p.x) < 1) player.wall = 1;
    if (Math.abs(player.x - p.x - p.w) < 1) player.wall = -1;
  }
  if (player.wall && !grounded && player.vy > PHYSICS.wallSlideSpeed && axis === player.wall) player.vy = PHYSICS.wallSlideSpeed;

  const oldY = player.y, oldBottom = oldY + player.h;
  let nextY = oldY + player.vy * dt;
  player.onGround = false; player.groundId = null;
  if (player.vy >= 0) {
    let highest = Infinity, landing = null;
    for (const platform of surfaces) {
      if (!enabled(platform) || (platform.oneWay && player.dropTimer > 0)) continue;
      if (player.x + player.w <= platform.x + 1 || player.x >= platform.x + platform.w - 1) continue;
      const footX = clamp(player.x + player.w * 0.5, platform.x, platform.x + platform.w);
      const top = surfaceY(platform, footX);
      const slopeSnap = platform.kind === "slope" && grounded && Math.abs(oldBottom - top) <= 24;
      const moverSlack = Math.max(0, platform.dy || 0) + 1;
      if ((oldBottom <= top + moverSlack || slopeSnap) && (nextY + player.h >= top || slopeSnap) && top < highest) { highest = top; landing = platform; }
    }
    if (landing) {
      if(!grounded&&player.vy>300){player.landTimer=.1;emit(world,"fx-landing-dust",player.x+player.w/2,highest,.3,.35);}
      nextY = highest - player.h; player.vy = 0; player.onGround = true; player.groundId = landing.id;
      player.extraJumps = player.powers.wings ? 1 : 0; player.airDashUsed = false;
      if (!landing.temporary && landing.kind !== "collapsing") player.lastSafe = { x: player.x, y: nextY };
    }
  } else {
    for (const platform of surfaces) {
      if (!enabled(platform) || platform.oneWay || platform.kind === "slope") continue;
      if (player.x + player.w <= platform.x + 1 || player.x >= platform.x + platform.w - 1) continue;
      const bottom = platform.y + platform.h;
      if (oldY >= bottom - 0.1 && nextY <= bottom) { nextY = Math.max(nextY, bottom); player.vy = 0; }
    }
  }
  player.y = nextY;
  if (player.jumpBuffer > 0 && player.onGround && !jumpPressed) {
    player.vy = -PHYSICS.jumpVelocity; player.onGround = false; player.coyote = 0; player.jumpBuffer = 0;
    player.airAction="jump";player.jumpStateTimer=.24;emit(world,"fx-jump-dust",player.x+player.w/2,player.y+player.h,.28,.35);
  }
  const state = player.hearts <= 0 ? "death" : player.hurtTimer > 0 ? "hurt" : player.dashTimer > 0 ? "dash"
    : player.attackTimer > 0 ? (player.abilityState || "attack") : player.wall && !player.onGround && player.vy > 0 ? "wallSlide"
    : player.shieldTimer>0?"shield"
    : !player.onGround ? (player.vy < 0 ? (player.jumpStateTimer>0?player.airAction:"jump") : player.powers.wings&&jumpHeld&&player.vy>=329?"glide":"fall")
    : player.landTimer>0?"land":player.accelerationTimer>0&&axis?"accelerate":player.stopTimer>0&&!axis?"stop"
    : Math.abs(player.vx)>200?"run":Math.abs(player.vx)>15?"walk":"idle";
  setState(player, state, dt);
}

export function damagePlayer(player, amount = 1, knockback = null) {
  if (player.invulnerable > 0 || player.hearts <= 0) return false;
  if (player.powers.shield) { player.powers.shield = false; player.invulnerable = 0.7; player.shieldTimer=.25; return false; }
  player.hearts = Math.max(0, player.hearts - amount); player.invulnerable = 1.05; player.hurtTimer = 0.22;
  if (knockback) { player.vx = knockback.x || 0; player.vy = knockback.y ?? -430; player.onGround = false; player.dashTimer = 0; }
  player.state = player.hearts <= 0 ? "death" : "hurt"; player.stateTime = 0; player.animTime = 0;
  return true;
}

// Timed hazards always show a warning interval; wind never exceeds normal air control.
export function updateWorldEnvironment(world, player, dt, time = world.time || 0) {
  world.time = time;
  for (const platform of world.platforms || []) {
    if (platform.kind === "collapsing") {
      if (platform.dead) {
        platform.resetTimer = (platform.resetTimer || 4) - dt;
        if (platform.resetTimer <= 0) { platform.dead = false; platform.collapseTimer = null; platform.shaking = false; }
      } else if (platform.collapseTimer != null || player.groundId === platform.id) {
        platform.collapseTimer = (platform.collapseTimer ?? 0.8) - dt; platform.shaking = true;
        if (platform.collapseTimer <= 0) { platform.dead = true; platform.resetTimer = 4; emit(world,"fx-rubble-destruction",platform.x+platform.w/2,platform.y,.5,.55); }
      }
    }
    if (platform.switchId) platform.open = (world.switches || []).some(s => s.id === platform.switchId && s.activated);
  }
  for (const sw of world.switches || []) {
    if (!sw.activated && overlaps(player, sw)) {
      sw.activated = true;
      emit(world,"fx-gold-impact",sw.x+sw.w/2,sw.y+sw.h,.3,.4);
      for (const gate of world.platforms || []) if (gate.switchId === sw.id) gate.open = true;
      for (const prop of world.props || []) if (prop.switchId === sw.id) { prop.open = true; prop.artFrame = "gate-open"; }
    }
  }
  for (const hazard of world.hazards || []) {
    if (hazard.dead) continue;
    hazard.animTime = (hazard.animTime || 0) + dt;
    if (hazard.type === "wind") {
      hazard.active = true;
      if (overlaps(player, hazard) && !player.onGround && !player.dashTimer) player.vx += Math.sin(time * 1.4 + (hazard.phase || 0)) * (hazard.force || 170) * dt;
      continue;
    }
    if (hazard.type === "fireJet" || hazard.type === "flameJet" || hazard.type === "lightning" || hazard.type === "fallingRock") {
      const cycle = (time + (hazard.phase || 0)) % (hazard.period || 3.6);
      const warn = hazard.warningDuration || 0.8, active = hazard.activeDuration || 0.65;
      hazard.warning = cycle < warn; hazard.active = cycle >= warn && cycle < warn + active;
      if (hazard.type === "fallingRock") {
        hazard.targetY ??= hazard.y + hazard.h - 44;
        hazard.w = 44; hazard.h = 44;
        const progress = clamp((cycle - warn) / active, 0, 1);
        hazard.y = hazard.targetY - 440 + progress * progress * 440;
        hazard.telegraph = hazard.warning ? { x:hazard.x-12, y:hazard.targetY+32, w:68, h:12 } : null;
      }
    } else if (hazard.type === "chase") {
      if (!hazard.triggered && player.x > hazard.triggerX) { hazard.triggered = true; hazard.x = player.x - 560; }
      hazard.active = Boolean(hazard.triggered && player.x < hazard.endX);
      if (hazard.active) hazard.x = Math.min(player.x - 75, hazard.x + (hazard.speed || 335) * dt);
    } else hazard.active = true;
    if (hazard.active && overlaps(player, hazard)) damagePlayer(player, hazard.damage || 1, { x: player.x < hazard.x + hazard.w / 2 ? -240 : 240, y: -400 });
  }
  world.furthestX = Math.max(world.furthestX || 0, player.x);
}
