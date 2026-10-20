import { PHYSICS, VIEW } from "./config.js";
import { approach, clamp, overlaps } from "./common.js";

export function createPlayer(x = 80, y = VIEW.floorY - PHYSICS.playerHeight) {
  return {
    x, y, previousX: x, previousY: y, w: PHYSICS.playerWidth, h: PHYSICS.playerHeight,
    vx: 0, vy: 0, facing: 1, onGround: false, groundId: null, wall: 0,
    coyote: 0, wallCoyote: 0, jumpBuffer: 0, dropTimer: 0, invulnerable: 0,
    dashTimer: 0, dashCooldown: 0, dashJump: false, airDashUsed: false, wallJumpLock: 0,
    state: "idle", stateTime: 0, animTime: 0, attackTimer: 0, hurtTimer: 0,
    jumpStateTimer: 0, airAction: "jump", landTimer: 0, accelerationTimer: 0, stopTimer: 0, shieldTimer: 0,
    hearts: 4, maxHearts: 4, extraJumps: 0, lastSafe: { x, y },
    boosts: {water:0,wind:0},
    powers: { sandals: false, wings: false, shield: false, bread: false, holyLight: 0, fireRain: 0 },
  };
}

export function surfaceY(surface, x) {
  if (surface.motion === "pivot" && Number.isFinite(surface.startX) && Number.isFinite(surface.endX)) {
    const span = surface.endX - surface.startX;
    return surface.startY + (surface.endY - surface.startY) * clamp(span ? (x - surface.startX) / span : 0, 0, 1);
  }
  if (surface.kind !== "slope") return surface.y;
  const u = clamp((x - surface.x) / surface.w, 0, 1);
  return surface.y + (surface.slopeDirection === "up" ? 1 - u : u) * (surface.rise || 48);
}

function enabled(platform) { return !platform.dead && !platform.open && !platform.hidden && platform.solid !== false; }
// Mandatory barriers remain a single continuous solid through the whole live
// passage. Opening artwork never shrinks this collider before the lock finishes.
export function mandatoryGateCollider(gate, world) {
  if (gate.kind !== "gate" || !gate.mandatory || gate.open) return gate;
  const top = Math.min(gate.y, gate.frameTop ?? world.bounds?.top ?? gate.y);
  const bottom = Math.max(gate.y + gate.h, gate.frameBottom ?? world.bounds?.bottom ?? gate.y + gate.h,
    (world.deathY ?? world.bounds?.bottom ?? gate.y + gate.h) + PHYSICS.playerHeight);
  return {...gate, y:top, h:bottom-top, oneWay:false, solid:true, hidden:false, dead:false};
}
function emit(world,type,x,y,scale=.4,duration=.4) {
  world.effects ||= [];if(world.effects.length>=96)return;
  world._motionEffectId=(world._motionEffectId||0)+1;
  world.effects.push({id:`${world.id||"world"}:motion:${world._motionEffectId}`,type,x,y,scale,time:0,duration});
}
function setState(player, state, dt) {
  if (player.state !== state) { player.state = state; player.stateTime = 0; player.animTime = 0; }
  else { player.stateTime = (player.stateTime || 0) + dt; player.animTime = (player.animTime || 0) + dt; }
}

const TAU = Math.PI * 2;
const modulo = (value, period) => ((value % period) + period) % period;

// The renderer, streamed chunks, and route prover all use this same clock sampler.
// phase remains radians for movers, including the original sinusoidal platforms.
export function sampleMover(mover, time) {
  const period = mover.period || TAU / (mover.speed || 1.2);
  const phase = (time + (mover.timeOffset || 0)) * TAU / period + (mover.phase || 0);
  const x = mover.baseX ?? mover.x, y = mover.baseY ?? mover.y;
  if (mover.motion === "pivot") {
    const length = mover.length || mover.w;
    const pivotX = mover.pivotX ?? x + length / 2, pivotY = mover.pivotY ?? y;
    const angle = (mover.baseAngle || 0) + Math.sin(phase) * (mover.angleRange ?? .35);
    const halfX = Math.cos(angle) * length / 2, halfY = Math.sin(angle) * length / 2;
    return { x:pivotX-halfX, y:pivotY-Math.abs(halfY), w:halfX*2,
      startX:pivotX-halfX, startY:pivotY-halfY, endX:pivotX+halfX, endY:pivotY+halfY,
      angle, solid:true, hidden:false, warning:Math.abs(Math.cos(phase)) < .12, active:true };
  }
  if (mover.motion === "timed") {
    const warning = mover.warningDuration ?? .9, active = mover.activeDuration ?? 3.5;
    const retract = mover.retractDuration ?? .45;
    const cycle = modulo(time + (mover.timeOffset || 0) + (mover.phase || 0) * period / TAU, period);
    const retracting = cycle >= warning + active && cycle < warning + active + retract;
    const beforeRetraction=cycle>=warning+active-(mover.retractWarningDuration ?? .9) && cycle<warning+active;
    return {x,y,warning:cycle < warning || beforeRetraction,warningMode:beforeRetraction ? "retraction" : "appearance",active:cycle >= warning && cycle < warning + active,
      retracting,solid:cycle < warning + active,hidden:cycle >= warning + active + retract,
      cycleProgress:cycle/period,activeProgress:clamp((cycle-warning)/Math.max(.01,active),0,1),
      progress:clamp((cycle-warning-active)/Math.max(.01,retract),0,1)};
  }
  return { x:x+Math.sin(phase)*(mover.axis === "x" ? mover.range || 0 : 0),
    y:y+Math.sin(phase)*(mover.axis === "y" ? mover.range || 0 : 0),
    solid:true,hidden:false,active:true,warning:Math.abs(Math.cos(phase)) < .12 };
}

export function syncMovers(world, time = world.time || 0) {
  for (const mover of world.movers || []) {
    mover.baseX ??= mover.x; mover.baseY ??= mover.y;
    Object.assign(mover,sampleMover(mover,time));
    mover.previousX=mover.x;mover.previousY=mover.y;
    mover.prevStartX=mover.startX;mover.prevStartY=mover.startY;
    mover.prevEndX=mover.endX;mover.prevEndY=mover.endY;mover.prevAngle=mover.angle;
    mover.dx=0;mover.dy=0;mover.vx=0;mover.vy=0;
  }
  return world;
}

export function updateMovers(world, time, dt) {
  syncHazards(world,time);
  for (const mover of world.movers || []) {
    mover.baseX ??= mover.x; mover.baseY ??= mover.y;
    const oldX=mover.x,oldY=mover.y,oldDirection=mover._direction,wasSolid=mover.solid;
    mover.previousX=oldX;mover.previousY=oldY;
    mover.prevStartX=mover.startX;mover.prevStartY=mover.startY;
    mover.prevEndX=mover.endX;mover.prevEndY=mover.endY;mover.prevAngle=mover.angle;
    Object.assign(mover,sampleMover(mover,time));
    mover.dx=mover.x-oldX;mover.dy=mover.y-oldY;
    mover.vx=mover.dx/Math.max(dt,.0001);mover.vy=mover.dy/Math.max(dt,.0001);
    mover._direction=Math.sign(mover.axis === "y" ? mover.dy : mover.dx);
    if(oldDirection && mover._direction && oldDirection !== mover._direction || wasSolid === false && mover.solid)
      emit(world,"fx-platform-activation",mover.x+mover.w/2,surfaceY(mover,mover.x+mover.w/2),.22,.47);
  }
}

// Fixed-step movement uses swept edges, so even a dash cannot tunnel through a wall.
export function updatePlayer(player, world, input, dt) {
  if (!(dt > 0)) return;
  const platforms = world.platforms || [], movers = world.movers || [];
  const surfaces = platforms.map(p => mandatoryGateCollider(p, world)).concat(movers,(world.hazards || []).filter(h => h.type === "timedGate" && h.active));
  // New exploration rooms have a physical world ceiling. Keeping the head on
  // its lower side also closes the top edge against repeated wall jumps.
  const ceilingY = (world.layoutVersion >= 3 || platforms.some(p => p.mandatory)) ? world.bounds?.top : null;
  if (Number.isFinite(ceilingY) && player.y < ceilingY) { player.y=ceilingY; player.vy=Math.max(0,player.vy); }
  let grounded = player.onGround;
  const airJumps = Number(Boolean(player.powers.wings)) + Number(player.boosts?.water > 0);
  const wind = player.boosts?.wind > 0;
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
  if (grounded) { player.airDashUsed = false; player.extraJumps = airJumps; }

  const ridden = grounded && oldGround ? movers.find(mover => mover.id === oldGround) : null;
  if (ridden && !enabled(ridden)) { grounded=false;player.onGround=false;player.coyote=0; }
  if (ridden && enabled(ridden)) {
    const carryFromX=player.x,carryFromY=player.y;
    if(ridden.motion === "pivot" && Number.isFinite(ridden.prevStartX)) {
      const span=ridden.prevEndX-ridden.prevStartX;
      const fraction=clamp(span ? (player.x+player.w/2-ridden.prevStartX)/span : .5,0,1);
      player.x=ridden.startX+(ridden.endX-ridden.startX)*fraction-player.w/2;
      player.y=surfaceY(ridden,player.x+player.w/2)-player.h;
    } else { player.x += ridden.dx || 0; player.y += ridden.dy || 0; }
    if (Number.isFinite(ceilingY)) player.y=Math.max(ceilingY,player.y);
    // Carrying participates in solid collisions too. A ferry cannot force a
    // passenger through a raised bank, and a lift cannot push through a ceiling.
    for(const solid of surfaces) {
      if(solid===ridden || solid.oneWay || solid.kind === "slope" || !enabled(solid))continue;
      if(solid.mandatory || player.y+player.h>solid.y+.01 && player.y<solid.y+solid.h-.01) {
        if(player.x>carryFromX && carryFromX+player.w<=solid.x+.01 && player.x+player.w>solid.x)player.x=solid.x-player.w;
        if(player.x<carryFromX && carryFromX>=solid.x+solid.w-.01 && player.x<solid.x+solid.w)player.x=solid.x+solid.w;
      }
      if(player.x+player.w>solid.x+.01 && player.x<solid.x+solid.w-.01) {
        if(player.y<carryFromY && carryFromY>=solid.y+solid.h-.01 && player.y<solid.y+solid.h)player.y=solid.y+solid.h;
        if(player.y>carryFromY && carryFromY+player.h<=solid.y+.01 && player.y+player.h>solid.y)player.y=solid.y-player.h;
      }
    }
  }

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
    emit(world,player.airAction==="doubleJump"?(player.boosts?.water>0?"fx-water-splash":"fx-wings-flight-aura"):"fx-jump-dust",player.x+player.w/2,player.y+player.h,.28,.35);
    player.onGround = false; player.coyote = 0; player.wallCoyote = 0; player.jumpBuffer = 0;
    if(player.dashTimer>0)player.dashJump=true;
    player.groundId = null;
  }

  if (dashPressed && player.dashCooldown <= 0 && (grounded || !player.airDashUsed)) {
    player.dashTimer = PHYSICS.dashDuration; player.dashCooldown = PHYSICS.dashCooldown * (wind ? .65 : 1);
    player.airDashUsed = !player.onGround; player.vx = (axis ? Math.sign(axis) : player.facing) * PHYSICS.dashSpeed * (wind ? 1.2 : 1);
    player.facing = Math.sign(player.vx); player.dashJump=player.vy<0;
    if(!player.dashJump)player.vy=0;
  }
  const dashing = player.dashTimer > 0;
  if (dashing) {
    player.dashTimer = Math.max(0, player.dashTimer - dt);
    if(player.dashJump){if(!jumpHeld&&player.vy< -PHYSICS.jumpCutVelocity)player.vy=-PHYSICS.jumpCutVelocity;player.vy=Math.min(PHYSICS.maxFall,player.vy+PHYSICS.gravity*dt);}
    else player.vy=0;
  }
  else {
    const speed = (player.powers.sandals ? PHYSICS.sandalsSpeed : PHYSICS.runSpeed) * (wind ? 1.2 : 1);
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
  {
    let highest = Infinity, landing = null;
    for (const platform of surfaces) {
      if (!enabled(platform) || (platform.oneWay && player.dropTimer > 0)) continue;
      if (player.x + player.w <= platform.x + 1 || player.x >= platform.x + platform.w - 1) continue;
      const footX = clamp(player.x + player.w * 0.5, platform.x, platform.x + platform.w);
      const top = surfaceY(platform, footX);
      const slopeSnap = (platform.kind === "slope" || platform.motion === "pivot" && oldGround === platform.id) && grounded && player.vy>=0 && Math.abs(oldBottom - top) <= 24;
      // Test crossing relative to the previous contact plane. This catches a
      // rising lift overtaking an upward jumper, without catching a player who
      // began below a descending one-way deck.
      let previousTop=top-(platform.dy || 0);
      if(platform.motion === "pivot" && Number.isFinite(platform.prevStartX)) {
        const span=platform.prevEndX-platform.prevStartX;
        const u=clamp(span ? (oldX+player.w/2-platform.prevStartX)/span : .5,0,1);
        previousTop=platform.prevStartY+(platform.prevEndY-platform.prevStartY)*u;
      }
      if(grounded && oldGround===platform.id)previousTop=top;
      const relativeFall=(nextY-oldY)+(previousTop-top);
      if(relativeFall<-.001 && !slopeSnap)continue;
      const keepsContact=Math.abs(top-highest)<.01 && platform.id===oldGround;
      if ((oldBottom <= previousTop + 1 || slopeSnap) && (nextY + player.h >= top || slopeSnap) && (top < highest || keepsContact)) { highest = top; landing = platform; }
    }
    if (landing) {
      if(!grounded&&player.vy>300){player.landTimer=.1;emit(world,"fx-landing-dust",player.x+player.w/2,highest,.3,.35);}
      nextY = highest - player.h; player.vy = 0; player.onGround = true; player.groundId = landing.id;
      player.extraJumps = airJumps; player.airDashUsed = false;
      if (!landing.temporary && landing.kind !== "collapsing") player.lastSafe = { x: player.x, y: nextY };
    } else if(player.vy<0) {
    for (const platform of surfaces) {
      if (!enabled(platform) || platform.oneWay || platform.kind === "slope") continue;
      if (player.x + player.w <= platform.x + 1 || player.x >= platform.x + platform.w - 1) continue;
      const bottom = platform.y + platform.h;
      if (oldY >= bottom - 0.1 && nextY <= bottom) { nextY = Math.max(nextY, bottom); player.vy = 0; }
    }
    }
  }
  player.y = Number.isFinite(ceilingY) ? Math.max(ceilingY,nextY) : nextY;
  if (player.y !== nextY && player.vy < 0) player.vy=0;
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

const MECHANISM_HAZARDS = new Set(["pendulum","crusher","retractSpikes","rotatingBeam","timedGate"]);
const LEGACY_TIMED_HAZARDS = new Set(["fireJet","flameJet","lightning","fallingRock"]);
export function sampleHazard(hazard,time) {
  const legacy=LEGACY_TIMED_HAZARDS.has(hazard.type);
  const period=hazard.period || (legacy ? 3.6 : 5.8),warning=hazard.warningDuration ?? (legacy ? .8 : 1.2),active=hazard.activeDuration ?? (legacy ? .65 : 1);
  const cycle=modulo(time+(hazard.timeOffset || 0)+(hazard.phase || 0),period);
  const progress=clamp((cycle-warning)/Math.max(active,.01),0,1);
  const state={warning:cycle<warning,active:cycle>=warning && cycle<warning+active,progress};
  if(hazard.type === "pendulum") {
    const anchorX=hazard.pivotX ?? hazard.anchorX ?? hazard.x+hazard.w/2;
    const anchorY=hazard.pivotY ?? hazard.anchorY ?? hazard.baseY ?? hazard.y;
    const angle=(hazard.baseAngle || 0)-Math.cos(progress*TAU)*(hazard.angleRange ?? .65);
    const radius=hazard.ballRadius || 22,length=hazard.length || 145;
    const tipX=anchorX+Math.sin(angle)*length,tipY=anchorY+Math.cos(angle)*length;
    return {...state,anchorX,anchorY,tipX,tipY,angle,ballRadius:radius,x:tipX-radius,y:tipY-radius,w:radius*2,h:radius*2};
  }
  if(hazard.type === "rotatingBeam") {
    const pivotX=hazard.pivotX ?? hazard.x+hazard.w/2,pivotY=hazard.pivotY ?? hazard.baseY ?? hazard.y;
    const angle=(hazard.baseAngle || 0)+progress*TAU,length=hazard.length || 160,radius=hazard.radius || 10;
    const halfX=Math.cos(angle)*length/2,halfY=Math.sin(angle)*length/2;
    return {...state,angle,startX:pivotX-halfX,startY:pivotY-halfY,endX:pivotX+halfX,endY:pivotY+halfY,
      x:pivotX-Math.abs(halfX)-radius,y:pivotY-Math.abs(halfY)-radius,w:Math.abs(halfX)*2+radius*2,h:Math.abs(halfY)*2+radius*2,radius};
  }
  if(hazard.type === "crusher") {
    const stroke=Math.sin(progress*Math.PI),baseY=hazard.baseY ?? hazard.y;
    return {...state,y:baseY+stroke*(hazard.travel ?? 120)};
  }
  if(hazard.type === "retractSpikes") {
    const maxHeight=hazard.maxHeight || 46,baseY=hazard.baseY ?? hazard.y;
    const height=state.active ? maxHeight*Math.min(1,progress*6,(1-progress)*6) : 0;
    return {...state,y:baseY-height,h:height,maxHeight};
  }
  if(hazard.type === "fallingRock") {
    const targetY=hazard.targetY ?? (hazard.baseY ?? hazard.y)+(hazard.baseHeight ?? hazard.h)-44;
    return {...state,targetY,x:hazard.x,y:targetY-440+progress*progress*440,w:44,h:44,
      telegraph:state.warning ? {x:hazard.x-12,y:targetY+32,w:68,h:12} : null};
  }
  if(legacy)return state;
  return {...state,kind:"gate",oneWay:false,solid:state.active};
}

export function syncHazards(world,time=world.time || 0) {
  for(const hazard of world.hazards || []) {
    if(!MECHANISM_HAZARDS.has(hazard.type) && !LEGACY_TIMED_HAZARDS.has(hazard.type))continue;
    const first=!hazard._clockSynced;
    hazard.baseY ??= hazard.y;
    hazard.baseHeight ??= hazard.h;
    hazard.pivotX ??= hazard.anchorX ?? hazard.x+hazard.w/2;
    hazard.pivotY ??= hazard.anchorY ?? hazard.y;
    hazard.previousX=hazard.x;hazard.previousY=hazard.y;
    hazard.prevStartX=hazard.startX;hazard.prevStartY=hazard.startY;
    hazard.prevEndX=hazard.endX;hazard.prevEndY=hazard.endY;
    hazard.prevTipX=hazard.tipX;hazard.prevTipY=hazard.tipY;
    Object.assign(hazard,sampleHazard(hazard,time));
    if(first) {
      hazard.previousX=hazard.x;hazard.previousY=hazard.y;
      hazard.prevStartX=hazard.startX;hazard.prevStartY=hazard.startY;
      hazard.prevEndX=hazard.endX;hazard.prevEndY=hazard.endY;
      hazard.prevTipX=hazard.tipX;hazard.prevTipY=hazard.tipY;
    }
    hazard._clockSynced=true;
  }
  return world;
}

function segmentHitsBox(x1,y1,x2,y2,box,radius=0) {
  let low=0,high=1;
  for(const [origin,delta,min,max] of [[x1,x2-x1,box.x-radius,box.x+box.w+radius],[y1,y2-y1,box.y-radius,box.y+box.h+radius]]) {
    if(Math.abs(delta)<1e-9) { if(origin<min || origin>max)return false;continue; }
    const a=(min-origin)/delta,b=(max-origin)/delta;
    low=Math.max(low,Math.min(a,b));high=Math.min(high,Math.max(a,b));
    if(low>high)return false;
  }
  return true;
}

function pointSegmentDistanceSquared(x,y,x1,y1,x2,y2) {
  const dx=x2-x1,dy=y2-y1,length=dx*dx+dy*dy;
  const t=clamp(length ? ((x-x1)*dx+(y-y1)*dy)/length : 0,0,1);
  return (x-x1-t*dx)**2+(y-y1-t*dy)**2;
}

function capsuleHitsBox(hazard,box) {
  const {startX:x1,startY:y1,endX:x2,endY:y2}=hazard,radius=hazard.radius || 10;
  if(segmentHitsBox(x1,y1,x2,y2,box))return true;
  for(const [x,y] of [[x1,y1],[x2,y2]]) {
    const nearX=clamp(x,box.x,box.x+box.w),nearY=clamp(y,box.y,box.y+box.h);
    if((x-nearX)**2+(y-nearY)**2<=radius*radius)return true;
  }
  for(const x of [box.x,box.x+box.w])for(const y of [box.y,box.y+box.h])
    if(pointSegmentDistanceSquared(x,y,x1,y1,x2,y2)<=radius*radius)return true;
  return false;
}

export function hazardHitsPlayer(hazard,player) {
  if(!hazard.active)return false;
  if(hazard.type === "retractSpikes") {
    if(!(hazard.h>0) || !overlaps(player,hazard))return false;
    // The supplied strip has three triangular prongs. At the lowest overlapping
    // row each triangle is widest, so testing that section is an exact triangle
    // versus axis-aligned player test rather than the trap's empty outer box.
    const bottom=Math.min(player.y+player.h,hazard.y+hazard.h);
    const half=hazard.w*(hazard.spikeWidthFraction ?? .25)*.5*clamp((bottom-hazard.y)/hazard.h,0,1);
    return (hazard.spikeCenters || [.125,.5,.875]).some(u=>{
      const x=hazard.x+hazard.w*u;return player.x<x+half && player.x+player.w>x-half;
    });
  }
  if(hazard.type === "pendulum") {
    const x=clamp(hazard.tipX,player.x,player.x+player.w),y=clamp(hazard.tipY,player.y,player.y+player.h);
    return (x-hazard.tipX)**2+(y-hazard.tipY)**2<=(hazard.ballRadius || 22)**2;
  }
  if(hazard.type === "rotatingBeam")
    return capsuleHitsBox(hazard,player);
  return hazard.h>0 && overlaps(player,hazard);
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
    if (platform.switchId && !platform.exploration) platform.open = (world.switches || []).some(s => s.id === platform.switchId && s.activated);
  }
  for (const sw of world.switches || []) {
    if (sw.exploration) continue; // Identity checks, guardians and opening animation live in gate-progress.
    if (!sw.activated && overlaps(player, sw)) {
      sw.activated = true;
      emit(world,"fx-platform-activation",sw.x+sw.w/2,sw.y+sw.h,.3,.47);
      for (const gate of world.platforms || []) if (gate.switchId === sw.id) gate.open = true;
      for (const prop of world.props || []) if (prop.switchId === sw.id) { prop.open = true; prop.artFrame = "gate-open"; }
    }
  }
  for (const hazard of world.hazards || []) {
    if (hazard.dead) continue;
    hazard.animTime = (hazard.animTime || 0) + dt;
    if (hazard.type === "wind") {
      hazard.active = true;
      if (overlaps(player, hazard) && !player.onGround && !player.dashTimer) player.vx += Math.sin((time + (hazard.timeOffset || 0)) * 1.4 + (hazard.phase || 0)) * (hazard.force || 170) * dt;
      continue;
    }
    if (MECHANISM_HAZARDS.has(hazard.type) || LEGACY_TIMED_HAZARDS.has(hazard.type)) {
      hazard.baseY ??= hazard.y;
      hazard.baseHeight ??= hazard.h;
      hazard.pivotX ??= hazard.anchorX ?? hazard.x+hazard.w/2;
      hazard.pivotY ??= hazard.anchorY ?? hazard.y;
      Object.assign(hazard,sampleHazard(hazard,time));
    } else if (hazard.type === "chase") {
      if (!hazard.triggered && player.x > hazard.triggerX) { hazard.triggered = true; hazard.x = player.x - 560; }
      hazard.active = Boolean(hazard.triggered && player.x < hazard.endX);
      if (hazard.active) hazard.x = Math.min(player.x - 75, hazard.x + (hazard.speed || 335) * dt);
    } else hazard.active = true;
    if (hazardHitsPlayer(hazard, player)) damagePlayer(player, hazard.damage || 1, { x: player.x < hazard.x + hazard.w / 2 ? -240 : 240, y: -400 });
  }
  world.furthestX = Math.max(world.furthestX || 0, player.x);
}

// Offline route proofs are valid only for this complete controller dependency
// set. Add every new helper here when extending the physics implementation.
export const PHYSICS_PROOF_SIGNATURE = [
  createPlayer,surfaceY,enabled,mandatoryGateCollider,emit,setState,modulo,sampleMover,syncMovers,
  updateMovers,updatePlayer,damagePlayer,sampleHazard,syncHazards,segmentHitsBox,
  pointSegmentDistanceSquared,capsuleHitsBox,hazardHitsPlayer,updateWorldEnvironment,
  approach,clamp,overlaps,
].map(fn=>fn.toString().replace(/\s+/g," ")).join("\n")+JSON.stringify({
  TAU,VIEW,mechanisms:[...MECHANISM_HAZARDS],legacyTimed:[...LEGACY_TIMED_HAZARDS],
});
