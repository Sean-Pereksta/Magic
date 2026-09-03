import { PHYSICS, VIEW } from "./config.js";
import { approach, clamp, overlaps } from "./common.js";

export function createPlayer(x = 80, y = VIEW.floorY - PHYSICS.playerHeight) {
  return {
    x, y,
    w: PHYSICS.playerWidth,
    h: PHYSICS.playerHeight,
    vx: 0,
    vy: 0,
    facing: 1,
    onGround: false,
    groundId: null,
    coyote: 0,
    jumpBuffer: 0,
    dropTimer: 0,
    invulnerable: 0,
    hearts: 4,
    maxHearts: 4,
    powers: {
      sandals: false,
      wings: false,
      shield: false,
      bread: false,
      holyLight: 0,
      fireRain: 0,
    },
    extraJumps: 0,
    lastSafe: { x, y },
  };
}

function horizontalCollision(player, platform) {
  if (platform.oneWay || platform.dead || !overlaps(player, platform)) return;
  if (player.vx > 0) player.x = platform.x - player.w;
  else if (player.vx < 0) player.x = platform.x + platform.w;
  player.vx = 0;
}

function resolveLanding(player, platform, previousBottom) {
  if (platform.dead || player.dropTimer > 0) return false;
  const currentBottom = player.y + player.h;
  const crossedTop = previousBottom <= platform.y + 7 && currentBottom >= platform.y;
  const hasWidth = player.x + player.w > platform.x + 3 && player.x < platform.x + platform.w - 3;
  if (player.vy >= 0 && crossedTop && hasWidth) {
    player.y = platform.y - player.h;
    player.vy = 0;
    player.onGround = true;
    player.groundId = platform.id;
    if (!platform.temporary) player.lastSafe = { x: player.x, y: player.y };
    return true;
  }
  return false;
}

export function updateMovers(world, time, dt) {
  for (const mover of world.movers) {
    const previousX = mover.x;
    const previousY = mover.y;
    const phase = time * mover.speed + mover.phase;
    mover.x = mover.baseX + Math.sin(phase) * (mover.axis === "x" ? mover.range : 0);
    mover.y = mover.baseY + Math.sin(phase) * (mover.axis === "y" ? mover.range : 0);
    mover.dx = mover.x - previousX;
    mover.dy = mover.y - previousY;
    mover.vx = mover.dx / Math.max(dt, 0.0001);
    mover.vy = mover.dy / Math.max(dt, 0.0001);
  }
}

export function updatePlayer(player, world, input, dt) {
  const axis = input.horizontal();
  const jumpPressed = input.jumpPressed();
  const maxSpeed = player.powers.sandals ? PHYSICS.sandalsSpeed : PHYSICS.runSpeed;
  const acceleration = player.onGround ? PHYSICS.groundAcceleration : PHYSICS.airAcceleration;
  const friction = player.onGround ? PHYSICS.groundFriction : PHYSICS.airFriction;

  if (Math.abs(axis) > 0.02) {
    player.vx = approach(player.vx, axis * maxSpeed, acceleration * dt);
    player.facing = axis > 0 ? 1 : -1;
  } else {
    player.vx = approach(player.vx, 0, friction * dt);
  }

  if (jumpPressed) player.jumpBuffer = PHYSICS.jumpBuffer;
  player.jumpBuffer = Math.max(0, player.jumpBuffer - dt);
  player.coyote = player.onGround ? PHYSICS.coyoteTime : Math.max(0, player.coyote - dt);
  player.dropTimer = Math.max(0, player.dropTimer - dt);
  player.invulnerable = Math.max(0, player.invulnerable - dt);

  if (input.downHeld() && player.onGround && jumpPressed) {
    player.dropTimer = PHYSICS.dropThroughTime;
    player.onGround = false;
    player.coyote = 0;
    player.jumpBuffer = 0;
    player.y += 6;
  } else if (player.jumpBuffer > 0 && (player.coyote > 0 || player.extraJumps > 0)) {
    if (player.coyote <= 0) player.extraJumps--;
    player.vy = -PHYSICS.jumpVelocity;
    player.onGround = false;
    player.coyote = 0;
    player.jumpBuffer = 0;
  }

  if (!input.jumpHeld() && player.vy < -310) player.vy += PHYSICS.gravity * 2.15 * dt;

  const ridden = player.groundId
    ? world.movers.find((mover) => mover.id === player.groundId)
    : null;
  if (player.onGround && ridden) {
    player.x += ridden.dx;
    player.y += ridden.dy;
  }

  player.x += player.vx * dt;
  for (const platform of [...world.platforms, ...world.movers]) horizontalCollision(player, platform);

  const previousBottom = player.y + player.h;
  player.vy = clamp(player.vy + PHYSICS.gravity * dt, -1400, PHYSICS.maxFall);
  player.y += player.vy * dt;
  player.onGround = false;
  player.groundId = null;
  for (const platform of [...world.platforms, ...world.movers]) {
    if (resolveLanding(player, platform, previousBottom)) break;
  }
  if (player.onGround) player.extraJumps = player.powers.wings ? 1 : 0;
}

export function damagePlayer(player, amount = 1, knockback = null) {
  if (player.invulnerable > 0) return false;
  if (player.powers.shield) {
    player.powers.shield = false;
    player.invulnerable = 0.7;
    return false;
  }
  player.hearts = Math.max(0, player.hearts - amount);
  player.invulnerable = 1.05;
  if (knockback) {
    player.vx = knockback.x || 0;
    player.vy = knockback.y || -430;
  }
  return true;
}
