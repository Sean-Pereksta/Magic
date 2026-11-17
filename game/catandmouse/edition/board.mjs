import { MAPS } from './battlefield.mjs';
import { isWall, wallConnections, wallKey } from './walls.mjs';

const artRoot = new URL('./art/', import.meta.url);
const sources = { title: 0, 'mill-preview': 0, ground: 0, walls: 0, terrain: 160, units: 160, facing: 160, towers: 160, effects: 112 };
const art = {};
const groundCache = new Map();
const tilePixels = 24;
const waterSwatch = 260;
const towers = {
  wall: 0, turret: 1, generator: 2, rocket: 3, repair: 4, acorn: 5, web: 6, rabbit: 7,
  marksman: 8, burst: 9, tesla: 10, laser: 11, barricade: 0, farm: 2, shield: 10,
  drum: 4, slime: 6, flea: 7, payload: 3, cluster: 3,
};
export const towerCell = type => towers[type] ?? 0;
export const artUrl = name => new URL(name + '-v1.webp', artRoot).href;

export async function loadPaintings() {
  await Promise.all(Object.keys(sources).map(async name => {
    if (art[name]) return;
    const image = new Image();
    image.src = artUrl(name);
    await image.decode();
    art[name] = image;
  }));
}

function sprite(ctx, sheet, index, x, y, width, height = width, anchor = .91) {
  const size = sources[sheet];
  ctx.drawImage(art[sheet], index % 4 * size, Math.floor(index / 4) * size, size, size,
    x - width / 2, y - height * anchor, width, height);
}

function pattern(ctx, column, row, pixelSpan) {
  const swatch = document.createElement('canvas');
  swatch.width = swatch.height = pixelSpan * 2;
  const brush = swatch.getContext('2d');
  const size = art.ground.width / 2;
  for (let y = 0; y < 2; y++) {
    for (let x = 0; x < 2; x++) {
      brush.save();
      brush.translate(x * pixelSpan + (x ? pixelSpan : 0), y * pixelSpan + (y ? pixelSpan : 0));
      brush.scale(x ? -1 : 1, y ? -1 : 1);
      brush.drawImage(art.ground, column * size, row * size, size, size, 0, 0, pixelSpan, pixelSpan);
      brush.restore();
    }
  }
  return ctx.createPattern(swatch, 'repeat');
}

function paintGround(map) {
  if (groundCache.has(map.id)) return groundCache.get(map.id);
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = map.size * tilePixels;
  const ctx = canvas.getContext('2d', { alpha: false });
  const floor = pattern(ctx, 0, 0, 155);
  const path = pattern(ctx, 1, 0, 180);
  const water = pattern(ctx, 0, 1, waterSwatch);
  const damp = pattern(ctx, 1, 1, 190);
  ctx.fillStyle = floor;
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  for (const [tile, paint] of [[':', path], ['#', damp], ['~', water]]) {
    ctx.fillStyle = paint;
    map.tiles.forEach((line, row) => [...line].forEach((cell, column) => {
      if (cell === tile) ctx.fillRect(column * tilePixels, row * tilePixels, tilePixels, tilePixels);
    }));
  }
  const shoreline = new Path2D();
  const waterShape = new Path2D();
  const waterBounds = { left: Infinity, top: Infinity, right: -Infinity, bottom: -Infinity };
  map.tiles.forEach((row, y) => [...row].forEach((tile, x) => {
    if (tile !== '~') return;
    const left = x * tilePixels;
    const top = y * tilePixels;
    const right = left + tilePixels;
    const bottom = top + tilePixels;
    waterShape.rect(left, top, tilePixels, tilePixels);
    waterBounds.left = Math.min(waterBounds.left, x - .5);
    waterBounds.top = Math.min(waterBounds.top, y - .5);
    waterBounds.right = Math.max(waterBounds.right, x + .5);
    waterBounds.bottom = Math.max(waterBounds.bottom, y + .5);
    if (map.tiles[y - 1]?.[x] !== '~') { shoreline.moveTo(left, top); shoreline.lineTo(right, top); }
    if (map.tiles[y + 1]?.[x] !== '~') { shoreline.moveTo(left, bottom); shoreline.lineTo(right, bottom); }
    if (row[x - 1] !== '~') { shoreline.moveTo(left, top); shoreline.lineTo(left, bottom); }
    if (row[x + 1] !== '~') { shoreline.moveTo(right, top); shoreline.lineTo(right, bottom); }
  }));
  strokeShore(ctx, shoreline);
  const ground = { canvas, water, waterShape, shoreline, waterBounds };
  groundCache.set(map.id, ground);
  return ground;
}

function strokeShore(ctx, shoreline) {
  ctx.lineCap = 'round';
  ctx.lineWidth = 6;
  ctx.strokeStyle = '#172c2bc7';
  ctx.stroke(shoreline);
  ctx.lineWidth = 2.6;
  ctx.strokeStyle = '#a69269c7';
  ctx.stroke(shoreline);
}

export function prepareBattlefield(mapId) {
  const map = MAPS[mapId];
  if (!map || map.tiles.length !== map.size || map.tiles.some(row => row.length !== map.size)) {
    throw new Error('The battlefield file is invalid.');
  }
  paintGround(map);
}

const shots = { bullet: 0, rabbit: 0, rocket: 2, acorn: 3, web: 1 };
const colors = { runner: '#edbb6c', stink: '#a6c676', ox: '#da8362', vulture: '#b9a3d4', king: '#edd472', cat: '#f28f63' };
const unitScale = { mouse: 49, rat: 43, runner: 38, stink: 51, ox: 61, vulture: 44, king: 62, cat: 105, rabbit: 43, flea: 28 };
const clamp = (x, lower, upper) => Math.max(lower, Math.min(upper, x));
const mix = (from, to, amount) => from + (to - from) * amount;

function screenDirection(entity, previous) {
  const moved = previous && Math.hypot(entity.x - previous.x, entity.y - previous.y) > .006;
  const moving = Math.hypot(entity.vx || 0, entity.vy || 0) > .01;
  const [dx, dy] = moving ? [entity.vx, entity.vy]
    : moved ? [entity.x - previous.x, entity.y - previous.y]
      : Number.isFinite(entity.facing) ? [Math.cos(entity.facing), Math.sin(entity.facing)] : [0, 0];
  return { horizontal: dx - dy, vertical: (dx + dy) / 2, moving: moving || moved };
}

export class Battlefield {
  constructor(canvas, minimap) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d', { alpha: false });
    this.minimap = minimap;
    this.mini = minimap.getContext('2d', { alpha: false });
    this.camera = { x: 23, y: 25, zoom: 1, follow: true };
    this.effects = [];
    this.facing = new Map();
  }

  reset(mapId) {
    this.map = MAPS[mapId];
    this.previous = this.state = null;
    this.localId = null;
    this.camera.x = this.map.playerSpawns[0].x;
    this.camera.y = this.map.playerSpawns[0].y;
    this.camera.follow = true;
    this.effects = [];
    this.facing.clear();
    this.eventId = 0;
    this.lastMini = -Infinity;
    this.lastFrame = 0;
    this.ground = paintGround(this.map);
    this.scenery = this.map.tiles.flatMap((line, y) => [...line].flatMap((tile, x) =>
      tile === '#' ? [{ x, y, index: (x + y * 3) % 4 === 0 ? 4 : 5 }] : [],
    ));
    const canvas = document.createElement('canvas');
    canvas.width = this.map.size;
    canvas.height = this.map.size;
    const map = canvas.getContext('2d');
    this.map.tiles.forEach((row, y) => [...row].forEach((tile, x) => {
      map.fillStyle = { '.': '#436654', ':': '#a48753', '~': '#458a95', '#': '#243c33' }[tile];
      map.fillRect(x, y, 1, 1);
    }));
    this.miniTerrain = canvas;
  }

  push(state, events, id) {
    const now = performance.now();
    const first = !this.state;
    if (this.state?.tick !== state.tick) {
      const delay = this.state ? clamp(now - this.received, 45, 140) : 50;
      this.previous = this.state;
      this.previousEntities = new Map();
      for (const group of ['players', 'enemies', 'allies', 'projectiles']) {
        for (const entity of this.previous?.[group] || []) this.previousEntities.set(entity.id, entity);
      }
      this.received = now;
      this.duration = delay;
    }
    this.state = state;
    this.localId = id;
    const player = state.players.find(entry => entry.id === id);
    if (first && player) Object.assign(this.camera, { x: player.x, y: player.y });
    const recent = [];
    for (const event of events || []) {
      if (event.id <= this.eventId) continue;
      this.eventId = event.id;
      if (event.time < state.time - 400 || (event.type === 'impact' && event.reason === 'expired')) continue;
      const type = event.type === 'shot' ? 'launch'
        : event.type === 'attack' ? ({ tesla: 'arc', laser: 'beam', claw: 'claw', pounce: 'claw' }[event.kind] || event.type)
          : event.type;
      this.effects.push({ ...event, type, birth: now });
      recent.push(event);
    }
    return recent;
  }

  scale(direction) {
    this.camera.zoom = clamp(this.camera.zoom * (direction > 0 ? 1.16 : 1 / 1.16), .55, 1.8);
  }

  pan(x, y) {
    Object.assign(this.camera, { x: clamp(x, 0, this.map.size - 1), y: clamp(y, 0, this.map.size - 1), follow: false });
  }

  coordinates(screenX, screenY) {
    const x = (screenX - this.width / 2) / this.tileX;
    const y = (screenY - this.height * .52) / this.tileY;
    return { x: this.camera.x + (x + y) / 2, y: this.camera.y + (y - x) / 2 };
  }

  project(x, y) {
    const dx = x - this.camera.x;
    const dy = y - this.camera.y;
    return [this.width / 2 + (dx - dy) * this.tileX, this.height * .52 + (dx + dy) * this.tileY];
  }

  position(entity, fraction) {
    const previous = this.previousEntities.get(entity.id);
    return previous && Math.hypot(previous.x - entity.x, previous.y - entity.y) < 4
      ? { x: mix(previous.x, entity.x, fraction), y: mix(previous.y, entity.y, fraction) }
      : entity;
  }

  draw(now, settings, target) {
    if (!this.state || !art.terrain) return;
    const ctx = this.ctx;
    const ratio = Math.min(devicePixelRatio || 1, settings.quality === 'low' ? 1 : 2);
    this.width = this.canvas.clientWidth;
    this.height = this.canvas.clientHeight;
    if (!this.width || !this.height) return;
    if (this.canvas.width !== Math.round(this.width * ratio) || this.canvas.height !== Math.round(this.height * ratio)) {
      this.canvas.width = Math.round(this.width * ratio);
      this.canvas.height = Math.round(this.height * ratio);
    }
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    this.tileX = (this.width < 600 ? 23 : 31) * this.camera.zoom;
    this.tileY = this.tileX / 2;
    const fraction = clamp((now - this.received) / this.duration, 0, 1);
    const elapsed = Math.min(100, now - (this.lastFrame || now - 16));
    this.lastFrame = now;
    const player = this.state.players.find(entry => entry.id === this.localId);
    if (player && this.camera.follow) {
      const point = this.position(player, fraction);
      const follow = settings.reduced ? 1 : 1 - Math.exp(-elapsed / 58);
      this.camera.x = mix(this.camera.x, point.x, follow);
      this.camera.y = mix(this.camera.y, point.y, follow);
    }
    ctx.fillStyle = '#142722';
    ctx.fillRect(0, 0, this.width, this.height);
    const waterTime = this.previous ? mix(this.previous.time, this.state.time, fraction) : this.state.time;
    this.drawGround(waterTime, settings.reduced);
    const walls = new Map(this.state.structures.filter(entry => isWall(entry.type)).map(entry => [wallKey(entry.x, entry.y), entry]));
    if (target) {
      this.diamond(target.x, target.y, '#efcc78', '#efcc7840');
      if (isWall(target.type) && !walls.has(wallKey(target.x, target.y))) {
        this.wall(target, target, walls, true);
      }
    }
    const depth = [];
    const enqueue = (group, draw) => group.forEach(entity => {
      const position = this.position(entity, fraction);
      const [x, y] = this.project(position.x, position.y);
      if (x < -100 || x > this.width + 100 || y < -65 || y > this.height + 150) return;
      depth.push({ depth: position.x + position.y, draw: () => draw(entity, position) });
    });
    enqueue(this.map.spawns, (e, p) => this.prop(6, p, 1.6));
    enqueue(this.state.objectives.filter(e => e.hp > 0), (e, p) => {
      this.prop(e.id === 'sacks' ? 4 : e.id === 'hoist' ? 5 : 7, p, 1.4);
      const [x, y] = this.project(p.x, p.y);
      this.health(x, y - 49 * this.camera.zoom, e.hp, e.maxHp, '#eed095', true);
    });
    enqueue(this.state.pickups, (e, p) => {
      const [x, y] = this.project(p.x, p.y);
      sprite(ctx, 'effects', 8, x, y - (settings.reduced ? 0 : Math.sin(now / 310 + p.x) * 2), 24 * this.camera.zoom);
    });
    enqueue(this.state.structures, (e, p) => {
      if (isWall(e.type)) this.wall(e, p, walls);
      else this.structure(e, p, settings);
    });
    enqueue(this.state.players, (e, p) => this.unit(e, p, 'mouse', now, settings));
    enqueue(this.state.enemies, (e, p) => this.unit(e, p, e.type, now, settings));
    enqueue(this.state.allies, (e, p) => this.unit(e, p, e.type, now, settings));
    enqueue(this.scenery, (e, p) => this.prop(e.index, p, 1.08));
    depth.sort((a, b) => a.depth - b.depth).forEach(item => item.draw());
    this.state.projectiles.forEach(shot => this.projectile(shot, this.position(shot, fraction)));
    this.drawEffects(now, settings);
    if (now - this.lastMini > 125) {
      this.drawMinimap();
      this.lastMini = now;
    }
  }

  waterInView(bounds) {
    if (!Number.isFinite(bounds.left)) return false;
    const corners = [this.project(bounds.left, bounds.top), this.project(bounds.right, bounds.top),
      this.project(bounds.left, bounds.bottom), this.project(bounds.right, bounds.bottom)];
    const xs = corners.map(point => point[0]);
    const ys = corners.map(point => point[1]);
    return Math.max(...xs) >= 0 && Math.min(...xs) <= this.width
      && Math.max(...ys) >= 0 && Math.min(...ys) <= this.height;
  }

  drawGround(time, reduced) {
    const ctx = this.ctx;
    const { canvas, water, waterShape, shoreline, waterBounds } = this.ground;
    const [x, y] = this.project(-.5, -.5);
    ctx.save();
    ctx.transform(this.tileX / tilePixels, this.tileY / tilePixels,
      -this.tileX / tilePixels, this.tileY / tilePixels, x, y);
    ctx.drawImage(canvas, 0, 0);
    if (!reduced && this.waterInView(waterBounds)) {
      // Shift the seamless painted current in world space and keep the shoreline fixed.
      const drift = 8 * Math.sin(time / 4300);
      const flow = (time * .015) % (waterSwatch * 2);
      water.setTransform(new DOMMatrix().translate(drift, flow));
      ctx.fillStyle = water;
      ctx.fill(waterShape);
      strokeShore(ctx, shoreline);
    }
    ctx.restore();
  }

  prop(index, point, scale) {
    const [x, y] = this.project(point.x, point.y);
    sprite(this.ctx, 'terrain', index, x, y + 5 * this.camera.zoom, this.tileX * 2 * scale);
  }

  diamond(x, y, stroke, fill) {
    const [sx, sy] = this.project(x, y);
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.moveTo(sx - this.tileX, sy);
    ctx.lineTo(sx, sy - this.tileY);
    ctx.lineTo(sx + this.tileX, sy);
    ctx.lineTo(sx, sy + this.tileY);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.strokeStyle = stroke;
    ctx.fill();
    ctx.stroke();
  }

  health(x, y, hp, maxHp, color, always = false) {
    if ((!always && hp >= maxHp) || maxHp <= 0) return;
    const ctx = this.ctx;
    ctx.fillStyle = '#12201c';
    ctx.fillRect(x - 16, y, 32, 5);
    ctx.fillStyle = color;
    ctx.fillRect(x - 15, y + 1, 30 * clamp(hp / maxHp, 0, 1), 3);
  }

  wall(entity, point, walls, ghost = false) {
    const ctx = this.ctx;
    const [x, y] = this.project(point.x, point.y);
    const thick = entity.type === 'barricade';
    const height = (thick ? 39 : 29) * this.camera.zoom;
    const size = art.walls.height;
    const cell = thick ? 2 : 0;
    const arms = wallConnections(entity, walls);
    ctx.save();
    if (ghost || entity.readyAt > this.state.time) ctx.globalAlpha = .57;
    if (entity.hp < entity.maxHp * .38) ctx.filter = 'saturate(.68) brightness(.75)';
    const arm = direction => {
      const [toX, toY] = this.project(point.x + direction.x * .51, point.y + direction.y * .51);
      const left = toX < x;
      const startX = left ? toX : x;
      const startY = left ? toY : y;
      const endY = left ? y : toY;
      ctx.save();
      ctx.transform(Math.abs(toX - x) + .5, endY - startY, 0, 1, startX, startY);
      ctx.drawImage(art.walls, cell * size, 0, size, size, 0, -height, 1, height);
      ctx.restore();
    };
    arms.filter(direction => direction.x + direction.y < 0).forEach(arm);
    arms.filter(direction => direction.x + direction.y > 0).forEach(arm);
    const postWidth = height * .66;
    ctx.drawImage(art.walls, size, 0, size, size, x - postWidth / 2, y - height * 1.12, postWidth, height * 1.19);
    ctx.restore();
    if (!ghost) this.health(x, y - height * 1.18, entity.hp, entity.maxHp, '#e5b161');
  }

  structure(entity, point, settings) {
    const [x, y] = this.project(point.x, point.y);
    const ctx = this.ctx;
    const time = this.state.time;
    const zoom = this.camera.zoom;
    if (entity.readyAt > time) this.diamond(point.x, point.y, '#e7ba69', '#e7ba692f');
    ctx.save();
    ctx.translate(x, y);
    const facing = (entity.aimX - entity.x) - (entity.aimY - entity.y);
    if (facing < -.12) ctx.scale(-1, 1);
    const recoil = !settings.reduced && time - entity.lastAttackAt < 140 ? 3 * zoom : 0;
    if (entity.disabledUntil > time) ctx.filter = 'saturate(.4) brightness(.75)';
    else if (entity.hp < entity.maxHp * .38) ctx.filter = 'saturate(.7) brightness(.72)';
    if (entity.readyAt > time) ctx.globalAlpha = .5;
    sprite(ctx, 'towers', towerCell(entity.branch || entity.type), -recoil, 6 * zoom, 57 * zoom);
    if (entity.windupUntil > time || entity.branch === 'payload' || entity.branch === 'cluster') {
      ctx.fillStyle = entity.branch === 'cluster' ? '#81d8cb' : '#fac578';
      ctx.beginPath();
      ctx.arc(11 * zoom, -24 * zoom, 2.7 * zoom, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
    this.health(x, y - 43 * zoom, entity.hp, entity.maxHp, '#e5b161');
  }

  unit(entity, point, type, now, settings) {
    const ctx = this.ctx;
    const [x, y] = this.project(point.x, point.y);
    const time = this.state.time;
    const zoom = this.camera.zoom;
    const previous = this.previousEntities.get(entity.id);
    const { horizontal, vertical, moving } = screenDirection(entity, previous);
    if (Math.abs(horizontal) > .045) this.facing.set(entity.id, Math.sign(horizontal));
    const useVertical = type === 'mouse' && Math.abs(vertical) > .045 && Math.abs(horizontal) < Math.abs(vertical) * .65;
    const attacking = entity.windupUntil > time || time - entity.lastAttackAt < 150;
    const hit = time - entity.lastHitAt < 140;
    const phase = attacking ? 3 : moving && !settings.reduced ? 1 + Math.floor(now / 102) % 2 : 0;
    const row = type === 'mouse' ? 0 : type === 'cat' ? 2 : type === 'rabbit' || type === 'flea' ? 3 : 1;
    if (entity.id === this.localId || colors[type]) this.diamond(point.x, point.y, colors[type] || '#fff1ac', '#251f1744');
    ctx.save();
    ctx.translate(x, y + (type === 'vulture' ? -12 * zoom : 2 * zoom));
    ctx.scale(useVertical ? 1 : this.facing.get(entity.id) || 1, 1);
    if (hit) ctx.filter = 'brightness(1.55) saturate(.65)';
    else if (type === 'stink') ctx.filter = 'sepia(.3) hue-rotate(36deg)';
    else if (type === 'vulture') ctx.filter = 'grayscale(.62)';
    else if (type === 'king') ctx.filter = 'sepia(.55) saturate(1.5)';
    if (entity.hp <= 0) ctx.globalAlpha = .35;
    const width = (unitScale[type] || 44) * zoom;
    sprite(ctx, useVertical ? 'facing' : 'units', useVertical ? (vertical < 0 ? 4 : 0) + phase : row * 4 + phase,
      0, 0, width * (1 + (phase === 0 && !settings.reduced ? Math.sin(now / 310) * .012 : 0)), width);
    ctx.restore();
    this.health(x, y - (type === 'cat' ? 73 : 33) * zoom, entity.hp, entity.maxHp, row === 1 || row === 2 ? '#ed856e' : '#90d2a1', type === 'cat');
  }

  projectile(shot, point) {
    const [x, y] = this.project(point.x, point.y);
    const ctx = this.ctx;
    const scale = this.camera.zoom;
    const width = (shot.kind === 'rocket' ? 33 : shot.kind === 'web' ? 21 : 18) * scale;
    ctx.save();
    ctx.translate(x, y - 18 * scale);
    if (shot.kind !== 'web') ctx.rotate(Math.atan2((shot.vx + shot.vy) * this.tileY, (shot.vx - shot.vy) * this.tileX));
    sprite(ctx, 'effects', shots[shot.kind] ?? 0, 0, 0, width, width, .53);
    ctx.restore();
  }

  drawEffects(now, settings) {
    const ctx = this.ctx;
    this.effects = this.effects.filter(event => now - event.birth < (event.type === 'windup' ? 1000 : 530)).slice(-140);
    for (const event of this.effects) {
      const age = now - event.birth;
      const hasTarget = Number.isFinite(event.targetX) && Number.isFinite(event.targetY);
      if (event.type === 'windup' && hasTarget && event.kind === 'pounce') {
        this.diamond(event.targetX, event.targetY, '#ff9a6a', '#c2423a65');
      }
      if (!Number.isFinite(event.x) || !Number.isFinite(event.y)) continue;
      const [x, y] = this.project(event.x, event.y);
      const zoom = this.camera.zoom;
      if (event.type === 'impact' || event.type === 'launch' || event.type === 'destroy' || event.type === 'defeat' || event.type === 'shield-break' || event.type === 'claw') {
        const index = event.type === 'launch' ? 4 : event.type === 'shield-break' ? 7 : event.type === 'destroy' ? 9 : event.type === 'claw' ? 10 : age < 150 ? 5 : 6;
        if ((event.type === 'launch' && age > 120) || (settings.quality === 'low' && age > 190)) continue;
        ctx.save();
        ctx.globalAlpha = clamp(1 - age / 600, 0, 1);
        sprite(ctx, 'effects', index, x, y - (event.type === 'launch' ? 16 * zoom : 0),
          (event.kind === 'rocket' ? 62 : 35) * zoom * (settings.reduced ? 1 : 1 + age / 900));
        ctx.restore();
      }
      if (hasTarget && (event.type === 'beam' || event.type === 'arc') && age < 240) {
        const [tx, ty] = this.project(event.targetX, event.targetY);
        ctx.strokeStyle = event.type === 'arc' ? '#9ae0ff' : '#fdd687';
        ctx.lineWidth = 2.2 * zoom;
        ctx.beginPath();
        ctx.moveTo(x, y - 23 * zoom);
        if (event.type === 'arc') ctx.lineTo((x + tx) / 2 + 9 * zoom, (y + ty) / 2 - 17 * zoom);
        ctx.lineTo(tx, ty - 14 * zoom);
        ctx.stroke();
      }
    }
  }

  drawMinimap() {
    const ctx = this.mini;
    const size = this.minimap.width;
    const scale = size / this.map.size;
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(this.miniTerrain, 0, 0, size, size);
    const plot = (entries, color, radius) => {
      ctx.fillStyle = color;
      entries.forEach(e => ctx.fillRect(e.x * scale - radius, e.y * scale - radius, radius * 2, radius * 2));
    };
    plot(this.map.spawns, '#dfa473', 2.5);
    plot(this.state.structures, '#dbc19a', 1);
    plot(this.state.objectives.filter(e => e.hp > 0), '#edcd75', 2.5);
    plot(this.state.enemies, '#f98268', 1);
    plot(this.state.players, '#a3e4ab', 2);
    ctx.strokeStyle = '#fff4ca';
    ctx.lineWidth = 1;
    ctx.strokeRect(this.camera.x * scale - 6, this.camera.y * scale - 6, 12, 12);
  }
}
