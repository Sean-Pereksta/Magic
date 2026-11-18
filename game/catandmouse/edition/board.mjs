import { AttackEffects } from './attack-effects.mjs';
import { ENEMIES, MAPS } from './battlefield.mjs';
import { isWall, wallConnections, wallKey } from './walls.mjs';

const artRoot = new URL('./art/', import.meta.url);
const sources = {
  title: 0, 'mill-preview': 0, ground: 0, walls: 0, terrain: 160, units: 160,
  facing: 160, towers: 160, effects: 112, pivot: 160, 'late-rats': 160, 'cat-moves': 160,
};
const art = {};
const groundCache = new Map();
let barricadeArt;
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

function paintBarricades() {
  const size = art.walls.height;
  const armor = document.createElement('canvas');
  armor.width = size * 2;
  armor.height = size;
  const ctx = armor.getContext('2d');
  ctx.filter = 'grayscale(.88) brightness(.88) contrast(1.12)';
  ctx.drawImage(art.walls, size * 2, 0, size, size, 0, 0, size, size);
  ctx.drawImage(art.walls, size, 0, size, size, size, 0, size, size);
  ctx.filter = 'none';
  ctx.globalCompositeOperation = 'source-atop';
  ctx.fillStyle = '#45678266';
  ctx.fillRect(0, 0, armor.width, armor.height);
  ctx.globalCompositeOperation = 'source-over';
  ctx.scale(size / 128, size / 128);

  // Cast iron surrounds the atlas' painted timbers. Brass reveals its seams at play scale.
  const straps = new Path2D('M 6 28 H 122 M 6 101 H 122 M 26 39 L 103 92 M 102 39 L 25 92');
  ctx.lineJoin = 'round';
  ctx.strokeStyle = '#161e25';
  ctx.lineWidth = 10;
  ctx.stroke(straps);
  ctx.strokeStyle = '#9f7440';
  ctx.lineWidth = 5;
  ctx.stroke(straps);
  ctx.strokeStyle = '#f5ce8a';
  ctx.lineWidth = 1.75;
  ctx.stroke(straps);
  ctx.fillStyle = '#ffe0a3';
  for (const x of [12, 37, 64, 91, 116]) {
    ctx.fillRect(x - 2, 25, 4, 5);
    ctx.fillRect(x - 2, 99, 4, 5);
  }

  ctx.save();
  ctx.translate(128, 0);
  const crown = new Path2D('M 14 16 L 76 0 L 117 10 L 119 26 L 57 44 L 13 33 Z');
  const metal = ctx.createLinearGradient(35, 3, 65, 44);
  metal.addColorStop(0, '#9bb8c5');
  metal.addColorStop(.48, '#526f82');
  metal.addColorStop(.52, '#243a4d');
  metal.addColorStop(1, '#111f2c');
  ctx.fillStyle = metal;
  ctx.fill(crown);
  ctx.strokeStyle = '#bc8b4b';
  ctx.lineWidth = 2.5;
  ctx.stroke(crown);
  const crest = new Path2D('M 78 48 L 96 55 L 94 72 L 74 86 L 61 77 L 62 60 Z');
  ctx.fillStyle = '#152733';
  ctx.fill(crest);
  ctx.strokeStyle = '#e3b872';
  ctx.stroke(crest);
  ctx.fillStyle = '#f5cf88';
  ctx.fill(new Path2D('M 79 56 L 87 62 L 76 77 L 74 68 L 68 66 Z'));
  ctx.restore();

  const worn = document.createElement('canvas');
  worn.width = armor.width;
  worn.height = size;
  const damaged = worn.getContext('2d');
  damaged.filter = 'saturate(.66) brightness(.72)';
  damaged.drawImage(armor, 0, 0);
  damaged.filter = 'none';
  damaged.scale(size / 128, size / 128);
  damaged.globalCompositeOperation = 'source-atop';
  const cracks = new Path2D('M 70 19 L 57 45 L 71 64 L 49 91 L 53 119 M 70 64 L 94 73 L 116 62 M 218 31 L 205 51 L 222 68 L 205 87 L 208 105 M 222 68 L 242 59');
  damaged.strokeStyle = '#ba8050';
  damaged.lineWidth = 6;
  damaged.stroke(cracks);
  damaged.strokeStyle = '#121b22';
  damaged.lineWidth = 3;
  damaged.stroke(cracks);
  return { armor, worn };
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
const unitScale = { mouse: 49, rat: 43, runner: 38, stink: 51, ox: 61, vulture: 44, king: 62, sapper: 61, mender: 53, cat: 105, rabbit: 43, flea: 28 };
const pivotHeads = {
  turret: { cell: 1, width: 54, pin: .31 },
  marksman: { cell: 2, width: 65, pin: .27 },
  burst: { cell: 3, width: 53, pin: .33 },
  rocket: { cell: 5, width: 60, pin: .31 },
  payload: { cell: 6, width: 65, pin: .34 },
  cluster: { cell: 7, width: 61, pin: .33 },
};
const powerLights = { generator: '#efb850', farm: '#edbc64', repair: '#8cd9d4', shield: '#73c9e4', drum: '#fac98b' };
const catMoves = {
  claw: { prepare: 0, strike: 1, recovery: 250 },
  pounce: { prepare: 0, strike: 2, landing: 3, recovery: 440 },
  'tail-sweep': { prepare: 4, strike: 5, recovery: 390 },
  'cat-charge': { prepare: 6, strike: 7, recovery: 460 },
};
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
    this.effects = new AttackEffects();
    this.facing = new Map();
    this.towerAngles = new Map();
    this.muzzles = new Map();
  }

  reset(mapId) {
    this.map = MAPS[mapId];
    this.previous = this.state = null;
    this.localId = null;
    this.camera.x = this.map.playerSpawns[0].x;
    this.camera.y = this.map.playerSpawns[0].y;
    this.camera.follow = true;
    this.effects.clear();
    this.facing.clear();
    this.towerAngles.clear();
    this.muzzles.clear();
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
      this.effects.add(event, state.time);
      if (event.type === 'destroy') this.towerAngles.delete(event.entityId);
      if (event.type === 'defeat') this.facing.delete(event.entityId);
      if (event.time < state.time - 400 || (event.type === 'impact' && event.reason === 'expired')) continue;
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
    this.muzzles.clear();
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
    this.effects.drawGround(this, now, settings);
    const walls = new Map(this.state.structures.filter(entry => isWall(entry.type)).map(entry => [wallKey(entry.x, entry.y), entry]));
    let plannedWalls;
    if (target) {
      ctx.save();
      ctx.lineWidth = 2;
      if (!target.available) ctx.setLineDash([4, 3]);
      this.diamond(target.x, target.y, target.available ? '#b7efad' : '#ff9a81', target.available ? '#b7efad55' : '#cc595255');
      ctx.restore();
      if (isWall(target.type) && target.available) {
        plannedWalls = new Map(walls);
        plannedWalls.set(wallKey(target.x, target.y), target);
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
    enqueue(this.state.pickups, (e, p) => {
      const [x, y] = this.project(p.x, p.y);
      sprite(ctx, 'effects', 8, x, y - (settings.reduced ? 0 : Math.sin(now / 310 + p.x) * 2), 24 * this.camera.zoom);
    });
    enqueue(this.state.structures, (e, p) => {
      if (isWall(e.type)) this.wall(e, p, walls, false, plannedWalls);
      else this.structure(e, p, settings, elapsed);
    });
    if (plannedWalls) enqueue([target], (e, p) => this.wall(e, p, plannedWalls, true));
    else if (target?.available) enqueue([target], (entity, point) => {
      const [x, y] = this.project(point.x, point.y);
      const zoom = this.camera.zoom;
      ctx.save();
      ctx.globalAlpha = .58;
      sprite(ctx, 'towers', towerCell(entity.type), x, y + 6 * zoom, 57 * zoom);
      ctx.restore();
    });
    enqueue(this.state.players, (e, p) => this.unit(e, p, 'mouse', now, settings));
    enqueue(this.state.enemies, (e, p) => this.unit(e, p, e.type, now, settings));
    enqueue(this.state.allies, (e, p) => this.unit(e, p, e.type, now, settings));
    enqueue(this.scenery, (e, p) => this.prop(e.index, p, 1.08));
    depth.sort((a, b) => a.depth - b.depth).forEach(item => item.draw());
    if (target?.fixed) this.placementMarker(target);
    this.state.projectiles.forEach(shot => {
      const point = this.position(shot, fraction);
      this.effects.drawProjectileTrail(this, shot, point, settings);
      this.projectile(shot, point);
    });
    this.effects.drawForeground(this, now, settings, (index, x, y, width, height, anchor) =>
      sprite(ctx, 'effects', index, x, y, width, height, anchor),
    );
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

  placementMarker(target) {
    const [x, tileY] = this.project(target.x, target.y);
    const y = tileY - 46 * this.camera.zoom;
    const ctx = this.ctx;
    ctx.save();
    ctx.fillStyle = '#142722';
    ctx.strokeStyle = target.available ? '#b7efad' : '#ff9a81';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(x, y, 10, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(x - 4, y);
    if (target.available) {
      ctx.lineTo(x - 1, y + 3);
      ctx.lineTo(x + 5, y - 4);
    } else {
      ctx.moveTo(x - 4, y - 4);
      ctx.lineTo(x + 4, y + 4);
      ctx.moveTo(x - 4, y + 4);
      ctx.lineTo(x + 4, y - 4);
    }
    ctx.stroke();
    ctx.restore();
  }

  health(x, y, hp, maxHp, color, always = false) {
    if ((!always && hp >= maxHp) || maxHp <= 0) return;
    const ctx = this.ctx;
    ctx.fillStyle = '#12201c';
    ctx.fillRect(x - 16, y, 32, 5);
    ctx.fillStyle = color;
    ctx.fillRect(x - 15, y + 1, 30 * clamp(hp / maxHp, 0, 1), 3);
  }

  wall(entity, point, walls, ghost = false, plannedWalls) {
    const ctx = this.ctx;
    const [x, y] = this.project(point.x, point.y);
    const reinforced = entity.type === 'barricade';
    const damaged = entity.hp < entity.maxHp * .38;
    if (reinforced) barricadeArt ??= paintBarricades();
    const texture = reinforced ? (damaged ? barricadeArt.worn : barricadeArt.armor) : art.walls;
    const height = (reinforced ? 45 : 29) * this.camera.zoom;
    const size = texture.height;
    const arms = wallConnections(entity, walls);
    const previewArms = plannedWalls ? wallConnections(entity, plannedWalls).filter(direction => !arms.includes(direction)) : [];
    ctx.save();
    if (ghost || entity.readyAt > this.state.time) ctx.globalAlpha = .57;
    if (!reinforced && damaged) ctx.filter = 'saturate(.68) brightness(.75)';
    const arm = direction => {
      const [toX, toY] = this.project(point.x + direction.x * .51, point.y + direction.y * .51);
      const left = toX < x;
      const startX = left ? toX : x;
      const startY = left ? toY : y;
      const endY = left ? y : toY;
      ctx.save();
      ctx.transform(Math.abs(toX - x) + .5, endY - startY, 0, 1, startX, startY);
      ctx.drawImage(texture, 0, 0, size, size, 0, -height, 1, height);
      ctx.restore();
    };
    if (previewArms.length) {
      ctx.save();
      ctx.globalAlpha *= .55;
      previewArms.forEach(arm);
      ctx.restore();
    }
    arms.forEach(arm);
    const postWidth = height * (arms.length ? .66 : .84);
    ctx.drawImage(texture, size, 0, size, size, x - postWidth / 2, y - height * 1.12, postWidth, height * 1.19);
    ctx.restore();
    if (!ghost) this.health(x, y - height * 1.18, entity.hp, entity.maxHp, reinforced && damaged ? '#e08354' : '#e5b161');
  }

  structure(entity, point, settings, elapsed) {
    const [x, y] = this.project(point.x, point.y);
    const ctx = this.ctx;
    const time = this.state.time;
    const zoom = this.camera.zoom;
    if (entity.readyAt > time) this.diamond(point.x, point.y, '#e7ba69', '#e7ba692f');
    ctx.save();
    ctx.translate(x, y);
    if (entity.disabledUntil > time) ctx.filter = 'saturate(.4) brightness(.75)';
    else if (entity.hp < entity.maxHp * .38) ctx.filter = 'saturate(.7) brightness(.72)';
    if (entity.readyAt > time) ctx.globalAlpha = .5;
    if (entity.type === 'turret' || entity.type === 'rocket') {
      this.pivotTower(entity, elapsed, settings);
    } else {
      const facing = (entity.aimX - entity.x) - (entity.aimY - entity.y);
      if (Number.isFinite(entity.aimX) && facing < -.12) ctx.scale(-1, 1);
      const recoil = !settings.reduced && time - entity.lastAttackAt < 140 ? 3 * zoom : 0;
      sprite(ctx, 'towers', towerCell(entity.type), -recoil, 6 * zoom, 57 * zoom);
      if (entity.windupUntil > time) {
        ctx.fillStyle = '#f8c472';
        ctx.beginPath();
        ctx.arc(11 * zoom, -24 * zoom, 2.7 * zoom, 0, Math.PI * 2);
        ctx.fill();
      }
      const light = powerLights[entity.type];
      if (light && entity.readyAt <= time && !(entity.disabledUntil > time)) {
        ctx.globalAlpha *= settings.reduced ? .68 : .58 + Math.sin(time / 460 + entity.x * .3) * .16;
        ctx.fillStyle = light;
        ctx.beginPath();
        ctx.arc(3 * zoom, -20 * zoom, 2.3 * zoom, 0, Math.PI * 2);
        ctx.fill();
      }
    }
    ctx.restore();
    this.health(x, y - 43 * zoom, entity.hp, entity.maxHp, '#e5b161');
  }

  pivotTower(entity, elapsed, settings) {
    const ctx = this.ctx;
    const zoom = this.camera.zoom;
    const rocket = entity.type === 'rocket';
    const head = pivotHeads[entity.branch] || pivotHeads[entity.type];
    const pivotY = -(rocket ? 26 : 24) * zoom;
    const oldAngle = this.towerAngles.get(entity.id) ?? .12;
    const hasAim = Number.isFinite(entity.aimX) && Number.isFinite(entity.aimY) &&
      Math.hypot(entity.aimX - entity.x, entity.aimY - entity.y) > .01;
    const aim = hasAim ? Math.atan2((entity.aimX - entity.x + entity.aimY - entity.y) * this.tileY,
      (entity.aimX - entity.x - entity.aimY + entity.y) * this.tileX) : oldAngle;
    const turn = Math.atan2(Math.sin(aim - oldAngle), Math.cos(aim - oldAngle));
    const age = this.state.time - entity.lastAttackAt;
    const angle = oldAngle + turn * (settings.reduced || (age >= 0 && age < 55) ? 1 : 1 - Math.exp(-elapsed / 42));
    this.towerAngles.set(entity.id, Math.atan2(Math.sin(angle), Math.cos(angle)));

    sprite(ctx, 'pivot', rocket ? 4 : 0, 0, 4 * zoom, 58 * zoom, 52 * zoom, .94);
    const width = head.width * zoom;
    const recoil = !settings.reduced && age >= 0 ? (rocket ? 7 : 4.5) * zoom * clamp(1 - age / 180, 0, 1) ** 2 : 0;
    const muzzle = width * (.96 - head.pin) - recoil;
    ctx.save();
    ctx.translate(0, pivotY);
    ctx.rotate(angle);
    if (Math.cos(angle) < 0) ctx.scale(1, -1);
    const cell = sources.pivot;
    ctx.drawImage(art.pivot, head.cell % 4 * cell, Math.floor(head.cell / 4) * cell, cell, cell,
      -width * head.pin - recoil, -width / 2, width, width);
    if (entity.windupUntil > this.state.time && entity.readyAt <= this.state.time) {
      const lit = ctx.globalAlpha;
      ctx.globalAlpha *= .74;
      ctx.fillStyle = entity.branch === 'cluster' ? '#9de7ee' : '#ffd694';
      ctx.beginPath();
      ctx.arc(muzzle, 0, (rocket ? 3.3 : 2.1) * zoom, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = lit;
    }
    ctx.restore();
    const [x, y] = this.project(entity.x, entity.y);
    this.muzzles.set(entity.id, [x + Math.cos(angle) * muzzle, y + pivotY + Math.sin(angle) * muzzle]);
  }

  muzzlePoint(shooterId) {
    return this.muzzles.get(shooterId);
  }

  unit(entity, point, type, now, settings) {
    const ctx = this.ctx;
    const [x, y] = this.project(point.x, point.y);
    const time = this.state.time;
    const zoom = this.camera.zoom;
    const previous = this.previousEntities.get(entity.id);
    let { horizontal, vertical, moving } = screenDirection(entity, previous);
    const preparing = entity.windupUntil > time;
    const kind = preparing ? entity.attackKind : entity.lastAttackKind;
    if (preparing && Number.isFinite(entity.attackX) && Number.isFinite(entity.attackY)) {
      const dx = entity.attackX - entity.x;
      const dy = entity.attackY - entity.y;
      if (Math.hypot(dx, dy) > .06) {
        horizontal = dx - dy;
        vertical = (dx + dy) / 2;
      }
    }
    if (Math.abs(horizontal) > .045) this.facing.set(entity.id, Math.sign(horizontal));
    const useVertical = type === 'mouse' && Math.abs(vertical) > .045 && Math.abs(horizontal) < Math.abs(vertical) * .65;
    const move = type === 'cat' ? (catMoves[kind] || catMoves.claw) : null;
    const age = time - entity.lastAttackAt;
    const strikeTime = move?.recovery || 225;
    const striking = age >= 0 && age < strikeTime;
    const anticipated = preparing ? clamp(1 - (entity.windupUntil - time) / (ENEMIES[type]?.windup || 420), 0, 1) : 0;
    const lunge = striking ? Math.sin(Math.PI * age / strikeTime) : 0;
    const hit = time - entity.lastHitAt < 140;
    const phase = striking || (preparing && (anticipated > .5 || settings.reduced))
      ? 3 : moving && !settings.reduced && !preparing ? 1 + Math.floor(now / 102) % 2 : 0;
    const row = type === 'mouse' ? 0 : type === 'cat' ? 2 : type === 'rabbit' || type === 'flea' ? 3 : 1;
    let sheet = useVertical ? 'facing' : 'units';
    let cell = useVertical ? (vertical < 0 ? 4 : 0) + phase : row * 4 + phase;
    if (type === 'sapper' || type === 'mender') {
      sheet = 'late-rats';
      cell = (type === 'mender' ? 4 : 0) + phase;
    }
    if (move && (preparing || striking)) {
      sheet = 'cat-moves';
      cell = preparing ? move.prepare : move.landing && age > strikeTime * .5 ? move.landing : move.strike;
    }
    if (entity.id === this.localId) this.diamond(point.x, point.y, '#fff1ac', '#251f1744');
    ctx.save();
    ctx.translate(x, y + (type === 'vulture' ? -12 * zoom : 2 * zoom));
    ctx.scale(useVertical ? 1 : this.facing.get(entity.id) || 1, 1);
    if (!settings.reduced) {
      const isGroundedStrike = kind !== 'cat-charge' && kind !== 'pounce';
      const airborne = kind === 'pounce' && striking && age < strikeTime / 2;
      const lift = airborne ? Math.sin(Math.PI * age / (strikeTime / 2)) * 10 * zoom : 0;
      ctx.translate(isGroundedStrike ? (lunge * (move ? 9 : 4) - anticipated * 2) * zoom : 0, -lift);
      ctx.scale(1 + anticipated * .045, 1 - anticipated * .08 + lunge * .035);
    }
    if (hit) ctx.filter = 'brightness(1.55) saturate(.65)';
    else if (type === 'stink') ctx.filter = 'sepia(.3) hue-rotate(36deg)';
    else if (type === 'vulture') ctx.filter = 'grayscale(.62)';
    else if (type === 'king') ctx.filter = 'sepia(.55) saturate(1.5)';
    if (entity.hp <= 0) ctx.globalAlpha = .35;
    const width = (unitScale[type] || 44) * zoom;
    sprite(ctx, sheet, cell, 0, 0,
      width * (1 + (phase === 0 && !settings.reduced ? Math.sin(now / 310) * .012 : 0)), width);
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
    plot(this.state.enemies.filter(e => e.type !== 'cat'), '#f98268', 1);
    plot(this.state.enemies.filter(e => e.type === 'cat'), '#ff654a', 3);
    plot(this.state.players.filter(e => e.hp > 0), '#a3e4ab', 2);
    ctx.strokeStyle = '#fff4ca';
    ctx.lineWidth = 1;
    ctx.strokeRect(this.camera.x * scale - 6, this.camera.y * scale - 6, 12, 12);
  }
}
