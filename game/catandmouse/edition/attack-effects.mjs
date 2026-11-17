const TAU = Math.PI * 2;
const ROOT_TWO = Math.SQRT2;
const MAX_EFFECTS = 200;

const danger = {
  pounce: { color: '#ffae77', fill: '#ba3f4270', radius: 1.5 },
  'tail-sweep': { color: '#ffd895', fill: '#d57a485c', radius: 2.1 },
  'cat-charge': { color: '#ffad7b', fill: '#c744435e', radius: 1 },
  'powder-blast': { color: '#ffc66a', fill: '#d7884264', radius: 1.65 },
};
const weapons = {
  bullet: { color: '#ffce7c', width: 1.2, reach: .33, launch: 25, impact: 27, duration: 290, sprite: 4 },
  rabbit: { color: '#d7f2a1', width: 1.6, reach: .32, launch: 18, impact: 30, duration: 310, sprite: 4 },
  rocket: { color: '#fda55d', width: 4, reach: .64, launch: 33, duration: 760 },
  acorn: { color: '#e6ba79', width: 2.1, reach: .33, launch: 23, impact: 44, duration: 440, sprite: 6 },
  web: { color: '#d7f1f5', width: 2, reach: .45, launch: 26, impact: 45, duration: 690, sprite: 1 },
};
const blasts = {
  rocket: { color: '#ffcc87', ember: '#fff2b9', debris: '#b77a57', width: 76, smokeRise: 39, spread: 1, fragments: 6 },
  'powder-blast': { color: '#ffb665', ember: '#ffce81', debris: '#86573e', width: 98, smokeRise: 24, spread: 1.22, fragments: 8 },
};
const strikes = {
  claw: { color: '#ff886d', radius: 1, size: 50, count: 3, duration: 370 },
  'rat-bite': { color: '#f4a68b', radius: .62, size: 32, count: 2, duration: 290 },
  'flea-bite': { color: '#ddd1ff', radius: .63, size: 28, count: 2, duration: 260 },
  ram: { color: '#ffd48b', radius: 1.2, size: 50, count: 2, duration: 390 },
  talon: { color: '#d9c6ff', radius: .8, size: 43, count: 3, duration: 350 },
  'king-bash': { color: '#ffe694', radius: 1.25, size: 52, count: 3, duration: 460 },
};
const utility = {
  repair: { color: '#a8dfc7', radius: 4, glyph: 'cross' },
  shield: { color: '#9ad7ff', radius: 3.5, glyph: 'shield' },
  slime: { color: '#a7dc92', radius: 3.6, glyph: 'drop' },
  drum: { color: '#ffe8a2', radius: 3.5, glyph: 'spark' },
  generator: { color: '#ffdd92', radius: .8, glyph: 'spark' },
  farm: { color: '#f6d28d', radius: .9, glyph: 'spark' },
  rabbit: { color: '#ecdaab', radius: .9, glyph: 'spark' },
  flea: { color: '#ddd0ff', radius: .75, glyph: 'spark' },
  mend: { color: '#93e8c3', radius: 3, glyph: 'cross' },
  'stink-cloud': { color: '#cee190', radius: 2.05, glyph: 'drop' },
};

const clamp = (number, low, high) => Math.max(low, Math.min(high, number));
const coordinates = (x, y) => Number.isFinite(x) && Number.isFinite(y);
const centered = (event, atTarget) =>
  atTarget && coordinates(event.targetX, event.targetY) ? [event.targetX, event.targetY] : [event.x, event.y];

function duration(event, start) {
  if (event.type === 'windup') return Math.max(60, (event.until ?? start + 250) - start);
  if (event.type === 'shot') return 145;
  if (event.type === 'impact') return event.reason === 'expired' ? 0 : (weapons[event.kind]?.duration ?? 350);
  if (event.type === 'attack') return event.kind === 'mend' ? 620
    : event.kind === 'cat-charge' ? (event.duration ?? 450) + 120
      : event.kind === 'powder-blast' ? 820 : danger[event.kind] ? 720 : (strikes[event.kind]?.duration ?? 380);
  if (event.type === 'activation') return 540;
  if (event.type === 'interrupt') return 300;
  if (event.type === 'heal' || event.type === 'charge-end') return 510;
  if (event.type === 'disrupt' || event.type === 'shield-break') return 480;
  if (event.type === 'destroy' || event.type === 'defeat') return 450;
  return 0;
}

function paintTime(board, now) {
  const simulationTime = board.state?.time ?? 0;
  const elapsed = Number.isFinite(board.received) ? clamp(now - board.received, 0, 50) : 0;
  return simulationTime + elapsed;
}

function outline(board, x, y, radiusX, radiusY = radiusX) {
  const [xScreen, yScreen] = board.project(x, y);
  const ctx = board.ctx;
  ctx.beginPath();
  ctx.ellipse(xScreen, yScreen, radiusX * board.tileX * ROOT_TWO, radiusY * board.tileY * ROOT_TWO, 0, 0, TAU);
}

function ring(board, x, y, radius, color, width, fill) {
  outline(board, x, y, radius);
  const ctx = board.ctx;
  if (fill) {
    ctx.fillStyle = fill;
    ctx.fill();
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = width * board.camera.zoom;
  ctx.stroke();
}

function line(ctx, points, color, width) {
  ctx.beginPath();
  ctx.moveTo(...points[0]);
  for (const point of points.slice(1)) ctx.lineTo(...point);
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.stroke();
}

function outsideView(board, x, y, targetX, targetY, margin) {
  if (!Number.isFinite(board.width)) return false;
  const [a, b] = board.project(x, y);
  const [c, d] = coordinates(targetX, targetY) ? board.project(targetX, targetY) : [a, b];
  return Math.max(a, c) < -margin || Math.min(a, c) > board.width + margin
    || Math.max(b, d) < -margin || Math.min(b, d) > board.height + margin;
}

function lane(board, event, radius, style, progress, reduced) {
  const ctx = board.ctx;
  const [endX, endY] = centered(event, true);
  const dx = endX - event.x;
  const dy = endY - event.y;
  const length = Math.hypot(dx, dy);
  if (length < .01) {
    ring(board, event.x, event.y, radius, style.color, 2.5, style.fill);
    return;
  }
  const px = -dy / length * radius;
  const py = dx / length * radius;
  const corners = [
    board.project(event.x + px, event.y + py), board.project(endX + px, endY + py),
    board.project(endX - px, endY - py), board.project(event.x - px, event.y - py),
  ];
  ctx.beginPath();
  ctx.moveTo(...corners[0]);
  for (const corner of corners.slice(1)) ctx.lineTo(...corner);
  ctx.closePath();
  ctx.fillStyle = style.fill;
  ctx.fill();
  ctx.strokeStyle = style.color;
  ctx.lineWidth = 2 * board.camera.zoom;
  ctx.stroke();
  const spacing = Math.min(.29, 1.3 / length);
  const lead = reduced ? 0 : progress * spacing;
  for (let t = spacing + lead; t < .97; t += spacing) {
    const midpoint = board.project(event.x + dx * t, event.y + dy * t);
    const baseX = event.x + dx * (t - spacing * .42);
    const baseY = event.y + dy * (t - spacing * .42);
    line(ctx, [board.project(baseX + px * .55, baseY + py * .55), midpoint,
      board.project(baseX - px * .55, baseY - py * .55)], style.color, 2 * board.camera.zoom);
  }
}

function mark(board, event, progress, settings) {
  const ctx = board.ctx;
  const style = danger[event.kind];
  if (style) {
    const radius = event.radius ?? style.radius;
    if (event.kind === 'cat-charge') {
      lane(board, event, radius, style, progress, settings.reduced);
    } else {
      const [x, y] = centered(event, event.kind !== 'tail-sweep');
      ring(board, x, y, radius, style.color, 2.3, style.fill);
      if (!settings.reduced) ring(board, x, y, radius * (1 - .55 * progress), style.color, 1.25);
      if (event.kind === 'pounce') {
        ctx.setLineDash([5 * board.camera.zoom, 5 * board.camera.zoom]);
        line(ctx, [board.project(event.x, event.y), board.project(x, y)], style.color, 1.6 * board.camera.zoom);
        ctx.setLineDash([]);
      }
    }
    return;
  }
  if (event.kind === 'mend') {
    const [x, y] = centered(event, true);
    ring(board, x, y, event.radius ?? utility.mend.radius, utility.mend.color, 1.6);
  } else if (strikes[event.kind] && coordinates(event.targetX, event.targetY)) {
    ctx.globalAlpha = .62;
    ring(board, event.targetX, event.targetY, .25, strikes[event.kind].color, 1.5);
  }
}

function glyph(ctx, x, y, size, color, type) {
  ctx.strokeStyle = color;
  ctx.lineWidth = Math.max(1, size / 3);
  ctx.beginPath();
  if (type === 'cross') {
    ctx.moveTo(x - size, y);
    ctx.lineTo(x + size, y);
    ctx.moveTo(x, y - size);
    ctx.lineTo(x, y + size);
  } else if (type === 'shield') {
    ctx.moveTo(x - size, y - size);
    ctx.lineTo(x + size, y - size);
    ctx.quadraticCurveTo(x + size, y, x, y + size);
    ctx.quadraticCurveTo(x - size, y, x - size, y - size);
  } else if (type === 'drop') {
    ctx.moveTo(x, y - size);
    ctx.quadraticCurveTo(x + 2 * size, y + size * 1.3, x, y + size);
    ctx.quadraticCurveTo(x - 2 * size, y + size * 1.3, x, y - size);
  } else {
    ctx.moveTo(x - size, y);
    ctx.lineTo(x, y + size);
    ctx.lineTo(x + size, y);
    ctx.lineTo(x, y - size);
    ctx.closePath();
  }
  ctx.stroke();
}

function sparks(ctx, x, y, progress, scale, color, count, salt) {
  ctx.fillStyle = color;
  for (let i = 0; i < count; i++) {
    const angle = TAU * (i / count + salt * .17);
    const reach = scale * (.28 + (i % 3) * .16) * progress;
    ctx.beginPath();
    ctx.arc(x + Math.cos(angle) * reach, y + Math.sin(angle) * reach * .64 - 13 * progress,
      Math.max(.6, scale * .038 * (1 - .7 * progress)), 0, TAU);
    ctx.fill();
  }
}

function blastFragments(ctx, x, y, size, progress, style, salt) {
  if (progress >= .72) return;
  const flight = progress / .72;
  const opacity = (1 - flight) ** 1.35;
  ctx.globalAlpha = opacity;
  ctx.lineWidth = Math.max(1, size * .023);
  ctx.strokeStyle = style.ember;
  ctx.beginPath();
  for (let i = 0; i < style.fragments; i++) {
    const angle = TAU * (i / style.fragments + salt * .17);
    const distance = size * (.48 + (i % 3) * .16);
    const reach = distance * (.14 + .86 * flight);
    const dx = Math.cos(angle);
    const dy = Math.sin(angle) * .56;
    const height = size * .52 * flight * (1 - flight);
    const tail = reach - size * (.14 - flight * .09);
    ctx.moveTo(x + dx * tail, y + dy * tail - height * .8);
    ctx.lineTo(x + dx * reach, y + dy * reach - height);
  }
  ctx.stroke();

  const chipSize = Math.max(.8, size * .036 * (1 - flight * .48));
  ctx.globalAlpha = opacity * .94;
  ctx.fillStyle = style.debris;
  ctx.beginPath();
  for (let i = 0; i < 4; i++) {
    const angle = TAU * (i / 4 + .11 + salt * .17);
    const reach = size * (.13 + flight * (.41 + (i % 2) * .24));
    const left = x + Math.cos(angle) * reach;
    const top = y + Math.sin(angle) * reach * .4 - size * 1.13 * flight * (1 - flight);
    ctx.moveTo(left - chipSize, top);
    ctx.lineTo(left + chipSize * .26, top - chipSize);
    ctx.lineTo(left + chipSize * .85, top + chipSize * .56);
    ctx.closePath();
  }
  ctx.fill();
}

function explosion(board, event, progress, settings, drawSprite) {
  const powder = event.kind === 'powder-blast';
  const style = blasts[event.kind];
  const ctx = board.ctx;
  const zoom = board.camera.zoom;
  const [x, y] = centered(event, powder);
  const [sx, sy] = board.project(x, y);
  const radius = event.radius ?? (powder ? danger['powder-blast'].radius : 0);
  const size = style.width * zoom * clamp(.83 + radius * .1, .83, 1.1);

  if (radius > 0 && progress < .62) {
    const shock = progress / .62;
    const reach = settings.reduced ? radius : radius * (1 - .61 * (1 - shock) ** 3);
    ctx.globalAlpha = .85 * (1 - shock);
    ring(board, x, y, reach, style.color, 2.15);
  }

  if (settings.reduced) {
    ctx.globalAlpha = .73 * (1 - progress);
    drawSprite(5, sx, sy - 8 * zoom, size, size, .54);
    return;
  }

  const detailed = settings.quality !== 'low';
  const direction = (event.id ?? 0) % 2 ? -1 : 1;
  if (progress > .14) {
    const smoke = (progress - .14) / .86;
    const opacity = Math.min(smoke * 4, 1) * (1 - smoke) ** 1.45;
    const breadth = size * (.62 + smoke * .5);
    ctx.globalAlpha = .65 * opacity;
    drawSprite(6, sx + direction * smoke * 13 * zoom, sy - (7 + style.smokeRise * smoke) * zoom,
      breadth * style.spread, breadth, .48);
    if (detailed && powder) {
      ctx.globalAlpha = .38 * opacity;
      drawSprite(6, sx - direction * smoke * 20 * zoom, sy - (3 + 13 * smoke) * zoom,
        breadth * 1.1, breadth * .7, .55);
    }
  }

  if (progress < .52) {
    const fire = progress / .52;
    const breadth = size * (.67 + .39 * (1 - (1 - fire) ** 3));
    ctx.globalAlpha = (1 - fire) ** 1.3;
    drawSprite(5, sx, sy - (8 + 9 * fire) * zoom, breadth * style.spread, breadth, .53);
  }
  if (progress < .17) {
    const flash = progress / .17;
    const spark = size * (.48 + .15 * flash);
    ctx.globalAlpha = .91 * (1 - flash) ** 2;
    drawSprite(4, sx, sy - 16 * zoom, spark, spark, .5);
  }
  if (detailed) blastFragments(ctx, sx, sy - 11 * zoom, size, progress, style, event.id ?? 0);
}

function support(board, event, progress, settings) {
  const { kind } = event;
  const style = utility[kind];
  if (!style) return;
  const ctx = board.ctx;
  const zoom = board.camera.zoom;
  const [x, y] = centered(event, kind === 'mend');
  const [sx, sy] = board.project(x, y);
  const radius = event.radius ?? style.radius;
  ctx.globalAlpha = .6 * (1 - progress) + .13;
  const cloud = kind === 'stink-cloud' ? '#98bd5559' : undefined;
  ring(board, x, y, settings.reduced ? radius : radius * (.45 + .55 * progress), style.color, 1.7, cloud);
  ctx.globalAlpha = .69 + .31 * (1 - progress);
  const count = settings.quality === 'low' || settings.reduced ? 1 : 3;
  for (let i = 0; i < count; i++) {
    const xOffset = (i - (count - 1) / 2) * zoom * 13;
    glyph(ctx, sx + xOffset, sy - zoom * (17 + (settings.reduced ? 0 : 17 * progress) + i % 2 * 8),
      4.1 * zoom, style.color, style.glyph);
  }
}

function beam(board, event, progress, settings) {
  if (!coordinates(event.targetX, event.targetY)) return;
  const ctx = board.ctx;
  const zoom = board.camera.zoom;
  const [x, y] = board.project(event.x, event.y);
  const [tx, ty] = board.project(event.targetX, event.targetY);
  const isTower = board.state.structures?.some(entry => entry.id === event.entityId && entry.x === event.x && entry.y === event.y);
  const start = (isTower ? board.muzzlePoint?.(event.entityId) : null) ?? [x, y - (isTower ? 23 : 12) * zoom];
  const end = [tx, ty - 10 * zoom];
  const thunder = event.kind === 'tesla';
  const color = thunder ? '#94d9fe' : '#ffbd6d';
  const points = [start];
  if (thunder && !settings.reduced) {
    for (let i = 1; i < 6; i++) {
      const ratio = i / 6;
      points.push([x + (tx - x) * ratio,
        start[1] + (end[1] - start[1]) * ratio + Math.sin(i * 4.9 + Math.floor(progress * 6)) * 9 * zoom]);
    }
  }
  points.push(end);
  ctx.globalAlpha = .38 * (1 - progress);
  line(ctx, points, color, (thunder ? 9 : 13) * zoom);
  ctx.globalAlpha = .98 * (1 - progress * .82);
  line(ctx, points, thunder ? '#d8f4ff' : '#fff0be', (thunder ? 2.55 : 3.4) * zoom);
}

function melee(board, event, progress, settings, drawSprite) {
  const style = strikes[event.kind] ?? strikes.claw;
  const ctx = board.ctx;
  const zoom = board.camera.zoom;
  const [tx, ty] = centered(event, true);
  const [sx, sy] = board.project(event.x, event.y);
  const [txScreen, tyScreen] = board.project(tx, ty);
  const reach = event.hit === false ? .6 : 1;
  const x = sx + (txScreen - sx) * reach;
  const y = sy + (tyScreen - sy) * reach;
  const angle = Math.atan2(y - sy, x - sx) + Math.PI / 2;
  const scale = style.size * zoom;
  ctx.globalAlpha = (event.hit === false ? .65 : 1) * (1 - progress * .85);
  ctx.save();
  ctx.translate(x, y - 13 * zoom);
  ctx.rotate(angle);
  if (event.kind === 'claw' || event.kind === 'talon') drawSprite(10, 0, 0, scale, scale, .5);
  for (let i = 0; i < style.count; i++) {
    const radius = scale * (.19 + i * .055 + (settings.reduced ? 0 : progress * .09));
    ctx.beginPath();
    ctx.ellipse(0, 0, radius * 1.22, radius, 0, .3, 2.75);
    ctx.strokeStyle = style.color;
    ctx.lineWidth = (i === 0 ? 2.7 : 1.3) * zoom;
    ctx.stroke();
  }
  ctx.restore();
  if (!settings.reduced && event.hit !== false) sparks(ctx, x, y - 7 * zoom, progress, scale, style.color, 5, event.id ?? 0);
}

function special(board, event, progress, settings, drawSprite) {
  if (event.kind === 'powder-blast') {
    explosion(board, event, progress, settings, drawSprite);
    return;
  }
  const style = danger[event.kind];
  const ctx = board.ctx;
  const [x, y] = centered(event, event.kind !== 'tail-sweep');
  const [sx, sy] = board.project(x, y);
  const zoom = board.camera.zoom;
  const scale = 78 * zoom;
  ctx.globalAlpha = 1 - progress * .88;
  if (event.kind === 'cat-charge') {
    lane(board, event, event.radius ?? style.radius, { ...style, fill: '#fba56b35' }, progress, settings.reduced);
    const cat = board.state.enemies?.find(enemy => enemy.id === event.entityId);
    if (cat?.chargeUntil > board.state.time && !settings.reduced) {
      const [catX, catY] = board.project(cat.x, cat.y);
      drawSprite(6, catX, catY, scale * .6, scale * .6, .65);
    }
    return;
  } else if (event.kind === 'tail-sweep') {
    const radius = (event.radius ?? style.radius) * (settings.reduced ? 1 : .56 + .44 * progress);
    ring(board, x, y, radius, style.color, 3.4, '#f29e593b');
    drawSprite(10, sx, sy - 23 * zoom, scale * 1.15, scale * .65, .45);
  } else {
    const radius = (event.radius ?? style.radius) * (settings.reduced ? 1 : .56 + .44 * progress);
    ring(board, x, y, radius, style.color, 2.4);
    drawSprite(11, sx, sy - 8 * zoom, scale, scale, .57);
  }
  if (!settings.reduced && settings.quality !== 'low') {
    sparks(ctx, sx, sy - 8 * zoom, progress, scale, style.color, 9, event.id ?? 2);
  }
}

export class AttackEffects {
  constructor() {
    this.events = [];
  }

  clear() {
    this.events.length = 0;
  }

  add(event, stateTime) {
    if (!coordinates(event.x, event.y)) return false;
    const start = event.time ?? stateTime;
    if (event.entityId && ['interrupt', 'disrupt', 'defeat', 'destroy'].includes(event.type)) {
      this.events = this.events.filter(entry =>
        !(entry.type === 'windup' && entry.entityId === event.entityId && entry.start <= start),
      );
    }
    const length = duration(event, start);
    if (!length || start + length <= stateTime) return false;
    if ((event.type === 'activation' || event.type === 'disrupt') && this.events.some(entry =>
      entry.type === event.type && entry.kind === event.kind && entry.entityId === event.entityId && start - entry.start < 270,
    )) return false;
    if (this.events.length >= MAX_EFFECTS) {
      const expired = this.events.findIndex(entry => entry.start + entry.length <= stateTime);
      const transient = this.events.findIndex(entry => entry.type !== 'windup' || !danger[entry.kind]);
      this.events.splice(expired >= 0 ? expired : Math.max(0, transient), 1);
    }
    this.events.push({ ...event, start, length });
    return true;
  }

  drawGround(board, now, settings) {
    const time = paintTime(board, now);
    const liveWarnings = new Set();
    this.events = this.events.filter(event => time < event.start + event.length);
    for (const event of this.events) {
      if (event.type !== 'windup' || time < event.start
        || outsideView(board, event.x, event.y, event.targetX, event.targetY, board.tileX * 7)) continue;
      liveWarnings.add(event.entityId);
      board.ctx.save();
      mark(board, event, (time - event.start) / event.length, settings);
      board.ctx.restore();
    }
    // Keep major warnings visible if a busy match has rolled over the original event.
    for (const enemy of board.state.enemies ?? []) {
      const kind = enemy.attackKind;
      if (!danger[kind] || enemy.windupUntil <= time || liveWarnings.has(enemy.id)
        || outsideView(board, enemy.x, enemy.y, enemy.attackX, enemy.attackY, board.tileX * 7)) continue;
      const progress = clamp(1 - (enemy.windupUntil - time) / 1000, 0, 1);
      board.ctx.save();
      mark(board, { x: enemy.x, y: enemy.y, targetX: enemy.attackX, targetY: enemy.attackY,
        radius: enemy.attackRadius, kind }, progress, settings);
      board.ctx.restore();
    }
  }

  drawForeground(board, now, settings, drawSprite) {
    const time = paintTime(board, now);
    const ctx = board.ctx;
    const zoom = board.camera.zoom;
    for (const event of this.events) {
      if (time < event.start || time >= event.start + event.length
        || outsideView(board, event.x, event.y, event.targetX, event.targetY, board.tileX * 7)) continue;
      const progress = (time - event.start) / event.length;
      const [x, y] = board.project(event.x, event.y);
      const missile = weapons[event.kind] ?? weapons.bullet;
      ctx.save();
      ctx.lineCap = 'round';
      if (event.type === 'shot') {
        const [mx, my] = board.muzzlePoint?.(event.shooterId, event.vx, event.vy) ?? [x, y - 18 * zoom];
        const width = missile.launch * zoom * (settings.reduced ? .75 : 1.03 - .4 * progress);
        ctx.globalAlpha = 1 - progress * .9;
        drawSprite(event.kind === 'web' ? 1 : 4, mx, my, width, width, .5);
      } else if (event.type === 'impact' && event.kind === 'rocket') {
        explosion(board, event, progress, settings, drawSprite);
      } else if (event.type === 'impact') {
        const width = missile.impact * zoom * (settings.reduced ? 1 : .75 + .53 * progress);
        ctx.globalAlpha = 1 - progress * .9;
        if (event.kind === 'web') {
          ring(board, event.x, event.y, (event.radius || .45)
            * (settings.reduced ? 1 : .42 + .58 * progress), missile.color, 1.4);
        }
        const sprite = event.reason === 'terrain' && event.kind !== 'web' ? 6 : missile.sprite;
        drawSprite(sprite, x, y - 11 * zoom, width, width, .52);
        if (!settings.reduced && settings.quality !== 'low') {
          sparks(ctx, x, y - 11 * zoom, progress, width, missile.color, 4, event.id ?? 2);
        }
      } else if (event.type === 'attack') {
        if (event.kind === 'laser' || event.kind === 'tesla') beam(board, event, progress, settings);
        else if (danger[event.kind]) special(board, event, progress, settings, drawSprite);
        else if (event.kind === 'mend') support(board, event, progress, settings);
        else if (event.kind === 'rabbit') {
          ctx.globalAlpha = .65 * (1 - progress);
          drawSprite(4, x, y - 16 * zoom, 18 * zoom, 18 * zoom, .5);
        } else melee(board, event, progress, settings, drawSprite);
      } else if (event.type === 'windup' && (weapons[event.kind] || ['tesla', 'laser'].includes(event.kind))) {
        const color = event.kind === 'tesla' ? '#a4ddff' : missile.color;
        const radius = zoom * (settings.reduced ? 3 : 2 + progress * 3.5);
        const [mx, my] = board.muzzlePoint?.(event.entityId) ?? [x, y - 23 * zoom];
        ctx.globalAlpha = .63;
        ctx.beginPath();
        ctx.arc(mx, my, radius, 0, TAU);
        ctx.fillStyle = color;
        ctx.fill();
      } else if (event.type === 'activation') {
        support(board, event, progress, settings);
      } else if (event.type === 'heal') {
        ctx.globalAlpha = 1 - progress * .85;
        glyph(ctx, x, y - (20 + (settings.reduced ? 0 : 14 * progress)) * zoom, 4.8 * zoom, utility.mend.color, 'cross');
      } else if (event.type === 'charge-end') {
        const width = (settings.reduced ? 52 : 40 + 25 * progress) * zoom;
        ctx.globalAlpha = .9 * (1 - progress);
        drawSprite(6, x, y - 8 * zoom, width, width, .57);
      } else if (event.type === 'interrupt') {
        ctx.globalAlpha = .82 * (1 - progress);
        const point = [x, y - 23 * zoom];
        const wing = 4 * zoom;
        line(ctx, [[point[0] - wing, point[1] - wing], [point[0] + wing, point[1] + wing]], '#cce8f2', 1.7 * zoom);
        line(ctx, [[point[0] - wing, point[1] + wing], [point[0] + wing, point[1] - wing]], '#cce8f2', 1.7 * zoom);
      } else if (event.type === 'disrupt') {
        ctx.globalAlpha = .67 * (1 - progress);
        ring(board, event.x, event.y, .55 + (settings.reduced ? 0 : progress * .25), '#c5d978', 1.85, '#7a8b4552');
        glyph(ctx, x, y - 27 * zoom, 4.2 * zoom, '#d9e7a6', 'drop');
      } else if (event.type === 'shield-break' || event.type === 'destroy' || event.type === 'defeat') {
        const index = event.type === 'shield-break' ? 7 : event.type === 'destroy' ? 9 : 11;
        const size = (event.kind === 'cat' ? 75 : event.type === 'defeat' ? 30 : 47) * zoom;
        ctx.globalAlpha = .88 * (1 - progress);
        drawSprite(index, x, y, size, size, .67);
      }
      ctx.restore();
    }
  }

  drawProjectileTrail(board, shot, point, settings) {
    if (settings.reduced || !coordinates(shot.vx, shot.vy)) return;
    const style = weapons[shot.kind] ?? weapons.bullet;
    const speed = Math.hypot(shot.vx, shot.vy);
    const elapsed = Math.max(0, (board.state.time - (shot.launchedAt ?? board.state.time - 50)) / 1000);
    const reach = Math.min(style.reach, speed * elapsed);
    if (reach < .025 || speed < .001) return;
    const zoom = board.camera.zoom;
    const [headX, headY] = board.project(point.x, point.y);
    const [tailX, tailY] = board.project(point.x - reach * shot.vx / speed, point.y - reach * shot.vy / speed);
    const ctx = board.ctx;
    ctx.save();
    ctx.lineCap = 'round';
    const lift = 18 * zoom;
    ctx.globalAlpha = shot.kind === 'rocket' ? .83 : .48;
    line(ctx, [[tailX, tailY - lift], [headX, headY - lift]], style.color, style.width * zoom);
    if (shot.kind === 'rocket' && settings.quality !== 'low') {
      ctx.fillStyle = '#e8c2a1';
      for (let i = 1; i <= 4; i++) {
        const interval = i / 5;
        ctx.globalAlpha = .33 * interval;
        ctx.beginPath();
        ctx.arc(tailX + (headX - tailX) * interval, tailY + (headY - tailY) * interval - lift,
          (3.5 - 1.7 * interval) * zoom, 0, TAU);
        ctx.fill();
      }
    }
    ctx.restore();
  }
}
