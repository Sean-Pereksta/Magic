/* Shared, cached canvas artwork: physical standards, ground scratches and spoor.
 * No downloaded files, per-unit particles or simulation-dependent timing. */
(() => {
  'use strict';
  const P = ATSRenderer.prototype, baseEffects = P.drawEffects, baseDraw = P.draw;
  const KINDS = Object.freeze(['charge', 'follow', 'hold', 'defend', 'retreat', 'attack', 'movement']);
  const INK = Object.freeze({ charge: '#b65343', follow: '#c4ac70', hold: '#83a2a2', defend: '#719b86', retreat: '#bc9a6d', attack: '#bc6952', movement: '#b7b38a' });
  const cache = new Map(), MAX_MARKERS = 8, MAX_DUST = 24, MAX_CACHE = 31;
  const TRAVEL_ORDERS = new Set(['charge', 'follow', 'retreat', 'movement', 'attack']);
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const poly = (c, points, fill, stroke, width = 1) => {
    c.beginPath(); points.forEach(([x, y], i) => i ? c.lineTo(x, y) : c.moveTo(x, y)); c.closePath();
    if (fill) { c.fillStyle = fill; c.fill(); }
    if (stroke) { c.strokeStyle = stroke; c.lineWidth = width; c.stroke(); }
  };
  const line = (c, x, y, u, v, color, width = 1) => {
    c.beginPath(); c.moveTo(x, y); c.lineTo(u, v); c.strokeStyle = color; c.lineWidth = width; c.lineCap = 'round'; c.stroke();
  };
  function claws(c, color, size = 1) {
    c.save(); c.scale(size, size);
    for (let i = 0; i < 3; i++) poly(c, [[-10 + i * 7, 9], [-5 + i * 7, -10], [-1 + i * 7, -12], [-5 + i * 7, 5]], color);
    c.restore();
  }
  function shield(c, color) {
    poly(c, [[-11, -10], [0, -14], [11, -10], [9, 5], [0, 13], [-9, 5]], '#293d36', color, 1.7);
    line(c, 0, -9, 0, 7, color, 2); line(c, -6, -3, 6, -3, color, 2);
  }
  function foot(c, color) {
    poly(c, [[-3, 8], [-5, 3], [-3, -4], [2, -6], [5, -3], [4, 1], [1, 3], [1, 8]], color);
    for (let i = 0; i < 3; i++) line(c, -2 + i * 3, -8, -1 + i * 3, -10 + i % 2, color, 2.4);
  }
  function emblem(c, kind, color = '#f3dfac') {
    if (kind === 'charge' || kind === 'attack') {
      claws(c, color, kind === 'charge' ? .86 : 1);
      if (kind === 'charge') { line(c, -15, 10, 13, 10, color, 2); line(c, 7, 5, 13, 10, color, 2); }
    } else if (kind === 'defend') shield(c, color);
    else if (kind === 'hold') {
      shield(c, color); line(c, -16, 14, 16, 14, color, 2.2);
    } else if (kind === 'retreat') {
      poly(c, [[-14, 0], [-3, -10], [-3, -4], [12, -4], [12, 4], [-3, 4], [-3, 10]], color);
      line(c, 16, -8, 16, 8, color, 2);
    } else {
      c.save(); c.translate(-5, 4); c.rotate(-.2); c.scale(.8, .8); foot(c, color); c.restore();
      c.save(); c.translate(7, -5); c.rotate(.15); c.scale(.8, .8); foot(c, color); c.restore();
      if (kind === 'follow') line(c, -13, 13, 13, 13, color, 1.8);
    }
  }
  function asset(kind, frame = 0) {
    const key = kind + ':' + frame; if (cache.has(key)) return cache.get(key);
    const canvas = document.createElement('canvas'); canvas.width = 96; canvas.height = 112;
    const c = canvas.getContext('2d'); c.translate(48, 96);
    if (kind === 'dust') {
      for (let i = 0; i < 4; i++) poly(c, [[-23 + i * 9, -3], [-20 + i * 9, -9 - i % 2 * 3], [-9 + i * 9, -10], [2 + i * 9, -4], [-6 + i * 9, 1]], i % 2 ? '#b0a07d44' : '#d0bb8f37');
    } else if (kind === 'footprint') {
      foot(c, '#d8c596');
    } else if (kind === 'scratch') {
      c.save(); c.transform(.8, .42, -.8, .42, 0, 0); claws(c, '#d7ad7a', 1.4); c.restore();
    } else {
      const color = INK[kind] || INK.follow, flap = [-2, 0, 2, 0][frame % 4];
      // The cloth stays above a wooden shaft; its foot is planted in a small
      // patch of disturbed earth in the same projection as the terrain.
      poly(c, [[-24, 0], [-11, -5], [5, 1], [-8, 7]], '#0c1b16a8');
      line(c, -13, 1, -13, -75, '#352e22', 5); line(c, -14, -1, -14, -75, '#b59b65', 1.5);
      poly(c, [[-16, -74], [-13, -83], [-10, -74]], '#d1bf8f', '#514933');
      poly(c, [[-12, -71], [27, -67 + flap], [22, -48 + flap], [29, -29 + flap], [8, -36], [-12, -38]], '#25332a', '#11251f', 2);
      poly(c, [[-11, -70], [25, -66 + flap], [21, -49 + flap], [27, -32 + flap], [8, -38], [-11, -40]], color, '#ddc18b', .8);
      line(c, -8, -65, 19, -62 + flap, '#f2d9a14d', 2); line(c, -7, -44, 18, -40 + flap, '#352e263b', 2);
      c.save(); c.translate(6, -53 + flap * .4); c.scale(.68, .68); emblem(c, kind); c.restore();
      for (let i = 0; i < 3; i++) line(c, -19 + i * 8, 2 + i % 2, -16 + i * 8, 4 + i % 2, '#b4a27b', 1.5);
    }
    if (cache.size >= MAX_CACHE) cache.delete(cache.keys().next().value);
    cache.set(key, canvas); return canvas;
  }
  function target(game, e) {
    if (!e.targetId || !game) return null;
    const t = e.targetKind === 'human' ? game.humansById?.get(e.targetId) : e.targetKind === 'vehicle' ?
      game.vehicleGrid?.get?.(e.targetId) || game.vehicles?.find(a => a.id === e.targetId) : game.world?.objects?.get(e.targetId);
    return t && t.hp > 0 && !t.dead ? t : null;
  }
  P.draw = function (game, ...args) {
    this._commandGame = game; this.commandMarkerDraws = 0; this.commandDustDraws = 0;
    this.commandDustCells = this.commandDustCells || new Set(); this.commandDustCells.clear();
    return baseDraw.call(this, game, ...args);
  };
  P.drawCommandMarker = function (kind, x, y, options = {}) {
    if (this.commandMarkerDraws >= MAX_MARKERS || !Number.isFinite(x) || !Number.isFinite(y)) return false;
    kind = KINDS.includes(kind) ? kind : 'follow'; const c = this.ctx, z = this.camera.zoom;
    const p = this.project(x, y); if (!this.visible(p, 95 * z)) return false;
    this.commandMarkerDraws = (this.commandMarkerDraws || 0) + 1;
    const frame = this.reducedMotion ? 0 : Math.floor(this.time * 9) % 4;
    const alpha = clamp(options.alpha ?? 1, 0, 1), isTarget = kind === 'attack';
    c.save(); c.globalAlpha = alpha;
    if (isTarget) {
      c.drawImage(asset('scratch'), p.x - 48 * z, p.y - 96 * z, 96 * z, 112 * z);
      // A target's compact standard sits beside its footprint, never over the
      // soldier, vehicle roof or prison artwork.
      const offset = clamp(options.footprint || 30, 25, 65) * z;
      c.drawImage(asset(kind, frame), p.x + offset - 48 * z * .62, p.y + 6 * z - 96 * z * .62, 96 * z * .62, 112 * z * .62);
    } else {
      const size = options.selected ? .7 : .9;
      c.drawImage(asset(kind, frame), p.x - 48 * z * size, p.y - 96 * z * size, 96 * z * size, 112 * z * size);
    }
    c.restore(); return true;
  };
  P.drawCommandFootprints = function (from, to, alpha = .5, count = 5) {
    if (!from || !to) return; const dx = to.x - from.x, dy = to.y - from.y, distance = Math.hypot(dx, dy);
    if (distance < 12) return; const c = this.ctx, z = this.camera.zoom, n = Math.min(6, count, Math.ceil(distance / 22));
    // Only acknowledge the start of a heading. Long straight lines would imply
    // a walkable path through walls, and crowd the map with unrelated overlays.
    const span = Math.min(distance, 90), angle = Math.atan2(dy, dx) + Math.PI / 2;
    for (let i = 0; i < n; i++) {
      const t = (i + 1) / (n + 1) * span / distance, side = i % 2 ? 4 : -4;
      const p = this.project(from.x + dx * t - dy / distance * side, from.y + dy * t + dx / distance * side);
      if (!this.visible(p, 12)) continue; c.save(); c.translate(p.x, p.y); c.transform(.8, .42, -.8, .42, 0, 0); c.rotate(angle);
      c.globalAlpha = alpha * (1 - i / (n + 1)); c.drawImage(asset('footprint'), -48 * z * .5, -96 * z * .5, 96 * z * .5, 112 * z * .5); c.restore();
    }
  };
  P.drawCommandDust = function (x, y, heavy = false, alpha = .55) {
    const max = this.reducedMotion ? 0 : this.detailLevel >= 3 || this.quality === 'low' ? 8 : MAX_DUST;
    if ((this.commandDustDraws || 0) >= max) return; const p = this.project(x, y), z = this.camera.zoom;
    if (!this.visible(p, 45 * z)) return;
    const cells = this.commandDustCells || (this.commandDustCells = new Set()), key = Math.floor(x / 78) + ',' + Math.floor(y / 78);
    if (cells.has(key)) return; cells.add(key); this.commandDustDraws = (this.commandDustDraws || 0) + 1;
    const size = heavy ? .95 : .65, c = this.ctx; c.save(); c.globalAlpha = alpha;
    c.drawImage(asset('dust'), p.x - 48 * z * size, p.y - 96 * z * size, 96 * z * size, 112 * z * size); c.restore();
  };
  P.drawEffects = function (c, effects) {
    const regular = [], game = this._commandGame;
    for (const e of effects) {
      if (e.type !== 'wave' && e.type !== 'charge' && e.type !== 'command' || e.type === 'wave' && !e.commandKind) { regular.push(e); continue; }
      const t = clamp(1 - e.life / (e.maxLife || 1), 0, 1);
      if (e.life <= 0) continue;
      const kind = e.commandKind || (e.type === 'charge' ? 'charge' : 'follow'), subject = target(game, e);
      let x = subject?.x ?? e.x, y = subject?.y ?? e.y;
      if (e.type === 'charge' || e.type === 'wave' && (kind === 'charge' || kind === 'attack')) { const dir = e.dir ?? Math.atan2(e.dy || 0, e.dx || 1); x += Math.cos(dir) * 570; y += Math.sin(dir) * 570; }
      const alpha = Math.min(1, (1 - t) * 2.8), drawn = this.drawCommandMarker(kind, x, y, { alpha, footprint: subject?.r || 30 });
      if (drawn && t < .6 && kind !== 'attack' && TRAVEL_ORDERS.has(kind)) {
        const origin = { x: e.originX ?? e.x, y: e.originY ?? e.y }, dir = e.dir ?? 0;
        this.drawCommandFootprints(origin, { x: origin.x + Math.cos(dir) * 95, y: origin.y + Math.sin(dir) * 95 }, alpha * .7, 5);
        if (t < .4) this.drawCommandDust(origin.x, origin.y, kind === 'charge', alpha * .8);
      }
    }
    baseEffects.call(this, c, regular);
    // Dust uses the already culled visible list and a shared world-cell budget.
    // Paint here, outside actor-local transforms; no new particles are created.
    if (!this.reducedMotion) for (const entry of this.drawEntries || []) {
      if (this.commandDustDraws >= (this.detailLevel >= 3 || this.quality === 'low' ? 8 : MAX_DUST)) break;
      const a = entry.a;
      if (entry.type !== 'ape' || !a.moving) continue;
      const phase = (this.time * (a.species === 'gorilla' ? 2.4 : 3.8) + (a.phase || 0)) % 1;
      if (phase < .2 && TRAVEL_ORDERS.has(window.ATSCommandAnimation?.orderFor(a, game, this.time))) this.drawCommandDust(a.x - Math.cos(a.dir || 0) * 12, a.y - Math.sin(a.dir || 0) * 12, a.species === 'gorilla', .4 - phase);
    }
  };
  P.drawCommandSiege = function (game) {
    const siege = game.siege; if (!siege) return; const c = this.ctx, z = this.camera.zoom;
    for (const b of siege.stones || []) {
      const p = this.project(b.x, b.y, b.z ?? b.z0); if (!this.visible(p, 20)) continue;
      c.save(); c.fillStyle = '#d8c49c'; c.fillRect(p.x - 2 * z, p.y - 2 * z, 4 * z, 3 * z); c.restore();
    }
    if (!siege.selected.length) return;
    const members = siege.selectedMembers(), stride = Math.max(1, Math.ceil(members.length / 24));
    for (let i = 0; i < members.length; i += stride) {
      const a = members[i], p = this.project(a.x, a.y, a.elevation || a.wallClimbHeight || 0);
      if (!this.visible(p, 24)) continue;
      line(c, p.x - 10 * z, p.y + 4 * z, p.x - 4 * z, p.y + 7 * z, '#d4d5a0', 1.5);
      line(c, p.x + 4 * z, p.y + 7 * z, p.x + 10 * z, p.y + 4 * z, '#d4d5a0', 1.5);
    }
    const seen = new Set(); let routes = 0;
    for (const species of siege.selected) {
      const group = siege.groups[species], representative = members.find(a => a.species === species);
      if (!representative) continue;
      const start = representative.armyOrder?.index || 0, orders = [...(group?.orders || []).slice(start), ...(siege.drafts[species] || [])];
      let previous = representative;
      for (const order of orders) {
        const key = order.id + ':' + order.x + ':' + order.y;
        if (seen.has(key)) { previous = order; continue; } seen.add(key);
        if (routes++ >= 8) break;
        const kind = window.ATSCommandAnimation?.canonical(order.type) || order.type, subject = target(game, order), at = subject || order;
        if ((this.commandMarkerDraws || 0) < 4) this.drawCommandMarker(kind, at.x, at.y, { selected: true, alpha: group?.blocked ? .9 : .6, footprint: subject?.r });
        if (routes <= 4 && !group?.blocked) this.drawCommandFootprints(previous, at, .28, 4);
        previous = at;
      }
    }
    // Preserve functional climb/control discovery cues from the former renderer.
    for (const o of game.world.getObjects(game.king.x, game.king.y, game.viewRadius || 900)) {
      if (o.dead || !siege.known(o) || !o.climbAccess && o.type !== 'gateControl') continue;
      const p = this.project(o.x, o.y, (o.visualHeight || o.height || 20) + 10); if (!this.visible(p, 20)) continue;
      if (o.climbAccess) {
        line(c, p.x - 5, p.y + 7, p.x - 5, p.y - 7, '#b4d58d', 1.5); line(c, p.x + 5, p.y + 7, p.x + 5, p.y - 7, '#b4d58d', 1.5);
        for (let y = -5; y <= 5; y += 5) line(c, p.x - 5, p.y + y, p.x + 5, p.y + y, '#b4d58d', 1);
      } else poly(c, [[p.x - 5, p.y - 5], [p.x + 5, p.y - 5], [p.x + 5, p.y + 5], [p.x - 5, p.y + 5]], null, '#e2c384', 1.5);
    }
  };
  function icon(canvas, kind) {
    if (!canvas || !KINDS.includes(kind)) return false;
    canvas.width = canvas.height = 48; const c = canvas.getContext('2d'); c.translate(24, 24); c.scale(1.1, 1.1);
    emblem(c, kind, '#ead5a3'); return true;
  }
  window.ATSCommandVisuals = { version: 1, kinds: KINDS, icon,
    status: () => ({ cachedSprites: cache.size, cacheLimit: MAX_CACHE, groupMarkersPerFrame: MAX_MARKERS, dustPerFrame: MAX_DUST }),
    manifest: Object.freeze({ generated: 'shared canvas atlas', externalAssets: false, framesPerStandard: 4,
      frameWidth: 96, frameHeight: 112, anchor: Object.freeze({ x: 48, y: 96 }), animationHz: 9 }) };
})();
