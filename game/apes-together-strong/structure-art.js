/* Read-only illustrated buildings and exact-footprint river decks.
 * Cached ground artwork shares the renderer's existing 8M-pixel cache.
 * Construction, damage, defenses and population remain simulation-owned. */
(() => {
  'use strict';
  const P = ATSRenderer.prototype, base = {};
  for (const key of ['draw', 'drawObject', 'drawBuilding', 'drawSettlement', 'drawHut', 'drawSettlementProp', 'drawConstruction', 'drawVillageGeometry', 'drawGroundChunk']) base[key] = P[key];
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const iso = (x, y) => [(x - y) * .8, (x + y) * .42];
  function descriptor(name, stage = 4) { const m = window.ATSVisualAssets?.manifest?.settlements; return m?.construction?.[name]?.[stage] || m?.frames?.[name]; }
  function image(name) { const f = descriptor(name); return f && window.ATSVisualAssets?.get(f.atlas); }
  function art(c, name, width, height = width, x = 0, y = 0, stage = 4) {
    const f = descriptor(name, stage), im = f && window.ATSVisualAssets?.get(f.atlas); if (!im) return false;
    c.drawImage(im, ...f.rect, x - f.anchor[0] / f.rect[2] * width, y - f.anchor[1] / f.rect[3] * height, width, height); return true;
  }
  function env(c, name, width, height = width, x = 0, y = 0) {
    const f = window.ATSVisualAssets?.manifest?.environment?.frames?.[name], im = f && window.ATSVisualAssets.get(f.atlas);
    if (!im) return false;
    c.drawImage(im, ...f.rect, x - f.anchor[0] / f.rect[2] * width, y - f.anchor[1] / f.rect[3] * height, width, height); return true;
  }
  function line(c, x, y, u, v, color, width = 1) { c.beginPath(); c.moveTo(x, y); c.lineTo(u, v); c.strokeStyle = color; c.lineWidth = width; c.stroke(); }
  function shadow(c, width) { c.beginPath(); c.ellipse(0, 3, width * .39, width * .14, 0, 0, Math.PI * 2); c.fillStyle = 'rgba(0,9,8,.25)'; c.fill(); }
  function appearance(kind) {
    const map = {
      hut: ['hut', 76, 79], lodge: ['lodge', 148, 144], longhouse: ['lodge', 102, 91], canopyHut: ['warLodge', 119, 115],
      spearTower: ['lookout', 83, 144], lookout: ['lookout', 75, 127],
      spearBattery: ['spearBattery', 102, 149], spearBallista: ['spearBallista', 94, 88],
      training: ['training', 116, 108], nursery: ['nursery', 90, 98],
      orchard: ['orchard', 89, 88], garden: ['garden', 75, 60], cooking: ['cooking', 82, 70], barrier: ['barrier', 98, 70],
      rallyGrove: ['rallyGrove', 91, 83], communal: ['rallyGrove', 80, 61], gathering: ['rallyGrove', 80, 61],
      workShelter: ['workShelter', 87, 83], workshop: ['workShelter', 87, 83],
      storage: ['storage', 85, 78], food: ['storage', 85, 78]
    };
    return map[kind] || null;
  }
  function lodgeStage(s) { return s.expansionLevel >= 2 ? 2 : s.expansionLevel >= 1 || (s.level || 1) >= 3 ? 1 : 0; }
  function constructionKind(kind) { return /Expansion$/.test(kind) ? kind === 'warlordExpansion' ? 'canopyHut' : 'longhouse' : kind; }
  function damage(r, c, object, width, height) { r.drawArtworkDamage?.(c, object, width * .65, height * .6); }
  function ambient(r, c, name, width, height, x, y) {
    if (r.detailLevel >= 3 || r._structureAmbientBudget <= 0) return;
    r._structureAmbientBudget--;
    c.save(); c.globalAlpha *= .48; env(c, name, width, height, x, y); c.restore();
  }
  P.draw = function (g, dt) {
    this._structureAmbientBudget = this.quality === 'low' ? 5 : 18;
    this._structureWorld = g.world;
    return base.draw.call(this, g, dt);
  };
  P.drawHut = function (c, h) {
    const look = appearance(h.kind || 'hut');
    if (!look || !image(look[0])) return base.drawHut.call(this, c, h);
    if (h.stage !== undefined && h.stage < 4) return this.drawConstruction(c, h);
    if (h.hp <= 0) return this.drawRubble(c, h);
    const [name, width, height] = look;
    shadow(c, width); art(c, name, width, height, 0, 5); damage(this, c, h, width, height);
    if (h.hp < h.maxHp) this.health(c, h.hp / h.maxHp, -height * .78, 34, '#c4c894');
  };
  P.drawSettlement = function (c, s) {
    const stage = lodgeStage(s), name = ['hut', 'lodge', 'warLodge'][stage];
    if (!image(name)) return base.drawSettlement.call(this, c, s);
    const width = [102, 148, 198][stage], height = [101, 144, 188][stage];
    if (s.lodge?.hp === 0) { this.drawRubble(c, s.lodge); return; }
    shadow(c, width); art(c, name, width, height, 0, 6);
    if (s.lodge) damage(this, c, s.lodge, width, height);
    // The existing communal fire is a visual activity, never a new worker job.
    const fx = width * .37, fy = 15;
    env(c, 'campfire', 25, 27, fx, fy);
    c.save(); const flicker = this.reducedMotion ? 1 : 1 + Math.sin(this.time * 8.7) * .05;
    c.translate(fx, fy - 3); c.scale(1, flicker); env(c, 'fire', 15, 23); c.restore();
    if (this.detailLevel < 2) this.glow(c, fx, fy - 5, 28, '236,163,75', .11);
    const phase = this.reducedMotion ? .4 : this.time * .37 % 1;
    ambient(this, c, 'smoke', 14 + phase * 14, 25, fx + phase * 6, -10 - phase * 17);
    const lodgeWork = (s.projects || []).some(p => !p.done && (p.stage ?? 0) < 4
      && ['lodge', 'royalExpansion', 'warlordExpansion'].includes(p.kind)
      && (p.progress > 0 || p.work > 0));
    if (lodgeWork) this.drawBuildingScaffold(c, width * .75, height * .7);
    if (s.attack) {
      c.font = '700 10px system-ui'; c.textAlign = 'center'; c.fillStyle = '#eda282';
      c.fillText(s.name || 'Ape village', 0, -height * .85); c.fillText('UNDER ATTACK', 0, -height * .85 - 14);
    }
    if (s.maxDefense > 0) this.health(c, (s.defense || 0) / s.maxDefense, 27, 55, '#9db899');
  };
  P.drawSettlementProp = function (c, o) {
    const kind = o.kind || o.type, look = appearance(kind);
    if (!look || !image(look[0])) return base.drawSettlementProp.call(this, c, o);
    if (o.stage !== undefined && o.stage < 4) return this.drawConstruction(c, o);
    if (o.hp <= 0) return this.drawRubble(c, o);
    const [name, width, height] = look;
    shadow(c, width); art(c, name, width, height, 0, 4); damage(this, c, o, width, height);
    if (o.hp > 0 && o.hp < o.maxHp) this.health(c, o.hp / o.maxHp, -height * .84, 38, '#b7c895');
    if (this.time - (o.lastShot ?? -10) < .2) this.glow(c, 6, -height * .65, 18, '229,205,119', .2);
    if (kind === 'cooking') { const flicker = this.reducedMotion ? 1 : 1 + Math.sin(this.time * 9) * .09; c.save(); c.translate(0, -6); c.scale(1, flicker); env(c, 'fire', 17, 25); c.restore(); ambient(this, c, 'smoke', 17, 22, 5, -30); }
    // Keep the existing workshop's real crafting progress visible over its new art.
    const job = kind === 'workShelter' && this._equipmentGame?.equipment?.queue?.[0];
    if (job && job.workshopId === o.id) {
      this.health(c, clamp(job.progress / job.duration, 0, 1), -height * .94, 55, '#e4bd74');
      if (this.detailLevel < 3 && job.progress > 0 && !this.reducedMotion && Math.sin(this.time * 8) > .25) {
        line(c, -9, -24, -16, -33, '#f5d17e', 1.5); line(c, -6, -25, -2, -37, '#f5d17e', 1.5);
      }
    }
  };
  P.drawVillageGeometry = function (c, kind, variant, ruin) {
    const look = appearance(kind);
    if (!look || !image(look[0])) return base.drawVillageGeometry.call(this, c, kind, variant, ruin);
    if (ruin) return this.drawRubble(c, {id: kind + variant});
    shadow(c, look[1]); art(c, ...look, 0, 4);
  };
  P.drawBuildingScaffold = function (c, width, height) {
    for (const sign of [-1, 1]) { const x = sign * width * .41; line(c, x, 8, x, -height * .78, '#ad9262', 2.6); }
    for (const level of [.25, .53, .78]) line(c, -width * .41, -height * level, width * .41, -height * level + 8, '#9f895c', 1.7);
    line(c, -width * .41, 8, width * .41, -height * .78, '#87764f', 1.5);
    env(c, 'fallenLog', width * .4, 22, -width * .25, 10);
  };
  P.drawConstruction = function (c, p) {
    const look = appearance(constructionKind(p.kind || 'hut'));
    if (!look || !image(look[0])) return base.drawConstruction.call(this, c, p);
    const [name, width, height] = look, stage = clamp(p.stage || 0, 0, 3), progress = clamp(p.progress || 0, 0, 1);
    shadow(c, width);
    // Every stage is an authored sprite, including the completed frame used
    // by drawHut/Props. The simulation's saved stage is authoritative.
    c.save(); if (p.kind === 'barrier' && (p.h || 0) > (p.w || 0)) c.scale(-1, 1);
    art(c, name, width, height, 0, 4, stage); c.restore();
    this.health(c, progress, 22, 38, '#b6c88a');
  };
  function humanLook(o, kind, world) {
    if (o.commandCenter) return 'command';
    if (o.repairBay) return 'garage';
    const site = world?.sites?.get?.(o.siteId);
    if (kind === 'barracks' && site?.prison) return 'prison';
    return kind === 'barracks' || kind === 'depot' ? kind : null;
  }
  P.drawBuilding = function (c, o, kind) {
    const name = humanLook(o, kind, this._structureWorld);
    if (!name || !image(name)) return base.drawBuilding.call(this, c, o, kind);
    // The painted foundation follows the collision rectangle's projected width.
    const width = clamp(((o.w || 77) + (o.h || 59)) * .8 * 1.1, 83, 190), height = width * (name === 'command' ? 1.14 : .91);
    shadow(c, width); art(c, name, width, height, 0, 8); damage(this, c, o, width, height);
    if (o.powered !== false && this.detailLevel < 3) this.glow(c, -width * .17, -height * .22, 22, '235,181,91', .1);
  };
  P.drawObject = function (c, o) {
    // Supersede old repair-bay and command-building geometric overlays too.
    if (!o.dead && (o.commandCenter || o.repairBay) && image(o.commandCenter ? 'command' : 'garage')) {
      this.drawBuilding(c, o, o.type);
      if (o.hp > 0 && o.hp < o.maxHp) this.health(c, o.hp / o.maxHp, -98, 38, '#d7ae6b');
      return;
    }
    return base.drawObject.call(this, c, o);
  };
  P.drawCrossingDeck = function (c, crossing, cx, cy) {
    const f = descriptor(crossing.type), im = f && image(crossing.type); if (!im) return;
    const origin = iso(crossing.minX - cx, crossing.minY - cy), width = crossing.maxX - crossing.minX, length = crossing.maxY - crossing.minY;
    c.save(); c.translate(...origin); c.transform(.8, .42, -.8, .42, 0, 0);
    c.beginPath(); c.rect(0, 0, width, length); c.clip();
    // The natural rocks lie on a continuous shallow stone bed: tiny painted
    // gaps never imply deep-water collision inside the traversable corridor.
    c.fillStyle = crossing.type === 'ford' ? '#354c45' : '#474d3d'; c.fillRect(0, 0, width, length);
    const tile = crossing.type === 'ford' ? 88 : 96;
    for (let x = 0; x < width; x += tile) for (let y = 0; y < length; y += tile) c.drawImage(im, ...f.rect, x, y, tile, tile);
    if (crossing.type !== 'ford') {
      c.strokeStyle = crossing.type === 'wood' ? '#aea079' : '#7c8378'; c.lineWidth = 3;
      c.beginPath(); c.moveTo(2, 0); c.lineTo(2, length); c.moveTo(width - 2, 0); c.lineTo(width - 2, length); c.stroke();
      // Low edge beams follow the deck sides, never span the open approaches.
      for (let y = 14; y < length; y += 45) for (const x of [3, width - 3]) { c.fillStyle = '#353d32'; c.fillRect(x - 2, y - 2, 4, 5); }
    }
    c.restore();
  };
  P.drawGroundChunk = function (c, world, bx, by, size, tiles, decoration) {
    const hasDecks = !!image('wood') && typeof world.crossingsNear === 'function';
    const before = this.illustratedCrossings; this.illustratedCrossings = hasDecks;
    try { base.drawGroundChunk.call(this, c, world, bx, by, size, tiles, decoration); } finally { this.illustratedCrossings = before; }
    if (!hasDecks) return;
    const span = size * tiles, cx = (bx + .5) * span, cy = (by + .5) * span;
    c.save(); c.beginPath(); c.moveTo(0, -span * .42); c.lineTo(span * .8, 0); c.lineTo(0, span * .42); c.lineTo(-span * .8, 0); c.closePath(); c.clip();
    for (const crossing of world.crossingsNear(cx, cy, span).slice(0, 12)) this.drawCrossingDeck(c, crossing, cx, cy);
    c.restore();
  };
  window.ATSStructureArt = { appearance, lodgeStage, humanLook, constructionKind, descriptor };
})();
