'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = name => fs.readFileSync(path.join(__dirname, '..', name + '.js'), 'utf8');

function loadAssets(sources, behavior = {}) {
  const timers = new Set();
  class Image {
    set src(value) {
      this.source = value;
      const result = behavior[value] || 'ready';
      if (result === 'hang') return;
      queueMicrotask(() => {
        this.naturalWidth = result === 'ready' ? 256 : 0;
        this.naturalHeight = result === 'ready' ? 128 : 0;
        if (result === 'ready') this.onload?.();
        else this.onerror?.();
      });
    }
  }
  const c = vm.createContext({ Image, Promise, Object,
    setTimeout(fn) { timers.add(fn); return fn; },
    clearTimeout(fn) { timers.delete(fn); }
  });
  c.window = c;
  c.ATS_VISUAL_BUNDLE = { manifest: { characters: { version: 1 } }, sources };
  vm.runInContext(source('visual-assets'), c);
  return { c, timers, assets: c.ATSVisualAssets };
}

test('artwork preloading resolves only decoded images and releases the embedded source map', async () => {
  const { c, assets, timers } = loadAssets({ king: 'king.png', forest: 'forest.png' });
  assert.equal(assets.status, 'loading');
  await assets.ready;
  assert.equal(assets.status, 'ready');
  assert.equal(assets.get('king').naturalWidth, 256);
  assert.equal(assets.get('forest').naturalHeight, 128);
  assert.equal(assets.get('unknown'), null);
  assert.equal(assets.failures.length, 0);
  assert.equal(timers.size, 0, 'successful requests do not leave timeout tasks');
  assert.equal(c.ATS_VISUAL_BUNDLE, undefined, 'duplicate source references are released');
});

test('missing and stalled artwork settle without deadlocking the playable fallback', async () => {
  const { assets, timers } = loadAssets({ king: 'ok.png', lost: 'lost.png', stuck: 'stuck.png' }, { 'lost.png': 'error', 'stuck.png': 'hang' });
  await Promise.resolve();
  for (const expire of [...timers]) expire();
  await assets.ready;
  assert.equal(assets.status, 'degraded');
  assert.deepEqual(Array.from(assets.failures).sort(), ['lost', 'stuck']);
  assert.equal(assets.get('lost'), null);
  assert.ok(assets.get('king'));
  assert.equal(timers.size, 0);
});

test('launch progress follows completed image decodes monotonically, including failures', async () => {
  const { assets } = loadAssets({ first: 'first.png', failed: 'failed.png', last: 'last.png' }, { 'failed.png': 'error' });
  const events = [];
  assets.onProgress(a => events.push({ progress: a.progress, completed: a.completed, loaded: a.loaded, status: a.status }));
  await assets.ready;
  assert.equal(events[0].progress, 0);
  assert.equal(events.at(-1).progress, 1);
  assert.equal(events.at(-1).completed, 3);
  assert.equal(events.at(-1).loaded, 2, 'failed images count as settled work, not successfully decoded art');
  assert.equal(events.at(-1).status, 'degraded');
  for (let i = 1; i < events.length; i++) {
    assert.ok(events[i].progress >= events[i - 1].progress);
    assert.ok(events[i].progress <= 1);
  }
  assert.ok(events.some(e => e.progress > 0 && e.progress < 1), 'reports real intermediate progress');
});

test('quality settings change bounded presentation budgets and respect actual screen density', () => {
  class Renderer {}
  const c = vm.createContext({ window: null, ATSRenderer: Renderer, devicePixelRatio: 3 }); c.window = c;
  vm.runInContext(source('graphics'), c);
  const r = new Renderer();
  let previous = { animationHz: 0, particleBudget: 0, foliage: 0 };
  for (const quality of ['low', 'medium', 'high', 'ultra']) {
    r.quality = quality;
    const profile = r.graphicsProfile;
    assert.ok(Object.isFrozen(profile), 'settings cannot mutate global preset budgets');
    assert.ok(profile.animationHz >= previous.animationHz);
    assert.ok(profile.particleBudget >= previous.particleBudget && profile.particleBudget <= 512);
    assert.ok(profile.foliage >= previous.foliage);
    assert.ok(r.desiredDpr() <= 2, 'large phone DPR cannot allocate an unbounded framebuffer');
    previous = profile;
  }
  assert.equal(c.ATSGraphics.normalize('legacy-corrupt-setting'), 'high');
  r.quality = 'low'; assert.equal(r.desiredDpr(), 1);
  r.quality = 'ultra'; r.detailLevel = 4; assert.equal(r.desiredDpr(), 1);
  c.devicePixelRatio = 1; r.detailLevel = 0; assert.equal(r.desiredDpr(), 1);
  assert.deepEqual(Object.keys(Renderer.prototype), ['desiredDpr'], 'quality module adds no simulation entry points');
});

test('every packaged atlas and frame has a valid local image, crop and ground anchor', () => {
  const root = path.join(__dirname, '..', 'assets', 'visual'), ids = new Set();
  let count = 0;
  for (const family of ['characters', 'environment', 'equipment']) {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, family, 'manifest.json'), 'utf8'));
    const atlases = new Map(manifest.atlases.map(atlas => [atlas.id, atlas]));
    for (const atlas of manifest.atlases) {
      assert.ok(!ids.has(atlas.id), 'globally unique atlas id: ' + atlas.id); ids.add(atlas.id);
      const file = path.resolve(root, atlas.file);
      assert.ok(file.startsWith(root + path.sep), 'asset stays inside its package');
      const bytes = fs.readFileSync(file);
      assert.equal(bytes.subarray(0, 8).toString('hex'), '89504e470d0a1a0a', 'original PNG image');
      assert.equal(bytes.readUInt32BE(16), atlas.width, atlas.id + ' actual width');
      assert.equal(bytes.readUInt32BE(20), atlas.height, atlas.id + ' actual height');
      if (atlas.id !== 'materials-atlas') assert.equal(bytes[25], 6, 'RGBA transparency is retained for cutouts');
      if (atlas.sha256) assert.equal(require('node:crypto').createHash('sha256').update(bytes).digest('hex'), atlas.sha256);
    }
    const validate = (label, atlas, rect, anchor) => {
      assert.ok(atlas, label + ' points to an atlas');
      assert.ok(rect.length === 4 && rect.every(Number.isFinite));
      const [x, y, w, h] = rect;
      assert.ok(x >= 0 && y >= 0 && w > 0 && h > 0 && x + w <= atlas.width && y + h <= atlas.height, label + ' fits its atlas');
      assert.ok(anchor.length === 2 && anchor.every(Number.isFinite));
      assert.ok(anchor[0] >= 0 && anchor[0] <= w && anchor[1] >= 0 && anchor[1] <= h, label + ' ground anchor fits crop');
      count++;
    };
    if (family === 'characters') {
      for (const [name, frames] of Object.entries(manifest.frames)) for (const [key, f] of Object.entries(frames)) validate(name + ':' + key, atlases.get('characters-' + name), [f.x, f.y, f.w, f.h], [f.anchorX, f.anchorY]);
      for (const atlas of manifest.atlases) assert.equal(Object.keys(manifest.frames[atlas.id.replace('characters-', '')]).length, atlas.frameCount, 'all authored poses are exported');
      assert.deepEqual(manifest.species.slice().sort(), ['capuchin', 'chimpanzee', 'gibbon', 'gorilla', 'mandrill', 'orangutan']);
    } else if (Array.isArray(manifest.frames)) {
      for (const f of manifest.frames) validate(f.name, manifest.atlases[0], [f.x, f.y, f.w, f.h], f.anchor);
    } else {
      for (const [name, f] of Object.entries(manifest.frames)) validate(name, atlases.get(f.atlas), f.rect, f.anchor);
    }
  }
  assert.ok(count >= 130, 'full character, world and equipment artwork is packaged');
});

test('painted log damage follows real durability and never changes shield health', () => {
  class Renderer {}
  const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'assets', 'visual', 'equipment', 'manifest.json'), 'utf8'));
  const image = {}, calls = [], draw = new Proxy({}, { get: (_, method) => (...args) => calls.push([method, ...args]) });
  const c = vm.createContext({ ATSRenderer: Renderer, Math }); c.window = c;
  c.ATSVisualAssets = { get: () => image, manifest: { equipment: manifest } };
  vm.runInContext(source('equipment-art'), c);
  const r = new Renderer(); r.time = 10;
  const ape = { dir: Math.PI, moving: true, phase: .5, shield: { hp: 100, maxHp: 100 } };
  for (const [health, stage] of [[100, 0], [50, 1], [20, 2]]) {
    ape.shield.hp = health; calls.length = 0;
    const before = JSON.stringify(ape);
    assert.equal(r.drawLogShield(draw, ape), true);
    const atlasDraw = calls.find(call => call[0] === 'drawImage');
    assert.ok(atlasDraw);
    assert.equal(atlasDraw[2], manifest.frames[stage].x, 'damage selects the matching painted variant');
    assert.equal(JSON.stringify(ape), before, 'visual drawing does not consume durability or movement');
  }
  ape.shield.hp = 0; calls.length = 0;
  assert.equal(r.drawLogShield(draw, ape), false);
  assert.equal(calls.length, 0, 'broken shields disappear');
  c.ATSVisualAssets.get = () => null; ape.shield.hp = 100;
  assert.equal(r.drawLogShield(draw, ape), false, 'undecoded art preserves legacy shield fallback');
});

function characterArt() {
  class Renderer {}
  const c = vm.createContext({ ATSRenderer: Renderer, Math, Map, WeakMap }); c.window = c;
  const characters = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'assets', 'visual', 'characters', 'manifest.json'), 'utf8'));
  c.ATSVisualAssets = { get: () => ({}), manifest: { characters } };
  vm.runInContext(source('character-art'), c);
  return c.ATSCharacterArt;
}

test('directional animation resolves authored frames across species and combat states without entity writes', () => {
  const art = characterArt(), seen = new Set();
  const states = [
    {}, { moving: true }, { moving: true, state: 'charge' }, { hp: 10 }, { hitTimer: .1 },
    { knockbackUntil: 11 }, { climbingWallId: 'wall', wallClimbUntil: 11 },
    { animation: Object.freeze({ kind: 'slam', start: 9.9, duration: .5 }) },
    { animation: Object.freeze({ kind: 'throw', start: 9.9, duration: .5 }) },
    { animation: Object.freeze({ kind: 'rally', start: 9.9, duration: .5 }) },
    { hp: 0 }, { activity: 'rest' }, { celebrating: true }, { carrying: 'wood' }
  ];
  for (const species of art.species) for (const fields of states) for (let sector = 0; sector < 16; sector++) {
    const actor = Object.freeze({ id: 'qa-' + species, species, x: 3, y: 5, hp: 100, maxHp: 100, dir: sector * Math.PI / 8, phase: .2, ...fields });
    const before = JSON.stringify(actor);
    for (const king of [false, true]) {
      const pose = art.resolve(actor, 10, { king });
      assert.ok(art.frameRect(pose.atlas, pose.row, pose.column), `${species} ${pose.state} ${pose.directionName} has an authored crop`);
      assert.ok(Number.isFinite(pose.phase));
      assert.ok(pose.direction >= 0 && pose.direction < 8);
      seen.add(pose.directionName);
    }
    assert.equal(JSON.stringify(actor), before);
  }
  assert.equal(seen.size, 8, 'movement projection resolves all eight screen directions');
  const human = Object.freeze({ id: 'rifle', type: 'human', kind: 'rifle', hp: 100, maxHp: 100, gunFlashUntil: 11, dir: 0 });
  assert.equal(art.resolve(human, 10).state, 'fire');
  assert.equal(art.resolve(human, 10).event, 'muzzle');
});

test('animation phases are desynchronized, quantized by quality and stationary in reduced motion', () => {
  const art = characterArt();
  const actor = { id: 'walker', species: 'chimpanzee', hp: 100, maxHp: 100, moving: true, dir: 0 };
  const phases = new Set();
  for (let i = 0; i < 40; i++) phases.add(art.resolve({ ...actor, id: 'walker-' + i }, 5).phase);
  assert.ok(phases.size > 20, 'a moving horde does not share a single phase');
  const reducedA = art.resolve(actor, 4, { reducedMotion: true }), reducedB = art.resolve(actor, 7, { reducedMotion: true });
  assert.equal(reducedA.column, reducedB.column);
  assert.equal(reducedA.phase, reducedB.phase);
  assert.equal(reducedA.event, null, 'reduced motion emits no animated footstep events');
  assert.equal(art.resolve(actor, 1.01, { animationHz: 8 }).phase, art.resolve(actor, 1.02, { animationHz: 8 }).phase, 'low quality shares quantized animation sampling');
});

test('King, every species and each human class switch authored limb poses in every facing', () => {
  const art = characterArt();
  const actors = [
    { id: 'king', species: 'gorilla' },
    ...art.species.map(species => ({ id: species, species })),
    ...['pistol', 'rifle', 'machine', 'sniper'].flatMap(kind => ['patrol', 'combat'].map(state => ({ id: kind + '-' + state, type: 'human', kind, state })))
  ];
  for (const actor of actors) for (let direction = 0; direction < 8; direction++) {
    let dir = 0;
    for (let n = 0; n < 720; n++) if (art.direction(n * Math.PI / 360).sector === direction) { dir = n * Math.PI / 360; break; }
    const entity = Object.freeze({ hp: 100, maxHp: 100, state: 'patrol', moving: true, phase: .2, ...actor, dir });
    const crops = new Set();
    for (let frame = 0; frame < 72; frame++) {
      const pose = art.resolve(entity, frame / 60);
      const crop = art.frameRect(pose.atlas, pose.row, pose.column);
      assert.ok(crop);
      crops.add([pose.atlas, crop.x, crop.y, crop.w, crop.h].join(':'));
    }
    assert.ok(crops.size >= 2, `${actor.id} facing ${direction} changes actual painted limb poses, rather than translating a still`);
  }
});

function environmentArt() {
  class Renderer {}
  for (const name of ['draw', 'drawObject', 'drawGroundChunk', 'drawRock', 'drawBerry', 'drawBuilding', 'drawTower', 'drawCage', 'drawRubble', 'drawWallFaces', 'drawGate', 'drawLights', 'drawEffects', 'drawAtmosphere', 'drawMenu']) Renderer.prototype[name] = () => {};
  const math = Object.create(Math); math.random = () => { throw new Error('Decorative art must not consume gameplay randomness'); };
  const c = vm.createContext({ ATSRenderer: Renderer, Math: math, WeakMap }); c.window = c;
  const environment = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'assets', 'visual', 'environment', 'manifest.json'), 'utf8'));
  c.ATSVisualAssets = { get: () => ({}), manifest: { environment } };
  vm.runInContext(source('environment-art'), c);
  const calls = [], context = new Proxy({ globalAlpha: 1 }, { get(object, method) { if (method in object) return object[method]; return (...args) => { for (const arg of args) if (typeof arg === 'number') assert.ok(Number.isFinite(arg)); calls.push([method, ...args]); }; } });
  const renderer = new Renderer(); renderer.time = 10; renderer.camera = { zoom: 1 }; renderer.ctx = context;
  renderer.graphicsProfile = { particleBudget: 80, foliage: 0, atmosphere: 0 };
  renderer.project = (x, y, z) => { calls.push(['project', x, y, z]); return { x, y: y - z }; };
  renderer.visible = () => true;
  return { c, renderer, context, calls };
}

test('destruction particles reuse fixed storage, honor quality limits and expire', () => {
  const { renderer: r, context } = environmentArt();
  const wall = Object.freeze({ id: 'gate', x: 5, y: 8, height: 80 });
  r.emitStructureArtwork(wall, true);
  const pool = r._environmentArt.pool, members = [...pool];
  for (let i = 0; i < 1000; i++) r.emitStructureArtwork(wall, true);
  assert.equal(r._environmentArt.pool, pool);
  assert.equal(pool.length, 96);
  assert.ok(pool.every((particle, i) => particle === members[i]), 'particles recycle rather than allocate on every impact');
  assert.ok(pool.filter(p => p.life > 0).length <= 20, 'low quality emission fits its particle budget');
  r.drawEffects(context, []);
  assert.ok(r._environmentArt.active <= 20);
  r.time += 2; r.drawEffects(context, []);
  assert.equal(pool.filter(p => p.life > 0).length, 0, 'expired debris is reclaimed');
  r.reducedMotion = true; r.emitStructureArtwork(wall, true);
  assert.equal(pool.filter(p => p.life > 0).length, 0, 'reduced motion avoids decorative debris');
});

test('painted muzzle flashes follow the shot direction and preserve their source event', () => {
  const { renderer: r, context, calls } = environmentArt();
  const angles = [];
  for (const dir of [0, Math.PI]) {
    const effect = Object.freeze({ type: 'muzzle', x: 10, y: 20, z: 88, dir, life: .08, maxLife: .1 });
    calls.length = 0; r.drawEffects(context, [effect]);
    angles.push(calls.find(call => call[0] === 'rotate')[1]);
    assert.ok(calls.some(call => call[0] === 'drawImage'), 'painted light is actually drawn');
    assert.ok(calls.some(call => call[0] === 'project' && call[3] === 88), 'elevated weapons keep their actual muzzle height');
  }
  assert.ok(Math.abs(Math.abs(angles[0] - angles[1]) - Math.PI) < 1e-8, 'opposite shots produce opposite projected muzzle directions');
});

test('cage occupants stand inside the painted floor and render behind clipped front bars', () => {
  const { c, renderer: r, context, calls } = environmentArt();
  r.drawApeMini = (ctx, x, y, scale) => calls.push(['captive', x, y, scale]);
  const inside = (point, polygon) => {
    let hit = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const [xi, yi] = polygon[i], [xj, yj] = polygon[j];
      if ((yi > point.y) !== (yj > point.y) && point.x < (xj - xi) * (point.y - yi) / (yj - yi) + xi) hit = !hit;
    }
    return hit;
  };
  for (const w of [30, 45, 62, 95, 150]) for (const count of [0, 1, 2, 3, 4]) {
    const cage = Object.freeze({ id: 'cell-' + w + '-' + count, type: 'cage', x: 0, y: 0, w, h: 50, count, hp: 100, maxHp: 100, prisonKind: 'research', prisonLock: 'electronic' });
    const layout = c.ATSEnvironmentArt.cageLayout(cage);
    assert.equal(layout.occupants.length, count);
    for (const captive of layout.occupants) {
      assert.ok(inside(captive, layout.floor), 'feet are inside the painted floor at every cage size');
      assert.ok(inside(captive, layout.interior), 'captives stay in the cage clipping silhouette');
      assert.ok(captive.scale > 0 && captive.scale < .6, 'character scale fits the cage roof');
    }
    calls.length = 0; r.drawObject(context, cage);
    const captiveIndices = calls.flatMap((call, i) => call[0] === 'captive' ? [i] : []);
    assert.equal(captiveIndices.length, count);
    if (count) {
      assert.ok(calls.slice(0, captiveIndices[0]).some(call => call[0] === 'clip'), 'interior clips before drawing occupants');
      assert.ok(calls.slice(captiveIndices.at(-1) + 1).some(call => call[0] === 'clip'), 'front faces are clipped separately');
      assert.ok(calls.slice(captiveIndices.at(-1) + 1).some(call => call[0] === 'drawImage'), 'painted front bars draw in front of the captives');
    }
  }
});

test('isometric wall joins follow both axes and never bridge real gaps or open gates', () => {
  const { c } = environmentArt(), { wallLayout, gateLayout } = c.ATSEnvironmentArt;
  for (const axis of ['x', 'y']) {
    const a = { id: 'a', type: 'wall', x: 0, y: 0, w: axis === 'x' ? 100 : 14, h: axis === 'y' ? 100 : 14, hp: 100, solid: true };
    const b = { ...a, id: 'b', [axis]: 100 };
    const joint = wallLayout(a, [b]).joints.find(j => j.side === 1);
    assert.equal(joint.kind, 'straight'); assert.equal(joint.connected, true);
    assert.equal(joint.drawPost, false, 'straight connection has no doubled end post');
    assert.equal(wallLayout(a, [{ ...b, [axis]: 102.1 }]).joints.find(j => j.side === 1).connected, false, 'real gap remains unbridged');
    for (const unavailable of [{ ...b, dead: true }, { ...b, hp: 0 }, { ...b, type: 'gate', gateState: 'open' }, { ...b, type: 'gate', forcedOpen: true }]) {
      assert.equal(wallLayout(a, [unavailable]).joints.find(j => j.side === 1).connected, false);
    }
    const gate = gateLayout({ type: 'gate', w: axis === 'x' ? 120 : 14, h: axis === 'y' ? 120 : 14, gateState: 'open' });
    assert.equal(gate.axis, axis); assert.equal(gate.open, true); assert.equal(gate.opening, 120);
    assert.ok(gate.posts.every(post => post[axis === 'x' ? 'y' : 'x'] === 0), 'open gate posts follow the actual long axis');
    assert.ok(gate.posts[0][axis] < -60 && gate.posts[1][axis] > 60, 'posts leave the full navigation opening clear');
  }
  const a = { id: 'a', type: 'wall', x: 0, y: 0, w: 100, h: 14, hp: 100 }, b = { id: 'b', type: 'wall', x: 50, y: 43, w: 14, h: 100, hp: 100 };
  const corner = wallLayout(a, [b]).joints.find(j => j.side === 1);
  assert.equal(corner.kind, 'corner'); assert.equal(corner.drawPost, true);
  assert.ok(corner.x - corner.w / 2 >= 43 && corner.x + corner.w / 2 <= 50, 'corner cap stays in the shared footprint');
  assert.ok(corner.y - corner.h / 2 >= -7 && corner.y + corner.h / 2 <= 7);
});

test('wall adjacency caching invalidates with world revisions and caps spatial queries per frame', () => {
  const { renderer: r } = environmentArt();
  let queries = 0;
  const wall = { id: 'cached-wall', type: 'wall', x: 0, y: 0, w: 100, h: 14, hp: 100 };
  const world = { navRevision: 0, chunkRevision: 0, getObjects() { queries++; return []; } }, game = { world, king: { x: 0, y: 0 }, apes: [] };
  r.draw(game, 0);
  const first = r.wallArtworkLayout(wall);
  assert.equal(r.wallArtworkLayout(wall), first); assert.equal(queries, 1);
  world.navRevision++;
  assert.notEqual(r.wallArtworkLayout(wall), first); assert.equal(queries, 2, 'a newly opened gate invalidates neighboring visual joins');
  for (let i = 0; i < 100; i++) r.wallArtworkLayout({ ...wall, id: 'wall-' + i });
  assert.equal(queries, 16, 'dense fortresses stay within the per-frame adjacency budget');
  r.draw(game, 0); r.wallArtworkLayout({ ...wall, id: 'next-frame' });
  assert.equal(queries, 17, 'the next frame resumes pending visual work');
});
