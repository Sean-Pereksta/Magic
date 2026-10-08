'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { loadEngine } = require('./performance-harness.cjs');

const root = path.resolve(__dirname, '..');
const source = name => fs.readFileSync(path.join(root, name + '.js'), 'utf8');
const engine = loadEngine();

function artHarness() {
  class Renderer {}
  for (const name of ['drawPrimateGeometry', 'drawHuman', 'drawApe', 'drawHumanSprite', 'drawApeSprite', 'drawCorpses', 'drawEquipment', 'drawWorkerDetail', 'drawChampionGear']) Renderer.prototype[name] = () => {};
  const math = Object.create(Math);
  math.random = () => { throw new Error('Held artwork must not consume simulation randomness'); };
  const c = vm.createContext({ ATSRenderer: Renderer, Math: math, Map, WeakMap, Set }); c.window = c;
  const manifest = {};
  for (const family of ['characters', 'equipment', 'held-items']) manifest[family] = JSON.parse(fs.readFileSync(path.join(root, 'assets', 'visual', family, 'manifest.json'), 'utf8'));
  c.ATSVisualAssets = { get: id => ({ id }), manifest };
  vm.runInContext(source('character-art'), c, { filename: 'character-art.js' });
  vm.runInContext(source('held-item-art'), c, { filename: 'held-item-art.js' });
  return { c, art: c.ATSHeldItemArt, character: c.ATSCharacterArt, Renderer };
}

function freeze(value) {
  if (value && typeof value === 'object') { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}
const actor = fields => freeze({ id: 'coverage-actor', species: 'gorilla', type: 'ape', hp: 100, maxHp: 100, state: 'follow', dir: 0, ...fields });
const ids = entries => Array.from(entries, item => item.id);

test('all live human roles retain their actual firearm class as raster equipment', () => {
  const { art, character } = artHarness(), firearms = new Set(['pistol', 'rifle', 'shotgun', 'sniper', 'assault', 'machine', 'rotary']);
  assert.equal(Object.keys(engine.ATSHumanRoles).length, 22, 'test includes both the original force roles and arsenal additions');
  for (const [role, spec] of Object.entries(engine.ATSHumanRoles)) {
    const a = actor({ type: 'human', role, kind: spec.weapon, state: 'patrol' }), before = JSON.stringify(a);
    const equipment = ids(art.resolve(a, character.resolve(a, 10)));
    const weapon = role === 'rotary' ? 'rotary' : spec.weapon;
    assert.ok(equipment.includes(weapon), role + ' visibly carries ' + weapon);
    assert.equal(equipment.filter(id => firearms.has(id)).length, 1, role + ' has one firearm');
    assert.equal(JSON.stringify(a), before, 'appearance leaves health and equipment unchanged');
  }
});

test('role equipment and temporary hand actions stay distinguishable', () => {
  const { art, character } = artHarness();
  const resolved = fields => { const a = actor({ type: 'human', kind: 'rifle', state: 'patrol', ...fields }); return ids(art.resolve(a, character.resolve(a, 10))); };
  for (const [role, item] of [['medic', 'medkit'], ['spotter', 'binoculars'], ['mortar', 'mortar']]) assert.ok(resolved({ role }).includes(item), role + ' carries ' + item);
  assert.ok(resolved({ role: 'shield' }).some(id => /^riotShield/.test(id)));
  const radio = resolved({ role: 'officer', state: 'radio', kind: 'pistol' });
  assert.ok(radio.includes('radio'));
  assert.ok(!radio.includes('pistol'), 'radio pose does not put a second object in the speaking hand');
  for (const [role, item] of [['grenadier', 'grenade'], ['bombardier', 'grenade'], ['spotter', 'flare']]) {
    const equipment = resolved({ role, animation: { kind: 'throw', start: 9.85, duration: .6 } });
    assert.ok(equipment.includes(item), role + ' shows its real thrown payload during anticipation');
    assert.ok(!equipment.includes('rifle'), 'throwing hand is freed for the payload');
  }
  assert.ok(resolved({ role: 'engineer', engineerJob: { id: 'wall-job' } }).includes('hammer'));
});

test('every existing ape gear, worker activity, carry resource and scout sash is covered', () => {
  const { art, character } = artHarness();
  const resolved = fields => { const a = actor(fields); return ids(art.resolve(a, character.resolve(a, 10))); };
  for (const [gear, spec] of Object.entries(engine.ATSEquipmentGear)) assert.ok(resolved({ species: spec.species[0], equipment: { [gear]: true } }).includes(gear), gear + ' has raster art');
  for (const activity of ['building', 'chopping', 'clearing woodland', 'repairing defenses', 'gardening', 'cooking', 'breaching']) assert.ok(resolved({ activity }).includes('hammer'), activity + ' has a held work tool');
  assert.ok(resolved({ carrying: 'wood' }).includes('wood'));
  assert.ok(resolved({ carrying: false, _carryingWood: true }).includes('wood'), 'legacy render wrapper preserves carried logs');
  assert.ok(resolved({ carrying: 'food' }).includes('food'));
  assert.ok(resolved({ carrying: true }).includes('food'), 'legacy food-carry flag remains visible');
  assert.ok(resolved({ state: 'scout' }).some(id => /^scoutSash/.test(id)));
  assert.ok(resolved({ shield: { hp: 50, maxHp: 100 } }).includes('logShield'));
  assert.ok(!resolved({ shield: { hp: 0, maxHp: 100 } }).includes('logShield'), 'broken shields are not resurrected by the artwork');
});

test('all 24 champion identities receive actual equipment, including their named hand tools', () => {
  const { art, character } = artHarness();
  assert.equal(Object.keys(engine.ATSChampionClasses).length, 24);
  for (const spec of Object.values(engine.ATSChampionClasses)) {
    const a = actor({ species: spec.species, champion: { archetype: spec.id } });
    const equipment = ids(art.resolve(a, character.resolve(a, 10)));
    assert.ok(equipment.length > 0, spec.label + ': ' + spec.item + ' is visible');
    const tool = { wallbreaker: 'maul', saboteur: 'cutters', shockRaider: 'baton', skyRunner: 'hook', guardCaptain: 'staff', warcaller: 'drum', alarmSpoiler: 'jammer' }[spec.id];
    if (tool) assert.ok(equipment.includes(tool), spec.label + ' carries its named tool');
  }
});

test('all equipment resolves finite sockets and directional layers without actor writes', () => {
  const { art, character } = artHarness(), directions = new Set();
  const subjects = [
    ...Object.entries(engine.ATSHumanRoles).map(([role, spec]) => ({ type: 'human', role, kind: spec.weapon })),
    ...Object.values(engine.ATSChampionClasses).map(spec => ({ species: spec.species, champion: { archetype: spec.id } })),
    { state: 'scout', equipment: { cuffs: true, torch: true }, carrying: 'wood' },
    { species: 'capuchin', equipment: { spear: true } }, { shield: { hp: 10, maxHp: 100 } }
  ];
  for (const fields of subjects) for (let step = 0; step < 32; step++) {
    const a = actor({ ...fields, dir: step * Math.PI / 16 }), before = JSON.stringify(a), pose = character.resolve(a, 10);
    directions.add(pose.direction);
    const anchors = art.sockets(a, pose, pose.human ? 60 : 76);
    for (const name of ['left', 'right', 'both', 'body', 'back', 'hip', 'ground']) assert.ok(Number.isFinite(anchors[name]?.x) && Number.isFinite(anchors[name]?.y), name + ' socket has finite coordinates');
    for (const item of art.resolve(a, pose)) {
      assert.ok(item.id && typeof item.id === 'string');
      assert.ok(item.socket, item.id + ' attaches to a named pose socket');
      assert.ok(item.layer === 'front' || item.layer === 'rear', item.id + ' has a direction-aware body layer');
      const at = art.placement(a, pose, item, anchors);
      assert.ok([at.x, at.y, at.rotation].every(Number.isFinite), item.id + ' placement is finite');
    }
    assert.equal(JSON.stringify(a), before);
  }
  assert.equal(directions.size, 8);
  const dead = actor({ hp: 0, state: 'fallen', carrying: 'wood', equipment: { torch: true }, shield: { hp: 30, maxHp: 100 } });
  assert.equal(art.resolve(dead, character.resolve(dead, 10)).length, 0, 'fallen actors do not regain standing equipment overlays');
});

test('scout fabric and riot shields select their visible face while hands follow body depth', () => {
  const { art } = artHarness();
  for (const rear of [false, true]) {
    const suffix = rear ? 'Rear' : 'Front', pose = { rear, human: false, state: 'idle' };
    const scout = art.resolve(actor({ state: 'scout', equipment: { torch: true } }), pose);
    assert.ok(ids(scout).includes('scoutSash' + suffix));
    assert.equal(scout.find(i => i.id === 'scoutSash' + suffix).layer, 'front', 'the visible fabric covers the body surface');
    assert.equal(scout.find(i => i.id === 'torch').layer, rear ? 'rear' : 'front', 'rear-facing hand-held item can be occluded by the torso');
    const shield = art.resolve(actor({ type: 'human', role: 'shield', kind: 'shotgun' }), { ...pose, human: true });
    assert.ok(ids(shield).includes('riotShield' + suffix));
    assert.ok(!ids(shield).includes('riotShield' + (rear ? 'Front' : 'Rear')));
  }
});

test('legal champion loadouts use free hands and stow surplus kit without changing ownership', () => {
  const { art, character } = artHarness();
  const combinations = [
    { species: 'capuchin', champion: { archetype: 'slingerAce' }, equipment: { spear: true } },
    { species: 'capuchin', champion: { archetype: 'alarmSpoiler' }, equipment: { spear: true }, carrying: 'food' },
    { species: 'gibbon', champion: { archetype: 'skyRunner' }, equipment: { torch: true } },
    { species: 'gorilla', champion: { archetype: 'wallbreaker' }, equipment: { cuffs: true }, shield: { hp: 70, maxHp: 100 } },
    { species: 'orangutan', champion: { archetype: 'greatForager' }, carrying: 'wood' }
  ];
  for (const fields of combinations) for (const rear of [false, true]) {
    const a = actor(fields), before = JSON.stringify(a), pose = { ...character.resolve(a, 10), rear }, items = art.resolve(a, pose), hands = new Set();
    for (const item of items) {
      if (['cuffs', 'chainWrap'].includes(item.id) || !['both', 'left', 'right'].includes(item.socket)) continue;
      for (const hand of item.socket === 'both' ? ['left', 'right'] : [item.socket]) {
        assert.ok(!hands.has(hand), a.champion.archetype + ' has only one held item per palm');
        hands.add(hand);
      }
    }
    for (const item of items.filter(i => i.socket === 'back')) assert.equal(item.layer, rear ? 'front' : 'rear', 'stowed item follows the back surface');
    assert.ok(items.length >= 2, 'owned kit remains visible while stowed');
    assert.equal(JSON.stringify(a), before);
  }
});

test('actual human and ape raster draw calls place rear equipment under the body and front equipment above it', () => {
  const { c, art, character, Renderer } = artHarness(), calls = [], r = new Renderer();
  r.time = 10; r.detailLevel = 2; r.camera = { zoom: 1 }; r.graphicsProfile = { animationHz: 18 };
  r.glow = r.drawSignal = r.drawAwarenessIcon = r.health = () => {};
  const context = new Proxy({}, { get: (target, name) => name in target ? target[name] : (...args) => { for (const v of args) if (typeof v === 'number') assert.ok(Number.isFinite(v), String(name) + ' uses finite coordinates'); calls.push([name, ...args]); } });
  const manifest = c.ATSVisualAssets.manifest['held-items'], seen = new Set();
  const signature = f => [f.atlas, f.x, f.y, f.w, f.h].join(':');
  for (let step = 0; step < 32; step++) for (const fields of [
    { species: 'orangutan', activity: 'building', champion: { archetype: 'rescueBearer' } },
    { type: 'human', role: 'shield', kind: 'shotgun', state: 'patrol' },
    { species: 'gibbon', state: 'scout', equipment: { torch: true } }
  ]) {
    const a = actor({ ...fields, dir: step * Math.PI / 16 }), before = JSON.stringify(a), p = character.resolve(a, r.time), items = art.resolve(a, p);
    calls.length = 0;
    if (p.human) r.drawHuman(context, a); else r.drawPrimateGeometry(context, a);
    const images = calls.filter(call => call[0] === 'drawImage'), body = images.findIndex(call => call[1].id.startsWith('characters-'));
    assert.ok(body >= 0, 'painted body was drawn');
    for (const item of items) {
      const frame = manifest.frames[item.id]; assert.ok(frame, item.id + ' has a packaged raster');
      const at = images.findIndex(call => [call[1].id, ...call.slice(2, 6)].join(':') === signature(frame));
      assert.ok(at >= 0, item.id + ' actually issues a cropped image draw');
      assert.ok(item.layer === 'rear' ? at < body : at > body, item.id + ' appears on its correct side of the body draw');
      seen.add(item.layer + ':' + p.direction);
    }
    assert.equal(JSON.stringify(a), before);
  }
  assert.equal(seen.size, 16, 'both compositing layers are exercised in all eight screen facings');
});

test('every held-item crop points to a packaged transparent PNG and valid grip', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'assets', 'visual', 'held-items', 'manifest.json'), 'utf8'));
  const packageRoot = path.join(root, 'assets', 'visual'), atlases = new Map(manifest.atlases.map(a => [a.id, a]));
  assert.ok(atlases.size > 0);
  for (const atlas of atlases.values()) {
    const file = path.resolve(packageRoot, atlas.file);
    assert.ok(file.startsWith(packageRoot + path.sep), 'image remains inside visual package');
    const bytes = fs.readFileSync(file);
    assert.equal(bytes.subarray(0, 8).toString('hex'), '89504e470d0a1a0a');
    assert.equal(bytes.readUInt32BE(16), atlas.width);
    assert.equal(bytes.readUInt32BE(20), atlas.height);
    assert.equal(bytes[25], 6, 'held item has RGBA transparency');
    if (atlas.sha256) assert.equal(require('node:crypto').createHash('sha256').update(bytes).digest('hex'), atlas.sha256);
  }
  for (const [id, f] of Object.entries(manifest.frames)) {
    const atlas = atlases.get(f.atlas);
    assert.ok(atlas, id + ' references a packaged atlas');
    assert.ok([f.x, f.y, f.w, f.h, f.gripX, f.gripY, f.drawWidth, f.drawHeight].every(Number.isFinite), id + ' has finite crop and socket dimensions');
    assert.ok(f.x >= 0 && f.y >= 0 && f.w > 0 && f.h > 0 && f.x + f.w <= atlas.width && f.y + f.h <= atlas.height, id + ' crop stays in bounds');
    assert.ok(f.gripX >= 0 && f.gripX <= f.w && f.gripY >= 0 && f.gripY <= f.h, id + ' grip lies on the source crop');
    assert.ok(f.drawWidth > 0 && f.drawHeight > 0);
  }
});
