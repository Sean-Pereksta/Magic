#!/usr/bin/env node
'use strict';

// Run offline: node --test game/neon-arena/tests/characters-models.test.cjs
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const models = require('../neon-models.js');
const root = path.resolve(__dirname, '..');
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'assets/characters/manifest.json'), 'utf8'));
const humanIds = [...models.HUMANS, ...models.UNLOCKABLE_HUMANS];
const bytesById = new Map(humanIds.map(id => [id, fs.readFileSync(path.join(root, 'assets/characters', id + '.glb'))]));
const parsedById = new Map([...bytesById].map(([id, bytes]) => [id, models.parseGLB(bytes, 'characters/' + id + '.glb')]));
const near = (actual, expected, message, tolerance = 1e-5) =>
  assert.ok(Math.abs(actual - expected) <= tolerance, `${message}: ${actual} != ${expected}`);

function glbJSON(bytes) {
  for (let offset = 12; offset < bytes.length;) {
    const length = bytes.readUInt32LE(offset), type = bytes.readUInt32LE(offset + 4);
    if (type === 0x4e4f534a) return JSON.parse(bytes.subarray(offset + 8, offset + 8 + length).toString('utf8').trim());
    offset += 8 + length;
  }
  assert.fail('Missing GLB JSON');
}

function assets(config = {}) {
  const context = vm.createContext({ URL, console, location: { href: 'https://game.test/repository/game/neon-arena/' } });
  context.window = context;
  context.NEON_ASSET_CONFIG = config;
  vm.runInContext(fs.readFileSync(path.join(root, 'neon-assets-config.js'), 'utf8'), context);
  vm.runInContext(fs.readFileSync(path.join(root, 'neon-assets.js'), 'utf8'), context);
  return context.NeonAssets;
}

const response = (bytes, status = 200) => ({
  ok: status >= 200 && status < 300, status,
  arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
});
async function until(predicate, message) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.fail('Did not settle: ' + message);
}

test('the 13 deployed human GLBs are complete, opaque, self-contained, and have unit normals', () => {
  assert.equal(humanIds.length, 13);
  assert.equal(models.UNLOCKABLE_HUMANS.length, 6);
  assert.deepEqual(manifest.models.map(item => item.characterId).sort(), [...humanIds].sort());
  for (const [id, bytes] of bytesById) {
    const entry = manifest.models.find(item => item.characterId === id), json = glbJSON(bytes), model = parsedById.get(id);
    assert.equal(entry.bytes, bytes.length, id + ': manifest length');
    assert.equal(entry.sha256, crypto.createHash('sha256').update(bytes).digest('hex'), id + ': manifest hash');
    assert.equal(json.asset.version, '2.0', id);
    assert.equal(json.buffers.length, 1, id);
    assert.equal(json.buffers[0].uri, undefined, id + ': geometry is embedded');
    assert.ok(!json.images?.length && !json.skins?.length && !json.extensionsRequired?.length, id + ': supported renderer features');
    assert.ok(json.meshes.every(mesh => mesh.primitives.every(p => p.attributes.NORMAL !== undefined)), id + ': authored normals');
    assert.ok(json.materials.every(mat => (mat.alphaMode || 'OPAQUE') === 'OPAQUE' && (mat.pbrMetallicRoughness.baseColorFactor[3] ?? 1) === 1), id + ': opaque materials');
    assert.ok(model.vertexCount > 10000 && model.vertexCount < 25000, id + ': detailed geometry stays bounded');
    assert.ok(model.parts.reduce((sum, part) => sum + part.vertices.length, 0) === model.vertices.length, id + ': every vertex belongs to one part');
    assert.ok(model.bounds.max[0] - model.bounds.min[0] > 1.5, id + ': broad armor silhouette');
    for (let offset = 0; offset < model.vertices.length; offset += 12) {
      assert.ok(Array.from(model.vertices.subarray(offset, offset + 12)).every(Number.isFinite), id + ': finite geometry');
      near(Math.hypot(...model.vertices.subarray(offset + 3, offset + 6)), 1, id + ': unit normal', 1e-4);
    }
  }
});

test('every named head, glove, shoulder, and leg attachment follows its intended limb', () => {
  for (const [id, model] of parsedById) {
    const json = glbJSON(bytesById.get(id));
    const classified = new Map(model.parts.flatMap(part => part.names.map(name => [name, part.key])));
    assert.deepEqual(model.parts.map(part => part.key).sort(), ['armL', 'armR', 'body', 'head', 'legL', 'legR'].sort(), id);
    for (const node of json.nodes) {
      if (node.mesh === undefined) continue;
      const name = node.name || '', side = (node.translation?.[0] ?? node.matrix?.[12] ?? 0) < 0 ? 'L' : 'R';
      let expected = 'body';
      if (/^(?:helmet|visor|neck_guard|head)(?:_|$)/.test(name)) expected = 'head';
      else if (/^(?:shoulder|upper_arm|elbow|forearm|glove|foreclaw)(?:_|$)/.test(name)) expected = 'arm' + side;
      else if (/^(?:boot|shin|knee|thigh|calf|ankle|upper_leg|lower_leg)(?:_|$)/.test(name)) expected = 'leg' + side;
      assert.equal(classified.get(name), expected, `${id}: ${name}`);
    }
  }
});

test('rig metadata grounds soles and gives all bodies the same 2.39 height despite ornaments', () => {
  near(models.HUMAN_HEIGHT, 2.39, 'human world height');
  const authoredHeights = new Set();
  for (const [id, model] of parsedById) {
    const rig = glbJSON(bytesById.get(id)).asset.extras.neonBreach;
    const actor = { x: 3.25, y: 1.86, z: -4.5, alien: false, r: .5 };
    const before = structuredClone(actor), fit = models.actorFit(model, actor);
    assert.deepEqual(actor, before, id + ': fitting cannot mutate gameplay state');
    assert.ok(Number.isFinite(rig.bodyHeight) && rig.bodyHeight > 0, id + ': body height metadata');
    near(model.bodyHeight, rig.bodyHeight, id + ': body normalization excludes ornaments');
    near(model.bodyHeight * fit.scale[1], 2.39, id + ': common world body height');
    near(model.bounds.min[1] * fit.scale[1] + fit.offset[1], 0, id + ': soles on ground');
    near(fit.scale[0], fit.scale[1], id + ': uniform fit');
    near(fit.scale[2], fit.scale[1], id + ': uniform fit');
    assert.deepEqual(model.anchors.head, rig.aimHead, id + ': head aim anchor');
    assert.deepEqual(model.anchors.core, rig.aimBody, id + ': chest aim anchor');
    for (const part of model.parts) {
      if (!/^(?:arm|leg|head)/.test(part.key)) continue;
      const anchor = part.key === 'head' ? rig.headPivot : part.key.startsWith('arm') ? rig.shoulder : rig.hip;
      const expected = anchor.map((value, axis) => axis === 0 && part.key !== 'head' ? Math.abs(value) * (part.key.endsWith('L') ? -1 : 1) : value);
      assert.deepEqual(part.pivot, expected, `${id}: ${part.key} uses its authored pivot`);
    }
    authoredHeights.add(model.height.toFixed(4));
  }
  assert.ok(authoredHeights.size > 3, 'ornaments have varied silhouette heights without shrinking the body');
});

test('all six unlocks have distinct geometry rather than only recolored materials', () => {
  const hashes = models.UNLOCKABLE_HUMANS.map(id => {
    const model = parsedById.get(id), positions = new Float32Array(model.vertexCount * 3);
    for (let i = 0, p = 0; i < model.vertices.length; i += 12, p += 3) positions.set(model.vertices.subarray(i, i + 3), p);
    return crypto.createHash('sha256').update(Buffer.from(positions.buffer)).digest('hex');
  });
  assert.equal(new Set(hashes).size, 6);
});

test('actual combat rays hit exposed shins, low mobile aim, torso, and visors from level and elevated views', () => {
  // Execute the production hit test with independent authored-rig aim references.
  // This catches targets that visually exist but the game's ray test cannot hit.
  const source = fs.readFileSync(path.join(root, 'neon-arena.js'), 'utf8');
  const start = source.indexOf('function rayEntity('), end = source.indexOf('function lineClear(', start);
  assert.ok(start >= 0 && end > start, 'production combat ray test is available');
  const context = vm.createContext({
    PLAYER_H: 1.86, HUMAN_VISUAL_WIDTH: 1.10,
    humanTargetY(actor, kind) {
      const model = parsedById.get(actor.modelId), fit = models.actorFit(model, actor);
      return actor.y - 1.86 + fit.offset[1] + model.anchors[kind === 'head' ? 'head' : 'core'][1] * fit.scale[1];
    }
  });
  vm.runInContext(source.slice(start, end), context);
  const shoot = (target, origin, point, range = 30) => {
    const dx = point.x - origin.x, dy = point.y - origin.y, dz = point.z - origin.z, length = Math.hypot(dx, dy, dz);
    return context.rayEntity(origin, { x: dx / length, y: dy / length, z: dz / length }, target, range);
  };
  for (const [id, model] of parsedById) for (const ground of [0, 4]) {
    const actor = { modelId: id, x: 0, y: ground + 1.86, z: 12, r: .5, alien: false };
    const fit = models.actorFit(model, actor), visor = {
      x: actor.x + fit.offset[0] + model.anchors.head[0] * fit.scale[0],
      y: ground + fit.offset[1] + model.anchors.head[1] * fit.scale[1],
      z: actor.z + fit.offset[2] + model.anchors.head[2] * fit.scale[2]
    };
    for (const [label, y] of [['shins', ground + .30], ['low mobile aim', ground + .55], ['torso', context.humanTargetY(actor, 'body')]]) {
      const hit = shoot(actor, { x: 0, y, z: 0 }, { x: 0, y, z: 12 });
      assert.equal(hit?.head, false, `${id} on ground ${ground}: ${label} is a body hit`);
      assert.ok(Number.isFinite(hit.dist) && hit.dist > 0, id + ': valid body impact depth');
    }
    assert.equal(shoot(actor, { x: 0, y: visor.y, z: 0 }, visor)?.head, true, id + ': level visor shot');
    assert.equal(shoot(actor, { x: 0, y: ground + 9, z: 4 }, visor)?.head, true, id + ': steep downward visor shot');
    for (const [x, y] of [[2, ground + .55], [2, visor.y], [0, ground - .5], [0, ground + 3.5]]) {
      assert.equal(shoot(actor, { x, y, z: 0 }, { x, y, z: 12 }), null, `${id}: clear miss at ${x},${y}`);
    }
    assert.equal(shoot(actor, { x: 0, y: visor.y, z: 0 }, visor, 5), null, id + ': maximum range is respected');
  }
});

test('asset URLs use characters/v3 on R2 and repository-relative local fallbacks', () => {
  const api = assets();
  for (const id of humanIds) {
    assert.equal(api.characters[id], 'characters/' + id + '.glb');
    assert.equal(api.url('characters/' + id + '.glb'), api.baseURL + '/characters/v3/' + id + '.glb?v=2');
    assert.equal(api.localCharacterURL('characters/' + id + '.glb'), 'https://game.test/repository/game/neon-arena/assets/characters/' + id + '.glb');
  }
  assert.equal(api.url('weapons/pistol.glb'), api.baseURL + '/weapons/pistol.glb?v=2');
  assert.equal(api.url('characters/alien_blue.glb'), api.baseURL + '/characters/alien_blue.glb?v=2');
  assert.equal(api.url('ui/menu/logo.png'), 'https://game.test/repository/game/neon-arena/assets/ui/menu/logo.png?v=2');
  for (const unsafe of ['characters/../player_one.glb', 'characters/unknown.glb', 'weapons/pistol.glb']) {
    assert.equal(api.localCharacterURL(unsafe), '', 'fallback only serves known humans: ' + unsafe);
  }
  assert.equal(api.url('characters/../player_one.glb'), '');
  const local = assets({ ASSET_BASE_URL: '' });
  assert.equal(local.url('characters/player_one.glb'), local.localCharacterURL('characters/player_one.glb'), 'local characters remain playable without R2');
});

test('HTTP errors, CORS failures, and corrupt remote GLBs each use the bundled character', async () => {
  const api = assets(), modelPath = 'characters/character_ronin.glb', bytes = bytesById.get('character_ronin');
  for (const failure of ['http', 'network', 'corrupt']) {
    const requests = [], cache = new models.ModelCache({
      resolve: api.url, fallback: api.localCharacterURL,
      fetch: async (url, options) => {
        requests.push({ url, options });
        if (url === api.localCharacterURL(modelPath)) return response(bytes);
        if (failure === 'http') return response(Buffer.alloc(0), 404);
        if (failure === 'network') throw new TypeError('CORS/network unavailable');
        return response(Buffer.from('corrupt model response'));
      }
    });
    assert.equal(cache.get(modelPath), null);
    cache.get(modelPath);
    await until(() => cache.active === 0, failure + ' fallback');
    assert.deepEqual(requests.map(request => request.url), [api.url(modelPath), api.localCharacterURL(modelPath)], failure);
    assert.ok(cache.get(modelPath), failure + ': parsed fallback is cached');
    assert.equal(requests.length, 2, failure + ': no duplicate downloads');
    assert.equal(cache.status().failed, 0, failure + ': successful fallback does not fail match loading');
    for (const request of requests) {
      assert.equal(request.options.credentials, 'omit');
      assert.equal(request.options.mode, 'cors');
    }
  }
});

test('a timed-out remote fetch aborts and retries locally with a fresh abort signal', async () => {
  const timers = new Map(), requests = [];
  let nextTimer = 0;
  const context = vm.createContext({
    TextDecoder, AbortController, ArrayBuffer, console,
    setTimeout(callback, delay) { const id = ++nextTimer; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); }
  });
  vm.runInContext(fs.readFileSync(path.join(root, 'neon-models.js'), 'utf8'), context);
  const cache = new context.NeonModels.ModelCache({
    resolve: () => 'https://remote.test/characters/v3/player_one.glb',
    fallback: () => 'https://game.test/assets/characters/player_one.glb',
    fetch: (url, options) => {
      requests.push({ url, options });
      if (url.startsWith('https://game.test/')) return Promise.resolve(response(bytesById.get('player_one')));
      return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(new Error('AbortError')), { once: true }));
    }
  });
  cache.get('characters/player_one.glb');
  await until(() => requests.length === 1, 'remote request starts');
  assert.equal(requests[0].options.signal.aborted, false);
  const timeout = [...timers.values()][0];
  assert.equal(timeout.delay, 12000, 'remote requests have a bounded deadline');
  timeout.callback();
  await until(() => cache.active === 0, 'timeout fallback finishes');
  assert.equal(requests.length, 2);
  assert.equal(requests[0].options.signal.aborted, true);
  assert.notEqual(requests[0].options.signal, requests[1].options.signal);
  assert.equal(requests[1].options.signal.aborted, false, 'an expired signal cannot cancel local fallback');
  assert.ok(cache.get('characters/player_one.glb'));
  assert.equal(timers.size, 0, 'completed requests clean up their deadline timers');
});

test('failure of both character sources remains a readable loading failure', async () => {
  const cache = new models.ModelCache({ resolve: () => 'https://remote.test/model.glb', fallback: () => 'https://game.test/local.glb', fetch: async () => response(Buffer.alloc(0), 404) });
  await assert.rejects(cache.preload(['characters/player_one.glb']), /Unable to load characters\/player_one\.glb: Model HTTP 404/);
  assert.equal(cache.status().failed, 1);
  assert.equal(cache.status().parsed, 0);
});
