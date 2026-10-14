const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto, createHash } = require('node:crypto');
const source = fs.readFileSync(path.join(__dirname, 'graphics.js'), 'utf8');
const threeSource = process.env.THREE_SOURCE;
const integration = (name, fn) => test(name, { skip: !threeSource && 'Set THREE_SOURCE to the Three.js 0.160.0 browser build' }, fn);

function makeGLB(change) {
  const binary = Buffer.alloc(36 + 8 + 24);
  [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1].forEach((v, i) => binary.writeFloatLE(v, i * 4));
  const data = {
    asset: { version: '2.0' }, scene: 0, scenes: [{ nodes: [0] }],
    nodes: [{ name: 'torso', translation: [2, 0, 0], mesh: 0, children: [1] }, { name: 'head', translation: [0, 2, 0], mesh: 0 }],
    meshes: [{ primitives: [{ attributes: { POSITION: 0 }, material: 0 }] }],
    materials: [{ name: 'shirt', pbrMetallicRoughness: { baseColorFactor: [0.2, 0.5, 0.8, 1], roughnessFactor: 0.8, metallicFactor: 0 } }],
    buffers: [{ byteLength: binary.length }],
    bufferViews: [{ buffer: 0, byteOffset: 0, byteLength: 36 }, { buffer: 0, byteOffset: 36, byteLength: 8 }, { buffer: 0, byteOffset: 44, byteLength: 24 }],
    accessors: [{ bufferView: 0, componentType: 5126, type: 'VEC3', count: 3 }, { bufferView: 1, componentType: 5126, type: 'SCALAR', count: 2 }, { bufferView: 2, componentType: 5126, type: 'VEC3', count: 2 }],
    animations: [{ name: 'Run', samplers: [{ input: 1, output: 2, interpolation: 'LINEAR' }], channels: [{ sampler: 0, target: { node: 1, path: 'translation' } }] }]
  };
  if (change) change(data, binary);
  let json = Buffer.from(JSON.stringify(data));
  json = Buffer.concat([json, Buffer.alloc((4 - json.length % 4) % 4, 32)]);
  const result = Buffer.alloc(12 + 8 + json.length + 8 + binary.length);
  result.writeUInt32LE(0x46546c67, 0); result.writeUInt32LE(2, 4); result.writeUInt32LE(result.length, 8);
  result.writeUInt32LE(json.length, 12); result.writeUInt32LE(0x4e4f534a, 16); json.copy(result, 20);
  result.writeUInt32LE(binary.length, 20 + json.length); result.writeUInt32LE(0x004e4942, 24 + json.length); binary.copy(result, 28 + json.length);
  return result;
}

function fixture(bytes = makeGLB()) {
  const checksum = createHash('sha256').update(bytes).digest('hex');
  const assets = Array.from({ length: 262 }, (_, i) => ({ id: `characters:model-${i}`, path: `characters/model-${i}.glb`, category: 'characters', bytes: bytes.length, sha256: checksum, bounds: { min: [0, 0, 0], max: [3, 3, 0] } }));
  return { manifest: { format: 'chris-runner-glb-pack', version: 'cartoon-v1', assetCount: 262, totalGLBBytes: 262 * bytes.length, assets }, bytes };
}

function runtime(pack, options = {}) {
  const calls = new Map();
  let active = 0, maximum = 0;
  const context = { console, URL, AbortController, TextDecoder, Uint8Array, Int8Array, Int16Array, Uint16Array, Uint32Array, Float32Array, ArrayBuffer, DataView,
    document: { currentScript: { src: 'https://game.invalid/game/chris-runner/graphics.js' } }, location: { href: 'https://game.invalid/game/chrisrunner.html' },
    crypto: webcrypto, setTimeout: (fn, ms) => setTimeout(fn, ms < 2000 ? 0 : ms), clearTimeout };
  context.window = context;
  context.fetch = async url => {
    const parsed = new URL(url);
    const relative = parsed.origin === 'https://game.invalid' ? 'manifest.json' : parsed.pathname.slice(1);
    calls.set(relative, (calls.get(relative) || 0) + 1);
    if (relative === 'manifest.json') return new Response(JSON.stringify(pack.manifest));
    active++; maximum = Math.max(maximum, active);
    try {
      await new Promise(resolve => setImmediate(resolve));
      if (options.beforeAsset) await options.beforeAsset(relative, calls.get(relative));
      const bytes = options.asset ? options.asset(relative, pack.bytes) : pack.bytes;
      return new Response(new Uint8Array(bytes));
    } finally { active--; }
  };
  vm.createContext(context);
  if (threeSource) vm.runInContext(fs.readFileSync(threeSource, 'utf8'), context);
  vm.runInContext(source, context);
  return { api: context.RunnerGraphics, context, calls, get maximum() { return maximum; } };
}

test('standalone graphics script parses', () => new vm.Script(source));

test('manifest rejects paths outside the pack before requesting any model', async () => {
  for (const invalid of ['https://evil.invalid/model.glb', '../model.glb', 'characters/%2e%2e/model.glb', 'characters/model.glb?url=bad']) {
    const pack = fixture(); pack.manifest.assets[0].path = invalid;
    const { api, calls } = runtime(pack);
    await assert.rejects(api.start());
    assert.equal(api.ready, false); assert.equal(api.status.phase, 'error'); assert.equal(calls.size, 1);
  }
});

integration('all models preload with bounded concurrency and real progress before readiness', async () => {
  const pack = fixture(), events = [];
  let release;
  const hold = new Promise(resolve => { release = resolve; });
  const run = runtime(pack, { beforeAsset: async relative => { if (relative === 'characters/model-0.glb') await hold; } });
  const loading = run.api.start(event => events.push(event));
  while (run.calls.size < 263 || run.api.status.loaded < 261) await new Promise(resolve => setImmediate(resolve));
  assert.equal(run.api.ready, false); assert.equal(run.api.status.loaded, 261);
  release(); await loading;
  assert.equal(run.api.ready, true); assert.equal(run.api.status.loaded, 262);
  assert.equal(run.api.status.loadedBytes, pack.manifest.totalGLBBytes);
  assert.equal(run.api.status.totalBytes, pack.manifest.totalGLBBytes);
  assert.equal(run.maximum, 5);
  assert.ok(events.some(event => event.loadedBytes > 0 && event.loadedBytes < event.totalBytes));
  assert.ok([...run.calls.values()].every(count => count === 1));
  await run.api.start(); assert.equal(run.calls.size, 263);
});

integration('actors retain named parts, own clip bindings and optional private materials', async () => {
  const { api, context } = runtime(fixture()); await api.start();
  const first = api.create('characters:model-0'), second = api.create('characters/model-0.glb');
  const own = api.create('characters:model-0', { ownMaterials: true });
  assert.ok(first.getObjectByName('head')); assert.ok(first.getObjectByName('torso'));
  const a = first.getObjectByName('head').children[0], b = second.getObjectByName('head').children[0], c = own.getObjectByName('head').children[0];
  assert.notEqual(first, second); assert.equal(a.geometry, b.geometry); assert.equal(a.material, b.material);
  assert.equal(a.geometry.userData.runnerAssetShared, true); assert.equal(a.material.userData.runnerAssetShared, true);
  assert.notEqual(a.material, c.material); assert.equal(c.geometry, a.geometry); assert.equal(c.material.userData.runnerAssetShared, undefined);
  c.material.color.setHex(0xff0000); assert.notEqual(c.material.color.getHex(), a.material.color.getHex());
  const mixer = new context.THREE.AnimationMixer(first); mixer.clipAction(first.userData.clips[0]).play(); mixer.update(0.5);
  assert.equal(first.getObjectByName('head').position.z, 0.5);
  assert.equal(second.getObjectByName('head').position.z, 0);
  assert.ok(first.userData.clips[0].tracks[0].name.startsWith(first.getObjectByName('head').uuid));
  const primitives = api.primitives('characters:model-0');
  assert.equal(primitives.length, 2); assert.equal(primitives[0].geometry, a.geometry);
  assert.equal(primitives[0].matrix.elements[12], 2); assert.equal(primitives[1].matrix.elements[13], 2);
});

integration('failure is explicit and Retry downloads only missing assets', async () => {
  let fail = true;
  const pack = fixture();
  const run = runtime(pack, { beforeAsset: relative => { if (fail && relative === 'characters/model-7.glb') throw new Error('offline'); } });
  await assert.rejects(run.api.start(), /1 model could not load/);
  assert.equal(run.api.ready, false); assert.equal(run.api.status.phase, 'error'); assert.equal(run.api.status.loaded, 261);
  assert.equal(run.calls.get('characters/model-7.glb'), 3);
  const cached = run.api.template('characters:model-0'); fail = false;
  await run.api.retry();
  assert.equal(run.api.ready, true); assert.equal(run.calls.get('characters/model-7.glb'), 4);
  assert.equal(run.calls.get('characters/model-0.glb'), 1); assert.equal(run.api.template('characters:model-0'), cached);
  assert.equal(run.api.status.loadedBytes, pack.manifest.totalGLBBytes);
});

integration('a broad network failure stops dispatching the pack and Retry can finish', async () => {
  let offline = true;
  const pack = fixture();
  const run = runtime(pack, { beforeAsset: () => { if (offline) throw new Error('host unreachable'); } });
  await assert.rejects(run.api.start(), /Loading paused after repeated errors/);
  assert.equal(run.api.ready, false); assert.equal(run.api.status.phase, 'error');
  assert.equal(run.api.status.loaded, 0); assert.equal(run.api.status.loadedBytes, 0);
  // Five failures stop dispatch. Up to four workers may already have started
  // their next file, each with the existing maximum of three attempts.
  const assetCalls = [...run.calls.entries()].filter(([name]) => name !== 'manifest.json');
  assert.ok(assetCalls.length >= 5 && assetCalls.length <= 9);
  assert.ok(assetCalls.reduce((sum, [, count]) => sum + count, 0) <= 27);
  offline = false;
  await run.api.retry();
  assert.equal(run.api.ready, true); assert.equal(run.api.status.loaded, 262);
  assert.equal(run.api.status.loadedBytes, pack.manifest.totalGLBBytes);
  assert.equal(run.calls.size, 263);
});

integration('malformed GLB hierarchy cannot become ready', async () => {
  const broken = makeGLB(data => { data.nodes[1].children = [0]; });
  const good = fixture();
  good.manifest.assets[0].bytes = broken.length;
  good.manifest.assets[0].sha256 = createHash('sha256').update(broken).digest('hex');
  good.manifest.totalGLBBytes += broken.length - good.bytes.length;
  const { api } = runtime(good, { asset: (relative, bytes) => relative === 'characters/model-0.glb' ? broken : bytes });
  await assert.rejects(api.start(), /Cyclic GLB hierarchy/);
  assert.equal(api.status.loaded, 261); assert.equal(api.ready, false);
});

integration('checksum mismatch remains an actionable loading failure', async () => {
  const pack = fixture(); pack.manifest.assets[0].sha256 = '0'.repeat(64);
  const { api } = runtime(pack);
  await assert.rejects(api.start(), /checksum did not match/);
  assert.equal(api.status.loaded, 261); assert.equal(api.ready, false);
});

integration('uploaded pack parses all 262 production models', async () => {
  const root = process.env.RUNNER_PACK_ROOT;
  if (!root) return;
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
  const { api } = runtime({ manifest }, { asset: relative => fs.readFileSync(path.join(root, relative)) });
  await api.start();
  assert.equal(api.status.loaded, 262); assert.equal(api.status.loadedBytes, manifest.totalGLBBytes);
  for (const entry of manifest.assets) {
    const object = api.create(entry.id);
    assert.equal(object.userData.runnerAsset.id, entry.id);
    assert.ok(api.primitives(entry.id).length > 0);
    for (const part of entry.parts || []) assert.ok(object.getObjectByName(part), `${entry.id}: missing ${part}`);
    assert.equal(object.userData.clips.length, (entry.animations || []).length);
  }
});
