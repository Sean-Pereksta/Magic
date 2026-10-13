#!/usr/bin/env node
'use strict';

// Dependency-free checks: node game/neon-arena/tests/model-renderer.cjs
// Validate the optional, uncommitted art pack too:
//   node game/neon-arena/tests/model-renderer.cjs --assets /path/to/unpacked
// or set NEON_ASSET_SOURCE to that directory. The scratch art pack is autodetected.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
require('../neon-models.js');
const { parseGLB, actorFit, ModelCache, createRenderer, compose, multiply, transform } = globalThis.NeonModels;

let passed = 0;
async function test(name, run) {
  await run();
  passed++;
  console.log(`PASS ${name}`);
}
const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(predicate, description) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await tick();
  }
  assert.fail(`Did not settle: ${description}`);
}
const near = (actual, expected, tolerance = 1e-5) =>
  assert.ok(Math.abs(actual - expected) <= tolerance, `${actual} != ${expected}`);

function fixture({ indices = [0, 1, 2], normals, nodes, mutate } = {}) {
  const positions = new Float32Array([-.15, 0, 0, .15, 0, 0, 0, .3, .05]);
  const rawIndices = new Uint16Array(indices);
  const authoredNormals = normals === undefined ? null : new Float32Array(normals);
  const normalOffset = Math.ceil((positions.byteLength + rawIndices.byteLength) / 4) * 4;
  const geometry = Buffer.alloc(normalOffset + (authoredNormals?.byteLength || 0));
  Buffer.from(positions.buffer).copy(geometry, 0);
  Buffer.from(rawIndices.buffer).copy(geometry, positions.byteLength);
  if (authoredNormals) Buffer.from(authoredNormals.buffer).copy(geometry, normalOffset);
  const json = {
    asset: { version: '2.0' },
    buffers: [{ byteLength: geometry.byteLength }],
    bufferViews: [
      { buffer: 0, byteOffset: 0, byteLength: positions.byteLength },
      { buffer: 0, byteOffset: positions.byteLength, byteLength: rawIndices.byteLength }
    ],
    accessors: [
      { bufferView: 0, componentType: 5126, count: 3, type: 'VEC3' },
      { bufferView: 1, componentType: 5123, count: indices.length, type: 'SCALAR' }
    ],
    materials: [{ pbrMetallicRoughness: { baseColorFactor: [.3, .5, .7, 1] }, emissiveFactor: [.02, .04, .06] }],
    meshes: [{ primitives: [{ attributes: { POSITION: 0 }, indices: 1, material: 0 }] }],
    nodes: nodes || [
      { name: 'torso', mesh: 0, translation: [0, .8, 0] },
      { name: 'helmet', mesh: 0, translation: [0, 1.7, 0] },
      { name: 'upper_leg_left', mesh: 0, translation: [-.25, 0, 0] },
      { name: 'upper_leg_right', mesh: 0, translation: [.25, 0, 0] },
      { name: 'shoulder_left', mesh: 0, translation: [-.5, 1.45, 0] },
      { name: 'shoulder_right', mesh: 0, translation: [.5, 1.45, 0] }
    ],
    scenes: [{ nodes: [] }], scene: 0
  };
  if (authoredNormals) {
    json.bufferViews.push({ buffer: 0, byteOffset: normalOffset, byteLength: authoredNormals.byteLength });
    json.accessors.push({ bufferView: 2, componentType: 5126, count: authoredNormals.length / 3, type: 'VEC3' });
    json.meshes[0].primitives[0].attributes.NORMAL = 2;
  }
  json.scenes[0].nodes = json.nodes.map((_, i) => i);
  mutate?.(json, geometry);
  let encoded = Buffer.from(JSON.stringify(json));
  encoded = Buffer.concat([encoded, Buffer.alloc((4 - encoded.length % 4) % 4, 0x20)]);
  const glb = Buffer.alloc(12 + 8 + encoded.length + 8 + geometry.length);
  glb.writeUInt32LE(0x46546c67, 0);
  glb.writeUInt32LE(2, 4);
  glb.writeUInt32LE(glb.length, 8);
  glb.writeUInt32LE(encoded.length, 12);
  glb.writeUInt32LE(0x4e4f534a, 16);
  encoded.copy(glb, 20);
  const binStart = 20 + encoded.length;
  glb.writeUInt32LE(geometry.length, binStart);
  glb.writeUInt32LE(0x004e4942, binStart + 4);
  geometry.copy(glb, binStart + 8);
  return glb;
}
const actorBytes = fixture();
const weaponBytes = fixture({ nodes: [
  { name: 'receiver', mesh: 0 },
  { name: 'magazine', mesh: 0, translation: [0, -.15, 0] },
  { name: 'barrel', mesh: 0, translation: [0, 0, .4] },
  { name: 'energy_core', mesh: 0, translation: [0, .1, .1] }
] });
const response = (bytes, status = 200) => ({
  ok: status >= 200 && status < 300, status,
  arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
});
function glbJSON(bytes) {
  let offset = 12;
  while (offset < bytes.length) {
    const length = bytes.readUInt32LE(offset), type = bytes.readUInt32LE(offset + 4);
    if (type === 0x4e4f534a) return JSON.parse(bytes.subarray(offset + 8, offset + 8 + length).toString('utf8').trim());
    offset += 8 + length;
  }
  assert.fail('GLB is missing JSON');
}

function controlledFetch() {
  const pending = [], requested = [];
  const fetch = (url, options) => new Promise((resolve, reject) => {
    requested.push({ url, options });
    pending.push({ url, resolve, reject });
  });
  return { fetch, pending, requested };
}

// A strict recording WebGL subset catches incorrect uploads, missing uniforms,
// invalid strides, and draw counts without requiring a browser or GPU.
function mockGL() {
  let nextId = 1, currentBuffer = null;
  const calls = { shaders: [], uploads: [], draws: [], depthRanges: [], deletedBuffers: [], deletedPrograms: [], uniforms: {}, uniformEvents: [] };
  const gl = {
    VERTEX_SHADER: 35633, FRAGMENT_SHADER: 35632, COMPILE_STATUS: 35713,
    LINK_STATUS: 35714, ARRAY_BUFFER: 34962, STATIC_DRAW: 35044,
    FLOAT: 5126, TRIANGLES: 4, DEPTH_TEST: 2929, LEQUAL: 515,
    BLEND: 3042, CULL_FACE: 2884,
    createShader: type => ({ id: nextId++, type }),
    shaderSource: (shader, source) => calls.shaders.push({ type: shader.type, source }),
    compileShader() {}, getShaderParameter: () => true,
    getShaderInfoLog: () => '', deleteShader() {},
    createProgram: () => ({ id: nextId++ }), attachShader() {}, linkProgram() {},
    getProgramParameter: () => true, getProgramInfoLog: () => '',
    getAttribLocation: (_, name) => ['aPosition', 'aNormal', 'aColor', 'aEmissive'].indexOf(name),
    getUniformLocation: (_, name) => name,
    createBuffer: () => ({ id: nextId++ }),
    bindBuffer: (target, buffer) => { assert.equal(target, gl.ARRAY_BUFFER); currentBuffer = buffer; },
    bufferData: (target, vertices, usage) => {
      assert.equal(target, gl.ARRAY_BUFFER); assert.equal(usage, gl.STATIC_DRAW);
      assert.ok(currentBuffer); assert.equal(vertices.length % 12, 0);
      currentBuffer.vertices = new Float32Array(vertices);
      calls.uploads.push(currentBuffer);
    },
    useProgram() {}, enable() {}, disable() {},
    depthFunc: value => assert.equal(value, gl.LEQUAL),
    uniform3fv: (name, value) => {
      assert.equal(value.length, 3);
      calls.uniforms[name] = Array.from(value);
      calls.uniformEvents.push([name, Array.from(value)]);
    },
    uniform1f: (name, value) => { assert.ok(Number.isFinite(value)); calls.uniforms[name] = value; },
    uniformMatrix4fv: (name, transpose, value) => {
      assert.equal(transpose, false); assert.equal(value.length, 16);
      assert.ok(Array.from(value).every(Number.isFinite));
      calls.uniforms[name] = Array.from(value);
    },
    enableVertexAttribArray: index => assert.ok(index >= 0 && index <= 3),
    vertexAttribPointer: (index, size, type, normalized, stride, offset) => {
      assert.equal(size, 3); assert.equal(type, gl.FLOAT); assert.equal(normalized, false);
      assert.equal(stride, 48); assert.equal(offset, index * 12);
    },
    drawArrays: (mode, start, count) => {
      assert.equal(mode, gl.TRIANGLES); assert.equal(start, 0);
      assert.ok(currentBuffer?.vertices); assert.equal(count, currentBuffer.vertices.length / 12);
      for (const name of ['uModel', 'uCamera', 'uRight', 'uUp', 'uForward', 'uAspect', 'uFlash', 'uTeamTint', 'uTeamTintAmount']) assert.ok(name in calls.uniforms, `Missing ${name}`);
      calls.draws.push({ buffer: currentBuffer, count, uniforms: structuredClone(calls.uniforms), depthRange: calls.depthRanges.at(-1) || [0, 1] });
    },
    depthRange: (min, max) => calls.depthRanges.push([min, max]),
    deleteBuffer: buffer => calls.deletedBuffers.push(buffer),
    deleteProgram: program => calls.deletedPrograms.push(program)
  };
  return { gl, calls };
}
const camera = { position: [0, 1.86, 0], right: [1, 0, 0], up: [0, 1, 0], forward: [0, 0, 1], aspect: 9 / 16 };
const actor = overrides => ({ x: 0, y: 1.86, z: 5, hp: 100, armor: 50, onGround: true, vx: 0, vz: 0, yaw: 0, walk: 1, ...overrides });
async function warm(renderer, modelPath) {
  renderer.get(modelPath);
  await until(() => renderer.cache.status().active === 0, `load ${modelPath}`);
  assert.ok(renderer.get(modelPath), modelPath);
}

async function main() {
  await test('GLB parser applies node transforms and builds finite flat-shaded parts', () => {
    const model = parseGLB(actorBytes, 'characters/player_one.glb');
    assert.equal(model.nodeCount, 6); assert.equal(model.vertexCount, 18);
    near(model.height, 2);
    assert.deepEqual(model.parts.map(p => p.key).sort(), ['armL', 'armR', 'body', 'head', 'legL', 'legR'].sort());
    assert.ok(Array.from(model.vertices).every(Number.isFinite));
    near(model.bounds.min[0], -.65); near(model.bounds.max[0], .65);
    for (let i = 0; i < model.vertices.length; i += 12) {
      near(Math.hypot(...model.vertices.subarray(i + 3, i + 6)), 1);
      near(model.vertices[i + 6], .3); near(model.vertices[i + 9], .02);
    }
    const embedded = Buffer.concat([Buffer.alloc(11), actorBytes, Buffer.alloc(7)]);
    assert.equal(parseGLB(embedded.subarray(11, 11 + actorBytes.length)).vertexCount, 18, 'honor typed-array offsets');
  });

  await test('GLB parser rejects invalid headers, chunks, bounds, indices and unsupported features', () => {
    for (const length of [0, 4, 19]) assert.throws(() => parseGLB(Buffer.alloc(length)), /header/);
    for (const [offset, value] of [[0, 0], [4, 1], [8, actorBytes.length - 1]]) {
      const bytes = Buffer.from(actorBytes); bytes.writeUInt32LE(value, offset);
      assert.throws(() => parseGLB(bytes), /header/);
    }
    const truncated = Buffer.from(actorBytes); truncated.writeUInt32LE(actorBytes.length, 12);
    assert.throws(() => parseGLB(truncated), /Truncated/);
    assert.throws(() => parseGLB(fixture({ indices: [0, 1, 3] })), /index/);
    assert.throws(() => parseGLB(fixture({ indices: [0, 1] })), /triangles/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.accessors[0].count = 30; } })), /bounds/);
    assert.throws(() => parseGLB(fixture({ mutate: (_, b) => b.writeFloatLE(NaN, 0) })), /Non-finite/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.nodes[0].children = [0]; } })), /hierarchy/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.images = [{}]; } })), /Unsupported/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.skins = [{}]; } })), /Unsupported/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.extensionsRequired = ['KHR_draco_mesh_compression']; } })), /Unsupported/);
    assert.throws(() => parseGLB(fixture({ mutate: j => { j.materials[0].alphaMode = 'BLEND'; } })), /Transparent/);
  });

  await test('authored varying vertex normals survive nonuniform and reflected node transforms', () => {
    const diagonal = Math.SQRT1_2, normals = [1, 0, 0, 0, diagonal, diagonal, diagonal, diagonal, 0];
    const indices = [2, 0, 1];
    const angle = Math.PI / 4, rotation = [0, Math.sin(angle / 2), 0, Math.cos(angle / 2)];
    for (const scale of [[2, .5, 3], [-2, .5, 3]]) {
      const model = parseGLB(fixture({ indices, normals, nodes: [{ name: 'chest_core_1', mesh: 0, translation: [3, 1, -2], rotation, scale }] }), 'characters/player_one.glb');
      assert.equal(model.vertexCount, 3);
      for (let vertex = 0; vertex < 3; vertex++) {
        const sourceIndex = indices[vertex];
        const corrected = normals.slice(sourceIndex * 3, sourceIndex * 3 + 3).map((value, axis) => value / scale[axis]);
        const expected = transform(compose([0, 0, 0], [0, angle, 0]), corrected), length = Math.hypot(...expected);
        const actual = model.vertices.subarray(vertex * 12 + 3, vertex * 12 + 6);
        near(Math.hypot(...actual), 1);
        for (let axis = 0; axis < 3; axis++) near(actual[axis], expected[axis] / length);
      }
      assert.notDeepEqual(Array.from(model.vertices.subarray(3, 6)), Array.from(model.vertices.subarray(15, 18)), 'smooth per-vertex normals cannot become one flat-face normal');
    }
    const flat = parseGLB(fixture({ nodes: [{ name: 'chest_core_1', mesh: 0 }] }));
    for (const offset of [15, 27]) assert.deepEqual(Array.from(flat.vertices.subarray(offset, offset + 3)), Array.from(flat.vertices.subarray(3, 6)), 'absent NORMAL preserves flat-face fallback');
  });

  await test('malformed authored NORMAL accessors fail closed', () => {
    const unit = [1, 0, 0, 0, 1, 0, 0, 0, 1];
    for (const mutate of [
      j => { j.accessors[2].type = 'VEC2'; },
      j => { j.accessors[2].count = 2; },
      j => { j.accessors[2].count = 4; },
      j => { j.accessors[2].sparse = {}; },
      j => { j.meshes[0].primitives[0].attributes.NORMAL = 99; }
    ]) assert.throws(() => parseGLB(fixture({ normals: unit, mutate })), /normal|accessor|bounds/i);
    for (const normals of [[0, 0, 0, 0, 1, 0, 0, 0, 1], [2, 0, 0, 0, 1, 0, 0, 0, 1], [NaN, 0, 0, 0, 1, 0, 0, 0, 1], [Infinity, 0, 0, 0, 1, 0, 0, 0, 1]]) {
      assert.throws(() => parseGLB(fixture({ normals })), /normal|Non-finite/i);
    }
    assert.throws(() => parseGLB(fixture({ normals: unit, nodes: [{ name: 'head_1', mesh: 0, scale: [1, 1, 0] }] })), /normal|singular|transform/i);
  });

  await test('detailed limb and head attachments stay with their animated component', () => {
    const expected = [
      ['arm_119', 'armL', -.5], ['arm_135', 'armR', .5],
      ['calf_muscle_89', 'legL', -.25], ['ankle_107', 'legR', .25],
      ['rear_digging_leg_171', 'legL', -.3], ['rear_digging_foot_179', 'legR', .3],
      ['rear_digging_talon_173', 'legL', -.3],
      ['jaw_38', 'head', 0], ['jaw_lip_80', 'head', 0], ['jaw_point_79', 'head', 0],
      ['acid_mouth_nozzle_187', 'head', 0], ['acid_mouth_flange_188', 'head', 0],
      ['head_burrowing_brow_170', 'head', -.2],
      ['head_eye_socket_44', 'head', -.18], ['head_upper_tooth_48', 'head', -.18],
      ['forearm_glove_finger_70', 'armL', -.5], ['foreclaw_hook_129', 'armR', .5],
      ['upper_arm_pauldron_60', 'armL', -.5], ['shoulder_scale_33', 'armR', .5],
      ['boots_sole_143', 'legL', -.25], ['lower_leg_flex_rib_115', 'legR', .25],
      ['wing_root_membrane_167', 'wingL', -.4], ['wing_tip_hook_182', 'wingR', .4],
      ['tail_vertebral_plate_153', 'tail', 0], ['tail_grasping_tip_164', 'tail', 0],
      ['chest_core_pack_hose_177', 'body', -.3], ['chest_core_radio_antenna_158', 'body', .3],
      ['pelvis_utility_pouch_27', 'body', -.2], ['neck_fold_57', 'body', 0],
      ['helmet_10', 'head', 0], ['neck_guard_11', 'head', 0],
      ['torso_1', 'body', 0], ['impact_shoulder_14', 'armL', -.5]
    ];
    const nodes = expected.map(([name, , x]) => ({ name, mesh: 0, translation: [x, 1, 0] }));
    const model = parseGLB(fixture({ nodes }), 'characters/alien_blue.glb');
    const actual = new Map(model.parts.flatMap(part => part.names.map(name => [name, part.key])));
    for (const [name, group] of expected) assert.equal(actual.get(name), group, name);
    const weaponExpected = [
      ['magazine_1', 'magazine'], ['ammo_block_2', 'magazine'], ['chamber_3', 'magazine'],
      ['barrel_4', 'barrel'], ['muzzle_5', 'barrel'], ['slide_6', 'barrel'], ['pump_7', 'barrel'],
      ['energy_core_8', 'energy'], ['power_fin_9', 'energy'], ['capacitor_10', 'energy'],
      ['receiver_11', 'body'], ['grip_12', 'body'], ['stock_13', 'body']
    ];
    const weaponModel = parseGLB(fixture({ nodes: weaponExpected.map(([name]) => ({ name, mesh: 0 })) }), 'weapons/assault.glb');
    const weaponGroups = new Map(weaponModel.parts.flatMap(part => part.names.map(name => [name, part.key])));
    for (const [name, group] of weaponExpected) assert.equal(weaponGroups.get(name), group, name);
  });

  await test('cache deduplicates URLs and limits in-flight downloads while prioritizing queued weapons', async () => {
    const network = controlledFetch();
    const cache = new ModelCache({ fetch: network.fetch, resolve: p => `https://art.test/${p === 'alias.glb' ? 'one.glb' : p}`, maxConcurrent: 2 });
    for (const p of ['one.glb', 'alias.glb', 'two.glb', 'three.glb']) assert.equal(cache.get(p), null);
    cache.get('weapons/pistol.glb', true);
    await tick();
    assert.equal(network.requested.length, 2); assert.equal(cache.status().active, 2); assert.equal(cache.status().queued, 2);
    assert.deepEqual(network.requested.map(r => r.url), ['https://art.test/one.glb', 'https://art.test/two.glb']);
    for (const request of network.requested) {
      assert.equal(request.options.mode, 'cors'); assert.equal(request.options.credentials, 'omit');
      assert.ok(request.options.signal instanceof AbortSignal);
    }
    network.pending.shift().resolve(response(actorBytes));
    await until(() => network.requested.length === 3, 'next priority load');
    assert.equal(network.requested[2].url, 'https://art.test/weapons/pistol.glb');
    assert.equal(cache.status().active, 2);
    assert.equal(cache.get('one.glb'), cache.get('alias.glb'));
    network.pending.shift().resolve(response(actorBytes));
    await until(() => network.requested.length === 4, 'last queued load');
    while (network.pending.length) network.pending.shift().resolve(response(actorBytes));
    await until(() => cache.status().active === 0, 'all queued loads');
    assert.deepEqual(cache.status(), { active: 0, queued: 0, requests: 4, parsed: 4, failed: 0 });
    for (let i = 0; i < 100; i++) assert.ok(cache.get('one.glb'));
    assert.equal(network.requested.length, 4);
  });

  await test('cache fails safely, retries after cooldown, and follows a changed asset origin', async () => {
    let clock = 1000, origin = 'https://old.test', fail = true, requestCount = 0;
    const cache = new ModelCache({ now: () => clock, resolve: p => `${origin}/${p}`, fetch: async () => { requestCount++; return response(actorBytes, fail ? 404 : 200); } });
    assert.equal(cache.get('characters/player_one.glb'), null);
    await until(() => cache.status().active === 0, 'failed request');
    assert.equal(cache.get('characters/player_one.glb'), null); assert.equal(requestCount, 1);
    assert.match([...cache.records.values()][0].error, /404/);
    fail = false; clock += 60001;
    assert.equal(cache.get('characters/player_one.glb'), null);
    await until(() => cache.status().active === 0, 'retry');
    const oldModel = cache.get('characters/player_one.glb'); assert.ok(oldModel);
    origin = 'https://new.test';
    assert.equal(cache.get('characters/player_one.glb'), null, 'old model must not mask a changed URL');
    await until(() => cache.status().active === 0, 'new origin');
    assert.notEqual(cache.get('characters/player_one.glb'), oldModel); assert.equal(requestCount, 3);
    const invalid = new ModelCache({ resolve: () => 'invalid.glb', fetch: async () => response(Buffer.from('invalid')) });
    assert.equal(invalid.get('bad'), null);
    await until(() => invalid.status().active === 0, 'parse failure');
    assert.equal(invalid.get('bad'), null); assert.equal(invalid.status().failed, 1);
    const disconnected = new ModelCache({ resolve: () => '', fetch: networkNever });
    assert.equal(disconnected.get('none'), null); assert.equal(disconnected.status().requests, 0);
  });

  await test('actor rendering shares static GPU buffers and bounds animated draw calls', async () => {
    const { gl, calls } = mockGL();
    const renderer = createRenderer(gl, { resolve: p => `https://art.test/${p}`, fetch: async () => response(actorBytes) });
    renderer.beginView(camera, 1);
    assert.equal(renderer.drawActor('characters/player_one.glb', actor(), { animate: true }), false, 'cold cache preserves fallback');
    await warm(renderer, 'characters/player_one.glb');
    const model = renderer.get('characters/player_one.glb'), live = actor({ vx: 7 });
    renderer.beginView(camera, 1);
    assert.equal(renderer.drawActor(model.path, live, { animate: true, animationBudget: 1 }), true);
    assert.equal(calls.draws.length, model.parts.length); assert.equal(calls.uploads.length, model.parts.length + 1);
    const animated = calls.draws.map(d => d.uniforms.uModel);
    assert.ok(animated.some(m => JSON.stringify(m) !== JSON.stringify(animated[0])), 'limbs have distinct poses');
    assert.equal(renderer.drawActor(model.path, live, { animate: true, animationBudget: 1 }), true);
    assert.equal(calls.draws.length, model.parts.length + 1, 'budget overflow uses one whole-model draw');
    const uploads = calls.uploads.length;
    for (let i = 0; i < 250; i++) renderer.drawActor(model.path, actor({ x: i, z: 7 }), { animate: false });
    assert.equal(calls.uploads.length, uploads, 'actor poses never allocate another mesh');
    assert.equal(renderer.stats().gpuModels, 1); assert.equal(renderer.stats().actorCount, 252);
    renderer.beginView(camera, 1.08);
    live.hp -= 5;
    renderer.drawActor(model.path, live, { animate: true, animationBudget: 1 });
    assert.equal(renderer.stats().animatedActors, 1, 'new view gets its own animation budget');
    assert.ok(calls.draws.at(-1).uniforms.uFlash > 0, 'damage flashes the existing geometry');
    renderer.dispose();
    assert.equal(calls.deletedBuffers.length, uploads); assert.equal(new Set(calls.deletedBuffers).size, uploads);
    assert.equal(calls.deletedPrograms.length, 1);
  });

  await test('perspective depth is monotonic and split views upload independent camera state', async () => {
    const { gl, calls } = mockGL();
    const renderer = createRenderer(gl, { resolve: p => p, fetch: async () => response(actorBytes) });
    await warm(renderer, 'characters/player_one.glb');
    const vertexSource = calls.shaders.find(s => s.type === gl.VERTEX_SHADER).source;
    assert.match(vertexSource, /dot\(delta,uRight\)/); assert.match(vertexSource, /dot\(delta,uUp\)/); assert.match(vertexSource, /dot\(delta,uForward\)/);
    assert.match(vertexSource, /A\*camera\.z\+B,camera\.z/);
    assert.match(vertexSource, /camera\.x\*1\.44\*uAspect,camera\.y\*1\.44/);
    const nearPlane = Number(vertexSource.match(/float near=([\d.]+)/)[1]);
    const farPlane = Number(vertexSource.match(/float far=([\d.]+)/)[1]);
    const depth = z => ((farPlane + nearPlane) / (farPlane - nearPlane) * z - 2 * farPlane * nearPlane / (farPlane - nearPlane)) / z;
    near(depth(nearPlane), -1); near(depth(farPlane), 1);
    assert.ok(depth(.1) < depth(1) && depth(1) < depth(10), 'near geometry wins depth testing');
    renderer.beginView(camera, 2);
    renderer.drawActor('characters/player_one.glb', actor(), { animate: false });
    const first = calls.draws.at(-1);
    const secondCamera = { position: [10, 3, -2], right: [0, 0, -1], up: [0, 1, 0], forward: [1, 0, 0], aspect: .75 };
    renderer.beginView(secondCamera, 2);
    renderer.drawActor('characters/player_one.glb', actor({ x: 15, z: -2 }), { animate: false });
    const second = calls.draws.at(-1);
    assert.deepEqual(first.uniforms.uCamera, camera.position);
    for (const [uniform, key] of [['uCamera', 'position'], ['uRight', 'right'], ['uUp', 'up'], ['uForward', 'forward']]) assert.deepEqual(second.uniforms[uniform], secondCamera[key]);
    assert.equal(second.uniforms.uAspect, .75); assert.equal(renderer.stats().actorCount, 1);
    assert.equal(calls.uploads.length, 7, 'split views share GPU geometry');
    renderer.dispose();
  });

  await test('weapon reload/recoil transforms separate components and restores foreground depth', async () => {
    const { gl, calls } = mockGL();
    const renderer = createRenderer(gl, { resolve: p => p, fetch: async () => response(weaponBytes) });
    await warm(renderer, 'weapons/pistol.glb');
    renderer.beginView(camera, 1.3);
    const model = renderer.get('weapons/pistol.glb');
    assert.deepEqual(model.parts.map(p => p.key), ['body', 'magazine', 'barrel', 'energy']);
    const placement = compose([.2, -.2, .6]);
    assert.equal(renderer.drawWeapon(model.path, placement, {}, false), true);
    const rest = calls.draws.slice(-4);
    assert.equal(renderer.drawWeapon(model.path, placement, { reload: .5, recoil: 1, charge: 1 }, true), true);
    const active = calls.draws.slice(-4);
    assert.deepEqual(active[0].uniforms.uModel, rest[0].uniforms.uModel, 'receiver stays fixed');
    assert.notDeepEqual(active[1].uniforms.uModel, rest[1].uniforms.uModel, 'magazine reloads');
    near(active[2].uniforms.uModel[14] - rest[2].uniforms.uModel[14], -.1, 1e-6);
    assert.notDeepEqual(active[3].uniforms.uModel, rest[3].uniforms.uModel, 'charged energy component moves');
    assert.ok(active.every(d => d.depthRange[0] === 0 && d.depthRange[1] === .12));
    assert.deepEqual(calls.depthRanges, [[0, .12], [0, 1]]);
    renderer.drawActor(model.path, actor(), { animate: false });
    assert.deepEqual(calls.draws.at(-1).depthRange, [0, 1], 'next world draw uses normal depth');
    assert.equal(calls.uploads.length, 5, 'weapon animations reuse component buffers');
    renderer.dispose();
  });

  await test('human team accents stay distinct and never carry into alien or weapon materials', async () => {
    const { gl, calls } = mockGL();
    const renderer = createRenderer(gl, { resolve: p => p, fetch: async p => response(p.startsWith('weapons/') ? weaponBytes : actorBytes) });
    for (const modelPath of ['characters/player_one.glb', 'characters/alien_blue.glb', 'weapons/pistol.glb']) await warm(renderer, modelPath);
    renderer.beginView(camera, 1);
    const cyan = [.22, .89, 1], magenta = [1, .25, .75];
    renderer.drawActor('characters/player_one.glb', actor(), { teamTint: cyan, animate: false });
    const teamOne = calls.draws.at(-1);
    renderer.drawActor('characters/player_one.glb', actor({ x: 3 }), { teamTint: magenta, animate: false });
    const teamTwo = calls.draws.at(-1);
    assert.deepEqual(teamOne.uniforms.uTeamTint, cyan); assert.equal(teamOne.uniforms.uTeamTintAmount, 1);
    assert.deepEqual(teamTwo.uniforms.uTeamTint, magenta); assert.equal(teamTwo.uniforms.uTeamTintAmount, 1);
    assert.equal(teamOne.buffer, teamTwo.buffer, 'team colours require no duplicate GPU meshes');
    renderer.drawActor('characters/alien_blue.glb', actor({ alien: true, alienScale: 1.3, r: .6 }), { teamTint: magenta, animate: false });
    assert.equal(calls.draws.at(-1).uniforms.uTeamTintAmount, 0, 'aliens preserve class colours even when tint is supplied');
    renderer.drawActor('characters/player_one.glb', actor(), { teamTint: cyan, animate: false });
    renderer.drawWeapon('weapons/pistol.glb', compose([.2, -.2, .6]), {}, true);
    for (const draw of calls.draws.slice(-4)) {
      assert.equal(draw.uniforms.uTeamTintAmount, 0, 'every weapon component resets the previous human tint');
      assert.deepEqual(draw.uniforms.uTeamTint, [1, 1, 1]);
    }
    renderer.drawActor('characters/player_one.glb', actor(), { teamTint: magenta, animate: false });
    renderer.drawActor('characters/player_one.glb', actor({ x: 2 }), { animate: false });
    assert.equal(calls.draws.at(-1).uniforms.uTeamTintAmount, 0, 'untinted human draw cannot inherit previous team state');
    assert.deepEqual(calls.draws.at(-1).uniforms.uTeamTint, [1, 1, 1]);
    const fragment = calls.shaders.find(s => s.type === gl.FRAGMENT_SHADER).source;
    assert.match(fragment, /step\(0\.001,dot\(vEmissive,vEmissive\)\)\*uTeamTintAmount/, 'team tint is restricted to authored emissive accents');
    assert.match(fragment, /mix\(vColor,uTeamTint,accent\*0\.78\)/, 'visible armour accents distinguish teams');
    assert.match(fragment, /mix\(vEmissive,uTeamTint\*length\(vEmissive\),accent\)/, 'team glow retains authored emission strength');
    renderer.dispose();
  });

  await test('shader normals remain perpendicular after nonuniform alien fitting', () => {
    const { gl, calls } = mockGL();
    const renderer = createRenderer(gl, { resolve: () => '' });
    const vertex = calls.shaders.find(s => s.type === gl.VERTEX_SHADER).source;
    // Evaluate the shader's actual column cross products, rather than assuming a
    // uniform scale. A plain mat3(uModel)*normal fails this sloped-face check.
    const terms = [...vertex.matchAll(/cross\(uModel\[(\d)\]\.xyz,uModel\[(\d)\]\.xyz\)\*aNormal\.([xyz])/g)];
    assert.equal(terms.length, 3, 'inverse-transpose normal expression');
    const model = parseGLB(actorBytes, 'characters/player_one.glb');
    const originalNormal = Array.from(model.vertices.subarray(3, 6));
    const matrix = compose([3, 2, -4], [.18, .7, -.22], [1.5, .4, .9]);
    const cross = (a, b) => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]];
    const columns = [0, 1, 2].map(c => Array.from(matrix.subarray(c * 4, c * 4 + 3)));
    const normal = [0, 0, 0];
    for (const [, left, right, axis] of terms) {
      const vector = cross(columns[Number(left)], columns[Number(right)]), weight = originalNormal['xyz'.indexOf(axis)];
      for (let j = 0; j < 3; j++) normal[j] += vector[j] * weight;
    }
    const length = Math.hypot(...normal); assert.ok(length > 0);
    for (let j = 0; j < 3; j++) normal[j] /= length;
    const points = [0, 12, 24].map(offset => transform(matrix, model.vertices.subarray(offset, offset + 3)));
    for (const point of points.slice(1)) {
      const edge = point.map((value, j) => value - points[0][j]);
      near(edge.reduce((sum, value, j) => sum + value * normal[j], 0), 0, 1e-5);
    }
    renderer.dispose();
  });

  const assetsFlag = process.argv.indexOf('--assets');
  if (assetsFlag !== -1 && !process.argv[assetsFlag + 1]) throw Error('--assets requires a directory');
  const explicitAssets = assetsFlag !== -1 ? process.argv[assetsFlag + 1] : process.env.NEON_ASSET_SOURCE;
  const assetRoot = path.resolve(explicitAssets || path.join(__dirname, '../../../../assets-source/unpacked'));
  if (explicitAssets || fs.existsSync(path.join(assetRoot, 'weapons/pistol.glb'))) {
    await test('all 35 supplied GLBs load with valid bounds, materials and expected categories', () => {
      const { WEAPONS, HUMANS, ALIENS, AUTHOR_SCALE } = globalThis.NeonModels;
      const names = [
        ...WEAPONS.map(n => `weapons/${n}.glb`),
        ...HUMANS.map(n => `characters/${n}.glb`),
        ...ALIENS.map(n => `characters/alien_${n}.glb`)
      ];
      assert.equal(names.length, 35);
      for (const name of names) {
        const model = parseGLB(fs.readFileSync(path.join(assetRoot, name)), name);
        assert.ok(model.vertexCount > 0, name); assert.ok(model.nodeCount > 0, name);
        assert.ok(model.height > 0 && Number.isFinite(model.height), name);
        assert.ok(Array.from(model.vertices).every(Number.isFinite), name);
        for (let offset = 3; offset < model.vertices.length; offset += 12) near(Math.hypot(...model.vertices.subarray(offset, offset + 3)), 1, 1e-4);
        assert.equal(model.parts.reduce((sum, p) => sum + p.vertices.length / 12, 0), model.vertexCount, name);
        for (let axis = 0; axis < 3; axis++) assert.ok(model.bounds.max[axis] > model.bounds.min[axis], `${name}: bounds ${axis}`);
        if (name.startsWith('weapons/')) {
          assert.equal(model.weapon, true, name);
          assert.ok(model.parts.every(p => ['body', 'magazine', 'barrel', 'energy'].includes(p.key)), name);
        } else {
          assert.equal(model.weapon, false, name);
          assert.ok(model.parts.some(p => p.key === 'body'), name);
          assert.ok(model.parts.some(p => /^(leg|wing)/.test(p.key)), `${name}: locomotion parts`);
        }
        const boss = name.replace(/^characters\/alien_|\.glb$/g, '');
        near(model.authorScale, AUTHOR_SCALE[boss] || 1);
      }
    });
    await test('all 21 supplied characters keep detailed limb and head attachments in the same animation group', () => {
      const { HUMANS, ALIENS } = globalThis.NeonModels;
      const names = [...HUMANS.map(n => `characters/${n}.glb`), ...ALIENS.map(n => `characters/alien_${n}.glb`)];
      assert.equal(names.length, 21);
      let detailedAttachments = 0;
      for (const name of names) {
        const bytes = fs.readFileSync(path.join(assetRoot, name)), json = glbJSON(bytes), model = parseGLB(bytes, name);
        const groups = new Map(model.parts.flatMap(part => part.names.map(nodeName => [nodeName, part.key])));
        for (const node of json.nodes) {
          if (node.mesh === undefined) continue;
          const nodeName = node.name || '', base = nodeName.replace(/_\d+$/, '');
          // Pack components are named for their physical attachment, independently
          // of the runtime's classifier. Every finger/tooth/plate must follow its
          // arm, head or leg rather than remain frozen with the torso.
          let expected;
          if (/^(head(?:_|$)|snout$|jaw(?:_|$)|acid_mouth_(?:nozzle|flange)$|helmet|visor|neck_guard|luminous_eye|beak|lower_jaw|crown_horn|front_horn)/.test(base)) expected = 'head';
          else if (/^(wing(?:_|$))/.test(base)) expected = 'wing';
          else if (/^(tail(?:_|$))/.test(base)) expected = 'tail';
          else if (/^(upper_leg|lower_leg|thigh|knee|calf_muscle|ankle|boots?|foot|talon|rear_talon|rear_digging)/.test(base)) expected = 'leg';
          else if (/^(arm$|upper_arm|forearm|shoulder|elbow|glove|foreclaw)/.test(base)) expected = 'arm';
          else if (/^(chest|pelvis|abdomen|abdominal|rib|dorsal|back|neck(?:_|$)|acid_(?!mouth)|reservoir|brood|fore_chest|shield_rim|psychic_chest|camouflage)/.test(base)) expected = 'body';
          if (!expected) continue;
          if (['arm', 'leg', 'wing'].includes(expected)) {
            const centerX = node.matrix?.[12] ?? node.translation?.[0] ?? 0;
            expected += centerX < 0 ? 'L' : 'R';
          }
          assert.equal(groups.get(nodeName), expected, `${name}: ${nodeName}`);
          if (/^(arm$|calf_muscle|ankle|jaw(?:_|$)|rear_digging_leg|acid_mouth_(?:nozzle|flange))/.test(base)) detailedAttachments++;
        }
      }
      // Original packs use flatter names; detailed v2 packs contain these nodes.
      if (glbJSON(fs.readFileSync(path.join(assetRoot, 'characters/alien_blue.glb'))).meshes.some(mesh => mesh.primitives.some(p => p.attributes.NORMAL !== undefined))) {
        assert.ok(detailedAttachments > 100, 'all detailed v2 attachment families were checked');
      }
    });
    await test('all 14 alien models fit their existing gameplay head/body targets and ground surface', () => {
      // Read only the factory's numeric radius/scale registry. No gameplay code
      // executes here, and the independently checked envelopes stay unchanged.
      const arenaSource = fs.readFileSync(path.join(__dirname, '../neon-arena.js'), 'utf8');
      const registrySource = arenaSource.match(/function makeAlien\(kind,wave\)\{[^\n]*?const specs=[^\n]*?\{([\s\S]*?)\n\}\[kind\]/)?.[1];
      assert.ok(registrySource, 'makeAlien registry must be available');
      const sizeMultiplier = Number(arenaSource.match(/ALIEN_SIZE_MULT=([\d.]+)/)?.[1]);
      assert.equal(sizeMultiplier, 1.5);
      const registry = Object.fromEntries(registrySource.split('\n').flatMap(line => {
        const kind = line.match(/^\s*(\w+):\{/)?.[1];
        if (!kind) return [];
        const radius = Number(line.match(/\br:([\d.]+)/)?.[1]);
        const scale = Number(line.match(/\bscale:([\d.]+)/)?.[1]);
        assert.ok(radius > 0 && scale > 0, kind);
        return [[kind, { radius, scale }]];
      }));
      assert.equal(Object.keys(registry).length, 14);
      for (const kind of globalThis.NeonModels.ALIENS) {
        const model = parseGLB(fs.readFileSync(path.join(assetRoot, `characters/alien_${kind}.glb`)), `characters/alien_${kind}.glb`);
        assert.ok(model.anchors.head && model.anchors.core, `${kind}: authored head/core anchors`);
        for (const yaw of [0, .73, 2.8]) {
          const target = actor({ alien: true, alienKind: kind, x: 3.25, z: -6.5, yaw, faceYaw: yaw,
            alienScale: registry[kind].scale * sizeMultiplier, r: registry[kind].radius * sizeMultiplier });
          const before = structuredClone(target), fit = actorFit(model, target);
          assert.deepEqual(target, before, `${kind}: fitting cannot change gameplay state`);
          const matrix = multiply(compose([target.x, target.y - 1.86, target.z], [0, yaw, 0]), compose(fit.offset, [0, 0, 0], fit.scale));
          const head = transform(matrix, model.anchors.head);
          near(head[0], target.x); near(head[2], target.z);
          const headCenter = target.y - .45 * target.alienScale, wide = target.r * 1.10;
          assert.ok(Math.abs(head[1] - headCenter) <= wide * .62, `${kind}: visual head is inside existing head target`);
          let minY = Infinity;
          for (let i = 0; i < model.vertices.length; i += 12) minY = Math.min(minY, transform(matrix, model.vertices.subarray(i, i + 3))[1]);
          near(minY, 0, 1e-5);
          const core = transform(matrix, model.anchors.core), dx = core[0] - target.x, dz = core[2] - target.z;
          const forward = dx * Math.sin(yaw) + dz * Math.cos(yaw), side = dx * Math.cos(yaw) - dz * Math.sin(yaw);
          const scale = target.alienScale, sideRadius = wide + .08 * scale,
            forwardRadius = forward > 0 ? wide + .10 * scale : wide + .82 * scale,
            verticalRadius = wide * .88 + .06 * scale;
          const score = Math.min(...[.38, .62, .86, 1.10, 1.34, 1.58, 1.82].map(band => {
            const vertical = core[1] - (target.y - band * scale);
            return (side / sideRadius) ** 2 + (vertical / verticalRadius) ** 2 + (forward / forwardRadius) ** 2;
          }));
          assert.ok(score < 1, `${kind}: visual core fits existing swept body bands (score ${score})`);
        }
      }
    });
  } else console.log('SKIP supplied art pack (provide --assets or NEON_ASSET_SOURCE to validate all 35 GLBs)');
  console.log(`\n${passed} model/renderer checks passed.`);
}
function networkNever() { assert.fail('An empty asset URL must never be fetched'); }
main().catch(error => { console.error(error); process.exitCode = 1; });
