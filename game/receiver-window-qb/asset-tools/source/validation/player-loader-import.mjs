#!/usr/bin/env node
// Exercise the prepared game-side cache/clone contract with a real exported GLB.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { clone } from 'three/addons/utils/SkeletonUtils.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(process.argv[2] || path.join(here, '../..'));
const require = createRequire(import.meta.url);
const loaderSource = path.join(root, 'source/player-assets.js');
const Assets = require(loaderSource);
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json'), 'utf8'));
globalThis.self = globalThis;
globalThis.ProgressEvent ??= class ProgressEvent { constructor(type, data) { Object.assign(this, data); } };
// Actual PNG decompression is checked separately in three-load-smoke.mjs.
globalThis.createImageBitmap = async () => ({ width: 512, height: 512, close() {} });
let reads = 0, parses = 0, template;
const prefix = 'https://asset-import.invalid/football/';
const cache = Assets.create({ baseURL: prefix,
  fetch: async () => { reads++; return { ok: true, json: async () => manifest }; },
  loadGLTF: async url => {
    parses++;
    assert.ok(url.startsWith(prefix));
    const bytes = fs.readFileSync(path.join(root, url.slice(prefix.length)));
    template = await new Promise((resolve, reject) => new GLTFLoader().parse(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength), '', resolve, reject));
    return template;
  }, clone,
});
const id = manifest.assets[0].id;
const [one, two] = await Promise.all([cache.load(id), cache.load(id)]);
assert.equal(reads, 1); assert.equal(parses, 1);
const mesh = scene => { let result; scene.traverse(o => { if (!result && o.isSkinnedMesh) result = o; }); return result; };
const [a, b, original] = [one, two, template].map(v => mesh(v.scene));
assert.ok(a && b && original);
assert.equal(a.geometry, b.geometry);
assert.notEqual(a.material, b.material);
assert.notEqual(a.skeleton, b.skeleton);
assert.notEqual(a.skeleton.boneInverses, b.skeleton.boneInverses);
assert.notEqual(a.skeleton.boneInverses[0], original.skeleton.boneInverses[0]);
const inverse = b.skeleton.boneInverses[0].elements[0], pose = b.skeleton.bones[0].rotation.x, morph = b.morphTargetInfluences[0];
const clipValue = two.animations[0].tracks[0].values[0];
a.skeleton.boneInverses[0].elements[0] += .1; a.skeleton.bones[0].rotation.x += .2; a.morphTargetInfluences[0] = 1; one.animations[0].tracks[0].values[0] += .1;
assert.equal(b.skeleton.boneInverses[0].elements[0], inverse);
assert.equal(original.skeleton.boneInverses[0].elements[0], inverse);
assert.equal(b.skeleton.bones[0].rotation.x, pose); assert.equal(b.morphTargetInfluences[0], morph);
assert.equal(two.animations[0].tracks[0].values[0], clipValue);
assert.equal(template.animations[0].tracks[0].values[0], clipValue);
let disposals = 0; original.geometry.addEventListener('dispose', () => disposals++);
one.release(); cache.dispose(); assert.equal(disposals, 0, 'An active athlete retains shared geometry after cache retirement');
two.release(); assert.equal(disposals, 1, 'The final release disposes shared geometry exactly once');
two.release(); assert.equal(disposals, 1, 'Repeated release is harmless');
const report = { passed: true, scope: 'Prepared loader with real Three.js GLB parsing, clone ownership, cache and disposal; no runtime activation or GPU claims.',
  model: id, independentBones: true, independentInverseBinds: true, independentMorphWeights: true, independentClips: true, sharedGeometry: true,
  manifestRequests: reads, modelParses: parses, sourceSHA256: crypto.createHash('sha256').update(fs.readFileSync(loaderSource)).digest('hex') };
fs.writeFileSync(path.join(root, 'validation/player-loader-import.json'), JSON.stringify(report, null, 2) + '\n');
console.log(JSON.stringify(report, null, 2));
