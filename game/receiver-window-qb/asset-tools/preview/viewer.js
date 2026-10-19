import * as THREE from 'three';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { createPhysiqueController } from './physique.js';

const $ = id => document.getElementById(id);
const status = (message, error = false) => { $('status').textContent = message; $('status').classList.toggle('loadError', error); };
const renderer = new THREE.WebGLRenderer({ canvas: $('scene'), antialias: true, alpha: true });
renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 1.7));
renderer.outputColorSpace = THREE.SRGBColorSpace;
renderer.toneMapping = THREE.ACESFilmicToneMapping;
renderer.toneMappingExposure = 1.15;
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFSoftShadowMap;
const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(33, 1, .02, 100);
camera.position.set(3.4, 2, 4.4);
const controls = new OrbitControls(camera, renderer.domElement);
controls.target.set(0, 1, 0); controls.enableDamping = true;
controls.minDistance = 1.2; controls.maxDistance = 8;
controls.maxPolarAngle = Math.PI * .51; controls.autoRotateSpeed = 1.15;
scene.add(new THREE.HemisphereLight(0xd8ebff, 0x2b3132, 2.5));
const key = new THREE.DirectionalLight(0xffe7cc, 3.4);
key.position.set(3, 5, 4); key.castShadow = true;
key.shadow.mapSize.set(1024, 1024); key.shadow.camera.left = -2; key.shadow.camera.right = 2;
key.shadow.camera.top = 3; key.shadow.camera.bottom = -2;
key.shadow.normalBias = .025; scene.add(key);
const rim = new THREE.DirectionalLight(0x91bdff, 2.9); rim.position.set(-3, 3, -2); scene.add(rim);
const fill = new THREE.DirectionalLight(0xd5f5e8, 1.2); fill.position.set(-2, 1, 4); scene.add(fill);
const floor = new THREE.Mesh(new THREE.CircleGeometry(2.25, 80), new THREE.MeshStandardMaterial({ color: 0x172834, roughness: .85, metalness: .18 }));
floor.rotation.x = -Math.PI / 2; floor.position.y = -.008; floor.receiveShadow = true; scene.add(floor);
const ring = new THREE.Mesh(new THREE.RingGeometry(1.8, 1.805, 96), new THREE.MeshBasicMaterial({ color: 0x709488, transparent: true, opacity: .35, side: THREE.DoubleSide }));
ring.rotation.x = -Math.PI / 2; ring.position.y = -.004; scene.add(ring);
const loader = new GLTFLoader();
const holder = new THREE.Group(); scene.add(holder);
const clock = new THREE.Clock();
let manifest, entries = [], current = null, currentEntry = null, mixer = null, action = null;
let loadSequence = 0, paused = false, low = false, frames = 0, frameTotal = 0, physique = null;
const skeletons = new Set();
const physicalSliders = ['shoulders', 'legs', 'reach', 'hands', 'chest', 'arms', 'weight', 'extension'];

function safePath(path) {
  const root = new URL('../', location.href), url = new URL(path, root);
  if (typeof path !== 'string' || url.origin !== root.origin || !url.pathname.startsWith(root.pathname) || !/\.glb$/i.test(url.pathname)) throw new Error('Invalid model path in manifest.');
  return url.href;
}
function height(entry) { return Number(entry.heightM ?? entry.heightMeters ?? entry.physique?.heightM ?? entry.height ?? 1.85); }
function modelPath(entry) {
  if (low) return entry.lodPath ?? entry.lod?.path ?? entry.lods?.[1]?.path ?? entry.lods?.[0]?.path ?? entry.path;
  return entry.path ?? entry.file ?? entry.glb;
}
function morphNames() {
  const result = new Set(); current?.traverse(o => Object.keys(o.morphTargetDictionary || {}).forEach(k => result.add(k))); return [...result];
}
function morphKey(kind) {
  const names = morphNames(), expressions = {
    strength: /muscularity|strength|upper.?body|muscle/i,
    shoulders: /shoulder|chest/i, legs: /leg.?power|leg.?mus|lower.?body/i,
    reach: /wingspan|reach|arm.?length/i, hands: /hand.?size|hands/i,
    chest: /chest.?size/i, arms: /arm.?thickness/i, weight: /body.?weight/i, extension: /^reach$/i,
  };
  const named = manifest?.physique?.morphTargets?.[kind] ?? manifest?.physique?.targets?.[kind];
  return names.includes(named) ? named : names.find(k => expressions[kind].test(k));
}
function applyPhysique() {
  if (!current) return;
  const h = Number($('height').value), strength = Number($('strength').value);
  const oldTime = action?.time ?? 0;
  mixer.stopAllAction(); mixer.uncacheRoot(current); action = null;
  physique.apply({ heightM: h, strength, ...Object.fromEntries(physicalSliders.map(k => [k, $(k).value])) });
  mixer = new THREE.AnimationMixer(current); playClip();
  if (action) action.time = oldTime;
  mixer.update(0);
  $('heightValue').textContent = `${h.toFixed(2)} m`;
  $('heightStat').textContent = `${Math.round(h * 100)} cm`;
  $('strengthValue').textContent = String(strength);
  for (const k of physicalSliders) $(k + 'Value').textContent = `${Math.round(Number($(k).value) * 100)}%`;
  current.updateMatrixWorld(true);
}
function applyColors() {
  current?.traverse(o => {
    if (!o.material) return;
    for (const material of Array.isArray(o.material) ? o.material : [o.material]) {
      const name = material.name.toLowerCase();
      if (/jersey|uniform.?primary/.test(name) && !/number|trim|stripe|accent/.test(name)) material.color.set($('jersey').value);
      else if (/helmet/.test(name) && !/foam|guard|visor/.test(name)) material.color.set($('helmet').value);
      else if (/accent|trim|stripe/.test(name)) material.color.set($('accent').value);
    }
  });
}
function playClip() {
  if (!mixer || !current) return;
  const clip = current.animations?.find(c => c.name === $('clip').value);
  if (action) action.fadeOut(.12);
  if (!clip) { action = null; mixer.stopAllAction(); return; }
  action = mixer.clipAction(clip); action.reset().setEffectiveTimeScale(1).setEffectiveWeight(1).fadeIn(.12).play();
}
function release(root) {
  const geometries = new Set(), materials = new Set(), textures = new Set();
  root.traverse(o => { if (o.geometry) geometries.add(o.geometry); if (o.skeleton) skeletons.add(o.skeleton); for (const m of Array.isArray(o.material) ? o.material : [o.material]) if (m) { materials.add(m); for (const v of Object.values(m)) if (v?.isTexture) textures.add(v); } });
  for (const s of skeletons) s.dispose(); skeletons.clear();
  for (const g of geometries) g.dispose(); for (const m of materials) m.dispose(); for (const t of textures) t.dispose();
}
async function loadModel(reset = true) {
  const sequence = ++loadSequence;
  $('quality').disabled = true; $('clip').disabled = true;
  status('Loading exported GLB…');
  try {
    const entry = entries.find(e => e.id === $('model').value) ?? entries[0];
    const gltf = await loader.loadAsync(safePath(modelPath(entry)));
    if (sequence !== loadSequence) { release(gltf.scene); return; }
    if (current) { mixer.stopAllAction(); mixer.uncacheRoot(current); holder.remove(current); release(current); }
    current = gltf.scene; current.animations = gltf.animations; currentEntry = entry;
    current.traverse(o => { if (o.isMesh) { o.castShadow = true; o.receiveShadow = false; o.frustumCulled = false; } });
    holder.add(current); mixer = new THREE.AnimationMixer(current); action = null;
    physique = createPhysiqueController(current, gltf.animations, entry);
    if (reset) {
      $('height').value = String(height(entry)); $('strength').value = '60';
      for (const k of physicalSliders) $(k).value = '0';
    }
    for (const k of ['strength', ...physicalSliders]) $(k).disabled = !morphKey(k);
    const previousClip = $('clip').value;
    $('clip').replaceChildren(...gltf.animations.map(c => new Option(c.name.replaceAll('_', ' '), c.name)));
    if (gltf.animations.some(c => c.name === previousClip)) $('clip').value = previousClip;
    else $('clip').value = gltf.animations.find(c => /idle|stance/i.test(c.name))?.name ?? gltf.animations[0]?.name ?? '';
    $('clip').disabled = !gltf.animations.length;
    $('name').textContent = entry.name ?? entry.label ?? entry.id.replaceAll('_', ' ');
    $('position').textContent = `${entry.position ?? entry.role ?? 'FOOTBALL'} / ${low ? 'LOD' : 'FULL DETAIL'}`;
    $('weightStat').textContent = entry.weightKg ? `${entry.weightKg} kg` : 'See manifest';
    $('clipStat').textContent = String(gltf.animations.length);
    $('description').textContent = `${morphNames().length} physique targets · ${new Set(current.getObjectsByProperty('isBone', true)).size} rig bones · Meter scale`;
    applyPhysique(); applyColors();
    status('Drag to inspect the athlete. Physique controls modify the actual exported model.');
    window.__ASSET_PREVIEW__ = { ready: true, id: entry.id, clips: gltf.animations.map(c => c.name), targets: morphNames(), scene: current, mixer, renderer, camera, controls, manifest, stats: () => ({ ...renderer.info.render, meanFrameMs: frameTotal / Math.max(1, frames), modelHeight: new THREE.Box3().setFromObject(current, true).getSize(new THREE.Vector3()).y }) };
  } catch (error) {
    status(`Could not load model: ${error.message}. Serve this folder with preview/serve.py.`, true);
    window.__ASSET_PREVIEW__ = { ready: false, error: error.message };
  } finally { if (sequence === loadSequence) $('quality').disabled = false; }
}
function resetView() { camera.position.set(3.4, 2, 4.4); controls.target.set(0, 1, 0); controls.update(); }
$('model').addEventListener('change', () => loadModel()); $('clip').addEventListener('change', playClip);
for (const k of ['height', 'strength', ...physicalSliders]) $(k).addEventListener('input', applyPhysique);
for (const k of ['jersey', 'helmet', 'accent']) $(k).addEventListener('input', applyColors);
$('pause').addEventListener('click', () => { paused = !paused; $('pause').textContent = paused ? 'Play animation' : 'Pause animation'; });
$('spin').addEventListener('click', () => { controls.autoRotate = !controls.autoRotate; $('spin').setAttribute('aria-pressed', String(controls.autoRotate)); });
$('reset').addEventListener('click', resetView);
$('quality').addEventListener('click', () => { low = !low; $('quality').textContent = low ? 'Lower detail' : 'Full detail'; loadModel(false); });
const resize = () => { const width = $('viewport').clientWidth, height = $('viewport').clientHeight; renderer.setSize(width, height, false); camera.aspect = width / Math.max(1, height); camera.updateProjectionMatrix(); };
new ResizeObserver(resize).observe($('viewport')); resize();
renderer.setAnimationLoop(() => { const dt = Math.min(.05, clock.getDelta()); if (mixer && !paused) mixer.update(dt); controls.update(); renderer.render(scene, camera); frames++; frameTotal += dt * 1000; });
try {
  const response = await fetch('../manifest.json'); if (!response.ok) throw new Error(`Manifest HTTP ${response.status}`);
  manifest = await response.json(); entries = manifest.models ?? manifest.players ?? [];
  if (!Array.isArray(entries) || !entries.length) throw new Error('Manifest has no player models.');
  entries = entries.filter(e => e.id && (e.path || e.file || e.glb));
  $('model').replaceChildren(...entries.map(e => new Option(e.name ?? e.label ?? e.id, e.id))); $('model').disabled = false;
  await loadModel();
} catch (error) { status(`${error.message}. Run preview/serve.py from the extracted pack.`, true); window.__ASSET_PREVIEW__ = { ready: false, error: error.message }; }
