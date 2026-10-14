/* Chris Runner section planner and gameplay extension; cartoon assets load before startup. */
const RUNNER_ABILITIES = Object.freeze({
  chris: { name: 'Repulse', cost: 30, cooldown: 2.5, description: 'Push nearby pursuers away', strength: 'Efficient endurance', weakness: 'Short ability reach' },
  dawit: { name: 'Dash', cost: 30, cooldown: 1.8, description: 'Burst in your movement direction', strength: 'Fastest runner', weakness: 'High sprint drain' },
  jay: { name: 'Sidestep', cost: 20, cooldown: 1, description: 'Quick dodge with a brief shield', strength: 'Excellent lateral control', weakness: 'Slower forward pace' },
  anthony: { name: 'Parkour', cost: 50, cooldown: 1.5, description: 'Vault a striped obstacle ahead', strength: 'Agile terrain traversal', weakness: 'Expensive ability' },
  kevon: { name: 'Smash', cost: 25, cooldown: 1.2, description: 'Break amber striped obstacles ahead', strength: 'Opens direct shortcuts', weakness: 'Slowest runner' }
});

const RUNNER_SECTION_KINDS = ['parkour', 'slalom', 'encounter', 'narrow', 'shortcut', 's-curve', 'zigzag', 'gates', 'split', 'gauntlet', 'sprint'];
let runnerSectionState = null;

function runnerRouteX(plan, z) {
  const points = plan.points;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1], b = points[i];
    if (z <= b.z) return a.center + (b.center - a.center) * Math.max(0, (z - a.z) / (b.z - a.z));
  }
  return plan.exit;
}

// Reserve the entire swept corridor, not only the open cell in each row.
// Two players can follow at +/-1.3 with their full radius inside this envelope.
function runnerRouteClear(plan, x, z, radiusX = 2.2, radiusZ = .7) {
  const from = Math.max(0, z - radiusZ - 1.15), to = Math.min(42, z + radiusZ + 1.15);
  const xs = [runnerRouteX(plan, from), runnerRouteX(plan, to)];
  for (const p of plan.points) if (p.z > from && p.z < to) xs.push(p.center);
  const clearance = radiusX + 1.15 + plan.halfWidth;
  return x + clearance <= Math.min(...xs) || x - clearance >= Math.max(...xs);
}

function planRunnerSection(index, random, previous = null, biome = '') {
  const entry = previous?.exit || 0;
  const recovery = index < 2 || index % 7 === 0;
  const emphasis = {crystal: 'narrow', clockwork: 'gates', neon: 'gates', carnival: 'encounter', harbor: 'slalom', volcanic: 'gauntlet'}[biome];
  const available = [...RUNNER_SECTION_KINDS, ...(emphasis ? [emphasis, emphasis] : [])].filter(k => k !== previous?.kind && (index > 12 || k !== 'gauntlet'));
  const kind = recovery ? 'recovery' : available[Math.floor(random() * available.length)];
  const centerChallenge = !recovery && kind !== 'sprint' && random() < .52;
  const side = random() < .5 ? -6 : 6;
  const step = x => x === 0 ? side : 0;
  let centers = [entry, step(entry), step(step(entry))];
  if (kind === 's-curve' || kind === 'gates') centers = [entry, 0, side];
  if (kind === 'narrow' || kind === 'split') centers = [entry, entry || side, entry || side];
  if (recovery || kind === 'sprint') centers = [entry, entry, entry];
  if (centerChallenge && centers.every(x => x === 0)) centers[2] = side;
  const plan = {kind, recovery, entry, exit: centers[2], halfWidth: 1.3, centerChallenge,
    points: [{z: 0, center: entry}, ...[9, 21, 33].map((z, i) => ({z, center: centers[i]})), {z: 42, center: centers[2]}]};
  plan.rows = plan.points.slice(1, -1).map((point, r) => {
    const blocks = [];
    for (const x of [-13.5, -9, -4.5, 0, 4.5, 9, 13.5]) {
      if ((x === 0 && !centerChallenge) || !runnerRouteClear(plan, x, point.z)) continue;
      const gapChance = .32 - Math.min(1, index / 160) * .24;
      if (recovery || kind === 'sprint' || (x !== 0 && !['narrow', 'gauntlet', 'gates'].includes(kind) && random() < gapChance)) continue;
      blocks.push({x, z: point.z, breakable: ['shortcut', 'parkour', 'split'].includes(kind) || random() < .35,
        shape: (r + Math.floor(random() * 4)) % 4});
    }
    return {...point, low: point.center - plan.halfWidth, high: point.center + plan.halfWidth, blocks};
  });
  if (['shortcut', 'parkour', 'split', 'gauntlet'].includes(kind)) {
    plan.rewardX = (plan.rows[1].center >= 0 ? -1 : 1) * 13.5;
  }
  return plan;
}

const RUNNER_NEW_REALMS = Object.freeze({
  carnival: {label: 'Haunted Carnival', groundColor: 0x292437, roadColor: 0x30232f, shoulderColor: 0x584058, skyColor: 0x150e28, fogColor: 0x382842, hemiSky: 0xd7a9e9, hemiGround: 0x342334, sunColor: 0xffb18e, sunIntensity: 1.1, obstacle: 0xa84466, accent: 0xffc568, event: 'SWINGING BARRIERS · Follow the cyan corridor', enemies: ['ambusher', 'jumper', 'elite']},
  volcanic: {label: 'Volcanic Rift', groundColor: 0x332624, roadColor: 0x252325, shoulderColor: 0x552d25, skyColor: 0x3d1419, fogColor: 0x673329, hemiSky: 0xffac76, hemiGround: 0x382426, sunColor: 0xff7a45, sunIntensity: 1.5, obstacle: 0x584445, accent: 0xff7135, event: 'ERUPTION VENTS · Leave the amber warning rings', enemies: ['heavy', 'ranged']},
  crystal: {label: 'Crystal Caverns', groundColor: 0x18334b, roadColor: 0x203747, shoulderColor: 0x325c6e, skyColor: 0x0a1d34, fogColor: 0x22465a, hemiSky: 0xa1f2ff, hemiGround: 0x242449, sunColor: 0xb5d6ff, sunIntensity: 1.3, obstacle: 0x74c7dd, accent: 0xb0faff, event: 'CRYSTAL COLLAPSE · Watch for falling shards', enemies: ['jumper', 'ambusher']},
  neon: {label: 'Neon Grid', groundColor: 0x13182e, roadColor: 0x091829, shoulderColor: 0x123d50, skyColor: 0x090e20, fogColor: 0x15263e, hemiSky: 0x87caff, hemiGround: 0x301944, sunColor: 0xf5a4ff, sunIntensity: 1.1, obstacle: 0x256a84, accent: 0x54ffe0, event: 'ENERGY GATES · Time a shortcut or take the open route', enemies: ['fast', 'blocker', 'elite']},
  harbor: {label: 'Storm Harbor', groundColor: 0x163e53, roadColor: 0x4b5055, shoulderColor: 0x2d5364, skyColor: 0x354858, fogColor: 0x637f8e, hemiSky: 0xc9e7ef, hemiGround: 0x273645, sunColor: 0xd4e8ff, sunIntensity: 1.15, obstacle: 0x9a6146, accent: 0xffdb91, event: 'SHIFTING CARGO · Wind gusts ahead', enemies: ['heavy', 'blocker', 'ranged']},
  clockwork: {label: 'Clockwork Citadel', groundColor: 0x433829, roadColor: 0x352e2b, shoulderColor: 0x7b643e, skyColor: 0x453d3b, fogColor: 0x776752, hemiSky: 0xffe5ae, hemiGround: 0x3e352e, sunColor: 0xffd17d, sunIntensity: 1.7, obstacle: 0x97764b, accent: 0xffdd82, event: 'TIMED PISTONS · Open passages pulse green', enemies: ['blocker', 'heavy', 'elite']}
});
const RUNNER_REALMS = ['forest', 'city', 'rooftops', 'construction', 'dream', 'industrial', 'tunnel', 'ruins', 'snow', 'desert', ...Object.keys(RUNNER_NEW_REALMS)];
const RUNNER_INTRO_REALMS = RUNNER_REALMS.slice(0, 5);
let runnerRealmSeed = 1, runnerRealmRoute = RUNNER_INTRO_REALMS.slice(), runnerRealmSituations = [], runnerWorldTime = 0, runnerNoticeChunk = -1, runnerUpcomingNotice = -1;

function resetRunnerEvolution(seed = runnerRealmSeed) {
  runnerRealmSeed = seed >>> 0;
  runnerRealmRoute = RUNNER_INTRO_REALMS.slice();
  runnerSectionState = null;
  runnerRealmSituations = [];
  runnerWorldTime = 0;
  runnerNoticeChunk = -1;
  runnerUpcomingNotice = -1;
}

function runnerRealmForIndex(index) {
  index = Math.max(0, index);
  while (runnerRealmRoute.length <= index) {
    const n = runnerRealmRoute.length;
    let state = (runnerRealmSeed ^ Math.imul(n + 1, 0x9e3779b1)) >>> 0;
    state = Math.imul(state ^ state >>> 16, 0x85ebca6b) >>> 0;
    const recent = runnerRealmRoute.slice(-4);
    const pool = RUNNER_REALMS.filter(name => n < RUNNER_REALMS.length ? !runnerRealmRoute.includes(name) : !recent.includes(name));
    // A realm becomes more likely the longer it has been absent. Pure indexed
    // randomness makes look-ahead, sky queries and replay seeking order-independent.
    const weights = pool.map(name => 1 + Math.min(24, n - runnerRealmRoute.lastIndexOf(name)) / 6 + (name === 'dream' ? .5 : 0));
    let roll = (state / 4294967296) * weights.reduce((a, b) => a + b, 0);
    let selected = pool.at(-1);
    for (let i = 0; i < pool.length; i++) { roll -= weights[i]; if (roll < 0) {selected = pool[i]; break;} }
    runnerRealmRoute.push(selected);
  }
  return runnerRealmRoute[index];
}

function addRunnerRealmScenery(chunk, zStart, biome) {
  if (window.RunnerGraphics?.ready) return window.RunnerWorldArt.realmScenery(chunk,zStart,biome);
  const cfg = RUNNER_NEW_REALMS[biome];
  if (!cfg) return;
  const group = new THREE.Group(); chunk.add(group);
  const material = new THREE.MeshStandardMaterial({color: cfg.accent, emissive: cfg.accent, emissiveIntensity: .5, roughness: .3, metalness: .25});
  const add = (geometry, x, y, z) => {const mesh = new THREE.Mesh(geometry, material); mesh.position.set(x, y, z); group.add(mesh); return mesh;};
  for (const side of [-1, 1]) for (let i = 0; i < 3; i++) {
    const x = side * 26, z = zStart + 5 + i * 14;
    if (biome === 'carnival') {
      runnerBox(group, [7, 4, 6], [x, 2, z], i % 2 ? 0x64334d : 0x32485f);
      const roof = add(new THREE.ConeGeometry(5, 3, 4), x, 5.5, z); roof.rotation.y = Math.PI / 4;
      for (const dx of [-2, 0, 2]) runnerBox(group, [.3, .3, .3], [x + dx, 3.8, z - 3.1], cfg.accent, true);
      if (i === 1 && Math.floor(zStart / 42) % 3 === 0) {const wheel = add(new THREE.TorusGeometry(6, .3, 5, 16), x + side * 6, 11, z); wheel.userData.spin = .13; for (let a = 0; a < 6; a++) {const spoke = runnerBox(wheel, [.2, 12, .25], [0, 0, 0], 0x8e638b); spoke.rotation.z = a * Math.PI / 6;} batchRunnerArchitecture(wheel);}
      if (i === 1 && Math.floor(zStart / 42) % 3 === 1) {add(new THREE.CylinderGeometry(4, 4, .5, 10), x + side * 5, .5, z); add(new THREE.ConeGeometry(4.5, 2.5, 8), x + side * 5, 7, z); runnerBox(group, [.4, 6, .4], [x + side * 5, 3.5, z], 0x937078);}
    } else if (biome === 'volcanic') {
      const crag = add(new THREE.ConeGeometry(5, 12 + i * 2, 6), x, 6 + i, z); crag.material = new THREE.MeshStandardMaterial({color: 0x463a3a, roughness: .95});
      runnerBox(group, [3, .08, 13], [side * 19, .06, z], cfg.accent, true).rotation.y = side * .2;
      for (let e = 0; e < 3; e++) add(new THREE.OctahedronGeometry(.15, 0), x + Math.sin(e * 3) * 4, 4 + e * 2, z + e).userData.ember = e;
    } else if (biome === 'crystal') {
      const crystal = add(new THREE.ConeGeometry(2 + i * .3, 12, 5), x, 5.5, z); crystal.rotation.z = side * .28;
      add(new THREE.ConeGeometry(1.8, 8, 5), side * 20, 4, z + 4).rotation.z = -side * .4;
      runnerBox(group, [52, 2, 4], [0, 15, z], 0x173249);
    } else if (biome === 'neon') {
      runnerBox(group, [5, 13 + i * 4, 6], [x, 6.5 + i * 2, z], 0x182744);
      runnerBox(group, [5.2, .12, 6.2], [x, 10 + i * 3, z], cfg.accent, true);
      runnerBox(group, [1.8, 7, .06], [x - side * 3.2, 7, z], i % 2 ? 0xff68df : cfg.accent, true);
      runnerBox(group, [30, .025, .08], [0, .13, z], 0x206d7a, true);
    } else if (biome === 'harbor') {
      runnerBox(group, [8, 5, 11], [x, 2.5, z], i % 2 ? 0x385c70 : 0x986447);
      for (let rib = -3; rib <= 3; rib += 1.5) runnerBox(group, [.12, 4.8, 11.1], [x + rib, 2.5, z], 0x78919a);
      runnerBox(group, [.6, 16, .6], [side * 32, 8, z], 0x86939c);
      runnerBox(group, [17, .5, .5], [side * 25, 16, z], 0xa9a18a);
      runnerBox(group, [.6, 1.2, 7], [side * 19, .6, z], 0x756c5d);
    } else {
      runnerBox(group, [5, 13, 5], [x, 6.5, z], 0x68543b);
      const gear = add(new THREE.TorusGeometry(4, .6, 5, 12), x - side * 3, 8, z); gear.userData.spin = side * .35;
      for (let tooth = 0; tooth < 8; tooth++) {const a = tooth * Math.PI / 4; const block = new THREE.Mesh(new THREE.BoxGeometry(1.3, 1.3, 1), material); block.position.set(Math.cos(a) * 4, Math.sin(a) * 4, 0); block.rotation.z = a; gear.add(block);}
      batchRunnerArchitecture(gear);
      runnerBox(group, [.5, 14, .5], [side * 20, 7, z + 3], cfg.accent, true);
    }
  }
  batchRunnerArchitecture(group);
  chunk.userData.realmScenery = group;
  if (biome === 'harbor') {
    for (const side of [-1, 1]) {
      const water = new THREE.Mesh(new THREE.PlaneGeometry(7, 42), new THREE.MeshStandardMaterial({color: 0x24566b, roughness: .18, metalness: .35}));
      water.rotation.x = -Math.PI / 2; water.position.set(side * 19.5, .03, zStart + 21); water.userData.water = true; group.add(water);
    }
    const rain = new THREE.Group(); group.add(rain);
    for (let i = 0; i < (isMobileDevice ? 12 : 24); i++) {
      const streak = runnerBox(rain, [.035, 1.8, .035], [((i * 17) % 43) - 21, 6 + i % 5, zStart + (i * 11) % 42], 0x9bc2d2, true); streak.rotation.z = -.35;
    }
    batchRunnerArchitecture(rain); rain.userData.rain = true;
  }
  // Frame a chunk-aligned transition with a recognizable, open arch.
  if (Math.floor(zStart / CHUNK_LENGTH) % 6 === 0) {
    for (const side of [-1, 1]) runnerBox(chunk, [1, 9, 1], [side * 18, 4.5, zStart + 1], cfg.accent, true);
    runnerBox(chunk, [37, .5, 1], [0, 9, zStart + 1], cfg.accent, true);
  }
}

function addRunnerRealmSituation(chunk, zStart, biome, plan) {
  const cfg = RUNNER_NEW_REALMS[biome];
  if (!cfg || plan.recovery || plan.kind === 'sprint') return;
  const block = plan.rows[1].blocks.find(b => Math.abs(b.x) > 2) || plan.rows[2].blocks.find(b => Math.abs(b.x) > 2);
  if (!block) return;
  const obstacle = chunk.children.find(o => o.userData.type === 'wall' && o.position.x === block.x && o.position.z === zStart + block.z);
  if (!obstacle) return;
  const visual = new THREE.Group(); obstacle.add(visual);
  if (biome === 'harbor') {
    const cargo = new THREE.Group(); obstacle.add(cargo);
    for (const mesh of obstacle.children.slice()) if ((mesh.isMesh || mesh.userData.runnerAsset) && !mesh.userData.isReadabilityEffect) cargo.add(mesh);
    cargo.scale.x = .82; visual.userData.cargo = cargo;
  }
  const authoredMoving={carnival:'carnival-swing-barrier',crystal:'crystal-falling-shard'}[biome];
  let moving;
  if(window.RunnerGraphics?.ready && authoredMoving){moving=new THREE.Group();const art=window.RunnerActorArt.prop(authoredMoving),entry=window.RunnerGraphics.asset('props/'+authoredMoving+'.glb');art.position.y=-entry.bounds.max[1]/2;moving.add(art);moving.position.y=3.7;visual.add(moving);}else moving=runnerBox(visual,[3.2,.35,.7],[0,3.7,0],cfg.accent,true);
  const ring = new THREE.Mesh(new THREE.RingGeometry(1.9, 2.15, 16), new THREE.MeshBasicMaterial({color: 0xffc361, side: THREE.DoubleSide, depthWrite: false}));
  ring.rotation.x = -Math.PI / 2; ring.scale.y = .65; ring.position.y = .14; obstacle.add(ring);
  if (biome === 'crystal' && !window.RunnerGraphics?.ready) {const shard = new THREE.Mesh(new THREE.ConeGeometry(.75, 2.7, 5), moving.material); moving.add(shard);}
  runnerRealmSituations.push({owner: chunk, obstacle, moving, cargo: visual.userData.cargo, ring, biome, phase: (Math.floor(zStart / 42) % 5) * .7, baseY: 3.7});
  chunk.userData.notice = cfg.event;
}

function updateRunnerWorld(dt) {
  runnerWorldTime += dt;
  const anchor = getLeaderRunner() || player;
  if (!anchor?.rig) return;
  const z = anchor.rig.position.z;
  const chunk = worldChunks.find(c => z >= c.userData.zStart && z < c.userData.zStart + CHUNK_LENGTH);
  const upcoming = worldChunks.find(c => c.userData.zStart > z && c.userData.zStart < z + 20 && c.userData.section === 'encounter');
  if (upcoming && runnerUpcomingNotice !== upcoming.userData.zStart) {
    runnerUpcomingNotice = upcoming.userData.zStart;
    showRoundToast('PURSUIT WAVE AHEAD · Choose your route', 1800);
  }
  if (chunk && chunk.userData.zStart !== runnerNoticeChunk) {
    runnerNoticeChunk = chunk.userData.zStart;
    const kind = chunk.userData.section;
    const label = kind === 'encounter' ? 'PURSUIT WAVE · Watch the approaching runners' : kind === 'gauntlet' ? 'RARE GAUNTLET · Gold route on the edge' : kind === 'sprint' || kind === 'recovery' ? 'OPEN STRETCH · Recover your energy' : chunk.userData.notice;
    if (label) showRoundToast(label, 1800);
    if (Math.floor(runnerNoticeChunk / CHUNK_LENGTH) % 6 === 0) showRoundToast(getBiomeLabel(chunk.userData.biome), 1900);
  }
  runnerRealmSituations = runnerRealmSituations.filter(s => !s.owner.userData.disposed && !s.obstacle.userData.disposed);
  for (const s of runnerRealmSituations) {
    const t = (runnerWorldTime + s.phase) % 5.4, warning = t < 2.2, active = t >= 2.2 && t < 3.4;
    s.ring.material.color.setHex(warning ? 0xffc361 : active ? 0xff6658 : 0x7fffc8);
    s.ring.scale.x = warning ? 1 + Math.sin(t * 12) * .06 : 1;
    const y = s.biome === 'crystal' ? (warning ? 6 : active ? .5 : 6) : s.baseY;
    s.moving.position.y += (y - s.moving.position.y) * (1 - Math.exp(-dt * 12));
    if (s.biome === 'carnival') s.moving.rotation.z = Math.sin(runnerWorldTime * 1.8 + s.phase) * .7;
    if (s.biome === 'volcanic') {s.moving.scale.y = active ? 9 : .3; s.moving.position.y = active ? 1.7 : .3;}
    if (s.biome === 'harbor') {s.moving.position.x = Math.sin(runnerWorldTime + s.phase) * .3; s.moving.rotation.z = Math.sin(runnerWorldTime * 2) * .08; if (s.cargo) s.cargo.position.x = s.moving.position.x;}
    const occupied = getActiveRunners().some(p => Math.abs(p.rig.position.x - s.obstacle.position.x) < 3.5 && Math.abs(p.rig.position.z - s.obstacle.position.z) < 2.1);
    const opens = ['neon', 'clockwork'].includes(s.biome) && ((!warning && !active) || (occupied && s.obstacle.userData.runnerOpen));
    s.obstacle.userData.runnerOpen = opens;
    if (s.biome === 'clockwork') {
      s.pistonLift = (s.pistonLift || 0) + ((opens ? 5.5 : 0) - (s.pistonLift || 0)) * (1 - Math.exp(-dt * 10));
      s.obstacle.userData.runnerOpen = opens || s.pistonLift > .15;
      for (const child of s.obstacle.children) if (child !== s.ring && !child.userData.isReadabilityEffect) {
        if (child.userData.pistonBaseY === undefined) child.userData.pistonBaseY = child.position.y;
        child.position.y = child.userData.pistonBaseY + s.pistonLift;
      }
    }
    if (s.biome === 'neon') {for (const child of s.obstacle.children) if (child !== s.ring) child.visible = !opens;}
    // Hazards share the conservative obstacle envelope, which is disjoint from
    // the swept two-player route for every phase. No gust applies forced input.
    if (active && ['volcanic', 'crystal'].includes(s.biome)) for (const p of getActiveRunners()) {
      if (Math.abs(p.rig.position.x - s.obstacle.position.x) < 2.2 && Math.abs(p.rig.position.z - s.obstacle.position.z) < 1.4) p.energy = Math.max(0, p.energy - dt * 18);
    }
  }
  for (const c of worldChunks) {
    // Fully fogged distant terrain does not need to submit geometry. Collision
    // and encounters remain generated and active regardless of render detail.
    c.visible = c.userData.zStart <= z + (isMobileDevice ? 195 : 240);
    const scenery = c.userData.realmScenery;
    if (!scenery || Math.abs(c.userData.zStart - z) > (isMobileDevice ? 100 : 150)) continue;
    for (const mesh of scenery.children) {
      if (mesh.userData.spin) mesh.rotation.z += dt * mesh.userData.spin;
      if (mesh.userData.ember !== undefined) mesh.position.y = 4 + mesh.userData.ember * 2 + Math.sin(runnerWorldTime * 1.7 + mesh.userData.ember);
      if (mesh.userData.rain) {mesh.position.y = -(runnerWorldTime * 7 % 5); mesh.position.x = Math.sin(runnerWorldTime * .7) * 1.2;}
      if (mesh.userData.water) mesh.position.y = .03 + Math.sin(runnerWorldTime * 1.8 + c.userData.zStart) * .02;
    }
    if (c.userData.biome === 'harbor') scenery.rotation.z = Math.sin(runnerWorldTime * .9) * .003;
  }
}

function disposeRunnerObject(root) {
  if (!root || root.userData?.disposed) return;
  root.userData.disposed = true;
  root.removeFromParent();
  const geometries = new Set(), materials = new Set();
  root.traverse(node => {
    if (node.isInstancedMesh) node.dispose();
    if (node.geometry) geometries.add(node.geometry);
    if (node.material) for (const mat of (Array.isArray(node.material) ? node.material : [node.material])) materials.add(mat);
  });
  geometries.forEach(g => {if (!g.userData.runnerAssetShared) g.dispose();});
  materials.forEach(m => {if (!m.userData.runnerAssetShared) m.dispose();});
}

function runnerBox(group, size, position, color, glow = false) {
  const mesh = new THREE.Mesh(new THREE.BoxGeometry(...size), new THREE.MeshStandardMaterial({
    color, roughness: .78, emissive: glow ? color : 0, emissiveIntensity: glow ? .65 : 0
  }));
  mesh.position.set(...position);
  mesh.castShadow = !glow;
  mesh.receiveShadow = true;
  group.add(mesh);
  return mesh;
}

function createRunnerObstacle(biome, breakable, shape = 0) {
  if (window.RunnerGraphics?.ready) return window.RunnerWorldArt.obstacle(biome,breakable,shape);
  const group = new THREE.Group();
  const colors = { forest: 0x705236, rooftops: 0x64748b, construction: 0xb98440, industrial: 0x527375, tunnel: 0x596779, ruins: 0x897d70, city: 0x43536b, snow: 0x9aabbc, desert: 0xaa784f, dream: 0x684894 };
  const h = breakable ? 2.1 : 3.4;
  const color = RUNNER_NEW_REALMS[biome]?.obstacle || colors[biome] || colors.city;
  if (shape === 1 || ['volcanic', 'crystal'].includes(biome)) {
    for (const x of [-1.1, .8]) {
      const rock = new THREE.Mesh(new THREE.ConeGeometry(1.15, h, biome === 'crystal' ? 5 : 7), new THREE.MeshStandardMaterial({color, roughness: biome === 'crystal' ? .15 : .8, metalness: biome === 'crystal' ? .4 : .05}));
      rock.position.set(x, h / 2, 0); rock.scale.z = .5; group.add(rock);
    }
  } else if (shape === 2) {
    for (const [x, y, width, height] of [[-1.25, h / 2, 1.5, h], [.8, h * .33, 2.3, h * .66]]) runnerBox(group, [width, height, 1.2], [x, y, 0], color);
  } else if (shape === 3) {
    runnerBox(group, [4.1, h * .75, 1.2], [0, h * .375, 0], color);
    runnerBox(group, [2.8, h * .25, 1.1], [-.5, h * .875, 0], color);
  } else runnerBox(group, [4.2, h, 1.2], [0, h / 2, 0], color);
  if (breakable) {
    // Consistent amber braces advertise both Smash and Parkour opportunities.
    for (const x of [-1.3, 1.3]) {
      const brace = runnerBox(group, [.28, 2.25, 1.3], [x, 1.1, 0], 0xffbd59, true);
      brace.rotation.z = x > 0 ? -.35 : .35;
    }
  } else {
    runnerBox(group, [4.4, .24, 1.4], [0, h, 0], 0xcbd5e1);
  }
  group.userData = { type: 'wall', radiusX: 2.2, radiusZ: .7, breakable, vaultable: breakable, height: h };
  return group;
}

function addRunnerSection(chunk, zStart, biome) {
  const plan = planRunnerSection(Math.floor(zStart / CHUNK_LENGTH), gameRandom, runnerSectionState, biome);
  runnerSectionState = {exit: plan.exit, kind: plan.kind};
  chunk.userData.section = plan.kind;
  chunk.userData.plan = plan;
  for (const row of plan.rows) {
    for (const block of row.blocks) {
      const wall = createRunnerObstacle(biome, block.breakable, block.shape);
      if (plan.kind === 'parkour' && block.x === plan.rewardX) {
        wall.scale.y = .55;
        wall.userData.autoVault = true;
        wall.userData.height = 1.2;
      }
      wall.position.set(block.x, 0, zStart + row.z);
      chunk.add(wall);
      registerObstacle(wall, biome);
    }
    // Small illuminated guide marks lead into the always-open route.
    runnerBox(chunk, [.32, .035, 1.8], [row.center, .13, zStart + row.z], 0x8de4de, true);
  }
  if (plan.rewardX !== undefined) {
    const x = plan.rewardX;
    for (const z of [7, 35]) runnerBox(chunk, [2, .04, .25], [x, .14, zStart + z], 0xffbd59, true);
  }
  addRunnerRealmSituation(chunk, zStart, biome, plan);
  return plan;
}

function addRunnerArchitecture(chunk, zStart, biome) {
  if (window.RunnerGraphics?.ready) return window.RunnerWorldArt.architecture(chunk,zStart,biome);
  const urban = ['city', 'rooftops', 'construction', 'industrial', 'tunnel', 'ruins'].includes(biome);
  if (!urban) return;
  const architecture = new THREE.Group(); chunk.add(architecture);
  chunk = architecture;
  for (const side of [-1, 1]) {
    for (let i = 0; i < 3; i++) {
      const x = side * 27, z = zStart + 7 + i * 14;
      const h = biome === 'rooftops' ? 7 : rand(10, 21);
      if (biome === 'construction') {
        runnerBox(chunk, [1, 14, 1], [x, 7, z], 0xf0a849);
        runnerBox(chunk, [8, .6, 1], [x, 13, z], 0xf0a849);
        runnerBox(chunk, [.5, 6, .5], [x - side * 3, 10, z], 0x475569);
      } else if (biome === 'industrial') {
        const tank = new THREE.Mesh(new THREE.CylinderGeometry(3, 3, h, 10), new THREE.MeshStandardMaterial({color: 0x58787b, metalness: .35, roughness: .6}));
        tank.position.set(x, h / 2, z); chunk.add(tank);
        runnerBox(chunk, [7, .7, .7], [x, 6, z], 0xdda65c);
      } else if (biome === 'ruins') {
        runnerBox(chunk, [4, h * .45, 5], [x, h * .225, z], 0x817b74).rotation.z = side * .12;
        runnerBox(chunk, [2, 1, 3], [x - side * 3, .5, z + 3], 0xa39887);
      } else {
        runnerBox(chunk, [9, h, 10], [x, h / 2 - (biome === 'rooftops' ? h + .2 : 0), z], 0x28384d);
        if (biome !== 'tunnel') for (let floor = 2; floor < h; floor += 4) {
          runnerBox(chunk, [.05, 1.1, 6], [x - side * 4.53, floor - (biome === 'rooftops' ? h + .2 : 0), z], 0xffd291, true);
        }
      }
    }
    if (biome === 'rooftops') runnerBox(chunk, [.5, 1, 42], [side * 21.5, .5, zStart + 21], 0x7e91a9);
    if (biome === 'tunnel') {
      runnerBox(chunk, [1, 14, 42], [side * 23, 7, zStart + 21], 0x394553);
      runnerBox(chunk, [.1, .2, 40], [side * 22.4, 6, zStart + 21], 0x6de1db, true);
    }
  }
  if (biome === 'tunnel') for (const z of [4, 18, 32]) runnerBox(chunk, [46, .5, .65], [0, 14, zStart + z], 0x718392);
  if (biome === 'rooftops') for (const side of [-1, 1]) runnerBox(chunk, [3, 1, 4], [side * 18, .5, zStart + 20], 0x52677a);
  batchRunnerArchitecture(architecture);
}

function batchRunnerArchitecture(group) {
  // Scenery has no collision state. Merge repeated box materials into instanced
  // draws per chunk, so cleanup retains a single, unambiguous resource owner.
  const batches = new Map();
  for (const mesh of group.children.slice()) {
    if (mesh.geometry?.type !== 'BoxGeometry') continue;
    const key = `${mesh.material.color.getHex()}/${mesh.material.emissiveIntensity}`;
    if (!batches.has(key)) batches.set(key, []);
    batches.get(key).push(mesh);
  }
  for (const meshes of batches.values()) {
    if (meshes.length < 2) continue;
    const material = meshes[0].material;
    const instances = new THREE.InstancedMesh(new THREE.BoxGeometry(1, 1, 1), material, meshes.length);
    instances.castShadow = true; instances.receiveShadow = true;
    meshes.forEach((mesh, i) => {
      const {width, height, depth} = mesh.geometry.parameters;
      const scale = mesh.scale.clone().multiply(new THREE.Vector3(width, height, depth));
      instances.setMatrixAt(i, new THREE.Matrix4().compose(mesh.position, mesh.quaternion, scale));
      mesh.removeFromParent(); if (!mesh.geometry.userData.runnerShared) mesh.geometry.dispose();
      if (mesh.material !== material) mesh.material.dispose();
    });
    instances.instanceMatrix.needsUpdate = true;
    group.add(instances);
  }
}

function runnerImpact(position, color = 0xffc063) {
  if (tempEffects.length > 45) return;
  const group = new THREE.Group(); group.position.copy(position); group.position.y = 1.2;
  const material = new THREE.MeshStandardMaterial({ color, transparent: true });
  const geometry = new THREE.BoxGeometry(.32, .32, .32);
  for (let i = 0; i < (isMobileDevice ? 8 : 16); i++) {
    const bit = new THREE.Mesh(geometry, material);
    // Cosmetic RNG must not change procedural generation or replay seeds.
    const angle = i * 2.39996;
    bit.userData.velocity = new THREE.Vector3(Math.cos(angle) * 6, 3 + i % 4, Math.sin(angle) * 6);
    group.add(bit);
  }
  const dustMaterial = new THREE.MeshBasicMaterial({color: 0xd5c1a4, transparent: true, opacity: .24, depthWrite: false});
  const dustGeometry = new THREE.SphereGeometry(.8, 6, 4);
  for (let i = 0; i < 3; i++) {
    const dust = new THREE.Mesh(dustGeometry, dustMaterial);
    dust.userData.velocity = new THREE.Vector3((i - 1) * 2, 1.5, 1);
    dust.userData.dust = true; group.add(dust);
  }
  scene.add(group);
  addTempEffect(group, .65, (effect, dt) => {
    for (const bit of group.children) {
      bit.position.addScaledVector(bit.userData.velocity, dt);
      bit.userData.velocity.y -= (bit.userData.dust ? .5 : 17) * dt;
      if (bit.userData.dust) bit.scale.addScalar(dt * 2);
      bit.rotation.x += dt * 7; bit.rotation.z += dt * 4;
    }
    material.opacity = Math.max(0, effect.life / effect.maxLife);
    dustMaterial.opacity = Math.max(0, effect.life / effect.maxLife) * .24;
  });
}

function breakRunnerObstacle(wall) {
  if (!wall?.userData.breakable) return false;
  runnerImpact(wall.position);
  walls = walls.filter(w => w !== wall);
  disposeRunnerObject(wall);
  return true;
}

function sweepRunnerMove(actor, direction, length, ignored = null) {
  const start = actor.rig.position.clone(), end = start.clone();
  for (let d = .25; d <= length + .001; d += .25) {
    const next = start.clone().addScaledVector(direction, d);
    if (Math.abs(next.x) > WORLD_WIDTH / 2 - 2 || next.z < Math.max(START_Z - 4, distance - HARD_BACK_LIMIT)) break;
    const blocked = walls.some(w => w !== ignored && !w.userData.runnerOpen && Math.abs(next.x - w.position.x) < (w.userData.radiusX || 1.6) + PLAYER_RADIUS && Math.abs(next.z - w.position.z) < (w.userData.radiusZ || 1.6) + PLAYER_RADIUS);
    if (blocked) break;
    end.copy(next);
  }
  return end;
}

function useRunnerAbility(p, actor, movement) {
  const info = RUNNER_ABILITIES[p.characterKey] || RUNNER_ABILITIES.chris;
  if ((p.abilityCooldown || 0) > 0 || p.energy < info.cost || p.traversal) return false;
  const direction = movement.clone(); direction.y = 0;
  if (direction.lengthSq() < .01) direction.z = 1;
  direction.normalize();
  let destination = null, obstacle = null;
  if (p.characterKey === 'anthony' || p.characterKey === 'kevon') {
    let nearest = Infinity;
    for (const wall of walls) {
      if (wall.userData.runnerOpen) continue;
      const delta = wall.position.clone().sub(actor.rig.position); delta.y = 0;
      const along = delta.dot(direction), across = Math.abs(delta.x * direction.z - delta.z * direction.x);
      if (along > 0 && along < 6 && across < 2.4 && along < nearest && wall.userData.breakable) { obstacle = wall; nearest = along; }
    }
    if (!obstacle) { showRoundToast('Get closer to an amber striped obstacle', 900); return false; }
    // Never vault into a second obstacle or through an unmarked solid wall.
    const length = Math.ceil((nearest + 3.5) * 4) / 4;
    destination = sweepRunnerMove(actor, direction, length, obstacle);
    if (destination.distanceTo(actor.rig.position) < length - .1 || getCollidingWall(destination, PLAYER_RADIUS)) {
      showRoundToast('Landing blocked — use the open route', 900); return false;
    }
  } else if (p.characterKey !== 'chris') {
    destination = sweepRunnerMove(actor, direction, p.characterKey === 'dawit' ? 9 : 6);
    if (destination.distanceTo(actor.rig.position) < .5) return false;
  }
  p.energy -= info.cost; p.abilityCooldown = info.cooldown; p.abilityTime = .55;
  if (p.characterKey === 'kevon') breakRunnerObstacle(obstacle);
  if (destination) p.traversal = { start: actor.rig.position.clone(), end: destination, elapsed: p.characterKey === 'anthony' ? -.1 : 0, duration: p.characterKey === 'anthony' ? .42 : .22, height: p.characterKey === 'anthony' ? 3.6 : .3 };
  if (p.characterKey === 'jay') p.abilityShield = .5;
  if (p.characterKey === 'chris' || p.characterKey === 'kevon') {
    for (const enemy of getThreatActors()) {
      if (!enemy.rig || flatDistance(enemy.rig.position, actor.rig.position) > (p.characterKey === 'chris' ? 10 : 5)) continue;
      const push = enemy.rig.position.clone().sub(actor.rig.position); push.y = 0;
      if (!push.lengthSq()) push.z = -1;
      enemy.rig.position.copy(sweepRunnerMove(enemy, push.normalize(), 4));
      enemy.runnerStun = 1.5;
    }
    runnerImpact(actor.rig.position, 0x83e6ed);
  }
  recordReplayEvent('ability', { player: p.id, character: p.characterKey });
  return true;
}

function updateRunnerTraversal(p, actor, dt) {
  const traversal = p.traversal;
  if (!traversal) return false;
  traversal.elapsed += dt;
  const t = Math.max(0, Math.min(1, traversal.elapsed / traversal.duration));
  actor.rig.position.lerpVectors(traversal.start, traversal.end, t);
  actor.rig.position.y = Math.sin(t * Math.PI) * traversal.height;
  if (t === 1) { p.traversal = null; actor.rig.position.y = 0; }
  if (!p.isChaser) distance = Math.max(distance, actor.rig.position.z);
  return true;
}

function tryRunnerAutoVault(actor, oldPos, wall) {
  // Low hurdles are optional and usable by every runner without another button.
  // Anthony's ability traverses them faster and also clears taller barriers.
  if (!wall?.userData.autoVault || !actor.characterKey || actor.traversal || actor.velocity.z <= 0) return false;
  const end = oldPos.clone(); end.z = wall.position.z + 2.1;
  if (getCollidingWall(end, PLAYER_RADIUS)) return false;
  actor.traversal = { start: oldPos.clone(), end, elapsed: -.08, duration: .7, height: 2 };
  actor.rig.position.copy(oldPos);
  return true;
}

function runnerArticulatedLimb(parent, parts, name, x, y, material, width, length, endMaterial) {
  const joint = new THREE.Group(); joint.position.set(x, y, 0); parent.add(joint);
  const geometry = new THREE.CapsuleGeometry(width / 2, Math.max(.15, length - width), 3, 6);
  const upper = new THREE.Mesh(geometry, material); upper.position.y = -length / 2; upper.castShadow = true; joint.add(upper);
  const lower = new THREE.Group(); lower.position.y = -length; joint.add(lower);
  const calf = new THREE.Mesh(geometry, material); calf.position.y = -length / 2; calf.castShadow = true; lower.add(calf);
  const leg = name.endsWith('Leg');
  const end = new THREE.Mesh(new THREE.BoxGeometry(leg ? width * 1.25 : width, leg ? .22 : .25, leg ? .75 : width), endMaterial);
  end.position.set(0, -length, leg ? .2 : 0); lower.add(end);
  parts[name] = joint; parts[name + 'Lower'] = lower;
  parts[name + 'End'] = end;
  joint.userData.length = length;
  return joint;
}

function buildRunnerEnemyModel(role = 'chaser', variant = 0, realm = '', equipped = false) {
  if (window.RunnerGraphics?.ready) return window.RunnerActorArt.enemy(role,variant,realm,equipped);
  const rig = new THREE.Group(), parts = {};
  const colors = {chaser: 0xa7bbc9, fast: 0xe45a77, heavy: 0xc59c54, ambusher: 0x766084, blocker: 0x478ab2, jumper: 0x4dbf9d, ranged: 0xd38651, elite: 0xd43962};
  const cloth = new THREE.MeshStandardMaterial({color: colors[role], roughness: .75});
  const skin = new THREE.MeshStandardMaterial({color: [0xd5a280, 0x774e3c, 0x402921][variant % 3], roughness: .8});
  const dark = new THREE.MeshStandardMaterial({color: realm === 'clockwork' ? 0x433527 : 0x202b3d, roughness: .65, metalness: realm === 'clockwork' ? .4 : .1});
  const trim = new THREE.MeshStandardMaterial({color: realm === 'neon' ? 0x70fff0 : role === 'elite' ? 0xffd294 : 0xc8d6dc, emissive: realm === 'neon' || role === 'elite' ? colors[role] : 0, emissiveIntensity: .5});
  const hair = new THREE.MeshStandardMaterial({color: [0x171923, 0x583b2b, 0x9b8270][variant % 3]});
  const boxGeometry = new THREE.BoxGeometry(1, 1, 1);
  boxGeometry.userData.runnerShared = true;
  const box = (parent, size, pos, mat = cloth) => {const m = new THREE.Mesh(boxGeometry, mat); m.scale.set(...size); m.position.set(...pos); m.castShadow = true; parent.add(m); return m;};
  const upper = new THREE.Group(); upper.position.y = 2.25; rig.add(upper); parts.torso = upper;
  const broad = ['heavy', 'blocker'].includes(role), lean = ['fast', 'jumper'].includes(role);
  const width = broad ? 2.05 : lean ? 1.2 : 1.55;
  box(rig, [width * .9, .7, .82], [0, 1.9, 0], dark);
  const torso = box(upper, [width, 1.9, broad ? 1.15 : .78], [0, .9, 0]);
  const head = new THREE.Mesh(new THREE.SphereGeometry(.53, 10, 7), skin); head.position.set(0, 2.35, .06); upper.add(head); parts.head = head;
  box(head, [.15, .19, .2], [0, -.02, .49], skin);
  box(head, [.3, .045, .04], [0, -.22, .5], dark);
  for (const side of [-1, 1]) {
    box(head, [.17, .1, .04], [side * .2, .08, .49], trim);
    box(head, [.06, .07, .05], [side * .2, .07, .52], dark);
    box(head, [.22, .05, .05], [side * .2, .2, .47], hair).rotation.z = side * -.15;
    box(head, [.16, .25, .2], [side * .52, -.04, 0], skin);
  }
  const hairstyle = new THREE.Mesh(new THREE.SphereGeometry(.55, 9, 5), hair); hairstyle.position.y = .33; hairstyle.scale.set(1, variant % 2 ? .65 : .35, .95); head.add(hairstyle);
  box(upper, [.08, 1.6, .06], [0, .9, .44], dark); // jacket seam
  box(upper, [width + .06, .2, .88], [0, .02, 0], dark);
  for (const side of [-1, 1]) box(upper, [.35, .4, .1], [side * width * .3, .5, .45], dark);
  runnerArticulatedLimb(upper, parts, 'leftArm', -width * .62, 1.65, cloth, broad ? .5 : .3, .65, skin);
  runnerArticulatedLimb(upper, parts, 'rightArm', width * .62, 1.65, cloth, broad ? .5 : .3, .65, skin);
  runnerArticulatedLimb(rig, parts, 'leftLeg', -width * .25, 1.85, dark, broad ? .55 : .36, .82, trim);
  runnerArticulatedLimb(rig, parts, 'rightLeg', width * .25, 1.85, dark, broad ? .55 : .36, .82, trim);
  if (role === 'chaser') {box(upper, [1.3, 1.1, .3], [0, 1.3, -.55], dark); box(upper, [.3, 1.3, .13], [-.45, 1.1, .5], trim);}
  if (role === 'fast') {box(upper, [.9, .7, .08], [0, 1.1, .44], trim); box(head, [1.05, .15, .9], [0, .3, 0], cloth); torso.scale.z *= .8;}
  if (role === 'heavy') {
    for (const side of [-1, 1]) {box(upper, [.8, .6, 1.2], [side * 1.05, 1.65, 0], dark); box(parts[side < 0 ? 'leftLegLower' : 'rightLegLower'], [.65, .7, .5], [0, -.4, .2], cloth);}
    box(upper, [1.5, 1.4, .65], [0, 1, -.85], dark);
  }
  if (role === 'ambusher') {
    const hood = new THREE.Mesh(new THREE.SphereGeometry(.73, 9, 6), dark); hood.position.set(0, 2.4, -.2); hood.scale.z = .7; upper.add(hood);
    box(upper, [1.8, 2.1, .12], [0, .45, -.65], cloth).rotation.x = -.18;
    upper.rotation.x = .32;
  }
  if (role === 'blocker') {
    box(parts.leftArm, [1.9, 2, .22], [0, -.5, .55], dark);
    box(parts.leftArm, [1.65, .18, .25], [0, -.3, .56], trim);
    box(head, [1.15, .35, 1], [0, .4, 0], dark);
  }
  if (role === 'jumper') for (const side of [-1, 1]) {box(parts[side < 0 ? 'leftLegLower' : 'rightLegLower'], [.45, .35, .2], [0, -.1, .25], cloth); box(parts[side < 0 ? 'leftArmLower' : 'rightArmLower'], [.38, .25, .38], [0, -.5, 0], dark);}
  let handTorch = null;
  if (role === 'ranged' || role === 'elite' || equipped) {
    box(upper, [.4, 1.8, .12], [.35, 1, .5], dark).rotation.z = -.5;
    for (let i = 0; i < 3; i++) box(upper, [.25, .4, .25], [-.4 + i * .35, .4, .6], trim);
    handTorch = new THREE.Group(); parts.rightArmLower.add(handTorch); handTorch.position.y = -.65;
    box(handTorch, [.12, .65, .12], [0, 0, .05], dark);
    const flame = new THREE.Mesh(new THREE.ConeGeometry(.18, .45, 6), new THREE.MeshStandardMaterial({color: 0xffb667, emissive: 0xff7528, emissiveIntensity: 1})); flame.position.y = .5; handTorch.add(flame);
  }
  if (role === 'elite') {
    for (const side of [-1, 1]) {box(upper, [.8, .35, .9], [side * .9, 1.8, 0], trim); box(upper, [.18, 1.8, .1], [side * .55, .8, .5], trim);}
    box(upper, [1.8, 2.4, .15], [0, .2, -.6], dark).rotation.x = -.2;
    box(head, [1.1, .18, .16], [0, .08, .52], trim);
  }
  if (realm === 'clockwork') {box(head, [.18, .8, .18], [.4, .65, 0], trim); box(upper, [.65, .65, .12], [0, 1.3, .52], trim).rotation.z = .8;}
  if (realm === 'carnival') {const hat = new THREE.Mesh(new THREE.ConeGeometry(.6, 1.1, 6), cloth); hat.position.y = .9; head.add(hat);}
  if (variant % 2) box(upper, [.2, 1.6, .1], [-width * .3, .9, .48], trim);
  if (variant % 3 === 1) box(head, [1, .13, .18], [0, .07, .5], dark);
  const groups = []; rig.traverse(node => {if (node.isGroup || node === head) groups.push(node);});
  for (const group of groups) batchRunnerArchitecture(group);
  rig.scale.set(broad ? 1.07 : lean ? .94 : 1, broad ? 1.08 : 1 + (variant % 3 - 1) * .04, 1);
  rig.userData.role = role;
  return {rig, parts, handTorch};
}

function configureRunnerEnemy(enemy, z, requestedRole = null) {
  const biome = getBiomeForZ(z);
  const pools = { forest: ['chaser', 'ambusher', 'jumper'], city: ['blocker', 'fast', 'chaser'], rooftops: ['jumper', 'fast'], construction: ['heavy', 'blocker'], industrial: ['heavy', 'ranged'], tunnel: ['ambusher', 'ranged'], ruins: ['ambusher', 'jumper'] };
  const pool = RUNNER_NEW_REALMS[biome]?.enemies || pools[biome] || ['chaser', 'fast', 'blocker'];
  enemy.role = requestedRole || (z < 84 ? 'chaser' : z > 650 && gameRandom() < .06 ? 'elite' : pool[Math.floor(gameRandom() * pool.length)]);
  const colors = {chaser: 0xb6c8d6, fast: 0xe76879, heavy: 0xd69d48, ambusher: 0x8d74ba, blocker: 0x4e9ed0, jumper: 0x62cdb0, ranged: 0xdf8d57, elite: 0xff465c};
  const made = buildRunnerEnemyModel(enemy.role, Math.abs(Math.round(enemy.rig.position.x * 17 + z)) % 6, biome, enemy.hasTorch);
  made.rig.position.copy(enemy.rig.position); made.rig.rotation.copy(enemy.rig.rotation);
  const parent = enemy.rig.parent; disposeRunnerObject(enemy.rig); parent?.add(made.rig);
  enemy.rig = made.rig; enemy.parts = made.parts; enemy.handTorch = made.handTorch;
  enemy.roleClock = 0; enemy.roleCooldown = 1.5;
  if (enemy.role === 'ranged') enemy.hasTorch = true;
  const ring = new THREE.Mesh(new THREE.RingGeometry(1.4, 1.65, 16), new THREE.MeshBasicMaterial({color: colors[enemy.role], side: THREE.DoubleSide, transparent: true, opacity: .55, depthWrite: false}));
  ring.rotation.x = -Math.PI / 2; ring.position.y = .1; enemy.rig.add(ring); enemy.roleRing = ring;
  return enemy;
}

function updateRunnerEnemy(enemy, target, dt) {
  enemy.roleClock = (enemy.roleClock || 0) + dt;
  enemy.roleCooldown = Math.max(0, (enemy.roleCooldown || 0) - dt);
  const d = flatDistance(enemy.rig.position, target.rig.position);
  if (enemy.role === 'ambusher' && !enemy.activated) {
    if (d < 19) enemy.alertTime = (enemy.alertTime || 0) + dt;
    if (enemy.roleRing) enemy.roleRing.material.opacity = enemy.alertTime ? .9 : .25;
    animateRunner(enemy, dt, false, true);
    if ((enemy.alertTime || 0) > .9) enemy.activated = true;
    return true;
  }
  if (enemy.role === 'blocker' && enemy.rig.position.z > target.rig.position.z + 3 && d < 30) {
    const old = enemy.rig.position.clone();
    enemy.rig.position.x += clamp(target.rig.position.x - old.x, -dt * 5, dt * 5);
    if (collidesWithWall(enemy.rig.position, 1)) enemy.rig.position.copy(old);
    animateRunner(enemy, dt, true, true); return true;
  }
  if (enemy.role === 'heavy' && enemy.roleCooldown <= 0) {
    const wall = walls.find(w => w.userData.breakable && flatDistance(w.position, enemy.rig.position) < 4);
    if (wall) { breakRunnerObstacle(wall); enemy.roleCooldown = 2.5; }
  }
  if (enemy.role === 'jumper') {
    if (enemy.hop) {
      enemy.hop.elapsed += dt;
      const t = Math.max(0, Math.min(1, enemy.hop.elapsed / .65));
      enemy.rig.position.lerpVectors(enemy.hop.start, enemy.hop.end, t);
      enemy.rig.position.y = Math.sin(t * Math.PI) * 3.4;
      animateRunner(enemy, dt, true, true);
      if (t === 1) enemy.hop = null;
      return true;
    }
    const wall = walls.find(w => w.userData.vaultable && flatDistance(w.position, enemy.rig.position) < 4);
    if (wall && enemy.roleCooldown <= 0) {
      const direction = target.rig.position.clone().sub(enemy.rig.position); direction.y = 0; direction.normalize();
      const end = sweepRunnerMove(enemy, direction, 7, wall);
      if (end.distanceTo(enemy.rig.position) > 6.5 && !getCollidingWall(end, 1)) {
        // A short visible crouch precedes the collision-checked leap.
        enemy.hop = {start: enemy.rig.position.clone(), end, elapsed: -.18}; enemy.roleCooldown = 3; return true;
      }
    }
  }
  if (enemy.role === 'ranged' && d < 28 && d > 10) { updateEnemyTorch(enemy, dt); animateRunner(enemy, dt, false, true); return true; }
  const burst = enemy.role === 'fast' || enemy.role === 'elite';
  const phase = enemy.roleClock % 4;
  if (enemy.roleRing && burst) enemy.roleRing.material.opacity = phase < 1 ? .95 : .4;
  const multiplier = enemy.role === 'heavy' ? .7 : burst ? (phase < 1 ? .45 : phase < 2 ? 1.85 : .95) : 1;
  chaseWithPathfinding(enemy, dt, enemy.speed * multiplier, .95);
  if (enemy.role === 'elite' && enemy.roleCooldown <= 0 && d < 20) { enemy.hasTorch = true; updateEnemyTorch(enemy, dt); }
  else updateEnemyTorch(enemy, dt);
  return true;
}

function runnerTurn(actor, angle) {actor.targetHeading = angle;}

function runnerMotion(actor, dt, moving) {
  const s = actor.runnerMotion || (actor.runnerMotion = {x: actor.rig.position.x, z: actor.rig.position.z, elapsed: 0, speed: 0, phase: actor.animTime || 0, blend: 0, air: false, landing: 0, heading: actor.rig.rotation.y});
  s.elapsed += dt;
  const dx = actor.rig.position.x - s.x, dz = actor.rig.position.z - s.z;
  const speed = Math.min(32, Math.hypot(dx, dz) / Math.max(.001, s.elapsed));
  s.acceleration = (speed - s.speed) / Math.max(.001, s.elapsed);
  s.speed += (speed - s.speed) * (1 - Math.exp(-s.elapsed * 12));
  s.blend += ((moving ? Math.min(1, s.speed / 6) : 0) - s.blend) * (1 - Math.exp(-s.elapsed * 10));
  s.stepTime = s.elapsed;
  s.x = actor.rig.position.x; s.z = actor.rig.position.z; s.elapsed = 0;
  return s;
}

function animateRunnerMotion(actor, dt, moving, allowFarSkip) {
  const far = allowFarSkip && Math.abs(actor.rig.position.z - player.rig.position.z) > 65;
  if (actor.runnerFar !== far) {
    actor.runnerFar = far;
    if (!actor.rig.userData.runnerAsset) for (const child of actor.parts.head?.children || []) child.visible = !far || child.geometry?.type === 'SphereGeometry';
    if (actor.roleRing) actor.roleRing.visible = !far;
    actor.rig.traverse(mesh => {if (mesh.isMesh) mesh.castShadow = !far && mesh.material.type !== 'MeshBasicMaterial';});
  }
  const s = actor.runnerMotion;
  if (allowFarSkip && Math.abs(actor.rig.position.z - player.rig.position.z) > 85 && frameCount % 3 !== 0) {
    if (s) s.elapsed += dt;
    return;
  }
  const state = runnerMotion(actor, dt, moving), parts = actor.parts;
  const elapsed = state.stepTime;
  const blend = 1 - Math.exp(-elapsed * 14);
  const exhausted = !!actor.exhausted, heavy = actor.role === 'heavy', elite = actor.role === 'elite';
  const sprint = actor.isSprinting || state.speed > 16;
  const stride = exhausted ? 2.8 : heavy ? 4.6 : sprint ? 6.3 : 4.8;
  state.phase += elapsed * (state.speed > .2 ? state.speed / stride * Math.PI * 2 : 1.7);
  actor.animTime = state.phase;
  const phase = state.phase, amount = state.blend;
  const air = actor.traversal || actor.hop;
  if (state.air && !air) state.landing = .22;
  state.air = !!air; state.landing = Math.max(0, state.landing - elapsed);
  const preparation = air && air.elapsed < 0;
  const compression = preparation ? .3 : state.landing * 1.1;
  const lean = (exhausted ? .24 : heavy ? .08 : sprint || actor.role === 'fast' ? .22 : .1) * amount + Math.max(-.1, Math.min(.15, state.acceleration * .006));
  const targetHeading = actor.targetHeading ?? actor.rig.rotation.y;
  let turn = Math.atan2(Math.sin(targetHeading - actor.rig.rotation.y), Math.cos(targetHeading - actor.rig.rotation.y));
  actor.rig.rotation.y += turn * (1 - Math.exp(-elapsed * (heavy ? 5 : 11)));
  const pose = (node, axis, target) => {if (node) node.rotation[axis] += (target - node.rotation[axis]) * blend;};
  for (const [name, offset] of [['leftLeg', 0], ['rightLeg', Math.PI]]) {
    const limb = parts[name]; if (!limb) continue;
    const p = ((phase + offset) / (2 * Math.PI) % 1 + 1) % 1;
    const stance = p < .58, u = stance ? p / .58 : (p - .58) / .42;
    // The stance foot moves backward at ground level; only the returning foot
    // lifts. Two-bone IK bends the knee instead of rotating a rigid whole leg.
    const reach = (sprint ? .9 : exhausted ? .38 : .66) * amount;
    const footZ = stance ? reach * (1 - 2 * u) : reach * (-1 + 2 * (u * u * (3 - 2 * u)));
    const footY = .16 + (stance ? 0 : Math.sin(u * Math.PI) * (sprint ? .6 : .38) * amount);
    const length = limb.userData.length || .82;
    if (limb.userData.baseY === undefined) limb.userData.baseY = limb.position.y;
    limb.position.y = limb.userData.baseY - amount * .2 - compression;
    const vertical = Math.max(.4, limb.position.y - footY);
    const d = Math.min(length * 1.999, Math.hypot(vertical, footZ));
    const knee = Math.acos(Math.max(-1, Math.min(1, (d * d - 2 * length * length) / (2 * length * length))));
    pose(limb, 'x', air && !preparation ? -.8 + offset * .15 : -Math.atan2(footZ, vertical) - knee / 2);
    pose(parts[name + 'Lower'], 'x', air ? 1.3 : knee);
    if (parts[name + 'End']) parts[name + 'End'].rotation.x = -limb.rotation.x - (parts[name + 'Lower']?.rotation.x || 0);
  }
  for (const [name, offset] of [['leftArm', Math.PI], ['rightArm', 0]]) {
    let swing = Math.cos(phase + offset) * (sprint ? .85 : exhausted ? .3 : .6) * amount;
    if (actor.role === 'blocker' && name === 'leftArm') swing = -.25;
    if (actor.role === 'ranged' && name === 'rightArm' && actor.torchCooldown < .8) swing = -1.8;
    if (actor.role === 'ambusher' && !actor.activated) swing = -.5;
    pose(parts[name], 'x', air ? -1.1 : swing);
    pose(parts[name + 'Lower'], 'x', -(sprint ? 1.1 : .65) + Math.sin(phase + offset) * .12 * amount);
  }
  if (parts.torso) {
    const body = parts.torso;
    if (body.userData.baseY === undefined) body.userData.baseY = body.position.y;
    body.position.y = body.userData.baseY - compression;
    pose(body, 'x', actor.role === 'ambusher' && !actor.activated ? .4 : lean);
    pose(body, 'y', Math.sin(phase) * amount * (elite ? .035 : .065));
    pose(body, 'z', Math.sin(phase) * amount * .025 - Math.max(-.18, Math.min(.18, turn * .18)));
  }
  if (parts.head) pose(parts.head, 'x', -lean * .35);
  if (heavy && actor.roleRing) actor.roleRing.scale.setScalar(1 + (1 - Math.cos(phase * 2)) * .045 * amount);
  if (!air) actor.rig.position.y = Math.max(0, (1 - Math.cos(phase * 2)) * (heavy ? .045 : .025) * amount - compression * .2);
  state.mode = air ? preparation ? 'preparing' : 'airborne' : state.landing > 0 ? 'landing' : exhausted ? 'exhausted' : sprint ? 'sprinting' : state.speed > 9 ? 'running' : 'jogging';
}

function animateRunnerCanine(dog, dt, moving) {
  if (Math.abs(dog.rig.position.z - player.rig.position.z) > 85 && frameCount % 3 !== 0) {dog.animationDebt = (dog.animationDebt || 0) + dt; return;}
  dt += dog.animationDebt || 0; dog.animationDebt = 0;
  const s = runnerMotion(dog, dt, moving);
  s.phase += dt * Math.max(2, s.speed / 3.8 * 2 * Math.PI); dog.animTime = s.phase;
  const t = s.phase, amount = s.blend;
  // Diagonal pairs at a trot blend into a front/rear gallop at speed.
  const gallop = Math.max(0, Math.min(1, (s.speed - 9) / 7));
  const trotOffsets = [0, Math.PI, Math.PI, 0], gallopOffsets = [0, .45, Math.PI + .45, Math.PI];
  for (let i = 0; i < dog.legs.length; i++) {
    const phase = t + trotOffsets[i] * (1 - gallop) + gallopOffsets[i] * gallop;
    dog.legs[i].rotation.x = (Math.sin(phase) * .7 + Math.sin(phase * 2) * .15) * amount;
    dog.legs[i].position.y = (dog.legs[i].userData.runnerBaseY ?? .55) + Math.max(0, Math.cos(phase)) * .12 * amount;
  }
  if (dog.body) {dog.body.rotation.z = Math.sin(t) * .035 * amount; dog.body.scale.z = 1 + Math.cos(t) * .04 * gallop;}
  if (dog.tail) {dog.tail.rotation.z = Math.sin(t * .5) * .2; dog.tail.rotation.x = -.9 + Math.cos(t) * .12 * amount;}
  if (dog.head) dog.head.rotation.x = Math.sin(t + .3) * .06 * amount;
  dog.rig.position.y = dog.type === 'spiritDog' ? .22 + Math.sin(t * .5) * .14 : Math.max(0, Math.sin(t * 2)) * .08 * amount;
  if (dog.type === 'spiritDog') {
    dog.eyeMaterial.color.setHex(Math.sin(t * .5) > 0 ? 0xffffff : 0x83fff2);
    dog.trails.forEach((trail, i) => {trail.position.y = 1 + Math.sin(t * .4 - i) * .16; trail.scale.setScalar(.6 + Math.sin(t * .7 - i) * .1);});
  }
}

function makeRunnerSpiritDog(x, z) {
  if (window.RunnerGraphics?.ready) {
    const made=window.RunnerActorArt.canine(true);made.rig.position.set(x,0,z);scene.add(made.rig);gameRandom();
    return {type:'spiritDog',...made,speed:15.2+enemyBaseSpeedBonus,turnRate:2.65,heading:gameRandom()*Math.PI*2,animTime:gameRandom()*5,phase:gameRandom()*Math.PI*2,wanderDir:null,wanderTimer:0};
  }
  const dog = createDog(x, z); dog.type = 'spiritDog';
  dog.speed = 15.2 + enemyBaseSpeedBonus; dog.turnRate = 2.65; dog.phase = gameRandom() * Math.PI * 2;
  const bodyMaterial = new THREE.MeshStandardMaterial({color: 0x8edce8, emissive: 0x225b86, emissiveIntensity: .75, roughness: .4, opacity: 1});
  const eyeMaterial = new THREE.MeshBasicMaterial({color: 0xffffff, fog: false, toneMapped: false});
  const outlineMaterial = new THREE.MeshBasicMaterial({color: 0xbaffff, side: THREE.BackSide, fog: false, toneMapped: false, depthTest: true, depthWrite: false});
  const violet = new THREE.MeshBasicMaterial({color: 0xa183ff, side: THREE.BackSide, transparent: true, opacity: .22, fog: false, depthTest: true, depthWrite: false});
  const originals = [], oldMaterials = new Set();
  dog.rig.traverse(mesh => {if (mesh.isMesh) originals.push(mesh);});
  for (const mesh of originals) {
    oldMaterials.add(mesh.material);
    const eye = mesh.geometry.type === 'SphereGeometry'; mesh.material = eye ? eyeMaterial : bodyMaterial;
    if (eye) continue;
    const shell = new THREE.Mesh(mesh.geometry, outlineMaterial); shell.scale.setScalar(1.14); shell.userData.spiritOutline = true; mesh.add(shell);
    if (!isMobileDevice && mesh === dog.body) {const glow = new THREE.Mesh(mesh.geometry, violet); glow.scale.setScalar(1.22); mesh.add(glow);}
  }
  oldMaterials.forEach(m => m.dispose());
  dog.eyeMaterial = eyeMaterial; dog.trails = [];
  const trailMaterial = new THREE.MeshBasicMaterial({color: 0xb8a4ff, transparent: true, opacity: .18, depthWrite: false, depthTest: true, fog: false});
  const trailGeometry = new THREE.SphereGeometry(.5, 6, 4);
  for (let i = 0; i < 3; i++) {const trail = new THREE.Mesh(trailGeometry, trailMaterial); trail.position.set(0, 1, -2.1 - i * .65); dog.rig.add(trail); dog.trails.push(trail);}
  return dog;
}
