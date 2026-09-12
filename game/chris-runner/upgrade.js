/* Chris Runner's section planner and gameplay extension. No external assets. */
const RUNNER_ABILITIES = Object.freeze({
  chris: { name: 'Repulse', cost: 30, cooldown: 2.5, description: 'Push nearby pursuers away', strength: 'Efficient endurance', weakness: 'Short ability reach' },
  dawit: { name: 'Dash', cost: 30, cooldown: 1.8, description: 'Burst in your movement direction', strength: 'Fastest runner', weakness: 'High sprint drain' },
  jay: { name: 'Sidestep', cost: 20, cooldown: 1, description: 'Quick dodge with a brief shield', strength: 'Excellent lateral control', weakness: 'Slower forward pace' },
  anthony: { name: 'Parkour', cost: 50, cooldown: 1.5, description: 'Vault a striped obstacle ahead', strength: 'Agile terrain traversal', weakness: 'Expensive ability' },
  kevon: { name: 'Smash', cost: 25, cooldown: 1.2, description: 'Break amber striped obstacles ahead', strength: 'Opens direct shortcuts', weakness: 'Slowest runner' }
});

// Every section starts and ends on the same clear center approach. Intermediate
// corridors reserve swept player clearance, including neighboring obstacle rows.
function planRunnerSection(index, random) {
  const names = ['parkour', 'slalom', 'encounter', 'narrow', 'shortcut', 'recovery'];
  const kind = index < 2 ? 'recovery' : names[Math.floor(random() * names.length)];
  const bend = random() < .5 ? -4.5 : 4.5;
  const centers = kind === 'slalom' ? [0, bend, 0] : [0, 0, 0];
  const rows = [10, 21, 32].map((z, r) => {
    const low = centers[r] - 1.2;
    const high = centers[r] + 1.2;
    const blocks = [];
    for (const x of [-13.5, -9, -4.5, 0, 4.5, 9, 13.5]) {
      if (x + 2.4 > low && x - 2.4 < high) continue;
      const required = (x === 9 && ['parkour', 'shortcut'].includes(kind)) || (kind === 'slalom' && r === 1 && x === 0);
      if (kind === 'recovery' || (kind !== 'narrow' && !required && random() < .32)) continue;
      blocks.push({ x, z, breakable: kind === 'shortcut' || kind === 'parkour' || random() < .4 });
    }
    return { z, center: centers[r], low, high, blocks };
  });
  return { kind, rows, recovery: kind === 'recovery' };
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
  geometries.forEach(g => g.dispose());
  materials.forEach(m => m.dispose());
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

function createRunnerObstacle(biome, breakable) {
  const group = new THREE.Group();
  const colors = { forest: 0x705236, rooftops: 0x64748b, construction: 0xb98440, industrial: 0x527375, tunnel: 0x596779, ruins: 0x897d70, city: 0x43536b, snow: 0x9aabbc, desert: 0xaa784f, dream: 0x684894 };
  const h = breakable ? 2.1 : 3.4;
  runnerBox(group, [4.2, h, 1.2], [0, h / 2, 0], colors[biome] || colors.city);
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
  const plan = planRunnerSection(Math.floor(zStart / CHUNK_LENGTH), gameRandom);
  chunk.userData.section = plan.kind;
  for (const row of plan.rows) {
    for (const block of row.blocks) {
      const wall = createRunnerObstacle(biome, block.breakable);
      if (plan.kind === 'parkour' && block.x === 9) {
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
  if (plan.kind === 'shortcut' || plan.kind === 'parkour') {
    const x = 9;
    for (const z of [7, 35]) runnerBox(chunk, [2, .04, .25], [x, .14, zStart + z], 0xffbd59, true);
  }
  return plan;
}

function addRunnerArchitecture(chunk, zStart, biome) {
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
    const material = meshes[0].material;
    const instances = new THREE.InstancedMesh(new THREE.BoxGeometry(1, 1, 1), material, meshes.length);
    instances.castShadow = true; instances.receiveShadow = true;
    meshes.forEach((mesh, i) => {
      const {width, height, depth} = mesh.geometry.parameters;
      const scale = mesh.scale.clone().multiply(new THREE.Vector3(width, height, depth));
      instances.setMatrixAt(i, new THREE.Matrix4().compose(mesh.position, mesh.quaternion, scale));
      mesh.removeFromParent(); mesh.geometry.dispose();
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
    const blocked = walls.some(w => w !== ignored && Math.abs(next.x - w.position.x) < (w.userData.radiusX || 1.6) + PLAYER_RADIUS && Math.abs(next.z - w.position.z) < (w.userData.radiusZ || 1.6) + PLAYER_RADIUS);
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
  if (destination) p.traversal = { start: actor.rig.position.clone(), end: destination, elapsed: 0, duration: p.characterKey === 'anthony' ? .42 : .22, height: p.characterKey === 'anthony' ? 3.6 : .3 };
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
  const t = Math.min(1, traversal.elapsed / traversal.duration);
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
  actor.traversal = { start: oldPos.clone(), end, elapsed: 0, duration: .7, height: 2 };
  actor.rig.position.copy(oldPos);
  return true;
}

function configureRunnerEnemy(enemy, z) {
  const biome = getBiomeForZ(z);
  const pools = { forest: ['chaser', 'ambusher', 'jumper'], city: ['blocker', 'fast', 'chaser'], rooftops: ['jumper', 'fast'], construction: ['heavy', 'blocker'], industrial: ['heavy', 'ranged'], tunnel: ['ambusher', 'ranged'], ruins: ['ambusher', 'jumper'] };
  const pool = pools[biome] || ['chaser', 'fast', 'blocker'];
  enemy.role = z < 84 ? 'chaser' : z > 650 && gameRandom() < .06 ? 'elite' : pool[Math.floor(gameRandom() * pool.length)];
  const colors = {chaser: 0xb6c8d6, fast: 0xe76879, heavy: 0xd69d48, ambusher: 0x8d74ba, blocker: 0x4e9ed0, jumper: 0x62cdb0, ranged: 0xdf8d57, elite: 0xff465c};
  enemy.rig.children[0].material.color.setHex(colors[enemy.role]);
  if (enemy.role === 'heavy') enemy.rig.scale.set(1.35, 1.15, 1.35);
  if (enemy.role === 'fast') enemy.rig.scale.set(.8, .95, .8);
  enemy.roleClock = 0; enemy.roleCooldown = 1.5;
  if (enemy.role === 'blocker') runnerBox(enemy.rig, [2.5, 2.3, .25], [0, 2.3, .7], 0x397ca8);
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
      const t = Math.min(1, enemy.hop.elapsed / .65);
      enemy.rig.position.lerpVectors(enemy.hop.start, enemy.hop.end, t);
      enemy.rig.position.y = Math.sin(t * Math.PI) * 3.4;
      if (t === 1) enemy.hop = null;
      return true;
    }
    const wall = walls.find(w => w.userData.vaultable && flatDistance(w.position, enemy.rig.position) < 4);
    if (wall && enemy.roleCooldown <= 0) {
      const direction = target.rig.position.clone().sub(enemy.rig.position); direction.y = 0; direction.normalize();
      const end = sweepRunnerMove(enemy, direction, 7, wall);
      if (end.distanceTo(enemy.rig.position) > 6.5 && !getCollidingWall(end, 1)) {
        enemy.hop = {start: enemy.rig.position.clone(), end, elapsed: 0}; enemy.roleCooldown = 3; return true;
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
