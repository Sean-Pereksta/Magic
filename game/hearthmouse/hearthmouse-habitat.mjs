// Small authored assemblies become one static mesh per material, without extra
// lights, shadows, per-frame scene searches, or external model dependencies.
export function mergeBoxes(I, boxes) {
  const sample = new I.BoxGeometry(1, 1, 1);
  const Geometry = Object.getPrototypeOf(sample.constructor.prototype).constructor;
  const Attribute = sample.attributes.position.constructor;
  sample.dispose();
  const positions = [], normals = [], uvs = [];
  for (const b of boxes) {
    const geometry = new I.BoxGeometry(b.w, b.h, b.d).toNonIndexed();
    geometry.rotateY(b.rotationY ?? 0);
    geometry.translate(b.x ?? 0, b.y ?? 0, b.z ?? 0);
    positions.push(...geometry.attributes.position.array);
    normals.push(...geometry.attributes.normal.array);
    uvs.push(...geometry.attributes.uv.array);
    geometry.dispose();
  }
  const geometry = new Geometry();
  geometry.setAttribute("position", new Attribute(new Float32Array(positions), 3));
  geometry.setAttribute("normal", new Attribute(new Float32Array(normals), 3));
  geometry.setAttribute("uv", new Attribute(new Float32Array(uvs), 2));
  geometry.computeBoundingBox(); geometry.computeBoundingSphere();
  return geometry;
}

export function addNestHabitat(engine, I, parent) {
  const n = engine.world.nestCenter;
  const batches = new Map();
  const add = (color, stage, box) => {
    const key = `${color}:${stage}`;
    let batch = batches.get(key);
    if (!batch) batches.set(key, batch = { color, stage, boxes: [] });
    batch.boxes.push(box);
  };
  for (let i = 0; i < 42; i++) {
    const a = i * 2.39996, radius = 0.22 + (i % 5) * 0.039;
    add(i % 3 ? 0x9e8161 : 0xc7b491, 0, {
      x: n.x + Math.cos(a) * radius - 0.08, y: 0.035 + (i % 3) * 0.009,
      z: n.z + Math.sin(a) * radius * 0.65 - 0.06,
      w: 0.105, h: 0.006, d: i % 3 ? 0.009 : 0.025, rotationY: -a + 0.8,
    });
  }
  for (let i = 0; i < 12; i++) {
    add(0xb8a88c, 0, { x: n.x - 0.24 + (i % 4) * 0.11, y: 0.026 + (i % 3) * 0.003,
      z: n.z - 0.23 + Math.floor(i / 4) * 0.12, w: 0.09, h: 0.005, d: 0.045, rotationY: i * 0.71 });
  }
  // A tiny sheltered nursery arch behind the bedding, away from the nest exit.
  for (const side of [-1, 1]) add(0x66513e, 0, { x: n.x - 0.34 + side * 0.09, y: 0.07, z: n.z + 0.28, w: 0.025, h: 0.14, d: 0.19 });
  add(0x66513e, 0, { x: n.x - 0.34, y: 0.143, z: n.z + 0.28, w: 0.21, h: 0.018, d: 0.19 });
  add(0x403026, 0, { x: n.x - 0.34, y: 0.06, z: n.z + 0.37, w: 0.16, h: 0.12, d: 0.009 });
  for (const { color, stage, boxes } of batches.values()) {
    const material = new I.MeshStandardMaterial({ color, roughness: 0.98, emissive: 0x4b2911, emissiveIntensity: 0.12 });
    const mesh = new I.Mesh(mergeBoxes(I, boxes), material);
    mesh.name = "nest-woven-fibers-and-paper";
    mesh.userData.__nestStage = stage;
    mesh.castShadow = false; mesh.receiveShadow = true;
    mesh.updateMatrix(); mesh.matrixAutoUpdate = false;
    parent.add(mesh);
  }
}

export function carveMouseOpening(engine, I, entrance) {
  if (!["gnawed-wall", "plumbing"].includes(entrance.style)) return 0;
  const walls = [];
  engine.world.root.updateWorldMatrix(true, true);
  engine.world.root.traverse(node => {
    if (!node.isMesh || (!node.visible && !node.userData?.__hearthmouseBatchedStructure && !node.userData?.__staticBatched) || node.userData?.__hearthmouseStructureRemoved || node.userData?.mouseOpening) return;
    if (!/(?:wall|partition|baseboard)/i.test(node.name) || /mouse-|nest|decor|paper/i.test(node.name)) return;
    if (node.geometry?.type === "BoxGeometry") walls.push(node);
  });
  let cuts = 0;
  for (const node of walls) {
    const p = new I.Vector3(entrance.x, 0, entrance.z);
    node.worldToLocal(p);
    const { width: w, height: h, depth: d } = node.geometry.parameters;
    const axisX = entrance.axis === "x";
    if (axisX ? w > 0.35 || Math.abs(p.x) > w / 2 + 0.25 : d > 0.35 || Math.abs(p.z) > d / 2 + 0.25) continue;
    const center = axisX ? p.z : p.x, extent = (axisX ? d : w) / 2;
    if (Math.abs(center) + 0.17 >= extent || p.y > h / 2 || p.y + 0.205 < -h / 2) continue;
    const low = Math.max(-h / 2, p.y), high = Math.min(h / 2, p.y + 0.205);
    const left = center - 0.16, right = center + 0.16;
    const boxes = [];
    const segment = (lo, hi, bottom, top) => {
      if (hi - lo <= 0.001 || top - bottom <= 0.001) return;
      boxes.push({ x: axisX ? 0 : (lo + hi) / 2, z: axisX ? (lo + hi) / 2 : 0, y: (bottom + top) / 2,
        w: axisX ? w : hi - lo, d: axisX ? hi - lo : d, h: top - bottom });
    };
    segment(-extent, left, -h / 2, h / 2); segment(right, extent, -h / 2, h / 2);
    segment(left, right, high, h / 2); segment(left, right, -h / 2, low);
    // Keep the original mesh identity in the vision index and leave collision
    // intact: only the existing mouse transit can cross; cats cannot fit.
    node.geometry = mergeBoxes(I, boxes);
    node.userData.mouseOpening = entrance.id;
    node.frustumCulled = false;
    cuts++;
  }
  return cuts;
}

export function isFoodPositionClear(position, colliders, radius = 0.38) {
  return !colliders.some(c => c.active !== false && (c.minY ?? 0) < 0.2 && (c.maxY ?? 1) > 0.015 &&
    position.x > c.minX - radius && position.x < c.maxX + radius &&
    position.z > c.minZ - radius && position.z < c.maxZ + radius);
}

export function mergeStaticMeshes(I, nodes, parent) {
  const sample = new I.BoxGeometry(1, 1, 1);
  const Geometry = Object.getPrototypeOf(sample.constructor.prototype).constructor;
  const Attribute = sample.attributes.position.constructor;
  sample.dispose();
  parent.updateWorldMatrix(true, false);
  const inverse = parent.matrixWorld.clone().invert();
  const positions = [], normals = [], uvs = [];
  for (const node of nodes) {
    node.updateWorldMatrix(true, false);
    const geometry = node.geometry.index ? node.geometry.toNonIndexed() : node.geometry.clone();
    geometry.applyMatrix4(inverse.clone().multiply(node.matrixWorld));
    const p = geometry.attributes.position.array, n = geometry.attributes.normal.array, uv = geometry.attributes.uv.array;
    for (let i = 0; i < p.length; i++) { positions.push(p[i]); normals.push(n[i]); }
    for (let i = 0; i < uv.length; i++) uvs.push(uv[i]);
    geometry.dispose();
  }
  const merged = new Geometry();
  merged.setAttribute("position", new Attribute(new Float32Array(positions), 3));
  merged.setAttribute("normal", new Attribute(new Float32Array(normals), 3));
  merged.setAttribute("uv", new Attribute(new Float32Array(uvs), 2));
  merged.computeBoundingBox(); merged.computeBoundingSphere();
  return merged;
}
