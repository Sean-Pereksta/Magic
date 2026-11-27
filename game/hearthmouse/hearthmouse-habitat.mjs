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

const installedStockpiles = new WeakSet();

function organizeStockpile(engine, I, parent) {
  const world = engine.world;
  if (installedStockpiles.has(world) || typeof world.setStockpile !== "function") return;
  const legacy = world.root.getObjectByName("visible-winter-stockpile");
  if (!legacy) return;
  world.setStockpile(0);
  installedStockpiles.add(world);

  const rack = new I.Group();
  rack.name = "nest-pantry-trays";
  rack.position.copy(legacy.position);
  rack.updateMatrix();
  rack.matrixAutoUpdate = false;
  parent.add(rack);
  const rails = [];
  for (const x of [-0.245, -0.122, 0, 0.122, 0.245]) rails.push({ x, y: 0.026, w: 0.009, h: 0.024, d: 0.215 });
  for (const z of [-0.105, 0.105]) rails.push({ z, y: 0.026, w: 0.50, h: 0.024, d: 0.009 });
  const railMesh = new I.Mesh(mergeBoxes(I, rails), new I.MeshStandardMaterial({ color: 0xa47749, roughness: 0.89 }));
  railMesh.name = "nest-pantry-wood-dividers";
  railMesh.matrixAutoUpdate = false;
  railMesh.receiveShadow = true;
  rack.add(railMesh);

  const colors = [0xd8ad50, 0xaf734c, 0x84914c, 0xcba366];
  const trays = colors.map((color, column) => {
    const pieces = Array.from({ length: 12 }, (_, index) => ({
      x: (column - 1.5) * 0.122 + (index % 3 - 1) * 0.03,
      y: 0.022,
      z: (Math.floor(index / 3) - 1.5) * 0.041,
      w: index % 3 === 0 ? 0.022 : 0.017,
      h: index % 2 === 0 ? 0.019 : 0.013,
      d: index % 3 === 0 ? 0.023 : 0.018,
      rotationY: index * 0.52,
    }));
    const geometry = mergeBoxes(I, pieces);
    const material = new I.MeshStandardMaterial({ color, roughness: 0.84 });
    const tray = new I.Mesh(geometry, material);
    tray.name = "nest-pantry-food";
    tray.matrixAutoUpdate = false;
    tray.receiveShadow = true;
    rack.add(tray);
    return { tray, verticesPerPiece: geometry.attributes.position.count / pieces.length };
  });
  world.setStockpile = value => {
    const pieces = Math.max(0, Math.min(48, Math.floor((Number(value) || 0) / 2)));
    for (let column = 0; column < trays.length; column++) {
      const { tray, verticesPerPiece } = trays[column];
      const shown = Math.floor((pieces + trays.length - 1 - column) / trays.length);
      tray.visible = shown > 0;
      tray.geometry.setDrawRange(0, shown * verticesPerPiece);
    }
  };
  world.setStockpile(engine.snapshot?.stockpile);
}

export function addNestHabitat(engine, I, parent) {
  const n = engine.world.nestCenter;
  organizeStockpile(engine, I, parent);
  const batches = new Map();
  const add = (color, stage, box) => {
    const key = `${color}:${stage}`;
    let batch = batches.get(key);
    if (!batch) batches.set(key, batch = { color, stage, boxes: [] });
    batch.boxes.push(box);
  };
  for (let i = 0; i < 28; i++) {
    const a = i * 2.39996, radius = 0.28 + (i % 4) * 0.019;
    add(i % 3 ? 0x9e8161 : 0xc7b491, 0, {
      x: n.x + Math.cos(a) * radius - 0.12, y: 0.034,
      z: n.z + Math.sin(a) * radius * 0.65 - 0.17,
      w: 0.105, h: 0.006, d: i % 3 ? 0.009 : 0.025, rotationY: -a + 0.8,
    });
  }
  add(0x8e6353, 0, { x: n.x - 0.12, y: 0.027, z: n.z - 0.17, w: 0.43, h: 0.007, d: 0.25, rotationY: -0.08 });
  // A sheltered nursery nook against the wall, facing the clear center.
  for (const side of [-1, 1]) add(0x66513e, 0, { x: n.x + 0.23 + side * 0.16, y: 0.056, z: n.z - 0.49, w: 0.023, h: 0.112, d: 0.164 });
  add(0x66513e, 0, { x: n.x + 0.23, y: 0.114, z: n.z - 0.49, w: 0.345, h: 0.014, d: 0.164 });
  add(0x403026, 0, { x: n.x + 0.23, y: 0.05, z: n.z - 0.57, w: 0.30, h: 0.10, d: 0.009 });
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
