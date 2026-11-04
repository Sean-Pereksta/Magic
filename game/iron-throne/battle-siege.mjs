// The simulation and the view use the same world-space route landmarks.
export function provisionLadders(layout) {
  for (const ladder of layout.ladders) {
    const wall = layout.walls.find(w => w.width > w.depth && Math.abs(w.x - ladder.x) <= w.width / 2 && Math.abs(w.z - ladder.z) < 1);
    if (!wall) continue;
    const west = ladder.x < 0;
    Object.assign(ladder, {
      name: west ? 'West ladder' : 'East ladder', wallId: wall.id,
      foot: { x: ladder.x, z: ladder.z - 3.4 }, landing: { x: ladder.x, z: ladder.z },
      height: wall.height, rungs: 12, active: wall.hp > 0, held: false
    });
    delete ladder.climberId; // Old checkpoints reserved the entire lane for one soldier.
  }
}

export function ramHandle(ram, index) {
  const x = (index % 2 ? 1 : -1) * 1.05, z = (Math.floor(index / 2) - 1) * 1.45;
  const sin = Math.sin(ram.facing), cos = Math.cos(ram.facing);
  return { x: ram.x + x * cos + z * sin, z: ram.z - x * sin + z * cos };
}

export function ramApproach(wall) {
  const sideWall = wall.depth > wall.width;
  const nx = sideWall ? Math.sign(wall.x) : 0, nz = sideWall ? 0 : wall.id === 'wall-rear' ? 1 : -1;
  const edge = sideWall ? wall.width / 2 : wall.depth / 2;
  return { x: wall.x + nx * (edge + 2.8), z: wall.z + nz * (edge + 2.8), facing: Math.atan2(-nx, -nz) };
}

// A collapsed section may have a local opening with low rubble shoulders.
export function wallHeightAt(w, x, z, margin = 0) {
  if (Math.abs(x - w.x) > w.width / 2 + margin || Math.abs(z - w.z) > w.depth / 2 + margin) return 0;
  if (w.hp > 0) return w.height;
  if (!w.breach) return 0;
  const offset = w.width > w.depth ? x - w.breach.x : z - w.breach.z;
  return Math.abs(offset) < w.breach.width / 2 - margin ? 0 : 2;
}

export function battlementHeightAt(w, x, z) {
  const base = wallHeightAt(w, x, z);
  if (!base || w.hp <= 0 || w.id === 'gate') return base;
  const vertical = w.depth > w.width, length = vertical ? w.depth : w.width;
  const outer = vertical ? Math.sign(w.x) : w.id === 'wall-rear' ? 1 : -1;
  const across = vertical ? x - w.x : z - w.z, along = vertical ? z - w.z : x - w.x;
  if (Math.abs(across - outer * ((vertical ? w.width : w.depth) / 2 - .26)) > .26) return base;
  const first = -length / 2 + .7, center = first + Math.round((along - first) / 2) * 2;
  if (center >= first && center < length / 2 && Math.abs(along - center) <= .5 && (w.hp / w.maxHp > .6 || Math.round(center) % 3)) return base + 1.1;
  return base;
}
