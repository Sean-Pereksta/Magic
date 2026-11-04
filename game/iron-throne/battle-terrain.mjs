// Physics and rendering share this triangulated surface, including the diagonal of each cell.
export const TERRAIN_STEP = 2.5;
const grids = new WeakMap();
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
export function battlefieldClimate(seed) {
  let bits = 2166136261;
  for (const c of String(seed)) bits = Math.imul(bits ^ c.charCodeAt(0), 16777619) >>> 0;
  const phase = (bits & 65535) / 65536 * Math.PI * 2;
  const hour = 7 + ((bits >>> 16) % 11);
  const angle = (hour - 6) * Math.PI / 12;
  return { phase, hour, sun: [Math.cos(angle), Math.sin(angle) * .88, -.475 * Math.sin(angle)] };
}

function sample(layout, x, z) {
  const phase = layout.climate.phase, terrain = layout.terrain;
  let y = terrain === 'hills' || terrain === 'mountain' || terrain === 'pass'
    ? 2.8 * Math.sin(x / 25 + phase) + 2.3 * Math.cos(z / 23 - phase) + 1.1 * Math.sin(z / 10 + x / 35 + phase * 2)
    : 1.4 * Math.sin(z / 12 + x / 33 + phase) + .85 * Math.cos(x / 20 - phase);
  if (layout.bridges?.length) {
    // Banks slope to the river. The bridge deck meets both banks on level ground.
    const bank = clamp((Math.abs(z) - 5) / 8, 0, 1);
    const bridge = layout.bridges.some(b => Math.abs(b.x - x) < b.width / 2 + TERRAIN_STEP);
    y = bank * y + (1 - bank) * (bridge ? 0 : -.9);
  }
  if (layout.fortified) {
    const outer=layout.castle?.outer;
    const outside=outer?Math.max(outer.left-x,x-outer.right,outer.front-z,z-outer.rear)-3:Math.max(Math.abs(x)-39,3-z,z-65);
    const apron = clamp(outside / 15, 0, 1);
    y *= apron;
  }
  return y;
}

export function battleGroundHeight(layout, x, z) {
  if (!layout.climate) return layout.terrain === 'hills' ? 2.8 * Math.sin(x / 25) + 2.3 * Math.cos(z / 23) : 0;
  const width = layout.width || 160, depth = layout.depth || 160;
  const cols = Math.ceil(width / TERRAIN_STEP) + 1, rows = Math.ceil(depth / TERRAIN_STEP) + 1;
  let grid = grids.get(layout);
  if (!grid) {
    grid = new Float32Array(cols * rows);
    for (let j = 0; j < rows; j++) for (let i = 0; i < cols; i++)
      grid[j * cols + i] = sample(layout, i * TERRAIN_STEP - width / 2, j * TERRAIN_STEP - depth / 2);
    grids.set(layout, grid);
  }
  const gx = clamp((x + width / 2) / TERRAIN_STEP, 0, cols - 1);
  const gz = clamp((z + depth / 2) / TERRAIN_STEP, 0, rows - 1);
  const i = Math.min(Math.floor(gx), cols - 2), j = Math.min(Math.floor(gz), rows - 2);
  const tx = gx - i, tz = gz - j, at = j * cols + i;
  const a = grid[at], b = grid[at + 1], c = grid[at + cols], d = grid[at + cols + 1];
  return tx + tz <= 1 ? a + tx * (b - a) + tz * (c - a) : d + (1 - tx) * (c - d) + (1 - tz) * (b - d);
}
