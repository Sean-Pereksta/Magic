// Physics and rendering share this triangulated surface, including the diagonal of each cell.
export const TERRAIN_STEP = 2.5;
const grids = new WeakMap();
const aprons = new WeakMap();
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
export function needsSettlementTerrainRepair(layout) { return layout.settlementTerrainVersion !== 1 && !!settlementApron(layout); }

export function settlementApron(layout) {
  if (aprons.has(layout)) return aprons.get(layout);
  let bounds=layout.castle?.outer;
  if (!bounds) {
    // Pre-ward checkpoints stored only the physical walls or town buildings.
    const structures=(layout.walls || []).concat(layout.gate || [],(layout.obstacles || []).filter(o=>o.kind==='building'));
    if (structures.length) bounds={
      left:Math.min(...structures.map(o=>o.x-o.width/2)),right:Math.max(...structures.map(o=>o.x+o.width/2)),
      front:Math.min(...structures.map(o=>o.z-o.depth/2)),rear:Math.max(...structures.map(o=>o.z+o.depth/2))
    };
  }
  // Clear the exterior wall, ladder queues, room to turn a ram at a flank,
  // and the road leading up from the attackers' deployment zone.
  const apron=bounds ? {left:bounds.left-10,right:bounds.right+10,front:bounds.front-16,rear:bounds.rear+8} : null;
  aprons.set(layout,apron);
  return apron;
}

function outsideApron(rect, apron) {
  const halfX=rect.radius ?? rect.width/2, halfZ=rect.radius ?? rect.depth/2;
  return rect.x+halfX<=apron.left || rect.x-halfX>=apron.right || rect.z+halfZ<=apron.front || rect.z-halfZ>=apron.rear;
}

function trim(rect,apron) {
  if (outsideApron(rect,apron)) return [rect];
  if (rect.kind==='tree') return [];
  const left=rect.x-rect.width/2,right=rect.x+rect.width/2,front=rect.z-rect.depth/2,rear=rect.z+rect.depth/2;
  const pieces=[];
  function part(lo,hi,near,far,suffix) {
    if (hi<=lo || far<=near) return;
    pieces.push({...rect,...(rect.id ? {id:rect.id+suffix} : {}),x:(lo+hi)/2,z:(near+far)/2,width:hi-lo,depth:far-near});
  }
  // Keep cliffs on their original flank. Clipping never introduces a new
  // obstacle beneath soldiers already fighting in a saved battle.
  if (rect.id==='mountain-west') part(left,Math.min(right,apron.left),front,rear,'');
  else if (rect.id==='mountain-east') part(Math.max(left,apron.right),right,front,rear,'');
  else {
    part(left,Math.min(right,apron.left),front,rear,'-west');
    part(Math.max(left,apron.right),right,front,rear,'-east');
    const midLeft=Math.max(left,apron.left),midRight=Math.min(right,apron.right);
    part(midLeft,midRight,front,Math.min(rear,apron.front),'-front');
    part(midLeft,midRight,Math.max(front,apron.rear),rear,'-rear');
  }
  return pieces;
}

const landscapeObstacle=o=>['tree','rock','water'].includes(o.kind);
export function fitSettlementTerrain(layout) {
  const bounds=settlementApron(layout);
  if (!bounds) return false;
  // Physical obstacles include a 35cm walking clearance around their mesh.
  const apron={left:bounds.left-.5,right:bounds.right+.5,front:bounds.front-.5,rear:bounds.rear+.5};
  const changed=layout.obstacles.some(o=>landscapeObstacle(o) && !outsideApron(o,apron)) ||
    (layout.bridges || []).some(o=>!outsideApron(o,apron));
  if (changed) {
    layout.obstacles=layout.obstacles.flatMap(o=>landscapeObstacle(o) ? trim(o,apron) : [o]);
    layout.bridges=(layout.bridges || []).flatMap(o=>trim(o,apron));
  }
  layout.settlementTerrainVersion=1;
  grids.delete(layout);
  return changed;
}

export function validSettlementTerrain(layout) {
  if (layout.settlementTerrainVersion===undefined) return true; // Migrated after validating the saved roster.
  const apron=settlementApron(layout);
  return layout.settlementTerrainVersion===1 && !!apron && Array.isArray(layout.bridges || []) &&
    !layout.obstacles.some(o=>landscapeObstacle(o) && !outsideApron(o,apron)) &&
    !(layout.bridges || []).some(o=>!outsideApron(o,apron));
}

function plateauFactor(apron,x,z) {
  if (!apron) return 1;
  const outside=Math.max(apron.left-x,x-apron.right,apron.front-z,z-apron.rear);
  // All mesh vertices under the footprint, including its edge cells, are level.
  return clamp((outside-TERRAIN_STEP)/15,0,1);
}
export function battlefieldClimate(seed) {
  let bits = 2166136261;
  for (const c of String(seed)) bits = Math.imul(bits ^ c.charCodeAt(0), 16777619) >>> 0;
  const phase = (bits & 65535) / 65536 * Math.PI * 2;
  const hour = 7 + ((bits >>> 16) % 11);
  const angle = (hour - 6) * Math.PI / 12;
  return { phase, hour, sun: [Math.cos(angle), Math.sin(angle) * .88, -.475 * Math.sin(angle)] };
}

function sample(layout, x, z, apron, hasRiver) {
  const phase = layout.climate.phase, terrain = layout.terrain;
  let y = terrain === 'hills' || terrain === 'mountain' || terrain === 'pass'
    ? 2.8 * Math.sin(x / 25 + phase) + 2.3 * Math.cos(z / 23 - phase) + 1.1 * Math.sin(z / 10 + x / 35 + phase * 2)
    : 1.4 * Math.sin(z / 12 + x / 33 + phase) + .85 * Math.cos(x / 20 - phase);
  if (hasRiver) {
    // Banks slope to the river. The bridge deck meets both banks on level ground.
    const bank = clamp((Math.abs(z-(layout.riverZ || 0)) - 5) / 8, 0, 1);
    const bridge = layout.bridges?.some(b => Math.abs(b.x - x) < b.width / 2 + TERRAIN_STEP && (!apron || Math.abs(b.z-z)<b.depth/2+TERRAIN_STEP));
    y = bank * y + (1 - bank) * (bridge ? 0 : -.9);
  }
  return y*plateauFactor(apron,x,z);
}

export function battleGroundHeight(layout, x, z) {
  if (!layout.climate) return layout.terrain === 'hills' ? (2.8 * Math.sin(x / 25) + 2.3 * Math.cos(z / 23))*plateauFactor(settlementApron(layout),x,z) : 0;
  const width = layout.width || 160, depth = layout.depth || 160;
  const cols = Math.ceil(width / TERRAIN_STEP) + 1, rows = Math.ceil(depth / TERRAIN_STEP) + 1;
  let grid = grids.get(layout);
  if (!grid) {
    grid = new Float32Array(cols * rows);
    const apron=settlementApron(layout), hasRiver=!!layout.bridges?.length || layout.obstacles?.some(o=>o.kind==='water');
    for (let j = 0; j < rows; j++) for (let i = 0; i < cols; i++)
      grid[j * cols + i] = sample(layout, i * TERRAIN_STEP - width / 2, j * TERRAIN_STEP - depth / 2,apron,hasRiver);
    grids.set(layout, grid);
  }
  const gx = clamp((x + width / 2) / TERRAIN_STEP, 0, cols - 1);
  const gz = clamp((z + depth / 2) / TERRAIN_STEP, 0, rows - 1);
  const i = Math.min(Math.floor(gx), cols - 2), j = Math.min(Math.floor(gz), rows - 2);
  const tx = gx - i, tz = gz - j, at = j * cols + i;
  const a = grid[at], b = grid[at + 1], c = grid[at + cols], d = grid[at + cols + 1];
  return tx + tz <= 1 ? a + tx * (b - a) + tz * (c - a) : d + (1 - tx) * (c - d) + (1 - tz) * (b - d);
}
