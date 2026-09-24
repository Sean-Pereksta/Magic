// Point-up axial hexes, clockwise in Canvas coordinates: E, SE, SW, W, NW, NE.
// Keep this order independent of core.neighbors(), whose ordering is reversed.
export const HEX_DIRECTIONS = Object.freeze([[1,0],[0,1],[-1,1],[-1,0],[0,-1],[1,-1]].map(Object.freeze));
export const oppositeEdge = edge => (edge + 3) % 6;
export const maskEdges = mask => [0,1,2,3,4,5].filter(i => mask & (1 << i));
export const neighborId = (t, edge) => `${t.q + HEX_DIRECTIONS[edge][0]},${t.r + HEX_DIRECTIONS[edge][1]}`;
export const rotateMask = (mask, turns) => { turns = ((turns % 6) + 6) % 6; return ((mask << turns) | (mask >> (6 - turns))) & 63; };
export function canonicalMask(mask) {
  let canonical = mask & 63, rotation = 0;
  for (let i = 1; i < 6; i++) if (rotateMask(mask, i) < canonical) { canonical = rotateMask(mask, i); rotation = (6 - i) % 6; }
  return { mask: canonical, rotation };
}
export function waterMask(tiles, t) {
  if (t.terrain === 'water') return 0;
  // This game's finite board is surrounded by ocean, including imported edge land.
  return HEX_DIRECTIONS.reduce((mask, _, edge) => {
    const n = tiles[neighborId(t, edge)];
    return !n || n.terrain === 'water' ? mask | (1 << edge) : mask;
  }, 0);
}
export function shorelineRuns(mask) {
  if (!mask) return [];
  if (mask === 63) return [[0,1,2,3,4,5]];
  const runs = [];
  for (let i = 0; i < 6; i++) if ((mask & (1 << i)) && !(mask & (1 << ((i+5)%6)))) {
    const run = [];
    for (let j = i; mask & (1 << (j % 6)); j++) run.push(j % 6);
    runs.push(run);
  }
  return runs;
}
export function riverKind(mask, mouthMask = 0) {
  const edges = maskEdges(mask);
  if (mouthMask) return 'mouth';
  if (edges.length <= 1) return 'source';
  if (edges.length === 2) return oppositeEdge(edges[0]) === edges[1] ? 'straight' : 'bend';
  return edges.length === 3 ? 'fork' : 'junction';
}
export function coastGround(tiles, t) {
  if (t.terrain !== 'coast') return t.terrain;
  const counts = new Map();
  for (let i=0;i<6;i++) {
    const terrain = tiles[neighborId(t,i)]?.terrain;
    if (terrain && terrain !== 'coast' && terrain !== 'water') counts.set(terrain, (counts.get(terrain)||0)+1);
  }
  // Mountain coasts use foothills so the inland side stays readable under the shore.
  const terrain = [...counts].sort((a,b)=>b[1]-a[1] || a[0].localeCompare(b[0]))[0]?.[0] || 'plains';
  return terrain === 'mountain' ? 'hills' : terrain;
}

// Derive the visual graph without changing terrain, movement costs or save data.
// Legacy maps stop their river one coast tile short of the ocean. A bounded
// outlet search repairs that small gap; it never routes across unrelated inland hexes.
export function buildGeography(tiles) {
  const riverTiles = new Set(Object.values(tiles).filter(t=>t.river && t.terrain !== 'water').map(t=>t.id));
  const links = new Map([...riverTiles].map(id=>[id,0]));
  for (const id of riverTiles) for (let edge=0;edge<6;edge++) {
    if (riverTiles.has(neighborId(tiles[id],edge))) links.set(id, links.get(id) | (1<<edge));
  }
  const mouths = new Map(), seen = new Set();
  const connect = (id, edge, next) => {
    riverTiles.add(next); links.set(id,(links.get(id)||0)|(1<<edge));
    links.set(next,(links.get(next)||0)|(1<<oppositeEdge(edge)));
  };
  // Process each existing connected river component independently. Prefer an
  // actual neighboring sea; otherwise bridge at most two coastal tiles.
  for (const root of [...riverTiles].sort()) {
    if (seen.has(root)) continue;
    const component=[], queue=[root]; seen.add(root);
    for (let i=0;i<queue.length;i++) {
      const id=queue[i]; component.push(id);
      for (const edge of maskEdges(links.get(id))) {
        const next=neighborId(tiles[id],edge);
        if (!seen.has(next)) { seen.add(next); queue.push(next); }
      }
    }
    const endpoints=component.filter(id=>maskEdges(links.get(id)).length<=1);
    const candidates=[];
    for (const id of component) {
      const t=tiles[id];
      for (const edge of maskEdges(waterMask(tiles,t))) candidates.push({id,edge,path:[],distance:0});
    }
    if (!candidates.length) for (const start of endpoints) {
      const pending=[{id:start,path:[]}], visited=new Set([start]);
      for (let i=0;i<pending.length;i++) {
        const item=pending[i], t=tiles[item.id];
        for (let edge=0;edge<6;edge++) {
          const next=neighborId(t,edge), n=tiles[next];
          if (!n || n.terrain==='water') { candidates.push({...item,edge,distance:item.path.length}); continue; }
          if (item.path.length>=2 || visited.has(next) || riverTiles.has(next) || n.building || n.terrain==='mountain') continue;
          if (n.terrain!=='coast' && !waterMask(tiles,n)) continue;
          visited.add(next); pending.push({id:next,path:[...item.path,{id:item.id,edge,next}]});
        }
      }
    }
    // Equal-distance legacy endpoints have no elevation. Use stable southern
    // outlet preference; the other end is drawn explicitly as a spring.
    candidates.sort((a,b)=>a.distance-b.distance || tiles[b.id].r-tiles[a.id].r || tiles[a.id].q-tiles[b.id].q || a.edge-b.edge);
    const outlet=candidates[0];
    if (outlet) {
      for (const step of outlet.path) connect(step.id,step.edge,step.next);
      links.set(outlet.id,(links.get(outlet.id)||0)|(1<<outlet.edge));
      mouths.set(outlet.id,1<<outlet.edge);
    }
  }
  return new Map(Object.values(tiles).map(t=>{
    const coastMask=waterMask(tiles,t), riverMask=links.get(t.id)||0, mouthMask=mouths.get(t.id)||0;
    return [t.id,{coastMask,riverMask,mouthMask,hasRiver:riverTiles.has(t.id),ground:coastGround(tiles,t),kind:riverKind(riverMask,mouthMask)}];
  }));
}

export class GeographyCache {
  get(tiles) {
    // Terrain is mutable; object identity or turn number alone misses editor/import changes.
    const signature=Object.values(tiles).map(t=>`${t.id}:${t.q}:${t.r}:${t.terrain}:${t.river?1:0}:${t.building||''}`).join('|');
    if (this.signature!==signature) { this.signature=signature; this.value=buildGeography(tiles); }
    return this.value;
  }
}
