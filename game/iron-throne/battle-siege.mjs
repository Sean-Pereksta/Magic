const WARDS = [
  { half:25, front:16, rear:54, height:3, wallHp:80, gateHp:62 },
  { half:29, front:14, rear:57, height:4, wallHp:115, gateHp:85 },
  { half:35, front:12, rear:59, height:6, wallHp:220, gateHp:150 },
  { half:43, front:8, rear:66, height:7, wallHp:285, gateHp:190 },
  { half:52, front:5, rear:72, height:8, wallHp:350, gateHp:225 }
];
export function castleSite(tile) {
  const level = role => tile.levels?.[role] || tile.buildings?.[role] || (tile.building === role ? tile.level || tile.buildingLevel || 1 : 0);
  const fort = tile.building === 'fort' ? level('fort') : 0, city = tile.building === 'city' ? level('city') : 0;
  const wall = level('wall') || (tile.walls > 0 ? 1 : 0);
  if (!fort && !city && !wall && tile.building !== 'town' && !tile.wallSections?.length && !tile.wallBreaches?.length) return null;
  const tier = fort >= 3 || city >= 4 ? 4 : fort >= 2 || city >= 3 || wall >= 3 ? 3
    : fort || city || wall >= 2 ? 2 : tile.building === 'town' ? 1 : 0;
  const profile = WARDS[tier], enclosed = !!(fort || wall || tile.wallBreaches?.length);
  const name = ['Outpost stockade', 'Town palisade', city ? 'Walled city' : 'Stone fort', city ? 'Grand city' : 'Stone fortress', city ? 'Royal city citadel' : 'Great fortress'][tier];
  const outer = { left:-profile.half, right:profile.half, front:profile.front, rear:profile.rear };
  const inner = enclosed && tier >= 3 ? {left:tier===4?-29:-23, right:tier===4?29:23, front:tier===4?35:36, rear:outer.rear-4} : null;
  return { tier, name:enclosed ? name : tile.building === 'city' ? 'Open city' : 'Market town', enclosed, outer, inner };
}

function wardSurfaces(bounds, layer, height, hp, gateHp, material) {
  const {left,right,front,rear} = bounds, halfGate = layer === 'inner' ? 3.6 : 4;
  const prefix = layer === 'inner' ? 'inner-' : '';
  const specs = [
    ['wall-west-front',(left+1-halfGate)/2,front,-halfGate-left-1,2],
    ['wall-east-front',(right-1+halfGate)/2,front,right-1-halfGate,2],
    ['wall-west',left,(front+rear-1)/2,2,rear-front+1],
    ['wall-east',right,(front+rear-1)/2,2,rear-front+1],
    ['wall-rear',0,rear,right-left+2,2], ['gate',0,front,halfGate*2,2]
  ];
  return specs.map(([id,x,z,width,depth]) => {
    const gate=id==='gate';
    return { id:prefix+id, layer, kind:gate?'gate':'wall', material:gate?'timber':material,
      x,z,width,depth,height:gate?height-.3:height,maxHp:gate?gateHp:hp };
  });
}

export function buildCastle(layout, tile) {
  const castle=castleSite(tile);
  if (!castle) return;
  layout.castle=castle;
  const {tier,outer,inner}=castle, profile=WARDS[tier];
  const ward=inner || outer;
  const keep = {kind:tier<2?'hall':'keep',x:0,z:ward.rear-7,width:6+tier*1.8,depth:5+tier,height:5+tier*3};
  castle.keep=keep; castle.items=[];
  const place=(kind,x,z,width,depth,height,solid=true) => {
    const item={kind,x,z,width,depth,height};
    castle.items.push(item);
    if (solid) layout.obstacles.push({ ...item, id:`castle-${kind}-${castle.items.length}`, kind:'castleItem',itemKind:kind });
  };
  place(keep.kind,keep.x,keep.z,keep.width,keep.depth,keep.height);
  layout.objective={x:keep.x,z:keep.z-keep.depth/2-2.4};
  place('well',outer.right*.47,outer.front+12,2.1,2.1,2);
  place('house',outer.left+8,outer.rear-12,5,6,4);
  if (tier>=1) place('market',outer.right-9,outer.rear-10,6,5,4);
  if (tier>=2 || tile.barracks) place('barracks',outer.left+9,outer.front+16,7,7,5);
  if (tier>=2 || tile.stable) place('stable',outer.right-9,outer.front+21,7,6,4.5);
  if (tier>=3 || tile.storehouse) place('storehouse',outer.left+10,outer.rear-22,7,5,5.5);
  if (tier>=4) place('chapel',inner.right-6,inner.rear-8,5,7,9);
  if (tier>=1) place('cart',outer.left+14,outer.front+10,2,3,1.7);
  place('banner',keep.x-keep.width/2-1.5,keep.z-keep.depth/2,1,1,4,false);
  if (!castle.enclosed) return;
  const walls=wardSurfaces(outer,'outer',profile.height,profile.wallHp,profile.gateHp,tier<2?'timber':'stone');
  if (inner) walls.push(...wardSurfaces(inner,'inner',profile.height+1.5,profile.wallHp+60,profile.gateHp+35,'stone'));
  layout.gate=walls.find(w=>w.id==='gate');
  layout.walls=walls.filter(w=>w!==layout.gate);
  layout.ladders=[outer,inner].filter(Boolean).flatMap((ward,i) => [-1,1].map((sign,j) => ({
    id:i?`inner-ladder-${j}`:`ladder-${j}`, x:sign*Math.round(ward.right*.57),z:ward.front,layer:i?'inner':'outer',progress:0
  })));
  provisionLadders(layout);
  layout.stairs=layout.walls.filter(w=>w.kind!=='gate').map(w=>{
    const rear=w.id.endsWith('wall-rear'), side=w.depth>w.width;
    return {id:w.id+'-stairs',wallId:w.id,x:w.x+(side?-Math.sign(w.x)*6:0),z:w.z+(side?0:rear?-6:6),
      topX:w.x,topZ:w.z,height:w.height,width:3,depth:6};
  });
  if (tier>=2) for(const side of [-1,1]) {
    place('tower',side*(outer.right-2.8),outer.front+3.5,4,4,profile.height+3);
    if (inner) place('tower',side*(inner.right-2.8),inner.front+3.3,4,4,profile.height+5);
    if (tier>=4) place('tower',side*(outer.right-2.8),outer.rear-3,4,4,profile.height+4);
  }
}

export function matchesCastleLayout(layout, tile) {
  // A battle already underway on the original six-section fort keeps that map.
  if (!layout.castle) return true;
  const expected={walls:[],gate:null,ladders:[],stairs:[],obstacles:[]};
  buildCastle(expected,tile);
  if (!expected.castle || expected.castle.tier!==layout.castle.tier || expected.castle.enclosed!==layout.fortified) return false;
  const boundsMatch=(a,b)=>!a || !b ? a===b : ['left','right','front','rear'].every(key=>a[key]===b[key]);
  if (!boundsMatch(expected.castle.outer,layout.castle.outer) || !boundsMatch(expected.castle.inner,layout.castle.inner)) return false;
  const surfaces=[...layout.walls,layout.gate].filter(Boolean);
  const planned=[...expected.walls,expected.gate].filter(Boolean);
  if (surfaces.length!==planned.length || new Set(surfaces.map(w=>w.id)).size!==planned.length) return false;
  const geometry=['id','x','z','width','depth','height','maxHp','kind','layer','material'];
  return planned.every(w=>surfaces.some(saved=>geometry.every(key=>saved[key]===w[key])));
}

// The simulation and the view use the same world-space route landmarks.
export function provisionLadders(layout) {
  for (const ladder of layout.ladders) {
    const wall = layout.walls.find(w => w.width > w.depth && Math.abs(w.x - ladder.x) <= w.width / 2 && Math.abs(w.z - ladder.z) < 1);
    if (!wall) continue;
    const west = ladder.x < 0;
    Object.assign(ladder, {
      name: (ladder.layer==='inner'?'Inner ':'') + (west ? 'West ladder' : 'East ladder'), wallId: wall.id,
      foot: { x: ladder.x, z: ladder.z - 3.4 }, landing: { x: ladder.x, z: ladder.z },
      height: wall.height, rungs: Math.round(wall.height * 2), active: wall.hp !== 0, held: false
    });
    delete ladder.climberId; // Old checkpoints reserved the entire lane for one soldier.
  }
}

export function ramHandle(ram, index) {
  const x = (index % 2 ? 1 : -1) * 1.4, z = (Math.floor(index / 2) - 1) * 1.75;
  const sin = Math.sin(ram.facing), cos = Math.cos(ram.facing);
  return { x: ram.x + x * cos + z * sin, z: ram.z - x * sin + z * cos };
}

export function ramApproach(wall) {
  const sideWall = wall.depth > wall.width;
  const nx = sideWall ? Math.sign(wall.x) : 0, nz = sideWall ? 0 : wall.id.endsWith('wall-rear') ? 1 : -1;
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
  if (!base || w.hp <= 0 || w.kind === 'gate' || w.id === 'gate') return base;
  const vertical = w.depth > w.width, length = vertical ? w.depth : w.width;
  const outer = vertical ? Math.sign(w.x) : w.id.endsWith('wall-rear') ? 1 : -1;
  const across = vertical ? x - w.x : z - w.z, along = vertical ? z - w.z : x - w.x;
  if (Math.abs(across - outer * ((vertical ? w.width : w.depth) / 2 - .26)) > .26) return base;
  const first = -length / 2 + .7, center = first + Math.round((along - first) / 2) * 2;
  if (center >= first && center < length / 2 && Math.abs(along - center) <= .5 && (w.hp / w.maxHp > .6 || Math.round(center) % 3)) return base + 1.1;
  return base;
}
