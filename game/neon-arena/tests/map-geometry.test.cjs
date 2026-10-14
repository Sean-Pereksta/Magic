const test=require('node:test'),assert=require('node:assert/strict');
const maps=require('../neon-survival-maps.js'),tactical=require('../neon-tactical-ai.js');
const {standardFixture,survivalFixture,standardMaps,source}=require('./map-fixtures.cjs');
test('clearance validator measures the 2.39m model and catches raised/deck-to-deck voids',()=>{
  assert.equal(maps.STANDING_HEIGHT,2.39);
  const p={x:0,z:0,w:8,d:8,y:2.45,h:.55};
  assert.equal(maps.validateGeometry({platforms:[p],walls:[]}).valid,false,'Eye-height-safe deck still hits the full model');
  assert.equal(maps.validateGeometry({platforms:[{...p,y:2.57}],walls:[]}).valid,true);
  assert.equal(maps.validateGeometry({platforms:[{...p,y:0,h:1},{...p,y:3.2}],walls:[]}).valid,false,'Raised ground support counts toward overhead');
  assert.equal(maps.validateGeometry({platforms:[p],walls:[{x:0,z:0,w:8,d:8,y:0,h:2.45}]}).valid,true,'A visible solid plinth is allowed');
  assert.equal(maps.validateGeometry({platforms:[{...p,w:NaN}],walls:[]}).valid,false);
});
for(const item of maps.catalog())test(item.name+': authored solids, standing spawns, ramp entrances and ambiance',()=>{
  const {b,definitions,pieces}=survivalFixture(item.id);assert.equal(maps.validateGeometry(b).valid,true);
  assert.ok(b.landmarks.length>=2);assert.ok(b.ambientDetails.length>=2);
  for(const p of b.props){
    const mesh=definitions[p.prefab].mesh;
    if(p.decorative){assert.ok(p.background||p.decorativeReason);continue;}
    assert.ok(mesh.collision.length>0,p.prefab+' needs physical components');
    // Every authored vertex has a close physical component. Small trim tolerance
    // covers subpixel bevels while still catching missing arch crowns/whole props.
    for(let i=0;i<mesh.positions.length;i+=3){const [x,y,z]=mesh.positions.slice(i,i+3);assert.ok(mesh.collision.some(c=>Math.abs(x-c.x)<=c.w/2+.045&&Math.abs(z-c.z)<=c.d/2+.045&&y>=c.y-.045&&y<=c.y+c.h+.045),`${b.id}/${p.prefab}: visible vertex ${i/3} lacks collision`);}
  }
  for(const s of b.playerSpawns){const info=maps.pointInfo(s.x*1.45,s.z*1.45,{y:(s.y||0)+1.86});assert.equal(info.obstacle,null,'Player spawn stays physically open');}
  for(let seed=1;seed<25;seed++){let n=seed;const rng=()=>{n=(n*1664525+1013904223)>>>0;return n/4294967296;};for(const kind of ['raptor','flyer','warder','burrowerBoss']){const s=maps.spawnPoint(kind,rng);assert.equal(maps.pointInfo(s.x,s.z,{y:s.y}).obstacle,null,'Species spawn avoids solid scenery');}}
  for(const r of b.ramps){assert.ok(Math.min(r.w,r.d)>=6,'Ramp is broad enough for combat');for(let i=1;i<40;i++)for(const lane of [-.3,0,.3]){const t=i/40,x=(r.x+(r.axis==='x'?(t-.5)*r.w:lane*r.w))*1.45,z=(r.z+(r.axis==='z'?(t-.5)*r.d:lane*r.d))*1.45,y=r.fromHeight+(r.toHeight-r.fromHeight)*t+1.86,info=maps.pointInfo(x,z,{y});assert.ok(!info.obstacle,`${b.id}: ramp at ${t}, lane ${lane} is obstructed by ${info.obstacle?.prefab||'wall'}`);}}
  const frames=Array.from({length:10},(_,i)=>Array.from(maps.buildAmbientGeometry(i*.23)));assert.ok(frames.some(f=>f.length>0));assert.ok(frames.every(f=>f.length<3600&&f.every(Number.isFinite)));assert.ok(new Set(frames.map(f=>JSON.stringify(f))).size>1,'Scenery is visibly animated');
  for(const p of pieces.filter(p=>p.compoundPieces)){assert.ok(p.compoundPieces.includes(p));assert.ok(p.compoundPieces.every(q=>q.compoundPieces===p.compoundPieces),'Compound destruction group is atomic');}
});
for(const map of standardMaps)test(map+': standard arena standing clearance, safe spawns and ramp routes',()=>{
  const s=standardFixture(map),report=maps.validateGeometry({id:map,...s,worldCoordinates:true});assert.deepEqual(report.errors,[]);
  assert.ok(s.nav.length>200);assert.ok(s.ramps.length>=2);assert.ok(s.props.length>=3,'Standard arena reuses authored prefab artwork');assert.ok(s.walls.every(w=>w.surfaceTile||w.safety||w.survivalEnvironment),'Every authored wall uses the atlas or safety bounds');
  const surfaces=maps.buildSurfaceChunks([...s.walls,...s.platforms]),scenery=maps.buildChunks(),vertices=surfaces.reduce((n,c)=>n+c.data.length/maps.SURFACE_STRIDE,0)+scenery.reduce((n,c)=>n+c.data.length/10,0);assert.ok(vertices<60000,'Standard arena stays inside its cached static vertex budget');assert.ok(s.nav.length<900,'Navigation remains bounded');
  for(const spawn of s.spawns)assert.ok(s.safeSpawnCandidate({r:.5},spawn.x,spawn.z,0),'Spawn has floor and full standing clearance');
  for(const r of s.ramps)for(let i=0;i<=20;i++){const t=i/20,x=(r.x+(r.axis==='x'?(t-.5)*r.w:0))*1.45,z=(r.z+(r.axis==='z'?(t-.5)*r.d:0))*1.45,h=r.fromHeight+(r.toHeight-r.fromHeight)*t;assert.ok(Math.abs(s.arenaRampHeight(x,z)-h)<1e-6);const blocked=s.walls.some(w=>Math.abs(x-w.x)<w.w/2+.5&&Math.abs(z-w.z)<w.d/2+.5&&h<w.y+w.h/2-.05&&h+2.39>w.y-w.h/2+.05);assert.equal(blocked,false,'Standing actor clears whole ramp route');}
  const exposed=s.spawns[0];s.setEnemies([{x:exposed.x,z:exposed.z+5,y:1.86,dead:false,team:2}]);const chosen=s.chooseSafeSpawn({r:.5,team:1});assert.ok(Math.hypot(chosen.x-exposed.x,chosen.z-exposed.z)>25,'Spawn selection reacts to enemy pressure');
  let clock=0;const ai=tactical.createController({navigation:()=>s.nav,revision:()=>1,time:()=>clock,pointInfo:s.tacticalPointInfo,canTraverse:s.tacticalCanTraverse,lineClear:()=>false});
  const actor={...s.spawns[0],r:.52,team:1};
  for(const deck of s.platforms.filter(p=>p.authoredDeck)){clock+=1;const goal={x:deck.x,z:deck.z,y:deck.y+deck.h+1.86};assert.ok(ai.findPath(actor,goal).length>0,`${map}: bot can reach the elevated deck through its walking route`);}
});
test('the new arenas have distinct authored architecture and combat routes',()=>{
  const terminal=standardFixture('terminal'),foundry=standardFixture('foundry'),outpost=standardFixture('outpost');
  assert.ok(terminal.props.some(p=>p.prefab==='train_carriage')&&terminal.props.some(p=>p.prefab==='platform_canopy'),'Terminal keeps real train and platform architecture');
  assert.ok(foundry.props.some(p=>p.prefab==='turbine')&&foundry.props.some(p=>p.prefab==='reactor_core'),'Foundry has industrial machinery');
  assert.ok(outpost.props.some(p=>p.prefab==='rock_arch')&&outpost.platforms.filter(p=>p.authoredDeck&&p.y===0&&p.h>=4).length>=2,'Outpost has an open natural arch and grounded cliff mesas');
  const shapes=[terminal,foundry,outpost].map(s=>JSON.stringify(s.platforms.filter(p=>p.authoredDeck).map(p=>[p.x,p.z,p.w,p.d,p.y,p.h])));assert.equal(new Set(shapes).size,3,'New arenas do not share a reskinned platform layout');
});
for(const map of ['terminal','foundry','outpost'])test(map+': Frag boundaries leave the complete walking routes open',()=>{
  const s=standardFixture(map),applyFrag=source.slice(source.indexOf('function applyFragArena(){'),source.indexOf('function enforceFragLoadout(')),addNav=source.slice(source.indexOf('function addNav('),source.indexOf('function addPickup('));
  const arena=s.evaluate(addNav+applyFrag+"game.mode='frag';applyFragArena();({id:game.map,walls,platforms,ramps:arenaRamps,worldCoordinates:true})");
  assert.deepEqual(maps.validateGeometry(arena).errors,[],'Tight match boundaries do not cross an ascent');
  for(const ramp of arena.ramps){const axis=ramp.axis,length=ramp[axis==='x'?'w':'d'],ends=[ramp[axis]-length/2,ramp[axis]+length/2];for(const end of ends)assert.ok(Math.abs(end)<=41,'Standing landing margin stays clear of the Frag boundary');}
});
