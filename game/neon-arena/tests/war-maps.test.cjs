const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const war=require('../neon-war-maps.js'),maps=require('../neon-survival-maps.js'),tactical=require('../neon-tactical-ai.js'),{warFixture}=require('./war-maps-fixture.cjs');

test('six authored battlefields and ten campaign sites have coherent metadata',()=>{
  assert.equal(war.IDS.length,6);assert.equal(new Set(war.IDS).size,6);
  const missions=war.IDS.flatMap(id=>Object.keys(war.worldMetadata(id).missionSites));assert.equal(missions.length,10);assert.equal(new Set(missions).size,10);
  for(const id of war.IDS){const m=war.worldMetadata(id);assert.equal(m.sectors.length,5);assert.equal(m.sectors[0].links.includes('cat_hq'),true);assert.equal(m.sectors[4].links.includes('mouse_hq'),true);
    for(const s of m.sectors)for(const n of s.links.filter(n=>n.length===1))assert.ok(m.sectors.find(p=>p.id===n).links.includes(s.id));
    assert.ok(m.sectors.some(s=>s.links.length>=4),'Conquest branches across the battlefield');
    for(const team of [1,2])assert.ok(m.factionSpawns[team].length>=3);
    assert.ok(Math.hypot(m.hq[1].x-m.hq[2].x,m.hq[1].z-m.hq[2].z)>100);
    for(const key of ['entry','intel','security','prison','escort','defend','vault','sabotage','extract'])assert.ok(m.anchors[key]);
    const l=war.createLayout(id),light=m.lighting;
    assert.ok(light.ambient>=.6&&light.ambient<=.75);assert.ok(light.exposure>=1&&light.exposure<=1.15);
    assert.ok(light.zones.length>=4&&light.zones.length<=6);
    for(const [i,z] of light.zones.entries()){assert.ok(z.radius>=8&&z.radius<=18);assert.ok(z.intensity>0&&z.intensity<=.8);assert.equal(z.y,l.theme.lighting.zones[i].y,'Lights use physical height, not actor eye height');assert.equal(z.x,l.theme.lighting.zones[i].x*war.SCALE);assert.ok(z.color.every(c=>c>=0&&c<=1));}
    assert.ok(m.minimap.length>=l.buildings.filter(b=>b.base<1).length&&m.minimap.length<=140);
    for(const b of m.minimap){assert.ok([b.x,b.z,b.w,b.d].every(Number.isFinite));assert.ok(b.w>0&&b.d>0);assert.ok(Math.abs(b.x)<m.bounds&&Math.abs(b.z)<m.bounds);}
    assert.ok(l.features.length>=3);assert.ok(new Set(l.features.map(f=>f.label)).size>=3);assert.ok(m.combatSpaces.length>=2);
  }
});

for(const id of war.IDS)test(id+': geometry, physical openings, safe HQs and real walking routes',()=>{
  const s=warFixture(id),l=s.layout,world=war.worldMetadata(id);
  assert.deepEqual(maps.validateGeometry({id,...s,worldCoordinates:true}).errors,[]);
  assert.ok(l.buildings.length>=3);assert.ok(l.windows.length>=4);assert.ok(l.doors.length>=8);assert.ok(l.ramps.length>=2);assert.ok(l.props.length>=3);
  assert.ok(s.nav.length<900,'Navigation has a bounded graph');
  const surfaces=maps.buildSurfaceChunks([...s.walls,...s.platforms]),scenery=maps.buildChunks(),vertices=surfaces.reduce((n,c)=>n+c.data.length/maps.SURFACE_STRIDE,0)+scenery.reduce((n,c)=>n+c.data.length/10,0);
  assert.ok(vertices<60000,'Static geometry stays inside existing arena budget: '+vertices);
  for(const p of [...Object.values(world.anchors),...world.sectors,...Object.values(world.factionSpawns).flat()])assert.equal(s.tacticalPointInfo(p.x,p.z,{y:p.y,r:.52}).walkable,true,p.label+' is physically walkable');
  for(const area of world.combatSpaces)for(const dx of [-.4,0,.4])for(const dz of [-.4,0,.4])assert.equal(s.tacticalPointInfo(area.x+dx*area.w,area.z+dz*area.d,{y:area.y,r:.52}).walkable,true,area.label+' keeps a clear combat floor');
  for(const d of l.doors){const p={x:d.x*1.45,z:d.z*1.45,y:d.y+1.86};assert.ok(d.top-d.y>=3.3);assert.equal(s.tacticalPointInfo(p.x,p.z,p).walkable,true,'Door has standing clearance');}
  for(const w of l.windows){const x=w.x*1.45,z=w.z*1.45,y=w.y+.81;
    assert.ok(w.y<y&&w.top>y,'Firing aperture contains weapon eye height');
    assert.equal(s.walls.some(b=>Math.abs(b.x-x)<b.w/2-.01&&Math.abs(b.z-z)<b.d/2-.01&&y>b.y-b.h/2&&y<b.y+b.h/2),false,'Window is a real hole through the collision wall');
    assert.equal(s.platforms.some(b=>Math.abs(b.x-x)<b.w/2+.01&&Math.abs(b.z-z)<b.d/2+.01&&y>b.y&&y<b.y+b.h),false,'Window firing hole is not obstructed by a ramp or platform');
  }
  let clock=0;const ai=tactical.createController({navigation:()=>s.nav,revision:()=>1,time:()=>clock,pointInfo:s.tacticalPointInfo,canTraverse:s.tacticalCanTraverse,lineClear:()=>false});
  let actor={...world.hq[1],r:.52,team:1};
  for(const target of [...world.sectors,world.hq[2]]){clock+=1;assert.ok(ai.findPath(actor,target).length>0,'Bot reaches '+(target.label||target.name)+' through physical walking routes');actor={...target,r:.52,team:1};}
  const nearest=(target,points)=>points.slice().sort((a,b)=>Math.hypot(a.x-target.x,a.z-target.z)-Math.hypot(b.x-target.x,b.z-target.z))[0];
  for(const target of Object.values(world.anchors)){clock+=1;assert.ok(ai.findPath({...nearest(target,world.sectors),r:.52,team:1},target).length>0,'Campaign anchor connects to sector network: '+target.label);}
  for(const target of world.combatSpaces){clock+=1;assert.ok(ai.findPath({...nearest(target,world.sectors),r:.52,team:1},target).length>0,'Clear combat area connects to sector network: '+target.label);}
  const landings=[];
  for(const r of s.ramps){const high=r.fromHeight>r.toHeight?-.5:.5,target={x:(r.x+(r.axis==='x'?high*r.w:0))*1.45,z:(r.z+(r.axis==='z'?high*r.d:0))*1.45,y:Math.max(r.fromHeight,r.toHeight)+1.86},start={x:(r.x-(r.axis==='x'?high*r.w:0))*1.45,z:(r.z-(r.axis==='z'?high*r.d:0))*1.45,y:1.86,r:.52};clock+=1;assert.ok(ai.findPath(start,target).length>0,'Bot reaches elevated ramp landing');landings.push(target);}
  for(const p of s.platforms.filter(p=>p.authoredDeck)){clock+=1;const target={x:p.x,z:p.z,y:p.y+p.h+1.86};assert.ok(ai.findPath({...nearest(target,landings),r:.52},target).length>0,'Bot reaches every authored upper fighting surface');}
});

test('menu preview paths point to locally shipped authored map diagrams',()=>{
  for(const m of war.catalog()){const file=path.resolve(__dirname,'..',m.preview);assert.ok(fs.existsSync(file),m.id+' preview exists');const source=fs.readFileSync(file,'utf8');assert.ok(source.includes('<svg')&&source.includes(m.name)&&source.includes('HQ'));for(const f of war.createLayout(m.id).features)assert.ok(source.includes(f.label.split(' ').slice(0,2).join(' ')),'Preview identifies authored landmarks');assert.ok(source.includes('CLEAR COMBAT AREAS')&&source.includes('AUTHORED LIGHT ZONES'));}
});
