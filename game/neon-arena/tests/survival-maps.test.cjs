/* Run: node game/neon-arena/tests/survival-maps.test.cjs */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const maps=require('../neon-survival-maps.js'),base=path.resolve(__dirname,'../assets/survival');
const read=file=>JSON.parse(fs.readFileSync(file,'utf8'));
const manifest=read(path.join(base,'manifest.json'));
assert.equal(manifest.maps.length,8);assert.equal(manifest.worldScale,1.45);
for(const item of manifest.maps){
  const b=read(path.join(base,item.blueprint)),t=read(path.join(base,item.tileset)),prefabs=read(path.join(base,item.prefabs)),defs={};
  for(const asset of [t.atlasColor,t.atlasMobile,'map_card.webp'])assert.ok(fs.existsSync(path.join(base,'tilesets',b.id,asset)),`${b.id}: committed image ${asset} exists`);
  for(const p of prefabs.items){const file=path.join(base,'tilesets',b.id,p.file),def=read(file);defs[p.id]={...def,mesh:read(path.resolve(path.dirname(file),def.geometry))};}
  for(const tile of t.tiles){const [x,y,w,h]=tile.pixelRect;assert.equal(w,240);assert.equal(h,240);assert.equal(x%256,8);assert.equal(y%256,8);const [u0,v0,u1,v1]=tile.uv;assert.ok(u0>=x/2048&&v0>=y/2048&&u1<(x+w)/2048&&v1<(y+h)/2048,'Crop stays within requested tile');}
  maps.hydrateForTest(b,t,defs);
  const structures=[],make=(kind,a)=>{const [x,z,w,d]=a,p={kind,x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,active:true,destructible:kind==='wall'};structures.push(p);return p;};
  const installed=maps.install({addPlatform:(...a)=>make('platform',a),addWall:(...a)=>make('wall',a)});
  const nav=maps.navigation();assert.ok(nav.nodes.length>300);assert.ok(nav.links.length>nav.nodes.length);
  const adjacency=new Map(nav.nodes.map(n=>[n.id,[]]));for(const edge of nav.links){assert.ok(adjacency.has(edge.from));assert.ok(adjacency.has(edge.to));if(!['walk','ramp','stairs'].includes(edge.kind))continue;adjacency.get(edge.from).push(edge.to);adjacency.get(edge.to).push(edge.from);}
  const nearest=(x,z,y=0)=>nav.nodes.reduce((best,n)=>{const d=Math.hypot(n.x-x,n.z-z)+Math.abs(n.y-y)*8;return !best||d<best.d?{n,d}:best;},null).n;
  const start=nearest(b.playerSpawns[0].x*1.45,b.playerSpawns[0].z*1.45),reachable=new Set([start.id]),queue=[start.id];
  for(let i=0;i<queue.length;i++)for(const id of adjacency.get(queue[i]))if(!reachable.has(id)){reachable.add(id);queue.push(id);}
  for(const zone of nav.spawnZones)assert.ok(reachable.has(nearest(zone.x,zone.z).id),`${b.id}: enemy spawn connects to player`);
  for(const p of b.platforms){const top=p.y+p.h;assert.ok(reachable.has(nearest(p.x*1.45,p.z*1.45,top).id),`${b.id}: high platform connects by ramp`);}
  for(const ramp of b.ramps){const x=ramp.x*1.45,z=ramp.z*1.45;assert.ok(Math.abs(maps.rampHeight(x,z)-(ramp.fromHeight+ramp.toHeight)/2)<1e-7);const h=maps.rampHeight(x,z),a={x,z,y:h+1.86,vy:0,onGround:true};assert.equal(maps.rampSupport(a),true);assert.equal(maps.followRamp(a,{x,z,y:a.y,grounded:true}),true);}
  const destructible=structures.find(s=>s.destructible);assert.ok(destructible,`${b.id} has destructible cover`);const previous=maps.revision;destructible.active=false;maps.environmentStateChanged();assert.ok(maps.revision>previous);assert.notEqual(maps.navigation(),nav);
  for(const kind of ['raptor','flyer','stalker','burrowerBoss','warder']){const p=maps.spawnPoint(kind,()=>.5);assert.ok(Math.hypot(p.x,p.z)>20,`Species ${kind} uses map ingress, not player center`);}
  const dynamic=installed.objects.find(o=>o.eventZone);if(dynamic){const index=b.hazardZones.indexOf(dynamic.eventZone),zone=dynamic.eventZone,time=zone.warning+.2-index*17+zone.period*2;maps.updateEvents(time);assert.equal(dynamic.piece.active,true);dynamic.piece.active=false;maps.updateEvents(time+.2);assert.equal(dynamic.piece.active,false,'Destroyed growth stays down for this event');maps.updateEvents(time+zone.duration+2);maps.updateEvents(time+zone.period);assert.equal(dynamic.piece.active,true,'Next event may regrow geometry');}
  for(let i=0;i<20;i++){const p=maps.pickupPoint();assert.ok(Number.isFinite(p.x+p.y+p.z));assert.ok(!maps.pointInfo(p.x,p.z).obstacle,'Pickup must avoid obstacles');}
  const chunks=maps.buildChunks();assert.ok(chunks.length>8);assert.ok(chunks.every(c=>Array.from(c.data).every(Number.isFinite)),'All geometry finite');assert.ok(chunks.reduce((n,c)=>n+c.data.length/10,0)<160000,'Static geometry budget');
  console.log(`${b.id}: ${nav.nodes.length} nodes, ${nav.links.length} links, ${reachable.size} reachable, ${chunks.length} cached chunks`);
}
console.log('8 map blueprints, padded UVs, meshes, spawns, ramps, destruction and geometry budgets passed.');
