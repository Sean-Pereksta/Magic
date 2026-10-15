const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const crypto=require('node:crypto');
const source=fs.readFileSync(path.join(__dirname,'../maps.js'),'utf8');
function client(){const context={};vm.runInNewContext(source,context);return context.TurncraftMaps}
const maps=client(),strategic=maps.definitions.filter(d=>!d.legacy);
function flood(layout,start,footprint=1){
  const {width:w,height:h,terrain}=layout,seen=new Uint8Array(w*h),distance=new Int32Array(w*h).fill(-1),q=[];
  function open(x,y){if(x<0||y<0||x+footprint>w||y+footprint>h)return false;for(let dy=0;dy<footprint;dy++)for(let dx=0;dx<footprint;dx++)if(terrain[(y+dy)*w+x+dx].blocked)return false;return true}
  assert(open(start.x,start.y),'spawn supports footprint '+footprint);const origin=start.y*w+start.x;seen[origin]=1;distance[origin]=0;q.push(origin);
  for(let i=0;i<q.length;i++){const key=q[i],x=key%w,y=Math.floor(key/w);for(const [dx,dy]of[[1,0],[-1,0],[0,1],[0,-1]]){const nx=x+dx,ny=y+dy,k=ny*w+nx;if(!open(nx,ny)||seen[k])continue;seen[k]=1;distance[k]=distance[key]+1;q.push(k)}}
  return {seen,distance,has:p=>!!seen[p.y*w+p.x],count:q.length};
}
function nodePositions(p,expansion=false){
  const rotation=p.rotation||0,spots=expansion?[[2,0],[2,1],[1,2],[0,2],[-1,2],[-2,-1]]:[[3,0],[3,1],[2,2],[1,3],[0,3],[-1,3],[-3,1]];
  return spots.map(([x,y])=>({x:p.x+Math.round(x*Math.cos(rotation)-y*Math.sin(rotation)),y:p.y+Math.round(x*Math.sin(rotation)+y*Math.cos(rotation))}));
}
test('six preserved legacy maps and eight dedicated strategic battlefields have honest capacities',()=>{
  assert.equal(maps.definitions.length,14);assert.equal(strategic.length,8);
  for(const d of maps.definitions){const l=maps.generate(d.id,0);assert.equal(l.starts.length,d.capacity,d.id);assert.equal(l.width,d.width);assert.equal(l.height,d.height);assert.equal(d.playerCount,d.capacity);if(d.legacy){assert.equal(d.width,42);assert.equal(d.height,30)}else{assert.equal(d.width,({2:64,3:80,4:96,6:112,8:128})[d.capacity]);assert.equal(d.category,d.capacity>4?'expanded':d.capacity+'-player')}}
});
test('separate clients reproduce exact terrain and fixed topology for zero, signed and boundary seeds',()=>{
  const remote=client();
  for(const d of strategic)for(const seed of [0,1,-1,0x7fffffff,0xffffffff]){
    const a=maps.generate(d.id,seed),b=remote.generate(d.id,seed);assert.equal(JSON.stringify(a),JSON.stringify(b),d.id+' seed '+seed);
    const next=maps.generate(d.id,seed+1);assert.deepEqual(a.terrain.map(t=>t.blocked),next.terrain.map(t=>t.blocked),'cosmetic seeds cannot alter competitive collision');assert.equal(JSON.stringify(a.starts),JSON.stringify(next.starts));
  }
});
for(const d of strategic)test(d.name+': every faction footprint reaches resources, objectives, expansions, enemies and both base gates',()=>{
  const layout=maps.generate(d.id,0x71c4),one=flood(layout,layout.starts[0]),large=flood(layout,layout.starts[0],2);
  assert.equal(layout.terrain.length,d.width*d.height);assert.equal(one.count,layout.terrain.filter(t=>!t.blocked).length,'all walkable terrain belongs to one playable component');
  for(const p of [...layout.starts,...layout.expansions,...layout.conquestSites,layout.relic,...layout.neutralSites,...layout.forwardPositions]){assert(one.has(p),d.id+' inaccessible '+JSON.stringify(p));assert(large.has(p),d.id+' 2x2 cannot reach '+JSON.stringify(p))}
  const resourcePoints=[...layout.starts.flatMap(p=>nodePositions(p)),...layout.expansions.flatMap(p=>nodePositions(p,true))],resourceKeys=new Set(resourcePoints.map(p=>p.x+','+p.y));
  assert.equal(resourceKeys.size,resourcePoints.length,d.id+' no resource overlap');
  for(const p of resourcePoints)assert(one.has(p),d.id+' resource blocked '+JSON.stringify(p));
  for(const p of [...layout.starts,...layout.expansions,...layout.neutralSites,...layout.conquestSites,layout.relic])assert(!resourceKeys.has(p.x+','+p.y),d.id+' resource covers a structure or objective '+JSON.stringify(p));
  for(const route of layout.routes)for(const p of route.points){assert(one.has(p),d.id+' route waypoint blocked '+route.id+' '+JSON.stringify(p));assert(large.has(p),d.id+' route waypoint too small '+route.id+' '+JSON.stringify(p))}
  for(const choke of layout.chokes)for(const p of [choke,...choke.staging]){assert(one.has(p),d.id+' choke or staging blocked '+choke.id+' '+JSON.stringify(p));assert(large.has(p),d.id+' choke or staging lacks clearance '+choke.id+' '+JSON.stringify(p))}
  for(const b of layout.bases){assert.equal(b.entrances.length,2);let free=0;for(let y=b.y-7;y<=b.y+7;y++)for(let x=b.x-7;x<=b.x+7;x++)if(!layout.terrain[y*d.width+x].blocked)free++;assert(free>=175,'room for buildings and an assembling army');for(const gate of b.entrances){assert(one.has(gate));assert(large.has(gate));assert(one.has(gate.staging));assert(large.has(gate.staging));assert(Math.hypot(gate.staging.x-b.x,gate.staging.y-b.y)>b.radius+5,'staging is outside the base')}
    const rim=Array.from({length:360},(_,i)=>{const a=i*Math.PI/180,x=Math.round(b.x+Math.cos(a)*10.5),y=Math.round(b.y+Math.sin(a)*10.5);return !layout.terrain[y*d.width+x]?.blocked});
    assert.equal(rim.filter((open,i)=>open&&!rim[(i+359)%360]).length,2,d.id+' exactly two gaps in base wall '+b.player);
  }
  const ratio=one.count/layout.terrain.length;assert(ratio>.18&&ratio<.7,'geography contains substantial usable terrain and meaningful barriers');
});
test('balanced natural resource access and long starts exist for every commander',()=>{
  for(const d of strategic){const l=maps.generate(d.id,55),distances=[];for(let i=0;i<l.starts.length;i++){const natural=l.expansions.filter(p=>p.naturalFor===i);assert.equal(natural.length,1,d.id+' natural count');const f=flood(l,l.starts[i]);distances.push(f.distance[natural[0].y*l.width+natural[0].x]);for(let j=0;j<l.starts.length;j++)if(j!==i){assert(Math.hypot(l.starts[i].x-l.starts[j].x,l.starts[i].y-l.starts[j].y)>=33,d.id+' spacious spawn separation');assert(f.distance[l.starts[j].y*l.width+l.starts[j].x]>=35,d.id+' useful travel distance')}}assert(Math.max(...distances)-Math.min(...distances)<=6,d.id+' natural route balance '+distances)}
});
test('authored topologies are distinct and each battlefield advertises independent flank routes',()=>{
  const hashes=new Set();for(const d of strategic){const l=maps.generate(d.id,0),digest=crypto.createHash('sha256').update(l.terrain.map(t=>t.blocked?'#':'.').join('')).digest('hex');assert(!hashes.has(digest),d.id+' duplicates another layout');hashes.add(digest);assert(l.routes.filter(r=>r.role==='flank').length>=2);assert(l.chokes.filter(c=>c.role==='base').length===d.capacity*2);assert(l.chokes.some(c=>c.role==='flank'||c.role==='main'));assert(l.expansions.some(e=>e.naturalFor===null&&e.rich));}
});
test('a sealed base entrance leaves an independent route for workers, armor and support units',()=>{
  for(const d of strategic){const original=maps.generate(d.id,0);for(const b of original.bases)for(const gate of b.entrances){
    const l=maps.generate(d.id,0);for(let dy=-3;dy<=3;dy++)for(let dx=-3;dx<=3;dx++){const x=gate.x+dx,y=gate.y+dy;if(x>=0&&y>=0&&x<l.width&&y<l.height)l.terrain[y*l.width+x].blocked=true}
    const available=b.entrances.find(e=>e.id!==gate.id),small=flood(l,b),large=flood(l,b,2);
    assert(small.has(available.staging),d.id+' infantry bypass '+gate.id);assert(large.has(available.staging),d.id+' armor/support bypass '+gate.id);
    for(const enemy of l.starts)assert(large.has(enemy),d.id+' sealed entrance isolates commander '+gate.id);
  }}
});
test('returned metadata is fresh and cannot corrupt later multiplayer layouts',()=>{
  for(const d of strategic){const a=maps.generate(d.id,4),expected=JSON.stringify(a);a.starts[0].x=-1;a.routes[0].points[0].x=-9;a.bases[0].entrances[0].staging.y=-5;a.terrain[0].blocked=false;assert.equal(JSON.stringify(maps.generate(d.id,4)),expected)}
});
