const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const context={};
vm.runInNewContext(fs.readFileSync(path.resolve(__dirname,'../maps.js'),'utf8'),context);
const {definitions,generate}=context.TurncraftMaps;
const newMaps=definitions.filter(d=>d.id!=='classic');
const key=p=>`${p.x},${p.y}`;
function reachable(map,radius=0){
  const open=(x,y)=>{for(let dy=-radius;dy<=radius;dy++)for(let dx=-radius;dx<=radius;dx++){const t=map.terrain[(y+dy)*map.width+x+dx];if(x+dx<0||x+dx>=map.width||y+dy<0||y+dy>=map.height||!t||t.blocked)return false}return true};
  const queue=[map.starts[0]],seen=new Set([key(queue[0])]);
  assert.ok(open(queue[0].x,queue[0].y));
  for(let i=0;i<queue.length;i++)for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){const p={x:queue[i].x+dx,y:queue[i].y+dy};if(!seen.has(key(p))&&open(p.x,p.y)){seen.add(key(p));queue.push(p)}}
  return seen;
}
function resourceFootprints(map){
  const nodes=[],push=(origin,dx,dy,rotation,type)=>nodes.push({x:origin.x+Math.round(dx*Math.cos(rotation)-dy*Math.sin(rotation)),y:origin.y+Math.round(dx*Math.sin(rotation)+dy*Math.cos(rotation)),type});
  map.starts.forEach((start,i)=>{const rotation=[0,Math.PI/2,-Math.PI/2][i];for(const [dx,dy] of [[3,0],[3,1],[2,2],[1,3],[0,3],[-1,3]])push(start,dx,dy,rotation,'mineral');push(start,-3,1,rotation,'gas')});
  for(const expansion of map.expansions){for(const [dx,dy] of [[2,0],[2,1],[1,2],[0,2],[-1,2]])push(expansion,dx,dy,expansion.rotation,'mineral');push(expansion,-2,-1,expansion.rotation,'gas')}
  return nodes;
}

test('map catalog preserves Classic and offers five distinct deterministic battlefields',()=>{
  assert.equal(definitions.length,6);assert.equal(newMaps.length,5);assert.equal(generate('classic',123).terrain,null);
  assert.equal(generate('missing-map',123).id,'classic');
  const masks=[];
  for(const definition of newMaps){
    const map=generate(definition.id,123),again=generate(definition.id,123),changed=generate(definition.id,456);
    assert.equal(JSON.stringify(map),JSON.stringify(again),definition.id);
    assert.ok(JSON.stringify(map.terrain)!==JSON.stringify(changed.terrain),`${definition.id}: seed changes floor texture`);
    assert.equal(map.width,42);assert.equal(map.height,30);assert.equal(map.terrain.length,1260);
    assert.equal(map.starts.length,3);assert.equal(map.expansions.length,6);assert.equal(map.conquestSites.length,3);assert.equal(map.neutralSites.length,7);
    assert.deepEqual(Array.from(map.expansions.filter(e=>e.naturalFor!==null).map(e=>e.naturalFor).sort()),[0,1,2]);
    assert.equal(map.expansions.filter(e=>e.rich).length,1);assert.ok(definition.palette.accent&&definition.palette.rock&&definition.palette.lane);
    masks.push({id:definition.id,mask:map.terrain.map(t=>t.blocked)});
  }
  for(let i=0;i<masks.length;i++)for(let j=i+1;j<masks.length;j++)assert.ok(masks[i].mask.filter((v,k)=>v!==masks[j].mask[k]).length>=30,`${masks[i].id} and ${masks[j].id} need different obstacle topology`);
});

for(const definition of newMaps)test(`${definition.name}: connected army routes, safe economy footprints and unobstructed objectives`,()=>{
  for(const seed of [0,1,17,12345,4294967295]){
    const map=generate(definition.id,seed),seen=reachable(map),wide=reachable(map,1),points=[...map.starts,...map.expansions,...map.conquestSites,...map.neutralSites,map.relic];
    assert.ok(map.terrain.some(t=>t.blocked),definition.id);
    for(const t of map.terrain){assert.equal(t.blocked,t.type==='rock'||t.type==='crystal');if(!t.blocked)assert.ok(seen.has(key(t)),`${definition.id}: isolated tile ${key(t)}`)}
    for(const p of points){assert.ok(Number.isInteger(p.x)&&Number.isInteger(p.y));assert.ok(wide.has(key(p)),`${definition.id}: ${key(p)} lacks a three-tile-wide route`)}
    for(const [points,radius] of [[map.starts,4.6],[map.expansions,2.7]])for(const p of points)for(const t of map.terrain)if(Math.hypot(t.x-p.x,t.y-p.y)<=radius)assert.equal(t.blocked,false,`${definition.id}: clearing ${key(p)}`);
    const nodes=resourceFootprints(map),occupied=new Set(nodes.map(key));assert.equal(occupied.size,nodes.length,`${definition.id}: resources overlap`);
    for(const n of nodes){assert.ok(n.x>=1&&n.x<=40&&n.y>=1&&n.y<=28,`${definition.id}: clamped resource`);assert.equal(map.terrain[n.y*42+n.x].blocked,false,`${definition.id}: blocked resource ${key(n)}`)}
    for(const p of points)assert.equal(occupied.has(key(p)),false,`${definition.id}: placement ${key(p)} overlaps a resource`);
    const neutralKeys=new Set(map.neutralSites.map(key));for(const p of [...map.starts,...map.expansions,...map.conquestSites,map.relic])assert.equal(neutralKeys.has(key(p)),false,`${definition.id}: neutral structure occupies ${key(p)}`);
  }
});
