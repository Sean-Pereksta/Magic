import assert from 'node:assert/strict';
import test from 'node:test';
import { groundLineClear, groundRoute } from '../battle-navigation.mjs';

function fort() {
  return {width:160,depth:160,navRevision:0,obstacles:[],
    walls:[-42,42].map(x=>({x,z:5,width:76,depth:2,hp:100})),
    gate:{id:'gate',kind:'gate',x:0,z:5,width:8,depth:2,hp:100}};
}
function route(layout,start,goal) {
  const blocked=(x,z)=>!groundLineClear(layout,{x,z},{x,z},start.side);
  return groundRoute(layout,start,goal,blocked);
}

test('routes distinguish gate ownership, sealed wards, breach revisions and restored checkpoints',()=>{
  const layout=fort(), start={x:0,z:-10,side:0}, goal={x:0,z:18};
  const partial=route(layout,start,goal);
  assert.ok(partial.length && partial.every(p=>p.z<4));
  assert.deepEqual(route(layout,{...start,side:1},goal).at(-1),goal);
  layout.gate.hp=0; layout.navRevision++;
  assert.deepEqual(route(layout,start,goal).at(-1),goal);
  assert.deepEqual(route(JSON.parse(JSON.stringify(layout)),start,goal),route(layout,start,goal));
});
test('a ram near the gate corner receives a traversable first segment and a complete route',()=>{
  const layout=fort(); layout.gate.hp=0;
  const start={x:-3.6917,z:3.6266,side:0}, goal={x:0,z:31};
  assert.equal(groundLineClear(layout,start,goal,0),false);
  const path=route(layout,start,goal);
  assert.deepEqual(path.at(-1),goal);
  let from=start;
  for (const step of path) { assert.equal(groundLineClear(layout,from,step,0),true); from=step; }
});
