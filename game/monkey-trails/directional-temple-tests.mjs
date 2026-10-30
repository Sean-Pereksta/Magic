import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as W from './living-world.mjs';
import {specialLocationUI} from './living-world-ui.mjs';
import {LANDS,normalizeLands,selectLand} from './lands.mjs';
const close=(a,b)=>assert.ok(Math.abs(a-b)<1e-6,`${a} != ${b}`);
function fixture(direction='east',visit=true){
 const s=C.newSave('Temple walk','capuchin','practice',42);s.run.nextCamp=s.run.nextEncounter=1e9;
 C.advance(s,3500*C.FEET);const row=W.specialLocations(s)[0];s.meta.livingWorld.locations[row.id].definitionId=`emerald-jungle:temple-${direction}`;
 s.world.regions[s.world.activeLand].position={...row.position};if(visit)W.visitLocation(s,row.id);
 return{s,id:row.id};
}
const walk=(s,feet,east=0,north=0)=>C.advance(s,feet*C.FEET,east*C.FEET,north*C.FEET);
test('each cardinal temple counts only its accepted directional component and grants distinct supplies once',()=>{
 for(const d of W.TEMPLE_DIRECTIONS){let {s,id}=fixture(d.id);const vector=n=>d.axis==='east'?[n*d.sign,0]:[0,n*d.sign];
  walk(s,100,...vector(-100));assert.equal(W.specialLocation(s,id).challenge.current,0);
  walk(s,100,...(d.axis==='east'?[0,100]:[100,0]));assert.equal(W.specialLocation(s,id).challenge.current,0);
  walk(s,100,...vector(100));close(W.specialLocation(s,id).challenge.current,100);
  s=C.validate(JSON.parse(JSON.stringify(s)));const remaining=W.specialLocation(s,id).goalFeet-100;walk(s,remaining,...vector(remaining));
  const view=W.specialLocation(s,id);assert.equal(view.ready,true);const before=s.run.items[d.item]||0,receipt=W.claimLocation(s,id);assert.equal(s.run.items[d.item],before+1);assert.ok(receipt.reward.essence[d.element]>=2);
  const saved=JSON.stringify(s);assert.throws(()=>W.claimLocation(s,id));assert.equal(JSON.stringify(s),saved);
 }
});
test('diagonal projection, GPS chunk sizes and oversized vectors cannot multiply progress',()=>{
 let {s,id}=fixture();walk(s,100,60,80);close(W.specialLocation(s,id).challenge.current,60);
 const other=C.validate(JSON.parse(JSON.stringify(s)));walk(s,200,120,160);for(let i=0;i<4;i++)walk(other,50,30,40);
 close(W.specialLocation(s,id).challenge.current,W.specialLocation(other,id).challenge.current);
 const before=W.specialLocation(s,id).challenge.current;walk(s,100,1000,0);close(W.specialLocation(s,id).challenge.current-before,100);
});
test('unvisited temples, battles and walking in another region do not advance the challenge',()=>{
 const {s,id}=fixture('north',false);walk(s,300,0,300);assert.equal(W.specialLocation(s,id).challenge.current,0);const position=W.specialLocation(s,id).position;s.world.regions[s.world.activeLand].position={...position};W.visitLocation(s,id);
 s.run.phase='battle';assert.equal(walk(s,100,0,100),0);s.run.phase='trail';s.world.totalDistance=150000;normalizeLands(s);selectLand(s,LANDS[1].id);walk(s,100,0,100);assert.equal(W.specialLocation(s,id).challenge.current,0);
});
test('flexible route keeps earned feet and requires a longer goal for identical rewards',()=>{
 let {s,id}=fixture();walk(s,100,100,0);let row=W.useTempleFlexibleRoute(s,id);assert.equal(row.challenge.goal,1000);close(row.challenge.current,100);
 s=C.validate(JSON.parse(JSON.stringify(s)));walk(s,900,-900,0);row=W.specialLocation(s,id);assert.equal(row.ready,true);
 const html=specialLocationUI(s,id);assert.match(html,/Any safe route/);assert.match(html,/Guaranteed reward/);const receipt=W.claimLocation(s,id);assert.equal(receipt.reward.items.bondCharm,1);
});
test('the first four follow-up destinations introduce all four directional temples',()=>{
 const s=C.newSave('Temple sequence','capuchin','practice',22);s.run.nextCamp=s.run.nextEncounter=1e9;
 for(let serial=1;serial<=5;serial++){walk(s,3500,0,3500);const rows=W.specialLocations(s),row=rows.find(r=>r.serial===serial);assert.ok(row);if(serial>1)assert.equal(row.direction,W.TEMPLE_DIRECTIONS[serial-2].id);W.dismissLocation(s,row.id);}
});
test('extreme finite vectors cannot overflow directional progress',()=>{
 const {s,id}=fixture();W.advanceLivingWorld(s,10,Number.MAX_VALUE,Number.MAX_VALUE);assert.equal(W.specialLocation(s,id).challenge.current,0);
});
