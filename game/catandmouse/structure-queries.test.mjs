import test from 'node:test';
import assert from 'node:assert/strict';
import { createStructureQueries } from './structure-queries.mjs';

function fixture(){
  const cells=new Map();let revision=0,time=0;
  const add=(x,y,type)=>{cells.set(`${x}_${y}`,{x,y,type,key:`${x}_${y}`});revision++;};
  const queries=createStructureQueries({size:30,lookup:(x,y)=>cells.get(`${x}_${y}`),revision:()=>revision,now:()=>time,disabledTypes:['🔫','⚡'],hash:()=>0});
  return {queries,cells,add,advance:ms=>time+=ms,remove(x,y){cells.delete(`${x}_${y}`);revision++;}};
}
test('stink targeting keeps the distance/key rotation while inspecting only nearby cells once per window',()=>{
  const h=fixture();h.add(9,10,'🔫');h.add(11,10,'⚡');h.add(20,20,'🔫');h.add(10,11,'🛡️');
  const rat={id:'r',x:10,y:10,health:5};assert.equal(h.queries.stinkTarget(rat),'11_10');
  const reads=h.queries.stats.cellReads;for(let i=0;i<1000;i++)assert.equal(h.queries.stinkTarget(rat),'11_10');
  assert.equal(h.queries.stats.cellReads,reads);assert.ok(reads<=13);
  h.advance(5000);assert.equal(h.queries.stinkTarget(rat),'9_10');assert.equal(h.queries.stats.stinkBuilds,2);
});
test('movement, death and topology changes invalidate stink cues without invalidating them for health changes',()=>{
  const h=fixture();h.add(11,10,'🔫');const rat={id:'r',x:10,y:10,health:5};
  assert.equal(h.queries.stinkTarget(rat),'11_10');h.cells.get('11_10').health=2;h.queries.stinkTarget(rat);assert.equal(h.queries.stats.stinkBuilds,1);
  rat.x=15;assert.equal(h.queries.stinkTarget(rat),null);rat.x=10;h.remove(11,10);assert.equal(h.queries.stinkTarget(rat),null);
  h.add(10,11,'🔫');assert.equal(h.queries.stinkTarget(rat),'10_11');rat.health=0;assert.equal(h.queries.stinkTarget(rat),null);
});
test('shield coverage matches Manhattan range, reuses health-only updates and refreshes after a tower removal',()=>{
  const h=fixture(),st={x:10,y:10};h.add(14,10,'🛡️');h.add(13,12,'🛡️');h.add(8,8,'🛡️');
  assert.equal(h.queries.shieldSupports(st).length,2);const reads=h.queries.stats.cellReads;assert.ok(reads<=41);
  for(let i=0;i<1000;i++)h.queries.shieldSupports(st);assert.equal(h.queries.stats.cellReads,reads);
  h.remove(14,10);assert.equal(h.queries.shieldSupports(st).length,1);
});
test('nearby lookups stay within the board and stink cache stays bounded across retired wave IDs',()=>{
  const h=fixture();h.add(0,0,'🔫');assert.equal(h.queries.nearby(0,0,2).length,1);assert.equal(h.queries.stats.cellReads,6);
  for(let i=0;i<1000;i++)h.queries.stinkTarget({id:String(i),x:1,y:1,health:5});assert.ok(h.queries.stats.stinkCached<=16);
});
