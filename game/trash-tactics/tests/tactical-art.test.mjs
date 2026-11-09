import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../engine.js';
import {drawBattleEffect} from '../battle-fx.js';
import {tabletopDetailPlacements,paintTabletopDetails} from '../tabletop-details.js';
import {drawWarfareEntityOverlay,drawWarfarePlacement,drawWarfareFields} from '../warfare-art.js';
import {FORCE_FIELD_RADIUS} from '../warfare.js';

function context(){
 const calls=[],gradient={addColorStop(){}};
 return new Proxy({calls},{get(o,k){if(k==='createLinearGradient'||k==='createRadialGradient')return ()=>gradient;if(k in o)return o[k];return (...args)=>{for(const n of args)if(typeof n==='number')assert(Number.isFinite(n),`${String(k)} has finite coordinates`);calls.push({method:k,args,color:o.strokeStyle||o.fillStyle});};},set(o,k,v){o[k]=v;return true;}});
}
test('new map illustrations are deterministic, bounded and clear of ramps, lanes and resources',()=>{
 for(const map of ['desktop','breakroom','canyon','conveyor'])for(const players of [2,3,4]){
  const g=new Game({map,players}),points=tabletopDetailPlacements(g);
  assert.deepEqual(points,tabletopDetailPlacements(g));assert(points.length>5&&points.length<=180);
  for(const p of points){
   for(const r of g.map.ramps)assert(!(p.x>r.x-75&&p.x<r.x+r.w+75&&p.y>r.y-75&&p.y<r.y+r.h+75));
   for(const resource of g.entities.filter(e=>e.type==='trash'))assert(Math.hypot(p.x-resource.x,p.y-resource.y)>=95);
   for(const lane of g.map.lanes)for(let i=1;i<lane.length;i++){
    const a=lane[i-1],b=lane[i],dx=b[0]-a[0],dy=b[1]-a[1],length=dx*dx+dy*dy,t=length?Math.max(0,Math.min(1,((p.x-a[0])*dx+(p.y-a[1])*dy)/length)):0;
    assert(Math.hypot(p.x-a[0]-t*dx,p.y-a[1]-t*dy)>=150,'the illustration footprint does not cover a main route');
   }
  }
  const c=context();paintTabletopDetails({game:g},c);assert(c.calls.length>200,'each layout receives detailed original markings');
 }
});
test('every combat family has a distinct readable impact and finite effect lifecycle',()=>{
 const renderer={visible:()=>true},signatures=[];
 for(const family of ['rock','paper','scissors']){
  const c=context();drawBattleEffect(renderer,c,{type:'hit',x:400,y:400,fromX:350,fromY:370,radius:18,family,team:0,life:.38,total:.38});
  signatures.push(JSON.stringify(c.calls));
 }
 assert.equal(new Set(signatures).size,3);
 for(const type of ['hit','death','leap','crumple','abilityCast','forcefieldPush'])for(const life of [.7,.35,0]){
  const c=context();assert(drawBattleEffect(renderer,c,{type,x:400,y:400,sourceType:'mage',life,total:.7,team:0}));assert(c.calls.length>10);
 }
 const c=context();assert(drawBattleEffect({visible:()=>false},c,{type:'hit',x:5000,y:400,life:.3,total:.38}));assert.equal(c.calls.length,0,'offscreen bursts incur no drawing');
});

test('forcefield displacement streaks follow the actual push and conceal an unseen origin',()=>{
 const effect={type:'forcefieldPush',x:420,y:300,fromX:340,fromY:300,radius:18,team:0,life:.4,total:.4},renderer={visible:()=>true,pointVisible:()=>true},c=context();
 assert(drawBattleEffect(renderer,c,effect));
 assert(c.calls.some(q=>q.method==='moveTo'&&q.args[0]===340&&q.args[1]===290));
 assert(c.calls.some(q=>q.method==='lineTo'&&q.args[0]===420&&q.args[1]===290),'streak terminates at the actual displaced troop position');
 renderer.pointVisible=x=>x>=400;const hidden=context();drawBattleEffect(renderer,hidden,effect);
 assert(!hidden.calls.some(q=>q.method==='moveTo'&&q.args[0]===340),'a newly visible troop does not reveal its hidden origin');
 assert(hidden.calls.some(q=>q.method==='ellipse'&&q.args[0]===420),'arrival pulse still acknowledges the push');
});
test('crash warning shows the impact radius only on currently visible ground',()=>{
 const e={id:1,type:'airplane',team:1,x:300,y:300,crash:{x:400,y:400,diving:true,remaining:.3}},renderer={time:2,game:{},camera:{zoom:1},pointVisible:()=>true};
 const seen=context();drawWarfareEntityOverlay(renderer,seen,e,false);
 assert(seen.calls.some(q=>q.method==='ellipse'&&q.args[0]===400&&q.args[1]===400&&q.args[2]===75),'unselected visible hostile aircraft warns of its true impact footprint');
 renderer.pointVisible=()=>false;const hidden=context();drawWarfareEntityOverlay(renderer,hidden,e,false);
 assert(!hidden.calls.some(q=>q.method==='ellipse'&&q.args[0]===400&&q.args[1]===400),'an unscouted destination is not revealed');
});

test('forcefield placement and live artwork share the smaller simulation footprint',()=>{
 assert.equal(FORCE_FIELD_RADIUS,80);
 const game={entities:[{id:1,type:'mage',team:0,hp:100,x:300,y:300}],forceFields:[{x:400,y:400,team:0,remaining:10}]},renderer={game,time:1,visible:()=>true,pointVisible:()=>true,camera:{zoom:1}},preview=context(),live=context();
 drawWarfarePlacement(renderer,preview,{mode:'specialist',abilityType:'forcefield',selected:new Set([1]),pointer:{x:400,y:400}});
 drawWarfareFields(renderer,live,'ground');
 for(const c of [preview,live])assert(c.calls.some(q=>q.method==='ellipse'&&q.args[0]===400&&q.args[1]===400&&q.args[2]===FORCE_FIELD_RADIUS&&q.args[3]===FORCE_FIELD_RADIUS));
});

test('queued casts show an approach cue without exposing hidden targets or hostile orders',()=>{
 const target={id:2,type:'paper',team:1,x:1200,y:500},e={id:1,type:'sniper',team:0,x:300,y:300,pendingAbility:{name:'snipe',target:{targetId:2},range:700}},renderer={game:{getEntity:()=>target},time:1,camera:{zoom:1},pointVisible:()=>true,entityVisible:()=>true};
 const seen=context();drawWarfareEntityOverlay(renderer,seen,e,true);
 assert(seen.calls.some(q=>q.method==='lineTo'&&q.args[0]===target.x&&q.args[1]===target.y));
 assert(seen.calls.some(q=>q.method==='fillText'&&q.args[0]==='MOVING TO CAST'));
 renderer.entityVisible=()=>false;const hidden=context();drawWarfareEntityOverlay(renderer,hidden,e,true);
 assert(!hidden.calls.some(q=>q.method==='lineTo'&&q.args[0]===target.x&&q.args[1]===target.y));
 assert(!hidden.calls.some(q=>q.method==='ellipse'&&q.args[0]===target.x&&q.args[1]===target.y));
 e.team=1;const enemy=context();drawWarfareEntityOverlay(renderer,enemy,e,true);assert.equal(enemy.calls.length,0,'enemy pending orders stay private');
});
