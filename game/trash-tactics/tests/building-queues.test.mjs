import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

function arena(){
 const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g.map.cliffs=[];g._collisionBuildings=null;
 g.canSee=()=>true;g.teams[0].trash=10000;return g;
}
function advance(g,seconds){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.04,seconds-t);g.update(dt);t+=dt;}}

test('one Twig queues many mixed paid structures and finishes them in placement order',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000);g._spawn('printer',0,750,950);
 const types=['cabinet','mouse','printer','cdreader','workshop','desk','wall'],completed=[];
 g.onEvent=e=>{if(e.type==='buildingCompleted')completed.push(e.entity.id);};
 const jobs=types.map((type,i)=>g.build([worker.id],type,1150+i*190,1000,{queue:true}));assert(jobs.every(r=>r.ok));
 assert(!jobs[0].queued);assert(jobs.slice(1).every(r=>r.queued));assert.equal(worker.order.targetId,jobs[0].entityId);
 const queue=g.getBuildQueue(worker.id);assert.deepEqual(queue.map(j=>j.type),types);assert.equal(queue[0].status,'active');assert(queue.slice(1).every(j=>j.status==='queued'));
 assert.equal(g.teams[0].trash,10000-types.reduce((sum,type)=>sum+TYPES[type].cost,0));
 advance(g,120);assert.deepEqual(completed,jobs.map(r=>r.entityId));assert.deepEqual(g.getBuildQueue(worker.id),[]);assert(jobs.every(r=>g.getEntity(r.entityId).constructing===0));
});

test('placing a queued job preserves current construction progress and does not pay twice at activation',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000),first=g.build([worker.id],'printer',1100,1000,{queue:true});
 advance(g,2);const progress=g.getEntity(first.entityId).constructing;
 const second=g.build([worker.id],'cabinet',1300,1000,{queue:true}),bank=g.teams[0].trash;
 assert(second.queued);assert.equal(g.getEntity(first.entityId).constructing,progress);assert.equal(worker.order.targetId,first.entityId);
 advance(g,24);assert.equal(g.getEntity(second.entityId).constructing,0);assert.equal(g.teams[0].trash,bank);
});

test('dead queued blueprints are skipped while following paid jobs continue',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000),jobs=['cabinet','mouse','printer'].map((type,i)=>g.build([worker.id],type,1150+i*190,1000,{queue:true}));
 g.getEntity(jobs[1].entityId).hp=0;const bank=g.teams[0].trash;advance(g,30);
 assert.equal(g.getEntity(jobs[0].entityId).constructing,0);assert(!g.getEntity(jobs[1].entityId));assert.equal(g.getEntity(jobs[2].entityId).constructing,0);assert.equal(g.teams[0].trash,bank);
});

test('Stop and new movement release all assignments while retaining paid blueprints for another worker',()=>{
 for(const order of [{type:'stop'},{type:'move',x:900,y:1200}]){
  const g=arena(),worker=g._spawn('twig',0,1000,1000),jobs=['printer','cabinet','mouse'].map((type,i)=>g.build([worker.id],type,1150+i*190,1000,{queue:true})),bank=g.teams[0].trash;
  advance(g,1);g.command([worker.id],order);assert.deepEqual(g.getBuildQueue(worker.id),[]);
  for(const result of jobs){const b=g.getEntity(result.entityId);assert(b.constructing>0);assert.equal(b.builderId,undefined);}
  const replacement=g._spawn('twig',0,1100,1100);assert(g.command([replacement.id],{type:'build',targetId:jobs[0].entityId}).ok);advance(g,15);
  assert.equal(g.getEntity(jobs[0].entityId).constructing,0);assert.equal(g.teams[0].trash,bank);
 }
});

test('worker death releases active and pending paid blueprints without losing their construction progress',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000),first=g.build([worker.id],'printer',1100,1000,{queue:true}),second=g.build([worker.id],'cabinet',1320,1000,{queue:true});
 advance(g,2);const remaining=g.getEntity(first.entityId).constructing,bank=g.teams[0].trash;worker.hp=0;advance(g,.08);
 assert.equal(g.getEntity(first.entityId).constructing,remaining);assert.equal(g.getEntity(first.entityId).builderId,undefined);assert.equal(g.getEntity(second.entityId).builderId,undefined);
 const replacement=g._spawn('twig',0,1000,1000);g.command([replacement.id],{type:'build',targetId:first.entityId});advance(g,13);
 assert.equal(g.getEntity(first.entityId).constructing,0);assert.equal(g.teams[0].trash,bank);
});

test('worker choice and takeover are deterministic and cannot construct one blueprint twice',()=>{
 const g=arena(),a=g._spawn('twig',0,1000,1000),b=g._spawn('twig',0,1000,1000);
 const first=g.build([b.id,a.id,b.id],'printer',1150,1000,{queue:true});assert.equal(g.getEntity(first.entityId).builderId,a.id);
 const next=g.build([a.id,b.id],'cabinet',1350,1000,{queue:true});assert.equal(g.getEntity(next.entityId).builderId,b.id);
 assert(g.command([b.id],{type:'build',targetId:first.entityId}).ok);assert.equal(g.getEntity(first.entityId).builderId,b.id);assert.notEqual(a.order.targetId,first.entityId);assert.equal(g.getEntity(next.entityId).builderId,undefined);
 const before=g.getEntity(first.entityId).constructing;g.update(.08);assert(g.getEntity(first.entityId).constructing>=before-.08000001);
});

test('failed prerequisites and unaffordable placements leave the paid queue and balance unchanged',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000),first=g.build([worker.id],'printer',1150,1000,{queue:true});
 const bank=g.teams[0].trash,queue=g.getBuildQueue(worker.id);
 assert(!g.build([worker.id],'workshop',1400,1000,{queue:true}).ok,'queued printer does not satisfy a completed prerequisite');
 assert.equal(g.teams[0].trash,bank);assert.deepEqual(g.getBuildQueue(worker.id),queue);
 g.teams[0].trash=0;assert(!g.build([worker.id],'cabinet',1400,1000,{queue:true}).ok);assert.equal(worker.order.targetId,first.entityId);
});

test('legacy ordinary placement may replace an ordinary job while walls still append automatically',()=>{
 const g=arena(),worker=g._spawn('twig',0,1000,1000),first=g.build([worker.id],'printer',1150,1000),second=g.build([worker.id],'cabinet',1350,1000);
 assert(!second.queued);assert.equal(worker.order.targetId,second.entityId);assert.equal(g.getEntity(first.entityId).builderId,undefined);
 const wall=g.build([worker.id],'wall',1536,1008);assert(wall.queued);assert.deepEqual(g.getBuildQueue(worker.id).map(j=>j.id),[second.entityId,wall.entityId]);
});

test('resuming a paid blueprint skips trapped nearer workers and cannot steal its builder on failure',()=>{
 const g=arena();
 for(let x=960;x<=1344;x+=48){g._spawn('wall',0,x,1488);g._spawn('wall',0,x,1872);}
 for(let y=1536;y<=1824;y+=48){g._spawn('wall',0,960,y);g._spawn('wall',0,1344,y);}
 const trapped=g._spawn('twig',0,1240,1680),reachable=g._spawn('twig',0,1850,1960),built=g.build([reachable.id],'printer',1536,1680,{queue:true});assert(built.ok);
 const blueprint=g.getEntity(built.entityId),before=g.getBuildQueue(reachable.id),bank=g.teams[0].trash;
 assert(Math.hypot(trapped.x-blueprint.x,trapped.y-blueprint.y)<Math.hypot(reachable.x-blueprint.x,reachable.y-blueprint.y));
 assert(!g.command([trapped.id],{type:'build',targetId:blueprint.id}).ok);assert.equal(blueprint.builderId,reachable.id);assert.deepEqual(g.getBuildQueue(reachable.id),before);assert.equal(trapped.order.type,'idle');
 assert(g.command([trapped.id,reachable.id],{type:'build',targetId:blueprint.id}).ok);assert.equal(blueprint.builderId,reachable.id);assert.notEqual(trapped.order.targetId,blueprint.id);assert.equal(g.teams[0].trash,bank);
 advance(g,15);assert.equal(blueprint.constructing,0);
});
