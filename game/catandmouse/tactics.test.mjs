import test from 'node:test';
import assert from 'node:assert/strict';
import { findOpenSpawn, createReservations, createTacticalDirector, scoreObjective } from './tactics.mjs';

function harness({blocked=new Set(),maxDecisions=5,maxPaths=10}={}) {
  let time=0,revision=0,objectives=[{key:'mouse:a',type:'mouse',x:8,y:3,value:0}];
  const units=[],key=(x,y)=>`${x},${y}`,passable=(_kind,x,y)=>x>=0&&y>=0&&x<12&&y<12&&!blocked.has(key(x,y));
  const path=(x,y,tx,ty,allowed)=>{
    const queue=[{x,y,path:[]}],seen=new Set([key(x,y)]);
    for(let i=0;i<queue.length;i++){
      const p=queue[i];if(p.x===tx && p.y===ty)return p.path;
      for(const [dx,dy] of [[1,0],[0,1],[-1,0],[0,-1]]){
        const nx=p.x+dx,ny=p.y+dy,k=key(nx,ny);
        if(nx<0||ny<0||nx>=12||ny>=12||seen.has(k)||!allowed(nx,ny))continue;
        seen.add(k);queue.push({x:nx,y:ny,path:[...p.path,{x:nx,y:ny}]});
      }
    }return null;
  };
  const director=createTacticalDirector({size:12,now:()=>time,findPath:path,passable,targets:()=>({enemy:objectives,friendly:objectives}),occupants:()=>units,revision:()=>revision,maxDecisions:()=>maxDecisions,maxPaths,
    resolveTarget:target=>{const live=objectives.find(t=>t.key===target.key);return live?.alive===false?null:live || null;}});
  const add=(kind='rat',x=2,y=3,team='enemy')=>{const unit={key:`${kind}:${units.length}`,kind,x,y,team};units.push(unit);return unit;};
  const step=(unit,opts={})=>{const result=director.plan(unit,opts);if(result.next)Object.assign(unit,result.next);return result;};
  return {director,units,blocked,add,step,targets:items=>objectives=items,advance:ms=>time+=ms,dirty:()=>revision++};
}

test('blocked producers return without looping or spawning on their building',()=>{
  let checked=0;
  const spawn=findOpenSpawn({x:4,y:4},{size:9,passable:()=>{checked++;return false;},occupied:()=>false});
  assert.equal(spawn,null);assert.equal(checked,4);
});
test('spawns use reachable open cells, respect reservations and stay inside the board',()=>{
  const blocked=new Set(['2,1','1,2','0,1']);
  const spawn=findOpenSpawn({x:1,y:1},{size:5,passable:(x,y)=>!blocked.has(`${x},${y}`),occupied:(x,y)=>x===1&&y===0,reserved:(x,y)=>x===2&&y===0});
  assert.deepEqual(spawn,{x:0,y:0});
  const corner=findOpenSpawn({x:0,y:0},{size:5,passable:()=>true,occupied:()=>false});
  assert.deepEqual(corner,{x:1,y:0});
});
test('reservations expire and can be released after a unit disappears',()=>{
  let now=0;const r=createReservations({now:()=>now,ttlMs:300});
  r.reserve(2,3,'one');assert.equal(r.blocked(2,3,'two'),true);assert.equal(r.blocked(2,3,'one'),false);
  now=301;assert.equal(r.blocked(2,3,'two'),false);assert.equal(r.size,0);
  r.reserve(1,1,'one');r.release('one');assert.equal(r.size,0);
});
test('target commitment prevents oscillation between nearly equivalent objectives',()=>{
  const h=harness(),unit=h.add();
  h.targets([{key:'a',type:'mouse',x:7,y:3,value:0},{key:'b',type:'mouse',x:8,y:3,value:0}]);
  assert.equal(h.step(unit,{stopRange:20}).target.key,'a');
  h.targets([{key:'a',type:'mouse',x:8,y:3,value:0},{key:'b',type:'mouse',x:7,y:3,value:0}]);h.advance(700);
  assert.equal(h.step(unit,{stopRange:20}).target.key,'a');
});
test('dead targets are released immediately, even when no thinking budget remains',()=>{
  const h=harness({maxDecisions:1}),unit=h.add();
  const result=h.step(unit);assert.equal(result.target.key,'mouse:a');
  h.targets([{key:'mouse:a',type:'mouse',x:8,y:3,alive:false}]);
  assert.equal(h.step(unit).target,null);
});
test('dramatically higher priority threats can override commitment',()=>{
  const h=harness(),unit=h.add();h.step(unit,{stopRange:20});h.advance(700);
  h.targets([{key:'mouse:a',type:'mouse',x:8,y:3},{key:'boss',type:'structure',x:3,y:3,value:80}]);
  assert.equal(h.step(unit,{stopRange:20}).target.key,'boss');
});
test('each plan moves only one adjacent step and reuses valid routes',()=>{
  const h=harness(),unit=h.add();const first=h.step(unit);assert.deepEqual(first.next,{x:3,y:3});
  const paths=h.director.stats.paths;h.advance(220);h.step(unit);
  assert.equal(h.director.stats.paths,paths);assert.equal(unit.x,4);
});
test('a blocked route is repathed; enclosed mice produce reachable breach targets',()=>{
  const h=harness(),unit=h.add();h.step(unit);h.blocked.add('4,3');h.dirty();h.advance(250);
  const result=h.step(unit);assert.ok(result.next);assert.notDeepEqual(result.next,{x:4,y:3});
  const siege=harness(),rat=siege.add('rat',2,3);
  for(const p of ['7,3','8,2','9,3','8,4'])siege.blocked.add(p);
  siege.targets([{key:'mouse:a',type:'mouse',x:8,y:3},{key:'wall',type:'structure',x:7,y:3,wall:true}]);
  assert.equal(siege.step(rat).target.key,'wall');
});
test('crowds reserve destinations and never trade places through each other',()=>{
  const h=harness(),a=h.add('rat',2,3),b=h.add('rat',2,4);
  const first=h.step(a),second=h.step(b);assert.notDeepEqual(first.next,second.next);
  const swap=harness(),left=swap.add('rat',2,3),right=swap.add('rat',3,3);
  swap.targets([{key:'l',type:'mouse',x:1,y:3},{key:'r',type:'mouse',x:5,y:3}]);
  const before={x:right.x,y:right.y};const result=swap.step(left);
  if(result.next)assert.notDeepEqual(result.next,before);
});
test('stuck units recover after local congestion clears and reservations expire',()=>{
  const h=harness(),unit=h.add();
  h.director.reservations.reserve(3,3,'block',1200);h.director.reservations.reserve(2,2,'block',1200);
  h.director.reservations.reserve(2,4,'block',1200);h.director.reservations.reserve(1,3,'block',1200);
  for(let i=0;i<3;i++){assert.equal(h.step(unit).next,null);h.advance(300);}
  h.advance(500);assert.ok(h.step(unit).next);
});
test('shared decision and path budgets remain bounded across all enemy types',()=>{
  const h=harness({maxDecisions:5,maxPaths:6});
  for(let i=0;i<22;i++)h.add(['rat','ox','vulture','ratking','stinkrat','termite'][i%6],1,1+i%9);
  for(const unit of h.units)h.step(unit);
  assert.ok(h.director.stats.decisions<=5);assert.ok(h.director.stats.paths<=6);
  assert.ok(h.director.stats.peakDecisions<=5);assert.ok(h.director.stats.peakPaths<=6);
});
test('enemy profiles prefer their intended roles; allies favor nearby player threats',()=>{
  const unit={key:'x',kind:'ox',team:'enemy',x:0,y:0};
  const wall={key:'wall',type:'structure',x:3,y:0,wall:true},economy={key:'econ',type:'structure',x:3,y:0,economy:true};
  assert.ok(scoreObjective(unit,wall)>scoreObjective(unit,economy));
  unit.kind='vulture';assert.ok(scoreObjective(unit,economy)>scoreObjective(unit,wall));
  unit.kind='stinkrat';assert.ok(scoreObjective(unit,{...wall,disruptable:true})>scoreObjective(unit,{...wall,disruptable:true,disabled:true}));
  unit.team='friendly';unit.kind='rabbit';assert.ok(scoreObjective(unit,{...economy,playerThreat:true})>scoreObjective(unit,economy));
});
test('structure specialists stay in contact range and do not repeatedly repath into walls',()=>{
  const h=harness(),termite=h.add('termite',2,3);h.blocked.add('3,3');
  h.targets([{key:'wall',type:'structure',x:3,y:3,wall:true},{key:'mouse',type:'mouse',x:2,y:3}]);
  const result=h.step(termite);assert.equal(result.target.key,'wall');assert.equal(result.next,null);
  h.advance(250);h.step(termite);assert.equal(h.director.stats.paths,0);
});

test('movement-only planning resolves one current target instead of every building on every tick',()=>{
  const h=harness(),unit=h.add();
  h.targets(Array.from({length:500},(_,i)=>({key:String(i),type:'structure',x:3+i%9,y:3+Math.floor(i/9)%8,wall:true})));
  h.step(unit,{stopRange:99,reactionMs:100000});const before=h.director.stats.targetResolutions;
  for(let i=0;i<1000;i++)h.step(unit,{stopRange:99,reactionMs:100000});
  assert.equal(h.director.stats.targetResolutions-before,1000);assert.equal(h.director.stats.decisions,1);
});
