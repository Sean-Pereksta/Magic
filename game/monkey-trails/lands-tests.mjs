import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {normalizeLands,advanceLand} from './lands.mjs';

const save=(mode='gps')=>C.newSave('Trail Explorer','capuchin',mode,7213);
const walk=(s,meters,east=meters,north=0)=>{s.run.nextEncounter=1e8;s.run.nextCamp=1e8;return C.advance(s,meters,east,north);};

test('ten lands unlock at cumulative accepted-meter thresholds, without auto travel',()=>{
  const s=save();assert.equal(C.LANDS.length,10);assert.equal(C.landProgress(s).unlocked.length,1);
  for(const land of C.LANDS.slice(1)){
    const before=land.unlock-C.landProgress(s).totalDistance-.01;walk(s,before);
    assert.ok(!C.landProgress(s).unlocked.some(entry=>entry.id===land.id));assert.throws(()=>C.selectLand(s,land.id),/unlock/);
    walk(s,.01);assert.ok(C.landProgress(s).unlocked.some(entry=>entry.id===land.id));
    assert.equal(C.landProgress(s).active.id,'emerald-jungle');
  }
  assert.equal(C.landProgress(s).next,null);assert.equal(C.landProgress(s).remaining,0);
  assert.equal(C.landProgress(s).totalDistance,35000);assert.throws(()=>C.selectLand(s,'made-up-land'));
});

test('encounter clipping and rejected advances never bank extra world distance',()=>{
  const s=save(),used=C.advance(s,10000,10000,0);
  assert.equal(used,C.ENCOUNTER_STEP);assert.equal(s.run.phase,'battle');
  assert.equal(C.landProgress(s).totalDistance,used);assert.equal(C.landProgress(s).position.east,used);
  const before=structuredClone(s.world);
  assert.equal(C.advance(s,1000,1000,0),0);assert.throws(()=>C.selectLand(s,'sunstone-savannah'),/encounter/);
  assert.deepEqual(s.world,before);
  s.run.phase='trail';
  for(const args of [[0,0,0],[-1,-1,0],[NaN,1,0],[10,Infinity,0],[10,0,NaN]])assert.equal(C.advance(s,...args),0);
  assert.deepEqual(s.world,before);
});

test('each land keeps its distance and position through selection, reload, and restart',()=>{
  let s=save();walk(s,500,300,400);const jungle=structuredClone(C.landProgress(s).region);
  C.selectLand(s,'sunstone-savannah');assert.deepEqual(C.landProgress(s).position,{east:0,north:0});
  walk(s,100,-60,80);assert.equal(C.landProgress(s).region.distance,100);
  C.selectLand(s,'emerald-jungle');assert.deepEqual(C.landProgress(s).region,jungle);
  s=C.validate(structuredClone(s));assert.deepEqual(C.landProgress(s).position,{east:300,north:400});
  C.selectLand(s,'sunstone-savannah');assert.deepEqual(C.landProgress(s).position,{east:-60,north:80});
  s.run.phase='dead';C.restart(s);assert.equal(s.run.distance,0);
  assert.equal(C.landProgress(s).active.id,'sunstone-savannah');assert.equal(C.landProgress(s).totalDistance,600);
  assert.deepEqual(C.landProgress(s).position,{east:-60,north:80});
  walk(s,25,0,-25);assert.equal(C.landProgress(s).totalDistance,625);assert.equal(C.landProgress(s).region.distance,125);
});

test('land position survives encounter direction resets without changing encounter steering',()=>{
  const s=save();walk(s,100,60,80);s.run.vector={east:0,north:0};s.run.weights={N:0,S:0,E:0,W:0};
  walk(s,50,-50,0);assert.deepEqual(C.landProgress(s).position,{east:10,north:80});
  assert.deepEqual(s.run.vector,{east:-50,north:0});assert.equal(s.run.direction,'W');
});

test('terrain coordinates and facing follow the latest north, east, south, or west step',()=>{
  const s=save();assert.equal(C.landProgress(s).heading,0);
  for(const [east,north,position,heading] of [
    [0,20,{east:0,north:20},0],
    [20,0,{east:20,north:20},90],
    [0,-20,{east:20,north:0},180],
    [-20,0,{east:0,north:0},270]
  ]){
    walk(s,20,east,north);const p=C.landProgress(s);
    assert.deepEqual(p.position,position);assert.equal(p.heading,heading);assert.equal(p.region.heading,heading);
  }
  // The encounter's accumulated vector is zero after this loop, but facing
  // remembers the most recent accepted step for the walking-map avatar.
  assert.deepEqual(s.run.vector,{east:0,north:0});
  assert.equal(C.landProgress(s).heading,270);
});

test('each land remembers its facing through travel, reload, and expedition restart',()=>{
  let s=save();walk(s,500,500,0);assert.equal(C.landProgress(s).heading,90);
  C.selectLand(s,'sunstone-savannah');assert.equal(C.landProgress(s).heading,0);
  walk(s,25,-25,0);assert.equal(C.landProgress(s).heading,270);
  C.selectLand(s,'emerald-jungle');s=C.validate(structuredClone(s));assert.equal(C.landProgress(s).heading,90);
  C.selectLand(s,'sunstone-savannah');assert.equal(C.landProgress(s).heading,270);
  s.run.phase='dead';C.restart(s);assert.equal(C.landProgress(s).heading,270);
  // Saves written before facing was recorded remain usable.
  delete s.world.regions['sunstone-savannah'].heading;
  assert.equal(C.landProgress(C.validate(structuredClone(s))).heading,0);
});

test('facing changes only for actual accepted displacement, including clipped movement',()=>{
  const s=save();walk(s,10,0,-10);assert.equal(C.landProgress(s).heading,180);
  walk(s,10,0,0);assert.equal(C.landProgress(s).heading,180);
  for(const args of [[0,1,0],[-1,1,0],[NaN,1,0],[10,Infinity,0],[10,0,NaN]]){
    assert.equal(C.advance(s,...args),0);assert.equal(C.landProgress(s).heading,180);
  }
  s.run.nextEncounter=s.run.distance+5;
  assert.equal(C.advance(s,100,100,0),5);
  assert.deepEqual(C.landProgress(s).position,{east:5,north:-10});assert.equal(C.landProgress(s).heading,90);
  const before=structuredClone(s.world);assert.equal(C.advance(s,10,-10,0),0);assert.deepEqual(s.world,before);
  s.run.phase='trail';s.world.regions['emerald-jungle'].position={east:1e7,north:0};
  assert.equal(advanceLand(s,10,10,0),10);assert.equal(C.landProgress(s).heading,90);
  // Hitting the coordinate limit cannot turn the avatar without moving it.
  s.world.regions['emerald-jungle'].heading=180;
  advanceLand(s,10,10,0);assert.equal(C.landProgress(s).heading,180);
  advanceLand(s,10,10,10);assert.equal(C.landProgress(s).position.east,1e7);assert.ok(Math.abs(C.landProgress(s).position.north-Math.SQRT1_2*10)<1e-12);
  assert.equal(C.landProgress(s).heading,0);
});

test('legacy migration retains verifiable distance once and never treats score as meters',()=>{
  let s=save();delete s.world;s.run.distance=200;s.run.vector={east:60,north:80};s.meta.best=999999;
  s.meta.history=[{id:'old',feet:2000,mode:'gps'},{id:'practice',feet:99999,mode:'practice'}];
  s=C.validate(s);assert.equal(C.landProgress(s).totalDistance,609.6);assert.deepEqual(C.landProgress(s).position,{east:60,north:80});
  const before=structuredClone(s.world);s=C.validate(structuredClone(s));assert.deepEqual(s.world,before);
  walk(s,10,10,0);assert.equal(C.landProgress(s).totalDistance,619.6);
  const fresh=save();delete fresh.world;fresh.meta.best=1000000;normalizeLands(fresh);assert.equal(C.landProgress(fresh).totalDistance,0);
});

test('practice progress belongs to its own save and does not unlock GPS lands',()=>{
  const gps=save('gps'),practice=save('practice');walk(practice,35000);C.selectLand(practice,'ancient-monkey-kingdom');
  const restored=C.validate(structuredClone(practice));assert.equal(C.landProgress(restored).active.id,'ancient-monkey-kingdom');
  assert.equal(C.landProgress(gps).totalDistance,0);assert.equal(C.landProgress(gps).unlocked.length,1);
  assert.throws(()=>C.selectLand(gps,'sunstone-savannah'),/unlock/);walk(gps,500);assert.equal(C.landProgress(practice).totalDistance,35000);
});

test('restored landscape data stays finite and bounded; oversized vectors cannot teleport',()=>{
  const s=save();walk(s,50,5000,0);assert.deepEqual(C.landProgress(s).position,{east:50,north:0});
  s.world.totalDistance=Infinity;s.world.activeLand='ancient-monkey-kingdom';
  s.world.regions['emerald-jungle']={distance:-50,heading:Infinity,position:{east:Infinity,north:NaN},trail:Array.from({length:500},()=>({east:1,north:2}))};
  normalizeLands(s);const p=C.landProgress(s);assert.equal(p.totalDistance,0);assert.equal(p.active.id,'emerald-jungle');
  assert.equal(p.region.distance,0);assert.deepEqual(p.position,{east:0,north:0});assert.equal(p.region.trail.length,128);
  assert.equal(p.heading,0);p.region.heading=359.25;normalizeLands(s);assert.equal(C.landProgress(s).heading,359.25);
  assert.ok(JSON.stringify(s.world).length<10000);
});
