import test from 'node:test';
import assert from 'node:assert/strict';
import { createMotionTrack, retargetMotion, sampleMotion, sampleProjectile, MOTION_PROFILES } from './motion.mjs';

test('interpolation never mutates or aliases authoritative coordinates',()=>{
  const first=Object.freeze({x:2,y:3}),next=Object.freeze({x:3,y:3});
  const track=createMotionTrack(first,0,'rat');retargetMotion(track,next,100,{duration:300});
  const halfway=sampleMotion(track,250);
  assert.ok(halfway.x>2 && halfway.x<3);assert.equal(halfway.y,3);
  assert.deepEqual(first,{x:2,y:3});assert.deepEqual(next,{x:3,y:3});
  assert.notEqual(track.grid,next);assert.notEqual(track.rendered,track.grid);
  assert.deepEqual(sampleMotion(track,450),{x:3,y:3,moving:false,lift:Math.sin(Math.PI)*MOTION_PROFILES.rat.hop});
});
test('retargeting starts at the sampled rendered position without a backwards jump',()=>{
  const track=createMotionTrack({x:0,y:0},0,'mouse');
  retargetMotion(track,{x:1,y:0},10,{duration:120});
  const current=sampleMotion(track,60).x;
  retargetMotion(track,{x:2,y:0},60);
  assert.equal(sampleMotion(track,60).x,current);
  assert.equal(sampleMotion(track,400).x,2);
});
test('facing-only snapshots do not restart travel, local input begins immediately',()=>{
  const track=createMotionTrack({x:0,y:0},0,'mouse');
  retargetMotion(track,{x:1,y:0},10,{local:true});
  assert.equal(track.duration,85);
  assert.equal(retargetMotion(track,{x:1,y:0,facing:'west'},30),false);
  assert.equal(track.startedAt,10);assert.equal(track.facing,'west');assert.ok(sampleMotion(track,31).x>0);
});
test('large corrections catch up quickly; teleports and invalid input are safe',()=>{
  const track=createMotionTrack({x:0,y:0},0,'ox');
  retargetMotion(track,{x:3,y:0},10);assert.equal(track.duration,110);
  assert.equal(sampleMotion(track,150).x,3);
  retargetMotion(track,{x:25,y:25},200);assert.equal(track.duration,0);assert.equal(sampleMotion(track,200).x,25);
  retargetMotion(track,{x:24,y:25},210,{teleport:true});assert.equal(sampleMotion(track,210).x,24);
  assert.equal(retargetMotion(track,{x:NaN,y:1},300),false);assert.equal(track.grid.x,24);
});
test('distinct body motion respects reduced motion without dropping travel',()=>{
  const rabbit=createMotionTrack({x:0,y:0},0,'rabbit'),ox=createMotionTrack({x:0,y:0},0,'ox');
  retargetMotion(rabbit,{x:1,y:0},10,{duration:400});retargetMotion(ox,{x:1,y:0},10,{duration:400});
  assert.ok(sampleMotion(rabbit,210).lift>sampleMotion(ox,210).lift);
  const normal=sampleMotion(rabbit,210),reduced=sampleMotion(rabbit,210,true);
  assert.equal(normal.x,reduced.x);assert.equal(reduced.lift,0);
});
test('projectiles travel continuously and finish exactly at the gameplay destination',()=>{
  const shot=Object.freeze({from:Object.freeze({x:1,y:2}),to:Object.freeze({x:6,y:4}),startedAt:100,duration:300,arc:12});
  assert.deepEqual(sampleProjectile(shot,100),{x:1,y:2,lift:0,finished:false});
  const middle=sampleProjectile(shot,250);assert.equal(middle.x,3.5);assert.equal(middle.lift,12);
  assert.equal(sampleProjectile(shot,450).x,6);assert.equal(sampleProjectile(shot,450).finished,true);
  assert.equal(sampleProjectile(shot,250,true).lift,0);
});
