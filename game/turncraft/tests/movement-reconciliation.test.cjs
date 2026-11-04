const {test}=require('node:test');
const assert=require('node:assert/strict');
const path=require('node:path');
const {simulation}=require('./helpers/expansion-harness.cjs');
const animated=()=>simulation('frontier',{preloadScripts:[path.resolve(__dirname,'../animation.js')]});

test('750ms follower snapshots interpolate skipped short hops for the whole observed cadence without restarting duplicates',()=>{
  const run=animated(),r=run.plain(`(()=>{const e=addEntity('unit','hellion',0,5,5);updateSpritePresentation();let data=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(data);testClock+=750;data=JSON.parse(JSON.stringify(serializeState()));data.simulationTick++;data.entities[0].x=7;data.entities[0].moveReadyAt=testClock+100;data.entities[0].moveStepDuration=431.25;applyRemoteState(data);const start=spriteRuntime.states.get(e.id).segment.start,end=spriteRuntime.states.get(e.id).segment.end;testClock+=375;updateSpritePresentation();const half={x:entities[0].px,y:entities[0].py};applyRemoteState(data);const duplicateStart=spriteRuntime.states.get(e.id).segment.start;testClock=end;updateSpritePresentation();const result={start,end,half,duplicateStart,x:entities[0].px,logical:entities[0].x,ready:entities[0].moveReadyAt};data=JSON.parse(JSON.stringify(data));data.simulationTick++;data.entities[0].x=9;applyRemoteState(data);const segment=spriteRuntime.states.get(e.id).segment;result.nextDuration=segment.end-segment.start;return result})()`);
  assert.equal(r.end-r.start,750);assert.deepEqual(r.half,{x:6,y:5});assert.equal(r.duplicateStart,r.start);assert.equal(r.x,7);assert.equal(r.logical,7);assert.equal(r.ready,r.start+100);assert.equal(r.nextDuration,750,'duplicate snapshots do not distort the next interpolation interval');
});

test('a follower reconstructs skipped corner hops on legal tiles and preserves authoritative cooldowns',()=>{
  const run=animated(),r=run.plain(`(()=>{const e=addEntity('unit','hellion',0,5,5);Object.assign(tileAt(6,5),{blocked:true,type:'rock'});updateSpritePresentation();let data=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(data);testClock+=750;data=JSON.parse(JSON.stringify(serializeState()));data.simulationTick++;data.entities[0].x=6;data.entities[0].y=6;data.entities[0].moveReadyAt=testClock+80;data.entities[0].moveStepDuration=431.25;applyRemoteState(data);const segment=spriteRuntime.states.get(e.id).segment,points=segment.points;testClock+=375;updateSpritePresentation();return {points,half:{x:entities[0].px,y:entities[0].py},ready:entities[0].moveReadyAt,end:segment.end}})()`);
  assert.deepEqual(r.points,[{x:5,y:5},{x:5,y:6},{x:6,y:6}]);assert.deepEqual(r.half,{x:5,y:6});assert.equal(r.ready,100830);assert.equal(r.end,101500);
});

test('same-cadence authoritative reversal starts at the displayed position and stops at the accepted tile',()=>{
  const run=animated(),r=run.plain(`(()=>{const e=addEntity('unit','phoenix',0,5,5);updateSpritePresentation();let data=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(data);testClock+=750;data=JSON.parse(JSON.stringify(serializeState()));data.simulationTick++;data.entities[0].x=7;data.entities[0].moveReadyAt=testClock+100;applyRemoteState(data);testClock+=750;updateSpritePresentation();const before=entities[0].px;data=JSON.parse(JSON.stringify(data));data.simulationTick++;data.entities[0].x=5;data.entities[0].moveDestination=null;data.entities[0].order='stop';applyRemoteState(data);const after=entities[0].px;testClock+=375;updateSpritePresentation();const half=entities[0].px;testClock+=375;updateSpritePresentation();return {before,after,half,end:entities[0].px,logical:entities[0].x,direction:spriteRuntime.states.get(e.id).direction}})()`);
  assert.equal(r.before,7);assert.equal(r.after,r.before);assert.equal(r.half,6);assert.equal(r.end,5);assert.equal(r.logical,5);assert.equal(r.direction,'W');
});

test('Phase Step preserves its authored recovery timer and next normal movement uses a short cooldown',()=>{
  const run=animated(),r=run.plain(`(()=>{players[0].factionId='ascendant';const e=addEntity('unit','dragoon',0,10,10);issueMoveOrder(e,30,10);const now=testClock,normal=effectiveMoveCd(e)*1000,ok=useAbility(e,'phaseStep',{x:12,y:11});const recover=e.moveReadyAt-now,position={x:e.x,y:e.y},segment=spriteRuntime.states.get(e.id).segment;testClock=e.moveReadyAt+.001;moveEntity(e,e.x+1,e.y);return {ok,recover,normal,position,segment,next:e.moveStepDuration,range:UNITS[e.type].move}})()`);
  assert.equal(r.ok,true);assert.equal(r.recover,r.normal*.45);assert.deepEqual(r.position,{x:12,y:11});assert.equal(r.segment,null);assert.equal(r.next,r.normal/r.range);
});

test('a failed bounded cosmetic route corrects once rather than animating a ground troop through a wall',()=>{
  const run=animated(),r=run.plain(`(()=>{const e=addEntity('unit','hellion',0,5,5);for(let y=0;y<BOARD_H;y++)Object.assign(tileAt(6,y),{blocked:true,type:'rock'});updateSpritePresentation();let data=JSON.parse(JSON.stringify(serializeState()));applyRemoteState(data);testClock+=750;data=JSON.parse(JSON.stringify(serializeState()));data.simulationTick++;data.entities[0].x=7;data.entities[0].moveReadyAt=testClock+120;applyRemoteState(data);const immediate={px:entities[0].px,segment:spriteRuntime.states.get(e.id).segment};testClock+=375;updateSpritePresentation();return {immediate,px:entities[0].px,logical:entities[0].x,ready:entities[0].moveReadyAt}})()`);
  assert.deepEqual(r.immediate,{px:7,segment:null});assert.equal(r.px,7);assert.equal(r.logical,7);assert.equal(r.ready,100870);
});

test('adjacent cosmetic hops avoid a route search and rejected authoritative movement clears the preview',()=>{
  const run=animated(),r=run.plain(`(()=>{const e=addEntity('unit','rifleman',0,5,5);updateSpritePresentation();const lookup=entityAt;let occupantQueries=0;entityAt=(...args)=>{occupantQueries++;return lookup(...args)};const route=cosmeticRoute(e,6,5);entityAt=lookup;Object.assign(tileAt(6,5),{type:'rock',blocked:true});const accepted=moveEntity(e,6,5);return {route,occupantQueries,accepted,x:e.x,y:e.y,pending:spriteRuntime.states.get(e.id).pendingPath}})()`);
  assert.deepEqual(r,{route:[{x:6,y:5}],occupantQueries:0,accepted:false,x:5,y:5,pending:null});
});
