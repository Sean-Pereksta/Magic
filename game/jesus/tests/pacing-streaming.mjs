import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {BIOME_ORDER,REGIONS,endlessChunkCount,regionForDepth} from '../js/world/regions.js';
import {JOURNEY_NODES,createJourneySave} from '../js/world/journey.js';
import {createLevel,updateWorldStreaming,persistWorldState,restoreWorldState,finalTemplate,layoutTemplate,CHUNK_WIDTH} from '../js/world/generator.js';
import {ROOM_LIBRARY,DYNAMIC_ROOM_LIBRARY,EXPLORATION_ROOM_LIBRARY} from '../js/world/rooms.js';
import {routeProofKey} from '../js/world/route-proof-key.js';
import {ROUTE_PROOFS} from '../js/world/route-proofs.js';
import {syncMovers,syncHazards,sampleMover,updateMovers} from '../js/physics.js';
import {GameStorage} from '../js/save/storage.js';

const values=new Map();globalThis.localStorage={getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,v)};
const storage=new GameStorage();
// A private collision-helper change must invalidate offline proofs as well as
// public controller changes; otherwise stale proofs could accept a broken route.
const physicsURL=new URL('../js/physics.js',import.meta.url);
let changedPhysics=await readFile(physicsURL,'utf8');
changedPhysics=changedPhysics.replace('function enabled(platform) { return ','function enabled(platform) { return platform.kind !== "moving" && ')
 .replace('"./config.js"',JSON.stringify(new URL('../js/config.js',import.meta.url).href))
 .replace('"./common.js"',JSON.stringify(new URL('../js/common.js',import.meta.url).href));
const changedPhysicsURL='data:text/javascript;base64,'+Buffer.from(changedPhysics).toString('base64');
const keySource=(await readFile(new URL('../js/world/route-proof-key.js',import.meta.url),'utf8'))
 .replace("'../config.js'",JSON.stringify(new URL('../js/config.js',import.meta.url).href))
 .replace("'../physics.js'",JSON.stringify(changedPhysicsURL))
 .replace("'./room-validator.js'",JSON.stringify(new URL('../js/world/room-validator.js',import.meta.url).href))
 .replace("'./gate-progress.js'",JSON.stringify(new URL('../js/world/gate-progress.js',import.meta.url).href))
 .replace("'../powers/blessings.js'",JSON.stringify(new URL('../js/powers/blessings.js',import.meta.url).href));
const changedKey=await import('data:text/javascript;base64,'+Buffer.from(keySource).toString('base64'));
assert.notEqual(changedKey.routeProofKey(DYNAMIC_ROOM_LIBRARY[0]),routeProofKey(DYNAMIC_ROOM_LIBRARY[0]),'private enabled collision policy is part of proof identity');
for(const room of [...ROOM_LIBRARY,...DYNAMIC_ROOM_LIBRARY,...DYNAMIC_ROOM_LIBRARY.filter(r=>r.platforms.some(p=>p.pressureGate)).map(r=>layoutTemplate(r)),...EXPLORATION_ROOM_LIBRARY,...ROOM_LIBRARY.map(finalTemplate)])assert.ok(ROUTE_PROOFS[routeProofKey(room)]?.valid,`${room.id}: runtime proof must match current controller and geometry`);
assert.deepEqual(Array.from({length:22},(_,i)=>endlessChunkCount(i)),[6,6,6,7,7,7,8,8,8,9,9,9,10,10,10,11,11,11,12,12,12,12]);
for(let depth=0;depth<100;depth++){assert.equal(regionForDepth(depth),BIOME_ORDER[depth%6]);assert.ok(endlessChunkCount(depth)<=12);}
assert.deepEqual(JOURNEY_NODES.map(n=>n.targetTime),[225,263,300,338,363,388]);
for(const biome of BIOME_ORDER){
 const modern=createLevel({biome,seed:1512,difficulty:4});
 assert.equal(modern.chunkCount,REGIONS[biome].legacyChunks/4);
 assert.ok(modern.chunkPlan.filter(c=>c.requiresDynamic).length>=Math.floor(modern.chunkCount/5),'mandatory encounters recur throughout each region');
 assert.ok(modern.checkpoints.every(c=>c.chunkIndex%5===0||c.arena));
 assert.ok(modern.chunkPlan.at(-1).final&&!modern.chunkPlan.at(-1).requiresDynamic,'guardian seam remains statically safe');
 for(let depth=0;depth<22;depth+=3){
  const segment=createLevel({biome,seed:depth+21,mode:'endless',chunkCount:endlessChunkCount(depth)});
  assert.equal(segment.chunkCount,endlessChunkCount(depth));assert.ok(segment.routeValid&&segment.chunkPlan.some(c=>c.requiresDynamic));
 }
 // Old checkpoints retain their complete original dimensions and content IDs.
 const legacy=createLevel({biome,seed:1512,layoutVersion:1});
 assert.equal(legacy.chunkCount,REGIONS[biome].legacyChunks);assert.ok(legacy.chunkPlan.every(c=>!c.requiresDynamic));
 const late=legacy.checkpoints.at(-2);assert.ok(late.x>modern.width);
 const save=createJourneySave();save.completed=['countryside'];save.unlocked=5;save.permanentPowers=['wings','shield'];save.records={countryside:{bestTime:790,runs:4}};
 save.checkpoint={nodeId:biome,x:late.x,y:late.y,seed:1512,worldState:{version:1,seed:1512,biome,collected:[],dead:[],activated:[],furthestX:late.x}};
 storage.saveJourney(save);const loaded=storage.loadJourney();
 assert.equal(loaded.checkpoint.x,late.x);assert.deepEqual(loaded.records,save.records);assert.deepEqual(loaded.permanentPowers,save.permanentPowers);
 const resumed=createLevel({biome,seed:loaded.checkpoint.seed,layoutVersion:loaded.checkpoint.worldState.layoutVersion||1});restoreWorldState(resumed,loaded.checkpoint.worldState,late.x);
 assert.ok(late.x<resumed.width&&resumed.platforms.some(p=>late.x>=p.x&&late.x<=p.x+p.w),'late old checkpoint has real support');
}

const options={biome:'mountains',seed:2026,difficulty:5,chunkCount:16},w=createLevel(options);
w.time=7.375;syncMovers(w,w.time);syncHazards(w,w.time);
const mover=w.movers.find(m=>m.required);assert.ok(mover);
const expected={x:mover.x,y:mover.y};const snapshot=persistWorldState(w);
const save=createJourneySave();save.checkpoint={nodeId:'mountains',seed:w.seed,x:100,y:406,worldState:snapshot};storage.saveJourney(save);
assert.deepEqual(storage.loadJourney().checkpoint.worldState,snapshot,'clock, layout and mechanism state survive save sanitization');
const restored=createLevel(options);restoreWorldState(restored,snapshot,100);
assert.equal(restored.time,w.time);const same=restored.movers.find(m=>m.id===mover.id);assert.deepEqual({x:same.x,y:same.y},expected);assert.equal(same.dx,0);assert.equal(same.dy,0);
const before=same.x;updateMovers(restored,restored.time+1/120,1/120);assert.ok(Math.abs(same.x-before)<12,'restored mover advances one physical step without a teleport');
updateWorldStreaming(restored,restored.width-1000);restored.time=21.5;updateWorldStreaming(restored,100);
for(const m of restored.movers){const sampled=sampleMover(m,restored.time);assert.equal(m.x,sampled.x);assert.equal(m.y,sampled.y);assert.equal(m.dx,0);}

// A collapsed platform recovers while unloaded and cannot strand a returning player.
let collapse;
for(let chunk=0;chunk<w.chunkCount&&!collapse;chunk++){updateWorldStreaming(w,chunk*CHUNK_WIDTH+100);collapse=w.platforms.find(p=>p.kind==='collapsing');}
assert.ok(collapse,'generated content includes recovering collapse surfaces');
const collapseChunk=collapse.chunkIndex,collapseId=collapse.id;collapse.dead=true;collapse.resetTimer=1;collapse.shaking=true;collapse.collapseTimer=0;
w.time=30;persistWorldState(w);updateWorldStreaming(w,collapseChunk<8?w.width-100:100);w.time=33;updateWorldStreaming(w,collapseChunk*CHUNK_WIDTH+100);
const recovered=w.platforms.find(p=>p.id===collapseId);assert.equal(recovered.dead,false);assert.equal(recovered.collapseTimer,null);
const cross=createLevel({...options,biome:'desert'});restoreWorldState(cross,snapshot,100);assert.equal(cross.time,0,'foreign-biome state cannot move the mechanism clock');
console.log('Short Journey/Endless pacing, original late-checkpoint compatibility, saved clocks, streamed movers and unloaded collapse recovery passed.');
