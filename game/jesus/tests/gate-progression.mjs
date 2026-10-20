import assert from 'node:assert/strict';
import {updateGateProgress} from '../js/world/gate-progress.js';
import {collectPickups} from '../js/powers/blessings.js';
import {createPlayer} from '../js/physics.js';
import {JesusRunGame} from '../js/game.js';
import {GameStorage} from '../js/save/storage.js';
import {createJourneySave} from '../js/world/journey.js';
import {createLevel,persistWorldState,restoreWorldState} from '../js/world/generator.js';

const player=createPlayer();player.x=100;player.y=406;
const mechanism=(id,kind,x=100)=>({id,exploration:true,kind,x,y:406,w:34,h:62,activated:false});
const gate=(id,unlockIds)=>({id,kind:'gate',mandatory:true,exploration:true,unlockIds,x:400,y:-700,w:64,h:1700,open:false});
const key=mechanism('room-a:key','key'),second=mechanism('room-b:key','key',600),pressure=mechanism('room-c:plate','pressure',800),switcher=mechanism('room-c:switch','switch',1000);
key.guardianId='guard-a';
const firstGate=gate('gate-a',[key.id]),secondGate=gate('gate-b',[second.id]),multiGate=gate('gate-c',[pressure.id,switcher.id]);
const pickup={id:'item-a',type:'gateKey',unlockId:key.id,x:100,y:406,w:28,h:28,collected:false};
const guardian={id:'guard-a',dead:false,hp:5};
const world={switches:[key,second,pressure,switcher],platforms:[firstGate,secondGate,multiGate],pickups:[pickup],enemies:[guardian],effects:[],_state:{activated:new Set(),dead:new Set(),collected:new Set()}};
updateGateProgress(world,player,1/120);assert.equal(pickup.locked,true);assert.deepEqual(collectPickups(world,player),[]);assert.equal(firstGate.open,false);
guardian.dead=true;guardian.hp=0;world._state.dead.add(guardian.id);
updateGateProgress(world,player,1/120);assert.equal(pickup.locked,false);assert.deepEqual(collectPickups(world,player),['gateKey']);
updateGateProgress(world,player,0);assert.equal(key.activated,true);assert.equal(world._state.activated.has(key.id),true);
assert.equal(firstGate.opening,true);assert.equal(firstGate.open,false,'a barrier remains solid throughout opening');assert.equal(secondGate.open,false,'an unrelated key cannot open another gate');
updateGateProgress(world,player,.5);assert.equal(firstGate.openProgress,.5);assert.equal(firstGate.open,false);
updateGateProgress(world,player,.5);assert.equal(firstGate.openProgress,1);assert.equal(firstGate.open,true);
player.x=800;updateGateProgress(world,player,1);assert.equal(pressure.activated,true);assert.equal(multiGate.open,false,'all mechanisms are required');
player.x=1000;updateGateProgress(world,player,.25);assert.equal(switcher.activated,true);assert.equal(multiGate.opening,true);assert.equal(multiGate.open,false);
updateGateProgress(world,player,.75);assert.equal(multiGate.open,true);
assert.equal(second.activated,false,'key mechanisms cannot activate by overlap without their pickup');
// A guardian can be dead in an unloaded chunk; its persistent ID is sufficient.
key.activated=false;pickup.collected=false;world.enemies=[];updateGateProgress(world,player,0);assert.equal(pickup.locked,false);
// Return-to-gate hint appears once and follows the chamber's vertical direction.
const lower=mechanism('lower','relic',200);lower.y=700;
const cave=gate('cave',[lower.id]);world.switches.push(lower);world.platforms.push(cave);player.x=150;
let events=updateGateProgress(world,player,0);assert.ok(events.some(e=>e.type==='hint'&&e.detail.includes('below')));
events=updateGateProgress(world,player,0);assert.ok(!events.some(e=>e.type==='hint'));

// Completing a mandatory puzzle commits its identity to the existing shrine,
// including when the player dies before reaching another shrine.
const values=new Map();globalThis.localStorage={getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value)};
let generated,required;
for(let seed=1;seed<=100&&!required;seed++){
  generated=createLevel({biome:'countryside',seed,chunkCount:6});
  required=generated.switches.find(sw=>sw.guardianId&&['key','relic'].includes(sw.kind));
}
assert.ok(required,'a generated required guardian holds an obtainable key');
const game=Object.create(JesusRunGame.prototype),journey=createJourneySave(),storage=new GameStorage();
const checkpoint={nodeId:'countryside',seed:generated.seed,x:80,y:406,stats:{time:12,collectibles:1,secrets:0,totalSecrets:3,deaths:0,damage:0},
  player:{hearts:4,powers:{holyLight:3}},worldState:persistWorldState(generated)};
const shrineState=JSON.parse(JSON.stringify(checkpoint));
Object.assign(game,{mode:'journey',world:generated,checkpoint,journeySave:journey,storage});
const carried=generated.pickups.find(p=>p.unlockId===required.id),guard=generated.enemies.find(e=>e.id===required.guardianId);
guard.dead=true;guard.hp=0;generated._state.dead.add(guard.id);
const ordinary=generated.pickups.find(p=>!p.unlockId);ordinary.collected=true;
player.x=carried.x;player.y=carried.y;
updateGateProgress(generated,player,0);collectPickups(generated,player);
const solved=updateGateProgress(generated,player,0);game.commitGateProgress(solved);
assert.equal(checkpoint.worldState.activated.includes(required.id),true);assert.equal(checkpoint.worldState.collected.includes(carried.id),true);
assert.equal(checkpoint.worldState.dead.includes(guard.id),true);assert.equal(checkpoint.worldState.collected.includes(ordinary.id),false,'ordinary supplies retain normal checkpoint rules');
assert.equal(checkpoint.stats.collectibles,2);assert.deepEqual(checkpoint.player,shrineState.player);assert.equal(checkpoint.x,shrineState.x);
assert.deepEqual(checkpoint.worldState.dynamics,shrineState.worldState.dynamics);
game.commitGateProgress(solved);assert.equal(checkpoint.stats.collectibles,2,'repeated save events cannot duplicate collectibles');
const restoredCheckpoint=storage.loadJourney().checkpoint;assert.equal(restoredCheckpoint.worldState.layoutVersion,3);
const resumed=createLevel({biome:'countryside',seed:generated.seed,chunkCount:6,layoutVersion:3});restoreWorldState(resumed,restoredCheckpoint.worldState,80);
assert.equal(resumed.switches.find(sw=>sw.id===required.id).activated,true);assert.equal(resumed.pickups.find(p=>p.id===carried.id).collected,true);
assert.equal(resumed.enemies.find(e=>e.id===guard.id).dead,true);assert.equal(resumed.platforms.find(p=>p.id===required.gateId).open,true,'death/reload reopens the completed gate');
console.log('Guardian keys, per-gate identities, multi-switch locks, opening animation, clues and puzzle progress through death/save/reload passed.');
