import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { declareWar, orderArmy, parseSave, resolveMovement, sizeOf } from '../core.mjs';
import { endTurn } from '../diplomacy.mjs';
import { emptyUnits } from '../economy.mjs';
import { applyEncounterResult, beginEncounter, commitEncounterSupport, configureOfflineBattles, encounterPreview, pendingEncounter, reinforcementRoute, resolveEncounterAutomatically, returnFromEncounter, saveEncounterCheckpoint, validateOfflineBattles } from '../offline-encounters.mjs';
import { battleResult, createBattle, finishAutomatically } from '../battle-sim.mjs';

function fixture({support=true,defenders=40,fort=true}={}) {
  const s=createGame(321),a=s.armies[0],d=s.armies[1];
  declareWar(s,a.owner,d.owner);
  for(let q=7;q<=15;q++)for(let r=7;r<=13;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),levy:140,archer:12,ram:2},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{tile:'11,10',units:{...emptyUnits(),levy:defenders},path:[],target:null,order:'hold',morale:1});
  const tile=s.tiles[d.tile];Object.assign(tile,{owner:d.owner,building:fort?'fort':null,levels:fort?{fort:2,wall:2}:{},walls:fort?74:0,fortIntegrity:fort?65:0,name:'Encounter fort'});
  if(support)s.armies.push({...structuredClone(a),id:`army-${s.nextId++}`,tile:'10,11',units:{...emptyUnits(),spearman:17},name:'Support'});
  configureOfflineBattles(s);orderArmy(s,a.owner,a.id,tile.id,'attack');return {s,a,d,tile};
}
function finish(s,{winner=0,loss=1,walls=20,fortIntegrity=30}={}) {
  const e=pendingEncounter(s);
  return {battleId:e.id,winner,armies:e.participants.map(p=>({armyId:p.armyId,units:Object.fromEntries(Object.entries(p.units).map(([type,count])=>[type,Math.max(0,count-(count?loss:0))])),morale:.7})),walls:Math.min(walls,e.tile.walls),fortIntegrity:Math.min(fortIntegrity,e.tile.fortIntegrity)};
}

test('attack orders stay strategic and movement pauses before any casualty or damage',()=>{
  const {s,a,d,tile}=fixture();assert.equal(pendingEncounter(s),null);
  const before=[structuredClone(a.units),structuredClone(d.units),tile.walls,tile.fortIntegrity];
  resolveMovement(s);const e=pendingEncounter(s);assert.equal(e.status,'pending');assert.equal(e.tile.id,tile.id);
  assert.deepEqual([a.units,d.units,tile.walls,tile.fortIntegrity],before);
  const frozen=structuredClone(s);resolveMovement(s);endTurn(s);assert.deepEqual(s,frozen);
});
test('support follows legal land steps, preserves identities, and selection locks',()=>{
  const {s,a,tile}=fixture();const support=s.armies.find(a=>a.name==='Support');
  assert.equal(reinforcementRoute(s,support,tile.id).length,1);
  resolveMovement(s);const e=pendingEncounter(s);assert.ok(e.supportOptions.some(p=>p.armyId===support.id&&p.available));
  commitEncounterSupport(s,[]);assert.equal(e.participants.some(p=>p.armyId===support.id),false);
  commitEncounterSupport(s,[support.id]);assert.equal(e.participants.some(p=>p.armyId===support.id),false,'cannot reroll a locked commitment');
  assert.equal(e.participants.find(p=>p.armyId===a.id).commander.id,`captain:${a.id}`);
});
test('mountains, embarked armies, and closed borders do not create nearby support',()=>{
  const {s,tile}=fixture();const support=s.armies.find(a=>a.name==='Support');
  support.tile='9,10';for(const id of ['10,10','10,9','9,11'])s.tiles[id].terrain='mountain';
  assert.equal(reinforcementRoute(s,support,tile.id),null);
  support.tile='10,11';support.embarkedFleetId='fleet-1';assert.equal(reinforcementRoute(s,support,tile.id),null);
  delete support.embarkedFleetId;s.tiles[tile.id].owner='thornwall';assert.equal(reinforcementRoute(s,support,tile.id),null);
});
test('trade and a neutral ally never silently join the war',()=>{
  const {s,tile}=fixture();const neutral=s.armies[2];neutral.tile='12,10';
  s.treaties.push({type:'alliance',parties:['ashen',neutral.owner],expires:s.turn+9});
  resolveMovement(s);assert.equal(pendingEncounter(s).supportOptions.some(p=>p.armyId===neutral.id),false);
});
test('manual results atomically apply exact counts and existing siege damage once',()=>{
  const {s,a,tile}=fixture();resolveMovement(s);beginEncounter(s);const e=pendingEncounter(s),result=finish(s);
  const starting=structuredClone(e.participants);applyEncounterResult(s,result);
  for(const p of starting){const row=e.result.armies.find(a=>a.armyId===p.armyId);for(const type of Object.keys(p.units))assert.equal(p.units[type],row.units[type]+row.losses[type]);}
  assert.equal(tile.walls,20);assert.equal(tile.fortIntegrity,30);assert.equal(tile.owner,a.owner);
  const after=structuredClone(s);applyEncounterResult(s,result);assert.deepEqual(s,after);
  assert.equal(returnFromEncounter(s).resumeTurn,false);assert.equal(s.offlineBattle,null);
  assert.throws(()=>returnFromEncounter(s));assert.equal(tile.walls,20);
});
test('invalid results cannot alter the pending state or resurrect troops',()=>{
  const {s}=fixture();resolveMovement(s);const bad=finish(s);bad.armies[0].units.levy++;
  bad.armies[0].units.levy=999999;const before=structuredClone(s);
  assert.throws(()=>applyEncounterResult(s,bad),/count/);assert.deepEqual(s,before);
  const overlap=finish(s);overlap.armies[0].escaped={levy:100};overlap.armies[0].undeployed={levy:100};
  assert.throws(()=>applyEncounterResult(s,overlap),/overlap/);assert.deepEqual(s,before);
});
test('unarrived support survives and returns to its source with one spent action',()=>{
  const {s}=fixture();resolveMovement(s);const e=pendingEncounter(s),support=e.participants.find(p=>p.isSupport),result=finish(s,{winner:1});
  const row=result.armies.find(a=>a.armyId===support.armyId);row.units=structuredClone(support.units);row.undeployed=structuredClone(support.units);
  applyEncounterResult(s,result);const army=s.armies.find(a=>a.id===support.armyId);
  assert.deepEqual(army.units,support.units);assert.equal(army.tile,support.sourceTile);assert.equal(army.resolvedTurn,s.turn);assert.deepEqual(army.path,[]);
});
test('all hostile armies on the battle tile participate; trapped survivors prevent capture',()=>{
  const {s,d,tile}=fixture({support:false});s.armies.push({...structuredClone(d),id:`army-${s.nextId++}`});
  for(const t of Object.values(s.tiles))if(!['10,10',tile.id].includes(t.id))t.terrain='mountain';
  resolveMovement(s);assert.equal(pendingEncounter(s).participants.filter(p=>p.side===1).length,2);
  applyEncounterResult(s,finish(s));assert.equal(tile.owner,d.owner);assert.notEqual(s.armies[0].tile,tile.id);
});
test('pending and applied reports survive save validation without duplicating outcomes',()=>{
  const {s}=fixture();resolveMovement(s);const pending=parseSave(JSON.stringify(s));assert.equal(pending.offlineBattle.id,s.offlineBattle.id);
  resolveEncounterAutomatically(pending);const applied=parseSave(JSON.stringify(pending));const before=structuredClone(applied);
  resolveEncounterAutomatically(applied);assert.deepEqual(applied,before);
  const broken=structuredClone(s);broken.offlineBattle.participants[0].units.levy++;assert.throws(()=>validateOfflineBattles(broken),/match/);
});
test('preview is read-only and hides unobserved enemy reinforcements',()=>{
  const {s,a,d}=fixture({support:false});a.units={...emptyUnits(),levy:140};
  const hidden={...structuredClone(d),id:`army-${s.nextId++}`,name:'Unobserved reinforcement',tile:'13,10'};s.armies.push(hidden);
  resolveMovement(s);const before=JSON.stringify(s),preview=encounterPreview(s);
  assert.match(preview.automaticForecast.label,/Strategic/);assert.equal(JSON.stringify(s),before);
  assert.equal('checkpoint' in preview,false);
  assert.ok(pendingEncounter(s).participants.some(p=>p.armyId===hidden.id));
  assert.equal(preview.participants.some(p=>p.armyId===hidden.id),false);
  assert.equal(preview.supportOptions.some(p=>p.armyId===hidden.id),false);
  const outcome=finish(s,{winner:1}),row=outcome.armies.find(p=>p.armyId===hidden.id);row.units=structuredClone(hidden.units);row.undeployed=structuredClone(hidden.units);
  applyEncounterResult(s,outcome);const aftermath=encounterPreview(s);
  assert.equal(aftermath.result.armies.some(p=>p.armyId===hidden.id),false,'an unobserved army that never arrived stays private in the report');
});
test('the resumed turn matches uninterrupted automatic resolution and does not replay AI or economy',()=>{
  const {s}=fixture({support:false});const automatic=structuredClone(s);configureOfflineBattles(automatic,{preference:'auto'});
  endTurn(automatic);endTurn(s);assert.ok(s.offlineBattle);assert.equal(s.turn,1);
  let battles=0;
  while(pendingEncounter(s)){resolveEncounterAutomatically(s);const round=s.turn;assert.equal(returnFromEncounter(s).resumeTurn,true);endTurn(s);assert.ok(s.turn===round||s.turn===round+1);if(++battles>20)assert.fail('turn failed to resume');}
  assert.equal(s.turn,automatic.turn);assert.equal(s.rng,automatic.rng);assert.deepEqual(s.armies,automatic.armies);
  assert.deepEqual(s.kingdoms.map(k=>k.resources),automatic.kingdoms.map(k=>k.resources));assert.deepEqual(s.militaryEvents,automatic.militaryEvents);assert.equal(s.offlineTurn,undefined);
});
test('online campaigns ignore preference injection and reject local tactical results',()=>{
  const {s}=fixture({support:false});s.controllers=Object.fromEntries(s.kingdoms.map(k=>[k.id,{kind:k.id==='ashen'?'human':'ai',uid:k.id==='ashen'?'user':null}]));
  assert.equal(configureOfflineBattles(s),null);resolveMovement(s);assert.equal(pendingEncounter(s),null);assert.ok(s.militaryEvents.some(e=>e.action==='battle'));
  assert.throws(()=>applyEncounterResult(s,{}),/offline/);
});
test('checkpoints recover exact casualties and damage, rejecting resurrection or rollback',()=>{
  const {s}=fixture({support:false});resolveMovement(s);beginEncounter(s);const e=pendingEncounter(s),b=createBattle(e);
  const dead=b.units.find(u=>u.owner==='ashen');dead.status='dead';dead.hp=0;
  b.fortifications.walls=15;b.fortifications.fortIntegrity=20;b.time=5;
  saveEncounterCheckpoint(s,b);const recovered=parseSave(JSON.stringify(s));assert.equal(recovered.offlineBattle.checkpoint.units.find(u=>u.id===dead.id).status,'dead');
  const resurrected=structuredClone(b);Object.assign(resurrected.units.find(u=>u.id===dead.id),{status:'active',hp:dead.maxHp});
  assert.throws(()=>saveEncounterCheckpoint(s,resurrected),/restores/);
  const older=structuredClone(b);older.time=4;assert.throws(()=>saveEncounterCheckpoint(s,older),/older/);
  const invalid=structuredClone(b);invalid.units.pop();assert.throws(()=>saveEncounterCheckpoint(s,invalid),/damaged/);
  const inflated=finish(s,{loss:0,walls:15,fortIntegrity:20});assert.throws(()=>applyEncounterResult(s,inflated),/checkpoint/);
  b.phase='complete';b.winner=0;saveEncounterCheckpoint(s,b);
  const expected=battleResult(b);resolveEncounterAutomatically(s);
  assert.equal(s.offlineBattle.result.armies[0].units[dead.type],expected.armies[0].units[dead.type]);assert.equal(s.tiles[e.tile.id].walls,15);
});
test('a mutual withdrawal does not capture and an escaped victor cannot occupy',()=>{
  for(const draw of [true,false]){
    const {s,tile,d}=fixture({support:false});resolveMovement(s);const result=finish(s);result.winner=draw?null:0;
    if(!draw)result.armies[0].escaped=structuredClone(result.armies[0].units);
    applyEncounterResult(s,result);assert.equal(tile.owner,d.owner);
  }
});

test('legacy battle saves respect the larger ceiling without healing, rearming or losing troops',()=>{
  const {s}=fixture({support:false,fort:false,defenders:100});resolveMovement(s);beginEncounter(s);
  const b=createBattle(pendingEncounter(s),{maxActive:100});b.maxActive=240;b.activeCaps=[145,95];delete b.battleRulesVersion;delete b.nextReserveWave;
  const wounded=b.units.find(u=>u.status==='active'&&u.type==='archer');wounded.hp-=7;wounded.ammo-=1;wounded.morale=63;
  const corpse=b.units.find(u=>u.status==='active'&&u.id!==b.commanderId&&u.id!==wounded.id);corpse.status='dead';corpse.hp=0;
  const escaped=b.units.find(u=>u.status==='active'&&u.side!==b.playerSide);escaped.status='escaped';
  while(b.units.filter(u=>u.status==='active').length<240)b.units.find(u=>u.status==='reserve').status='active';
  b.time=21;saveEncounterCheckpoint(s,b);
  const identities=b.units.map(u=>({id:u.id,armyId:u.armyId,type:u.type,hp:u.hp,ammo:u.ammo,morale:u.morale}));
  const before=battleResult(b),loaded=parseSave(JSON.stringify(s)),migrated=loaded.offlineBattle.checkpoint;
  assert.equal(migrated.maxActive,200);assert.ok(migrated.units.filter(u=>u.status==='active').length<=200);
  assert.deepEqual(migrated.units.map(u=>({id:u.id,armyId:u.armyId,type:u.type,hp:u.hp,ammo:u.ammo,morale:u.morale})),identities);
  assert.equal(migrated.units.find(u=>u.id===corpse.id).status,'dead');assert.equal(migrated.units.find(u=>u.id===escaped.id).status,'escaped');
  assert.equal(migrated.units.find(u=>u.id===b.commanderId).status,'active');
  assert.deepEqual(battleResult(migrated).armies.map(a=>a.units),before.armies.map(a=>a.units));
  assert.ok(migrated.units.some(u=>u.status==='reserve'&&u.wasDeployed),'overflow troops wait for a later wave');
  saveEncounterCheckpoint(loaded,migrated);assert.deepEqual(parseSave(JSON.stringify(loaded)).offlineBattle.checkpoint,migrated);
});
test('configuring the existing campaign preserves tactical timing preferences',()=>{
  const {s}=fixture();configureOfflineBattles(s,{tacticalTime:'slow'});configureOfflineBattles(s);
  assert.equal(s.offlineBattles.tacticalTime,'slow');assert.throws(()=>configureOfflineBattles(s,{tacticalTime:'invalid'}));
});

test('battle size preferences persist and cannot resize a saved encounter',()=>{
  const {s}=fixture({support:false,fort:false,defenders:100});assert.equal(s.offlineBattles.battleSize,150);
  for(const battleSize of [100,150,200]){configureOfflineBattles(s,{battleSize});configureOfflineBattles(s);assert.equal(parseSave(JSON.stringify(s)).offlineBattles.battleSize,battleSize);}
  for(const battleSize of [0,99,201,300,'200',NaN])assert.throws(()=>configureOfflineBattles(s,{battleSize}),/Choose/);
  resolveMovement(s);beginEncounter(s);const b=createBattle(pendingEncounter(s),{maxActive:s.offlineBattles.battleSize});saveEncounterCheckpoint(s,b);
  configureOfflineBattles(s,{battleSize:100});const restored=parseSave(JSON.stringify(s));
  assert.equal(restored.offlineBattles.battleSize,100);assert.equal(restored.offlineBattle.checkpoint.maxActive,200);
  assert.deepEqual(restored.offlineBattle.checkpoint.units,b.units,'changing the next battle setting leaves the live roster untouched');
  const broken=structuredClone(s);broken.offlineBattles.battleSize=400;assert.throws(()=>parseSave(JSON.stringify(broken)),/preferences/);
  const legacy=structuredClone(s);delete legacy.offlineBattles.battleSize;assert.doesNotThrow(()=>parseSave(JSON.stringify(legacy)));
});

test('a real east-side attack deploys armies at opposite ends and repairs only unsafe deployment saves',()=>{
  const {s,a,tile}=fixture({support:false,fort:false,defenders:100});a.tile='12,10';orderArmy(s,a.owner,a.id,tile.id,'attack');
  resolveMovement(s);beginEncounter(s);const e=pendingEncounter(s);
  assert.ok(e.participants.every(p=>p.entryDirection==='east'),'reproduce the campaign approach that formerly shared a spawn edge');
  const b=createBattle(e,{maxActive:200});
  const separated=state=>{
    const attackers=state.units.filter(u=>u.status==='active'&&u.side===0),defenders=state.units.filter(u=>u.status==='active'&&u.side===1);
    assert.ok(attackers.length&&defenders.length);assert.ok(Math.max(...attackers.map(u=>u.z))<-30);assert.ok(Math.min(...defenders.map(u=>u.z))>30);
  };
  separated(b);
  const health=b.units.map(u=>[u.id,u.hp,u.ammo,u.morale]);
  for(const u of b.units.filter(u=>u.status==='active')){u.x=50;u.z=0;u.order={kind:'hold',x:50,z:0};}
  saveEncounterCheckpoint(s,b);const repaired=parseSave(JSON.stringify(s));separated(repaired.offlineBattle.checkpoint);
  assert.deepEqual(repaired.offlineBattle.checkpoint.units.map(u=>[u.id,u.hp,u.ammo,u.morale]),health);
  const fighting=structuredClone(repaired.offlineBattle.checkpoint);fighting.phase='combat';fighting.time=8;
  for(const u of fighting.units.filter(u=>u.status==='active')){u.x=4;u.z=0;}
  saveEncounterCheckpoint(repaired,fighting);
  assert.deepEqual(parseSave(JSON.stringify(repaired)).offlineBattle.checkpoint.units,JSON.parse(JSON.stringify(fighting.units)),'loading active combat never teleports fighters back to deployment');
});
test('a completed tactical simulation round-trips through the real campaign save schema',()=>{
  const {s,a,d}=fixture({support:false});a.units={...emptyUnits(),menAtArms:14,spearman:8,archer:6,knight:3,ram:1};d.units={...emptyUnits(),levy:18,crossbow:4};
  resolveMovement(s);beginEncounter(s);const e=pendingEncounter(s),b=createBattle(e);
  saveEncounterCheckpoint(s,b);const result=finishAutomatically(b);applyEncounterResult(s,result);
  assert.ok(Number.isInteger(s.tiles[e.tile.id].fortIntegrity));
  const loaded=parseSave(JSON.stringify(s));assert.equal(loaded.offlineBattle.status,'applied');assert.deepEqual(loaded.offlineBattle.result.armies,s.offlineBattle.result.armies);
  assert.deepEqual(loaded.tiles[e.tile.id].wallSections,s.tiles[e.tile.id].wallSections);
});
test('oversized checkpoints leave the last valid battlefield save untouched',()=>{
  const {s}=fixture({support:false});resolveMovement(s);beginEncounter(s);const b=createBattle(pendingEncounter(s));
  saveEncounterCheckpoint(s,b);const saved=structuredClone(s.offlineBattle.checkpoint),oversized=structuredClone(b);
  oversized.annotation='x'.repeat(6000001);assert.throws(()=>saveEncounterCheckpoint(s,oversized),/checkpoint is too large/);
  assert.deepEqual(s.offlineBattle.checkpoint,saved);
  s.testPadding='x'.repeat(12000000);assert.throws(()=>saveEncounterCheckpoint(s,b),/campaign save size limit/);
  assert.deepEqual(s.offlineBattle.checkpoint,saved);
});
