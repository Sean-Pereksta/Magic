import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, parseSave, declareWar, orderArmy, orderStructureAttack, resolveMovement, economyProjection } from '../core.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
import { foundCity, foundAIKingdoms, planFoundings, capitalSeparation } from '../founding.mjs';
import { distance } from '../world-hex.mjs';
import { armyVision, structureVision, knowledgeView, refreshKnowledge, visionTiles } from '../fog.mjs';
import { emptyUnits } from '../economy.mjs';
import { battlePreview } from '../battle-preview.mjs';
import { buildingInspection, foreignEconomy, rivalTurnReports, battleReports } from '../expansion-ui.mjs';
import { structureActions } from '../intelligence-ui.mjs';
import { warRoomPanel } from '../war-room.mjs';
import { makeContext, scriptedReply, endTurn, disclosedDeal, validateIntent } from '../diplomacy.mjs';
import { recruitSpy, assignSpy, gatherIntelligence } from '../espionage.mjs';
import { splitCampaign, playerView, joinCampaign, packCampaign, decodePayload } from '../multiplayer-state.mjs';
import { HOUSES, WORLD_SIZES } from '../data.mjs';

function emptyField() {
  const s=legacyGame(513);
  for(const t of Object.values(s.tiles)){Object.assign(t,{terrain:'plains',building:null,owner:null,capital:null,name:null,resource:null,road:false,river:false,levels:{},walls:0,market:false,project:null});}
  s.armies=s.armies.slice(0,2);s.armies[0].tile='5,6';s.armies[0].units={...emptyUnits(),levy:20};s.armies[1].tile='15,10';
  for(const [i,k]of s.kingdoms.entries()){const t=s.tiles[`${5+i*5},${i?20:6}`];Object.assign(t,{building:'city',owner:k.id,capital:k.id,name:k.name+' capital',levels:{city:1}});}
  return s;
}
for(const n of [6,8,10,12])test(`${n}-House campaign has room, separated complete openings, and round-trips`,()=>{
  const s=createGame(8147,'random',n),plan=planFoundings(s);assert.equal(s.kingdoms.length,n);assert.deepEqual({width:s.width,height:s.height},WORLD_SIZES[n]);assert.ok(s.width*s.height/n>=200);
  for(const x of plan)for(const y of plan)if(x!==y)assert.ok(distance(s.tiles[x.capital],s.tiles[y.capital])>=capitalSeparation(s));
  assert.equal(foundCity(s,'ashen',plan[0].capital).ok,true);assert.equal(foundAIKingdoms(s).ok,true);assert.equal(s.turn,1);assert.equal(s.armies.length,n);
  for(const k of s.kingdoms)for(const type of ['farm','lumber','quarry','mine'])assert.ok(Object.values(s.tiles).some(t=>t.owner===k.id&&t.building===type));
  assert.deepEqual(parseSave(JSON.stringify(s)),s);
});
test('full founding preview is discarded when the player confirms their capital',()=>{
  const s=createGame(92),site=planFoundings(s)[0].capital;
  assert.ok(Object.values(knowledgeView(s).tiles).every(t=>t.fog==='visible'));assert.deepEqual(s.fog.houses,{});
  assert.equal(foundCity(s,'ashen',site).ok,true);foundAIKingdoms(s);
  const v=knowledgeView(s),unknown=Object.values(v.tiles).filter(t=>t.fog==='unknown');assert.ok(unknown.length>1000);
  for(const t of unknown){assert.equal(t.resource,null);assert.equal(t.owner,null);assert.equal(t.project,null);assert.equal(t.road,false);if(t.building)assert.ok(t.knownCapital);}
  assert.equal(unknown.filter(t=>t.knownCapital).length,5);assert.equal(v.armies.length,1);
});
test('best scouting unit controls mixed-army vision and towers provide frontier observation',()=>{
  const a={units:emptyUnits()};for(const [id,range]of [['ram',1],['levy',2],['archer',3],['cavalry',4],['scout',5]]){a.units[id]=1;assert.equal(armyVision(a),range);}
  assert.equal(structureVision({building:'lumber',levels:{lumber:3}}),0);assert.equal(structureVision({building:'watchtower',levels:{watchtower:3}}),8);
  const s=emptyField(),t=s.tiles['14,10'];t.owner='ashen';t.building='watchtower';t.levels={watchtower:1};assert.ok(knowledgeView(s).armies.some(a=>a.owner==='wintermere'));
  t.owner='wintermere';assert.ok(!knowledgeView(s).armies.some(a=>a.owner==='wintermere'));
});
test('remembered terrain and structures stay frozen when hidden ownership and buildings change',()=>{
  const s=emptyField(),a=s.armies[0],t=s.tiles['15,10'];a.tile=t.id;Object.assign(t,{owner:'wintermere',building:'fort',name:'Observed Fort',levels:{fort:1},road:true,river:true});refreshKnowledge(s);
  a.tile='5,6';s.turn++;Object.assign(t,{owner:'vesper',building:'city',name:'UNOBSERVED_NEW_CITY',levels:{city:4},road:false,river:false,market:true,project:{type:'wall',remaining:2}});
  const v=knowledgeView(s),memory=v.tiles[t.id];assert.equal(memory.fog,'explored');assert.equal(memory.name,'Observed Fort');assert.equal(memory.owner,'wintermere');assert.equal(memory.building,'fort');assert.equal(memory.road,true);assert.equal(memory.project,null);assert.equal(memory.observedTurn,1);
  assert.doesNotMatch(JSON.stringify(v),/UNOBSERVED_NEW_CITY/);
});
test('lost armies leave a dated fixed-location estimate; re-observation updates or clears it',()=>{
  const s=emptyField(),own=s.armies[0],enemy=s.armies[1];own.tile=enemy.tile;refreshKnowledge(s);const old=enemy.tile,observedSize=Object.values(enemy.units).reduce((a,b)=>a+b,0);
  own.tile='5,6';enemy.tile='25,10';enemy.units.levy=777;s.turn++;
  const v=knowledgeView(s);assert.equal(v.armies.length,1);assert.equal(v.lastSeenArmies[0].tile,old);assert.equal(v.lastSeenArmies[0].turn,1);assert.ok(v.lastSeenArmies[0].minimum<=observedSize&&v.lastSeenArmies[0].maximum>=observedSize);
  own.tile=old;refreshKnowledge(s);assert.equal(knowledgeView(s).lastSeenArmies.length,0);
  own.tile=enemy.tile;refreshKnowledge(s);assert.equal(knowledgeView(s).armies.find(a=>a.id===enemy.id).units.levy,777);
});
test('a spy supplies real dated map observations, never live tracking after moving or capture',()=>{
  const s=emptyField(),home=s.tiles['5,6'];home.intelligenceOffice=true;home.levels.intelligenceOffice=3;s.kingdoms[0].resources.gold=400;
  const id=recruitSpy(s,'ashen').spyId;assert.ok(id);assert.equal(assignSpy(s,'ashen',id,'wintermere','military').ok,true);
  const spy=s.intelligence.agents.find(a=>a.id===id);spy.status='Embedded';spy.network=90;
  const report=gatherIntelligence(s,spy),enemy=s.armies[1],tile=enemy.tile;assert.equal(report.snapshot.armies[0].tile,tile);
  let v=knowledgeView(s);assert.equal(v.tiles[tile].intelligenceFresh,true);assert.equal(v.lastSeenArmies[0].tile,tile);assert.ok(!v.armies.some(a=>a.id===enemy.id));
  enemy.tile='27,9';enemy.units.levy=909;v=knowledgeView(s);assert.equal(v.lastSeenArmies[0].tile,tile);assert.doesNotMatch(JSON.stringify(v.lastSeenArmies),/909/);
  spy.status='Captured';v=knowledgeView(s);assert.equal(v.tiles[tile].intelligenceFresh,false);assert.equal(v.lastSeenArmies[0].intelligenceFresh,false);
  spy.status='Embedded';s.turn+=3;assert.equal(knowledgeView(s).tiles[tile].intelligenceFresh,false);
});
test('alliances share nearby owned sources without transitive exploration or spy sharing',()=>{
  const s=emptyField();s.treaties.push({type:'alliance',parties:['ashen','wintermere'],expires:20});s.kingdoms[1].relations.ashen.trust=60;
  const ally=s.armies[1];ally.tile='15,10';s.armies.push({...structuredClone(ally),id:'remote',owner:'vesper',tile:'18,10'});
  let v=knowledgeView(s);assert.ok(v.armies.some(a=>a.id===ally.id));assert.ok(!v.armies.some(a=>a.id==='remote'));assert.equal(v.tiles['18,10'].fog,'unknown');
  s.treaties=[];v=knowledgeView(s);assert.ok(!v.armies.some(a=>a.id===ally.id));
});
test('hidden mutations cannot leak through panels, forecasts, targets, dialogue or alerts',()=>{
  const s=emptyField(),t=s.tiles['15,10'];declareWar(s,'ashen','wintermere');Object.assign(t,{owner:'wintermere',building:'fort',name:'SECRET_FORT',levels:{fort:3},fortIntegrity:180});s.armies[1].units.levy=98765;
  s.events.push({turn:1,message:'SECRET_FORT built with 98765 troops',kind:'world'});s.militaryEvents.push({id:1234,turn:1,attacker:'wintermere',defender:'vesper',tile:t.id,action:'battle'});
  const before=JSON.stringify(s),context=makeContext(s,'wintermere','Where are your armies near my border?');
  const text=[JSON.stringify(knowledgeView(s)),JSON.stringify(context),scriptedReply(s,'wintermere','your armies near my border').reply,battlePreview(s,s.armies[0].id,t.id),buildingInspection(s,t),structureActions(s,t,s.armies[0].id),foreignEconomy(s,'wintermere'),rivalTurnReports(s),battleReports(s),warRoomPanel(s)].join('\n');
  assert.doesNotMatch(text,/SECRET_FORT|98765|Undefended|1234/);assert.match(text,/Scout/);assert.equal(JSON.stringify(s),before);
  assert.equal(orderStructureAttack(s,'ashen',s.armies[0].id,t.id,'fort','bombard').ok,false);
});
test('orders into unknown land use known roads; scouting records intermediate movement',()=>{
  const s=emptyField(),a=s.armies[0];a.tile='5,10';a.units={...emptyUnits(),scout:5};refreshKnowledge(s);
  assert.equal(orderArmy(s,'ashen',a.id,'18,10').ok,true);resolveMovement(s);assert.ok(Object.keys(s.fog.houses.ashen.tiles).length>61);
  assert.equal(s.fog.houses.ashen.tiles['5,10'].turn,s.turn);
});
test('multiplayer public and private transport preserve canonical simulation without hidden board data',async()=>{
  const s=emptyField();s.controllers=Object.fromEntries(HOUSES.map(h=>[h.id,{kind:'human',uid:h.id}]));s.courts={ashen:{conversations:{},messages:{turn:1,regular:0,hosts:{}},offers:{wintermere:[validateIntent({type:'AID',giveAmount:20})]}}};s.armies[1].units.levy=98765;s.tiles['15,10'].name='SECRET_REMOTE';
  const {canonical,world,privateByHouse}=splitCampaign(s);const v=playerView(world,privateByHouse.ashen,'ashen');
  assert.doesNotMatch(JSON.stringify(world),/SECRET_REMOTE|98765/);assert.doesNotMatch(JSON.stringify(v),/SECRET_REMOTE|98765/);assert.equal(v.armies.length,1);assert.equal(disclosedDeal(v,'wintermere',s.courts.ashen.offers.wintermere[0]).status,'accept');
  const restored=joinCampaign(canonical,privateByHouse);assert.equal(restored.armies[1].units.levy,98765);assert.equal(restored.tiles['15,10'].name,'SECRET_REMOTE');
  const packed=await packCampaign(s,{stateVersion:1,epoch:1});assert.ok((await decodePayload(packed.privateByHouse.ashen.payload)).view.knowledgeView==='ashen');
  assert.equal(s.kingdoms.length,6);assert.equal(s.width,40);assert.equal(s.height,30);
});
test('own economy remains accurate under fog and forged exploration records fail validation',()=>{
  const s=legacyGame();refreshKnowledge(s);assert.deepEqual(economyProjection(knowledgeView(s),'ashen'),economyProjection(s,'ashen'));
  for(const mutate of [x=>x.fog.houses.ashen.tiles['5,6'].turn=9999,x=>x.fog.houses.ashen.tiles['5,6'].tile.building='evil',x=>x.width=64]){const x=structuredClone(s);mutate(x);assert.throws(()=>parseSave(JSON.stringify(x)));}
  delete s.fog;const loaded=parseSave(JSON.stringify(s));assert.deepEqual(loaded.fog.houses,{});
});
test('a twelve-House campaign advances and reloads deterministically with diplomacy and fog',()=>{
  const s=createGame(77,'random',12),site=planFoundings(s)[0].capital;foundCity(s,'ashen',site);foundAIKingdoms(s);const r=parseSave(JSON.stringify(s));
  for(let i=0;i<3;i++){endTurn(s);endTurn(r);assert.deepEqual(parseSave(JSON.stringify(s)),r);}
});
