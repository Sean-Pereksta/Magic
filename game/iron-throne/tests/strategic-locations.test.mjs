import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { armiesOf, kingdom, relation, settlements, parseSave, canEnter } from '../core.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { strategicLocationInfo, strategicPickerView, strategicMarkers } from '../strategic-locations.mjs';
import { createOperation, respondOperation, operationArmyOrder, updateOperations, operationPledgeProgress } from '../operations.mjs';
import { operationFor } from '../cooperation-state.mjs';
import { beginCouncilMessage, makeCouncilContext, sendCouncilMessage } from '../alliance-council.mjs';
import { ownCouncil } from '../council-state.mjs';
import { sanitizeCouncilContext } from '../worker/worker.mjs';
import { splitCampaign, playerView } from '../multiplayer-state.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { warRoomPanel } from '../war-room.mjs';
function ally(s,a='ashen',b='wintermere') {
  s.treaties.push({id:`test-${a}-${b}`,type:'alliance',parties:[a,b],expires:100});
  relation(s,a,b).trust=relation(s,b,a).trust=65;
}
function setup(){const s=createGame(311);ally(s);for(const k of s.kingdoms)k.commands=0;refreshKnowledge(s);return s;}
function terms(s,targetTile,objectiveType='defend',houses=['ashen','wintermere']){return {name:'Operation Exact Pass',targetTile,objectiveType,attackStart:s.turn+1,attackEnd:s.turn+5,participants:houses.map((house,i)=>({house,role:i?'flank':'assault',rally:settlements(s,house)[0].id,requiredTroops:10,requiredSiege:0,food:0}))};}
const empty=s=>Object.values(s.tiles).find(t=>t.owner==='ashen'&&!t.building&&canEnter(s,'ashen',t)).id;

test('visible and remembered locations expose only allowed current or dated metadata',()=>{
  const s=setup(),id=empty(s),visible=strategicLocationInfo(s,'ashen',id);
  assert.equal(visible.fogState,'visible');assert.equal(visible.knownOwner,'ashen');assert.equal(visible.tileId,id);
  const remote=Object.values(s.tiles).find(t=>knowledgeView(s,'ashen').tiles[t.id].fog==='unknown'&&!t.capital);
  s.fog.houses.ashen.tiles[remote.id]={turn:1,source:'sight',tile:{id:remote.id,q:remote.q,r:remote.r,terrain:remote.terrain,owner:'vesper',name:'Remembered Pass',building:null,levels:{}}};
  remote.name='HIDDEN NEW NAME';remote.owner='sunspire';remote.building='fort';s.turn=3;
  const remembered=strategicLocationInfo(s,'ashen',remote.id);
  assert.equal(remembered.displayName,'Remembered Pass');assert.equal(remembered.knownOwner,'vesper');assert.equal(remembered.fogState,'explored');assert.equal(remembered.lastObservedTurn,1);
  assert.doesNotMatch(JSON.stringify(remembered),/HIDDEN|sunspire|fort/);
});
test('opening and selecting unexplored hexes cannot reveal authoritative tile fields or forces',()=>{
  const s=setup(),id=Object.values(knowledgeView(s,'ashen').tiles).find(t=>t.fog==='unknown'&&!t.knownCapital).id;
  const before=strategicPickerView(s,'ashen').tiles[id],beforeInfo=strategicLocationInfo(s,'ashen',id);
  Object.assign(s.tiles[id],{name:'SECRET VAULT',owner:'sunspire',building:'fort',resource:'iron',terrain:'mountain',walls:99,river:true,project:{type:'wall',remaining:1,total:2}});
  s.armies.push({...structuredClone(s.armies.find(a=>a.owner==='sunspire')),id:'army-secret',tile:id});
  const fogBefore=JSON.stringify(s.fog),view=strategicPickerView(s,'ashen'),info=strategicLocationInfo(view,'ashen',id);
  assert.deepEqual(view.tiles[id],before);assert.deepEqual(info,beforeInfo);assert.equal(info.terrain,null);assert.equal(info.knownOwner,null);assert.match(info.displayName,/Unexplored/);
  assert.ok(!view.armies.some(a=>a.id==='army-secret'));assert.equal(JSON.stringify(s.fog),fogBefore);
  assert.equal(view.tiles[id].building,null);assert.equal(view.tiles[id].resource,null);assert.equal(view.tiles[id].owner,null);
});
test('empty objectives create exact plans, accept allies, issue AI orders and survive saves',()=>{
  const s=setup(),id=empty(s),result=createOperation(s,'ashen',terms(s,id));assert.equal(result.ok,true,result.error);
  const o=operationFor(s,result.operationId);assert.equal(o.target,null);assert.equal(o.targetHouse,null);assert.equal(o.objectiveType,'defend');
  assert.equal(respondOperation(s,'wintermere',o.id,'accept').ok,true);
  s.turn=o.attackStart;updateOperations(s);assert.equal(o.status,'Executing');assert.deepEqual(s.wars,[]);
  const a=armiesOf(s,'wintermere')[0],plan=operationArmyOrder(s,kingdom(s,'wintermere'),a,{});
  assert.equal(a.target,id);assert.equal(plan.targetTile,id);assert.equal(plan.type,'defendFrontier');assert.equal(plan.target,null);
  assert.equal(parseSave(JSON.stringify(s)).cooperation.operations[0].targetTile,id);
  assert.match(warRoomPanel(s),/View on Map/);
});
test('defense completes only after real presence for two turns at the exact objective',()=>{
  const s=setup(),id=empty(s),r=createOperation(s,'ashen',terms(s,id));respondOperation(s,'wintermere',r.operationId,'accept');const o=operationFor(s,r.operationId);
  s.turn=o.attackStart;updateOperations(s);
  const obligations=s.pledges.filter(p=>p.operationTask==='defend');assert.equal(obligations.length,2);
  assert.equal(operationPledgeProgress(s,obligations[0]),'pending');
  for(const a of s.armies.filter(a=>['ashen','wintermere'].includes(a.owner)))a.tile=id;
  s.turn++;updateOperations(s,{afterMovement:true});assert.equal(o.status,'Executing');
  s.turn++;updateOperations(s,{afterMovement:true});assert.equal(o.status,'Completed');assert.deepEqual(s.wars,[]);
});
test('unexplored operation target does not acquire hidden owner or share new knowledge with allies',()=>{
  const s=setup(),id=Object.values(knowledgeView(s,'ashen').tiles).find(t=>t.fog==='unknown'&&!t.knownCapital).id;
  s.tiles[id].owner='sunspire';s.tiles[id].building='fort';s.tiles[id].name='SECRET FORT';
  const old=JSON.stringify(s.fog),r=createOperation(s,'ashen',terms(s,id,'move'));assert.equal(r.ok,true,r.error);
  const o=operationFor(s,r.operationId);assert.equal(o.target,null);assert.equal(o.targetTile,id);
  respondOperation(s,'wintermere',o.id,'accept');assert.equal(JSON.stringify(s.fog),old);
  const split=splitCampaign(s),allyView=playerView(split.world,split.privateByHouse.wintermere,'wintermere');
  assert.equal(allyView.cooperation.operations[0].targetTile,id);assert.equal(allyView.tiles[id].owner,null);assert.doesNotMatch(warRoomPanel(allyView),/SECRET FORT/);
});
test('map rally locations retain route and treaty checks, and empty friendly hexes are valid',()=>{
  const s=setup(),draft=terms(s,empty(s)),rally=empty(s);draft.participants[1].rally=rally;
  assert.equal(createOperation(s,'ashen',draft).ok,true);
  for(const id of ['999,999','__proto__','constructor',settlements(s,'vesper')[0].id]){
    const fresh=setup(),bad=terms(fresh,empty(fresh));bad.participants[1].rally=id;
    assert.equal(createOperation(fresh,'ashen',bad).ok,false,id);
  }
});
test('settlement attacks and old operation saves continue to work; siege requires a visible structure',()=>{
  const s=setup(),id=settlements(s,'vesper')[0].id,draft=terms(s,id,'attack');delete draft.objectiveType;
  const r=createOperation(s,'ashen',draft);assert.equal(r.ok,true,r.error);
  const o=operationFor(s,r.operationId);assert.equal(o.target,'vesper');delete o.objectiveType;delete o.targetHouse;
  assert.equal(parseSave(JSON.stringify(s)).cooperation.operations[0].target,'vesper');
  const fresh=setup();assert.equal(createOperation(fresh,'ashen',terms(fresh,empty(fresh),'siege')).ok,false);
});
test('operation and Council markers are private to authorized participants',()=>{
  const s=setup(),id=empty(s),r=createOperation(s,'ashen',terms(s,id));
  assert.ok(strategicMarkers(knowledgeView(s,'ashen'),'ashen').some(m=>m.operationId===r.operationId));
  assert.ok(strategicMarkers(knowledgeView(s,'wintermere'),'wintermere').some(m=>m.operationId===r.operationId));
  assert.deepEqual(strategicMarkers(s,'sunspire'),[]);
  assert.deepEqual(strategicMarkers(knowledgeView(s,'sunspire'),'sunspire'),[]);
  const c=ownCouncil(s,'ashen',true);beginCouncilMessage(s,'ashen',c.id,'Hold here',{targetTile:id,objectiveType:'hold'});
  assert.ok(strategicMarkers(knowledgeView(s,'wintermere'),'wintermere').some(m=>m.councilId===c.id));
  assert.deepEqual(strategicMarkers(knowledgeView(s,'sunspire'),'sunspire'),[]);
});
test('Council carries structured coordinates to the model and save without client metadata',()=>{
  const s=setup(),id=Object.values(knowledgeView(s,'ashen').tiles).find(t=>t.fog==='unknown'&&!t.knownCapital).id,c=ownCouncil(s,'ashen',true);
  const location={targetTile:id,objectiveType:'defend',owner:'FORGED',name:'FORGED'};
  const start=beginCouncilMessage(s,'ashen',c.id,'Hold this crossing.',location);assert.equal(start.ok,true,start.error);
  assert.deepEqual(c.messages.at(-1).location,{targetTile:id,objectiveType:'defend'});
  const context=makeCouncilContext(s,c,'ashen','Hold this crossing.',location),clean=sanitizeCouncilContext(context);
  assert.deepEqual(context.world.locationProposal,c.messages.at(-1).location);assert.deepEqual(clean.history.at(-1).location,c.messages.at(-1).location);
  assert.doesNotMatch(JSON.stringify(context.world.locationProposal),/FORGED/);
  assert.deepEqual(parseSave(JSON.stringify(s)).allianceCouncils[0].messages[0].location,c.messages[0].location);
  const before=JSON.stringify(s);assert.equal(beginCouncilMessage(s,'ashen',c.id,'Invalid',{targetTile:'__proto__',objectiveType:'defend'}).ok,false);assert.equal(JSON.stringify(s),before);
  assert.equal(sendCouncilMessage(s,'sunspire',c.id,'Forged',null,location).ok,false);
});
test('multiplayer rejects invalid location/action and forged ownership and keeps deterministic tile IDs',()=>{
  const {state:s,meta}=onlineGame(2),[owner,partner]=Object.keys(meta.seats).filter(id=>meta.seats[id].kind==='human');ally(s,owner,partner);activateForTest(s,meta,owner);
  const id=settlements(s,owner)[0].id,draft=terms(s,id,'defend',[owner,partner]);
  const envelope={id:'map-command',clientId:'map-picker-tests',sequence:1,uid:meta.seats[owner].uid,actorHouseId:owner,turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type:'operationCreate'};
  for(const change of [{targetTile:'999,999'},{targetTile:'__proto__'},{objectiveType:'teleport'}])assert.equal(applyCommand(s,meta,{...envelope,args:{...draft,...change}}).ok,false);
  assert.equal(applyCommand(s,meta,{...envelope,actorHouseId:partner,args:draft}).ok,false);
  const result=applyCommand(s,meta,{...envelope,args:{...draft,terrain:'FORGED',owner:'sunspire',name:'Operation Multiplayer Pass'}});assert.equal(result.ok,true,result.error);
  const o=operationFor(s,result.operationId);assert.equal(o.targetTile,id);assert.equal(o.targetHouse,null);assert.equal(o.terrain,undefined);assert.equal(o.owner,owner);
});
