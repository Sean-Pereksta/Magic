import test from 'node:test';
import assert from 'node:assert/strict';
import { PLAYER, armiesOf, atWar, kingdom, parseSave, relation, settlements } from '../core.mjs';
import { RESOURCES } from '../data.mjs';
import { emptyUnits } from '../economy.mjs';
import { commitDeal, verifyPledges } from '../diplomacy.mjs';
import { assignSpy, discoverPlan, recruitSpy } from '../espionage.mjs';
import { createJointOperation, refreshOperations } from '../plans.mjs';
import { warRoomPanel } from '../war-room-ui.mjs';
import { createGame } from './fixtures/legacy-game.mjs';

const jointIntent=targetId=>({type:'JOINT_WAR',targetId,duration:8});
function stock(s){for(const k of s.kingdoms){for(const r of RESOURCES)k.resources[r]=500;k.commands=8;k.population=180;}}
function office(s,owner=PLAYER,level=3){const t=settlements(s,owner)[0];t.intelligenceOffice=true;t.levels.intelligenceOffice=level;return t;}
function embeddedSpy(s,host,network){
  office(s);kingdom(s,PLAYER).resources.gold=500;kingdom(s,PLAYER).commands=8;
  const recruited=recruitSpy(s,PLAYER);assert.equal(recruited.ok,true);
  const a=s.intelligence.agents.find(a=>a.id===recruited.spyId);
  assert.equal(assignSpy(s,PLAYER,a.id,host,'plans').ok,true);
  Object.assign(a,{status:'Embedded',network});return a;
}

test('ratified joint war creates one shared operation, linked plans and reciprocal commitments without instant war',()=>{
  const s=createGame(2201);stock(s);
  const result=commitDeal(s,'wintermere',jointIntent('thornwall'),PLAYER,{consentingHuman:true});
  assert.equal(result.ok,true);assert.equal(atWar(s,PLAYER,'thornwall'),false);assert.equal(atWar(s,'wintermere','thornwall'),false);
  assert.equal(s.intrigue.operations.length,1);
  const op=s.intrigue.operations[0];assert.deepEqual(op.participants,[PLAYER,'wintermere']);assert.equal(op.target,'thornwall');assert.equal(op.planIds.length,2);
  const plans=op.planIds.map(id=>s.intrigue.plans.find(p=>p.id===id));assert.ok(plans.every(p=>p?.operationId===op.id&&p.status==='Preparing'));
  const pledges=s.pledges.filter(p=>p.operationId===op.id);assert.equal(pledges.length,2);assert.deepEqual(new Set(pledges.map(p=>p.debtor)),new Set([PLAYER,'wintermere']));
  assert.deepEqual(new Set(op.commitments),new Set(pledges.map(p=>p.id)));assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});

test('additional agreements can expand one operation to a multi-House coalition',()=>{
  const s=createGame(2208);stock(s);
  const first=createJointOperation(s,PLAYER,'wintermere','thornwall',{targetTile:'31,5'});
  const joined=createJointOperation(s,'wintermere','sunspire','thornwall');
  assert.equal(joined.id,first.id);assert.equal(joined.participants.length,3);assert.ok(joined.participants.includes('sunspire'));
  assert.equal(joined.planIds.length,3);assert.ok(s.intrigue.plans.some(p=>p.operationId===joined.id&&p.actor==='sunspire'&&p.status==='Preparing'));
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});

test('a human coalition commitment declares war only after real rally preparation and the agreed attack window',()=>{
  const s=createGame(2202);stock(s);commitDeal(s,'wintermere',jointIntent('thornwall'),PLAYER,{consentingHuman:true});
  const op=s.intrigue.operations[0],plan=s.intrigue.plans.find(p=>p.operationId===op.id&&p.actor===PLAYER),a=armiesOf(s,PLAYER)[0];
  a.units={...emptyUnits(),levy:Math.max(40,plan.requiredForces)};a.tile=op.rallyPoints[PLAYER];
  refreshOperations(s);assert.equal(atWar(s,PLAYER,'thornwall'),false);assert.equal(plan.status,'Preparing');
  s.turn=op.attackWindow[0];refreshOperations(s);
  assert.equal(plan.status,'Committed');assert.equal(atWar(s,PLAYER,'thornwall'),true);
});

test('visible mobilization and siege preparation raise operation exposure',()=>{
  const s=createGame(2203);stock(s);const op=createJointOperation(s,PLAYER,'wintermere','thornwall',{targetTile:'31,5'});
  refreshOperations(s);const quiet=op.exposure;
  for(const id of op.participants){const a=armiesOf(s,id)[0];a.tile=op.targetTile;a.target=op.targetTile;a.path=['31,5'];a.units={...emptyUnits(),levy:40,ram:3};}
  refreshOperations(s);assert.ok(op.exposure>quiet,'expected exposure above '+quiet+', got '+op.exposure);assert.ok(op.exposure>=40);
});

test('operation intelligence progresses from suspicion to participants to exact attack window without fabricating state',()=>{
  const s=createGame(2204);stock(s);const op=createJointOperation(s,'wintermere','sunspire',PLAYER,{targetTile:'5,6'});
  const p=s.intrigue.plans.find(p=>p.operationId===op.id&&p.actor==='wintermere'),spy=embeddedSpy(s,'wintermere',40);
  let r=discoverPlan(s,spy,p);assert.equal(r.detail,1);assert.match(r.text,/preparing a military operation/);assert.equal(r.snapshot.target,undefined);assert.equal(r.snapshot.operation,undefined);
  spy.network=60;r=discoverPlan(s,spy,p);assert.equal(r.detail,2);assert.equal(r.snapshot.target,PLAYER);assert.deepEqual(r.snapshot.operation.participants,op.participants);assert.equal(r.snapshot.operation.attackWindow,undefined);
  spy.network=100;r=discoverPlan(s,spy,p);assert.equal(r.detail,3);assert.deepEqual(r.snapshot.operation.attackWindow,op.attackWindow);assert.equal(r.snapshot.operation.targetTile,op.targetTile);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});

test('War Room never reads an undiscovered enemy operation directly',()=>{
  const s=createGame(2205);stock(s);const op=createJointOperation(s,'wintermere','sunspire',PLAYER,{targetTile:'5,6'}),p=s.intrigue.plans.find(p=>p.operationId===op.id&&p.actor==='wintermere');
  let html=warRoomPanel(s);assert.doesNotMatch(html,new RegExp(op.name));assert.match(html,/No enemy operation has been discovered/);
  const spy=embeddedSpy(s,'wintermere',40);discoverPlan(s,spy,p);html=warRoomPanel(s);assert.match(html,/Suspected operation/);assert.doesNotMatch(html,new RegExp(op.name));
  spy.network=100;discoverPlan(s,spy,p);html=warRoomPanel(s);assert.match(html,new RegExp(op.name));assert.match(html,new RegExp('T'+op.attackWindow[0]+'–T'+op.attackWindow[1]));
});

test('operation pledges change reputation and trust according to actual performance',()=>{
  for(const fulfilled of [true,false]){
    const s=createGame(2206+(fulfilled?1:0));stock(s);commitDeal(s,'wintermere',jointIntent('thornwall'),PLAYER,{consentingHuman:true});
    const p=s.pledges.find(p=>p.operationId&&p.debtor==='wintermere'),before=relation(s,PLAYER,'wintermere').trust;
    if(fulfilled)s.militaryEvents.push({id:s.nextId++,turn:s.turn,attacker:'wintermere',defender:'thornwall',tile:s.intrigue.operations[0].targetTile,action:'battle'});
    s.turn=p.deadline;verifyPledges(s);
    assert.equal(p.status,fulfilled?'fulfilled':'broken');assert.equal(relation(s,PLAYER,'wintermere').trust>before,fulfilled);assert.ok(kingdom(s,'wintermere').reputation[fulfilled?'kept':'broken']>0);
  }
});
