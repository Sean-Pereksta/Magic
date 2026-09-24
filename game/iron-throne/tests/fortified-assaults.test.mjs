import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { PLAYER, declareWar, kingdom, orderArmy, orderStructureAttack, parseSave, projectedBattleLosses, resolveMovement, sizeOf, strategyTurn } from '../core.mjs';
import { emptyUnits, fortMaximum } from '../economy.mjs';
import { damageFortifications, fortificationDefense, protection, resolveFieldBattle, siegePower, siegeStep } from '../warfare.mjs';
import { assaultAssessment, createPlan, dangerousTiles, plannedArmyOrder, preparePlans } from '../plans.mjs';
import { battlePreview } from '../battle-preview.mjs';
import { structureActions } from '../intelligence-ui.mjs';

function encounter({building='city',walls=180,fort=0,attackers={levy:300},defenders=null}={}) {
  const s=createGame(321),a=s.armies[0],d=s.armies[1];
  declareWar(s,PLAYER,d.owner);
  for(let q=8;q<=14;q++)for(let r=8;r<=12;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),...attackers},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{tile:'11,10',units:{...emptyUnits(),...defenders},path:[],target:null,order:'hold',morale:1});
  const t=s.tiles[d.tile];
  Object.assign(t,{owner:d.owner,building,levels:{[building]:building==='fort'?3:1,wall:3},walls,fortIntegrity:fort,name:'Test Citadel'});
  s.armies=defenders?[a,d]:[a];
  return {s,a,d,t};
}
function aiContext(s,a,t) {
  const k=kingdom(s,a.owner);
  return {k,c:{home:s.tiles[a.tile],forces:[a],enemyTowns:[t],enemies:s.armies.filter(e=>e.owner!==a.owner),threats:[],war:true,crisis:false,wary:false}};
}

test('300 attackers immediately occupy every empty fortified settlement without losses',()=>{
  for(const building of ['city','town','fort','watchtower']) {
    const {s,a,t}=encounter({building,fort:building==='fort'?180:0});
    t.siege={turns:200,morale:.4};t.project={type:'wall',owner:t.owner,level:3,total:3,remaining:2};
    assert.equal(orderArmy(s,PLAYER,a.id,t.id,'attack').ok,true);resolveMovement(s);
    assert.equal(t.owner,PLAYER);assert.equal(a.tile,t.id);assert.equal(sizeOf(a),300);
    assert.equal(t.walls,180);assert.equal(t.fortIntegrity,building==='fort'?180:0);
    assert.equal(t.project,null);assert.equal(t.siege,null);assert.equal(a.order,'hold');assert.equal(a.target,null);assert.deepEqual(a.path,[]);
    assert.equal(s.tiles['12,10'].owner,PLAYER,'territory recalculates');
    assert.ok(s.militaryEvents.some(e=>e.action==='capture'));assert.ok(!s.militaryEvents.some(e=>e.action==='siege'));
  }
});
test('a single troop can occupy an empty fortress; dead armies do not act as defenders',()=>{
  const {s,a,t}=encounter({building:'fort',fort:180,attackers:{levy:1},defenders:{levy:0}});
  orderArmy(s,PLAYER,a.id,t.id,'attack');resolveMovement(s);
  assert.equal(t.owner,PLAYER);assert.equal(sizeOf(a),1);assert.equal(t.fortIntegrity,180);
});
test('ordinary infantry assault intact walls, route the defenders and capture in the same resolution',()=>{
  const {s,a,d,t}=encounter({defenders:{levy:25}});
  orderArmy(s,PLAYER,a.id,t.id,'attack');resolveMovement(s);
  assert.ok(s.militaryEvents.some(e=>e.action==='battle'));assert.equal(a.tile,t.id);assert.equal(t.owner,PLAYER);
  assert.ok(t.walls>0);assert.ok(t.walls<180,'successful infantry assault causes modest incidental damage');
  assert.ok(sizeOf(d)===0||d.tile!==t.id);assert.equal(t.siege,null);
});
test('a losing assault never captures or occupies the defended fortress',()=>{
  const {s,a,t}=encounter({attackers:{levy:40},defenders:{heavyInfantry:100,archer:30},building:'fort',fort:180});
  orderArmy(s,PLAYER,a.id,t.id,'attack');resolveMovement(s);
  assert.equal(t.owner,'wintermere');assert.notEqual(a.tile,t.id);assert.equal(t.walls,180);
  assert.ok(!s.militaryEvents.some(e=>e.action==='capture'));
});
test('a second garrison prevents capture after the first army loses',()=>{
  const {s,a,d,t}=encounter({attackers:{heavyInfantry:500},defenders:{levy:2}});
  s.armies.push({...structuredClone(d),id:'last-guard',units:{...emptyUnits(),levy:2}});
  orderArmy(s,PLAYER,a.id,t.id,'attack');resolveMovement(s);
  assert.notEqual(a.tile,t.id);assert.equal(t.owner,'wintermere');
  s.turn++;resolveMovement(s);assert.equal(a.tile,t.id);assert.equal(t.owner,PLAYER);
});
test('surviving trapped defenders still contest the fortress',()=>{
  const {s,a,d,t}=encounter({attackers:{levy:300},defenders:{levy:100}});
  for(const tile of Object.values(s.tiles))if(![a.tile,t.id].includes(tile.id))tile.terrain='mountain';
  orderArmy(s,PLAYER,a.id,t.id,'attack');resolveMovement(s);
  assert.ok(sizeOf(d)>0);assert.equal(d.tile,t.id);assert.equal(t.owner,d.owner);assert.notEqual(a.tile,t.id);
});
test('wall bonuses scale linearly through every tier, including zero HP',()=>{
  const {t}=encounter();
  for(const [level,maximum] of [[1,.25],[2,.40],[3,.55]])for(const fraction of [1,.75,.5,.25,0]){
    t.levels.wall=level;t.walls=60*level*fraction;
    assert.ok(Math.abs(fortificationDefense(t).wallBonus-maximum*fraction)<1e-10);
  }
});
test('fort protection scales with integrity and missing legacy integrity means full strength',()=>{
  const {t}=encounter({building:'fort',walls:0,fort:180});
  for(const fraction of [1,.75,.5,.25,0]){t.fortIntegrity=180*fraction;assert.equal(fortificationDefense(t).fortBonus,.75*fraction);}
  delete t.fortIntegrity;assert.equal(fortificationDefense(t).fortBonus,.75);
});
test('60 troops behind intact walls survive better and hurt attackers more than on open ground',()=>{
  const {a,d,t}=encounter({attackers:{levy:120},defenders:{levy:40,archer:20}});
  const fight=tile=>{const x=structuredClone(a),y=structuredClone(d);resolveFieldBattle(x,y,tile);return [sizeOf(a)-sizeOf(x),sizeOf(d)-sizeOf(y)];};
  const open=fight({...t,walls:0,building:null}),damaged=fight({...t,walls:90}),intact=fight(t);
  assert.ok(intact[0]>open[0]);assert.ok(intact[1]<open[1]);assert.ok(intact[1]<=damaged[1]);
  assert.ok(protection(t)>protection({...t,walls:90}));
});
test('siege engines damage walls substantially faster than infantry, with tiered engine value',()=>{
  const {a,t}=encounter();const infantry=siegePower(a,t,{assault:true});
  const powers=['ram','catapult','trebuchet'].map(id=>siegePower({...a,units:{...emptyUnits(),[id]:2}},t));
  assert.ok(powers[0]>=infantry*3);assert.ok(powers[1]>powers[0]);assert.ok(powers[2]>powers[1]);
  const heavy=siegePower({...a,units:{...emptyUnits(),heavyInfantry:300}},t,{assault:true});assert.ok(heavy>infantry);
});
test('siege maintenance has no percentage attrition or imaginary wall damage without engines',()=>{
  const {s,a,t}=encounter();
  for(let i=0;i<100;i++)siegeStep(s,a,t,null,()=>.1);
  assert.equal(sizeOf(a),300);assert.equal(t.walls,180);
  const guard={...a,units:{...emptyUnits(),levy:300}};siegeStep(s,a,t,guard,()=>.1);assert.equal(sizeOf(a),300);
  guard.units.archer=100;siegeStep(s,a,t,guard,()=>.1);assert.ok(sizeOf(a)<300,'real defending ranged troops can cause casualties');
});
test('bombardment damages guarded walls once per turn and never occupies from range',()=>{
  const {s,a,t}=encounter({attackers:{levy:100,catapult:3},defenders:{levy:100}});
  assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'wall','bombard').ok,true);
  const protectionBefore=protection(t);resolveMovement(s);const hp=t.walls;
  assert.ok(hp<180);assert.ok(protection(t)<protectionBefore);assert.equal(sizeOf(a),103);assert.equal(t.owner,'wintermere');assert.equal(a.tile,'10,10');
  resolveMovement(s);assert.equal(t.walls,hp);
  s.turn++;resolveMovement(s);assert.ok(t.walls<hp);
});
test('rams work up close and only engines in range contribute at a distance',()=>{
  const {s,a,t}=encounter({attackers:{ram:2},defenders:{levy:100}});
  assert.equal(orderStructureAttack(s,PLAYER,a.id,t.id,'wall','bombard').ok,true);resolveMovement(s);assert.ok(t.walls<180);
  const lone={...a,units:{...emptyUnits(),trebuchet:1}},mixed={...a,units:{...emptyUnits(),trebuchet:1,ram:100,catapult:100}};
  assert.equal(siegePower(lone,t,{range:3}),siegePower(mixed,t,{range:3}));
});
test('bombarding fort integrity preserves its building and other wall health',()=>{
  const {s,a,t}=encounter({building:'fort',fort:180,attackers:{trebuchet:2},defenders:{levy:100}});
  orderStructureAttack(s,PLAYER,a.id,t.id,'fort','bombard');resolveMovement(s);
  assert.ok(t.fortIntegrity<180);assert.equal(t.walls,180);assert.equal(t.building,'fort');assert.equal(t.levels.fort,3);
});
test('forecasts show integrity and protection, use real assault losses and never mutate saves',()=>{
  const {s,a,t}=encounter({walls:142,defenders:{levy:60,archer:20}}),before=structuredClone(s);
  const forecast=projectedBattleLosses(s,a.id,t.id),html=battlePreview(s,a.id,t.id);
  assert.equal(forecast.kind,'battle');assert.ok(forecast.yours.high>0);assert.match(html,/Citadel Walls — 142 \/ 180/);assert.match(html,/Defender Protection: \+43%/);
  assert.match(html,/Projected assault losses/);assert.doesNotMatch(html,/must fall/);assert.deepEqual(s,before);
  assert.match(structureActions(s,t,a.id),/Assault defenders/);
  s.armies=[a];assert.equal(projectedBattleLosses(s,a.id,t.id).kind,'capture');assert.match(battlePreview(s,a.id,t.id),/capture on arrival/);
});
test('AI captures empty walls immediately even with a saved hard siege requirement',()=>{
  const s=createGame(420);for(const k of s.kingdoms)k.commands=0;
  const a=s.armies.find(a=>a.owner==='wintermere'),t=s.tiles['18,4'];
  a.units={...emptyUnits(),levy:30};s.armies=s.armies.filter(x=>x.owner!=='thornwall');
  Object.assign(t,{owner:'thornwall',building:'town',terrain:'plains',levels:{town:1,wall:3},walls:180});
  declareWar(s,a.owner,t.owner);
  const plan=createPlan(s,a.owner,'invasion',{target:t.owner,targetTile:t.id,delay:0,requiredForces:20,requiredSiege:50});
  strategyTurn(s);assert.equal(a.target,t.id);assert.equal(plan.requiredSiege,0);
  resolveMovement(s);assert.equal(t.owner,a.owner);assert.equal(t.walls,180);
});
test('AI assesses actual troops and casualties instead of requiring siege',()=>{
  const {s,a,d,t}=encounter({attackers:{levy:250},defenders:{levy:25},walls:120}),{k}=aiContext(s,a,t);
  assert.equal(assaultAssessment(s,k,a,t).assault,true);
  a.units={...emptyUnits(),levy:120};d.units={...emptyUnits(),levy:100};t.walls=180;
  assert.equal(assaultAssessment(s,k,a,t).assault,false);assert.equal(assaultAssessment(s,k,a,t).bombard,false);
  assert.ok(dangerousTiles(s,k,a).has(t.id));s.armies=[a];assert.ok(!dangerousTiles(s,k,a).has(t.id));
});
test('AI can bombard a difficult fortress and reassess to assault after its protection falls',()=>{
  const {s,a,t}=encounter({attackers:{levy:180,catapult:4},defenders:{levy:100}}),{k,c}=aiContext(s,a,t);
  const plan=createPlan(s,k.id,'invasion',{target:t.owner,targetTile:t.id,delay:0,requiredForces:20});
  const assessment=assaultAssessment(s,k,a,t);assert.equal(assessment.assault,false);assert.equal(assessment.bombard,true);
  preparePlans(s,k,c);plannedArmyOrder(s,k,a,c);assert.equal(a.order,'bombard');
  resolveMovement(s);assert.ok(t.walls<180);
  damageFortifications(t,180);s.turn++;preparePlans(s,k,c);plannedArmyOrder(s,k,a,c);
  assert.equal(a.order,'attack');assert.equal(a.target,t.id);assert.equal(plan.status,'Executing');
});
test('legacy siege saves preserve integrity, orders and troops and resume into capture',()=>{
  const {s,a,t}=encounter({building:'fort',fort:90,walls:142});t.siege={turns:200,morale:.23};
  orderArmy(s,PLAYER,a.id,t.id,'attack');const restored=parseSave(JSON.stringify(s));
  assert.equal(restored.tiles[t.id].walls,142);assert.equal(restored.tiles[t.id].fortIntegrity,90);assert.deepEqual(restored.tiles[t.id].siege,t.siege);
  assert.deepEqual(restored.armies[0].units,a.units);resolveMovement(restored);
  assert.equal(restored.tiles[t.id].owner,PLAYER);assert.equal(restored.tiles[t.id].walls,142);assert.equal(restored.tiles[t.id].fortIntegrity,90);assert.equal(restored.tiles[t.id].siege,null);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(restored)));
  delete s.tiles[t.id].fortIntegrity;assert.equal(fortificationDefense(parseSave(JSON.stringify(s)).tiles[t.id]).fortBonus,.75);
});

test('structure assault orders capture an empty settlement on arrival rather than demolishing it',()=>{
  const {s,a,t}=encounter();
  orderStructureAttack(s,PLAYER,a.id,t.id,'wall','attack');resolveMovement(s);
  assert.equal(t.owner,PLAYER);assert.equal(a.tile,t.id);assert.equal(t.walls,180);assert.equal(a.structureTarget,null);
});
test('marching through a friendly settlement preserves a distant infrastructure attack order',()=>{
  const {s,a,t}=encounter({building:'lumber',walls:0});
  a.tile='9,10';Object.assign(s.tiles['10,10'],{owner:PLAYER,building:'town',levels:{town:1}});
  orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','attack');resolveMovement(s);
  assert.equal(t.owner,'wintermere');assert.ok(s.militaryEvents.some(e=>e.action==='structure'&&e.tile===t.id));
});
test('defeating an industrial garrison preserves the explicit infrastructure attack',()=>{
  const {s,a,t}=encounter({building:'lumber',walls:0,defenders:{levy:2},attackers:{heavyInfantry:300}});
  orderStructureAttack(s,PLAYER,a.id,t.id,'lumber','attack');resolveMovement(s);
  assert.equal(a.tile,t.id);assert.equal(t.owner,'wintermere');assert.equal(t.building,null);
  assert.ok(s.militaryEvents.some(e=>e.action==='structure'&&e.destroyed));
});
test('queued bombardment, damage events and siege progress survive a save round-trip',()=>{
  const {s,a,t}=encounter({attackers:{levy:100,catapult:2},defenders:{levy:100}});
  orderStructureAttack(s,PLAYER,a.id,t.id,'wall','bombard');resolveMovement(s);
  const restored=parseSave(JSON.stringify(s));assert.deepEqual(restored.tiles[t.id].siege,t.siege);
  assert.equal(restored.armies[0].order,'bombard');assert.deepEqual(restored.militaryEvents,s.militaryEvents);
  s.turn++;restored.turn++;resolveMovement(s);resolveMovement(restored);assert.deepEqual(restored,s);
});
