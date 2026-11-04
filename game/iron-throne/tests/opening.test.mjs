import test from 'node:test';
import assert from 'node:assert/strict';
import { FOUNDING_WAGON, OPENING_RESOURCES, chooseRoyalDecree, decreeRemaining, initializeOpeningHouse, openingEnabled } from '../opening.mjs';
import { createGame, kingdom, commandLimit, build, buildCheck, recruit, recruitmentCost, parseSave, resolveEconomy, economyProjection, resolveMovement } from '../core.mjs';
import { constructionDuration, constructionSpec, buildingLevel, productionPlan, emptyUnits } from '../economy.mjs';
import { foundCity, foundAIKingdoms, planFoundings } from '../founding.mjs';
import { movementBudget, effectiveArmySpeed, armyArrivalTurns } from '../movement-timing.mjs';
import { createArmyDraft, moveDraftUnits, reorganizeArmy } from '../army-organization.mjs';
import { diplomaticCapacity, ambassadorCapacity } from '../living.mjs';
import { knowledgeView } from '../fog.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { constructionBrowser } from '../expansion-ui.mjs';

let baseline;
function campaign(){
  if(!baseline){const s=createGame(42,'heartlands');const site=planFoundings(s).find(p=>p.owner==='ashen').capital;assert.equal(foundCity(s,'ashen',site).ok,true);assert.equal(foundAIKingdoms(s).ok,true);baseline=s;}
  return structuredClone(baseline);
}
test('new Houses receive resources, capped opening orders and weak scouts under ordinary vision and movement',()=>{
  const s=campaign();
  for(const k of s.kingdoms){
    assert.deepEqual(k.resources,OPENING_RESOURCES);
    assert.equal(k.commands,commandLimit(s,k.id));
    assert.equal(commandLimit(s,k.id,3)-commandLimit(s,k.id,4),2);
    const scout=s.armies.find(a=>a.owner===k.id&&a.scout);
    assert.equal(scout.units.scout,2);assert.equal(Object.values(scout.units).reduce((n,x)=>n+x,0),2);assert.equal(movementBudget(s,scout),5);
  }
  const cap=Object.values(s.tiles).find(t=>t.capital==='ashen');cap.workshop=true;cap.levels.workshop=3;cap.levels.city=3;
  assert.equal(commandLimit(s,'ashen'),8);
  const view=knowledgeView(s,'ashen');assert.equal(view.kingdoms.find(k=>k.id==='wintermere').opening,undefined);
});

test('the founding wagon is delivered once per new House and never re-granted on load',()=>{
  const s=campaign();
  for(const k of s.kingdoms){
    assert.equal(k.opening.foundingWagonReceived,true);
    assert.deepEqual([k.resources.wood,k.resources.stone,k.resources.gold,k.resources.tools,k.resources.arms],[200,160,340,0,0]);
    const capital=Object.values(s.tiles).find(t=>t.capital===k.id);
    k.resources.wood-=FOUNDING_WAGON.wood;
    initializeOpeningHouse(s,k.id,capital);
    assert.equal(k.resources.wood,170);
  }
  const restored=parseSave(JSON.stringify(s));
  assert.deepEqual(restored.kingdoms.map(k=>k.resources),s.kingdoms.map(k=>k.resources));
  delete restored.kingdoms[0].opening.foundingWagonReceived;
  initializeOpeningHouse(restored,'ashen',Object.values(restored.tiles).find(t=>t.capital==='ashen'));
  assert.equal(restored.kingdoms[0].resources.wood,170,'pre-update saves gain no wagon retroactively');
});

test('selected construction tiers and the turn-four bonus match their exact boundaries',()=>{
  const s=campaign(),tile={terrain:'plains',building:null,levels:{},road:false};
  for(const type of ['tradeOutpost','fishingDock','watchtower','envoyOffice']){
    assert.equal(constructionSpec(tile,type).turns,1);
    const built={...tile,building:type,[type]:true,levels:{[type]:1}};
    assert.equal(constructionSpec(built,type).turns,3);
  }
  for(const type of ['farm','lumber']){
    assert.equal(constructionSpec({...tile,building:type,levels:{[type]:1}},type).turns,1);
    assert.equal(constructionSpec({...tile,building:type,levels:{[type]:2}},type).turns,3);
  }
  for(const type of ['quarry','mine','barracks','range']){
    s.turn=4;assert.equal(constructionSpec(tile,type,s,'ashen').turns,1);
    s.turn=5;assert.equal(constructionSpec(tile,type,s,'ashen').turns,2);
    const k=kingdom(s,'ashen');k.opening.decree='prosperity';k.opening.decreeTurn=1;
    s.turn=6;assert.equal(constructionSpec(tile,type,s,'ashen').turns,1);
    s.turn=7;assert.equal(constructionSpec(tile,type,s,'ashen').turns,2);
    const built={...tile,building:type,[type]:true,levels:{[type]:1}};
    s.turn=4;assert.equal(constructionSpec(built,type,s,'ashen').turns,3);
    k.opening.decree=null;k.opening.decreeTurn=null;
  }
  s.turn=4;
  const capital=Object.values(s.tiles).find(t=>t.capital==='ashen');
  assert.match(constructionBrowser(s,capital),/data-build="envoyOffice"[^]*?Ready turn 5/);
  assert.equal(constructionSpec(tile,'fort',s,'ashen').turns,3);
});

test('delayed Prosperity selection never extends construction acceleration beyond turn six',()=>{
  const tile={terrain:'plains',building:null,levels:{},road:false};
  for(const chosenTurn of [2,3,4,5,6]){
    const s={ambitionVersion:1,phase:'playing',turn:chosenTurn,kingdoms:[{id:'ashen',opening:{decree:null,decreeTurn:null,charterUsed:false}}]};
    assert.equal(chooseRoyalDecree(s,'ashen','prosperity').ok,true);
    assert.equal(decreeRemaining(s,'ashen'),7-chosenTurn);
    for(const type of ['mine','quarry','barracks','range']){
      s.turn=6;assert.equal(constructionSpec(tile,type,s,'ashen').turns,1);
      for(const turn of [7,8,9,10,11]){
        s.turn=turn;
        assert.equal(constructionSpec(tile,type,s,'ashen').turns,2,`${type}, decree chosen turn ${chosenTurn}, current turn ${turn}`);
        assert.equal(decreeRemaining(s,'ashen'),0);
      }
    }
  }
});

test('construction previews and actual projects share all existing efficiency bonuses',()=>{
  for(const bonus of ['workshop','siegeFoundry','lumber']){
    const s=legacyGame();s.turn=10;
    const capital=Object.values(s.tiles).find(t=>t.capital==='ashen'),k=kingdom(s,'ashen');
    for(const resource of Object.keys(k.resources))k.resources[resource]=1000;
    let type='city';
    if(bonus==='workshop'){capital.workshop=true;capital.levels.workshop=3;}
    if(bonus==='siegeFoundry'){
      capital.workshop=true;capital.levels.workshop=1;
      capital.siegeFoundry=true;capital.levels.siegeFoundry=1;
      capital.siegeWorks=true;capital.levels.siegeWorks=1;type='siegeWorks';
    }
    if(bonus==='lumber'){
      const lumber=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.building==='lumber');lumber.levels.lumber=3;
    }
    const spec=constructionSpec(capital,type,s,'ashen'),turns=constructionDuration(s,'ashen',capital,type,spec);
    assert.equal(turns,spec.turns-1);
    assert.match(constructionBrowser(s,capital),new RegExp(`data-build="${type}"[^]*?${turns} turns · Ready turn ${s.turn+turns}`));
    assert.equal(build(s,'ashen',capital.id,type).ok,true);
    assert.equal(capital.project.remaining,turns);assert.equal(capital.project.total,turns);
  }
});
test('First Frontier Charter pays real discounted materials once, keeps terrain checks and completes in two turns',()=>{
  const s=campaign(),k=kingdom(s,'ashen'),tile=Object.values(s.tiles).find(t=>!buildCheck(s,'ashen',t.id,'town'));
  assert.ok(tile);const normal=constructionSpec(tile,'town'),spec=constructionSpec(tile,'town',s,'ashen'),before={...k.resources};
  assert.equal(spec.turns,2);assert.equal(spec.cost.gold,normal.cost.gold);assert.equal(spec.cost.wood,Math.ceil(normal.cost.wood*.75));
  assert.equal(build(s,'ashen',tile.id,'town').ok,true);assert.equal(k.opening.charterUsed,true);
  for(const [r,n]of Object.entries(spec.cost))assert.equal(k.resources[r],before[r]-n);
  resolveEconomy(s);assert.equal(tile.project.remaining,1);resolveEconomy(s);assert.equal(tile.building,'town');
  const next=Object.values(s.tiles).find(t=>!buildCheck(s,'ashen',t.id,'town'));
  if(next)assert.deepEqual(constructionSpec(next,'town',s,'ashen').cost,constructionSpec(next,'town').cost);
  const water=Object.values(s.tiles).find(t=>t.terrain==='water');water.owner='ashen';assert.match(buildCheck(s,'ashen',water.id,'town'),/impassable/);
});
test('construction acceleration is temporary and never applies to upgrades or advanced defenses',()=>{
  const s=campaign(),tile={terrain:'plains',building:null,levels:{},road:false};
  assert.equal(constructionSpec(tile,'farm',s,'ashen').turns,Math.max(1,constructionSpec(tile,'farm').turns-1));
  tile.building='farm';tile.levels.farm=1;
  assert.deepEqual(constructionSpec(tile,'farm',s,'ashen'),constructionSpec(tile,'farm'));
  tile.building=null;tile.levels={};s.turn=4;
  assert.deepEqual(constructionSpec(tile,'farm',s,'ashen'),constructionSpec(tile,'farm'));
  assert.equal(chooseRoyalDecree(s,'ashen','prosperity').ok,true);
  assert.equal(constructionSpec(tile,'farm',s,'ashen').turns,Math.max(1,constructionSpec(tile,'farm').turns-1));
  assert.deepEqual(constructionSpec(tile,'fort',s,'ashen'),constructionSpec(tile,'fort'));
  s.turn=7;assert.deepEqual(constructionSpec(tile,'farm',s,'ashen'),constructionSpec(tile,'farm'));
});
test('decrees have distinct bounded effects, are chosen once, survive loading and expire after turn six',()=>{
  for(const id of ['prosperity','mobilization','influence']){
    const s=campaign(),cap=Object.values(s.tiles).find(t=>t.capital==='ashen'),k=kingdom(s,'ashen'),a=s.armies.find(a=>a.owner==='ashen'&&!a.scout);
    const normal={production:productionPlan(s,'ashen'),cost:recruitmentCost(cap,'levy'),speed:effectiveArmySpeed(s,a),dispatches:diplomaticCapacity(s,'ashen')};
    assert.equal(chooseRoyalDecree(s,'ashen',id).ok,true);assert.equal(chooseRoyalDecree(s,'ashen','influence').ok,false);assert.equal(decreeRemaining(s,'ashen'),6);
    if(id==='prosperity'){assert.ok(productionPlan(s,'ashen').gross.food>normal.production.gross.food);assert.equal(productionPlan(s,'ashen').gross.arms,normal.production.gross.arms);}
    if(id==='mobilization'){
      const cost=recruitmentCost(cap,'levy',null,s,'ashen');assert.equal(cost.gold,Math.ceil(normal.cost.gold*.9));assert.equal(effectiveArmySpeed(s,a),normal.speed+1);
      const gold=k.resources.gold;assert.equal(recruit(s,'ashen',cap.id,'levy').ok,true);assert.equal(k.resources.gold,gold-cost.gold);
    }
    if(id==='influence')assert.equal(diplomaticCapacity(s,'ashen'),normal.dispatches+1);
    const restored=parseSave(JSON.stringify(s));assert.deepEqual(restored.kingdoms.find(k=>k.id==='ashen').opening,k.opening);
    s.turn=7;assert.equal(decreeRemaining(s,'ashen'),0);assert.deepEqual(recruitmentCost(cap,'levy',null,s,'ashen'),normal.cost);assert.equal(effectiveArmySpeed(s,a),normal.speed);assert.equal(diplomaticCapacity(s,'ashen'),normal.dispatches);
  }
});
test('turn three replenishes normal turn four command capacity and old saves gain no opening assets',()=>{
  const s=campaign();s.turn=3;resolveEconomy(s);assert.equal(kingdom(s,'ashen').commands,commandLimit(s,'ashen',4));
  const old=legacyGame(),before=JSON.stringify(old),restored=parseSave(before);
  assert.equal(restored.ambitionVersion,undefined);assert.ok(restored.kingdoms.every(k=>!openingEnabled(restored,k.id)));assert.equal(chooseRoyalDecree(restored,'ashen','prosperity').ok,false);
  assert.deepEqual(restored.armies,old.armies);assert.deepEqual(restored.tiles,old.tiles);assert.deepEqual(restored.kingdoms.map(k=>k.resources),old.kingdoms.map(k=>k.resources));
  const tile=Object.values(restored.tiles).find(t=>!buildCheck(restored,'ashen',t.id,'town'));assert.equal(constructionSpec(tile,'town',restored,'ashen').turns,3);
});

test('Influence raises dispatch capacity without bypassing or disabling ambassador infrastructure',()=>{
  const s=campaign(),cap=Object.values(s.tiles).find(t=>t.capital==='ashen');
  assert.equal(chooseRoyalDecree(s,'ashen','influence').ok,true);
  assert.equal(diplomaticCapacity(s,'ashen'),4);assert.equal(ambassadorCapacity(s,'ashen'),0);
  cap.envoyOffice=true;cap.levels.envoyOffice=1;
  assert.equal(diplomaticCapacity(s,'ashen'),5);assert.equal(ambassadorCapacity(s,'ashen'),1);
  cap.chancery=true;cap.levels.chancery=1;
  assert.equal(diplomaticCapacity(s,'ashen'),6);assert.equal(ambassadorCapacity(s,'ashen'),3);
  s.turn=7;assert.equal(diplomaticCapacity(s,'ashen'),5);assert.equal(ambassadorCapacity(s,'ashen'),3);
});

test('Mobilization movement spends its extra point once, survives reorganization and expires on the next route leg',()=>{
  const s=campaign(),a=s.armies.find(a=>a.owner==='ashen'&&!a.scout);
  s.turn=6;assert.equal(chooseRoyalDecree(s,'ashen','mobilization').ok,true);
  s.armies=[a];
  for(let q=10;q<=15;q++)Object.assign(s.tiles[`${q},10`],{terrain:'plains',owner:'ashen',road:false,river:false});
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),levy:20},path:['11,10','12,10','13,10','14,10','15,10'],target:'15,10',order:'move',movementTurn:6,movementSpent:1});
  delete a.resolvedTurn;
  assert.equal(armyArrivalTurns(s,a),1);resolveMovement(s,'ashen');
  assert.equal(a.tile,'13,10');assert.equal(a.movementSpent,4);assert.equal(movementBudget(s,a),0);
  const draft=createArmyDraft(s,'ashen',a.id);moveDraftUnits(draft,0,1,'levy',4);
  assert.equal(reorganizeArmy(s,'ashen',draft).ok,true);
  for(const force of s.armies){
    assert.equal(force.movementSpent,4);assert.equal(force.resolvedTurn,6);assert.equal(movementBudget(s,force),0);
    force.path=['14,10','15,10'];force.target='15,10';force.order='move';
  }
  resolveMovement(s,'ashen');assert.ok(s.armies.every(a=>a.tile==='13,10'));
  s.turn=7;
  for(const force of s.armies)assert.equal(movementBudget(s,force),3);
  resolveMovement(s,'ashen');assert.ok(s.armies.every(a=>a.tile==='15,10'&&a.movementSpent===2));
});
test('multiplayer decree command uses active-House validation and cannot select twice',()=>{
  const {state:s,meta:m}=onlineGame(2);activateForTest(s,m,'ashen');let sequence=10;
  const command=(actor,decree)=>({id:`decree-${++sequence}`,clientId:'opening-test',sequence,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type:'royalDecree',args:{decree}});
  assert.equal(applyCommand(s,m,command('wintermere','mobilization')).ok,false);
  assert.equal(applyCommand(s,m,command('ashen','influence')).ok,true);assert.equal(applyCommand(s,m,command('ashen','prosperity')).ok,false);
  assert.equal(parseSave(JSON.stringify(s)).kingdoms.find(k=>k.id==='ashen').opening.decree,'influence');
});
