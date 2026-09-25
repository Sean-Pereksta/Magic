import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { armiesOf, atWar, kingdom, makePeace, orderArmy, parseSave, recruit, relation, resolveMovement, settlements, strategyTurn, treaty } from '../core.mjs';
import { emptyUnits } from '../economy.mjs';
import { endTurn, makeContext, verifyPledges } from '../diplomacy.mjs';
import { createOperation, defaultRally, leaveOperation, operationArmyOrder, operationPledgeProgress, operationProgress, prepareOperationAI, respondOperation, supplyOperation, updateOperations } from '../operations.mjs';
import { initializeCooperation, operationFor, operationMember } from '../cooperation-state.mjs';
import { balanceResponse, cooperationInterest, proposeCooperation, respondCooperation, runStrategicDiplomacy } from '../strategic-diplomacy.mjs';
import { assignSpy, captureSpy, discoverOperation, recruitSpy, visibleOperations } from '../espionage.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { warRoomPanel } from '../war-room.mjs';
import { rankObjectives, strategicLocation } from '../strategic-geography.mjs';

function ally(s,a,b){s.treaties.push({id:`test-${a}-${b}`,type:'alliance',parties:[a,b],expires:100});relation(s,a,b).trust=relation(s,b,a).trust=65;}
function setup({owner='ashen',partner='wintermere',target='vesper',role='flank',siege=0,food=0}={}) {
  const s=createGame(311);initializeCooperation(s);ally(s,owner,partner);
  for(const k of s.kingdoms){k.resources.food=k.resources.gold=400;k.commands=0;}
  const targetTile=settlements(s,target)[0].id;
  const participants=[{house:owner,role:'assault',rally:defaultRally(s,owner,targetTile),requiredTroops:10,requiredSiege:0,food:0},{house:partner,role,rally:defaultRally(s,partner,targetTile),requiredTroops:role==='supply'?0:10,requiredSiege:siege,food}];
  const terms={name:'Operation Hidden Gate',targetTile,attackStart:3,attackEnd:6,participants};
  const result=createOperation(s,owner,terms);assert.equal(result.ok,true,result.error);
  return {s,o:operationFor(s,result.operationId),terms,owner,partner,target};
}
function accept(f){assert.equal(respondOperation(f.s,f.partner,f.o.id,'accept').ok,true);return f;}
function spy(s,owner,host,network=90){
  const home=settlements(s,owner)[0];home.intelligenceOffice=true;home.levels.intelligenceOffice=3;kingdom(s,owner).commands=8;kingdom(s,owner).resources.gold=400;
  const id=recruitSpy(s,owner).spyId;assert.ok(id);assert.equal(assignSpy(s,owner,id,host,owner===host?'counter':'plans').ok,true);
  const a=s.intelligence.agents.find(a=>a.id===id);a.status='Embedded';a.network=network;return a;
}

test('shared operations require consent, link individual jointWar plans and survive saves',()=>{
  const f=setup(),{s,o}=f;assert.equal(o.status,'Preparing');assert.equal(s.intrigue.plans.filter(p=>p.operationId===o.id).length,1);assert.equal(s.pledges.filter(p=>p.operationId).length,0);
  assert.equal(respondOperation(s,'sunspire',o.id,'accept','wintermere').ok,false);
  accept(f);assert.equal(relation(s,'ashen','wintermere').personal.campaigns.length,0);assert.equal(s.intrigue.plans.filter(p=>p.operationId===o.id).length,2);assert.equal(s.pledges.filter(p=>p.operationId).length,4);
  assert.equal(respondOperation(s,'wintermere',o.id,'accept').ok,false);assert.equal(s.pledges.length,4);
  assert.equal(parseSave(JSON.stringify(s)).cooperation.operations[0].name,o.name);
});
test('malformed, unaffordable-role and treaty-conflicting operation terms cannot mutate the campaign',()=>{
  const f=setup(),s=createGame(311);ally(s,'ashen','wintermere');initializeCooperation(s);
  for(const change of [t=>t.participants=[null,null],t=>t.participants[1].house='ashen',t=>t.participants[1].requiredTroops=-1,t=>t.attackEnd=90,t=>t.participants[1].rally='999,999']){
    const terms=structuredClone(f.terms);change(terms);const before=JSON.stringify(s);assert.equal(createOperation(s,'ashen',terms).ok,false);assert.equal(JSON.stringify(s),before);
  }
  ally(s,'wintermere','vesper');assert.equal(createOperation(s,'ashen',f.terms).ok,false);
});
test('counteroffers change only the consenting House troop commitment after leader acceptance',()=>{
  const {s,o}=setup();assert.equal(respondOperation(s,'wintermere',o.id,'counter').ok,true);assert.equal(operationMember(o,'wintermere').requiredTroops,10);
  assert.equal(respondOperation(s,'wintermere',o.id,'accept','wintermere').ok,false);
  assert.equal(respondOperation(s,'ashen',o.id,'accept','wintermere').ok,true);assert.equal(operationMember(o,'wintermere').requiredTroops,5);assert.equal(s.pledges.length,4);
});
test('supplies transfer once, readiness gates the shared window, and nobody attacks early',()=>{
  const {s,o}=accept(setup({role:'supply',food:35}));s.turn=3;updateOperations(s);assert.equal(o.status,'Preparing');assert.equal(atWar(s,'ashen','vesper'),false);
  const before=kingdom(s,'wintermere').resources.food;assert.equal(supplyOperation(s,'wintermere',o.id).ok,true);assert.equal(kingdom(s,'wintermere').resources.food,before-35);assert.equal(kingdom(s,'ashen').resources.food,435);
  assert.equal(supplyOperation(s,'wintermere',o.id).ok,false);updateOperations(s);assert.equal(o.status,'Executing');assert.equal(o.launchedTurn,3);assert.equal(atWar(s,'ashen','vesper'),true);assert.equal(atWar(s,'wintermere','vesper'),false);
  verifyPledges(s,{operationsOnly:true});assert.equal(s.pledges.find(p=>p.operationTask==='supply').status,'fulfilled');
});
test('rally and attack commitments require arrival and actual combat, not issued orders',()=>{
  const {s,o}=accept(setup());const p=s.pledges.find(p=>p.debtor==='ashen'&&p.operationTask==='attack'),rally=s.pledges.find(p=>p.debtor==='ashen'&&p.operationTask==='rally');
  assert.equal(operationPledgeProgress(s,rally),'fulfilled');updateOperations(s);assert.equal(o.status,'Preparing');
  s.turn=3;updateOperations(s);assert.equal(o.status,'Executing');orderArmy(s,'ashen',armiesOf(s,'ashen')[0].id,o.targetTile,'attack');assert.equal(operationPledgeProgress(s,p),'pending');
  s.militaryEvents.push({id:s.nextId++,turn:s.turn,attacker:'ashen',defender:o.target,tile:o.targetTile,action:'battle'});assert.equal(operationPledgeProgress(s,p),'fulfilled');
});
test('siege commitments use paid recruitment events, not preexisting equipment',()=>{
  const {s,o}=accept(setup({role:'siege',siege:2})),k=kingdom(s,'wintermere'),home=settlements(s,k.id)[0],a=armiesOf(s,k.id)[0];
  a.units.ram=5;assert.equal(operationProgress(s,o,operationMember(o,k.id)).built,false);
  Object.assign(home,{siegeWorks:true});home.levels.siegeWorks=1;k.commands=8;k.population=180;for(const id of Object.keys(k.resources))k.resources[id]=500;
  assert.equal(recruit(s,k.id,home.id,'ram').ok,true);const p=s.pledges.find(p=>p.debtor===k.id&&p.operationTask==='siege');assert.equal(p.produced,2);assert.equal(operationPledgeProgress(s,p),'fulfilled');
});
test('missed promises affect trust and reputation once; peace releases remaining commitments',()=>{
  const {s,o}=accept(setup());const before=relation(s,'ashen','wintermere').trust;leaveOperation(s,'wintermere',o.id);verifyPledges(s,{operationsOnly:true});
  assert.ok(relation(s,'ashen','wintermere').trust<before);const broken=kingdom(s,'wintermere').reputation.broken;verifyPledges(s,{operationsOnly:true});assert.equal(kingdom(s,'wintermere').reputation.broken,broken);
  const next=accept(setup());next.s.turn=3;updateOperations(next.s);makePeace(next.s,'ashen','vesper');updateOperations(next.s);assert.equal(next.o.status,'Abandoned');assert.ok(next.s.pledges.every(p=>p.status==='released'));
});
test('AI follows shared rally orders instead of unrelated invasion plans',()=>{
  const {s,o}=accept(setup()),k=kingdom(s,'wintermere'),a=armiesOf(s,k.id)[0],p=operationMember(o,k.id);a.tile='18,3';
  const plan=operationArmyOrder(s,k,a,{home:s.tiles[p.rally]});assert.equal(plan.operationId,o.id);assert.equal(a.target,p.rally);assert.equal(atWar(s,k.id,o.target),false);
  const target=s.tiles[o.targetTile];Object.assign(target,{terrain:'plains'});a.tile=p.rally;s.turn=3;updateOperations(s);assert.equal(o.status,'Executing');assert.ok(atWar(s,k.id,o.target));
});
test('a shared campaign issues real orders, captures its objective and records fulfilled combat',()=>{
  const {s,o}=accept(setup());
  for(const owner of ['ashen','wintermere'])armiesOf(s,owner)[0].units={...emptyUnits(),knight:160};
  s.armies=s.armies.filter(a=>a.owner!==o.target);
  for(let n=0;n<14&&o.status!=='Completed';n++){
    for(const k of s.kingdoms)k.commands=0;
    if(atWar(s,'ashen',o.target))orderArmy(s,'ashen',armiesOf(s,'ashen')[0].id,o.targetTile,'attack');
    endTurn(s);parseSave(JSON.stringify(s));
  }
  assert.ok(relation(s,'ashen','wintermere').personal.campaigns.includes(`campaign:${o.id}`),'completed cooperation must count separately from its planning memory');
  assert.ok(relation(s,'wintermere','ashen').personal.campaigns.includes(`campaign:${o.id}`));
  assert.equal(o.status,'Completed');assert.ok(o.launchedTurn>=o.attackStart&&o.launchedTurn<=o.attackEnd);
  assert.ok(s.militaryEvents.some(e=>e.tile===o.targetTile&&e.action==='capture'&&['ashen','wintermere'].includes(e.attacker)));
  assert.ok(s.pledges.some(p=>p.operationId===o.id&&p.operationTask==='attack'&&p.status==='fulfilled'));
});
test('observed troop orders increase exposure while counterintelligence lowers it',()=>{
  const {s,o}=accept(setup());updateOperations(s);const quiet=o.exposure;
  const a=armiesOf(s,'ashen')[0];a.units.catapult=3;assert.equal(orderArmy(s,'ashen',a.id,'6,6','move').ok,true);
  updateOperations(s);assert.ok(o.exposure>quiet);assert.ok(o.exposureReasons.includes('Large troop movements'));
  const loud=o.exposure;for(const id of ['ashen','wintermere']){const guard=spy(s,id,id,100);guard.mission='counter';}
  updateOperations(s);assert.ok(o.exposure<loud);
});
test('failed preparation releases ready allies from an attack that never launched',()=>{
  const {s,o}=accept(setup({role:'supply',food:35}));verifyPledges(s,{operationsOnly:true});s.turn=o.attackEnd+1;updateOperations(s);verifyPledges(s,{operationsOnly:true});
  assert.equal(o.status,'Abandoned');assert.equal(s.pledges.find(p=>p.operationTask==='supply').status,'broken');assert.equal(s.pledges.find(p=>p.operationTask==='attack').status,'released');
});
test('operation exposure increases with actual musters and reports disclose graduated snapshots',()=>{
  const {s,o}=accept(setup({owner:'wintermere',partner:'thornwall',target:'ashen'})),a=spy(s,'ashen','wintermere',40);
  o.exposure=0;const low=discoverOperation(s,a,o);assert.equal(low.detail,1);assert.deepEqual(Object.keys(low.snapshot).sort(),['actor','observedTurn']);assert.doesNotMatch(low.text,/Hidden Gate|Ashen|Thornwall/);
  a.network=65;const medium=discoverOperation(s,a,o);assert.equal(medium.detail,2);assert.equal(medium.snapshot.target,'ashen');assert.equal(medium.snapshot.targetTile,undefined);
  a.network=90;const high=discoverOperation(s,a,o);assert.equal(high.detail,3);assert.equal(high.snapshot.targetTile,o.targetTile);
  const old=JSON.stringify(high);leaveOperation(s,'wintermere',o.id);assert.equal(JSON.stringify(high),old);assert.equal(visibleOperations(s,'ashen')[0].snapshot.status,'Preparing');parseSave(JSON.stringify(s));
});
test('strong counterintelligence conceals operations and captures identify the actual investigation',()=>{
  const {s,o}=accept(setup({owner:'wintermere',partner:'thornwall',target:'ashen'})),a=spy(s,'ashen','wintermere',85);o.exposure=0;
  const clear=discoverOperation(s,a,o);assert.equal(clear.detail,2);
  const defender=spy(s,'wintermere','wintermere');defender.mission='counter';defender.status='Embedded';defender.assignedHouse='wintermere';defender.network=100;
  const protectedReport=discoverOperation(s,a,o);assert.ok(protectedReport.detail<clear.detail);captureSpy(s,a);assert.match(s.intelligence.incidents.at(-1).action,/Acquire Strategic Plans/);
});
test('War Room, model context and multiplayer public documents cannot leak secret operation fields',()=>{
  const {s,o}=accept(setup({owner:'wintermere',partner:'thornwall',target:'ashen'}));
  assert.doesNotMatch(warRoomPanel(s),/Operation Hidden Gate/);assert.doesNotMatch(JSON.stringify(makeContext(s,'wintermere','What are your plans?')),/Operation Hidden Gate/);
  const split=splitCampaign(s);assert.equal(split.world.cooperation.operations.length,0);assert.equal(split.world.pledges.filter(p=>p.operationId).length,0);
  assert.equal(split.privateByHouse.ashen.operations.length,0);assert.equal(split.privateByHouse.wintermere.operations.length,1);
  const reconstructed=joinCampaign(split.canonical,split.privateByHouse);assert.deepEqual(reconstructed.cooperation,s.cooperation);
  const opponent=playerView(split.world,split.privateByHouse.ashen,'ashen');assert.doesNotMatch(warRoomPanel(opponent),/Operation Hidden Gate/);
  const partner=playerView(split.world,split.privateByHouse.thornwall,'thornwall');assert.match(warRoomPanel(partner),/Operation Hidden Gate/);assert.equal(partner.pledges.filter(p=>p.operationId).length,4);
  const a=spy(s,'ashen','wintermere',40);discoverOperation(s,a,o);assert.doesNotMatch(warRoomPanel(s),/Operation Hidden Gate/);
});
test('AI political plans negotiate on later turns and can reject, counter or accept',()=>{
  const s=createGame();initializeCooperation(s);const p=proposeCooperation(s,'wintermere','thornwall','alliance');assert.ok(p);runStrategicDiplomacy(s);assert.equal(p.status,'pending');assert.equal(treaty(s,p.from,p.to,'alliance'),undefined);
  relation(s,'thornwall','wintermere').trust=80;s.turn++;runStrategicDiplomacy(s);assert.equal(p.status,'accepted');assert.ok(treaty(s,p.from,p.to,'access'));
  const q=proposeCooperation(s,'sunspire','vesper','trade');assert.ok(q);assert.equal(respondCooperation(s,'vesper',q.id,'counter').ok,true);assert.equal(q.duration,6);assert.equal(treaty(s,q.from,q.to,'trade'),undefined);assert.equal(respondCooperation(s,'sunspire',q.id,'accept').ok,true);
  const r=proposeCooperation(s,'ashen','dunmere','alliance');if(r){assert.equal(respondCooperation(s,r.to,r.id,'decline').ok,true);assert.equal(treaty(s,r.from,r.to,'alliance'),undefined);}
});
test('balance-of-power responses differ with personality, fear and economic dependence',()=>{
  const s=createGame(),k=kingdom(s,'wintermere'),r=relation(s,k.id,'ashen');k.honor=.9;k.greed=.5;r.dependency=0;assert.equal(balanceResponse(s,k.id,'ashen'),'coalition');
  r.dependency=40;assert.equal(balanceResponse(s,k.id,'ashen'),'neutral');r.dependency=0;r.fear=60;k.honor=.3;assert.equal(balanceResponse(s,k.id,'ashen'),'align');
  const before=cooperationInterest(s,k.id,'thornwall','alliance').score;relation(s,k.id,'thornwall').grievance=80;assert.ok(cooperationInterest(s,k.id,'thornwall','alliance').score<before);
});
test('strategic geography can favor an important crossing over a closer plain settlement',()=>{
  const s=createGame(),origin=s.tiles['10,10'],near=s.tiles['11,10'],hub=s.tiles['13,10'];
  Object.assign(near,{terrain:'plains',building:'town',owner:'vesper',capital:null,road:false,river:false,market:false});Object.assign(hub,{terrain:'plains',building:'fort',owner:'vesper',road:true,river:true,market:true});
  assert.ok(strategicLocation(s,hub,'ashen').reasons.includes('River crossing'));assert.equal(rankObjectives(s,'ashen',[near,hub],origin)[0].tile.id,hub.id);
});
test('save validation rejects forged operation references and leaks in low-detail snapshots',()=>{
  const {s,o}=accept(setup({owner:'wintermere',partner:'thornwall',target:'ashen'}));const a=spy(s,'ashen','wintermere',40);o.exposure=0;discoverOperation(s,a,o);
  for(const mutate of [x=>x.cooperation.operations[0].attackEnd=999999,x=>x.pledges[0].operationId='OP-999999',x=>x.intelligence.reports[0].snapshot.targetTile=o.targetTile]){const bad=structuredClone(s);mutate(bad);assert.throws(()=>parseSave(JSON.stringify(bad)),/cooperation|intelligence/);}
  const old=structuredClone(s);delete old.cooperation;old.intrigue={plans:[],audit:[]};old.intelligence.reports=[];old.pledges=[];assert.deepEqual(parseSave(JSON.stringify(old)).cooperation.operations,[]);
});
test('multiplayer operation commands honor authenticated ownership and reject forged replies',()=>{
  const {state:s,meta}=onlineGame(2);const [owner,partner]=Object.keys(meta.seats).filter(id=>meta.seats[id].kind==='human');ally(s,owner,partner);const target=s.kingdoms.find(k=>![owner,partner].includes(k.id)).id,targetTile=settlements(s,target)[0].id;
  const args={name:'Online Iron Gate',targetTile,attackStart:4,attackEnd:7,participants:[owner,partner].map((house,i)=>({house,role:i?'flank':'assault',rally:defaultRally(s,house,targetTile),requiredTroops:10,requiredSiege:0,food:0}))};
  const envelope={id:'operation-command',clientId:'cooperation-test',sequence:1,uid:meta.seats[owner].uid,actorHouseId:owner,turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch};
  const result=applyCommand(s,meta,{...envelope,type:'operationCreate',args});assert.equal(result.ok,true,result.error);
  const forged=applyCommand(s,meta,{...envelope,id:'forged',sequence:2,type:'operationAnswer',args:{id:result.operationId,member:partner,decision:'accept'}});assert.equal(forged.ok,false);assert.equal(operationMember(operationFor(s,result.operationId),partner).status,'invited');
});
test('seeded strategic simulations remain deterministic and reload after each turn',()=>{
  const left=createGame(732),right=createGame(732);
  for(let n=0;n<12;n++){endTurn(left);endTurn(right);assert.deepEqual(left,right);parseSave(JSON.stringify(left));}
});
