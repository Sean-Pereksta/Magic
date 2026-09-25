import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { armiesOf, declareWar, kingdom, orderArmy, orderStructureAttack, parseSave, relation, strategicThreat, strategyTurn } from '../core.mjs';
import { refreshKnowledge, recordSpyMapIntelligence, visionTiles } from '../fog.mjs';
import { planningView, requestAlliedIntelligence, scoutOrder } from '../ai-knowledge.mjs';
import { borderThreat, updatePoliticalState } from '../living.mjs';
import { cooperationInterest, runStrategicDiplomacy } from '../strategic-diplomacy.mjs';
import { assaultAssessment, createPlan, infrastructureTarget } from '../plans.mjs';
import { emptyUnits } from '../economy.mjs';
import { runAISpies } from '../espionage.mjs';

function field() {
  const s=createGame(551);
  for(const t of Object.values(s.tiles))Object.assign(t,{terrain:'plains',building:null,owner:null,capital:null,name:'',resource:null,road:false,river:false,levels:{},walls:0,project:null});
  for(const [i,k] of s.kingdoms.entries()){
    k.commands=0;
    const id=['22,22','5,5','35,24','5,24','20,5','35,5'][i];
    Object.assign(s.tiles[id],{owner:k.id,building:'city',capital:k.id,name:k.name,levels:{city:1}});
    const a=armiesOf(s,k.id)[0];a.tile=id;a.units={...emptyUnits(),levy:40};a.path=[];a.target=null;a.order='hold';
  }
  s.fog={version:1,houses:{}};refreshKnowledge(s);
  return s;
}
const own=s=>armiesOf(s,'wintermere')[0],enemy=s=>armiesOf(s,'ashen')[0];
function enemyChanges(s){
  enemy(s).tile='32,10';enemy(s).units={...emptyUnits(),knight:9999};enemy(s).target='5,5';
  Object.assign(s.tiles['30,12'],{owner:'ashen',building:'fort',name:'SECRET NEW FORT',levels:{fort:3},project:{type:'wall',remaining:2}});
  createPlan(s,'ashen','invasion',{target:'wintermere',targetTile:'5,5',objective:'SECRET INVASION',requiredForces:9999});
}

test('all Houses record their own vision and controlled territory',()=>{
  const s=field();assert.equal(Object.keys(s.fog.houses).length,s.kingdoms.length);
  assert.ok(s.fog.houses.wintermere.tiles['5,5']);assert.equal(s.fog.houses.wintermere.tiles['22,22'],undefined);
  s.tiles['14,10'].owner='wintermere';enemy(s).tile='14,10';refreshKnowledge(s);
  assert.ok(planningView(s,'wintermere').armies.some(a=>a.id===enemy(s).id));
  assert.ok(visionTiles(s,'wintermere').has('14,10'));
});

test('unseen troops, construction, orders and private plans cannot change a ruler decision view',()=>{
  const a=field(),b=structuredClone(a);enemyChanges(b);
  const va=planningView(a,'wintermere'),vb=planningView(b,'wintermere');
  assert.deepEqual(vb,va);
  assert.doesNotMatch(JSON.stringify(vb),/SECRET|9999/);
  assert.deepEqual(borderThreat(a,'wintermere','ashen'),borderThreat(b,'wintermere','ashen'));
  assert.equal(strategicThreat(a,'wintermere',a.tiles['5,5']),strategicThreat(b,'wintermere',b.tiles['5,5']));
  assert.deepEqual(cooperationInterest(a,'wintermere','ashen','alliance'),cooperationInterest(b,'wintermere','ashen','alliance'));
});

test('same House knowledge produces identical army decisions despite hidden enemy changes',()=>{
  const a=field();declareWar(a,'wintermere','ashen');
  // Other rulers are human controlled so their independent decisions do not
  // create new public events during this paired comparison.
  a.controllers=Object.fromEntries(a.kingdoms.map(k=>[k.id,{kind:k.id==='wintermere'?'ai':'human',uid:k.id}]));
  const b=structuredClone(a);enemyChanges(b);
  strategyTurn(a);strategyTurn(b);
  assert.deepEqual(armiesOf(a,'wintermere'),armiesOf(b,'wintermere'));
  assert.deepEqual(a.intrigue.plans.filter(p=>p.actor==='wintermere').map(({id,...p})=>p),b.intrigue.plans.filter(p=>p.actor==='wintermere').map(({id,...p})=>p));
  assert.deepEqual(a.strategy.history.at(-1).houses,b.strategy.history.at(-1).houses);
});

test('last-known armies stay fixed, weaken in confidence and widen their strength range',()=>{
  const s=field();s.turn=14;own(s).tile='15,5';enemy(s).tile='17,5';enemy(s).units={...emptyUnits(),levy:200};refreshKnowledge(s);
  own(s).tile='5,5';enemy(s).tile='30,10';enemy(s).units.levy=800;s.turn=15;refreshKnowledge(s);
  let report=planningView(s,'wintermere').armies.find(a=>a.id===enemy(s).id);
  assert.equal(report.tile,'17,5');assert.equal(report.lastSeenTurn,14);assert.ok(report.remembered);assert.equal(report.units.levy,200);
  assert.doesNotThrow(()=>assaultAssessment(s,kingdom(s,'wintermere'),own(s),s.tiles[report.tile]));
  const confidence=report.confidence,range=report.estimatedMaximum-report.estimatedMinimum;
  s.turn=20;report=planningView(s,'wintermere').armies.find(a=>a.id===enemy(s).id);
  assert.ok(report.confidence<confidence);assert.ok(report.estimatedMaximum-report.estimatedMinimum>range);
  assert.equal(report.tile,'17,5');
  own(s).tile='17,5';refreshKnowledge(s);assert.ok(!planningView(s,'wintermere').armies.some(a=>a.id===enemy(s).id));
});

test('re-observation updates the contact and old contacts expire without checking live existence',()=>{
  const s=field();own(s).tile='15,5';enemy(s).tile='17,5';refreshKnowledge(s);own(s).tile='5,5';enemy(s).tile='30,10';s.turn=2;refreshKnowledge(s);
  const removed=structuredClone(s);removed.armies=removed.armies.filter(a=>a.id!==enemy(s).id);
  assert.deepEqual(planningView(s,'wintermere').lastSeenArmies,planningView(removed,'wintermere').lastSeenArmies);
  s.turn=22;refreshKnowledge(s);assert.equal(planningView(s,'wintermere').lastSeenArmies.length,0);
  own(s).tile='29,10';refreshKnowledge(s);assert.ok(planningView(s,'wintermere').armies.some(a=>a.id===enemy(s).id&&!a.remembered));
});

test('AI spies create dated observations only for the House that acquired them',()=>{
  const s=field(),spy={id:'spy-100',owner:'wintermere',status:'Embedded',assignedHouse:'ashen',mission:'military',network:90};
  s.intelligence.agents.push(spy);recordSpyMapIntelligence(s,spy);
  assert.equal(s.fog.houses.wintermere.armies[enemy(s).id].source,'spy');
  assert.ok(!s.fog.houses.thornwall.armies[enemy(s).id]);
  const original=enemy(s).tile;enemy(s).tile='31,11';s.turn++;
  assert.equal(planningView(s,'wintermere').armies.find(a=>a.id===enemy(s).id).tile,original);
});

test('allied reports retain source dates, do not become live tracking and do not spread transitively',()=>{
  const s=field(),ally=armiesOf(s,'thornwall')[0];ally.tile='20,10';enemy(s).tile='22,10';refreshKnowledge(s);
  ally.tile='28,22';enemy(s).tile='30,10';s.turn++;
  s.treaties.push({id:'a',type:'alliance',parties:['wintermere','thornwall'],expires:30});relation(s,'thornwall','wintermere').trust=60;
  assert.ok(requestAlliedIntelligence(s,'wintermere')>=1);
  const r=s.fog.houses.wintermere.armies[enemy(s).id];assert.equal(r.tile,'22,10');assert.equal(r.turn,1);assert.equal(r.source,'ally');assert.equal(r.sharedBy,'thornwall');
  s.treaties.push({id:'b',type:'alliance',parties:['wintermere','sunspire'],expires:30});relation(s,'wintermere','sunspire').trust=60;
  requestAlliedIntelligence(s,'sunspire');assert.ok(!s.fog.houses.sunspire.armies[enemy(s).id]);
  s.treaties=[];s.turn++;assert.equal(requestAlliedIntelligence(s,'wintermere'),0);assert.equal(r.turn,1);
});

test('target selection and assault assessment cannot inspect a hidden structure or garrison',()=>{
  const a=field(),b=structuredClone(a);enemyChanges(b);declareWar(a,'wintermere','ashen');declareWar(b,'wintermere','ashen');
  assert.equal(infrastructureTarget(b,'wintermere','ashen',b.tiles['5,5']),undefined);
  assert.deepEqual(assaultAssessment(a,kingdom(a,'wintermere'),own(a),a.tiles['22,22']),assaultAssessment(b,kingdom(b,'wintermere'),own(b),b.tiles['22,22']));
  assert.equal(orderStructureAttack(b,'wintermere',own(b).id,'30,12','fort').ok,false);
});

test('AI paths cannot take advantage of secret roads',()=>{
  const a=field(),b=structuredClone(a);
  for(let q=12;q<30;q++)Object.assign(b.tiles[`${q},10`],{road:true,levels:{road:3}});
  assert.equal(orderArmy(a,'wintermere',own(a).id,'30,10').ok,true);
  assert.equal(orderArmy(b,'wintermere',own(b).id,'30,10').ok,true);
  assert.deepEqual(own(a).path,own(b).path);
});

test('scouts investigate missing contacts and keep urgent defense and commitments first',()=>{
  const s=field();own(s).units={...emptyUnits(),scout:6};enemy(s).tile='9,5';refreshKnowledge(s);enemy(s).tile='30,10';s.turn++;declareWar(s,'wintermere','ashen');
  // Lose direct sight of the last report before requesting a reconnaissance order.
  own(s).tile='3,5';s.tiles['5,5'].building=null;s.tiles['5,5'].owner=null;
  const c={threats:[],pledges:[],war:true};
  assert.equal(scoutOrder(s,kingdom(s,'wintermere'),own(s),c),true);assert.equal(own(s).target,'9,5');
  assert.equal(scoutOrder(s,kingdom(s,'wintermere'),own(s),{...c,threats:[{}]}),false);
  assert.equal(scoutOrder(s,kingdom(s,'wintermere'),own(s),{...c,pledges:[{}]}),false);
});

test('AI hires spies for military intelligence during war',()=>{
  const s=field(),k=kingdom(s,'wintermere'),t=s.tiles['5,5'];t.levels.intelligenceOffice=1;t.intelligenceOffice=true;k.resources.gold=95;k.commands=3;
  declareWar(s,k.id,'ashen');createPlan(s,k.id,'invasion',{target:'ashen',targetTile:'22,22'});
  assert.equal(runAISpies(s,k),true);const spy=s.intelligence.agents.find(a=>a.owner===k.id);
  assert.equal(spy.assignedHouse,'ashen');assert.equal(spy.mission,'military');assert.equal(k.resources.gold,35);
});

test('political observation history and balance decisions ignore hidden mobilization',()=>{
  const a=field(),b=structuredClone(a);enemyChanges(b);a.turn=b.turn=6;
  updatePoliticalState(a,{sendDispatches:false});updatePoliticalState(b,{sendDispatches:false});
  assert.deepEqual(relation(a,'wintermere','ashen'),relation(b,'wintermere','ashen'));
  runStrategicDiplomacy(a);runStrategicDiplomacy(b);
  assert.deepEqual(a.cooperation.balance.filter(x=>x.house==='wintermere'),b.cooperation.balance.filter(x=>x.house==='wintermere'));
});

test('House observations persist through save and load without refreshing hidden data',()=>{
  const s=field();own(s).tile='15,5';enemy(s).tile='17,5';refreshKnowledge(s);own(s).tile='5,5';enemy(s).tile='30,10';s.turn=2;refreshKnowledge(s);
  const loaded=parseSave(JSON.stringify(s));assert.deepEqual(loaded.fog,s.fog);
  assert.deepEqual(planningView(loaded,'wintermere'),planningView(s,'wintermere'));
});
