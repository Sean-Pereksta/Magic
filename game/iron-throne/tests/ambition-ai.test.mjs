import test from 'node:test';
import assert from 'node:assert/strict';
import { armiesOf, createGame, distance, kingdom, rebuildTerritory, relation, settlements } from '../core.mjs';
import { emptyUnits } from '../economy.mjs';
import { refreshKnowledge } from '../fog.mjs';
import { premiumExpansionScore, premiumObjectives } from '../ambition-ai.mjs';
import { chooseAIOpeningDecree, runStrategyTurn } from '../strategy.mjs';
import { runStrategicDiplomacy, strategicOpportunities, respondCooperation } from '../strategic-diplomacy.mjs';
import { foundCity, foundAIKingdoms, planFoundings } from '../founding.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';

function field(){
  const s=legacyGame(551);
  for(const t of Object.values(s.tiles))Object.assign(t,{terrain:'plains',building:null,owner:null,capital:null,resource:null,road:false,river:false,levels:{},walls:0,project:null});
  for(const [i,k] of s.kingdoms.entries()){
    k.commands=0;k.resources={food:220,wood:170,stone:140,iron:75,gold:300,horses:16,tools:0,arms:0};
    const id=['11,5','5,5','35,24','5,24','20,24','35,5'][i];
    Object.assign(s.tiles[id],{owner:k.id,building:'city',capital:k.id,name:k.name,levels:{city:1}});
    Object.assign(armiesOf(s,k.id)[0],{tile:id,units:{...emptyUnits(),levy:40},path:[],target:null,order:'hold'});
  }
  const scout={...structuredClone(armiesOf(s,'wintermere')[0]),id:'royal-scout',scout:true,units:{...emptyUnits(),scout:2}};
  s.armies.push(scout);
  s.fog={version:1,houses:{}};
  return s;
}
function rich(s,id='9,5',resource='iron'){
  Object.assign(s.tiles[id],{terrain:resource==='iron'?'hills':'plains',resource,premiumRegion:`rich-${id}`,premiumYield:resource,premiumFactor:2});
  return s.tiles[id];
}

test('premium objectives favor actual shortages and legal frontier progress without reading undiscovered deposits',()=>{
  const s=field(),k=kingdom(s,'wintermere');k.resources.iron=0;
  const deposit=rich(s);refreshKnowledge(s);
  const objectives=premiumObjectives(s,k.id);
  assert.equal(objectives[0].tile.id,deposit.id);assert.equal(objectives[0].resource,'iron');
  assert.ok(premiumExpansionScore(s,k.id,s.tiles['8,5'],objectives)>premiumExpansionScore(s,k.id,s.tiles['2,5'],objectives));
  const hidden=structuredClone(s);rich(hidden,'30,12','food');hidden.tiles['30,12'].owner='ashen';
  assert.deepEqual(premiumObjectives(hidden,k.id),objectives);
  assert.deepEqual(strategicOpportunities(hidden,k.id),strategicOpportunities(s,k.id));
  const comfortable=structuredClone(s);kingdom(comfortable,k.id).resources.iron=500;
  assert.ok(premiumObjectives(comfortable,k.id)[0].score<objectives[0].score);
});

test('AI decrees respond to security, development and observed trade needs, and never appear in older campaigns',()=>{
  const old=field();assert.equal(chooseAIOpeningDecree(old,'wintermere'),null);assert.equal(kingdom(old,'wintermere').opening,undefined);
  for(const [expected,personality] of [['mobilization',{aggression:.95,ambition:.9,greed:.1,honor:.2}],['prosperity',{aggression:.1,ambition:.3,greed:.9,honor:.2}],['influence',{aggression:.1,ambition:.3,greed:.2,honor:.95}]]){
    const s=field(),k=kingdom(s,'wintermere');s.ambitionVersion=1;
    for(const h of s.kingdoms)h.opening={charterUsed:false,decree:null,decreeTurn:null};
    Object.assign(k,personality);
    if(expected==='influence'){
      k.resources.iron=0;Object.assign(s.tiles['10,5'],{owner:'ashen',terrain:'hills',resource:'iron',building:'mine',levels:{mine:1}});
    }
    refreshKnowledge(s);
    assert.equal(chooseAIOpeningDecree(s,k.id),expected);assert.equal(k.opening.decree,expected);
    assert.equal(chooseAIOpeningDecree(s,k.id),null,'the choice is made once');
  }
});

test('dedicated scouts remain separate and explore without receiving a combat assignment',()=>{
  const s=field();refreshKnowledge(s);const scout=armiesOf(s,'wintermere').find(a=>a.scout),count=armiesOf(s,'wintermere').length;
  runStrategyTurn(s,()=>{},'wintermere');
  assert.equal(armiesOf(s,'wintermere').length,count);assert.equal(scout.scout,true);assert.equal(scout.order,'move');assert.ok(scout.path.length);
  assert.notEqual(scout.target,settlements(s,'ashen')[0].id);assert.equal(scout.commandId,undefined);
});

test('AI develops a known rich producer when its resource is scarce',()=>{
  const s=field(),k=kingdom(s,'wintermere');k.commands=1;k.resources.iron=0;k.resources.food=800;
  const tile=rich(s,'7,5');tile.owner=k.id;refreshKnowledge(s);runStrategyTurn(s,()=>{},k.id);
  assert.equal(tile.project?.type,'mine');
  const action=s.strategy.history.at(-1).houses.find(h=>h.owner===k.id).actions.find(a=>a.kind==='build');
  assert.equal(action.tile,tile.id);
});

test('the AI directs a legal frontier charter toward a known useful rich site',()=>{
  const s=field(),k=kingdom(s,'wintermere');s.ambitionVersion=1;
  for(const h of s.kingdoms)h.opening={charterUsed:false,decree:null,decreeTurn:null};
  Object.assign(s.tiles['14,5'],{...s.tiles['11,5'],id:'14,5',q:14,r:5});
  Object.assign(s.tiles['11,5'],{owner:null,building:null,capital:null,levels:{}});armiesOf(s,'ashen')[0].tile='14,5';
  rebuildTerritory(s);const tile=rich(s);tile.owner=null;k.commands=1;k.resources.iron=0;k.resources.food=800;
  refreshKnowledge(s);runStrategyTurn(s,()=>{},k.id);
  const action=s.strategy.history.at(-1).houses.find(h=>h.owner===k.id).actions.find(a=>a.kind==='build');
  assert.equal(action.building,'town');assert.ok(distance(s.tiles[action.tile],tile)<=3);
  assert.notEqual(action.tile,tile.id,'the mine site is preserved for productive development');assert.equal(k.opening.charterUsed,true);
});

test('an observed contested site creates early contact once per material issue with no scheduled deadline',()=>{
  const quiet=field();refreshKnowledge(quiet);runStrategicDiplomacy(quiet,'wintermere');
  assert.equal(quiet.conversations.wintermere?.filter(m=>m.role==='ruler').length||0,0);
  const s=field(),k=kingdom(s,'wintermere');k.resources.iron=0;rich(s);
  s.tiles['10,5'].owner='ashen';refreshKnowledge(s);
  assert.ok(strategicOpportunities(s,k.id).some(o=>o.kind==='frontier'));
  runStrategicDiplomacy(s,k.id);
  const first=s.conversations.wintermere.filter(m=>m.role==='ruler');assert.equal(first.length,1);assert.match(first[0].text,/iron|boundaries|claims/);
  assert.equal(first[0].turn,1);
  for(const turn of [2,6,12,20]){s.turn=turn;runStrategicDiplomacy(s,k.id);}
  assert.equal(s.conversations.wintermere.filter(m=>m.role==='ruler').length,1,'an unresolved concern is not repeated after a simple cooldown');
  assert.equal(s.wars.length,0,'competition alone never declares war');
  s.tiles['9,5'].owner='ashen';s.turn=21;refreshKnowledge(s);const opinion=relation(s,k.id,'ashen').opinion;
  runStrategicDiplomacy(s,k.id);
  assert.equal(s.conversations.wintermere.filter(m=>m.role==='ruler').length,2,'observed control is a material change');
  assert.ok(relation(s,k.id,'ashen').opinion<opinion);assert.equal(s.wars.length,0);
});

test('genuine supply contact offers structured terms and remembers a refusal',()=>{
  const s=field(),k=kingdom(s,'wintermere');k.resources.iron=0;
  armiesOf(s,k.id).find(a=>a.scout).tile='6,5';
  for(let q=5;q<=11;q++)Object.assign(s.tiles[`${q},5`],{road:true,levels:{road:1,...s.tiles[`${q},5`].levels}});
  Object.assign(s.tiles['10,5'],{owner:'ashen',terrain:'hills',resource:'iron',building:'mine',levels:{mine:1,road:1}});
  refreshKnowledge(s);runStrategicDiplomacy(s,k.id);
  const offer=s.cooperation.proposals.find(p=>p.from===k.id&&p.to==='ashen');assert.ok(offer);assert.equal(offer.kind,'trade');assert.match(offer.reason,/iron/);
  assert.equal(s.treaties.some(t=>t.type==='trade'),false,'an invitation does not ratify an agreement');
  assert.equal(respondCooperation(s,'ashen',offer.id,'decline').ok,true);
  assert.ok(k.memories.some(m=>m.subject==='ashen'&&/declined/.test(m.text)));
});

test('new AI Houses spend the accelerated opening on real projects and use their frontier charter',()=>{
  const s=createGame(42,'heartlands'),site=planFoundings(s).find(p=>p.owner==='ashen').capital;
  assert.equal(foundCity(s,'ashen',site).ok,true);assert.equal(foundAIKingdoms(s).ok,true);
  runStrategyTurn(s,()=>{},'wintermere');const k=kingdom(s,'wintermere'),report=s.strategy.history.at(-1).houses.find(h=>h.owner===k.id);
  assert.ok(k.opening.decree);assert.equal(k.opening.charterUsed,true);
  assert.ok(report.orders>=4,`uses productive opening commands: ${report.orders}`);
  assert.ok(report.actions.some(a=>a.kind==='build'&&a.building==='town'));
  assert.ok(Object.values(k.resources).every(n=>n>=0));
});
