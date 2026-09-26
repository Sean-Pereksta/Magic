import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { declareWar, makePeace, atWar, treaty, relation, kingdom, settlements, parseSave } from '../core.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { warDesperation, capitulationCheck, qualitativeWarPosition, submissionResistance, desperateDiplomacy } from '../war-desperation.mjs';
import { commitDeal, evaluateDeal, makeContext, scriptedReply, relationshipResponse } from '../diplomacy.mjs';
import { runStrategyTurn } from '../strategy.mjs';
const setForce=(a,n)=>{for(const u of Object.keys(a.units))a.units[u]=0;a.units.levy=n;a.morale=1;};
function campaign(){
  const s=createGame(),ruler='sunspire',capital=settlements(s,ruler)[0];
  for(const id of ['31,20','31,21','32,20','33,20'])Object.assign(s.tiles[id],{owner:ruler,building:'town',name:`Sunspire ${id}`,capital:null});
  const ours=s.armies.find(a=>a.owner==='ashen'),theirs=s.armies.find(a=>a.owner===ruler);
  setForce(ours,100);setForce(theirs,100);ours.tile='33,21';
  declareWar(s,'ashen',ruler);refreshKnowledge(s);
  return {s,ruler,capital,ours,theirs};
}
function devastated(){
  const c=campaign(),{s,ruler,ours,theirs}=c;
  setForce(ours,400);setForce(theirs,1);
  for(const tile of settlements(s,ruler).filter(t=>!t.capital).slice(0,3))tile.owner='ashen';
  Object.assign(relation(s,ruler,'ashen'),{trust:50,opinion:30,grievance:10,reliability:75});
  refreshKnowledge(s);return c;
}
test('declarations record each House baseline once and peace/redeclaration resets it',()=>{
  const {s,ruler}=campaign(),baseline=s.warBaselines['ashen:sunspire'];
  assert.equal(baseline.sides[ruler].settlements,5);assert.equal(baseline.started,1);assert.ok(baseline.sides[ruler].militaryStrength>0);
  declareWar(s,'ashen',ruler);assert.equal(s.warBaselines['ashen:sunspire'],baseline);
  makePeace(s,'ashen',ruler);assert.equal(s.warBaselines['ashen:sunspire'],undefined);
  s.turn++;declareWar(s,'ashen',ruler);assert.equal(s.warBaselines['ashen:sunspire'].started,2);
});
test('55/45 and 65/35 wars never unlock capitulation despite perfect trust or huge gifts',()=>{
  for(const [a,b]of [[55,45],[65,35],[500,100]]){
    const {s,ruler,ours,theirs}=campaign();setForce(ours,a);setForce(theirs,b);refreshKnowledge(s);
    Object.assign(relation(s,ruler,'ashen'),{trust:100,opinion:100,fear:100,reliability:100,grievance:0});kingdom(s,'ashen').resources.gold=1000;
    assert.equal(capitulationCheck(s,ruler,'ashen',{giveAmount:1000}).eligible,false);
    assert.equal(evaluateDeal(s,ruler,{type:'VASSALAGE',giveAmount:1000}).status,'reject');
    assert.ok(atWar(s,'ashen',ruler));
  }
});
test('catastrophic defeat enables a single ratified surrender that ends war and creates directed vassalage',()=>{
  const {s,ruler}=devastated();const w=warDesperation(s,ruler,'ashen');
  assert.equal(w.band,'Broken');assert.equal(w.eligible,true);
  const intent={type:'VASSALAGE',duration:10,giveAmount:100};
  assert.equal(evaluateDeal(s,ruler,intent).status,'accept');assert.ok(atWar(s,'ashen',ruler));
  assert.equal(commitDeal(s,ruler,intent).ok,true);assert.equal(atWar(s,'ashen',ruler),false);
  assert.ok(treaty(s,'ashen',ruler,'peace'));assert.equal(treaty(s,'ashen',ruler,'vassalage').vassal,ruler);
  assert.equal(treaty(s,'ashen',ruler,'vassalage').liege,'ashen');
});
test('desperation and submission resistance are distinct; promises cannot bypass the hard gate',()=>{
  const {s,ruler}=devastated();Object.assign(kingdom(s,ruler),{honor:1,ambition:1,aggression:1,paranoia:1});
  Object.assign(relation(s,ruler,'ashen'),{trust:-80,grievance:90,reliability:0});
  kingdom(s,'ashen').reputation.broken=5;
  assert.ok(submissionResistance(s,ruler,'ashen')>=90);
  const check=capitulationCheck(s,ruler,'ashen');assert.equal(check.eligible,true);assert.equal(check.willing,false);
  assert.equal(evaluateDeal(s,ruler,{type:'VASSALAGE'}).status,'reject');
  const before=warDesperation(s,ruler,'ashen').score;
  scriptedReply(s,ruler,'I promise mercy if you become my vassal.');
  assert.equal(warDesperation(s,ruler,'ashen').score,before);
});
test('a last city with a virtually destroyed army recognizes overwhelming nearby force',()=>{
  const s=createGame(),ruler='sunspire',ours=s.armies.find(a=>a.owner==='ashen'),theirs=s.armies.find(a=>a.owner===ruler);
  declareWar(s,'ashen',ruler);setForce(ours,500);setForce(theirs,1);ours.tile='33,21';refreshKnowledge(s);
  const w=warDesperation(s,ruler,'ashen');assert.equal(w.eligible,true);assert.equal(w.capitalStatus,'under siege');
});
test('hidden enemy armies cannot produce surrender leverage or dialogue troop totals',()=>{
  const {s,ruler,ours,theirs}=campaign();setForce(ours,1000);setForce(theirs,1);ours.tile='5,6';
  s.fog.houses[ruler].armies={};refreshKnowledge(s);
  assert.equal(warDesperation(s,ruler,'ashen').eligible,false);
  const position=qualitativeWarPosition(s,ruler,'ashen');assert.equal(position.capitulationEligibility,'unavailable');
  assert.doesNotMatch(JSON.stringify(position),/1000|ownStrength|observedEnemyStrength|score/);
});
test('real military recovery and allied reinforcements reduce desperation and revoke eligibility',()=>{
  const {s,ruler,theirs}=devastated();const before=warDesperation(s,ruler,'ashen').score;
  setForce(theirs,500);refreshKnowledge(s);const recovered=warDesperation(s,ruler,'ashen');
  assert.ok(recovered.score<before);assert.equal(recovered.eligible,false);
  setForce(theirs,1);s.treaties.push({type:'alliance',parties:[ruler,'thornwall'],expires:20,id:'relief'});
  declareWar(s,'ashen','thornwall');relation(s,ruler,'thornwall').trust=70;
  const ally=s.armies.find(a=>a.owner==='thornwall');ally.tile='32,21';setForce(ally,800);refreshKnowledge(s);
  const relieved=warDesperation(s,ruler,'ashen');assert.ok(relieved.alliedAssistance>0);assert.equal(relieved.eligible,false);assert.ok(relieved.score<before);
});
test('qualitative diplomacy acknowledges collapse and corrects delusional model bravado',()=>{
  const {s,ruler}=devastated(),context=makeContext(s,ruler,'Become my vassal.');
  assert.equal(context.world.warPosition.warPosition,'catastrophic');
  assert.match(scriptedReply(s,ruler,'Become my vassal.').reply,/shattered|survival/i);
  const guarded=relationshipResponse(s,ruler,'Surrender.',{reply:'We shall see your armies shattered against our walls.',intents:[],tone:'hostile'});
  assert.doesNotMatch(guarded.reply,/against our walls/);assert.match(guarded.reply,/shattered|survival/i);
  assert.equal(knowledgeView(s,'public').warBaselines,undefined);
});
test('threshold outreach is current, unsolicited, nonbinding and does not repeat each turn',()=>{
  const {s,ruler}=devastated();desperateDiplomacy(s);const count=s.conversations[ruler].length,entry=s.conversations[ruler].at(-1);
  assert.equal(entry.kind,'war-desperation');assert.equal(entry.dispatch.mode,'ai-initiated');assert.equal(entry.dispatch.turn,s.turn);
  assert.ok(atWar(s,'ashen',ruler));assert.equal(s.diplomacy.offers[ruler][0].type,'VASSALAGE');
  s.turn++;desperateDiplomacy(s);assert.equal(s.conversations[ruler].length,count);
});
test('a collapsing AI preserves remaining forces instead of following an offensive order',()=>{
  const {s,ruler,theirs}=devastated();theirs.path=['31,21','30,21'];theirs.target='5,6';theirs.order='attack';
  runStrategyTurn(s);assert.notEqual(theirs.order,'attack');
  assert.equal(kingdom(s,ruler).goal,'DEFEND');
});
test('baseline save validation supports old wars and rejects malformed records',()=>{
  const {s}=campaign();assert.equal(parseSave(JSON.stringify(s)).warBaselines['ashen:sunspire'].sides.sunspire.settlements,5);
  delete s.warBaselines;assert.ok(parseSave(JSON.stringify(s)).warBaselines['ashen:sunspire'].legacy);
  s.warBaselines={'ashen:sunspire':{started:1000,sides:{},outreach:{}}};assert.throws(()=>parseSave(JSON.stringify(s)),/war baseline/);
});
