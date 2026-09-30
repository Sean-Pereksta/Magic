import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { kingdom, sizeOf, settlements, splitArmy, mergeArmies, orderArmy, resolveMovement, economyProjection, parseSave, declareWar } from '../core.mjs';
import { GENERAL_QUALITIES, syncCommanders, generalUpkeep } from '../command-state.mjs';
import { refreshGeneralCandidates, hireGeneral, assignGeneral, detachGeneral, approveGeneralOrder, prepareGenerals, musterGenerals, recordGeneralConversation, generalContext } from '../generals.mjs';
import { issueVassalCommand, vassalArmyOrder, updateFealty } from '../vassals.mjs';
import { changeRelation } from '../living.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { resolveRound, resolutionDue } from '../multiplayer-rounds.mjs';
import { splitCampaign, joinCampaign } from '../multiplayer-state.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { resolveFieldBattle, armySpeed } from '../warfare.mjs';
import { sanitizeContext } from '../worker/worker.mjs';
import { finishRound, relationshipResponse, validateIntent } from '../diplomacy.mjs';

function candidate(s,owner='ashen',quality=1,specialty='mustering'){
  s.turn=8;s.commanders.nextOffer[owner]=8;refreshGeneralCandidates(s);
  const g=s.commanders.candidates.find(g=>g.owner===owner);assert.ok(g);g.quality=quality;g.specialty=specialty;return g;
}
function hire(s,owner='ashen',quality=1,specialty='mustering'){
  const g=candidate(s,owner,quality,specialty);kingdom(s,owner).resources.gold=1000;
  assert.equal(hireGeneral(s,owner,g.id).ok,true);assert.equal(assignGeneral(s,owner,g.id,s.armies.find(a=>a.owner===owner).id).ok,true);return g;
}
test('candidate schedules survive reload; recruitment charges once and upkeep is disclosed in the shared forecast',()=>{
  const s=createGame(),g=candidate(s),saved=parseSave(JSON.stringify(s)),before=JSON.stringify(saved.commanders);
  refreshGeneralCandidates(saved);assert.equal(JSON.stringify(saved.commanders),before);
  const gold=kingdom(s,'ashen').resources.gold,forecast=economyProjection(s,'ashen').income.gold;
  assert.equal(hireGeneral(s,'ashen',g.id).ok,true);assert.equal(hireGeneral(s,'ashen',g.id).ok,false);
  assert.equal(kingdom(s,'ashen').resources.gold,gold-GENERAL_QUALITIES[g.quality].cost);
  assert.equal(economyProjection(s,'ashen').income.gold,forecast-GENERAL_QUALITIES[g.quality].upkeep);
  assert.equal(generalUpkeep(s,'ashen'),1);
});
test('command identity, soldiers and spent movement survive splits and merges; dismissal leaves troops',()=>{
  const s=createGame(),g=hire(s,'ashen',3,'movement'),a=s.armies[0],count=sizeOf(a);
  a.movementTurn=s.turn;a.movementSpent=2;a.resolvedTurn=s.turn;
  const speed=armySpeed(a),r=splitArmy(s,'ashen',a.id);assert.equal(r.ok,true);
  const b=s.armies.find(x=>x.id===r.armyId);assert.equal(b.commandId,undefined);assert.equal(a.commandId,g.commandId);assert.equal(a.movementSpent,b.movementSpent);
  assert.equal(sizeOf(a)+sizeOf(b),count);assert.equal(s.commanders.roster.length,1);
  mergeArmies(s,'ashen',a.tile);assert.equal(sizeOf(a),count);assert.equal(a.movementSpent,2);assert.equal(armySpeed(a),speed);
  assert.equal(detachGeneral(s,'ashen',g.id,null,true,false).ok,false);
  assert.equal(detachGeneral(s,'ashen',g.id,null,true,true).ok,true);assert.equal(sizeOf(a),count);assert.equal(a.commandId,undefined);assert.equal(a.movementSpent,2);assert.equal(generalUpkeep(s,'ashen'),0);
});
test('mustering is paid, once per command per round, and cannot be farmed through detachments or reassignment',()=>{
  const s=createGame(),g=hire(s,'ashen',4),a=s.armies[0],k=kingdom(s,'ashen'),pop=k.population,food=k.resources.food,count=sizeOf(a);
  musterGenerals(s);assert.equal(sizeOf(a)-count,5);assert.equal(pop-k.population,5);assert.ok(k.resources.food<food);
  splitArmy(s,'ashen',a.id);musterGenerals(s);assert.equal(s.armies.filter(x=>x.owner==='ashen').reduce((n,a)=>n+sizeOf(a),0),count+5);
  detachGeneral(s,'ashen',g.id);assignGeneral(s,'ashen',g.id,a.id);musterGenerals(s);assert.equal(k.population,pop-5);
  s.turn++;s.commanders.lastRound=0;k.population=20;const before=sizeOf(a);musterGenerals(s);assert.equal(sizeOf(a),before);
});
test('general replies do not execute; manual orders survive replanning and late conversation records',()=>{
  const s=createGame(),g=hire(s),a=s.armies[0],home=s.tiles[a.tile],target=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.id!==a.tile&&!['mountain','water'].includes(t.terrain));
  refreshKnowledge(s);const order={kind:'rally',targets:[target.id],lossLimit:35,allowSplit:false};
  recordGeneralConversation(s,'ashen',g.id,'Gather at '+target.id,{reply:'I recommend this plan.',order});assert.equal(a.path.length,0);
  assert.equal(approveGeneralOrder(s,'ashen',g.id,order).ok,true);assert.equal(a.target,target.id);
  orderArmy(s,'ashen',a.id,home.id,'hold');g.lastPlanned=0;prepareGenerals(s,'ashen');assert.equal(a.order,'hold');assert.equal(a.target,null);
  recordGeneralConversation(s,'ashen',g.id,'Where now?',{reply:'A new plan.',order});assert.equal(a.order,'hold');
  const view=knowledgeView(s,'wintermere');assert.equal(view.commanders.roster.length,0);assert.ok(!JSON.stringify(view).includes('Gather at '+target.id));
  assert.ok(sanitizeContext(generalContext(s,'ashen',g.id,'Why have you stopped?')));
});
test('battle calculations use one command bonus and imported bonuses cannot be forged',()=>{
  const s=createGame(),g=hire(s,'ashen',4,'battle'),a=s.armies[0],enemy=s.armies[1];syncCommanders(s);
  const plain=structuredClone(a);delete plain.commandBonus;
  const fight=x=>{const b=structuredClone(enemy),copy=structuredClone(x);return resolveFieldBattle(copy,b,s.tiles[b.tile],{roll:()=>.5});};
  const normal=fight(plain),commanded=fight(a);
  assert.ok(commanded.phases[1].loss[1]>=normal.phases[1].loss[1]);assert.equal(a.commandBonus,.25);
  a.commandBonus=99;const loaded=parseSave(JSON.stringify(s));assert.equal(loaded.armies[0].commandBonus,.25);
});
test('vassal orders wait for their own activation and link to real strategic plans',()=>{
  const s=createGame(),v='wintermere',a=s.armies.find(a=>a.owner===v),target=settlements(s,'ashen')[0];
  s.sequential={version:1,order:s.kingdoms.map(k=>k.id),index:0,id:1,round:s.turn};
  s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen',v],liege:'ashen',vassal:v,expires:100});refreshKnowledge(s);
  const before=a.tile,r=issueVassalCommand(s,'ashen',v,{kind:'defend',target:target.id});assert.equal(r.ok,true);assert.equal(a.tile,before);assert.equal(a.path.length,0);
  const o=s.cooperation.vassalOrders[0];assert.ok(s.intrigue.plans.some(p=>p.id===o.planId&&p.actor===v));
  const c={home:settlements(s,v)[0],towns:settlements(s,v),threats:[]};
  assert.equal(vassalArmyOrder(s,kingdom(s,v),a,c),false);assert.equal(a.path.length,0);
  s.sequential.index=s.sequential.order.indexOf(v);s.sequential.id++;
  assert.equal(vassalArmyOrder(s,kingdom(s,v),a,c),true);assert.ok(a.path.length);assert.equal(o.status,'Marching');
  a.tile=a.path[0];a.morale=.4;refreshKnowledge(s);
  assert.equal(vassalArmyOrder(s,kingdom(s,v),a,c),true);assert.equal(o.status,'Blocked');assert.match(o.reason,/recovery/);
  assert.equal(o.target,target.id);assert.equal(a.target,c.home.id);assert.equal(a.order,'retreat');assert.ok(a.path.length);
  a.morale=1;c.threats=[{tile:c.home}];
  assert.equal(vassalArmyOrder(s,kingdom(s,v),a,c),true);assert.match(o.reason,/capital/);assert.equal(o.target,target.id);assert.equal(a.target,c.home.id);
  c.threats=[];assert.equal(vassalArmyOrder(s,kingdom(s,v),a,c),true);assert.equal(o.status,'Marching');assert.equal(a.target,target.id);
  assert.equal(parseSave(JSON.stringify(s)).cooperation.vassalOrders.length,1);
});
test('stable vassals never randomly rebel even on Insane; grave history produces a warning before rebellion',()=>{
  const s=createGame(),v='vesper';s.difficulty='insane';s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen',v],liege:'ashen',vassal:v,expires:100});
  for(let n=1;n<=10;n++){s.turn=n;updateFealty(s);}assert.equal(s.fealty[v].status,'Loyal');assert.equal(declareWar(s,v,'ashen'),false);
  const ownArmy=s.armies.find(a=>a.owner===v),liege=s.armies[0];ownArmy.units.levy=300;liege.tile=ownArmy.tile;refreshKnowledge(s);
  for(let n=11;n<=12;n++){s.turn=n;changeRelation(s,v,'ashen',{trust:-45,grievance:45},'A broken protection obligation abandoned our defense.');}
  updateFealty(s);assert.equal(s.fealty[v].status,'Rebellion Warning');assert.equal(s.wars.length,0);
  s.turn=14;updateFealty(s);assert.equal(s.fealty[v].status,'Rebellion Warning');s.turn=15;updateFealty(s);assert.equal(s.fealty[v].status,'Rebel');assert.equal(s.wars.length,1);
});
let serial=0;
const envelope=(s,m,actor,type,args={})=>({id:`new-${++serial}`,clientId:'two-clients',sequence:serial,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type,args});
const presence=()=>Object.fromEntries(Array.from({length:6},(_,i)=>[`u${i}`,{at:1000}]));
test('independent clients cannot bind on the same activation; stale replies and duplicate commands fail',()=>{
  const {state:s,meta:m}=onlineGame(6),active=m.activeHouse,other=m.turnOrder[(m.turnOrder.indexOf(active)+1)%6];
  const old=envelope(s,m,active,'tax',{policy:'low'});assert.equal(applyCommand(s,m,old).ok,true);assert.equal(applyCommand(s,m,old).ok,false);
  assert.equal(applyCommand(s,m,envelope(s,m,other,'tax',{policy:'high'})).ok,false);
  const late=envelope(s,m,active,'chat',{targetHouseId:other,message:'Accept this late reply.'});
  const end=envelope(s,m,active,'endActivation');assert.equal(applyCommand(s,m,end).ok,true);assert.equal(resolutionDue(m,presence(),1000,s),true);
  m.phase='resolving';resolveRound(s,m,presence(),1000);assert.equal(m.activeHouse,other);assert.equal(s.turn,1);assert.equal(applyCommand(s,m,late).ok,false);
  assert.throws(()=>resolveRound(s,m,presence(),1000),/locked/);
  const packed=splitCampaign(s),restored=joinCampaign(packed.canonical,packed.privateByHouse);assert.deepEqual(restored.sequential,s.sequential);
  m.epoch++;assert.equal(applyCommand(restored,m,{...envelope(restored,m,other,'tax',{policy:'low'}),epoch:m.epoch-1}).ok,false);
});
test('six activations create one economy boundary, do not move waiting armies, and survive save reload',()=>{
  const {state:s,meta:m}=onlineGame(6),first=m.activeHouse,army=s.armies.find(a=>a.owner===first),other=s.armies.find(a=>a.owner!==first),gold=kingdom(s,first).resources.gold;
  const destination=Object.values(s.tiles).find(t=>t.owner===other.owner&&t.id!==other.tile&&!['water','mountain'].includes(t.terrain));
  orderArmy(s,other.owner,other.id,destination.id);const before=other.tile;
  m.phase='resolving';resolveRound(s,m,presence(),1000);assert.equal(other.tile,before);assert.equal(kingdom(s,first).resources.gold,gold);
  for(let i=1;i<6;i++){m.phase='resolving';resolveRound(s,m,presence(),1000);}
  assert.equal(s.turn,2);assert.equal(s.roundFinished,1);assert.equal(m.activeHouse,first);assert.ok(army.resolvedTurn===1);assert.equal(parseSave(JSON.stringify(s)).sequential.id,7);
});
test('old online worlds migrate without executing queued simultaneous orders',()=>{
  const {state:s,meta:m}=onlineGame(6),a=s.armies[0];delete s.sequential;delete m.activeHouse;m.schema=1;a.target='5,6';a.path=['5,6'];a.order='move';const tile=a.tile;
  m.phase='resolving';resolveRound(s,m,presence(),1000);assert.equal(s.turn,1);assert.equal(a.tile,tile);assert.deepEqual(a.path,[]);assert.equal(a.legacyOrder.target,'5,6');assert.equal(m.schema,2);
});
test('unresolved negotiations retain their actual objection and exact terms without inventing execution',()=>{
  const s=createGame(),intent=validateIntent({type:'ALLIANCE',duration:10,giveAmount:0});s.diplomacy.offers.wintermere=[intent];s.conversations.wintermere=[{role:'player',turn:1,text:'Discuss these terms.'}];kingdom(s,'wintermere').relations.ashen.trust=-20;
  const response=relationshipResponse(s,'wintermere','Why reject those terms?',{reply:'What proposal?',intents:[],tone:'neutral'});
  assert.match(response.reply,/same terms|trust/);assert.match(response.reply,/trust/i);assert.deepEqual(response.intents,[intent]);
});

test('mustering quality ranges are bounded in both settlements and unavailable in occupied settlements',()=>{
  for(let quality=0;quality<5;quality++)for(const building of ['city','town']){
    const s=createGame(),g=hire(s,'ashen',quality),a=s.armies[0],t=s.tiles[a.tile],k=kingdom(s,'ashen');
    t.building=building;t.levels={[building]:1};k.population=45;
    const before=sizeOf(a);musterGenerals(s);
    assert.equal(sizeOf(a)-before,building==='city'?Math.min(5,2+quality):quality>=2?2:1);
    s.turn++;t.owner='wintermere';const after=sizeOf(a);musterGenerals(s);assert.equal(sizeOf(a),after);
  }
});
test('a general splits only two viable forces, preserves command identity, and proposes different objectives',()=>{
  const s=createGame(),g=hire(s,'ashen',2,'battle'),a=s.armies[0];a.units.levy=100;
  const targets=Object.values(s.tiles).filter(t=>t.owner==='ashen'&&t.id!==a.tile&&!['water','mountain'].includes(t.terrain)).slice(0,2);
  for(const t of targets){t.owner='wintermere';t.building='town';t.levels={town:1};t.walls=0;t.fortIntegrity=0;}
  declareWar(s,'ashen','wintermere');refreshKnowledge(s);const total=sizeOf(a);
  const result=approveGeneralOrder(s,'ashen',g.id,{kind:'attack',targets:targets.map(t=>t.id),allowSplit:true,lossLimit:35});
  assert.equal(result.ok,true,result.error);
  const commanded=s.armies.filter(a=>a.commandId===g.commandId);assert.equal(commanded.length,2);
  assert.equal(commanded.reduce((n,a)=>n+sizeOf(a),0),total);assert.ok(commanded.every(a=>sizeOf(a)>=32));assert.equal(new Set(commanded.map(a=>a.target)).size,2);
  assert.equal(parseSave(JSON.stringify(s)).commanders.roster.length,1);
});
test('reinforcement objectives follow observed friendly armies and stop when contact is lost',()=>{
  const s=createGame(),g=hire(s),a=s.armies[0],r=splitArmy(s,'ashen',a.id),b=s.armies.find(a=>a.id===r.armyId);
  detachGeneral(s,'ashen',g.id,b.id);delete a.playerOverride;
  const sites=Object.values(s.tiles).filter(t=>t.owner==='ashen'&&t.id!==a.tile&&!['water','mountain'].includes(t.terrain));b.tile=sites[0].id;refreshKnowledge(s);
  assert.equal(approveGeneralOrder(s,'ashen',g.id,{kind:'reinforce',army:b.id,targets:[b.tile],allowSplit:false,lossLimit:35}).ok,true);assert.equal(a.target,b.tile);
  s.turn++;b.tile=sites[1].id;refreshKnowledge(s);prepareGenerals(s,'ashen');assert.equal(a.target,b.tile);
  s.turn++;s.armies=s.armies.filter(x=>x!==b);prepareGenerals(s,'ashen');assert.equal(g.objective.status,'Blocked');assert.equal(a.order,'hold');assert.equal(a.path.length,0);
  assert.equal(parseSave(JSON.stringify(s)).commanders.roster[0].objective.army,b.id);
});
test('online general upkeep is charged once for a full round, including unassigned officers',()=>{
  const {state:s,meta:m}=onlineGame(6),g=hire(s,'ashen',3,'battle');detachGeneral(s,'ashen',g.id);
  s.sequential.round=s.turn;m.turn=s.turn;const baseline=structuredClone(s),bm=structuredClone(m);baseline.commanders.roster=[];
  for(let i=0;i<6;i++){m.phase='resolving';bm.phase='resolving';resolveRound(s,m,presence(),1000);resolveRound(baseline,bm,presence(),1000);}
  assert.equal(kingdom(baseline,'ashen').resources.gold-kingdom(s,'ashen').resources.gold,GENERAL_QUALITIES[g.quality].upkeep);
});
test('completed vassal objectives release their plan slot and imported orphan plans fail validation',()=>{
  const s=createGame(),v='wintermere',a=s.armies.find(a=>a.owner===v),target=settlements(s,'ashen')[0];a.tile=target.id;
  s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen',v],liege:'ashen',vassal:v,expires:100});refreshKnowledge(s);
  assert.equal(issueVassalCommand(s,'ashen',v,{kind:'rally',target:target.id}).ok,true);
  const c={home:target,towns:[target],threats:[]};vassalArmyOrder(s,kingdom(s,v),a,c);
  const o=s.cooperation.vassalOrders[0],p=s.intrigue.plans.find(p=>p.id===o.planId);assert.equal(o.status,'Completed');assert.equal(p.status,'Completed');
  assert.deepEqual(parseSave(JSON.stringify(s)).cooperation.vassalOrders,s.cooperation.vassalOrders);
  p.status='Preparing';assert.throws(()=>parseSave(JSON.stringify(s)),/vassal command/);
});
test('invented completion claims are replaced by the current engine status',()=>{
  const s=createGame(),g=hire(s);recordGeneralConversation(s,'ashen',g.id,'How is the campaign?',{reply:'We have captured every enemy town.',order:null});
  assert.doesNotMatch(g.history.at(-1).text,/captured every enemy/);
});

test('standing vassal defenses persist after arrival without repeated announcements',()=>{
  const s=createGame(),v='wintermere',a=s.armies.find(a=>a.owner===v),target=settlements(s,'ashen')[0];
  s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen',v],liege:'ashen',vassal:v,expires:100});refreshKnowledge(s);
  assert.equal(issueVassalCommand(s,'ashen',v,{kind:'defend',target:target.id}).ok,true);
  const c={home:settlements(s,v)[0],towns:settlements(s,v),threats:[]};a.tile=target.id;vassalArmyOrder(s,kingdom(s,v),a,c);
  const o=s.cooperation.vassalOrders[0],p=s.intrigue.plans.find(p=>p.id===o.planId),messages=s.conversations[v].length;
  assert.equal(o.status,'Holding');assert.notEqual(p.status,'Completed');s.turn++;vassalArmyOrder(s,kingdom(s,v),a,c);
  assert.equal(o.status,'Holding');assert.equal(a.order,'hold');assert.equal(s.conversations[v].length,messages);
  a.tile=c.home.id;vassalArmyOrder(s,kingdom(s,v),a,c);assert.equal(o.status,'Marching');assert.equal(a.target,target.id);
  assert.equal(parseSave(JSON.stringify(s)).cooperation.vassalOrders[0].target,target.id);
});

test('blocked general and vassal routes clear obsolete automatic movement',()=>{
  const s=createGame(),g=hire(s),a=s.armies[0],target=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.id!==a.tile&&!['mountain','water'].includes(t.terrain));
  refreshKnowledge(s);assert.equal(approveGeneralOrder(s,'ashen',g.id,{kind:'rally',targets:[target.id],allowSplit:false,lossLimit:35}).ok,true);assert.ok(a.path.length);
  target.terrain='mountain';s.turn++;refreshKnowledge(s);prepareGenerals(s,'ashen');
  assert.equal(g.objective.status,'Blocked');assert.equal(a.order,'hold');assert.equal(a.path.length,0);
  const v='wintermere',b=s.armies.find(a=>a.owner===v),home=settlements(s,v)[0];
  s.treaties.push({id:'vassal-test',type:'vassalage',parties:['ashen',v],liege:'ashen',vassal:v,expires:100});
  target.terrain='plains';refreshKnowledge(s);assert.equal(issueVassalCommand(s,'ashen',v,{kind:'rally',target:target.id}).ok,true);
  const c={home,towns:[home],threats:[]};assert.equal(vassalArmyOrder(s,kingdom(s,v),b,c),true);assert.ok(b.path.length);
  target.terrain='mountain';refreshKnowledge(s);vassalArmyOrder(s,kingdom(s,v),b,c);
  assert.equal(s.cooperation.vassalOrders[0].status,'Blocked');assert.equal(b.order,'hold');assert.equal(b.path.length,0);
});

test('joint-war consent defers each waiting House declaration until its own activation',async()=>{
  const {commitDeal}=await import('../diplomacy.mjs'),{activateForTest}=await import('./fixtures/online-game.mjs');
  const {state:s,meta:m}=onlineGame(2);activateForTest(s,m,'ashen');
  s.treaties.push({type:'alliance',parties:['ashen','wintermere'],expires:30});
  const intent=validateIntent({type:'JOINT_WAR',targetId:'thornwall',duration:10,giveAmount:0});
  const result=commitDeal(s,'wintermere',intent,'ashen',{consentingHuman:true});assert.equal(result.ok,true,result.error);
  assert.ok(s.wars.includes('ashen:thornwall'));assert.ok(!s.wars.includes('thornwall:wintermere'));
  assert.deepEqual(s.pledges.at(-1).pendingWarParties,['wintermere']);const tile=s.armies.find(a=>a.owner==='wintermere').tile;
  m.phase='resolving';resolveRound(s,m,presence(),1000);assert.equal(m.activeHouse,'wintermere');assert.ok(s.wars.includes('thornwall:wintermere'));
  assert.equal(s.armies.find(a=>a.owner==='wintermere').tile,tile);assert.deepEqual(s.pledges.at(-1).pendingWarParties,[]);
  assert.equal(parseSave(JSON.stringify(s)).sequential.id,s.sequential.id);
});
