import test from 'node:test';
import assert from 'node:assert/strict';
import {setupMeta,claimSeat,startCampaign,houseIds,resolutionDue,resolveRound,applyBoundaryTakeovers,requestTakeover,seatFor,ownsLease,GRACE_MS} from '../multiplayer-rounds.mjs';
import {applyCommand} from '../multiplayer-commands.mjs';
import {court,aiControlledHouseIds} from '../house-control.mjs';
import {splitCampaign,joinCampaign,playerView,packCampaign,encodePayload,decodePayload,guardDocument} from '../multiplayer-state.mjs';
import {kingdom,settlements,treaty,atWar,declareWar,parseSave,createGame} from '../core.mjs';
import {commitDeal,makeContext,endTurn,acceptRulerMemories} from '../diplomacy.mjs';
import {consumeMessage,appendConversation} from '../living.mjs';
function setup(count=2){const meta=setupMeta({hostUid:'u0'},1000);for(let i=0;i<count;i++)claimSeat(meta,`u${i}`,`Ruler ${i}`,houseIds[i]);const state=startCampaign(meta,'u0',1000);meta.epoch=1;return {state,meta};}
let serial=0;
function command(s,m,actor,type,args={}){const i=++serial;return {id:`cmd-${i}`,clientId:'test-client',sequence:i,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,type,args};}
const terms=(type,fields={})=>({type,duration:10,giveAmount:0,giveResource:'gold',receiveAmount:0,receiveResource:'food',targetId:'',...fields});

test('two humans get distinct orders and four full AI turns in several deterministic rounds',()=>{
 const one=setup(),two=setup();
 for(const {state:s,meta:m} of [one,two])for(let turn=1;turn<=3;turn++){
  for(const id of houseIds.slice(0,2)){const cap=settlements(s,id)[0];assert.equal(applyCommand(s,m,command(s,m,id,'recruit',{tile:cap.id,unit:'levy'})).ok,true);}
  const humanTax=kingdom(s,'wintermere').tax='low';m.phase='resolving';resolveRound(s,m,{u0:{at:1000},u1:{at:1000}},1000);
  assert.equal(s.strategy.history.at(-1).houses.length,4);assert.equal(kingdom(s,'wintermere').tax,humanTax);assert.equal(s.turn,turn+1);
 }
 assert.deepEqual(one.state,two.state);
});
test('six humans can rule all six Houses without normal strategy running for any of them',()=>{
 const {state:s,meta:m}=setup(6);
 for(const id of houseIds)assert.equal(applyCommand(s,m,command(s,m,id,'tax',{policy:'low'})).ok,true);
 m.phase='resolving';resolveRound(s,m,Object.fromEntries(houseIds.map((id,i)=>[`u${i}`,{at:1000}])),1000);
 assert.deepEqual(aiControlledHouseIds(s),[]);assert.equal(s.strategy.history.at(-1).houses.length,0);assert.ok(s.kingdoms.every(k=>k.tax==='low'));
 assert.equal(parseSave(JSON.stringify(s)).turn,2);
});
test('orders cannot spend another House resources or take another army, and replay/turn/epoch checks fail closed',()=>{
 const {state:s,meta:m}=setup();const before=JSON.stringify(s);
 for(const [type,args]of [['recruit',{tile:'17,4',unit:'levy'}],['order',{army:s.armies[1].id,tile:'17,4',order:'hold'}],['build',{tile:'17,4',building:'market'}]])assert.equal(applyCommand(s,m,command(s,m,'ashen',type,args)).ok,false);
 assert.equal(JSON.stringify(s),before);
 const c=command(s,m,'ashen','tax',{policy:'low'});assert.equal(applyCommand(s,m,c).ok,true);assert.equal(applyCommand(s,m,c).ok,false);
 for(const patch of [{uid:'u1'},{turn:0},{epoch:0},{stateVersion:m.stateVersion+1}])assert.equal(applyCommand(s,m,{...command(s,m,'ashen','tax',{policy:'high'}),...patch}).ok,false);
});
test('seat claims reject a second claimant and one UID can move but cannot hold two seats',()=>{
 const m=setupMeta({hostUid:'u0'},1000);claimSeat(m,'u0','A','ashen');assert.throws(()=>claimSeat(m,'u1','B','ashen'),/claimed/);
 claimSeat(m,'u0','A','wintermere');assert.equal(m.seats.ashen.uid,null);assert.equal(seatFor(m,'u0'),'wintermere');
 startCampaign(m,'u0',1000);assert.throws(()=>claimSeat(m,'u1','B','ashen'),/locked/);assert.throws(()=>startCampaign(m,'u0',1000));
});
test('all Ready and timers resolve once; absence is reserved, AI substitutes act at the boundary, reconnect returns control',()=>{
 const {state:s,meta:m}=setup();const p={u0:{at:1000},u1:{at:1000}};
 m.ready.ashen=true;assert.equal(resolutionDue(m,p,1000,s),false);m.ready.wintermere=true;assert.equal(resolutionDue(m,p,1000,s),true);
 m.phase='resolving';resolveRound(s,m,p,1000);assert.equal(resolutionDue(m,p,1000,s),false);
 m.options.absent='ai';m.deadline=1100;assert.equal(resolutionDue(m,p,1100,s),true);
 m.phase='resolving';resolveRound(s,m,{u0:{at:200000},u1:{at:1000}},200000);
 assert.equal(s.controllers.wintermere.uid,'u1');assert.equal(s.controllers.wintermere.substitute,true);assert.ok(s.strategy.history.at(-1).houses.some(h=>h.owner==='wintermere'));
 assert.equal(applyCommand(s,m,command(s,m,'wintermere','tax',{policy:'low'})).ok,false);
 m.phase='resolving';resolveRound(s,m,{u0:{at:200000},u1:{at:200000}},200000);assert.equal(s.controllers.wintermere.substitute,false);
 assert.equal(applyCommand(s,m,command(s,m,'wintermere','tax',{policy:'low'})).ok,true);
});
test('host takeover waits for the grace period and permanent surrender releases the UID',()=>{
 const {state:s,meta:m}=setup();assert.throws(()=>requestTakeover(m,'u0','wintermere',{u1:{at:1000}},1001));
 m.planningAt=100000; // New rounds must not restart an absent ruler's grace.
 requestTakeover(m,'u0','wintermere',{u1:{at:1000}},1001+GRACE_MS,true);applyBoundaryTakeovers(m);assert.equal(m.seats.wintermere.kind,'ai');assert.equal(seatFor(m,'u1'),undefined);
 m.phase='resolving';resolveRound(s,m,{u0:{at:1001+GRACE_MS}},1001+GRACE_MS);assert.ok(aiControlledHouseIds(s).includes('wintermere'));
});
test('human chat cannot ratify a treaty; exact structured terms require the receiver and are charged once',()=>{
 const {state:s,meta:m}=setup();const a='ashen',b='wintermere';
 assert.equal(applyCommand(s,m,command(s,m,a,'chat',{targetHouseId:b,message:'We are allies now.'})).ok,true);assert.equal(treaty(s,a,b,'alliance'),undefined);
 assert.equal(commitDeal(s,b,terms('ALLIANCE'),a).ok,false);
 const c=command(s,m,a,'humanProposal',{targetHouseId:b,intent:terms('ALLIANCE',{giveAmount:20})});assert.equal(applyCommand(s,m,c).ok,true);
 assert.equal(applyCommand(s,m,command(s,m,a,'respondProposal',{id:c.id,decision:'accept'})).ok,false);
 const gold=kingdom(s,a).resources.gold;const accept=command(s,m,b,'respondProposal',{id:c.id,decision:'accept'});
 assert.equal(applyCommand(s,m,accept).ok,true);assert.ok(treaty(s,a,b,'alliance'));assert.equal(kingdom(s,a).resources.gold,gold-20);
 assert.equal(applyCommand(s,m,{...accept,id:'another',sequence:++serial}).ok,false);
 assert.equal(applyCommand(s,m,command(s,m,b,'ratify',{targetHouseId:a,intent:terms('WAR')})).ok,true);assert.ok(atWar(s,a,b));
});
test('a non-Ashen ruler can ally with an AI and its accepted defense pledge drives actual AI orders',()=>{
 const {state:s}=setup();const actor='wintermere',ai='thornwall';kingdom(s,actor).resources.gold=1000;
 assert.equal(commitDeal(s,ai,terms('ALLIANCE',{giveAmount:250}),actor).ok,true);
 const capital=settlements(s,actor)[0];s.armies.find(a=>a.owner===ai).tile=capital.id;
 assert.equal(commitDeal(s,ai,terms('DEFEND',{targetId:capital.id,giveAmount:150,duration:4}),actor).ok,true);
 endTurn(s);endTurn(s);const pledge=s.pledges.find(p=>p.debtor===ai&&p.creditor===actor);assert.equal(pledge.status,'fulfilled');assert.equal(kingdom(s,ai).reputation.kept,1);
});
test('dispatch allowances, promises, Gemini context, and private reports belong to the acting ruler',async()=>{
 const {state:s,meta:m}=setup(3);
 for(let i=0;i<3;i++)assert.equal(consumeMessage(s,'sunspire','wintermere').ok,true);
 assert.equal(consumeMessage(s,'sunspire','wintermere').ok,false);assert.equal(consumeMessage(s,'sunspire','ashen').ok,true);
 appendConversation(s,'sunspire','player','WINTERMERE-SECRET',{actorHouseId:'wintermere'});
 acceptRulerMemories(s,'sunspire',{relationshipSummary:'SECRET interpretation',memoryCandidates:[]},'wintermere');
 s.intelligence.reports.push({owner:'wintermere',text:'SECRET SPY REPORT'});
 const context=makeContext(s,'sunspire','hello',{actorHouseId:'wintermere'});assert.equal(context.actorHouseId,'wintermere');assert.equal(context.targetHouseId,'sunspire');assert.ok(context.history.some(m=>m.text==='WINTERMERE-SECRET'));
 const split=splitCampaign(s);assert.equal(JSON.stringify(split.world).includes('SECRET'),false);
 const view=playerView(split.world,split.privateByHouse.ashen,'ashen');assert.equal(JSON.stringify(view).includes('SECRET'),false);
 assert.equal(makeContext(view,'sunspire','hello',{actorHouseId:'ashen'}).world.conversationInterpretation,'');
 assert.match(makeContext(playerView(split.world,split.privateByHouse.wintermere,'wintermere'),'sunspire','hello',{actorHouseId:'wintermere'}).world.conversationInterpretation,/SECRET interpretation/);
 assert.ok(JSON.stringify(playerView(split.world,split.privateByHouse.wintermere,'wintermere')).includes('SECRET SPY REPORT'));
 const restored=joinCampaign(split.canonical,split.privateByHouse);assert.deepEqual(restored.intelligence.reports,s.intelligence.reports);assert.equal(court(restored,'wintermere').conversations.sunspire.at(-1).text,'WINTERMERE-SECRET');
 const packed=await packCampaign(s,m);assert.deepEqual(await decodePayload(packed.canonical.payload),split.canonical);assert.ok(packed.world.payload.data.length<750000);assert.throws(()=>guardDocument({large:'x'.repeat(760000)}));
});
test('coalition victory crowns the ruler and includes allied humans and AI; old schema-3 saves still load',()=>{
 const {state:s}=setup(2);for(const id of ['wintermere','thornwall','sunspire'])s.treaties.push({id:`a-${id}`,type:'alliance',parties:['ashen',id],expires:50});
 endTurn(s);endTurn(s);endTurn(s);assert.equal(s.outcome.winnerHouseId,'ashen');assert.ok(s.outcome.coalition.includes('wintermere'));assert.ok(s.outcome.coalition.includes('thornwall'));
 assert.equal(parseSave(JSON.stringify(createGame())).version,3);
});
test('controller token and epoch protect different tabs sharing a UID',()=>{
 const m={lease:{uid:'same',token:'tab1',expiresAt:10000}};
 assert.equal(ownsLease(m,'same','tab1',9999),true);assert.equal(ownsLease(m,'same','tab2',9999),false);assert.equal(ownsLease(m,'same','tab1',10000),false);
});
