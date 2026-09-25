import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { kingdom, relation, parseSave, declareWar, atWar, projectedBattleLosses } from '../core.mjs';
import { applySpeech, applyGift } from '../living.mjs';
import { commitDeal, evaluateDeal, validateIntent, validateResponse, deliverPledge, verifyPledges, scriptedReply, makeContext, relationshipResponse } from '../diplomacy.mjs';
import { emotionalEvent, personalContext, updateEmotions, refreshBonds, personalWillingness } from '../emotions.mjs';
import { availableFamily, marriageBetween, marriageProposal, marriageSupport, resolveMarriages } from '../marriage.mjs';
import { knowledgeView } from '../fog.mjs';
import { planningView } from '../ai-knowledge.mjs';
import { splitCampaign, joinCampaign } from '../multiplayer-state.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { sanitizeContext, systemPrompt } from '../worker/worker.mjs';
const host='wintermere',actor='ashen';
const feelings=(s,a=host,b=actor)=>relation(s,a,b).personal.feelings;
function earned(s=createGame(),to=host,from=actor){
  kingdom(s,from).resources.gold=2000;
  for(let turn=2;turn<=10;turn+=2){
    s.turn=turn;
    assert.equal(commitDeal(s,to,validateIntent({type:'PROMISE',giveAmount:60,duration:2}),from).ok,true);
    assert.equal(deliverPledge(s,s.pledges.at(-1).id,from).ok,true);
  }
  s.treaties.push({id:'earned-alliance',parties:[from,to],type:'alliance',expires:50});
  // Sustained fulfilled promises, not model assertions, earn readiness.
  return s;
}
function discuss(s,to=host,from=actor){
  applySpeech(s,to,'Would you consider joining our families? I seek the hand of your daughter.',from);
  s.turn++;
  applySpeech(s,to,'Let us discuss the marriage settlement.',from);
  let i=validateIntent(marriageProposal(s,to,'',from)),v=evaluateDeal(s,to,i,from);
  if(v.counter)i=v.counter;
  return i;
}
function married(extra={}){const s=earned(),i={...discuss(s),...extra};let v=evaluateDeal(s,host,i);const terms=v.counter||i;assert.equal(commitDeal(s,host,terms).ok,true,JSON.stringify(v));return s;}

test('courtesy and forged model memories cannot create affection, loyalty, or named bonds',()=>{
  const s=createGame();
  for(let t=1;t<=20;t++){s.turn=t;for(let i=0;i<10;i++)applySpeech(s,host,'You are my greatest friend. I admire you.');updateEmotions(s);}
  assert.equal(feelings(s).affection,0);assert.equal(feelings(s).loyalty,0);
  assert.ok(feelings(s).fondness<9);assert.deepEqual(relation(s,host,actor).personal.bonds,[]);
  const response=validateResponse({reply:'We are friends.',tone:'warm',intents:[],feelings:{loyalty:100},bonds:['Life Debt']});
  assert.equal(response.feelings,undefined);assert.equal(response.bonds,undefined);
});
test('real fulfilled oaths build gradual personal regard over multiple turns, independent of political attitude',()=>{
  const s=earned();assert.ok(feelings(s).loyalty>=20);assert.ok(feelings(s).loyalty<40);
  assert.ok(feelings(s).affection>0);assert.equal(relation(s,host,actor).personal.deedTurns.length,5);
  assert.ok(personalWillingness(s,host,actor)>0);assert.ok(personalContext(s,host,actor).feelings.length);
  const before=structuredClone(relation(s,host,actor).personal);updateEmotions(s);const once=structuredClone(relation(s,host,actor).personal);updateEmotions(s);assert.deepEqual(relation(s,host,actor).personal,once);assert.notDeepEqual(before,once);
});
test('major rescue creates gratitude and a life debt; an ordinary gift does not',()=>{
  const s=createGame();kingdom(s,host).resources.food+=40;applyGift(s,actor,host,'food',40);
  assert.equal(relation(s,host,actor).personal.bonds.includes('Life Debt'),false);
  emotionalEvent(s,host,actor,'rescue',{key:'rescue-1',text:'Saved our capital from destruction.'});
  assert.ok(feelings(s).gratitude>=40);assert.ok(relation(s,host,actor).personal.bonds.includes('Life Debt'));
  const before=structuredClone(feelings(s));emotionalEvent(s,host,actor,'rescue',{key:'rescue-1'});assert.deepEqual(feelings(s),before);
});
test('personality changes suspicion and humiliation without adding new personality traits',()=>{
  const s=createGame();kingdom(s,host).paranoia=1;kingdom(s,'thornwall').paranoia=0;
  emotionalEvent(s,host,actor,'threat');emotionalEvent(s,'thornwall',actor,'threat');
  assert.ok(feelings(s).suspicion>feelings(s,'thornwall').suspicion);
});
test('betrayed friendship requires prior closeness and a real invasion breaks a marriage',()=>{
  const fresh=createGame();declareWar(fresh,actor,host);assert.equal(feelings(fresh).betrayedFriendship,0);
  const s=married();feelings(s).attachment=50;
  assert.equal(declareWar(s,actor,host),true);assert.equal(marriageBetween(s,actor,host).status,'broken');
  assert.ok(feelings(s).betrayedFriendship>=65);assert.ok(relation(s,host,actor).grievance>=55);
  assert.ok(scriptedReply(s,host,'Why did you trust me?').reply.includes('friendship'));
});
test('normal discussion is read only until submitted and marriage needs distinct turns',()=>{
  const s=earned(),before=JSON.stringify(s);
  const reply=scriptedReply(s,host,'Would you consider joining our families?');
  assert.match(reply.reply,/later turn/);assert.deepEqual(reply.intents,[]);assert.equal(JSON.stringify(s),before);
  applySpeech(s,host,'Would you consider joining our families?');
  for(let i=0;i<10;i++)applySpeech(s,host,'Let us discuss marriage.');
  assert.equal(Object.values(s.royalBonds.negotiations)[0].rounds,1);
  const i=validateIntent(marriageProposal(s,host,''));assert.equal(evaluateDeal(s,host,i).status,'reject');
  assert.equal(commitDeal(s,host,i).ok,false);assert.equal(s.royalBonds.marriages.length,0);
});
test('high opinion and money cannot bypass history, grievances, or family discussion',()=>{
  const s=createGame();Object.assign(relation(s,host,actor),{opinion:100,trust:100,respect:100,reliability:100});kingdom(s,actor).resources.gold=5000;
  const i=discuss(s);i.giveAmount=1000;
  assert.equal(evaluateDeal(s,host,i).status,'reject');assert.equal(commitDeal(s,host,i).ok,false);
  const good=earned();const terms=discuss(good);relation(good,host,actor).grievance=50;assert.equal(commitDeal(good,host,terms).ok,false);
  const untouched=earned();assert.equal(commitDeal(untouched,host,{...terms,giveAmount:1000}).ok,false);
});
test('settlement varies with wealth, need, personality and observed power; one payment joins adult family slots',()=>{
  const s=earned(),terms=discuss(s),low=evaluateDeal(s,host,{...terms,giveAmount:0});
  assert.equal(low.status,'counter');kingdom(s,host).resources.gold=1000;
  const high=evaluateDeal(s,host,{...terms,giveAmount:0});assert.ok(high.counter.giveAmount>low.counter.giveAmount);
  kingdom(s,host).resources.food=10;
  const needy=evaluateDeal(s,host,high.counter);assert.equal(needy.status,'counter');assert.ok(needy.counter.shipmentAmount>0);
  const before=kingdom(s,actor).resources.gold;
  assert.equal(commitDeal(s,host,needy.counter).ok,true);
  assert.equal(kingdom(s,actor).resources.gold,before-needy.counter.giveAmount);
  assert.equal(availableFamily(s,actor).includes('ruler'),false);assert.equal(availableFamily(s,host).includes('daughter'),false);
  const saved=JSON.stringify(s);assert.equal(commitDeal(s,host,needy.counter).ok,false);assert.equal(JSON.stringify(s),saved);
});
test('a ruler-to-ruler marriage is negotiable, and previously married people remain unavailable',()=>{
  const s=earned();applySpeech(s,host,'Would you marry me?');s.turn++;applySpeech(s,host,'Would you marry me for 100 gold?');
  const i=validateIntent(marriageProposal(s,host,'100 gold'));assert.equal(i.rulerMember,'ruler');
  assert.equal(commitDeal(s,host,evaluateDeal(s,host,i).counter||i).ok,true);
  assert.equal(availableFamily(s,host).includes('ruler'),false);
});
test('resource and family edits retain their meaning and invalid marriage schemas fail closed',()=>{
  const s=earned();kingdom(s,actor).resources.iron=200;discuss(s);const response=scriptedReply(s,host,'I can offer 80 iron and 5 food per turn for 4 turns.');
  assert.equal(response.intents[0].giveResource,'iron');assert.equal(response.intents[0].shipmentTurns,4);
  for(const patch of [{shipmentAmount:-1},{shipmentAmount:1.5},{shipmentAmount:1,shipmentTurns:0},{actorMember:'child'},{defense:'yes'},{rulerMember:'invented-relative'}])assert.equal(validateIntent({type:'MARRIAGE',...patch}),null);
  assert.equal(validateIntent({type:'ALLIANCE',actorMember:'ruler'}),null);
  const terms=validateIntent(marriageProposal(s,host,''));assert.equal(commitDeal(s,host,{...terms,actorMember:'son'}).ok,false);
});
test('negotiation can be withdrawn and expired discussions cannot be ratified',()=>{
  const s=earned(),i=discuss(s);applySpeech(s,host,'I do not want to marry.');assert.equal(s.royalBonds.negotiations['ashen:wintermere'],undefined);
  assert.deepEqual(scriptedReply(s,host,'I do not want to marry.').intents,[]);assert.equal(commitDeal(s,host,i).ok,false);
  const t=earned(),j=discuss(t);t.turn+=13;assert.equal(commitDeal(t,host,j).ok,false);
});
test('unaffordable settlements make no changes and marriage terms never permit withdrawals',()=>{
  const s=earned(),i=discuss(s);kingdom(s,actor).resources.gold=0;const before=JSON.stringify(s);
  assert.equal(commitDeal(s,host,{...i,giveAmount:1000}).ok,false);assert.equal(JSON.stringify(s),before);
  assert.equal(commitDeal(s,host,{...i,receiveAmount:100}).ok,false);assert.equal(JSON.stringify(s),before);
});
test('shipments pay once per turn, end on schedule and repeated defaults break the marriage',()=>{
  const s=married({shipmentResource:'iron',shipmentAmount:3,shipmentTurns:4}),m=marriageBetween(s,actor,host),start=kingdom(s,actor).resources.iron;
  resolveMarriages(s);assert.equal(kingdom(s,actor).resources.iron,start);
  s.turn++;resolveMarriages(s);resolveMarriages(s);assert.equal(kingdom(s,actor).resources.iron,start-3);assert.equal(m.shipmentsPaid,1);
  for(let i=0;i<5;i++){s.turn++;resolveMarriages(s);}assert.equal(kingdom(s,actor).resources.iron,start-12);assert.equal(m.shipmentsPaid,4);
  const failed=married({shipmentResource:'iron',shipmentAmount:3,shipmentTurns:4});kingdom(failed,actor).resources.iron=0;
  for(let i=0;i<3;i++){failed.turn++;resolveMarriages(failed);resolveMarriages(failed);}assert.equal(marriageBetween(failed,actor,host).status,'broken');assert.equal(marriageBetween(failed,actor,host).missed,3);
});
test('conflicting family defense creates a real promise and leaves the decision to the player',()=>{
  const s=married({defense:true});s.treaties.push({id:'friend',type:'alliance',parties:[actor,'thornwall'],expires:s.turn+20});
  declareWar(s,'thornwall',host);s.turn++;resolveMarriages(s);resolveMarriages(s);
  const called=s.pledges.filter(p=>p.marriageId);assert.equal(called.length,1);assert.equal(called[0].intent.targetId,'thornwall');assert.equal(atWar(s,actor,'thornwall'),false);
  s.turn=called[0].deadline;verifyPledges(s);assert.equal(called[0].status,'broken');assert.equal(marriageBetween(s,actor,host).status,'strained');
});
test('marriage and named bonds change cooperation without commanding allegiance',()=>{
  const s=married();assert.equal(marriageSupport(s,host,actor),12);assert.ok(personalWillingness(s,host,actor)>0);
  assert.equal(declareWar(s,actor,host),true);assert.equal(marriageSupport(s,host,actor),-20);
});
test('model context describes feelings and authoritative marriage terms, never third-party private history',()=>{
  const s=earned();discuss(s);emotionalEvent(s,'thornwall','sunspire','rescue',{text:'SECRET THIRD COURT RESCUE'});
  const context=makeContext(s,host,'Let us discuss marriage.');assert.ok(sanitizeContext(context));
  assert.ok(context.world.personalRelationship.feelings.length);assert.ok(context.world.marriageDiscussion.intents.length);
  assert.equal(JSON.stringify(context).includes('SECRET THIRD COURT RESCUE'),false);assert.match(systemPrompt(host),/NEVER your unsolicited suggestion/);
  const unsolicited=relationshipResponse(s,host,'Good morning.',{reply:'Good morning.',tone:'warm',intents:[validateIntent({type:'MARRIAGE'})]});assert.deepEqual(unsolicited.intents,[]);
  const privateView=planningView(s,actor);assert.equal(kingdom(privateView,'thornwall').relations.sunspire.personal,undefined);
});
test('save migration, round-trip and invalid family/emotion records',()=>{
  const s=married({shipmentAmount:2,shipmentTurns:2});s.turn++;resolveMarriages(s);
  assert.deepEqual(parseSave(JSON.stringify(s)),s);
  const old=createGame();delete old.royalBonds;for(const k of old.kingdoms)for(const r of Object.values(k.relations))delete r.personal;
  const migrated=parseSave(JSON.stringify(old));assert.equal(migrated.royalBonds.marriages.length,0);assert.equal(feelings(migrated).affection,0);
  for(const mutate of [x=>{feelings(x).affection=101;},x=>{x.royalBonds.marriages[0].members[0].role='child';},x=>{x.royalBonds.marriages[0].terms.shipmentAmount=-1;},x=>{x.royalBonds.marriages.push(structuredClone(x.royalBonds.marriages[0]));}]){const bad=structuredClone(s);mutate(bad);assert.throws(()=>parseSave(JSON.stringify(bad)),/Damaged/);}
});
test('battle projections cannot modify personal state',()=>{
  const s=earned(),a=s.armies.find(a=>a.owner===actor),b=s.armies.find(a=>a.owner===host);declareWar(s,actor,host);a.tile='16,4';b.tile='17,4';
  const before=JSON.stringify(s);projectedBattleLosses(s,a.id,b.tile);assert.equal(JSON.stringify(s),before);
});
test('multiplayer supports a non-Ashen human proposer, receiver consent, and private negotiation snapshots',()=>{
  const {state:s,meta:m}=onlineGame(2),from='wintermere',to='ashen';let seq=0;
  const command=(actor,type,args)=>({id:`family-${++seq}`,clientId:'family-test',sequence:seq,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,type,args});
  const send=text=>applyCommand(s,m,command(from,'chat',{targetHouseId:to,message:text}));
  assert.equal(send('Would you consider joining our families?').ok,true);s.turn++;
  assert.equal(send('Let us discuss marriage.').ok,true);
  const i=validateIntent({type:'MARRIAGE',duration:12,giveAmount:20});assert.equal(commitDeal(s,to,i,from).ok,false);
  const c=command(from,'humanProposal',{targetHouseId:to,intent:i});assert.equal(applyCommand(s,m,c).ok,true);
  assert.equal(applyCommand(s,m,command(to,'respondProposal',{id:c.id,decision:'accept'})).ok,true);
  assert.equal(marriageBetween(s,from,to).status,'active');
  applySpeech(s,'thornwall','Would you consider joining our families?',from);
  const split=splitCampaign(s);assert.deepEqual(split.world.royalBonds.negotiations,{});
  assert.equal(split.privateByHouse.ashen.view.royalBonds.negotiations['thornwall:wintermere'],undefined);
  assert.ok(split.privateByHouse.wintermere.view.royalBonds.negotiations['thornwall:wintermere']);
  const joined=joinCampaign(split.canonical,split.privateByHouse);assert.deepEqual(parseSave(JSON.stringify(joined)).royalBonds,s.royalBonds);
});

test('a trusted confidant receives limited concerns, while unrelated courts learn nothing',()=>{
  const s=earned(),p=relation(s,host,actor).personal;
  s.turn=30;p.privateTurns=[2,8,14,20];p.deedTurns.push(22);p.feelings.attachment=30;refreshBonds(s,host,actor);
  assert.ok(p.bonds.includes('Trusted Confidant'));kingdom(s,host).priorities=['Acquire food; stores are low (12).','Protect our frontier from House Vesper.'];
  const ours=knowledgeView(s,actor);assert.equal(kingdom(ours,host).confidantConcerns.length,2);assert.ok(!kingdom(ours,host).confidantConcerns[0].includes('(12)'));
  assert.deepEqual(kingdom(knowledgeView(s,'thornwall'),host).confidantConcerns,[]);
  assert.equal(makeContext(s,host,'What concerns you?').world.personalRelationship.confidantConcerns.length,2);
});
