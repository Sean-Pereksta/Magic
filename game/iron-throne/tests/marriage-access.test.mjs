import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { onlineGame } from './fixtures/online-game.mjs';
import { kingdom, relation, parseSave, treaty, declareWar } from '../core.mjs';
import { applySpeech } from '../living.mjs';
import { court } from '../house-control.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { validateIntent, evaluateDeal, commitDeal, deliverPledge, scriptedReply, relationshipResponse, describeIntent } from '../diplomacy.mjs';
import { continueMarriageReview, discussMarriage, isMarriageTopic, marriageBetween, marriageProposal, availableFamily, resolveMarriages } from '../marriage.mjs';
import { splitCampaign, joinCampaign } from '../multiplayer-state.mjs';

const host='wintermere',actor='ashen';
function dependable(turns=[2,4,6,8,10]){
 const s=createGame();kingdom(s,actor).resources.gold=3000;
 for(const turn of turns){
  s.turn=turn;assert.equal(commitDeal(s,host,validateIntent({type:'PROMISE',giveAmount:60,duration:2})).ok,true);
  assert.equal(deliverPledge(s,s.pledges.at(-1).id).ok,true);
 }
 const ally=validateIntent({type:'ALLIANCE',giveAmount:100,duration:15});
 const v=evaluateDeal(s,host,ally);assert.equal(commitDeal(s,host,v.counter||ally).ok,true);
 return s;
}
const terms=extra=>validateIntent({type:'MARRIAGE',duration:12,giveAmount:100,actorMember:'son',rulerMember:'daughter',...extra});
let sequence=0;
const cmd=(s,m,a,type,args)=>({id:`marriage-access-${++sequence}`,clientId:'marriage-access-tests',sequence,uid:m.seats[a].uid,actorHouseId:a,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,type,args});

test('a direct Treaty Desk match starts review, waits a turn and ratifies selected available adults',()=>{
 const s=dependable(),i=terms({trade:true,shipmentResource:'iron',shipmentAmount:2,shipmentTurns:3});
 const before=kingdom(s,actor).resources.gold;
 continueMarriageReview(s,host,actor,i);
 assert.equal(s.royalBonds.negotiations['ashen:wintermere'].actorMember,'son');
 const first=evaluateDeal(s,host,i);assert.equal(first.status,'reject');assert.match(first.reason,/later turn/);
 assert.equal(kingdom(s,actor).resources.gold,before);assert.equal(s.royalBonds.marriages.length,0);
 s.turn++;
 const verdict=evaluateDeal(s,host,i),accepted=verdict.counter||i;
 assert.equal(commitDeal(s,host,accepted).ok,true);
 const m=marriageBetween(s,actor,host);assert.equal(m.members[0].role,'son');assert.equal(m.members[1].role,'daughter');
 assert.equal(availableFamily(s,actor).includes('son'),false);
 assert.equal(kingdom(s,actor).resources.gold,before-accepted.giveAmount);
 assert.ok(treaty(s,actor,host,'non-aggression'));assert.ok(treaty(s,actor,host,'trade'));
 const iron=kingdom(s,actor).resources.iron;s.turn++;resolveMarriages(s);resolveMarriages(s);
 assert.equal(kingdom(s,actor).resources.iron,iron-2);
 const saved=JSON.stringify(s);assert.equal(commitDeal(s,host,accepted).ok,false);assert.equal(JSON.stringify(s),saved);
 assert.equal(parseSave(saved).royalBonds.marriages[0].status,'active');
});

test('a strategic marriage is achievable through real promises and partnership, not only an eight-turn personal gate',()=>{
 const s=dependable([2,4,5]),i=terms();
 assert.ok(s.turn-relation(s,host,actor).personal.started<8);
 continueMarriageReview(s,host,actor,i);s.turn++;
 const v=evaluateDeal(s,host,i);assert.notEqual(v.status,'reject',v.reason);
 assert.equal(commitDeal(s,host,v.counter||i).ok,true);
});

test('get married, marry me, wed and adult-relative language select the actual proposed people',()=>{
 for(const message of ['Could we get married?','Will you marry me?','Would you wed me?']){
  const s=dependable();assert.equal(isMarriageTopic(message),true);applySpeech(s,host,message);
  const n=s.royalBonds.negotiations['ashen:wintermere'];assert.equal(n.rulerMember,'ruler',message);assert.equal(n.actorMember,'ruler');
  assert.match(scriptedReply(s,host,message).reply,/later turn/);
 }
 const s=dependable();applySpeech(s,host,'Could my adult son marry your adult daughter?');
 const n=s.royalBonds.negotiations['ashen:wintermere'];assert.equal(n.actorMember,'son');assert.equal(n.rulerMember,'daughter');
});

test('changing the proposed adults requires a new AI review, not silently marrying a substitute',()=>{
 const s=dependable(),a=terms(),b=terms({actorMember:'ruler',rulerMember:'ruler'});
 continueMarriageReview(s,host,actor,a);s.turn++;
 assert.equal(commitDeal(s,host,b).ok,false);
 continueMarriageReview(s,host,actor,b);
 assert.equal(evaluateDeal(s,host,b).status,'reject');s.turn++;
 const v=evaluateDeal(s,host,b);assert.equal(commitDeal(s,host,v.counter||b).ok,true);
 assert.equal(marriageBetween(s,actor,host).members[0].role,'ruler');
});

test('model wording cannot invent a wedding, deny a legal match or substitute an unrelated gift',()=>{
 const s=dependable(),i=terms();continueMarriageReview(s,host,actor,i);
 const model={reply:'We are married now and you have paid me 999 gold.',tone:'warm',intents:[validateIntent({type:'AID',giveAmount:999})],relationshipSummary:'Already married.',memoryCandidates:['A wedding happened.']};
 const early=relationshipResponse(s,host,describeIntent(i),model,{proposal:i});
 assert.match(early.reply,/later turn/);assert.deepEqual(early.intents,[]);assert.equal(early.memoryCandidates,undefined);assert.equal(s.royalBonds.marriages.length,0);
 s.turn++;
 const ready=relationshipResponse(s,host,describeIntent(i),{...model,reply:'Marriage is not possible in this realm.'},{proposal:i});
 assert.doesNotMatch(ready.reply,/not possible|married now|999/);assert.ok(ready.intents.some(i=>i.type==='MARRIAGE'));
 assert.equal(ready.intents.some(i=>i.type==='AID'),false);assert.equal(s.royalBonds.marriages.length,0);
});

test('an explicit human proposal can be accepted the same turn, but never by sender or chat alone',()=>{
 const {state:s,meta:m}=onlineGame(),from='wintermere',to='ashen';
 const i=terms({actorMember:'ruler',rulerMember:'son',giveAmount:20});
 const start=kingdom(s,from).resources.gold;
 const p=cmd(s,m,from,'humanProposal',{targetHouseId:to,intent:i});
 assert.equal(applyCommand(s,m,p).ok,true);assert.equal(s.royalBonds.marriages.length,0);
 assert.equal(applyCommand(s,m,cmd(s,m,from,'respondProposal',{id:p.id,decision:'accept'})).ok,false);
 assert.equal(commitDeal(s,to,i,from).ok,false);assert.equal(kingdom(s,from).resources.gold,start);
 assert.equal(applyCommand(s,m,cmd(s,m,to,'respondProposal',{id:p.id,decision:'accept'})).ok,true);
 assert.equal(marriageBetween(s,from,to).status,'active');assert.equal(kingdom(s,from).resources.gold,start-20);
 const parts=splitCampaign(s),joined=joinCampaign(parts.canonical,parts.privateByHouse);
 assert.equal(parseSave(JSON.stringify(joined)).royalBonds.marriages[0].members[1].role,'son');
});

test('human consent does not bypass available spouses, legal peace, or affordability',()=>{
 for(const block of ['war','funds']){
  const {state:s,meta:m}=onlineGame(),from='wintermere',to='ashen',i=terms({giveAmount:20});
  const p=cmd(s,m,from,'humanProposal',{targetHouseId:to,intent:i});assert.equal(applyCommand(s,m,p).ok,true);
  if(block==='war')declareWar(s,from,to);else kingdom(s,from).resources.gold=0;
  const before=kingdom(s,from).resources.gold;
  assert.equal(applyCommand(s,m,cmd(s,m,to,'respondProposal',{id:p.id,decision:'accept'})).ok,false);
  assert.equal(kingdom(s,from).resources.gold,before);assert.equal(s.royalBonds.marriages.length,0);
 }
});

test('online AI formal review uses the exact selected people and a truthful canonical response',()=>{
 const {state:s,meta:m}=onlineGame(),from='wintermere',to='sunspire',i=terms({actorMember:'daughter',rulerMember:'ruler'});
 const r=applyCommand(s,m,cmd(s,m,from,'chat',{targetHouseId:to,message:describeIntent(i),proposal:i,response:{reply:'You are already wed to my son.',tone:'warm',intents:[]}}));
 assert.equal(r.ok,true);
 const n=s.royalBonds.negotiations['sunspire:wintermere'];assert.equal(n.actorMember,'daughter');assert.equal(n.rulerMember,'ruler');
 assert.match(court(s,from).conversations[to].at(-1).text,/later turn/);
 assert.equal(s.royalBonds.marriages.length,0);
});

test('a refused marriage can be withdrawn in natural wording without buying trust',()=>{
 const s=createGame();continueMarriageReview(s,host,actor,terms({giveAmount:1000}));s.turn++;
 assert.equal(evaluateDeal(s,host,terms({giveAmount:1000})).status,'reject');
 discussMarriage(s,host,"I don't want to get married.");assert.equal(s.royalBonds.negotiations['ashen:wintermere'],undefined);
 assert.equal(s.royalBonds.marriages.length,0);
});
