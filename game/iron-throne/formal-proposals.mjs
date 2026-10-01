import { alive, armiesOf, atWar, kingdom, relation, treaty } from './core.mjs';
import { knowledgeView } from './fog.mjs';
import { evaluateDeal, commitDeal, validateIntent, describeIntent } from './diplomacy.mjs';
import { isPlayerPromise, detectPromise } from './promises.mjs';
import { isAiHouse, court } from './house-control.mjs';
import { appendConversation, borderThreat, diplomaticCapacity } from './living.mjs';
import { councilActive, appendCouncil } from './council-state.mjs';
import { operationFor, operationMember, memberOperation } from './cooperation-state.mjs';
import { respondOperation } from './operations.mjs';
import { economicNeeds } from './trade.mjs';
import { packageTrade, tradeItems, withItems } from './trade-package.mjs';
import { consumeDiplomaticMessage, privateConversation } from './proposal-followup.mjs';

const fail=error=>({ok:false,error});
const MILITARY=new Set(['JOINT_WAR','DEFEND','POSITION','BUILD_DEFENSES','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND','PLEDGE_BUILD','GUARANTEE']);
const OFFENSIVE=new Set(['JOINT_WAR','PLEDGE_WAR','PLEDGE_ATTACK']);
const validHouse=(s,id)=>typeof id==='string'&&s.kingdoms.some(k=>k.id===id);
const record=(s,id)=>s.cooperation?.formalProposals?.find(p=>p.id===id);
const targetOf=(s,p,house)=>{const view=knowledgeView(s,house);return validHouse(s,p.intent?.targetId)?p.intent.targetId:view.tiles[p.intent?.targetId]?.owner||view.tiles[p.intent?.targetId]?.knownCapital||null;};
const RECEIVER_ACTIONS=new Set(['DEFEND','POSITION','BUILD_DEFENSES','WITHDRAW','EMBARGO']);
export const defaultProposalDirection=i=>RECEIVER_ACTIONS.has(i?.type)||i?.type==='JOINT_WAR'?'request':'offer';
const requestedSender=i=>isPlayerPromise(i)||['AID','EXCHANGE','RECURRING','LOAN'].includes(i?.type);
const parties=(p,house)=>(p.direction==='request'&&requestedSender(p.intent)||p.direction==='offer'&&RECEIVER_ACTIONS.has(p.intent.type))?{actor:house,ruler:p.proposer}:{actor:p.proposer,ruler:house};
const reversePackage=i=>withItems(i,tradeItems(i,'receive'),tradeItems(i,'give'));
const audience=(s,actor,councilId,requested)=>{
  const c=councilId&&s.allianceCouncils?.find(c=>c.id===councilId);
  if(councilId&&(!c||!councilActive(s,c)||!c.participants.includes(actor)))return null;
  if(!Array.isArray(requested)||!requested.length||requested.length>11||new Set(requested).size!==requested.length||requested.some(id=>id===actor||!validHouse(s,id)||!alive(s,id)||c&&!c.participants.includes(id)))return null;
  if(!c&&requested.length!==1)return null;
  return c?c.participants:[actor,...requested];
};
export function formalDescription(p){const requested=p.direction==='request'&&requestedSender(p.intent);return p.operationId?`Join ${p.operationId}`:`${requested?'Requested commitment: ':''}${requested?describeIntent(p.intent).replaceAll('Proposer','Requested House'):describeIntent(p.intent)}${p.targetTile?` · Exact hex ${p.targetTile}`:''}`;}
function initialize(s){s.cooperation??={operations:[],proposals:[],balance:[],lastDiplomacyTurn:0};s.cooperation.formalProposals??=[];}
function trim(s){const rows=s.cooperation.formalProposals;while(rows.length>40){const i=rows.findIndex(p=>['resolved','dismissed'].includes(p.status));if(i<0)break;rows.splice(i,1);}}
export function submitFormalProposal(s,actor,raw,{inferred=false}={}){
  if(s.outcome||s.phase==='founding'||!alive(s,actor)||!raw||typeof raw!=='object')return fail('Proposals require an active ruler.');
  const councilId=raw.councilId||null,members=audience(s,actor,councilId,raw.requestedHouses);if(!members)return fail('Choose current participants in this conversation.');
  const direction=raw.direction??'offer';if(!['offer','request'].includes(direction))return fail('Choose who is offering the commitment.');
  const operation=raw.operationId&&operationFor(s,raw.operationId),intent=operation?null:validateIntent(raw.intent);
  if(raw.operationId&&(!operation||operation.owner!==actor||operation.status!=='Preparing'||raw.requestedHouses.some(h=>operationMember(operation,h)?.status!=='invited')))return fail('Choose an operation with invitations for these Houses.');
  if(!operation&&!intent)return fail('Choose valid structured terms.');
  if(intent&&['WAR','BETRAY','VASSALAGE','MARRIAGE','INTELLIGENCE'].includes(intent.type)&&councilId)return fail('These terms belong in a private ruler conversation.');
  if(intent?.targetId&&!validHouse(s,intent.targetId)&&!Object.hasOwn(s.tiles,intent.targetId)&&!knowledgeView(s,actor).armies.some(a=>a.id===intent.targetId))return fail('Choose a valid known target or map coordinate.');
  initialize(s);if(s.cooperation.formalProposals.filter(p=>!['resolved','dismissed'].includes(p.status)).length>=24)return fail('Resolve or dismiss earlier proposals first.');
  const p={id:`FORMAL-${s.nextId++}`,proposer:actor,councilId,audience:[...members],requestedHouses:[...raw.requestedHouses],source:inferred?'conversation_inferred':'explicit',direction,intent,operationId:operation?.id||null,targetTile:operation?.targetTile||(intent&&Object.hasOwn(s.tiles,intent.targetId)?intent.targetId:null),created:s.turn,expires:s.turn+3,status:'draft',approved:false,responses:{},reactions:0};
  s.cooperation.formalProposals.push(p);trim(s);
  if(!inferred){const result=ratifyFormalProposal(s,actor,p.id);if(!result.ok){s.cooperation.formalProposals=s.cooperation.formalProposals.filter(x=>x!==p);return result;}}
  return {ok:true,proposalId:p.id};
}
export function ratifyFormalProposal(s,actor,id){
  const p=record(s,id);if(!p||p.proposer!==actor||p.status!=='draft'||p.approved||s.turn>p.expires||!audience(s,actor,p.councilId,p.requestedHouses))return fail('This draft is no longer available.');
  if(p.councilId&&p.requestedHouses.length===1){const spent=consumeDiplomaticMessage(s,p.requestedHouses[0],actor,p.intent,p.councilId);if(!spent.ok)return spent;}
  else if(p.councilId){const c=court(s,actor),used=c.messages.turn===s.turn?c.messages:{turn:s.turn,regular:0,hosts:{}};if(used.regular>=diplomaticCapacity(s,actor))return fail('Your shared dispatches are used for this turn.');c.messages=used;if(!s.controllers)s.diplomacy.messages=used;used.regular++;}
  else {const spent=consumeDiplomaticMessage(s,p.requestedHouses[0],actor,p.intent,privateConversation(p.requestedHouses[0]));if(!spent.ok)return spent;}
  p.approved=true;p.status='processing';p.sentTurn=s.turn;
  p.responses=Object.fromEntries(p.requestedHouses.map(h=>[h,{status:isAiHouse(s,h)?'waiting':'awaiting-human',reasonCodes:[],message:'',spoken:false}]));
  if(p.councilId)appendCouncil(s,s.allianceCouncils.find(c=>c.id===p.councilId),actor,`Formal proposal: ${formalDescription(p)}`,{formalProposalId:p.id});
  else appendConversation(s,p.requestedHouses[0],'player',`Formal proposal: ${formalDescription(p)}`,{actorHouseId:actor,kind:'formal-proposal',formalProposalId:p.id});
  return {ok:true,proposalId:p.id};
}
export function dismissFormalProposal(s,actor,id){const p=record(s,id);if(!p||p.proposer!==actor||p.status!=='draft')return fail('Only your unsubmitted draft can be dismissed.');p.status='dismissed';return {ok:true};}
function alternatives(s,house,p){
  const k=kingdom(s,house),r=relation(s,house,p.proposer);if(r.trust<20||r.grievance>35||atWar(s,house,p.proposer))return [];
  const needs=economicNeeds(s,house),items=['food','iron','gold'].map(resource=>{const reserve=needs.find(n=>n.resource===resource)?.reserve||40;return {resource,amount:Math.min(resource==='food'?60:25,Math.floor((k.resources[resource]-reserve-20)/2))};}).filter(x=>x.amount>=10).slice(0,2);
  const intents=items.length?[validateIntent({type:'AID',giveItems:items,giveResource:items[0].resource,giveAmount:items[0].amount})]:[];
  if(!treaty(s,house,p.proposer,'access')&&!treaty(s,house,p.proposer,'alliance')&&!treaty(s,house,p.proposer,'vassalage'))intents.push(validateIntent({type:'ACCESS',duration:p.intent?.duration||10}));
  return intents.filter(Boolean);
}
export function evaluateCouncilProposal(s,house,p){
  if(!p?.approved||!p.requestedHouses.includes(house))return {decision:'invalid',reasonCodes:['unapproved'],reason:'No approved request.'};
  if(s.outcome||s.turn>p.expires||!alive(s,house)||!audience(s,p.proposer,p.councilId,p.requestedHouses))return {decision:'invalid',reasonCodes:['circumstances_changed'],reason:'This proposal expired or its audience changed.'};
  if(p.operationId){const o=operationFor(s,p.operationId);if(!o||o.status!=='Preparing'||operationMember(o,house)?.status!=='invited')return {decision:'invalid',reasonCodes:['operation_changed'],reason:'This operation invitation changed.'};}
  const i=p.intent,r=relation(s,house,p.proposer),k=kingdom(s,house),target=i?targetOf(s,p,house):operationFor(s,p.operationId).target;
  let reason=null,code=null;
  const aiOwes=p.operationId||i&&(isPlayerPromise(i)?parties(p,house).actor===house:parties(p,house).ruler===house);
  if(aiOwes&&(p.operationId||MILITARY.has(i?.type))&&isAiHouse(s,house)){
    if(target&&(OFFENSIVE.has(i?.type)||p.operationId&&['attack','capture','siege'].includes(operationFor(s,p.operationId).objectiveType||'attack'))&&['alliance','vassalage','peace','non-aggression'].some(type=>treaty(s,house,target,type))){code='treaty_conflict';reason='I gave that House my word. I will not break our protective treaty.';}
    else if(target&&OFFENSIVE.has(i?.type)&&(treaty(s,house,target,'trade')||treaty(s,house,target,'recurring'))&&((relation(s,house,target)?.dependency||0)>=15||k.greed>=.6)){code='trade_dependency_with_target';reason='Trade with that House is important to my realm. I will not join this war.';}
    else if(!p.operationId&&(memberOperation(s,house)||s.pledges.some(x=>x.debtor===house&&x.status==='pending'&&MILITARY.has(x.intent.type)))){code='military_committed';reason='My armies already have binding duties. I cannot promise the same forces twice.';}
    else if(!['DEFEND','PLEDGE_DEFEND'].includes(i?.type)&&s.kingdoms.some(h=>atWar(s,house,h.id)&&borderThreat(knowledgeView(s,house),house,h.id).score>=20)){code='frontier_threat';reason='My frontier is threatened. I cannot strip it for another campaign.';}
    else if(k.resources.food<20){code='food_shortage';reason='My people and armies need food before I can sustain another campaign.';}
    else if(!armiesOf(s,house).length){code='no_forces';reason='I have no army available to make this commitment.';}
  }
  if(!reason&&isAiHouse(s,house)&&p.direction==='request'&&(isPlayerPromise(i)||['AID','LOAN'].includes(i?.type)||p.operationId)&&r.trust+r.reliability*.25-r.grievance<30){code='insufficient_trust';reason='Our relationship does not justify that commitment.';}
  if(!reason&&isAiHouse(s,house)&&p.direction==='request'&&i?.type==='LOAN'&&i.giveAmount>(economicNeeds(s,house).find(n=>n.resource===i.giveResource)?.surplus||0)){code='resources_reserved';reason='Those resources are needed by my realm; I cannot lend them.';}
  if(reason){const alternativeIntents=alternatives(s,house,p);return {decision:alternativeIntents.length?'alternative':'declined',reasonCodes:[code,...(alternativeIntents.length?['willing_to_supply']:[])],reason,alternativeIntents};}
  if(p.operationId)return {decision:'accepted',reasonCodes:['shared_operation'],reason:'I accept the role and exact rally point in this operation.'};
  const who=parties(p,house),view=knowledgeView(s,who.actor),tile=p.targetTile&&view.tiles[p.targetTile];
  if(tile?.fog==='unknown'&&!tile.knownCapital&&isPlayerPromise(i)&&i.type!=='PLEDGE_PEACE')return {decision:'invalid',reasonCodes:['unknown_target'],reason:'The location is identified, but I need legitimate knowledge before making this specific promise.'};
  // Judge commercial requests from the requested ruler's economic perspective,
  // then restore the approved orientation for counters and actual transfers.
  const reversed=who.actor===house&&isAiHouse(s,house)&&packageTrade(i);
  const verdict=reversed?evaluateDeal(s,house,reversePackage(i),p.proposer,{formalSupport:true}):evaluateDeal(s,who.ruler,i,who.actor,{consentingHuman:who.actor===house||!isAiHouse(s,house),formalSupport:true});
  if(reversed&&verdict.counter)verdict.counter=reversePackage(verdict.counter);
  return {decision:verdict.status==='accept'?'accepted':verdict.status==='counter'?'counter':'declined',reasonCodes:verdict.status==='accept'?['valid_terms',r.trust>=60?'high_trust':'mutual_interest']:['terms_unacceptable'],reason:verdict.status==='accept'?'I accept these exact terms. My commitment is active.':verdict.reason,counterIntent:verdict.counter||null};
}
function commit(s,p,house,intent=p.intent){
  if(p.operationId)return respondOperation(s,house,p.operationId,'accept');
  const who=parties({...p,intent},house),before=s.pledges.length;
  const result=commitDeal(s,who.ruler,intent,who.actor,{consentingHuman:true,formalSupport:true});
  if(result.ok)for(const pledge of s.pledges.slice(before))pledge.formalProposalId=p.id;
  return result;
}
function reactions(s,p){
  if(!p.councilId||p.reactions||p.status!=='resolved')return;
  const c=s.allianceCouncils.find(c=>c.id===p.councilId),accepted=p.requestedHouses.find(h=>isAiHouse(s,h)&&p.responses[h].status==='accepted'),other=p.requestedHouses.find(h=>['declined','alternative'].includes(p.responses[h].status));
  if(!c||!accepted||!other)return;
  const message=p.responses[other].status==='alternative'?`${kingdom(s,other).name}, your offered supplies could help those of us taking the field. Our own commitment stands.`:`${kingdom(s,other).name}, I hear your refusal. We will plan with the Houses that have committed.`;
  appendCouncil(s,c,accepted,message,{formalProposalId:p.id,source:'scripted'});p.reactions=1;
}
function complete(p){if(Object.values(p.responses).every(r=>!['waiting','considering','awaiting-human'].includes(r.status)))p.status='resolved';}
export function resolveFormalResponse(s,actor,id,house){
  const p=record(s,id);if(!p||p.proposer!==actor||!p.approved||!isAiHouse(s,house)||p.responses[house]?.status!=='waiting')return fail('This response is not pending.');
  if(p.requestedHouses.find(h=>p.responses[h].status==='waiting')!==house)return fail('Council responses must be resolved in order.');
  const v=evaluateCouncilProposal(s,house,p),r=p.responses[house];
  if(v.decision==='accepted'){const result=commit(s,p,house);if(!result.ok){v.decision='invalid';v.reason=result.error;v.reasonCodes=['terms_changed'];}}
  Object.assign(r,{status:v.decision,reasonCodes:v.reasonCodes,message:v.reason,...(v.counterIntent?{counterIntent:v.counterIntent}:{}),...(v.alternativeIntents?.length?{alternativeIntents:v.alternativeIntents}:{}),resolvedTurn:s.turn});
  complete(p);reactions(s,p);return {ok:true,proposalId:id,house,status:r.status};
}
export function answerFormalProposal(s,actor,id,house,decision){
  const p=record(s,id),r=p?.responses[house];if(!p||!r||!p.approved||s.turn>p.expires||!['accept','decline'].includes(decision)||!audience(s,p.proposer,p.councilId,p.requestedHouses))return fail('This offer is no longer available.');
  if(r.status==='awaiting-human'){
    if(actor!==house||isAiHouse(s,house))return fail('Only that human ruler can answer.');
    if(decision==='accept'){const v=evaluateCouncilProposal(s,house,p);if(v.decision!=='accepted')return fail(v.reason);const result=commit(s,p,house);if(!result.ok)return result;}
    r.status=decision==='accept'?'accepted':'declined';r.message=decision==='accept'?'These exact terms are accepted and active.':'The requested commitment was declined.';
  }else{
    if(actor!==p.proposer||!['counter','alternative'].includes(r.status)||r.offerAnswered)return fail('Only the proposer can answer these new terms.');
    if(decision==='accept'){
      // Revalidate the whole offer on a copy before transferring anything.
      const next=structuredClone(s),copy=record(next,id),response=copy.responses[house];
      const intents=r.status==='counter'?[r.counterIntent]:r.alternativeIntents;
      for(const intent of intents){const result=r.status==='counter'?commit(next,copy,house,intent):commitDeal(next,p.proposer,intent,house,{consentingHuman:true,formalSupport:true});if(!result.ok)return result;}
      response.offerAnswered='accepted';response.message+=' The offered terms are now active.';Object.assign(s,next);return {ok:true};
    }
    r.offerAnswered='declined';
  }
  complete(p);reactions(s,p);return {ok:true};
}
export function localFormalVoice(s,p,house){
  const k=kingdom(s,house),r=p.responses[house],opening=r.status==='accepted'?(k.honor>=.7?'You have my word. ':k.aggression>=.7?'Let us act decisively. ':'We have an understanding. '):r.reasonCodes.includes('trade_dependency_with_target')?'My realm depends on its merchants. ':k.honor>=.7?'I must honor my obligations. ':'I must weigh the needs of my realm. ';
  return opening+r.message+(r.status==='alternative'?' I can offer the listed support instead.':'');
}
// This only changes the prose attached to an existing deterministic decision.
export function recordFormalVoice(s,actor,id,house,message){const p=record(s,id),r=p?.responses[house];if(!p||p.proposer!==actor||!r||r.spoken||['waiting','awaiting-human'].includes(r.status))return fail('No completed decision awaits a voice.');if(typeof message!=='string'||!message.trim()||message.length>900)return fail('Invalid response text.');const text=message.trim(),accepting=/\b(?:Agreed|Accepted|I agree|We agree|I accept|We accept|I will (?:attack|join|march|fight)|we will (?:attack|join|march|fight))\b/i.test(text),refusing=/\b(?:I refuse|I decline|I (?:will not|cannot|can't|won't) (?:accept|join|attack|commit|march|fight))\b/i.test(text);r.spoken=true;r.voice=(r.status!=='accepted'&&accepting||r.status==='accepted'&&refusing)?r.message:text;return {ok:true};}
export async function runFormalResponseQueue({next,resolve,voice,record,onStatus=()=>{}}){
  while(true){const item=next();if(!item)return;onStatus(item,'considering');const result=await resolve(item);if(!result?.ok){onStatus(item,'invalid');return;}try{const text=await voice(item);if(text)await record(item,text);}catch{/* Decision remains active; continue to the next ruler. */}onStatus(item,'complete');}
}
const words={one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,eleven:11,twelve:12,fifteen:15,twenty:20,thirty:30,forty:40,fifty:50,sixty:60,hundred:100};
export function inferFormalProposal(s,actor,message,{councilId=null,ruler=null,location=null}={}){
  if(typeof message!=='string'||/\b(?:do not|don't|never|cancel)\b/i.test(message))return null;
  const text=message.toLowerCase(),view=knowledgeView(s,actor),number=x=>/^\d+$/.test(x)?Number(x):words[x];
  const promise=ruler&&detectPromise(view,ruler,message,actor);
  if(promise)return {intent:validateIntent(promise),direction:'offer',councilId,requestedHouses:[ruler]};
  const turn=text.match(/\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|fifteen|twenty)\s+turns?\b/),duration=Math.min(20,Math.max(2,turn?number(turn[1]):10));
  const tile=location?.targetTile&&view.tiles[location.targetTile]||Object.values(view.tiles).find(t=>(t.fog!=='unknown'||t.knownCapital)&&t.name&&text.includes(t.name.toLowerCase()))||view.tiles[text.match(/\b\d+,\d+\b/)?.[0]];
  const house=view.kingdoms.find(k=>new RegExp(`\\b${k.id}\\b`,'i').test(text)),resources=text.match(/\b(\d+|twenty|thirty|forty|fifty|sixty|hundred)\s+(food|iron|wood|gold|stone|arms|tools|horses)\b/);
  let intent,direction='request';
  if(/\b(?:supply|provide|send|give)\b/.test(text)&&resources)intent={type:'AID',giveResource:resources[2],giveAmount:number(resources[1]),duration};
  else if(/\b(?:won't|not to|no|avoid) attack\b|\bpromise.*peace\b/.test(text)&&house)intent={type:'PLEDGE_PEACE',targetId:house.id,duration};
  else if(/\b(?:declare|join|enter)\b.*\bwar\b/.test(text)&&house)intent={type:'JOINT_WAR',targetId:house.id,duration};
  else if(/\b(?:attack|take|capture|strike)\b/.test(text)&&tile)intent={type:'PLEDGE_ATTACK',targetId:tile.id,duration};
  else if(/\b(?:attack|invade)\b/.test(text)&&house)intent={type:'JOINT_WAR',targetId:house.id,duration};
  else if(/\b(?:defend|protect)\b/.test(text)&&tile)intent={type:'DEFEND',targetId:tile.id,duration};
  else if(/\b(?:hold|meet|rally|position|reinforce|move)\b/.test(text)&&tile)intent={type:'POSITION',targetId:tile.id,duration};
  else if(/\b(?:grant|give)\b.*\baccess\b/.test(text)){if(house&&![actor,ruler].includes(house.id))return null;intent={type:'ACCESS',duration};}
  if(!intent)return null;
  if(/\bi (?:will|shall|promise)|\bi can (?:send|give)|\blet me\b/.test(text)){direction='offer';if(intent.type==='DEFEND')intent.type='PLEDGE_DEFEND';}
  const c=councilId&&s.allianceCouncils?.find(c=>c.id===councilId),requestedHouses=c?c.participants.filter(h=>h!==actor):ruler?[ruler]:[];
  const normalized=validateIntent(intent);return normalized&&requestedHouses.length?{intent:normalized,direction,councilId,requestedHouses}:null;
}
export function stageConversationProposal(s,actor,message,options){const raw=inferFormalProposal(s,actor,message,options);return raw?submitFormalProposal(s,actor,raw,{inferred:true}):null;}
