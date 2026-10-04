import { alive, armiesOf, atWar, kingdom, relation, treaty } from './core.mjs';
import { knowledgeView } from './fog.mjs';
import { evaluateDeal, commitDeal, validateIntent, describeIntent } from './diplomacy.mjs';
import { isPlayerPromise, detectPromise } from './promises.mjs';
import { isAiHouse, court } from './house-control.mjs';
import { appendConversation, borderThreat, diplomaticCapacity } from './living.mjs';
import { councilActive, appendCouncil, councilDiagnostic } from './council-state.mjs';
import { operationFor, operationMember, memberOperation } from './cooperation-state.mjs';
import { respondOperation } from './operations.mjs';
import { economicNeeds } from './trade.mjs';
import { packageTrade, tradeItems, withItems } from './trade-package.mjs';
import { consumeDiplomaticMessage, privateConversation } from './proposal-followup.mjs';
import { COUNCIL_RESPONSE_GAP_MS, pauseCouncilResponse } from './council-sequence.mjs';
import { makeDiagnostic } from './diagnostics.mjs';

const fail=error=>({ok:false,error});
const MILITARY=new Set(['JOINT_WAR','DEFEND','POSITION','BUILD_DEFENSES','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND','PLEDGE_BUILD','GUARANTEE']);
const OFFENSIVE=new Set(['JOINT_WAR','PLEDGE_WAR','PLEDGE_ATTACK']);
const validHouse=(s,id)=>typeof id==='string'&&s.kingdoms.some(k=>k.id===id);
const record=(s,id)=>s.cooperation?.formalProposals?.find(p=>p.id===id);
const pending=r=>r&&(['waiting','considering'].includes(r.status)||r.status!=='awaiting-human'&&!r.spoken&&!r.voiceComplete);
export function formalConversationCurrent(s,p){
  if(!p?.approved||p.conversationCancelled||!['processing','resolved'].includes(p.status)||p.sentTurn!==s.turn||s.outcome||!alive(s,p.proposer)||!audience(s,p.proposer,p.councilId,p.requestedHouses))return false;
  return !p.councilId||s.allianceCouncils.find(c=>c.id===p.councilId)?.messages.some(m=>m.formalProposalId===p.id&&m.speakerHouseId===p.proposer&&m.turn===p.sentTurn);
}
export const isFormalCouncilBusy=(s,id)=>!!id&&(s.cooperation?.formalProposals||[]).some(p=>p.councilId===id&&formalConversationCurrent(s,p)&&p.requestedHouses.some(h=>isAiHouse(s,h)&&pending(p.responses[h])));
export function cancelFormalConversation(s,actor){
  let cancelled=0;
  for(const p of s.cooperation?.formalProposals||[]){
    if(p.proposer!==actor||!p.approved||p.conversationCancelled||!p.requestedHouses.some(h=>isAiHouse(s,h)&&pending(p.responses[h])))continue;
    p.conversationCancelled=true;cancelled++;
    for(const h of p.requestedHouses){const r=p.responses[h];if(isAiHouse(s,h)&&['waiting','considering'].includes(r.status))Object.assign(r,{status:'invalid',reasonCodes:['circumstances_changed'],message:'The active ruler changed before this response was completed.',resolvedTurn:s.turn});}
    complete(p);
  }
  return cancelled;
}
const targetOf=(s,p,house)=>{const view=knowledgeView(s,house);return validHouse(s,p.intent?.targetId)?p.intent.targetId:view.tiles[p.intent?.targetId]?.owner||view.tiles[p.intent?.targetId]?.knownCapital||null;};
const RECEIVER_ACTIONS=new Set(['DEFEND','POSITION','BUILD_DEFENSES','WITHDRAW','EMBARGO']);
export const isFactionPeace=i=>i?.type==='PEACE'&&!!i.targetId;
export const defaultProposalDirection=i=>RECEIVER_ACTIONS.has(i?.type)||i?.type==='JOINT_WAR'||isFactionPeace(i)?'request':'offer';
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
export function formalDescription(p){const requested=p.direction==='request'&&requestedSender(p.intent);return p.operationId?`Join ${p.operationId}`:isFactionPeace(p.intent)?`Request selected rulers to make peace with ${p.intent.targetId} · Peace treaty for ${p.intent.duration} turns · No resource payment`:`${requested?'Requested commitment: ':''}${requested?describeIntent(p.intent).replaceAll('Proposer','Requested House'):describeIntent(p.intent)}${p.targetTile?` · Exact hex ${p.targetTile}`:''}`;}
function factionPeaceTerms(s,actor,direction,intent,houses){
  if(direction!=='request')return 'Ask the selected rulers to make peace with the target faction.';
  if(!validHouse(s,intent.targetId)||!alive(s,intent.targetId)||[actor,...houses].includes(intent.targetId))return 'Choose a surviving third faction, separate from you and the requested rulers.';
  if(intent.giveAmount||intent.receiveAmount)return 'Mediated peace requests do not transfer resources. Negotiate payments directly with the faction.';
  return null;
}
function peaceWarCommitment(s,house,target){
  const operation=memberOperation(s,house);
  return operation&&['attack','capture','siege'].includes(operation.objectiveType||'attack')&&(operation.targetHouse||operation.target)===target||s.pledges.some(p=>p.debtor===house&&p.status==='pending'&&OFFENSIVE.has(p.intent.type)&&(p.intent.targetId===target||p.targetOwner===target||s.tiles[p.intent.targetId]?.owner===target||s.armies.find(a=>a.id===p.intent.targetId)?.owner===target));
}
function evaluateFactionPeace(s,house,p){
  const i=p.intent,target=i.targetId,terms=factionPeaceTerms(s,p.proposer,p.direction,i,p.requestedHouses),result=(decision,code,reason)=>({decision,reasonCodes:[code],reason});
  if(terms)return result('invalid','invalid_peace_target',terms);
  const name=kingdom(s,target).name;
  if(!atWar(s,house,target))return result('invalid','already_at_peace',`${kingdom(s,house).name} is already at peace with ${name}. No war was changed.`);
  if(!isAiHouse(s,target))return result('declined','target_consent_required',`${name} is human-controlled and must accept peace in direct negotiations. This request cannot consent on their behalf.`);
  for(const party of [house,target])if(peaceWarCommitment(s,party,party===house?target:house))return result('declined','peace_commitment_conflict',`An existing offensive commitment prevents this peace with ${name}. Resolve that commitment first.`);
  // Both warring courts judge the same zero-payment treaty. The mediator cannot
  // spend either court's resources, accept a hidden counter, or force a human's consent.
  const treatyIntent={type:'PEACE',duration:i.duration};
  if(isAiHouse(s,house)&&evaluateDeal(s,house,treatyIntent,target).status!=='accept')return result('declined','peace_terms_unacceptable',`I am not willing to end my war with ${name} on these terms. The war remains active.`);
  if(evaluateDeal(s,target,treatyIntent,house).status!=='accept')return result('declined','target_declined_peace',`${name} is not willing to accept this peace. Our war remains active.`);
  return result('accepted','mediated_peace',`${kingdom(s,house).name} and ${name} have agreed to end their war and establish a peace treaty for ${i.duration} turns. No resources change hands; your other wars are unchanged.`);
}
function initialize(s){s.cooperation??={operations:[],proposals:[],balance:[],lastDiplomacyTurn:0};s.cooperation.formalProposals??=[];}
const canPrune=(s,p)=>p.status==='dismissed'||p.status==='resolved'&&(s.turn>p.expires||!Object.values(p.responses).some(r=>['counter','alternative'].includes(r.status)&&!r.offerAnswered));
function trim(s){const rows=s.cooperation.formalProposals;while(rows.length>40){const i=rows.findIndex(p=>canPrune(s,p));if(i<0)break;rows.splice(i,1);}}
export function formalReplyAvailable(s,p,actor,house){
  const r=p?.responses?.[house];
  return !!(p?.approved&&p.proposer===actor&&s.turn<=p.expires&&audience(s,actor,p.councilId,p.requestedHouses)&&r&&['counter','alternative'].includes(r.status)&&!r.offerAnswered);
}
function replyTarget(s,actor,value,councilId,houses){
  if(!value||typeof value!=='object'||!/^FORMAL-\d+$/.test(value.proposalId)||!validHouse(s,value.house))return null;
  const parent=record(s,value.proposalId);
  return parent&&parent.councilId===councilId&&houses.length===1&&houses[0]===value.house&&formalReplyAvailable(s,parent,actor,value.house)?parent.responses[value.house]:null;
}
export function submitFormalProposal(s,actor,raw,{inferred=false}={}){
  if(s.outcome||s.phase==='founding'||!alive(s,actor)||!raw||typeof raw!=='object')return fail('Proposals require an active ruler.');
  const councilId=raw.councilId||null,members=audience(s,actor,councilId,raw.requestedHouses);if(!members)return fail('Choose current participants in this conversation.');
  const direction=raw.direction??'offer';if(!['offer','request'].includes(direction))return fail('Choose who is offering the commitment.');
  if(raw.replyTo!==undefined&&(inferred||!replyTarget(s,actor,raw.replyTo,councilId,raw.requestedHouses)))return fail('This counteroffer is no longer available to modify.');
  const operation=raw.operationId&&operationFor(s,raw.operationId),intent=operation?null:validateIntent(raw.intent);
  if(raw.operationId&&(!operation||operation.owner!==actor||operation.status!=='Preparing'||raw.requestedHouses.some(h=>operationMember(operation,h)?.status!=='invited')))return fail('Choose an operation with invitations for these Houses.');
  if(!operation&&!intent)return fail('Choose valid structured terms.');
  if(isFactionPeace(intent)){const error=factionPeaceTerms(s,actor,direction,intent,raw.requestedHouses);if(error)return fail(error);}
  if(intent&&['WAR','BETRAY','VASSALAGE','MARRIAGE','INTELLIGENCE'].includes(intent.type)&&councilId)return fail('These terms belong in a private ruler conversation.');
  if(intent?.targetId&&!validHouse(s,intent.targetId)&&!Object.hasOwn(s.tiles,intent.targetId)&&!knowledgeView(s,actor).armies.some(a=>a.id===intent.targetId))return fail('Choose a valid known target or map coordinate.');
  initialize(s);if(s.cooperation.formalProposals.filter(p=>!['resolved','dismissed'].includes(p.status)).length>=24)return fail('Resolve or dismiss earlier proposals first.');
  if(s.cooperation.formalProposals.length>=40&&!s.cooperation.formalProposals.some(p=>canPrune(s,p)))return fail('Answer an earlier offer before sending another proposal.');
  const p={id:`FORMAL-${s.nextId++}`,proposer:actor,councilId,audience:[...members],requestedHouses:[...raw.requestedHouses],source:inferred?'conversation_inferred':'explicit',direction,intent,operationId:operation?.id||null,targetTile:operation?.targetTile||(intent&&Object.hasOwn(s.tiles,intent.targetId)?intent.targetId:null),created:s.turn,expires:s.turn+3,status:'draft',approved:false,responses:{},reactions:0};
  if(raw.replyTo)p.replyTo={proposalId:raw.replyTo.proposalId,house:raw.replyTo.house};
  s.cooperation.formalProposals.push(p);
  if(!inferred){const result=ratifyFormalProposal(s,actor,p.id);if(!result.ok){s.cooperation.formalProposals=s.cooperation.formalProposals.filter(x=>x!==p);return result;}}
  trim(s);
  return {ok:true,proposalId:p.id};
}
export function ratifyFormalProposal(s,actor,id){
  const p=record(s,id);if(!p||p.proposer!==actor||p.status!=='draft'||p.approved||s.turn>p.expires||!audience(s,actor,p.councilId,p.requestedHouses))return fail('This draft is no longer available.');
  const replying=p.replyTo&&replyTarget(s,actor,p.replyTo,p.councilId,p.requestedHouses);
  if(p.replyTo&&!replying)return fail('This counteroffer has already been answered or expired.');
  const sequence=p.councilId&&s.allianceCouncils.find(c=>c.id===p.councilId)?.activeSequence;
  if(p.councilId&&(isFormalCouncilBusy(s,p.councilId)||sequence?.status==='pending'&&sequence.turn===s.turn))return fail('Wait for the current council conversation to finish.');
  if(p.councilId&&p.requestedHouses.length===1){const spent=consumeDiplomaticMessage(s,p.requestedHouses[0],actor,p.intent,p.councilId);if(!spent.ok)return spent;}
  else if(p.councilId){const c=court(s,actor),used=c.messages.turn===s.turn?c.messages:{turn:s.turn,regular:0,hosts:{}};if(used.regular>=diplomaticCapacity(s,actor))return fail('Your shared dispatches are used for this turn.');c.messages=used;if(!s.controllers)s.diplomacy.messages=used;used.regular++;}
  else {const spent=consumeDiplomaticMessage(s,p.requestedHouses[0],actor,p.intent,privateConversation(p.requestedHouses[0]));if(!spent.ok)return spent;}
  p.approved=true;p.status='processing';p.sentTurn=s.turn;
  p.responses=Object.fromEntries(p.requestedHouses.map(h=>[h,{status:isAiHouse(s,h)?'waiting':'awaiting-human',reasonCodes:[],message:'',spoken:false}]));
  if(replying){replying.offerAnswered='modified';replying.replacementProposalId=p.id;}
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
  if(isFactionPeace(p.intent))return evaluateFactionPeace(s,house,p);
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
  if(isFactionPeace(intent)){
    const verdict=evaluateFactionPeace(s,house,{...p,intent});
    if(verdict.decision!=='accepted')return fail(verdict.reason);
    return commitDeal(s,intent.targetId,{type:'PEACE',duration:intent.duration},house,{consentingHuman:true});
  }
  const who=parties({...p,intent},house),before=s.pledges.length;
  const result=commitDeal(s,who.ruler,intent,who.actor,{consentingHuman:true,formalSupport:true});
  if(result.ok)for(const pledge of s.pledges.slice(before))pledge.formalProposalId=p.id;
  return result;
}
function complete(p){if(Object.values(p.responses).every(r=>!['waiting','considering','awaiting-human'].includes(r.status)))p.status='resolved';}
export function markFormalConsidering(s,actor,id,house){
  const p=record(s,id),r=p?.responses[house];
  if(p?.proposer===actor&&formalConversationCurrent(s,p)&&isAiHouse(s,house)&&r?.retryRequested&&!r.voiceComplete&&!r.spoken){r.retryRequested=false;return {ok:true,voiceRetry:true};}
  if(!p||p.proposer!==actor||!formalConversationCurrent(s,p)||!isAiHouse(s,house)||!r||!['waiting','considering'].includes(r.status)||p.requestedHouses.find(h=>['waiting','considering'].includes(p.responses[h].status))!==house)return fail('This response is no longer pending.');
  r.status='considering';return {ok:true};
}
export function retryFormalVoice(s,actor,id,house){
  const p=record(s,id),r=p?.responses[house],c=p?.councilId&&s.allianceCouncils.find(c=>c.id===p.councilId);
  if(!p||p.proposer!==actor||!formalConversationCurrent(s,p)||!isAiHouse(s,house)||!r||r.source!=='failed'||!r.voiceComplete||r.spoken)return fail('This Gemini reply cannot be retried now.');
  if(isFormalCouncilBusy(s,p.councilId)||c?.activeSequence?.status==='pending'&&c.activeSequence.turn===s.turn)return fail('Wait for the current council conversation to finish.');
  r.voiceComplete=false;r.retryRequested=true;return {ok:true};
}
export function resolveFormalResponse(s,actor,id,house){
  const p=record(s,id);if(!p||p.proposer!==actor||!p.approved||!formalConversationCurrent(s,p)||!isAiHouse(s,house)||!['waiting','considering'].includes(p.responses[house]?.status))return fail('This response is not pending.');
  if(p.requestedHouses.find(h=>['waiting','considering'].includes(p.responses[h].status))!==house)return fail('Council responses must be resolved in order.');
  const v=evaluateCouncilProposal(s,house,p),r=p.responses[house];
  if(v.decision==='accepted'){const result=commit(s,p,house);if(!result.ok){v.decision='invalid';v.reason=result.error;v.reasonCodes=['terms_changed'];}}
  Object.assign(r,{status:v.decision,reasonCodes:v.reasonCodes,message:v.reason,...(v.counterIntent?{counterIntent:v.counterIntent}:{}),...(v.alternativeIntents?.length?{alternativeIntents:v.alternativeIntents}:{}),resolvedTurn:s.turn});
  complete(p);return {ok:true,proposalId:id,house,status:r.status};
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
  complete(p);return {ok:true};
}
// This only changes the prose attached to an existing deterministic decision.
export function recordFormalVoice(s,actor,id,house,message,source='failed',rawDiagnostic=null){
  const p=record(s,id),r=p?.responses[house];
  if(!p||p.proposer!==actor||!r||!isAiHouse(s,house))return fail('No completed decision awaits a voice.');
  if(r.spoken||r.voiceComplete)return {ok:true,duplicate:true};
  if(!formalConversationCurrent(s,p)||['waiting','considering','awaiting-human'].includes(r.status))return fail('No completed decision awaits a voice.');
  if(!['gemini','failed','scripted'].includes(source)||typeof message!=='string'||message.length>900)return fail('Invalid response text.');
  const text=message.trim(),accepting=/\b(?:Agreed|Accepted|I agree|We agree|I accept|We accept|I will (?:attack|join|march|fight)|we will (?:attack|join|march|fight))\b/i.test(text),refusing=/\b(?:I refuse|I decline|I (?:will not|cannot|can't|won't) (?:accept|join|attack|commit|march|fight))\b/i.test(text);
  const peaceAcceptance=isFactionPeace(p.intent)&&/\b(?:I|we) (?:will|shall|have) (?:make|made|sign|signed) (?:a )?(?:peace|truce)\b/i.test(text);
  const peaceRefusal=isFactionPeace(p.intent)&&/\b(?:I|we) (?:will not|cannot|can't|won't) (?:make|sign) (?:a )?(?:peace|truce)\b/i.test(text);
  const contradictory=r.status!=='accepted'&&(accepting||peaceAcceptance)||r.status==='accepted'&&(refusing||peaceRefusal);
  const voiced=source==='gemini'&&text&&!contradictory;
  let diagnostic=councilDiagnostic(rawDiagnostic);
  if(!voiced&&!diagnostic)diagnostic=makeDiagnostic(contradictory||source==='gemini'?'GEMINI_RESPONSE_INVALID':'WORKER_UNAVAILABLE');
  r.voiceComplete=true;r.retryRequested=false;r.spoken=!!voiced;r.source=voiced?'gemini':'failed';
  if(diagnostic)r.diagnostic=diagnostic;
  if(!voiced){delete r.voice;return {ok:true,voiceUnavailable:true};}
  r.voice=text;
  if(p.councilId){
    const c=s.allianceCouncils.find(c=>c.id===p.councilId);
    if(!c.messages.some(m=>m.formalProposalId===id&&m.speakerHouseId===house&&m.formalResponse))appendCouncil(s,c,house,r.voice,{formalProposalId:id,formalResponse:true,source:'gemini'});
  }
  return {ok:true};
}
export async function runFormalResponseQueue({next,resolve,voice,record,fallback=()=>({message:'',source:'failed'}),onStatus=()=>{},isCurrent=()=>true,gapMs=COUNCIL_RESPONSE_GAP_MS,pause=pauseCouncilResponse}){
  const completed=new Set();
  while(isCurrent()){
    const item=next();if(!item)return {ok:true};
    const key=`${item.id}:${item.house}`;if(completed.has(key))return fail('This council response has already been processed.');
    const considering=await onStatus(item,'considering');if(considering?.ok===false)return considering;if(!isCurrent())break;
    const result=await resolve(item);if(!result?.ok){await onStatus(item,'invalid');return result;}
    await onStatus(item,'resolved');if(!isCurrent())break;
    // A saved authoritative decision may already have started a paid request
    // before the page disconnected. Mark its voice unavailable, never repeat the paid request.
    let reply;try{reply=item.resumeVoice?await fallback(item):await voice(item);}catch(error){if(isCurrent())reply=await fallback(item,error);}
    if(!isCurrent()||reply?.source==='cancelled')break;
    if(!reply)reply=await fallback(item);
    if(!isCurrent())break;
    if(reply){const committed=await record(item,reply);if(committed?.ok===false)return committed;}
    completed.add(key);await onStatus(item,'complete');
    if(!isCurrent())break;
    if(!next())return {ok:true};
    if(gapMs>0)await pause(gapMs);
  }
  return {ok:false,cancelled:true};
}
const words={one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,eleven:11,twelve:12,fifteen:15,twenty:20,thirty:30,forty:40,fifty:50,sixty:60,hundred:100};
export function inferFormalProposal(s,actor,message,{councilId=null,ruler=null,location=null}={}){
  if(typeof message!=='string'||/\b(?:do not|don't|never|cancel)\b/i.test(message))return null;
  const text=message.toLowerCase().replaceAll('’',"'"),view=knowledgeView(s,actor),number=x=>/^\d+$/.test(x)?Number(x):words[x];
  const turn=text.match(/\b(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|fifteen|twenty)\s+turns?\b/),duration=Math.min(20,Math.max(2,turn?number(turn[1]):10));
  const peace=text.match(/\b(?:(?:make|negotiate|sign|agree to|seek)\s+(?:a\s+)?(?:peace|truce)(?:\s+treaty)?\s+with|end\s+(?:(?:your|the|our)\s+)?war\s+(?:with|against))\s+(?:the\s+)?(?:faction\s+|house\s+)?([a-z]+)/);
  if(peace){
    if(/\b(?:not|don't|won't|never|if|unless|maybe|perhaps|i will|i'll|i promise|i shall|we will|we'll|i can|let me)\b/.test(text))return null;
    const target=s.kingdoms.find(k=>k.id===peace[1]);if(!target||target.id===actor||target.id===ruler)return null;
    const council=councilId&&s.allianceCouncils?.find(c=>c.id===councilId),members=council?council.participants.filter(h=>h!==actor&&h!==target.id):ruler?[ruler]:[];
    const addressed=members.filter(h=>new RegExp(`\\b${h}\\b`).test(text.slice(0,peace.index)));
    const requestedHouses=addressed.length?addressed:members;
    return requestedHouses.length?{intent:validateIntent({type:'PEACE',targetId:target.id,duration}),direction:'request',councilId,requestedHouses}:null;
  }
  const promise=ruler&&detectPromise(view,ruler,message,actor);
  if(promise)return {intent:validateIntent(promise),direction:'offer',councilId,requestedHouses:[ruler]};
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
