import { relationshipResponse } from './diplomacy.mjs';
import { discussMarriage, continueMarriageReview } from './marriage.mjs';
import { createOperation, respondOperation, supplyOperation, leaveOperation } from './operations.mjs';
import { respondCooperation } from './strategic-diplomacy.mjs';
import { foundCity, foundAIKingdoms } from './founding.mjs';
import { build, buildHighway, recruit, orderArmy, orderStructureAttack, splitArmy, mergeArmies, kingdom, alive } from './core.mjs';
import { setFormation } from './warfare.mjs';
import { recruitSpy, assignSpy, paySpyRansom, resolveCaptive } from './espionage.mjs';
import { recruitAmbassador, assignAmbassador, ambassadorIncident, appendConversation, consumeMessage, applySpeech, markRead } from './living.mjs';
import { commitDeal, deliverPledge, scriptedReply, validateIntent, validateResponse, acceptRulerMemories, describeIntent } from './diplomacy.mjs';
import { court, isHumanHouse } from './house-control.mjs';
import { houseIds, requestTakeover } from './multiplayer-rounds.mjs';

const ok=()=>({ok:true}), fail=error=>({ok:false,error});
const TEXT_LIMIT=600;
export const COMMAND_TYPES=['operationCreate','operationAnswer','operationSupply','operationLeave','cooperationAnswer','found','build','highway','recruit','order','structure','split','merge','formation','tax','recruitSpy','assignSpy','ransom','captive','recruitAmbassador','assignAmbassador','ambassadorIncident','deliver','ratify','declineTrade','chat','humanProposal','respondProposal','read','ready','takeover'];
export function commandError(s, meta, command) {
  if(!command||!COMMAND_TYPES.includes(command.type)||!command.args||Array.isArray(command.args)||typeof command.args!=='object'||JSON.stringify(command.args).length>14000)return 'Invalid command.';
  if(typeof command.id!=='string'||command.id.length>120||typeof command.clientId!=='string'||!/^[a-zA-Z0-9_-]{8,64}$/.test(command.clientId)||!Number.isSafeInteger(command.sequence)||command.sequence<1)return 'Invalid command identity.';
  const seat=meta.seats[command.actorHouseId];
  if(!seat||seat.kind!=='human'||seat.uid!==command.uid||seat.substitute)return 'You do not control that House.';
  if(command.epoch!==meta.epoch)return 'The controller changed. Review and submit this order again.';
  if(command.turn!==s.turn)return 'This order belongs to an earlier round.';
  if(!Number.isSafeInteger(command.stateVersion)||command.stateVersion<1||command.stateVersion>meta.stateVersion)return 'Invalid campaign version.';
  if(s.outcome)return 'This campaign has ended.';
  if(meta.phase==='founding'){
    if(s.phase!=='founding'||command.type!=='found')return 'Found all six kingdoms before issuing orders.';
  }else if(meta.phase!=='planning'||command.type==='found')return 'Orders are closed for this campaign phase.';
  if(meta.ready[command.actorHouseId]&&!['ready','read'].includes(command.type))return 'Unready your House before issuing more orders.';
  if(meta.phase!=='founding'&&!alive(s,command.actorHouseId))return 'This House has lost its last settlement.';
  if((meta.sequences[`${command.uid}:${command.clientId}`]||0)>=command.sequence)return 'This command has already been processed.';
  return null;
}
// Called only by the controller with the authenticated envelope from Firestore.
// No command can set resources, results, RNG, or replacement state.
export function applyCommand(s, meta, c, {presence={},now=0}={}) {
  const error=commandError(s,meta,c);if(error)return fail(error);
  const a=c.actorHouseId,p=c.args,target=p.targetHouseId;
  let result;
  const validTarget=()=>houseIds.includes(target)&&target!==a&&alive(s,target);
  switch(c.type){
    case 'found':{
      // Work on a copy: any placement/AI failure leaves the authoritative state untouched.
      const next=structuredClone(s);result=foundCity(next,a,p.tile);
      if(result.ok)result=foundAIKingdoms(next);
      if(result.ok){
        Object.assign(s,next);
        if(s.phase==='playing'){
          meta.phase='planning';meta.turn=1;meta.ready={};meta.planningAt=now;
          meta.deadline=meta.options.timerSeconds?now+meta.options.timerSeconds*1000:0;
        }
      }
      break;
    }
    case 'operationCreate':result=createOperation(s,a,p);break;
    case 'operationAnswer':result=respondOperation(s,a,p.id,p.decision,p.member||a);break;
    case 'operationSupply':result=supplyOperation(s,a,p.id);break;
    case 'operationLeave':result=leaveOperation(s,a,p.id);break;
    case 'cooperationAnswer':result=respondCooperation(s,a,p.id,p.decision);break;
    case 'build':result=build(s,a,p.tile,p.building);break;
    case 'highway':result=buildHighway(s,a,p.from,p.to);break;
    case 'recruit':result=recruit(s,a,p.tile,p.unit);break;
    case 'order':result=orderArmy(s,a,p.army,p.tile,p.order);break;
    case 'structure':result=orderStructureAttack(s,a,p.army,p.tile,p.structure,p.mode);break;
    case 'split':result=splitArmy(s,a,p.army);break;
    case 'merge':result=mergeArmies(s,a,p.tile);break;
    case 'formation':result=setFormation(s,a,p.army,p.formation);break;
    case 'tax':if(!['low','medium','high'].includes(p.policy))return fail('Invalid tax policy.');kingdom(s,a).tax=p.policy;result=ok();break;
    case 'recruitSpy':result=recruitSpy(s,a);break;
    case 'assignSpy':result=assignSpy(s,a,p.spy,p.host,p.mission);break;
    case 'ransom':result=paySpyRansom(s,a,p.spy);break;
    case 'captive':result=resolveCaptive(s,a,p.spy,p.action);break;
    case 'recruitAmbassador':result=recruitAmbassador(s,a);break;
    case 'assignAmbassador':result=assignAmbassador(s,a,p.envoy,p.host);break;
    case 'ambassadorIncident':result=ambassadorIncident(s,a,p.envoy,p.action,p.confirmed===true);break;
    case 'deliver':result=deliverPledge(s,p.pledge,a);break;
    case 'read':if(!validTarget())return fail('Unknown ruler.');markRead(s,target,a);result=ok();break;
    case 'ratify':{
      if(!validTarget())return fail('Unknown ruler.');
      const i=validateIntent(p.intent);if(!i)return fail('Invalid terms.');
      result=commitDeal(s,target,i,a);
      if(result.ok){
        const offer=s.commerce.offers.find(o=>o.id===p.tradeId&&o.from===target&&(o.to||'ashen')===a);
        if(offer)offer.status='accepted';
        appendConversation(s,target,'council',`${describeIntent(i)} — ratified on turn ${s.turn}.`,{actorHouseId:a});
        court(s,a).offers[target]=[];
      }break;
    }
    case 'declineTrade':{
      const offer=s.commerce.offers.find(o=>o.id===p.id&&(o.to||'ashen')===a&&o.status==='pending');
      if(!offer)return fail('Offer no longer available.');offer.status='declined';result=ok();break;
    }
    case 'chat':{
      if(!validTarget()||typeof p.message!=='string'||!p.message.trim()||p.message.length>TEXT_LIMIT)return fail('Enter a message of up to 600 characters.');
      if(p.proposal&&!validateIntent(p.proposal))return fail('Invalid terms.');
      const spent=consumeMessage(s,target,a);if(!spent.ok)return spent;
      appendConversation(s,target,'player',p.message,{actorHouseId:a});
      if(isHumanHouse(s,target)){
        discussMarriage(s,target,p.message,a);
        appendConversation(s,a,'ruler',p.message,{actorHouseId:target,unread:true,kind:'human'});
      }else{
        if(!p.proposal)applySpeech(s,target,p.message,a);
        else if(p.proposal.type==='MARRIAGE')continueMarriageReview(s,target,a);
        // A model reply is untrusted dialogue/proposals. Rule checks run only on ratification.
        const model=p.response&&validateResponse(p.response);
        const options={actorHouseId:a,proposal:p.proposal||null};
        const response=relationshipResponse(s,target,p.message,model||scriptedReply(s,target,p.message,options),options);
        appendConversation(s,target,'ruler',response.reply,{actorHouseId:a,unread:true});
        if(model)acceptRulerMemories(s,target,model,a);
        const candidates=[p.proposal,...response.intents,response.proposal,response.counterProposal,response.promiseDetected].filter(Boolean);
        court(s,a).offers[target]=[...new Map(candidates.map(i=>[JSON.stringify(i),i])).values()].slice(0,4);
      }
      result=ok();break;
    }
    case 'humanProposal':{
      if(!validTarget()||!isHumanHouse(s,target))return fail('Select a human ruler.');
      const intent=validateIntent(p.intent);if(!intent||['WAR','BETRAY'].includes(intent.type))return fail('Declare war directly from the council.');
      s.humanProposals||=[];
      if(s.humanProposals.filter(o=>o.status==='pending'&&o.from===a).length>=10)return fail('Resolve outstanding proposals before sending more.');
      const spent=consumeMessage(s,target,a);if(!spent.ok)return spent;
      if(intent.type==='MARRIAGE')continueMarriageReview(s,target,a);
      s.humanProposals.push({id:c.id,from:a,to:target,intent,turn:s.turn,expires:s.turn+3,status:'pending'});
      for(const [actor,other] of [[a,target],[target,a]])appendConversation(s,other,'council',`Proposal from ${kingdom(s,a).name}: ${describeIntent(intent)}. Awaiting ${kingdom(s,target).name}.`,{actorHouseId:actor,unread:actor===target,kind:'human-proposal'});
      result=ok();break;
    }
    case 'respondProposal':{
      const offer=(s.humanProposals||[]).find(o=>o.id===p.id&&o.to===a&&o.status==='pending'&&o.expires>=s.turn);
      if(!offer||!['accept','decline'].includes(p.decision))return fail('This proposal is no longer available.');
      if(p.decision==='accept'){
        result=commitDeal(s,a,offer.intent,offer.from,{consentingHuman:true});if(!result.ok)return result;
      }
      offer.status=p.decision==='accept'?'accepted':'declined';offer.resolvedTurn=s.turn;
      for(const [actor,other] of [[a,offer.from],[offer.from,a]])appendConversation(s,other,'council',`${describeIntent(offer.intent)} — ${offer.status}.`,{actorHouseId:actor,unread:true});
      result=ok();break;
    }
    case 'ready':if(typeof p.ready!=='boolean')return fail('Invalid readiness.');meta.ready[a]=p.ready;result=ok();break;
    case 'takeover':try{requestTakeover(meta,c.uid,p.houseId,presence,now,p.permanent===true);result=ok();}catch(e){return fail(e.message);}break;
    default:return fail('Unknown order.');
  }
  if(result.ok){
    meta.sequences[`${c.uid}:${c.clientId}`]=c.sequence;
    // Immutable command receipts also prevent replay after an old session is pruned.
    const entries=Object.entries(meta.sequences);if(entries.length>48)delete meta.sequences[entries[0][0]];
  }
  return result;
}
