// Shared quick-action rules use the real upgrade costs and existing party setter.
import {CONFIG,ELEMENTS,trainingCost,evolutionCost,setParty} from './core.mjs';

export function upgradeReadiness(s,id){
  const p=s.collection?.[id];
  // These are action indicators, so resource eligibility alone is insufficient:
  // core train/evolve also require a conscious teammate between encounters.
  if(!p||!(p.hp>0)||!['trail','camp'].includes(s.run?.phase)||!s.run.party.includes(p.id))return{train:false,evolve:false,elements:[]};
  const cost=trainingCost(p),materialsReady=Object.entries(cost.materials||{}).every(([id,n])=>(s.run.items[id]||0)>=n),train=materialsReady&&p.level<CONFIG.maxLevel&&(p.training||0)>=cost.xp&&(s.run.fruit||0)>=cost.fruit;
  const elements=Object.keys(ELEMENTS).filter(type=>{const offer=evolutionCost(s,type,id);return offer&&p.level>=offer.level&&Object.entries(offer.cost).every(([type,amount])=>(s.run.essence[type]||0)>=amount);});
  return{train,evolve:elements.length>0,elements};
}
export function lineupPermission(s){
  const r=s.run;
  if(!['trail','camp'].includes(r?.phase))return{allowed:false,reason:'Change the lineup on the trail or at camp, between encounters.'};
  return{allowed:true,reason:''};
}
// Preview the exact resulting lineup, including replacement of a knocked-out slot.
export function lineupChoice(s,incomingId,outgoingId=null){
  const permission=lineupPermission(s);if(!permission.allowed)return permission;
  const incoming=s.collection?.[incomingId],party=s.run.party;
  if(!incoming||party.includes(incomingId))return{allowed:false,reason:'Choose a collected companion outside the lineup.'};
  const ids=[...party];
  if(outgoingId!==null){const index=ids.indexOf(outgoingId);if(index<0)return{allowed:false,reason:'Choose a current lineup companion.'};ids[index]=incomingId;}
  else {if(ids.length>=CONFIG.partySize)return{allowed:false,reason:'Choose which lineup companion to replace.'};ids.push(incomingId);}
  if(!ids.some(id=>s.collection[id]?.hp>0))return{allowed:false,reason:'Keep at least one conscious monkey in your lineup.'};
  return{allowed:true,reason:''};
}
export function companionAction(s,id){
  const p=s.collection?.[id];if(!p)throw Error('Choose a collected companion.');
  const permission=lineupPermission(s),inLineup=s.run.party.includes(id),full=s.run.party.length>=CONFIG.partySize;
  const choices=full?s.run.party.map(outgoing=>lineupChoice(s,id,outgoing)):[lineupChoice(s,id)];
  const allowed=permission.allowed&&(inLineup||choices.some(choice=>choice.allowed));
  return{inLineup,action:inLineup?'swap':'add',label:inLineup?'Swap':'Add to lineup',allowed,
    reason:!permission.allowed?permission.reason:!allowed?choices[0]?.reason||'Keep at least one conscious monkey in your lineup.':'',full};
}
export function replaceLineupCompanion(s,outgoingId,incomingId){
  const index=s.run.party.indexOf(outgoingId);
  if(index<0)throw Error('That companion is no longer in the lineup.');
  if(outgoingId===incomingId||s.run.party.includes(incomingId))throw Error('Choose a different Sanctuary companion who is not already in the lineup.');
  const ids=[...s.run.party];ids[index]=incomingId;return setParty(s,ids);
}
export function addLineupCompanion(s,incomingId){
  if(s.run.party.includes(incomingId))throw Error('That companion is already in the lineup.');
  if(s.run.party.length>=CONFIG.partySize)throw Error('Choose which lineup companion to replace.');
  return setParty(s,[...s.run.party,incomingId]);
}

