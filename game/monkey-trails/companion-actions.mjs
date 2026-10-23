// Shared quick-action rules use the real upgrade costs and existing party setter.
import {CONFIG,ELEMENTS,trainingCost,evolutionCost,setParty} from './core.mjs';

export function upgradeReadiness(s,id){
  const p=s.collection?.[id];if(!p)return{train:false,evolve:false,elements:[]};
  const conscious=p.hp>0,cost=trainingCost(p),train=conscious&&p.level<CONFIG.maxLevel&&(p.training||0)>=cost.xp&&(s.run.fruit||0)>=cost.fruit;
  const elements=conscious?Object.keys(ELEMENTS).filter(type=>{const offer=evolutionCost(s,type,id);return offer&&p.level>=offer.level&&Object.entries(offer.cost).every(([type,amount])=>(s.run.essence[type]||0)>=amount);}):[];
  return{train,evolve:elements.length>0,elements};
}
export function lineupPermission(s){
  const r=s.run;
  if(!(r?.phase==='camp'||r?.phase==='trail'&&r.distance===0&&r.encounters===0))return{allowed:false,reason:'Change the lineup before departure or at an earned trail camp.'};
  if(r.party.some(id=>!(s.collection[id]?.hp>0)))return{allowed:false,reason:'Revive knocked-out teammates before changing the lineup.'};
  return{allowed:true,reason:''};
}
export function companionAction(s,id){
  const p=s.collection?.[id];if(!p)throw Error('Choose a collected companion.');
  const permission=lineupPermission(s),inLineup=s.run.party.includes(id);
  return{inLineup,action:inLineup?'swap':'add',label:inLineup?'Swap':'Add to lineup',allowed:permission.allowed&&p.hp>0,
    reason:p.hp<=0?'Revive this companion before changing its lineup slot.':permission.reason,full:s.run.party.length>=CONFIG.partySize};
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
