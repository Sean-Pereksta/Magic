import { kingdom } from './core.mjs';
import { OPERATION_ROLES, OPERATION_STATUSES, initializeCooperation } from './cooperation-state.mjs';
import { NEGOTIATION_KINDS } from './strategic-diplomacy.mjs';

export function validateCooperation(s) {
  initializeCooperation(s);
  const d=s.cooperation,fail=()=>{throw new Error('Damaged strategic cooperation data.');};
  const int=(x,min=0,max=1000000)=>Number.isInteger(x)&&x>=min&&x<=max;
  const text=(x,max)=>typeof x==='string'&&x.length<=max;
  const list=(x,max)=>Array.isArray(x)&&x.length<=max;
  const house=x=>!!kingdom(s,x);
  if(!list(d.operations,24)||!list(d.proposals,60)||!list(d.balance,s.kingdoms.length-1)||!int(d.lastDiplomacyTurn,0,s.turn)||new Set(d.operations.map(o=>o?.id)).size!==d.operations.length||new Set(d.proposals.map(o=>o?.id)).size!==d.proposals.length)fail();
  for(const o of d.operations) {
    if(!o||!/^OP-\d+$/.test(o.id)||!house(o.owner)||!house(o.target)||o.owner===o.target||!s.tiles[o.targetTile]||!text(o.name,60)||!text(o.reason,240)||!int(o.createdTurn,1,s.turn)||!int(o.updatedTurn,o.createdTurn,s.turn)||!OPERATION_STATUSES.includes(o.status)||!int(o.attackStart,o.createdTurn+1,o.createdTurn+20)||!int(o.attackEnd,o.attackStart,Math.min(o.createdTurn+24,o.attackStart+6))||!int(o.exposure,0,100)||!list(o.exposureReasons,8)||o.exposureReasons.some(x=>!text(x,100))||o.launchedTurn!==null&&!int(o.launchedTurn,o.attackStart,Math.min(s.turn,o.attackEnd)))fail();
    if(!list(o.participants,5)||o.participants.length<2||new Set(o.participants.map(p=>p?.house)).size!==o.participants.length||o.participants.find(p=>p.house===o.owner)?.role!=='assault')fail();
    for(const p of o.participants) {
      if(!p||!house(p.house)||p.house===o.target||!Object.hasOwn(OPERATION_ROLES,p.role)||!s.tiles[p.rally]||!int(p.requiredTroops,p.role==='supply'?0:1,500)||!int(p.requiredSiege,0,20)||!int(p.food,0,250)||!['invited','accepted','declined','counter','withdrawn'].includes(p.status)||p.status==='counter'&&!int(p.counterTroops,1,p.requiredTroops)||p.acceptedTurn!==null&&!int(p.acceptedTurn,o.createdTurn,s.turn)||p.planId!==null&&!/^PLAN-\d+$/.test(p.planId))fail();
      const plan=s.intrigue.plans.find(x=>x.id===p.planId);
      if(plan&&(plan.actor!==p.house||plan.operationId!==o.id||plan.target!==o.target||plan.targetTile!==o.targetTile))fail();
      if(['Preparing','Executing'].includes(o.status)&&p.status==='accepted'&&!plan)fail();
    }
  }
  for(const p of s.intrigue.plans)if(p.operationId!==undefined&&(!/^OP-\d+$/.test(p.operationId)||['Considering','Preparing','Committed','Executing'].includes(p.status)&&!d.operations.some(o=>o.id===p.operationId)))fail();
  for(const p of s.pledges.filter(p=>p.operationId)) {
    const o=d.operations.find(o=>o.id===p.operationId),member=o?.participants.find(x=>x.house===p.debtor);
    if(!o||!member||!o.participants.some(x=>x.house===p.creditor)||p.creditor===p.debtor||!['rally','attack','siege','supply','defend','hold'].includes(p.operationTask)||!int(p.produced,0,10000)||typeof p.delivered!=='boolean'||typeof p.breached!=='boolean'||!int(p.deadline,o.createdTurn+1,o.attackEnd+8))fail();
  }
  for(const p of d.proposals) {
    if(!p||!/^NEG-\d+$/.test(p.id)||!house(p.from)||!house(p.to)||p.from===p.to||!NEGOTIATION_KINDS.includes(p.kind)||p.target!==null&&(!house(p.target)||[p.from,p.to].includes(p.target))||p.kind==='embargo'&&!p.target||!['pending','counter','accepted','rejected','expired'].includes(p.status)||!text(p.reason,200)||!text(p.response,240)||!int(p.created,1,s.turn)||!int(p.updated,p.created,s.turn)||!int(p.expires,p.created,p.created+4)||![6,12].includes(p.duration)||p.planId!==null&&!/^PLAN-\d+$/.test(p.planId))fail();
  }
  for(const b of d.balance)if(!b||!house(b.house)||!house(b.dominant)||b.house===b.dominant||!['neutral','align','coalition','support','fortify'].includes(b.response)||!int(b.turn,1,s.turn))fail();
}
