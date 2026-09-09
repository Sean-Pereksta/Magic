/* Colony race cancellation. Refund only paid credits, committed materials and
   embarked population. Unrecruited volunteers and undelivered orders are not
   resources and must never be credited. Optional ledgers use the existing save. */
function stxCMWorld(owner,from){
  return state.planets.find(p=>p.id===from&&p.owner===owner)||
    owned(owner).find(p=>p.home)||owned(owner)[0]||null;
}
function stxCMRefund(owner,from,materials={},population=0,credits=0){
  const e=empire(owner);if(!e)return;
  e.credits+=Math.max(0,Number(credits)||0);
  const p=stxCMWorld(owner,from),goods=Object.fromEntries(Object.entries(materials)
    .filter(([r,n])=>r!=='population'&&Number.isFinite(n)&&n>0));
  if(!p){
    // Keep the owning empire's property recoverable if its sponsor was lost.
    e.stxColonyRefunds=e.stxColonyRefunds||[];
    e.stxColonyRefunds.push({from,materials:goods,population:Math.max(0,population)});return;
  }
  for(const [r,n] of Object.entries(goods))p.stock[r]=(p.stock[r]||0)+n;
  p.pop+=Math.max(0,population||0);
}
function stxCMCancelProject(p,reason='Destination already claimed'){
  const q=p?.expansionProject;if(!q||q.stxColonyRefunded)return false;
  q.stxColonyRefunded=true;stxActionSet(q,'CANCELLED',reason);
  const owner=q.stxColonyOwner??p.owner,id=q.stxProjectId||q.stxSupply?.projectId;
  const orders=new Set(Object.values(q.stxSupply?.orderIds||{}));
  stxCMRefund(owner,p.id,q.stxSupply?.delivered||{},0,q.stxColonyCredits||0);
  for(const order of p.orders||[])if(orders.has(order.id)||(id&&order.stxProjectId===id)){
    order.status='cancelled';order.cancelledAt=state.simTime;
  }
  for(const ship of state.ships){
    if(ship.stxCancelled||ship.stxIntercepted||ship.type==='colony')continue;
    if((id&&ship.stxProjectId===id)||orders.has(ship.orderId)){
      ship.stxCancelled=true;ship.stxColonyFreightRefunded=true;stxCMRefund(ship.owner,ship.from,ship.cargo,0,0);
    }
  }
  p.expansionProject=null;
  if(owner===0)logEvent(`COLONY MISSION CANCELLED: ${p.name} — ${reason}. Paid credits and committed supplies refunded.`,'warning');
  return true;
}
function stxCMLegacyRefund(s){
  // Older saves omitted the credit ledger. A fully recruited mission above
  // the minimum settler floor encodes its paid fee in the passenger count.
  // Below that floor only the fixed 8-credit payment can be recovered safely.
  const materials=s.stxIFColonyMission?
    {iron:24,silicates:22,titanium:8,helium:16,components:14,equipment:9,trained:s.cargo?.trained||0}:
    {components:24};
  const settlers=Number(s.cargo?.population)||0;
  const credits=s.stxIFColonyMission?8+(settlers>.004+1e-9?Math.min(.025,settlers)*200:0):0;
  return {owner:s.owner,from:s.from,materials,credits};
}
function stxCMCancelShip(s,reason='Destination already claimed'){
  if(!s||s.type!=='colony'||s.stxColonyRefunded||s.stxIntercepted)return false;
  const refund=s.stxColonyRefund||stxCMLegacyRefund(s);
  s.stxColonyRefunded=true;s.stxCancelled=true;stxActionSet(s,'CANCELLED',reason);
  const materials={...refund.materials};
  // Crew also appears in the launch recipe, so merge rather than add it twice.
  for(const [r,n] of Object.entries(s.cargo||{}))if(r!=='population')materials[r]=Math.max(materials[r]||0,n||0);
  stxCMRefund(refund.owner,refund.from,materials,s.cargo?.population||0,refund.credits);
  if(s.owner===0){
    logEvent(`COLONY MISSION CANCELLED: ${s.missionTitle||s.vesselName||'Settlement mission'} — ${reason}. Settlers and colony supplies refunded.`,'warning');
    if(typeof stxGVPulse==='function')stxGVPulse('interruption',s,'#efc572');
  }
  return true;
}
function stxCMCancelInvalid(){
  const planets=new Map(state.planets.map(p=>[p.id,p]));
  for(const p of state.planets){
    const q=p.expansionProject;if(!q)continue;
    const target=planets.get(q.targetId);
    if(!target||target.owner!==null)stxCMCancelProject(p,target?`${target.name} has been claimed`:'Destination unavailable');
  }
  for(const s of state.ships){
    if(s.type!=='colony'||s.stxCancelled||s.stxIntercepted)continue;
    const target=planets.get(s.to);
    if(!target||target.owner!==null)stxCMCancelShip(s,target?`${target.name} has been claimed`:'Destination unavailable');
  }
  state.ships=state.ships.filter(s=>!s.stxColonyRefunded&&!s.stxColonyFreightRefunded);
  for(const e of state.empires){
    if(!e.stxColonyRefunds?.length||!owned(e.id).length)continue;
    const refunds=e.stxColonyRefunds;e.stxColonyRefunds=[];
    for(const r of refunds)stxCMRefund(e.id,r.from,r.materials,r.population,0);
  }
}
const STX_CM_startExpansion=startExpansionProject;
startExpansionProject=function(e,p,target,directed=false){
  const ok=STX_CM_startExpansion(e,p,target,directed);
  if(ok&&p.expansionProject)p.expansionProject.stxColonyOwner=e.id;
  return ok;
};
const STX_CM_createShip=createShip;
createShip=function(type,from,to,owner,extra={}){
  const q=type==='colony'?from?.expansionProject:null;
  const ship=STX_CM_createShip(type,from,to,owner,extra);
  if(ship&&q)ship.stxColonyRefund={owner,from:from.id,credits:q.stxColonyCredits||0,
    materials:{...(q.stxSupply?.delivered||{})}};
  return ship;
};
const STX_CM_tickExpansion=tickExpansionProject;
tickExpansionProject=function(p,dt){
  const q=p?.expansionProject,target=q&&state.planets.find(x=>x.id===q.targetId);
  if(q&&(!target||target.owner!==null)){stxCMCancelProject(p);return}
  return STX_CM_tickExpansion(p,dt);
};
const STX_CM_arriveShip=arriveShip;
arriveShip=function(s,p){
  if(s?.type==='colony'&&(!p||p.owner!==null)){stxCMCancelShip(s);return}
  return STX_CM_arriveShip(s,p);
};
const STX_CM_tickShips=tickShips;
tickShips=function(dt){
  stxCMCancelInvalid();const result=STX_CM_tickShips(dt);stxCMCancelInvalid();return result;
};
const STX_CM_simulate=simulate;
simulate=function(dt){stxCMCancelInvalid();return STX_CM_simulate(dt)};
// A diplomatic agreement abandoning a colonial claim follows the same refund
// path instead of silently deleting the queued project.
stxRDCancelExpansion=function(owner,targetId){
  for(const p of owned(owner))if(p.expansionProject?.targetId===targetId)stxCMCancelProject(p,'Colonial claim withdrawn by agreement');
  for(const s of state.ships)if(s.owner===owner&&s.type==='colony'&&s.to===targetId)stxCMCancelShip(s,'Colonial claim withdrawn by agreement');
};
