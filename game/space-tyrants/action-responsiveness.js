/* Final validation and feedback adapters. Transactions use the common receipt
   and rollback helpers instead of maintaining separate payment/ship snapshots. */
const STX_LOCAL_RATES={iron:.018,silicates:.016,titanium:.009,helium:.012,rare:.006,components:.012,equipment:.008};
function stxActionLocalRate(p,r){return (STX_LOCAL_RATES[r]||0)/Math.sqrt(Math.max(1,owned(p.owner).length))}
const STX_ACTION_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){const result=STX_ACTION_tickPlanet(p,dt);if(p.owner!==null&&p.pop>0&&dt>0){p.stxLocalProduction={};for(const r of Object.keys(STX_LOCAL_RATES)){const rate=stxActionLocalRate(p,r);p.stxLocalProduction[r]=rate;p.stock[r]=(Number(p.stock[r])||0)+rate*dt;stxIFRecord(p,"production",r,rate*dt)}}return result};
function stxActionFreightCheck(legs,payments={}){
  if(state.ships.length+legs.length>280)return "Vessel capacity is full";
  const counts={};for(const leg of legs)counts[leg.owner]=(counts[leg.owner]||0)+1;
  for(const [id,n] of Object.entries(counts)){const owner=Number(id);if(stxIFFreightUsed(owner)+n>stxIFFreightLimit(owner))return "No freight capacity";if((empire(owner)?.credits||0)<n*.04+(payments[id]||0))return "Insufficient credits for payment and freight"}
  return "";
}
function stxActionTradePlan(seller,buyer,resource,amount,payment){
  if(!stxRTActiveEmpire(seller)||!stxRTActiveEmpire(buyer)||seller===buyer)return {error:"Trading partner is unavailable"};
  if(empiresAtWar(seller,buyer)||stxRTEmbargoed(seller,buyer))return {error:"War or embargo blocks this exchange"};
  if(!STX_IF_RESOURCES.includes(resource)||!Number.isFinite(amount)||amount<=0||!payment||!Number.isFinite(payment.credits||0)||(payment.credits||0)<0||payment.resource&&(!STX_IF_RESOURCES.includes(payment.resource)||!Number.isFinite(payment.amount)||payment.amount<=0))return {error:"Invalid trade terms"};
  const delivery=stxRTAllocateStock(seller,resource,amount),returns=payment.resource?stxRTAllocateStock(buyer,payment.resource,payment.amount):[];
  if(!delivery)return {error:`${stxRTLabel(resource)} required: ${amount}; available: ${stxRTEmpireAvailable(seller,resource).toFixed(2)}`};
  if(!returns)return {error:`Payment stock required: ${payment.amount} ${stxRTLabel(payment.resource)}`};
  if(empire(buyer).credits<(payment.credits||0))return {error:"Insufficient payment credits"};
  const dest=stxRTDestination(buyer,resource),back=stxRTDestination(seller,payment.resource||resource);
  if(!dest||!back||dest.underAttack||back.underAttack)return {error:"No safe receiving port"};
  const legs=[...delivery.map(a=>({...a,to:dest,owner:seller,resource,leg:"delivery"})),...returns.map(a=>({...a,to:back,owner:buyer,resource:payment.resource,leg:"payment"}))];
  return {legs,error:stxActionFreightCheck(legs,{[buyer]:payment.credits||0})};
}
stxRTExchange=function(seller,buyer,resource,amount,payment,extra={}){
  const plan=stxActionTradePlan(seller,buyer,resource,amount,payment);if(plan.error)return false;
  return stxActionAtomic(()=>{
    // Reserve every leg before dispatch: charter fuel cannot consume sold cargo.
    for(const leg of plan.legs)leg.source.stock[leg.resource]-=leg.amount;
    for(const leg of plan.legs){const ship=stxRTDispatchCargo(leg.source,leg.to,leg.owner,leg.resource,leg.amount,{...extra,tradeLeg:leg.leg});if(!ship)return false;stxActionSet(ship,"ACTIVE")}
    empire(buyer).credits-=payment.credits||0;empire(seller).credits+=payment.credits||0;return true;
  });
};
function stxActionResponse(q,work){
  if(!q||q.status!=="pending"||["EXECUTING","COMPLETED"].includes(stxAction(q,"response").state))return false;
  stxActionSet(q,"EXECUTING");const ok=stxActionAtomic(()=>{const result=work();return result!==false&&(q.status!=="pending"||result===true)});
  if(ok){if(q.status==="pending")stxActionSet(q,"ACTIVE");else stxActionComplete(q)}else stxActionSet(q,"BLOCKED","Commitment unavailable; no payment or cargo was taken");return !!ok;
}
function stxActionOfferCheck(offer){
  const request=state.resourceTradeEconomy?.requests.find(r=>r.id===offer?.requestId);
  if(!offer||offer.status!=="pending"||!request||request.status!=="open"||state.simTime>=offer.expiresAt||state.simTime>=request.expiresAt)return "Offer has expired or closed";
  return stxActionTradePlan(offer.from,0,offer.resource,offer.amount,offer.payment).error;
}
const STX_ACTION_acceptOffer=stxRTAcceptOffer;
stxRTAcceptOffer=function(id){const q=state.resourceTradeEconomy?.offers.find(o=>o.id===id),error=stxActionOfferCheck(q);if(error){showToast(error);return false}return stxActionResponse(q,()=>STX_ACTION_acceptOffer(id))};
function stxActionAidPlan(q){
  if(!q||!STX_RT_RESOURCES.includes(q.resource)||!Number.isFinite(q.amount)||q.amount<=0)return {error:"Invalid aid request"};
  if(!stxRTActiveEmpire(q.from)||!stxRTActiveEmpire(q.to)||empiresAtWar(q.from,q.to)||stxRTEmbargoed(q.from,q.to))return {error:"War, embargo or unavailable partner blocks aid"};
  const allocations=stxRTAllocateStock(q.to,q.resource,q.amount),target=stxRTDestination(q.from,q.resource);
  if(!allocations)return {error:`${q.amount} ${stxRTLabel(q.resource)} required; ${stxRTEmpireAvailable(q.to,q.resource).toFixed(2)} available`};
  if(!target||target.underAttack)return {error:"No safe receiving port"};
  const legs=allocations.map(a=>({...a,to:target,owner:q.to,resource:q.resource}));return {legs,error:stxActionFreightCheck(legs)};
}
stxRDSendResource=function(q){const plan=stxActionAidPlan(q);if(plan.error)return false;return stxActionAtomic(()=>{for(const a of plan.legs)a.source.stock[a.resource]-=a.amount;for(const a of plan.legs){const s=stxRTDispatchCargo(a.source,a.to,a.owner,a.resource,a.amount,{tradeKind:"diplomatic-aid",requestId:q.id});if(!s)return false;stxActionSet(s,"ACTIVE")}return true})};
function stxActionWithdrawalPlan(owner,foe){
  const enemies=owned(foe),distance=p=>Math.min(...enemies.map(q=>dist(p,q))),plan=[];
  for(const {f} of stxRDBorderFleets(owner,foe).slice(0,3)){const from=state.planets.find(p=>p.id===f.location),to=owned(owner).filter(p=>!p.underAttack&&p!==from&&(!from||distance(p)>distance(from))).sort((a,b)=>distance(b)-distance(a))[0];if(from&&to&&!state.ships.some(s=>s.fleetId===f.id))plan.push({f,to})}return plan;
}
stxRDWithdrawBorderFleets=function(owner,foe){const plan=stxActionWithdrawalPlan(owner,foe);if(!plan.length||state.ships.length+plan.length>280)return 0;return stxActionAtomic(()=>plan.every(({f,to})=>stxRDRepositionFleet(f,to,"diplomatic withdrawal"))?plan.length:false)||0};
function stxActionRequestCheck(q){
  if(!q||q.status!=="pending"||state.simTime>=q.expiresAt)return "Request has expired or closed";
  if(!stxRTActiveEmpire(q.from)||!stxRTActiveEmpire(q.to))return "Faction is unavailable";
  if(q.actionKind==="resource")return stxActionAidPlan(q).error;
  if(q.actionKind==="tribute"&&(!Number.isFinite(q.amount)||q.amount<=0||(empire(q.to)?.credits||0)<q.amount))return "Insufficient credits for tribute";
  if(q.actionKind==="tradeAccess"&&(empiresAtWar(q.from,q.to)||stxRTEmbargoed(q.from,q.to)))return "War or embargo prevents trade access";
  if(q.actionKind==="nonColonization"&&!state.planets.some(p=>p.id===q.targetId&&p.owner===null))return "That world is no longer unclaimed";
  if(q.actionKind==="withdrawal"&&!stxActionWithdrawalPlan(q.to,q.from).length)return "No border fleet can withdraw to a safer port";
  return "";
}
stxRDCanAcceptRequest=q=>!stxActionRequestCheck(q);
const STX_ACTION_applyRequest=stxRDApplyAcceptedRequest;
stxRDApplyAcceptedRequest=function(q){if(stxActionRequestCheck(q))return false;if(q.actionKind==="withdrawal"){if(!stxRDWithdrawBorderFleets(q.to,q.from))return false;stxRDAddAgreement("border-withdrawal",q.to,q.from,95);return true}return STX_ACTION_applyRequest(q)};
const STX_ACTION_respondRequest=stxRDRespondRequest;
stxRDRespondRequest=function(id,action,automatic=false){const q=state.rivalDiplomacy.requests.find(q=>q.id===id);if(!q||q.status!=="pending"||state.simTime>=q.expiresAt)return false;if(action==="accept"){const error=stxActionRequestCheck(q);if(error){if(!automatic)showToast(error);return false}}return stxActionResponse(q,()=>STX_ACTION_respondRequest(id,action,automatic))};
function stxActionProposalTerms(p,counter=false){const factor=counter?.85:1,amount=(r,n)=>r==="trained"?n*factor:Math.ceil(n*factor);return {amount:amount(p.offer?.resource,p.offer?.amount),payment:{credits:Math.ceil((p.request?.credits||0)*factor),...(p.request?.resource?{resource:p.request.resource,amount:amount(p.request.resource,p.request.amount)}:{})}}}
acceptTradeProposal=function(p,counter=false){
  const terms=stxActionProposalTerms(p,counter);if(!stxRTExchange(p.from,0,p.offer?.resource,terms.amount,terms.payment,{tradeKind:p.stxProcurement?"emergency-procurement":"treaty",stxProjectId:p.projectId}))return false;
  if(p.duration>1)state.contracts.push({id:stxRDId("contract"),title:p.title,a:p.from,b:0,aGives:{resource:p.offer.resource,amount:terms.amount},bGives:terms.payment.resource?{resource:terms.payment.resource,amount:terms.payment.amount}:null,cyclesRemaining:p.duration-1,active:true,startedAt:state.simTime,lastDelivery:state.simTime,stxGuaranteed:true});
  adjustRelation(0,p.from,.12);return true;
};
function stxActionProposalCheck(p,action="accept"){
  if(!p||p.status!=="pending"||state.simTime>=p.expiresAt)return "Transmission has expired or closed";if(action==="decline")return "";
  if(p.kind==="trade"){const t=stxActionProposalTerms(p,action==="counter");return stxActionTradePlan(p.from,0,p.offer?.resource,t.amount,t.payment).error}
  if(p.kind==="governor"){const world=state.planets.find(x=>x.id===p.planetId),cost=action==="autonomy"?Math.ceil(p.cost*.45):p.cost;if(!world||world.owner!==0)return "World is no longer under Imperial control";if(!Number.isFinite(cost)||cost<0||empire(0).credits<cost)return "Insufficient credits";if(p.project&&world.localProject)return "Existing local project must finish first";if(p.type==="rebuild"&&world.reconstruction)return "Reconstruction already underway"}
  if(p.kind==="access"&&(!stxRTActiveEmpire(p.from)||empiresAtWar(0,p.from)||stxRTEmbargoed(0,p.from)))return "War or embargo blocks access";
  if(p.kind==="migration"){const target=state.planets.find(x=>x.id===p.planetId),source=owned(p.from).sort((a,b)=>b.pop/b.capacity-a.pop/a.capacity)[0];if(!target||target.owner!==0||target.underAttack||!source||!Number.isFinite(p.amount)||p.amount<=0||p.amount>source.pop*.04||state.ships.length>=280)return "Migration cannot embark or land safely"}
  return "";
}
const STX_ACTION_respondProposal=respondProposal;
respondProposal=function(id,action){
  const q=state.proposals.find(p=>p.id===id),error=stxActionProposalCheck(q,action);if(error){showToast(error);return false}
  return stxActionResponse(q,()=>{
    if(q.kind==="migration"&&action!=="decline"){const target=state.planets.find(x=>x.id===q.planetId),source=owned(q.from).sort((a,b)=>b.pop/b.capacity-a.pop/a.capacity)[0],s=createShip("refugee",source,target,q.from,{cargo:{population:q.amount},commercial:true,crossBorder:true,tradeKind:"refugee",tradePartner:0,refugees:true});if(!s)return false;source.pop-=q.amount;empire(0).credits+=18;stxRDAddCooperation(0,q.from,"migration","Migration agreement fulfilled",8);q.status="accepted";return true}
    STX_ACTION_respondProposal(id,action);
    if(q.kind==="governor"&&q.project&&action!=="decline"&&state.planets.find(p=>p.id===q.planetId)?.localProject?.type!==q.project)return false;
    return q.status!=="pending";
  });
};
const STX_ACTION_contractTick=contractTick;
contractTick=function(){const all=state.contracts,guaranteed=all.filter(c=>c.stxGuaranteed);state.contracts=all.filter(c=>!c.stxGuaranteed);try{STX_ACTION_contractTick()}finally{state.contracts=all}
  for(const c of guaranteed){if(!c.active||state.simTime-c.lastDelivery<12)continue;if(c.cyclesRemaining<=0){c.active=false;stxActionComplete(c);continue}const ok=stxRTExchange(c.a,c.b,c.aGives.resource,c.aGives.amount,c.bGives?{resource:c.bGives.resource,amount:c.bGives.amount}:{});if(ok){c.cyclesRemaining--;c.lastDelivery=state.simTime;stxActionSet(c,"ACTIVE");if(!c.cyclesRemaining){c.active=false;stxActionComplete(c)}}else stxActionSet(c,"BLOCKED","Reciprocal shipment cannot dispatch; awaiting stock or freight")}
};
function stxActionMilitaryCheck(q){const p=state.planets.find(p=>p.id===q?.planetId);return !q||q.status!=="pending"||state.simTime>=q.expiresAt?"Request expired":!p||p.owner!==0||!p.infra.shipyard?"Owned shipyard required":""}
const STX_ACTION_militaryRequest=stxRespondMilitaryRequest;
stxRespondMilitaryRequest=function(id,action){const q=state.militaryRequests.find(q=>q.id===id);if(!q||q.status!=="pending")return false;const error=action==="approve"?stxActionMilitaryCheck(q):"";if(error){showToast(error);return false}const p=state.planets.find(p=>p.id===q.planetId),before=p?.buildQueue.length;return stxActionResponse(q,()=>{STX_ACTION_militaryRequest(id,action);return action!=="approve"||p.buildQueue.length===before+1})};
function stxActionCommandCheck(destinations,count,kind){
  if(!Number.isInteger(count)||count<=0)return "Choose a positive whole number of fleets";
  if(!destinations.length||destinations.some(d=>!d||(d.base?d.base.status!=="operational":!state.planets.includes(d.planet||d))))return "Destination is unavailable";
  if(destinations.some(d=>{const p=d.base||d.planet||d;return kind==="invade"?!empiresAtWar(0,p.owner):p.owner!==0}))return "Destination is not valid for that order";
  if(destinations.some(d=>d.facility&&d.facility.hp<=0))return "Facility is unavailable";
  const fleets=stxFCAFree(destinations).slice(0,count);if(fleets.length!==count)return `Only ${fleets.length} fleets are available`;
  if(state.ships.length+fleets.filter(f=>!stxFCATransit(f)).length>280)return "Vessel capacity is full";return "";
}
const STX_ACTION_move=stxFCAExecuteMove;
stxFCAExecuteMove=function(kind,destination,count){const d=destination?.planet||destination?.base?destination:{key:`planet:${destination?.id}`,type:"planet",id:destination?.id,name:destination?.name,planet:destination},error=stxActionCommandCheck([d],count,kind);if(error){showToast(error);return 0}return stxActionAtomic(()=>{const n=STX_ACTION_move(kind,d,count);return n===count?n:false},[...stxActionEconomyRoots(),stxFCACommitted])||0};
const STX_ACTION_invade=stxFCAExecuteInvasionOrder;
stxFCAExecuteInvasionOrder=function(destinations,allocations){const count=destinations.reduce((n,d)=>n+(allocations[d.key]||0),0);if(destinations.length>2||new Set(destinations.map(d=>d.key)).size!==destinations.length||destinations.some(d=>!Number.isInteger(allocations[d.key])||allocations[d.key]<=0))return 0;const error=stxActionCommandCheck(destinations,count,"invade");if(error){showToast(error);return 0}return stxActionAtomic(()=>{const n=STX_ACTION_invade(destinations,allocations);return n===count?n:false},[...stxActionEconomyRoots(),stxFCACommitted])||0};
const STX_ACTION_redirect=stxFCARedirectTransit;
stxFCARedirectTransit=function(f,s,...args){const ok=STX_ACTION_redirect(f,s,...args);if(ok){delete s.stxAction;delete s.stxArrivalAttemptAt;delete s.stxArrivalBlocked;stxActionSet(s,"ACTIVE")}return ok};

function stxActionWatchdog(){
  for(const p of state.planets){if(p.owner===null)continue;
    for(const d of stxSDDescriptors(p)){stxActionProgress(d);if(d.kind==="ship"&&d.q.progress>=1&&stxSDAllDelivered(d)&&!p.underAttack)stxActionFinalizeShip(p,d.q);if(d.kind==="expansion"&&stxSDProgress(d)>=1)tickExpansionProject(p,0)}
    for(const o of p.orders||[])if(o.status==="in transit"&&!state.ships.some(s=>s.orderId===o.id&&!s.stxIntercepted&&!s.stxCancelled)){o.status="waiting";stxActionSet(o,"RETRYING","Freight carrier unavailable; sourcing a replacement")}
  }
  for(const e of state.empires)for(const q of e.invasionPlans||[])if(q.status==="fleet inbound"&&!state.ships.some(s=>s.invasionPlanId===q.id&&!s.stxCancelled&&!s.stxIntercepted))q.status=state.battles.some(b=>b.planetId===q.targetId&&b.attacker===e.id)?"engaged":"awaiting reinforcements";
  for(const s of state.ships){
    if(s.stxDeepTransit)continue;
    if(s.stxStationed){if(state.simTime-(s.stxStationCheckAt??-999)<5)continue;s.stxStationCheckAt=state.simTime;const p=state.planets.find(p=>p.id===s.from),target=p&&chooseShipTarget(p,s.type);if(target&&target!==p){s.to=target.id;s.startX=s.x;s.startY=s.y;s.distance=Math.max(1,dist(s,target));s.progress=0;delete s.stxStationed;delete s.stxAction;stxActionSet(s,"ACTIVE")}continue}
    if(!state.planets.some(p=>p.id===s.to)){
      const home=owned(s.owner).filter(p=>!p.underAttack).sort((a,b)=>dist(s,a)-dist(s,b))[0];
      if(home&&Number.isFinite(s.x)&&Number.isFinite(s.y)){s.from=home.id;s.to=home.id;s.startX=s.x;s.startY=s.y;s.distance=Math.max(1,dist(s,home));s.progress=0;s.stxIFReturnCargo=true;s.retreat=true;delete s.stxAction;delete s.stxArrivalAttemptAt;stxActionSet(s,"ACTIVE");s.status="Returning from an unavailable destination"}
      else stxActionSet(s,"BLOCKED","No safe route endpoint is available");
    }else if(s.progress<1&&s.stxAction?.state!=="BLOCKED")stxActionSet(s,"ACTIVE");
  }
  for(const f of state.fleets){if(f.destroyed||f.location||f.dockedAt||f.deepSpaceBaseId||state.ships.some(s=>s.fleetId===f.id)||[...state.battles,...(state.deepSpaceBattles||[])].some(b=>[...(b.attackerFleetIds||[]),...(b.defenderFleetIds||[])].includes(f.id))||(state.deepSpaceOperations||[]).some(o=>o.active&&o.fleetId===f.id))continue;
    stxActionSet(f,"BLOCKED","Fleet has no active route or port");if(Number.isFinite(f.lastX)&&Number.isFinite(f.lastY)&&owned(f.owner).length){stxDSReturnFleet(f,{x:f.lastX,y:f.lastY},f.owner,"Recovering interrupted fleet route");if(state.ships.some(s=>s.fleetId===f.id))stxActionSet(f,"ACTIVE")}
  }
}
const STX_ACTION_simulate=simulate;let stxActionLastAudit=-999,stxActionLastUI=-999;
simulate=function(dt){const result=STX_ACTION_simulate(dt);if(state.simTime<stxActionLastAudit){stxActionLastAudit=-999;stxActionLastUI=-999}if(state.simTime-stxActionLastAudit>=2){stxActionLastAudit=state.simTime;stxActionWatchdog();stxConflictTick()}if(state.simTime-stxActionLastUI>=.5){stxActionLastUI=state.simTime;if(!$("hubModal")?.hidden)renderTransmissions()}return result};
function stxActionBattleCard(b){
  const incoming=state.ships.filter(s=>(b.baseId?s.deepBaseId===b.baseId:s.to===b.planetId)&&[b.attacker,b.defender].includes(s.owner)&&["fleet","patrol"].includes(s.type)&&!s.retreat);
  return `<div class="project-row battle stx-live-battle"><strong>Battle in progress</strong>${["attacker","defender"].map(side=>`<div><span>${stxRDEscape(empire(b[side])?.name)} · ${Number(b[side+"Strength"]).toFixed(1)} strength</span><meter min="0" max="${Math.max(1,b[side+"Initial"])}" value="${Math.max(0,b[side+"Strength"])}" style="width:100%"></meter></div>`).join("")}${incoming.map(s=>`<div>${stxRDEscape(s.vesselName)} · reinforcing · ETA ${fmtEta(shipEta(s))}</div>`).join("")}</div>`;
}
const STX_ACTION_renderPlanet=renderPlanet;
renderPlanet=function(...args){const result=STX_ACTION_renderPlanet(...args),p=state.selected;if(p&&p.owner===0){const panel=$("planetBody")||$("planetPanel");if(panel&&!panel.querySelector(".stx-local-economy"))panel.insertAdjacentHTML("beforeend",`<div class="project-row stx-local-economy"><strong>Local recovery per minute</strong><small>${Object.keys(STX_LOCAL_RATES).map(r=>`${stxSDResourceLabel(r)} ${(stxActionLocalRate(p,r)*60).toFixed(2)}`).join(" · ")}</small></div>`)}return result};
function stxActionButtonCheck(b){const d=b.dataset;if(d.stxTradeAccept)return stxActionOfferCheck(state.resourceTradeEconomy?.offers.find(o=>o.id===d.stxTradeAccept));if(d.rdRequest&&d.rdAction==="accept")return stxActionRequestCheck(state.rivalDiplomacy.requests.find(q=>q.id===d.rdRequest));if(d.proposal)return stxActionProposalCheck(state.proposals.find(p=>p.id===d.proposal),d.response);if(d.militaryId&&d.militaryResponse==="approve")return stxActionMilitaryCheck(state.militaryRequests.find(q=>q.id===d.militaryId));return null}
let stxTransmissionPointer=false;
const STX_ACTION_transmissions=renderTransmissions;
renderTransmissions=function(){
  if(stxTransmissionPointer)return;STX_ACTION_transmissions();const box=$("transmissionList");if(!box)return;
  for(const q of state.rivalDiplomacy?.incidents||[])if(q.kind==="standoff"&&q.status==="pending"&&q.to===0)box.insertAdjacentHTML("beforeend",`<article class="transmission-card urgent"><strong>${stxRDEscape(q.title)}</strong><p>Weapons are silent. The visiting fleet is returning to safety.</p><div class="choice-row">${[["withdraw","Confirm withdrawal"],["hold","Maintain claim"],["escalate","Threaten escalation"]].map(([action,label])=>`<button class="choice-btn" data-stx-incident="${q.id}" data-stx-incident-action="${action}">${label}</button>`).join("")}</div></article>`);
  box.querySelectorAll("[data-stx-incident]").forEach(b=>b.onclick=()=>stxConflictRespondIncident(b.dataset.stxIncident,b.dataset.stxIncidentAction));
  box.querySelectorAll("button").forEach(b=>{const error=stxActionButtonCheck(b);if(error!==null){b.disabled=!!error;b.title=error||"";if(error)b.insertAdjacentHTML("afterend",`<small class="subtle">${stxRDEscape(error)}</small>`)}const click=b.onclick;if(!click)return;b.onclick=event=>{if(b.disabled||b.stxProcessing)return;const current=stxActionButtonCheck(b);if(current){showToast(current);renderTransmissions();return}b.stxProcessing=true;b.disabled=true;b.textContent="Processing…";stxTransmissionPointer=false;try{return click.call(b,event)}finally{renderTransmissions()}}});
};
document.addEventListener("pointerdown",e=>{if(e.target?.closest?.("#transmissionList"))stxTransmissionPointer=true});
for(const event of ["pointerup","pointercancel"])document.addEventListener(event,()=>{if(stxTransmissionPointer)setTimeout(()=>{stxTransmissionPointer=false;renderTransmissions()},0)});

function stxActionProjectTicker(previous,key){return function(p,...args){const q=p?.[key];if(q)stxAction(q,key);const result=previous(p,...args);if(q&&p[key]!==q)stxActionComplete(q);return result}}
tickLocalProject=stxActionProjectTicker(tickLocalProject,"localProject");
tickOrbitalProject=stxActionProjectTicker(tickOrbitalProject,"orbitalProject");
tickReconstruction=stxActionProjectTicker(tickReconstruction,"reconstruction");
stxTickScanProject=stxActionProjectTicker(stxTickScanProject,"scanProject");
stxTickTradeStationProject=stxActionProjectTicker(stxTickTradeStationProject,"tradeStationProject");
const STX_ACTION_completePhysical=stxOLCompleteProject;
stxOLCompleteProject=function(p,q){if(q.phase==="operations"||stxAction(q,"physical project").state==="COMPLETED")return true;if(["module","repair"].includes(q.kind)&&!stxOLFacilityAt(q.option.facilityId)?.f){stxActionSet(q,"BLOCKED","Target facility is unavailable");return false}const ok=stxActionAtomic(()=>{STX_ACTION_completePhysical(p,q);return q.phase==="operations"},[p,empire(p.owner),q,state.effects,state.events,state.news]);if(ok)stxActionComplete(q);else stxActionSet(q,"RETRYING","Commissioning blocked; retrying");return ok};
const STX_ACTION_upgrade=stxIFCompleteUpgrade;
stxIFCompleteUpgrade=function(base){const q=base.upgradeProject;if(q)stxAction(q,"station upgrade");const result=STX_ACTION_upgrade(base);if(q&&base.upgradeProject!==q)stxActionComplete(q);return result};
const STX_ACTION_cancelExpansion=stxRDCancelExpansion;
stxRDCancelExpansion=function(owner,targetId){for(const p of owned(owner)){const q=p.expansionProject;if(q?.targetId===targetId&&q.stxAction?.state!=="COMPLETED"){for(const [r,a] of Object.entries(q.stxSupply?.delivered||{}))p.stock[r]=(p.stock[r]||0)+a;stxActionComplete(q)}}return STX_ACTION_cancelExpansion(owner,targetId)};
const STX_ACTION_tickShips=tickShips;
tickShips=function(dt){const result=STX_ACTION_tickShips(dt);for(const s of state.ships){const f=fleetRecord(s.fleetId);if(f&&Number.isFinite(s.x)&&Number.isFinite(s.y)){f.lastX=s.x;f.lastY=s.y}}return result};
globalThis.SpaceTyrantsResourceTrade.acceptOffer=stxRTAcceptOffer;
globalThis.SpaceTyrantsResponsiveness={watchdog:stxActionWatchdog,localRate:stxActionLocalRate,tradePlan:stxActionTradePlan,offerCheck:stxActionOfferCheck,requestCheck:stxActionRequestCheck,proposalCheck:stxActionProposalCheck,commandCheck:stxActionCommandCheck};
