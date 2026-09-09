/* One action router for the legacy, Admiralty, diplomatic and resource-offer
   transmission cards. Queues retain identifiers, never stale request objects. */
let stxTXPointerActive=false,stxTXRenderPending=false,stxTXProcessing=false;
// The existing integrated-economy startLocalProject charges this authorization
// fee in addition to the transmission's grant. Readiness must cover both.
const STX_TX_PROJECT_FEE=3;
function stxTXLists(){return {
  proposal:state.proposals||[],military:state.militaryRequests||[],
  diplomacy:state.rivalDiplomacy?.requests||[],trade:state.resourceTradeEconomy?.offers||[],
  peace:state.deepSpacePeaceOffers||[]
}}
function stxTXFind(type,id){return stxTXLists()[type]?.find(q=>q.id===id)}
function stxTXQueue(){const e=empire(0);return e.stxTransmissionQueue||(e.stxTransmissionQueue=[])}
function stxTXHistory(type,q,status,reason=''){
  const e=empire(0);e.stxTransmissionHistory=e.stxTransmissionHistory||[];
  const previous=e.stxTransmissionHistory.find(h=>h.type===type&&h.id===q.id&&h.status===status);
  if(previous)return;
  e.stxTransmissionHistory.unshift({type,id:q.id,title:q.title||`${q.amount||''} ${q.resource||'Peace agreement'}`,
    status,reason,time:state.simTime});e.stxTransmissionHistory=e.stxTransmissionHistory.slice(0,40);
}
function stxTXTradePlan(seller,buyer,resource,amount,payment,preferred=null){
  const plan=stxActionTradePlan(seller,buyer,resource,amount,payment);
  if(plan.error)return {ok:false,reason:plan.error};
  const delivery=plan.legs.filter(leg=>leg.leg==='delivery'),returns=plan.legs.filter(leg=>leg.leg==='payment');
  const target=preferred?.owner===buyer?preferred:delivery[0]?.to,back=returns[0]?.to||stxRTDestination(seller,resource);
  if(!target||target.underAttack||!back)return {ok:false,reason:'No safe receiving port'};
  return {ok:true,reason:'Ready to accept',delivery,returns,target,back,payment,resource,seller,buyer};
}
function stxTXExecuteTrade(plan,extra={}){
  if(!plan.ok)return false;
  const {delivery,returns,target,back,payment,resource,seller,buyer}=plan;
  const {deliveryTradeKind,paymentTradeKind,...shipmentExtra}=extra,rng=state.rngState;
  // Reuse the shared action transaction so exceptions also restore object
  // identities, freight fees, fuel, receipts and every committed cargo leg.
  const ok=stxActionAtomic(()=>{
    for(const a of delivery)a.source.stock[resource]-=a.amount;
    for(const a of returns)a.source.stock[payment.resource]-=a.amount;
    for(const [allocations,to,owner,r,leg] of [[delivery,target,seller,resource,'delivery'],[returns,back,buyer,payment.resource,'payment']]){
      for(const a of allocations){
        const ship=stxRTDispatchCargo(a.source,to,owner,r,a.amount,{
          ...shipmentExtra,tradeLeg:leg,
          tradeKind:(leg==='delivery'?deliveryTradeKind:paymentTradeKind)||shipmentExtra.tradeKind||'requested-trade'});
        if(!ship)return false;stxActionSet(ship,'ACTIVE');
      }
    }
    empire(buyer).credits-=payment.credits||0;empire(seller).credits+=payment.credits||0;return true;
  });
  if(!ok)state.rngState=rng;return ok;
}
// Resource offers already track contacts, accepted amounts and fulfillment.
// Keep that lifecycle, but make its exchange atomic including fuel and fees.
stxRTExchange=function(seller,buyer,resource,amount,payment,extra={}){
  return stxTXExecuteTrade(stxTXTradePlan(seller,buyer,resource,amount,payment),extra);
};
const STX_TX_acceptTrade=acceptTradeProposal;
acceptTradeProposal=function(q,counter=false){
  const factor=counter?.85:1,quantity=(r,n)=>r==='trained'?n*factor:Math.ceil(n*factor);
  const payment={...q.request};
  if(payment.credits)payment.credits=Math.ceil(payment.credits*factor);
  if(payment.resource)payment.amount=quantity(payment.resource,payment.amount);
  if(q.duration>1)return STX_TX_acceptTrade({...q,request:payment,
    offer:{...q.offer,amount:quantity(q.offer.resource,q.offer.amount)}},false);
  const target=q.projectPlanetId&&state.planets.find(p=>p.id===q.projectPlanetId&&p.owner===0);
  const ok=stxTXExecuteTrade(stxTXTradePlan(q.from,0,q.offer?.resource,quantity(q.offer?.resource,q.offer?.amount),payment,target),
    {proposalId:q.id,deliveryTradeKind:q.stxProcurement?'emergency-procurement':'treaty',paymentTradeKind:q.stxProcurement?'emergency-payment':'treaty'});
  if(ok){adjustRelation(0,q.from,q.stxProcurement?.08:.12);galacticNews('ACCEPTED EXCHANGE DISPATCHED',`${q.title} was accepted and its physical cargo is in transit.`,'trade',target?.id)}
  return ok;
};
function stxTXEligibility(type,q,action='accept'){
  const no=(reason,terminal=false)=>({ok:false,reason,terminal});
  if(!q||q.status!=='pending')return no('Request is no longer pending',true);
  if(Number.isFinite(q.expiresAt)&&q.expiresAt<=state.simTime)return no('Offer expired',true);
  if(type==='proposal'&&q.to!=null&&q.to!==0&&q.empireId!==0)return no('This request is not addressed to the Mandate',true);
  if(type==='diplomacy'&&q.to!==0)return no('This request is not addressed to the Mandate',true);
  if((type==='proposal'||type==='diplomacy'||type==='trade')&&q.from>0&&!owned(q.from).length)return no('Sender is no longer active',true);
  if(type==='proposal'&&q.kind!=='trade'){
    const error=stxActionProposalCheck(q,action);if(error)return no(error);
  }
  const p=state.planets.find(p=>p.id===q.planetId);
  if(type==='military'||type==='proposal'&&q.kind==='governor'){
    if(!p||p.owner!==0)return no('The requesting world is no longer controlled',true);
    if(p.underAttack)return no('The world is under attack');
    const cost=type==='military'?Math.max(0,Math.round(q.creditCost||0)):action==='autonomy'?Math.ceil((q.cost||0)*.45):(q.cost||0);
    const startsProject=type==='military'?q.type==='capital':!!q.project,total=cost+(startsProject?STX_TX_PROJECT_FEE:0);
    if(empire(0).credits<total)return no(`Need ${total} credits${startsProject?' including project authorization':''}; treasury ${Math.floor(empire(0).credits)}`);
    if((q.type==='capital'||q.project)&&p.localProject)return no('Waiting for the current local project to finish');
    if(type==='military'&&q.type!=='capital'&&p.infra.shipyard<=0)return no('An operational shipyard is required');
  }else if(type==='trade'||type==='proposal'&&q.kind==='trade'){
    if(type==='trade'){
      const request=state.resourceTradeEconomy.requests.find(r=>r.id===q.requestId);
      if(!request||request.status!=='open'||request.expiresAt<=state.simTime)return no('Trade request is closed',true);
    }
    if(empiresAtWar(0,q.from)||stxRTEmbargoed(0,q.from))return no('War or embargo invalidated the offer',true);
    const factor=action==='counter'?.85:1,pay={...(type==='trade'?q.payment:q.request)},offer=type==='trade'?q:q.offer;
    if(pay.credits)pay.credits=Math.ceil(pay.credits*factor);
    if(pay.resource)pay.amount=pay.resource==='trained'?pay.amount*factor:Math.ceil(pay.amount*factor);
    return stxTXTradePlan(q.from,0,offer.resource,offer.resource==='trained'?offer.amount*factor:Math.ceil(offer.amount*factor),pay);
  }else if(type==='proposal'&&q.kind==='migration'){
    const error=stxActionProposalCheck(q,action);if(error)return no(error);
  }else if(type==='diplomacy'){
    const error=stxActionRequestCheck(q);if(error)return no(error);
  }else if(type==='peace'&&!state.wars.some(w=>w.id===q.warId&&w.active))return no('War is already over',true);
  return {ok:true,reason:'Ready to accept',terminal:false};
}
stxRespondMilitaryRequest=function(id,action){
  const q=stxTXFind('military',id);if(!q||q.status!=='pending')return false;
  if(action==='decline'){q.status='declined';q.respondedAt=state.simTime;stxActionComplete(q);return true}
  if(!['approve','accept'].includes(action))return false;
  const check=stxTXEligibility('military',q);if(!check.ok){showToast(check.reason);return false}
  const p=state.planets.find(p=>p.id===q.planetId),cost=Math.max(0,Math.round(q.creditCost||0));
  return stxActionResponse(q,()=>{
    const ok=q.type==='capital'?startLocalProject(p,q.capitalKind==='shipyard'?'shipyard':'defense','Approved local capital request'):
      stxQueueFleetCommission(p,q.type,'Approved Admiralty request');
    if(!ok)return false;
    empire(0).credits-=cost;q.status='accepted';q.acceptedAt=state.simTime;q.respondedAt=state.simTime;
    if(q.type!=='capital')setModifier(empire(0),'shipbuilding',1.22,70);
    return true;
  });
};
const STX_TX_respondProposal=respondProposal;
respondProposal=function(id,action){
  const q=stxTXFind('proposal',id);if(!q||q.status!=='pending')return false;
  if(action==='accept')action=q.kind==='governor'?'approve':'accept';
  if(!['accept','approve','counter','autonomy','decline'].includes(action))return false;
  if(action!=='decline'){
    const check=stxTXEligibility('proposal',q,action);if(!check.ok){showToast(check.reason);return false}
  }
  if(q.kind==='governor'&&action!=='decline'){
    const done=stxActionResponse(q,()=>{
      const p=state.planets.find(p=>p.id===q.planetId),cost=action==='autonomy'?Math.ceil((q.cost||0)*.45):(q.cost||0);
      if(q.project&&!startLocalProject(p,q.project,action==='autonomy'?'Local autonomy':'Imperial approval'))return false;
      if(q.type==='rebuild')beginReconstruction(p);
      if(q.type==='migration')setModifier(empire(0),'migration',2.2,55);
      if(!q.project&&action==='autonomy')addOrder(p,'governor','components',Math.max(20,q.cost||0),3,q.title);
      empire(0).credits-=cost;p.unrest=Math.max(0,p.unrest-.06);q.status='accepted';return true;
    });if(!done)return false;
  }else STX_TX_respondProposal(id,action);
  if(q.status==='accepted'){q.acceptedAt=state.simTime;q.respondedAt=state.simTime}
  return q.status==='accepted'||q.status==='declined';
};
function stxTXRefresh(){updateBadges();renderTransmissions();if(!$('rivalsModal')?.hidden)renderRivals();updateHud(true)}
function stxTXAct(type,id,action='accept',automatic=false){
  const actions={proposal:['accept','approve','counter','autonomy','decline'],military:['accept','approve','decline'],
    diplomacy:['accept','defer','decline','refuse'],trade:['accept','decline'],peace:['accept','decline']};
  if(!actions[type]?.includes(action))return false;
  const q=stxTXFind(type,id);if(!q||q.status!=='pending')return false;
  const accepts=['accept','approve','counter','autonomy'].includes(action);
  if(accepts){const check=stxTXEligibility(type,q,action);if(!check.ok){if(!automatic)showToast(check.reason);return false}}
  if(type==='proposal')respondProposal(id,action);
  else if(type==='military')stxRespondMilitaryRequest(id,action);
  else if(type==='diplomacy')stxRDRespondRequest(id,action==='decline'?'refuse':action,automatic);
  else if(type==='trade'){if(accepts)stxRTAcceptOffer(id);else if(action==='decline')stxRTDeclineOffer(id)}
  else if(type==='peace'){
    if(accepts)stxDSAcceptPeace(q);
    else if(action==='decline'){q.status='declined';const w=state.wars.find(w=>w.id===q.warId);if(w){w.stxLimitedWar=false;if(w.warGoal)w.warGoal.status='escalated'}}
  }
  const finished=q.status!=='pending';
  if(finished){
    q.respondedAt=state.simTime;if(q.status==='accepted')q.acceptedAt=state.simTime;
    stxTXHistory(type,q,q.status);empire(0).stxTransmissionQueue=stxTXQueue().filter(x=>x.type!==type||x.id!==id);
    if(!automatic){showToast(`${q.title||'Transmission'}: ${q.status}`);stxTXRefresh();saveGame(false)}
  }else if(!automatic&&action!=='defer')showToast('Request remains pending; its commitment could not be completed yet');
  return finished;
}
function stxTXEnqueue(type,id,action='accept'){
  const q=stxTXFind(type,id),check=stxTXEligibility(type,q,action);
  if(!q||check.terminal)return false;
  const queue=stxTXQueue();if(queue.some(x=>x.type===type&&x.id===id))return true;
  if(queue.length>=40){showToast('Acceptance queue is full');return false}
  queue.push({type,id,action,queuedAt:state.simTime});stxTXHistory(type,q,'queued','Accept when affordable, while this offer remains valid');
  stxTXProcessQueue();stxTXRefresh();saveGame(false);return true;
}
function stxTXProcessQueue(){
  const e=empire(0);if(stxTXProcessing||!e?.stxTransmissionQueue?.length)return;
  stxTXProcessing=true;let changed=false;
  try{
    for(const entry of [...e.stxTransmissionQueue]){
      const q=stxTXFind(entry.type,entry.id),check=stxTXEligibility(entry.type,q,entry.action);
      if(check.terminal){
        const expired=['expired','ignored'].includes(q?.status)||q?.expiresAt<=state.simTime;
        stxTXHistory(entry.type,q||{id:entry.id,title:'Transmission'},q?.status==='accepted'?'accepted':expired?'expired':'cancelled',expired?'Offer expired':check.reason);
        e.stxTransmissionQueue=e.stxTransmissionQueue.filter(x=>x!==entry);changed=true;
      }else if(check.ok&&stxTXAct(entry.type,entry.id,entry.action,true))changed=true;
    }
  }finally{stxTXProcessing=false}
  if(changed){stxTXRefresh();saveGame(false)}
}
function stxTXCancelQueue(type,id){
  const q=stxTXFind(type,id);empire(0).stxTransmissionQueue=stxTXQueue().filter(x=>x.type!==type||x.id!==id);
  if(q)stxTXHistory(type,q,'unqueued','Automatic acceptance cancelled; request remains pending');
  stxTXRefresh();saveGame(false);
}
const STX_TX_simulate=simulate;
simulate=function(dt){const result=STX_TX_simulate(dt);stxTXProcessQueue();return result};

function stxTXButtonIdentity(button){
  const d=button?.dataset||{};
  if(d.txType)return {type:d.txType,id:d.txId,action:d.txAction};
  if(d.proposal)return {type:'proposal',id:d.proposal,action:d.response};
  if(d.militaryId)return {type:'military',id:d.militaryId,action:d.militaryResponse};
  if(d.rdRequest)return {type:'diplomacy',id:d.rdRequest,action:d.rdAction};
  if(d.stxTradeAccept)return {type:'trade',id:d.stxTradeAccept,action:'accept'};
  if(d.stxTradeDecline)return {type:'trade',id:d.stxTradeDecline,action:'decline'};
  if(d.dsPeace)return {type:'peace',id:d.dsPeace,action:d.dsPeaceAction==='accept'?'accept':'decline'};
  return null;
}
function stxTXActionMarkup(type,q){
  const escape=stxRTEscape,check=stxTXEligibility(type,q),queued=(empire(0).stxTransmissionQueue||[]).some(x=>x.type===type&&x.id===q.id);
  const button=(action,label,disabled=false,primary=false)=>`<button class="choice-btn ${primary?'primary-choice':''}" data-tx-type="${type}" data-tx-id="${escape(q.id)}" data-tx-action="${action}" ${disabled?'disabled':''}>${label}</button>`;
  const normal=type==='proposal'&&q.kind==='governor'?'approve':'accept';
  let html=button(normal,'Accept',!check.ok,true)+button(queued?'unqueue':'queue',queued?'Cancel queued acceptance':'Queue acceptance',check.terminal);
  if(type==='proposal'&&q.kind==='trade')html+=button('counter','Counter at 85%',!stxTXEligibility(type,q,'counter').ok);
  if(type==='proposal'&&q.kind==='governor')html+=button('autonomy','Local autonomy',!stxTXEligibility(type,q,'autonomy').ok);
  if(type==='diplomacy')html+=button('defer','Defer',q.deferred||q.kind==='ultimatum');
  html+=button('decline',type==='peace'?'Continue war':'Decline');
  return html+`<div class="subtle stx-tx-status" style="flex-basis:100%" role="status">${queued?'QUEUED · Auto-accept before this offer expires. ':''}${escape(check.reason)}</div>`;
}
const STX_TX_renderTransmissions=renderTransmissions;
renderTransmissions=function(){
  if(stxTXPointerActive){stxTXRenderPending=true;return}
  STX_TX_renderTransmissions();const box=$('transmissionList');if(!box)return;
  const seen=new Set();
  for(const card of box.querySelectorAll('article.transmission-card')){
    const button=card.querySelector('[data-proposal],[data-military-id],[data-rd-request],[data-stx-trade-accept],[data-ds-peace]');
    const entry=stxTXButtonIdentity(button);if(!entry)continue;
    const key=`${entry.type}:${entry.id}`;
    if(seen.has(key)){card.remove();continue}seen.add(key);
    const q=stxTXFind(entry.type,entry.id),row=card.querySelector('.choice-row');
    if(q&&row)row.innerHTML=stxTXActionMarkup(entry.type,q);
  }
  const history=empire(0)?.stxTransmissionHistory||[];
  if(history.length)box.insertAdjacentHTML('beforeend',`<section class="stx-tx-history"><div class="section-label">TRANSMISSION HISTORY</div>${history.slice(0,12).map(h=>`<div class="order-row"><span>${stxRTEscape(h.title)}<small style="display:block">${stxRTEscape(h.reason)}</small></span><b>${stxRTEscape(h.status.toUpperCase())}</b></div>`).join('')}</section>`);
};
const stxTXBox=$('transmissionList');
if(stxTXBox){
  stxTXBox.addEventListener('pointerdown',()=>{stxTXPointerActive=true},{capture:true});
  // Delegation survives every subsequent innerHTML replacement in older UI
  // layers. Exact action names avoid treating an Accept click as a decline.
  stxTXBox.addEventListener('click',event=>{
    const button=event.target.closest?.('button'),entry=stxTXButtonIdentity(button);
    if(!entry||button.disabled)return;
    event.preventDefault();event.stopImmediatePropagation();stxTXPointerActive=false;stxTransmissionPointer=false;
    if(entry.action==='queue')stxTXEnqueue(entry.type,entry.id);
    else if(entry.action==='unqueue')stxTXCancelQueue(entry.type,entry.id);
    else stxTXAct(entry.type,entry.id,entry.action);
  },{capture:true});
}
function stxTXReleasePointer(){
  stxTXPointerActive=false;stxTransmissionPointer=false;
  if(stxTXRenderPending){stxTXRenderPending=false;setTimeout(()=>renderTransmissions(),0)}
}
window.addEventListener('pointerup',stxTXReleasePointer);
window.addEventListener('pointercancel',stxTXReleasePointer);
window.addEventListener('blur',stxTXReleasePointer);
