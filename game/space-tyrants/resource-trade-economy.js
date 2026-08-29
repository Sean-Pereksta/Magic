/* Space Tyrants — visible economic allocation and player-requested resource
   trade. This final economy layer uses real faction stockpiles, reciprocal
   cargo, credits, and the existing Transmissions screen. */

const STX_RT_VERSION=1;
const STX_RT_RESOURCES=["components","equipment","iron","titanium","helium","rare","silicates"];
const STX_RT_RAW=["iron","titanium","rare","silicates","helium"];
const STX_RT_VALUES={components:1.35,equipment:1.65,iron:.72,titanium:1.15,helium:1.05,rare:1.4,silicates:.82};
const STX_RT_FOCUSES={
  mining:{label:"Mining Focus",description:"Push labor and capital toward extraction.",mining:1.55,components:.55,equipment:.55,summary:"+55% mining · reduced Components and Equipment"},
  balanced:{label:"Balanced",description:"Keep extraction and both manufacturing lines operating together.",mining:1,components:1,equipment:1,summary:"No major production penalty"},
  components:{label:"Component Industry",description:"Concentrate factories on hull sections, modules, and construction assemblies.",mining:.72,components:1.8,equipment:.46,summary:"High Components · lower mining and Equipment"},
  equipment:{label:"Equipment Industry",description:"Concentrate factories on weapons, machinery, and advanced equipment.",mining:.74,components:.5,equipment:2.05,summary:"High Equipment · lower mining and Components"}
};

function stxRTId(prefix){return`${prefix}${Math.floor(random()*1e9)}`}
function stxRTEscape(value){return String(value??"").replace(/[&<>\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[c])}
function stxRTLabel(resource){return({components:"Components",equipment:"Equipment",iron:"Iron",titanium:"Titanium",helium:"Helium",rare:"Rare Materials",silicates:"Silicates"})[resource]||RESOURCE_LABEL?.[resource]||resource}
function stxRTFmt(amount){return Math.abs(amount)<1?Number(amount).toFixed(3):String(Math.round(amount))}
function stxRTReserve(resource){return resource==="components"?32:resource==="equipment"?18:42}
function stxRTActiveEmpire(id){return !!empire(id)&&owned(id).length>0&&!empire(id).stxEliminated}

function stxRTDefaultState(){return{version:STX_RT_VERSION,requests:[],offers:[],aiTrades:[],nextRequestAt:0,nextAITradeAt:state.simTime+35,lastTickAt:state.simTime}}
function stxRTEnsurePlanet(p){
  if(!p||p.owner===null)return p;
  if(!STX_RT_FOCUSES[p.stxEconomicFocus]){
    const doctrine=empire(p.owner)?.doctrine;
    p.stxEconomicFocus=p.owner===0?"balanced":doctrine==="industry"?(p.stxManufacturingFocus==="equipment"?"equipment":"components"):doctrine==="commerce"?"equipment":doctrine==="expansion"?"mining":"balanced";
  }
  return p;
}
function stxRTEnsureState(reset=false){
  if(!state.empires?.length)return null;
  const saved=!reset&&empire(0)?.stxResourceTradeEconomy;
  if(reset||!state.resourceTradeEconomy||state.resourceTradeEconomy.version!==STX_RT_VERSION)state.resourceTradeEconomy=saved?.version===STX_RT_VERSION?saved:stxRTDefaultState();
  const s=state.resourceTradeEconomy;s.requests=Array.isArray(s.requests)?s.requests:[];s.offers=Array.isArray(s.offers)?s.offers:[];s.aiTrades=Array.isArray(s.aiTrades)?s.aiTrades:[];s.nextRequestAt=Number.isFinite(s.nextRequestAt)?s.nextRequestAt:0;s.nextAITradeAt=Number.isFinite(s.nextAITradeAt)?s.nextAITradeAt:state.simTime+35;s.lastTickAt=Number.isFinite(s.lastTickAt)?s.lastTickAt:state.simTime;
  state.planets.forEach(stxRTEnsurePlanet);empire(0).stxResourceTradeEconomy=s;return s;
}
function stxRTPersist(){if(state.empires?.length&&state.resourceTradeEconomy)empire(0).stxResourceTradeEconomy=state.resourceTradeEconomy}

const STX_RT_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_RT_generateGalaxy();state.resourceTradeEconomy=null;stxRTEnsureState(true)};
const STX_RT_loadGame=loadGame;
loadGame=function(){const ok=STX_RT_loadGame();if(ok){state.resourceTradeEconomy=empire(0)?.stxResourceTradeEconomy||null;stxRTEnsureState(false)}return ok};
const STX_RT_saveGame=saveGame;
saveGame=function(notify=true){stxRTPersist();return STX_RT_saveGame(notify)};

function stxRTSetFocus(planetOrId,focus){
  const p=typeof planetOrId==="string"?state.planets.find(x=>x.id===planetOrId):planetOrId;if(!p||p.owner!==0||!STX_RT_FOCUSES[focus])return false;
  p.stxEconomicFocus=focus;p.stxManufacturingFocus=focus==="components"?"components":focus==="equipment"?"equipment":p.stxManufacturingFocus;
  p.stxFocusChangedAt=state.simTime;p.mandateGlow=1;logEvent(`${p.name} adopted ${STX_RT_FOCUSES[focus].label}. ${STX_RT_FOCUSES[focus].summary}.`,"good");showToast(`${p.name} · ${STX_RT_FOCUSES[focus].label}`);renderPlanet();return true;
}
function stxRTApplyFactoryAllocation(p,dt,profile,before){
  if(!p.infra?.factory||dt<=0)return;const flow=Math.max(0,p.infra.factory*Math.max(0,p.factoryEfficiency||0)*dt),compBase=flow*.055,equipBase=flow*.018;
  if(profile.components>1)p.stock.components=(p.stock.components||0)+compBase*(profile.components-1);
  else if((p.stock.components||0)>before.components)p.stock.components-=Math.min((p.stock.components||0)-before.components,compBase*(1-profile.components));
  if(profile.equipment>1)p.stock.equipment=(p.stock.equipment||0)+equipBase*(profile.equipment-1);
  else if((p.stock.equipment||0)>before.equipment)p.stock.equipment-=Math.min((p.stock.equipment||0)-before.equipment,equipBase*(1-profile.equipment));
  p.stxAllocationOutput={focus:p.stxEconomicFocus,mining:profile.mining,components:profile.components,equipment:profile.equipment,updatedAt:state.simTime};
}
const STX_RT_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){
  if(!p||p.owner===null)return STX_RT_tickPlanet(p,dt);stxRTEnsurePlanet(p);const profile=STX_RT_FOCUSES[p.stxEconomicFocus]||STX_RT_FOCUSES.balanced,before={components:Number(p.stock.components)||0,equipment:Number(p.stock.equipment)||0},yieldMap=p.stxResourceYield,original=yieldMap?{...yieldMap}:null;
  if(yieldMap)STX_RT_RAW.forEach(r=>yieldMap[r]=(Number(yieldMap[r])||1)*profile.mining);
  try{STX_RT_tickPlanet(p,dt)}finally{if(original)Object.assign(yieldMap,original)}
  stxRTApplyFactoryAllocation(p,dt,profile,before);
};

function stxRTDiversifyShipNeed(q){
  if(!q||q.stxStrategicNeed)return q;const military=q.type==="fleet"||q.type==="patrol",scale=q.type==="fleet"?1:q.type==="patrol"?.65:.34;
  q.need=q.need||{};q.need.components=Math.max(Number(q.need.components)||0,Math.ceil(36*scale));q.need.equipment=Math.max(Number(q.need.equipment)||0,Math.ceil((military?18:10)*scale));q.need.titanium=Math.max(Number(q.need.titanium)||0,Math.ceil(15*scale));q.need.helium=Math.max(Number(q.need.helium)||0,Math.ceil(18*scale));q.stxStrategicNeed=true;return q;
}
const STX_RT_tickBuildQueue=tickBuildQueue;
tickBuildQueue=function(p,dt){if(p?.buildQueue?.[0])stxRTDiversifyShipNeed(p.buildQueue[0]);const result=STX_RT_tickBuildQueue(p,dt);if(p?.buildQueue?.[0])stxRTDiversifyShipNeed(p.buildQueue[0]);return result};

function stxRTEmpireAvailable(owner,resource){return owned(owner).reduce((sum,p)=>sum+Math.max(0,(Number(p.stock?.[resource])||0)-stxRTReserve(resource)),0)}
function stxRTSource(owner,resource,amount=0){return owned(owner).filter(p=>!p.underAttack&&(p.stock?.[resource]||0)>=stxRTReserve(resource)+Math.max(0,amount)).sort((a,b)=>(b.stock[resource]||0)-(a.stock[resource]||0))[0]||null}
function stxRTAllocateStock(owner,resource,amount){const sources=owned(owner).filter(p=>!p.underAttack&&(p.stock?.[resource]||0)>stxRTReserve(resource)).sort((a,b)=>(b.stock[resource]||0)-(a.stock[resource]||0)),allocations=[];let remaining=amount;for(const source of sources){if(remaining<=.01)break;const take=Math.min(remaining,Math.max(0,(source.stock[resource]||0)-stxRTReserve(resource)));if(take>.01){allocations.push({source,amount:take});remaining-=take}}return remaining<=.01?allocations:null}
function stxRTDestination(owner,resource){return owned(owner).filter(p=>!p.underAttack).sort((a,b)=>{const ao=(a.orders||[]).filter(o=>o.resource===resource&&o.status!=="filled").length,bo=(b.orders||[]).filter(o=>o.resource===resource&&o.status!=="filled").length;return bo-ao||(a.stock?.[resource]||0)-(b.stock?.[resource]||0)})[0]||owned(owner)[0]||null}
function stxRTDesiredAmount(resource){const orders=playerWorlds().reduce((n,p)=>n+(p.orders||[]).filter(o=>o.resource===resource&&o.status!=="filled").reduce((s,o)=>s+Math.max(0,(o.amount||0)-(o.filled||0)),0),0),stock=empireResource(0,resource);return Math.ceil(clamp(42+orders*.7-stock*.06,28,140))}
function stxRTPaymentFor(supplierId,resource,offerAmount){
  const value=offerAmount*(STX_RT_VALUES[resource]||1),needs=empireNeed(supplierId).filter(x=>x.resource!==resource&&STX_RT_RESOURCES.includes(x.resource)&&stxRTEmpireAvailable(0,x.resource)>12).sort((a,b)=>(b.orders*32-b.amount)-(a.orders*32-a.amount)),wanted=needs[0],combined=!!wanted&&random()<.34;
  if(wanted){const amount=Math.max(6,Math.ceil(value*(combined?.58:.92)/(STX_RT_VALUES[wanted.resource]||1))),available=stxRTEmpireAvailable(0,wanted.resource);if(available>=amount)return{resource:wanted.resource,amount,credits:combined?Math.ceil(value*.34):0}}
  return{credits:Math.max(18,Math.ceil(value*(.92+random()*.2)))};
}
function stxRTPaymentText(payment){const parts=[];if(payment?.resource)parts.push(`${payment.amount} ${stxRTLabel(payment.resource)}`);if(payment?.credits)parts.push(`${payment.credits} Credits`);return parts.join(" + ")||"open commercial access"}
function stxRTEmbargoed(a,b){return(state.rivalDiplomacy?.agreements||[]).some(x=>x.active!==false&&x.kind==="embargo"&&x.expiresAt>state.simTime&&((x.by===a&&x.with===b)||(x.by===b&&x.with===a)))}
function stxRTEligibleSupplier(e,resource){return e.id!==0&&stxRTActiveEmpire(e.id)&&!empiresAtWar(0,e.id)&&!stxRTEmbargoed(0,e.id)&&relation(0,e.id)>-.58&&stxRTEmpireAvailable(e.id,resource)>=8}

function stxRTRequestTrade(resource,desired=stxRTDesiredAmount(resource)){
  const s=stxRTEnsureState(false);if(!STX_RT_RESOURCES.includes(resource))return null;const existing=s.requests.find(r=>r.status==="open"&&r.resource===resource);if(existing){showToast(`${stxRTLabel(resource)} request already open`);return existing}if(state.simTime<s.nextRequestAt){showToast(`Trade ministry can issue another request in ${fmtEta(s.nextRequestAt-state.simTime)}`);return null}
  const request={id:stxRTId("trq"),resource,desired:Math.max(1,Math.ceil(desired)),accepted:0,status:"open",createdAt:state.simTime,expiresAt:state.simTime+95,offerIds:[]};
  const suppliers=state.empires.filter(e=>stxRTEligibleSupplier(e,resource)).map(e=>({e,available:stxRTEmpireAvailable(e.id,resource),source:stxRTSource(e.id,resource)})).filter(x=>x.source).sort((a,b)=>b.available-a.available||relation(0,b.e.id)-relation(0,a.e.id)).slice(0,5);
  suppliers.forEach((candidate,index)=>{const amount=Math.max(6,Math.ceil(Math.min(candidate.available*.42,request.desired*(.32+index*.05),62))),payment=stxRTPaymentFor(candidate.e.id,resource,amount),offer={id:stxRTId("tro"),requestId:request.id,from:candidate.e.id,resource,amount,payment,status:"pending",createdAt:state.simTime,expiresAt:request.expiresAt,sourcePlanetId:candidate.source.id};s.offers.push(offer);request.offerIds.push(offer.id)});
  s.requests.unshift(request);s.requests=s.requests.slice(0,18);s.offers=s.offers.slice(-70);s.nextRequestAt=state.simTime+12;logEvent(`Trade request broadcast for ${request.desired} ${stxRTLabel(resource)}; ${request.offerIds.length} foreign offer${request.offerIds.length===1?"":"s"} received.`,request.offerIds.length?"good":"warning");if(request.offerIds.length)galacticNews(`${stxRTLabel(resource).toUpperCase()} OFFERS RECEIVED`,`${request.offerIds.length} foreign power${request.offerIds.length===1?" has":"s have"} answered the Mandate's open request. Each offer may be accepted independently in Transmissions.`,"trade");renderTransmissions();updateBadges();return request;
}
function stxRTCanPay(payment){return(!payment?.credits||empire(0).credits>=payment.credits)&&(!payment?.resource||stxRTEmpireAvailable(0,payment.resource)>=payment.amount)}
function stxRTDispatchCargo(from,to,owner,resource,amount,extra={}){const type=resource==="helium"?"tanker":resource==="equipment"?"supply":resource==="components"?"construction":"freighter";return createShip(type,from,to,owner,{cargo:{[resource]:amount},commercial:true,crossBorder:true,tradeKind:"requested-trade",tradePartner:to.owner,vesselName:vesselName(type),...extra})}
function stxRTAcceptOffer(id){
  const s=stxRTEnsureState(false),offer=s.offers.find(o=>o.id===id&&o.status==="pending"),request=offer&&s.requests.find(r=>r.id===offer.requestId);if(!offer||!request||request.status!=="open")return false;const supplierDestination=stxRTDestination(offer.from,offer.payment?.resource||offer.resource),destination=stxRTDestination(0,offer.resource),deliveryAllocations=stxRTAllocateStock(offer.from,offer.resource,offer.amount),paymentAllocations=offer.payment.resource?stxRTAllocateStock(0,offer.payment.resource,offer.payment.amount):[];if(!supplierDestination||!destination||!deliveryAllocations)return showToast("That faction no longer has the offered surplus"),false;if(!stxRTCanPay(offer.payment)||offer.payment.resource&&!paymentAllocations)return showToast("The Mandate cannot meet those terms"),false;const legs=deliveryAllocations.length+paymentAllocations.length;if(state.ships.length+legs>279)return showToast("Trade lanes are at current vessel capacity"),false;
  for(const allocation of paymentAllocations){allocation.source.stock[offer.payment.resource]-=allocation.amount;stxRTDispatchCargo(allocation.source,supplierDestination,0,offer.payment.resource,allocation.amount,{tradeOfferId:offer.id,tradeLeg:"payment"})}
  if(offer.payment.credits){empire(0).credits-=offer.payment.credits;empire(offer.from).credits+=offer.payment.credits}
  for(const allocation of deliveryAllocations){allocation.source.stock[offer.resource]-=allocation.amount;stxRTDispatchCargo(allocation.source,destination,offer.from,offer.resource,allocation.amount,{tradeOfferId:offer.id,tradeLeg:"delivery"})}
  offer.status="accepted";offer.acceptedAt=state.simTime;request.accepted+=offer.amount;adjustRelation(0,offer.from,.035);if(typeof stxRDAddCooperation==="function")stxRDAddCooperation(0,offer.from,"requested resource trade",`${offer.amount} ${stxRTLabel(offer.resource)} exchange agreed`,4);
  if(request.accepted>=request.desired){request.status="fulfilled";request.fulfilledAt=state.simTime;s.offers.filter(o=>o.requestId===request.id&&o.status==="pending").forEach(o=>o.status="closed")}
  galacticNews("REQUESTED TRADE CONVOYS DEPART",`${empire(offer.from).name} dispatched ${offer.amount} ${stxRTLabel(offer.resource)} toward ${destination.name}. The Mandate is providing ${stxRTPaymentText(offer.payment)} in return.`,"trade",destination.id);showToast(`${offer.amount} ${stxRTLabel(offer.resource)} en route`);renderTransmissions();updateBadges();updateHud(true);return true;
}
function stxRTDeclineOffer(id){const s=stxRTEnsureState(false),offer=s.offers.find(o=>o.id===id&&o.status==="pending");if(!offer)return false;offer.status="declined";offer.declinedAt=state.simTime;/* Routine trade negotiation deliberately has no friendship penalty. */renderTransmissions();updateBadges();return true}

function stxRTExpire(){const s=stxRTEnsureState(false);s.requests.forEach(r=>{if(r.status==="open"&&state.simTime>=r.expiresAt)r.status="expired"});s.offers.forEach(o=>{const r=s.requests.find(x=>x.id===o.requestId);if(o.status==="pending"&&(!r||r.status!=="open"||state.simTime>=o.expiresAt))o.status="expired"});s.aiTrades=s.aiTrades.filter(x=>state.simTime-x.time<180)}
function stxRTAIChooseFocus(e){
  const worlds=owned(e.id);if(!worlds.length)return;const needs=empireNeed(e.id),top=needs[0]?.resource,raw=STX_RT_RAW.includes(top);worlds.forEach((p,i)=>{if(state.simTime-(p.stxFocusChangedAt||-999)<65)return;let focus=raw?"mining":top==="components"?"components":top==="equipment"?"equipment":"balanced";if(i>0&&worlds.length>=3){if(i%3===1)focus="mining";else if(e.doctrine==="industry")focus=i%2?"components":"equipment"}p.stxEconomicFocus=focus;p.stxFocusChangedAt=state.simTime})
}
function stxRTAITrade(){
  const s=stxRTEnsureState(false);if(state.simTime<s.nextAITradeAt)return;s.nextAITradeAt=state.simTime+rand(28,48);state.empires.slice(1).filter(e=>stxRTActiveEmpire(e.id)).forEach(stxRTAIChooseFocus);
  const candidates=[];for(let buyer=1;buyer<state.empires.length;buyer++){if(!stxRTActiveEmpire(buyer))continue;const need=empireNeed(buyer).find(n=>STX_RT_RESOURCES.includes(n.resource));if(!need)continue;for(let seller=1;seller<state.empires.length;seller++){if(seller===buyer||!stxRTActiveEmpire(seller)||empiresAtWar(seller,buyer)||stxRTEmbargoed(seller,buyer))continue;const available=stxRTEmpireAvailable(seller,need.resource);if(available>16)candidates.push({buyer,seller,resource:need.resource,available,score:(need.orders||0)*35-need.amount+available+relation(seller,buyer)*30})}}
  const deal=candidates.sort((a,b)=>b.score-a.score)[0];if(!deal)return;const source=stxRTSource(deal.seller,deal.resource),dest=stxRTDestination(deal.buyer,deal.resource),amount=Math.ceil(Math.min(36,deal.available*.28));if(!source||!dest||amount<4)return;const payNeed=empireNeed(deal.seller).find(n=>n.resource!==deal.resource&&STX_RT_RESOURCES.includes(n.resource)&&stxRTEmpireAvailable(deal.buyer,n.resource)>10),payment=payNeed?{resource:payNeed.resource,amount:Math.ceil(amount*(STX_RT_VALUES[deal.resource]||1)/(STX_RT_VALUES[payNeed.resource]||1)*.88)}:{credits:Math.ceil(amount*(STX_RT_VALUES[deal.resource]||1))};if(!stxRTCanAIPay(deal.buyer,payment))return;
  source.stock[deal.resource]-=amount;if(!stxRTDispatchCargo(source,dest,deal.seller,deal.resource,amount,{stxAITrade:true})){source.stock[deal.resource]+=amount;return}if(payment.resource){const ps=stxRTSource(deal.buyer,payment.resource,payment.amount),pd=stxRTDestination(deal.seller,payment.resource);if(ps&&pd){ps.stock[payment.resource]-=payment.amount;stxRTDispatchCargo(ps,pd,deal.buyer,payment.resource,payment.amount,{stxAITrade:true})}}else{empire(deal.buyer).credits-=payment.credits;empire(deal.seller).credits+=payment.credits}s.aiTrades.push({time:state.simTime,...deal,amount,payment});adjustRelation(deal.seller,deal.buyer,.012);
}
function stxRTCanAIPay(owner,payment){return(!payment.credits||empire(owner).credits>=payment.credits)&&(!payment.resource||stxRTEmpireAvailable(owner,payment.resource)>=payment.amount)}
const STX_RT_commerceTick=commerceTick;
commerceTick=function(){const result=STX_RT_commerceTick();stxRTExpire();stxRTAITrade();stxRTPersist();return result};

/* Core-generated routine resource offers follow the same low-stakes rule as
   player-requested offers: declining one is not a diplomatic insult. Mark it
   as observed before the rival-memory layer runs, then preserve the exact
   relationship value. */
const STX_RT_respondProposal=respondProposal;
respondProposal=function(id,action){const p=state.proposals.find(x=>x.id===id&&x.status==="pending"),routine=action==="decline"&&p?.kind==="trade"&&p.from>0,before=routine?relation(0,p.from):null;if(routine)p.rdMemoryRecorded=true;const result=STX_RT_respondProposal(id,action);if(routine&&empire(p.from)){empire(0).relations[p.from]=before;empire(p.from).relations[0]=before}return result};

function stxRTFocusBlock(p){const current=STX_RT_FOCUSES[p.stxEconomicFocus]||STX_RT_FOCUSES.balanced;return`<section class="stx-rt-focus"><div class="stx-rt-focus-title"><span>ECONOMIC ALLOCATION</span><b>${stxRTEscape(current.label)}</b></div><div class="stx-rt-focus-grid">${Object.entries(STX_RT_FOCUSES).map(([id,f])=>`<button class="${id===p.stxEconomicFocus?"active":""}" data-stx-focus="${id}" data-stx-focus-planet="${p.id}"><b>${stxRTEscape(f.label)}</b><small>${stxRTEscape(f.summary)}</small></button>`).join("")}</div><p>${stxRTEscape(current.description)} This changes actual production; no allocation is universally best.</p></section>`}
function stxRTDecoratePlanet(){const p=state.selected,body=$("planetBody");if(!p||p.owner!==0||!body||body.querySelector?.(".stx-rt-focus"))return;const anchor=body.querySelector?.(".stx-resource-specialization")||[...body.querySelectorAll?.(".section-label")||[]].find(x=>/resource/i.test(x.textContent||""));if(anchor)anchor.insertAdjacentHTML("afterend",stxRTFocusBlock(p));else body.insertAdjacentHTML("beforeend",stxRTFocusBlock(p));body.querySelectorAll?.("[data-stx-focus]").forEach(b=>b.onclick=()=>stxRTSetFocus(b.dataset.stxFocusPlanet,b.dataset.stxFocus))}
const STX_RT_renderPlanet=renderPlanet;
renderPlanet=function(){STX_RT_renderPlanet();stxRTDecoratePlanet()};

function stxRTRequestPanel(){const s=stxRTEnsureState(false),cool=Math.max(0,s.nextRequestAt-state.simTime),active=s.requests.filter(r=>r.status==="open"),status=active.length?active.map(r=>`${stxRTLabel(r.resource)} ${r.accepted}/${r.desired}`).join(" · "):"No open market request";return`<section class="stx-rt-request-panel"><div><div class="card-kicker">FOREIGN TRADE MINISTRY</div><strong>Request Resource Trade</strong><p>Broadcast one shortage through Transmissions. Every answer uses the faction's real surplus and can be accepted independently.</p></div><div class="stx-rt-request-controls"><select id="stxRTResource" aria-label="Requested resource">${STX_RT_RESOURCES.map(r=>`<option value="${r}">${stxRTLabel(r)}</option>`).join("")}</select><button class="choice-btn primary-choice" id="stxRTRequest" ${cool>0?"disabled":""}>${cool>0?`Ready in ${fmtEta(cool)}`:"Request Trade"}</button></div><small>${stxRTEscape(status)}</small></section>`}
function stxRTOfferCard(offer){const request=state.resourceTradeEconomy.requests.find(r=>r.id===offer.requestId),can=stxRTCanPay(offer.payment)&&stxRTEmpireAvailable(offer.from,offer.resource)>=offer.amount;return`<article class="transmission-card trade stx-rt-offer"><div class="card-kicker">REQUESTED TRADE OFFER // ${stxRTEscape(empire(offer.from)?.name)}</div><div class="card-title"><strong>${offer.amount} ${stxRTEscape(stxRTLabel(offer.resource))}</strong><span class="news-time">${fmtEta(offer.expiresAt-state.simTime)}</span></div><p class="card-copy">They request ${stxRTEscape(stxRTPaymentText(offer.payment))}. Their offered cargo remains in real stockpiles until accepted.</p><div class="stx-rt-offer-progress"><span>Request progress</span><b>${request?.accepted||0} / ${request?.desired||offer.amount}</b></div><div class="choice-row"><button class="choice-btn primary-choice" data-stx-trade-accept="${offer.id}" ${can?"":"disabled"}>Accept Offer</button><button class="choice-btn" data-stx-trade-decline="${offer.id}">Decline · No Penalty</button></div></article>`}
const STX_RT_renderTransmissions=renderTransmissions;
renderTransmissions=function(){STX_RT_renderTransmissions();const box=$("transmissionList");if(!box)return;stxRTExpire();box.insertAdjacentHTML("afterbegin",stxRTRequestPanel()+state.resourceTradeEconomy.offers.filter(o=>o.status==="pending").map(stxRTOfferCard).join(""));const request=$("stxRTRequest");if(request)request.onclick=()=>{const select=$("stxRTResource");stxRTRequestTrade(select?.value||"components")};box.querySelectorAll?.("[data-stx-trade-accept]").forEach(b=>b.onclick=()=>stxRTAcceptOffer(b.dataset.stxTradeAccept));box.querySelectorAll?.("[data-stx-trade-decline]").forEach(b=>b.onclick=()=>stxRTDeclineOffer(b.dataset.stxTradeDecline))};
const STX_RT_updateBadges=updateBadges;
updateBadges=function(){STX_RT_updateBadges();const m=$("messageCount"),count=state.resourceTradeEconomy?.offers?.filter(o=>o.status==="pending").length||0;if(m&&count){m.textContent=Math.min(99,(Number(m.textContent)||0)+count);m.hidden=false}};

function stxRTInstallStyles(){if($("stxRTStyles"))return;const style=document.createElement("style");style.id="stxRTStyles";style.textContent=`
.stx-rt-focus{margin:9px 0 12px;padding:9px;border:1px solid rgba(255,194,91,.18);border-radius:11px;background:linear-gradient(135deg,rgba(255,183,83,.07),rgba(58,86,150,.08))}.stx-rt-focus-title{display:flex;justify-content:space-between;gap:8px}.stx-rt-focus-title span{font-size:.51rem;font-weight:950;letter-spacing:.11em;color:#b49669}.stx-rt-focus-title b{font-size:.62rem;color:#ffe0a8}.stx-rt-focus-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin:7px 0}.stx-rt-focus-grid button{padding:7px;text-align:left;border:1px solid rgba(112,145,209,.15);border-radius:8px;background:rgba(5,12,27,.42);color:#dce9f8;cursor:pointer}.stx-rt-focus-grid button.active{border-color:rgba(255,202,102,.58);background:rgba(255,190,83,.12);box-shadow:0 0 12px rgba(255,190,83,.08)}.stx-rt-focus-grid b,.stx-rt-focus-grid small{display:block}.stx-rt-focus-grid b{font-size:.57rem}.stx-rt-focus-grid small,.stx-rt-focus p{font-size:.49rem;line-height:1.35;color:#7f94af}.stx-rt-focus p{margin:5px 0 0}.stx-rt-request-panel{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;padding:13px;margin-bottom:10px;border:1px solid rgba(78,231,255,.25);border-radius:13px;background:linear-gradient(135deg,rgba(78,231,255,.09),rgba(111,87,255,.08))}.stx-rt-request-panel strong{display:block;margin:4px 0;font-size:.82rem}.stx-rt-request-panel p{margin:0;color:#9db1ce;font-size:.65rem;line-height:1.4}.stx-rt-request-panel>small{grid-column:1/-1;color:#74ddec;font-size:.57rem;font-weight:800}.stx-rt-request-controls{display:flex;gap:6px;align-items:center}.stx-rt-request-controls select{max-width:150px;padding:7px;border:1px solid rgba(108,151,230,.28);border-radius:8px;background:#091326;color:#edf7ff;font-size:.62rem}.stx-rt-offer-progress{display:flex;justify-content:space-between;margin-top:8px;padding:6px 8px;border-radius:7px;background:rgba(255,255,255,.035);font-size:.58rem;color:#8fa5c2}.stx-rt-offer-progress b{color:#ffe09d}@media(max-width:650px){.stx-rt-request-panel{grid-template-columns:1fr}.stx-rt-request-controls{align-items:stretch}.stx-rt-request-controls select{max-width:none;flex:1}}
`;document.head.appendChild(style)}

globalThis.SpaceTyrantsResourceTrade={version:STX_RT_VERSION,resources:STX_RT_RESOURCES,focuses:STX_RT_FOCUSES,ensureState:stxRTEnsureState,setFocus:stxRTSetFocus,requestTrade:stxRTRequestTrade,acceptOffer:stxRTAcceptOffer,declineOffer:stxRTDeclineOffer,desiredAmount:stxRTDesiredAmount,available:stxRTEmpireAvailable,diversifyShipNeed:stxRTDiversifyShipNeed};
stxRTEnsureState(false);stxRTInstallStyles();
