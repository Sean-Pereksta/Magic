/* Space Tyrants — player project priority + stronger planetary resource specialization.
   Loaded after supply-emergency.js so it owns the final project-routing and inspector UI. */

const STX_PS_PRIORITY = 100;
const STX_PS_RAW_RESOURCES = [...RESOURCES];

function stxPSHash(text){
  let h=2166136261>>>0;
  for(let i=0;i<String(text).length;i++){h^=String(text).charCodeAt(i);h=Math.imul(h,16777619)}
  return h>>>0;
}
function stxPSUnit(seed,salt=0){
  let x=(seed+Math.imul(salt+1,0x9e3779b1))>>>0;
  x^=x>>>16;x=Math.imul(x,0x7feb352d);x^=x>>>15;x=Math.imul(x,0x846ca68b);x^=x>>>16;
  return (x>>>0)/4294967296;
}
function stxPSResourceProfile(p,rescale=false){
  if(!p)return null;
  if(p.stxResourcePrimary&&p.stxResourceYield){
    p.stxManufacturingFocus=p.stxManufacturingFocus||((stxPSHash(`${p.id}:${p.name}:factory`)&1)?"components":"equipment");
    return p;
  }
  const seed=stxPSHash(`${p.id}|${p.name}|${Math.round(p.x||0)}|${Math.round(p.y||0)}`);
  const primaryIndex=seed%STX_PS_RAW_RESOURCES.length;
  const primary=STX_PS_RAW_RESOURCES[primaryIndex];
  const secondary=STX_PS_RAW_RESOURCES[(primaryIndex+1+Math.floor(stxPSUnit(seed,2)*(STX_PS_RAW_RESOURCES.length-1)))%STX_PS_RAW_RESOURCES.length];
  const remaining=STX_PS_RAW_RESOURCES.filter(r=>r!==primary&&r!==secondary).sort((a,b)=>stxPSHash(`${seed}:${a}`)-stxPSHash(`${seed}:${b}`));
  const weak=new Set(remaining.slice(0,2));
  const quality={},yieldMap={};
  STX_PS_RAW_RESOURCES.forEach((r,i)=>{
    const oldQ=Math.max(1,Number(p.quality?.[r]||1));
    let q,m;
    if(r===primary){q=5;m=2.25+stxPSUnit(seed,10+i)*.55}
    else if(r===secondary){q=4+(stxPSUnit(seed,20+i)>.55?1:0);m=1.4+stxPSUnit(seed,30+i)*.35}
    else if(weak.has(r)){q=1;m=.42+stxPSUnit(seed,40+i)*.25}
    else{q=2+(stxPSUnit(seed,50+i)>.5?1:0);m=.82+stxPSUnit(seed,60+i)*.32}
    quality[r]=q;yieldMap[r]=Number(m.toFixed(2));
    if(rescale&&p.reserve&&p.stock){
      const scale=clamp((q/oldQ)*m,.38,4.1);
      p.reserve[r]=Math.max(120,Number(p.reserve[r]||0)*scale);
      p.stock[r]=Math.max(0,Number(p.stock[r]||0)*clamp(.72+scale*.22,.55,1.75));
    }
  });
  p.quality={...(p.quality||{}),...quality};
  p.stxResourcePrimary=primary;
  p.stxResourceSecondary=secondary;
  p.stxResourceWeak=[...weak];
  p.stxResourceYield=yieldMap;
  p.stxManufacturingFocus=(stxPSUnit(seed,90)>.45?"components":"equipment");
  return p;
}
function stxPSProfileLabel(p,r){
  stxPSResourceProfile(p,false);
  if(r===p.stxResourcePrimary)return"PRIMARY";
  if(r===p.stxResourceSecondary)return"SECONDARY";
  if((p.stxResourceWeak||[]).includes(r))return"POOR";
  return"STANDARD";
}

const STX_PS_makePlanet=makePlanet;
makePlanet=function(...args){
  const p=STX_PS_makePlanet(...args);
  return stxPSResourceProfile(p,true);
};

const STX_PS_loadGame=loadGame;
loadGame=function(){
  const ok=STX_PS_loadGame();
  if(ok)state.planets.forEach(p=>stxPSResourceProfile(p,false));
  return ok;
};

const STX_PS_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){
  if(!p||p.owner===null)return STX_PS_tickPlanet(p,dt);
  stxPSResourceProfile(p,false);
  const baseQuality={};
  STX_PS_RAW_RESOURCES.forEach(r=>{
    baseQuality[r]=p.quality[r];
    p.quality[r]=Math.max(.2,baseQuality[r]*(p.stxResourceYield?.[r]||1));
  });
  try{
    STX_PS_tickPlanet(p,dt);
  }finally{
    STX_PS_RAW_RESOURCES.forEach(r=>p.quality[r]=baseQuality[r]);
  }
  /* The final resource-trade layer owns explicit Mining / Balanced /
     Components / Equipment allocation. Preserve the legacy inferred focus for
     old saves that have not been migrated yet, but do not stack its bonus on
     top of the player's visible allocation choice. */
  if(p.infra?.factory>0&&!p.stxEconomicFocus){
    const flow=Math.max(0,p.infra.factory*.018*Math.max(0,p.factoryEfficiency||0)*dt);
    if(p.stxManufacturingFocus==="components")p.stock.components=(p.stock.components||0)+flow*1.35;
    else p.stock.equipment=(p.stock.equipment||0)+flow*.95;
  }
};

function stxPSDiversifiedNeed(desc){
  const q=desc.q||{};
  if(desc.kind==="local"){
    const c=Math.max(20,Number(q.cost||40)),type=q.type||"local";
    if(type==="city")return{components:c*.42,silicates:c*.34,iron:c*.24};
    if(type==="mine")return{components:c*.34,equipment:c*.24,iron:c*.32,titanium:c*.14};
    if(type==="factory")return{components:c*.38,iron:c*.38,rare:c*.2,silicates:c*.18};
    if(type==="training")return{components:c*.34,equipment:c*.38,iron:c*.16};
    if(type==="defense")return{components:c*.34,titanium:c*.34,equipment:c*.24,iron:c*.14};
    if(type==="shipyard")return{components:c*.36,titanium:c*.34,iron:c*.3,helium:c*.12};
    if(type==="research")return{components:c*.3,rare:c*.34,silicates:c*.26,equipment:c*.14};
  }
  if(desc.kind==="reconstruction"){
    const c=Math.max(18,Number(q.need||30));return{components:c*.4,silicates:c*.34,iron:c*.3,equipment:c*.12};
  }
  if(desc.kind==="expansion"){
    const c=Math.max(18,Number(q.componentCost||24));return{components:c*.46,silicates:c*.3,helium:c*.22,titanium:c*.18};
  }
  return desc.need||{};
}
function stxPSCleanNeed(need){
  return Object.fromEntries(Object.entries(need||{}).filter(([,v])=>Number(v)>0).map(([r,v])=>[r,r==="trained"?Number(v):Math.max(.5,Number(v))]));
}

const STX_PS_stxSDPriority=stxSDPriority;
stxSDPriority=function(kind,q,p){
  const normal=STX_PS_stxSDPriority(kind,q,p);
  if(!p||p.owner!==0)return normal;
  const id=stxSDProjectId(q,kind);
  return empire(0)?.stxPriorityProjectId===id?STX_PS_PRIORITY:normal;
};
const STX_PS_stxSDPriorityLabel=stxSDPriorityLabel;
stxSDPriorityLabel=function(n){return n>=STX_PS_PRIORITY?"PLAYER PRIORITY · FIRST CLAIM ON SUPPLY":STX_PS_stxSDPriorityLabel(n)};

const STX_PS_stxSDDescriptors=stxSDDescriptors;
stxSDDescriptors=function(p){
  const descs=STX_PS_stxSDDescriptors(p);
  descs.forEach(d=>{
    if(["local","reconstruction","expansion"].includes(d.kind)){
      d.need=stxPSCleanNeed(stxPSDiversifiedNeed(d));
      stxSDEnsureSupply(d);
    }
    d.priority=stxSDPriority(d.kind,d.q,d.p);
  });
  return descs;
};

function stxPSAllProjectDescriptors(owner=0){return owned(owner).flatMap(p=>stxSDDescriptors(p))}
function stxPSPriorityProject(owner=0){
  const e=empire(owner),id=e?.stxPriorityProjectId;
  if(!id)return null;
  const d=stxPSAllProjectDescriptors(owner).find(x=>x.id===id);
  if(!d){e.stxPriorityProjectId=null;e.stxPriorityPlanetId=null;return null}
  return d;
}
function stxPSPriorityStillNeedsSupply(d){
  return !!d&&Object.keys(d.need||{}).some(r=>stxSDRemaining(d,r)>Math.max(r==="trained"?.0001:.15,Number(d.need[r]||0)*.0015));
}
function stxPSRefreshOrderPriorities(owner=0){
  stxPSAllProjectDescriptors(owner).forEach(d=>{
    stxSDEnsureOrders(d);
    Object.values(d.q.stxSupply?.orderIds||{}).forEach(orderId=>{
      const o=d.p.orders.find(x=>x.id===orderId);
      if(o)o.priority=d.priority;
    });
  });
}
function stxPSSuppressLegacyProjectOrders(owner=0){
  owned(owner).forEach(p=>{
    const tracked=new Set(stxSDDescriptors(p).flatMap(d=>Object.values(d.q.stxSupply?.orderIds||{}).filter(Boolean)));
    p.orders.forEach(o=>{
      if(tracked.has(o.id)||String(o.type||"").startsWith("stx-")||state.ships.some(s=>s.orderId===o.id))return;
      if(["construction","reconstruction","ship","crew","sensor","trade-station"].includes(o.type)&&p.orders.some(x=>tracked.has(x.id)&&x.resource===o.resource)){
        o.filled=o.amount;o.status="filled";
      }
    });
  });
}
function stxPSSetPriority(projectId,planetId){
  const e=empire(0);if(!e)return;
  const candidate=stxPSAllProjectDescriptors(0).find(d=>d.id===projectId&&d.p.id===planetId);
  if(!candidate){showToast("That project is no longer active");renderPlanet();return}
  if(e.stxPriorityProjectId===projectId){
    e.stxPriorityProjectId=null;e.stxPriorityPlanetId=null;
    stxPSRefreshOrderPriorities(0);
    showToast("Project priority cleared");
  }else{
    const prior=stxPSPriorityProject(0);
    e.stxPriorityProjectId=projectId;e.stxPriorityPlanetId=planetId;e.stxPrioritySetAt=state.simTime;
    stxPSRefreshOrderPriorities(0);
    stxSDAllocatePlanet(candidate.p);
    const replaced=prior&&prior.id!==projectId?` · replaced ${prior.title}`:"";
    showToast(`${candidate.title} prioritized${replaced}`);
    logEvent(`${candidate.title} at ${candidate.p.name} now has first claim on project freight and strategic materials.`,"good");
  }
  renderPlanet();updateHud(true);
}

const STX_PS_fillOrder=fillOrder;
fillOrder=function(dest,o){
  if(dest?.owner===0&&String(o?.type||"").startsWith("stx-")){
    const priority=stxPSPriorityProject(0);
    const projectId=String(o.type).slice(4);
    if(priority&&priority.id!==projectId){
      const need=Number(priority.need?.[o.resource]||0),remaining=need?stxSDRemaining(priority,o.resource):0,incoming=need?stxSDIncomingAmount(priority,o.resource):0;
      if(need>0&&remaining-incoming>Math.max(o.resource==="trained"?.0001:.15,need*.0015))return;
    }
  }
  return STX_PS_fillOrder(dest,o);
};
function stxPSDispatchPriority(){
  const d=stxPSPriorityProject(0);if(!d)return;
  stxSDEnsureOrders(d);
  const ids=new Set(Object.values(d.q.stxSupply?.orderIds||{}).filter(Boolean));
  d.p.orders.filter(o=>ids.has(o.id)&&o.status==="waiting").sort((a,b)=>b.priority-a.priority||b.age-a.age).slice(0,4).forEach(o=>fillOrder(d.p,o));
}
const STX_PS_logisticsTick=logisticsTick;
logisticsTick=function(){
  stxPSRefreshOrderPriorities(0);
  stxPSSuppressLegacyProjectOrders(0);
  stxPSDispatchPriority();
  return STX_PS_logisticsTick();
};

function stxPSPriorityButton(desc){
  if(desc.p.owner!==0)return"";
  const active=empire(0)?.stxPriorityProjectId===desc.id;
  return `<div class="stx-priority-strip"><button class="stx-priority-btn ${active?"active":""}" data-stx-prioritize="${desc.id}" data-stx-priority-planet="${desc.p.id}">${active?"★ PRIORITIZED":"☆ PRIORITIZE"}</button><small>${active?"All project freight routes here first":"Make this the empire’s single supply priority"}</small></div>`;
}
const STX_PS_stxSDProjectSupplyCard=stxSDProjectSupplyCard;
stxSDProjectSupplyCard=function(desc){
  const card=STX_PS_stxSDProjectSupplyCard(desc);
  const button=stxPSPriorityButton(desc);
  return button?card.replace('<div class="project-head">',`${button}<div class="project-head">`):card;
};

function stxPSResourceSummary(p){
  if(!p||p.owner===null)return"";
  stxPSResourceProfile(p,false);
  const primary=p.stxResourcePrimary,secondary=p.stxResourceSecondary,weak=p.stxResourceWeak||[];
  return `<div class="stx-resource-specialization"><div class="stx-resource-title"><span>PLANETARY RESOURCE SPECIALIZATION</span><b>${stxSDResourceLabel(primary)} world</b></div><div class="stx-resource-pills"><span class="primary">${stxSDResourceLabel(primary)} ×${(p.stxResourceYield[primary]||1).toFixed(1)}</span><span>${stxSDResourceLabel(secondary)} ×${(p.stxResourceYield[secondary]||1).toFixed(1)}</span>${weak.map(r=>`<span class="poor">${stxSDResourceLabel(r)} ×${(p.stxResourceYield[r]||1).toFixed(1)}</span>`).join("")}</div><small>Mining output is intentionally uneven. This world should export its strengths and import its weak resources.</small></div>`;
}
function stxPSDecoratePlanetInspector(){
  const p=state.selected,body=$("planetBody");if(!p||!body)return;
  body.querySelectorAll("[data-stx-prioritize]").forEach(btn=>btn.onclick=e=>{e.preventDefault();e.stopPropagation();stxPSSetPriority(btn.dataset.stxPrioritize,btn.dataset.stxPriorityPlanet)});
  const labels=[...body.querySelectorAll(".section-label")],resourceLabel=labels.find(x=>x.textContent.trim()==="Resource Quality");
  if(resourceLabel){
    resourceLabel.textContent="Resource Specialization & Quality";
    resourceLabel.insertAdjacentHTML("beforebegin",stxPSResourceSummary(p));
    const list=resourceLabel.nextElementSibling;
    list?.querySelectorAll(".resource-row").forEach(row=>{
      const label=row.querySelector("span")?.textContent?.trim(),r=STX_PS_RAW_RESOURCES.find(k=>stxSDResourceLabel(k)===label||RESOURCE_LABEL[k]===label);
      if(!r)return;
      const tag=document.createElement("small");tag.className=`stx-resource-tag ${stxPSProfileLabel(p,r).toLowerCase()}`;
      tag.textContent=`${stxPSProfileLabel(p,r)} · ×${(p.stxResourceYield?.[r]||1).toFixed(1)} output`;
      row.querySelector("span")?.appendChild(tag);
    });
  }
}
const STX_PS_renderPlanet=renderPlanet;
renderPlanet=function(){STX_PS_renderPlanet();stxPSDecoratePlanetInspector()};

function stxPSInstallStyles(){
  if($("stxPrioritySpecializationStyles"))return;
  const style=document.createElement("style");style.id="stxPrioritySpecializationStyles";style.textContent=`
.stx-priority-strip{display:flex;align-items:center;justify-content:space-between;gap:7px;margin:-2px -2px 7px;padding:5px 6px;border-radius:8px;background:rgba(80,106,166,.09);border:1px solid rgba(116,155,233,.12)}
.stx-priority-strip small{font-size:.5rem;color:#7189ad;text-align:right;line-height:1.25}
.stx-priority-btn{border:1px solid rgba(255,206,105,.38);background:rgba(255,206,105,.08);color:#ffda83;border-radius:7px;padding:5px 7px;font-size:.54rem;font-weight:950;letter-spacing:.05em;cursor:pointer;white-space:nowrap}
.stx-priority-btn:hover{filter:brightness(1.16);border-color:#ffdc8a}.stx-priority-btn.active{background:linear-gradient(135deg,rgba(255,196,75,.26),rgba(156,123,255,.22));border-color:#ffce69;color:#fff2bf;box-shadow:0 0 14px rgba(255,206,105,.17)}
.stx-resource-specialization{margin:12px 0 8px;padding:9px;border-radius:11px;background:linear-gradient(135deg,rgba(78,231,255,.08),rgba(156,123,255,.08));border:1px solid rgba(78,231,255,.16)}
.stx-resource-title{display:flex;justify-content:space-between;gap:8px;align-items:center}.stx-resource-title span{font-size:.51rem;font-weight:950;letter-spacing:.1em;color:#7fa5cf}.stx-resource-title b{font-size:.63rem;color:#dff9ff}
.stx-resource-pills{display:flex;gap:5px;flex-wrap:wrap;margin:7px 0}.stx-resource-pills span{padding:4px 6px;border-radius:999px;background:rgba(114,150,225,.11);border:1px solid rgba(114,150,225,.14);font-size:.52rem;color:#b9c9e2}.stx-resource-pills span.primary{color:#7ff3ff;border-color:rgba(78,231,255,.35);background:rgba(78,231,255,.1)}.stx-resource-pills span.poor{color:#ff9cac;border-color:rgba(255,95,118,.25);background:rgba(255,95,118,.07)}
.stx-resource-specialization>small{font-size:.53rem;color:#748daf;line-height:1.35}
.resource-row>span:first-child{display:flex;flex-direction:column;gap:1px}.stx-resource-tag{font-size:.46rem!important;color:#7389aa!important;font-weight:800;letter-spacing:.04em}.stx-resource-tag.primary{color:#5eeeff!important}.stx-resource-tag.secondary{color:#c6b4ff!important}.stx-resource-tag.poor{color:#ff8294!important}
`;
  document.head.appendChild(style);
}
stxPSInstallStyles();
state.planets.forEach(p=>stxPSResourceProfile(p,false));
