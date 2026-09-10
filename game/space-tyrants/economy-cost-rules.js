/* Space Tyrants — explicit economy cost rules.
   Factories/shipyards use raw resources only; fleet commissioning charges credits;
   personnel transit charges a small credit fare; individual ship construction uses
   raw resources only. Loaded after living-planets.js so these are authoritative. */

const STX_ECR_VERSION=1;
const STX_ECR_FLEET_CREDITS_PER_VESSEL=2;
const STX_ECR_FREE_INFRA=new Set(["factory","shipyard"]);
const STX_ECR_SHIP_COST=Object.freeze({iron:4,titanium:3,helium:2,rare:1});

function stxECRRawOnlyNeed(need){
  const entries=Object.entries(need||{}).filter(([,v])=>Number(v)>0),manufactured=entries.filter(([r])=>r==="components"||r==="equipment").reduce((n,[,v])=>n+Number(v),0),raw=entries.filter(([r])=>r!=="components"&&r!=="equipment");
  if(!manufactured)return Object.fromEntries(raw);
  const rawTotal=raw.reduce((n,[,v])=>n+Number(v),0)||1;
  return Object.fromEntries(raw.map(([r,v])=>[r,Number((Number(v)+manufactured*(Number(v)/rawTotal)).toFixed(4))]));
}
function stxECRRawRecipe(recipe){
  const raw=stxECRRawOnlyNeed(recipe),total=Object.values(raw).reduce((n,v)=>n+Number(v),0)||1;
  return Object.fromEntries(Object.entries(raw).map(([r,v])=>[r,Number((Number(v)/total).toFixed(6))]));
}
function stxECRScaleCost(cost,count=1){return Object.fromEntries(Object.entries(cost).map(([r,v])=>[r,Number(v)*count]))}
function stxECRCostText(cost){return Object.entries(cost).map(([r,v])=>`${stxSDResourceLabel(r)} ${r==="trained"?Math.round(Number(v)*1e6).toLocaleString():Number(v).toLocaleString(undefined,{maximumFractionDigits:4})}`).join(" · ")}
function stxECRFleetCreditCost(count){return Math.max(1,Math.ceil(Number(count)*STX_ECR_FLEET_CREDITS_PER_VESSEL))}
function stxECRTransitCreditCost(from,to,count){return Math.max(1,Math.ceil(Number(count)/6)+Math.floor(dist(from,to)/1800))}

// Local development recipes: redistribute the manufactured-goods share across
// the raw materials already used by the recipe. Total material burden is stable.
STX_PS_RECIPES.factory=stxECRRawRecipe(STX_PS_RECIPES.factory);
STX_PS_RECIPES.shipyard=stxECRRawRecipe(STX_PS_RECIPES.shipyard);

// The physical infrastructure path has its own recipe system. Keep its existing
// scaling curve, but redistribute Components/Equipment into raw materials.
const STX_ECR_olBuildCost=stxOLBuildCost;
stxOLBuildCost=function(kind,p,option=null){
  const need=STX_ECR_olBuildCost(kind,p,option);
  return STX_ECR_FREE_INFRA.has(kind)?stxECRRawOnlyNeed(need):need;
};

function stxECRCancelManufacturedProjectOrders(p,q){
  const ids=q?.stxSupply?.orderIds;if(!ids)return;
  for(const r of ["components","equipment"]){
    const id=ids[r],o=id&&p.orders?.find(x=>x.id===id),carrier=id&&state.ships.some(s=>s.orderId===id);
    if(o&&!carrier){o.filled=o.amount;o.status="filled"}
    delete ids[r];
  }
}
function stxECRNormalizeLocalProject(p,q){
  if(!q||!STX_ECR_FREE_INFRA.has(q.type)||q.stxEconomyCostRules===STX_ECR_VERSION)return;
  const recipe=STX_PS_RECIPES[q.type],cost=Math.max(18,Number(q.cost)||30);
  q.stxRecipeNeed=Object.fromEntries(Object.entries(recipe).map(([r,share])=>[r,cost*share]));
  stxECRCancelManufacturedProjectOrders(p,q);
  if(Number(q.stxCredits)>0){const e=empire(p.owner);if(e)e.credits+=Number(q.stxCredits);q.stxCredits=0}
  q.stxEconomyCostRules=STX_ECR_VERSION;
}
function stxECRNormalizePhysicalProject(p,q){
  if(!q||q.phase==="operations"||!STX_ECR_FREE_INFRA.has(q.kind)||q.stxEconomyCostRules===STX_ECR_VERSION)return;
  q.need=stxOLBuildCost(q.kind,p,q.option||{});
  q.delivered=q.delivered||{};for(const r of Object.keys(q.need))q.delivered[r]=Number(q.delivered[r]||0);
  delete q.delivered.components;delete q.delivered.equipment;
  if(Number(q.creditCost)>0){const e=empire(p.owner);if(e)e.credits+=Number(q.creditCost);q.creditCost=0}
  q.stxEconomyCostRules=STX_ECR_VERSION;
}
function stxECRNormalizePlanet(p){
  if(!p||p.owner==null)return;
  for(const q of p.stxLocalProjects||[])stxECRNormalizeLocalProject(p,q);
  if(p.localProject)stxECRNormalizeLocalProject(p,p.localProject);
  for(const q of p.physicalProjects||[])stxECRNormalizePhysicalProject(p,q);
}
function stxECRNormalizeState(){for(const p of state.planets||[])stxECRNormalizePlanet(p)}

const STX_ECR_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_ECR_generateGalaxy();stxECRNormalizeState()};
const STX_ECR_loadGame=loadGame;
loadGame=function(){const ok=STX_ECR_loadGame();if(ok)stxECRNormalizeState();return ok};

// Factory and shipyard authorization has no credit gate. The underlying legacy
// path still applies its 3-credit fee, so temporarily satisfy that gate and then
// refund exactly the fee actually attached to the new project.
const STX_ECR_startLocalProject=startLocalProject;
startLocalProject=function(p,type,source="Player commission"){
  if(!STX_ECR_FREE_INFRA.has(type))return STX_ECR_startLocalProject(p,type,source);
  const e=p&&empire(p.owner);if(!e)return false;
  const before=(p.stxLocalProjects||[]).length,topup=Math.max(0,3-Number(e.credits||0));e.credits+=topup;
  let ok=false;
  try{ok=STX_ECR_startLocalProject(p,type,source)}finally{
    const q=(p.stxLocalProjects||[]).slice(before).find(x=>x.type===type)||((p.localProject?.type===type)?p.localProject:null),charged=ok?Number(q?.stxCredits||0):0;
    if(charged>0)e.credits+=charged;e.credits-=topup;
    if(ok&&q){q.stxCredits=0;stxECRNormalizeLocalProject(p,q)}
  }
  return ok;
};

// Physical factory/shipyard projects also become raw-resource-only and have no
// authorization-credit charge.
const STX_ECR_olQueueProject=stxOLQueueProject;
stxOLQueueProject=function(p,kind,option={}){
  if(!STX_ECR_FREE_INFRA.has(kind))return STX_ECR_olQueueProject(p,kind,option);
  const e=empire(0);if(!e)return false;
  const expected=Math.ceil(Object.values(stxOLBuildCost(kind,p,option)).reduce((n,v)=>n+Number(v||0),0)*.11),before=(p.physicalProjects||[]).length,topup=Math.max(0,expected-Number(e.credits||0));e.credits+=topup;
  let ok=false;
  try{ok=STX_ECR_olQueueProject(p,kind,option)}finally{
    const q=(p.physicalProjects||[]).slice(before).find(x=>x.kind===kind),charged=ok?Number(q?.creditCost||0):0;
    if(charged>0)e.credits+=charged;e.credits-=topup;
    if(ok&&q){q.creditCost=0;stxECRNormalizePhysicalProject(p,q);updateHud(true)}
  }
  return ok;
};

// Fleet commissioning still consumes the ships and personnel physically staged
// on the planet. Credits are the only commissioning fee; no materials are spent.
const STX_ECR_fleetPlan=stxLPFleetPlan;
stxLPFleetPlan=function(p,count){
  const plan=STX_ECR_fleetPlan(p,count);if(!plan.ok)return plan;
  const creditCost=stxECRFleetCreditCost(plan.vessels),e=empire(p.owner);
  if(!e||Number(e.credits||0)<creditCost)return {...plan,ok:false,creditCost,reason:`Need ${creditCost} credits to commission ${plan.vessels} vessels`};
  return {...plan,creditCost,reason:`${plan.reason} · Commission fee ${creditCost} credits`};
};
const STX_ECR_mobilize=stxLPMobilize;
stxLPMobilize=function(p,count=15,role="fleet",name=""){
  const plan=stxLPFleetPlan(p,count);if(!plan.ok)return false;
  const f=STX_ECR_mobilize(p,count,role,name);if(!f)return false;
  const e=empire(p.owner);e.credits=Math.max(0,Number(e.credits||0)-plan.creditCost);f.commissionCredits=plan.creditCost;return f;
};

// Build one reserve ship directly at any player shipyard. This is deliberately
// raw-resource-only: no credits, Components, Equipment, or personnel are charged.
function stxECRShipPlan(p,count=1){
  const n=Number(count),need=stxECRScaleCost(STX_ECR_SHIP_COST,n);
  if(!p||p.owner!==0||p.underAttack)return{ok:false,need,reason:"Choose a safe owned planet"};
  if(!Number.isSafeInteger(n)||n<1||n>100)return{ok:false,need,reason:"Choose 1–100 ships"};
  if((p.infra.shipyard||0)<1)return{ok:false,need,reason:"An operational shipyard is required"};
  const missing=Object.entries(need).filter(([r,a])=>Number(p.stock[r]||0)+1e-9<a);
  if(missing.length)return{ok:false,need,reason:`Need ${missing.map(([r,a])=>`${stxSDResourceLabel(r)} ${a}`).join(", ")}`};
  return{ok:true,count:n,need,reason:`${stxECRCostText(need)} · raw resources only`};
}
function stxECRBuildShip(p,count=1){
  const plan=stxECRShipPlan(p,count);if(!plan.ok)return false;
  for(const [r,a] of Object.entries(plan.need))p.stock[r]-=a;
  stxLPEnsure(p);p.reserveVessels+=plan.count;p.mandateGlow=1;
  logEvent(`${p.name} completed ${plan.count===1?"a reserve ship":`${plan.count} reserve ships`} using local raw materials.`,"good");return true;
}
stxLPRush=function(p,count=1){return stxECRBuildShip(p,count)};

// Personnel transit is planet-to-planet only. It moves no reserve ships and
// consumes no materials; the fare is a small credit amount based on crew size
// and distance.
function stxECRTransitPlan(from,to,count){
  const n=Number(count),crew=n*STX_LP_CREW,cost=from&&to&&Number.isFinite(n)?stxECRTransitCreditCost(from,to,n):0;
  if(!from||!to||from.owner!==0||to.owner!==0||from===to||from.underAttack||to.underAttack)return{ok:false,crew,cost,reason:"Choose two safe owned planets"};
  if(!Number.isSafeInteger(n)||n<1||n>500)return{ok:false,crew,cost,reason:"Choose a valid fleet-size crew batch"};
  if(Number(from.stock.trained||0)+1e-12<crew)return{ok:false,crew,cost,reason:`Need ${Math.round(crew*1e6).toLocaleString()} personnel at ${from.name}`};
  if(Number(empire(0).credits||0)<cost)return{ok:false,crew,cost,reason:`Need ${cost} credits for transit`};
  return{ok:true,crew,cost,reason:`${Math.round(crew*1e6).toLocaleString()} personnel · ${cost} credits`};
}
stxLPNearby=function(p){return owned(p.owner).filter(x=>x!==p&&!x.underAttack).sort((a,b)=>dist(a,p)-dist(b,p))};
stxLPTransfer=function(from,to,count){
  const plan=stxECRTransitPlan(from,to,count);if(!plan.ok)return false;
  const ship=createShip("supply",from,to,0,{cargo:{trained:plan.crew},stxLPMuster:true,stxLPPersonnelTransit:true,vesselName:`${from.name} personnel transit`,strength:0});
  if(!ship)return false;
  from.stock.trained-=plan.crew;empire(0).credits-=plan.cost;ship.transitCredits=plan.cost;return true;
};

// Keep command descriptions synchronized with the authoritative actions.
for(const c of COMMANDS){
  if(c.id==="reserveActivation"){c.desc="Commission six staged reserve vessels and 1,200 local personnel for 12 credits.";c.effects=["Uses staged ships and personnel","Credits are the only commissioning fee"]}
  if(c.id==="fleetCommission"){c.desc="Commission 15 staged reserve vessels and 3,000 local personnel for 30 credits.";c.effects=["Uses staged ships and personnel","No Components or Equipment are consumed","Remains at the named staging world"]}
  if(c.id==="stxEmergencyVessels"){
    c.title="Build Reserve Ship";c.desc=`Build one reserve ship at a shipyard for ${stxECRCostText(STX_ECR_SHIP_COST)}.`;c.effects=["1 reserve ship","Raw resources only","No credits, Components or Equipment"];
    c.target=()=>playerWorlds().find(p=>stxECRShipPlan(p,1).ok);c.score=()=>c.target()?60:0;c.apply=(e,p)=>stxECRBuildShip(p,1);
  }
}

// Update the living-planet inspector without duplicating its large renderer.
const STX_ECR_lpPanel=stxLPPanel;
stxLPPanel=function(p){
  let html=STX_ECR_lpPanel(p);if(p.owner!==0)return html;
  const draft=stxLPDraft(p),count=Number(draft.count)||6,people=Math.round(count*STX_LP_CREW*1e6).toLocaleString(),ship=stxECRShipPlan(p,1),esc=stxRTEscape;
  html=html.replace(`<summary>Commission Fleet · ${esc(p.name)}</summary>`,`<summary>Fleet & Shipyard Orders · ${esc(p.name)}</summary>`);
  html=html.replace(`All vessels and 200 personnel per vessel come from ${esc(p.name)}. Nearby reserves must arrive by transport first.`,`Commissioning uses reserve ships and 200 local personnel per vessel. Credits are the only commissioning fee; move personnel between owned planets with transit below.`);
  html=html.replace(/<button class="choice-btn" data-lp-action="rush">.*?<\/button>/,`<button class="choice-btn" data-lp-action="rush" ${ship.ok?"":"disabled"}>Build 1 reserve ship · ${stxECRCostText(STX_ECR_SHIP_COST)}</button>`);
  html=html.replace(`<div class="section-label">NEARBY STAGING SOURCES · 900u RANGE</div>`,`<div class="section-label">PERSONNEL TRANSIT · OWNED PLANETS</div><p class="subtle">The selected fleet size sends ${people} personnel. Reserve ships stay on their planet. Transit costs only a small credit fare; no Components, Equipment or raw materials.</p>`);
  html=html.replace(/<button class="choice-btn" data-lp-source="([^"]+)">Transfer selected vessel count here<\/button>/g,(_all,id)=>{const source=state.planets.find(q=>String(q.id)===String(id)),plan=stxECRTransitPlan(source,p,count);return `<button class="choice-btn" data-lp-source="${id}" ${plan.ok?"":"disabled"}>Send ${people} personnel to ${esc(p.name)} · ${plan.cost||"—"} cr</button>`});
  html=html.replace(`Order development · 3 cr authorization + listed materials`,`Order development · listed materials (factory & shipyard: raw resources only, no credit fee)`);
  return html;
};

// Replace only the two handlers whose meaning changed; the living-planets layer
// continues to own the rest of the inspector lifecycle.
const STX_ECR_renderPlanet=renderPlanet;
renderPlanet=function(){
  STX_ECR_renderPlanet();const p=state.selected,body=$("planetBody");if(!p||p.owner!==0||!body)return;const draft=stxLPDraft(p);
  const build=body.querySelector?.('[data-lp-action="rush"]');if(build)build.onclick=()=>{const plan=stxECRShipPlan(p,1),ok=stxECRBuildShip(p,1);showToast(ok?"Reserve ship built from raw resources":plan.reason);saveGame(false);renderPlanet();updateHud(true)};
  body.querySelectorAll?.("[data-lp-source]").forEach(b=>b.onclick=()=>{const source=state.planets.find(q=>String(q.id)===String(b.dataset.lpSource)),plan=stxECRTransitPlan(source,p,Number(draft.count)),ok=stxLPTransfer(source,p,Number(draft.count));showToast(ok?`Personnel transit dispatched · ${plan.cost} credits`:plan.reason);saveGame(false);renderPlanet();updateHud(true)});
};
