/* Mandate cards quote the same project recipes used at authorization. Receipts
   reject excess spending instead of permitting a hidden fallback purchase. */
const STX_IM_LOCAL={city:36,mine:30,factory:46,training:40,defense:34,shipyard:62,research:55};
function stxIMLocal(type,worlds){const need={};for(const p of worlds)for(const [r,n] of Object.entries(STX_PS_RECIPES[type]))need[r]=(need[r]||0)+n*STX_IM_LOCAL[type];return {credits:['factory','shipyard'].includes(type)?0:worlds.length*3,resources:need,worlds:worlds.map(p=>p.id),completion:'Construction; depends on labor and delivered materials'}}
function stxIMQuote(c){
  const p=c.targetObj,quote={credits:0,resources:{},worlds:p?.id?[p.id]:[],completion:'Immediate order'};
  if(c.policyId){const d=STX_POLICIES[c.policyId],worlds=playerWorlds();quote.credits=d.cost;quote.completion=`Active for ${d.duration} seconds`;quote.recurring={};for(const [r,n] of Object.entries(d.upkeep||{}))quote.recurring[r]=n*d.duration*worlds.length;
    if(c.policyId==='veteran-recall')quote.resources.equipment=worlds.filter(p=>(p.infra.shipyard||p.infra.training)&&p.pop>.02&&p.stock.equipment>=2).slice(0,3).reduce((n,p)=>n+Math.min(.003,p.pop*.008,p.stock.equipment/800)*800,0);return quote;
  }
  if(['fleetCommission','reserveActivation','fleet','shipbuilding'].includes(c.id)){const count=c.id==='reserveActivation'?6:c.id==='shipbuilding'?30:15;quote.credits=count*2;quote.personnel=count*200;quote.vessels=count;return quote}
  if(c.id==='stxEmergencyVessels'){quote.resources={...STX_ECR_SHIP_COST};return quote}
  if(['mineWorks','factoryWave','distributed'].includes(c.id)){const type=c.id==='mineWorks'?'mine':'factory',worlds=c.id==='mineWorks'?stxMineTargets().slice(0,4):c.id==='distributed'?playerWorlds().filter(p=>p.infra.factory<1).slice(0,3):stxFactoryTargets().slice(0,4);return stxIMLocal(type,worlds)}
  if(c.id==='frontierShipyard'||String(c.id).startsWith('path-')){const type=c.id==='frontierShipyard'||/yard/.test(c.id)?'shipyard':/sensor|research/.test(c.id)?'research':/fleet/.test(c.id)?'shipyard':'factory',world=p||stxMEWorldForProject(type);return stxIMLocal(type,world?[world]:[])}
  if(c.id==='spaceStation'||c.id==='sectorBase'){quote.resources={...ORBITAL_COST[c.id==='spaceStation'?'station':'base']};quote.completion='Orbital construction';return quote}
  if(c.id==='sensorArray'){quote.resources={...STX_SCAN_COST};quote.completion='Sensor construction';return quote}
  if(c.id==='tradeStation'){quote.resources={...(STX_TRADE_STATION_LEVELS[p?.tradeStation?.level||0]?.need||{})};quote.completion='Trade station construction';return quote}
  if(c.id==='claim'||c.id==='homesteads'){const source=p&&stxDIExpansionSource(p);if(source){const goal=Math.max(.004,clamp(Math.min(.025,source.pop*.035),.003,.025));quote.credits=8+Math.min(.025,source.pop*.035)*200;quote.resources={iron:24,silicates:22,titanium:8,helium:16,components:14,equipment:9,trained:Math.max(.00015,goal*.035)};quote.worlds=[source.id]}quote.completion='Recruitment, construction and transit';return quote}
  return quote;
}
function stxIMText(q){return `${q.credits||Object.keys(q.resources).length?`Credits: ${q.credits} · Resources: ${stxECRCostText(q.resources)||'None'}`:'Cost: FREE'}${q.personnel?` · ${q.personnel.toLocaleString()} personnel · ${q.vessels} staged vessels`:''}${Object.keys(q.recurring||{}).length?` · Maximum additional policy spend: ${stxECRCostText(q.recurring)}`:''} · ${q.completion}`}
// Legacy fleet programs and factory mandates must respect the current cost
// rules, and may not silently commission additional paid fleets later.
for(const c of COMMANDS){
  if(['fleet','shipbuilding'].includes(c.id)){const count=c.id==='fleet'?15:30;c.apply=()=>{const p=playerWorlds().find(p=>stxLPFleetPlan(p,count).ok);return p?!!stxLPMobilize(p,count):false};c.desc=`Commission ${count} staged vessels. Credits are the only fee; no deferred fleet purchases.`}
  if(c.id==='distributed')c.apply=()=>playerWorlds().filter(p=>p.infra.factory<1).slice(0,3).map(p=>startLocalProject(p,'factory','Distributed manufacturing')).some(Boolean);
  if(c.id==='frontierShipyard')c.apply=(e,p)=>p&&startLocalProject(p,'shipyard','Imperial shipyard charter');
}
function stxIMSpending(){return {credits:empire(0).credits,stock:Object.fromEntries([...new Set([...RESOURCES,'components','equipment','trained'])].map(r=>[r,empireResource(0,r)])),projects:new Set(playerWorlds().flatMap(p=>stxIDProjects(p)).map(d=>d.id))}}
function stxIMWithin(before,quotes){const credits=quotes.reduce((n,q)=>n+q.credits,0),need={};for(const q of quotes)for(const [r,n] of Object.entries(q.resources))need[r]=(need[r]||0)+n;
  if(before.credits-empire(0).credits>credits+1e-6)return false;
  const planned={};for(const d of playerWorlds().flatMap(p=>stxIDProjects(p)))if(!before.projects.has(d.id))for(const [r,n] of Object.entries(d.need||{}))planned[r]=(planned[r]||0)+n;
  for(const [r,n] of Object.entries(before.stock)){if(r==='trained')continue;if(Math.max(n-empireResource(0,r),planned[r]||0)>(need[r]||0)+1e-5)return false}return true;
}
const STX_IM_render=renderCommands;
renderCommands=function(){STX_IM_render();const cards=$('commandGrid')?.children||[];state.commandChoices.forEach((c,i)=>{const q=stxIMQuote(c);c.imperialQuote=q;cards[i]?.insertAdjacentHTML('beforeend',`<div class="command-target"><b>${stxRTEscape(stxIMText(q))}</b></div>`)});
  const selected=[...state.commandSelected].map(i=>state.commandChoices[i]).filter(Boolean);if(selected.length&&$('issueBtn'))$('issueBtn').textContent=`Confirm ${selected.length} mandate${selected.length>1?'s':''} · ${selected.reduce((n,c)=>n+stxIMQuote(c).credits,0)} credits`;
};
const STX_IM_issue=issueCommands;
issueCommands=function(){
  const selected=[...state.commandSelected].map(i=>state.commandChoices[i]).filter(Boolean),quotes=selected.map(stxIMQuote);if(!selected.length)return STX_IM_issue();
  if(selected.some((c,i)=>c.imperialQuote&&JSON.stringify(c.imperialQuote)!==JSON.stringify(quotes[i]))){renderCommands();showToast('Mandate terms changed. Review the refreshed costs before confirming.');return false}
  if(quotes.reduce((n,q)=>n+q.credits,0)>empire(0).credits){showToast('Treasury cannot cover the selected mandates');return false}
  const before=stxIMSpending(),undo=stxActionCheckpoint([state,stxFCAQueue]),oldFallback=stxMEFallbackFor;
  // A free policy or order cannot trigger an unrelated paid fallback project.
  stxMEFallbackFor=()=>false;
  try{STX_IM_issue();if(!stxIMWithin(before,quotes)){undo();$('commandModal').hidden=false;renderCommands();saveGame(false);showToast('Order held: execution exceeded its advertised cost. No spending retained.');return false}}
  catch(error){undo();$('commandModal').hidden=false;saveGame(false);throw error}
  finally{stxMEFallbackFor=oldFallback}
  empire(0).imperialMandateReceipt={time:state.simTime,titles:selected.map(c=>c.title),credits:before.credits-empire(0).credits,authorized:quotes};saveGame(false);return true;
};
// Policy running costs have a fixed lifetime allowance. New colonies cannot
// cause an already authorized policy to exceed its displayed maximum.
const STX_IM_activate=stxPolicyActivate;
stxPolicyActivate=function(owner,id){const q=owner===0?stxIMQuote({policyId:id}):null,ok=STX_IM_activate(owner,id);if(ok&&q){const a=stxPolicyState(empire(owner)).active.find(a=>a.id===id);if(a)a.imperialAllowance={...q.recurring}}return ok};
const STX_IM_pay=stxPolicyPay;
stxPolicyPay=function(p,costs,dt){if(p.owner!==0)return STX_IM_pay(p,costs,dt);const active=stxPolicyState(empire(0)).active.find(a=>STX_POLICIES[a.id]?.upkeep===costs);if(!active?.imperialAllowance)return STX_IM_pay(p,costs,dt);
  const factor=Object.entries(costs).reduce((n,[r,c])=>c>0?Math.min(n,(active.imperialAllowance[r]||0)/Math.max(1e-12,c*dt)):n,1),paid=STX_IM_pay(p,costs,dt*factor);for(const [r,c] of Object.entries(costs))active.imperialAllowance[r]=Math.max(0,(active.imperialAllowance[r]||0)-c*dt*factor*paid);return paid*factor;
};
const STX_IM_transmissionMarkup=stxTXActionMarkup;
stxTXActionMarkup=function(type,q){let html=STX_IM_transmissionMarkup(type,q);if(type==='military'&&q.type!=='capital'){const total=Math.max(Number(q.creditCost)||0,stxECRFleetCreditCost(q.type==='patrol'?6:15));html+=`<p class="subtle">Total commission: ${total} credits, including commissioning fee. No materials.</p>`}return html};
