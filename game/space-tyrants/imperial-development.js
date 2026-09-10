/* Imperial development uses existing projects, supply ledgers and save objects. */
const STX_ID_PRIORITIES={Emergency:8,High:4,Normal:2,Low:1,Suspended:0};
const STX_ID_PLANS={Balanced:['city','factory','mine','defense'],Industrial:['factory','city','mine','research'],Mining:['mine','factory','city'],Shipbuilding:['shipyard','factory','training','city'],Trade:['city','mine','factory'],Fortress:['defense','training','city'],Population:['city','training','research'],Research:['research','city','factory'],Frontier:['city','mine','defense']};
const STX_ID_TRAITS=[['Titanium-rich crust','mining',1.2],['Ancient ruins','research',1.2],['Natural fortress','defense',1.2],['Underground caverns','housing',1.2],['Hostile biosphere','growth',.8],['Unstable tectonics','construction',.85],['Ideal orbital position','construction',1.15],['Exceptional farmland','growth',1.2],['Dense asteroid satellites','mining',1.15],['Violent electrical storms','construction',.9]];
function stxIDHistory(o,text){o.imperialHistory=o.imperialHistory||[];o.imperialHistory.unshift({time:state.simTime,text});o.imperialHistory=o.imperialHistory.slice(0,50)}
function stxIDEnsure(p){
  if(!p.imperialGeography){const seed=stxPSHash(p.id+':geography'),size=seed%5;
    p.imperialGeography={size:['Tiny','Small','Medium','Large','Massive'][size],sizeFactor:[.75,.9,1,1.15,1.3][size],gravity:['Low','Normal','High'][Math.floor(seed/5)%3],atmosphere:['None','Thin','Breathable','Dense','Toxic','Exotic'][Math.floor(seed/17)%6],traits:[seed%STX_ID_TRAITS.length,(seed+3)%STX_ID_TRAITS.length]};
  }
  p.imperialPlan=p.imperialPlan||'Balanced';p.imperialHistory=p.imperialHistory||[];
  return p;
}
function stxIDTrait(p,kind){stxIDEnsure(p);return p.imperialGeography.traits.reduce((n,i)=>n*(STX_ID_TRAITS[i]?.[1]===kind?STX_ID_TRAITS[i][2]:1),1)}
function stxIDProjects(p){return stxSDDescriptors(p)}
function stxIDWeight(d){return (STX_ID_PRIORITIES[d.q.imperialPriority||'Normal']??2)*(d.priority>=100?3:1)*clamp(Number(d.q.imperialAllocation)||1,1,100)}
function stxIDCapacity(p){return (1+Math.max(0,p.infra.factory-1)*.22+(p.infra.shipyard||0)*.12)*clamp(stxLPWorkforce(p),.2,1)*stxIDTrait(p,'construction')*(p.imperialGeography.gravity==='High'?.9:p.imperialGeography.gravity==='Low'?1.05:1)*(1-clamp(p.warDamage||0,0,.8))}
function stxIDShare(d){const total=stxIDProjects(d.p).reduce((n,x)=>n+stxIDWeight(x),0);return total?stxIDWeight(d)/total:0}
const STX_ID_capacity=stxIFCapacityShare;
stxIFCapacityShare=function(d){if(!d.q)return STX_ID_capacity(d);return Math.min(1,stxIDCapacity(d.p)*stxIDShare(d))*(d.q.imperialRush||1)};
const STX_ID_advance=stxSDAdvance;
stxSDAdvance=function(d,dt,rate){if(d.q.imperialPriority==='Suspended'||d.q.imperialCancelled)return 0;return STX_ID_advance(d,dt,rate)};
const STX_ID_physical=stxOLTickProject;
stxOLTickProject=function(p,q,dt){
  if(q.imperialPriority==='Suspended'||p.underAttack){if(q.commissioningEnds)q.commissioningEnds+=dt;return}
  const before=q.phase;STX_ID_physical(p,q,dt);
  if(before!=='operations'&&q.phase==='operations')stxIDHistory(p,`${q.name} completed`);
};
// Suspended projects keep delivered cargo but request no additional material.
const STX_ID_remaining=stxSDRemaining;
stxSDRemaining=function(d,r){return d.q.imperialPriority==='Suspended'?0:STX_ID_remaining(d,r)};
const STX_ID_source=stxOLSourceProject;
stxOLSourceProject=function(p,q,...args){if(q.imperialPriority==='Suspended')return;return STX_ID_source(p,q,...args)};
function stxIDFind(p,id){return stxIDProjects(p).find(d=>d.id===id)}
function stxIDPriority(p,id,value,allocation){const d=stxIDFind(p,id);if(p.owner!==0||!d||!(value in STX_ID_PRIORITIES))return false;if(allocation!=null&&(!Number.isFinite(allocation)||allocation<1||allocation>100))return false;d.q.imperialPriority=value;if(allocation!=null)d.q.imperialAllocation=allocation;return true}
function stxIDRefund(d){if(d.base)return stxIDRemoteRefund(d).total;const paid=d.kind==='physical'?d.q.delivered:d.q.stxSupply?.delivered,progress=stxSDProgress(d);return Object.fromEntries(Object.entries(paid||{}).map(([r,a])=>[r,Math.max(0,a-(d.need?.[r]||0)*progress)]).filter(([,n])=>n>1e-8))}
function stxIDCancel(p,id){
  const d=stxIDFind(p,id);if(!d||p.owner!==0||d.kind==='expansion')return false;if(d.base)return stxIDCancelRemote(d);
  const refund=stxIDRefund(d);for(const [r,n] of Object.entries(refund))p.stock[r]=(p.stock[r]||0)+n;
  const orders=new Set(Object.values(d.q.stxSupply?.orderIds||{}));p.orders=p.orders.filter(o=>!orders.has(o.id));
  // Already dispatched cargo remains physical and arrives in stock, unattached.
  for(const s of state.ships)if(s.stxProjectId===id||s.projectId===id||s.projectId===d.q.id||orders.has(s.orderId)){delete s.stxProjectId;delete s.projectId;delete s.orderId}
  for(const field of ['stxLocalProjects','stxOrbitalProjects','physicalProjects','buildQueue'])p[field]=(p[field]||[]).filter(q=>q!==d.q);
  for(const field of ['localProject','orbitalProject','scanProject','tradeStationProject','reconstruction'])if(p[field]===d.q)p[field]=null;
  p.localProject=p.stxLocalProjects[0]||null;p.orbitalProject=p.stxOrbitalProjects[0]||null;d.q.imperialCancelled=true;
  stxIDHistory(p,`${d.title} cancelled; unused materials returned`);return true;
}
function stxIDRushCost(d,multiplier){return Math.ceil(Object.values(d.need||{}).reduce((n,v)=>n+v,0)*(1-stxSDProgress(d))*(multiplier===3?1.5:.45))}
function stxIDRush(p,id,multiplier){const d=stxIDFind(p,id);if(!d||p.owner!==0||p.underAttack||![1.5,3].includes(multiplier)||(d.q.imperialRush||1)>=multiplier)return false;const cost=stxIDRushCost(d,multiplier);if(empire(0).credits<cost)return false;empire(0).credits-=cost;d.q.imperialRush=multiplier;stxIDHistory(p,`${d.title} accelerated ×${multiplier} for ${cost} credits`);return true}
const STX_ID_parallel=stxLPParallelTick;
stxLPParallelTick=function(p,dt,field,list,ticker){const before=[...(p[list]||[])];STX_ID_parallel(p,dt,field,list,(world,elapsed)=>{if(world[field]?.imperialPriority!=='Suspended')ticker(world,elapsed)});for(const q of before)if(!p[list].includes(q)){stxIDHistory(p,`${q.type} completed`);if(q.type==='city')p.capacity+=p.stxCityCapacity*(stxIDTrait(p,'housing')*p.imperialGeography.sizeFactor-1)}};
const STX_ID_growth=stxLPGrowPopulation;
stxLPGrowPopulation=function(p,dt){return STX_ID_growth(p,dt*stxIDTrait(p,'growth'))};
const STX_ID_generate=generateGalaxy;
generateGalaxy=function(){STX_ID_generate();state.planets.forEach(stxIDEnsure)};
const STX_ID_load=loadGame;
loadGame=function(){const ok=STX_ID_load();if(ok)state.planets.forEach(stxIDEnsure);return ok};
const STX_ID_tick=tickPlanet;
tickPlanet=function(p,dt){
  if(p.owner==null)return STX_ID_tick(p,dt);stxIDEnsure(p);
  const quality={...p.quality};for(const r of RESOURCES)p.quality[r]*=stxIDTrait(p,'mining');
  try{STX_ID_tick(p,dt)}finally{p.quality=quality}
  if(p.owner===0&&p.imperialAutomatic&&!p.underAttack&&state.simTime>=(p.imperialNextBuild||0)){
    p.imperialNextBuild=state.simTime+30;
    if(stxIDProjects(p).length<Math.max(1,Math.floor(stxIDCapacity(p)*2))){
      const types=STX_ID_PLANS[p.imperialPlan]||STX_ID_PLANS.Balanced,type=[...types].sort((a,b)=>(p.infra[a]||0)-(p.infra[b]||0)||types.indexOf(a)-types.indexOf(b))[0];
      // Explicitly authorized governor budget; no spending outside its allowance.
      const cost=['factory','shipyard'].includes(type)?0:3;
      if((p.imperialBudget||0)>=cost&&empire(0).credits>=cost){const sim=stxLPSimulating;stxLPSimulating=false;try{if(startLocalProject(p,type,'Authorized governor development'))p.imperialBudget-=cost}finally{stxLPSimulating=sim}}
    }
  }
};
function stxIDProjectHTML(p){return stxIDProjects(p).map(d=>{
  const pr=stxSDProgress(d),weight=d.q.imperialAllocation||1,share=stxIDShare(d),refund=stxECRCostText(stxIDRefund(d))||'None';
  return `<div class="project-row"><b>${stxRTEscape(d.title)} · ${Math.floor(pr*100)}%</b><progress max="1" value="${pr}"></progress><small>Capacity ${(stxIDCapacity(p)*100*share).toFixed(1)} · ${(share*100).toFixed(0)}% allocated · ETA ${stxIDEta(d)} · Speed ×${d.q.imperialRush||1}</small><small>Materials committed: ${stxRTEscape(stxECRCostText(d.kind==='physical'?d.q.delivered:d.q.stxSupply?.delivered||{}))}</small>${p.owner===0?`<label>Priority<select data-id-priority="${d.id}">${Object.keys(STX_ID_PRIORITIES).map(k=>`<option ${k===(d.q.imperialPriority||'Normal')?'selected':''}>${k}</option>`).join('')}</select></label><label>Labor weight<input type="number" min="1" max="100" value="${weight}" data-id-weight="${d.id}"></label>${[1.5,3].filter(n=>n>(d.q.imperialRush||1)).map(n=>`<button class="choice-btn" data-id-rush="${d.id}" data-mult="${n}">${n===3?'Imperial rush':'Priority construction'} ×${n} · ${stxIDRushCost(d,n)} cr</button>`).join('')}${d.kind!=='expansion'?`<button class="choice-btn" data-id-cancel="${d.id}">Cancel · return ${stxRTEscape(refund)}</button>`:''}`:''}</div>`}).join('')}
const STX_ID_panel=stxLPPanel;
stxLPPanel=function(p){stxIDEnsure(p);const g=p.imperialGeography;return STX_ID_panel(p)+`<section class="stx-lp-panel"><div class="section-label">IMPERIAL DEVELOPMENT</div><p>${g.size} · ${g.gravity} gravity · ${g.atmosphere} atmosphere</p><small>${g.traits.map(i=>`${STX_ID_TRAITS[i][0]} (${STX_ID_TRAITS[i][1]} ×${STX_ID_TRAITS[i][2]})`).join(' · ')}</small><p>Construction capacity: ${(stxIDCapacity(p)*100).toFixed(0)}</p>${p.owner===0?`<label>Development plan<select id="stxIDPlan">${Object.keys(STX_ID_PLANS).map(k=>`<option ${k===p.imperialPlan?'selected':''}>${k}</option>`).join('')}</select></label><label><input id="stxIDAuto" type="checkbox" ${p.imperialAutomatic?'checked':''}> Governor may start projects</label><label>Governor credit allowance<input id="stxIDBudget" type="number" min="0" max="10000" value="${p.imperialBudget||0}"></label><small>Allowance covers authorization fees. Governors also commit each project's normal material recipe. Manual orders always remain available.</small>`:''}${stxIDProjectHTML(p)}<details><summary>Imperial record</summary>${p.imperialHistory.map(h=>`<p>Cycle ${Math.floor(h.time/6)+1} — ${stxRTEscape(h.text)}</p>`).join('')||'<p>No recorded development yet.</p>'}</details></section>`};
const STX_ID_render=renderPlanet;
renderPlanet=function(){STX_ID_render();const p=state.selected,body=$('planetBody');if(!p||p.owner!==0||!body)return;const finish=()=>{saveGame(false);renderPlanet();updateHud(true)};
  if($('stxIDPlan'))$('stxIDPlan').onchange=e=>{p.imperialPlan=e.target.value;stxIDHistory(p,`Development plan: ${p.imperialPlan}`);finish()};
  if($('stxIDAuto'))$('stxIDAuto').onchange=e=>{p.imperialAutomatic=e.target.checked;finish()};
  if($('stxIDBudget'))$('stxIDBudget').onchange=e=>{p.imperialBudget=clamp(Number(e.target.value)||0,0,10000);finish()};
  body.querySelectorAll('[data-id-priority]').forEach(b=>b.onchange=()=>{stxIDPriority(p,b.dataset.idPriority,b.value);finish()});
  body.querySelectorAll('[data-id-weight]').forEach(b=>b.onchange=()=>{const d=stxIDFind(p,b.dataset.idWeight);if(d)stxIDPriority(p,d.id,d.q.imperialPriority||'Normal',Number(b.value));finish()});
  body.querySelectorAll('[data-id-rush]').forEach(b=>b.onclick=()=>{showToast(stxIDRush(p,b.dataset.idRush,Number(b.dataset.mult))?'Construction accelerated':'Rush unavailable');finish()});
  body.querySelectorAll('[data-id-cancel]').forEach(b=>b.onclick=()=>{const d=stxIDFind(p,b.dataset.idCancel);if(d&&window.confirm(`Cancel ${d.title}? Resources returned: ${stxECRCostText(stxIDRefund(d))||'None'}`)){stxIDCancel(p,d.id);finish()}});
};
function stxIDEta(d){
  if(d.q.imperialPriority==='Suspended'||d.p.underAttack)return 'Suspended';
  if(d.kind==='expansion')return 'Recruitment and freight';
  if(d.kind==='physical')return d.q.phase==='commissioning'?fmtEta(Math.max(0,(d.q.commissioningEnds||0)-state.simTime)):'Assembly and commissioning';
  if(!stxSDAllDelivered(d))return 'Awaiting materials';
  const p=d.p,q=d.q,rates={local:.0065*(1+p.infra.factory*.22)*(stxSDSourceIsImperial(q)?1.25:1),orbital:()=>orbitalProjectRate(p,q),reconstruction:.0045*(1+p.infra.factory*.18),scan:.0052*(1+p.infra.factory*.28+p.infra.research*.52)*(q.mandated?1.35:1),trade:.0036*(1+p.infra.factory*.24+p.infra.shipyard*.28)*(q.mandated?1.3:1),ship:()=>buildQueueRate(p)},base=typeof rates[d.kind]==='function'?rates[d.kind]():rates[d.kind];
  return base?`~${fmtEta((1-stxSDProgress(d))/Math.max(1e-9,base*stxIFCapacityShare(d)))}`:'Awaiting capacity';
}
// All geography bonuses advertised by the inspector affect real output.
const STX_ID_research=typeof stxPolicyResearch==='function'?stxPolicyResearch:null;
if(typeof STX_ID_research==='function')stxPolicyResearch=function(p,dt,...args){return STX_ID_research(p,dt*stxIDTrait(p,'research'),...args)};
const STX_ID_reviewRender=renderPlanet;
renderPlanet=function(){STX_ID_reviewRender();const p=state.selected,body=$('planetBody');if(!p||p.owner!==0||!body)return;body.querySelectorAll('[data-lp-build]').forEach(button=>{const type=button.dataset.lpBuild,need=Object.fromEntries(Object.entries(STX_PS_RECIPES[type]).map(([r,n])=>[r,n*STX_IM_LOCAL[type]])),credits=['factory','shipyard'].includes(type)?0:3;button.textContent=`Build ${type} · ${credits} cr`;button.onclick=()=>{if(!window.confirm(`Build ${type} at ${p.name}? Credits: ${credits}. Materials: ${stxECRCostText(need)}. Completion depends on workforce and supply. This project runs alongside existing construction.`))return;showToast(startLocalProject(p,type,'Player commission')?'Project authorized':'Project unavailable');saveGame(false);renderPlanet();updateHud(true)}})};

function stxIDRemoteRefund(d){const local={...(d.q.staged||{})},remote={},total={...local};for(const [r,n] of Object.entries(d.q.delivered||{})){remote[r]=Math.max(0,n-(d.need[r]||0)*stxSDProgress(d));total[r]=(total[r]||0)+remote[r]}return {local,remote,total}}
function stxIDCancelRemote(d){
  const {p,q,base}=d,refund=stxIDRemoteRefund(d),hasRemote=Object.values(refund.remote).some(n=>n>0);
  if(hasRemote&&!createShip('freighter',{id:p.id,x:base.x,y:base.y},p,p.owner,{cargo:refund.remote,vesselName:'Cancelled construction material return'}))return false;
  for(const [r,n] of Object.entries(refund.local))p.stock[r]=(p.stock[r]||0)+n;
  const ids=new Set(Object.values(q.stxSupply?.orderIds||{}));p.orders=p.orders.filter(o=>!ids.has(o.id));
  for(const s of state.ships)if(s.deepBaseId===base.id&&['construction','fortress-upgrade'].includes(s.stxDeepMission)){
    s.stxDeepTransit=false;s.from=p.id;s.to=p.id;s.startX=s.x;s.startY=s.y;s.distance=Math.max(1,dist(s,p));s.progress=0;delete s.targetX;delete s.targetY;delete s.deepBaseId;delete s.stxDeepMission;delete s.stxAction;
  }
  if(d.kind==='deep-base'){state.deepSpaceBases=state.deepSpaceBases.filter(b=>b!==base);p.deepSpaceProjectIds=(p.deepSpaceProjectIds||[]).filter(id=>id!==base.id)}else delete base.upgradeProject;
  q.imperialCancelled=true;stxIDHistory(p,`${d.title} cancelled; unused remote materials returning by freighter`);return true;
}
