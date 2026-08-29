/* Space Tyrants — explicit, unrestricted fleet command allocation.
   Every surviving named ship formation can be redirected. Current activity only
   determines automatic selection order: idle/patrol, en route, other duty,
   then active combat. Invade and Concentrate Forces always use a command panel. */

let stxFCAQueue=[],stxFCAActive=false,stxFCAResume=1,stxFCAUsedTargets=new Set(),stxFCACommitted=new Set();
let stxFCADirect=false,stxFCAUiDestinations=[],stxFCAAllocations={};

function stxFCAState(){
  const s=state.stxFleetAllocation||(state.stxFleetAllocation={phase:0,lastUpgradeOffer:-4});
  if(!Number.isFinite(s.phase))s.phase=0;
  if(!Number.isFinite(s.lastUpgradeOffer))s.lastUpgradeOffer=-4;
  return s;
}
function stxFCABattleContext(f){
  for(const battle of state.battles){
    if((battle.attackerFleetIds||[]).includes(f?.id))return{battle,side:"attacker",planet:state.planets.find(p=>p.id===battle.planetId)};
    if((battle.defenderFleetIds||[]).includes(f?.id))return{battle,side:"defender",planet:state.planets.find(p=>p.id===battle.planetId)};
  }
  return null;
}
function stxFCABattleIds(){
  const out=new Set();state.battles.forEach(b=>{(b.attackerFleetIds||[]).forEach(x=>out.add(x));(b.defenderFleetIds||[]).forEach(x=>out.add(x))});return out;
}
function stxFCATransit(f){return state.ships.find(s=>s.fleetId===f?.id)||null}
function stxFCAEffectiveStrength(f){
  const ctx=stxFCABattleContext(f);if(!ctx)return Number(stxFCATransit(f)?.strength??f?.strength)||0;
  const ids=ctx.battle[`${ctx.side}FleetIds`]||[],records=ids.map(fleetRecord).filter(Boolean),weight=Math.max(.01,Number(f.strength)||1),total=records.reduce((n,x)=>n+Math.max(.01,Number(x.strength)||1),0)||weight;
  return Math.max(0,(Number(ctx.battle[`${ctx.side}Strength`])||0)*weight/total);
}
function stxFCACommandableFleet(f,includeCommitted=false){
  return !!f&&f.owner===0&&!f.destroyed&&(includeCommitted||!stxFCACommitted.has(f.id))&&stxFCAEffectiveStrength(f)>.05;
}
/* Kept as the public compatibility predicate for older tactical layers. Unlike
   the former implementation it deliberately ignores location and activity. */
function stxFCAFreeFleet(f){return stxFCACommandableFleet(f)}
function stxFCAStatusBucket(f){
  if(stxFCABattleContext(f))return{priority:4,key:"combat",label:"Active combat"};
  if(stxFCATransit(f))return{priority:2,key:"enroute",label:"En route"};
  const text=`${f.status||""} ${f.readiness||""} ${f.role||""}`.toLowerCase(),assigned=!!f.stxAssignment||!!f.stxRefit||/defend|escort|reinforc|rally|concentrat|holding|occupy|repair|return|stage|upgrade|refit/.test(text);
  if(!assigned&&(!text.trim()||/idle|patrol|commissioned|stationed|ready|orbit/.test(text)))return{priority:1,key:"idle",label:"Idle / patrol"};
  return{priority:3,key:"other",label:"Other assignments"};
}
function stxFCAPosition(f){
  const transit=stxFCATransit(f);if(transit)return{x:Number(transit.x)||0,y:Number(transit.y)||0};
  const battle=stxFCABattleContext(f);if(battle?.planet)return battle.planet;
  return state.planets.find(p=>p.id===f.location)||state.planets.find(p=>p.id===f.homePort)||playerWorlds()[0]||null;
}
function stxFCADestinationPlanet(target){return target?.planet||target||null}
function stxFCADistanceToTargets(f,targets){
  const from=stxFCAPosition(f),list=(Array.isArray(targets)?targets:[targets]).map(stxFCADestinationPlanet).filter(Boolean);if(!from||!list.length)return Infinity;
  return Math.min(...list.map(p=>Math.hypot((from.x||0)-(p.x||0),(from.y||0)-(p.y||0))));
}
function stxFCAFree(target=null){
  const targets=Array.isArray(target)?target:[target].filter(Boolean);
  return state.fleets.filter(f=>stxFCACommandableFleet(f)).sort((a,b)=>{
    const ap=stxFCAStatusBucket(a).priority,bp=stxFCAStatusBucket(b).priority;if(ap!==bp)return ap-bp;
    const ad=stxFCADistanceToTargets(a,targets),bd=stxFCADistanceToTargets(b,targets);if(ad!==bd)return ad-bd;
    return (Number(b.strength)||0)-(Number(a.strength)||0)||String(a.id).localeCompare(String(b.id));
  });
}
function stxFCAWars(){return state.wars.filter(w=>w.active&&(w.a===0||w.b===0))}
function stxFCATargets(kind){
  if(kind==="invade"){
    const foes=new Set(stxFCAWars().map(w=>w.a===0?w.b:w.a));
    return state.planets.filter(p=>p.owner!==null&&p.owner!==0&&foes.has(p.owner)).sort((a,b)=>{
      const ad=typeof stxNearestPlayerDistance==="function"?stxNearestPlayerDistance(a):0,bd=typeof stxNearestPlayerDistance==="function"?stxNearestPlayerDistance(b):0;
      return ad-bd||String(a.name).localeCompare(String(b.name));
    });
  }
  return playerWorlds().slice().sort((a,b)=>kind==="defend"?borderThreat(b)-borderThreat(a):String(a.name).localeCompare(String(b.name)));
}
function stxFCAConcentrationTargets(){
  const worlds=playerWorlds().slice().sort((a,b)=>String(a.name).localeCompare(String(b.name))).map(p=>({key:`planet:${p.id}`,type:"planet",id:p.id,name:p.name,planet:p,subtitle:"Player world"}));
  const facilities=state.planets.filter(p=>p.owner===0).flatMap(p=>(p.orbitalFacilities||[]).filter(f=>(Number(f.hp)||0)>0).map(f=>({key:`facility:${f.id}`,type:"facility",id:f.id,name:f.name,planet:p,facility:f,subtitle:`Orbital station at ${p.name}`}))).sort((a,b)=>String(a.name).localeCompare(String(b.name)));
  return [...worlds,...facilities];
}
function stxFCAUpgradeCandidates(){
  const engaged=stxFCABattleIds();
  return state.fleets.filter(f=>f.owner===0&&!f.destroyed&&!f.stxRefit&&!engaged.has(f.id)&&!stxFCATransit(f)&&f.location&&state.planets.find(p=>p.id===f.location)?.owner===0&&stxFCAEffectiveStrength(f)>.05&&state.simTime-(f.lastUpgradeAt??-999)>70).sort((a,b)=>(a.strength||0)-(b.strength||0));
}
function stxFCAClear(f,removeDock=true){
  if(!f)return;f.stxAssignment=null;
  if(removeDock){if(typeof stxOLRemoveDocking==="function")stxOLRemoveDocking(f);else f.dockedAt=null}
}

function stxFCAIsInvade(c){
  if(c?.stxFleetOrderKind==="invade")return true;
  if(typeof stxWDBIsInvasion==="function")return stxWDBIsInvasion(c);
  if(typeof stxDGIsInvasionChoice==="function")return stxDGIsInvasionChoice(c);
  return /invasion|\binvade\b/i.test(`${c?.id||""} ${c?.title||""}`);
}
function stxFCAIsConcentrate(c){return c?.stxFleetOrderKind==="concentrate"||/\bconcentrat(e|ion)/i.test(`${c?.id||""} ${c?.title||""}`)}
function stxFCAChoice(kind){
  const total=stxFCAFree().length,upgrade=stxFCAUpgradeCandidates().length;
  const base={stxFleetOrderKind:kind,targetObj:null,apply:()=>{stxFCAQueue.push({kind});return true}};
  if(kind==="invade")return{...base,id:`fca-invade-${Math.floor(state.simTime)}`,cat:"War",title:"Invade",desc:"Choose a ship count and one or two enemy planets. The order launches only after your confirmation.",effects:[`${total} surviving ship${total===1?"":"s"}`,"Choose up to two enemy planets","Combat ships are selected last"]};
  if(kind==="defend")return{...base,id:`fca-defend-${Math.floor(state.simTime)}`,cat:"Military",title:"Defend",desc:"Choose one of your planets and assign a specific number of surviving ships to defend it.",effects:[`${total} surviving ships`,"Choose friendly target","Existing duties affect priority only"]};
  if(kind==="concentrate")return{...base,id:`fca-concentrate-${Math.floor(state.simTime)}`,cat:"War",title:"Concentrate Forces",desc:"Choose a ship count and assemble them at one player planet or orbital station.",effects:[`${total} surviving ship${total===1?"":"s"}`,"Planets and stations are valid","Combat ships are selected last"]};
  return{...base,id:`fca-upgrade-${Math.floor(state.simTime)}`,cat:"Military",title:"Upgrade a Fleet",desc:"Choose one named fleet to refit. It must remain at its current friendly planet until the work completes.",effects:[`${upgrade} eligible fleets`,"Uses Components, Equipment, Titanium","Fleet is unavailable during refit"]};
}
function stxFCADisableLegacyInvasion(){
  const invasion=COMMANDS.find(c=>c.id==="invasion");if(invasion){invasion.score=()=>0;invasion.cat="War";invasion.title="Invade"}
}
function stxFCAChoiceCandidate(c){
  if(!c||stxFCAIsInvade(c)||stxFCAIsConcentrate(c)||c.stxFleetOrderKind)return null;
  let targetObj=c.targetObj||null;if(typeof c.target==="function"&&!targetObj){try{targetObj=c.target()||null}catch(_){return null}if(!targetObj)return null}
  const choice={...c,targetObj};if(typeof stxDGCanExecuteChoice==="function"&&!stxDGCanExecuteChoice(choice))return null;return choice;
}
function stxFCAWarExtras(existing){
  const out=[],ids=new Set();
  const add=c=>{const choice=stxFCAChoiceCandidate(c);if(!choice||ids.has(choice.id)||out.length>=2)return;ids.add(choice.id);out.push(choice)};
  existing.forEach(add);
  if(out.length<2){
    COMMANDS.map(c=>{let score=0;try{score=Number(c.score?.()||0)+rand(-10,10)}catch(_){score=0}return{c,score}}).filter(x=>x.score>5).sort((a,b)=>b.score-a.score).forEach(x=>add(x.c));
  }
  return out.slice(0,2);
}
function stxFCAFillPeaceHand(existing){
  const out=[],ids=new Set();
  const add=c=>{const choice=c?.stxFleetOrderKind==="upgrade"?c:stxFCAChoiceCandidate(c);if(!choice||ids.has(choice.id)||out.length>=4||(typeof stxWarOnlyDirective==="function"&&stxWarOnlyDirective(choice)))return;ids.add(choice.id);out.push(choice)};
  existing.forEach(add);
  if(out.length<4)COMMANDS.map(c=>{let score=0;try{score=Number(c.score?.()||0)+rand(-8,8)}catch(_){score=0}return{c,score}}).filter(x=>x.score>5).sort((a,b)=>b.score-a.score).forEach(x=>add(x.c));
  return out.slice(0,4);
}
function stxFCAPostHand(){
  if($("commandModal").hidden)return;const s=stxFCAState();s.phase++;stxFCADisableLegacyInvasion();
  const existing=state.commandChoices.slice();
  if(stxFCAWars().length){
    state.commandChoices=[stxFCAChoice("invade"),stxFCAChoice("concentrate"),...stxFCAWarExtras(existing)].slice(0,4);
  }else{
    let choices=existing.filter(c=>!stxFCAIsInvade(c)&&!stxFCAIsConcentrate(c));
    const drought=s.phase-s.lastUpgradeOffer;
    if(stxFCAUpgradeCandidates().length&&(drought>=5||(drought>=3&&random()<.42))&&!choices.some(c=>c.stxFleetOrderKind==="upgrade")){
      const upgrade=stxFCAChoice("upgrade"),i=choices.findIndex(c=>/military/i.test(c.cat||""));if(i>=0)choices[i]=upgrade;else if(choices.length<4)choices.push(upgrade);else choices[choices.length-1]=upgrade;s.lastUpgradeOffer=s.phase;
    }
    state.commandChoices=stxFCAFillPeaceHand(choices);
  }
  state.commandSelected.clear();renderCommands();
}
const STX_FCA_openCommandPhase=openCommandPhase;
openCommandPhase=function(){STX_FCA_openCommandPhase();stxFCAPostHand()};

const STX_FCA_renderCommands=renderCommands;
renderCommands=function(){
  STX_FCA_renderCommands();const grid=$("commandGrid");if(!grid)return;
  [...grid.children].forEach((button,i)=>{const choice=state.commandChoices[i],kind=choice?.stxFleetOrderKind;if(!["invade","concentrate"].includes(kind))return;
    button.dataset.fleetOrder=kind;button.style.setProperty("--cat","#ff435f");button.classList.add("stx-fca-war-command");const cat=button.querySelector(".command-cat");if(cat)cat.textContent="WAR COMMAND";
    button.onclick=()=>stxFCAOpenDirect(kind);
  });
};

function stxFCAEscape(value){return String(value??"").replace(/[&<>\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"})[c])}
function stxFCAInstallUi(){
  if($("stxFCAOrderModal"))return;
  const modal=document.createElement("div");modal.id="stxFCAOrderModal";modal.className="modal-wrap";modal.hidden=true;
  modal.innerHTML=`<section class="glass stx-fca-box" role="dialog" aria-modal="true" aria-labelledby="stxFCATitle"><div class="stx-fca-head"><div><div class="kicker" id="stxFCAKicker">WAR COMMAND</div><h2 id="stxFCATitle"></h2></div><button type="button" class="choice-btn" id="stxFCACancel">Back</button></div><p class="subtle" id="stxFCADesc"></p><div class="stx-fca-total"><span>Total surviving ships</span><b id="stxFCATotal">0</b></div><div id="stxFCACountWrap"><label class="section-label" for="stxFCACount">Ships to Commit</label><div class="stx-fca-count"><button type="button" id="stxFCAMinus" aria-label="Commit one fewer ship">−</button><input id="stxFCACount" type="number" min="1" value="1"><button type="button" id="stxFCAPlus" aria-label="Commit one more ship">+</button><strong id="stxFCACountRatio">1 / 1</strong></div><input id="stxFCARange" class="stx-fca-range" type="range" min="1" value="1"></div><div class="section-label" id="stxFCATargetLabel">Select Target</div><div id="stxFCATargetList" class="stx-fca-targets"></div><div id="stxFCAAllocationWrap" hidden><div class="section-label">Ship Allocation</div><div id="stxFCAAllocation"></div></div><div class="section-label">Reassignment Consequences</div><div id="stxFCAConsequences" class="stx-fca-consequences"></div><div class="command-footer"><span class="command-note" id="stxFCAAvailable"></span><button class="issue-btn" id="stxFCAConfirm">Confirm Order</button></div></section>`;
  document.body.appendChild(modal);
  const style=document.createElement("style");style.id="stxFCAStyles";style.textContent=`.stx-fca-box{width:min(720px,94vw);max-height:90vh;overflow:auto;border-radius:20px;padding:22px;background:linear-gradient(155deg,rgba(30,13,30,.99),rgba(5,8,20,.99));border:1px solid rgba(255,76,103,.34)}.stx-fca-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}.stx-fca-box h2{margin:6px 0}.stx-fca-total{display:flex;align-items:center;justify-content:space-between;margin:13px 0;padding:11px 13px;border:1px solid rgba(255,80,107,.2);border-radius:10px;background:rgba(255,67,95,.07)}.stx-fca-total span{font:800 10px system-ui;letter-spacing:.1em;text-transform:uppercase;color:#aebbd1}.stx-fca-total b{font:900 20px system-ui;color:#fff}.stx-fca-count{display:grid;grid-template-columns:38px 82px 38px 1fr;gap:7px;align-items:center}.stx-fca-count button,.stx-fca-allocation button{border:1px solid rgba(255,95,119,.28);background:rgba(255,67,95,.09);color:#fff;border-radius:8px;padding:8px;cursor:pointer}.stx-fca-box input,.stx-fca-box select{border:1px solid rgba(120,153,224,.25);background:#091326;color:#eef5ff;border-radius:9px;padding:9px;font:inherit}.stx-fca-count input{width:62px}.stx-fca-count strong{text-align:right;color:#ffabb8}.stx-fca-range{width:100%;margin:9px 0 4px;padding:0!important}.stx-fca-targets{display:grid;grid-template-columns:1fr 1fr;gap:7px;max-height:235px;overflow:auto;margin:7px 0 14px}.stx-fca-target{display:grid;grid-template-columns:auto 1fr;gap:9px;align-items:center;padding:10px;border:1px solid rgba(122,151,208,.14);border-radius:10px;background:rgba(54,79,126,.07);cursor:pointer}.stx-fca-target:has(input:checked){border-color:rgba(255,83,111,.55);background:rgba(255,67,95,.1)}.stx-fca-target input{width:auto;padding:0}.stx-fca-target b,.stx-fca-target small{display:block}.stx-fca-target small{margin-top:2px;color:#879bb8;font-size:.66rem}.stx-fca-allocation{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;padding:9px 10px;margin:6px 0;border-radius:9px;background:rgba(255,255,255,.035)}.stx-fca-allocation div{display:flex;align-items:center;gap:6px}.stx-fca-allocation b{color:#fff}.stx-fca-consequences{display:grid;gap:7px;margin-top:7px}.stx-fca-breakdown{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}.stx-fca-breakdown span{padding:8px;border-radius:8px;background:rgba(80,108,169,.09);border:1px solid rgba(114,150,225,.1);font-size:.66rem;color:#9fb0c8}.stx-fca-breakdown b{display:block;margin-top:3px;font-size:.9rem;color:#fff}.stx-fca-warning{padding:9px;border:1px solid rgba(255,85,108,.35);border-radius:8px;background:rgba(255,67,95,.1);color:#ffb5c0;font-weight:800}.stx-fca-ship-list{color:#8fa4bf;font-size:.68rem;line-height:1.45}.stx-fca-box .command-footer{margin-top:18px}@media(max-width:650px){.stx-fca-targets{grid-template-columns:1fr}.stx-fca-breakdown{grid-template-columns:1fr 1fr}.stx-fca-count{grid-template-columns:36px 70px 36px 1fr}}`;
  document.head.appendChild(style);
  $("stxFCACount").addEventListener("input",()=>stxFCASetCount($("stxFCACount").value));$("stxFCARange").addEventListener("input",()=>stxFCASetCount($("stxFCARange").value));
  $("stxFCAMinus").onclick=()=>stxFCASetCount((Number($("stxFCACount").value)||1)-1);$("stxFCAPlus").onclick=()=>stxFCASetCount((Number($("stxFCACount").value)||0)+1);
  $("stxFCATargetList").addEventListener("change",stxFCAHandleTargetChange);$("stxFCAAllocation").addEventListener("click",e=>{const b=e.target.closest("[data-fca-allocation]");if(b)stxFCAAdjustAllocation(b.dataset.fcaAllocation,Number(b.dataset.fcaDelta)||0)});
  $("stxFCAConfirm").onclick=stxFCAConfirm;$("stxFCACancel").onclick=stxFCACancel;
}
stxFCAInstallUi();

function stxFCARemaining(){return Math.max(0,stxFCAQueue.length-1)}
function stxFCASelectedDestinations(){
  const selected=new Set([...$("stxFCATargetList").querySelectorAll("input:checked")].map(i=>i.value));return stxFCAUiDestinations.filter(x=>selected.has(x.key));
}
function stxFCACount(){const total=stxFCAFree().length;return total?clamp(Math.round(Number($("stxFCACount").value)||1),1,total):0}
function stxFCASetCount(value){
  const total=stxFCAFree().length,n=total?clamp(Math.round(Number(value)||1),1,total):0;$("stxFCACount").value=String(n);$("stxFCARange").value=String(n);stxFCAAutoAllocate();stxFCASyncUi();
}
function stxFCAEvenAllocation(selected,n){
  const allocation={};if(!selected.length)return allocation;const base=Math.floor(n/selected.length),extra=n%selected.length;selected.forEach((t,i)=>allocation[t.key]=base+(i<extra?1:0));return allocation;
}
function stxFCAAutoAllocate(){
  stxFCAAllocations=stxFCAEvenAllocation(stxFCASelectedDestinations(),stxFCACount());
}
function stxFCAHandleTargetChange(e){
  const order=stxFCAQueue[0];if(!order)return;
  if(order.kind==="invade"&&e.target.matches('input[type="checkbox"]')){
    const checked=[...$("stxFCATargetList").querySelectorAll('input[type="checkbox"]:checked')];if(checked.length>2){e.target.checked=false;showToast("Choose no more than two invasion targets");return}
    if(checked.length===2&&stxFCACount()<2&&stxFCAFree().length>=2){$("stxFCACount").value="2";$("stxFCARange").value="2"}
  }
  stxFCAAutoAllocate();stxFCASyncUi();
}
function stxFCAAdjustAllocation(key,delta){
  const selected=stxFCASelectedDestinations();if(selected.length!==2||!delta)return;const other=selected.find(x=>x.key!==key);if(!other)return;
  const current=stxFCAAllocations[key]||0,otherCount=stxFCAAllocations[other.key]||0;if(delta>0&&otherCount>1){stxFCAAllocations[key]=current+1;stxFCAAllocations[other.key]=otherCount-1}else if(delta<0&&current>1){stxFCAAllocations[key]=current-1;stxFCAAllocations[other.key]=otherCount+1}stxFCASyncUi();
}
function stxFCARenderAllocation(selected){
  const wrap=$("stxFCAAllocationWrap"),box=$("stxFCAAllocation"),order=stxFCAQueue[0];wrap.hidden=order?.kind!=="invade"||!selected.length;if(wrap.hidden){box.innerHTML="";return}
  box.innerHTML=selected.map(t=>`<div class="stx-fca-allocation"><span><b>${stxFCAEscape(t.name)}</b><small>${stxFCAEscape(t.planet?.name===t.name?"Planetary invasion":t.planet?.name||"")}</small></span><div><button type="button" data-fca-allocation="${stxFCAEscape(t.key)}" data-fca-delta="-1">−</button><b>${stxFCAAllocations[t.key]||0} ship${stxFCAAllocations[t.key]===1?"":"s"}</b><button type="button" data-fca-allocation="${stxFCAEscape(t.key)}" data-fca-delta="1">+</button></div></div>`).join("");
}
function stxFCARenderConsequences(selected,n){
  const fleets=stxFCAFree(selected).slice(0,n),counts={idle:0,enroute:0,other:0,combat:0};fleets.forEach(f=>counts[stxFCAStatusBucket(f).key]++);
  const labels={idle:"Idle / patrol",enroute:"En route",other:"Other duties",combat:"Active combat"},rows=Object.keys(labels).map(k=>`<span>${labels[k]}<b>${counts[k]}</b></span>`).join(""),warning=counts.combat?`<div class="stx-fca-warning">⚠ ${counts.combat} ship${counts.combat===1?" will":"s will"} disengage from active combat and retain current damage.</div>`:"",names=fleets.map(f=>`${stxFCAEscape(f.name)} · ${stxFCAStatusBucket(f).label} · ${Math.round(stxFCAEffectiveStrength(f))} power`).join("<br>");
  $("stxFCAConsequences").innerHTML=`<div class="stx-fca-breakdown">${rows}</div>${warning}<div class="stx-fca-ship-list">${names||"Select a target to preview which ships will move."}</div>`;
}
function stxFCASyncUi(renderAllocation=true){
  const order=stxFCAQueue[0];if(!order)return;const total=stxFCAFree().length,n=stxFCACount(),selected=stxFCASelectedDestinations();
  $("stxFCATotal").textContent=total;$("stxFCAAvailable").textContent=total?`${n} of ${total} surviving ships will receive this order`:"No surviving ships are available";$("stxFCACountRatio").textContent=`${n} / ${total}`;$("stxFCACount").max=String(Math.max(1,total));$("stxFCARange").max=String(Math.max(1,total));$("stxFCACount").disabled=!total;$("stxFCARange").disabled=!total;$("stxFCAMinus").disabled=n<=1;$("stxFCAPlus").disabled=n>=total;
  if(order.kind==="upgrade"){
    const f=selected[0]?.fleet;$("stxFCAConsequences").innerHTML=f?`<div class="stx-fca-ship-list">Upgrade ${stxFCAEscape(f.name)} from ${Math.round(f.strength)} to ${Math.ceil((f.strength||5)*1.24+3)} strength at ${stxFCAEscape(state.planets.find(p=>p.id===f.location)?.name||"its current planet")}.</div>`:'<div class="stx-fca-ship-list">Choose the fleet that will enter refit.</div>';$("stxFCAConfirm").disabled=!f;return;
  }
  if(renderAllocation)stxFCARenderAllocation(selected);stxFCARenderConsequences(selected,n);
  const allocationTotal=selected.reduce((sum,t)=>sum+(stxFCAAllocations[t.key]||0),0),validTargets=order.kind==="invade"?selected.length>=1&&selected.length<=2&&selected.every(t=>t.planet&&t.planet.owner!==0&&t.planet.owner!==null&&empiresAtWar(0,t.planet.owner))&&selected.every(t=>(stxFCAAllocations[t.key]||0)>=1)&&allocationTotal===n:selected.length===1;
  $("stxFCAConfirm").disabled=!total||!validTargets||n<1;
}
function stxFCARenderTargets(order){
  const box=$("stxFCATargetList");stxFCAAllocations={};
  if(order.kind==="upgrade")stxFCAUiDestinations=stxFCAUpgradeCandidates().map(f=>{const p=state.planets.find(q=>q.id===f.location);return{key:`fleet:${f.id}`,type:"fleet",id:f.id,name:f.name,fleet:f,planet:p,subtitle:`${Math.round(f.strength)} strength · ${p?.name||"Unknown"}`} });
  else if(order.kind==="invade")stxFCAUiDestinations=stxFCATargets("invade").map(p=>({key:`planet:${p.id}`,type:"planet",id:p.id,name:p.name,planet:p,subtitle:`${empire(p.owner)?.name||"Enemy"} · ${Math.round(typeof stxWBTargetDefense==="function"?stxWBTargetDefense(p):(p.garrison||0)+(p.infra?.defense||0)*8)} defense`}));
  else if(order.kind==="concentrate")stxFCAUiDestinations=stxFCAConcentrationTargets();
  else stxFCAUiDestinations=stxFCATargets(order.kind).map(p=>({key:`planet:${p.id}`,type:"planet",id:p.id,name:p.name,planet:p,subtitle:"Player world"}));
  const inputType=order.kind==="invade"?"checkbox":"radio",name=`stx-fca-${order.kind}`;box.innerHTML=stxFCAUiDestinations.map(t=>`<label class="stx-fca-target"><input type="${inputType}" name="${name}" value="${stxFCAEscape(t.key)}"><span><b>${stxFCAEscape(t.name)}</b><small>${stxFCAEscape(t.subtitle||"")}</small></span></label>`).join("")||`<div class="subtle">${order.kind==="invade"?"No enemy planets remain in an active war.":"No valid destination is currently available."}</div>`;
}
function stxFCAShow(){
  if(!stxFCAQueue.length)return stxFCAFinish();const order=stxFCAQueue[0],upgrade=order.kind==="upgrade",total=stxFCAFree().length;
  $("stxFCAOrderModal").hidden=false;$("stxFCACancel").hidden=!stxFCADirect;$("stxFCAKicker").textContent=order.kind==="invade"?"WAR COMMAND · INVASION":order.kind==="concentrate"?"WAR COMMAND · FLEET MOVEMENT":`${order.kind.toUpperCase()} ORDER`;
  $("stxFCATitle").textContent=upgrade?"Upgrade a Fleet":order.kind==="invade"?"Invasion Command":order.kind==="defend"?"Planetary Defense Order":"Concentrate Forces";
  $("stxFCADesc").textContent=upgrade?"The selected fleet remains at its current world until its supply-backed refit completes.":order.kind==="invade"?"Choose any number of surviving ships and one or two planets belonging to factions currently at war with you.":order.kind==="concentrate"?"Choose any number of surviving ships and one friendly planet or orbital station. Existing assignments affect selection order only.":"Choose the destination and exact ship count.";
  $("stxFCACountWrap").hidden=upgrade;$("stxFCATargetLabel").textContent=upgrade?"Select Fleet":order.kind==="invade"?"Select One or Two Enemy Planets":"Select Destination";stxFCARenderTargets(order);
  const initial=total?1:0;$("stxFCACount").value=String(initial);$("stxFCARange").min="1";$("stxFCARange").value=String(initial);stxFCASyncUi();
}
function stxFCAOpenDirect(kind){
  if(stxFCAActive)return;stxFCAQueue=[{kind,direct:true}];stxFCAActive=true;stxFCADirect=true;stxFCAUsedTargets=new Set();stxFCACommitted=new Set();stxFCAResume=state.preCommandSpeed||state.speed||1;state.speed=0;$("commandModal").hidden=true;updateSpeedButtons();stxFCAShow();
}
function stxFCAStart(){
  if(stxFCAActive||!stxFCAQueue.length)return;stxFCAActive=true;stxFCADirect=false;stxFCAUsedTargets=new Set();stxFCACommitted=new Set();stxFCAResume=state.speed||state.preCommandSpeed||1;state.speed=0;updateSpeedButtons();stxFCAShow();
}
function stxFCACancel(){
  if(!stxFCADirect)return;$("stxFCAOrderModal").hidden=true;stxFCAQueue=[];stxFCACommitted.clear();stxFCAActive=false;stxFCADirect=false;state.speed=0;$("commandModal").hidden=false;renderCommands();updateSpeedButtons();
}
function stxFCAFinish(consumed=stxFCADirect){
  const direct=stxFCADirect;$("stxFCAOrderModal").hidden=true;stxFCAActive=false;stxFCAQueue=[];stxFCAUsedTargets.clear();stxFCACommitted.clear();stxFCADirect=false;if(consumed&&direct)state.nextCommand=state.simTime+state.commandCycle;state.speed=stxFCAResume||1;updateSpeedButtons();updateHud(true);saveGame(false);
}

function stxFCADetachBattle(f){
  const ctx=stxFCABattleContext(f);if(!ctx)return null;const b=ctx.battle,idsKey=`${ctx.side}FleetIds`,strengthKey=`${ctx.side}Strength`,initialKey=`${ctx.side}Initial`,ids=b[idsKey]||[],records=ids.map(fleetRecord).filter(Boolean),weight=Math.max(.01,Number(f.strength)||1),total=records.reduce((n,x)=>n+Math.max(.01,Number(x.strength)||1),0)||weight,current=Number(b[strengthKey])||0,initial=Number(b[initialKey])||current,remaining=Math.max(.05,current*weight/total),initialShare=Math.max(remaining,initial*weight/total),damage=Math.max(0,initialShare-remaining);
  b[strengthKey]=Math.max(0,current-remaining);b[initialKey]=Math.max(b[strengthKey],initial-initialShare);b[idsKey]=ids.filter(id=>id!==f.id);f.strength=remaining;f.maxServiceStrength=Math.max(Number(f.maxServiceStrength)||0,initialShare,remaining);f.battles=(f.battles||0)+1;f.shipsLost=(f.shipsLost||0)+Math.max(0,Math.round(damage*.55));f.location=ctx.planet?.id||f.location;f.status=`Disengaging from Battle of ${ctx.planet?.name||"Unknown"}`;return ctx;
}
function stxFCAClearShipMission(s){
  ["battleId","invasionPlanId","warId","orbitalRaidFacilityId","stationRaid","rdReposition","rdReason","rdWarObjective","reinforcement","retreat","olRelayFinalId","olRelayQueue","olRelayNames","launchedFromFacilityId","stationTargetId","stxManualFleetOrder","stxFleetOrderKind"].forEach(k=>delete s[k]);
}
function stxFCAOrderLabel(kind,destination){return kind==="invade"?`Invading ${destination.name}`:kind==="defend"?`Redeploying to defend ${destination.name}`:`Concentrating at ${destination.name}`}
function stxFCARedirectTransit(f,s,destination,kind,meta={}){
  const p=destination.planet,origin=state.planets.find(x=>x.id===s.from)||state.planets.find(x=>x.id===f.homePort)||playerWorlds()[0];if(!p||!origin)return false;const strength=Math.max(.05,Number(s.strength)||Number(f.strength)||.05),label=stxFCAOrderLabel(kind,destination);stxFCAClearShipMission(s);s.type="fleet";s.from=origin.id;s.to=p.id;s.startX=Number(s.x)||origin.x;s.startY=Number(s.y)||origin.y;s.progress=0;s.distance=Math.max(1,Math.hypot((Number(s.x)||origin.x)-p.x,(Number(s.y)||origin.y)-p.y));s.speed=Math.max(60,Number(s.speed)||86);s.owner=0;s.strength=strength;s.fleetId=f.id;s.vesselName=f.name;s.status=label;s.stxFleetOrderKind=kind;s.servicePurpose=kind==="invade"?"planetary invasion":"fleet concentration";if(destination.facility)s.stationTargetId=destination.facility.id;Object.assign(s,meta);f.location=null;f.strength=strength;f.readiness="inbound";f.status=label;return true;
}
function stxFCAFallbackShip(type,from,to,owner,extra){
  const distance=Math.max(1,dist(from,to)),ship={id:`s${Math.floor(random()*1e9)}`,type,from:from.id,to:to.id,x:from.x,y:from.y,startX:from.x,startY:from.y,progress:0,distance,speed:86*(1+(empire(owner).tech?.propulsion||0)*.08)*(extra.speedBoost||1),owner,cargo:{},strength:0,phase:rand(0,6.28),vesselName:extra.vesselName||vesselName(type),...extra};state.ships.push(ship);return ship;
}
function stxFCAAssign(f,destination,kind){
  const dest=destination?.planet?destination:{key:`planet:${destination.id}`,type:"planet",id:destination.id,name:destination.name,planet:destination},p=dest.planet;if(!f||!p)return false;stxFCAClear(f,dest.type!=="facility");f.location=p.id;f.readiness=kind==="defend"?"rapid response ready":"ready";f.status=kind==="defend"?`Defending ${p.name}`:`Concentrated at ${dest.name}`;f.stxAssignment={kind,targetId:dest.id,targetType:dest.type,locked:false,assignedAt:state.simTime};p.mandateGlow=1;
  if(dest.facility){if(f.dockedAt&&f.dockedAt!==dest.facility.id&&typeof stxOLRemoveDocking==="function")stxOLRemoveDocking(f);f.dockedAt=dest.facility.id;dest.facility.dockedShipIds=[...new Set([...(dest.facility.dockedShipIds||[]),f.id])];if(typeof stxOLAddFacilityActivity==="function")stxOLAddFacilityActivity(dest.facility,`${f.name} concentrated at the station under direct order.`,"good")}
  return true;
}
function stxFCADispatchFleet(f,destination,kind,meta={}){
  if(!stxFCACommandableFleet(f))return false;const battle=stxFCADetachBattle(f),transit=stxFCATransit(f);if(f.stxRefit){f.stxRefit=null;f.status="Refit interrupted by direct order"}f.stxAssignment=null;
  if(transit){const ok=stxFCARedirectTransit(f,transit,destination,kind,meta);if(ok){stxFCACommitted.add(f.id);if(battle)f.status=`Disengaging from Battle of ${battle.planet?.name||"Unknown"} → ${stxFCAOrderLabel(kind,destination)}`}return ok}
  const source=battle?.planet||state.planets.find(p=>p.id===f.location)||state.planets.find(p=>p.id===f.homePort)||playerWorlds()[0],target=destination.planet;if(!source||!target)return false;
  if(kind!=="invade"&&source.id===target.id){stxFCAAssign(f,destination,kind);stxFCACommitted.add(f.id);return true}
  const strength=Math.max(.05,Number(f.strength)||stxFCAEffectiveStrength(f)||.05),label=stxFCAOrderLabel(kind,destination),extra={strength,fleetId:f.id,vesselName:f.name,stxFleetOrderKind:kind,servicePurpose:kind==="invade"?"planetary invasion":"fleet concentration",speedBoost:1.08+(source.orbitals?.base||0)*(kind==="invade"?.38:.22),...meta};if(destination.facility)extra.stationTargetId=destination.facility.id;
  const oldDock=f.dockedAt,status=battle?`Disengaging from Battle of ${battle.planet?.name||"Unknown"} → ${label}`:label;f.location=null;f.readiness="inbound";f.status=status;let ship=createShip("fleet",source,target,0,extra);if(!ship)ship=stxFCAFallbackShip("fleet",source,target,0,extra);if(!ship)return false;if(oldDock&&f.dockedAt&&typeof stxOLRemoveDocking==="function")stxOLRemoveDocking(f);f.status=status;ship.status=status;stxFCACommitted.add(f.id);return true;
}
function stxFCAInvasionPlan(target){return(empire(0).invasionPlans||[]).find(p=>p.targetId===target.id)}
function stxFCAExecuteInvasionOrder(destinations,allocations){
  const count=destinations.reduce((n,t)=>n+(allocations[t.key]||0),0);if(!count||destinations.some(t=>!t.planet||!empiresAtWar(0,t.planet.owner)))return 0;
  for(const target of destinations)if(!stxQueueInvasion(target.planet,`Manual invasion of ${target.name}`,false))return 0;
  const chosen=stxFCAFree(destinations).slice(0,count);if(chosen.length!==count)return 0;let offset=0,moved=0;
  destinations.forEach(destination=>{const amount=allocations[destination.key]||0,plan=stxFCAInvasionPlan(destination.planet),war=getWar(0,destination.planet.owner),ids=[];for(const f of chosen.slice(offset,offset+amount)){if(stxFCADispatchFleet(f,destination,"invade",{warId:war?.id,invasionPlanId:plan?.id,stxManualFleetOrder:true})){moved++;ids.push(f.id)}}offset+=amount;if(plan&&ids.length){plan.stxManualAllocation=true;plan.stxRequestedFleetCount=ids.length;plan.stxAssignedFleetIds=ids;plan.stxManualLaunchedAt=state.simTime;plan.lastLaunchAt=state.simTime;plan.status="fleet inbound"}if(ids.length){logEvent(`INVASION LAUNCHED: ${ids.length} ship${ids.length===1?"":"s"} committed to ${destination.name}.`,"warning");if(typeof stxActivity==="function")stxActivity(`${ids.length} directly commanded ship${ids.length===1?"":"s"} launched for ${destination.name}.`,destination.planet.id,null,"warning")}});
  if(moved===count&&typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();return moved;
}
function stxFCAExecuteInvade(target,count){const destination={key:`planet:${target.id}`,type:"planet",id:target.id,name:target.name,planet:target};return stxFCAExecuteInvasionOrder([destination],{[destination.key]:count})}
function stxFCAExecuteMove(kind,destination,count){
  const dest=destination?.planet?destination:{key:`planet:${destination.id}`,type:"planet",id:destination.id,name:destination.name,planet:destination},chosen=stxFCAFree([dest]).slice(0,count);if(chosen.length!==count)return 0;let moved=0;chosen.forEach(f=>{if(stxFCADispatchFleet(f,dest,kind))moved++});
  if(moved===count){logEvent(`${kind==="defend"?"DEFENSE ORDER":"FLEET CONCENTRATION"}: exactly ${moved} ship${moved===1?"":"s"} assigned to ${dest.name}.`,"good");if(typeof stxActivity==="function")stxActivity(`${moved} ship${moved===1?"":"s"} assigned to ${dest.name}.`,dest.planet.id,null,"good");if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator()}return moved;
}

function stxFCARefitNeed(f){const s=Math.max(5,f.strength||5);return{components:Math.ceil(18+s*1.25),equipment:Math.ceil(12+s*.82),titanium:Math.ceil(10+s*.68)}}
function stxFCAPrimeRefit(f,p){
  const q=f.stxRefit;if(!q||state.simTime<(q.nextSupplyAt||-999))return;q.nextSupplyAt=state.simTime+6;
  Object.entries(q.need).forEach(([r,a])=>{const left=Math.max(0,a-(q.delivered?.[r]||0));if(left>.01)addOrder(p,"fleetRefit",r,left,7,`${f.name} fleet upgrade`)});
}
function stxFCAStartRefit(f){
  if(!f||!stxFCAUpgradeCandidates().some(x=>x.id===f.id))return false;stxFCACommitted.add(f.id);const p=state.planets.find(x=>x.id===f.location);if(!p)return false;stxFCAClear(f);
  const need=stxFCARefitNeed(f);f.stxRefit={planetId:p.id,need,delivered:Object.fromEntries(Object.keys(need).map(r=>[r,0])),progress:0,targetStrength:Math.ceil((f.strength||5)*1.24+3),startedAt:state.simTime,nextSupplyAt:-999};
  f.stxAssignment={kind:"upgrade",targetId:p.id,locked:true};f.readiness="refitting";f.status=`Upgrading at ${p.name} · awaiting materials`;p.mandateGlow=1;stxFCAPrimeRefit(f,p);
  logEvent(`FLEET UPGRADE: ${f.name} entered refit at ${p.name} and cannot deploy until complete.`,"good");if(typeof stxActivity==="function")stxActivity(`${f.name} began a supply-backed refit at ${p.name}.`,p.id,f.id,"good");return true;
}
function stxFCATickRefits(p,dt){
  state.fleets.filter(f=>f.owner===0&&!f.destroyed&&f.stxRefit?.planetId===p.id&&f.location===p.id).forEach(f=>{
    const q=f.stxRefit;Object.entries(q.need).forEach(([r,a])=>{const have=q.delivered[r]||0,left=Math.max(0,a-have),floor=["components","equipment"].includes(r)?4:8,take=Math.min(left,Math.max(0,(p.stock?.[r]||0)-floor));if(take>.01)q.delivered[r]=have+consume(p,r,take)});
    const ready=Object.entries(q.need).every(([r,a])=>(q.delivered[r]||0)>=a-.01);
    if(!ready){stxFCAPrimeRefit(f,p);const missing=Object.entries(q.need).filter(([r,a])=>(q.delivered[r]||0)<a-.01).map(([r])=>RESOURCE_LABEL[r]||r).join(", ");f.status=`Upgrading at ${p.name} · waiting for ${missing}`;return}
    q.progress=clamp(q.progress+dt*.012*(1+(p.infra?.shipyard||0)*.22+(p.orbitals?.base||0)*.18+(p.infra?.factory||0)*.08),0,1);f.status=`Upgrading at ${p.name} · ${Math.floor(q.progress*100)}%`;
    if(q.progress<1)return;const old=Math.round(f.strength||0);f.strength=Math.max(f.strength||0,q.targetStrength);f.maxServiceStrength=Math.max(f.maxServiceStrength||0,f.strength);f.lastUpgradeAt=state.simTime;f.stxRefit=null;f.stxAssignment=null;f.readiness="ready";f.supplyReadiness=1;f.status=`Upgraded and ready at ${p.name}`;
    logEvent(`FLEET UPGRADE COMPLETE: ${f.name} increased from ${old} to ${Math.round(f.strength)} strength.`,"good");if(typeof stxActivity==="function")stxActivity(`${f.name} completed its refit at ${p.name}.`,p.id,f.id,"good");showToast(`${f.name} upgrade complete`);if(typeof stxRefreshFleetLocator==="function")stxRefreshFleetLocator();
  });
}
const STX_FCA_tickPlanet=tickPlanet;
tickPlanet=function(p,dt){STX_FCA_tickPlanet(p,dt);stxFCATickRefits(p,dt)};

const STX_FCA_arriveShip=arriveShip;
arriveShip=function(s,p){
  const kind=s?.stxFleetOrderKind,id=s?.fleetId;if((kind==="defend"||kind==="concentrate")&&p?.owner===0&&id){const f=fleetRecord(id),hit=s.stationTargetId&&typeof stxOLFacilityAt==="function"?stxOLFacilityAt(s.stationTargetId):null,facility=hit?.p?.id===p.id&&(Number(hit.f?.hp)||0)>0?hit.f:null,destination=facility?{key:`facility:${facility.id}`,type:"facility",id:facility.id,name:facility.name,planet:p,facility}:{key:`planet:${p.id}`,type:"planet",id:p.id,name:p.name,planet:p};if(f&&!f.destroyed){f.strength=Math.max(.05,Number(s.strength)||Number(f.strength)||.05);stxFCAAssign(f,destination,kind);if(typeof stxActivity==="function")stxActivity(`${f.name} arrived and concentrated at ${destination.name}.`,p.id,f.id,"good")}return}
  return STX_FCA_arriveShip(s,p);
};

if(typeof stxMobilizeInvasionPlan==="function"){
  const old=stxMobilizeInvasionPlan;stxMobilizeInvasionPlan=function(plan,force=false){
    if(plan?.stxManualAllocation){
      const t=state.planets.find(p=>p.id===plan.targetId);if(!t||t.owner===0||t.owner===null){plan.status="completed";return false}
      if(!empiresAtWar(0,t.owner)){plan.status="awaiting war";return false}
      if(t.underAttack||state.ships.some(s=>s.owner===0&&s.invasionPlanId===plan.id)){plan.status=t.underAttack?"engaged":"fleet inbound";return true}
      if(plan.stxManualLaunchedAt!=null){plan.status="awaiting player reinforcement";return false}
    }
    return old(plan,force);
  };
}

function stxFCAConfirm(){
  const order=stxFCAQueue[0];if(!order)return;const selected=stxFCASelectedDestinations();let ok=false;
  if(order.kind==="upgrade")ok=stxFCAStartRefit(selected[0]?.fleet);
  else if(order.kind==="invade"){const n=stxFCACount(),moved=stxFCAExecuteInvasionOrder(selected,stxFCAAllocations);ok=moved===n&&n>0}
  else{const n=stxFCACount(),moved=selected[0]?stxFCAExecuteMove(order.kind,selected[0],n):0;ok=moved===n&&n>0}
  if(!ok)return showToast("The exact fleet order could not be completed");const direct=stxFCADirect;stxFCAQueue.shift();saveGame(false);stxFCAQueue.length?stxFCAShow():stxFCAFinish(direct);
}

const STX_FCA_issueCommands=issueCommands;
issueCommands=function(){const before=stxFCAQueue.length;STX_FCA_issueCommands();if(stxFCAQueue.length>before)stxFCAStart()};

const STX_FCA_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_FCA_generateGalaxy();stxFCAState();stxFCADisableLegacyInvasion();stxFCAQueue=[];stxFCACommitted.clear();stxFCAActive=false;stxFCADirect=false};
const STX_FCA_loadGame=loadGame;
loadGame=function(){const ok=STX_FCA_loadGame();if(ok){stxFCAState();stxFCADisableLegacyInvasion();stxFCAQueue=[];stxFCACommitted.clear();stxFCAActive=false;stxFCADirect=false}return ok};
stxFCADisableLegacyInvasion();

if(typeof renderShipLedger==="function"){
  const old=renderShipLedger;renderShipLedger=function(p){
    const base=old(p),rows=state.fleets.filter(f=>f.stxRefit?.planetId===p.id).map(f=>{const q=f.stxRefit,m=Object.entries(q.need).map(([r,a])=>`${RESOURCE_LABEL[r]||r} ${Math.floor(q.delivered[r]||0)}/${a}`).join(" · ");return`<div class="project-row mandate"><div class="project-head"><strong>Fleet Refit · ${f.name}</strong><b>${Math.floor(q.progress*100)}%</b></div><div class="project-desc">Locked at ${p.name} · target strength ${q.targetStrength}</div><div class="project-track"><i style="width:${Math.floor(q.progress*100)}%"></i></div><div class="project-meta"><span>${m}</span><span>Direct orders may interrupt refit</span></div></div>`}).join("");
    return rows+base;
  };
}
