/* Space Tyrants — mission completion + responsive productivity feedback.
   Keeps the simulation architecture intact while removing the visible 100%
   mission stall and making the selected-world / mandate UI react quickly. */

const STX_MR_COMPLETE_AT=.995;
const STX_MR_HOT_HUD_MS=125;
let stxMRLastHotHud=0;

function stxMRBaseCommandId(c){
  return String(c?.id||"").replace(/-p\d+-\d+$/,'');
}

function stxMRFinishReadyShips(){
  if(!Array.isArray(state?.ships)||!Array.isArray(state?.planets))return 0;
  let completed=0;
  for(let i=state.ships.length-1;i>=0;i--){
    const s=state.ships[i];
    if(!s||Number(s.progress||0)<STX_MR_COMPLETE_AT)continue;
    const destination=state.planets.find(p=>p.id===s.to);
    if(!destination)continue;
    s.progress=1;s.x=destination.x;s.y=destination.y;
    arriveShip(s,destination);
    state.ships.splice(i,1);completed++;
  }
  return completed;
}

function stxMRCurrentProductivity(){
  const worlds=typeof playerWorlds==="function"?playerWorlds():[];
  if(!worlds.length)return 0;
  return worlds.reduce((n,p)=>n+Number(p.efficiency||0),0)/worlds.length;
}

function stxMRProjectedUrbanProductivity(target){
  if(!target)return null;
  const current=typeof populationEfficiency==="function"?populationEfficiency(target):Number(target.efficiency||1);
  const capacity=Number(target.capacity||0)+.28+Math.sqrt(Math.max(0,Number(target.pop||0)))*.08;
  let projected=current;
  if(typeof populationEfficiency==="function"){
    try{projected=populationEfficiency({...target,capacity})}catch(_){projected=current}
  }
  return{current,projected,capacity};
}

function stxMRProductivityLines(c){
  if(!c)return[];
  const id=stxMRBaseCommandId(c),lines=[];
  if(id==="industry")lines.push("Factory production multiplier → ×1.45 for 70 cycles");
  else if(id==="shipbuilding")lines.push("Ship construction multiplier → ×1.65","Factory production multiplier → ×1.18");
  else if(id==="distributed")lines.push("Factory production multiplier → ×1.16");
  else if(id==="totalWar")lines.push("Ship construction multiplier → ×1.55","Combat multiplier → ×1.45","Civilian growth multiplier → ×0.58");
  else if(id==="automation")lines.push("Automation technology +0.30","Extraction technology +0.18");
  else if(id==="urbanRenewal"){
    const q=stxMRProjectedUrbanProductivity(c.targetObj);
    if(q)lines.push(`Target productivity ${Math.round(q.current*100)}% → ${Math.round(q.projected*100)}%`,"Civil factory efficiency multiplier → ×1.16");
    else lines.push("Civil factory efficiency multiplier → ×1.16");
  }
  if(!lines.length){
    const effectLines=(c.effects||[]).filter(x=>/(product|output|factory|industr|manufactur|shipbuild|labor|automation)/i.test(String(x)));
    if(effectLines.length)lines.push(...effectLines.slice(0,3));
  }
  return lines;
}

function stxMRInstallStyle(){
  if(document.getElementById?.("stx-mission-responsiveness-style"))return;
  const style=document.createElement("style");style.id="stx-mission-responsiveness-style";
  style.textContent=`
    .stx-productivity-preview{margin-top:10px;padding:8px 9px;border-radius:10px;border:1px solid rgba(78,231,255,.18);background:rgba(78,231,255,.055);font-size:.6rem;line-height:1.38;color:#9fb7d9;transition:opacity .1s ease,transform .1s ease,border-color .1s ease}
    .command-card.selected .stx-productivity-preview{border-color:rgba(92,242,162,.46);background:rgba(92,242,162,.075);color:#d7f8e8;transform:translateY(-1px)}
    .stx-productivity-preview strong{display:block;margin-bottom:4px;color:var(--green);font-size:.57rem;letter-spacing:.1em;text-transform:uppercase}
    .stx-productivity-preview span{display:block}
    .stx-productivity-preview.is-hint{opacity:.72}
    .stx-live-productivity{display:block;margin-top:4px;color:#8fd9bd;font-size:.56rem;line-height:1.3}
  `;
  document.head.appendChild(style);
}

function stxMRDecorateCommandCards(){
  const grid=$("commandGrid");if(!grid?.querySelectorAll)return;
  const cards=[...grid.querySelectorAll(".command-card")];
  cards.forEach((card,i)=>{
    const c=state.commandChoices?.[i],lines=stxMRProductivityLines(c);if(!lines.length)return;
    const selected=state.commandSelected?.has(i),box=document.createElement("div");
    box.className=`stx-productivity-preview${selected?"":" is-hint"}`;
    box.innerHTML=selected?`<strong>Productivity / output preview</strong>${lines.map(x=>`<span>${x}</span>`).join("")}`:`<span>Select to preview the productivity boost.</span>`;
    card.appendChild(box);
  });
}

function stxMRDecoratePlanetProductivity(){
  const p=state.selected;if(!p||p.owner!==0)return;
  const body=$("planetBody");if(!body?.querySelectorAll)return;
  const metric=[...body.querySelectorAll(".metric")].find(m=>m.querySelector?.("label")?.textContent?.trim()==="Productivity");
  if(!metric)return;
  const e=empire(0),parts=[];
  const industry=modifier(e,"industry"),shipbuilding=modifier(e,"shipbuilding"),mining=modifier(e,"mining"),training=modifier(e,"training");
  if(Math.abs(industry-1)>.001)parts.push(`factory ×${industry.toFixed(2)}`);
  if(Math.abs(shipbuilding-1)>.001)parts.push(`shipbuilding ×${shipbuilding.toFixed(2)}`);
  if(Math.abs(mining-1)>.001)parts.push(`mining ×${mining.toFixed(2)}`);
  if(Math.abs(training-1)>.001)parts.push(`training ×${training.toFixed(2)}`);
  if(!parts.length)return;
  const line=document.createElement("small");line.className="stx-live-productivity";line.textContent=`Active mandate boosts: ${parts.join(" · ")}`;metric.appendChild(line);
}

stxMRInstallStyle();

const STX_MR_tickShips=tickShips;
tickShips=function(dt){
  const result=STX_MR_tickShips(dt);
  stxMRFinishReadyShips();
  return result;
};

const STX_MR_renderCommands=renderCommands;
renderCommands=function(){
  const result=STX_MR_renderCommands();
  stxMRDecorateCommandCards();
  return result;
};

const STX_MR_renderPlanet=renderPlanet;
renderPlanet=function(){
  const result=STX_MR_renderPlanet();
  stxMRDecoratePlanetProductivity();
  return result;
};

const STX_MR_updateHud=updateHud;
updateHud=function(force=false){
  const now=performance.now(),commandOpen=!$("commandModal")?.hidden,hot=!!state.selected||commandOpen;
  if(!force&&hot&&now-stxMRLastHotHud>=STX_MR_HOT_HUD_MS){
    stxMRLastHotHud=now;
    return STX_MR_updateHud(true);
  }
  return STX_MR_updateHud(force);
};

globalThis.SpaceTyrantsMissionResponsiveness={
  completionThreshold:STX_MR_COMPLETE_AT,
  finishReadyShips:stxMRFinishReadyShips,
  currentProductivity:stxMRCurrentProductivity,
  productivityLines:stxMRProductivityLines,
  projectedUrbanProductivity:stxMRProjectedUrbanProductivity
};
