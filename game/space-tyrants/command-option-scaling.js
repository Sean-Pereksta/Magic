/* Space Tyrants — empire-scaled command hand.
   Small empires see 8 options, established empires see 10-12, and major
   20+ world empires see 16. Expansion cards are deliberately common while
   preserving the executable-choice guarantees and special generated cards. */

const STX_CO_EXPANSION_IDS=new Set(["claim","homesteads"]);

function stxCOOptionCount(worldCount=playerWorlds().length){
  if(worldCount>=20)return 16;
  if(worldCount>=8)return 12;
  if(worldCount>=3)return 10;
  return 8;
}
function stxCOExpansionQuota(optionCount){
  return Math.max(2,Math.round(optionCount*.25));
}
function stxCOBaseId(c){return String(c?.id||"").replace(/-p\d+-\d+$/,'').replace(/^peace-invasion-.+$/,'peace-invasion')}
function stxCOIsExpansionChoice(c){
  const id=stxCOBaseId(c);if(STX_CO_EXPANSION_IDS.has(id))return true;
  const text=`${c?.title||""} ${c?.desc||""}`.toLowerCase();
  return !!c?.targetObj&&c.targetObj.owner===null&&/(colon|settle|settlement|homestead|claim.+world|expedition)/.test(text);
}
function stxCOChoiceKey(c){return `${stxCOBaseId(c)}:${c?.targetObj?.id||"-"}`}
function stxCOAtWar(){return typeof stxPlayerAtWar==="function"?!!stxPlayerAtWar():state.wars?.some(w=>w.active&&(w.a===0||w.b===0))}
function stxCOCanOffer(c){
  if(!c)return false;
  if(!stxCOAtWar()){
    if(typeof stxWarOnlyDirective==="function"&&stxWarOnlyDirective(c))return false;
    if(typeof stxDGIsInvasionChoice==="function"&&stxDGIsInvasionChoice(c))return false;
  }
  if(typeof stxDGCanExecuteChoice==="function")return !!stxDGCanExecuteChoice(c);
  if(typeof c.target==="function"&&!c.targetObj)return false;
  return true;
}
function stxCOChoiceFromCommand(c,targetObj){
  let target=targetObj;
  if(arguments.length<2)target=typeof c.target==="function"?(c.target()||null):(c.targetObj||null);
  return {...c,targetObj:target};
}
function stxCOExpansionTargets(){
  const worlds=playerWorlds();
  return state.planets.filter(p=>p.owner===null&&!p.underAttack).map(p=>{
    const qualities=Object.values(p.quality||{}).map(Number).filter(Number.isFinite),quality=qualities.reduce((a,b)=>a+b,0)+(qualities.length?Math.max(...qualities)*4:0);
    const distance=worlds.length&&typeof dist==="function"?Math.min(...worlds.map(w=>dist(w,p))):0;
    return{p,score:quality-distance/180};
  }).sort((a,b)=>b.score-a.score).map(x=>x.p);
}
function stxCOExpansionVariants(limit){
  if(limit<=0)return[];
  const bases=COMMANDS.filter(c=>STX_CO_EXPANSION_IDS.has(stxCOBaseId(c))),targets=stxCOExpansionTargets();
  if(!bases.length||!targets.length)return[];
  const out=[];
  for(let i=0;i<targets.length&&out.length<limit;i++){
    const base=bases[i%bases.length],choice=stxCOChoiceFromCommand(base,targets[i]);
    if(stxCOCanOffer(choice))out.push(choice);
  }
  return out;
}
function stxCOScoreCommand(c,categoryCounts){
  let score=0;
  try{score=Number(c.score?.()||0)}catch(_){score=0}
  if(!Number.isFinite(score))score=0;
  score+=rand(-10,10);
  if(STX_CO_EXPANSION_IDS.has(stxCOBaseId(c)))score+=20;
  score-=(categoryCounts.get(c.cat)||0)*8;
  return score;
}
function stxCOBuildCommandHand(current=state.commandChoices,targetCount=stxCOOptionCount()){
  const choices=(Array.isArray(current)?current:[]).filter(Boolean),keys=new Set(choices.map(stxCOChoiceKey));
  const expansionQuota=stxCOExpansionTargets().length?stxCOExpansionQuota(targetCount):0;
  let expansionCount=choices.filter(stxCOIsExpansionChoice).length;

  if(expansionCount<expansionQuota){
    for(const choice of stxCOExpansionVariants(expansionQuota*3)){
      const key=stxCOChoiceKey(choice);if(keys.has(key))continue;
      choices.push(choice);keys.add(key);expansionCount++;
      if(expansionCount>=expansionQuota||choices.length>=targetCount)break;
    }
  }

  const categoryCounts=new Map();choices.forEach(c=>categoryCounts.set(c.cat,(categoryCounts.get(c.cat)||0)+1));
  const candidates=COMMANDS.map(c=>{
    const choice=stxCOChoiceFromCommand(c);return{choice,score:stxCOScoreCommand(c,categoryCounts)};
  }).filter(x=>x.score>5&&stxCOCanOffer(x.choice)&&!keys.has(stxCOChoiceKey(x.choice))).sort((a,b)=>b.score-a.score);

  while(choices.length<targetCount&&candidates.length){
    let bestIndex=0,bestAdjusted=-Infinity;
    for(let i=0;i<candidates.length;i++){
      const entry=candidates[i],adjusted=entry.score-(categoryCounts.get(entry.choice.cat)||0)*7;
      if(adjusted>bestAdjusted){bestAdjusted=adjusted;bestIndex=i}
    }
    const [{choice}]=candidates.splice(bestIndex,1),key=stxCOChoiceKey(choice);
    if(keys.has(key))continue;
    choices.push(choice);keys.add(key);categoryCounts.set(choice.cat,(categoryCounts.get(choice.cat)||0)+1);
  }

  return choices.slice(0,targetCount);
}
function stxCOApplyScrollablePanel(){
  const grid=$("commandGrid");if(!grid)return;
  const panel=grid.closest?.(".command-panel");if(panel)panel.classList.add("stx-scaled-command-panel");
}
function stxCOUpdateSlotLabel(){
  const box=$("commandSlots");if(!box)return;
  const base=box.textContent.replace(/\s·\s\d+ options$/,'');
  box.textContent=`${base} · ${state.commandChoices.length} options`;
}

(function stxCOInstallStyle(){
  if(document.getElementById?.("stx-command-option-scaling-style"))return;
  const style=document.createElement("style");style.id="stx-command-option-scaling-style";
  style.textContent=`
    .command-panel.stx-scaled-command-panel{max-height:min(88vh,860px);display:flex;flex-direction:column;overflow:hidden}
    .command-panel.stx-scaled-command-panel .command-head,.command-panel.stx-scaled-command-panel .command-footer{flex:0 0 auto}
    .command-panel.stx-scaled-command-panel .command-grid{flex:1 1 auto;min-height:0;max-height:62vh;overflow-y:auto;overscroll-behavior:contain;padding:2px 7px 8px 2px;scrollbar-width:thin;scrollbar-color:#42598c transparent;grid-template-columns:repeat(4,minmax(0,1fr));align-content:start}
    .command-panel.stx-scaled-command-panel .command-card{min-height:185px}
    @media(max-width:980px){.command-panel.stx-scaled-command-panel .command-grid{grid-template-columns:repeat(2,minmax(0,1fr));max-height:68vh}}
    @media(max-width:620px){.command-wrap{padding:8px}.command-panel.stx-scaled-command-panel{max-height:94vh}.command-panel.stx-scaled-command-panel .command-grid{grid-template-columns:1fr;max-height:72vh}.command-panel.stx-scaled-command-panel .command-card{min-height:0}}
  `;
  document.head.appendChild(style);
})();

const STX_CO_renderCommands=renderCommands;
renderCommands=function(){STX_CO_renderCommands();stxCOApplyScrollablePanel()};

const STX_CO_openCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_CO_openCommandPhase();if($("commandModal").hidden)return;
  const targetCount=stxCOOptionCount(),expanded=stxCOBuildCommandHand(state.commandChoices,targetCount);
  state.commandChoices=expanded;state.commandSelected.clear();
  stxCOUpdateSlotLabel();renderCommands();
};

globalThis.SpaceTyrantsCommandOptions={
  optionCount:stxCOOptionCount,
  expansionQuota:stxCOExpansionQuota,
  isExpansionChoice:stxCOIsExpansionChoice,
  buildHand:stxCOBuildCommandHand
};
