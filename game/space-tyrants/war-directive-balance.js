/* Space Tyrants — war directive balance.
   Invasion becomes a real, recurring option once the Mandate has deployable
   fleets, while active wars strongly bias the command hand toward executable
   invasions without turning peacetime into nonstop war spam. */

function stxWDBEnsureState(){
  const s=state.warDirectiveBalance||(state.warDirectiveBalance={});
  if(!Number.isFinite(s.phase))s.phase=0;
  if(!Number.isFinite(s.lastPeaceInvasionPhase))s.lastPeaceInvasionPhase=-3;
  return s;
}

function stxWDBPlayerWars(){
  return state.wars.filter(w=>w.active&&(w.a===0||w.b===0));
}

function stxWDBHasDeployableFleet(){
  if(typeof stxDGHasDeployableFleet==="function")return stxDGHasDeployableFleet();
  return state.fleets.some(f=>f.owner===0&&!f.destroyed&&f.location&&(Number(f.strength)||0)>4&&!state.ships.some(s=>s.fleetId===f.id));
}

function stxWDBRankedTargets(warOnly=false){
  const wars=stxWDBPlayerWars();
  const foes=new Set(wars.map(w=>w.a===0?w.b:w.a));
  let ranked=[];
  if(typeof stxInvasionCandidates==="function")ranked=stxInvasionCandidates("balanced");
  else ranked=state.planets.filter(p=>p.owner!==null&&p.owner!==0&&!p.underAttack).map(p=>({p,score:-Math.min(...playerWorlds().map(q=>dist(q,p)))}));
  return ranked.filter(r=>r?.p&&r.p.owner!==null&&r.p.owner!==0&&!r.p.underAttack&&(!warOnly||foes.has(r.p.owner)));
}

function stxWDBInvasionChoice(r,index=0,wartime=false){
  const target=r?.p||r;if(!target)return null;
  const opening=Number.isFinite(r?.opening)?Math.round(r.opening*100):null;
  const defense=Number.isFinite(r?.def)?Math.round(r.def):Math.round((target.garrison||0)+(target.infra?.defense||0)*8);
  const distance=Number.isFinite(r?.d)?Math.round(r.d):(typeof stxNearestPlayerDistance==="function"?Math.round(stxNearestPlayerDistance(target)):null);
  const foe=empire(target.owner)?.name||"rival empire";
  return {
    id:`invasion-${target.id}-${Math.floor(state.simTime)}-${index}`,
    cat:index===0?"Military":index===1?"War":"Campaign",
    title:wartime?`Invade ${target.name}`:`Authorize Invasion of ${target.name}`,
    desc:wartime
      ?`Commit a deployable named fleet against ${target.name}. This is a live objective in the war with ${foe}.`
      :`Open a deliberate war with ${foe} and make ${target.name} the first persistent invasion objective.`,
    effects:[
      opening!==null?`Estimated opening ${opening}%`:`Target defense ${defense}`,
      opening!==null?`${defense} defense strength`:(distance!==null?`${distance}u from nearest Mandate world`:"Persistent invasion objective"),
      distance!==null?`${distance}u from nearest Mandate world`:"Named fleets launch immediately"
    ],
    targetObj:target,
    apply:(e,t)=>{
      const objective=t||target;
      if(typeof stxQueueInvasion==="function")return stxQueueInvasion(objective,`${wartime?"War campaign":"Authorized invasion"} of ${objective.name}`,true);
      const base=COMMANDS.find(c=>c.id==="invasion");
      return base?.apply?.(e,objective);
    }
  };
}

function stxWDBIsInvasion(c){
  if(typeof stxDGIsInvasionChoice==="function")return stxDGIsInvasionChoice(c);
  return /invasion|\binvade\b/i.test(`${c?.id||""} ${c?.title||""}`);
}

function stxWDBPatchBaseInvasion(){
  const invasion=COMMANDS.find(c=>c.id==="invasion");if(!invasion)return;
  invasion.target=()=>stxWDBRankedTargets(stxWDBPlayerWars().length>0)[0]?.p||null;
  invasion.score=()=>{
    if(!stxWDBHasDeployableFleet())return 0;
    if(!stxWDBRankedTargets(stxWDBPlayerWars().length>0).length)return 0;
    return stxWDBPlayerWars().length?180:64;
  };
  invasion.apply=(e,t)=>{
    if(!t)return false;
    if(typeof stxQueueInvasion==="function")return stxQueueInvasion(t,`Invasion of ${t.name}`,true);
    addDirective(e,"invasion",t.id,150);
    if(!empiresAtWar(0,t.owner))declareWar(0,t.owner,"an imperial invasion order");
    return true;
  };
}

function stxWDBExecutable(c){
  if(!c)return false;
  if(typeof stxDGCanExecuteChoice==="function")return stxDGCanExecuteChoice(c);
  if(stxWDBIsInvasion(c))return stxWDBHasDeployableFleet()&&!!c.targetObj;
  return true;
}

function stxWDBWarHand(){
  if(!stxWDBHasDeployableFleet())return false;
  const ranked=stxWDBRankedTargets(true);if(!ranked.length)return false;
  const stage=stageInfo?.()[1]||1;
  const invasionCount=Math.min(stage>=2?3:2,ranked.length,3);
  const invasions=ranked.slice(0,invasionCount).map((r,i)=>stxWDBInvasionChoice(r,i,true)).filter(stxWDBExecutable);
  if(!invasions.length)return false;

  const existing=state.commandChoices.filter(c=>!stxWDBIsInvasion(c)&&stxWDBExecutable(c));
  const warSupport=existing.filter(c=>/military|war|campaign/i.test(c.cat||""));
  const civilian=existing.filter(c=>!warSupport.includes(c));
  const next=[...invasions];
  for(const c of [...warSupport,...civilian]){
    if(next.length>=4)break;
    if(next.some(x=>x.id===c.id))continue;
    next.push(c);
  }
  state.commandChoices=next.slice(0,4);
  state.commandSelected.clear();
  renderCommands();
  return true;
}

function stxWDBMaybePeaceInvasion(){
  const s=stxWDBEnsureState();
  if(stxWDBPlayerWars().length||!stxWDBHasDeployableFleet())return false;
  const ranked=stxWDBRankedTargets(false);if(!ranked.length)return false;
  if(state.commandChoices.some(stxWDBIsInvasion)){
    s.lastPeaceInvasionPhase=s.phase;
    return true;
  }
  const drought=s.phase-s.lastPeaceInvasionPhase;
  if(drought<2)return false;
  const chance=drought>=5?1:drought===4?.55:drought===3?.38:.24;
  if(random()>=chance)return false;
  const choice=stxWDBInvasionChoice(ranked[0],0,false);if(!choice||!stxWDBExecutable(choice))return false;
  const military=state.commandChoices.findIndex(c=>/military/i.test(c.cat||""));
  const replace=military>=0?military:Math.max(0,state.commandChoices.length-1);
  if(state.commandChoices.length<4)state.commandChoices.push(choice);else state.commandChoices[replace]=choice;
  s.lastPeaceInvasionPhase=s.phase;
  if(state.directiveImpact)state.directiveImpact.lastPeaceInvasionOffer=state.directiveImpact.phase||state.directiveImpact.lastPeaceInvasionOffer;
  state.commandSelected.clear();
  renderCommands();
  return true;
}

stxWDBPatchBaseInvasion();
stxWDBEnsureState();

const STX_WDB_generateGalaxy=generateGalaxy;
generateGalaxy=function(){STX_WDB_generateGalaxy();stxWDBEnsureState();stxWDBPatchBaseInvasion()};
const STX_WDB_loadGame=loadGame;
loadGame=function(){const ok=STX_WDB_loadGame();if(ok){stxWDBEnsureState();stxWDBPatchBaseInvasion()}return ok};

const STX_WDB_openCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_WDB_openCommandPhase();if($("commandModal").hidden)return;
  const s=stxWDBEnsureState();s.phase++;
  if(stxWDBPlayerWars().length){
    stxWDBWarHand();
    return;
  }
  stxWDBMaybePeaceInvasion();
};
