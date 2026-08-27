function stxQueueScanProject(p,mandated=false){
  if(!p||p.owner===null||p.scanProject||(p.scanArray||0)>=3) return false;
  stxEnsurePlanetState(p);stxEnsureEmpireState(empire(p.owner));
  p.scanProject={progress:0,need:{...STX_SCAN_COST},startedAt:state.simTime,mandated};p.mandateGlow=1;
  Object.entries(STX_SCAN_COST).forEach(([r,a])=>{if((p.stock[r]||0)<a)addOrder(p,"sensor",r,a,mandated?6:3,"Deep-space sensor array")});
  if(p.owner===0) logEvent(`${p.name} began construction of a deep-space sensor array.`,"good");
  return true;
}
function stxTickScanProject(p,dt){
  const q=p.scanProject;if(!q||p.owner===null)return;
  Object.entries(q.need).forEach(([r,a])=>{if((p.stock[r]||0)<a)addOrder(p,"sensor",r,a,q.mandated?6:3,"Deep-space sensor array")});
  if(!Object.entries(q.need).every(([r,a])=>(p.stock[r]||0)>=a)) return;
  q.progress+=dt*.0052*(1+p.infra.factory*.28+p.infra.research*.52)*(q.mandated?1.35:1);
  if(q.progress<1)return;
  Object.entries(q.need).forEach(([r,a])=>consume(p,r,a));
  p.scanArray=(p.scanArray||0)+1;p.scanProject=null;p.mandateGlow=1;
  const e=empire(p.owner);e.tech.sensors=(e.tech.sensors||0)+.14;e.tech.orbital=(e.tech.orbital||0)+.04;
  state.effects.push({type:"shock",x:p.x,y:p.y,life:1.6,maxLife:1.6,size:48,color:e.color});
  if(p.owner===0){logEvent(`${p.name}'s sensor array is online. Enemy fleets can now be tracked much farther from the frontier.`,"good");galacticNews(`${p.name.toUpperCase()} SENSOR ARRAY ONLINE`,`Deep-space tracking coverage has expanded around ${p.name}. Military contacts inside the new envelope will remain visible on the galactic map.`,"good",p.id)}
}
const STX_coreTickPlanet=tickPlanet;
tickPlanet=function(p,dt){STX_coreTickPlanet(p,dt);stxTickScanProject(p,dt)};
const STX_coreGovernorTick=governorTick;
governorTick=function(){
  STX_coreGovernorTick();
  state.planets.forEach(p=>{
    if(p.owner===null||p.scanProject||(p.scanArray||0)>=2)return;
    const e=empire(p.owner);stxEnsureEmpireState(e);
    const sophisticated=(e.tech.orbital||0)>.28||p.infra.research>=2||p.infra.shipyard>=2;
    const threatened=borderThreat(p)>.42||state.wars.some(w=>w.active&&(w.a===p.owner||w.b===p.owner));
    if(sophisticated&&threatened&&random()<(p.owner===0?.08:.055))stxQueueScanProject(p,false);
  });
};

function stxMineTargets(){return playerWorlds().filter(p=>!p.localProject&&!p.underAttack&&p.infra.mine<Math.min(6,Math.ceil(sustainableMines(p)))).sort((a,b)=>Math.max(...Object.values(b.quality))-Math.max(...Object.values(a.quality))||a.infra.mine-b.infra.mine)}
function stxFactoryTargets(){return playerWorlds().filter(p=>!p.localProject&&!p.underAttack&&p.infra.factory<6).sort((a,b)=>(b.infra.mine+b.pop*8)-(a.infra.mine+a.pop*8)||a.infra.factory-b.infra.factory)}
function stxSensorTarget(){return playerWorlds().filter(p=>!p.scanProject&&(p.scanArray||0)<3).sort((a,b)=>borderThreat(b)-borderThreat(a)||(b.infra.research+b.infra.shipyard)-(a.infra.research+a.infra.shipyard))[0]||null}
function stxDevelopmentTick(){
  const e=empire(0);if(!e)return;
  if(hasDirective(e,"mineWorks"))stxMineTargets().slice(0,2).forEach(p=>startLocalProject(p,"mine","Imperial mineworks directive"));
  if(hasDirective(e,"factoryWave"))stxFactoryTargets().slice(0,2).forEach(p=>startLocalProject(p,"factory","Imperial factory directive"));
  const sd=hasDirective(e,"sensorArray");if(sd){const p=state.planets.find(x=>x.id===sd.target)||stxSensorTarget();if(p&&!p.scanProject&&(p.scanArray||0)<3)stxQueueScanProject(p,true)}
}
COMMANDS.push(
  {id:"mineWorks",cat:"Resources",title:"Open New Mining Districts",desc:"Turn the richest underdeveloped worlds into visible mine-construction programs.",effects:["Up to four mining projects","Mining output +45%","Projects continue as sites finish"],score:()=>stxMineTargets().length?82:12,apply:e=>{setModifier(e,"mining",1.45,115);addDirective(e,"mineWorks",null,115);stxMineTargets().slice(0,4).forEach(p=>startLocalProject(p,"mine","Imperial mineworks directive"))}},
  {id:"factoryWave",cat:"Industry",title:"Factory Construction Wave",desc:"Authorize a sustained buildout of factories instead of only temporary output bonuses.",effects:["Up to four factory builds","Industry +35%","Shipyards receive more components"],score:()=>stxFactoryTargets().length?84:14,apply:e=>{setModifier(e,"industry",1.35,120);setModifier(e,"shipbuilding",1.15,120);addDirective(e,"factoryWave",null,120);stxFactoryTargets().slice(0,4).forEach(p=>startLocalProject(p,"factory","Imperial factory directive"))}},
  {id:"sensorArray",cat:"Research",title:"Construct Frontier Sensor Array",desc:"Build a permanent deep-space tracking installation on a strategic world.",effects:["Enemy fleet detection expands","Approaching invasions revealed","Nearby defense can mobilize early"],score:()=>stxSensorTarget()?86:0,target:()=>stxSensorTarget(),apply:(e,t)=>{if(t){e.tech.sensors=(e.tech.sensors||0)+.08;addDirective(e,"sensorArray",t.id,150);stxQueueScanProject(t,true)}}}
);

function stxTargetStrength(p){return p.garrison+p.infra.defense*8+(p.orbitals?.station||0)*9+(p.orbitals?.base||0)*22+(p.infra.shipyard||0)*4}
function stxNearestPlayerDistance(p){const worlds=playerWorlds();return worlds.length?Math.min(...worlds.map(q=>dist(p,q))):Infinity}
function stxInvasionCandidates(mode="balanced"){
  const wars=state.wars.filter(w=>w.active&&(w.a===0||w.b===0)),foes=new Set(wars.map(w=>w.a===0?w.b:w.a));
  let worlds=state.planets.filter(p=>p.owner!==null&&p.owner!==0&&(foes.size?foes.has(p.owner):true)&&!p.underAttack);
  worlds=worlds.filter(p=>stxNearestPlayerDistance(p)<3600||foes.has(p.owner));
  return worlds.map(p=>{
    const d=stxNearestPlayerDistance(p),def=stxTargetStrength(p),value=p.infra.factory*15+p.infra.shipyard*20+p.infra.mine*7+(p.orbitals?.station||0)*18+(p.orbitals?.base||0)*30+p.pop*35;
    let score=value-def*.55-d/155;
    if(mode==="weak")score=-def-d/180+value*.2;
    if(mode==="industry")score=value*1.6-def*.45-d/180;
    if(mode==="strategic")score=(p.infra.shipyard*25+(p.orbitals?.station||0)*35+(p.orbitals?.base||0)*55+p.home*30)+value*.45-def*.5-d/190;
    const opening=clamp(.72-def/(Math.max(35,empireFleet(0))*1.7)-d/9000+value/420, .08,.92);
    return{p,score,opening,def,value,d};
  }).sort((a,b)=>b.score-a.score);
}
function stxQueueInvasion(target,label="Imperial invasion directive",launchNow=true){
  if(!target||target.owner===null||target.owner===0)return false;
  const e=empire(0);stxEnsureEmpireState(e);
  let w=getWar(0,target.owner);if(!w)w=declareWar(0,target.owner,label.toLowerCase());
  if(!w)return false;
  e.invasionPlans=e.invasionPlans.filter(pl=>state.planets.find(p=>p.id===pl.targetId)?.owner!==0&&pl.status!=="cancelled");
  let plan=e.invasionPlans.find(pl=>pl.targetId===target.id);
  if(!plan){plan={id:`ip${Math.floor(random()*1e9)}`,targetId:target.id,foeId:target.owner,createdAt:state.simTime,lastLaunchAt:-999,status:"assembling",label};e.invasionPlans.push(plan)}
  addDirective(e,"invasion",target.id,150);plan.status="assembling";
  if(launchNow)stxMobilizeInvasionPlan(plan,true);
  logEvent(`INVASION DIRECTIVE: ${target.name} is now the active objective. Fleets will stage when a viable launch window appears.`,"warning");
  return true;
}
function stxInvasionSources(target){
  return playerWorlds().filter(p=>p!==target&&!p.underAttack&&p.garrison>17&&dist(p,target)<3500).sort((a,b)=>dist(a,target)-dist(b,target)||b.garrison-a.garrison);
}
function stxMobilizeInvasionPlan(plan,force=false){
  const target=state.planets.find(p=>p.id===plan.targetId);if(!target||target.owner===0||target.owner===null){plan.status="completed";return}
  if(!empiresAtWar(0,target.owner)){plan.status="awaiting war";return}
  const inbound=state.ships.filter(s=>s.owner===0&&STX_MILITARY_TYPES.has(s.type)&&s.to===target.id);
  if(target.underAttack||inbound.length){plan.status=target.underAttack?"engaged":"fleet inbound";return}
  if(!force&&state.simTime-plan.lastLaunchAt<18)return;
  const sources=stxInvasionSources(target);if(!sources.length){plan.status="waiting for fleet strength";return}
  const count=Math.min(force?2:1,sources.length);
  sources.slice(0,count).forEach(source=>{
    const strength=Math.max(8,source.garrison*(force?.36:.3));source.garrison=Math.max(4,source.garrison-strength);
    deployFleet(source,target,0,strength,{warId:getWar(0,target.owner)?.id,status:`Invading ${target.name}`,invasionPlanId:plan.id,speedBoost:1+(source.orbitals?.base||0)*.38});
  });
  plan.lastLaunchAt=state.simTime;plan.status="fleet inbound";stxActivity(`Fleet groups departed for the invasion of ${target.name}.`,target.id,null,"warning");
}
function stxCampaignTick(){
  const e=empire(0);if(!e)return;stxEnsureEmpireState(e);
  e.invasionPlans.forEach(plan=>{
    const target=state.planets.find(p=>p.id===plan.targetId);
    if(!target||target.owner===0){plan.status="completed";return}
    if(!empiresAtWar(0,target.owner)){plan.status="awaiting war";return}
    stxMobilizeInvasionPlan(plan,false);
  });
  const active=e.invasionPlans.find(pl=>!["completed","cancelled"].includes(pl.status)&&state.planets.find(p=>p.id===pl.targetId)?.owner!==0);
  if(active){const t=state.planets.find(p=>p.id===active.targetId);if(t)addDirective(e,"invasion",t.id,Math.max(40,(hasDirective(e,"invasion")?.until||state.simTime)-state.simTime+30))}
}
const STX_coreNearestEnemy=nearestEnemy;
nearestEnemy=function(){const d=hasDirective(empire(0),"invasion"),t=d&&state.planets.find(p=>p.id===d.target);return t&&t.owner!==0?t:STX_coreNearestEnemy()};
const STX_coreLaunchWarCampaign=launchWarCampaign;
launchWarCampaign=function(e,foeId){
  if(e?.id!==0)return STX_coreLaunchWarCampaign(e,foeId);
  const d=hasDirective(e,"invasion"),target=d&&state.planets.find(p=>p.id===d.target);
  if(!target||target.owner!==foeId||!getWar(0,foeId))return STX_coreLaunchWarCampaign(e,foeId);
  if(state.simTime-e.lastAttack<12||target.underAttack)return;
  const sources=stxInvasionSources(target);if(!sources.length)return;
  const source=sources[0],strength=Math.max(9,source.garrison*(.4+e.aggression*.18)*modifier(e,"militaryMorale"));
  source.garrison=Math.max(4,source.garrison-strength);deployFleet(source,target,0,strength,{warId:getWar(0,foeId)?.id,status:`Invading ${target.name}`});e.lastAttack=state.simTime;
};
const STX_coreOpenCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_coreOpenCommandPhase();
  if($("commandModal").hidden)return;
  const wars=state.wars.filter(w=>w.active&&(w.a===0||w.b===0));if(!wars.length)return;
  const ranked=stxInvasionCandidates("balanced").slice(0,3);if(!ranked.length)return;
  const count=Math.min(stageInfo()[1]>=2&&ranked.length>=3?3:2,ranked.length);
  const warChoices=ranked.slice(0,count).map((r,i)=>({
    id:`invasion-${r.p.id}-${Math.floor(state.simTime)}`,cat:i===0?"Military":i===1?"War":"Campaign",title:`Invade ${r.p.name}`,
    desc:`Commit fleets to ${r.p.name} and keep the objective active until the opening is used or the world falls.`,
    effects:[`Estimated opening ${Math.round(r.opening*100)}%`,`${Math.round(r.def)} defense strength`,`${Math.round(r.d)}u from nearest Mandate world`],
    targetObj:r.p,apply:(e,t)=>stxQueueInvasion(t,`Invasion of ${t.name}`,true)
  }));
  const keep=state.commandChoices.filter(c=>c.id!=="invasion"&&!c.id.startsWith("invasion-")).slice(0,Math.max(0,4-warChoices.length));
  state.commandChoices=[...warChoices,...keep];state.commandSelected.clear();renderCommands();
};
const STX_existingInvasion=COMMANDS.find(c=>c.id==="invasion");
if(STX_existingInvasion){
  STX_existingInvasion.title="Invade Best Available World";
  STX_existingInvasion.desc="Select the best current planetary opening instead of blindly attacking the nearest rival.";
  STX_existingInvasion.target=()=>stxInvasionCandidates("balanced")[0]?.p||STX_coreNearestEnemy();
  STX_existingInvasion.apply=(e,t)=>{if(t)stxQueueInvasion(t,`Invasion of ${t.name}`,true)};
}

