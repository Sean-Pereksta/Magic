function stxIncomingThreat(p){
  return state.ships.filter(s=>s.owner!==0&&STX_MILITARY_TYPES.has(s.type)&&s.to===p.id&&empiresAtWar(0,s.owner)&&stxMilitaryShipVisible(s)).sort((a,b)=>shipEta(a)-shipEta(b))[0]||null;
}
function stxMobilizeDefenders(threat,target){
  if(!threat||!target||target.owner!==0)return;
  threat.stxDefenseAlerted=threat.stxDefenseAlerted||{};if(threat.stxDefenseAlerted[target.id])return;
  const range=1450*modifier(empire(0),"responseRange")+(target.scanArray||0)*260;
  const nearby=playerWorlds().filter(p=>p!==target&&!p.underAttack&&dist(p,target)<range).sort((a,b)=>dist(a,target)-dist(b,target));
  let dispatched=0;
  for(const source of nearby){
    const fleet=state.fleets.filter(f=>f.owner===0&&!f.destroyed&&f.location===source.id&&!state.ships.some(s=>s.fleetId===f.id)).sort((a,b)=>b.strength-a.strength)[0];
    if(fleet&&fleet.strength>4){fleet.location=null;fleet.status=`Rapid response to ${target.name}`;createShip("fleet",source,target,0,{strength:fleet.strength,fleetId:fleet.id,vesselName:fleet.name,reinforcement:true,speedBoost:1.35+(source.orbitals?.base||0)*.35});dispatched++}
    else if(source.garrison>16){const strength=Math.max(7,source.garrison*.3);source.garrison-=strength;deployFleet(source,target,0,strength,{reinforcement:true,status:`Defending ${target.name}`,speedBoost:1.25});dispatched++}
    if(dispatched>=2)break;
  }
  threat.stxDefenseAlerted[target.id]=true;target.incomingThreat={shipId:threat.id,owner:threat.owner,detectedAt:state.simTime};
  if(dispatched){logEvent(`Long-range sensors detected ${empire(threat.owner).name} forces approaching ${target.name}. ${dispatched} nearby response group${dispatched===1?" is":"s are"} mobilizing.`,"danger");stxActivity(`Rapid-response forces mobilized to protect ${target.name}.`,target.id,null,"danger")}
}
function stxThreatResponseTick(){
  playerWorlds().forEach(p=>{const t=stxIncomingThreat(p);if(t)stxMobilizeDefenders(t,p);else if(p.incomingThreat&&state.simTime-(p.incomingThreat.detectedAt||0)>18)p.incomingThreat=null});
}
const STX_coreDispatchBattleReinforcements=dispatchBattleReinforcements;
dispatchBattleReinforcements=function(b,p,owner,attacking){
  STX_coreDispatchBattleReinforcements(b,p,owner,attacking);
  if(owner!==0||attacking)return;
  const threat={id:`battle-${b.id}`,owner:b.attacker,stxDefenseAlerted:{}};stxMobilizeDefenders(threat,p);
};

function stxRetreatDestination(p){return playerWorlds().filter(q=>q!==p&&!q.underAttack).sort((a,b)=>dist(a,p)-dist(b,p)||a.pop/a.capacity-b.pop/b.capacity)[0]||null}
function stxFallbackFromPlanet(p,silent=false){
  if(!p||p.owner!==0)return false;const safe=stxRetreatDestination(p);if(!safe){if(!silent)showToast("No Mandate world is available to fall back to");return false}
  const b=activeBattleAt(p);let moved=0;
  if(b&&b.defender===0){
    const ids=[...(b.defenderFleetIds||[])];const each=Math.max(3,b.defenderStrength*.62/Math.max(1,ids.length||1));
    if(ids.length){ids.forEach(id=>{const f=fleetRecord(id);if(!f||f.destroyed)return;f.location=null;f.status=`Falling back to ${safe.name}`;f.strength=Math.min(Math.max(3,f.strength*.72),each);createShip("fleet",p,safe,0,{strength:f.strength,fleetId:f.id,vesselName:f.name,retreat:true,speedBoost:1.35});moved++});b.defenderFleetIds=[]}
    else if(b.defenderStrength>8){const strength=b.defenderStrength*.48, f=registerFleet(0,p,strength,"fleet");f.location=null;f.status=`Emergency withdrawal to ${safe.name}`;createShip("fleet",p,safe,0,{strength,fleetId:f.id,vesselName:f.name,retreat:true,speedBoost:1.3});moved++}
    b.defenderStrength=Math.max(.25,b.defenderStrength*.18);b.maxDuration=Math.min(b.maxDuration,b.elapsed+4.5);
  }else{
    const fleets=state.fleets.filter(f=>f.owner===0&&!f.destroyed&&f.location===p.id&&!state.ships.some(s=>s.fleetId===f.id));
    fleets.forEach(f=>{f.location=null;f.status=`Falling back to ${safe.name}`;createShip("fleet",p,safe,0,{strength:f.strength,fleetId:f.id,vesselName:f.name,retreat:true,speedBoost:1.3});moved++});
    if(p.garrison>8){const strength=p.garrison*.65;p.garrison-=strength;deployFleet(p,safe,0,strength,{retreat:true,status:`Falling back from ${p.name}`,speedBoost:1.22});moved++}
  }
  p.fallbackOrdered=state.simTime;stxActivity(`${moved||"Local"} force group${moved===1?"":"s"} fell back from ${p.name} toward ${safe.name}.`,p.id,null,"warning");
  if(!silent){logEvent(`FALL BACK: forces at ${p.name} are withdrawing toward ${safe.name}. The planet may be left exposed.`,"warning");showToast(`Withdrawal from ${p.name} underway`)}
  return true;
}
function stxSurrenderOdds(p,enemyId){
  const w=getWar(0,enemyId);if(!w)return 0;const b=activeBattleAt(p),enemy=empire(enemyId),age=Math.max(0,state.simTime-w.startedAt),value=p.infra.factory*8+p.infra.mine*4+p.infra.shipyard*10+(p.orbitals?.station||0)*10+(p.orbitals?.base||0)*18+p.pop*16;
  let battleEdge=.5;if(b&&b.attacker===enemyId)battleEdge=b.attackerStrength/Math.max(.1,b.attackerStrength+b.defenderStrength);
  const relative=empireFleet(enemyId)/Math.max(1,empireFleet(0)+empireFleet(enemyId));
  let odds=.16+battleEdge*.34+relative*.18+enemy.aggression*.09+Math.min(.15,value/180)+Math.min(.1,age/500);
  if(p.home)odds-=.1;if(playerWorlds().length<=1)odds-=.12;
  return clamp(odds,.08,.94);
}
function stxAcceptPlanetSurrender(p,enemyId){
  const w=getWar(0,enemyId);if(!w)return false;const b=activeBattleAt(p);stxFallbackFromPlanet(p,true);
  if(b){const af=(b.attackerFleetIds||[]).map(fleetRecord).filter(Boolean);af.forEach(f=>{f.location=p.id;f.status=`Occupying ${p.name}`;f.strength=Math.max(2,b.attackerStrength/Math.max(1,af.length))});state.battles=state.battles.filter(x=>x.id!==b.id)}
  p.owner=enemyId;p.governor=makeGovernor(enemyId);p.underAttack=false;p.incomingThreat=null;p.unrest=.42;p.garrison=Math.max(7,b?.attackerStrength*.32||12);p.warDamage=clamp((p.warDamage||0)+.08,0,1);p.ownershipHistory=[...(p.ownershipHistory||[]),{owner:enemyId,time:state.simTime}];p.localProject=null;p.expansionProject=null;empire(enemyId).intel.add(p.id);w.conquests[enemyId]=(w.conquests[enemyId]||0)+1;
  endWar(w,"a planetary surrender");w.peaceGuaranteedUntil=state.simTime+60;w.cededPlanetId=p.id;w.cededBy=0;
  logEvent(`${p.name} was ceded to ${empire(enemyId).name}. A minimum 10-cycle peace is now guaranteed unless the Mandate declares war again.`,"danger");
  galacticNews(`${p.name.toUpperCase()} CEDED FOR PEACE`,`${empire(enemyId).name} accepted the planet in exchange for peace. The treaty guarantees at least ten cycles without renewed foreign hostilities unless the Aurelian Mandate breaks it.`,"warning",p.id);
  renderRivals();updateHud(true);stxRefreshTacticalRail(true);return true;
}
function stxOfferPlanetSurrender(p,enemyId){
  if(!p||p.owner!==0||!getWar(0,enemyId))return;const odds=stxSurrenderOdds(p,enemyId);
  if(p.lastSurrenderOffer&&state.simTime-p.lastSurrenderOffer<10)return showToast("The enemy has not reconsidered your last offer yet");p.lastSurrenderOffer=state.simTime;
  if(random()<=odds){stxAcceptPlanetSurrender(p,enemyId);showToast(`${empire(enemyId).name} accepted the surrender`)}else{adjustRelation(0,enemyId,-.025);logEvent(`${empire(enemyId).name} rejected the offer of ${p.name} for peace.`,"danger");showToast(`Peace offer rejected · ${Math.round(odds*100)}% estimated chance`);renderPlanet()}
}

const STX_coreArriveShip=arriveShip;
arriveShip=function(s,p){
  if(s.owner===0){
    const cargo=Object.entries(s.cargo||{}).filter(([r])=>r!=="population").map(([r,v])=>`${Math.round(v)} ${RESOURCE_LABEL[r]||r}`).join(", ");
    if(s.type==="fleet"||s.type==="patrol")stxActivity(`${s.vesselName||TYPE_LABEL[s.type]} reached ${p.name}${p.owner!==0?" for combat":" and is now reinforcing/patrolling the system"}.`,p.id,s.fleetId,p.owner!==0?"danger":"good");
    else if(s.type==="scout")stxActivity(`${s.vesselName||"Scout"} completed a reconnaissance pass at ${p.name}.`,p.id,null,"good");
    else if(s.type==="colony")stxActivity(`${s.vesselName||"Colony transport"} reached ${p.name} with ${fmtNum(s.cargo?.population||0)} settlers.`,p.id,null,"good");
    else if(cargo)stxActivity(`${s.vesselName||TYPE_LABEL[s.type]} delivered ${cargo} to ${p.name}.`,p.id,null,"good");
  }
  STX_coreArriveShip(s,p);
};
const STX_coreTickBuildQueue=tickBuildQueue;
tickBuildQueue=function(p,dt){
  const before=p.buildQueue?.[0],type=before?.type;STX_coreTickBuildQueue(p,dt);
  if(p.owner===0&&before&&p.buildQueue?.[0]!==before){
    const launched=state.ships.filter(s=>s.owner===0&&s.from===p.id&&s.type===type).sort((a,b)=>b.id.localeCompare(a.id))[0],target=launched&&state.planets.find(q=>q.id===launched.to);
    stxActivity(`${p.name} completed a ${TYPE_LABEL[type]||type}${target?`; it is now assigned toward ${target.name}`:"; it joined local defenses"}.`,p.id,launched?.fleetId,"good");
  }
};
const STX_coreRenderShipLedger=renderShipLedger;
renderShipLedger=function(p){
  let active=STX_coreRenderShipLedger(p),extras=[];
  const stationed=state.fleets.filter(f=>f.owner===p.owner&&!f.destroyed&&f.location===p.id).sort((a,b)=>b.strength-a.strength).slice(0,5);
  stationed.forEach(f=>extras.push(`<div class="project-row stx-stationed" data-stx-fleet="${f.id}"><div class="project-head"><strong>◆ ${f.name}</strong><b>${Math.round(f.strength)} power</b></div><div class="project-desc">Admiral ${f.admiral.name} · ${f.status}. This completed fleet is physically stationed at ${p.name}.</div><div class="project-meta"><span>${Math.round(f.veterans*100)}% veterans</span><span>${f.victories}/${f.battles} victories</span></div></div>`));
  const history=(empire(0)?.shipActivity||[]).filter(a=>a.planetId===p.id&&state.simTime-a.time<120).slice(0,6);
  history.forEach(a=>extras.push(`<div class="project-row stx-activity"><div class="project-head"><strong>Mission update</strong><b>Cycle ${Math.floor(a.time/6)+1}</b></div><div class="project-desc">${a.text}</div></div>`));
  if(extras.length&&active.includes("No tracked ships"))active="";
  return active+extras.join("");
};

function stxDrawOrbitals(p,s,r){
  const owner=p.owner!==null?empire(p.owner):null,stations=p.orbitals?.station||0,bases=p.orbitals?.base||0,now=performance.now()/1000,zoom=state.camera.zoom;
  if(!stations&&!bases&&!p.scanProject&&!(p.scanArray||0))return;
  ctx.save();ctx.lineWidth=Math.max(1,zoom);
  const drawBody=(kind,index,count)=>{
    const baseR=r*(kind==="base"?3.1:2.45)+index*5,angle=now*(kind==="base"?.23:.34)*(index%2?1:-1)+(+p.id.slice(1))*1.17+index*2.2,x=s.x+Math.cos(angle)*baseR,y=s.y+Math.sin(angle)*baseR*.42,color=kind==="base"?"#ffad74":owner?.color||"#9cecff";
    ctx.strokeStyle=color+"66";ctx.setLineDash([3,5]);ctx.beginPath();ctx.ellipse(s.x,s.y,baseR,baseR*.42,0,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.translate(x,y);ctx.rotate(angle+Math.PI/2);ctx.shadowColor=color;ctx.shadowBlur=12;ctx.strokeStyle=color;ctx.fillStyle="rgba(8,18,35,.92)";
    if(kind==="station"){ctx.fillRect(-5,-3,10,6);ctx.strokeRect(-5,-3,10,6);ctx.beginPath();ctx.moveTo(-9,0);ctx.lineTo(9,0);ctx.moveTo(0,-7);ctx.lineTo(0,7);ctx.stroke();ctx.fillStyle="#fff";ctx.fillRect(-1,-1,2,2)}else{ctx.beginPath();for(let i=0;i<8;i++){const a=i*Math.PI/4,rr=i%2?5:8;const px=Math.cos(a)*rr,py=Math.sin(a)*rr;i?ctx.lineTo(px,py):ctx.moveTo(px,py)}ctx.closePath();ctx.fill();ctx.stroke();ctx.fillStyle=color;ctx.fillRect(-2,-2,4,4)}
    ctx.setTransform(dpr(),0,0,dpr(),0,0);
    if(zoom>.68){ctx.fillStyle=color;ctx.font="8px system-ui";ctx.textAlign="left";ctx.fillText(kind==="base"?"MILITARY BASE":"ORBITAL STATION",x+9,y-7)}
  };
  for(let i=0;i<stations;i++)drawBody("station",i,stations);for(let i=0;i<bases;i++)drawBody("base",i,bases);
  if(p.scanArray){const rr=r*(3.8+p.scanArray*.12),a=now*.16+(+p.id.slice(1));ctx.strokeStyle="rgba(92,242,210,.42)";ctx.setLineDash([2,6]);ctx.beginPath();ctx.ellipse(s.x,s.y,rr,rr*.38,0,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.fillStyle="#83ffe5";ctx.beginPath();ctx.arc(s.x+Math.cos(a)*rr,s.y+Math.sin(a)*rr*.38,2.4,0,6.283);ctx.fill()}
  if(p.scanProject){const pr=clamp(p.scanProject.progress||0,0,1),rr=r+11;ctx.strokeStyle="#83ffe5";ctx.lineWidth=2;ctx.beginPath();ctx.arc(s.x,s.y,rr,-Math.PI/2,-Math.PI/2+6.283*pr);ctx.stroke()}
  ctx.restore();
}
const STX_coreDrawConstructionState=drawConstructionState;
drawConstructionState=function(p,s,r){STX_coreDrawConstructionState(p,s,r);stxDrawOrbitals(p,s,r)};

const STX_coreDrawShips=drawShips;
drawShips=function(){
  const all=state.ships,filtered=all.filter(s=>!STX_MILITARY_TYPES.has(s.type)||stxMilitaryShipVisible(s));
  if(filtered.length===all.length)return STX_coreDrawShips();
  state.ships=filtered;try{STX_coreDrawShips()}finally{state.ships=all}
};
function stxDrawFleetHighlights(){
  const now=performance.now()/1000,pulse=.5+.5*Math.sin(now*4.5);ctx.save();
  state.fleets.filter(stxFleetVisible).forEach(f=>{
    const pos=stxFleetPosition(f);if(!pos||!visible(pos.x,pos.y,150))return;const s=worldToScreen(pos.x,pos.y),mine=f.owner===0,color=mine?"#78f2ff":empire(f.owner).color,r=mine?11+pulse*4:9+pulse*2;
    ctx.strokeStyle=color;ctx.lineWidth=mine?2:1.4;ctx.globalAlpha=mine?.88:.7;ctx.beginPath();ctx.arc(s.x,s.y,r,0,6.283);ctx.stroke();ctx.setLineDash([3,4]);ctx.beginPath();ctx.arc(s.x,s.y,r+6,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=1;
    ctx.fillStyle="rgba(3,7,18,.86)";ctx.fillRect(s.x+10,s.y-17,Math.min(150,Math.max(70,f.name.length*5.2)),25);ctx.fillStyle=color;ctx.font="700 9px system-ui";ctx.textAlign="left";ctx.fillText(`${mine?"◆ OUR FLEET":"◇ CONTACT"} · ${Math.round(f.strength)}`,s.x+14,s.y-7);ctx.fillStyle="#dceaff";ctx.font="8px system-ui";ctx.fillText(f.name.slice(0,24),s.x+14,s.y+2);
  });ctx.restore();
}
function stxDrawBattleBeacons(){
  const now=performance.now()/1000,pulse=.5+.5*Math.sin(now*6);ctx.save();
  state.battles.forEach(b=>{const p=state.planets.find(q=>q.id===b.planetId);if(!p||!visible(p.x,p.y,230))return;const s=worldToScreen(p.x,p.y),r=27+pulse*8,total=Math.max(1,b.attackerStrength+b.defenderStrength),ar=b.attackerStrength/total;
    ctx.strokeStyle="rgba(255,82,108,.92)";ctx.lineWidth=2.4;ctx.beginPath();ctx.arc(s.x,s.y,r,0,6.283);ctx.stroke();ctx.beginPath();ctx.moveTo(s.x,s.y-r-5);ctx.lineTo(s.x,s.y-r-31);ctx.lineTo(s.x+16,s.y-r-25);ctx.lineTo(s.x,s.y-r-18);ctx.stroke();
    const w=150,x=s.x-w/2,y=s.y+r+11;ctx.fillStyle="rgba(4,7,18,.9)";ctx.fillRect(x,y,w,28);ctx.fillStyle=empire(b.attacker).color;ctx.fillRect(x+5,y+18,(w-10)*ar,4);ctx.fillStyle=empire(b.defender).color;ctx.fillRect(x+5+(w-10)*ar,y+18,(w-10)*(1-ar),4);ctx.fillStyle="#fff";ctx.font="800 9px system-ui";ctx.textAlign="center";ctx.fillText(`⚑ BATTLE · ${p.name}`,s.x,y+10);
  });ctx.restore();
}
function stxDrawScanCoverage(){
  if(!stxShowSensors||state.camera.zoom>.78)return;ctx.save();ctx.setLineDash([6,10]);ctx.lineWidth=1;
  playerWorlds().filter(p=>(p.scanArray||0)>0).forEach(p=>{const s=worldToScreen(p.x,p.y),rr=stxScanRange(p)*state.camera.zoom;ctx.strokeStyle="rgba(88,255,220,.16)";ctx.fillStyle="rgba(88,255,220,.018)";ctx.beginPath();ctx.arc(s.x,s.y,rr,0,6.283);ctx.fill();ctx.stroke()});ctx.restore();
}
const STX_coreDraw=draw;
draw=function(){STX_coreDraw();stxDrawScanCoverage();stxDrawFleetHighlights();stxDrawBattleBeacons()};
const STX_coreSpawnBattleBurst=spawnBattleBurst;
spawnBattleBurst=function(b,p){STX_coreSpawnBattleBurst(b,p);if(state.effects.length<152&&random()<.72){const a=rand(0,6.283),r=rand(35,125);state.effects.push({type:random()<.32?"shock":"spark",x:p.x+Math.cos(a)*r,y:p.y+Math.sin(a)*r,life:.55,maxLife:.55,size:rand(7,16),color:random()<.5?empire(b.attacker).color:empire(b.defender).color})}};

