/* Campaign adapters use the same characters, inputs, passes and fixed-step court. */
let riseSetupSnapshot=null;
let riseBoxScores=Object.create(null);
let riseSubClock=0;
let riseLastShot=null;
let riseLastOwner=null;
let riseDrillMarkers=[];

function riseRuntimeProfile(athlete,team=0){
  return {...(profiles[athlete.profileKey]||profiles.crafty),...RiseToGlory.toProfile(athlete,team===0?riseState?.team:{}),teamId:team};
}

function riseStoreBox(player){
  if(!player.campaignPlayerId||player.teamId!==0) return;
  const box=riseBoxScores[player.campaignPlayerId]||(riseBoxScores[player.campaignPlayerId]={});
  for(const [key,n] of Object.entries(player.stats)) box[key]=(box[key]||0)+(Number(n)||0);
}

function initializeRisePlayers(){
  riseSubClock=0;riseLastOwner=null;riseLastShot=null;
  matchPlayers.forEach(player=>{
    player.campaignPlayerId=player.profile.campaignPlayerId;
    player.riseBenchTime=0;
  });
  document.getElementById('p1HudName').textContent=riseState.team.name;
  document.getElementById('p2HudName').textContent=riseTraining?'TRAINING COURT':(riseMatch.opponent||'RIVALS');
  riseCreateCourtHUD();
}

function riseCreateCourtHUD(){
  document.getElementById('riseCourtHUD')?.remove();
  const hud=document.createElement('div');hud.id='riseCourtHUD';
  const status=document.createElement('span');status.id='riseCourtStatus';hud.append(status);
  const sub=document.createElement('button');sub.textContent=riseTraining?'Finish drill':'Substitutions';
  sub.onclick=()=>riseTraining?finishRiseTraining(false):showRiseSubstitutions();hud.append(sub);
  document.body.append(hud);
}

function riseSnapshotSetup(){
  if(riseSetupSnapshot) return;
  riseSetupSnapshot={selectedP1,selectedP2,selectedP1Sneaker,selectedP2Sneaker,selectedP1Jersey,selectedP2Jersey,
    selectedP1Equipment,selectedP2Equipment,selectedCourt,selectedDifficulty,teamSize,twoPlayerMode,
    localTeamMode,targetScore,winBy,gameplayPreset:gameSettings.get('gameplayPreset'),gameMode:{...gameMode},teamOffensiveStyles:[...teamOffensiveStyles]};
}

function leaveRiseMatch(){
  if(typeof stopRisePreview==='function') stopRisePreview();
  if(riseMatch&&riseState) RiseToGlory.save(riseState);
  riseMatch=null;riseTraining=null;riseBoxScores=Object.create(null);
  document.getElementById('riseCourtHUD')?.remove();
  document.getElementById('riseSubPanel')?.remove();
  for(const marker of riseDrillMarkers){scene.remove(marker);marker.geometry.dispose();marker.material.dispose();}
  riseDrillMarkers=[];
  if(!riseSetupSnapshot) return;
  ({selectedP1,selectedP2,selectedP1Sneaker,selectedP2Sneaker,selectedP1Jersey,selectedP2Jersey,
    selectedP1Equipment,selectedP2Equipment,selectedCourt,selectedDifficulty,teamSize,twoPlayerMode,
    localTeamMode,targetScore,winBy,gameMode,teamOffensiveStyles}=riseSetupSnapshot);
  gameSettings.set('gameplayPreset',riseSetupSnapshot.gameplayPreset||'simulation');
  riseSetupSnapshot=null;
}

function startRiseMatch(){
  if(!riseState) return;
  riseSnapshotSetup();
  riseTraining=null;riseMatch=RiseToGlory.prepareMatch(riseState);
  if(!riseMatch){leaveRiseMatch();showRiseCampaign();return;}
  riseBoxScores=Object.create(null);
  launchRiseCourt();
}

function launchRiseCourt(){
  isTournamentGame=false;isChampionshipGame=false;tutorialMode=false;twoPlayerMode=false;
  gameSettings.set('gameplayPreset','simulation');
  localTeamMode='competitive';teamSize=riseMatch.teamSize;
  selectedDifficulty=difficultySettings[riseMatch.difficulty]?riseMatch.difficulty:'normal';
  selectedCourt=courts[riseMatch.courtKey]?riseMatch.courtKey:'classicArena';
  selectedP1Sneaker='classic';selectedP2Sneaker='classic';selectedP1Equipment={};selectedP2Equipment={};
  const color=hex=>parseInt(String(hex||'#1d6cff').replace('#',''),16);
  const cosmetic=riseState.cosmetics.includes(riseState.team.cosmetic)?riseState.team.cosmetic:null;
  const rewardPatterns={streetStripe:'stripe',cityPulse:'pulse',regionalChevron:'chevron',nationalGold:'crown',eliteCrown:'crown'};
  jerseys.riseHome={...jerseys.classicBlue,name:riseState.team.name,primary:color(riseState.team.primaryColor),
    secondary:color(riseState.team.secondaryColor),pattern:rewardPatterns[cosmetic]||riseState.team.jerseyStyle||'stripe',
    trim:cosmetic==='nationalGold'||cosmetic==='eliteCrown'?0xffd56a:color(riseState.team.secondaryColor)};
  jerseys.riseAway={...jerseys.firestorm,name:'Away',primary:0xd84932,secondary:0x261c35};
  selectedP1Jersey='riseHome';selectedP2Jersey='riseAway';
  teamOffensiveStyles=[riseMatch.offense||riseState.team.offense||'motion',riseMatch.awayOffense||'balanced'];
  gameMode={targetScore:riseTraining?199:(riseMatch.targetScore||21),winBy2:true,makeItTakeIt:false,losersBall:false,arcade:false};
  targetScore=gameMode.targetScore;winBy=2;
  startGame();
  if(riseTraining) resetRiseDrillPossession();
}

function finishRiseMatch(winner){
  if(!riseMatch) return false;
  if(riseTraining){finishRiseTraining(false);return true;}
  matchPlayers.forEach(riseStoreBox);
  const result=RiseToGlory.recordGame(riseState,{gameId:riseMatch.game.id,won:winner==='p1',homeScore:scoreP1,awayScore:scoreP2,
    players:Object.entries(riseBoxScores).map(([id,stats])=>({id,stats:{...stats,threeMade:stats.tpm||0,threeAttempts:stats.tpa||0,goodShots:stats.goodShots||stats.perfect||0}}))});
  RiseToGlory.save(riseState);
  const text=winner==='p1'?'Victory!':'Game complete.';
  leaveRiseMatch();gameStarted=false;gameOver=false;hideWinnerOverlay();
  showRiseCampaign('hub',result?.message||text);
  return true;
}

function riseSubstitute(slotId,athleteId){
  if(!riseMatch||riseTraining||(!possessionFrozen&&!gamePaused)||dunkActive||layupActive) return false;
  // Slot IDs are also the scorer/pass target IDs. Never replace one in flight.
  if(['shot','pass','dunk','layup'].includes(ballState)||passFlight.active) return false;
  const old=getPlayerById(slotId),athlete=RiseToGlory.player(riseState,athleteId);
  if(!old||old.teamId!==0||!athlete||!RiseToGlory.roster(riseState).some(p=>p.id===athleteId)||
    matchPlayers.some(p=>p.campaignPlayerId===athleteId)) return false;
  riseStoreBox(old);
  const replacement=createCharacter(riseRuntimeProfile(athlete,0),old.group.position,old.id,0);
  replacement.teamId=0;replacement.controllerSlot=old.controllerSlot;replacement.campaignPlayerId=athlete.id;
  replacement.hasBall=old.hasBall;replacement.angle=old.angle;replacement.group.rotation.y=old.angle;
  replacement.stamina=riseMatch.benchStamina?.[athleteId]??1;
  replacement.sprint=replacement.stamina;
  replacement.previousPosition=old.group.position.clone();replacement.simulationPosition=old.group.position.clone();
  replacement.previousAngle=old.angle;replacement.simulationAngle=old.angle;
  for(const key of ['teamRing','controlLabel','receiverRing']){
    if(old[key]){replacement[key]=old[key];old.group.remove(old[key]);replacement.group.add(old[key]);}
  }
  replacement.passCooldown=0;replacement.passAnimation=0;replacement.catchAnimation=0;replacement.reachRecovery=0;
  replacement.aiBrain=createTeamBrain(replacement);
  const index=matchPlayers.indexOf(old);matchPlayers[index]=replacement;
  if(p1===old) p1=replacement;if(p2===old) p2=replacement;
  riseMatch.benchStamina||={};riseMatch.benchStamina[old.campaignPlayerId]=old.stamina;
  scene.remove(old.group);scene.add(replacement.group);
  if(typeof disposePlayerModel==='function') disposePlayerModel(old);
  const disposed=new Set();old.group.traverse(node=>{
    if(node.geometry&&!node.userData.sharedPlayerGeometry&&!disposed.has(node.geometry)){node.geometry.dispose();disposed.add(node.geometry);}
    for(const material of Array.isArray(node.material)?node.material:[node.material]) if(material&&!disposed.has(material)){
      material.map?.dispose();material.dispose();disposed.add(material);
    }
  });
  aiTeamPlans=Object.create(null);aiDefensePlans=Object.create(null);
  document.getElementById('riseSubPanel')?.remove();
  flash(`${athlete.name} checks in`);return true;
}

function showRiseSubstitutions(){
  if(!riseMatch||riseTraining) return;
  setPaused(true);document.getElementById('pauseOverlay').classList.remove('visible');
  document.getElementById('riseSubPanel')?.remove();
  const panel=document.createElement('div');panel.id='riseSubPanel';panel.className='rise-sub-panel';
  const heading=document.createElement('h2');heading.textContent='Substitutions';panel.append(heading);
  if(['shot','pass','dunk','layup'].includes(ballState)||passFlight.active){
    const message=document.createElement('p');message.textContent='Let the current shot or pass finish before making a substitution.';panel.append(message);
  }
  const active=matchPlayers.filter(p=>p.teamId===0),bench=RiseToGlory.roster(riseState).filter(p=>!active.some(c=>c.campaignPlayerId===p.id));
  const slot=document.createElement('select');slot.setAttribute('aria-label','Player to rest');
  active.forEach(p=>slot.add(new Option(`${p.profile.name} · ${Math.round(p.stamina*100)}% stamina`,p.id)));panel.append(slot);
  bench.forEach(p=>{const b=document.createElement('button');b.textContent=`Bring in ${p.name}`;
    b.onclick=()=>{if(riseSubstitute(slot.value,p.id)) setPaused(false);};panel.append(b);});
  if(!bench.length){const p=document.createElement('p');p.textContent='Recruit a bench player from the scouting center.';panel.append(p);}
  const close=document.createElement('button');close.textContent='Return to court';close.onclick=()=>{panel.remove();setPaused(false);};panel.append(close);
  document.body.append(panel);
}

function startRiseTraining(type,athleteId){
  if(!riseState) return;
  const athlete=RiseToGlory.player(riseState,athleteId)||RiseToGlory.roster(riseState)[0];
  const definition=RiseToGlory.TRAINING.find(d=>d.id===type);if(!definition) return;
  riseSnapshotSetup();
  riseMatch=RiseToGlory.prepareMatch(riseState);
  if(!riseMatch){
    const rival=riseState.season.opponents[0];
    riseMatch={home:[],away:rival.players.map(p=>JSON.parse(JSON.stringify(p))),teamSize:1,difficulty:'normal',offense:riseState.team.offense};
  }
  const tier=Math.min(3,1+(athlete.training?.[type]?.tier||0));
  const plan=RiseToGlory.trainingPlan(riseState,athlete.id,type,tier);
  riseTraining={type,athleteId:athlete.id,tier,plan,attempts:0,elapsed:0,progress:0,previous:{},waypoint:0,resetIn:0};
  riseMatch.home=[athlete,...RiseToGlory.roster(riseState).filter(p=>p.id!==athlete.id)].slice(0,3);
  riseMatch.teamSize=['passing','team'].includes(type)?Math.min(3,riseMatch.home.length):1;
  // A 1v1 rivalry fixture can be next on the schedule; team practice still needs
  // the complete opponent roster rather than that fixture's truncated lineup.
  const trainingOpponent=RiseToGlory.opponent(riseState)||riseState.season.opponents[0];
  riseMatch.home=riseMatch.home.slice(0,riseMatch.teamSize);riseMatch.away=trainingOpponent.players.slice(0,riseMatch.teamSize).map(p=>JSON.parse(JSON.stringify(p)));
  riseMatch.difficulty=tier===1?'rookie':tier===2?'normal':'hard';riseBoxScores=Object.create(null);
  launchRiseCourt();
  if(type==='dribbling') for(const [x,z] of [[-4,-3],[4,-3],[-3,1],[3,1],[0,-5]]){
    const marker=new THREE.Mesh(new THREE.ConeGeometry(.28,.65,8),new THREE.MeshBasicMaterial({color:0x78ffc6}));
    marker.position.set(x,.325,z);scene.add(marker);riseDrillMarkers.push(marker);
  }
}

function resetRiseDrillPossession(){
  if(!riseTraining) return;
  const p=matchPlayers.find(p=>p.campaignPlayerId===riseTraining.athleteId);
  if(!p) return;
  const type=riseTraining.type;
  const defense=type==='defense';
  resetPossession(defense?matchPlayers.find(c=>c.teamId===1).id:p.id,true);
  countdownTimer=0;possessionFrozen=false;
  if(!defense){
    p.group.position.set(type==='threePoint'?5.5:type==='dunking'?0:-2.7,0,
      HOOP_POS.z+(type==='threePoint'?6:type==='dunking'?5:4.7));
    p.angle=angleToward(p.group.position,HOOP_POS);p.group.rotation.y=p.angle;
    if(['midrange','threePoint','dunking','dribbling'].includes(type)) getOpponents(p).forEach((d,i)=>d.group.position.set(7,0,6+i));
  }
  for(const c of matchPlayers){c.previousPosition.copy(c.group.position);c.simulationPosition.copy(c.group.position);}
}

function finishRiseTraining(success){
  if(!riseTraining) return;
  const drill=riseTraining;
  const result=success?RiseToGlory.awardTraining(riseState,drill.athleteId,drill.type,{attempts:drill.attempts,successes:drill.progress,tier:drill.tier}):{message:'Drill ended. Complete the live action target to earn training XP.'};
  RiseToGlory.save(riseState);
  leaveRiseMatch();gameStarted=false;gameOver=false;gamePaused=false;hideWinnerOverlay();
  showRiseCampaign('training',result?.message||(success?'Drill complete.':'Drill ended.'));
}

function updateRiseRuntime(dt){
  if(!riseMatch||!gameStarted||gamePaused||gameOver) return;
  riseSubClock+=dt;
  const owner=getBallHandler();
  if(owner&&owner.id!==riseLastOwner){
    if(riseLastShot&&owner.teamId!==riseLastShot.team&&ballState==='dribble'){
      const defenders=matchPlayers.filter(p=>p.teamId===owner.teamId);
      const closest=defenders.slice().sort((a,b)=>flatDistance(a.group.position,riseLastShot.position)-flatDistance(b.group.position,riseLastShot.position))[0];
      if(closest) closest.stats.stops=(closest.stats.stops||0)+1;
      riseLastShot=null;
    }
    riseLastOwner=owner.id;
  }
  if(ballState==='shot'&&!riseLastShot){const shooter=getPlayerById(lastShotShooter);if(shooter) riseLastShot={team:shooter.teamId,position:shooter.group.position.clone()};}
  if(ballState==='dead') riseLastShot=null;
  if(!riseTraining){
    for(const id of Object.keys(riseMatch.benchStamina||{})) if(!matchPlayers.some(c=>c.campaignPlayerId===id)) riseMatch.benchStamina[id]=Math.min(1,riseMatch.benchStamina[id]+.018*dt);
    const priority=riseState.team.substitution||'balanced',threshold=priority==='fresh_legs'?.42:priority==='matchup'?.34:.3;
    if(riseSubClock>16&&possessionFrozen&&priority!=='starters'){
      const fatigue=matchPlayers.filter(p=>p.teamId===0&&p.stamina<threshold).sort((a,b)=>a.stamina-b.stamina)[0];
      const bench=RiseToGlory.roster(riseState).filter(p=>!matchPlayers.some(c=>c.campaignPlayerId===p.id));
      if(fatigue&&bench.length){
        const target=getNearestOpponent(fatigue),r=target?.profile?.ratings||{};
        const inside=(r.dunking||0)+(r.closeShot||0)>(r.threePoint||0)+(r.midrange||0);
        const fit=p=>inside?(p.ratings.interiorDefense+p.ratings.blocking+p.ratings.strength)/3:(p.ratings.perimeterDefense+p.ratings.lateralMovement+p.ratings.steals)/3;
        const score=p=>(riseMatch.benchStamina?.[p.id]??1)*70+(priority==='matchup'?fit(p)*.45:0)+p.overall*.08+(p.role==='sixth_man'?6:0);
        const best=bench.sort((a,b)=>score(b)-score(a))[0];
        if((riseMatch.benchStamina?.[best.id]??1)>fatigue.stamina+.2) riseSubstitute(fatigue.id,best.id);
      }
      riseSubClock=0;
    }
  }
  const status=document.getElementById('riseCourtStatus');
  if(!riseTraining){if(status) status.textContent=`RISE TO GLORY · ${riseState.team.name}`;return;}
  const t=riseTraining,p=matchPlayers.find(p=>p.campaignPlayerId===t.athleteId);if(!p) return;
  t.elapsed+=dt;const stats=p.stats,prev=t.previous;
  const delta=key=>Math.max(0,(stats[key]||0)-(prev[key]||0));
  const dist=flatDistance(p.group.position,HOOP_POS);
  if(delta('fga')){
    t.attemptType=ballState==='dunk'?'dunking':dist>THREE_RADIUS?'threePoint':dist>3?'midrange':'close';
    if(t.attemptType===t.type) t.attempts+=delta('fga');
  }
  if(['midrange','threePoint','dunking'].includes(t.type)&&t.attemptType===t.type) t.progress+=delta('fgm');
  if(t.type==='passing'){t.progress+=delta('passes');t.attempts+=delta('passAttempts');}
  if(t.type==='team'){t.progress+=delta('assists')+delta('screens');t.attempts+=delta('passes')+delta('screens');}
  if(t.type==='defense'){
    const stops=delta('blocks')+delta('steals')+delta('stops')+delta('rebounds');t.progress+=stops;t.attempts+=stops;
    const handler=getBallHandler();if(handler&&handler.teamId===1&&flatDistance(p.group.position,handler.group.position)<2.1&&p.handsUpTime>0) t.footwork=(t.footwork||0)+dt;
    if(t.footwork>=3){t.progress++;t.attempts++;t.footwork=0;}
  }
  if(t.type==='dribbling'&&playerHasBall(p)){
    const targets=[[-4,-3],[4,-3],[-3,1],[3,1],[0,-5]],point=targets[t.waypoint%targets.length];
    if(Math.hypot(p.group.position.x-point[0],p.group.position.z-point[1])<1){t.progress++;t.attempts++;t.waypoint++;}
    riseDrillMarkers.forEach((m,i)=>m.material.color.setHex(i===t.waypoint%5?0x78ffc6:0x405d5a));
    if(status) status.textContent=`DRIBBLE · Reach the bright cone · ${t.attempts}/${t.plan.attempts}`;
  }
  t.previous={...stats};
  const goal=t.plan.target;
  if(status&&t.type!=='dribbling') status.textContent=`${t.type.toUpperCase()} · ${t.progress}/${goal} successful · ${t.attempts}/${t.plan.attempts} actions · ${Math.max(0,Math.ceil(120-t.elapsed))}s`;
  if(t.progress>=goal&&t.attempts>=t.plan.attempts){finishRiseTraining(true);return;}
  if(t.elapsed>=120){finishRiseTraining(false);return;}
  if(!['passing','team','dribbling'].includes(t.type)){
    const reset=ballState==='dead'||(ballState==='dribble'&&owner&&owner.teamId!==(t.type==='defense'?1:0));
    t.resetIn=reset?t.resetIn+dt:0;if(t.resetIn>.65){t.resetIn=0;pendingInbound=null;resetRiseDrillPossession();}
  }
}
