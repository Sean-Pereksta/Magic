// Every AI player uses the human acceleration, stamina, collision and shot
// systems. Difficulty changes perception and decisions, never physical stats.
let teamAITime=0;
const OFFENSIVE_STYLES={
  balanced:{label:"Balanced",description:"Read the defense, space the wings, and mix cuts with ball screens."},
  motion:{label:"Motion",description:"Pass and cut, rotate into empty spots, and keep a weak-side outlet."},
  pick_roll:{label:"Pick & Roll",description:"Set a ball screen, turn the corner, then find the roller or weak-side wing."},
  pick_pop:{label:"Pick & Pop",description:"Screen the ball, then open a shooting window on the perimeter."},
  drive_kick:{label:"Drive & Kick",description:"Spread the floor, attack a lane, and kick to an open outlet when help arrives."},
  isolation:{label:"Isolation",description:"Clear a side for a scorer, with outlets ready when help commits."},
  give_go:{label:"Give & Go",description:"Pass to a hub and cut behind the defense for a return pass."},
  post_up:{label:"Post / Interior",description:"Establish a deep seal, create an entry angle, and play inside out."},
  fast_break:{label:"Fast Break",description:"Run open lanes after a stop, then organize when the defense retreats."}
};
let aiTeamPlans=Object.create(null);
let aiDefensePlans=Object.create(null);
let aiActiveOffenseTeam=null;
let aiWasFrozen=false;

// Identities affect reads and destinations, never the character's physical stats.
// Explicit profile identities also let new roster entries opt into the system.
function aiPlayerIdentity(char){
  const p=char.profile||{},stat=(key)=>p[key]??1;
  const aliases={point_guard:"playmaker",power:"interior",power_player:"interior",defensive_specialist:"defender",allrounder:"all_rounder"};
  let type=aliases[p.archetype]||p.archetype;
  if(!type){
    if(stat("shot")>=1.25) type="sharpshooter";
    else if(p.bodyType==="Playmaker"||stat("handle")>=1.4&&stat("shot")>=.95) type="playmaker";
    else if((stat("height")>=1.13||stat("width")>=1.13)&&stat("speed")<1.06) type="interior";
    else if(stat("block")>=1.5||stat("steal")>=1.35) type="defender";
    else if(stat("sprint")>=1.15||stat("layup")>=1.3||p.bodyType==="Small/Fast") type="slasher";
    else type="all_rounder";
  }
  return {type,shooting:stat("shot"),passing:p.passing??p.pass??stat("handle"),finishing:stat("layup"),
    screening:stat("width")*.65+stat("height")*.35,speed:stat("speed")*.55+stat("sprint")*.45,
    defense:stat("block")*.5+stat("steal")*.5};
}

// Assignments and learned skills bias decisions, never movement or make odds.
// Keep secondary options: even a specialist takes a free layup or finds help.
function aiPlayerTendencies(char){
  const p=char.profile||{},r=p.ratings,role=p.role||'balanced',identity=aiPlayerIdentity(char);
  const rank=id=>THREE.MathUtils.clamp(Number(p.abilities?.[id])||0,0,3);
  const three=r?.threePoint??60+((p.shot||1)-1)*60;
  const mid=r?.midrange??three,strength=r?.strength??60+((p.width||1)-1)*80;
  const perimeter=THREE.MathUtils.clamp((identity.type==='sharpshooter'?.42:0)+(role==='shooter'?.65:0)+
    Math.max(0,three-60)*.008+rank('catchAndShoot')*.08+rank('deepRange')*.06+rank('cornerSpecialist')*.06,0,1);
  const post=THREE.MathUtils.clamp((identity.type==='interior'?.52:0)+(role==='anchor'?.4:0)+
    Math.max(0,strength-65)*.006+rank('powerFinisher')*.07+rank('contactSpecialist')*.045-perimeter*.24,0,1);
  return {perimeter,post,midrange:THREE.MathUtils.clamp(Math.max(0,mid-three-5)*.018+
      rank('midrangeAssassin')*.12+rank('pullUpSpecialist')*.1,0,1),
    drive:THREE.MathUtils.clamp((identity.type==='slasher'?.5:0)+(role==='slasher'?.5:0)+
      (rank('highFlyer')+rank('aerialAcrobat')+rank('posterizer'))*.05,0,1),
    creation:THREE.MathUtils.clamp((identity.type==='playmaker'?.45:0)+(role==='playmaker'?.45:0)+rank('floorGeneral')*.09,0,1),
    screening:rank('screenSetter')*.12+(role==='anchor'?.25:0),catchShoot:rank('catchAndShoot'),quickRelease:rank('quickRelease')};
}

function aiRecentCatch(char){
  const now=typeof simulationTime==='number'?simulationTime:teamAITime;
  return Number.isFinite(char.lastCatchTime)&&now-char.lastCatchTime>=0&&now-char.lastCatchTime<1.35;
}

function aiQuickShotLook(char,read,lane,tendencies){
  if(read.closeFinish||!read.inRange||read.space<2.15||(read.pressure??0)>.2||read.quality<.68) return false;
  const speed=char.velocity.length(),toRim=HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
  // Do not turn an established sprint to the basket into an off-balance three.
  if(speed>2.2&&char.velocity.dot(toRim)>1.8&&!aiRecentCatch(char)) return false;
  const three=read.points===3||read.distance>(typeof THREE_RADIUS==='number'?THREE_RADIUS:7.15);
  const specialty=three?tendencies.perimeter:tendencies.midrange;
  const closeLane=read.distance<4.7&&lane.direct&&lane.drive&&lane.openness>.86&&lane.finishSpace>2.5&&!lane.blocked;
  return !closeLane&&(specialty>.38||aiRecentCatch(char)&&tendencies.catchShoot>0&&tendencies.perimeter>.25);
}

function aiTeamChemistry(handler){
  const players=aiRoster().filter(player=>player.teamId===handler.teamId);
  const identities=players.map(player=>({player,...aiPlayerIdentity(player)}));
  const best=(score)=>identities.reduce((a,b)=>!a||score(b)>score(a)?b:a,null);
  const shooter=best(p=>p.shooting+aiPlayerTendencies(p.player).perimeter*.22),interior=best(p=>p.screening+p.finishing*.25+aiPlayerTendencies(p.player).post*.2),
    creator=best(p=>p.passing+(p.type==="playmaker"?.4:0)+(p.player.profile.role==='playmaker'?.2:0)),runner=best(p=>p.speed+p.finishing*.2+(p.player.profile.role==='slasher'?.15:0));
  const count=type=>identities.filter(p=>p.type===type).length;
  return {identities,shooterId:shooter?.player.id,interiorId:interior?.player.id,creatorId:creator?.player.id,
    runnerId:runner?.player.id,shooters:identities.filter(p=>p.type==='sharpshooter'||p.player.profile.role==='shooter').length,playmakers:identities.filter(p=>p.type==='playmaker'||p.player.profile.role==='playmaker').length,
    slashers:identities.filter(p=>p.type==='slasher'||p.player.profile.role==='slasher').length,bigs:identities.filter(p=>p.type==='interior'||p.player.profile.role==='anchor').length,defenders:count("defender"),
    perimeter:Math.max(0,(shooter?.shooting||1)-1),size:Math.max(0,(interior?.screening||1)-1)};
}

function aiTransitionAdvantage(handler){
  const position=handler.group.position,distance=flatDistance(position,HOOP_POS);
  if(distance<4) return false;
  const defenders=getOpponents(handler);
  const back=defenders.filter(player=>flatDistance(player.group.position,HOOP_POS)<distance-1.2).length;
  const advancing=getTeammates(handler).filter(player=>flatDistance(player.group.position,HOOP_POS)<distance-1.8).length;
  return back<Math.min(2,advancing+1)&&defenders.some(player=>player.group.position.z>position.z+1.2);
}

function aiChooseOffensiveAction(handler,plan,avoid=null){
  const c=plan.chemistry,identity=aiPlayerIdentity(handler),opponents=getOpponents(handler);
  const paint=opponents.filter(player=>flatDistance(player.group.position,HOOP_POS)<4).length;
  const pressure=opponents.filter(player=>flatDistance(player.group.position,handler.group.position)<2.4).length;
  if(aiTransitionAdvantage(handler)&&plan.age<3) return "fast_break";
  const score={motion:1.05+c.shooters*.3,pick_roll:1+c.bigs*.6+c.playmakers*.3,
    pick_pop:.6+c.perimeter*1.7+c.shooters*.32,drive_kick:.65+c.slashers*.4+c.shooters*.4,
    isolation:.7+(identity.type==="slasher"?.45:0)+(identity.passing<.8?.15:0),
    give_go:.85+c.playmakers*.3+(c.identities.length===2?.25:0),
    post_up:.6+c.bigs*.5+c.size*1.5,fast_break:.1};
  if(paint>=2){score.pick_pop+=.65;score.motion+=.25;score.post_up-=.7;score.isolation-=.35;}
  if(pressure>1){score.give_go+=.5;score.drive_kick+=.35;}
  if(identity.type==="playmaker"&&c.bigs) score.pick_roll+=.45;
  if(identity.type==="slasher"&&c.bigs) score.pick_roll+=.45;
  if(c.shooters>=2) score.motion+=.65;
  if(c.defenders&&c.slashers&&aiTransitionAdvantage(handler)) score.fast_break+=2;
  if((handler.stamina??1)<.4){score.isolation-=.6;score.drive_kick-=.5;score.post_up+=.2;score.motion+=.3;}
  // This game is first to a target score. A needed three changes shot creation,
  // without inventing a clock that the match does not have.
  if(typeof scoreP1!=="undefined"&&typeof targetScore!=="undefined"){
    const own=handler.teamId===0?scoreP1:scoreP2,other=handler.teamId===0?scoreP2:scoreP1;
    if(other>=targetScore-2&&other-own>=3&&c.perimeter>.2){score.pick_pop+=.55;score.motion+=.3;}
  }
  if(avoid) score[avoid]-=1.2;
  return Object.keys(score).reduce((a,b)=>score[b]>score[a]?b:a,"motion");
}

function aiPlanAction(plan){return plan.action||plan.style;}

// A single optional desktop play-call can request an action. Mobile continues
// to use the existing controls; teammates respond to a human handler as usual.
function aiRequestOffensiveAction(teamId,action="screen"){
  const handler=typeof getBallHandler==="function"?getBallHandler():null;
  if(!handler||handler.teamId!==teamId||ballState!=="dribble") return false;
  if(!getTeammates(handler).some(player=>!aiIsHuman(player))) return false;
  const plan=aiGetTeamPlan(handler);
  const calls={screen:plan.chemistry.bigs?"pick_roll":"pick_pop",cut:"give_go",space:"isolation"};
  const selected=calls[action]||action;
  if(!OFFENSIVE_STYLES[selected]||selected==="balanced") return false;
  plan.action=selected;plan.calledUntil=teamAITime+5;plan.actorId=null;plan.screenTarget=null;
  plan.receivingRead=null;aiAdvancePlay(plan,selected==="give_go"&&action==="cut"?"cut":"setup");
  if(action==="cut"){plan.passerId=null;plan.passCutUntil=0;}
  return true;
}

function createTeamBrain(char){
  return {
    target:char.group.position.clone(),role:"space",roleUntil:0,thinkIn:Math.random()*.18,
    actionCooldown:.5,moveCooldown:1,passTargetId:null,passUntil:0,
    possessionAge:0,lastHandler:null,releaseAt:0,driveSide:Math.random()<.5?-1:1,
    assignedId:null,protect:false,sprint:false,fakeBiteChance:.25,
    recentMoves:[],preferredDriveSide:1,decision:"setup",driveUntil:0,shotKind:null,
    input:{moveX:0,moveY:0,sprint:false,protect:false,source:"ai",worldDirection:new THREE.Vector3()}
  };
}

function initBotBrain(){
  teamAITime=0;
  aiTeamPlans=Object.create(null);
  aiDefensePlans=Object.create(null);
  aiActiveOffenseTeam=null;
  aiWasFrozen=false;
  (matchPlayers.length?matchPlayers:[p1,p2]).filter(Boolean).forEach(char=>{
    char.aiBrain=createTeamBrain(char);
    char.aiInput=char.aiBrain.input;
    char.aiScreen=false;
  });
  botBrain=p2?.aiBrain||null;
  configureBotBrainFromDifficulty();
}

function configureBotBrainFromDifficulty(){
  if(botBrain) botBrain.fakeBiteChance=(difficultySettings[selectedDifficulty]||difficultySettings.normal).fakeBiteChance;
}

function aiRoster(){ return matchPlayers.length?matchPlayers:[p1,p2].filter(Boolean); }
function aiIsHuman(char){ return !!char.controllerSlot; }
function aiSettings(){ return difficultySettings[selectedDifficulty]||difficultySettings.normal; }

function aiOffensiveStyle(teamId){
  const selected=typeof teamOffensiveStyles!=="undefined"?teamOffensiveStyles[teamId]:"balanced";
  return OFFENSIVE_STYLES[selected]?selected:"balanced";
}

function aiNewTeamPlan(handler,previous){
  const style=aiOffensiveStyle(handler.teamId);
  const sameTeamPass=previous&&previous.handlerId!==handler.id&&getPlayerById(previous.handlerId)?.teamId===handler.teamId;
  return {teamId:handler.teamId,handlerId:handler.id,style,action:style,age:0,phase:"setup",phaseAge:0,cycle:0,
    actorId:null,actorSide:1,screenSide:handler.group.position.x>0?-1:1,screenTarget:null,
    chemistry:aiTeamChemistry(handler),passHistory:[],readIn:0,stalled:0,calledUntil:0,lastProgress:handler.group.position.clone(),
    passerId:sameTeamPass?previous.handlerId:null,passCutUntil:sameTeamPass?teamAITime+1.75:0};
}

function aiGetTeamPlan(handler){
  let plan=aiTeamPlans[handler.teamId];
  if(!plan||plan.style!==aiOffensiveStyle(handler.teamId)){
    plan=aiTeamPlans[handler.teamId]=aiNewTeamPlan(handler,plan);
  }else if(plan.handlerId!==handler.id){
    // A catch continues the possession and its pass history. Restarting the
    // director on each catch allowed two bots to exchange the same pass forever.
    const previous=getPlayerById(plan.handlerId);
    if(previous?.teamId===handler.teamId){
      const recent=plan.passHistory[plan.passHistory.length-1];
      if(!recent||recent.from!==previous.id||recent.to!==handler.id||teamAITime-recent.at>.9){
        plan.passHistory.push({from:previous.id,to:handler.id,at:teamAITime,
          fromPosition:previous.group.position.clone(),toPosition:handler.group.position.clone(),reason:"human_pass"});
        if(plan.passHistory.length>8) plan.passHistory.shift();
      }
      plan.passerId=previous.id;plan.passCutUntil=teamAITime+1.75;
      plan.receivingRead=null;plan.lastProgress.copy(handler.group.position);plan.stalled=0;
      if(aiPlanAction(plan)==="give_go") aiAdvancePlay(plan,"cut");
      if(aiPlanAction(plan)==="post_up"&&handler.id===plan.chemistry.interiorId) aiAdvancePlay(plan,"post");
      if(aiPlanAction(plan)==="drive_kick") aiAdvancePlay(plan,"rotate");
      if(plan.actorId===handler.id){plan.actorId=null;plan.screenTarget=null;}
    }
    plan.handlerId=handler.id;
  }
  const support=aiRoster().filter(player=>player.teamId===handler.teamId&&player!==handler&&!aiIsHuman(player));
  if(!support.length){ plan.phase="solo";plan.actorId=null;return plan; }
  if(!support.some(player=>player.id===plan.actorId)){
    const action=aiPlanAction(plan);
    const preference=player=>{
      const identity=aiPlayerIdentity(player);
      const t=aiPlayerTendencies(player);
      if(action==="pick_roll"||action==="post_up") return identity.screening+identity.finishing*.25+t.post*.18+t.screening;
      if(action==="pick_pop") return identity.shooting+.1*identity.screening+t.perimeter*.2+t.screening*.4;
      if(action==="give_go") return plan.phase==="cut"&&!plan.passerId?identity.speed+identity.finishing*.2+t.drive*.2:identity.passing+t.creation*.25;
      if(action==="fast_break") return identity.speed+identity.finishing*.2+t.drive*.25;
      return 1+t.drive*.2-t.perimeter*.15;
    };
    const ordered=support.slice(plan.cycle%support.length).concat(support.slice(0,plan.cycle%support.length));
    const actor=ordered.reduce((best,player)=>preference(player)>preference(best)?player:best,ordered[0]);
    plan.actorId=actor.id;plan.actorSide=actor.group.position.x<0?-1:1;
  }
  if((aiPlanAction(plan)==="pick_roll"||aiPlanAction(plan)==="pick_pop")&&!plan.screenTarget){
    const defender=getNearestOpponent(handler);
    const toRim=HOOP_POS.clone().sub(handler.group.position).setY(0).normalize();
    const actor=getPlayerById(plan.actorId);
    const sidePosition=actor.group.position.clone().sub(handler.group.position);
    plan.screenSide=sidePosition.x*toRim.z-sidePosition.z*toRim.x>=0?1:-1;
    plan.screenTarget=(defender?defender.group.position:handler.group.position).clone();
    plan.screenTarget.x+=toRim.z*plan.screenSide*.8;
    plan.screenTarget.z-=toRim.x*plan.screenSide*.8;
    plan.screenTarget.y=0;
    keepInCourt(plan.screenTarget);
  }
  return plan;
}

function aiAdvancePlay(plan,next){ plan.phase=next;plan.phaseAge=0; }

function updateAITactics(handler,dt,frozen){
  if(frozen){
    if(!aiWasFrozen){
      aiTeamPlans=Object.create(null);aiActiveOffenseTeam=null;
      for(const plan of Object.values(aiDefensePlans)){
        if(plan.trap) aiReleaseTrap(plan);
        plan.handlerAge=0;plan.lastTime=-1;
      }
      for(const player of aiRoster()) if(player.aiBrain){
        player.aiBrain.possessionAge=0;player.aiBrain.passTargetId=null;player.aiBrain.thinkIn=0;
      }
    }
    aiWasFrozen=true;
    return;
  }
  aiWasFrozen=false;
  if(typeof aiObserveTeamDefense==="function") aiObserveTeamDefense(handler,dt);
  const passer=ballState==="pass"&&passFlight?.active?getPlayerById(passFlight.passerId):null;
  const offense=passer||handler;
  if(offense&&(ballState==="dribble"||passer)){
    if(aiActiveOffenseTeam!==null&&aiActiveOffenseTeam!==offense.teamId) aiTeamPlans=Object.create(null);
    aiActiveOffenseTeam=offense.teamId;
    const plan=aiGetTeamPlan(offense);
    plan.age+=dt;plan.phaseAge+=dt;
    if(passer&&plan.passerId!==passer.id){plan.passerId=passer.id;plan.passCutUntil=teamAITime+1.75;}
    plan.readIn-=dt;
    if(plan.readIn<=0){
      plan.readIn=.24; // one shared alignment read, not one search per bot/frame
      const shotValue=aiAssessShot(offense,aiSettings()).value||0;
      const forward=flatDistance(plan.lastProgress,HOOP_POS)-flatDistance(offense.group.position,HOOP_POS)>.6;
      if(forward||shotValue>(plan.progressShotValue||0)+.1){
        plan.lastProgress.copy(offense.group.position);plan.progressShotValue=shotValue;plan.stalled=0;
      }else plan.stalled+=.24;
      const action=aiPlanAction(plan);
      const lostScreen=(action==="pick_roll"||action==="pick_pop")&&plan.phase==="setup"&&plan.phaseAge>3.2;
      const lostPost=action==="post_up"&&plan.phase==="setup"&&plan.phaseAge>4.5;
      if(plan.style==="balanced"&&plan.age>.55&&(action==="balanced"||plan.stalled>4)||
        lostScreen||lostPost||plan.phase==="reset"&&plan.phaseAge>1.2&&plan.calledUntil<teamAITime){
        plan.action=aiChooseOffensiveAction(offense,plan,plan.stalled>4||lostScreen||lostPost?action:null);
        plan.actorId=null;plan.screenTarget=null;plan.receivingRead=null;plan.stalled=0;
        aiAdvancePlay(plan,"setup");aiGetTeamPlan(offense);
      }
    }
    const action=aiPlanAction(plan);
    if(plan.phase!=="solo"){
      if(action==="motion"){
        if(plan.phase==="setup"&&plan.phaseAge>.55) aiAdvancePlay(plan,"cut");
        else if(plan.phase==="cut"&&plan.phaseAge>1.5) aiAdvancePlay(plan,"fill");
        else if(plan.phase==="fill"&&plan.phaseAge>1.2){plan.cycle++;plan.actorId=null;aiAdvancePlay(plan,"setup");}
      }else if(action==="pick_roll"||action==="pick_pop"){
        const screener=getPlayerById(plan.actorId);
        const ready=screener&&flatDistance(screener.group.position,plan.screenTarget)<.85;
        if(plan.phase==="setup"&&ready&&plan.phaseAge>.4) aiAdvancePlay(plan,"screen");
        else if(plan.phase==="screen"&&plan.phaseAge>1.05) aiAdvancePlay(plan,action==="pick_pop"?"pop":"roll");
        else if((plan.phase==="roll"||plan.phase==="pop")&&plan.phaseAge>1.75) aiAdvancePlay(plan,"reset");
        else if(plan.phase==="reset"&&plan.phaseAge>.85){plan.cycle++;plan.actorId=null;plan.screenTarget=null;aiAdvancePlay(plan,"setup");}
      }else if(action==="drive_kick"){
        const pressure=getOpponents(offense).filter(player=>flatDistance(player.group.position,offense.group.position)<2.5).length;
        if(plan.phase==="setup"&&plan.phaseAge>.5) aiAdvancePlay(plan,"drive");
        else if(plan.phase==="drive"&&(pressure>1||plan.phaseAge>2.5)) aiAdvancePlay(plan,"kick");
        else if(plan.phase==="kick"&&plan.phaseAge>1.3){plan.screenSide*=-1;plan.cycle++;aiAdvancePlay(plan,"setup");}
        else if(plan.phase==="rotate"&&plan.phaseAge>.8){plan.actorId=null;aiAdvancePlay(plan,"setup");}
      }else if(action==="give_go"){
        if(plan.phase==="setup"&&plan.phaseAge>.5) aiAdvancePlay(plan,"entry");
        else if(plan.phase==="cut"&&plan.phaseAge>1.8) aiAdvancePlay(plan,"fill");
        else if((plan.phase==="fill"||plan.phase==="entry")&&plan.phaseAge>2.4){plan.cycle++;plan.actorId=null;aiAdvancePlay(plan,"reset");}
      }else if(action==="post_up"){
        const post=getPlayerById(plan.chemistry.interiorId);
        if(plan.phase==="setup"&&post&&flatDistance(post.group.position,HOOP_POS)<4) aiAdvancePlay(plan,post===offense?"post":"entry");
        else if(plan.phase==="entry"&&plan.phaseAge>3||plan.phase==="post"&&plan.phaseAge>4) aiAdvancePlay(plan,"reset");
      }else if(action==="isolation"){
        if(plan.phase==="setup"&&plan.phaseAge>.65) aiAdvancePlay(plan,"attack");
        else if(plan.phase==="attack"&&plan.phaseAge>4.5) aiAdvancePlay(plan,"reset");
      }else if(action==="fast_break"){
        if(plan.phase==="setup") aiAdvancePlay(plan,"run");
        if(plan.phase==="run"&&plan.phaseAge>.8&&!aiTransitionAdvantage(offense)||plan.phaseAge>3.2){
          plan.action=aiChooseOffensiveAction(offense,plan,"fast_break");
          if(plan.action==="fast_break") plan.action="motion";
          plan.actorId=null;plan.screenTarget=null;aiAdvancePlay(plan,"setup");
        }
      }
    }
  }else if(ballState!=="loose"){
    // A shot starts a rebound possession. A briefly bobbled pass does not
    // erase the same team's history or let repeated loose catches reset it.
    if(aiActiveOffenseTeam!==null) aiTeamPlans=Object.create(null);
    aiActiveOffenseTeam=null;
  }
  for(const teamId of new Set(aiRoster().map(player=>player.teamId))){
    if(handler&&handler.teamId!==teamId&&ballState==="dribble") aiGetDefensePlan(teamId,handler,aiSettings(),dt);
    else if(aiDefensePlans[teamId]?.trap) aiReleaseTrap(aiDefensePlans[teamId]);
  }
}

function aiReleaseTrap(plan){
  plan.trap=null;
  plan.cooldownUntil=teamAITime+2.8;
  aiRoster().filter(player=>player.teamId===plan.teamId&&!aiIsHuman(player)&&player.aiBrain).forEach(player=>{player.aiBrain.thinkIn=0;});
}

function aiTrapSpace(handler,primary){
  const position=handler.group.position;
  const rimDistance=flatDistance(position,HOOP_POS);
  const halfWidth=typeof COURT_W!=="undefined"?COURT_W/2:9;
  if(Math.abs(position.x)>halfWidth-2.6&&rimDistance<10) return "sideline";
  if(rimDistance<4.6&&(!primary||flatDistance(primary.group.position,position)>2.2)) return "paint";
  return null;
}

function aiGetDefensePlan(teamId,handler,settings,dt=0){
  if(typeof aiUseTeamDefense==="function"&&aiUseTeamDefense(settings)) return aiGetCoordinatedDefensePlan(teamId,handler,settings,dt);
  let plan=aiDefensePlans[teamId];
  if(!plan) plan=aiDefensePlans[teamId]={teamId,handlerId:handler.id,handlerAge:0,cooldownUntil:0,trap:null,lastTime:-1};
  if(plan.handlerId!==handler.id){
    if(plan.trap) aiReleaseTrap(plan);
    plan.handlerId=handler.id;plan.handlerAge=0;
  }
  const defense=aiRoster().filter(player=>player.teamId===teamId);
  const primary=defense.find(player=>aiAssignedOpponent(player)===handler);
  const space=aiTrapSpace(handler,primary);
  if(plan.trap){
    const humanHelp=defense.some(player=>player.id!==plan.trap.primaryId&&aiIsHuman(player)&&flatDistance(player.group.position,handler.group.position)<2.6);
    if(ballState!=="dribble"||teamAITime>=plan.trap.until||!space||humanHelp||flatDistance(handler.group.position,plan.trap.origin)>3.1){
      aiReleaseTrap(plan);
    }
    return plan;
  }
  // One shared decision per frame, regardless of roster/update order.
  if(plan.lastTime===teamAITime) return plan;
  plan.lastTime=teamAITime;plan.handlerAge+=dt;
  if(ballState!=="dribble"||defense.length<2||!space||teamAITime<plan.cooldownUntil||settings.defensiveIQ<=.42) return plan;
  const exposed=handler.dribbleStyle==="sprint"||handler.dribbleStyle==="crossover"||handler.moveState==="sidehop";
  const stalled=handler.velocity.length()<.8&&plan.handlerAge>1.4;
  const toRim=HOOP_POS.clone().sub(handler.group.position).setY(0).normalize();
  const driving=handler.velocity.length()>1.5&&handler.velocity.clone().setY(0).normalize().dot(toRim)>.65;
  if(space!=="paint"&&!exposed&&!stalled&&!driving) return plan;
  // A human already helping occupies the second spot; never send a third body.
  if(defense.some(player=>player!==primary&&aiIsHuman(player)&&flatDistance(player.group.position,handler.group.position)<2.6)) return plan;
  let helper=null,best=Infinity;
  for(const defender of defense){
    if(defender===primary||aiIsHuman(defender)) continue;
    const distance=flatDistance(defender.group.position,handler.group.position);
    if(distance>5.8) continue;
    const assignment=aiAssignedOpponent(defender);
    const threat=assignment?flatDistance(assignment.group.position,HOOP_POS):20;
    // Don't abandon an open rim finisher to trap a harmless sideline dribble.
    if(space!=="paint"&&threat<3.2) continue;
    const value=distance+(threat<5?3:0)+(assignment?.profile.shot>1.12?1:0);
    if(value<best){best=value;helper=defender;}
  }
  if(helper){
    plan.trap={handlerId:handler.id,primaryId:primary?.id,helperId:helper.id,space,
      origin:handler.group.position.clone(),until:teamAITime+(space==="paint"?1.35:1.9)};
    defense.filter(player=>!aiIsHuman(player)&&player.aiBrain).forEach(player=>{player.aiBrain.thinkIn=0;});
  }
  return plan;
}

function updateTeamAI(dt,options={}){
  if(!gameStarted||gameOver||gamePaused) return;
  teamAITime+=dt;
  const roster=aiRoster();
  const settings=aiSettings();
  const handler=getBallHandler();
  const frozen=options.countdown||possessionFrozen;
  updateAITactics(handler,dt,frozen);
  for(const char of roster){
    if(aiIsHuman(char)) { char.aiScreen=false; continue; }
    const brain=char.aiBrain||(char.aiBrain=createTeamBrain(char));
    char.aiInput=brain.input;
    brain.thinkIn-=dt;
    brain.actionCooldown=Math.max(0,brain.actionCooldown-dt);
    brain.moveCooldown=Math.max(0,brain.moveCooldown-dt);
    if(brain.lastHandler!==(handler?.id||null)){
      brain.lastHandler=handler?.id||null;
      brain.possessionAge=0;
      brain.thinkIn=0;
      brain.passTargetId=null;
      brain.driveUntil=0;
      // Receiving a teammate's pass is a new read, not the passer's cooldown.
      if(handler===char&&aiRecentCatch(char)) brain.actionCooldown=Math.min(brain.actionCooldown,.08);
    }else if(handler===char&&ballState==="dribble"&&!frozen){
      brain.possessionAge+=dt;
    }
    // Gather movement belongs to the finish solver, not a rebound chase after
    // the shooter's own hand. Preserve the running takeoff's real velocity.
    if(!frozen&&(layupActive&&layupOwner===char.id||dunkActive&&dunkOwner===char.id)){
      brain.role="finish";brain.decision="finish";
      brain.input.moveX=0;brain.input.moveY=0;brain.input.sprint=false;
      continue;
    }
    if(brain.thinkIn<=0){
      // A visible reaction delay remains even at the top difficulty.
      const iq=char.profile.ratings?.basketballIQ;
      const mental=iq===undefined?1:.65+iq*.007;
      brain.thinkIn=THREE.MathUtils.clamp(THREE.MathUtils.randFloat(settings.reactionMin,settings.reactionMax)/mental,.075,.7);
      const reads=iq===undefined?settings:{...settings,lookAhead:Math.min(.6,(settings.lookAhead||.2)*mental),
        spacingIQ:Math.min(1,settings.spacingIQ*mental),offensiveIQ:Math.min(1,(settings.offensiveIQ||settings.spacingIQ)*mental)};
      decideTeamPlayer(char,brain,handler,reads,frozen);
    }
    char.aiScreen=brain.role==="screen"&&brain.roleUntil>teamAITime&&char.onGround&&char.moveState==="normal"&&char.velocity.length()<1&&flatDistance(char.group.position,brain.target)<.65;
    moveAIPlayer(char,brain,dt);
    if(frozen) continue;
    updateAIShotAndPass(char,brain,handler,dt);
  }
}

function decideTeamPlayer(char,brain,handler,settings,frozen){
  brain.protect=false;
  brain.sprint=false;
  if(frozen){
    brain.role="ready";
    brain.target.copy(char.group.position);
    if(handler&&handler.teamId!==char.teamId){
      const assigned=aiAssignedOpponent(char);
      if(assigned) brain.target.copy(getBetweenPlayerAndRimTarget(char,assigned,2.5));
    }
    return;
  }
  if(ballState==="layup"||ballState==="dunk"){
    const finisher=getPlayerById(ballState==="layup"?layupOwner:dunkOwner);
    if(finisher&&finisher.teamId!==char.teamId) decideAIDefense(char,brain,finisher,settings);
    else decideAIRebound(char,brain,settings);
    return;
  }
  if(ballState==="loose"||ballState==="shot"){
    decideAIRebound(char,brain,settings);
    return;
  }
  if(ballState==="pass"&&passFlight){
    if(passFlight.targetId===char.id){
      brain.role="receive";
      // Keep the planned cut, but stop overrunning a pass once it is nearby.
      if(flatDistance(char.group.position,ball.position)<2.2) brain.target.copy(ball.position).setY(0);
      brain.sprint=flatDistance(char.group.position,ball.position)>3;
      return;
    }
    const passer=getPlayerById(passFlight.passerId);
    if(passer?.teamId!==char.teamId){
      const receiver=getPlayerById(passFlight.targetId);
      const reach=flatDistance(char.group.position,ball.position);
      const interceptor=aiRoster().filter(player=>player.teamId===char.teamId).reduce((best,player)=>{
        const distance=flatDistance(player.group.position,ball.position);
        const value=distance-(aiAssignedOpponent(player)===receiver ? .45 : 0);
        return !best||value<best.value?{player,value}:best;
      },null)?.player;
      if(receiver&&reach<3.6&&interceptor===char){
        brain.role="intercept";
        brain.target.copy(ball.position).addScaledVector(ballVelocity,.16).setY(0);
        brain.sprint=reach>1.5;
        return;
      }
    }
    if(passer?.teamId===char.teamId) decideAIOffBall(char,brain,passer,settings);
    else decideAIDefense(char,brain,passer,settings);
    return;
  }
  if(!handler){ brain.target.copy(char.group.position); return; }
  if(handler===char) decideAIHandler(char,brain,settings);
  else if(handler.teamId===char.teamId) decideAIOffBall(char,brain,handler,settings);
  else decideAIDefense(char,brain,handler,settings);
}

function moveAIPlayer(char,brain,dt){
  const input=brain.input;
  input.worldDirection.copy(brain.target).sub(char.group.position).setY(0);
  const distance=input.worldDirection.length();
  const amount=char.aiScreen?0:THREE.MathUtils.clamp((distance-.22)/1.2,0,1);
  if(distance>.001) input.worldDirection.divideScalar(distance);
  const avoidance=typeof aiSteerTeammates==="function"?aiSteerTeammates(char,brain,input.worldDirection):1;
  input.moveX=amount*avoidance;
  input.moveY=0;
  input.protect=brain.protect;
  input.sprint=brain.sprint&&char.stamina>.32&&char.sprint>.2&&distance>1.2;
  updateModernPlayerControl(char,input,dt);
}

function aiAssignedOpponent(char){
  const coverage=typeof aiDefenseAssignment==="function"?aiDefenseAssignment(char):null;
  if(coverage) return coverage;
  if(typeof riseMatch!=="undefined"&&riseMatch&&char.teamId===0){
    const target=riseState?.team?.matchups?.[char.campaignPlayerId];
    const marked=target&&getOpponents(char).find(p=>p.campaignPlayerId===target);
    if(marked) return marked;
  }
  const roster=aiRoster();
  const team=roster.filter(player=>player.teamId===char.teamId);
  const opponents=getOpponents(char);
  if(!opponents.length) return null;
  const assigned=opponents[team.indexOf(char)%opponents.length];
  if(char.aiBrain) char.aiBrain.assignedId=assigned.id;
  return assigned;
}

function aiNearestDistance(char){
  const opponent=getNearestOpponent(char);
  return opponent?flatDistance(char.group.position,opponent.group.position):8;
}

function aiShotQuality(char){
  return aiAssessShot(char,aiSettings()).quality;
}

function aiPassLaneClearance(passer,receiver){
  const a=passer.group.position,b=receiver.group.position;
  const dx=b.x-a.x,dz=b.z-a.z;
  const lengthSq=dx*dx+dz*dz;
  if(lengthSq<.1) return 0;
  let clearance=8;
  for(const defender of getOpponents(passer)){
    const p=defender.group.position;
    const t=THREE.MathUtils.clamp(((p.x-a.x)*dx+(p.z-a.z)*dz)/lengthSq,0,1);
    if(t<.1||t>.94) continue;
    clearance=Math.min(clearance,Math.hypot(p.x-a.x-t*dx,p.z-a.z-t*dz));
  }
  return clearance;
}

function aiPassingPurpose(char,teammate,pass,shot,lane,plan,settings){
  if(!pass.safe) return null;
  const action=aiPlanAction(plan),identity=aiPlayerIdentity(char),tendencies=aiPlayerTendencies(char);
  const shotValue=shot.value??shot.quality,driveValue=aiDriveOpportunity(char,lane,shot.distance);
  const role=teammate.aiBrain?.role;
  const movingToRim=["cut","roll","dump_off","break"].includes(role)&&
    (teammate.velocity.length()>.6||pass.rimFeed);
  const last=plan.passHistory[plan.passHistory.length-1];
  const returnPass=last&&last.from===teammate.id&&last.to===char.id&&teamAITime-last.at<4;
  const developed=!returnPass||flatDistance(teammate.group.position,last.fromPosition)>1.65;
  const pressure=getOpponents(char).filter(player=>flatDistance(player.group.position,char.group.position)<2.1).length;
  let reason=null,bonus=0;
  // Each reason describes an observable advantage, not elapsed possession age.
  if(pass.rimFeed&&(pass.finishOpen||pass.finishFavorable)&&pass.value>shotValue-.04){reason=pass.finishFavorable?"mismatch":"rim_feed";bonus=.1;}
  else if(tendencies.post>.45&&shot.distance<5.5&&pressure>=2&&pass.catchSpace>2.25&&
    pass.catchValue>shotValue+.015){reason="inside_out";bonus=.09;}
  else if(pass.points===3&&pass.catchSpace>2.5&&pass.catchQuality>.73&&
    aiPlayerTendencies(teammate).perimeter>.45&&pass.catchValue>shotValue+.025){reason="shooter_window";bonus=.045;}
  else if(pass.catchValue>shotValue+(settings.passAdvantage??.1)){reason="better_shot";}
  else if(movingToRim&&pass.value>Math.max(shotValue,driveValue)-.06&&pass.catchSpace>1.65){reason="timed_cut";bonus=.1;}
  else if((action==="drive_kick"||role==="pop")&&pressure>=1&&pass.catchSpace>2.3&&
    pass.catchValue>shotValue+.03){reason=role==="pop"?"pick_pop":"kick_out";bonus=.05;}
  else if(action==="give_go"&&plan.phase==="entry"&&teammate.id===plan.actorId&&
    pass.catchSpace>1.8&&pressure>=1){
    const cutEnd=new THREE.Vector3(-plan.actorSide*1.45,0,HOOP_POS.z+2);
    if(aiCoordLaneSpace(char,char.group.position,cutEnd,settings)>1.2){reason="give_go_entry";bonus=.03;}
  }else if(action==="post_up"&&teammate.id===plan.chemistry.interiorId&&pass.rimFeed&&
    pass.catchSpace>1.1&&pass.value>shotValue+.02){reason="post_entry";bonus=.06;}
  else if(pressure>=1&&shot.space<1.5&&pass.catchSpace>2.2&&pass.value>shotValue+.05){reason="pressure_release";}
  else if((char.stamina??1)<.3&&pass.catchSpace>2&&pass.value>shotValue+.04){reason="fresh_creator";}
  if(!reason) return null;
  // A return is allowed after a real cut, a newly opened high quality shot, or
  // a trapped ball handler's safe outlet. Standing returns cannot farm a timer.
  if(returnPass&&!developed&&!(pass.catchValue>shotValue+.22&&pass.catchValue>.72)&&
    !(pressure>=2&&pass.catchSpace>2.8)) return null;
  const pair=plan.passHistory.filter(item=>teamAITime-item.at<8&&
    (item.from===char.id&&item.to===teammate.id||item.from===teammate.id&&item.to===char.id));
  if(pair.length>=3&&!developed&&reason!=="mismatch") return null;
  if(lane.direct&&lane.drive&&lane.openness>.82&&lane.finishSpace>2.65&&!lane.blocked&&
    !(pass.rimFeed&&pass.value>driveValue-.02)&&
    !(pass.points===3&&pass.catchQuality>.82&&pass.value>driveValue+.06)&&pass.value<driveValue+.16) return null;
  const priority=teammate.profile.offensivePriority;
  const preference=priority===undefined?0:(priority-2)*.025+(teammate.profile.role==='primary'?.025:0);
  return {reason,value:pass.value+bonus+(identity.type==="playmaker"?.025:0)+tendencies.creation*.035+preference,pass};
}

function decideAIHandler(char,brain,settings){
  brain.role="handle";
  const plan=aiGetTeamPlan(char);
  const action=aiPlanAction(plan),identity=aiPlayerIdentity(char),tendencies=aiPlayerTendencies(char);
  const position=char.group.position;
  const read=aiAssessShot(char,settings);
  const lane=aiFindDriveLane(char,settings);
  const {distance,space,quality}=read;
  const shotValue=read.value??quality;
  const vision=settings.courtVision??settings.spacingIQ;
  const toRim=HOOP_POS.clone().sub(position).setY(0).normalize();
  const committed=brain.driveUntil>teamAITime||char.velocity.dot(toRim)>2.2;
  const driveValue=aiDriveOpportunity(char,lane,distance);
  const driveBias=(identity.type==="slasher"?.055:identity.type==="sharpshooter"?-.045:0)+tendencies.drive*.055-tendencies.perimeter*.045;
  const waitingForScreen=(action==="pick_roll"||action==="pick_pop")&&plan.phase==="setup"&&plan.screenTarget&&space<3;
  const drivePreferred=lane.drive&&distance>2.5&&!waitingForScreen&&
    (committed||driveValue+driveBias>shotValue+.025||!read.inRange);
  const clearRun=lane.direct&&lane.openness>.82&&lane.finishSpace>2.65&&!lane.blocked;
  // A running finish retains its approach; a jumper first settles its feet.
  if(shotCharging&&chargingPlayer===char){
    if(brain.shotKind==="layup"||brain.shotKind==="dunk") brain.target.copy(position).addScaledVector(toRim,2);
    else brain.target.copy(position);
    return;
  }
  if(brain.passTargetId&&brain.passUntil>teamAITime){ brain.target.copy(position);return; }
  let receiver=null,bestRead=null,bestValue=-Infinity,bestPurpose=null;
  for(const teammate of getTeammates(char)){
    const pass=aiAssessPass(char,teammate,settings);
    const purpose=aiPassingPurpose(char,teammate,pass,read,lane,plan,settings);
    if(purpose&&purpose.value>bestValue){bestValue=purpose.value;receiver=teammate;bestRead=pass;bestPurpose=purpose;}
  }
  const quickLook=aiQuickShotLook(char,read,lane,tendencies);
  const catchReady=quickLook&&aiRecentCatch(char);
  const readTime=(settings.quickReadTime??.4)*(catchReady?Math.max(.25,.55-tendencies.catchShoot*.055-tendencies.quickRelease*.035):1);
  const ready=brain.actionCooldown<=0&&brain.possessionAge>Math.max(.085,readTime);
  // Take the easy basket immediately rather than waiting for a scripted play.
  if(ready&&read.closeFinish&&(read.finishOpen||read.finishFavorable)){
    brain.decision="finish";brain.role="finish";
    brain.finishRead=read.finishFavorable&&!read.finishOpen?"size_advantage":"open";
    brain.target.copy(position).addScaledVector(toRim,2);
    beginAIShot(char,brain);
    return;
  }
  // A specialist's clean window is worth taking before a long drive or a
  // scripted action. The normal meter, feet settling and shot physics remain.
  if(quickLook&&(!bestPurpose||bestValue<shotValue+.16)){
    brain.decision="shoot";brain.shotRead=catchReady?"catch_and_shoot":"specialty_window";
    brain.driveUntil=0;brain.sprint=false;brain.target.copy(position);
    if(ready&&char.velocity.length()<1.6) beginAIShot(char,brain);
    return;
  }
  const quickCatch=identity.type==="playmaker"||identity.type==="sharpshooter";
  const patient=brain.possessionAge>(settings.quickReadTime??.4)*(quickCatch?.8:1);
  const lateRisk=plan.age>16&&distance<8.2&&space>.95&&quality>.1;
  const forced=(brain.possessionAge>10||plan.age>14)&&space>.85&&quality>.3||lateRisk;
  // A completed catch does not give a stalled team a fresh possession budget.
  // After several unsuccessful actions, settle for a playable look unless a
  // clearly better receiver is already available. Normal shot physics apply.
  if(ready&&forced&&(read.inRange||lateRisk)&&(!bestPurpose||bestValue<shotValue+.2)){
    brain.decision="shoot";brain.driveUntil=0;brain.sprint=false;
    brain.target.copy(position);
    if(char.velocity.length()<1.6) beginAIShot(char,brain);
    return;
  }
  const smartPass=receiver&&patient&&bestPurpose;
  if(smartPass&&brain.actionCooldown<=0){
    brain.passTargetId=receiver.id;
    brain.passUntil=teamAITime+.8;
    brain.decision="pass";brain.passReason=bestPurpose.reason;brain.driveUntil=0;
    brain.target.copy(position);
    return;
  }
  if(aiTryPostPlay(char,brain,read,lane,plan,tendencies)) return;
  if(drivePreferred){
    brain.decision="drive";brain.role="drive";brain.target.copy(lane.target);
    if((action==="pick_roll"||action==="pick_pop")&&plan.phase==="screen"&&!clearRun){
      aiPlayHandlerTarget(char,brain,plan,toRim);
      if(aiRouteBlocked(char,brain.target)) brain.target.copy(lane.target);
    }
    brain.driveUntil=teamAITime+.35+(settings.driveCommitment??.65)*.45;
    brain.sprint=distance>3.7&&char.stamina>.45;
    brain.protect=space<1.25;
    keepInCourt(brain.target);
    return;
  }
  const openEnough=space>THREE.MathUtils.lerp(1.25,2.05,settings.shotDiscipline);
  const qualityEnough=quality>THREE.MathUtils.lerp(.52,.76,settings.shotDiscipline);
  const runningAction=action!=="balanced"&&plan.phase==="setup"&&plan.age<2.8;
  if(ready&&read.inRange&&((!runningAction&&openEnough&&qualityEnough)||forced)){
    brain.decision="shoot";
    brain.target.copy(position);
    if(char.velocity.length()<1.6||forced) beginAIShot(char,brain);
    return;
  }
  brain.decision="setup";brain.driveUntil=0;
  brain.target.copy(lane.setupTarget||lane.target);
  const coordinated=aiPlayHandlerTarget(char,brain,plan,toRim);
  // An occupied paint is a reason to create an angle, not to sprint into it.
  if(!lane.drive&&coordinated&&aiRouteBlocked(char,brain.target)){
    brain.target.copy(lane.setupTarget||lane.target);
  }
  // Bringing the ball to the arc does not require an already open layup all
  // the way to the rim. Otherwise a set defense makes local spacing reads
  // retreat indefinitely, moving every new screen farther from the basket.
  if(distance>7.2){
    const advance=aiAdvanceToFrontcourt(char,toRim);
    if(advance) brain.target.copy(advance);
    else if(flatDistance(brain.target,HOOP_POS)>distance+.1) brain.target.copy(position);
  }
  if(space<2.35){
    const side=brain.driveSide;
    // Use the same crossover action and cooldowns as a human.
    if(brain.moveCooldown<=0&&space<1.6&&Math.random()<settings.crossoverUse*.55){
      attemptCrossover(char,side<0?"left":"right");
      brain.moveCooldown=1.25;
      brain.driveSide*=-1;
    }else if(brain.moveCooldown<=0&&space<1.5&&distance<4.5&&Math.random()<settings.pumpFakeUse*.4){
      startPumpFake(char);
      brain.moveCooldown=1.5;
    }
  }
  // A tired handler slows down and shares rather than receiving a speed boost.
  brain.sprint=lane.drive&&!waitingForScreen&&space>1.7&&distance>3.7&&char.stamina>.45;
  brain.protect=space<1.25;
  keepInCourt(brain.target);
}

function aiTryPostPlay(char,brain,read,lane,plan,tendencies){
  if(tendencies.post<.45||read.distance<2.15||read.distance>6.1||plan.stalled>3.2||brain.possessionAge>4.5||
    lane.direct&&lane.drive&&lane.openness>.86&&lane.finishSpace>2.5&&!lane.blocked) return false;
  const defender=getNearestOpponent(char);
  if(!defender) return false;
  const pressure=getOpponents(char).filter(player=>flatDistance(player.group.position,char.group.position)<2.5);
  if(pressure.length>1) return false;
  const p=char.profile,g=defender.profile;
  const strength=p.ratings?.strength??60+((p.width||1)-1)*80;
  const guardStrength=g.ratings?.strength??60+((g.width||1)-1)*80;
  // Settle a seal against a single manageable defender; equal rim protection
  // and help still require a normal reset, pass or an actual open finish.
  const edge=strength-guardStrength+(p.height-g.height)*60;
  if(edge<-8||char.stamina<.35) return false;
  const side=char.group.position.x<0?-1:char.group.position.x>0?1:brain.driveSide;
  brain.decision='post';brain.role='post';brain.protect=true;brain.sprint=false;brain.driveUntil=0;
  brain.target.set(side*1.35,0,HOOP_POS.z+2.1);
  const near=flatDistance(char.group.position,defender.group.position)<1.9;
  if(near&&edge>4&&brain.moveCooldown<=0&&char.moveState==='normal'&&typeof startPostUp==='function'){
    startPostUp(char);brain.moveCooldown=1.05;brain.postRead='backdown';
  }else brain.postRead=near?'protect_seal':'establish_post';
  keepInCourt(brain.target);
  return true;
}

function aiDriveOpportunity(char,lane,distance){
  const finishing=.81+THREE.MathUtils.clamp(((char.profile.layup||1)-1)*.16,-.08,.12);
  const directBonus=lane.direct ? .05 : 0;
  const fatigue=(1-(char.stamina??1))*.1;
  const travel=Math.max(0,distance-7)*.02;
  return THREE.MathUtils.clamp((finishing+directBonus-fatigue-travel)*lane.openness,0,1);
}

function aiRouteBlocked(char,target){
  for(const other of aiRoster()){
    if(other===char) continue;
    const corridor=aiReadSegment(other.group.position,char.group.position,target);
    const radius=(char.bodyRadius||.55)+(other.bodyRadius||.55)+.1;
    if(corridor.t>.06&&corridor.distance<radius) return true;
  }
  return false;
}

function aiAdvanceToFrontcourt(char,toRim){
  const position=char.group.position,distance=flatDistance(position,HOOP_POS);
  const across=new THREE.Vector3(-toRim.z,0,toRim.x);
  const advance=THREE.MathUtils.clamp(distance-6.1,1,3.1);
  const preferred=char.aiBrain?.driveSide||1;
  let best=null,bestScore=-Infinity;
  for(const lateral of [0,preferred*1.8,-preferred*1.8,preferred*2.8,-preferred*2.8]){
    const target=position.clone().addScaledVector(toRim,advance).addScaledVector(across,lateral).setY(0);
    keepInCourt(target);
    const progress=distance-flatDistance(target,HOOP_POS);
    if(progress<.2||aiRouteBlocked(char,target)) continue;
    const score=progress-Math.abs(lateral)*.08;
    if(score>bestScore){best=target;bestScore=score;}
  }
  if(!best){
    // With a defender already on the body, first step around the pressure at
    // the same depth. Requiring immediate forward progress here would pin a
    // guarded handler forever just outside their shooting range.
    for(const side of [preferred,-preferred]){
      const target=position.clone().addScaledVector(across,side*2.3).sub(HOOP_POS).setY(0);
      target.normalize().multiplyScalar(distance-.12).add(HOOP_POS).setY(0);
      keepInCourt(target);
      if(!aiRouteBlocked(char,target)){best=target;break;}
    }
  }
  if(!best){
    for(const side of [preferred,-preferred]){
      const target=position.clone().addScaledVector(across,side*2.3).setY(0);
      keepInCourt(target);
      if(flatDistance(position,target)>.7&&flatDistance(target,HOOP_POS)<distance+.65&&
        !aiRouteBlocked(char,target)){best=target;break;}
    }
  }
  if(!best&&distance<8.6){
    const marker=getNearestOpponent(char);
    if(marker&&flatDistance(position,marker.group.position)<1.6){
      const away=position.clone().sub(marker.group.position).setY(0).normalize();
      for(const side of [preferred,-preferred,0]){
        const target=position.clone().addScaledVector(away,1.3).addScaledVector(across,side*.9);
        keepInCourt(target);
        if(flatDistance(target,HOOP_POS)<9.3&&!aiRouteBlocked(char,target)){best=target;break;}
      }
    }
  }
  return best;
}

function aiPlayHandlerTarget(char,brain,plan,toRim){
  if(typeof risePreferredSpot==='function'&&risePreferredSpot(char,brain,plan,true)) return true;
  const action=aiPlanAction(plan);
  if(plan.phase==="solo"||action==="balanced") return false;
  if((action==="pick_roll"||action==="pick_pop")&&plan.screenTarget){
    const side=plan.screenSide;
    if(plan.phase==="setup"){
      brain.target.copy(plan.screenTarget).addScaledVector(toRim,-1.6);
      brain.target.x+=toRim.z*side*.6;brain.target.z-=toRim.x*side*.6;
    }else if(plan.phase==="screen"){
      brain.target.copy(plan.screenTarget).addScaledVector(toRim,2.1);
      brain.target.x+=toRim.z*side*1.35;brain.target.z-=toRim.x*side*1.35;
    }else if(plan.phase==="roll"||plan.phase==="pop"){
      brain.target.set(-side*2.4,0,HOOP_POS.z+3.5);
    }
    return true;
  }
  if(action==="drive_kick"){
    if(plan.phase==="setup"||plan.phase==="kick") brain.target.set(plan.screenSide*1.7,0,HOOP_POS.z+7.1);
    else brain.target.set(plan.screenSide*2.65,0,HOOP_POS.z+2.8);
    return true;
  }
  if(action==="motion"){
    brain.target.set(plan.actorSide*2.7,0,HOOP_POS.z+(plan.phase==="cut"?6.2:7.6));
    return true;
  }
  if(action==="give_go"){
    const hub=getPlayerById(plan.actorId);
    if(plan.phase==="cut") brain.target.set(-plan.actorSide*3.2,0,HOOP_POS.z+5.5);
    else if(hub) brain.target.copy(hub.group.position).add(new THREE.Vector3(-plan.actorSide*3,0,1.8));
    return true;
  }
  if(action==="post_up"){
    const post=getPlayerById(plan.chemistry.interiorId);
    if(post===char){brain.target.set(plan.actorSide*.95,0,HOOP_POS.z+1.5);brain.protect=aiNearestDistance(char)<1.7;}
    else brain.target.set(plan.actorSide*4.9,0,HOOP_POS.z+5.3);
    return true;
  }
  if(action==="isolation"){
    brain.target.set(plan.screenSide*3.1,0,HOOP_POS.z+(plan.phase==="attack"?3.8:6.9));
    return true;
  }
  if(action==="fast_break"){
    brain.target.copy(HOOP_POS).setY(0);brain.target.x=plan.screenSide*1.25;
    return true;
  }
  return false;
}

function beginAIShot(char,brain){
  handleActionButton(char);
  if(!shotCharging||chargingPlayer!==char) return;
  const type=chooseShotType(char);
  brain.shotKind=type;
  if(type==="layup"||type==="dunk"){
    // Use the same tap/gather action available to humans. Holding a full
    // jumper meter here used to turn an open running layup into a stop shot.
    brain.releaseAt=.10;
    brain.actionCooldown=.65;
    return;
  }
  const profile=getShotMeterProfile(type,char,calculateShotContest(char));
  // Identical release variation at every difficulty; no guaranteed greens.
  const center=(profile.greenStart+profile.greenEnd)*.5;
  brain.releaseAt=Math.max(.25,(center+(Math.random()-.5)*.18)*1.35/profile.meterFillSpeed);
  brain.actionCooldown=.9;
}

function updateAIShotAndPass(char,brain,handler,dt){
  if(shotCharging&&chargingPlayer===char){
    const desired=Math.atan2(HOOP_POS.x-char.group.position.x,HOOP_POS.z-char.group.position.z);
    char.angle=lerpAngle(char.angle,desired,1-Math.exp(-dt*13));char.group.rotation.y=char.angle;
    if((char.moveMeta.chargeHeld||0)>=brain.releaseAt){
      releaseShootOrFake(char);
      brain.releaseAt=0;
      brain.possessionAge=0;
    }
    return;
  }
  if(!brain.passTargetId||handler!==char||ballState!=="dribble") return;
  const receiver=getPlayerById(brain.passTargetId);
  if(!receiver||receiver.teamId!==char.teamId||brain.passUntil<teamAITime||!aiAssessPass(char,receiver,aiSettings()).safe){
    brain.passTargetId=null;brain.thinkIn=0;return;
  }
  const desired=Math.atan2(receiver.group.position.x-char.group.position.x,receiver.group.position.z-char.group.position.z);
  char.angle=lerpAngle(char.angle,desired,1-Math.exp(-dt*13));
  char.group.rotation.y=char.angle;
  const direction=getForward(char.angle);
  const targetDirection=brain.input.worldDirection.copy(receiver.group.position).sub(char.group.position).setY(0).normalize();
  if(direction.dot(targetDirection)>.97){
    // A turning passer may lose the window before facing the receiver. Read
    // the advantage again at release, while preserving the normal reaction tick.
    const plan=aiGetTeamPlan(char),settings=aiSettings();
    const purpose=aiPassingPurpose(char,receiver,aiAssessPass(char,receiver,settings),
      aiAssessShot(char,settings),aiFindDriveLane(char,settings),plan,settings);
    if(!purpose){brain.passTargetId=null;brain.thinkIn=0;return;}
    brain.passReason=purpose.reason;
    const passed=tryDirectionalPass(char,receiver);
    if(passed){
      plan.passHistory.push({from:char.id,to:receiver.id,at:teamAITime,fromPosition:char.group.position.clone(),
        toPosition:receiver.group.position.clone(),reason:brain.passReason||"advantage"});
      if(plan.passHistory.length>8) plan.passHistory.shift();
      plan.passerId=char.id;plan.passCutUntil=teamAITime+1.75;
    }
    brain.passTargetId=null;
    brain.actionCooldown=passed ? .48 : 0;
    brain.possessionAge=0;
  }
}

function decideAIOffBall(char,brain,handler,settings){
  const plan=aiGetTeamPlan(handler);
  const action=aiPlanAction(plan),identity=aiPlayerIdentity(char);
  if(action==="balanced"){
    decideAIBalancedOffBall(char,brain,handler,settings);
    if(typeof risePreferredSpot==='function') risePreferredSpot(char,brain,plan,false);
    aiCoordinateOffBall(char,brain,handler,settings,plan);
    return;
  }
  const support=aiRoster().filter(player=>player.teamId===char.teamId&&player!==handler);
  const slot=Math.max(0,support.indexOf(char));
  const side=slot%2===0?-1:1;
  const activePasser=getPlayerById(plan.passerId);
  const passCut=(action==="motion"||action==="give_go")&&activePasser&&!aiIsHuman(activePasser)&&plan.passCutUntil>teamAITime;
  brain.role="space";brain.roleUntil=0;
  brain.target.set(side*(support.length>1?5.4:4.9),0,HOOP_POS.z+(slot===1?5.2:7.8));
  if(passCut&&plan.passerId===char.id){
    brain.role="cut";brain.roleUntil=plan.passCutUntil;
    brain.target.set(char.group.position.x<0?1.2:-1.2,0,HOOP_POS.z+2.1);brain.sprint=true;
  }else if(action==="motion"){
    if(char.id===plan.actorId&&plan.phase==="cut"&&!passCut){
      brain.role="cut";brain.roleUntil=teamAITime+.6;
      brain.target.set(-side*1.1,0,HOOP_POS.z+2.2);brain.sprint=true;
    }else if(plan.phase==="fill"||passCut){
      brain.role="fill";
      brain.target.set(side*5.3,0,HOOP_POS.z+8.5);
    }else if(plan.phase==="setup"&&support.length>1&&char.id!==plan.actorId){
      // In 3v3 the third player pins the cutter's defender before the cut.
      const cutter=getPlayerById(plan.actorId),defender=cutter&&getNearestOpponent(cutter);
      if(defender&&flatDistance(defender.group.position,cutter.group.position)<2.8){
        brain.target.copy(defender.group.position).add(new THREE.Vector3(-plan.actorSide*.8,0,.15));
        brain.role=flatDistance(char.group.position,brain.target)<.7?"screen":"screen_setup";
        brain.roleUntil=teamAITime+.45;
      }
    }
  }else if(action==="pick_roll"||action==="pick_pop"){
    if(char.id===plan.actorId){
      if(plan.phase==="setup"||plan.phase==="screen"){
        brain.role=plan.phase==="screen"?"screen":"screen_setup";brain.roleUntil=teamAITime+.6;
        brain.target.copy(plan.screenTarget);
      }else if(plan.phase==="roll"){
        brain.role="roll";brain.roleUntil=teamAITime+.6;
        brain.target.set(plan.screenSide*1.25,0,HOOP_POS.z+2.1);brain.sprint=true;
      }else if(plan.phase==="pop"){
        brain.role="pop";brain.roleUntil=teamAITime+.6;
        brain.target.set(plan.screenSide*5.1,0,HOOP_POS.z+6.5);brain.sprint=true;
      }
    }else{
      brain.target.set(plan.screenSide*5.9,0,HOOP_POS.z+4.2);
    }
  }else if(action==="drive_kick"){
    brain.role="outlet";
    const weakSide=-plan.screenSide;
    brain.target.set((slot===0?weakSide:plan.screenSide)*6,0,HOOP_POS.z+(slot===0?3.1:6.6));
    // When the drive crosses the outlet, lift behind it to keep a clear lane.
    if(flatDistance(handler.group.position,brain.target)<3) brain.target.z=handler.group.position.z+3.8;
    // A shooter lifts to the arc; an interior player stays a short corner outlet.
    if(identity.type==="sharpshooter") brain.target.z=Math.max(brain.target.z,HOOP_POS.z+5.8);
    if(identity.type==="interior"&&slot===0) brain.target.z=HOOP_POS.z+2.8;
  }else if(action==="give_go"){
    if(plan.phase==="cut"&&!passCut&&char.id===plan.actorId){
      brain.role="cut";brain.roleUntil=teamAITime+.55;
      brain.target.set(-plan.actorSide*1.4,0,HOOP_POS.z+2);brain.sprint=true;
    }else if(char.id===plan.actorId){
      brain.role="hub";
      brain.target.set(plan.actorSide*3.5,0,HOOP_POS.z+5.1);
    }else{brain.role="fill";brain.target.set(side*5.8,0,HOOP_POS.z+7.4);}
  }else if(action==="post_up"){
    if(char.id===plan.chemistry.interiorId){
      brain.role="post";brain.roleUntil=teamAITime+.65;
      brain.target.set(plan.actorSide*1.45,0,HOOP_POS.z+2.3);brain.protect=true;
      const marker=getNearestOpponent(char);
      if(marker&&flatDistance(marker.group.position,brain.target)<1.1){
        brain.target.x=marker.group.position.x+(handler.group.position.x>marker.group.position.x?.95:-.95);
      }
    }else{brain.role="outlet";brain.target.set(-plan.actorSide*5.7,0,HOOP_POS.z+6.1+slot*1.4);}
  }else if(action==="isolation"){
    brain.role="clear";
    brain.target.set(-plan.screenSide*(slot===0?5.8:3.9),0,HOOP_POS.z+(slot===0?3.8:8.8));
  }else if(action==="fast_break"){
    if(char.id===plan.actorId){
      brain.role="break";brain.roleUntil=teamAITime+.55;
      brain.target.set(side*2.8,0,HOOP_POS.z+2.1);brain.sprint=(char.stamina??1)>.35;
    }else{brain.role="outlet";brain.target.set(-side*5.4,0,HOOP_POS.z+6.8);brain.sprint=true;}
  }
  aiAdaptOffBallRoute(char,brain,handler,plan,settings);
  if(typeof risePreferredSpot==='function') risePreferredSpot(char,brain,plan,false);
  aiCoordinateOffBall(char,brain,handler,settings,plan);
  keepInCourt(brain.target);
}

function aiAdaptOffBallRoute(char,brain,handler,plan,settings){
  if(brain.role==="screen"||brain.role==="screen_setup") return;
  const targetedCut=["cut","roll","break"].includes(brain.role);
  const blocked=targetedCut&&getOpponents(char).some(defender=>{
    const gap=aiReadSegment(defender.group.position,char.group.position,brain.target);
    return gap.t>.12&&gap.distance<(char.bodyRadius||.55)+(defender.bodyRadius||.55)+.12;
  });
  if(blocked){
    // Curl behind an overplaying defender, or stop at an empty short corner.
    // Only the selected cutter changes route; a second player never joins it.
    const side=char.group.position.x<0?-1:1;
    const options=[new THREE.Vector3(side*2.65,0,HOOP_POS.z+1.8),new THREE.Vector3(side*4.4,0,HOOP_POS.z+4.3)];
    const alternate=options.find(target=>aiCoordLaneSpace(handler,char.group.position,target,settings)>1.15&&
      !getTeammates(char).some(player=>player!==handler&&flatDistance(player.aiBrain?.target||player.group.position,target)<2));
    if(alternate){brain.target.copy(alternate);brain.route="backdoor";}
    else{brain.target.copy(char.group.position);brain.sprint=false;brain.route="wait";}
  }else brain.route=null;
  if(!targetedCut&&brain.role!=="post"&&brain.role!=="hub"){
    const defender=getNearestOpponent(char);
    if(defender&&aiPassLaneClearance(handler,char)<1.1){
      const side=brain.target.x<0?-1:1;
      brain.target.x+=side*.45;
      brain.target.z+=defender.group.position.z>char.group.position.z?-1.15:1.15;
    }
  }
  // Targets are reserved by roster order so off-ball routes don't converge.
  for(const teammate of getTeammates(char)){
    if(teammate===handler||!teammate.aiBrain||teammate.id===char.id) continue;
    if(String(teammate.id)<String(char.id)&&flatDistance(teammate.aiBrain.target,brain.target)<1.65){
      brain.target.z+=1.9;
    }
  }
}

function decideAIBalancedOffBall(char,brain,handler,settings){
  const teammates=getTeammates(char);
  const tendencies=aiPlayerTendencies(char);
  const otherRoles=teammates.filter(player=>player!==handler&&player.aiBrain);
  const cutters=otherRoles.some(player=>player.aiBrain.role==="cut"&&player.aiBrain.roleUntil>teamAITime);
  const screeners=otherRoles.some(player=>player.aiBrain.role==="screen"&&player.aiBrain.roleUntil>teamAITime);
  const position=char.group.position;
  const team=aiRoster().filter(player=>player.teamId===char.teamId);
  const slot=team.filter(player=>player!==handler).indexOf(char);
  const side=slot===0?-1:1;
  const spacing=team.length===3?5.1:4.5;
  const defender=getNearestOpponent(char);
  const handlerPressure=aiNearestDistance(handler);
  if(brain.roleUntil>teamAITime&&(brain.role==="cut"||brain.role==="screen"||brain.role==="roll")){
    if(brain.role==="screen"){
      const onBallDefender=getNearestOpponent(handler);
      if(onBallDefender&&flatDistance(position,brain.target)>.8){
        const towardHandler=handler.group.position.clone().sub(onBallDefender.group.position).setY(0).normalize();
        brain.target.copy(onBallDefender.group.position).addScaledVector(towardHandler,-.7);
      }
    }
    brain.sprint=brain.role!=="screen";
    return;
  }
  if(brain.role==="screen"){
    brain.role="roll";
    brain.roleUntil=teamAITime+1.35;
    brain.target.set(side*1.4,0,HOOP_POS.z+2.6);
    brain.sprint=true;
    return;
  }
  brain.role="space";
  brain.target.set(side*spacing,0,HOOP_POS.z+(slot===1?5.5:7.7));
  if(teammates.some(player=>player!==handler&&flatDistance(player.group.position,brain.target)<2)) brain.target.z+=2.2;
  if(handler.group.position.x*side>2) brain.target.z=HOOP_POS.z+8.9;
  // Put the wing outside the defender's shadow to create an actual pass lane.
  if(defender&&aiPassLaneClearance(handler,char)<1.1){
    brain.target.z+=defender.group.position.z>position.z?-1.2:1.2;
    brain.target.x+=side*.65;
  }
  if(!cutters&&brain.moveCooldown<=0&&flatDistance(handler.group.position,HOOP_POS)>4&&handlerPressure>1.2&&Math.random()<
    (.1+settings.spacingIQ*.22)*(1+tendencies.drive*.7-tendencies.perimeter*.7)){
    brain.role="cut";
    brain.roleUntil=teamAITime+1.35;
    brain.moveCooldown=4.5;
    brain.target.set(-side*1.1,0,HOOP_POS.z+2.2);
    brain.sprint=true;
  }else if(!screeners&&brain.moveCooldown<=0&&handlerPressure<2&&flatDistance(handler.group.position,HOOP_POS)>4.4&&Math.random()<settings.spacingIQ*(.2+tendencies.screening*.3+tendencies.post*.1)){
    const onBallDefender=getNearestOpponent(handler);
    if(onBallDefender){
      brain.role="screen";
      brain.roleUntil=teamAITime+2;
      brain.moveCooldown=6;
      const direction=handler.group.position.clone().sub(onBallDefender.group.position).setY(0).normalize();
      brain.target.copy(onBallDefender.group.position).addScaledVector(direction,-.7);
    }
  }
  keepInCourt(brain.target);
}

function decideAIDefense(char,brain,handler,settings){
  const assigned=aiAssignedOpponent(char)||handler;
  if(!assigned){ brain.target.copy(char.group.position); return; }
  brain.role="guard";
  brain.protect=true;
  const guardingHandler=assigned===handler;
  const spacing=guardingHandler?THREE.MathUtils.lerp(1.9,1.25,settings.defensiveIQ):1.8;
  brain.target.copy(getBetweenPlayerAndRimTarget(char,assigned,spacing));
  const assignedRimDistance=flatDistance(assigned.group.position,HOOP_POS);
  if(!guardingHandler&&handler){
    const trap=aiGetDefensePlan(char.teamId,handler,settings).trap;
    if(trap&&trap.helperId===char.id){
      brain.role=trap.space==="paint"?"help":"double";
      // Close the escape side while the primary contains the basket side.
      const side=trap.space==="sideline"?(handler.group.position.x>0?-1:1):(char.group.position.x>handler.group.position.x?1:-1);
      brain.target.copy(handler.group.position);
      brain.target.x+=side*1.05;brain.target.z+=trap.space==="paint"?-.3:.25;brain.target.y=0;
    }else if(trap){
      brain.role="rotate";
      const helper=getPlayerById(trap.helperId);
      const uncovered=helper?aiAssignedOpponent(helper):null;
      if(uncovered&&uncovered!==handler){
        const openTarget=getBetweenPlayerAndRimTarget(char,uncovered,1.4);
        const threat=flatDistance(uncovered.group.position,HOOP_POS)<assignedRimDistance?.7:.4;
        brain.target.lerp(openTarget,threat);
        // Remain in the passing lanes, outside the two-player trap pocket.
        if(flatDistance(brain.target,handler.group.position)<2.8){
          brain.target.copy(assigned.group.position).lerp(uncovered.group.position,.5);
          if(flatDistance(brain.target,handler.group.position)<2.8) brain.target.z+=3.1;
        }
      }
    }else{
      // Shade toward visible passing direction; never inspect human input.
      const aim=getForward(handler.angle);
      const toAssigned=assigned.group.position.clone().sub(handler.group.position).setY(0).normalize();
      if(aim.dot(toAssigned)>.83&&assignedRimDistance<8.6){
        brain.target.lerp(assigned.group.position,.3+settings.spacingIQ*.2);
      }
    }
  }
  aiCoordinateDefense(char,brain,handler,settings);
  keepInCourt(brain.target);
  const recovery=flatDistance(char.group.position,brain.target);
  brain.sprint=brain.sprint||recovery>2.6;
  if(!handler) return;
  const range=flatDistance(char.group.position,handler.group.position);
  const gathering=shotCharging&&chargingPlayer===handler&&(handler.moveMeta.chargeHeld||0)>.42;
  const finishing=layupActive&&layupOwner===handler.id||dunkActive&&dunkOwner===handler.id;
  const fake=handler.moveState==="pumpfake";
  const guardian=Number(char.profile.abilities?.rimGuardian)||0;
  const discipline=Math.min(.65,guardian*.12+(char.profile.role==='anchor'?.2:0));
  if(range<2.5&&brain.actionCooldown<=0&&(gathering||finishing||fake&&Math.random()<settings.fakeBiteChance*(1-discipline))){
    const jump=finishing||gathering&&Math.random()<.25+settings.contestTiming*.45+guardian*.035||fake;
    if(jump&&char.onGround){ tryJumpBlock(char,false); brain.actionCooldown=.8; }
    else raiseHands(char);
  }else if(range<2.7&&(gathering||finishing)){
    raiseHands(char);
  }
  if(range<1.35&&ballState==="dribble"&&brain.actionCooldown<=0){
    const exposed=handler.dribbleStyle==="sprint"||handler.dribbleStyle==="crossover"||handler.moveState==="sidehop";
    const pickpocket=Number(char.profile.abilities?.pickpocket)||0;
    const visibleBall=pickpocket&&ball?.position&&flatDistance(char.group.position,ball.position)<1.2;
    const reachMistake=Math.random()<settings.mistakeChance*.12*(1-discipline*.5-pickpocket*.07);
    if(exposed&&Math.random()<settings.stealDiscipline*.25+(visibleBall?pickpocket*.035:0)||reachMistake){
      handleHandAction(char,ballSideValue<0?"left":"right","ai-steal");
      brain.actionCooldown=.85;
      if(reachMistake) punishBadReach(char);
    }
  }
}

function decideAIRebound(char,brain,settings){
  brain.role="rebound";
  const y=Math.max(.1,ball.position.y-.65);
  const fallTime=THREE.MathUtils.clamp((ballVelocity.y+Math.sqrt(ballVelocity.y*ballVelocity.y+19.6*y))/9.8,0,.8);
  brain.target.copy(ball.position).addScaledVector(ballVelocity,fallTime*.75).setY(0);
  if(ballState!=="loose"&&ball.position.y>HOOP_POS.y+.5) brain.target.lerp(HOOP_POS,.4);
  keepInCourt(brain.target);
  const team=aiRoster().filter(player=>player.teamId===char.teamId);
  const reboundValue=player=>flatDistance(player.group.position,brain.target)-(player.profile.ratings?((player.profile.ratings.rebounding??50)-50)*.012:0);
  const chaser=team.reduce((best,player)=>!best||reboundValue(player)<reboundValue(best)?player:best,null);
  if(chaser!==char){
    const assigned=aiAssignedOpponent(char);
    if(assigned&&flatDistance(assigned.group.position,brain.target)<4.5){
      brain.role="box_out";
      const towardBall=brain.target.clone().sub(assigned.group.position).setY(0).normalize();
      brain.target.copy(assigned.group.position).addScaledVector(towardBall,.85);
      brain.protect=true;
    }else{
      brain.role="outlet";
      brain.target.z=Math.max(brain.target.z+3.2,HOOP_POS.z+6.5);
      brain.target.x=char.group.position.x<0?-4:4;
    }
  }
  brain.sprint=chaser===char;
  const distance=flatDistance(char.group.position,ball.position);
  if(chaser===char&&distance<1.6&&ball.position.y>1.15&&ball.position.y<2.6&&ballVelocity.y<0&&char.onGround&&brain.actionCooldown<=0){
    tryJumpBlock(char,false);
    brain.actionCooldown=.8;
  }
}

// Compatibility for tournament/debug helpers. These still use the common
// movement controller, so legacy callers cannot restore difficulty speed boosts.
function updateBot(dt){ updateTeamAI(dt); }
function moveBotToward(bot,target,speed,dt){
  if(!bot) return;
  const brain=bot.aiBrain||(bot.aiBrain=createTeamBrain(bot));
  brain.target.copy(target);
  brain.sprint=false;
  bot.aiInput=brain.input;
  moveAIPlayer(bot,brain,dt);
}
function getDefensiveTarget(defender,handler,intent="balanced_defense"){
  return getBetweenPlayerAndRimTarget(defender,handler,intent==="pressure_steal"?.85:1.65);
}
function shouldBotJumpContest(defender,shooter,shotType,timingPct){
  return flatDistance(defender.group.position,shooter.group.position)<2.5&&timingPct>.45&&Math.random()<.3+aiSettings().contestTiming*.4;
}
