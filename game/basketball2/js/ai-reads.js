// Court reads use public positions and velocity. Better opponents recognize
// openings sooner and project movement more accurately; they keep human stats.
function aiReadClamp(value,min=0,max=1){ return Math.max(min,Math.min(max,value)); }
function aiReadDistance(a,b){ return Math.hypot(a.x-b.x,a.z-b.z); }
function aiReadVelocity(player){ return player.velocity || new THREE.Vector3(); }
function aiReadSettings(settings){
  const vision=aiReadClamp(settings?.courtVision ?? settings?.spacingIQ ?? .6);
  return {vision,error:Math.max(0,settings?.readError ?? (1-vision)*1.15),
    ahead:Math.max(0,Math.min(.8,settings?.lookAhead ?? .12+vision*.35))};
}
function aiReadHash(value){
  let hash=2166136261;
  for(let i=0;i<value.length;i++){ hash^=value.charCodeAt(i);hash=Math.imul(hash,16777619); }
  return (hash>>>0)/4294967296;
}
function aiReadPosition(observer,player,settings,horizon=0){
  const read=aiReadSettings(settings);
  const result=player.group.position.clone().setY(0);
  const velocity=aiReadVelocity(player);
  const time=Math.min(Math.max(0,horizon),read.ahead)*(.3+read.vision*.7);
  // Bound a forecast to a plausible next step, even during a collision impulse.
  const speed=Math.hypot(velocity.x,velocity.z);
  const scale=speed>8?8/speed:1;
  result.x+=velocity.x*time*scale;result.z+=velocity.z*time*scale;
  if(player!==observer&&read.error>0){
    // Stable for this observer/subject pair. No new random draw every frame.
    const identity=`${observer?.id || "observer"}:${observer?.aiBrain?.readSeed || 0}:${player.id || "player"}`;
    const angle=aiReadHash(identity)*Math.PI*2;
    const amount=read.error*(.55+.45*aiReadHash(identity+":radius"));
    result.x+=Math.cos(angle)*amount;result.z+=Math.sin(angle)*amount;
  }
  return result;
}
function aiReadOpponents(char){ return typeof getOpponents==="function"?getOpponents(char):[]; }
function aiReadTeammates(char){ return typeof getTeammates==="function"?getTeammates(char):[]; }
function aiReadSegment(point,start,end){
  const dx=end.x-start.x,dz=end.z-start.z,length=dx*dx+dz*dz;
  const rawT=length>.0001?((point.x-start.x)*dx+(point.z-start.z)*dz)/length:0;
  const t=aiReadClamp(rawT);
  return {t,rawT,distance:Math.hypot(point.x-start.x-dx*t,point.z-start.z-dz*t)};
}

// Read the race at the attacker's shoulder, rather than treating a nearby
// pursuing body as an obstacle along every metre of the lane. A defender who
// can visibly cross back in front still owns the angle.
function aiReadDriveEdge(char,defender,position,point,direction){
  const body=(char.bodyRadius || .55)+(defender.bodyRadius || .55);
  const dx=point.x-position.x,dz=point.z-position.z;
  const forward=dx*direction.x+dz*direction.z;
  const lateral=Math.abs(dx*direction.z-dz*direction.x);
  if(forward>.35||Math.hypot(dx,dz)<body+.08) return false;
  if(forward>-.35&&lateral<body+.08) return false;
  if(defender.blockTime>0&&defender.onGround===false&&Math.hypot(dx,dz)<2.4) return false;
  const velocity=aiReadVelocity(char),guardVelocity=aiReadVelocity(defender);
  const speed=Math.hypot(velocity.x,velocity.z),progress=velocity.x*direction.x+velocity.z*direction.z;
  if(speed>1.2&&progress<speed*.5) return false;
  // A stationary handler can start a normal drive; an existing runner uses
  // their actual forward momentum. This forecast never changes movement stats.
  const attackSpeed=Math.min(8,Math.max(2.8,progress)),horizon=.26;
  const guardSpeed=Math.hypot(guardVelocity.x,guardVelocity.z),guardScale=guardSpeed>8?8/guardSpeed:1;
  const futureX=dx+(guardVelocity.x*guardScale-direction.x*attackSpeed)*horizon;
  const futureZ=dz+(guardVelocity.z*guardScale-direction.z*attackSpeed)*horizon;
  const futureForward=futureX*direction.x+futureZ*direction.z;
  const futureLateral=Math.abs(futureX*direction.z-futureZ*direction.x);
  return !(futureForward>-.35&&futureLateral<body+.02);
}

function aiReadFinishMatchup(char,position,contests){
  const distance=aiReadDistance(position,HOOP_POS),stamina=char.stamina ?? 1;
  const nearby=contests.filter(read=>read.gap<2.35&&(read.pressure>.2||read.rimPressure>.3));
  nearby.sort((a,b)=>b.pressure+b.rimPressure-a.pressure-a.rimPressure);
  const primary=nearby[0],defender=primary?.defender;
  let mismatchAdvantage=0,activeBlockThreat=false;
  for(const read of nearby){
    const player=read.defender;
    const progress=1-aiReadClamp((player.blockTime || 0)/.42);
    const timedJump=player.blockTime>0&&player.onGround===false&&progress>.12&&progress<.86;
    if(timedJump&&read.gap<2.15) activeBlockThreat=true;
  }
  if(defender&&distance<=2.2){
    const attacker=char.profile || {},guard=defender.profile || {};
    const height=(attacker.height || 1)-(guard.height || 1);
    const width=(attacker.width || 1)-(guard.width || 1);
    // Size earns a contact opportunity, not a make. A long, skilled rim
    // protector, raised hands and exhaustion can remove that opportunity.
    const sizeEdge=height>.075||height>.015&&width>.18;
    if(sizeEdge){
      mismatchAdvantage=aiReadClamp(height*1.75+width*.65+Math.max(0,(attacker.layup || 1)-1)*.16-
        Math.max(0,(guard.block || 1)-.85)*.22-(defender.handsUpTime>0?.085:0));
      mismatchAdvantage*=aiReadClamp((stamina-.3)/.5);
    }
  }
  const extraHelp=Math.max(0,nearby.length-1);
  const finishRisk=primary?aiReadClamp((primary.pressure+primary.rimPressure)*.25+
    extraHelp*.29+(activeBlockThreat?.48:0)+Math.max(0,.6-stamina)*.6-mismatchAdvantage*.28):0;
  const finishFavorable=distance<=2.2&&!!defender&&mismatchAdvantage>.22&&
    extraHelp===0&&!activeBlockThreat&&stamina>.4&&finishRisk<.47;
  return {finishFavorable,mismatchAdvantage,finishRisk,finishDefender:defender || null,
    paintDefenders:nearby.length,activeBlockThreat};
}

function aiReadShotAt(char,observer,position,settings,horizon=0){
  const view=aiReadSettings(settings);
  const velocity=aiReadVelocity(char),speed=Math.hypot(velocity.x,velocity.z);
  const distance=aiReadDistance(position,HOOP_POS);
  const toRim=HOOP_POS.clone().sub(position).setY(0).normalize();
  const toward=speed>.01?(velocity.x*toRim.x+velocity.z*toRim.z)/speed:0;
  const running=speed>1.2;
  const closeFinish=distance<2.9 || distance<3.6&&running;
  let space=8,finishingSpace=8,rimPressure=0,pressure=0;
  const contests=[];
  for(const defender of aiReadOpponents(char)){
    const point=aiReadPosition(observer,defender,settings,horizon);
    const gap=aiReadDistance(position,point);
    space=Math.min(space,gap);
    const towardDefender=point.clone().sub(position).setY(0);
    const alignment=gap>.01?towardDefender.dot(toRim)/gap:1;
    // A current running gather advances with its pursuer. Forecasting only
    // the defender makes an equally fast trailer appear to jump in front.
    const motionTime=Math.min(Math.max(0,horizon),view.ahead)*(.3+view.vision*.7)*(speed>8?8/speed:1);
    const edgePoint=closeFinish&&running&&aiReadDistance(position,char.group.position)<.05
      ?point.clone().addScaledVector(velocity,-motionTime) :point;
    const driveEdge=closeFinish&&running&&toward>.35&&aiReadDriveEdge(char,defender,position,edgePoint,toRim);
    if(!driveEdge) finishingSpace=Math.min(finishingSpace,gap);
    const reach=aiReadClamp((defender.profile?.block || 1)-1,-.4,.8)*.18;
    const front=closeFinish?(driveEdge?.38:alignment<-.3?.68:1):1;
    const defenderPressure=aiReadClamp((2.45+reach-gap)/2.15)*front;
    pressure=Math.max(pressure,defenderPressure);
    let defenderRimPressure=0;
    if(closeFinish&&!driveEdge){
      const line=aiReadSegment(point,position,HOOP_POS);
      if(line.t>.05&&(line.t<1||aiReadDistance(point,HOOP_POS)<1.6)&&aiReadDistance(point,HOOP_POS)<3.1){
        defenderRimPressure=aiReadClamp((1.75+reach-line.distance)/1.75)*(1-line.t*.22);
        rimPressure=Math.max(rimPressure,defenderRimPressure);
      }
    }
    contests.push({defender,gap,pressure:defenderPressure,rimPressure:defenderRimPressure});
  }
  const shooting=char.profile?.shot || 1,finishing=char.profile?.layup || 1;
  const preferredRange=aiReadClamp(5.9+(shooting-1)*4.8,4.2,8.5);
  const inRange=distance<preferredRange+.6;
  const momentum=closeFinish?Math.max(0,speed-4)*.025+Math.max(0,-toward)*speed*.025:Math.min(.28,speed*.045);
  const fatigue=Math.max(0,1-(char.stamina ?? 1))*.13;
  const rangePenalty=Math.max(0,distance-preferredRange)*.15;
  const finishOpen=closeFinish&&finishingSpace>1.55&&pressure<.38&&rimPressure<.42;
  const matchup=aiReadFinishMatchup(char,position,contests);
  const contactScale=matchup.finishFavorable?1-Math.min(.36,matchup.mismatchAdvantage*.52):1;
  let quality=closeFinish?.91+(finishing-1)*.10:.79+(shooting-1)*.16;
  quality-=rangePenalty+momentum+fatigue+(pressure*(closeFinish?.43:.51)+rimPressure*.28)*contactScale;
  if(matchup.finishFavorable) quality+=Math.min(.095,matchup.mismatchAdvantage*.15);
  if(closeFinish&&matchup.activeBlockThreat) quality-=.14;
  if(closeFinish&&toward>.35&&running&&finishOpen) quality+=.055;
  quality=aiReadClamp(quality);
  const points=distance>(typeof THREE_RADIUS!=="undefined"?THREE_RADIUS:7.15)?3:2;
  // The extra point matters only after the shooter has earned a clean look.
  // Quality stays a feasibility grade so a bad three never gains an excuse.
  const value=aiReadClamp(quality+(points===3&&inRange&&quality>.7&&space>2.05?.065:0));
  return {quality,value,points,distance,closeFinish,finishOpen,space,finishingSpace,inRange,preferredRange,momentum,rimPressure,pressure,running,...matchup};
}

function aiAssessShot(char,settings,positionOverride){
  return aiReadShotAt(char,char,positionOverride || char.group.position,settings,aiCloseReadHorizon(char,settings));
}
function aiCloseReadHorizon(char,settings){
  return Math.min(aiReadSettings(settings).ahead,Math.hypot(aiReadVelocity(char).x,aiReadVelocity(char).z)>1.2?.32:.18);
}

function aiReadDrivePath(char,waypoint,finish,settings){
  const start=char.group.position,read=aiReadSettings(settings);
  const first=aiReadDistance(start,waypoint),second=aiReadDistance(waypoint,finish);
  const speed=Math.max(3.5,Math.hypot(aiReadVelocity(char).x,aiReadVelocity(char).z));
  const departure=waypoint.clone().sub(start).setY(0).normalize();
  const finishDirection=HOOP_POS.clone().sub(finish).setY(0).normalize();
  let clearance=8,finishSpace=8,immediateBlocked=false,angleAdvantage=false;
  for(const defender of aiReadOpponents(char)){
    const current=aiReadPosition(char,defender,settings);
    const firstProjection=aiReadSegment(current,start,waypoint);
    const future=aiReadPosition(char,defender,settings,first*firstProjection.t/speed);
    const one=aiReadSegment(future,start,waypoint);
    const twoProjection=aiReadSegment(current,waypoint,finish);
    const help=aiReadPosition(char,defender,settings,(first+second*twoProjection.t)/speed);
    const two=aiReadSegment(help,waypoint,finish);
    const body=(char.bodyRadius || .55)+(defender.bodyRadius || .55);
    const shoulder=aiReadDriveEdge(char,defender,start,current,departure);
    angleAdvantage=angleAdvantage||shoulder;
    // A defender behind the departure direction does not close the lane ahead.
    const movingAway=one.t<.06&&aiReadDistance(waypoint,future)>aiReadDistance(start,future)+.25;
    if(!movingAway&&!shoulder) clearance=Math.min(clearance,one.distance-body);
    // The run ends at the takeoff spot. A defender beyond it belongs to the
    // finish read; a clamped segment endpoint must not turn that distant body
    // into an obstacle along an otherwise empty driving corridor.
    const beyondFinish=two.rawT>1&&aiReadDistance(help,finish)>body+.18;
    const behindSecond=two.rawT<0&&aiReadDistance(finish,help)>aiReadDistance(waypoint,help)+.25;
    const finishEdge=shoulder&&aiReadDriveEdge(char,defender,finish,help,finishDirection);
    if(!beyondFinish&&!behindSecond&&!finishEdge) clearance=Math.min(clearance,two.distance-body);
    if(!finishEdge) finishSpace=Math.min(finishSpace,aiReadDistance(help,finish));
    // Physical bodies cannot be ignored by a noisy read in the immediate step.
    const nearEnd=start.clone().addScaledVector(waypoint.clone().sub(start).setY(0).normalize(),Math.min(1.45,first));
    const actual=aiReadSegment(defender.group.position,start,nearEnd);
    if(actual.t>.08&&actual.distance<body+.08) immediateBlocked=true;
  }
  for(const teammate of aiReadTeammates(char)){
    if(teammate.aiScreen||teammate.aiBrain?.role==="screen") continue;
    const point=aiReadPosition(char,teammate,settings,read.ahead*.5);
    const lane=aiReadSegment(point,start,waypoint);
    if(lane.t>.15) clearance=Math.min(clearance,lane.distance-(char.bodyRadius || .55)-(teammate.bodyRadius || .55)+.15);
  }
  const pathOpen=aiReadClamp(clearance/1.05);
  const finishOpen=aiReadClamp((finishSpace-1.1)/1.55);
  const finishRead=aiReadShotAt(char,char,finish,settings,Math.min(read.ahead,(first+second)/speed));
  const finishSafety=Math.min(finishOpen,aiReadClamp((.9-finishRead.rimPressure)/.65));
  const openness=immediateBlocked?0:pathOpen*(.42+.58*finishSafety);
  return {openness,finishSpace,clearance,angleAdvantage,blocked:immediateBlocked,clearCorridor:!immediateBlocked&&clearance>.48,
    finishRisk:finishRead.finishRisk,finishFavorable:finishRead.finishFavorable};
}

function aiFindDriveLane(char,settings){
  const start=char.group.position,distance=aiReadDistance(start,HOOP_POS);
  const direction=HOOP_POS.clone().sub(start).setY(0).normalize();
  const side=new THREE.Vector3(-direction.z,0,direction.x);
  const advance=Math.min(3.6,Math.max(.6,distance-1.6));
  let best=null;
  for(const offset of [0,-1,1,-2,2]){
    const lateral=offset===0?0:Math.sign(offset)*(Math.abs(offset)===1?1.65:2.65);
    const finish=HOOP_POS.clone().addScaledVector(direction,-1.65).addScaledVector(side,lateral*.32).setY(0);
    const target=distance<3.9?finish.clone():start.clone().addScaledVector(direction,advance).addScaledVector(side,lateral).setY(0);
    if(typeof keepInCourt==="function"){keepInCourt(target);keepInCourt(finish);}
    const read=aiReadDrivePath(char,target,finish,settings);
    const detour=aiReadDistance(start,target)+aiReadDistance(target,finish)-Math.max(0,distance-1.65);
    const score=read.openness-Math.max(0,detour)*.027-(offset===0?0:.018);
    const candidate={...read,target,finish,score,direct:offset===0,
      drive:read.openness>.52+aiReadSettings(settings).vision*.1&&!read.blocked&&distance>1.6};
    if(!best||candidate.score>best.score) best=candidate;
  }
  const setup=best.drive?{target:best.target.clone(),safe:true}:aiFindSetupSpace(char,settings);
  best.setupTarget=setup.target;best.setupSafe=setup.safe;
  return best;
}

function aiFindSetupSpace(char,settings){
  const start=char.group.position;
  const toward=HOOP_POS.clone().sub(start).setY(0).normalize();
  const side=new THREE.Vector3(-toward.z,0,toward.x);
  const preferred=char.aiBrain?.driveSide || 1;
  let best=null;
  for(const [lateral,retreat] of [[2.4*preferred,0],[-2.4*preferred,0],[1.7*preferred,1.2],[-1.7*preferred,1.2],[0,2.5]]){
    const target=start.clone().addScaledVector(side,lateral).addScaledVector(toward,-retreat).setY(0);
    if(typeof keepInCourt==="function") keepInCourt(target);
    if(aiReadDistance(start,target)<.65) continue;
    let clearance=8,safe=true;
    for(const player of [...aiReadOpponents(char),...aiReadTeammates(char)]){
      const point=player.group.position;
      const body=(char.bodyRadius || .55)+(player.bodyRadius || .55)+.12;
      const corridor=aiReadSegment(point,start,target);
      const movingAway=corridor.t<.05&&aiReadDistance(target,point)>aiReadDistance(start,point)+.35;
      if(movingAway) continue;
      clearance=Math.min(clearance,corridor.distance-body);
      if(corridor.distance<body) {safe=false;break;}
    }
    if(!safe) continue;
    const shot=aiReadShotAt(char,char,target,settings,aiReadSettings(settings).ahead);
    const score=shot.quality*.48+aiReadClamp(clearance/2)*.28-retreat*.035;
    if(!best||score>best.score) best={target,safe:true,score};
  }
  // A surrounded handler protects in place or passes rather than crossing a body.
  return best || {target:start.clone().setY(0),safe:false};
}

function aiAssessPass(passer,receiver,settings){
  const start=passer.group.position;
  const separation=aiReadDistance(start,receiver.group.position);
  const duration=aiReadClamp(separation/14,.22,.85);
  const target=aiReadPosition(passer,receiver,settings,duration*.72);
  if(typeof keepInCourt==="function") keepInCourt(target);
  let clearance=8,catchSpace=8;
  for(const defender of aiReadOpponents(passer)){
    const actualLane=aiReadSegment(defender.group.position,start,target);
    let point=aiReadPosition(passer,defender,settings);
    let projection=aiReadSegment(point,start,target);
    point=aiReadPosition(passer,defender,settings,duration*projection.t);
    projection=aiReadSegment(point,start,target);
    // Include both ends: a guarded receiver or close reach is still an unsafe pass.
    clearance=Math.min(clearance,projection.distance,actualLane.distance);
    catchSpace=Math.min(catchSpace,aiReadDistance(aiReadPosition(passer,defender,settings,duration),target));
  }
  const catchRead=aiReadShotAt(receiver,passer,target,settings,duration);
  const rimFeed=catchRead.distance<3.45;
  const finishAvailable=catchSpace>1.55&&catchRead.rimPressure<.5||catchRead.finishFavorable&&catchSpace>.85;
  const safe=separation>1.2&&separation<15&&clearance>.82&&(!rimFeed||finishAvailable);
  const laneValue=aiReadClamp((clearance-.5)/2.2)*.12;
  const finishBonus=rimFeed?(catchRead.finishOpen?.14:catchRead.finishFavorable?.09:0):0;
  const longPassPenalty=Math.max(0,separation-9)*.025;
  let value=catchRead.value+laneValue+finishBonus-longPassPenalty;
  if(!safe) value-=.5;
  return {value:aiReadClamp(value),safe,clearance,rimFeed,target,catchQuality:catchRead.quality,catchSpace,separation,
    catchValue:catchRead.value,points:catchRead.points,finishOpen:catchRead.finishOpen,
    finishFavorable:catchRead.finishFavorable,mismatchAdvantage:catchRead.mismatchAdvantage,finishRisk:catchRead.finishRisk};
}
