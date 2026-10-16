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
  const t=length>.0001?aiReadClamp(((point.x-start.x)*dx+(point.z-start.z)*dz)/length):0;
  return {t,distance:Math.hypot(point.x-start.x-dx*t,point.z-start.z-dz*t)};
}

function aiReadShotAt(char,observer,position,settings,horizon=0){
  const velocity=aiReadVelocity(char),speed=Math.hypot(velocity.x,velocity.z);
  const distance=aiReadDistance(position,HOOP_POS);
  const toRim=HOOP_POS.clone().sub(position).setY(0).normalize();
  const toward=speed>.01?(velocity.x*toRim.x+velocity.z*toRim.z)/speed:0;
  const running=speed>1.2;
  const closeFinish=distance<2.9 || distance<3.6&&running;
  let space=8,rimPressure=0,pressure=0;
  for(const defender of aiReadOpponents(char)){
    const point=aiReadPosition(observer,defender,settings,horizon);
    const gap=aiReadDistance(position,point);
    space=Math.min(space,gap);
    const towardDefender=point.clone().sub(position).setY(0);
    const alignment=gap>.01?towardDefender.dot(toRim)/gap:1;
    const reach=aiReadClamp((defender.profile?.block || 1)-1,-.4,.8)*.18;
    const front=closeFinish?(alignment<-.3?.68:1):1;
    pressure=Math.max(pressure,aiReadClamp((2.45+reach-gap)/2.15)*front);
    if(closeFinish){
      const line=aiReadSegment(point,position,HOOP_POS);
      if(line.t>.05&&(line.t<1||aiReadDistance(point,HOOP_POS)<1.6)&&aiReadDistance(point,HOOP_POS)<3.1){
        rimPressure=Math.max(rimPressure,aiReadClamp((1.75+reach-line.distance)/1.75)*(1-line.t*.22));
      }
    }
  }
  const shooting=char.profile?.shot || 1,finishing=char.profile?.layup || 1;
  const preferredRange=aiReadClamp(5.9+(shooting-1)*4.8,4.2,8.5);
  const inRange=distance<preferredRange+.6;
  const momentum=closeFinish?Math.max(0,speed-4)*.025+Math.max(0,-toward)*speed*.025:Math.min(.28,speed*.045);
  const fatigue=Math.max(0,1-(char.stamina ?? 1))*.13;
  const rangePenalty=Math.max(0,distance-preferredRange)*.15;
  const finishOpen=closeFinish&&space>1.55&&pressure<.38&&rimPressure<.42;
  let quality=closeFinish?.91+(finishing-1)*.10:.79+(shooting-1)*.16;
  quality-=rangePenalty+momentum+fatigue+pressure*(closeFinish?.43:.51)+rimPressure*.28;
  if(closeFinish&&toward>.35&&running&&finishOpen) quality+=.055;
  quality=aiReadClamp(quality);
  const points=distance>(typeof THREE_RADIUS!=="undefined"?THREE_RADIUS:7.15)?3:2;
  // The extra point matters only after the shooter has earned a clean look.
  // Quality stays a feasibility grade so a bad three never gains an excuse.
  const value=aiReadClamp(quality+(points===3&&inRange&&quality>.7&&space>2.05?.065:0));
  return {quality,value,points,distance,closeFinish,finishOpen,space,inRange,preferredRange,momentum,rimPressure,pressure,running};
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
  let clearance=8,finishSpace=8,immediateBlocked=false;
  for(const defender of aiReadOpponents(char)){
    const current=aiReadPosition(char,defender,settings);
    const firstProjection=aiReadSegment(current,start,waypoint);
    const future=aiReadPosition(char,defender,settings,first*firstProjection.t/speed);
    const one=aiReadSegment(future,start,waypoint);
    const twoProjection=aiReadSegment(current,waypoint,finish);
    const help=aiReadPosition(char,defender,settings,(first+second*twoProjection.t)/speed);
    const two=aiReadSegment(help,waypoint,finish);
    const body=(char.bodyRadius || .55)+(defender.bodyRadius || .55);
    // A defender behind the departure direction does not close the lane ahead.
    const movingAway=one.t<.06&&aiReadDistance(waypoint,future)>aiReadDistance(start,future)+.25;
    if(!movingAway) clearance=Math.min(clearance,one.distance-body);
    clearance=Math.min(clearance,two.distance-body);
    finishSpace=Math.min(finishSpace,aiReadDistance(help,finish));
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
  const pathOpen=aiReadClamp(clearance/1.45);
  const finishOpen=aiReadClamp((finishSpace-1.1)/1.55);
  const openness=immediateBlocked?0:pathOpen*(.28+.72*finishOpen);
  return {openness,finishSpace,clearance,blocked:immediateBlocked};
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
    let point=aiReadPosition(passer,defender,settings);
    let projection=aiReadSegment(point,start,target);
    point=aiReadPosition(passer,defender,settings,duration*projection.t);
    projection=aiReadSegment(point,start,target);
    // Include both ends: a guarded receiver or close reach is still an unsafe pass.
    clearance=Math.min(clearance,projection.distance);
    catchSpace=Math.min(catchSpace,aiReadDistance(aiReadPosition(passer,defender,settings,duration),target));
  }
  const catchRead=aiReadShotAt(receiver,passer,target,settings,duration);
  const rimFeed=catchRead.distance<3.45;
  const safe=separation>1.2&&separation<15&&clearance>.82&&(!rimFeed||catchSpace>1.55&&catchRead.rimPressure<.5);
  const laneValue=aiReadClamp((clearance-.5)/2.2)*.12;
  const finishBonus=rimFeed&&catchRead.finishOpen?.14:0;
  const longPassPenalty=Math.max(0,separation-9)*.025;
  let value=catchRead.value+laneValue+finishBonus-longPassPenalty;
  if(!safe) value-=.5;
  return {value:aiReadClamp(value),safe,clearance,rimFeed,target,catchQuality:catchRead.quality,catchSpace,separation,
    catchValue:catchRead.value,points:catchRead.points,finishOpen:catchRead.finishOpen};
}
