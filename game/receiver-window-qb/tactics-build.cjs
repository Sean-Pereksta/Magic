'use strict';
const fs=require('node:fs'),path=require('node:path');
const dir=__dirname,file=path.join(dir,'game.js');
let source=fs.readFileSync(file,'utf8');
function replace(old,next){if(!source.includes(old))throw Error('Integration anchor missing: '+old.slice(0,150));source=source.replace(old,next);}
function section(from,to,text){const a=source.indexOf(from),b=source.indexOf(to,a);if(a<0||b<0)throw Error('Section missing: '+from);source=source.slice(0,a)+text+'\n'+source.slice(b);}
replace('const P=QBProgression,V=QBVariety,F=QBFranchise,R=globalThis.QBPresentation;','const P=QBProgression,V=QBVariety,F=QBFranchise,R=globalThis.QBPresentation,T=V.tactics;');
replace('function setupPlay(increment=false){',String.raw`// Team audibles retain the actors and coverage. Only observed movement informs defense.
let queuedSnap=false,audibleQuickSnap=false;
function sampleAlignment(now){
  for(const r of receivers)if(!r.history.length||now-r.history[r.history.length-1].t>=30){
    const sample=r.history.length>=32?r.history.shift():{p:new THREE.Vector3(),v:new THREE.Vector3()};
    sample.t=now;sample.p.copy(r.mesh.position);sample.v.copy(r.velocity);r.history.push(sample);
  }
}
function watchFormation(){
  sampleAlignment(gameTime);
  for(const d of defenders){
    d.homeSet=d.homeSet||{x:d.zoneX,z:d.mesh.position.z};
    d.setLandmark=d.setLandmark||d.mesh.position.clone();
    d.setRead=T.reaction({now:gameTime,reaction:d.reaction,discipline:V.identity(activeRound()).discipline??.5,random:Math.random(),previous:d.setRead});
  }
}
function callPlay(index){
  if(inputBlocked()||playState!=='call'||queuedSnap||!Number.isInteger(index)||!plays[index])return false;
  if(!receivers.length){selectedPlay=index;setupPlay(false);return true;}
  const targets=T.formation(plays[index],worldZForYards(ballSpotYards));
  const changed=targets.some((p,i)=>Math.hypot(p.x-receivers[i].start.x,p.z-receivers[i].start.z)>.05);
  if(changed)watchFormation();
  selectedPlay=index;playCategory=V.category(plays[index]);formationMotion=null;audibleQuickSnap=true;closeAudible(true);
  targets.forEach((p,i)=>{
    const r=receivers[i];r.start.set(p.x,0,p.z);r.setTarget=r.mesh.position.distanceTo(r.start)>.05?r.start.clone():null;
    r.route=p.route;r.path=pathFor(r.route,r.start);r.distance=0;r.screenTarget=p.screenTarget;
    r.optionRead=!V.isOption(r.route);r.screenOutlet=null;r.audibled=true;
  });
  drawRouteVisuals();renderRoutes();updatePlayButtons();checkpoint();
  showMessage('TEAM AUDIBLE',plays[index].name+' — players move to the set; defense must read and adjust.',1200);return true;
}
function updateFormationShift(dt,now){
  for(const r of receivers){
    if(r.setTarget){
      const done=T.advance(r,r.setTarget,dt,Math.min(6,r.maxSpeed*.8));
      r.mesh.rotation.y=Math.atan2(r.heading.x,r.heading.z);
      r.runIntensity=Math.min(1,r.velocity.length()/5);r.runPhase+=dt*r.velocity.length()*2.3;
      if(done){r.setTarget=null;r.path=pathFor(r.route,r.start);r.distance=0;r.heading.set(0,0,-1);r.mesh.rotation.y=Math.PI;r.velocity.set(0,0,0);r.runIntensity=0;}
      animatePlayerContact(r,dt);
    }
  }
  sampleAlignment(now);
  if(queuedSnap&&!receivers.some(r=>r.setTarget))beginCountdown();
}
function updatePreSnapDefense(dt,now){
  for(const d of defenders){
    const read=d.setRead;if(!read)continue;
    if(now>=read.readyAt&&now>=read.nextReadAt){
      const observed=receivers.map(r=>observedReceiver(r,d.reaction*1000,now).p);
      const p=T.defensiveLandmark({kind:d.kind,mode:currentDefense.corner,target:d.target??0,zoneX:d.homeSet.x,homeZ:d.homeSet.z,observed,error:read.error});
      d.setLandmark.set(p.x,0,Math.max(ENDZONE_BACK_Z+1,p.z));read.nextReadAt=now+180+d.reaction*450;
    }
    if(now<read.readyAt)continue;
    T.advance(d,d.setLandmark,dt,Math.min(5.8,d.maxSpeed*.68));
    d.mesh.rotation.y=Math.atan2(d.heading.x,d.heading.z);d.runIntensity=Math.min(1,d.velocity.length()/5);d.runPhase+=dt*d.velocity.length()*2.3;
    if(d.kind==='corner'&&['zone','deepzone'].includes(currentDefense.corner))d.zoneX=d.mesh.position.x;
    animatePlayerContact(d,dt);
  }
}
function setupPlay(increment=false){
  queuedSnap=false;audibleQuickSnap=false;`);
replace("if(inputBlocked()||playState!=='call')return;playLog=F.newPlay(plays[selectedPlay].name);",String.raw`if(inputBlocked()||playState!=='call')return;
  if(receivers.some(r=>r.setTarget)){
    queuedSnap=true;closeAudible(true);$('playCallPanel').style.display='none';$('snapBtn').disabled=true;
    showMessage('GETTING SET','Start queued — the offense finishes moving before the snap.',900);return;
  }
  queuedSnap=false;
  if(V.motions(plays[selectedPlay],formationMotion).length)watchFormation();
  playLog=F.newPlay(plays[selectedPlay].name);`);
replace("playState='countdown';motionDuration=2200;","playState='countdown';motionDuration=audibleQuickSnap?450:2200;audibleQuickSnap=false;");
replace("if(playState!=='call'||!receivers[index])return;audibleReceiverIndex=index;","if(playState!=='call'||queuedSnap||!receivers[index])return;audibleReceiverIndex=index;");
replace("if(playState!=='call'||audibleReceiverIndex==null||!routePool.includes(route))return;","if(playState!=='call'||queuedSnap||audibleReceiverIndex==null||!routePool.includes(route))return;");
replace("if(inputBlocked()||playState!=='call'||audibleReceiverIndex==null)return;","if(inputBlocked()||playState!=='call'||queuedSnap||audibleReceiverIndex==null)return;");
replace('r.start.x=next;r.mesh.position.x=next;r.path=pathFor(r.route,r.start);r.distance=0;r.history=[];', 'watchFormation();r.start.x=next;r.setTarget=r.start.clone();r.path=pathFor(r.route,r.start);r.distance=0;');
replace("if(playState!=='call')return;selectedPlay=i;setupPlay(false)","if(playState!=='call')return;callPlay(i)");
replace('selectedPlay=Number(e.code.slice(-1))-1;playCategory=V.category(plays[selectedPlay]);setupPlay(false);return','callPlay(Number(e.code.slice(-1))-1);return');
replace("if(e.code==='KeyR'&&playState==='call'){e.preventDefault();setupPlay(false)}","if(e.code==='KeyR'&&playState==='call'){e.preventDefault();callPlay(selectedPlay)}");
replace("choosePlay:i=>{if(receivers.length&&playState!=='call')return;selectedPlay=i;playCategory=V.category(plays[i]);setupPlay(false);previewPlayIndex=null;}","choosePlay:i=>{if(receivers.length&&playState!=='call')return;if(receivers.length)callPlay(i);else{selectedPlay=i;playCategory=V.category(plays[i]);setupPlay(false);}previewPlayIndex=null;}");
section('// Blocks require actual front/side contact. Each defender gets a recovery window.','function updateRunAfterCatch',String.raw`function releaseBlock(e){
  if(!e)return;const b=e.blocker,d=e.defender;
  if(b.blockEngagement===e){b.blockEngagement=null;b.blockPose=0;b.blockCooldown=Math.max(b.blockCooldown||0,.55);}
  if(d.blockEngagement===e){d.blockEngagement=null;d.blockTime=0;d.blockSlow=1;d.blockCooldown=Math.max(d.blockCooldown||0,.85);}
}
function faceBlock(a,opponent,dt){
  const dx=opponent.mesh.position.x-a.mesh.position.x,dz=opponent.mesh.position.z-a.mesh.position.z;
  const face=Math.atan2(dx,dz),delta=Math.atan2(Math.sin(face-a.mesh.rotation.y),Math.cos(face-a.mesh.rotation.y));
  a.mesh.rotation.y+=THREE.MathUtils.clamp(delta,-12*dt,12*dt);
}
function updateBlocking(dt,screenReceiver=null){
  const runner=ballCarrier||screenReceiver;if(!runner)return;
  const carrier=runner.mesh.position,claimed=new Set();
  for(const d of defenders){d.blockCooldown=Math.max(0,(d.blockCooldown||0)-dt);if(!d.blockEngagement)d.blockTime=0;}
  for(const b of receivers){
    b.blockCooldown=Math.max(0,(b.blockCooldown||0)-dt);
    if(b.blockEngagement){
      const e=b.blockEngagement,d=e.defender;
      if(b===runner||b.trackingBall||b.mesh.position.distanceTo(d.mesh.position)>2.05||b.mesh.position.distanceTo(carrier)>18||d.diveTime>0)releaseBlock(e);
      else claimed.add(d);
    }
  }
  const blockers=receivers.filter(b=>b!==runner).sort((a,b)=>(b.style==='Blocking Receiver')-(a.style==='Blocking Receiver')||b.profile.strength-a.profile.strength);
  for(const b of blockers){
    if(b.blockEngagement)continue;
    b.blockPose=0;b.blockAim=null;
    if(!ballCarrier&&b.route!=='Lead'){b.blockTarget=null;continue;}
    const pos=b.mesh.position;if(pos.distanceTo(carrier)>18){b.blockTarget=null;continue;}
    const useful=d=>!claimed.has(d)&&!d.blockEngagement&&d.mesh.position.distanceTo(carrier)<16&&d.mesh.position.z<carrier.z+2&&d.diveRecovery<=0;
    const score=d=>pos.distanceTo(d.mesh.position)*.65+carrier.distanceTo(d.mesh.position)*.35+Math.abs(d.mesh.position.x-carrier.x)*.24-(b.blockTarget===d?1.6:0);
    const d=defenders.filter(useful).sort((a,c)=>score(a)-score(c))[0];
    if(!d){b.blockTarget=null;if(runner.screenTarget)b.blockAim=new THREE.Vector3(THREE.MathUtils.clamp(carrier.x+(b.start.x<runner.start.x?-3:3),-23,23),0,Math.max(ENDZONE_BACK_Z+1,carrier.z-5));continue;}
    claimed.add(d);b.blockTarget=d;
    const toRunner=carrier.clone().sub(d.mesh.position).setY(0).normalize();
    b.blockAim=d.mesh.position.clone().addScaledVector(d.velocity,.08+F.awareness(b.profile)*.0008).addScaledVector(toRunner,.95);
    b.blockAim.x=THREE.MathUtils.clamp(b.blockAim.x,-24.5,24.5);
    const toward=d.mesh.position.clone().sub(pos).setY(0),dist=toward.length();toward.normalize();
    if(dist<2.4)faceBlock(b,d,dt);
    const facing=Math.sin(b.mesh.rotation.y)*toward.x+Math.cos(b.mesh.rotation.y)*toward.z;
    if(dist>1.5||dist<.05||b.blockCooldown>0||d.blockCooldown>0||d.diveTime>0||b.jumpY>.1||d.jumpY>.1||facing<.35||b.trackingBall)continue;
    const leverage=pos.clone().sub(d.mesh.position).setY(0).normalize().dot(toRunner);
    if(leverage<-.1)continue;
    const contest=T.blockContest({strength:P.effective(b.profile.strength)+P.traits(b.profile).bodyBonus*100,defenseStrength:d.strength,alignment:leverage+(b.style==='Blocking Receiver'?.15:0),technique:d.technique});
    const e={...T.engagement(contest,Math.random()),blocker:b,defender:d};
    b.blockEngagement=e;d.blockEngagement=e;b.blockPrevious=pos.clone();
    b.blockPose=contest.duration;d.blockTime=contest.duration;d.blockSlow=.15;
    b.velocity.copy(toward).multiplyScalar(contest.driveSpeed);d.velocity.copy(b.velocity);
    b.impactVel.multiplyScalar(.1);d.impactVel.multiplyScalar(.1);F.event(playLog,b.profile,'blocks');
  }
}
function advanceBlockEngagements(dt){
  const runner=ballCarrier||receivers.find(r=>r.screenTarget);
  for(const b of receivers){
    const e=b.blockEngagement;if(!e||e.blocker!==b)continue;const d=e.defender;
    const normal=d.mesh.position.clone().sub(b.mesh.position).setY(0),gap=normal.length();normal.normalize();
    const toRunner=runner?runner.mesh.position.clone().sub(d.mesh.position).setY(0).normalize():normal.clone().negate();
    const leverage=-normal.dot(toRunner);
    if(gap>2.05||gap<.01||leverage<-.25||T.advanceEngagement(e,dt,leverage)){releaseBlock(e);continue;}
    b.blockPrevious=b.blockPrevious||new THREE.Vector3();b.blockPrevious.copy(b.mesh.position);
    // The pair translates together: a weak defender cannot shove through a strong blocker.
    const velocity=normal.clone().multiplyScalar(e.driveSpeed),dx=velocity.x*dt,dz=velocity.z*dt;
    const safeX=THREE.MathUtils.clamp(dx,-24.9-Math.min(b.mesh.position.x,d.mesh.position.x),24.9-Math.max(b.mesh.position.x,d.mesh.position.x));
    b.mesh.position.x+=safeX;d.mesh.position.x+=safeX;b.mesh.position.z+=dz;d.mesh.position.z+=dz;
    b.velocity.copy(velocity);d.velocity.copy(velocity);b.impactVel.multiplyScalar(Math.pow(.01,dt));d.impactVel.multiplyScalar(Math.pow(.01,dt));
    b.heading.copy(normal);d.heading.copy(normal).negate();faceBlock(b,d,dt);faceBlock(d,b,dt);
    b.blockPose=Math.max(.01,e.duration-e.elapsed);d.blockTime=b.blockPose;
    for(const a of [b,d]){a.runIntensity=Math.min(.65,Math.abs(e.driveSpeed)/3+.12);a.runPhase+=dt*(4+Math.abs(e.driveSpeed)*2);updateJump(a,dt,false);animatePlayerContact(a,dt);}
  }
}
function tackleRadiusFor(d,r){
  return T.tackleRadius({defenderSize:d.mesh.scale.x,runnerSize:r.mesh.scale.x,technique:d.technique,evasion:P.effective(r.profile.evasion)/100,fooled:d.fakeUntil>gameTime,diving:!!(d.divingThisStep||d.diveTime>0),blockReach:d.blockEngagement?.reach??1});
}
`);
replace('function updateReceiver(r,dt,now){\n  readOptionRoute(r);',String.raw`function updateReceiver(r,dt,now){
  if(r.blockEngagement){
    const approach=ballLive?ballApproach(r.mesh.position.clone().add(new THREE.Vector3(0,1.6,0)),.32):null;
    if(approach&&approach.time<.32&&approach.dist<1.5)releaseBlock(r.blockEngagement);else return;
  }
  readOptionRoute(r);`);
replace('d.catchPose=Math.max(0,(d.catchPose||0)-dt*5);\n  if(d.diveTime>0){','d.catchPose=Math.max(0,(d.catchPose||0)-dt*5);\n  if(d.blockEngagement)return;\n  if(d.diveTime>0){');
replace('function animatePlayerContact(a,dt){',String.raw`function animatePlayerContact(a,dt){
  if(a.profile&&!a.hasBall&&!a.trackingBall&&a.blockTarget&&a.blockAim&&a.mesh.position.distanceTo(a.blockTarget.mesh.position)<2.4)faceBlock(a,a.blockTarget,dt);`);
replace('const A=all[a],B=all[b];if(', 'const A=all[a],B=all[b];if(A.blockEngagement&&A.blockEngagement===B.blockEngagement)continue;if(');
replace("for(const r of receivers)for(const d of defenders){if(d.kind!=='corner')continue;", "for(const r of receivers)for(const d of defenders){if(d.kind!=='corner'||r.blockEngagement||d.blockEngagement)continue;");
replace('function startDivingTackle(d,r){\n  if(', 'function startDivingTackle(d,r){\n  if(d.blockEngagement)return false;\n  if(');
replace('  const closeBody=.40*(d.mesh.scale.x+r.mesh.scale.x);\n  const radius=d.fakeUntil>gameTime?closeBody:closeBody+.36+.14*d.technique-.06*Math.min(1,P.effective(r.profile.evasion)/100)+(d.divingThisStep?.28:0);','  const radius=tackleRadiusFor(d,r);');
replace('if(sweep.distance>radius)return null;\n  // A hurdle',String.raw`if(sweep.distance>radius)return null;
  if(d.blockEngagement){
    const b=d.blockEngagement.blocker,t=sweep.entry;
    const defender=start.clone().lerp(d.mesh.position,t),runner=runnerStart.clone().lerp(r.mesh.position,t);
    const blocker=(b.blockPrevious||b.mesh.position).clone().lerp(b.mesh.position,t);
    if(T.shielded(defender,runner,blocker,.40*b.mesh.scale.x+.25))return null;
  }
  // A hurdle`);
replace('    updateDefender(d,defenderTarget(d,now),dt);\n  }\n  attachBallToCarrier();','    updateDefender(d,defenderTarget(d,now),dt);\n  }\n  advanceBlockEngagements(dt);\n  attachBallToCarrier();');
replace('defenders.forEach(d=>updateDefender(d,defenderTarget(d,now),dt));solvePlayerCollisions(dt)', 'defenders.forEach(d=>updateDefender(d,defenderTarget(d,now),dt));advanceBlockEngagements(dt);solvePlayerCollisions(dt)');
replace('if(m.freeDive&&!m.down){\n    let hit=null;',String.raw`if(m.freeDive&&!m.down){
    updateBlocking(dt);for(const b of receivers)if(b!==r)updateReceiver(b,dt,gameTime);advanceBlockEngagements(dt);
    let hit=null;`);
replace('blocked:d.blockTime>0}));', 'blocked:!!d.blockEngagement,tackleRadius:tackleRadiusFor(d,r),diving:d.diveTime>0||d.divingThisStep}));');
replace('target:a.blockTarget,engaged:a.blockPose>0}));','target:a.blockTarget,engaged:!!a.blockEngagement,vx:a.velocity.x,vz:a.velocity.z,radius:.4*a.mesh.scale.x+.25}));');
replace('const lane=V.lane({x:pos.x,z:pos.z,bestZ:r.bestRunZ,', 'const laneOptions={x:pos.x,z:pos.z,speed:r.maxSpeed*(screenBoost?1.22:1),vx:r.velocity.x,vz:r.velocity.z,bestZ:r.bestRunZ,');
replace('previous:r.runLane,defenders:threats});\n  r.runLane=', 'previous:r.runLane,defenders:threats};\n  const lane=T.safeLane(laneOptions,V.lane(laneOptions));\n  r.runLane=');
replace('function defenderTarget(d,now){','function defenderTarget(d,now){\n  if(d.setRead&&now<d.setRead.readyAt&&d.setLandmark)return d.setLandmark.clone();');
replace("  if(playState==='countdown'){\n    const progress=", "  if(playState==='call'){updateFormationShift(dt,now);updatePreSnapDefense(dt,now);}\n  if(playState==='countdown'){\n    const progress=");
section('      // Man corners shade observed motion at a bounded speed; zone shells hold their landmarks.','      if(progress===1)', '      // Coverage adjustments below use delayed observations and bounded acceleration.');
replace('    const left=Math.max(0,snapTime-now),count=Math.ceil(left/1000);','    sampleAlignment(now);updatePreSnapDefense(dt,now);\n    const left=Math.max(0,snapTime-now),count=Math.ceil(left/1000);');
fs.writeFileSync(file,source);
const variety=path.join(dir,'variety.js');let v=fs.readFileSync(variety,'utf8');
const anchor="  if(typeof module!=='undefined')module.exports=api;root.QBVariety=api;";
if(!v.includes(anchor))throw Error('Variety export changed');
v=v.replace(anchor,"  api.tactics=root.QBTactics||(typeof require==='function'?require('./tactics.js'):null);\n"+anchor);fs.writeFileSync(variety,v);
const html=path.join(dir,'../receiver-window-qb.html');let page=fs.readFileSync(html,'utf8');
const scripts=/<script\b[^>]*src=["']receiver-window-qb\/variety\.js(?:\?[^"']*)?["'][^>]*><\/script>/;
if(!scripts.test(page))throw Error('Variety script tag not found');
page=page.replace(scripts,m=>'<script src="receiver-window-qb/tactics.js"></script>\n'+m);fs.writeFileSync(html,page);
const tests=path.join(dir,'gameplay-variety.test.cjs');
fs.appendFileSync(tests,String.raw`
// Whole-game coverage for physical audibles, linked blocks and tackle shielding.
test('team audibles preserve actors, ratings and coverage while changing full formations',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.5;
  const before=q.state(),rs=[...before.receivers],ds=[...before.defenders],positions=ds.map(d=>d.mesh.position.clone());
  q.run("callPlay(plays.findIndex(p=>p.screen!=null&&p.xs))");
  const s=q.state();assert.equal(s.currentDefense,before.currentDefense);
  rs.forEach((r,i)=>assert.equal(r,s.receivers[i]));ds.forEach((d,i)=>{assert.equal(d,s.defenders[i]);assert.ok(d.mesh.position.distanceTo(positions[i])<1e-9);});
  assert.equal(s.receivers.filter(r=>r.screenTarget).length,1);assert.ok(s.receivers.some(r=>r.setTarget));
  assert.ok(s.defenders.every(d=>d.setRead.readyAt>0));
});
test('queued start waits for physical formation movement and keeps the play menu closed',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.5;
  q.run("callPlay(plays.findIndex(p=>p.name==='Bunch Read Screen'))");q.beginCountdown();
  assert.equal(q.run('queuedSnap'),true);assert.equal(q.run("$('playCallPanel').style.display"),'none');
  let moved=0;
  for(let i=0;i<1800&&q.state().playState==='call';i++){
    const before=q.state().receivers.map(r=>r.mesh.position.clone());q.run('gameTime+=1000/120');q.update(1/120,q.run('gameTime'));
    q.state().receivers.forEach((r,k)=>{const delta=r.mesh.position.distanceTo(before[k]);assert.ok(delta<.16,'no teleport while changing sets');moved+=delta;});
  }
  assert.ok(moved>10);assert.equal(q.state().playState,'countdown');assert.equal(q.run('queuedSnap'),false);
});
test('pending defensive formation reads survive repeated audibles without being restarted',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.5;
  q.run("callPlay(plays.findIndex(p=>p.name==='Bunch Read Screen'))");
  const deadlines=q.state().defenders.map(d=>d.setRead.readyAt);q.run('gameTime+=100;callPlay(0)');
  q.state().defenders.forEach((d,i)=>assert.equal(d.setRead.readyAt,deadlines[i]));
});
function arrangeTacticalBlock(q,strength=95,defenseStrength=40){
  q.run("setupPlay();playState='run';ballCarrier=receivers[0];receivers[0].hasBall=true;receivers[0].mesh.position.set(0,0,3);receivers[0].velocity.set(0,0,-4);for(const a of [...receivers.slice(1),...defenders])a.mesh.position.set(50,0,30);");
  const b=q.state().receivers[1],d=q.state().defenders[0];
  b.mesh.position.set(0,0,0);d.mesh.position.set(0,0,-1.2);b.heading.set(0,0,-1);b.mesh.rotation.y=Math.PI;d.heading.set(0,0,1);d.mesh.rotation.y=0;
  b.velocity.set(0,0,0);d.velocity.set(0,0,0);b.profile.strength=strength;d.strength=defenseStrength;b.blockCooldown=0;d.blockCooldown=0;
  q.run('updateBlocking(1/120)');return {b,d,r:q.state().receivers[0]};
}
test('linked blocking is sustained, faces the opponent and drives according to relative strength',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999;
  let {b,d}=arrangeTacticalBlock(q);assert.ok(b.blockEngagement);assert.equal(b.blockEngagement,d.blockEngagement);
  const before=b.mesh.position.z;for(let i=0;i<30;i++)q.run('advanceBlockEngagements(1/120)');
  assert.ok(b.mesh.position.z<before);assert.ok(d.mesh.position.z<b.mesh.position.z);assert.ok(Math.cos(b.mesh.rotation.y)<-.8);
  ({b,d}=arrangeTacticalBlock(q,30,100));const next=b.mesh.position.z;
  for(let i=0;i<30;i++)q.run('advanceBlockEngagements(1/120)');assert.ok(b.mesh.position.z>next);
});
test('a held block protects the carrier behind it but never hides an exposed support tackler',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999;const {b,d,r}=arrangeTacticalBlock(q);
  b.mesh.position.set(0,0,0);b.blockPrevious=b.mesh.position.clone();d.mesh.position.set(0,0,-.55);r.mesh.position.set(0,0,.55);
  d.tacklePrevious=d.mesh.position.clone();r.tacklePrevious=r.mesh.position.clone();
  assert.equal(q.tackleContact(d,r),null);
  const support=q.state().defenders[1];support.mesh.position.set(.55,0,.55);support.tacklePrevious=support.mesh.position.clone();
  assert.ok(q.tackleContact(support,r));
  q.run('releaseBlock(receivers[1].blockEngagement)');assert.ok(q.tackleContact(d,r));
});
test('held defenders cannot dive or obtain a generic random shove through the blocker',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999;const {b,d,r}=arrangeTacticalBlock(q);
  d.velocity.set(0,0,8);assert.equal(q.startDivingTackle(d,r),false);
  const before=b.mesh.position.clone();c.Math.random=()=>0;q.run('solvePlayerCollisions(1/120)');
  assert.ok(b.mesh.position.distanceTo(before)<1e-9);assert.ok(b.blockEngagement);
});
test('block shedding releases both actors and enforces a recovery window',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.5;const {b,d}=arrangeTacticalBlock(q,30,100);
  for(let i=0;i<600&&b.blockEngagement;i++)q.run('advanceBlockEngagements(1/120)');
  assert.equal(b.blockEngagement,null);assert.equal(d.blockEngagement,null);assert.ok(d.blockCooldown>0);assert.equal(d.blockTime,0);
});
`);
console.log('Integrated physical team audibles, strength blocking and safe lanes.');
