/* Field adapter: every mode runs the game's same movement, ball and glove sweeps. */
(function(root){
'use strict';
function create(ctx){
  const M=root.QBModes,D=root.QBDevelopment,T=ctx.THREE;
  let source=null,profile=null,autoElapsed=0,releaseAt=0,lastContact=null,releaseMetrics={},keys=new Set();
  function saved(){const f=ctx.state().franchise;if(source!==f){source=f;profile=M.normalizeProfile(f.modes);M.recoverInterrupted?.(profile);f.modes=profile;}return profile;}
  function active(){return !!ctx.state().franchise.selectedMode&&!!saved().active;}
  function config(){return active()?M.setup(saved().active):null;}
  function summary(){return M.summary(saved());}
  function dispatch(event){
    const result=M.dispatch(saved(),event),f=ctx.state().franchise;
    for(const command of result.commands){
      if(command.type==='award')f.cash+=command.cash;
      if(command.type==='academy_result')D?.awardAcademy(f,{challengeId:command.challengeId,score:command.score,playerId:saved().active.traineeId||f.team[0].id});
      if(command.type==='force_end')endPlay(command.outcome,null,{yards:command.yards});
      if(command.type==='result')ctx.result(command.result,command.terminal);
    }
    if(saved().active?.mode==='career'&&D&&f.development&&saved().career.year===f.development.season+1)D.beginSeason(f,saved().career.year);
    if(event.type!=='tick')ctx.save();return result;
  }
  function start(id,options={}){
    if(!['call','dead','manager'].includes(ctx.state().playState))return false;
    const f=ctx.state().franchise;if(!f.selectedMode)f.campaignCheckpoint=JSON.parse(JSON.stringify(f.checkpoint||null));
    M.start(saved(),id,{seed:Date.now()%0xffffffff,dayKey:new Date().toISOString().slice(0,10),teamName:f.identity.name,...options});f.selectedMode=id;
    if(options.playerId&&D?.findPlayer(f,options.playerId))saved().active.traineeId=options.playerId;
    ctx.begin();ctx.applySetup(config());ctx.save();
  }
  function exit(){
    const f=ctx.state().franchise;delete f.selectedMode;if(Object.prototype.hasOwnProperty.call(f,'campaignCheckpoint')){f.checkpoint=f.campaignCheckpoint;delete f.campaignCheckpoint;}
    ctx.exit();ctx.save();
  }
  function next(){const r=dispatch({type:'continue'});if(r.commands.some(c=>c.type==='setup')){ctx.begin();ctx.applySetup(config());}return r;}
  function setup(){
    autoElapsed=0;releaseAt=0;lastContact=null;releaseMetrics={};keys.clear();
    const c=config(),pad=document.getElementById('roleControls');if(pad)pad.hidden=!c||c.controlledRole==='qb';return c;
  }
  function snap(){if(!active())return;dispatch({type:'snap'});autoElapsed=0;releaseAt=.85+(saved().active.seed%55)/100;}
  function targetPlan(){
    const s=ctx.state(),c=config(),r=s.receivers[0];if(!c||!r||!root.QBFootball||!ctx.snapshot)return null;
    const origin=c.autoQB?passOrigin():s.camera.position,velocity=ctx.peekLaunch?.(),speed=velocity?.length()||32;
    const flightTime=Math.max(.25,Math.min(2.5,r.mesh.position.distanceTo(origin)/speed));
    const receiver=ctx.snapshot(r),defenders=s.defenders.map(d=>({...ctx.snapshot(d),visible:true}));
    const target=root.QBFootball.placementTarget({mode:c.targetZone||'in_stride',origin,receiver,defenders,flightTime,amount:.5});
    return {origin,velocity,flightTime,receiver,defenders,target};
  }
  function release(){if(!active())return;dispatch({type:'release'});releaseMetrics.releaseTime=saved().active.playElapsed;
    const plan=targetPlan();if(plan){releaseMetrics.target=plan.target;releaseMetrics.flightTime=plan.flightTime;releaseMetrics.window=root.QBFootball.passingWindow({...plan,target:plan.target}).label;}
  }
  function contact(actor,hand){
    if(!active())return;const defense=!actor.profile;
    lastContact={defense,actor,hand:!!hand,technique:actor.passTechnique?.technique||actor.catchStyle||'hands'};
    if(!defense&&hand&&releaseMetrics.target){const p=ctx.state().ball.position,q=releaseMetrics.target;releaseMetrics.placementError=Math.hypot(p.x-q.x,p.y-q.y,p.z-q.z)/1.20745;releaseMetrics.timingError=Math.abs(saved().active.playElapsed-(releaseMetrics.releaseTime||0)-(releaseMetrics.flightTime||0));}
    dispatch({type:'ball_contact',side:defense?'defense':'offense',physicalContact:true,technique:lastContact.technique});
  }
  function endPlay(outcome,spot,extra={}){
    if(!active())return false;
    const s=ctx.state(),session=saved().active;if(session.phase!=='live')return true;
    const c=config(),receiver=lastContact&&!lastContact.defense?lastContact.actor:null;
    const gain=spot==null?0:spot-s.snapSpotYards;
    const target=receiver?.mesh.position;
    const separation=receiver?Math.min(99,...s.defenders.map(d=>d.mesh.position.distanceTo(receiver.mesh.position))):0;
    const routePoint=receiver?ctx.routePoint(receiver):null;
    const routeError=target&&routePoint?target.distanceTo(routePoint)/2:5;
    const technique=receiver?.catchStyle==='HIGH POINT'?'high_point':receiver?.catchStyle==='TOE TAP'?'toe_tap':receiver?.catchStyle==='BACK SHOULDER'?'back_shoulder':receiver?.catchOneHand?'one_hand':lastContact?.technique;
    const result=dispatch({type:'play_end',id:session.id+':'+session.playNumber,outcome,yards:gain,
      physicalContact:outcome==='interception'?!!lastContact?.defense&&lastContact.hand:!!receiver||!!lastContact?.defense&&lastContact.hand,
      passBreakup:!!lastContact?.defense,tip:!!lastContact?.defense,technique,separation,routeError,
      placementError:releaseMetrics.placementError??2,timingError:releaseMetrics.timingError??.5,
      window:releaseMetrics.window||'tight',releaseTime:releaseMetrics.releaseTime,
      outOfBounds:!!extra.outOfBounds,...extra});
    ctx.stop();
    if(result.commands.some(command=>command.type==='setup'))ctx.schedule(()=>ctx.applySetup(config()),850);
    ctx.save();return true;
  }
  function actor(){const s=ctx.state(),c=config();return !c?null:c.controlledRole==='receiver'?s.receivers[c.controlledSlot]:c.controlledRole==='defender'?s.defenders[c.controlledSlot]:null;}
  function inputDirection(){return new T.Vector3((keys.has('ArrowRight')?1:0)-(keys.has('ArrowLeft')?1:0),0,(keys.has('ArrowDown')?1:0)-(keys.has('ArrowUp')?1:0)).normalize();}
  function moveReceiver(r,dt,now){
    if(r!==actor()||config()?.controlledRole!=='receiver'||r.hasBall)return false;
    const input=inputDirection(),desired=input.multiplyScalar(r.maxSpeed),change=desired.sub(r.velocity),max=22*dt;
    if(change.length()>max)change.setLength(max);r.velocity.add(change);r.mesh.position.addScaledVector(r.velocity,dt).addScaledVector(r.impactVel,dt);
    r.mesh.position.x=T.MathUtils.clamp(r.mesh.position.x,-25.5,25.5);r.mesh.position.z=T.MathUtils.clamp(r.mesh.position.z,-69,53);
    if(r.velocity.length()>.1){r.heading.copy(r.velocity).normalize();r.mesh.rotation.y=Math.atan2(r.heading.x,r.heading.z);}
    r.distance+=r.velocity.length()*dt;r.trackingBall=ctx.state().ballLive;r.runPhase+=dt*r.velocity.length()*2.5;r.runIntensity=Math.min(1,r.velocity.length()/r.maxSpeed);
    ctx.jump(r,dt);ctx.animate(r,dt);
    if(!r.history.length||now-r.history.at(-1).t>=30){r.history.push({t:now,p:r.mesh.position.clone(),v:r.velocity.clone()});if(r.history.length>32)r.history.shift();}
    return true;
  }
  function defenderTarget(d,target){return d===actor()&&config()?.controlledRole==='defender'?d.mesh.position.clone().addScaledVector(inputDirection(),6):target;}
  function autoLaunch(){
    const c=config(),s=ctx.state();if(!c?.autoQB)return null;
    const r=c.controlledRole==='receiver'?actor():s.receivers[(saved().active.seed+saved().active.playNumber)%s.receivers.length];if(!r)return null;
    const origin=passOrigin(),time=Math.max(.45,Math.min(1.6,r.mesh.position.distanceTo(origin)/30));
    const point=r.mesh.position.clone().addScaledVector(r.velocity,time);point.y=1.8*r.mesh.scale.y+(c.technique==='high_point'?.55:0);
    if(c.technique==='back_shoulder')point.addScaledVector(r.heading,-.5);
    const ownQB=c.userOnOffense?D?.unitPlayer(s.franchise,'QB'):null;
    const solution=root.QBFootball.throwSolution({origin,target:point,qb:{accuracy:ownQB?.specialties?.accuracy||70+c.difficulty*20,strength:ownQB?.strength||75,footwork:ownQB?.turning||75,releaseTiming:.9},power:.8,type:'touch',random:()=>.5});
    return new T.Vector3(solution.velocity.x,solution.velocity.y,solution.velocity.z);
  }
  function passOrigin(){const s=ctx.state();return new T.Vector3(0,2.25,ctx.worldZ(s.snapSpotYards)+10);}
  function tick(dt){
    if(!active())return;const s=ctx.state(),c=config();if(!['live','thrown','run','tackle','call'].includes(s.playState))return;
    dispatch({type:'tick',seconds:dt});
    if(s.playState==='live'&&s.defenders.some(d=>d.assignment==='rush'&&Math.hypot(d.mesh.position.x,d.mesh.position.z-passOrigin().z)<.8)){endPlay('sack',s.snapSpotYards-5);return;}
    if(c.autoQB&&s.playState==='live'){autoElapsed+=dt;if(autoElapsed>=releaseAt)ctx.throwBall();}
    if(c.controlledRole!=='qb'&&['live','thrown'].includes(s.playState)){
      const a=actor();if(a){const eye=a.mesh.position.clone().add(new T.Vector3(0,6,10));s.camera.position.lerp(eye,Math.min(1,dt*8));s.camera.lookAt(a.mesh.position.clone().add(new T.Vector3(0,1.3,-3)));}
    }
    ctx.hud(c);
    const plan=c.targetZone&&!s.ballLive?targetPlan():null;ctx.drawTarget?.(plan?.target||null);
  }
  function action(){const a=actor();if(!a)return;if(a.profile){if(a.hasBall)ctx.juke(a);else if(a.jumpY<.01){a.jumpVel=ctx.jumpVelocity(a);a.jumpCooldown=.7;}}
    else{a.swatPose=1;if(a.jumpY<.01){a.jumpVel=a.jumpVelocity;a.jumpCooldown=.7;}}}
  function key(event,down){
    if(!down&&['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(event.code))keys.delete(event.code);
    if(!active()||config().controlledRole==='qb'||ctx.blocked())return false;
    if(['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(event.code)){if(down)keys.add(event.code);else keys.delete(event.code);event.preventDefault();return true;}
    if(event.code==='Space'&&ctx.state().playState!=='call'){event.preventDefault();if(down&&!event.repeat)action();return true;}return false;
  }
  const map={up:'ArrowUp',down:'ArrowDown',left:'ArrowLeft',right:'ArrowRight'};
  for(const button of document.querySelectorAll('[data-move]')){
    const code=map[button.dataset.move];button.onpointerdown=e=>{e.preventDefault();button.setPointerCapture?.(e.pointerId);keys.add(code);};
    for(const name of ['onpointerup','onpointercancel','onlostpointercapture'])button[name]=()=>keys.delete(code);
  }
  const actionButton=document.getElementById('roleAction');if(actionButton)actionButton.onclick=action;
  return {get active(){return active();},get profile(){return saved();},config,summary,start,exit,next,dispatch,setup,snap,release,contact,endPlay,moveReceiver,defenderTarget,autoLaunch,passOrigin,tick,key,actor};
}
root.QBModeRuntime={create};
})(typeof globalThis!=='undefined'?globalThis:this);
