/* Connect observed football decisions to the existing swept glove-contact runtime. */
(function(root){
'use strict';
const choices=[['manual','Manual reticle'],['outside_shoulder','Away from defender'],['inside_shoulder','Inside shoulder'],['high_point','High point'],['low_away','Low and away'],['back_shoulder','Back shoulder'],['in_stride','In stride'],['over_shoulder','Over shoulder'],['open_grass','Open grass'],['sideline','Sideline']];
function create(ctx){
  const T=ctx.THREE,F=root.QBFootball;
  let placement='manual',targetSlot=0,assist=false,windowAge=0;
  const panel=document.getElementById('placementPanel');
  if(panel){
    panel.innerHTML='<details><summary>PASS PLACEMENT</summary><label>Target<select id="placementTarget"></select></label><label>Placement<select id="placementStyle">'+choices.map(([id,label])=>`<option value="${id}">${label}</option>`).join('')+'</select></label><label class="placementCheck"><input type="checkbox" id="windowAssist"> Show observed passing window</label><p>Choose a shoulder, then aim and charge. Placement shifts your throw; it never locks onto a receiver. V cycles placement; B cycles target.</p></details><div id="windowRead" role="status" hidden></div>';
    document.getElementById('placementStyle').onchange=e=>placement=e.target.value;
    document.getElementById('placementTarget').onchange=e=>targetSlot=Number(e.target.value)||0;
    document.getElementById('windowAssist').onchange=e=>{assist=!!e.target.checked;try{localStorage.setItem('receiverWindowQB_windowAssist',String(assist));}catch{}};
    try{assist=localStorage.getItem('receiverWindowQB_windowAssist')==='true';document.getElementById('windowAssist').checked=assist;}catch{}
  }
  const vector=v=>({x:v?.x||0,y:v?.y||0,z:v?.z||0});
  function snapshot(a){
    // These dimensions are a broad-phase planning envelope. Only rendered glove
    // and arm sweeps may create a touch or possession in game.js.
    const s=a.mesh.scale;
    return {position:vector(a.mesh.position),velocity:vector(a.velocity),heading:vector(a.heading),
      maxSpeed:a.maxSpeed||a.speed||8,acceleration:a.accel||22,reaction:a.reaction||.2,
      reach:{horizontal:.96*Math.max(s.x,s.z),standingHeight:2.3*s.y,
        jumpHeight:Math.pow(a.jumpVelocity||ctx.jumpVelocity(a),2)/19.62,minHeight:.08},
      handHeight:1.65*s.y,awareness:a.profile?Math.min(100,(a.profile.catching+a.profile.turning)/2):ctx.skill()*100,
      athleticism:a.profile?Math.min(100,a.profile.athleticism):ctx.skill()*100,
      ballSkills:a.profile?Math.min(100,a.profile.catching):55+ctx.skill()*35,
      discipline:ctx.personality().discipline*100||70,aggression:ctx.personality().hands>0?85:50,
      fatigue:a.fatigue||0,strength:a.profile?.strength||a.strength||65};
  }
  function focused(){const s=ctx.state();return s.receivers[Math.min(targetSlot,s.receivers.length-1)]||null;}
  function refreshTargets(){
    const s=ctx.state(),select=document.getElementById('placementTarget');
    if(select){select.innerHTML=s.receivers.map((r,i)=>`<option value="${i}">${r.label}</option>`).join('');select.value=String(Math.min(targetSlot,Math.max(0,s.receivers.length-1)));}
    for(const d of s.defenders){d.passTechnique=null;d.passTechniqueState=null;d.ballObservedFor=0;d.passRecover=0;}
  }
  function cycle(kind){
    if(kind==='target'){targetSlot=(targetSlot+1)%Math.max(1,ctx.state().receivers.length);const select=document.getElementById('placementTarget');if(select)select.value=String(targetSlot);}
    else{placement=choices[(choices.findIndex(x=>x[0]===placement)+1)%choices.length][0];const select=document.getElementById('placementStyle');if(select)select.value=placement;}
  }
  function adjustLaunch(velocity,power){
    const s=ctx.state(),r=focused();if(!F||!r||placement==='manual')return velocity;
    const origin=vector(s.camera.position),receiver=snapshot(r),flightTime=Math.max(.18,Math.min(2.5,r.mesh.position.distanceTo(s.camera.position)/Math.max(18,velocity.length())));
    const direction=new T.Vector3();s.camera.getWorldDirection(direction);
    const observed=s.defenders.filter(d=>direction.dot(d.mesh.position.clone().sub(s.camera.position).normalize())>.58).map(d=>({...snapshot(d),visible:true}));
    const args={origin,receiver,defenders:observed,flightTime,amount:.55,field:{minX:-25.4,maxX:25.4}};
    const center=F.placementTarget({...args,mode:'in_stride'}),point=F.placementTarget({...args,mode:placement});
    // Retain all user aiming error. This is a positional bias of the manual ray,
    // not a correction toward the target's centre or an increased catch radius.
    velocity.x+=(point.x-center.x)/flightTime;velocity.y+=(point.y-center.y)/flightTime;velocity.z+=(point.z-center.z)/flightTime;
    const qb=s.franchise.quarterback||{},strength=Math.max(35,Math.min(100,Number(qb.strength)||75));
    velocity.multiplyScalar(.93+strength*.0009);
    return velocity;
  }
  function updateWindow(dt){
    if(!F)return;windowAge+=dt;if(windowAge<.12)return;windowAge=0;
    const s=ctx.state(),el=document.getElementById('windowRead'),r=focused();if(!el)return;
    el.hidden=!assist||!r||!['call','live'].includes(s.playState);if(el.hidden)return;
    const heading=new T.Vector3();s.camera.getWorldDirection(heading);
    const direction=r.mesh.position.clone().add(new T.Vector3(0,1.6*r.mesh.scale.y,0)).sub(s.camera.position).normalize();
    if(heading.dot(direction)<.72){el.textContent=r.label+' · OUT OF VIEW';el.dataset.window='unknown';return;}
    const flightTime=Math.max(.2,Math.min(2.5,r.mesh.position.distanceTo(s.camera.position)/32));
    const observed=s.defenders.filter(d=>{
      const v=d.mesh.position.clone().sub(s.camera.position).normalize();return heading.dot(v)>.58;
    }).map(d=>({...snapshot(d),visible:true}));
    const receiver=snapshot(r),target=F.placementTarget({mode:placement==='manual'?'in_stride':placement,origin:vector(s.camera.position),receiver,defenders:observed,flightTime});
    const result=F.passingWindow({origin:vector(s.camera.position),target,flightTime,receiver,defenders:observed,qbObservation:{visible:true,defendersVisible:true}});
    const category=result.label;el.dataset.window=category;el.textContent=r.label+' · '+String(category).toUpperCase()+' WINDOW';
  }
  function defense(a,dt){
    if(!F)return false;
    const s=ctx.state();a.ballObservedFor=ctx.seesBall(a)?(a.ballObservedFor||0)+dt:0;
    if(a.passTechniqueState){
      const next=F.advanceTechnique(a.passTechniqueState,dt);a.passTechniqueState=next;
      if(next.phase==='complete'||next.phase==='done'){a.passTechniqueState=null;a.passTechnique=null;a.finishPose=null;a.mesh.position.y=0;a.jumpY=0;return false;}
      if(next.position){a.mesh.position.set(next.position.x,next.position.y,next.position.z);a.jumpY=Math.max(0,next.position.y);}
      if(next.velocity)a.velocity.set(next.velocity.x,0,next.velocity.z);
      if(next.lockedDirection){a.heading.set(next.lockedDirection.x,0,next.lockedDirection.z).normalize();a.mesh.rotation.y=Math.atan2(a.heading.x,a.heading.z);}
      a.catchPose=1;a.swatPose=1;a.ballSeen=true;
      const recovery=next.phase==='recovery',pitch=recovery?.7:1.15;
      a.finishPose={pitch,roll:0,drop:recovery?.35:0,fold:.55};
      return true;
    }
    a.passRecover=Math.max(0,(a.passRecover||0)-dt);if(a.passRecover<=0)a.passTechnique=null;
    if(!s.ballLive){a.ballObservedFor=0;a.passTechnique=null;return false;}
    if(a.passRecover>0)return false;
    if(!a.ballSeen||a.ballObservedFor<a.reaction)return false;
    const receiver=s.receivers.reduce((best,r)=>!best||r.mesh.position.distanceToSquared(s.ball.position)<best.mesh.position.distanceToSquared(s.ball.position)?r:best,null);
    const decision=F.defensiveTechnique({defender:snapshot(a),ball:{position:vector(s.ball.position),velocity:vector(s.ballVel),tipped:s.throwBobbles>0,secured:!!s.pendingCatch},receiver:receiver?snapshot(receiver):null,observation:{visible:true,observedFor:a.ballObservedFor},game:{down:s.down,fourthDown:s.down===4}});
    if(!decision||decision.technique==='cover')return false;
    a.passTechnique=decision;a.passRecover=decision.commitment+decision.recovery;a.swatPose=1;
    if(decision.technique==='jump_swat'&&a.jumpY<.01&&a.jumpCooldown<=0)a.jumpVel=a.jumpVelocity;
    if(decision.dive){
      a.passTechniqueState=F.beginTechnique({defender:snapshot(a),decision});return defense(a,0);
    }
    return false;
  }
  return {adjustLaunch,updateWindow,defense,refreshTargets,cycle,snapshot,get placement(){return placement;},get targetSlot(){return targetSlot;}};
}
const api={create,choices};if(typeof module!=='undefined')module.exports=api;root.QBFootballExperience=api;
})(typeof globalThis!=='undefined'?globalThis:this);
