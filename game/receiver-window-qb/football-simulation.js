/* Shared football planning math. +Y up; positions/reach in the caller's world
 * units, velocities in units/second, accelerations in units/second squared.
 * Time is seconds; ratings are 0..100. No routes, hidden inputs, random contact
 * rolls, Three.js objects or possession decisions are used here. Reach results
 * are planning bounds only: rendered glove/ball sweeps remain authoritative.
 */
(function(root){
  'use strict';
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  function number(value,fallback,min=-1e6,max=1e6){
    if(value===undefined||value===null)return fallback;
    if(typeof value!=='number'||!Number.isFinite(value))throw new TypeError('Football values must be finite numbers');
    return clamp(value,min,max);
  }
  function vector(value,fallback={x:0,y:0,z:0}){
    if(value===undefined||value===null)return {...fallback};
    return {x:number(value.x,fallback.x),y:number(value.y,fallback.y),z:number(value.z,fallback.z)};
  }
  const add=(a,b)=>({x:a.x+b.x,y:a.y+b.y,z:a.z+b.z});
  const sub=(a,b)=>({x:a.x-b.x,y:a.y-b.y,z:a.z-b.z});
  const scale=(v,s)=>({x:v.x*s,y:v.y*s,z:v.z*s});
  const dot=(a,b)=>a.x*b.x+a.y*b.y+a.z*b.z;
  const length=v=>Math.hypot(v.x,v.y,v.z);
  const flatLength=v=>Math.hypot(v.x,v.z);
  const unit=(v,fallback={x:0,y:0,z:-1})=>length(v)>1e-9?scale(v,1/length(v)):{...fallback};
  const flat=v=>({x:v.x,y:0,z:v.z});
  const rating=(v,fallback=.7)=>number(v,fallback*100,0,100)/100;
  const dtLimit=.25;
  function snapshot(actor={}){
    const fatigue=number(actor.fatigue,0,0,1),reach=actor.reach||{};
    const maxSpeed=number(actor.maxSpeed,8.5,0,30)*(1-fatigue*.25);
    let velocity=vector(actor.velocity);
    const speed=flatLength(velocity);
    if(speed>maxSpeed&&speed>0){velocity.x*=maxSpeed/speed;velocity.z*=maxSpeed/speed;}
    if(actor.verticalVelocity!==undefined)velocity.y=number(actor.verticalVelocity,0,-25,25);
    return {position:vector(actor.position),velocity,heading:unit(flat(vector(actor.heading,{x:0,y:0,z:-1}))),
      maxSpeed,acceleration:number(actor.acceleration??actor.accel,18,0,80)*(1-fatigue*.4),fatigue,
      reaction:number(actor.reaction,.2,.08,.8),reach:{horizontal:number(reach.horizontal,.72,0,3),
        standingHeight:number(reach.standingHeight,2.2,.1,5),jumpHeight:number(reach.jumpHeight,.52,0,2),
        minHeight:number(reach.minHeight,.08,0,2),diveHeight:number(reach.diveHeight,1.25,.1,3)},
      groundY:number(actor.groundY,0),strength:rating(actor.strength),ballSkills:rating(actor.ballSkills??actor.hands, .65)};
  }
  function predictPosition(actor,time){const a=snapshot(actor);return add(a.position,scale(a.velocity,number(time,0,0,2.5)));}
  function acceleration(ball={}){const wind=vector(ball.wind??ball.acceleration);wind.y-=number(ball.gravity,9.81,.1,50);return wind;}
  function ballPoint(ball,time){const t=number(time,0,0,5),p=vector(ball.position),v=vector(ball.velocity),a=acceleration(ball);return add(add(p,scale(v,t)),scale(a,.5*t*t));}
  function reachAt(actor,point,time,options={}){
    const a=snapshot(actor),p=vector(point),t=number(time,0,0,3),reaction=number(options.reactionTime,0,0,.8);
    const usable=Math.max(0,t-reaction),future=add(a.position,scale(a.velocity,t));
    const correction=Math.min(a.maxSpeed*usable,.5*a.acceleration*usable*usable);
    const horizontal=flatLength(sub(p,future));
    const radial=flatLength(sub(p,a.position));
    const lateralMargin=a.reach.horizontal+correction-horizontal;
    const travelMargin=a.reach.horizontal+a.maxSpeed*t-radial;
    const gravity=number(options.gravity,9.81,.1,50),jumpHeight=options.allowJump===false?0:a.reach.jumpHeight;
    const jumpSpeed=Math.sqrt(2*gravity*jumpHeight),apex=jumpSpeed/gravity;
    const lift=usable>=apex?jumpHeight:Math.max(0,jumpSpeed*usable-.5*gravity*usable*usable);
    const maxY=a.position.y+a.reach.standingHeight+lift;
    const minY=a.groundY+a.reach.minHeight;
    const heightMargin=Math.min(maxY-p.y,p.y-minY);
    const margin=Math.min(lateralMargin,travelMargin,heightMargin);
    return {reachable:margin>=0,margin,horizontalMargin:Math.min(lateralMargin,travelMargin),heightMargin,
      maxY,minY,bodyPosition:future,correction,time:t};
  }
  function visibleToQB(d){return d&& (d.observed===true||d.visible===true);}
  const aliases={outside:'outside_shoulder',inside:'inside_shoulder',away:'outside_shoulder',high:'high_point',low:'low_away',back:'back_shoulder',instride:'in_stride',over:'over_shoulder',open:'open_grass'};
  const placementModes=['outside_shoulder','inside_shoulder','high_point','low_away','back_shoulder','in_stride','over_shoulder','open_grass','sideline'];
  function placementTarget(options={}){
    const receiver=snapshot(options.receiver),origin=vector(options.origin),time=number(options.flightTime,.55,0,2.5);
    const mode=aliases[options.mode]||options.mode||'in_stride';
    if(!placementModes.includes(mode))throw new RangeError('Unknown football placement mode: '+mode);
    const amount=number(options.amount,.55,0,2);
    const forward=unit(flatLength(receiver.velocity)>.15?flat(receiver.velocity):receiver.heading);
    const right={x:-forward.z,y:0,z:forward.x};
    const center=add(receiver.position,scale(receiver.velocity,time));
    center.y=receiver.position.y+number(options.receiver?.handHeight,1.65,.1,5);
    let side=dot(sub(center,{x:0,y:center.y,z:center.z}),right)>=0?1:-1;
    const defenders=(options.defenders||[]).filter(visibleToQB).map(d=>({d,p:predictPosition(d,time)})).sort((a,b)=>flatLength(sub(a.p,center))-flatLength(sub(b.p,center)));
    if(defenders.length){const leverage=dot(sub(defenders[0].p,center),right);if(Math.abs(leverage)>.025)side=leverage>0?-1:1;}
    const offset={x:0,y:0,z:0};
    if(mode==='outside_shoulder'||mode==='inside_shoulder'||mode==='low_away'){
      const direction=mode==='inside_shoulder'?-side:side;offset.x=right.x*amount*direction;offset.z=right.z*amount*direction;
    }
    if(mode==='high_point')offset.y=amount;
    if(mode==='low_away')offset.y=-amount;
    if(mode==='back_shoulder'){offset.x=-forward.x*amount;offset.z=-forward.z*amount;}
    if(mode==='over_shoulder'){offset.x=forward.x*amount;offset.z=forward.z*amount;offset.y=amount*.45;}
    if(mode==='open_grass'){offset.x=forward.x*amount*1.6;offset.z=forward.z*amount*1.6;}
    if(mode==='sideline'){
      const field=options.field||{},minX=number(field.minX,-25.6),maxX=number(field.maxX,25.6);
      if(maxX<=minX)throw new RangeError('Field maxX must exceed minX');
      const edge=center.x<(minX+maxX)/2?minX+.15:maxX-.15;
      offset.x=clamp(edge-center.x,-amount*1.6,amount*1.6);
    }
    // User freedom is preserved. This is never snapped back into catch range.
    return add(add(center,offset),vector(options.freeOffset));
  }
  function passingWindow(options={}){
    const origin=vector(options.origin),target=vector(options.target),time=number(options.flightTime,.55,.04,3);
    const gravity=number(options.gravity,9.81,.1,50),wind=vector(options.wind),a={x:wind.x,y:wind.y-gravity,z:wind.z};
    const velocity=options.velocity?vector(options.velocity):sub(scale(sub(target,origin),1/time),scale(a,time/2));
    const ball={position:origin,velocity,gravity,wind};
    const receiver=reachAt(options.receiver,target,time,{gravity});
    const threats=[];
    for(const defender of (options.defenders||[]).filter(visibleToQB)){
      const d=snapshot(defender);let best=null;
      const steps=Math.max(2,Math.ceil(time/.04));
      for(let i=1;i<=steps;i++){
        const t=time*i/steps,p=ballPoint(ball,t),r=reachAt(defender,p,t,{reactionTime:d.reaction,gravity});
        if(!best||r.margin>best.margin)best={time:t,point:p,margin:r.margin,reachable:r.reachable};
      }
      threats.push({id:defender.id??null,...best});
    }
    // Positive clearance means every observed defender misses the planning bound.
    const clearance=threats.length?Math.min(...threats.map(t=>-t.margin)):99;
    const label=!receiver.reachable||clearance<-.10?'danger':clearance<.35?'tight':'clean';
    return {label,kind:label,receiverReachable:receiver.reachable,receiverMargin:receiver.margin,clearance,
      observedDefenders:threats.length,threats,time,contactRequired:true};
  }
  function throwSolution(options={}){
    const origin=vector(options.origin),intended=vector(options.target),qb=options.qb||{},weather=options.weather||{};
    const accuracy=rating(qb.accuracy,.75),strength=rating(qb.strength,.75),footwork=rating(qb.footwork,.75),release=number(qb.releaseTiming,.9,0,1);
    const random=typeof options.random==='function'?options.random:Math.random;
    const sample=()=>number(random(),.5,0,1)*2-1;
    const wind=vector(weather.wind),gravity=number(options.gravity,9.81,.1,50),wet=number(weather.wetness,0,0,1),gust=number(weather.gust,0,0,5);
    const type=options.type||'bullet',factors={bullet:1,touch:1.22,fade:1.4,lob:1.6,layered:1.3};
    if(!Object.hasOwn(factors,type))throw new RangeError('Unknown football throw type');
    const power=number(options.power,.7,0,1),maxSpeed=(18.5+power*26.5)*(.78+.30*strength);
    const distance=length(sub(intended,origin)),spread=(.012+.8*(1-accuracy)**2+.25*(1-footwork)+.30*(1-release)+wet*.12+gust*.035)*(.3+distance*.035);
    const forward=unit(flat(sub(intended,origin))),right={x:-forward.z,y:0,z:forward.x};
    const error=add(scale(right,sample()*spread),{x:0,y:sample()*spread*.6,z:0});
    const target=add(intended,error),a={x:wind.x,y:wind.y-gravity,z:wind.z};
    const solve=t=>sub(scale(sub(target,origin),1/t),scale(a,t/2));
    let low=null,last=null;
    for(let i=0;i<=240;i++){
      const t=.06+(3-.06)*i/240,v=solve(t);
      if(length(v)<=maxSpeed){if(low===null)low=t;last=t;}
    }
    const possible=low!==null;
    const time=possible?clamp(low*factors[type],low,last):clamp(flatLength(sub(target,origin))/maxSpeed,.08,3);
    let velocity=solve(time);if(!possible)velocity=scale(unit(velocity),maxSpeed);
    return {velocity,flightTime:time,target,intended,error,spread,maxSpeed,reachable:possible,
      acceleration:a,gravity,wind,type};
  }
  function observeBall(options={}){
    const d=snapshot(options.defender),ball=options.ball||{},previous=options.previous||{},dt=number(options.dt,0,0,dtLimit);
    const eye=add(d.position,{x:0,y:number(options.eyeHeight,1.72,.1,5),z:0}),to=sub(vector(ball.position),eye),range=length(to);
    const flatTo=unit(flat(to)),facing=dot(d.heading,flatTo),cos=number(options.fovCos,-.05,-1,1);
    const visible=ball.live!==false&&!options.occluded&&range<=number(options.range,55,0,150)&&(flatLength(to)<.05||facing>=cos);
    const observedFor=visible?Math.min(5,number(previous.observedFor,0,0,5)+dt):0;
    return {visible,observedFor,canReact:visible&&observedFor>=d.reaction,range,facing};
  }
  function cover(reason){return {technique:'cover',target:null,contactTime:0,commitment:0,recovery:0,forceSwat:false,canIntercept:false,handedness:'both',reason};}
  function defensiveTechnique(options={}){
    const input=options.defender||{},d=snapshot(input),ball=options.ball||{},observation=options.observation||{},game=options.game||{};
    if(ball.live===false||observation.visible!==true||number(observation.observedFor,0,0,5)<d.reaction)return cover('unobserved_or_reacting');
    if(number(input.commitRemaining,0,0,5)>0||number(input.recoveryRemaining,0,0,5)>0)return cover('committed_or_recovering');
    const gravity=number(ball.gravity,9.81,.1,50),ballState={...ball,gravity};
    let candidate=null,dive=null;
    for(let t=.04;t<=1.201;t+=.04){
      const p=ballPoint(ballState,t);if(p.y<d.groundY+.05)break;
      const r=reachAt(input,p,t,{gravity});
      if(r.reachable){candidate={point:p,time:t,reach:r};break;}
      // A dive changes body travel; it never enlarges a glove's contact radius.
      const direction=unit(flat(sub(p,d.position))),alignment=dot(d.heading,direction),speed=Math.min(d.maxSpeed*1.05,Math.max(flatLength(d.velocity),d.maxSpeed*.82));
      const future=add(d.position,scale(direction,speed*t)),lift=Math.max(0,2.4*t-.5*gravity*t*t);
      if(!dive&&t<=.7&&alignment>.2&&flatLength(sub(p,future))<=d.reach.horizontal&&p.y>=d.groundY+.12&&p.y<=d.groundY+d.reach.diveHeight+lift)dive={point:p,time:t,reach:r};
    }
    if(!candidate&&!dive)return cover('no_physical_window');
    const selected=candidate||dive,target=selected.point,t=selected.time;
    const fourth=Number(game.down)===4,discipline=rating(input.discipline,.7),aggression=rating(input.aggression,.5);
    const ballHawk=input.personality==='ballhawk'||input.personality==='Ball Hawks';
    const intercept=d.ballSkills+(ballHawk?.18:0)+aggression*.08-d.fatigue*.20>=.66&&!(fourth&&discipline>.45);
    const high=target.y>d.position.y+d.reach.standingHeight;
    let technique=high?'jump_swat':'standing_swat',canIntercept=false,isDive=!candidate;
    if(isDive){technique=intercept?'interception':'dive_swat';canIntercept=intercept;}
    else if(number(ball.tipCount,ball.tipped?1:0,0,100)>0){technique='tip_drill';canIntercept=intercept;}
    else if(options.receiver?.securing||ball.secured===true){technique='late_disruption';}
    else {
      const r=options.receiver?snapshot(options.receiver):null;
      const trailing=r&&dot(flat(sub(d.position,r.position)),unit(flat(r.velocity),r.heading))<-.15&&flatLength(sub(d.position,r.position))<1.8;
      const origin=ball.origin?vector(ball.origin):vector(ball.position),lane=unit(flat(sub(r?.position||target,origin)));
      const undercut=r&&dot(flat(sub(d.position,r.position)),lane)<-.25&&t>.16;
      if(intercept){technique=undercut?'undercut':'interception';canIntercept=true;}
      else if(trailing)technique='trailing_breakup';
    }
    const forceSwat=!canIntercept,right={x:-d.heading.z,y:0,z:d.heading.x};
    const handedness=canIntercept?'both':dot(sub(target,d.position),right)>=0?'right':'left';
    return {technique,target,contactTime:t,commitment:isDive?.78:high?.70:.38,recovery:isDive?.72:high?.35:.24,
      forceSwat,canIntercept,handedness,dive:isDive,jump:high&&!isDive,contactRequired:true,reason:fourth?'situational_breakup':'observed_ball_window'};
  }
  function turnToward(from,to,maxAngle){
    const a=Math.atan2(from.x,from.z),b=Math.atan2(to.x,to.z),delta=Math.atan2(Math.sin(b-a),Math.cos(b-a));
    return {x:Math.sin(a+clamp(delta,-maxAngle,maxAngle)),y:0,z:Math.cos(a+clamp(delta,-maxAngle,maxAngle))};
  }
  function beginTechnique(options={}){
    const d=snapshot(options.defender),decision=options.decision||cover('missing');
    if(decision.technique==='cover'||!decision.target)return null;
    const desired=unit(flat(sub(vector(decision.target),d.position))),motion=flatLength(d.velocity)>.3?unit(flat(d.velocity)):d.heading;
    const direction=turnToward(motion,desired,.45),isDive=decision.dive===true||decision.technique==='dive_swat';
    const speed=isDive?Math.min(d.maxSpeed*1.05,Math.max(flatLength(d.velocity),d.maxSpeed*.82)):flatLength(d.velocity);
    const velocity=isDive?scale(direction,speed):{...d.velocity};
    const jump=decision.jump===true||decision.technique==='jump_swat';
    velocity.y=isDive?2.4:jump?Math.sqrt(2*9.81*d.reach.jumpHeight):d.velocity.y;
    const commitment=number(decision.commitment,isDive?.78:.38,.08,2),recovery=number(decision.recovery,isDive?.72:.24,.08,2);
    return {technique:decision.technique,position:{...d.position},velocity,lockedDirection:direction,target:vector(decision.target),
      elapsed:0,remaining:commitment+recovery,commitment,recovery,phase:'committed',groundY:d.groundY,gravity:9.81,
      forceSwat:decision.forceSwat===true,canIntercept:decision.canIntercept===true,handedness:decision.handedness||'both',
      dive:isDive,jump,landed:false,damping:Math.min(6,d.acceleration/Math.max(.1,speed)),consumedDt:0};
  }
  function advanceTechnique(state,dt){
    if(!state)return null;
    const requested=number(dt,0,0,dtLimit),s={...state,position:vector(state.position),velocity:vector(state.velocity),lockedDirection:vector(state.lockedDirection),target:vector(state.target)};
    const total=number(s.commitment,.38,.08,2)+number(s.recovery,.24,.08,2),elapsed=number(s.elapsed,0,0,total);
    const step=Math.min(requested,total-elapsed);s.consumedDt=step;
    let rest=step;
    const ground=number(s.groundY,0),g=number(s.gravity,9.81,.1,50),height=Math.max(0,s.position.y-ground);
    if(height>1e-8||s.velocity.y>0){
      const landing=(s.velocity.y+Math.sqrt(s.velocity.y*s.velocity.y+2*g*height))/g,air=Math.min(rest,landing);
      s.position=add(s.position,scale(s.velocity,air));s.position.y-=.5*g*air*air;s.velocity.y-=g*air;rest-=air;
      if(air>=landing-1e-8){s.position.y=ground;s.velocity.y=0;s.landed=true;}
    }
    if(rest>0){
      const damping=number(s.damping,3,0,6),decay=Math.exp(-damping*rest),distance=damping>1e-8?(1-decay)/damping:rest;
      s.position.x+=s.velocity.x*distance;s.position.z+=s.velocity.z*distance;
      s.velocity.x*=decay;s.velocity.z*=decay;s.position.y=ground;s.velocity.y=0;s.landed=true;
    }
    s.elapsed=Math.min(total,elapsed+step);s.remaining=Math.max(0,total-s.elapsed);
    s.phase=s.remaining<=1e-8?'complete':s.elapsed<s.commitment?'committed':'recovery';
    return s;
  }
  const api={placementModes,placementTarget,passingWindow,throwSolution,predictPosition,ballPoint,reachAt,observeBall,defensiveTechnique,beginTechnique,advanceTechnique,maxStep:dtLimit};
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(root)root.QBFootball=api;
})(typeof globalThis!=='undefined'?globalThis:this);
