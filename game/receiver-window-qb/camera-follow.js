/* Pure camera poses. The game owns release, possession, replay and snap timing. */
(function(root){
  'use strict';
  const finite=(value,fallback)=>Number.isFinite(Number(value))?Number(value):fallback;
  const clamp=(value,min,max)=>Math.max(min,Math.min(max,value));
  const vector=(value,fallback={x:0,y:0,z:0})=>({x:finite(value?.x,finite(fallback?.x,0)),y:finite(value?.y,finite(fallback?.y,0)),z:finite(value?.z,finite(fallback?.z,0))});
  function limits(raw={}){
    raw=raw&&typeof raw==='object'?raw:{};
    const ground=finite(raw.groundY,0),minX=finite(raw.minX,-26),minZ=finite(raw.minZ,-70);
    return {minX,maxX:Math.max(minX+1,finite(raw.maxX,26)),minZ,maxZ:Math.max(minZ+1,finite(raw.maxZ,56)),ground,
      margin:clamp(finite(raw.margin,6),0,16),minY:ground+clamp(finite(raw.clearance,3.2),1.5,12),maxY:ground+clamp(finite(raw.maxHeight,65),15,120)};
  }
  function bounded(value,b,focus=false){return {x:clamp(value.x,b.minX-(focus?0:b.margin),b.maxX+(focus?0:b.margin)),y:clamp(value.y,focus?b.ground+.12:b.minY,b.maxY),z:clamp(value.z,b.minZ-(focus?0:b.margin),b.maxZ+(focus?0:b.margin))};}
  const seconds=dt=>clamp(finite(dt,1/60),0,.1);
  function heading(velocity,ball,origin,previous,dt){
    const old=vector(previous?.direction),fallback=vector(previous?.lookAt),position=vector(previous?.position);
    let ox=old.x,oz=old.z;if(Math.hypot(ox,oz)<1e-6){ox=fallback.x-position.x;oz=fallback.z-position.z;}
    let dx=velocity.x,dz=velocity.z;
    if(Math.hypot(dx,dz)<.4){if(previous&&Math.hypot(ox,oz)>.1){dx=ox;dz=oz;}else{dx=ball.x-origin.x;dz=ball.z-origin.z;if(Math.hypot(dx,dz)<.1){dx=ox;dz=oz;}}}
    if(Math.hypot(dx,dz)<1e-6){dx=0;dz=-1;}
    let angle=Math.atan2(dx,dz);
    if(previous&&Math.hypot(ox,oz)>1e-6){const before=Math.atan2(ox,oz),delta=Math.atan2(Math.sin(angle-before),Math.cos(angle-before));angle=before+clamp(delta*(-Math.expm1(-3.5*dt)),-1.8*dt,1.8*dt);}
    return {x:Math.sin(angle),y:0,z:Math.cos(angle)};
  }
  function smooth(previous,previousTarget,target,rate,dt,maxSpeed){
    if(!previous)return {...target};
    const start=vector(previous,target),prior=vector(previousTarget,start),decay=Math.exp(-rate*dt),out={};
    // Exact first-order damping of a target moving linearly across this frame.
    // Retaining its previous target removes most 30/60/120 Hz tracking drift.
    for(const key of ['x','y','z']){
      const slope=dt>0?(target[key]-prior[key])/dt:0;
      out[key]=dt>0?target[key]-slope/rate+(start[key]-prior[key]+slope/rate)*decay:start[key];
    }
    const distance=Math.hypot(out.x-start.x,out.y-start.y,out.z-start.z),step=maxSpeed*dt;
    if(distance>step&&distance>0)for(const key of ['x','y','z'])out[key]=start[key]+(out[key]-start[key])*step/distance;
    return out;
  }
  function pose(position,lookAt,{previous,dt,field,direction}={}){
    const b=limits(field),step=seconds(dt),targetPosition=bounded(vector(position),b),targetLookAt=bounded(vector(lookAt),b,true);
    const result={position:bounded(smooth(previous?.position,previous?.targetPosition,targetPosition,7,step,85),b),
      lookAt:bounded(smooth(previous?.lookAt,previous?.targetLookAt,targetLookAt,14,step,160),b,true),
      direction:direction||vector(previous?.direction,{x:0,y:0,z:-1}),targetPosition,targetLookAt};
    // A degenerate look vector has undefined orientation in common renderers.
    if(Math.hypot(result.position.x-result.lookAt.x,result.position.y-result.lookAt.y,result.position.z-result.lookAt.z)<.01)result.lookAt.y=Math.max(b.ground+.12,result.position.y-1);
    return result;
  }
  function flightCamera({ball,velocity,origin,elapsed=0,field,previous,dt}={}){
    const b=limits(field),step=seconds(dt),start=vector(origin,{x:0,y:b.ground+2.25,z:b.maxZ-2}),point=bounded(vector(ball,previous?.lookAt||start),b,true),v=vector(velocity);
    for(const key of ['x','y','z'])v[key]=clamp(v[key],-100,100);
    const direction=heading(v,point,start,previous,step),speed=Math.hypot(v.x,v.z),trail=7.5+Math.min(2,speed*.045),side=2.4;
    const position={x:point.x-direction.x*trail-direction.z*side,y:point.y+4.2+Math.min(3.3,Math.max(0,point.y-b.ground)*.12),z:point.z-direction.z*trail+direction.x*side};
    const lead=finite(elapsed,0)<.08?.035:.075,lookAt={x:point.x+v.x*lead,y:point.y+v.y*lead,z:point.z+v.z*lead};
    return pose(position,lookAt,{previous,dt:step,field,direction});
  }
  function carrierCamera({carrier,velocity,field,previous,dt}={}){
    const b=limits(field),point=vector(carrier,previous?.lookAt||{x:0,y:b.ground,z:0}),v=vector(velocity),step=seconds(dt);
    const direction=heading(v,point,point,previous,step),position={x:point.x-direction.x*8-direction.z*1.8,y:Math.max(b.ground,point.y)+4.8,z:point.z-direction.z*8+direction.x*1.8};
    const lookAt={x:point.x+clamp(v.x,-20,20)*.15,y:Math.max(b.ground,point.y)+1.35,z:point.z+clamp(v.z,-20,20)*.15};
    return pose(position,lookAt,{previous,dt:step,field,direction});
  }
  // Optional bridge into/out of another camera owner. Call only from its loop;
  // initialize previous from the actual camera pose and clear it at the next snap.
  function transitionCamera({position,lookAt,previous,dt,field}={}){return pose(position,lookAt,{previous,dt,field});}
  const api={flightCamera,carrierCamera,transitionCamera};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;root.QBCameraFollow=api;
})(typeof globalThis!=='undefined'?globalThis:this);
