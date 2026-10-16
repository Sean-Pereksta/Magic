import { PHYSICS } from "../config.js";
import { createPlayer, updatePlayer, surfaceY } from "../physics.js";

function gap(a,b) { return Math.max(0,b.x-a.x-a.w,a.x-b.x-b.w); }
function abilitiesFor(value={}) { return {dash:value.dash!==false,wallJump:value.wallJump!==false,wings:Boolean(value.wings),sandals:Boolean(value.sandals)}; }

// Conservative ballistic gate; the room validator then runs the real controller.
export function canReachSurface(from,to,physics=PHYSICS,abilities={}) {
  if(from===to)return true;
  if(to.w<physics.requiredLandingWidth)return false;
  const a=abilitiesFor(abilities),speed=a.sandals?physics.sandalsSpeed:physics.runSpeed;
  const fromY=surfaceY(from,from.x+from.w/2),toY=surfaceY(to,to.x+to.w/2);
  const rise=fromY-toY, extra=a.wings?physics.jumpVelocity**2/(2*physics.gravity)*0.8:0;
  if(rise>physics.jumpVelocity**2/(2*physics.gravity)-7+extra)return false;
  if(toY-fromY>360)return false;
  const discriminant=physics.jumpVelocity**2+2*physics.gravity*(toY-fromY);
  const duration=(physics.jumpVelocity+Math.sqrt(Math.max(0,discriminant)))/physics.gravity;
  const dash=a.dash?Math.max(0,physics.dashSpeed-speed)*physics.dashDuration:0;
  return gap(from,to)+physics.playerWidth*0.5+12<=speed*duration+dash;
}

export function simulateSurfaceJump(from,to,room,{abilities={},physics=PHYSICS,includeHazards=false}={}) {
  if(!canReachSurface(from,to,physics,abilities))return false;
  const right=to.x+to.w/2>=from.x+from.w/2,dir=right?1:-1;
  const startX=right?from.x+from.w-physics.playerWidth-8:from.x+8;
  const targetX=to.x+to.w/2-physics.playerWidth/2;
  const available=abilitiesFor(abilities),speed=available.sandals?physics.sandalsSpeed:physics.runSpeed;
  const surfaces=(room.platforms||[]).filter(s=>!s.hidden).map((s,i)=>({...s,id:s.id||`surface-${i}`,dead:false}));
  const fromId=from.id||`surface-${room.platforms.indexOf(from)}`;
  const toId=to.id||`surface-${room.platforms.indexOf(to)}`;
  const world={width:room.width||Math.max(from.x+from.w,to.x+to.w)+200,platforms:surfaces,movers:[],bounds:{left:0}};
  for(const brake of [0,35,75]) {
    const p=createPlayer(startX,surfaceY(from,startX+physics.playerWidth/2)-physics.playerHeight);
    p.onGround=true;p.groundId=fromId;p.vx=dir*speed;p.powers.wings=available.wings;p.powers.sandals=available.sandals;
    let jumped=false,secondJump=false,dashed=false;
    for(let step=0;step<180;step++) {
      const remaining=targetX-p.x;
      const needsExtendedJump=from.y-to.y>120||gap(from,to)>220;
      const useSecond=available.wings&&!secondJump&&step>4&&p.vy>=-65&&p.extraJumps>0&&needsExtendedJump;
      const useDash=available.dash&&!dashed&&step>5&&p.vy>=-120&&gap(from,to)>220&&!useSecond;
      if(useSecond)secondJump=true;if(useDash)dashed=true;
      // Air braking is part of the shipped controller, not a generous jump envelope.
      const input={horizontal:()=>Math.abs(remaining)<brake?0:Math.sign(remaining),jumpPressed:()=>step===0||useSecond,jumpHeld:()=>true,downHeld:()=>false,dashPressed:()=>useDash};
      updatePlayer(p,world,input,physics.fixedStep);
      if(!p.onGround)jumped=true;
      if(includeHazards&&(room.hazards||[]).some(h=>!h.timed&&h.type!=="wind"&&p.x<h.x+h.w&&p.x+p.w>h.x&&p.y<h.y+h.h&&p.y+p.h>h.y))break;
      if(jumped&&p.onGround&&p.groundId===toId)return true;
      if(p.y>Math.max(from.y,to.y)+380)break;
      if(jumped&&p.onGround&&p.groundId!==fromId&&p.groundId!==toId)break;
    }
  }
  return false;
}

export function validateRoom(room,physics=PHYSICS,abilities={}) {
  const surfaces=(room.platforms||[]).filter(p=>!p.hidden&&!p.switchId&&p.kind!=="gate");
  if(!surfaces.length)return{valid:false,reason:"no surfaces",edges:[],visitedCount:0,surfaceCount:0};
  const starts=surfaces.map((s,i)=>s.entry?i:-1).filter(i=>i>=0);
  const exits=new Set(surfaces.map((s,i)=>s.exit?i:-1).filter(i=>i>=0));
  if(!starts.length)starts.push(0);if(!exits.size)exits.add(surfaces.length-1);
  const queue=[...starts],visited=new Set(queue),edges=[];
  while(queue.length) {
    const fi=queue.shift(),from=surfaces[fi];
    for(let ti=0;ti<surfaces.length;ti++) {
      const to=surfaces[ti];
      if(fi===ti||gap(from,to)>550||!canReachSurface(from,to,physics,abilities))continue;
      if(!simulateSurfaceJump(from,to,room,{physics,abilities}))continue;
      edges.push([fi,ti]);
      if(!visited.has(ti)){visited.add(ti);queue.push(ti);}
    }
  }
  const valid=[...exits].some(i=>visited.has(i));
  return{valid,surfaceCount:surfaces.length,visitedCount:visited.size,edges,reason:valid?"entrance-to-exit path proved with shipped movement":"no physical entrance-to-exit path"};
}

export function validateLevel(world) {
  const errors=[],modules=new Set();
  for(const descriptor of world.chunkPlan||[]) {
    if(!descriptor.validation?.valid)errors.push({chunk:descriptor.index,reason:descriptor.validation?.reason||"unvalidated chunk"});
    modules.add(descriptor.moduleId);
    if(descriptor.entryY!==descriptor.exitY)errors.push({chunk:descriptor.index,reason:"unsafe chunk seam"});
  }
  return{valid:errors.length===0,chunkCount:world.chunkCount||world.chunkPlan?.length||0,moduleCount:modules.size,errors};
}
