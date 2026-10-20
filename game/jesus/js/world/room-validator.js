import { PHYSICS, VIEW } from "../config.js";
import { createPlayer, updatePlayer, updateMovers, updateWorldEnvironment, syncMovers, syncHazards, surfaceY } from "../physics.js";
import { updateGateProgress } from "./gate-progress.js";
import { collectPickups } from "../powers/blessings.js";

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

function validateStaticRoom(room,physics=PHYSICS,abilities={}) {
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

const dynamicCache=new Map();
const copy=value=>JSON.parse(JSON.stringify(value));
const control=(axis=0,jump=false,held=true,down=false)=>({horizontal:()=>axis,jumpPressed:()=>jump,jumpHeld:()=>held,downHeld:()=>down,dashPressed:()=>false});
function proofWorld(room,time) {
  const world={width:room.width || 3200,time,platforms:copy(room.platforms || []),movers:copy(room.movers || []),
    hazards:copy(room.hazards || []),switches:copy(room.switches || []),props:[],effects:[],bounds:{left:0}};
  world.platforms.forEach((p,i)=>p.id=`p-${i}`);world.movers.forEach((p,i)=>p.id=`m-${i}`);
  world.switches.forEach((s,i)=>s.id=`s-${i}`);
  for(const p of world.platforms)if(p.switchLocal!=null)p.switchId=world.switches[p.switchLocal]?.id;
  for(const p of world.platforms)if(p.unlockLocals)p.unlockIds=p.unlockLocals.map(i=>world.switches[i]?.id);
  if(room.progressionGates){world.layoutVersion=3;world.bounds={left:0,top:-700,bottom:1000};world.pickups=[];world.enemies=[];}
  syncMovers(world,time);syncHazards(world,time);return world;
}
function surface(world,id) { return [...world.platforms,...world.movers].find(p=>p.id===id); }
function tick(state,axis=0,jump=false,held=true,down=false) {
  state.time+=PHYSICS.fixedStep;state.world.time=state.time;
  updateMovers(state.world,state.time,PHYSICS.fixedStep);
  updatePlayer(state.player,state.world,control(axis,jump,held,down),PHYSICS.fixedStep);
  updateWorldEnvironment(state.world,state.player,PHYSICS.fixedStep,state.time);
  if(state.exploration) {
    updateGateProgress(state.world,state.player,PHYSICS.fixedStep);
    collectPickups(state.world,state.player);
    updateGateProgress(state.world,state.player,0);
    // Search branches retain their own bounded effects; visual particles cannot
    // inflate the state copied by a lengthy elevator timing search.
    if(state.world.effects.length>16)state.world.effects.splice(0,state.world.effects.length-16);
  }
  return state.player.hearts===4 && state.player.y<(state.world.deathY || 1100);
}
function routeIds(room,world) {
  if(room.validationWaypoints?.length)return room.validationWaypoints.map(w=>
    typeof w === "string" ? w : `${w.surface === "movers" || w.kind === "mover" ? "m" : "p"}-${w.index}`);
  return [...world.platforms.filter(p=>p.entry||p.route||p.exit),...world.movers.filter(p=>p.route)]
    .sort((a,b)=>(a.baseX ?? a.x)-(b.baseX ?? b.x)).map(p=>p.id);
}
function cloneProof(state) { return copy(state); }

// Each edge continues the actual player state left by the previous edge. Waiting,
// approaching a launch edge, platform riding, and airborne steering all run the
// shipped controller at 120 Hz. No moving surface is replaced by its travel box.
function transfer(initial,fromId,toId,maxWait,options={}) {
  const waiting=cloneProof(initial),from=surface(waiting.world,fromId),target=surface(waiting.world,toId);
  if(!from||!target)return [];
  const solutions=[],dt=PHYSICS.fixedStep;
  const direction=(target.baseX ?? target.x)+target.w/2 >= (options.exploration?initial.player.x:(from.baseX ?? from.x)+from.w/2) ? 1 : -1;
  let lastSolution=-Infinity;
  for(let sample=0;sample<=Math.ceil(maxWait/.2);sample++) {
    const source=surface(waiting.world,fromId),destination=surface(waiting.world,toId),p=waiting.player;
    if(p.onGround && p.groundId===fromId && source.solid!==false && !source.hidden) {
      // Try a standing launch and a short real run-up. Feedback aims at the
      // destination's current surface, so ferry interception is not a static test.
      for(const [runup,hold] of [[0,Infinity],[.11,Infinity],[0,.12],[.11,.12]]) {
        const attempt=cloneProof(waiting),ap=attempt.player;let airborne=false,success=false;
        let safeApproach=true;
        // Each delayed attempt walks through the complete approach. A hazard on
        // the far edge can therefore be waited out from the actual safe arrival
        // point instead of being crossed before the timing search even begins.
        for(let step=0;step<240;step++) {
          const support=surface(attempt.world,fromId);
          let launch=direction>0 ? support.x+support.w-ap.w-17 : support.x+17;
          if(options.exploration) {
            const destination=surface(attempt.world,toId);
            const aim=(options.targetX??destination.x+destination.w/2-ap.w/2);
            // A broad chamber floor permits a real walk under the first ledge;
            // jumping from its far boundary would prove an unrelated route.
            launch=Math.max(support.x+17,Math.min(support.x+support.w-ap.w-17,aim));
          }
          // Ferry decks may travel under a raised solid bank. Launch before its
          // wall instead of trying to walk through the bank to the deck's edge.
          for(const wall of attempt.world.platforms) {
            if(wall.oneWay || wall.open || wall.dead || wall.hidden || ap.y+ap.h<=wall.y || ap.y>=wall.y+wall.h)continue;
            if(direction>0 && wall.x>=ap.x+ap.w-1)launch=Math.min(launch,wall.x-ap.w-12);
            if(direction<0 && wall.x+wall.w<=ap.x+1)launch=Math.max(launch,wall.x+wall.w+12);
          }
          const distance=launch-ap.x;
          if(Math.abs(distance)<6 && Math.abs(ap.vx)<30)break;
          const stopping=ap.vx*Math.abs(ap.vx)/(2*PHYSICS.groundFriction);
          const axis=Math.abs(distance)<Math.abs(stopping)+4 ? 0 : Math.sign(distance);
          if(!tick(attempt,axis) || !ap.onGround || ap.groundId!==fromId) {safeApproach=false;break;}
        }
        if(!safeApproach)continue;
        for(let step=0;step<Math.ceil((1.6+runup)/dt);step++) {
          const goal=surface(attempt.world,toId),support=surface(attempt.world,fromId);
          const aim=options.targetX??goal.x+goal.w/2-ap.w/2;
          const remaining=aim-ap.x,braking=ap.vx*Math.abs(ap.vx)/(2*PHYSICS.airFriction);
          let axis=Math.abs(remaining)<12 || (Math.sign(remaining)===Math.sign(braking)&&Math.abs(remaining)<Math.abs(braking)*.3) ? 0 : Math.sign(remaining);
          if(step*dt<runup)axis=direction;
          const jump=step===Math.ceil(runup/dt);
          if(!tick(attempt,axis,jump,step*dt<runup+hold))break;
          if(!ap.onGround)airborne=true;
          if(ap.onGround && ap.groundId===toId) {
            // A grazing landing with outward momentum is not a safe waiting
            // state. Brake on the real surface before accepting a transfer.
            success=true;
            for(let settle=0;settle<48;settle++) {
              const pad=surface(attempt.world,toId),distance=pad.x+pad.w/2-ap.w/2-ap.x;
              if(Math.abs(ap.vx)<12 && ap.x>pad.x+4 && ap.x+ap.w<pad.x+pad.w-4)break;
              const stopping=ap.vx*Math.abs(ap.vx)/(2*PHYSICS.groundFriction);
              const steer=Math.abs(distance)<Math.abs(stopping)+8 ? 0 : Math.sign(distance);
              if(!tick(attempt,steer) || !ap.onGround || ap.groundId!==toId) {success=false;break;}
            }
            break;
          }
          if(airborne && ap.onGround && ap.groundId!==fromId)break;
          if(ap.y>Math.max(surfaceY(support,ap.x+ap.w/2),surfaceY(goal,ap.x+ap.w/2))+380)break;
        }
        if(success && attempt.time-lastSolution>.65) {
          // Prefer boarding a returning platform with enough solid time to
          // cross its deck and leave. Late boarding is not used as a proof of a
          // longer sequence when waiting safely for the next cycle is available.
          const landing=surface(attempt.world,toId);
          if(landing.motion === "timed") {
            const period=landing.period || Math.PI*2/(landing.speed || 1.2);
            const cycle=((attempt.time+(landing.timeOffset || 0)+(landing.phase || 0)*period/(Math.PI*2))%period+period)%period;
            const solidDuration=(landing.warningDuration ?? .9)+(landing.activeDuration ?? 3.5);
            if(solidDuration-cycle<Math.min(3.2,solidDuration*.5))continue;
          }
          attempt.jumps=(initial.jumps || 0)+Number(airborne);solutions.push(attempt);lastSolution=attempt.time;
          if(landing.motion !== "timed" || solutions.length>=3)return solutions;
        }
      }
    }
    // A disappeared support is allowed to recover, but the player must actually
    // remain safe; falling into a gap is never silently reset by the search.
    for(let step=0;step<Math.round(.2/dt);step++)if(!tick(waiting,0))return solutions;
  }
  return solutions;
}

export function validateDynamicRoom(room,physics=PHYSICS) {
  const key=JSON.stringify([room.id,room.requiresDynamic,room.platforms,room.movers,room.hazards,room.switches,room.validationWaypoints,physics]);
  if(dynamicCache.has(key))return dynamicCache.get(key);
  const result={valid:false,dynamic:true,baseAbilities:true,phaseProofs:[],edges:[],surfaceCount:(room.platforms?.length || 0)+(room.movers?.length || 0),visitedCount:0};
  const save=reason=>{result.reason=reason;if(dynamicCache.size>=192)dynamicCache.clear();dynamicCache.set(key,result);return result;};
  if(!(room.movers || []).length)return save("required dynamic route has no moving supports");
  const staticBypass={...room,movers:[],platforms:(room.platforms || []).map(p=>p.kind === "gate" ? {...p,open:true} : p)};
  if(room.requiresDynamic && validateStaticRoom(staticBypass,physics,{dash:true,wallJump:false,wings:false,sandals:false}).valid)
    return save("mandatory moving encounter has a static bypass");
  const periods=(room.movers || []).map(m=>m.period || Math.PI*2/(m.speed || 1.2));
  const cycle=Math.max(4,...periods),maxWait=Math.min(20,Math.max(10,cycle*1.5));
  for(const startTime of [0,cycle*.23,cycle*.51,cycle*.79]) {
    const world=proofWorld(room,startTime),ids=routeIds(room,world),entry=surface(world,ids[0]);
    if(!entry || !ids.length)return save("missing dynamic validation waypoints");
    if(ids.some(id=>{const p=surface(world,id);return !p || p.requires || p.branch;}))return save("required route references an optional or missing support");
    const p=createPlayer(entry.x+20,surfaceY(entry,entry.x+37)-physics.playerHeight);
    p.onGround=true;p.groundId=entry.id;
    let states=[{world,player:p,time:startTime,jumps:0,exploration:Boolean(room.progressionGates)}];
    for(let index=1;index<ids.length;index++) {
      const arrivals=[];
      for(const state of states)arrivals.push(...transfer(state,ids[index-1],ids[index],maxWait));
      arrivals.sort((a,b)=>a.time-b.time);states=arrivals.slice(0,3);
      if(!states.length)return save(`no safe timed transfer ${ids[index-1]} -> ${ids[index]} at start ${startTime.toFixed(2)}`);
    }
    const finish=states[0];result.phaseProofs.push({startTime,elapsed:finish.time-startTime,jumps:finish.jumps,supports:ids});
    result.edges=ids.slice(1).map((id,i)=>[ids[i],id]);result.visitedCount=ids.length;
  }
  result.valid=true;return save("base movement route replayed at four independent initial mechanism phases");
}

export function validateRoom(room,physics=PHYSICS,abilities={}) {
  return room.exploration ? validateExplorationRoom(room,physics) : room.requiresDynamic || room.validationWaypoints?.length ? validateDynamicRoom(room,physics) : validateStaticRoom(room,physics,abilities);
}

function explorationWorld(room,time) {
  const world=proofWorld(room,time);
  world.layoutVersion=3;world.bounds={left:0,top:-700,bottom:1000};world.deathY=1000;
  world.pickups=copy(room.pickupSpawns||[]).filter(p=>p.unlockLocal!=null).map((p,i)=>({...p,id:`k-${i}`,w:28,h:28,unlockId:`s-${p.unlockLocal}`}));
  world.enemies=[];
  for(const gate of world.platforms)if(gate.unlockLocals)gate.unlockIds=gate.unlockLocals.map(i=>`s-${i}`);
  return world;
}
function pointId(point) {return `${point.surface==="movers"?"m":"p"}-${point.index}`;}
function walkOnSurface(state,id,targetX,maxSeconds=8) {
  const p=state.player;
  for(let step=0;step<maxSeconds/PHYSICS.fixedStep;step++) {
    const support=surface(state.world,id);
    if(!support||!p.onGround||p.groundId!==id)return false;
    const target=Math.max(support.x+5,Math.min(support.x+support.w-p.w-5,targetX));
    const distance=target-p.x,stopping=p.vx*Math.abs(p.vx)/(2*PHYSICS.groundFriction);
    if(Math.abs(distance)<8&&Math.abs(p.vx)<25)return true;
    const axis=Math.abs(distance)<Math.abs(stopping)+4?0:Math.sign(distance);
    if(!tick(state,axis))return false;
  }
  return false;
}
function climbShaft(state,toId,shaft) {
  const p=state.player,goal=surface(state.world,toId);let direction=1,lastJump=-1;
  for(let step=0;step<2400;step++) {
    // The resting half-ledges allow a pause, but their 220px spacing exceeds a
    // normal jump. Alternating real wall contacts is required between them.
    if(p.onGround&&p.groundId===toId)return true;
    let jump=false;
    if(p.onGround&&state.time-lastJump>.18){jump=true;lastJump=state.time;}
    else if(p.wall&&state.time-lastJump>.18){jump=true;direction=-p.wall;lastJump=state.time;}
    if(p.y+p.h<goal.y-6)direction=Math.sign(goal.x+goal.w/2-p.x-p.w/2)||direction;
    else if(p.x<=shaft.left+2)direction=1;
    else if(p.x+p.w>=shaft.right-2)direction=-1;
    if(!tick(state,direction,jump))return false;
  }
  return false;
}
function dropToSurface(initial,toId) {
  const state=cloneProof(initial),p=state.player,goal=surface(state.world,toId);
  if(!goal)return null;
  const source=surface(state.world,p.groundId);
  const aim=Math.max(goal.x+20,Math.min(goal.x+goal.w-p.w-20,p.x));
  if(!walkOnSurface(state,source.id,aim))return null;
  for(let step=0;step<600;step++) {
    const target=surface(state.world,toId),distance=(target.w>1000?aim:target.x+target.w/2-p.w/2)-p.x;
    const axis=p.x<target.x+6?1:p.x+p.w>target.x+target.w-6?-1:Math.abs(distance)<12?0:Math.sign(distance);
    if(!tick(state,axis,step===0,true,step===0))return null;
    if(p.onGround&&p.groundId===toId)return state;
    if(step>3&&p.onGround&&p.groundId!==source.id&&p.groundId!==toId)return null;
  }
  return null;
}

const explorationCache=new Map();
export function validateExplorationRoom(room,physics=PHYSICS) {
  const key=JSON.stringify([room.platforms,room.movers,room.switches,room.pickupSpawns,room.explorationRoute,room.alternateExplorationRoute,physics]);
  if(explorationCache.has(key))return explorationCache.get(key);
  const result={valid:false,exploration:true,baseAbilities:true,wallJump:room.chamberStyle==="shaft",phaseProofs:[],edges:[],surfaceCount:room.platforms.length+room.movers.length,visitedCount:0};
  const save=reason=>{result.reason=reason;explorationCache.set(key,result);return result;};
  const gate=room.platforms.find(p=>p.mandatory),ceiling=room.platforms.find(p=>p.kind==="ceiling");
  if(!gate||gate.oneWay||gate.y>-700||gate.y+gate.h<1000)return save("mandatory gate does not seal the complete world height");
  if(!ceiling||ceiling.oneWay||ceiling.x>gate.x||ceiling.x+ceiling.w<gate.x+gate.w||ceiling.y+ceiling.h<gate.y)return save("mandatory gate has no connected solid ceiling");
  if(!room.platforms.some(p=>p.kind==="ground"&&!p.oneWay&&p.x<=gate.x&&p.x+p.w>=gate.x+gate.w&&p.y<=VIEW.floorY&&p.y+p.h>=1000))return save("mandatory gate has no connected solid floor");
  if(!gate.unlockLocals?.length||gate.unlockLocals.some(i=>!room.switches[i]))return save("mandatory gate references missing mechanisms");
  const cycle=Math.max(4,...room.movers.map(m=>m.period||7));
  const routes=[room.explorationRoute,...(room.alternateExplorationRoute?[room.alternateExplorationRoute]:[])];
  for(let routeIndex=0;routeIndex<routes.length;routeIndex++)for(const startTime of room.movers.length?[0,cycle*.23,cycle*.51,cycle*.79]:[0]) {
    const world=explorationWorld(room,startTime),points=routes[routeIndex],entry=surface(world,pointId(points[0]));
    if(!entry)return save("missing exploration entrance");
    const p=createPlayer(entry.x+30,entry.y-physics.playerHeight);p.onGround=true;p.groundId=entry.id;
    let state={world,player:p,time:startTime,jumps:0,exploration:true};
    const visited=[];
    for(let i=0;i<points.length;i++) {
      const point=points[i],id=pointId(point),pad=surface(state.world,id);
      if(!pad||pad.requires)return save("exploration route requires a missing support or optional power");
      if(i>0&&id!==state.player.groundId) {
        const source=surface(state.world,state.player.groundId);
        if(point.drop||pad.w>1000&&pad.y>source.y&&source.oneWay) {
          const dropped=dropToSurface(state,id);
          if(!dropped)return save(`no safe return drop ${source.id} -> ${id}`);
          state=dropped;
        } else if(point.wallShaft) {
          if(!climbShaft(state,id,point.wallShaft))return save(`wall shaft cannot reach ${id}`);
        } else {
          const solutions=transfer(state,state.player.groundId,id,Math.max(12,cycle*1.5),{exploration:true,targetX:point.landingX});
          if(!solutions.length)return save(`no safe exploration transfer ${state.player.groundId} -> ${id} at ${startTime.toFixed(2)}`);
          state=solutions[0];
        }
      }
      if(point.x!=null&&!walkOnSurface(state,id,point.x))return save(`cannot walk to exploration waypoint ${id}/${point.x}`);
      for(const local of point.unlockLocals||[]) {
        const mechanism=state.world.switches[local];
        if(!walkOnSurface(state,id,mechanism.x))return save(`cannot reach mechanism ${local} on ${id}`);
        for(let step=0;step<6;step++)if(!tick(state))return save("mechanism collection leaves no safe footing");
        if(!mechanism.activated)return save(`mechanism ${local} never activates through the runtime pickup/switch system`);
      }
      if(point.returnToGate) {
        if(!state.world.switches.every(s=>s.activated))return save("return reaches a gate with missing unlock items");
        for(let step=0;step<240&&!state.world.platforms.find(p=>p.mandatory).open;step++)if(!tick(state))return save("gate opening wait is unsafe");
        if(!state.world.platforms.find(p=>p.mandatory).open)return save("correct mechanisms do not open the gate");
      }
      visited.push(id);
    }
    if(!points.at(-1).exit||state.player.x<gate.x+gate.w)return save("exploration proof never crosses the opened gate");
    result.phaseProofs.push({routeIndex,startTime,elapsed:state.time-startTime,jumps:state.jumps,supports:visited,
      activated:state.world.switches.filter(s=>s.activated).map(s=>s.id),crossedGate:true});
    result.edges=visited.slice(1).map((id,i)=>[visited[i],id]);result.visitedCount=new Set(visited).size;
  }
  result.valid=true;return save("continuous base-controller entry, mechanisms, safe return and animated gate crossing replayed for every route and mover phase");
}

export const ROOM_VALIDATION_PROOF_SIGNATURE=[validateStaticRoom,validateDynamicRoom,validateExplorationRoom,canReachSurface,simulateSurfaceJump,
  proofWorld,explorationWorld,routeIds,surface,tick,control,transfer,walkOnSurface,climbShaft,dropToSurface]
  .map(fn=>fn.toString().replace(/\s+/g," ")).join("|");

export function validateLevel(world) {
  const errors=[],modules=new Set();
  for(const descriptor of world.chunkPlan||[]) {
    if(!descriptor.validation?.valid)errors.push({chunk:descriptor.index,reason:descriptor.validation?.reason||"unvalidated chunk"});
    modules.add(descriptor.moduleId);
    if(descriptor.entryY!==descriptor.exitY)errors.push({chunk:descriptor.index,reason:"unsafe chunk seam"});
  }
  return{valid:errors.length===0,chunkCount:world.chunkCount||world.chunkPlan?.length||0,moduleCount:modules.size,errors};
}
