import assert from "node:assert/strict";
import {PHYSICS} from "../js/config.js";
import {createPlayer,updatePlayer,updateMovers,updateWorldEnvironment,surfaceY,sampleMover,syncMovers,sampleHazard,syncHazards,hazardHitsPlayer} from "../js/physics.js";
import {DYNAMIC_ROOM_LIBRARY} from "../js/world/dynamic-rooms.js";
import {validateRoom} from "../js/world/room-validator.js";

const dt=PHYSICS.fixedStep,clone=value=>JSON.parse(JSON.stringify(value));
const input=(axis=0,jump=false)=>({horizontal:()=>axis,jumpPressed:()=>jump,jumpHeld:()=>true,dashPressed:()=>false,downHeld:()=>false});
const world=(platforms=[],movers=[],hazards=[])=>({width:3200,platforms,movers,hazards,switches:[],effects:[],bounds:{left:0},deathY:1000});
const rider=(mover,fraction=.35)=>{
  const x=mover.x+mover.w*fraction-PHYSICS.playerWidth/2;
  const p=createPlayer(x,surfaceY(mover,x+PHYSICS.playerWidth/2)-PHYSICS.playerHeight);
  p.onGround=true;p.groundId=mover.id;return p;
};
const close=(a,b,message)=>assert.ok(Math.abs(a-b)<1e-7,`${message}: ${a} versus ${b}`);

// Two complete periods include both reversals and all pivot angles. Positions
// are tested against the actual collision plane, rather than a decorative sprite.
let ridden=0;
for(const room of DYNAMIC_ROOM_LIBRARY)for(const source of room.movers) {
  if(source.motion === "timed")continue;
  const m={...clone(source),id:`ride-${ridden++}`},w=world([],[m]);syncMovers(w,0);
  const p=rider(m),steps=Math.ceil((m.period || Math.PI*2/m.speed)*2/dt);
  for(let step=1;step<=steps;step++) {
    updateMovers(w,step*dt,dt);updatePlayer(p,w,input(),dt);
    assert.ok(p.onGround && p.groundId===m.id,`${room.id}/${m.motion}: rider stays attached through both reversals`);
    close(p.y+p.h,surfaceY(m,p.x+p.w/2),"feet match moving contact plane");
    close((p.x+p.w/2-m.x)/m.w,.35,"rider keeps contact position");
  }
  const at=sampleMover(m,123.456);syncMovers(w,123.456);
  close(m.x,at.x,"streamed synchronization samples exact x");close(m.y,at.y,"streamed synchronization samples exact y");
  assert.equal(m.dx,0);assert.equal(m.dy,0);
}

// A coplanar dock must not steal an idle rider at the ferry's turnaround.
{
  const m={id:"dock-ferry",x:100,y:468,baseX:100,baseY:468,w:150,h:24,oneWay:true,kind:"moving",motion:"ferry",axis:"x",range:90,period:2};
  const dock={id:"dock",x:0,y:468,w:150,h:500,kind:"ground",oneWay:false};
  const w=world([dock],[m]);syncMovers(w,0);const p=rider(m,.3);
  for(let step=1;step<=480;step++) {
    updateMovers(w,step*dt,dt);updatePlayer(p,w,input(),dt);
    assert.equal(p.groundId,m.id,"returning raft retains passenger on contact with its dock");
  }
}

// Carry displacement is swept against walls, independently of walking velocity.
{
  const m={id:"bank-ferry",x:100,y:468,baseX:100,baseY:468,w:150,h:24,oneWay:true,kind:"moving",motion:"ferry",axis:"x",range:120,period:2};
  const bank={id:"bank",x:280,y:400,w:500,h:500,kind:"ground",oneWay:false};
  const w=world([bank],[m]);syncMovers(w,0);const p=rider(m,.85);
  for(let step=1;step<=60;step++) {
    updateMovers(w,step*dt,dt);updatePlayer(p,w,input(),dt);
    assert.ok(p.x+p.w<=bank.x+.001,"moving deck cannot carry its passenger through a raised bank");
  }
}

// Relative sweeps catch a rising lift overtaking an upward-moving player, and
// refuse underside catches when a one-way deck descends past the player.
{
  const m={id:"rising",x:100,y:380,previousY:420,dy:-40,dx:0,w:180,h:24,oneWay:true,kind:"moving"};
  const w=world([],[m]),p=createPlayer(130,400-PHYSICS.playerHeight);p.vy=-100;
  updatePlayer(p,w,input(),dt);assert.equal(p.groundId,m.id);close(p.y+p.h,380,"fast rising lift catches relative downward crossing");
  const descending={...m,id:"descending",y:420,previousY:400,dy:20};
  const below=createPlayer(130,410-PHYSICS.playerHeight);below.vy=1280;
  updatePlayer(below,world([],[descending]),input(),dt);
  assert.equal(below.onGround,false,"descending platform never snaps a player who started below its old top");
}
{
  const source=DYNAMIC_ROOM_LIBRARY.flatMap(r=>r.movers).find(m=>m.motion === "pivot");
  const m={...clone(source),id:"pivot-takeoff"},w=world([],[m]);syncMovers(w,0);const p=rider(m,.6);
  updateMovers(w,dt,dt);updatePlayer(p,w,input(1,true),dt);
  assert.ok(!p.onGround && p.vy < -800,"slope contact snap cannot cancel a jump from a pivot bridge");
}

// Retraction has a visible advance warning, drops its collider at the same
// instant, and deterministically returns within one period from every phase.
{
  const m={id:"returning",x:100,y:400,baseX:100,baseY:400,w:220,h:24,oneWay:true,kind:"moving",motion:"timed",period:4,warningDuration:.5,activeDuration:2,retractDuration:.25};
  const before=sampleMover(m,2.2);assert.ok(before.warning && before.solid && before.warningMode === "retraction");
  const retract=sampleMover(m,2.6);assert.ok(retract.retracting && !retract.solid && !retract.hidden);
  const hidden=sampleMover(m,3.1);assert.ok(hidden.hidden && !hidden.solid);
  for(let phase=0;phase<4;phase+=.13) {
    let returns=false;for(let elapsed=0;elapsed<=4;elapsed+=.05)if(sampleMover(m,phase+elapsed).solid){returns=true;break;}
    assert.ok(returns,"every phase has a returning collision surface within one period");
  }
  const w=world([],[m]);syncMovers(w,2.4);const p=rider(m);
  updateMovers(w,2.51,dt);updatePlayer(p,w,input(),dt);
  assert.ok(!p.onGround && p.y+p.h>400,"retracted surface cannot keep an invisible passenger standing");
}

const hazardTemplates=[
  {type:"pendulum",x:100,y:100,w:44,h:44,pivotX:150,pivotY:80,length:145,ballRadius:22,angleRange:.65},
  {type:"crusher",x:100,y:100,baseY:100,w:70,h:60,travel:160},
  {type:"retractSpikes",x:100,y:400,baseY:400,w:90,h:46,maxHeight:46},
  {type:"rotatingBeam",x:100,y:100,w:160,h:20,pivotX:180,pivotY:180,length:160,radius:10},
  {type:"timedGate",x:100,y:200,w:40,h:150},
].map(h=>({...h,period:5.8,warningDuration:1.2,activeDuration:1,phase:0}));
for(const template of hazardTemplates) {
  const warning=sampleHazard(template,.3);assert.ok(warning.warning && !warning.active,`${template.type}: warning cannot deal damage`);
  const active={...template,...sampleHazard(template,1.7)};
  assert.ok(active.active && active.progress>.4 && active.progress<.6);
  const p=createPlayer(active.x+active.w/2-PHYSICS.playerWidth/2,active.y+active.h/2-PHYSICS.playerHeight/2);
  assert.ok(hazardHitsPlayer(active,p),`${template.type}: actual visible mechanism intersects player`);
  const w=world([],[],[clone(template)]);updateWorldEnvironment(w,p,dt,.3);assert.equal(p.hearts,4);
  const q=createPlayer(active.x+active.w/2-PHYSICS.playerWidth/2,active.y+active.h/2-PHYSICS.playerHeight/2);
  updateWorldEnvironment(w,q,dt,1.7);assert.equal(q.hearts,3,`${template.type}: damage uses the sampled active shape`);
  const shifted={...template,timeOffset:2};assert.deepEqual(sampleHazard(shifted,1),sampleHazard(template,3),"hazard clock offsets match moved-platform clocks");
}
{
  const beam={type:"rotatingBeam",active:true,startX:0,startY:0,endX:100,endY:0,radius:10};
  assert.equal(hazardHitsPlayer(beam,{x:109,y:9,w:1,h:1}),false,"beam's rounded tip cannot damage an empty corner of its rectangular bounds");
  assert.equal(hazardHitsPlayer(beam,{x:105,y:5,w:1,h:1}),true);
  const spikes={type:"retractSpikes",active:true,x:0,y:0,w:120,h:100};
  assert.equal(hazardHitsPlayer(spikes,{x:0,y:0,w:2,h:2}),false,"spike tip corners contain transparent air, not a rectangular damage box");
  assert.equal(hazardHitsPlayer(spikes,{x:14,y:0,w:2,h:2}),true,"actual triangular spike tip remains dangerous");
  const crusher=clone(hazardTemplates[1]),w=world([],[],[crusher]);syncHazards(w,.3);const raised=crusher.y;syncHazards(w,1.7);
  close(crusher.previousY,raised,"crusher publishes prior position for synchronized interpolation");
  const streamed=clone(hazardTemplates[1]),streamedWorld=world([],[],[streamed]);syncHazards(streamedWorld,1.7);
  close(streamed.previousY,streamed.y,"fresh streamed crusher does not interpolate from its raised spawn position");
  const rock={type:"fallingRock",x:100,y:390,w:38,h:78,period:4.2,warningDuration:.9,activeDuration:.65};
  const rockWorld=world([],[],[rock]);syncHazards(rockWorld,1.2);
  close(rock.previousY,rock.y,"fresh streamed rock starts at its actual cycle position");
  close(rock.targetY,424,"rock impact floor is derived from its original authored bounds");
  const rockTarget=rock.targetY;syncHazards(rockWorld,4.2+1.2);close(rock.targetY,rockTarget,"rock impact target survives repeated clock synchronization");
  const wind={type:"wind",x:0,y:0,w:1000,h:1000,force:120,phase:.4},p=createPlayer(100,100),q=clone(p);
  updateWorldEnvironment(world([],[],[{...wind,timeOffset:2}]),p,dt,1);updateWorldEnvironment(world([],[],[{...wind}]),q,dt,3);
  close(p.vx,q.vx,"wind uses the same deterministic time offset as platforms");
}

// The dynamic validator is conservative: no static bypass, missing support,
// broken ferry, or optional upgrade shelf is accepted as a mandatory route.
{
  const original=DYNAMIC_ROOM_LIBRARY.find(r=>r.id === "reed-ferry"),valid=validateRoom(original);
  assert.ok(valid.valid);assert.equal(valid.phaseProofs.length,4);assert.equal(valid.baseAbilities,true);
  assert.strictEqual(validateRoom(original),valid,"unchanged geometry reuses its live proof");
  const broken=clone(original);broken.movers[0].range=0;assert.equal(validateRoom(broken).valid,false,"stationary ferry across a huge gap is rejected");
  const optional=clone(original);optional.platforms[0].requires="wings";assert.equal(validateRoom(optional).valid,false,"optional upgrades never justify a required route");
  const bypass={id:"bypass",width:3200,requiresDynamic:true,platforms:[{x:0,y:468,w:3200,h:500,kind:"ground",entry:true,exit:true}],movers:[{x:100,y:200,baseX:100,baseY:200,w:100,h:24,motion:"ferry",axis:"x",range:30}],hazards:[]};
  assert.equal(validateRoom(bypass).valid,false,"a decorative mover over a complete static route cannot satisfy the requirement");
}

console.log(`Verified ${ridden} moving supports for two full cycles, coplanar docks, carried wall collision, relative lift landings, pivot takeoff, timed recovery, five synchronized hazards, and invalid-route rejection.`);
