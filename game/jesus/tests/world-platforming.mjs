import assert from "node:assert/strict";
import { PHYSICS, VIEW } from "../js/config.js";
import { createPlayer, updatePlayer, updateMovers, updateWorldEnvironment, surfaceY, damagePlayer } from "../js/physics.js";
import { Camera } from "../js/camera.js";
import { ROOM_LIBRARY } from "../js/world/rooms.js";
import { validateRoom, canReachSurface, simulateSurfaceJump } from "../js/world/room-validator.js";
import { createLevel, updateWorldStreaming, persistWorldState, restoreWorldState, BIOME_ORDER, CHUNK_WIDTH } from "../js/world/generator.js";
const dt=PHYSICS.fixedStep;
const input=(axis=0,jump=false,held=true,dash=false,down=false)=>({horizontal:()=>axis,jumpPressed:()=>jump,jumpHeld:()=>held,dashPressed:()=>dash,downHeld:()=>down});
const floor={id:"floor",x:0,y:VIEW.floorY,w:2400,h:600,kind:"ground",oneWay:false};
const world=(platforms=[{...floor}],movers=[])=>({width:3000,platforms,movers,hazards:[],switches:[],bounds:{left:0,top:-700,bottom:1000}});
const grounded=(x=80,y=VIEW.floorY-PHYSICS.playerHeight)=>{const p=createPlayer(x,y);p.onGround=true;p.groundId="floor";return p;};
assert.ok(ROOM_LIBRARY.length>=36,"dozens of authored modules exist");
for(const room of ROOM_LIBRARY){
 const v=validateRoom(room,PHYSICS,{dash:false,wallJump:false,wings:false});assert.ok(v.valid,`${room.id}: ${v.reason}`);
 const route=room.platforms.filter(p=>p.entry||p.route||p.exit);
 for(let i=1;i<route.length;i++)assert.ok(simulateSurfaceJump(route[i-1],route[i],{...room,platforms:route},{abilities:{dash:false,wallJump:false}}),`${room.id} critical jump ${i} uses base abilities`);
 for(const sw of room.switches)assert.ok(route.some(p=>sw.x>=p.x&&sw.x+sw.w<=p.x+p.w&&Math.abs(sw.y+20-p.y)<1),`${room.id} pressure switch stands on its approach surface`);
}
for(const room of ROOM_LIBRARY)for(const source of room.movers){
 const mover={...source,id:'long-ride',phase:0,dx:0,dy:0};
 assert.ok(mover.range>=(mover.axis==='x'?200:140),'Moving platforms cover a useful longer route');
 const w=world([],[mover]),p=grounded(mover.x+20,mover.y-PHYSICS.playerHeight);p.groundId=mover.id;
 let min=Infinity,max=-Infinity;
 const steps=Math.ceil(2*Math.PI/mover.speed/dt)+1;
 for(let i=1;i<=steps;i++){
  updateMovers(w,i*dt,dt);updatePlayer(p,w,input(),dt);
  const value=mover.axis==='x'?mover.x:mover.y;min=Math.min(min,value);max=Math.max(max,value);
  assert.ok(p.onGround&&p.groundId===mover.id,`${room.id}/${mover.axis} keeps its passenger through a complete cycle`);
  assert.ok(Math.abs(p.x-mover.x-20)<.001&&Math.abs(p.y+p.h-mover.y)<.001,'Rider stays on the actual moving collision surface');
 }
 assert.ok(max-min>mover.range*1.99,'Travel reaches both ends instead of only changing a range label');
}
assert.equal(canReachSurface({x:0,y:468,w:100},{x:500,y:100,w:100},PHYSICS,{dash:false,wings:false}),false,"unavailable wings cannot justify an impossible jump");
{
 const from={x:0,y:468,w:200,entry:true,oneWay:true,kind:"platform"};
 const high={x:245,y:230,w:200,exit:true,oneWay:true,kind:"platform"},far={x:520,y:468,w:200,exit:true,oneWay:true,kind:"platform"};
 const r={width:900,platforms:[from,high]};
 assert.equal(validateRoom(r,PHYSICS,{wings:false,dash:false}).valid,false);
 assert.equal(validateRoom(r,PHYSICS,{wings:true,dash:false}).valid,true,"real double jump opens a previously unreachable shelf");
 r.platforms=[from,far];assert.equal(validateRoom(r,PHYSICS,{wings:false,dash:false}).valid,false);
 assert.equal(validateRoom(r,PHYSICS,{wings:false,dash:true}).valid,true,"real dash opens a previously unreachable gap");
}
const fingerprint=w=>JSON.stringify({plan:w.chunkPlan.map(c=>[c.moduleId,c.seed,c.rest]),platforms:w.platforms,enemies:w.enemies,pickups:w.pickups});
for(const biome of BIOME_ORDER){
 const a=createLevel({biome,seed:123456,difficulty:4}),b=createLevel({biome,seed:123456,difficulty:4});
 assert.equal(fingerprint(a),fingerprint(b),`${biome} generation and IDs are deterministic`);assert.ok(a.routeValid);
 assert.ok(a.width>=(BIOME_ORDER.indexOf(biome)<2?200000:300000)&&a.width<=350000,"long travel budgets");
 assert.ok(a.checkpoints.length>=9);assert.ok(new Set(a.chunkPlan.map(c=>c.moduleId)).size>=10,"worlds rotate distinct layouts");
 const secretIds=new Set(),platformStyles=new Set();
 for(let x=0;x<a.width;x+=CHUNK_WIDTH/2){
  updateWorldStreaming(a,x,x-400);assert.ok(a.chunks.length<=4&&a._resident.size<=4,"resident chunks bounded");assert.ok(a.platforms.length<110&&a.enemies.length<=30&&a.hazards.length<=20,"resident entities bounded");
  for(const p of a.platforms)if(p.oneWay)platformStyles.add(p.visualKind);
  for(const p of a.pickups){
   if(p.secret)secretIds.add(p.id);
   if(["bread","fish","chest"].includes(p.type)){
    const support=a.platforms.find(surface=>surface.id===p.groundId);assert.ok(p.grounded&&support,"ground objects identify actual support");
    assert.equal(p.y+p.h,p.groundY,"ground object collider rests on its support");assert.equal(p.groundY,surfaceY(support,p.x+p.w/2));
   }
  }
  for(const c of a.checkpoints.filter(c=>a.chunks.some(chunk=>chunk.index===c.chunkIndex))){
   const support=a.platforms.find(p=>p.id===c.groundId);assert.ok(support,"checkpoint support remains resident");
   assert.equal(c.floorY,surfaceY(support,c.x+PHYSICS.playerWidth/2));assert.equal(c.y+PHYSICS.playerHeight,c.floorY,"checkpoint player spawn stands on the ground");
  }
 }
 assert.ok(platformStyles.has("bridge")&&platformStyles.has("supported")&&platformStyles.has("ledge"),"authored platforms mix bridges, supported terrain, and ledges without altering colliders");
 assert.equal(a.totalSecrets,secretIds.size,"secret denominator equals every actual collectible across streamed chunks");
 assert.equal(a.totalSecrets,a.chunkPlan.reduce((sum,c)=>sum+c.secretCount,0));assert.ok(a.totalSecrets>0);
 assert.ok(a.chunkPlan.every(c=>Array.isArray(c.tags)),"future hazards are visible to level asset planning");
 const finalChunk=a.chunks.at(-1);assert.equal(finalChunk.pickups.filter(p=>p.secret).length,0,"boss lead-in has no unreachable post-transition secrets");
 assert.ok(finalChunk.platforms.every(p=>!p.branch),"boss lead-in removes lower chambers that would lose their return stairs");
 assert.ok(validateRoom({width:a.width,platforms:finalChunk.platforms}).valid,"actual stamped final geometry is reachable");
 assert.ok(a.chunkPlan.at(-1).validation.valid,"final arena connection validated");updateWorldStreaming(a,100,0);
 const pickup=a.pickups[0],enemy=a.enemies[0];pickup.collected=true;if(enemy)enemy.dead=true;a.checkpoints[0].activated=true;
 const snap=persistWorldState(a);updateWorldStreaming(a,a.width-2000);updateWorldStreaming(a,100);
 assert.ok(a.pickups.find(p=>p.id===pickup.id)?.collected,"rewards survive eviction");if(enemy)assert.ok(a.enemies.find(p=>p.id===enemy.id)?.dead,"kills survive eviction");
 const restored=createLevel({biome,seed:123456,difficulty:4});restoreWorldState(restored,JSON.parse(JSON.stringify(snap)),100);
 assert.ok(restored.pickups.find(p=>p.id===pickup.id)?.collected&&restored.checkpoints[0].activated,"checkpoint JSON roundtrip");
}
// Include submerged-Sanctum seed202: a prior fixed arena stair left a tiny pad.
const fuzzSeeds=[1,42,101,202,303,404,505,606,0xC0FFEE,0xDEADBEEF];
const abilityProfiles=[{}, {sandals:true}, {wings:true}, {sandals:true,wings:true}];
let fuzzedWorlds=0;
for(const biome of BIOME_ORDER)for(let difficulty=1;difficulty<=6;difficulty++)for(const seed of fuzzSeeds)for(const abilities of abilityProfiles){
 const w=createLevel({biome,difficulty,seed,abilities});
 assert.ok(w.routeValid,`${biome}/tier${difficulty}/seed${seed}/${JSON.stringify(abilities)} final join must be reachable`);
 updateWorldStreaming(w,w.width-500,w.width-960);
 assert.ok(w.chunks.at(-1).platforms.every(p=>p.w>=PHYSICS.requiredLandingWidth),"final joins must not leave tiny clipped landing pads");
 assert.ok(w.chunks.length<=4&&w.chunkPlan.at(-1).validation.valid);fuzzedWorlds++;
}
function apex(heldFrames){const p=grounded(),w=world();let min=p.y;for(let i=0;i<150;i++){updatePlayer(p,w,input(0,i===0,i<heldFrames),dt);min=Math.min(min,p.y);}return VIEW.floorY-p.h-min;}
assert.ok(apex(90)>140&&apex(1)<75,"variable jump height reacts to release");
{
 const w=world([{...floor,w:100}]),p=grounded(65);p.vx=430;updatePlayer(p,w,input(1),dt);
 while(p.onGround)updatePlayer(p,w,input(1),dt);for(let i=0;i<4;i++)updatePlayer(p,w,input(1),dt);updatePlayer(p,w,input(1,true),dt);assert.ok(p.vy<-700,"coyote jump");
}
{
 const w=world(),p=createPlayer(100,VIEW.floorY-PHYSICS.playerHeight-20);p.vy=300;let buffered=false;
 for(let i=0;i<30;i++){updatePlayer(p,w,input(0,i===0),dt);if(p.vy<-700)buffered=true;}assert.ok(buffered,"jump buffer survives until landing");
}
{
 const wall={id:"wall",x:220,y:0,w:38,h:700,kind:"wall",oneWay:false},w=world([floor,wall]),p=grounded(145);
 updatePlayer(p,w,input(1,false,true,true),dt);assert.ok(p.vx===PHYSICS.dashSpeed&&p.dashTimer>0);
 for(let i=0;i<25;i++)updatePlayer(p,w,input(1),dt);assert.ok(p.x+p.w<=wall.x+0.1,"dash sweeps walls");
 const air=createPlayer(400,100);updatePlayer(air,w,input(1,false,true,true),dt);assert.ok(air.airDashUsed);air.dashCooldown=0;air.dashTimer=0;updatePlayer(air,w,input(-1,false,true,true),dt);assert.equal(air.dashTimer,0,"one air dash per landing");
}
{
 const wall={id:"wall",x:250,y:-200,w:40,h:680,kind:"wall",oneWay:false},w=world([wall]),p=createPlayer(215,100);p.vx=200;p.vy=700;
 updatePlayer(p,w,input(1),dt);assert.equal(p.wall,1);assert.ok(p.vy<=PHYSICS.wallSlideSpeed);updatePlayer(p,w,input(1,true),dt);assert.ok(p.vx<0&&p.vy<-650,"wall jump launches outward");
}
{
 const ceiling={id:"ceiling",x:0,y:250,w:400,h:30,kind:"wall",oneWay:false},w=world([floor,ceiling]),p=grounded();let blocked=false;
 for(let i=0;i<80;i++){updatePlayer(p,w,input(0,i===0),dt);if(p.y===280&&p.vy===0)blocked=true;}assert.ok(blocked,"ceiling collision");
 const q=grounded(),one=world([floor,{...ceiling,oneWay:true}]);let above=false;for(let i=0;i<100;i++){updatePlayer(q,one,input(0,i===0),dt);if(q.y<275)above=true;}assert.ok(above,"one-way ascent passes the solid underside");
}
{
 const mover={id:"lift",x:100,y:400,baseX:100,baseY:400,w:180,h:20,oneWay:true,kind:"moving",axis:"x",range:90,speed:1,phase:0,dx:0,dy:0},w=world([],[mover]),p=grounded(140,338);p.groundId="lift";
 updateMovers(w,dt,dt);updatePlayer(p,w,input(),dt);assert.ok(p.x>140&&p.onGround);updatePlayer(p,w,input(0,true,true,false,true),dt);assert.ok(p.dropTimer>0&&!p.onGround&&p.y>338,"drop through ride");
}
{
 const up={id:"up",x:100,y:420,w:180,h:100,rise:48,kind:"slope",slopeDirection:"up",oneWay:false},down={id:"down",x:280,y:420,w:180,h:100,rise:48,kind:"slope",slopeDirection:"down",oneWay:false};
 const w=world([floor,up,down]),p=grounded(82);let count=0;for(let i=0;i<100;i++){updatePlayer(p,w,input(1),dt);if(p.groundId==="up"||p.groundId==="down"){count++;assert.ok(Math.abs(p.y+p.h-surfaceY(p.groundId==="up"?up:down,p.x+p.w/2))<0.01);}}assert.ok(count>30,"continuous slope traversal");
}
{
 const platform={id:"collapse",x:0,y:468,w:200,h:24,kind:"collapsing",oneWay:true},w=world([platform]),p=grounded();p.groundId="collapse";
 for(let i=0;i<110;i++)updateWorldEnvironment(w,p,dt,i*dt);assert.ok(platform.dead,"collapse delayed");p.groundId=null;for(let i=0;i<500;i++)updateWorldEnvironment(w,p,dt,1+i*dt);assert.equal(platform.dead,false,"collapse recovers");
 w.hazards=[{type:"lightning",x:0,y:0,w:100,h:500,phase:0,period:4.2,warningDuration:0.9,activeDuration:0.65}];p.x=20;p.y=100;
 updateWorldEnvironment(w,p,dt,.3);assert.ok(w.hazards[0].warning&&!w.hazards[0].active&&p.hearts===4);updateWorldEnvironment(w,p,dt,1.1);assert.ok(w.hazards[0].active&&p.hearts===3,"hazard warning then damage");
 const s=grounded();s.powers.shield=true;assert.equal(damagePlayer(s),false);assert.equal(s.hearts,4);assert.equal(s.powers.shield,false);
}
{
 const rock={type:"fallingRock",x:80,y:390,w:38,h:78,phase:0,period:4.2,warningDuration:.9,activeDuration:.65};
 const w=world(),p=grounded(80);w.hazards=[rock];updateWorldEnvironment(w,p,dt,.3);const warningY=rock.y;
 assert.ok(rock.warning&&rock.telegraph&&p.hearts===4);updateWorldEnvironment(w,p,dt,1.2);assert.ok(rock.y>warningY,"rocks physically descend after warning");
 updateWorldEnvironment(w,p,dt,1.52);assert.equal(p.hearts,3,"falling rock collides at its actual descending position");
 const gate={id:"gate",x:300,y:300,w:40,h:168,kind:"gate",oneWay:false,switchId:"switch"};
 w.platforms.push(gate);w.switches=[{id:"switch",x:80,y:406,w:44,h:25,activated:false}];w.hazards=[];
 updateWorldEnvironment(w,p,dt,2);assert.ok(w.switches[0].activated&&gate.open,"touching a pressure switch opens its reward gate");
}
{
 const c=new Camera(),p=grounded(),w={width:320000,bounds:{top:-700,bottom:1000}};c.reset(p,w);p.x=30000;p.y=-500;p.vx=430;
 for(let i=0;i<120;i++)c.update(p,dt,w);assert.ok(c.x>29000&&c.y<-400);c.focusArena({left:30000,right:31540,top:-220,bottom:1000});c.update(p,dt,w);assert.ok(c.x>=30000&&c.x<=31540-VIEW.width);
}
{
 const p=grounded(1100,100-PHYSICS.playerHeight);p.facing=1;
 const lower={id:'landing',x:1250,y:400,w:240,h:28,oneWay:true},w=world([{id:'high',x:1000,y:100,w:200,h:28,oneWay:true},lower]);
 const c=new Camera();c.reset(p,w);
 assert.ok(lower.y-c.y<VIEW.height-80,'A lower landing is visible before stepping off the high ledge');
 assert.ok(p.y-c.y>=64,'Automatic drop framing keeps the player in view');
 const flat=world(),q=grounded(1100),look=new Camera();look.reset(q,flat);const neutral=look.y;
 for(let i=0;i<120;i++)look.update(q,dt,flat,{lookDown:true});
 assert.ok(look.y-neutral>140,'Holding down reveals substantially more ground below');
 assert.ok(q.y-look.y>=60,'Manual look-down never pushes the player above the screen');
 for(let i=0;i<120;i++)look.update(q,dt,flat);
 assert.ok(Math.abs(look.y-neutral)<20,'Camera returns smoothly after releasing down');
 const original={width:VIEW.width,height:VIEW.height};
 try{VIEW.width=360;VIEW.height=779;look.reset(p,w);for(let i=0;i<120;i++)look.update(p,dt,w,{lookDown:true});
  assert.ok(p.y-look.y>=60&&lower.y-look.y<VIEW.height-60,'Portrait look-down retains both player and landing');
 }finally{Object.assign(VIEW,original);}
}
console.log(`Validated ${ROOM_LIBRARY.length} modules with real base movement, six deterministic long worlds, ${fuzzedWorlds} seeded world/power/difficulty combinations, bounded streaming, state restoration, long moving-platform rides, and downward camera framing.`);
