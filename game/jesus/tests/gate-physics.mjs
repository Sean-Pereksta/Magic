import assert from 'node:assert/strict';
import { PHYSICS, VIEW } from '../js/config.js';
import { createPlayer, mandatoryGateCollider, updatePlayer, updateWorldEnvironment } from '../js/physics.js';
import { updateGateProgress } from '../js/world/gate-progress.js';
import { updateEnemies, ENEMY_TYPES } from '../js/enemies/enemies.js';
import { Camera } from '../js/camera.js';
import { Renderer } from '../js/render/renderer.js';

const dt=PHYSICS.fixedStep;
const input=(axis=1,jump=false,dash=false)=>({horizontal:()=>axis,jumpPressed:()=>jump,jumpHeld:()=>true,dashPressed:()=>dash});
const gate=()=>({id:'gate-a',kind:'gate',mandatory:true,exploration:true,x:500,y:-700,w:64,h:1700,frameTop:-700,frameBottom:1000,unlockIds:['key-a']});
const world=()=>({width:2400,height:1000,layoutVersion:3,deathY:1000,bounds:{top:-700,bottom:1000},
  platforms:[{id:'floor',x:0,y:468,w:2400,h:600},gate()],movers:[],hazards:[],
  switches:[{id:'key-a',exploration:true,kind:'key',x:100,y:100,w:28,h:28}],pickups:[{id:'pickup-a',type:'gateKey',unlockId:'key-a',gateId:'gate-a',x:100,y:100,w:28,h:28}],effects:[]});

// Sweep both faces at every height in the live passage, including exact cap
// and floor joins. Wind, wings and a long dash step cannot tunnel through it.
let sweeps=0;
for(const y of [-700,-699,-400,-1,200,405,900,999])for(const direction of [-1,1]){
  const w=world(),p=createPlayer(direction>0?430:600,y);
  p.powers.wings=true;p.powers.sandals=true;p.boosts.wind=10;
  p.facing=direction;p.vx=direction*PHYSICS.dashSpeed;
  updatePlayer(p,w,input(direction,true,true),.2);
  assert.ok(direction>0?p.x+p.w<=500+.001:p.x>=564-.001,`sealed gate face at y=${y}, direction ${direction}`);
  sweeps++;
}
{
  const w=world(),p=createPlayer(460,406);p.onGround=true;p.groundId='floor';
  p.powers.wings=true;p.boosts.wind=100;
  for(let frame=0;frame<2400;frame++){
    updatePlayer(p,w,input(1,frame%23===0,frame%41===0),dt);
    assert.ok(p.x+p.w<=500+.001,'repeated powered jumps, wall jumps and dashes remain on the locked side');
    assert.ok(p.y>=-700,'physical upper world bound closes the roof edge');
  }
}
// A ferry can sweep a passenger horizontally and vertically in the same step;
// even a carry beyond the roof or death plane must not cross a locked seal.
for(const dy of [-1600,-100,0,1200])for(const dx of [-700,700]){
  const w=world(),start=dx>0?350:680;
  const mover={id:'ferry',x:start+dx,y:468+dy,w:150,h:24,oneWay:true,dx,dy};
  w.movers.push(mover);const p=createPlayer(start+20,406);p.onGround=true;p.groundId='ferry';
  updatePlayer(p,w,input(0),dt);
  assert.ok(dx>0?p.x+p.w<=500+.001:p.x>=564-.001,'diagonal ferry carry respects the entire sealed frame');
}
{
  const w=world(),p=createPlayer(430,406),g=w.platforms[1];
  Object.assign(g,{y:350,h:30,oneWay:true,hidden:true,solid:false});
  const collider=mandatoryGateCollider(g,w);
  assert.equal(collider.y,-700);assert.ok(collider.y+collider.h>=1062);assert.equal(collider.oneWay,false);
  updatePlayer(p,w,input(1,false,true),.2);assert.ok(p.x+p.w<=500,'mandatory collision does not depend on decorative or one-way flags');
  // Unrelated switch and a claimed pickup never bypass a live required guardian.
  const sw=w.switches[0];sw.guardianId='guardian';w.enemies=[{id:'guardian',dead:false}];w.pickups[0].collected=true;
  updateGateProgress(w,p,2);assert.ok(!g.open&&!sw.activated&&w.pickups[0].locked);
  w.enemies[0].dead=true;updateGateProgress(w,p,.45);
  assert.ok(g.opening&&!g.open&&g.openProgress===.45,'correct identity begins a visible opening');
  updatePlayer(p,w,input(1,false,true),.2);assert.ok(p.x+p.w<=500,'collision remains through the opening animation');
  updateGateProgress(w,p,.55);assert.ok(g.open&&!g.opening);
  p.dashCooldown=0;updatePlayer(p,w,input(1,false,true),.2);assert.ok(p.x>500,'finished opening is physically passable');
}
{
  const w=world(),p=createPlayer(100,100);w.switches[0].activated=false;
  updateWorldEnvironment(w,p,dt);assert.equal(w.switches[0].activated,false,'legacy environment cannot bypass exploration guardian checks');
  w.switches.push({id:'old',x:100,y:100,w:34,h:62});w.platforms.push({id:'old-gate',kind:'gate',switchId:'old',x:1800,y:300,w:40,h:168});
  updateWorldEnvironment(w,p,dt);assert.equal(w.platforms.at(-1).open,true,'legacy pressure gates keep their existing behavior');
}
{
  const w=world(),p=createPlayer(100,406),guardian={id:'elite',type:'javelin',guardian:true,elite:true,hp:5,x:350,y:412,w:34,h:56};w.enemies=[guardian];
  updateEnemies(w,p,dt);assert.ok(guardian.maxHp>=5&&guardian.maxHp>ENEMY_TYPES.javelin.hp);
  guardian.aiState='tell';guardian.stateTime=0;guardian.targetX=p.x;guardian.targetY=p.y;
  updateEnemies(w,p,dt);assert.equal(w.enemyProjectiles.length,3,'elite ranged attack uses a telegraphed fan while ordinary enemies retain one shot');
  const flying={id:'upper-guardian',type:'fire-skull',elite:true,guardian:true,x:1600,y:-160,w:36,h:40};
  w.enemies=[flying];p.x=1500;p.y=-180;
  updateEnemies(w,p,dt);assert.ok(flying.y<0,'flying guardians remain in elevated chambers above the original ground camera range');
}
{
  const w=world(),p=createPlayer(150,406),camera=new Camera();
  const before=camera.verticalTarget(p,w);w.platforms.push({id:'climb',exploration:true,x:190,y:205,w:140,h:24,oneWay:true});
  assert.ok(camera.verticalTarget(p,w)<before,'nearby next climbing shelf is revealed before takeoff');
  camera.reset(p,w);
  for(let frame=0;frame<300;frame++){
    const oldY=camera.y;p.y=406-frame*2;p.vy=-240;camera.update(p,dt,w);
    assert.ok(Math.abs(camera.y-oldY)<12,'climb follows smoothly');
    assert.ok(p.y-camera.y>=60&&p.y+p.h-camera.y<=VIEW.height-60,'player retains headroom and landing visibility');
  }
  p.y=600;p.vy=1000;camera.update(p,dt,w);
  assert.ok(p.y+p.h-camera.y<=VIEW.height-60,'fast descent keeps player in view above lower landings');
}
// Exercise every texture-free procedural fallback and opening stage. Required
// graphics stay unchanged; loading an optional sheet is never necessary here.
{
  let marks=0;
  const ctx=new Proxy({createLinearGradient:()=>({addColorStop(){}})}, {get:(target,key)=>target[key]||((...args)=>{for(const n of args)if(typeof n==='number')assert.ok(Number.isFinite(n));marks++;}),set:(target,key,value)=>(target[key]=value,true)});
  const renderer=new Renderer({getContext:()=>ctx},{get:()=>null});
  for(const appearance of ['slab','boulder','temple','rubble','fortress','seal','cave','iron'])for(const progress of [0,.2,.7,1]){
    const g={...gate(),appearance,openProgress:progress,opening:progress>0&&progress<1,open:progress===1};
    renderer._gate(g,'mountains',{x:300,y:0,w:960,h:540},progress,{switches:[]});
  }
  renderer._chamber({x:0,y:-300,w:1650,h:780,chamberStyle:'cave'},'mountains',{x:0,y:0,w:960,h:540},2);
  renderer._pickup({type:'gateKey',x:200,y:300,w:28,h:28,gateId:'gate-a'},2);
  renderer._pickup({type:'gateRelic',x:200,y:300,w:28,h:28,gateId:'gate-a'},2);
  assert.ok(marks>100);assert.equal(renderer.error,null,'new barriers, atmosphere and keys need no added remote assets');
}
console.log(`Gate physics: ${sweeps} full-height sweeps, powered wall-jump endurance, diagonal carries, guarded animated unlock, legacy gates, elite patterns, camera and all eight visual fallbacks passed.`);
