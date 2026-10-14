'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../game/neon-arena/neon-arena.js'),'utf8');
const start=source.indexOf('function actorOverSurface('),end=source.indexOf('function recoverActor(',start);
assert(start>0&&end>start,'movement source functions exist');
const constants=source.match(/const JUMP_SPEED=.*?;/)[0];
const movement=source.slice(start,end);
function block(x,z,w,d,top,h=.55,kind='platform'){return {x,z,w,d,y:top-h/2,h,kind}}
const floor=()=>block(0,0,120,120,0,1);
function harness(course=[floor()],options={}){
  const actor={x:0,y:1.86,z:0,vx:0,vy:0,vz:0,r:.5,onGround:true,human:true,dead:false,weapon:'pistol',...options};
  const effects=[],state={course,recovered:false,dash:false,dashes:0};
  const context=vm.createContext({Math,Number,game:{started:true,over:false,upgradesOpen:false,shopOpen:false},GRAV:34,PLAYER_H:1.86,HEAD_CLEAR:.18,MAP_LIMIT:69,ARENA_SAFE:64.3,player:actor,
    clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),isHumanActor:e=>!!e.human,
    blocksNear:(x,z,r)=>state.course.filter(b=>Math.abs(x-b.x)<=b.w/2+r&&Math.abs(z-b.z)<=b.d/2+r),
    burst:(...args)=>effects.push(args),recoverActor:e=>{state.recovered=true;e.x=e.z=0;e.y=1.86;e.vx=e.vy=e.vz=0;e.onGround=true},
    tryVoltDash:e=>{if(!state.dash)return false;state.dash=false;state.dashes++;e.vx=29;e.vy=Math.max(e.vy,4);return true}
  });
  vm.runInContext(`${constants}\n${movement}\nthis.api={physics,requestHumanJump,resetHumanJump,verticalCollide,pushOutXZ,hasGroundSupport,requestHumanSlide,updateTacticalSlide,updateSlideView,actorEyePosition,slideSpreadMultiplier,actorStandingHeight};`,context);
  context.api.resetHumanJump(actor);
  return {actor,state,effects,game:context.game,...context.api};
}
function advance(h,seconds,frame=1/60){for(let t=0;t<seconds-1e-9;){const dt=Math.min(frame,seconds-t);h.physics(h.actor,dt);t+=dt}}
function close(actual,expected,tolerance=.002){assert(Math.abs(actual-expected)<tolerance,`${actual} should be near ${expected}`)}
function slideAdvance(h,seconds,frame=1/60,wishX=0,wishZ=1){for(let t=0;t<seconds-1e-9;){const dt=Math.min(frame,seconds-t);h.updateTacticalSlide(h.actor,dt,wishX,wishZ);h.physics(h.actor,dt);h.updateSlideView(h.actor,dt);t+=dt}}

test('a tap starts a 720ms slide with decaying momentum and a real recovery cooldown',()=>{
  const h=harness([floor()],{vz:18.4});assert.equal(h.requestHumanSlide(h.actor),true);
  slideAdvance(h,.10);const early=Math.hypot(h.actor.vx,h.actor.vz);assert(early>25);assert(h.actor.slidePose>.7);
  slideAdvance(h,.50);assert(Math.hypot(h.actor.vx,h.actor.vz)<early*.7);assert(h.actor.slideTimer>0);
  slideAdvance(h,.13);assert.equal(h.actor.slideTimer,0);assert(h.actor.slideCooldown>.5);assert.equal(h.requestHumanSlide(h.actor),false);
  slideAdvance(h,.63);assert.equal(h.actor.slideCooldown,0);assert.equal(h.requestHumanSlide(h.actor),true);
});

test('slide starts in existing movement direction and only permits limited steering',()=>{
  const h=harness([floor()],{vx:18.4,vz:0,yaw:Math.PI});h.requestHumanSlide(h.actor);h.updateTacticalSlide(h.actor,.1,0,1);
  assert(h.actor.vx>26);assert(h.actor.vz>0&&h.actor.vz<3,'a 90 degree input cannot snap the trajectory');
  assert(Math.abs(Math.atan2(h.actor.vz,h.actor.vx))<=.089);
  h.updateTacticalSlide(h.actor,.1,0,0);assert(h.actor.vx>24,'releasing movement keeps momentum');
});

test('idle, airborne, dead and overlay slide taps cannot launch or queue an endless slide',()=>{
  const idle=harness();idle.requestHumanSlide(idle.actor);slideAdvance(idle,.2,1/60,0,0);assert.equal(idle.actor.slideTimer,0);assert.equal(idle.actor.slideRequest,0);
  for(const opts of [{onGround:false},{dead:true}]){const h=harness([floor()],opts);assert.equal(h.requestHumanSlide(h.actor),false)}
  const h=harness();for(const overlay of ['over','upgradesOpen','shopOpen']){h.game[overlay]=true;assert.equal(h.requestHumanSlide(h.actor),false);h.game[overlay]=false}
});

test('slides stop against thin walls and boundaries at normal and low frame rates',()=>{
  for(const dt of [1/60,.1,.2]){
    const h=harness([floor(),block(0,0,.12,20,4,4,'wall')],{x:-2,vx:18.4});h.requestHumanSlide(h.actor);slideAdvance(h,.22,dt,1,0);
    assert(h.actor.x<=-.56+.003);assert.equal(h.actor.slideTimer,0);assert(h.actor.slideCooldown>0);
  }
  const boundary=harness([floor()],{x:63,vx:18.4});boundary.requestHumanSlide(boundary.actor);slideAdvance(boundary,.2,.2,1,0);assert(boundary.actor.x<=63.8);assert.equal(boundary.actor.slideTimer,0);
});

test('jump transitions out of slide without clipping, double jumping or snapping the camera',()=>{
  const h=harness([floor()],{vz:18.4});h.requestHumanSlide(h.actor);slideAdvance(h,.16);const before=h.actor.slidePose;
  h.requestHumanJump(h.actor);slideAdvance(h,.02);assert.equal(h.actor.slideTimer,0);assert(h.actor.vy>22);assert(h.actor.vz>23);assert(h.actor.slidePose>0&&h.actor.slidePose<before);
  const vy=h.actor.vy;h.requestHumanJump(h.actor);slideAdvance(h,.05);assert(h.actor.vy<vy);
  slideAdvance(h,.5);assert(h.actor.slidePose<.01);
});

test('full model height collides with overhead structures even though the camera and slide fit below',()=>{
  for(const sliding of [false,true]){
    const h=harness([floor(),block(0,0,.55,10,2.6,.5,'wall')],{x:-2,vx:18.4});
    if(sliding){h.requestHumanSlide(h.actor);slideAdvance(h,.15,.1,1,0)}else advance(h,.15,.1);
    assert(h.actor.x<=-.775+.003);assert.equal(h.actorStandingHeight(h.actor),2.39);
  }
});

test('slide view easing and shooting origins agree without changing physics height or another player',()=>{
  const h=harness([floor()],{vz:18.4}),other={...h.actor};h.requestHumanSlide(h.actor);slideAdvance(h,.12);
  const eye=h.actorEyePosition(h.actor);assert(eye.y<h.actor.y-.5);assert(eye.z>h.actor.z);close(h.actor.y,1.86);assert(h.slideSpreadMultiplier(h.actor)>1.2);
  assert.equal(other.slideTimer,0);assert.equal(other.slidePose,0);
  h.resetHumanJump(h.actor);assert.equal(h.actor.slideCooldown,0);assert.equal(h.actor.slideTimer,0);assert.equal(h.actor.slidePose,0);
});

test('keyboard, controller and split-screen slide actions use independent tap edges',()=>{
  assert.match(source,/e\.code==='ShiftLeft'.*!e\.repeat\)requestHumanSlide\(player\)/);
  assert.match(source,/e\.code==='NumpadDecimal'.*!e\.repeat\)requestHumanSlide\(player2\)/);
  assert.match(source,/slide:10/);assert.match(source,/slide=padDown\(pad,s\.slide\?\?10\)/);
  assert.match(source,/if\(now\.slide&&!prev\.slide\)requestHumanSlide\(ent\)/);
  const markup=fs.readFileSync(path.join(__dirname,'../game/neon-arena/index.html'),'utf8');assert.match(markup,/id="padSlide"/);assert.match(markup,/id="mobileSlide"[^>]+aria-label="Slide"/);
  const a=harness([floor()],{vx:18.4}),b=harness([floor()],{vz:18.4});a.requestHumanSlide(a.actor);slideAdvance(a,.1,1/60,1,0);assert(a.actor.slideTimer>0);assert.equal(b.actor.slideTimer,0);b.requestHumanSlide(b.actor);slideAdvance(b,.1);assert(b.actor.slideTimer>0);
});

test('normal jump retains gravity 34 and impulse 23, and a second press cannot double jump',()=>{
  const h=harness();assert.equal(h.requestHumanJump(h.actor),true);h.physics(h.actor,1/120);
  close(h.actor.vy,23-34/120);assert.equal(h.actor.onGround,false);
  h.requestHumanJump(h.actor);advance(h,.25);assert(h.actor.vy<16);assert.equal(h.actor.jumpBuffer,0);
});

test('coyote requests work after leaving a ledge, then expire',()=>{
  const ledge=block(0,0,4,4,4);
  const h=harness([floor(),ledge],{x:2.2,y:5.86,vx:18.4});advance(h,.06);
  assert.equal(h.actor.onGround,false);assert(h.actor.coyoteTime>0);
  h.requestHumanJump(h.actor);h.physics(h.actor,.01);assert(h.actor.vy>22);
  const expired=harness([floor(),ledge],{x:2.2,y:5.86,vx:18.4});advance(expired,.20);expired.requestHumanJump(expired.actor);expired.physics(expired.actor,.01);
  assert(expired.actor.vy<0);assert(expired.actor.jumpBuffer>0);
});

test('jump request before landing fires on contact and consumes the buffer once',()=>{
  const h=harness([floor()],{y:2.15,vy:-8,onGround:false});h.requestHumanJump(h.actor);h.physics(h.actor,.06);
  assert(h.actor.y>1.86);assert(h.actor.vy>21);assert.equal(h.actor.jumpBuffer,0);assert.equal(h.actor.coyoteTime,0);
  const oldVy=h.actor.vy;h.physics(h.actor,.02);assert(h.actor.vy<oldVy);
});

test('expired buffers do not jump on a later landing',()=>{
  const h=harness([floor()],{y:6,vy:-2,onGround:false});h.requestHumanJump(h.actor);advance(h,.6,.05);
  close(h.actor.y,1.86);assert.equal(h.actor.onGround,true);assert.equal(h.actor.vy,0);
});

test('touch/controller request helper respects dead actors and overlays',()=>{
  const h=harness();h.game.upgradesOpen=true;assert.equal(h.requestHumanJump(h.actor),false);h.game.upgradesOpen=false;
  h.actor.dead=true;assert.equal(h.requestHumanJump(h.actor),false);h.actor.dead=false;h.game.shopOpen=true;assert.equal(h.requestHumanJump(h.actor),false);
});

test('an accessible attached platform catches descending feet before a side push at low FPS',()=>{
  const wall=block(-2.275,0,.55,4,4,4,'wall'),attached=block(0,0,4,4,4);
  for(const dt of [1/60,.10,.25]){
    const h=harness([floor(),wall,attached],{x:2.28,y:6.02,vx:-7,vy:-8,onGround:false});h.physics(h.actor,dt);
    close(h.actor.y,5.86);assert.equal(h.actor.onGround,true);assert(h.actor.x<2.28,'actor should move inward without a sideways landing rejection');
  }
});

test('corner landings use a small rounded foot tolerance and reject far-away corners',()=>{
  const ledge=block(0,0,4,4,4);
  const caught=harness([floor(),ledge],{x:2.20,z:2.20,y:6.1,vy:-10,onGround:false});caught.physics(caught.actor,.08);close(caught.actor.y,5.86);assert.equal(caught.actor.onGround,true);
  const missed=harness([floor(),ledge],{x:2.4,z:2.4,y:6.1,vy:-10,onGround:false});missed.physics(missed.actor,.08);assert.equal(missed.actor.onGround,false);assert(missed.actor.y<5.86);
});

test('connected platforms and a tiny seam remain grounded without jitter at normal and low FPS',()=>{
  for(const gap of [0,.06])for(const dt of [1/120,1/60,.10]){
    const h=harness([floor(),block(-2-gap/2,0,4,4,4),block(2+gap/2,0,4,4,4)],{x:-3,y:5.86,vx:18.4});
    advance(h,.32,dt);close(h.actor.y,5.86);assert.equal(h.actor.onGround,true);assert.equal(h.actor.vy,0);
    h.actor.vx=-18.4;h.requestHumanJump(h.actor);h.physics(h.actor,dt);assert(h.actor.vy>19);
  }
});

test('grounding assistance only snaps down close steps and never raises actors onto distant surfaces',()=>{
  const low=block(-2,0,4,4,4),lower=block(2,0,4,4,3.94);
  const h=harness([floor(),low,lower],{x:-.3,y:5.86,vx:12});advance(h,.1);close(h.actor.y,5.8);
  const upward=harness([floor(),block(0,0,4,4,4)],{y:5.5,vy:-1,onGround:false});upward.physics(upward.actor,.04);assert(upward.actor.y<5.5);assert.equal(upward.actor.onGround,false);
  const distant=harness([floor(),block(0,0,4,4,3.5)],{y:5.86,vy:0,onGround:true});distant.physics(distant.actor,.01);assert(distant.actor.y>5.8);assert.equal(distant.actor.onGround,false);
});

test('ascending head collisions stop below a platform without falsely grounding',()=>{
  const ceiling=block(0,0,4,4,4,.55),h=harness([floor(),ceiling],{y:3.2,vy:23,onGround:false});h.physics(h.actor,.10);
  assert(h.actor.y<=3.45-(2.39-1.86)+.001,'the complete 2.39 model head stays below the ceiling');assert(h.actor.vy<=0);assert.equal(h.actor.onGround,false);
});

test('swept vertical collision catches thin high platforms during very fast falls',()=>{
  const h=harness([floor(),block(0,0,4,4,8,.06)],{y:14,vy:-80,onGround:false});h.physics(h.actor,.1);
  close(h.actor.y,9.86);assert.equal(h.actor.onGround,true);assert.equal(h.state.recovered,false);
});

test('stacked wall tops are landable and wall faces cannot be crossed by dash or grapple speed',()=>{
  const stacked=block(0,0,.55,6,8,8,'wall'),landing=harness([floor(),stacked],{x:0,y:10.3,vy:-10,onGround:false});landing.physics(landing.actor,.10);close(landing.actor.y,9.86);assert.equal(landing.actor.onGround,true);
  for(const speed of [29,75]){const h=harness([floor(),stacked],{x:-4,vx:speed});h.physics(h.actor,.15);assert(h.actor.x<=-.775+.002);assert.equal(h.actor.vx,0)}
});

test('wall contact retains tangential movement so corners can be rounded',()=>{
  const h=harness([floor(),block(0,0,.55,20,4,4,'wall')],{x:-.8,z:-3,vx:10,vz:12});h.physics(h.actor,.2);
  assert(h.actor.x<=-.775+.002);assert(h.actor.z>-1);assert(h.actor.vz>11);
});

test('Volt Dash consumes jump input, retains its short impulse and cannot become a double jump',()=>{
  const h=harness();h.state.dash=true;h.requestHumanJump(h.actor);h.physics(h.actor,.02);
  assert.equal(h.state.dashes,1);assert(h.actor.vy>3&&h.actor.vy<4);assert(h.actor.vx===29);assert.equal(h.actor.onGround,false);
  h.requestHumanJump(h.actor);h.physics(h.actor,.02);assert(h.actor.vy<3);assert.equal(h.state.dashes,1);
});

test('external grapple launch cannot retain a grounded coyote jump',()=>{
  const h=harness();h.actor.vy=12;h.requestHumanJump(h.actor);h.physics(h.actor,.02);
  assert(h.actor.vy<12);assert.equal(h.actor.onGround,false);assert.equal(h.actor.coyoteTime,0);
});

test('spawn reset clears pending jumps and landing feedback',()=>{
  const h=harness();h.actor.jumpBuffer=.14;h.actor.coyoteTime=0;h.actor.landingImpact=.5;h.resetHumanJump(h.actor);
  assert.equal(h.actor.jumpBuffer,0);assert.equal(h.actor.landingImpact,0);close(h.actor.coyoteTime,.11);
  assert.match(source,/ent\.onGround=true;resetHumanJump\(ent\);ent\.dead=false/);
  assert.match(source,/if\(now\.jump&&!prev\.jump\)requestHumanJump\(ent\)/);
  assert.match(source,/e\.code==='Space'.*requestHumanJump\(player\)/);
});

function authoredMap(name){
  const course=[],platforms=[],walls=[],scale=1.45;
  const addPlat=(x,z,w,d,y,h)=>{const p={x:x*scale,z:z*scale,w:w*scale,d:d*scale,y,h};platforms.push(p);course.push({...block(p.x,p.z,p.w,p.d,y+h,h),structure:p});return p};
  const context=vm.createContext({Math,PLAYER_H:1.86,WORLD_SCALE:scale,game:{mode:'dm',map:name},mapTheme:null,edgeSpawns:[],nav:[],platforms,walls,
    clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),addPlat,addTopPlat:(x,z,w,d,top,h)=>addPlat(x,z,w,d,top-h,h),
    addWall:(x,z,w,d,h,c,y=0)=>{const p={x:x*scale,z:z*scale,w:w*scale,d:d*scale,y:y+h/2,h};walls.push(p);course.push({...block(p.x,p.z,p.w,p.d,y+h,h,'wall'),structure:p});return p},
    addPickup(){},addArmorPickup(){},addNav(){}});
  for(const name of ['addArenaBounds','buildSurvivorBreachMap']){const a=source.indexOf('function '+name+'('),b=source.indexOf('\nfunction ',a+10),marker=source.indexOf('// Authored standard arenas',a);vm.runInContext(source.slice(a,marker>a&&marker<b?marker:b),context)}
  vm.runInContext(source.slice(source.indexOf('// Authored standard arenas'),source.indexOf('function applyFragArena(')),context);
  const entry={grid:'buildStarterGridArenaMap',pastel:'buildPastelArenaMap',reactor:'buildReactorArenaMap',survivor:'buildSurvivorBreachMap'}[name];
  context[entry]();return course;
}

test('authored platforms on all four maps support landing and immediate repeat jumps at 60 and 10 FPS',()=>{
  for(const map of ['grid','pastel','reactor','survivor']){
    const course=authoredMap(map),clearPlatforms=course.filter(b=>b.kind==='platform'&&!course.some(other=>other!==b&&other.y+other.h/2>b.y+b.h/2+.01&&Math.abs(b.x-other.x)<other.w/2+.5&&Math.abs(b.z-other.z)<other.d/2+.5));
    assert(clearPlatforms.length>0,map+' needs an exposed surface');
    for(const platform of clearPlatforms.slice(0,4))for(const dt of [1/60,.1]){
      const top=platform.y+platform.h/2,h=harness(course,{x:platform.x,z:platform.z,y:top+1.86+.7,vy:-8,onGround:false});advance(h,.12,dt);
      close(h.actor.y,top+1.86);assert.equal(h.actor.onGround,true,map+' landing');
      h.requestHumanJump(h.actor);h.physics(h.actor,dt);assert(h.actor.vy>19,map+' repeat jump');assert.equal(h.state.recovered,false);
    }
  }
});
