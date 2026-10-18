'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const feedback=require('../game/neon-arena/neon-combat-feedback.js');
const source=fs.readFileSync(path.join(__dirname,'../game/neon-arena/neon-arena.js'),'utf8');
function getFunction(name){const start=source.indexOf('function '+name+'('),end=source.indexOf('\nfunction ',start+1);return source.slice(start,end)}
test('authored material metadata selects distinct environmental reactions',()=>{
  for(const [material,expected] of [['steel_panel','metal'],['concrete','concrete'],['glass_window','glass'],['organic_membrane','organic'],['energy_shield','shield']])assert.equal(feedback.surfaceFor({structure:{material}}),expected);
  assert.equal(feedback.surfaceFor({},'living_hive'),'organic');assert.equal(feedback.surfaceFor({},'xenofall_wastes'),'concrete');
  assert.equal(new Set(Object.values(feedback.SURFACES).map(s=>s.sound)).size,5);
});
test('shield, armor, health, headshot and defeat feedback remain distinguishable',()=>{
  assert.equal(feedback.feedbackKind({shield:true,armor:15}),'shield');assert.equal(feedback.feedbackKind({armor:15}),'armor');assert.equal(feedback.feedbackKind({}),'health');assert.equal(feedback.feedbackKind({head:true}),'headshot');assert.equal(feedback.feedbackKind({head:true,killed:true}),'kill');
});
test('sound fades to silence at range and each weapon has a bounded firing profile',()=>{
  assert.equal(feedback.attenuation(52),0);assert.equal(feedback.attenuation(0),1);assert(feedback.attenuation(25)<.3);
  assert.equal(Object.keys(feedback.FIRING).length,20);for(const p of Object.values(feedback.FIRING)){assert(p.decay<=.24);assert(p.smoke<=3);assert(p.halo<=1)}
});

test('six ballistic weapons have distinct recoil and flashes, with subdued suppressor effects',()=>{
  const p=feedback.BALLISTICS;assert.equal(Object.keys(p).length,6);
  assert(p.clawbreaker.kick[0]>p.whisker.kick[0]);assert(p.purr90.kick[0]<p.whisker.kick[0]);
  assert(p.burrower.flash<p.whisker.flash*.4);assert(p.burrower.life<p.whisker.life);
  assert(p.squeakshot.speed>p.whisker.speed);assert(p.thunderpaw.tail>p.purr90.tail);
  for(const v of Object.values(p)){assert(v.life<=.1);assert(v.tail<=10);assert(v.width<=2.1);}
});

test('traveling tracers advance, stay within the physical hit and disappear at the endpoint',()=>{
  const shot={ballistic:true,len:50,speed:500,tail:5,maxLife:.13,life:.13};
  let last=0;
  for(const life of [.13,.11,.08,.05,.02,0]){const w=feedback.tracerWindow({...shot,life});assert(w.start>=last);assert(w.end<=50);assert(w.start>=0);assert(w.end>=w.start);assert(w.end-w.start<=5.000001);last=w.start;}
  assert.equal(feedback.tracerWindow({...shot,life:0}).alpha,0);
  assert.deepEqual(feedback.tracerWindow({len:50,life:.1}),{start:0,end:50,alpha:1},'energy beams retain their complete span');
  assert.equal(feedback.tracerWindow({...shot,len:0}).alpha,0);
  assert(feedback.tracerWindow({...shot,len:.25}).end<=.25,'close cover is not overshot');
});

test('gunfire stereo follows facing and stays centered for the firing player',()=>{
  const listener={x:0,z:0,yaw:0};
  assert(feedback.stereoPan(listener,{x:10,z:0})>.7);
  assert(feedback.stereoPan(listener,{x:-10,z:0})<-.7);
  assert.equal(feedback.stereoPan(listener,{x:0,z:15}),0);
  assert.equal(feedback.stereoPan(listener,{x:.1,z:.2}),0);
  assert.equal(feedback.stereoPan(listener,{x:.28,z:.58}),0,'the nearby first-person muzzle remains centered');
  assert(feedback.stereoPan({...listener,yaw:Math.PI},{x:10,z:0})<-.7);
});

test('close shots survive their first frame at 30 or 60 Hz and expire even if never visible',()=>{
  for(const dt of [1/30,1/60])for(const p of Object.values(feedback.BALLISTICS)){
    const life=5/p.speed+.025,context=vm.createContext({shots:[{ballistic:true,firstFrame:true,len:5,speed:p.speed,tail:p.tail,life,maxLife:life}]});
    vm.runInContext(getFunction('ageShotTrails')+';this.age=ageShotTrails',context);
    context.age(dt);assert.equal(context.shots.length,1);assert(feedback.tracerWindow(context.shots[0]).alpha>0);
    for(let i=0;i<30;i++)context.age(dt);assert.equal(context.shots.length,0,'offscreen shots do not accumulate');
  }
});
test('rays beginning inside physical cover stop immediately rather than hitting behind it',()=>{
  const context=vm.createContext({Math});vm.runInContext(getFunction('rayBox')+';this.rayBox=rayBox',context);
  const box={x:0,y:1,z:2,w:2,h:2,d:2};
  const inside=context.rayBox({x:0,y:1,z:2},{x:0,y:0,z:1},box,30);assert(inside);assert.equal(inside.dist,0);assert.equal(inside.z,2);
  const outside=context.rayBox({x:0,y:1,z:-2},{x:0,y:0,z:1},box,30);assert.equal(outside.dist,3);assert.equal(outside.normal.z,-1);
  assert.equal(context.rayBox({x:4,y:1,z:-2},{x:0,y:0,z:1},box,30),null);
});
test('tracers terminate exactly on collision surfaces from an offset muzzle',()=>{
  const context=vm.createContext({Math});vm.runInContext(getFunction('tracerToHit')+';this.trace=tracerToHit',context);
  const trace=context.trace({x:.28,y:1.6,z:.58},{x:0,y:1.81,z:0},{x:0,y:0,z:1},5);
  assert(Math.abs(trace.x+trace.dx*trace.len)<1e-9);assert(Math.abs(trace.y+trace.dy*trace.len-1.81)<1e-9);assert(Math.abs(trace.z+trace.dz*trace.len-5)<1e-9);
});
test('authoritative hitscan stops at cover, produces one actual muzzle event and uses the front collision point',()=>{
  const shooter={x:0,y:1.86,z:0,weapon:'pistol'},target={x:0,y:1.86,z:10},events=[],shots=[];
  const context=vm.createContext({Math,shooter,target,shots,
    ultimateActive:()=>false,ultimateClipActive:()=>false,hasUpgrade:()=>false,dirFrom:()=>({x:0,y:0,z:1}),
    actorEyePosition:e=>({x:e.x,y:e.y,z:e.z}),muzzlePoint:()=>({x:.28,y:1.72,z:.58}),targetsFor:()=>[target],
    rayEntity:()=>({dist:10,head:false}),rayWorld:()=>({dist:5,x:0,y:1.81,z:5,normal:{x:0,y:0,z:-1},block:{structure:{material:'metal'}}}),
    addMuzzleFlash:()=>events.push('muzzle'),addFloorDecal:h=>events.push(['decal',h.z]),damageStructure:()=>events.push('cover'),surfaceImpact:h=>events.push(['impact',h.z]),damage:()=>events.push('damage'),
    clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),rand:()=>0
  });
  vm.runInContext(getFunction('tracerToHit')+'\n'+getFunction('fireHitscan')+'\nfireHitscan(shooter,true,{kind:"hitscan",spread:0,range:40,color:"#ffffff",damage:25,falloff:0});',context);
  assert.equal(events.filter(e=>e==='muzzle').length,1);assert(!events.includes('damage'));assert(events.some(e=>Array.isArray(e)&&e[0]==='impact'&&e[1]===5));assert.equal(shots.length,1);
});
test('material fragments begin on the near side of a hit and obey the particle budget',()=>{
  const particles=[],impacts=[],sounds=[];const context=vm.createContext({Math,NeonCombatFeedback:feedback,particles,game:{mode:'dm',map:'grid'},ARENA_TILESETS:{grid:'breach_ring'},
    scaledVisualCount:n=>n,rand:(a,b)=>(a+b)/2,addImpact:h=>impacts.push(h),trimVisualArray:(a,n)=>{if(a.length>n)a.splice(0,a.length-n)},visualPreset:()=>({maxParticles:12}),combatAudio:(...args)=>sounds.push(args)});
  vm.runInContext(getFunction('surfaceImpact')+';this.impact=surfaceImpact',context);const hit={x:0,y:1,z:5,normal:{x:0,y:0,z:-1},block:{structure:{material:'metal'}}};context.impact(hit);context.impact(hit);
  assert.equal(impacts.length,1,'same physical impact cannot emit twice');assert(particles.every(p=>p.z<5&&p.vz<0));assert.equal(sounds[0][0],'impact:metal');
  for(let i=0;i<10;i++)context.impact({...hit,surfaceFx:false});assert(particles.length<=12);
});
test('explosive splash respects the same physical cover as bullets',()=>{
  const course=[{x:0,y:2,z:4,w:8,h:4,d:1,kind:'wall'}],context=vm.createContext({Math,PLAYER_H:1.86,blocksAlongRay:()=>course,alienTargetY:(_a,y)=>y,humanTargetY:a=>a.y-.7});
  vm.runInContext(['rayBox','rayWorld','lineClear','blastCanReach'].map(getFunction).join('\n')+';this.blast=blastCanReach',context);
  const target={x:0,y:1.86,z:8};assert.equal(context.blast(0,1,0,target),false);assert.equal(context.blast(0,1,6,target),true);course.length=0;assert.equal(context.blast(0,1,0,target),true);
});
test('weapon interaction cannot collect a nearby pickup through solid cover',()=>{
  const context=vm.createContext({game:{mode:'siege'},pickups:[{x:0,z:1,y:.7,kind:'weapon',taken:0}],hypot2:Math.hypot,actorEyePosition:e=>e,lineClear:()=>false});
  vm.runInContext(getFunction('nearestWeaponPickup')+';this.nearest=nearestWeaponPickup',context);const actor={x:0,y:1.86,z:0};assert.equal(context.nearest(actor),null);context.lineClear=()=>true;assert.equal(context.nearest(actor).z,1);
});
test('sliding head hitboxes lower with the model and cannot be hit above its compressed height',()=>{
  const context=vm.createContext({Math,PLAYER_H:1.86,HUMAN_VISUAL_WIDTH:1.10,humanTargetY:(t,kind)=>t.y-1.86+(kind==='head'?2.10:1.20)*(1-.27*(t.slidePose||0))});
  vm.runInContext(getFunction('rayEntity')+';this.hit=rayEntity',context);
  const target={x:0,y:1.86,z:10,r:.5,slidePose:1},dir={x:0,y:0,z:1};
  assert.equal(context.hit({x:0,y:1.88,z:0},dir,target,20),null,'bullets above the visible sliding body must miss');
  assert.equal(context.hit({x:0,y:2.10*.73,z:0},dir,target,20)?.head,true);
  assert.equal(context.hit({x:0,y:.36*.73,z:0},dir,target,20)?.head,false);
  target.slidePose=0;assert.equal(context.hit({x:0,y:2.10,z:0},dir,target,20)?.head,true);
});
