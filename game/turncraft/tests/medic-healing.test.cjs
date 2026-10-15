const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const vm=require('node:vm');
const {SpriteRuntime,TextureCache,frameForState,healingPresentation}=require('../animation.js');
const manifest=require('../assets/animation-manifest.json');
const {collectUrls}=require('../preload.js');
const medic=()=>({id:1,type:'medic',kind:'unit',alive:true,faction:0,x:2,y:3,px:2,py:3,hp:75,maxHp:75,order:'idle',healOrder:'idle',healAbilityReadyAt:0,healTargetId:2,healStartedAt:1000,healUntil:2400,healRange:3});
const patient=()=>({id:2,type:'rifleman',kind:'unit',alive:true,faction:0,x:4,y:3,px:4,py:3,hp:30,maxHp:100});
const context={save(){},restore(){},translate(){},rotate(){},beginPath(){},moveTo(){},lineTo(){},stroke(){},arc(){},fill(){},drawImage(){},globalAlpha:1};

test('Medic healing supplies eight real direction rows and four disjoint animation phases with stable ground anchors',()=>{
  const asset=manifest.assets.medic,seen=new Set();
  for(const [name,column] of [['healPrepare',0],['healActivate',2],['healLoop',4],['healComplete',6]]){
    const state=asset.states[name];assert.deepEqual(state.directions,['N','NE','E','SE','S','SW','W','NW']);assert.equal(state.firstColumn,column);
    for(const direction of state.directions)for(let i=0;i<2;i++){
      const frame=frameForState(state,direction,i?1000/state.fps:0);assert.equal(frame.index,i);assert.ok(frame.x+frame.w<=1024&&frame.y+frame.h<=1280);
      seen.add(`${frame.x},${frame.y}`);assert.ok(asset.healing.deviceAnchors[direction][column+i].every(Number.isFinite));assert.deepEqual(state.anchor,[.5,.9125]);
    }
  }
  assert.equal(seen.size,64);assert.equal(asset.states.healLoop.loop,true);assert.equal(asset.states.healComplete.loop,false);
  const index=require('../assets/visual-asset-index.json');
  for(const key of ['units/medic-healing.webp','effects/medic-healing.webp']){
    const data=fs.readFileSync(path.resolve(__dirname,'../../..',key));assert.equal(data.length,index.images[key].bytes);assert.equal(crypto.createHash('sha256').update(data).digest('hex'),index.images[key].sha256);assert.equal(data.toString('ascii',8,12),'WEBP');
  }
});

test('remote snapshots reproduce preparation, activation, loop, completion, idle and interpolated beam endpoints',()=>{
  const runtime=new SpriteRuntime({manifest}),healer=medic(),target=patient();
  runtime.beginFrame([healer,target],1000);assert.equal(runtime.states.get(1).state,'healPrepare');assert.equal(runtime.healingEndpoints(healer,1000),null);
  runtime.beginFrame([healer,target],1140);assert.equal(runtime.states.get(1).state,'healActivate');assert.equal(runtime.healingEndpoints(healer,1140),null);
  runtime.beginFrame([healer,target],1300);assert.equal(runtime.states.get(1).state,'healLoop');assert.equal(runtime.states.get(1).direction,'E');
  const first=runtime.healingEndpoints(healer,1300,{size:48,worldToScreen:(x,y)=>({x:x*40,y:y*40})});assert.ok(first.origin.x>100&&first.target.x===180);
  target.x=3;target.px=4;target.moveReadyAt=2200;healer.y=4;healer.py=3;healer.moveReadyAt=2200;
  runtime.beginFrame([healer,target],1400,{remote:true,moveDuration:()=>800});runtime.beginFrame([healer,target],1800,{remote:true,moveDuration:()=>800});
  const moving=runtime.healingEndpoints(healer,1800,{size:48,worldToScreen:(x,y)=>({x:x*40,y:y*40})});assert.ok(moving.target.x<first.target.x);assert.notEqual(moving.origin.y,first.origin.y);assert.equal(target.x,3);assert.equal(healer.y,4);assert.equal(healer.hp,75);
  runtime.beginFrame([healer,target],2350);runtime.beginFrame([healer,target],2400);assert.equal(runtime.healingEndpoints(healer,2400),null);assert.equal(runtime.states.get(1).state,'healComplete');
  runtime.beginFrame([healer,target],2621);assert.equal(runtime.states.get(1).state,'idle');
});

test('healing beam stops in the interrupting frame for orders, attacks, abilities, death, transport, hostility, range and fog',()=>{
  for(const interrupt of [
    (h,t)=>h.order='stop',(h,t)=>h.attackAnim={startAt:1301,endAt:1800},(h,t)=>h.abilityReadyAt=4000,
    (h,t)=>h.alive=false,(h,t)=>t.alive=false,(h,t)=>h.garrisonedIn=9,(h,t)=>t.garrisonedIn=9,
    (h,t)=>t.faction=1,(h,t)=>t.x=9,(h,t)=>h.healTargetId=null
  ]){
    const runtime=new SpriteRuntime({manifest}),healer=medic(),target=patient();runtime.beginFrame([healer,target],1300);assert.ok(runtime.healingEndpoints(healer,1300));
    interrupt(healer,target);runtime.beginFrame([healer,target],1301);assert.equal(runtime.healingEndpoints(healer,1301),null);
  }
  const healer=medic(),target=patient(),index=new Map([[1,healer],[2,target]]);assert.equal(healingPresentation(healer,index,1300,e=>e.id===1),null);
});

test('graphics quality bounds healing beams and reduces traveling particles while retaining visible stream and body impact',()=>{
  for(const [quality,density,limit] of [['low',1,12],['medium',3,32],['high',5,64]]){
    let arcs=0,draws=0;const cache={setBudget(){},setResolution(){},setCropRepairs(){},setLocalFallbacks(){},beginFrame(){},evict(){},sourceScale:()=>1,get:()=>({width:1024,height:1280})},runtime=new SpriteRuntime({manifest,cache,quality});
    const entities=[];for(let i=0;i<70;i++){const healer={...medic(),id:i*2+1,healTargetId:i*2+2},target={...patient(),id:i*2+2};entities.push(healer,target)}
    runtime.beginFrame(entities,1300);const count=runtime.drawHealingEffects({...context,arc(){arcs++},drawImage(){draws++}},1350,{worldToScreen:(x,y)=>({x:x*48,y:y*48})});
    assert.equal(count,limit);assert.equal(arcs,limit*density);assert.equal(draws,limit*2,'each effect has a beam atlas frame and target-body impact frame');
  }
});

test('new artwork loads locally before R2 and falls back to canonical Cloudflare URLs when local files are unavailable',()=>{
  const requests=[],images=[],cache=new TextureCache({baseUrl:'https://game.test/game/turncraft.html',createImage:()=>{const image={width:1024,height:1280};Object.defineProperty(image,'src',{set(value){requests.push(value)}});images.push(image);return image}});
  cache.setLocalFallbacks(manifest.localFallbacks);const file=manifest.assets.medic.states.healLoop.sheet;cache.get(file);assert.equal(requests[0],'https://game.test/units/medic-healing.webp');images[0].onerror();assert.equal(requests[1],file);images[0].onload();assert.ok(cache.get(file));assert.equal(cache.active,0);
  const urls=collectUrls({visualManifest:require('../assets/visual-asset-index.json'),animationManifest:manifest,expansionManifest:{assets:{fixture:{path:'fixture.webp'}}},pageUrl:'https://game.test/game/turncraft.html',includeCommandScenes:false});
  assert.ok(urls.includes('https://game.test/units/medic-healing.webp'));assert.ok(urls.includes('https://game.test/effects/medic-healing.webp'));assert.equal(urls.includes(file),false);
});

test('actual healing mechanics retain amount, cooldown and injury priority while serializing observer animation IDs and deadlines',()=>{
  const source=fs.readFileSync(path.resolve(__dirname,'../../turncraft.html'),'utf8'),start=source.indexOf('function updateRegeneration(dt)'),end=source.indexOf('// ---------- Frontier bunker garrison',start),healer={...medic(),healTargetId:null,supportReadyAt:0},target=patient(),healthier={...patient(),id:3,x:3,hp:80},floats=[];
  const sandbox={window:{TurncraftAnimation:{healingPresentation}},entities:[healer,healthier,target],UNITS:{medic:{heal:{amount:10,range:3,cooldown:1.5}}},nowMs:()=>1000,broodRegenRate:()=>0,hasResearch:()=>false,hasTag:()=>true,dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),scaledTime:s=>s*1.15,COOLDOWN_SCALE:{ability:1.15},floating:floats,markDirty(){}};
  vm.createContext(sandbox);vm.runInContext(source.slice(start,end),sandbox);vm.runInContext('updateRegeneration(.1)',sandbox);
  assert.equal(target.hp,40);assert.equal(healthier.hp,80);assert.ok(Math.abs(healer.supportReadyAt-2983.75)<1e-8);assert.equal(healer.healTargetId,2);assert.equal(healer.healStartedAt,1000);assert.equal(healer.healUntil,2400);assert.equal(floats.length,1);
  const serialize=source.match(/function serializeEntity\(e\)\{[^\n]+/)[0];vm.runInContext(serialize,sandbox);sandbox.entity=healer;const snapshot=vm.runInContext('serializeEntity(entity)',sandbox);assert.equal(snapshot.healTargetId,2);assert.equal(snapshot.healUntil,2400);assert.equal(snapshot.px,undefined);
  const observer=new SpriteRuntime({manifest});observer.beginFrame([JSON.parse(JSON.stringify(snapshot)),target,healthier],1350,{remote:true});assert.equal(observer.states.get(1).state,'healLoop');assert.ok(observer.healingEndpoints(snapshot,1350));
});
