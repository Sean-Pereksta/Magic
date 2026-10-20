import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {ASSET_CATALOG,DYNAMIC_ASSET_CATALOG,AssetManager,AssetLoadError,sampleFrame,namedFrame} from '../js/assets/asset-manager.js';
import {ASSET_CATALOG as LEGACY_CATALOG} from '../js/assets/catalog.js';
import {REGIONS} from '../js/world/regions.js';
import {Renderer,groundedArtPlacement,platformArtFrame,mechanismArtFrame} from '../js/render/renderer.js';
import {ART_ALPHA_BOUNDS,PLATFORM_SURFACES} from '../js/assets/art-anchors.js';
import {createRequire} from 'node:module';
import {sampleMover,sampleHazard} from '../js/physics.js';

const byPath=new Map(Object.values(ASSET_CATALOG).map(meta=>[meta.path,meta]));
assert.equal(Object.keys(LEGACY_CATALOG).length,263);
assert.equal(Object.keys(DYNAMIC_ASSET_CATALOG).length,15);
assert.equal(Object.keys(ASSET_CATALOG).length,278);
let frames=0,clips=0;
for(const meta of Object.values(ASSET_CATALOG)) {
  for(const frame of meta.frames) {
    frames++;
    assert(frame.rect.x>=0&&frame.rect.y>=0&&frame.rect.x+frame.rect.width<=meta.width&&frame.rect.y+frame.rect.height<=meta.height,meta.id);
    assert(Number.isFinite(frame.origin.x)&&Number.isFinite(frame.origin.y),meta.id);
  }
  for(const clip of Object.values(meta.animations||{})) {
    clips++;
    assert.equal(clip.durationMs,clip.frames.reduce((n,f)=>n+f[1],0));
    for(const [index,duration]of clip.frames){assert(meta.frames[index]);assert(duration>0);}
  }
  for(const name of Object.keys(meta.namedFrames||{}))assert(namedFrame(meta,name),meta.id+name);
}
assert.equal(Object.values(LEGACY_CATALOG).reduce((n,m)=>n+m.frames.length,0),1429);
assert.equal(Object.values(LEGACY_CATALOG).reduce((n,m)=>n+Object.keys(m.animations||{}).length,0),301);
assert.equal(frames,1517);
for(const meta of Object.values(DYNAMIC_ASSET_CATALOG))assert(!meta.path.includes('platforms/moving/'),'Uploaded biome graphics use flat platform paths.');
try {
  const manifest=JSON.parse(await readFile(new URL('../../../../../outputs/jesus-runner-platform-hazard-pack/_docs/assets.json',import.meta.url),'utf8'));
  for(const source of manifest.assets) {
    const packed=DYNAMIC_ASSET_CATALOG[source.id];
    assert.deepEqual(packed.frames,source.frames,'Preserve every measured source rectangle, origin and collision guide: '+source.id);
    assert.deepEqual(packed.animations,source.animations,source.id);
    assert.equal(packed.sha256,source.sha256);assert.equal(packed.path,source.path.replace(/^platforms\/moving\//,'platforms/'));
  }
} catch(error) {if(error.code!=='ENOENT')throw error;}
const idle=ASSET_CATALOG['player.jesus.idle'];
assert.equal(sampleFrame(idle,0).index,0);
assert.equal(sampleFrame(idle,.139999).index,0);
assert.equal(sampleFrame(idle,.14).index,1);
assert.equal(sampleFrame(idle,1.12).index,0);
const jump=ASSET_CATALOG['player.jesus.jump'];
assert.equal(sampleFrame(jump,100).index,jump.animations[jump.defaultAnimation].frames.at(-1)[0]);
const atlas=ASSET_CATALOG['collectibles-atlas'];
for(const name of ['bread','fish','secret-chest-closed','checkpoint-shrine','checkpoint-lantern','exit-gate-closed','exit-gate-open']) {
  const frame=namedFrame(atlas,name),placed=groundedArtPlacement(atlas,frame,120,468,name.startsWith('checkpoint')?112:46);
  assert.equal(placed.y+placed.h,468,'Visible alpha content must touch the ground plane: '+name);
  const b=ART_ALPHA_BOUNDS[`collectibles-atlas:${frame.index}`];
  assert.deepEqual(placed.source,{x:frame.rect.x+b.x,y:frame.rect.y+b.y,width:b.width,height:b.height});
}
assert.deepEqual(ART_ALPHA_BOUNDS['collectibles-atlas:8'],{x:34,y:8,width:189,height:216});
for(const [key,surface]of Object.entries(PLATFORM_SURFACES)) {
  const [id,index]=key.split(':'),f=ASSET_CATALOG[id].frames[Number(index)];
  assert(surface.y>=0&&surface.bottomY>surface.y&&surface.bottomY<=f.rect.height);
  assert(surface.spanX>=0&&surface.spanX+surface.spanWidth<=f.rect.width);
}

// When running in the authoring workspace, compare every packed rectangle, pivot,
// collider and duration against the unmodified first-stage source manifests.
try {
  const source=JSON.parse(await readFile(new URL('../../../../../outputs/jesus-runner-graphics/manifests/assets.json',import.meta.url),'utf8'));
  for(const original of source.assets) {
    const packed=ASSET_CATALOG[original.id];assert.equal(packed.path,original.path);
    for(const f of original.frames)for(const key of ['rect','origin','collider','contentBounds','tile'])assert.deepEqual(packed.frames[f.index][key],f[key],original.id+':'+key);
  }
  const sourceClips=JSON.parse(await readFile(new URL('../../../../../outputs/jesus-runner-graphics/manifests/animations.json',import.meta.url),'utf8'));
  for(const sheet of sourceClips.sheets)for(const [name,clip]of Object.entries(sheet.animations)) {
    const packed=ASSET_CATALOG[sheet.assetId].animations[name];
    assert.deepEqual(packed.frames,clip.frames.map(f=>[f.index,f.durationMs]));assert.equal(packed.loop,clip.loop);assert.equal(packed.holdLast,clip.holdLast);
  }
  // Source-pixel verification proves these are alpha bounds, not estimated cells
  // or hand-adjusted sprite offsets. This optional authoring check requires PNGs.
  const require=createRequire(import.meta.url),sharp=require('C:/Users/seanp/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/sharp');
  for(const id of [...new Set(Object.keys(ART_ALPHA_BOUNDS).map(key=>key.split(':')[0]))]) {
    const meta=ASSET_CATALOG[id],png=await readFile(new URL('../../../../../outputs/jesus-runner-graphics/'+meta.path,import.meta.url));
    const {data,info}=await sharp(png).ensureAlpha().raw().toBuffer({resolveWithObject:true});
    for(const frame of meta.frames) {
      const b=ART_ALPHA_BOUNDS[`${id}:${frame.index}`];if(!b)continue;
      let left=frame.rect.width,top=frame.rect.height,right=-1,bottom=-1;
      for(let y=0;y<frame.rect.height;y++)for(let x=0;x<frame.rect.width;x++)if(data[((frame.rect.y+y)*info.width+frame.rect.x+x)*4+3]>0){left=Math.min(left,x);right=Math.max(right,x);top=Math.min(top,y);bottom=Math.max(bottom,y);}
      assert.deepEqual(b,{x:left,y:top,width:right-left+1,height:bottom-top+1},id+':'+frame.index+' source alpha bounds');
      const deck=PLATFORM_SURFACES[`${id}:${frame.index}`];
      if(id.endsWith('-terrain')&&deck){
        let solid=0;
        for(let x=deck.x;x<deck.x+deck.width;x++)if(data[((frame.rect.y+deck.y)*info.width+frame.rect.x+x)*4+3]>=200)solid++;
        assert.equal(solid,deck.width,id+' uses an opaque stone contact row');
      }
    }
  }
} catch(error) {if(error.code!=='ENOENT')throw error;}

const requests=[],failureCounts=new Map();let active=0,maxActive=0;
class FakeImage {
  constructor(){this.naturalWidth=0;this.naturalHeight=0;this.timer=null;this.active=false;}
  set src(value) {
    this.value=value;
    if(!value){if(this.active){active--;this.active=false;}clearTimeout(this.timer);return;}
    assert(!('crossOrigin'in this),'No CORS flag should be added to plain image loads.');
    requests.push(value);active++;this.active=true;maxActive=Math.max(active,maxActive);
    this.timer=setTimeout(()=>{
      active--;this.active=false;
      const path=new URL(value).pathname.slice(1),meta=byPath.get(path),fail=failureCounts.get(meta?.id)||0;
      if(fail){failureCounts.set(meta.id,fail-1);this.onerror?.();return;}
      this.naturalWidth=meta.width;this.naturalHeight=meta.height;this.onload?.();
    },1);
  }
  get src(){return this.value;}
  decode(){return Promise.resolve();}
}
const manager=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:3});
await manager.initialize();
await Promise.all([manager.ensure('fx-jump-dust'),manager.ensure('fx-jump-dust')]);
assert.equal(requests.length,1,'Concurrent requests for one sheet must share a load.');
assert(manager.has('fx-jump-dust'));
failureCounts.set('fx-gold-impact',2);
await assert.rejects(manager.ensure('fx-gold-impact'),AssetLoadError);
assert(manager.stats().failed.includes('fx-gold-impact'));
await manager.ensure('fx-gold-impact');assert(manager.has('fx-gold-impact'));assert(!manager.stats().failed.length);

for(const [biome,region]of Object.entries(REGIONS)) {
  const plan=manager.planLevel({...region,biome});
  assert(plan.includes(`${biome}-terrain`));
  for(const other of Object.keys(REGIONS))if(other!==biome)assert(!plan.includes(`${other}-terrain`));
  assert(!plan.some(id=>ASSET_CATALOG[id].kind==='boss-sprite-sheet'),'Boss art should wait until the approach to the arena.');
  for(const enemy of region.enemyPool)for(const state of ['idle','move','attack','hurt','death'])assert(plan.includes(`enemy-${enemy}-${state}`));
  let started=false;
  await manager.loadLevel({...region,biome},progress=>{
    if(progress.loaded===0){started=true;assert(![...manager.cache.values()].some(e=>e.meta.biome&&e.meta.biome!==biome),'Release the old biome before downloading the next.');}
  });
  await manager.optionalLoad;assert(started);assert.equal(manager.stats().pending,0);
  await manager.prefetchBoss(region.bossId);
  assert(manager.has(`enemy-${region.bossId}-attack2`));
  assert(manager.stats().decodedBytes<192*1048576,`${biome} exceeds the bounded current-world texture budget`);
}
assert(maxActive<=3,'Image request concurrency must be bounded.');
manager.releaseLevel();
assert([...manager.cache.keys()].every(id=>manager.pinned.has(id)));
assert.equal(manager.stats().biome,null);
assert.throws(()=>manager.planLevel({biome:'countryside',enemyPool:['missing-enemy']}),AssetLoadError);
assert.throws(()=>manager.bossIDs('missing-boss'),AssetLoadError);

const pickupPlan=manager.planLevel({biome:'countryside',enemyPool:[],pickups:[{type:'wings'},{type:'fireRain'},{type:'holyLight'},{type:'shield',collected:true}]});
assert(pickupPlan.includes('player.jesus.glide'));assert(pickupPlan.includes('fx-wings-flight-aura'));
assert(pickupPlan.includes('player.jesus.fire-rain'));assert(pickupPlan.includes('fx-fire-eruption'));
assert(pickupPlan.includes('player.jesus.holy-light'));
assert(!pickupPlan.includes('player.jesus.shield'),'Already-collected pickups do not retain unused action sheets.');
await manager.loadLevel({...REGIONS.desert,biome:'desert',id:'desert-normal',pickups:[{type:'wings'}]});
const desertBackground=manager.get('desert-background-near');
assert(manager.has('enemy-hound-idle'));assert(manager.has('player.jesus.glide'));
const bossPrefetch=manager.prefetchBoss('pitLord');
// Start the arena while prefetch is still in flight: needed requests must survive.
await manager.loadLevel({id:'arena-pitLord',biome:'desert',enemyPool:[],bossId:'pitLord',loadBossNow:true,hazardTypes:['lava'],pickups:[]});
await bossPrefetch;
assert.equal(manager.get('desert-background-near'),desertBackground,'The same biome background should not decode again.');
assert(!manager.has('enemy-hound-idle'));assert(!manager.has('enemy-javelin-attack'));
assert(!manager.has('player.jesus.glide'),'An uncollected prior-level upgrade should leave the arena cache.');
const prefetchedBoss=manager.get('enemy-pitLord-idle'),bossRequests=requests.filter(url=>url.endsWith('/enemies/bosses/pitLord-idle.png')).length;
await manager.loadLevel({id:'arena-pitLord-retry',biome:'desert',enemyPool:[],bossId:'pitLord',loadBossNow:true,hazardTypes:['lava']});
assert.equal(manager.get('enemy-pitLord-idle'),prefetchedBoss,'A prefetched boss should keep its decoded image.');
assert.equal(requests.filter(url=>url.endsWith('/enemies/bosses/pitLord-idle.png')).length,bossRequests);
await manager.prefetchPowers(['wings','holyLight','wings']);assert(manager.has('player.jesus.glide'));assert(manager.has('player.jesus.holy-light'));
assert(!manager.pinned.has('player.jesus.glide'),'Streaming pickup prefetch must remain releasable.');
const transitioning=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:1});
const previousLoad=transitioning.loadLevel({...REGIONS.desert,biome:'desert',id:'old-desert'}).then(()=>null,error=>error);
await transitioning.loadLevel({id:'new-desert-arena',biome:'desert',enemyPool:[],bossId:'pitLord',loadBossNow:true,hazardTypes:['lava']});
assert(await previousLoad instanceof AssetLoadError,'Obsolete queued enemy requests should be cancelled during an in-flight transition.');
assert(transitioning.has('enemy-pitLord-idle'));assert(!transitioning.has('enemy-hound-idle'));
assert(!transitioning.stats().failed.length,'Cancelled obsolete requests must not surface as failures in the new world.');
transitioning.releaseLevel();
const raced=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:3});
await raced.loadLevel({biome:'countryside',enemyPool:[]});await raced.optionalLoad;
const beforeRace=requests.length;
const staleLoad=raced.loadLevel({biome:'countryside',enemyPool:[]}).catch(error=>error);
await raced.loadLevel({biome:'desert',enemyPool:[]});await raced.optionalLoad;
assert(await staleLoad instanceof AssetLoadError,'A superseded plan must not finish by starting old-biome optional requests.');
assert(!raced.levelIDs.has('platform-countryside')&&!raced.has('platform-countryside'));
assert(!requests.slice(beforeRace).some(url=>url.endsWith('/platforms/countryside-moving-platform.png')),'Late optional work must not reload the previous biome.');
const cachedCancellation=raced.loadLevel({biome:'desert',enemyPool:[]}).catch(error=>error);raced.cancelLoads();
assert(await cachedCancellation instanceof AssetLoadError);
await raced.optionalLoad;assert(!raced.levelIDs.size&&!raced.pending.size,'Cancellation must also invalidate a cached required phase.');
assert(!manager.planLevel({biome:'desert',enemyPool:[],bossArena:true}).includes('platform-desert'),'Guardian arenas should release unused traversal upgrade sheets.');

// An integration render exercises the selected graphics for every biome, enemy
// and boss. The recording context rejects any source rectangle outside its real
// sheet and confirms that culling avoids drawing distant actors.
let draws=0;const drawnDynamic=new Set();
const context=()=>new Proxy({}, {get(target,key){
  if(key==='createLinearGradient')return()=>({addColorStop(){}});
  if(key==='drawImage')return(image,...args)=>{
    draws++;
    if(args.length===8){const [sx,sy,sw,sh]=args;assert(args.every(Number.isFinite));assert(sx>=0&&sy>=0&&sw>0&&sh>0);assert(sx+sw<=image.naturalWidth+.001&&sy+sh<=image.naturalHeight+.001);}
    const meta=image.src&&byPath.get(new URL(image.src).pathname.slice(1));
    if(meta?.optional) {
      drawnDynamic.add(meta.id);const [sx,sy,sw,sh]=args;
      assert(meta.frames.some(f=>sx>=f.rect.x-.001&&sy>=f.rect.y-.001&&sx+sw<=f.rect.x+f.rect.width+.001&&sy+sh<=f.rect.y+f.rect.height+.001),'An upgrade draw must stay inside one measured frame: '+meta.id);
    }
  };
  return target[key] || (()=>{});
},set(target,key,value){target[key]=value;return true;}});
globalThis.document={createElement:()=>({width:512,height:512,getContext:context})};
const canvas={width:960,height:540,getContext:context},renderer=new Renderer(canvas,manager);
const mechanismTypes=['pendulum','crusher','retractSpikes','rotatingBeam','timedGate'];
const failureManager=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:2});
const failureWorld={biome:'countryside',enemyPool:[],hazardTypes:mechanismTypes};
for(const id of failureManager.planLevel(failureWorld))if(ASSET_CATALOG[id].optional)failureCounts.set(id,1);
await failureManager.loadLevel(failureWorld);
assert(failureManager.has('countryside-structures'),'Original artwork must be available before optional requests settle.');
assert(failureManager.stats().pending>0,'Optional upgrades must load after the playable base world is ready.');
await failureManager.optionalLoad;
assert(!failureManager.stats().failed.length,'Unavailable upgrade sheets must not become blocking graphics errors.');
assert(failureManager.stats().optionalUnavailable.includes('platform-countryside'));
const fallbackRenderer=new Renderer(canvas,failureManager);
fallbackRenderer._dynamicPlatform({x:200,y:300,w:200,h:28},'countryside',0,1,true);
for(const type of mechanismTypes){const h={type,x:300,y:300,w:90,h:65,baseY:300,pivotX:345,pivotY:200};Object.assign(h,sampleHazard(h,1.5));fallbackRenderer._hazard(h,1.5);}
assert.equal(fallbackRenderer.error,null,'Every unavailable new mechanism must retain a visible original-art fallback.');
failureManager.releaseLevel();
const mechanismPlan=manager.planLevel({biome:'countryside',enemyPool:[],chunkPlan:[{tags:mechanismTypes}]});
assert(mechanismPlan.includes('hazard-falling-rock')&&mechanismPlan.includes('hazard-spike-trap'),'Streamed mechanism fallback art must be prepared before its chunk arrives.');
await manager.loadLevel({biome:'countryside',enemyPool:[],hazardTypes:mechanismTypes});await manager.optionalLoad;
const geometryCalls=[];let matrix=[1,0,0,1,0,0];const matrixStack=[];
const geometryContext=new Proxy({
  save(){matrixStack.push([...matrix]);},restore(){matrix=matrixStack.pop();},
  translate(x,y){matrix[4]+=matrix[0]*x+matrix[2]*y;matrix[5]+=matrix[1]*x+matrix[3]*y;},
  rotate(angle){const [a,b,c,d]=matrix,co=Math.cos(angle),si=Math.sin(angle);matrix[0]=a*co+c*si;matrix[1]=b*co+d*si;matrix[2]=c*co-a*si;matrix[3]=d*co-b*si;},
  drawImage(image,...args){geometryCalls.push({image,args,matrix:[...matrix]});},
},{get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});
const geometryRenderer=new Renderer({width:960,height:540,getContext:()=>geometryContext},manager);
function sourcePoint(call,frame,point) {
  const [sx,sy,sw,sh,x,y,w,h]=call.args,m=call.matrix,dx=x+(frame.rect.x+point.x-sx)*w/sw,dy=y+(frame.rect.y+point.y-sy)*h/sh;
  return {x:m[0]*dx+m[2]*dy+m[4],y:m[1]*dx+m[3]*dy+m[5]};
}
const near=(a,b,message)=>assert.ok(Math.abs(a-b)<.001,message);
const rock={type:'fallingRock',x:80,y:250,w:44,h:44,active:true,period:4.2,warningDuration:.9,activeDuration:.65,timeOffset:1.0625-20};
geometryRenderer._hazard(rock,20);
const rockCall=geometryCalls.at(-1),rockFrame=ASSET_CATALOG['hazard-falling-rock'].frames[1],rockCenter=sourcePoint(rockCall,rockFrame,{x:128,y:128});
assert.equal(rockCall.args[0],rockFrame.rect.x,'Legacy timed art follows active progress including a restored clock offset.');
near(rockCenter.x,102,'Falling rock art center matches its physical box');near(rockCenter.y,272,'Falling rock art center matches its physical box');
near(rockCall.args[6]*68/rockCall.args[2],22,'Falling rock circle radius matches the physical half-width');
for(const type of mechanismTypes)for(const time of [1.25,1.5,2.12]) {
  const hazard={type,x:300,y:300,w:46,h:58,baseY:300,pivotX:323,pivotY:200,period:5.8,warningDuration:1.2,activeDuration:1,maxHeight:24};
  Object.assign(hazard,sampleHazard(hazard,time));geometryCalls.length=0;geometryRenderer._hazard(hazard,time);
  const frame=mechanismArtFrame(DYNAMIC_ASSET_CATALOG[({pendulum:'hazard-pendulum',crusher:'hazard-crusher',retractSpikes:'hazard-retracting-spikes',rotatingBeam:'hazard-rotating-beam',timedGate:'hazard-timed-gate'})[type]],hazard,time),guide=frame.collisionGuide,call=geometryCalls.at(-1);
  if(type==='pendulum') {
    const center=sourcePoint(call,frame,guide.center);near(center.x,hazard.tipX);near(center.y,hazard.tipY);
    const edge=sourcePoint(call,frame,{x:guide.center.x+guide.radius,y:guide.center.y});near(Math.hypot(edge.x-center.x,edge.y-center.y),hazard.ballRadius);
  } else if(type==='rotatingBeam') {
    const from=sourcePoint(call,frame,guide.from),to=sourcePoint(call,frame,guide.to);near(from.x,hazard.startX);near(from.y,hazard.startY);near(to.x,hazard.endX);near(to.y,hazard.endY);
  } else if(type==='crusher') {
    const corner=sourcePoint(call,frame,{x:guide.x,y:guide.y});near(corner.x,hazard.x);near(corner.y,hazard.y);
    const bottom=sourcePoint(call,frame,{x:guide.x+guide.width,y:guide.y+guide.height});near(bottom.x,hazard.x+hazard.w);near(bottom.y,hazard.y+hazard.h);
  } else if(type==='retractSpikes') {
    const tip=Math.min(...guide.polygons.flat().map(p=>p[1])),at=sourcePoint(call,frame,{x:guide.polygons[1][1][0],y:tip});
    near(at.y,hazard.y,'Emerging/full/returning visible tips must track the exact damage height');
    near(geometryCalls[0].args[5],hazard.baseY,'The spike plate remains fixed while tips extend');
  }
}
// Exercise the renderer with the simulation's actual warning, damage, recovery,
// pivot and disappearance geometry. No duplicated movement formulas or art cells.
for(const seconds of [0,1.45,2.3,4.9]) {
  for(const type of mechanismTypes) {
    const hazard={type,x:300,y:300,w:90,h:65,baseY:300,pivotX:345,pivotY:200,period:5.8,warningDuration:1.2,activeDuration:1};
    Object.assign(hazard,sampleHazard(hazard,seconds));renderer._hazard(hazard,seconds,.5);
  }
  for(const motion of ['pivot','timed']) {
    const mover={motion,x:200,y:300,baseX:200,baseY:300,w:240,h:28,length:240,period:5.8};
    Object.assign(mover,sampleMover(mover,seconds));renderer._dynamicPlatform(mover,'countryside',seconds,.5,true);
  }
}
assert.equal(renderer.error,null,'Mechanisms must retain working artwork before optional replacement sheets are available.');
const timedMeta=DYNAMIC_ASSET_CATALOG['platform-countryside'],gateMeta=DYNAMIC_ASSET_CATALOG['hazard-timed-gate'];
assert.equal(platformArtFrame(timedMeta,{hidden:true},0).collisionGuide.solid,false);
assert.equal(platformArtFrame(DYNAMIC_ASSET_CATALOG['platform-collapsing-stone'],{dead:true,resetTimer:3.9},0,false).collisionGuide.solid,false);
assert.equal(platformArtFrame(DYNAMIC_ASSET_CATALOG['platform-unstable-bridge'],{dead:true,resetTimer:3.9},0,false).collisionGuide.solid,false);
for(const time of [0,1.5,3]) {
  const gate={type:'timedGate',x:0,y:0,w:60,h:180};Object.assign(gate,sampleHazard(gate,time));
  assert.equal(mechanismArtFrame(gateMeta,gate,time).collisionGuide.solid,gate.active,'Displayed gate passage must agree with the physics blocker.');
  const spikes={type:'retractSpikes',x:0,y:0,w:100,h:46};Object.assign(spikes,sampleHazard(spikes,time));
  assert.equal(mechanismArtFrame(DYNAMIC_ASSET_CATALOG['hazard-retracting-spikes'],spikes,time).collisionGuide.dangerous,spikes.active,'Spike warning/recovery drawings must not display damaging tips.');
}
for(const name of ['move-right','move-up','wait','safe-landing'])renderer._navigationIcon(name,200,200);
renderer._effect({type:'fx-platform-activation',x:200,y:300,time:.15,scale:.3});
await manager.loadLevel({biome:'countryside',enemyPool:[],pickups:[{type:'bread',grounded:true,groundY:468}]});
const staticCalls=[],groundContext={drawImage:(image,...args)=>staticCalls.push(args)};
const groundedRenderer=new Renderer({width:960,height:540,getContext:()=>groundContext},manager);
groundedRenderer._pickup({type:'bread',x:165,y:440,w:28,h:28,grounded:true,groundY:468},0);
groundedRenderer._pickup({type:'bread',x:165,y:440,w:28,h:28,grounded:true,groundY:468},2.4);
assert.deepEqual(staticCalls[0],staticCalls[1],'A grounded collectible must not bob with animation time.');
assert.equal(staticCalls[0][5]+staticCalls[0][7],468,'The rendered bread alpha bottom must touch its physical supporting surface.');
for(const [biome,region]of Object.entries(REGIONS)) {
  await manager.loadLevel({...region,biome});await manager.optionalLoad;await manager.prefetchBoss(region.bossId);
  const surfaceCalls=[];
  const contactContext=new Proxy({drawImage:(image,...args)=>surfaceCalls.push({image,args})},{get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});
  const contactRenderer=new Renderer({width:960,height:540,getContext:()=>contactContext},manager);
  const p={x:200,y:400,w:190,h:28,oneWay:true,supportBottom:728};
  for(const visualKind of ['bridge','supported','ledge']){
    surfaceCalls.length=0;contactRenderer._oneWayPlatform({...p,visualKind},biome,manager.get(`${biome}-terrain`));
    const {args}=surfaceCalls.at(-1),[sx,sy,sw,sh,dx,dy,dw,dh]=args;
    const meta=manager.metadata(visualKind==='bridge'?'platform-unstable-bridge':`${biome}-terrain`),frame=namedFrame(meta,visualKind==='bridge'?'stable':'ledge');
    const contactY=visualKind==='bridge'?frame.collisionGuide.from.y:PLATFORM_SURFACES[`${meta.id}:${frame.index}`].y;
    const renderedContact=dy+(frame.rect.y+contactY-sy)*dh/sh;
    assert.ok(Math.abs(renderedContact-(p.y-8))<.001,`${biome}/${visualKind} top plane must extend above feet, never hang below them`);
  }
  for(const moving of [true,false]){
    surfaceCalls.length=0;contactRenderer._dynamicPlatform({...p,supportTop:100},biome,0,1,moving);
    const [sx,sy,sw,sh,dx,dy,dw,dh]=surfaceCalls.at(-1).args,meta=manager.metadata(moving?`platform-${biome}`:'platform-collapsing-stone'),frame=namedFrame(meta,moving?'idle':'stable'),contact=frame.collisionGuide.from;
    assert.ok(Math.abs(dy+(frame.rect.y+contact.y-sy)*dh/sh-(p.y-8))<.001,`${biome} moving/collapsing deck meets its rider`);
    assert.ok(Math.abs(dx+(frame.rect.x+contact.x-sx)*dw/sw-p.x)<.001,`${biome} measured deck left meets its collision surface`);
  }
  for(const [state,seconds]of [[{},0],[{warning:true},0],[{vx:80},0],[{vx:80},.14],[{motion:'timed',warning:true},0],[{hidden:true},0]]) {
    surfaceCalls.length=0;const platform={...p,...state};contactRenderer._dynamicPlatform(platform,biome,seconds,1,true);
    const frame=platformArtFrame(DYNAMIC_ASSET_CATALOG[`platform-${biome}`],platform,seconds),[sx,sy,sw,sh,dx,dy,dw,dh]=surfaceCalls.at(-1).args,guide=frame.collisionGuide;
    assert.deepEqual([sx,sy,sw,sh],[frame.rect.x,frame.rect.y,frame.rect.width,frame.rect.height]);
    assert.ok(Math.abs(dx+guide.from.x*dw/sw-p.x)<.001,`${biome}/${frame.name} left remains registered`);
    assert.ok(Math.abs(dx+guide.to.x*dw/sw-p.x-p.w)<.001,`${biome}/${frame.name} right remains registered`);
    assert.ok(Math.abs(dy+guide.from.y*dh/sh-p.y+8)<.001,`${biome}/${frame.name} landing plane remains registered`);
  }
  for(const name of region.props){
    surfaceCalls.length=0;contactRenderer._decoration({x:100,y:400,w:90,h:110,artFrame:name},biome,0,true);
    const meta=manager.metadata(`${biome}-terrain`),frame=namedFrame(meta,name),b=ART_ALPHA_BOUNDS[`${meta.id}:${frame.index}`];
    assert.deepEqual(surfaceCalls[0].args.slice(0,4),[frame.rect.x+b.x,frame.rect.y+b.y,b.width,b.height],`${biome}/${name} trims transparent padding below props`);
  }
  const projectileTypes={warBeast:['orb','shockwave','debris','sweep'],templeJudge:['orb','judgment','eruption','light','tablet'],pitLord:['fire','eruption','shockwave'],celestialTrial:['light','judgment','tablet','eruption']}[region.bossId];
  const world={biome,bossId:region.bossId,seed:1,platforms:[{id:'ground',x:0,y:468,w:1300,h:500,kind:'ground'},{id:'slope',x:150,y:420,w:70,h:76,rise:48,kind:'slope',slopeDirection:'up'},{id:'collapse',x:330,y:300,w:160,h:28,kind:'collapsing'},...['bridge','supported','ledge'].map((visualKind,i)=>({id:visualKind,x:100+i*250,y:340,w:180,h:28,oneWay:true,visualKind,supportBottom:728}))],
    movers:[{id:'mover',x:520,y:250,w:125,h:28}],props:[{x:660,y:468,w:90,h:110,artFrame:region.props[0]}],scenery:[],hazards:region.hazardTypes.map((type,i)=>({type,x:200+i*60,y:420,w:35,h:48,active:true})),
    checkpoints:[{x:100,y:406,floorY:468,w:48,h:68,activated:true}],switches:[{x:430,y:410,w:44,h:22}],pickups:['bread','holyLight','shield','fireRain','wings','sandals'].map((type,i)=>({type,x:150+i*80,y:type==='bread'?440:230,w:28,h:28,grounded:type==='bread',groundY:468})),
    enemies:region.enemyPool.map((type,i)=>({type,x:100+i*100,y:406,w:40,h:52,state:'attack',animTime:.15,facing:1})),effects:[{type:'fx-gold-impact',x:350,y:420,time:.15,duration:.5,scale:.5}],bossHazards:projectileTypes.map((type,i)=>({type,x:100+i*70,y:380,w:20,h:20,active:true})),exit:{x:840,y:376,w:110,h:92}};
  const game={world,time:1,player:{x:80,y:406,w:34,h:62,state:'run',animTime:.2,facing:1},camera:{x:0,y:0},bossController:{boss:{definition:{id:region.bossId},x:700,y:350,w:90,h:118,animState:'attack2',animTime:.2,facing:-1}}};
  renderer.draw(game,.5);assert.equal(renderer.error,null,`${biome}: ${[...renderer.missingAssets]}`);
  assert(renderer.terrainCache.size<=12);
  const before=renderer.drawnSprites;world.enemies.push({type:'missing-distant-actor',x:1e6,y:406,w:40,h:52,state:'idle'});
  renderer.draw(game,.5);assert.equal(renderer.error,null);assert.equal(renderer.drawnSprites,before);
}
assert(draws>100,'The integration scenes should draw actual image rectangles.');
for(const id of Object.keys(DYNAMIC_ASSET_CATALOG))assert(drawnDynamic.has(id),'The renderer must use every supplied sheet: '+id);
delete globalThis.document;

const cancelled=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:1});
const pending=cancelled.loadLevel({...REGIONS.storm,biome:'storm'});
cancelled.cancelLoads();
await assert.rejects(pending,AssetLoadError);
assert.equal(cancelled.queue.length,0);assert.equal(cancelled.aborters.size,0);
console.log(`Graphics checks passed: ${frames} exact frames, ${clips} timed clips, six selective world loads, retry, deduplication, cancellation and bounded texture retention.`);
