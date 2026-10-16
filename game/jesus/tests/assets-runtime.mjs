import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {ASSET_CATALOG,AssetManager,AssetLoadError,sampleFrame,namedFrame} from '../js/assets/asset-manager.js';
import {REGIONS} from '../js/world/regions.js';
import {Renderer,groundedArtPlacement} from '../js/render/renderer.js';
import {ART_ALPHA_BOUNDS,PLATFORM_SURFACES} from '../js/assets/art-anchors.js';
import {createRequire} from 'node:module';

const byPath=new Map(Object.values(ASSET_CATALOG).map(meta=>[meta.path,meta]));
assert.equal(Object.keys(ASSET_CATALOG).length,263);
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
assert.equal(frames,1429);assert.equal(clips,301);
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
  assert(started);assert.equal(manager.stats().pending,0);
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

// An integration render exercises the selected graphics for every biome, enemy
// and boss. The recording context rejects any source rectangle outside its real
// sheet and confirms that culling avoids drawing distant actors.
let draws=0;
const context=()=>new Proxy({}, {get(target,key){
  if(key==='createLinearGradient')return()=>({addColorStop(){}});
  if(key==='drawImage')return(image,...args)=>{
    draws++;
    if(args.length===8){const [sx,sy,sw,sh]=args;assert(sx>=0&&sy>=0&&sw>0&&sh>0);assert(sx+sw<=image.naturalWidth+.001&&sy+sh<=image.naturalHeight+.001);}
  };
  return target[key] || (()=>{});
},set(target,key,value){target[key]=value;return true;}});
globalThis.document={createElement:()=>({width:512,height:512,getContext:context})};
const canvas={width:960,height:540,getContext:context},renderer=new Renderer(canvas,manager);
await manager.loadLevel({biome:'countryside',enemyPool:[],pickups:[{type:'bread',grounded:true,groundY:468}]});
const staticCalls=[],groundContext={drawImage:(image,...args)=>staticCalls.push(args)};
const groundedRenderer=new Renderer({width:960,height:540,getContext:()=>groundContext},manager);
groundedRenderer._pickup({type:'bread',x:165,y:440,w:28,h:28,grounded:true,groundY:468},0);
groundedRenderer._pickup({type:'bread',x:165,y:440,w:28,h:28,grounded:true,groundY:468},2.4);
assert.deepEqual(staticCalls[0],staticCalls[1],'A grounded collectible must not bob with animation time.');
assert.equal(staticCalls[0][5]+staticCalls[0][7],468,'The rendered bread alpha bottom must touch its physical supporting surface.');
for(const [biome,region]of Object.entries(REGIONS)) {
  await manager.loadLevel({...region,biome});await manager.prefetchBoss(region.bossId);
  const surfaceCalls=[];
  const contactContext=new Proxy({drawImage:(image,...args)=>surfaceCalls.push({image,args})},{get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});
  const contactRenderer=new Renderer({width:960,height:540,getContext:()=>contactContext},manager);
  const p={x:200,y:400,w:190,h:28,oneWay:true,supportBottom:728};
  for(const visualKind of ['bridge','supported','ledge']){
    surfaceCalls.length=0;contactRenderer._oneWayPlatform({...p,visualKind},biome,manager.get(`${biome}-terrain`));
    const {args}=surfaceCalls.at(-1),[sx,sy,sw,sh,dx,dy,dw,dh]=args;
    const meta=manager.metadata(`${biome}-${visualKind==='bridge'?'structures':'terrain'}`),frame=namedFrame(meta,visualKind==='bridge'?'bridge':'ledge');
    const deck=PLATFORM_SURFACES[`${meta.id}:${frame.index}`];
    const renderedContact=dy+(frame.rect.y+deck.y-sy)*dh/sh;
    assert.ok(Math.abs(renderedContact-(p.y-8))<.001,`${biome}/${visualKind} top plane must extend above feet, never hang below them`);
  }
  for(const moving of [true,false]){
    surfaceCalls.length=0;contactRenderer._dynamicPlatform({...p,supportTop:100},biome,0,1,moving);
    const [sx,sy,sw,sh,dx,dy,dw,dh]=surfaceCalls.at(-1).args,meta=manager.metadata(`${biome}-structures`),frame=namedFrame(meta,moving?'moving-platform':'collapse-platform'),deck=PLATFORM_SURFACES[`${meta.id}:${frame.index}`];
    assert.ok(Math.abs(dy+(frame.rect.y+deck.y-sy)*dh/sh-(p.y-8))<.001,`${biome} moving/collapsing deck meets its rider`);
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
delete globalThis.document;

const cancelled=new AssetManager({imageFactory:()=>new FakeImage(),concurrency:1});
const pending=cancelled.loadLevel({...REGIONS.storm,biome:'storm'});
cancelled.cancelLoads();
await assert.rejects(pending,AssetLoadError);
assert.equal(cancelled.queue.length,0);assert.equal(cancelled.aborters.size,0);
console.log(`Graphics checks passed: ${frames} exact frames, ${clips} timed clips, six selective world loads, retry, deduplication, cancellation and bounded texture retention.`);
