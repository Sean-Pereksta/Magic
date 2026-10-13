import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeAssetBase,safeAssetUrl,frameRectangle,animationDefinition,createAssetLoader,drawSpriteFrame} from '../assets.js';
import {MotionTrack,animationFrame,resolveAnimation} from '../renderer.js';
import {forgeEligibility,upgradeCount} from '../ui.js';

const spec={path:'units/rifleman-tier1.webp',frameWidth:128,frameHeight:128,columns:6,rows:6,pivot:[.5,.9375],animations:{idle:{frames:[0,1,2,3,4,5],duration:900},move:{frames:[6,7,8,9,10,11],duration:540},ability:{frames:[18,19,20,21,22,23],duration:600},death:{frames:[30,31,32,33,34,35],duration:700,loop:false}}};
const image={width:768,height:768};

test('graphics are opt-in and cannot escape the configured R2 folder',()=>{
  assert.equal(normalizeAssetBase(''),'');
  assert.equal(normalizeAssetBase('https://assets.example.test/forge/'),'https://assets.example.test/forge/');
  assert.equal(safeAssetUrl('https://assets.example.test/forge/','units/golem-tier3.webp'),'https://assets.example.test/forge/units/golem-tier3.webp');
  for(const path of ['../othergame.webp','https://other.example/pic.webp','//evil.example/pic.webp','%2e%2e/othergame.webp','units/a.webp?token=secret'])assert.throws(()=>safeAssetUrl('https://assets.example.test/forge/',path));
  assert.throws(()=>normalizeAssetBase('http://assets.example/'));
});

test('every frame crops only its own integer source cell, including the final death frame',()=>{
  const seen=new Set();for(let frame=0;frame<36;frame++){const rect=frameRectangle(spec,frame,image);assert.equal(rect.width,128);assert.equal(rect.height,128);assert.ok(rect.x+128<=768 && rect.y+128<=768);assert.equal(rect.x%128,0);assert.equal(rect.y%128,0);seen.add(JSON.stringify(rect));}assert.equal(seen.size,36);
  assert.deepEqual(frameRectangle(spec,35,image),{x:640,y:640,width:128,height:128});
  assert.throws(()=>frameRectangle(spec,36,image));
  assert.throws(()=>frameRectangle({...spec,frames:[{x:1.5,y:0,width:128,height:128}]},0,image));
});

test('canvas drawing clips the destination and uses nine-argument sprite extraction',()=>{
  const calls=[];const context=Object.fromEntries(['save','beginPath','rect','clip','translate','scale','drawImage','restore'].map(name=>[name,(...args)=>calls.push([name,...args])]));
  const rect=frameRectangle(spec,12,image);drawSpriteFrame(context,image,rect,{flip:true,width:64,height:64});
  const draw=calls.find(c=>c[0]==='drawImage');assert.equal(draw.length,10);assert.deepEqual(draw.slice(2,6),[0,256,128,128]);assert.ok(calls.findIndex(c=>c[0]==='clip')<calls.findIndex(c=>c[0]==='drawImage'));assert.deepEqual(calls.find(c=>c[0]==='scale'),['scale',-1,1]);
});

test('movement remains smooth at any rendering cadence and never mutates logical units',()=>{
  const logical={x:1,y:2};const motion=new MotionTrack(logical.x,logical.y,0);motion.move(2,2,0,400);assert.deepEqual(logical,{x:1,y:2});assert.equal(motion.sample(200).x,1.5);assert.equal(motion.sample(400).x,2);assert.equal(motion.sample(400).moving,false);
  const cadenceA=new MotionTrack(0,0,0),cadenceB=new MotionTrack(0,0,0);cadenceA.move(2,1,0,400);cadenceB.move(2,1,0,400);for(let t=0;t<200;t+=16)cadenceA.sample(t);for(let t=0;t<200;t+=33)cadenceB.sample(t);assert.deepEqual(cadenceA.sample(200),cadenceB.sample(200));
  motion.move(0,3,401,400);assert.equal(motion.sample(601).facing,-1);assert.equal(motion.sample(801).x,0);
});

test('ability/hurt aliases work and death holds its final frame',()=>{
  assert.deepEqual(animationDefinition(spec,'cast').frames,[18,19,20,21,22,23]);
  assert.equal(animationFrame(spec,'death',2000),35);assert.equal(animationFrame(spec,'move',540),6);
  assert.equal(resolveAnimation({hp:3,animUntil:1200,animType:'cast'},1000,true),'cast');
  assert.equal(resolveAnimation({hp:3,hitUntil:1100},1000,false),'hit');
  assert.equal(resolveAnimation({hp:0,animUntil:1200,animType:'cast'},1000,true),'death');
});

function mockImageFactory(){return ()=>{const image={width:768,height:768,onload:null,onerror:null};Object.defineProperty(image,'src',{set(value){image.source=value;queueMicrotask(()=>value.includes('missing')?image.onerror?.():image.onload?.());},get(){return image.source;}});return image;};}
test('sheets load lazily, deduplicate concurrent requests, and bound decoded cache',async()=>{
  let manifestFetches=0;const loader=createAssetLoader({baseUrl:'https://assets.example/forge',maxSheets:2,imageFactory:mockImageFactory(),fetchImpl:async()=>{manifestFetches++;return {ok:true,json:async()=>({units:{rifleman:{tiers:{'1':spec}}}})};}});
  assert.equal(loader.stats.loads,0);const [a,b]=await Promise.all([loader.unit('rifleman',1),loader.unit('rifleman',1)]);assert.ok(a);assert.equal(a.image,b.image);assert.equal(loader.stats.loads,1);assert.equal(manifestFetches,1);
  await loader.loadImage('effects/a.webp');await loader.loadImage('effects/b.webp');assert.equal(loader.cacheSize,2);assert.equal(loader.stats.evictions,1);
  assert.equal(await loader.loadImage('units/missing.webp'),null);assert.equal(await loader.loadImage('units/missing.webp'),null);assert.equal(loader.stats.failures,1);
});

test('unconfigured or unavailable graphics gracefully keep legacy artwork',async()=>{
  let fetches=0;const disabled=createAssetLoader({fetchImpl:()=>{fetches++;throw new Error('Unexpected fetch');}});assert.equal(disabled.enabled(),false);assert.equal(await disabled.unit('rifleman',1),null);assert.equal(fetches,0);
  const failed=createAssetLoader({baseUrl:'https://assets.example/',fetchImpl:async()=>({ok:false})});assert.equal(await failed.unit('rifleman',1),null);assert.equal(await failed.unit('golem',3),null);assert.equal(failed.stats.failures,1);
});

test('Forge tiers and battle ranks stay separate and active matches block upgrades',()=>{
  const profile={forgeDust:450,troops:{rifleman:{forgeTier:1,cards:10,unlocked:true},golem:{forgeTier:2,cards:30,unlocked:true},railbreaker:{forgeTier:1,cards:20,unlocked:false}}};
  assert.equal(forgeEligibility(profile,'rifleman').available,true);assert.equal(forgeEligibility(profile,'rifleman',{active:true}).available,false);assert.equal(forgeEligibility(profile,'railbreaker').available,false);assert.equal(upgradeCount(profile),2);
  profile.troops.rifleman.battleRank=3;assert.equal(forgeEligibility(profile,'rifleman').tier,1);
  profile.troops.golem.forgeTier=3;assert.equal(forgeEligibility(profile,'golem').reason,'Elite tier reached.');
  profile.upgradeCosts={1:{cards:15,dust:90},2:{cards:45,dust:300}};assert.equal(forgeEligibility(profile,'rifleman').available,false);assert.equal(forgeEligibility(profile,'rifleman').cost.cards,15);
});
