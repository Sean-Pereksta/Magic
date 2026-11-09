import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {DEATH_TYPES,DEATH_FRAMES,DEATH_FRAME_SIZE,DEATH_CACHE_LIMIT,paintDeathPose,drawDeathAnimation,deathArtworkStatus,clearDeathSpriteCache} from '../death-art.js';

function context(){
 const calls=[];
 return new Proxy({calls,globalAlpha:1},{get(o,k){if(k in o)return o[k];return (...args)=>{for(const n of args)if(typeof n==='number')assert(Number.isFinite(n),`${String(k)} uses finite coordinates`);calls.push({method:k,args,alpha:o.globalAlpha,color:o.fillStyle||o.strokeStyle});};},set(o,k,v){o[k]=v;return true;}});
}

test('all eleven troops have distinct body disassembly artwork and evolving poses',()=>{
 assert.equal(DEATH_TYPES.length,11);
 const signatures=[];
 for(const type of DEATH_TYPES){
  const start=context(),middle=context(),end=context();paintDeathPose(start,type,.1);paintDeathPose(middle,type,.45);paintDeathPose(end,type,.85);
  assert(middle.calls.length>50,`${type} contains illustrated pieces, not a plain fade`);
  assert.notDeepEqual(start.calls,middle.calls,`${type} changes pose as it falls`);
  assert.notDeepEqual(middle.calls,end.calls,`${type} settles into a late pose`);
  signatures.push(JSON.stringify(middle.calls));
 }
 assert.equal(new Set(signatures).size,11,'every troop has its own death silhouette and sequence');
});

test('painted unit body pieces use real image crops and preserve their type-specific parts',()=>{
 for(const type of DEATH_TYPES.filter(type=>type!=='cd')){
  const sprite={complete:true,naturalWidth:512,naturalHeight:512},c=context();paintDeathPose(c,type,.35,sprite);
  assert(c.calls.filter(q=>q.method==='drawImage'&&q.args[0]===sprite).length>=2,`${type} separates the painted character into actual parts`);
  assert(c.calls.filter(q=>q.method==='clip').length>=2,`${type} crops its body pieces rather than duplicating full portraits`);
 }
});

test('death graphics obey fog and camera visibility, stop after their lifetime and leave buildings to the fallback',()=>{
 const fx={type:'death',entityType:'rock',team:1,x:300,y:300,life:1,total:1.3},c=context();
 assert(drawDeathAnimation({pointVisible:()=>false,visible:()=>true},c,fx));assert.equal(c.calls.length,0);
 assert(drawDeathAnimation({pointVisible:()=>true,visible:()=>false},c,fx));assert.equal(c.calls.length,0);
 assert(drawDeathAnimation({pointVisible:()=>true,visible:()=>true},c,{...fx,life:0}));assert.equal(c.calls.length,0);
 assert.equal(drawDeathAnimation({},c,{...fx,entityType:'printer'}),false,'buildings retain their existing destruction effect');
 assert.equal(drawDeathAnimation({},c,{...fx,type:'hit'}),false);
});

test('cached death sheets crop one frame and remain bounded to the eleven troop types',()=>{
 const previous=globalThis.document;globalThis.document={createElement:()=>({getContext:()=>context()})};clearDeathSpriteCache();
 try{
  const renderer={pointVisible:()=>true,visible:()=>true,artSprites:{}};
  for(const type of DEATH_TYPES){const c=context();drawDeathAnimation(renderer,c,{type:'death',entityType:type,x:300,y:300,life:.65,total:1.3});
   const image=c.calls.find(q=>q.method==='drawImage');assert(image,`${type} uses its bounded cached sprite sheet`);
   assert.equal(image.args.length,9);assert.deepEqual(image.args.slice(1,5),[DEATH_FRAME_SIZE*5,0,DEATH_FRAME_SIZE,DEATH_FRAME_SIZE]);
  }
  const status=deathArtworkStatus();assert.equal(status.cachedSheets,11);assert.equal(status.cacheLimit,DEATH_CACHE_LIMIT);
  assert(status.cacheBytes<=12*1024*1024,'all cached death sheets remain below twelve MiB');
 }finally{globalThis.document=previous;clearDeathSpriteCache();}
});

function pngSize(bytes){assert.deepEqual([...bytes.subarray(0,8)],[137,80,78,71,13,10,26,10]);return {width:bytes.readUInt32BE(16),height:bytes.readUInt32BE(20),color:bytes[25]};}
test('production includes eleven distinct transparent ten-frame PNG and WebP death atlases',async()=>{
 const manifest=JSON.parse(await readFile(new URL('../assets/death-animation.json',import.meta.url),'utf8'));
 assert.deepEqual(manifest.types,DEATH_TYPES);assert.equal(manifest.frames,DEATH_FRAMES);assert.equal(manifest.runtimeFrameSize,DEATH_FRAME_SIZE);
 const hashes=[];
 for(const type of DEATH_TYPES){
  const png=await readFile(new URL(`../assets/deaths/${type}-death.png`,import.meta.url)),webp=await readFile(new URL(`../assets/deaths/${type}-death.webp`,import.meta.url));
  assert.deepEqual(pngSize(png),{width:DEATH_FRAME_SIZE*DEATH_FRAMES,height:DEATH_FRAME_SIZE,color:6},`${type} has real RGBA frame artwork`);
  assert.equal(webp.subarray(0,4).toString(),'RIFF');assert.equal(webp.subarray(8,12).toString(),'WEBP');assert(webp.length>10000);
  hashes.push(createHash('sha256').update(png).digest('hex'));
 }
 assert.equal(new Set(hashes).size,11);
 const remains=await readFile(new URL('../assets/death-remains.png',import.meta.url));
 assert.deepEqual(pngSize(remains),{width:1448,height:1086,color:6},'generated remains have an actual transparent alpha channel');
});
