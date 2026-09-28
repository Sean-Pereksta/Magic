import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {ALL_ART_PATHS,ART,ironThronesAsset,preloadAllArt,AssetCache,loadArt,displayArtURL} from '../asset-manifest.mjs';
test('complete manifest matches supplied PNGs and game catalogs',async()=>{
  assert.equal(new Set(ALL_ART_PATHS).size,ALL_ART_PATHS.length);
  assert.equal(ALL_ART_PATHS.length,177);
  assert.equal(ironThronesAsset('/buildings/lumber_1.png'),ART.structures.lumber[1]);
  assert.equal(ART.terrain.mountain.length,6);
  assert.ok(!ALL_ART_PATHS.some(path=>/^(terrain\/coast_|overlays\/river_)/.test(path)), 'replaced scenic art is not preloaded');
});
test('preload probes lumber first, bounds concurrency, decodes and reuses every image',async()=>{
  const requests=[];let active=0,peak=0;
  globalThis.Image=class {
    naturalWidth=200;naturalHeight=100;
    set src(url){if(!url)return;requests.push(url);peak=Math.max(peak,++active);setTimeout(()=>{active--;this.onload?.();},1);}
    async decode(){}
  };
  const progress=[];const result=await preloadAllArt(p=>progress.push(p));
  assert.equal(result.failed,0);assert.equal(result.completed,ALL_ART_PATHS.length);
  assert.equal(requests[0],ART.structures.lumber[1]);assert.ok(peak<=6);
  const draw=[];const cache=new AssetCache();cache.draw({drawImage:(...args)=>draw.push(args)},ART.structures.lumber[1],0,0,40,40);
  assert.deepEqual(draw[0].slice(1),[0,10,40,20]);
  await preloadAllArt();assert.equal(requests.length,ALL_ART_PATHS.length);
  assert.equal(progress.at(-1).completed,ALL_ART_PATHS.length);
});
test('missing, stalled, and undecodable images settle safely',async()=>{
  globalThis.Image=class {set src(url){if(url.includes('missing'))queueMicrotask(()=>this.onerror?.());if(url.includes('decode'))queueMicrotask(()=>this.onload?.());} async decode(){throw Error('corrupt');}};
  for(const name of ['missing','stalled','decode'])assert.equal(await loadArt(ironThronesAsset(`test/${name}.png`),{timeout:10}),null);
});
