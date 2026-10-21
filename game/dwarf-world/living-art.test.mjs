import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createHash} from 'node:crypto';

const source=fs.readFileSync(new URL('./config.js',import.meta.url),'utf8');
const manifest=JSON.parse(fs.readFileSync(new URL('./art-manifest.json',import.meta.url),'utf8'));
function boot(config,failImage=false,gameURL='https://game.example/game/DwarfWorld.html'){
  const requests=[];
  class FakeImage {
    set src(value){this._src=value;if(!value)return;requests.push(value);const found=Object.values(manifest.atlases).find(a=>value.endsWith(a.path));
      if(failImage==='hang')return;
      queueMicrotask(()=>{if(failImage){this.onerror?.();return}this.naturalWidth=found?.width||0;this.naturalHeight=found?.height||0;this.onload?.()});}
    get src(){return this._src}
  }
  const window={DWARF_ART_CONFIG:config};
  const context={window,document:{currentScript:{src:new URL('./dwarf-world/config.js',gameURL).href},baseURI:gameURL},URL,Image:FakeImage,AbortController,Map,Set,Promise,setTimeout,clearTimeout,
    fetch:async url=>{requests.push(String(url));return{ok:true,json:async()=>manifest}}};
  vm.runInNewContext(source,context);
  return{loader:window.DwarfWorldArtLoader,requests};
}
test('repository artwork loads by default at root and project-subdirectory URLs',async()=>{
  for(const gameURL of ['https://game.example/game/DwarfWorld.html','https://game.example/Magic/game/DwarfWorld.html']){
    const {loader,requests}=boot(undefined,false,gameURL);const result=await loader.preloadEssentials();
    assert.equal(result.mode,'illustrated');assert.equal(result.loaded,manifest.essential.length);
    assert(requests.includes(new URL('./dwarf-world/assets/living-mountain-v1/dwarves/actions.webp',gameURL).href));
    assert(requests.every(url=>new URL(url).origin===new URL(gameURL).origin));
  }
});

test('explicitly disabled artwork preserves fallback without requests',async()=>{
  for(const config of [{assetBase:''},{enabled:false}]){
    const {loader,requests}=boot(config);const result=await loader.preloadEssentials();
    assert.equal(result.mode,'fallback');assert.equal(loader.enabled,false);assert.deepEqual(requests,[]);
  }
});

test('every committed atlas matches its manifest byte count and SHA-256',()=>{
  for(const [name,atlas]of Object.entries(manifest.atlases)){
    const bytes=fs.readFileSync(new URL('./assets/living-mountain-v1/'+atlas.path,import.meta.url));
    assert.equal(bytes.length,atlas.bytes,name);
    assert.equal(createHash('sha256').update(bytes).digest('hex'),atlas.sha256,name);
    assert.equal(bytes.toString('ascii',0,4),'RIFF',name);
    assert.equal(bytes.toString('ascii',8,12),'WEBP',name);
  }
});
test('essential preload resolves actual images and honors a separate game prefix',async()=>{
  const {loader,requests}=boot({assetBase:'https://assets.example/',assetPrefix:'/dwarf-world/living-mountain-v1/'});
  const result=await loader.preloadEssentials();assert.equal(result.loaded,manifest.essential.length);
  assert.equal(loader.images.size,manifest.essential.length);assert.equal(loader.revision,manifest.essential.length);
  assert(requests.some(url=>url==='https://assets.example/dwarf-world/living-mountain-v1/dwarves/actions.webp'));
  assert(!requests.some(url=>url.includes('terrain/abyssal.webp')));
  await Promise.all([loader.request('terrain_abyssal'),loader.request('terrain_abyssal')]);
  assert.equal(requests.filter(url=>url.endsWith('terrain/abyssal.webp')).length,1);
});
test('failed image requests settle and retain graceful fallback without retry storms',async()=>{
  const {loader,requests}=boot({assetBase:'https://assets.example'},true);
  const result=await loader.preloadEssentials();assert.equal(result.mode,'fallback');
  assert.equal(result.failed.length,manifest.essential.length);
  const count=requests.length;await loader.request('dwarves');assert.equal(requests.length,count);
});
test('manifest frames are within actual atlas bounds and authored animation poses are distinct',()=>{
  assert.equal(Object.keys(manifest.frames).length,248);
  for(const [name,frame]of Object.entries(manifest.frames)){
    const atlas=manifest.atlases[frame.atlas];assert(atlas,name);
    const[x,y,w,h]=frame.rect;assert(w===128&&h===128,name);
    assert(x>=0&&y>=0&&x+w<=atlas.width&&y+h<=atlas.height,name);
  }
  for(const animation of Object.values(manifest.animations)){
    assert.equal(new Set(animation.frames).size,animation.frames.length);
    for(const name of animation.frames)assert(manifest.frames[name],name);
  }
});
test('a stalled image host cannot hold the loading screen indefinitely',async()=>{
  const {loader}=boot({assetBase:'https://assets.example',timeoutMs:500},'hang');
  const start=Date.now();const result=await loader.preloadEssentials();
  assert.equal(result.mode,'fallback');assert(Date.now()-start<1200);
});
