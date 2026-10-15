import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('hosted-art.js',import.meta.url),'utf8');
function setup(config={}){
 const images=[],timers=new Map(),microtasks=[];let timerId=0,time=0,loads=0;
 const make=(id,region,category='enemy')=>({id,category,region,image:'assets/'+id+'.png',meta:{width:64,height:32,frames:[{id:'a',rect:{x:0,y:0,w:32,h:32},pivot:{x:16,y:28}},{id:'b',rect:{x:32,y:0,w:32,h:32},pivot:{x:16,y:28}}],animations:{SE:{idle:{frames:['a','b'],fps:2,loop:true},attack:{frames:['a','b'],fps:2,loop:false}}}}});
 const entries={hero:make('hero',null,'armor'),v:make('v','verdant','terrain'),m:make('m','meridian','terrain'),g:make('g','gloam','terrain'),s:make('s','shadow','terrain'),local:make('local','verdant'),late:make('late','verdant'),summon:make('summon','verdant')};
 const window={AWArtCatalog:{entries,regions:{verdant:['v','local','late'],meridian:['m'],gloam:['g'],shadow:['s']},summonDependencies:{local:['summon'],summon:['local']}},AW_ART_CONFIG:{maxParallel:2,...config},AWArtRenderer:{playerAssetId:()=> 'hero',enemyAssetId:e=>e.type},AWCampaignData:{nodes:{'meridian-city':{continent:'meridian'},'gloam-city':{continent:'gloam'},'shadow:1:0':{continent:'shadow',shadow:true}}},AWWorld:{invalidate(){loads++;}}};
 class Image{constructor(){this.naturalWidth=64;this.naturalHeight=32;images.push(this);}set src(v){this.url=v;}get src(){return this.url;}load(){this.onload?.();}fail(){this.onerror?.();}}
 const context=vm.createContext({window,document:{defaultView:window},queueMicrotask:fn=>microtasks.push(fn),Image,performance:{now:()=>time},setTimeout:fn=>{timers.set(++timerId,fn);return timerId;},clearTimeout:id=>timers.delete(id),game:{roomData:{biome:'meadow'},enemies:[]},loadRoom(){this.game.roomData={biome:'meadow'};},isTouch:false,Map,Set,Promise,Number,Date});vm.runInContext(source,context);
 return {art:window.AWArt,images,window,context,flushStartup:()=>{while(microtasks.length)microtasks.shift()();},advance:n=>{time+=n;},timeout:()=>{for(const fn of [...timers.values()])fn();},invalidate:()=>loads};
}
test('Cloudflare paths strip only the uploaded assets prefix',()=>{const {art}=setup();assert.equal(art.url('assets/characters/a.png'),'https://pub-982943ccc2b14477a62ef61fa83e5179.r2.dev/characters/a.png');assert.equal(art.entry('missing'),null);assert.equal(art.get('v'),null);});
test('deferred startup prepares a live document and ignores a document closed before its microtask',()=>{
 const live=setup();live.context.game.player={};assert.equal(live.images.length,0);live.flushStartup();
 assert.deepEqual(live.images.map(image=>image.url.split('/').at(-1)).sort(),['hero.png','v.png']);
 for(const document of [undefined,null,{defaultView:null}]){
  const closed=setup();closed.context.game.player={};closed.context.document=document;
  assert.doesNotThrow(()=>closed.flushStartup());assert.equal(closed.images.length,0);assert.equal(closed.art.roomVersion(),0);
 }
});
test('startup requests only the player and current region; metadata requires no fetch',async()=>{const {art,images}=setup();const done=art.prepare({biome:'meadow'});assert.equal(art.status().region,'verdant');assert.deepEqual(images.map(i=>i.url.split('/').at(-1)).sort(),['hero.png','v.png']);assert(images.every(i=>i.crossOrigin===undefined));images.forEach(i=>i.load());await done;assert(art.get('hero'));assert(art.get('v'));assert.equal(art.get('m'),null);});
test('image requests deduplicate and respect concurrency limits',async()=>{const {art,images}=setup({maxParallel:1});const a=art.ensure('v'),b=art.ensure('v'),c=art.ensure('m');assert.equal(a,b);assert.equal(images.length,1);assert.equal(art.status().queued,1);images[0].load();assert.equal(images.length,2);images[1].load();await Promise.all([a,b,c]);assert.equal(art.status().active,0);});
test('continent changes retain the player and release former region artwork',async()=>{const {art,images}=setup();const first=art.prepare({biome:'forest'});images.forEach(i=>i.load());await first;const next=art.prepare({campaignNode:'meridian-city'});assert.equal(art.get('v'),null);assert(art.get('hero'));assert.equal(images.length,3);images[2].load();await next;assert(art.get('m'));assert.equal(art.regionFor({campaignNode:'gloam-city'}),'gloam');assert.equal(art.regionFor({campaignNode:'shadow:1:0',biome:'forest'}),'shadow');});
test('failed and stalled images preserve fallback and retry after cooldown',async()=>{const s=setup();const failure=s.art.ensure('v');s.images[0].fail();assert.equal(await failure,null);assert.equal(s.art.get('v'),null);await s.art.ensure('v');assert.equal(s.images.length,1);s.advance(31000);const retry=s.art.ensure('v');assert.equal(s.images.length,2);s.images[1].load();assert(await retry);const stalled=s.art.ensure('m');s.timeout();assert.equal(await stalled,null);});
test('atlas draws use authored coordinates/pivots, preserve opacity and clamp attacks',async()=>{const {art,images}=setup();const ready=art.ensure('v');images[0].load();await ready;const calls=[],ctx={globalAlpha:.5,save(){this.saved=this.globalAlpha},restore(){this.globalAlpha=this.saved},drawImage(...a){calls.push(a)}};assert(art.draw('v',ctx,100,200,{state:'attack',time:100,scale:2,alpha:.5}));assert.deepEqual(calls[0].slice(1),[32,0,32,32,68,144,64,64]);assert.equal(ctx.globalAlpha,.5);assert.equal(art.draw('v',ctx,0,0,{frame:'missing'}),false);assert.equal(art.draw('missing',ctx,0,0),false);});
test('unexpected image sizes and disabled configuration never enter ready state',async()=>{const s=setup();const ready=s.art.ensure('v');s.images[0].naturalWidth=128;s.images[0].load();assert.equal(await ready,null);const disabled=setup({enabled:false});assert.equal(await disabled.art.ensure('v'),null);assert.equal(disabled.images.length,0);});
test('room loading requests present enemies and their summons without preloading the continent roster',async()=>{
 const {art,images,context}=setup();context.game.enemies=[{type:'local'}];
 const done=art.prepare({biome:'meadow'});for(const image of images)image.load();await done;
 assert.deepEqual(Array.from(art.status().cached).sort(),['hero','local','summon','v']);
 assert(!images.some(i=>i.url.endsWith('/late.png')));
 const late=art.request('late');images.at(-1).load();await late;
 assert(art.get('late'));assert(art.status().pinned.includes('late'));
});
test('a room revisit reloads evicted artwork and releases old room pins within the same continent',async()=>{
 const {art,images,context}=setup({maxCachedAtlases:2});context.game.enemies=[{type:'local'}];
 const first=art.prepare({biome:'forest'});for(const image of images)image.load();await first;
 assert(art.get('local'));context.game.enemies=[];await art.prepare({biome:'forest'});
 assert.equal(art.get('local'),null);assert.equal(art.get('summon'),null);
 assert.deepEqual(Array.from(art.status().pinned).sort(),['hero','v']);
 const revisit=art.request('local');for(const image of images)image.load();await revisit;
 assert(art.get('local'));assert(art.get('summon'));
});
test('continent travel cancels queued former enemies and discards late former-continent downloads',async()=>{
 const {art,images,context}=setup({maxParallel:1});context.game.enemies=[{type:'local'}];
 const first=art.prepare({biome:'forest'});images[0].load(); // Former terrain is now in flight.
 context.game.enemies=[];const next=art.prepare({campaignNode:'meridian-city'});
 for(const image of images)image.load();await Promise.all([first,next]);
 assert.deepEqual(Array.from(art.status().cached).sort(),['hero','m']);
 assert(!images.some(i=>/\/(local|summon)\.png$/.test(i.url)));
 assert.equal(art.status().active,0);assert.equal(art.status().queued,0);
});
test('bundled catalog has unique region references and in-bounds authored frames',()=>{const window={};vm.runInNewContext(readFileSync(new URL('hosted-art-catalog.js',import.meta.url),'utf8'),{window});const c=window.AWArtCatalog;assert.equal(Object.keys(c.entries).length,170);for(const ids of Object.values(c.regions))for(const id of ids)assert(c.entries[id]);for(const e of Object.values(c.entries)){const ids=new Set();for(const f of e.meta.frames){assert(!ids.has(f.id));ids.add(f.id);assert(f.rect.x>=0&&f.rect.y>=0&&f.rect.x+f.rect.w<=e.meta.width&&f.rect.y+f.rect.h<=e.meta.height);}if(e.category==='armor'||e.category==='enemy'){assert.equal(e.meta.frames.length,32);for(const dir of Object.values(e.meta.animations))for(const a of Object.values(dir))for(const id of a.frames)assert(ids.has(id));}}});


