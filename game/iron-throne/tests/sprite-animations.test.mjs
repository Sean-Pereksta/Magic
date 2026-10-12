import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {SPRITE_ANIMATIONS as sheets,ANIMATION_MANIFEST,SPRITE_PATHS,SpritePlayback,SpriteScheduler,spriteFrame,validSpriteSheet,constructionStage} from '../sprite-animations.mjs';
import {ART,AssetCache,ironThronesAsset,loadArt} from '../asset-manifest.mjs';
import {SpriteAtlasOutlines} from '../sprite-outline.mjs';
import {UNITS} from '../data.mjs';

const army=(id='a',tile='a',owner='ashen')=>({id,tile,owner,units:{levy:12},path:[],order:'hold',target:null});
const fleet=(id='f',tile='sea',owner='ashen')=>({id,tile,owner,node:`sea:${tile}`,ships:[{type:'warship'}],path:[],order:'hold'});
const world=()=>({seed:1,turn:1,knowledgeView:'ashen',tiles:Object.fromEntries(['a','b','sea','shore','retreat'].map(id=>[id,{id,fog:'visible'}])),armies:[army()],fleets:[],militaryEvents:[]});

test('21 exact paths, catalog IDs, dimensions, exported JSON and hosted URLs agree',async()=>{
  assert.equal(SPRITE_PATHS.length,21);assert.equal(new Set(SPRITE_PATHS).size,21);
  assert.deepEqual(Object.keys(sheets.troops).sort(),Object.keys(UNITS).sort());
  assert.equal(sheets.ships.warCanoe.path,'sprites/ships/war_canoe.png');
  assert.equal(sheets.troops.menAtArms.path,'sprites/troops/menAtArms.png');
  assert.deepEqual(JSON.parse(await readFile(new URL('../animation-manifest.json',import.meta.url),'utf8')),ANIMATION_MANIFEST);
  for(const [family,entries] of Object.entries(sheets))for(const [id,spec] of Object.entries(entries)) {
    assert.equal(ART.animations[family][id],ironThronesAsset(spec.path));
    assert.ok(spec.anchor.x>0 && spec.anchor.x<spec.cell.width && spec.anchor.y<spec.cell.height);
    const image={naturalWidth:spec.cell.width*8,naturalHeight:spec.cell.height*spec.rows};
    assert.equal(validSpriteSheet(image,spec),true);assert.equal(validSpriteSheet({...image,naturalWidth:1},spec),false);
    for(const [action,animation] of Object.entries(spec.actions))for(let n=0;n<16;n++) {
      const f=spriteFrame(spec,action,n*1000/animation.fps);
      assert.equal(f.frame,n%8);assert.equal(f.y,animation.row*spec.cell.height);
      assert.ok(f.x+f.width<=image.naturalWidth && f.y+f.height<=image.naturalHeight);
      assert.equal(spriteFrame(spec,action,n*1000,true).frame,0);
    }
  }
});

test('completed movement includes the final path step; queued and blocked orders stay idle',()=>{
  const s=world(),p=new SpritePlayback();p.ingest(s,0);
  s.armies[0].path=['b'];s.armies[0].order='move';p.ingest(s,10);
  assert.equal(p.resolve('army',s.armies,10).action,'idle');
  s.turn++;p.ingest(s,20);assert.equal(p.resolve('army',s.armies,20).action,'idle');
  s.armies[0].tile='b';s.armies[0].path=[];s.armies[0].order='hold';p.ingest(s,30);
  assert.equal(p.resolve('army',s.armies,30).action,'march');
  p.ingest(s,900);assert.equal(p.resolve('army',s.armies,900).start,30);
  p.ingest(s,1030);assert.equal(p.resolve('army',s.armies,1030).action,'idle');
});

test('only recorded participants attack, including ranged siege and ship bombardment',()=>{
  const s=world(),p=new SpritePlayback();s.armies.push(army('b','a'));s.fleets=[fleet()];p.ingest(s,0);
  s.militaryEvents.push({id:1,turn:1,action:'siege',attacker:'ashen',defender:'enemy',attackerArmyId:'a',tile:'b',from:'a'});
  const before=structuredClone(s);p.ingest(s,10);
  assert.equal(p.resolve('army',[s.armies[0]],10,'levy').action,'attack');
  assert.equal(p.resolve('army',[s.armies[1]],10).action,'idle');
  assert.equal(p.resolve('army',s.armies,10,'archer').action,'idle');
  p.ingest(s,50);assert.equal(p.resolve('army',s.armies,50).start,10);
  assert.deepEqual(s,before,'presentation must not mutate authoritative state');
  s.militaryEvents.push({id:2,turn:1,action:'structure',attacker:'ashen',defender:'enemy',attackerArmyId:'f',tile:'shore',from:'sea'});p.ingest(s,60);
  assert.equal(p.resolve('fleet',s.fleets,60).action,'attack');
  p.ingest(s,1100);assert.equal(p.resolve('army',s.armies,1100).action,'idle');
  assert.equal(p.resolve('fleet',s.fleets,1100).action,'idle');
});

test('naval playback requires an actual encounter and unambiguous participant evidence',()=>{
  const s=world(),p=new SpritePlayback();s.armies=[];
  s.fleets=[{...fleet(),order:'attack',attackTile:'shore'},fleet('enemy','shore','wintermere'),fleet('unrelated','b')];
  p.ingest(s,0);assert.equal(p.resolve('fleet',s.fleets,0).action,'idle');
  s.fleets[0].order='hold';s.fleets[0].attackTile=null;
  s.militaryEvents.push({id:1,turn:1,action:'naval',attacker:'ashen',defender:'wintermere',tile:'shore'});p.ingest(s,10);
  assert.equal(p.resolve('fleet',[s.fleets[0]],10).action,'attack');
  assert.equal(p.resolve('fleet',[s.fleets[1]],10).action,'attack');
  assert.equal(p.resolve('fleet',[s.fleets[2]],10).action,'idle');
  s.fleets[2].tile='a';s.fleets[2].node='sea:a';p.ingest(s,30);
  assert.equal(p.resolve('fleet',[s.fleets[2]],30).action,'sailing');
  s.fleets.push(fleet('ambiguous','shore','wintermere'));
  p.ingest(s,1200);s.militaryEvents.push({id:2,turn:1,action:'naval',attacker:'ashen',defender:'wintermere',tile:'shore'});p.ingest(s,1300);
  assert.equal(p.resolve('fleet',s.fleets.filter(f=>f.owner==='wintermere'),1300).action,'idle');
});

test('fog loss drops clocks; first sight, load, rewind and viewer switches never replay history',()=>{
  const s=world(),p=new SpritePlayback();s.militaryEvents=[{id:1,turn:1,action:'battle',attacker:'ashen',attackerArmyId:'a',tile:'a'}];p.ingest(s,0);
  assert.equal(p.resolve('army',s.armies,0).action,'idle');
  s.armies[0].tile='b';p.ingest(s,10);assert.equal(p.resolve('army',s.armies,10).action,'march');
  s.tiles.b.fog='explored';p.ingest(s,20);assert.equal(p.active.size,0);
  s.tiles.b.fog='visible';p.ingest(s,30);assert.equal(p.active.size,0);
  s.armies[0].tile='a';s.knowledgeView='wintermere';p.ingest(s,40);assert.equal(p.active.size,0);
  s.turn=0;s.armies[0].tile='b';p.ingest(s,50);assert.equal(p.active.size,0);
  s.seed=2;s.armies[0].tile='a';p.ingest(s,60);assert.equal(p.active.size,0);
});

test('construction uses existing progress boundaries and stops at completion',()=>{
  assert.equal(constructionStage(null),null);
  assert.equal(constructionStage({remaining:6,total:6}),1);
  assert.equal(constructionStage({remaining:4,total:6}),2);
  assert.equal(constructionStage({remaining:2,total:6}),3);
  assert.equal(constructionStage({remaining:0,total:6}),null);
  assert.equal(constructionStage({remaining:2,total:0}),null);
});

test('one scheduler serves hundreds of sprites, with slower idle, hidden pause and reduced motion',()=>{
  const calls=[],cleared=[];let redraws=0;
  const scheduler=new SpriteScheduler(()=>redraws++,{setTimer:(fn,ms)=>{calls.push({fn,ms});return calls.length;},clearTimer:id=>cleared.push(id)});
  scheduler.begin(0);for(let n=0;n<500;n++)scheduler.use(sheets.troops.levy,'idle',0);scheduler.end();
  assert.equal(calls.length,1);assert.equal(calls[0].ms,250);calls[0].fn();assert.equal(redraws,1);
  scheduler.begin(250);scheduler.use(sheets.troops.levy,'march',0);scheduler.end();assert.equal(calls[1].ms,125);
  scheduler.begin(300,false,true);scheduler.use(sheets.troops.levy,'attack',0);scheduler.defer(1);scheduler.end();assert.equal(calls.length,2);assert.equal(cleared.length,1);
  scheduler.begin(400,true);scheduler.use(sheets.troops.levy,'attack',0);scheduler.end();assert.equal(calls.length,2);
  scheduler.defer(60000);scheduler.end();assert.equal(calls[2].ms,60000,'reduced motion still permits an asset availability retry');
});

test('shared loads, malformed atlas fallback, retry and exact canvas source cropping',async()=>{
  const requests=[],draws=[];let correct=false;
  const originalImage=globalThis.Image;
  globalThis.Image=class {
    naturalWidth=1280;naturalHeight=480;
    set src(url){if(!url)return;requests.push(url);if(!correct)this.naturalWidth=1200;queueMicrotask(()=>this.onload?.());}
    async decode(){}
  };
  try {
    const cache=new AssetCache(),c={drawImage:(...args)=>draws.push(args)},spec={...sheets.troops.levy,path:'sprites/test-validation.png'},url=ironThronesAsset(spec.path);
    assert.equal(cache.drawSprite(c,spec,'attack',375,0,0,40,40),false);
    await loadArt(url);assert.equal(cache.drawSprite(c,spec,'attack',375,0,0,40,40),false);
    assert.equal(draws.length,0);assert.equal(requests.length,1);assert.ok(cache.spriteRetryAt>Date.now());
    // An expired failure is retried automatically, without changing its URL.
    correct=true;const realNow=Date.now;Date.now=()=>realNow()+61000;
    try {assert.equal(cache.drawSprite(c,spec,'attack',375,0,0,40,40),false);await loadArt(url);} finally {Date.now=realNow;}
    assert.equal(cache.drawSprite(c,spec,'attack',375,0,0,40,40),true);
    assert.deepEqual(draws.at(-1).slice(1),[480,320,160,160,0,0,40,40]);assert.equal(requests.length,2);
    const second=new AssetCache();assert.equal(second.drawSprite(c,spec,'idle',0,0,0,40,40),true);assert.equal(requests.length,2);
  } finally {globalThis.Image=originalImage;}
});

test('all ownership outlines are prepared once at display size and reused for playback',()=>{
  let allocations=0;const cropped=[];
  const make=()=>{allocations++;return {getContext:()=>({save(){},restore(){},translate(){},scale(){},clearRect(){},fillRect(){},drawImage(...args){if(args.length===9)cropped.push(args);}})};};
  const cache=new SpriteAtlasOutlines(16*1024*1024,make),image={naturalWidth:1280,naturalHeight:480};
  const atlas=cache.get(image,'levy',sheets.troops.levy,44,44,'red',1,2);
  assert.equal(allocations,2);assert.equal(cropped.length,72);assert.ok(cropped.every(args=>args[3]===160&&args[4]===160));
  for(let n=0;n<500;n++)assert.equal(cache.get(image,'levy',sheets.troops.levy,44,44,'red',1,2),atlas);
  assert.equal(allocations,2);assert.ok(cache.pixels<=cache.maxPixels);
});
