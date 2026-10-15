const {test}=require('node:test');
const assert=require('node:assert/strict');
const {MAP_IDS,describeTile,hash,geometryStamp,createRenderer}=require('../terrain.js');

function field(width=16,height=16){return Array.from({length:width*height},(_,i)=>({x:i%width,y:Math.floor(i/width),type:'ground',blocked:false}))}
function drawing(){const calls=[];const context={globalAlpha:1,drawImage(...args){calls.push(args)},save(){},restore(){},beginPath(){},rect(){},arc(){},clip(){},moveTo(){},lineTo(){},stroke(){},translate(){},rotate(){}};return {calls,context,canvas:(w,h)=>({width:w,height:h,getContext:()=>context})}}
test('terrain startup waits for decoding and retries a failed sheet without reloading ready sheets',async()=>{
  const original=global.Image,requests=[],decodes=[];let failGround=true;
  global.Image=class{
    constructor(){this.width=this.naturalWidth=this.height=this.naturalHeight=1024}
    set src(url){this.url=url;requests.push(url);queueMicrotask(()=>this.onload?.())}
    decode(){return new Promise((resolve,reject)=>decodes.push(()=>{if(failGround&&this.url.endsWith('classic.terrain.webp')){failGround=false;reject(new Error('Decode failed'))}else resolve()}))}
  };
  try{
    const renderer=createRenderer(),first=renderer.load('classic');let settled=false;first.then(()=>{settled=true});
    await new Promise(resolve=>setImmediate(resolve));assert.equal(settled,false);assert.equal(decodes.length,3);
    decodes.splice(0).forEach(done=>done());assert.equal(await first,false);assert.deepEqual(renderer.stats.failures,['classic']);
    assert.equal(renderer.stats.failureDetails[0].stage,'decode');assert.equal(renderer.stats.failureDetails[0].reason,'Decode failed');assert.match(renderer.stats.failureDetails[0].url,/classic.terrain.webp$/);
    const terrain=field();for(let now=0;now<3;now++)renderer.prepare({mapId:'classic',terrain,seed:1,width:16,height:16,quality:'medium',now});
    await new Promise(resolve=>setImmediate(resolve));assert.equal(requests.length,3,'rendering with a fallback must not retry a failed image every frame');
    const retry=renderer.load('classic');await new Promise(resolve=>setImmediate(resolve));assert.equal(decodes.length,1);
    decodes.shift()();assert.equal(await retry,true);assert.deepEqual(renderer.stats.failures,[]);assert.deepEqual(renderer.stats.failureDetails,[]);assert.equal(requests.length,4);
  }finally{global.Image=original}
});
async function renderer(){global.Image=class{constructor(){this.width=this.naturalWidth=1024;this.height=this.naturalHeight=1024}set src(value){this.url=value;this.onload()}};const paint=drawing(),r=createRenderer({makeCanvas:paint.canvas});await r.load('classic');return {r,paint}}

test('six battlefields use a deterministic independent visual hash including seed zero',()=>{
  assert.equal(MAP_IDS.length,6);const tiles=field(),at=(x,y)=>x>=0&&y>=0&&x<16&&y<16?tiles[y*16+x]:null;
  const before=JSON.stringify(tiles),a=describeTile(tiles[17],0,at),b=describeTile(tiles[17],0,at);
  assert.deepEqual(a,b);assert.notEqual(a.noise,describeTile(tiles[17],1,at).noise);assert.equal(JSON.stringify(tiles),before);
});

test('autotile masks distinguish straight cliff edges and concave corners from map geometry',()=>{
  const tiles=field(3,3),at=(x,y)=>x>=0&&y>=0&&x<3&&y<3?tiles[y*3+x]:null;
  tiles[4].blocked=true;tiles[4].type='rock';const cliff=describeTile(tiles[4],123,at);assert.equal(cliff.openMask&15,15);assert.ok(cliff.index>=8&&cliff.index<=11);
  tiles[4].blocked=false;tiles[4].type='ground';tiles[0].type='lane';const open=describeTile(tiles[4],123,at);assert.equal(open.sameMask&15,15);assert.ok(open.innerCorners&1);assert.ok(open.outerCorners&1);
});

test('static chunks reuse identical geometry across remote snapshots and invalidate changed tiles',async()=>{
  const {r,paint}=await renderer(),tiles=field();const input={mapId:'classic',seed:0,terrain:tiles,width:16,height:16,quality:'medium',now:10};r.prepare(input);const before=JSON.stringify(tiles);
  for(let i=0;i<8;i++)assert.equal(r.drawTile(paint.context,tiles[i],{x:i*50,y:0},50),true);
  assert.equal(r.stats.builds,1);assert.equal(r.stats.chunks,1);assert.equal(r.stats.hits,7);
  r.prepare({...input,terrain:JSON.parse(before),now:20});r.drawTile(paint.context,tiles[0],{x:0,y:0},50);assert.equal(r.stats.builds,1);
  const changed=JSON.parse(before);changed[0].blocked=true;changed[0].type='rock';r.prepare({...input,terrain:changed,now:30});r.drawTile(paint.context,changed[0],{x:0,y:0},50);assert.equal(r.stats.builds,2);assert.equal(JSON.stringify(tiles),before);assert.notEqual(geometryStamp(changed),geometryStamp(tiles));
});

test('resources, ambient loops and combat sheets draw only permitted visible information',async()=>{
  const {r,paint}=await renderer(),tiles=field();r.prepare({mapId:'classic',seed:42,terrain:tiles,width:16,height:16,quality:'medium',now:0});
  const node={x:1,y:1,type:'mineral',amount:500,maxAmount:500},screen={x:10,y:20},before=paint.calls.length;
  assert.equal(r.drawResource(paint.context,node,screen,64,1000,false),false);
  assert.equal(r.drawResource(paint.context,{...node,amount:0},screen,64,1000,true),false);
  assert.equal(r.drawAmbient(paint.context,tiles[17],screen,64,1000,false),false);
  assert.equal(r.drawEffect(paint.context,'explosion',10,10,40,70,{visible:false}),false);
  assert.equal(paint.calls.length,before);
  assert.equal(r.drawResource(paint.context,node,screen,64,1000,true),true);
  assert.equal(r.drawEffect(paint.context,'explosion',10,10,40,70,{visible:true}),true);
  assert.equal(r.drawEffect(paint.context,'dust',10,10,40,70,{quality:'low'}),false);
});

test('quality changes rebuild resolution once and low quality suppresses decorative loops',async()=>{
  const {r,paint}=await renderer(),tiles=field(),input={mapId:'classic',seed:12,terrain:tiles,width:16,height:16,now:0};r.prepare({...input,quality:'medium'});r.drawTile(paint.context,tiles[0],{x:0,y:0},64);const before=r.stats.builds;
  r.prepare({...input,quality:'low'});r.drawTile(paint.context,tiles[0],{x:0,y:0},64);assert.equal(r.stats.builds,before+1);assert.equal(r.stats.quality,'low');
  for(const tile of tiles)assert.equal(r.drawAmbient(paint.context,tile,{x:0,y:0},64,1000,true),false);
});

test('every map holds resource and ambient frames twice as long without slowing combat effects',async()=>{
  const {r,paint}=await renderer(),tiles=field(32,32),seed=42,at=(x,y)=>x>=0&&y>=0&&x<32&&y<32?tiles[y*32+x]:null;
  const ambient=tiles.find(tile=>describeTile(tile,seed,at).ambient);assert.ok(ambient);
  const screen={x:0,y:0},node={x:1,y:1,type:'mineral',amount:500,maxAmount:500};
  const frame=()=>paint.calls.at(-1)[1]/128;
  for(const mapId of MAP_IDS){
    await r.load(mapId);r.prepare({mapId,seed,terrain:tiles,width:32,height:32,quality:'medium',now:0});
    for(const type of ['mineral','gas']){
      const phase=2*(hash(seed,node.x,node.y)%2000),start=(480-phase%480)%480;
      r.drawResource(paint.context,{...node,type},screen,64,start,true);const first=frame();
      r.drawResource(paint.context,{...node,type},screen,64,start+479,true);assert.equal(frame(),first,mapId+' '+type+' holds its frame for 480 ms');
      r.drawResource(paint.context,{...node,type},screen,64,start+480,true);assert.equal(frame(),(first+1)%4);
      r.drawResource(paint.context,{...node,type},screen,64,start+1920,true);assert.equal(frame(),first,'resource cycle is 1920 ms');
    }
    const phase=2*(describeTile(ambient,seed,at).noise%1700),start=(520-phase%520)%520;
    r.drawAmbient(paint.context,ambient,screen,64,start,true);const first=frame();
    r.drawAmbient(paint.context,ambient,screen,64,start+519,true);assert.equal(frame(),first,mapId+' ambient holds its frame for 520 ms');
    r.drawAmbient(paint.context,ambient,screen,64,start+520,true);assert.equal(frame(),(first+1)%4);
    r.drawAmbient(paint.context,ambient,screen,64,start+2080,true);assert.equal(frame(),first,'ambient cycle is 2080 ms');
  }
  for(const type of ['ballistic','energy','organic','shield','explosion']){
    r.drawEffect(paint.context,type,0,0,64,69);assert.equal(frame(),0);
    r.drawEffect(paint.context,type,0,0,64,70);assert.equal(frame(),1,'combat still advances at 70 ms');
  }
});

test('normal high-quality desktop views keep every visible chunk cached within 32 MiB',async()=>{
  const {r,paint}=await renderer(),terrain=field(128,128),input={mapId:'classic',terrain,seed:42,width:128,height:128,quality:'high',tileSize:40,now:1000},bounds={left:0,top:0,right:34,bottom:22},screen=(x,y)=>({x:x*40,y:y*40});
  r.prepare(input);assert.equal(r.drawRegion(paint.context,bounds,screen,40),true);assert.equal(r.stats.builds,15);assert.equal(r.stats.chunks,15);assert.ok(r.stats.chunkBytes<=32*1024*1024);
  const warmed=r.stats.builds;
  for(let i=1;i<=4;i++){r.prepare({...input,now:1000+i*17});r.drawRegion(paint.context,bounds,screen,40)}
  assert.equal(r.stats.builds,warmed,'a stationary normal-zoom desktop view must not recreate visible chunks each frame');
  // A broad overview and subsequent return each change resolution once, then reuse.
  const overview={...input,tileSize:12,now:1200},whole={left:0,top:0,right:127,bottom:127};
  r.prepare(overview);r.drawRegion(paint.context,whole,screen,12);const wide=r.stats.builds;
  r.prepare({...overview,now:1217});r.drawRegion(paint.context,whole,screen,12);assert.equal(r.stats.builds,wide);assert.ok(r.stats.chunkBytes<=32*1024*1024);
  r.prepare({...input,now:1300});r.drawRegion(paint.context,bounds,screen,40);const returned=r.stats.builds;
  r.prepare({...input,now:1317});r.drawRegion(paint.context,bounds,screen,40);assert.equal(r.stats.builds,returned);assert.ok(r.stats.chunkBytes<=32*1024*1024);
});
