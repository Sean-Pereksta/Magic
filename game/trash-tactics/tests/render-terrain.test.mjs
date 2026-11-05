import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../engine.js';

function surface(width=1000,height=700){
 const calls=[];
 const gradient={addColorStop(){}};
 const ctx=new Proxy({calls},{get(target,key){
  if(key==='createLinearGradient'||key==='createRadialGradient')return ()=>gradient;
  if(key in target)return target[key];
  return (...args)=>{
   assert(args.every(a=>typeof a!=='number'||Number.isFinite(a)),`finite ${String(key)} coordinates`);
   calls.push({method:key,args,color:target.fillStyle});
  };
 },set(target,key,value){target[key]=value;return true;}});
 return {width,height,ctx,getContext:()=>ctx,getBoundingClientRect:()=>({left:0,top:0,width,height})};
}
globalThis.window={devicePixelRatio:1};
globalThis.document={createElement:()=>surface()};
const {Renderer}=await import('../render.js');

test('all six terrain layouts draw with finite geometry and reuse the static map',()=>{
 for(const map of ['desktop','breakroom'])for(const players of [2,3,4]){
  const game=new Game({map,players}),canvas=surface(),renderer=new Renderer(canvas,game);
  const selection=new Set(game.entities.filter(e=>e.team===0).map(e=>e.id));
  renderer.draw({selected:selection,pointer:{x:1200,y:800},mode:'printer'});
  assert(renderer._groundCanvas,'detailed static surface is cached');
  const ground=renderer._groundCanvas,paintCalls=ground.ctx.calls.length;
  assert(ground.width*ground.height<=8000000,'large-map scenery stays inside the 8 MP cache budget');
  renderer.draw({selected:selection});renderer.drawMinimap(surface(220,147));
  assert.equal(renderer._groundCanvas,ground);
  assert.equal(ground.ctx.calls.length,paintCalls,'camera and minimap frames do not repaint scenery');
 }
});

test('large battlefields cap scenery memory while keeping world coordinates intact',()=>{
 const game={width:4800,height:3200,map:{},entities:[]},canvas=surface(),renderer=new Renderer(canvas,game);
 renderer.paintGround=()=>{};renderer.drawGround(renderer.ctx);
 assert(renderer._groundCanvas.width*renderer._groundCanvas.height<=8000000);
 assert(renderer._groundCanvas.width<game.width,'only static scenery is downsampled');
 const draw=canvas.ctx.calls.find(c=>c.method==='drawImage');
 assert.deepEqual(draw.args.slice(1),[0,0,4800,3200],'cached scenery retains the entire battlefield extent');
});

test('resizing from desktop to mobile preserves the camera world center',()=>{
 const canvas=surface(1366,900),game={width:4800,height:3200,map:{},entities:[]};
 const renderer=new Renderer(canvas,game);renderer.centerOn(2100,1500);
 const before={x:renderer.camera.x+renderer.width/renderer.camera.zoom/2,y:renderer.camera.y+renderer.height/renderer.camera.zoom/2};
 canvas.getBoundingClientRect=()=>({left:0,top:0,width:390,height:844});renderer.resize();
 const after={x:renderer.camera.x+renderer.width/renderer.camera.zoom/2,y:renderer.camera.y+renderer.height/renderer.camera.zoom/2};
 assert(Math.abs(before.x-after.x)<1e-8);assert(Math.abs(before.y-after.y)<1e-8);
 assert.equal(renderer.width,390);assert.equal(renderer.height,844);
});

test('hidden enemies, resources, shots and combat effects never reach world drawing',()=>{
 const own={id:1,type:'rock',team:0,hp:100,x:300,y:400};
 const enemy={id:2,type:'paper',team:1,hp:30,x:350,y:400};
 const resource={id:3,type:'trash',team:-1,hp:1,x:350,y:420};
 const game={width:2400,height:1600,time:0,map:{},entities:[own,enemy,resource],
  canSee:(_team,x)=>x<340,isVisible:e=>e.x<340,
  effects:[{id:'seen',x:320,y:400},{id:'hidden',x:350,y:400}],
  projectiles:[{id:'seen',x:320,y:400},{id:'hidden',x:350,y:400}]};
 const renderer=new Renderer(surface(),game);renderer.centerOn(350,400);
 renderer.drawGround=()=>{};const entities=[],effects=[],shots=[];
 renderer.drawEntity=(_c,e)=>entities.push(e.id);
 renderer.drawEffect=(_c,e)=>effects.push(e.id);
 renderer.drawProjectile=(_c,e)=>shots.push(e.id);
 renderer.draw();
 assert.deepEqual(entities,[own.id]);assert.deepEqual(effects,['seen']);assert.deepEqual(shots,['seen']);
 game.canSee=()=>true;game.isVisible=()=>true;renderer.draw();
 assert(entities.includes(enemy.id)&&entities.includes(resource.id),'scouted entities become visible');
});

test('the minimap shares enemy visibility, while retaining the player army',()=>{
 const own={id:1,type:'rock',team:0,hp:100,x:400,y:400};
 const enemy={id:2,type:'paper',team:1,hp:100,x:800,y:400};
 let revealed=false;
 const game={width:2400,height:1600,map:{},entities:[own,enemy],isVisible:e=>e.team===0||revealed};
 const renderer=new Renderer(surface(),game),mini=surface(240,160);
 renderer.drawMinimap(mini);
 assert(mini.ctx.calls.some(c=>c.method==='fill'&&c.color==='#269b98'));
 assert(!mini.ctx.calls.some(c=>c.method==='fill'&&c.color==='#ed795d'));
 revealed=true;renderer.drawMinimap(mini);
 assert(mini.ctx.calls.some(c=>c.method==='fill'&&c.color==='#ed795d'));
});

test('fog caches the vision revision and distinguishes explored ground from unknown ground',()=>{
 const game={width:80,height:40,map:{},entities:[],fog:{cell:40,cols:2,rows:1,revision:1,
  teams:[{visible:new Uint8Array([0,0]),explored:new Uint8Array([1,0])}]}};
 const renderer=new Renderer(surface(),game);renderer.drawFog(renderer.ctx);
 const fog=renderer._fogCanvas,paints=fog.ctx.calls.filter(c=>c.method==='fillRect');
 assert.equal(paints.length,2);assert.notEqual(paints[0].color,paints[1].color);
 const count=fog.ctx.calls.length;renderer.drawFog(renderer.ctx);
 assert.equal(fog.ctx.calls.length,count,'no texture rebuild until vision changes');
 game.fog.teams[0].visible[0]=1;game.fog.revision++;renderer.drawFog(renderer.ctx);
 assert(fog.ctx.calls.length>count);
});

test('a build ghost cannot probe hidden enemy buildings through fog',()=>{
 let scouted=false,occupancyChecks=0;
 const game={width:2400,height:1600,map:{},entities:[],canSee:()=>scouted,
  canBuild:()=>{occupancyChecks++;return true;}};
 const renderer=new Renderer(surface(),game);
 renderer.drawPlacement(renderer.ctx,{pointer:{x:1800,y:1000},mode:'cabinet'});
 assert.equal(occupancyChecks,0,'unknown ground never queries live occupancy');
 assert(renderer.ctx.calls.some(c=>c.method==='fill'&&c.color==='#9aa9a522'),'the unknown preview is neutral');
 scouted=true;renderer.drawPlacement(renderer.ctx,{pointer:{x:1800,y:1000},mode:'cabinet'});
 assert.equal(occupancyChecks,1,'scouted ground uses normal placement validation');
});

test('hidden new enemy desks cannot leak through their live ground halo',()=>{
 const own={id:1,type:'desk',team:0,hp:100,x:400,y:400};
 const hidden={id:2,type:'desk',team:1,hp:100,x:1800,y:1000};
 let scouted=false;
 const game={width:2400,height:1600,map:{},entities:[own,hidden],isVisible:e=>e.team===0||scouted};
 const renderer=new Renderer(surface(),game);
 renderer._groundMap=game.map;renderer._groundCanvas=surface();
 renderer.drawGround(renderer.ctx);
 assert.equal(renderer.ctx.calls.filter(c=>c.method==='ellipse').length,1);
 scouted=true;renderer.drawGround(renderer.ctx);
 assert.equal(renderer.ctx.calls.filter(c=>c.method==='ellipse').length,3);
});
