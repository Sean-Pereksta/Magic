import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattlefield} from '../battlefields.js';
import {drawExpansionIcon,drawExpansionSprite,getAnimationFrame,drawWarfareFields,drawWarfareEffect,drawWarfareProjectile,animateWarfareMap,drawWarfarePlacement} from '../warfare-art.js';

function surface(width=1200,height=800){
 const calls=[],gradient={addColorStop(){}};
 const ctx=new Proxy({calls},{get(o,k){
  if(k==='createLinearGradient'||k==='createRadialGradient')return ()=>gradient;
  if(k in o)return o[k];
  return (...args)=>{for(const n of args)if(typeof n==='number')assert(Number.isFinite(n),`${String(k)} uses finite coordinates`);calls.push({method:k,args,color:o.strokeStyle||o.fillStyle});};
 },set(o,k,v){o[k]=v;return true;}});
 return {width,height,ctx,getContext:()=>ctx,getBoundingClientRect:()=>({left:0,top:0,width,height})};
}
globalThis.window={devicePixelRatio:1};
globalThis.document={createElement:()=>surface()};
const {Renderer,drawIcon}=await import('../render.js');

test('every specialist, production structure and map object has original finite artwork',()=>{
 for(const type of ['sniper','airplane','mage','divider','rollingrock','shears','workshop','laboratory','staplergate','station']){
  for(const active of [false,true]){const {ctx}=surface();const entity=active?{aim:{remaining:.8},crash:{diving:true,remaining:.5},roll:{},shadow:{},divisionCast:1}:{};
   assert.equal(drawExpansionIcon(ctx,type,'#269b98',{entity,phase:1.4,active,facing:-2,walk:active?4:0,attackAnim:active?.2:0}),true,type);
   assert(ctx.calls.length>20,`${type} includes a recognisable illustrated silhouette and details`);
  }
 }
 const {ctx}=surface();drawIcon(ctx,'rollingrock',100,100,120,0,{entity:{roll:{}},phase:3});assert(ctx.calls.some(c=>c.method==='fill'));
});

test('painted troop sprites animate in field and icon coordinates, with a loading fallback',()=>{
 const ready={complete:true,naturalWidth:512,naturalHeight:512},loading={complete:false,naturalWidth:0};
 for(const type of ['sniper','airplane','mage','divider','rollingrock','shears']){
  const {ctx}=surface();assert.equal(drawExpansionSprite(ctx,type,loading,'#269b98'),false,'unloaded artwork uses vector fallback');
  assert.equal(drawExpansionSprite(ctx,type,ready,'#269b98',{phase:1.5,walk:2,facing:2,attackAnim:.2,entity:{roll:{},shadow:{},aim:{},divisionCast:1,crash:{diving:true}}}),true);
  assert.equal(ctx.calls.filter(c=>c.method==='drawImage').length,1,`${type} paints its original raster cutout`);
 }
});

test('real troop frame rows advance and loop at their designated rates',()=>{
 for(const [state,row,fps]of [['idle',0,4],['move',1,8],['attack',2,12],['special',3,8]]){
  for(let frame=0;frame<4;frame++)assert.deepEqual(getAnimationFrame('sniper',{animationState:state,phase:(frame+.01)/fps}),{state,row,frame,index:row*4+frame,fps});
  assert.equal(getAnimationFrame('sniper',{animationState:state,phase:4/fps}).frame,0,`${state} loops rather than freezing on its final frame`);
 }
 assert.equal(getAnimationFrame('airplane',{walk:3}).state,'move');
 assert.equal(getAnimationFrame('sniper',{attackAnim:.2}).state,'attack');
 for(const [type,entity]of [['airplane',{crash:{diving:true}}],['rollingrock',{roll:{}}],['shears',{shadow:{}}],['mage',{mageCasting:.2}],['divider',{divisionCast:.8}]])assert.equal(getAnimationFrame(type,{entity}).row,3);
 assert.equal(getAnimationFrame('sniper',{entity:{aim:{remaining:1.5,total:1.5}}}).frame,0);
 assert.equal(getAnimationFrame('sniper',{entity:{aim:{remaining:.01,total:1.5}}}).frame,3,'aim reaches the final actual frame');
});

test('battlefield playback crops one actual sheet frame rather than painting the whole sheet',()=>{
 const {ctx}=surface(),sheet={complete:true,naturalWidth:1024,naturalHeight:1024};
 assert(drawExpansionSprite(ctx,'sniper',null,'#269b98',{sheet,animationState:'special',phase:.25}));
 const crop=ctx.calls.find(c=>c.method==='drawImage');assert.deepEqual(crop.args.slice(1,5),[512,768,256,256]);
 assert.equal(crop.args.length,9,'source and destination rectangles select a single real animation frame');
});

test('animated effect sheets crop early, middle and late frames for all six spell rows',async()=>{
 const PreviousImage=globalThis.Image;
 globalThis.Image=class{constructor(){this.complete=true;this.naturalWidth=768;this.naturalHeight=1536;}set src(value){this.url=value;}};
 let loaded;
 try{loaded=await import('../warfare-art.js?loaded-effects-test');}finally{if(PreviousImage===undefined)delete globalThis.Image;else globalThis.Image=PreviousImage;}
 const renderer={visible:()=>true,game:{},camera:{zoom:1}};
 for(const [row,type]of ['snipe','crash','shadowSlash','forcefield','division','rollhit'].entries())for(const [column,life]of [1,.5,.1].entries()){
  const {ctx}=surface();loaded.drawWarfareEffect(renderer,ctx,{type,x:300,y:300,life,total:1,radius:75});
  const crop=ctx.calls.find(c=>c.method==='drawImage');assert(crop,`${type} uses its painted frame artwork`);assert.deepEqual(crop.args.slice(1,5),[column*256,row*256,256,256]);
 }
});

test('all expansion maps render complete cached geometry and their animated mechanics',()=>{
 for(const id of ['canyon','conveyor'])for(const players of [2,3,4]){
  const map=createBattlefield(id,players),game={map,width:4800,height:3200,entities:[],time:2},renderer=new Renderer(surface(),game);renderer.centerOn(2400,1600);renderer.draw();
  const cache=renderer._groundCanvas,paints=cache.ctx.calls.length;assert(paints>1000,'map has detailed background and terrain');renderer.draw();assert.equal(cache.ctx.calls.length,paints,'dynamic graphics do not invalidate the scenery cache');
  renderer.drawMinimap(surface(240,160));
 }
});

test('airplanes retain the aerial draw layer above the ground army',()=>{
 const entities=[{id:1,type:'airplane',hp:80,x:400,y:300,team:0},{id:2,type:'rollingrock',hp:360,x:400,y:600,team:0},{id:3,type:'cd',hp:100,x:400,y:200,team:0}],game={map:{},entities,width:4800,height:3200,time:1};
 const renderer=new Renderer(surface(),game);renderer.centerOn(400,400);renderer.drawGround=()=>{};const order=[];renderer.drawEntity=(_c,e)=>order.push(e.id);renderer.draw();assert(order.indexOf(1)>order.indexOf(2));assert(order.indexOf(3)>order.indexOf(2));
});

test('fields and division barriers respect fog and remain finite through casting and expiry',()=>{
 const game={forceFields:[{x:400,y:400,r:110,remaining:.1,total:10}],divisions:[{x:500,y:500,r:350,remaining:9,casting:1.1,total:9}],map:{},entities:[],width:4800,height:3200},renderer=new Renderer(surface(),game);renderer.centerOn(450,450);
 for(const layer of ['ground','shield'])drawWarfareFields(renderer,renderer.ctx,layer);assert(renderer.ctx.calls.length>100);
 const count=renderer.ctx.calls.length;renderer.pointVisible=()=>false;drawWarfareFields(renderer,renderer.ctx,'ground');drawWarfareFields(renderer,renderer.ctx,'shield');assert.equal(renderer.ctx.calls.length,count,'hidden hostile fields do not reveal positions');
});

test('all special attack effects and projectiles remain finite during their full lifetime',()=>{
 const game={map:{},entities:[],width:4800,height:3200,getEntity:()=>({x:500,y:500})},renderer=new Renderer(surface(),game);renderer.centerOn(400,400);
 for(const type of ['snipe','dart','crash','forcefield','forcefieldEnd','division','divisionEnd','divisionCollapse','rollhit','shadowSlash','shadowEnd','shadowFinish','capture','gateCollapse'])for(const life of [1,.5,0])assert(drawWarfareEffect(renderer,renderer.ctx,{type,x:400,y:400,fromX:300,fromY:300,radius:110,life,total:1,team:0}));
 for(const type of ['snipe','dart','magic','stonebolt'])assert(drawWarfareProjectile(renderer,renderer.ctx,{type,x:400,y:400,fromX:300,fromY:300,life:1,team:0,targetId:1}));
});

test('belt tread motion and arrows reverse together when the control station reverses its belt',()=>{
 const belt={x:200,y:200,w:500,h:150,dx:1,dy:0,reversedUntil:0},game={map:{conveyors:[belt]},time:2,entities:[],width:4800,height:3200},renderer=new Renderer(surface(),game);renderer.time=2;renderer.centerOn(400,400);
 animateWarfareMap(renderer,renderer.ctx);const normal=renderer.ctx.calls.filter(c=>c.method==='rotate').map(c=>c.args[0]);renderer.ctx.calls.length=0;belt.reversedUntil=20;animateWarfareMap(renderer,renderer.ctx);const reverse=renderer.ctx.calls.filter(c=>c.method==='rotate').map(c=>c.args[0]);assert(normal.length>0);assert.equal(normal.length,reverse.length);assert(reverse.every((a,i)=>Math.abs(a-normal[i]-Math.PI)<1e-8),'every direction arrow reverses');
});

test('specialist route previews draw both committed waypoints and the final pending segment',()=>{
 const renderer=new Renderer(surface(),{map:{},entities:[],width:4800,height:3200});renderer.time=1;drawWarfarePlacement(renderer,renderer.ctx,{mode:'specialist',abilityType:'shadowpath',pointer:{x:700,y:600},routePoints:[{x:300,y:300},{x:500,y:350}]});assert(renderer.ctx.calls.some(c=>c.method==='setLineDash'&&c.args[0][0]===6));assert(renderer.ctx.calls.some(c=>c.method==='lineTo'&&c.args[0]===700&&c.args[1]===600));
});
