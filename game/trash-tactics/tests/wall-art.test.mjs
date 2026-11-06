import test from 'node:test';
import assert from 'node:assert/strict';
import {TYPES} from '../engine.js';
import {WALL_SIZE,snapWallPoint} from '../walls.js';
import {drawWallBlock,drawWallEntity,drawWallPlacement} from '../wall-art.js';

function surface(width=1000,height=700){
 const calls=[],gradient={addColorStop(){}};
 const ctx=new Proxy({calls,globalAlpha:1},{get(o,k){if(k==='createLinearGradient'||k==='createRadialGradient')return ()=>gradient;if(k in o)return o[k];return (...args)=>{for(const n of args)if(typeof n==='number')assert(Number.isFinite(n),`${String(k)} has finite coordinates`);calls.push({method:k,args,stroke:o.strokeStyle,fill:o.fillStyle});};},set(o,k,v){o[k]=v;return true;}});
 return {width,height,ctx,getContext:()=>ctx,getBoundingClientRect:()=>({left:0,top:0,width,height})};
}
globalThis.window={devicePixelRatio:1};
globalThis.document={createElement:()=>surface()};
const {Renderer,drawIcon}=await import('../render.js');
const fixture=()=>({width:4800,height:3200,map:{},entities:[],time:1,elevationAt:()=>0,canSee:()=>true,isVisible:()=>true,canBuild:()=>true});

test('every wall connection pattern retains a completely filled square core',()=>{
 for(let mask=0;mask<16;mask++){
  const {ctx}=surface();drawWallBlock(ctx,{mask,seed:mask});
  assert(ctx.calls.some(c=>c.method==='rect'&&c.args.join(',')==='-24,-24,48,48'),'solid core fills the exact 48px collision tile');
  assert(!ctx.calls.some(c=>['arc','ellipse'].includes(c.method)),'wall blocks and their reinforcements are square, without round-unit placeholders');
  assert(ctx.calls.length>100,'beveled faces, coping, tape and reinforcements are fully illustrated');
 }
});

test('wall damage and active construction use distinct visible states',()=>{
 const game=fixture(),renderer=new Renderer(surface(),game),wall={id:1,type:'wall',x:480,y:480,hp:1200,maxHp:1200,team:0,constructing:0};game.entities=[wall];
 drawWallEntity(renderer,renderer.ctx,wall,true);
 assert(!renderer.ctx.calls.some(c=>c.method==='stroke'&&c.stroke==='#6a604e'));
 renderer.ctx.calls.length=0;wall.hp=300;drawWallEntity(renderer,renderer.ctx,wall,false);
 assert(renderer.ctx.calls.some(c=>c.method==='stroke'&&c.stroke==='#6a604e'),'damaged slabs show physical cracks');
 renderer.ctx.calls.length=0;wall.constructing=1.5;wall.buildTotal=3;drawWallEntity(renderer,renderer.ctx,wall,false);
 assert(renderer.ctx.calls.some(c=>c.method==='clip'),'construction reveals the real block as it rises');
 assert(renderer.ctx.calls.some(c=>c.method==='setLineDash'&&c.args[0][0]===4),'unfinished block retains its square blueprint');
});

test('wall placement preview snaps to the same grid and uses its actual square footprint',()=>{
 const game=fixture(),canvas=surface(),renderer=new Renderer(canvas,game),pointer={x:511,y:493},expected=snapWallPoint(pointer.x,pointer.y);let placement;
 game.canBuild=(type,x,y)=>{placement={type,x,y};return true;};
 assert(drawWallPlacement(renderer,renderer.ctx,{mode:'wall',pointer}));
 assert.deepEqual(placement,{type:'wall',...expected});
 assert(renderer.ctx.calls.some(c=>c.method==='translate'&&c.args[0]===expected.x&&c.args[1]===expected.y));
 assert(renderer.ctx.calls.some(c=>c.method==='rect'&&c.args.join(',')==='-24,-24,48,48'));
 assert(!renderer.ctx.calls.some(c=>c.method==='ellipse'));
});

test('a wall ghost never probes hidden terrain occupancy or reveals connections in fog',()=>{
 const game=fixture(),renderer=new Renderer(surface(),game);let checks=0;game.canBuild=()=>{checks++;return true;};renderer.pointVisible=()=>false;
 drawWallPlacement(renderer,renderer.ctx,{mode:'wall',pointer:{x:700,y:800}});assert.equal(checks,0);
});

test('world and minimap walls draw as adjoining rectangular tiles',()=>{
 const game=fixture(),wall=(id,x)=>({id,type:'wall',x,y:480,team:0,hp:1200,maxHp:1200}),a=wall(1,480),b=wall(2,480+WALL_SIZE);game.entities=[a,b];
 const renderer=new Renderer(surface(),game);renderer.centerOn(480,480);renderer.drawGround=()=>{};renderer.draw();
 assert(!renderer.ctx.calls.some(c=>c.method==='ellipse'),'world wall artwork does not inherit circular building shadows or unit selection rings');
 const mini=surface(240,160);renderer.drawMinimap(mini);const tiles=mini.ctx.calls.filter(c=>c.method==='fillRect'&&c.fill==='#269b98');assert.equal(tiles.length,2);assert(Math.abs(tiles[0].args[0]+tiles[0].args[2]-tiles[1].args[0])<1e-8,'minimap tiles share the same boundary');
});

test('reinforced mouse coverage reads the actual weapon range in selection and placement',()=>{
 const game=fixture(),mouse={id:1,type:'mouse',team:0,hp:850,maxHp:850,x:480,y:480};game.entities=[mouse];const renderer=new Renderer(surface(),game);renderer.centerOn(480,480);renderer.drawGround=()=>{};renderer.drawEntity=()=>{};
 renderer.draw({selected:new Set([1])});assert(renderer.ctx.calls.some(c=>c.method==='ellipse'&&c.args[2]===TYPES.mouse.range&&c.args[3]===TYPES.mouse.range));
 renderer.ctx.calls.length=0;renderer.drawPlacement(renderer.ctx,{mode:'mouse',pointer:{x:600,y:600}});assert(renderer.ctx.calls.some(c=>c.method==='ellipse'&&c.args[2]===TYPES.mouse.range&&c.args[3]===TYPES.mouse.range));
 const icon=surface();drawIcon(icon.ctx,'mouse',100,100,100,0,{phase:2,attackAnim:.2,entity:{cooldown:.3}});assert(icon.ctx.calls.some(c=>c.method==='roundRect'&&c.args[0]===-26),'stronger mouse has a reinforced mount');
});
