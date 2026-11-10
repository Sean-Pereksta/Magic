import test from 'node:test';
import assert from 'node:assert/strict';

function surface(width=800,height=600){const calls=[],gradient={addColorStop(){}};const ctx=new Proxy({calls},{get(o,key){if(key==='createLinearGradient'||key==='createRadialGradient')return ()=>gradient;if(key in o)return o[key];return (...args)=>{assert(args.every(a=>typeof a!=='number'||Number.isFinite(a)),`finite ${String(key)}`);calls.push({method:key,args,...(['fillText','strokeText'].includes(key)?{font:o.font}:{})});};},set(o,k,v){o[k]=v;return true;}});return {width,height,ctx,getContext:()=>ctx,getBoundingClientRect:()=>({left:0,top:0,width,height})};}
globalThis.window={devicePixelRatio:1};globalThis.document={createElement:()=>surface()};
const {Renderer}=await import('../render.js');
const snapshot=Object.freeze({id:2,type:'printer',team:1,x:350,y:300,r:44,hp:250,maxHp:550,constructing:5,buildTotal:10,queue:Object.freeze([{remaining:4,total:8}]),lastSeen:7,lift:0});
test('remembered world artwork stays static as match time advances and performs no live lookup',()=>{
 const g={width:800,height:600,map:{},entities:[],getEntity(){throw new Error('memory may not inspect live entities');}},canvas=surface(),r=new Renderer(canvas,g);
 for(const type of ['printer','airplane','cd','mage','divider','scissors','wall']){const remembered=Object.freeze({...snapshot,type});r.ctx.calls.length=0;r.time=8;r.drawRememberedEntity(r.ctx,remembered);const before=JSON.stringify(r.ctx.calls);r.ctx.calls.length=0;r.time=900;r.drawRememberedEntity(r.ctx,remembered);assert.equal(JSON.stringify(r.ctx.calls),before,`${type} observation has a frozen pose and status`);}
});
test('world and minimap ghosts use the snapshot without becoming selected or live units',()=>{
 const g={width:800,height:600,time:1,map:{},entities:[],getRememberedEntities:()=>[snapshot]},canvas=surface(),r=new Renderer(canvas,g),seen=[],live=[];r.drawGround=()=>{};r.drawRememberedEntity=(_c,e)=>seen.push(e);r.drawEntity=(_c,e)=>live.push(e);r.draw({selected:new Set([snapshot.id])});assert.deepEqual(seen,[snapshot]);assert.deepEqual(live,[]);
 const mini=surface(160,120);r.drawMinimap(mini);assert(mini.ctx.calls.some(q=>q.method==='strokeRect'&&q.args[0]===66.5&&q.args[1]===56.5),'minimap marks the remembered position');
});

test('last-seen printer annotations clear its full artwork and stay legible at wide zoom',()=>{
 const g={width:800,height:600,map:{},entities:[]},canvas=surface(),r=new Renderer(canvas,g);r.camera.zoom=.4;r.drawRememberedEntity(r.ctx,snapshot);
 const top=snapshot.y-9-94*.72,bottom=snapshot.y-9+94*.72,label=r.ctx.calls.find(q=>q.method==='fillText'&&q.args[0]==='LAST SEEN');
 assert(label.args[2]>bottom,'the memory pill sits below the full printer, including its paper tray');assert(Number.parseFloat(label.font.match(/[\d.]+px/)[0])*.4>=9,'the marker remains at least nine screen pixels tall');
 assert(r.ctx.calls.some(q=>q.method==='roundRect'&&q.args[0]>100&&q.args[1]+q.args[3]<top),'the health bar stays above the full illustration');
});
test('live conveyor reversals and machine motion cannot animate outside current vision',()=>{
 const fog={cell:40,cols:20,rows:15,revision:1,teams:[{visible:new Uint8Array(300),explored:new Uint8Array(300).fill(1)}]},g={width:800,height:600,time:2,map:{conveyors:[{x:200,y:200,w:400,h:120,dx:1,dy:0}]},entities:[],fog},canvas=surface(),r=new Renderer(canvas,g);r.drawGround=()=>{};r.draw();assert(!canvas.ctx.calls.some(q=>q.method==='rotate'),'remembered belt ground remains static');
 g.map.conveyors[0].reversedUntil=20;g.time=5;canvas.ctx.calls.length=0;r.draw();assert(!canvas.ctx.calls.some(q=>q.method==='rotate'),'a hidden reversal does not update remembered terrain');
 fog.teams[0].visible.fill(1);fog.revision++;canvas.ctx.calls.length=0;r.draw();assert(canvas.ctx.calls.some(q=>q.method==='rotate'),'currently visible belt motion remains animated');
});
