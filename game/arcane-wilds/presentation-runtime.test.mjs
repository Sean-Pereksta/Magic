import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const read=name=>readFileSync(new URL(name,import.meta.url),'utf8');
function harness(){
 let now=1000,pads=[],casts=[],dodges=0;
 const events=new Map();const add=(name,fn)=>{if(!events.has(name))events.set(name,[]);events.get(name).push(fn);};
 const context={window:{},performance:{now:()=>now},document:{hidden:false,addEventListener:add,getElementById:()=>null},navigator:{getGamepads:()=>pads},canvas:{addEventListener:add},addEventListener:add,localStorage:{getItem:()=>null,setItem(){}},isTouch:false,keys:new Set(),mouse:{active:false},moveStick:{active:false,pointer:null,x:0,y:0},aimStick:{active:false,pointer:null,x:0,y:0},running:true,paused:false,modalPause:false,roomTransition:false,game:{player:{x:4,y:5,activeSpells:['firebolt','frostnova','chain','meteor','starfall'],spellState:{firebolt:{cd:.1}},dodgeCd:.1},roomData:{},enemies:[],projectiles:[],telegraphs:[],hazards:[],particles:[],effects:[]},clamp:(v,a,b)=>Math.max(a,Math.min(v,b)),lerp:(a,b,t)=>a+(b-a)*t,Math,Map,Set,WeakMap,Number,Array,console,$:()=>null,dodge(){dodges++;context.game.player.dodgeCd=1.25;},castSpell(slot){casts.push(slot);context.game.player.spellState[context.game.player.activeSpells[slot]]={cd:3};},interact(){},togglePause(){context.paused=!context.paused;},renderInventory(){},showOverlay(){context.modalPause=true;}};
 vm.createContext(context);vm.runInContext(read('presentation-core.js'),context);vm.runInContext(read('input-manager.js'),context);context.window.AWInput.install();
 return {context,input:context.window.AWInput,presentation:context.window.AWPresentation,casts,dodges:()=>dodges,time:value=>now=value,pads:value=>pads=value,emit:(name,data={})=>events.get(name)?.forEach(fn=>fn({preventDefault(){},target:{},...data}))};
}
test('spell and dodge buffers consume once after cooldown and never repeat',()=>{
 const h=harness();h.input.press('spell1');h.input.press('dodge');assert.equal(h.casts.length,0);h.time(1100);h.context.game.player.spellState.firebolt.cd=0;h.context.game.player.dodgeCd=0;h.input.flush();h.input.flush();assert.deepEqual(h.casts,[0]);assert.equal(h.dodges(),1);
});
test('expired intent, changed loadout, room change and modal never leak a queued spell',()=>{
 for(const condition of ['expired','loadout','room','modal']){const h=harness();h.input.press('spell1');h.context.game.player.spellState.firebolt.cd=0;if(condition==='expired')h.time(1151);if(condition==='loadout')h.context.game.player.activeSpells[0]='chain';if(condition==='room')h.context.game.roomData={};if(condition==='modal')h.context.modalPause=true;h.input.flush();assert.equal(h.casts.length,0,condition);assert.equal(h.input.queue.size,0);}
});
test('keyboard repeat does not cast again; text entry and focus loss do not move hero',()=>{
 const h=harness();h.context.game.player.spellState.firebolt.cd=0;h.emit('keydown',{code:'Digit1'});h.context.game.player.spellState.firebolt.cd=0;h.emit('keydown',{code:'Digit1',repeat:true});assert.deepEqual(h.casts,[0]);h.emit('keydown',{code:'KeyW',target:{tagName:'INPUT'}});h.input.poll();assert.equal(h.input.move.y,0);h.emit('keydown',{code:'KeyW'});h.input.poll();assert.equal(h.input.move.y,-1);h.emit('blur');h.input.poll();assert.equal(h.input.move.y,0);
});
test('radial deadzone preserves proportional analog movement and normalized diagonal speed',()=>{
 const h=harness();assert.equal(h.input.radial(.1,.1,.18).x,0);const half=h.input.radial(.59,0,.18);assert.ok(Math.abs(half.x-.5)<1e-9);const diagonal=h.input.radial(1,1,0);assert.ok(Math.abs(Math.hypot(diagonal.x,diagonal.y)-1)<1e-9);
});
test('gamepad edges cast slots four/five and disconnect clears movement',()=>{
 const h=harness(),pad={index:0,connected:true,id:'DualSense 054c',mapping:'standard',axes:[.9,0,1,0],buttons:Array.from({length:16},()=>({pressed:false,value:0}))};pad.buttons[4]={pressed:true,value:1};h.pads([pad]);h.input.poll();h.input.poll();assert.deepEqual(h.casts,[3]);assert.equal(h.input.prompt('spell4'),'L1');assert.ok(h.input.move.x>0);pad.buttons[5]={pressed:true,value:1};h.input.poll();assert.deepEqual(h.casts,[3,4]);h.pads([]);h.emit('gamepaddisconnected');h.input.poll();assert.equal(h.input.move.x,0);assert.equal(h.input.aim.active,false);
});
test('remapping swaps collisions and repeated initialization binds listeners only once',()=>{
 const h=harness();h.input.remap('keys','spell1','Digit2');assert.equal(h.input.mapping().spell2,'Digit1');h.input.install();h.context.game.player.spellState.firebolt.cd=0;h.emit('keydown',{code:'Digit2'});assert.deepEqual(h.casts,[0]);
});
test('cosmetic pool is bounded, reuses objects and clears stale payload fields',()=>{
 const h=harness(),pool=new h.presentation.Pool(2),a=pool.add({life:1,secret:'old'});pool.add({life:1});pool.tick(2);const b=pool.add({life:.5});assert.equal(a,b);assert.equal(b.secret,undefined);for(let i=0;i<10000;i++)pool.add({life:1,x:i});assert.equal(pool.items.length,2);pool.clear();assert.ok(pool.items.every(e=>e.life===0));
});
test('all entry scripts parse together without global declaration collisions',()=>{
 const html=read('../arcane-wilds.html');const scripts=[...html.matchAll(/<script src="arcane-wilds\/([^"]+)"/g)].map(m=>m[1]);assert.equal(new Set(scripts).size,scripts.length);new vm.Script(scripts.map(read).join('\n'));assert.ok(scripts.indexOf('performance.js')>scripts.indexOf('progression-expansion.js'));assert.equal(scripts.at(-1),'runtime-stability.js');
});
