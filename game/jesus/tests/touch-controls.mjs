import assert from 'node:assert/strict';
import {InputController} from '../js/input.js';
import {createPlayer,updatePlayer} from '../js/physics.js';
import {updateBlessings,useAbility} from '../js/powers/blessings.js';
import {cleanLayouts,resolveControls,controlsOverlap,layoutMode,TOUCH_CONTROLS} from '../js/touch-layout.js';
class Target{
 constructor(){this.listeners={};this.style={};this.classes=new Set();this.classList={add:n=>this.classes.add(n),remove:n=>this.classes.delete(n),toggle:(n,on)=>on?this.classes.add(n):this.classes.delete(n)};}
 addEventListener(name,fn){(this.listeners[name]||=[]).push(fn);}
 emit(type,extra={}){for(const fn of this.listeners[type]||[])fn({type,preventDefault(){},pointerId:1,clientX:90,clientY:50,...extra});}
 setPointerCapture(){}
 getBoundingClientRect(){return{left:0,top:0,width:100,height:100};}
}
globalThis.window=new Target();globalThis.document=new Target();globalThis.localStorage={getItem:()=>null,setItem(){}};
const input=new InputController(),pad=new Target(),stick=new Target(),jump=new Target(),dash=new Target(),ability=new Target();input.bindMobile(pad,stick,jump,dash,ability);input.enabled=true;
pad.emit('pointerdown',{pointerId:10});jump.emit('pointerdown',{pointerId:11});dash.emit('pointerdown',{pointerId:12});ability.emit('pointerdown',{pointerId:13});
assert.ok(input.horizontal()>.9);assert.equal(input.jumpHeld(),true);assert.equal(input.jumpPressed(),true);assert.equal(input.dashPressed(),true);assert.equal(input.abilityPressed(),true);assert.equal(input.jumpPressed(),false,'one press per edge');
dash.emit('pointerup',{pointerId:12});assert.equal(input.jumpHeld(),true);assert.equal(input.actionHeld('ability'),true);assert.ok(input.horizontal()>.9);
jump.emit('pointerdown',{pointerId:14});jump.emit('pointercancel',{pointerId:11});assert.equal(input.jumpHeld(),true);assert.ok(jump.classes.has('held'),'second finger keeps its own hold');assert.equal(input.jumpPressed(),true);
ability.emit('lostpointercapture',{pointerId:13});assert.equal(input.actionHeld('ability'),false);assert.equal(input.jumpHeld(),true);assert.ok(input.horizontal()>.9);
window.emit('pointerup',{pointerId:14});assert.equal(input.jumpHeld(),false,'window release is a safe fallback');assert.ok(input.horizontal()>.9);
jump.emit('pointerdown',{pointerId:15});dash.emit('pointerdown',{pointerId:16});jump.emit('pointercancel',{pointerId:15});assert.equal(input.jumpPressed(),false,'cancel removes only its own queued press');assert.equal(input.dashPressed(),true);
window.emit('keydown',{key:' ',repeat:false});jump.emit('pointerdown',{pointerId:17});jump.emit('pointerup',{pointerId:17});assert.equal(input.jumpHeld(),true,'touch release cannot cancel keyboard hold');window.emit('keyup',{key:' '});
input.reset();assert.equal(input.horizontal(),0);assert.equal(input.jumpHeld(),false);assert.equal(input.dashPressed(),false);assert.equal(jump.classes.size,0);assert.equal(pad.classes.size,0);
input.enabled=false;jump.emit('pointerdown',{pointerId:99});pad.emit('pointerdown',{pointerId:98});assert.equal(input.touchHeld.size,0);assert.equal(input.joystickPointer,null,'editing layout cannot play the game');
const dt=1/120,world=()=>({width:5000,platforms:[{id:'floor',x:0,y:468,w:5000,h:500}],movers:[],effects:[],enemies:[]}),controls=(jump=false,dash=false)=>({horizontal:()=>1,jumpPressed:()=>jump,jumpHeld:()=>true,dashPressed:()=>dash});
for(const order of ['together','jump-first','dash-first']){
 const w=world(),p=Object.assign(createPlayer(100,406),{onGround:true,groundId:'floor'});
 if(order==='together')updatePlayer(p,w,controls(true,true),dt);
 if(order==='jump-first'){updatePlayer(p,w,controls(true,false),dt);updatePlayer(p,w,controls(false,true),dt);}
 if(order==='dash-first'){updatePlayer(p,w,controls(false,true),dt);updatePlayer(p,w,controls(true,false),dt);}
 assert.ok(p.vx>600&&p.vy< -600&&p.dashTimer>0,`${order}: both horizontal burst and upward jump survive`);
 const motion={vx:p.vx,vy:p.vy,dash:p.dashTimer};updateBlessings(p,w,0);w.enemies=[{id:'target',type:'hound',x:p.x+150,y:p.y,w:40,h:52,hp:2}];assert.equal(useAbility(p,w,null),'holyLight');assert.deepEqual({vx:p.vx,vy:p.vy,dash:p.dashTimer},motion,'blessing does not stop movement');
}
for(const [width,height]of [[320,568],[390,844],[568,320],[844,390],[1280,720]])for(const insets of [false,true])for(const editing of [false,true]){
 const view={width,height,topLimit:editing?(width>height?120:180):0,...(insets?{left:32,right:20,bottom:24,top:24}:{})};
 for(const profile of [{},Object.fromEntries(Object.keys(TOUCH_CONTROLS).map(key=>[key,{x:.99,y:.99,size:160}]))]){
  const positions=Object.values(resolveControls(profile,view));
  positions.forEach((p,i)=>{assert.ok(p.x-p.size/2>=(view.left||0)+11);assert.ok(p.x+p.size/2<=width-(view.right||0)-11);assert.ok(p.y+p.size/2<=height-(view.bottom||0)-11);assert.ok(p.y-p.size/2>=76);for(const other of positions.slice(i+1))assert.equal(controlsOverlap(p,other),false,`${width}×${height}: hit areas overlap`);});
 }
}
const layouts=cleanLayouts({portrait:{jump:{x:.5,y:.7,size:90}},landscape:{jump:{x:.8,y:.8,size:110}}});assert.equal(layouts.portrait.jump.size,90);assert.equal(layouts.landscape.jump.size,110);assert.equal(layoutMode({width:844,height:390}),'landscape');
assert.deepEqual(cleanLayouts({portrait:{jump:{x:'bad',y:.9,size:NaN}}}),{portrait:{},landscape:{}});assert.equal(cleanLayouts({portrait:{jump:{x:100,y:-3,size:999}}}).portrait.jump.size,120);
console.log('Independent simultaneous touches, partial release/cancel, keyboard coexistence, combined jump/dash/blessing, and safe portrait/landscape layouts passed.');
