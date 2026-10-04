import test from 'node:test';
import assert from 'node:assert/strict';
import { createCombatPresentation, createEffectStore, createQualityGovernor, EFFECT_LIMITS } from './combat-presentation.mjs';

class Node {
  constructor(){this.children=[];this.parentNode=null;this.dataset={};this.className='';this.textContent='';this.style={setProperty(key,value){this[key]=value;}};
    this.classList={add:(...names)=>{this.className=[...new Set([...this.className.split(' '),...names])].join(' ');},remove:name=>{this.className=this.className.split(' ').filter(n=>n!==name).join(' ');},contains:name=>this.className.split(' ').includes(name),toggle:(name,on)=>on?this.classList.add(name):this.classList.remove(name)};}
  appendChild(node){node.remove();this.children.push(node);node.parentNode=this;return node;}
  prepend(node){node.remove();this.children.unshift(node);node.parentNode=this;}
  remove(){if(this.parentNode)this.parentNode.children=this.parentNode.children.filter(n=>n!==this);this.parentNode=null;}
  querySelector(selector){const name=selector.slice(1);for(const node of this.children){if(node.classList.contains(name))return node;const child=node.querySelector(selector);if(child)return child;}return null;}
}
function fixture(reduced=false,{inView=()=>true}={}){
  let time=0,nextId=0;const frames=new Map(),layer=new Node(),effects=new Node(),shakeOffsets=[];
  const document={hidden:false,createElement:()=>new Node()};
  const presentation=createCombatPresentation({document,now:()=>time,project:(x,y)=>({x:x*20,y:y*20}),depth:(x,y,z)=>x+y+z,
    reducedMotion:()=>reduced,inView,onFrame:(_focus,_dt,shake)=>{shakeOffsets.push(shake);},
    requestFrame:fn=>{const id=++nextId;frames.set(id,fn);return id;},cancelFrame:id=>frames.delete(id),
    createUnit:()=>{const node=new Node(),body=new Node();body.className='cm-unit-body';node.appendChild(body);return node;}});
  presentation.attach(layer,effects);
  const frame=ms=>{time+=ms;const callbacks=[...frames.values()];frames.clear();for(const fn of callbacks)fn();};
  return {presentation,layer,effects,document,frame,shakeOffsets,get time(){return time;}};
}
test('effect store enforces category/global limits and releases expired effects',()=>{
  let time=0,released=0;const store=createEffectStore({now:()=>time,release:()=>released++});
  for(let i=0;i<1000;i++)store.add({category:'particle',kind:'particle',duration:250});
  assert.equal(store.size,EFFECT_LIMITS.particles);
  for(let i=0;i<1000;i++)store.add({category:'trail',kind:'trail',duration:250});
  assert.equal(store.size,EFFECT_LIMITS.particles+EFFECT_LIMITS.trails);
  for(let i=0;i<1000;i++)store.add({kind:'ring',critical:true,duration:250});
  assert.equal(store.size,EFFECT_LIMITS.active);
  time=251;store.prune();assert.equal(store.size,0);assert.ok(released>=EFFECT_LIMITS.active);
});
test('quality governor degrades cosmetics under sustained frame pressure and recovers slowly',()=>{
  const quality=createQualityGovernor();for(let i=0;i<150;i++)quality.sample(38);
  assert.equal(quality.label,'low');
  for(let i=0;i<550;i++)quality.sample(16);assert.equal(quality.label,'high');
  quality.sample(30000);assert.equal(quality.label,'high');
});
test('stable creature nodes move between redraws; metadata updates do not restart motion',()=>{
  const h=fixture(),entity={key:'rat:one',type:'rat',x:1,y:1,facing:'east',health:20,artSignature:'east'};
  h.presentation.syncEntities([entity]);h.frame(16);const node=h.layer.children[0];
  h.presentation.syncEntities([{...entity,x:2}]);h.frame(100);
  assert.equal(h.layer.children[0],node);const first=node.style.transform;
  h.frame(100);assert.notEqual(node.style.transform,first);
  h.presentation.syncEntities([{...entity,x:2,health:18,label:'18'}]);assert.equal(h.layer.children[0],node);
  h.frame(500);assert.deepEqual(h.presentation.getPosition(entity.key),{x:2,y:1});
  assert.deepEqual(entity.x,1);
});
test('projectile impacts and removed-unit deaths clean up without requiring another redraw',()=>{
  const h=fixture(),unit={key:'rabbit:a',type:'rabbit',x:1,y:1,health:5,artSignature:'a'};
  h.presentation.syncEntities([unit]);h.frame(20);
  h.presentation.projectile({x:1,y:1},{x:3,y:1},'rocket',{duration:120});h.frame(60);
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-projectile')));
  h.frame(100);h.presentation.syncEntities([]);assert.equal(h.presentation.stats.dying,1);
  h.frame(1000);assert.equal(h.presentation.stats.effects,0);assert.equal(h.presentation.stats.dying,0);
  assert.equal(h.layer.children.length,0);assert.equal(h.effects.children.length,0);assert.ok(h.presentation.stats.pooled<=EFFECT_LIMITS.pool);
});
test('reduced motion retains attack telegraphs, projectiles, status and hit feedback',()=>{
  const h=fixture(true),unit={key:'ox:a',type:'ox',x:2,y:2,health:20,artSignature:'a',trappedUntil:1000,
    attackIntent:{x:3,y:2,executeAt:800}};
  h.presentation.syncEntities([unit]);h.presentation.projectile({x:0,y:0},{x:2,y:2},'web',{duration:300});
  h.presentation.shake(20);h.frame(20);
  assert.equal(h.presentation.stats.reducedMotion,true);assert.ok(h.effects.children.some(n=>n.classList.contains('cm-marker')));
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-projectile')));assert.ok(h.layer.children[0].classList.contains('cm-trapped'));
  assert.equal(h.layer.children[0].style['--cm-hop'],'0.00px');
  h.frame(1200);assert.equal(h.presentation.stats.markers,0);
});
test('hidden documents pause frames and resume at current authoritative positions',()=>{
  const h=fixture(),unit={key:'rat:a',type:'rat',x:0,y:0,health:4,artSignature:'a'};
  h.presentation.syncEntities([unit]);h.frame(20);h.presentation.syncEntities([{...unit,x:1}]);
  h.document.hidden=true;h.frame(500);assert.equal(h.presentation.stats.animating,false);
  h.document.hidden=false;h.presentation.resume();h.frame(20);
  assert.deepEqual(h.presentation.getPosition(unit.key),{x:1,y:0});
});
test('important projectiles displace cosmetic overload while the projectile ceiling remains fixed',()=>{
  const h=fixture();for(let i=0;i<300;i++)h.presentation.burst({x:2,y:2},'hit',10);
  h.presentation.projectile({x:0,y:0},{x:2,y:2},'web',{duration:300});h.frame(20);
  // Even heavy impact spam must leave room for a legible shot.
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-projectile')));
  assert.ok(h.presentation.stats.effects<=EFFECT_LIMITS.active);
  for(let i=0;i<300;i++)h.presentation.projectile({x:0,y:0},{x:2,y:2},'web',{duration:300});h.frame(20);
  assert.ok(h.effects.children.filter(n=>n.classList.contains('cm-projectile')).length<=EFFECT_LIMITS.projectiles);
  h.presentation.clear();assert.equal(h.presentation.stats.effects,0);assert.equal(h.presentation.stats.markers,0);
});

test('structure contacts emphasize direction and flash without shaking for ordinary attacks',()=>{
  const h=fixture(),rat=Object.freeze({key:'rat:a',type:'rat',x:1,y:1,health:5,artSignature:'a'}),target=Object.freeze({x:2,y:1}),structure=new Node();
  h.presentation.syncEntities([rat]);h.presentation.structureHit(rat.key,target,{kind:'rat',node:structure});h.frame(20);
  assert.ok(h.layer.children[0].classList.contains('cm-striking'));assert.ok(structure.classList.contains('cm-structure-hit'));
  assert.equal(structure.style['--cm-impact-x'],'2.50px');assert.equal(structure.style['--cm-impact-y'],'0.00px');
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-structure') && n.classList.contains('cm-slash')));
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-flare')));
  assert.ok(h.shakeOffsets.every(o=>Math.abs(o.x)+Math.abs(o.y)===0));
  assert.deepEqual(target,{x:2,y:1});h.frame(600);
  assert.equal(structure.classList.contains('cm-structure-hit'),false);assert.equal(h.effects.children.length,0);
  const offscreen=fixture(false,{inView:()=>false});offscreen.presentation.structureHit(null,target,{kind:'ox'});
  offscreen.presentation.captureMouse('mouse:offscreen',target,{eventId:1});assert.equal(offscreen.presentation.stats.effects,0);
});

test('heavy shield contacts retain their larger breach cue and bounded impact shake',()=>{
  const h=fixture(),ox={key:'ox:a',type:'ox',x:1,y:1,health:20,artSignature:'a'};
  h.presentation.syncEntities([ox]);h.presentation.structureHit(ox.key,{x:2,y:1},{kind:'ox',shielded:true});h.frame(20);
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-breach')));
  assert.ok(h.effects.children.some(n=>n.classList.contains('cm-flare') && n.classList.contains('cm-shield')));
  assert.ok(h.shakeOffsets.some(o=>Math.abs(o.x)+Math.abs(o.y)>0));
  assert.ok(h.shakeOffsets.every(o=>Math.abs(o.x)<=2.2 && Math.abs(o.y)<=2.2));
  h.frame(1000);assert.equal(h.effects.children.length,0);
});

test('captures follow the visible mouse, dedupe the death event and clean up without another redraw',()=>{
  const h=fixture(),mouse={key:'mouse:a',type:'mouse',x:1,y:1,health:1,artSignature:'a'},rat={key:'rat:a',type:'rat',x:1,y:1,health:5,artSignature:'a'};
  h.presentation.syncEntities([mouse,rat]);h.frame(20);h.presentation.syncEntities([{...mouse,x:2},rat]);h.frame(50);
  const position=h.presentation.getPosition(mouse.key),authority=Object.freeze({x:2,y:1});
  assert.equal(h.presentation.captureMouse(mouse.key,authority,{attackerKey:rat.key,eventId:123}),true);
  const count=h.presentation.stats.effects;assert.equal(h.presentation.captureMouse(mouse.key,authority,{eventId:123}),false);
  assert.equal(h.presentation.stats.effects,count);h.presentation.syncEntities([rat]);h.frame(20);
  const caption=h.effects.children.find(n=>n.classList.contains('cm-caption'));
  assert.equal(caption.textContent,'CAUGHT');assert.equal(caption.style.left,`${position.x*20}px`);
  assert.ok(h.layer.children.some(n=>n.classList.contains('cm-captured') && n.classList.contains('cm-dying')));
  assert.deepEqual(authority,{x:2,y:1});h.frame(1200);
  assert.equal(h.presentation.stats.captures,0);assert.equal(h.presentation.stats.dying,0);assert.equal(h.effects.children.length,0);
  assert.equal(h.layer.children.length,1);assert.equal(h.presentation.stats.animating,false);
  assert.equal(h.presentation.captureMouse(mouse.key,authority,{eventId:123}),false);
  assert.equal(h.presentation.stats.effects,0);
});

test('busy reduced-motion combat retains capture cues and contact flashes within the existing caps',()=>{
  const h=fixture(true),mouse={key:'mouse:a',type:'mouse',x:1,y:1,health:1,artSignature:'a'};
  h.presentation.syncEntities([mouse]);for(let i=0;i<300;i++)h.presentation.burst({x:1,y:1},'hit',8);
  h.presentation.captureMouse(mouse.key,mouse,{eventId:1});h.presentation.structureHit(null,{x:2,y:1},{kind:'ox'});
  h.presentation.syncEntities([]);h.frame(20);
  assert.ok(h.effects.children.some(n=>n.textContent==='CAUGHT'));assert.ok(h.effects.children.some(n=>n.classList.contains('cm-flare')));
  assert.ok(h.effects.children.every(n=>!n.style.transform || n.style.transform.includes('scale(1)')));
  assert.ok(h.shakeOffsets.every(o=>Math.abs(o.x)+Math.abs(o.y)===0));assert.ok(h.presentation.stats.effects<=EFFECT_LIMITS.active);
  assert.ok(h.effects.children.length<=EFFECT_LIMITS.active);h.frame(1200);
  assert.equal(h.presentation.stats.effects,0);assert.equal(h.effects.children.length,0);assert.equal(h.layer.children.length,0);
});

test('quick revives clear capture captions and remove the old dying mouse before the next capture',()=>{
  const h=fixture(),mouse={key:'mouse:a',type:'mouse',x:1,y:1,health:1,artSignature:'a'};
  h.presentation.syncEntities([mouse]);h.presentation.captureMouse(mouse.key,mouse,{eventId:1});
  h.presentation.syncEntities([]);h.frame(20);const dying=h.layer.children[0];
  h.presentation.syncEntities([{...mouse,x:3}]);h.frame(20);
  assert.equal(dying.parentNode,null);assert.equal(h.layer.children.length,1);assert.equal(h.presentation.stats.dying,0);
  assert.equal(h.effects.children.some(n=>n.textContent==='CAUGHT'),false);
  assert.equal(h.presentation.captureMouse(mouse.key,{x:3,y:1},{eventId:2}),true);
  h.presentation.clear();assert.equal(h.presentation.stats.captures,0);assert.equal(h.effects.children.length,0);
});
