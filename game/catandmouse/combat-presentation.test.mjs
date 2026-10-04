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
function fixture(reduced=false){
  let time=0,nextId=0;const frames=new Map(),layer=new Node(),effects=new Node();
  const document={hidden:false,createElement:()=>new Node()};
  const presentation=createCombatPresentation({document,now:()=>time,project:(x,y)=>({x:x*20,y:y*20}),depth:(x,y,z)=>x+y+z,
    reducedMotion:()=>reduced,requestFrame:fn=>{const id=++nextId;frames.set(id,fn);return id;},cancelFrame:id=>frames.delete(id),
    createUnit:()=>{const node=new Node(),body=new Node();body.className='cm-unit-body';node.appendChild(body);return node;}});
  presentation.attach(layer,effects);
  const frame=ms=>{time+=ms;const callbacks=[...frames.values()];frames.clear();for(const fn of callbacks)fn();};
  return {presentation,layer,effects,document,frame,get time(){return time;}};
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
