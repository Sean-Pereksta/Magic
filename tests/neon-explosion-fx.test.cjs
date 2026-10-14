'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fx=require('../game/neon-arena/neon-explosion-fx.js');
const camera={position:[0,2,-12],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:1};
test('explosions pool bounded effects, reject invalid locations and expire without affecting combat',()=>{
  const s=fx.createSystem();assert.equal(s.emit('rocket',NaN,0,0),null);for(let i=0;i<40;i++)s.emit('rocket',i,1,4,{radius:8});assert.equal(s.active.length,16);assert(s.stats().pooled<=1);s.update(2);assert.equal(s.active.length,0);assert(s.stats().pooled>=16);s.emit('plasma',0,1,4);assert.equal(s.active.length,1);s.clear();assert.equal(s.active.length,0);
});
test('rocket, mortar, plasma and seismic profiles have distinct short-lived fire and smoke',()=>{
  assert.equal(Object.keys(fx.PROFILES).length,4);for(const p of Object.values(fx.PROFILES)){assert(p.fire<p.life);assert(p.fire<=.43);assert(p.life<=1.4)}assert.notDeepEqual(fx.PROFILES.rocket.fireColor,fx.PROFILES.plasma.fireColor);
});
test('explosion geometry has real volume, fading smoke, finite bounded vertices and mobile budgets',()=>{
  const s=fx.createSystem();for(let i=0;i<16;i++)s.emit(i%2?'rocket':'plasma',i%3,1,5,{radius:8});s.update(.16);const high=fx.createGeometry(s,camera,'high'),low=fx.createGeometry(s,camera,'low');assert(high.length>low.length);assert(high.length<=18000*7);assert.equal(high.length%21,0);assert(Array.from(high).every(Number.isFinite));const z=[];for(let i=2;i<high.length;i+=7)z.push(high[i]);assert(Math.max(...z)-Math.min(...z)>2);assert(Array.from(high).filter((_,i)=>i%7===6).some(a=>a===0));s.update(1.5);assert.equal(fx.createGeometry(s,camera).length,0);
});
test('nearby cover bounds keep visual fragments and fire on the collision side',()=>{
  const s=fx.createSystem(),bounds={minX:-3,maxX:3,minY:0,maxY:4,minZ:0,maxZ:5};s.emit('rocket',0,1,4.8,{radius:10,bounds});s.update(.3);const vertices=fx.createGeometry(s,camera);for(let i=0;i<vertices.length;i+=7){assert(vertices[i]>=-3&&vertices[i]<=3);assert(vertices[i+1]>=0&&vertices[i+1]<=4);assert(vertices[i+2]>=0&&vertices[i+2]<=5)}
});
