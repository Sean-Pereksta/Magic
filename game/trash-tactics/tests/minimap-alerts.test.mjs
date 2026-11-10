import test from 'node:test';
import assert from 'node:assert/strict';
import {recordAttackAlert,getAttackAlerts,drawAttackAlerts,ATTACK_ALERT_LIMIT} from '../minimap-alerts.js';
const hit=(x=200,y=300,team=0,amount=10)=>({type:'hit',amount,target:{id:1,team,x,y},source:{team:1,get x(){throw new Error('attacker position is private');},get y(){throw new Error('attacker position is private');}}});

test('only actual positive damage to human assets creates a victim-location alert',()=>{
 const g={time:0,width:9600,height:6400};for(const event of [hit(200,300,1),hit(200,300,0,0),hit(200,300,0,-1),{type:'death',amount:10,target:{team:0,x:200,y:300}}])assert.equal(recordAttackAlert(g,event),false);
 assert(recordAttackAlert(g,hit()));assert.deepEqual(getAttackAlerts(g).map(({x,y})=>({x,y})),[{x:200,y:300}]);
});
test('rapid shots and damage-over-time cluster and rate-limit their pulse without losing an ongoing attack',()=>{
 const g={time:0};recordAttackAlert(g,hit());g.time=.1;recordAttackAlert(g,hit(205,305));assert.equal(getAttackAlerts(g)[0].pulseAt,0);g.time=.81;recordAttackAlert(g,hit(210,310));assert.equal(getAttackAlerts(g)[0].pulseAt,.81);
 for(let i=1;i<=30;i++){g.time=.81+i*.1;recordAttackAlert(g,hit(210,310,0,1));}assert.equal(getAttackAlerts(g).length,1);assert(getAttackAlerts(g)[0].lastHit>3);
 const paused=JSON.stringify(getAttackAlerts(g));for(let i=0;i<10;i++)assert.equal(JSON.stringify(getAttackAlerts(g)),paused,'the simulation clock freezes notices while paused');g.time+=6.01;assert.equal(getAttackAlerts(g).length,0);
});
test('simultaneous fronts have a bounded count and new matches reset notices',()=>{
 const g={time:0};for(let i=0;i<20;i++){g.time=i*.01;recordAttackAlert(g,hit(300+i*400,300));}assert.equal(getAttackAlerts(g).length,ATTACK_ALERT_LIMIT);assert.equal(getAttackAlerts({time:0}).length,0);
});
test('small red minimap rings draw at the harmed asset without reading an attacker',()=>{
 const g={time:2,width:2000,height:1200};recordAttackAlert(g,hit());const calls=[],c=new Proxy({},{get:(_o,key)=>(...args)=>calls.push({key,args}),set:()=>true});drawAttackAlerts(c,g,{width:200,height:120});
 const arcs=calls.filter(q=>q.key==='arc');assert.equal(arcs.length,3);assert(arcs.every(q=>q.args[0]===20&&q.args[1]===30));assert(arcs.every(q=>q.args[2]<=10));
});
