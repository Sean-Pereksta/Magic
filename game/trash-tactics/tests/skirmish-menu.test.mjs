import test from 'node:test';
import assert from 'node:assert/strict';
import {MAP_BRIEFS,DIFFICULTY_BRIEFS,paintBattlefieldPreview} from '../skirmish-menu.js';

test('map briefing draws actual obstacles, lanes, expansions and player camps without changing geometry',()=>{
 const calls=[],c=new Proxy({},{get:(_,method)=>(...args)=>{for(const value of args)if(typeof value==='number')assert(Number.isFinite(value));calls.push({method,args});},set:()=>true});
 const map={width:9600,height:6400,color:'#abc',obstacles:[{x:4200,y:400,w:500,h:1200,type:'inkpool'}],plateaus:[{x:900,y:1000,w:600,h:400}],lanes:[[[1000,3200],[4800,3200],[8500,3200]],[[1000,3200],[1600,900],[8500,3200]]],laneInfo:[{role:'main'},{role:'flank'}],expansions:[{x:1900,y:3000},{x:4800,y:1200,amountMultiplier:1.7}],camps:[{x:1000,y:3200},{x:8500,y:3200}]};
 const before=structuredClone(map);paintBattlefieldPreview({width:440,height:188,getContext:()=>c},map);
 assert.deepEqual(map,before);
 assert(calls.some(q=>q.method==='fillRect'&&q.args[0]===4200&&q.args[2]===500));
 assert(calls.some(q=>q.method==='lineTo'&&q.args[0]===4800&&q.args[1]===3200));
 assert(calls.some(q=>q.method==='arc'&&q.args[0]===1900&&q.args[1]===3000));
 assert.equal(Object.keys(MAP_BRIEFS).length,7);assert.equal(Object.keys(DIFFICULTY_BRIEFS).length,4);
});
