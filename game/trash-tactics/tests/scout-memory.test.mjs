import test from 'node:test';
import assert from 'node:assert/strict';
import {updateScoutMemory,getRememberedEntities} from '../scout-memory.js';
import {Game} from '../engine.js';

function world(){const cell=40,cols=20,rows=15;return {width:800,height:600,time:1,entities:[],fog:{cell,cols,rows,revision:1,teams:Array.from({length:2},()=>({visible:new Uint8Array(cols*rows),explored:new Uint8Array(cols*rows)}))}};}
function reveal(g,team,x,y,r=100){const f=g.fog,t=f.teams[team];for(let row=0;row<f.rows;row++)for(let col=0;col<f.cols;col++)if(Math.abs(col*f.cell+20-x)<=r&&Math.abs(row*f.cell+20-y)<=r){t.visible[row*f.cols+col]=1;t.explored[row*f.cols+col]=1;}}
function leave(g){for(const t of g.fog.teams)t.visible.fill(0);g.time++;g.fog.revision++;updateScoutMemory(g);}

test('hidden movement, ownership, health, construction and cargo remain immutable last-seen values',()=>{
 const g=world(),enemy={id:2,type:'printer',team:1,x:300,y:260,r:44,hp:500,maxHp:550,constructing:7,buildTotal:10,queue:[{remaining:4,total:8,type:'rock'}],cargo:[21,22],order:{type:'attack',targetId:88}};g.entities=[enemy];reveal(g,0,300,260);updateScoutMemory(g);assert.equal(getRememberedEntities(g).length,0,'currently visible entities are drawn live');
 leave(g);const record=getRememberedEntities(g)[0],before=JSON.stringify(record);assert.equal(record.lastSeen,1);assert(Object.isFrozen(record)&&Object.isFrozen(record.queue)&&Object.isFrozen(record.queue[0])&&Object.isFrozen(record.cargo));assert.deepEqual(record.cargo,[null,null],'a visible seat count does not expose passenger identities');assert(!('order'in record));assert(!('type'in record.queue[0]),'future production is not an observation');
 enemy.x=720;enemy.y=520;enemy.team=2;enemy.hp=20;enemy.constructing=0;enemy.queue[0].remaining=0;enemy.cargo.pop();g.time=40;updateScoutMemory(g);
 assert.equal(JSON.stringify(getRememberedEntities(g)[0]),before);assert.throws(()=>{record.queue[0].remaining=0;},TypeError);
 g.entities=[];updateScoutMemory(g);assert.equal(JSON.stringify(getRememberedEntities(g)[0]),before,'hidden destruction cannot erase an observation');
});

test('an empty remembered footprint clears only after full revisiting, while seeing elsewhere updates its ID',()=>{
 const g=world(),e={id:4,type:'rock',team:1,x:300,y:260,r:18,hp:100};g.entities=[e];reveal(g,0,e.x,e.y);updateScoutMemory(g);leave(g);e.x=700;e.y=500;
 const f=g.fog;f.teams[0].visible[Math.floor(260/40)*f.cols+Math.floor(300/40)]=1;updateScoutMemory(g);assert.equal(getRememberedEntities(g).length,1,'one visible center cell is not a full footprint revisit');
 reveal(g,0,300,260,100);updateScoutMemory(g);assert.equal(getRememberedEntities(g).length,0,'clear evidence of absence retires the old location');
 reveal(g,0,700,500);g.time=7;updateScoutMemory(g);assert.equal(getRememberedEntities(g).length,0);leave(g);const updated=getRememberedEntities(g)[0];assert.equal(updated.id,4);assert.equal(updated.x,700);assert.equal(updated.lastSeen,7);
});

test('neutral resources remember observed quantities, and unscouted entities create no records',()=>{
 const g=world(),resource={id:5,type:'trash',team:-1,x:300,y:260,r:27,hp:1,amount:1800,maxAmount:1800},unknown={id:6,type:'laboratory',team:1,x:720,y:520,r:64,hp:900};g.entities=[resource,unknown];reveal(g,0,300,260);updateScoutMemory(g);leave(g);resource.amount=0;updateScoutMemory(g);
 const records=getRememberedEntities(g);assert.equal(records.length,1);assert.equal(records[0].id,5);assert.equal(records[0].amount,1800);assert.equal(getRememberedEntities(g,1).length,0,'views do not share intelligence');
});

test('a fully revisited footprint at the map edge clears without requiring off-map visibility',()=>{
 const g=world(),e={id:8,type:'rock',team:1,x:782,y:300,r:18,hp:100};g.entities=[e];reveal(g,0,e.x,e.y);updateScoutMemory(g);leave(g);g.entities=[];assert.equal(getRememberedEntities(g).length,1);reveal(g,0,782,300);updateScoutMemory(g);assert.equal(getRememberedEntities(g).length,0);
});

test('enemy wall links use only observed neighbors and cannot update from hidden construction',()=>{
 const g=world(),a={id:1,type:'wall',team:1,x:288,y:240,r:24,hp:1200},b={id:2,type:'wall',team:1,x:336,y:240,r:24,hp:1200};g.entities=[a,b];reveal(g,0,300,240);updateScoutMemory(g);leave(g);const first=getRememberedEntities(g).find(e=>e.id===1);assert.equal(first.wallMask,2);
 b.hp=0;g.entities.push({id:3,type:'wall',team:1,x:288,y:192,r:24,hp:1200});updateScoutMemory(g);assert.equal(getRememberedEntities(g).find(e=>e.id===1).wallMask,2);
});

test('real engine memory never becomes a world entity, selection candidate or weapon target',()=>{
 const g=new Game();g._think=Infinity;g.entities=[];g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];
 const scout=g._spawn('rock',0,3000,1000),enemy=g._spawn('printer',1,3100,1000);g._updateVision();scout.x=1000;g._updateVision();assert.equal(g.getRememberedEntities(0).find(e=>e.id===enemy.id)?.hp,550);
 enemy.hp=0;g.entities=g.entities.filter(e=>e!==enemy);g._updateVision();const memory=g.getRememberedEntities(0).find(e=>e.id===enemy.id);assert(memory);assert(!g.entities.includes(memory));assert.equal(g.getEntity(enemy.id),undefined);assert(!g.canTarget(scout,memory));g.command([scout.id],{type:'attack',targetId:memory.id});assert.equal(scout.order.type,'idle');
 scout.x=3000;g._updateVision();assert(!g.getRememberedEntities(0).some(e=>e.id===enemy.id),'a physical revisit discovers the destruction');
});
