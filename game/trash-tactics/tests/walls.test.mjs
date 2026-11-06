import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES,WALL_SIZE,snapWallPoint} from '../engine.js';
import {wallBounds,wallConnections} from '../walls.js';

function arena(){const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g._collisionBuildings=null;return g;}
const advance=(g,seconds)=>{for(let t=0;t<seconds-1e-9;){const dt=Math.min(.04,seconds-t);g.update(dt);t+=dt;}};

test('walls snap to a shared square grid, cost 20 trash, finish in three seconds and add no supply',()=>{
  const g=arena(),worker=g._spawn('twig',0,1008,940);g.teams[0].trash=200;
  assert.equal(WALL_SIZE,48);assert.deepEqual(snapWallPoint(1000,1000),{x:1008,y:1008});
  const cap=g.getSupply(0).cap,built=g.build([worker.id],'wall',1000,1000);assert(built.ok);assert.deepEqual({x:built.x,y:built.y},{x:1008,y:1008});
  const wall=g.getEntity(built.entityId);assert.equal(g.teams[0].trash,180);assert.equal(wall.constructing,3);assert.equal(wall.maxHp,1200);assert.equal(TYPES.wall.vision,120);
  advance(g,2.5);assert(wall.constructing>0);advance(g,1.2);assert.equal(wall.constructing,0);assert.equal(wall.hp,1200);assert.equal(g.getSupply(0).cap,cap);
  const b=wallBounds(wall);assert.equal(b.w,48);assert.equal(b.h,48);
});

test('same-team level blocks join edge-to-edge while occupied, hidden, cliff and ramp placement is rejected',()=>{
  const g=arena(),worker=g._spawn('twig',0,1008,940);g.teams[0].trash=200;
  const first=g.build([worker.id],'wall',1008,1008);assert(first.ok);assert(g.canBuild('wall',1056,1008,0));
  const next=g.build([worker.id],'wall',1056,1008);assert(next.ok);const a=g.getEntity(first.entityId),b=g.getEntity(next.entityId);
  assert.equal(wallConnections(g,a),2);assert.equal(wallConnections(g,b),8);assert(!g.canBuild('wall',1008,1008));
  const enemy=g._spawn('wall',1,1008,1056);assert.equal(wallConnections(g,a),2,'enemy walls never acquire a friendly join');enemy.hp=0;
  const height=g.elevationAt.bind(g);g.elevationAt=(x,y)=>x>1030?1:height(x,y);assert.equal(wallConnections(g,a),0,'different elevations do not join');g.elevationAt=height;
  const bank=g.teams[0].trash;assert(!g.build([worker.id],'wall',3000,2600).ok);assert.equal(g.teams[0].trash,bank);
  g.map.obstacles=[{x:1090,y:980,w:80,h:80,type:'book'}];assert(!g.canBuild('wall',1104,1008));
  g.map.obstacles=[];g.map.ramps=[{x:1090,y:980,w:80,h:80}];assert(!g.canBuild('wall',1104,1008));
});

test('wall orders preserve a Twig existing paid construction and finish every queued block afterwards',()=>{
  const g=arena(),worker=g._spawn('twig',0,900,850);g.teams[0].trash=1000;
  const printer=g.build([worker.id],'printer',900,1000);assert(printer.ok);
  const plans=[g.build([worker.id],'wall',1104,1008),g.build([worker.id],'wall',1152,1008),g.build([worker.id],'wall',1200,1008)];
  assert(plans.every(p=>p.ok&&p.queued));assert.equal(worker.order.targetId,printer.entityId);assert.equal(worker.buildQueue.length,3);assert.equal(g.teams[0].trash,1000-150-60);
  advance(g,25);for(const id of [printer.entityId,...plans.map(p=>p.entityId)])assert.equal(g.getEntity(id).constructing,0);
  assert.equal(worker.buildQueue.length,0);assert(!g._blocked(worker.x,worker.y,worker.r));
});

test('a new closing wall cannot strand an earlier paid wall plan behind its enclosure',()=>{
  const g=arena(),worker=g._spawn('twig',0,1152,1680);g.teams[0].trash=1000;
  g._spawn('cd',0,1488,1680);
  for(let x=960;x<=1344;x+=48){g._spawn('wall',0,x,1488);g._spawn('wall',0,x,1872);}
  for(let y=1536;y<=1824;y+=48){g._spawn('wall',0,960,y);if(y!==1680)g._spawn('wall',0,1344,y);}
  const outside=g.build([worker.id],'wall',1488,1680);assert(outside.ok);const bank=g.teams[0].trash;
  const closing=g.build([worker.id],'wall',1344,1680);assert(!closing.ok);assert.equal(g.teams[0].trash,bank);assert(!g.entities.some(e=>e.type==='wall'&&e.x===1344&&e.y===1680));
  advance(g,10);assert.equal(g.getEntity(outside.entityId).constructing,0);
});

test('blueprints safely clear troops overlapping square corners and ordinary attacks reach rectangular faces',()=>{
  const g=arena(),worker=g._spawn('twig',0,1104,940),troop=g._spawn('rock',0,1139,1043);g.teams[0].trash=200;
  const built=g.build([worker.id],'wall',1104,1008);assert(built.ok);assert(!g._blocked(troop.x,troop.y,troop.r),'corner overlaps are cleared despite lying outside the inscribed circle');
  const wall=g.getEntity(built.entityId);wall.constructing=0;wall.hp=wall.maxHp;
  const enemy=g._spawn('rock',1,1290,1008);g.command([enemy.id],{type:'attack',targetId:wall.id});advance(g,3);assert(wall.hp<1200,'melee can reach the wall face');assert(!g._blocked(enemy.x,enemy.y,enemy.r));
});

test('low wall blocks preserve projectile fire while physically blocking ground movement',()=>{
  const g=arena(),sniper=g._spawn('sniper',0,1008,1008),wall=g._spawn('wall',0,1104,1008),paper=g._spawn('paper',1,1248,1008);paper.cooldown=100;paper.order={type:'move',x:paper.x,y:paper.y};
  assert(!g._lineFree(sniper.x,sniper.y,paper.x,paper.y,sniper.r));
  assert(g.useAbility([sniper.id],'snipe',{targetId:paper.id}).ok);advance(g,1.6);assert(!g.getEntity(paper.id));assert.equal(wall.hp,1200);
});

test('dismantling only owned walls refunds nothing, cleans pending work and opens a safe route for trapped workers',()=>{
  const g=arena(),worker=g._spawn('twig',0,1152,1680);g._spawn('cd',0,1488,1680);g.teams[0].trash=500;
  for(let x=960;x<=1344;x+=48){g._spawn('wall',0,x,1488);g._spawn('wall',0,x,1872);}
  for(let y=1536;y<=1824;y+=48){g._spawn('wall',0,960,y);g._spawn('wall',0,1344,y);}
  const enemy=g._spawn('wall',1,1584,1680),door=g.entities.find(e=>e.type==='wall'&&e.x===1344&&e.y===1680);
  assert(!g.removeWall(enemy.id).ok);assert.equal(enemy.hp,1200);assert.equal(g.teams[0].trash,500);
  assert(!g.build([worker.id],'wall',1488,1680).ok);
  const revision=g._barrierRevision;assert(g.removeWall(door.id).ok);assert.equal(g.teams[0].trash,500);assert(g._barrierRevision>revision);assert(!g.getEntity(door.id));assert(!g.removeWall(door.id).ok);
  const outside=g.build([worker.id],'wall',1488,1680);assert(outside.ok);advance(g,10);assert.equal(g.getEntity(outside.entityId).constructing,0);assert(worker.x>1344);assert(!g._blocked(worker.x,worker.y,worker.r));
  const a=g.build([worker.id],'wall',1488,1728),b=g.build([worker.id],'wall',1488,1776);assert(a.ok&&b.ok&&b.queued);const bank=g.teams[0].trash;
  assert(g.removeWall(b.entityId).ok);assert(!worker.buildQueue.includes(b.entityId));assert.equal(g.teams[0].trash,bank);advance(g,6);assert.equal(g.getEntity(a.entityId).constructing,0);
});
