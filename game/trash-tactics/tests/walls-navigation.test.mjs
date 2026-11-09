import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

const advance=(game,seconds)=>{for(let t=0;t<seconds-1e-9;){const dt=Math.min(.05,seconds-t);game.update(dt);t+=dt;}};
function arena(map='desktop'){
  const game=new Game({map});game._think=Infinity;
  game.entities=game.entities.filter(e=>TYPES[e.type].building);game._collisionBuildings=null;
  return game;
}
function wall(game,x,y,team=0){return game._spawn('wall',team,x,y);}
function enclosure(game){
  const walls=[];
  for(let x=960;x<=1344;x+=48){walls.push(wall(game,x,1488),wall(game,x,1872));}
  for(let y=1536;y<=1824;y+=48){walls.push(wall(game,960,y),wall(game,1344,y));}
  return walls;
}
function reachable(game,troop,target){
  const route=game._path(troop,target.x,target.y);let previous=troop;
  for(const p of route){assert(game._lineFree(previous.x,previous.y,p.x,p.y,troop.r),'every navigation segment clears walls and terrain');previous=p;}
  return Math.hypot(previous.x-target.x,previous.y-target.y)<1;
}

test('a connected five-block wall has no seams and ground routes detour around its ends',()=>{
  const game=arena(),troop=game._spawn('rock',0,1104,1632),target={x:1536,y:1632};
  for(let y=1536;y<=1728;y+=48)wall(game,1296,y);
  for(let y=1512;y<=1752;y+=3)assert(!game._lineFree(1104,y,1536,y,troop.r),'touching square blocks have no passable seams');
  assert(reachable(game,troop,target));assert.equal(game._navigation.cell,48);assert.equal(game._navigation.offset,0);
  game.command([troop.id],{type:'move',...target});advance(game,8);
  assert(Math.hypot(troop.x-target.x,troop.y-target.y)<20);assert(!game._blocked(troop.x,troop.y,troop.r));
});

test('closed wall boxes contain ground units on every map and a destroyed block opens a usable 48-pixel breach',()=>{
  for(const map of ['desktop','breakroom','canyon','conveyor']){
    const game=arena(map),walls=enclosure(game),troop=game._spawn('rock',0,1152,1680),target={x:1536,y:1680};
    assert(!reachable(game,troop,target),`${map}: an intact box has no ground exit`);
    for(const point of [{x:1536,y:1300},{x:1536,y:2000},{x:800,y:1300},{x:800,y:2000}])assert(!game._lineFree(troop.x,troop.y,point.x,point.y,troop.r),'connected corners cannot be cut diagonally');
    game.command([troop.id],{type:'move',...target});advance(game,3);
    assert(troop.x<1320,`${map}: movement never tunnels through the closed boundary`);
    const before=game._navigation,door=walls.find(e=>e.x===1344&&e.y===1680);door.hp=0;game._barrierRevision++;
    assert(reachable(game,troop,target),`${map}: a single destroyed block is traversable`);
    assert.notEqual(game._navigation,before,'destruction invalidates shared clearance');
    advance(game,6);assert(Math.hypot(troop.x-target.x,troop.y-target.y)<20,`${map}: the existing movement order resumes through the breach`);
    assert(!game._blocked(troop.x,troop.y,troop.r));
  }
});

test('large Tier 3 bodies cannot squeeze through a one-block opening',()=>{
  const game=arena(),walls=enclosure(game),large=game._spawn('rollingrock',0,1152,1680),small=game._spawn('paper',0,1104,1680),target={x:1536,y:1680};
  walls.find(e=>e.x===1344&&e.y===1680).hp=0;game._barrierRevision++;
  assert(reachable(game,small,target));assert(!reachable(game,large,target),'a 66-pixel boulder cannot fit a 48-pixel breach');
});

test('flying transports and Paper Airplanes pass over intact walls',()=>{
  const game=arena();enclosure(game);
  for(const type of ['cd','airplane']){
    const unit=game._spawn(type,0,1152,1680),target={x:1536,y:1680};game.command([unit.id],{type:'move',...target});advance(game,3);
    assert(Math.hypot(unit.x-target.x,unit.y-target.y)<25,`${type} crosses the wall line in the air`);
  }
});

test('wall navigation refreshes when force fields or division barriers expire around a breach',()=>{
  for(const kind of ['forceFields','divisions']){
    const game=arena(),walls=enclosure(game),troop=game._spawn('rock',0,1152,1680),target={x:1536,y:1680};
    walls.find(e=>e.x===1344&&e.y===1680).hp=0;game._barrierRevision++;
    game[kind]=[{id:900,x:1344,y:1680,r:kind==='forceFields'?110:350,remaining:1.5,total:1.5,casting:0,team:1}];game._barrierRevision++;
    game.command([troop.id],{type:'move',...target});advance(game,.8);
    assert(troop.x<1344,`${kind} still blocks the breach`);
    advance(game,6);assert.equal(game[kind].length,0);assert(Math.hypot(troop.x-target.x,troop.y-target.y)<20,`${kind}: route resumes after expiry`);
  }
});

test('adding or removing the last wall switches navigation grids without reusing stale clearance',()=>{
  const game=arena(),troop=game._spawn('rock',0,1104,1632),target={x:1536,y:1632};
  game._path(troop,target.x,target.y);assert.equal(game._navigation.cell,64);const old=game._navigation;
  const block=wall(game,1296,1632);assert(reachable(game,troop,target));assert.equal(game._navigation.cell,48);assert.notEqual(game._navigation,old);
  block.hp=0;game._path(troop,target.x,target.y);assert.equal(game._navigation.cell,64);
});

test('a distant wall preserves team-to-team routes across every original and expansion layout',()=>{
  for(const map of ['desktop','breakroom','canyon','conveyor'])for(const players of [2,3,4]){
    const game=new Game({map,players});game._think=Infinity;wall(game,96,96);
    const troops=game.entities.filter(e=>e.type==='rock');
    for(const source of troops)for(const target of troops)if(source.team!==target.team)assert(reachable(game,source,target),`${map}/${players}: aligned wall grid preserves the ${source.team} to ${target.team} route`);
  }
});

test('one Twig completes a queued wall row from reachable construction faces',()=>{
  const game=arena(),worker=game._spawn('twig',0,1104,1632),blocks=[];game.teams[0].trash=1000;
  for(let y=1536;y<=1728;y+=48){const result=game.build([worker.id],'wall',1296,y);assert(result.ok);blocks.push(game.getEntity(result.entityId));}
  assert.equal(game.teams[0].trash,900);advance(game,28);
  assert(blocks.every(e=>e.constructing===0&&e.hp===e.maxHp),'the queue finishes every block');
  assert(!game._blocked(worker.x,worker.y,worker.r));
});

test('queueing an unreachable outside block after closing a wall box fails without spending trash',()=>{
  const game=arena();enclosure(game).find(e=>e.x===1344&&e.y===1680).hp=0;
  const worker=game._spawn('twig',0,1152,1680);game.teams[0].trash=1000;
  assert(game.build([worker.id],'wall',1344,1680).ok);const bank=game.teams[0].trash;
  const unreachable=game.build([worker.id],'wall',1488,1680);
  assert.equal(unreachable.ok,false,'a queued plan cannot strand its only builder behind solid walls');
  assert.equal(game.teams[0].trash,bank,'unreachable work is rejected before payment');
  assert(!game.entities.some(e=>e.type==='wall'&&e.x===1488&&e.y===1680&&e.hp>0),'no unfinishable construction ghost blocks the outside lane');
  advance(game,8);assert(!game.entities.some(e=>e.type==='wall'&&e.constructing));
});

test('valid ground beside wall corners permits movement and Shadow Path away from the block',()=>{
  const game=arena();wall(game,1296,1632);const radius=TYPES.shears.r;let start=null;
  for(const sx of [-1,1])for(const sy of [-1,1])for(let clearance=17;clearance<=28;clearance++){
    const p={x:1296+sx*(24+clearance),y:1632+sy*(24+clearance)};
    if(game._blocked(p.x,p.y,radius))continue;
    const target={x:p.x+sx*120,y:p.y+sy*120};
    assert(game._lineFree(p.x,p.y,target.x,target.y,radius),'a valid corner position must have a clear outward escape segment');
    if(!start)start={...p,target};
  }
  assert(start);const unit=game._spawn('shears',0,start.x,start.y);
  assert(game.useAbility([unit.id],'shadowpath',{points:[start.target]}).ok);advance(game,.4);
  assert(Math.hypot(unit.x-start.x,unit.y-start.y)>100,'the dash does not consume its cooldown without leaving a valid corner');
});

test('walls can seal the Canyon ruler bridge and a destroyed block restores its short crossing',()=>{
  const game=arena('canyon'),bridge=game.map.bridges[0],cx=bridge.x+bridge.w/2,cy=bridge.y+bridge.h/2,troop=game._spawn('rock',0,bridge.x-400,cy),target={x:bridge.x+bridge.w+400,y:cy},blocks=[];
  const first=Math.ceil((bridge.y+24)/48)*48,last=Math.floor((bridge.y+bridge.h-24)/48)*48;
  for(let y=first;y<=last;y+=48){assert(game.canBuild('wall',cx,y));blocks.push(wall(game,cx,y));}
  const detour=game._path(troop,target.x,target.y);assert(reachable(game,troop,target));assert(detour.some(p=>p.y>game.height*.7),'the sealed bridge sends ground forces to the southern crossing');
  blocks.reduce((best,e)=>Math.abs(e.y-cy)<Math.abs(best.y-cy)?e:best).hp=0;game._barrierRevision++;
  const breach=game._path(troop,target.x,target.y);assert(reachable(game,troop,target));assert(breach.every(p=>p.y<bridge.y+bridge.h+100),'a one-block breach makes the northern route usable again');
});
