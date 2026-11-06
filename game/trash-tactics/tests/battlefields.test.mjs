import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattlefield,conveyorVelocity,conveyorTravelCost,initBattlefield,updateBattlefield,reverseConveyor,dynamicBarrierBlocked,dynamicBarrierCrossed,divisionBarrierCrossed,divisionShotBlocked} from '../battlefields.js';
import {Game,TYPES} from '../engine.js';

function fieldGame(){return {time:0,forceFields:[],divisions:[]};}
function advance(game,seconds){for(let t=0;t<seconds;t+=.05)game.update(.05);}

test('expansion maps preserve 4800 by 3200 geometry for every player count',()=>{
  for(const id of ['canyon','conveyor'])for(const players of [2,3,4]){
    const map=createBattlefield(id,players);
    assert.equal(map.width,4800);assert.equal(map.height,3200);assert.equal(map.camps.length,players);
    assert(map.plateaus.length>=2);assert.equal(map.ramps.length,map.plateaus.length*2);
    assert(map.expansions.length>=8);assert(map.lanes.length>=3);
    for(const camp of map.camps)assert(!map.obstacles.some(o=>camp.x>o.x-100&&camp.x<o.x+o.w+100&&camp.y>o.y-100&&camp.y<o.y+o.h+100),`${id}: headquarters has clear ground`);
  }
  assert.equal(createBattlefield('desktop',2),null,'original maps retain their existing layout path');
});

test('canyon has solid chasms, a narrow northern bridge and a broader southern crossing',()=>{
  const map=createBattlefield('canyon',2),blocked=(x,y)=>map.obstacles.some(o=>x>o.x&&x<o.x+o.w&&y>o.y&&y<o.y+o.h);
  assert(blocked(2400,100));assert(blocked(2400,1100));assert(blocked(2400,2100));assert(blocked(2400,3100));
  assert(!blocked(2400,590));assert(!blocked(2400,2680));assert(blocked(2400,1600),'gate initially closes the center');
  assert(map.bridges[1].h>map.bridges[0].h,'the southern route accommodates wider formations');
});

test('conveyors accelerate, resist and drift ground movement while aircraft ignore them',()=>{
  const game={map:createBattlefield('conveyor',2),time:0},unit={type:'rock',x:1500,y:1600};
  assert.deepEqual(conveyorVelocity(game,unit,100,0),{vx:145,vy:0});
  assert.deepEqual(conveyorVelocity(game,unit,-100,0),{vx:-75,vy:0});
  const crossing=conveyorVelocity(game,unit,0,100);assert.equal(crossing.vx,12);assert.equal(crossing.vy,100);
  for(const type of ['airplane','cd'])assert.deepEqual(conveyorVelocity(game,{...unit,type},100,0),{vx:100,vy:0});
  assert(conveyorTravelCost(game,1500,1600,1,0)<conveyorTravelCost(game,1500,1600,-1,0));
  game.map.conveyors[0].reversedUntil=18;
  assert.equal(conveyorVelocity(game,unit,100,0).vx,75);
  game.time=19;assert.equal(conveyorVelocity(game,unit,100,0).vx,145);
});

test('control stations require uncontested capture and reverse only their connected belt',()=>{
  const game={map:createBattlefield('conveyor',2),time:0,entities:[],_id:1,_spawn(type,team,x,y,extra){const e={id:this._id++,type,team,x,y,hp:500,...extra};this.entities.push(e);return e;},getEntity(id){return this.entities.find(e=>e.id===id&&e.hp>0);}};
  initBattlefield(game);const station=game.map.stations[0],worker={id:100,type:'twig',team:0,x:station.x+50,y:station.y,hp:40};game.entities.push(worker);
  assert.equal(reverseConveyor(game,station.entityId,0).ok,false);
  game.entities.push({id:101,type:'rock',team:1,x:station.x+70,y:station.y,hp:100});updateBattlefield(game,5);
  assert.equal(station.owner,-1,'contested stations cannot be captured');
  game.entities.at(-1).hp=0;updateBattlefield(game,4.1);
  assert.equal(station.owner,0);assert.equal(game.getEntity(station.entityId).team,0);
  const belt=game.map.conveyors.find(b=>b.id===station.beltId);assert.equal(belt.reversedUntil,18);
  assert.equal(game.map.conveyors.filter(b=>b.reversedUntil>0).length,1);
  assert.equal(reverseConveyor(game,station.entityId,0).ok,false,'capture starts the station cooldown');
  game.time=25;assert(reverseConveyor(game,station.entityId,0).ok);assert.equal(belt.reversedUntil,43);
});

test('destroying the neutral stapler permanently removes the central collision obstacle',()=>{
  const game={map:createBattlefield('canyon',2),time:0,entities:[],_id:1,_spawn(type,team,x,y,extra){const e={id:this._id++,type,team,x,y,hp:1800,...extra};this.entities.push(e);return e;},getEntity(id){return this.entities.find(e=>e.id===id&&e.hp>0);}};
  initBattlefield(game);const obstacle=game.map.gateObstacle;assert(game.map.obstacles.includes(obstacle));
  game.getEntity(game.map.gate.entityId).hp=0;updateBattlefield(game,.05);
  assert(game.map.gate.open);assert(!game.map.obstacles.includes(obstacle));assert.equal(game._barrierRevision,1);
  updateBattlefield(game,1);assert.equal(game._barrierRevision,1,'opening happens once');
});

test('overlapping dome rings block entry, exit and tunneling while preserving their interiors',()=>{
  const game=fieldGame();game.forceFields=[{id:1,x:500,y:500,r:110,remaining:10},{id:2,x:660,y:500,r:110,remaining:10}];
  assert(!dynamicBarrierBlocked(game,500,500,17));assert(dynamicBarrierBlocked(game,610,500,17));
  assert(dynamicBarrierCrossed(game,500,500,700,500,17),'a trapped troop cannot leave');
  assert(dynamicBarrierCrossed(game,300,500,800,500,17),'a long step cannot skip both ring boundaries');
  assert(!dynamicBarrierCrossed(game,300,300,800,300,17),'a route around the domes stays usable');
  assert(dynamicBarrierBlocked(game,580,575,17),'overlap intersections have no gap');
  assert(!divisionBarrierCrossed(game,300,500,800,500,17),'aircraft ignore ordinary domes');
  game.forceFields.forEach(f=>f.remaining=0);assert(!dynamicBarrierCrossed(game,300,500,800,500,17));
});

test('division walls contain movement and shots but permit movement inside each sector',()=>{
  const game=fieldGame();game.divisions=[{id:1,x:1000,y:1000,r:350,remaining:9,casting:1.5}];
  assert(!dynamicBarrierCrossed(game,1100,1000,1100,1150,17),'casting preview has no collision');
  game.divisions[0].casting=0;
  assert(!dynamicBarrierCrossed(game,1100,970,1170,980,17),'same-sector movement is clear');
  assert(dynamicBarrierCrossed(game,1100,1000,1000,1180,17),'radial walls separate the families');
  assert(dynamicBarrierCrossed(game,1100,1000,1500,1000,17),'the outside ring prevents escape');
  assert(divisionShotBlocked(game,{x:1100,y:1000},{x:1000,y:1180}));
  assert(!divisionShotBlocked(game,{x:1100,y:970},{x:1170,y:980}));
  assert(divisionBarrierCrossed(game,1100,1000,1000,1180,17),'flying airplanes are contained too');
});

test('integrated expansion navigation routes ground armies through genuine canyon crossings',()=>{
  const game=new Game({map:'canyon'});game._think=Infinity;
  assert.equal(game.map.id,'canyon');
  assert(game._blocked(2400,1100,18));assert(!game._lineFree(1800,1100,3000,1100,18));
  const rock=game._spawn('rock',0,1900,590),target={x:2900,y:590};
  const route=game._path(rock,target.x,target.y);assert(route.length);
  let previous=rock;for(const p of route){assert(game._lineFree(previous.x,previous.y,p.x,p.y,rock.r));previous=p;}
  assert(Math.hypot(previous.x-target.x,previous.y-target.y)<1);
  game.command([rock.id],{type:'move',...target});advance(game,10);
  assert(rock.x>2550,'the northern ruler bridge is traversable');
  for(const id of ['canyon','conveyor']){
    const g=new Game({map:id});for(const can of g.entities.filter(e=>e.type==='trash'))assert(!g._blocked(can.x,can.y,can.r),`${id}: trash resources have valid ground`);
    for(const building of g.entities.filter(e=>TYPES[e.type].building&&e.team>=0))assert(!g.map.obstacles.some(o=>building.x>o.x&&building.x<o.x+o.w&&building.y>o.y&&building.y<o.y+o.h));
  }
});

test('a large ground formation crosses the narrow ruler bridge without freezing or clipping the canyon',()=>{
  const game=new Game({map:'canyon'});game._think=Infinity;
  game.entities=game.entities.filter(e=>['desk','staplergate'].includes(e.type));
  const troops=[];
  for(let i=0;i<40;i++)troops.push(game._spawn('rock',0,1800-Math.floor(i/4)*48,520+(i%4)*46));
  game.command(troops.map(e=>e.id),{type:'move',x:3100,y:590});advance(game,30);
  assert.equal(troops.filter(e=>e.x>2600).length,40);
  assert(troops.every(e=>!game._blocked(e.x,e.y,e.r)),'formation members finish on valid ground');
});

test('barrier revisions invalidate shared routes and find a detour around overlapping domes',()=>{
  const game=new Game();game._think=Infinity;game.map.obstacles=[];game.entities=game.entities.filter(e=>e.type==='desk');
  const troop=game._spawn('rock',0,800,1000),target={x:1500,y:1000};game._path(troop,target.x,target.y);
  const oldGraph=game._navigation;
  game.forceFields=[{id:100,x:1100,y:1000,r:110,remaining:10,total:10},{id:101,x:1260,y:1000,r:110,remaining:10,total:10}];game._barrierRevision++;
  const route=game._path(troop,target.x,target.y);assert.notEqual(game._navigation,oldGraph);assert(route.length>1);
  let previous=troop;for(const p of route){assert(game._lineFree(previous.x,previous.y,p.x,p.y,troop.r));previous=p;}
  assert(Math.hypot(previous.x-target.x,previous.y-target.y)<1,'navigation reaches the other side by going around');
  const fieldGraph=game._navigation;game.forceFields=[];game._barrierRevision++;
  assert(game._lineFree(troop.x,troop.y,target.x,target.y,troop.r));game._path(troop,target.x,target.y);
  assert.notEqual(game._navigation,fieldGraph,'expired fields clear cached collision');
});

test('every team has reachable attack routes on original and expansion maps',()=>{
  for(const map of ['desktop','breakroom','canyon','conveyor'])for(const players of [2,3,4]){
    const game=new Game({map,players}),troops=game.entities.filter(e=>e.type==='rock');
    for(const start of troops)for(const target of troops){
      if(start.team===target.team)continue;
      const route=game._path(start,target.x,target.y);let previous=start;
      for(const p of route){assert(game._lineFree(previous.x,previous.y,p.x,p.y,start.r),`${map}/${players}: route segment clears terrain`);previous=p;}
      assert(Math.hypot(previous.x-target.x,previous.y-target.y)<1,`${map}/${players}: team ${start.team} can reach team ${target.team}`);
    }
  }
});

test('trapped ground units stay inside overlapping fields and resume their order after expiry',()=>{
  const game=new Game();game._think=Infinity;game.entities=game.entities.filter(e=>e.type==='desk');game.map.obstacles=[];
  const trapped=game._spawn('rock',0,1200,1000),outside=game._spawn('rock',0,900,1200);
  game.forceFields=[{id:100,x:1200,y:1000,r:110,remaining:10,total:10},{id:101,x:1360,y:1000,r:110,remaining:10,total:10}];game._barrierRevision++;
  game.command([trapped.id],{type:'move',x:1600,y:1000});game.command([outside.id],{type:'move',x:1600,y:1200});
  for(let t=0;t<9;t+=.05){game.update(.05);assert(Math.hypot(trapped.x-1200,trapped.y-1000)<110,'the inner troop cannot cross a dome');assert(!game._blocked(trapped.x,trapped.y,trapped.r));}
  assert(outside.x>1500,'the outside troop finds a clear route');
  advance(game,5);assert.equal(game.forceFields.length,0);assert(trapped.x>1500,'the cached movement order resumes when the fields expire');
});

test('conveyor control survives an eliminated owner in a multiplayer match',()=>{
  const game=new Game({map:'conveyor',players:3});game._think=Infinity;
  const station=game.map.stations[0],original=game.getEntity(station.entityId);
  station.owner=1;original.team=1;game.map.conveyors.find(b=>b.id===station.beltId).owner=1;
  game.teams[1].eliminated=true;original.hp=0;
  updateBattlefield(game,.05);
  const replacement=game.getEntity(station.entityId);
  assert(replacement);assert.equal(replacement.type,'station');assert.equal(replacement.team,-1);assert.equal(station.owner,-1);
  assert.equal(game.map.conveyors.find(b=>b.id===station.beltId).owner,-1);
});

test('ordinary melee troops destroy the full-health gate and traverse the newly opened center',()=>{
  const game=new Game({map:'canyon'});game._think=Infinity;game.entities=game.entities.filter(e=>['desk','staplergate'].includes(e.type));
  const gate=game.entities.find(e=>e.type==='staplergate'),troops=[];
  for(let i=0;i<12;i++)troops.push(game._spawn('rock',0,2160-Math.floor(i/4)*45,1530+(i%4)*45));
  assert(game.canTarget(troops[0],gate));game.command(troops.map(e=>e.id),{type:'attack',targetId:gate.id});advance(game,35);
  assert.equal(gate.hp,0);assert(game.map.gate.open);
  game.command(troops.map(e=>e.id),{type:'move',x:2900,y:1600});advance(game,15);
  assert(troops.every(e=>Math.hypot(e.x-2900,e.y-1600)<180),'the central crossing now leads to the far bank');
  assert(troops.every(e=>!game._blocked(e.x,e.y,e.r)));
});
