import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const sim=require('./living-simulation.js');
function fixture(roles=['miner','hauler','smith','cook','farmer','mason']) {
  const wall=new Set(),seen=new Set(),dwarves=roles.map((role,i)=>({id:'d'+i,name:'Dwarf '+i,x:3,y:3,speed:6,hp:100,maxHp:100,pickaxe:{},miningRate:2,miningPower:8}));
  let gold=10000,engine;
  const host={width:40,height:30,base:()=>({x:3,y:3}),dwarves:()=>dwarves,spend:n=>{if(gold<n)return false;gold-=n;return true},explored:(x,y)=>x>0&&y>0&&x<39&&y<29&&!seen.has(x+','+y),walkable:(x,y)=>x>0&&y>0&&x<39&&y<29&&!wall.has(x+','+y),diggable:(x,y)=>wall.has(x+','+y),route:(sx,sy,tx,ty)=>{
    const queue=[{x:sx,y:sy}],visited=new Set([sx+','+sy]),prev=new Map();let head=0;
    while(head<queue.length){const p=queue[head++];if(p.x===tx&&p.y===ty){const path=[];let cur=p;while(cur.x!==sx||cur.y!==sy){path.push(cur);cur=prev.get(cur.x+','+cur.y)}return path.reverse()}
      for(const[dx,dy]of[[1,0],[-1,0],[0,1],[0,-1]]){const x=p.x+dx,y=p.y+dy,k=x+','+y;if(visited.has(k)||!host.walkable(x,y)||!engine.canWalk(x,y))continue;visited.add(k);prev.set(k,p);queue.push({x,y})}}
    return null;
  },mine:(x,y)=>{wall.delete(x+','+y);engine.deposit(x,y,'stone',3)},equip:d=>{if(d.weapon)return false;d.weapon={name:'Forged axe'};return true}};
  engine=sim.create(host);roles.forEach((r,i)=>engine.setProfession(dwarves[i].id,r));
  function tick(seconds){for(let n=0;n<Math.ceil(seconds*30);n++){engine.begin(1/30);for(const d of dwarves)engine.updateDwarf(d,1/30)}}
  function room(type,x,y,active=true){const result=engine.designate(type,x,y,3,3);assert.equal(result.ok,true,result.reason);if(active){result.room.active=true;result.room.progress=1}return result.room}
  return {engine,host,dwarves,wall,seen,tick,room,get gold(){return gold}};
}
test('legacy migration initializes a bounded additive state without modifying its input',()=>{
  const old={version:0,time:3,rooms:[],priorities:{food:99},chronicle:[{text:'Old event'}]},copy=structuredClone(old);
  const s=sim.migrate(old,{x:10,y:10});assert.equal(s.version,1);assert.equal(s.priorities.food,3);assert.equal(s.stores[0].x,10);assert.deepEqual(old,copy);
  assert.throws(()=>sim.migrate({version:999},{x:0,y:0}),/newer/);
  assert.throws(()=>sim.migrate({version:1,rooms:new Array(129).fill({})},{x:0,y:0}),/capacity/);
});
test('room validation rejects fog, overlap, non-floor, invalid dimensions and no corridor',()=>{
  const f=fixture();f.seen.add('8,8');assert.equal(f.engine.preview('garden',8,8,3,3).ok,false);f.seen.clear();f.wall.add('8,8');assert.equal(f.engine.preview('garden',8,8,3,3).ok,false);f.wall.clear();
  f.room('garden',8,8);assert.equal(f.engine.preview('kitchen',9,9,3,3).ok,false);assert.equal(f.engine.preview('kitchen',20,10,0,2).ok,false);
  for(let x=20;x<23;x++){f.wall.add(x+',9');f.wall.add(x+',13')}for(let y=10;y<13;y++){f.wall.add('19,'+y);f.wall.add('23,'+y)}assert.equal(f.engine.preview('garden',20,10,3,3).ok,false);
});
test('construction requires delivered stone and an on-site builder',()=>{
  const f=fixture(['hauler','mason']);const r=f.room('quarters',8,8,false);f.tick(.5);assert.equal(r.active,false);assert.equal(r.inventory.stone,0);f.tick(40);assert.equal(r.active,true);assert.equal(r.inventory.stone,0);assert.equal(f.engine.state.stores[0].inventory.stone,33);assert.ok(f.engine.state.chronicle.some(v=>v.text.includes('Completed Residential')));
});
test('ore travels through smelting and smithing to the armory before a soldier equips it',()=>{
  const f=fixture(['hauler','hauler','smith','smith','soldier']);const smelter=f.room('smelter',7,4),smithy=f.room('smithy',13,4),armory=f.room('armory',19,4);f.engine.deposit(5,5,'ore',12);
  f.tick(2);assert.equal(smelter.inventory.ingot,0);assert.equal(smithy.inventory.equipment,0);assert.equal(f.dwarves[4].weapon,undefined);
  f.tick(160);assert.ok(f.engine.state.stats.produced>=2);assert.ok(f.engine.state.stats.hauled>0);assert.ok(f.dwarves[4].weapon,'soldier must collect forged equipment physically');assert.equal(armory.type,'armory');
});
test('food production feeds a dining hall and hungry dwarf at its location',()=>{
  const f=fixture(['hauler','farmer','cook','mason']);f.room('garden',7,5);f.room('kitchen',13,5);f.room('dining',19,5);f.engine.state.stores[0].inventory.meal=0;f.dwarves[3].living.hunger=80;
  f.tick(150);assert.ok(f.engine.state.stats.produced>0);assert.ok(f.dwarves[3].living.hunger<58,'dwarf eats real meal');assert.ok(f.engine.state.stats.hauled>0);
});
test('unreachable supplies never teleport and reservations do not duplicate ore',()=>{
  const f=fixture(['hauler','hauler','hauler']);const room=f.room('smelter',24,5);f.engine.deposit(5,5,'ore',8);for(let y=1;y<29;y++)f.wall.add('20,'+y);
  f.tick(40);assert.equal(room.inventory.ore,0);
  const total=()=>f.engine.state.deposits.reduce((n,p)=>n+p.inventory.ore,0)+f.engine.stores().reduce((n,p)=>n+p.inventory.ore,0)+f.dwarves.reduce((n,d)=>n+(d.living.cargo?.resource==='ore'?d.living.cargo.amount:0),0);
  assert.equal(total(),8);f.wall.delete('20,6');f.tick(80);assert.ok(room.inventory.ore>0);assert.equal(total(),8);
});
test('pausing a profession, canceling a room and save roundtrip preserve held cargo',()=>{
  const f=fixture(['hauler']);const r=f.room('smelter',15,6);f.engine.deposit(4,3,'ore',8);f.tick(1.5);assert.ok(f.dwarves[0].living.cargo);const cargo=structuredClone(f.dwarves[0].living.cargo);f.engine.setProfession('d0','farmer');assert.deepEqual(f.dwarves[0].living.cargo,cargo);f.engine.removeRoom(r.id);f.engine.restore(f.engine.serialize());assert.deepEqual(f.dwarves[0].living.cargo,cargo);f.tick(20);assert.equal(f.dwarves[0].living.cargo,null);assert.equal(f.engine.state.stores[0].inventory.ore,8);
});
test('walls and lockdown gates change traversability and retain their health in saves',()=>{
  const f=fixture();assert.equal(f.engine.placeInfrastructure('gate',8,8).ok,true);assert.equal(f.engine.canWalk(8,8),true);assert.equal(f.engine.canWalk(8,8,true),false);f.engine.setReadiness('lockdown');assert.equal(f.engine.canWalk(8,8),false);const s=f.engine.serialize();f.engine.restore(s);assert.equal(f.engine.state.readiness,'lockdown');assert.equal(f.engine.infraAt(8,8).hp,420);
});
test('dig reservations produce quarry material only after a miner reaches an adjacent tile',()=>{
  const f=fixture(['miner','miner']);f.wall.add('9,7');assert.equal(f.engine.digArea(9,7,1,1).ok,true);f.tick(.25);assert.equal(f.wall.has('9,7'),true);f.tick(10);assert.equal(f.wall.has('9,7'),false);assert.equal(f.engine.state.deposits.reduce((n,v)=>n+v.inventory.stone,0),3);
});
test('fractional position inside a destination tile finishes its route and starts work',()=>{
  const f=fixture(['farmer']);const r=f.room('garden',7,5);f.dwarves[0].x=8.3;f.dwarves[0].y=6.2;
  f.tick(15);assert.ok(r.inventory.mushroom>0,'empty point route still centers the dwarf');assert.equal(f.dwarves[0].x,8);assert.equal(f.dwarves[0].y,6);
});
test('interrupting production refunds ingredients once, while completed work consumes them',()=>{
  const f=fixture(['smith']);const r=f.room('smelter',7,5);r.inventory.ore=3;f.dwarves[0].x=8;f.dwarves[0].y=6;
  f.tick(1);assert.equal(r.inventory.ore,0);assert.equal(f.dwarves[0].living.job.started,true);
  f.engine.setProfession('d0','farmer');assert.equal(r.inventory.ore,3);f.engine.cancel(f.dwarves[0]);assert.equal(r.inventory.ore,3);
  f.engine.setProfession('d0','smith');f.tick(16);assert.equal(r.inventory.ore,0);assert.equal(r.inventory.ingot,2);
});
test('dismantling infrastructure restores access and leaves stored supplies at the actual tile',()=>{
  const f=fixture(['hauler']);const gate=f.engine.placeInfrastructure('gate',8,8).infrastructure;f.engine.setReadiness('lockdown');assert.equal(f.engine.canWalk(8,8),false);
  assert.equal(f.engine.removeInfrastructure(gate.id).ok,true);assert.equal(f.engine.canWalk(8,8),true);
  const depot=f.engine.placeInfrastructure('depot',12,8).infrastructure;depot.inventory.ore=7;
  assert.equal(f.engine.removeInfrastructure(depot.id).ok,true);const pile=f.engine.state.deposits.find(p=>p.x===12&&p.y===8);assert.equal(pile.inventory.ore,7);assert.equal(f.engine.state.stores[0].inventory.ore,0);
});
test('contending haulers conserve every material through pickup, delivery, cancellation and restored reservations',()=>{
  const f=fixture(new Array(10).fill('hauler'));
  for(const[type,x,y]of[['smelter',7,4],['smithy',13,4],['kitchen',19,4],['dining',7,10],['medical',13,10],['armory',19,10],['vault',25,10]])f.room(type,x,y);
  for(let i=0;i<sim.MATERIALS.length;i++){f.engine.deposit(4+i,18,sim.MATERIALS[i],17);f.engine.deposit(25+i,20,sim.MATERIALS[i],11)}
  const total=()=>{const result=Object.fromEntries(sim.MATERIALS.map(k=>[k,0]));for(const source of [...f.engine.state.rooms,...f.engine.state.stores,...f.engine.state.infrastructure,...f.engine.state.deposits])for(const k of sim.MATERIALS)result[k]+=source.inventory?.[k]||0;for(const d of f.dwarves)if(d.living.cargo)result[d.living.cargo.resource]+=d.living.cargo.amount;return result;};
  const baseline=total();
  for(let round=0;round<30;round++){
    f.tick(2);
    if(round%4===0)f.engine.setProfession('d0',round%8===0?'engineer':'hauler');
    if(round%5===0)f.engine.restore(f.engine.serialize());
    assert.deepEqual(total(),baseline,'all goods remain at a real source, destination or carrier');
    const pending=new Map();for(const d of f.dwarves){const j=d.living.job;if(j?.kind==='haul'&&!d.living.cargo){const k=j.source+':'+j.resource;pending.set(k,(pending.get(k)||0)+j.amount)}}
    for(const source of [...f.engine.state.rooms,...f.engine.state.stores,...f.engine.state.infrastructure,...f.engine.state.deposits])for(const k of sim.MATERIALS){assert.ok((source.inventory?.[k]||0)>=0);assert.ok((pending.get(source.id+':'+k)||0)<=(source.inventory?.[k]||0),'outstanding pickup reservations never exceed available stock');}
  }
  assert.ok(f.engine.state.stats.hauled>0,'contention exercise must deliver real cargo');
});
test('large pile fields keep haul searches bounded and eventually inspect distant supplies',()=>{
  const base={x:3,y:3},d={id:'remote-hauler',name:'Remote hauler',x:3,y:3,speed:2,hp:100,maxHp:100},raw=sim.fresh(base);
  for(let i=0;i<8192;i++)raw.deposits.push({id:'remote-'+i,x:100+i%128,y:50+Math.floor(i/128),inventory:{knowledge:1,...(i===8191?{ore:6}:{})}});
  let engine;
  const host={width:256,height:160,base:()=>base,dwarves:()=>[d],spend:()=>true,explored:()=>true,walkable:()=>true,route:(sx,sy,tx,ty)=>[{x:sx+Math.sign(tx-sx),y:sy+Math.sign(ty-sy)}]};
  engine=sim.create(host,raw);engine.setProfession(d.id,'hauler');const room=engine.designate('smelter',8,8,3,3).room;room.active=true;room.progress=1;engine.reindex();
  let found=false;
  for(let n=0;n<300&&!found;n++){engine.cancel(d);d.living.nextThink=0;engine.begin(.001);engine.updateDwarf(d,.001);found=d.living.job?.resource==='ore';}
  assert.equal(found,true,'round-robin source windows eventually include the far ore pile');
  assert.ok(engine.diagnostics.maxSourceCandidates<=128,'each hauling query has a strict source budget');
  assert.ok(engine.diagnostics.sourceChecks<=engine.diagnostics.haulQueries*128);
  assert.equal(engine.state.deposits.length,8192,'query test does not discard distant piles');
});
test('off-shift communal visits form friendships and hardship gradually lowers morale without starvation damage',()=>{
  const f=fixture(['mason','mason']);f.room('dining',7,5);for(const d of f.dwarves){d.x=8;d.y=6;d.living.shift=2;d.living.fatigue=25;d.living.morale=75;}
  for(const time of [200,440,680]){f.engine.state.time=time;f.tick(8);}
  assert.ok(f.dwarves[0].living.relationships.d1>=3);assert.ok(f.engine.state.chronicle.some(e=>e.text.includes('Became friends')));
  const d=f.dwarves[0];d.living.hunger=95;d.living.fatigue=95;d.living.morale=75;d.hp=40;f.tick(1);
  assert.ok(d.living.morale<75);assert.equal(d.hp,40,'needs do not introduce new starvation deaths');
});
test('masterwork completion briefly celebrates before accepting another work order',()=>{
  const f=fixture(['farmer']);f.room('garden',7,5);const d=f.dwarves[0];d.x=8;d.y=6;d.living.skill=19;f.tick(10);
  assert.equal(f.engine.state.stats.masterworks,1);assert.equal(d.living.activity,'Celebrating');assert.equal(d.living.job,null);f.tick(4);assert.notEqual(d.living.activity,'Celebrating');
});

test('haulers leave scarce workplace ingredients in place and transfer only surplus',()=>{
  const f=fixture(['hauler','hauler']);const a=f.room('smelter',7,5),b=f.room('smelter',15,5);a.inventory.ore=8;
  f.tick(25);assert.equal(a.inventory.ore,8);assert.equal(b.inventory.ore,0);assert.equal(f.engine.state.stats.hauled,0);
  a.inventory.ore=14;f.tick(40);assert.equal(a.inventory.ore,9);assert.equal(b.inventory.ore,5);assert.equal(f.engine.state.stats.hauled,5);
});

test('full pile capacity preserves carried goods, construction contents and unfinished ingredients',()=>{
  const base={x:3,y:3},d={id:'carrier',name:'Carrier',x:3,y:3,speed:2,hp:100,maxHp:100},raw=sim.fresh(base);
  for(let i=0;i<8192;i++)raw.deposits.push({id:'capacity-'+i,x:100+i%128,y:50+Math.floor(i/128),inventory:{knowledge:1}});
  const host={width:256,height:160,base:()=>base,dwarves:()=>[d],spend:()=>true,explored:()=>true,walkable:()=>true,route:()=>null,threat:()=>true};
  const engine=sim.create(host,raw);engine.person(d).cargo={resource:'ore',amount:5};
  assert.equal(engine.dropCargo(d),false);assert.equal(d.living.cargo.amount,5);assert.equal(engine.state.deposits.length,8192);
  const room=engine.designate('smelter',8,8,3,3).room;room.active=true;room.progress=1;
  d.living.job={kind:'produce',dest:room.id,started:true,inputs:{ore:3},goal:{x:9,y:9},elapsed:2};
  assert.equal(engine.removeRoom(room.id),false);assert.equal(d.living.job.started,true);assert.equal(engine.state.rooms.length,1);
  const depot=engine.placeInfrastructure('depot',12,8).infrastructure;depot.inventory.ore=4;
  assert.equal(engine.removeInfrastructure(depot.id).ok,false);assert.equal(depot.inventory.ore,4);
  engine.state.rooms=[];engine.reindex();assert.equal(engine.cancel(d),false);assert.equal(engine.setProfession(d.id,'farmer'),false);
  engine.state.readiness='alert';engine.updateDwarf(d,.1);assert.equal(d.living.job.kind,'produce','emergency shelter cannot overwrite held orphaned ingredients');
  engine.state.deposits.pop();engine.reindex();assert.equal(engine.cancel(d),true);assert.equal(engine.state.deposits.find(v=>v.x===3&&v.y===3).inventory.ore,3);
  assert.equal(engine.dropCargo(d),true);assert.equal(engine.state.deposits.find(v=>v.x===3&&v.y===3).inventory.ore,8);
  const full=engine.state.deposits[0];full.inventory.knowledge=1000000;assert.equal(engine.deposit(full.x,full.y,'knowledge',1),false);assert.equal(full.inventory.knowledge,1000000);
});
