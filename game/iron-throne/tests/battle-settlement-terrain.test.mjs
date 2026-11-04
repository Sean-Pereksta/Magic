import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattle,beginBattle,terrainHeight,nearestBattlePosition,normalizeBattleCheckpoint,validateBattleCheckpoint} from '../battle-sim.mjs';
import {settlementApron} from '../battle-terrain.mjs';

const sites=[
  {building:'town',levels:{town:1}}, {building:'city',levels:{city:4}},
  {levels:{wall:1},walls:35}, {building:'town',levels:{town:1,wall:1},walls:45},
  {building:'fort',levels:{fort:1,wall:1},walls:60,fortIntegrity:45},
  {building:'fort',levels:{fort:2,wall:2},walls:120,fortIntegrity:100},
  {building:'city',levels:{city:4,wall:3},walls:180}
];
function encounter(tile) {
  return {id:'mountain-city',seed:116,playerOwner:'red',playerSide:0,tile,
    participants:[{armyId:'red',owner:'red',side:0,units:{menAtArms:2}},{armyId:'white',owner:'white',side:1,units:{menAtArms:2}}]};
}
function clearApron(b) {
  const apron=settlementApron(b.layout);
  for(const o of [...b.layout.obstacles.filter(o=>['rock','water','tree'].includes(o.kind)),...b.layout.bridges]) {
    const w=(o.radius ?? o.width/2)+.35, d=(o.radius ?? o.depth/2)+.35;
    assert.ok(o.x+w<=apron.left || o.x-w>=apron.right || o.z+d<=apron.front || o.z-d>=apron.rear,`${o.id ?? 'bridge'} intrudes on ${b.layout.castle?.name ?? 'old town'}`);
  }
  for(const x of [apron.left,0,apron.right]) for(const z of [apron.front,(apron.front+apron.rear)/2,apron.rear]) {
    assert.ok(Math.abs(terrainHeight(b,x,z))<1e-7,`uneven at ${x}, ${z}`);
  }
}

test('every settlement tier has level roads, ladder feet and exterior staging areas on every terrain',()=>{
  for(const site of sites) for(const terrain of ['plains','hills','mountain','pass','forest','river','desert']) for(const crossed of [false,true]) {
    const e=encounter({...site,terrain,crossing:crossed}), b=createBattle(e);
    clearApron(b);
    for(const ladder of b.layout.ladders) assert.equal(nearestBattlePosition(b,ladder.foot).reachable,true);
    assert.ok(validateBattleCheckpoint(b,e));
    assert.deepEqual(createBattle(e).layout,b.layout);
    const natural=b.layout.obstacles;
    if (terrain==='mountain' || terrain==='pass') assert.equal(natural.filter(o=>o.kind==='rock').length,2);
    if (crossed || terrain==='river') {
      assert.ok(natural.some(o=>o.kind==='water'));
      assert.equal(b.layout.bridges.length,2);
      assert.ok(b.layout.bridges.every(bridge=>bridge.z+bridge.depth/2<settlementApron(b.layout).front));
      const bridge=b.layout.bridges[0];
      assert.equal(nearestBattlePosition(b,{x:bridge.x,z:bridge.z}).reachable,true);
      assert.equal(nearestBattlePosition(b,{x:0,z:bridge.z}).reachable,false);
    }
    if (terrain==='forest') assert.ok(natural.some(o=>o.kind==='tree'));
  }
});

test('unsettled fields keep both original pass formations, river crossings, woodland and intervening hills',()=>{
  const pass=createBattle(encounter({terrain:'pass'})), river=createBattle(encounter({terrain:'river'})), woods=createBattle(encounter({terrain:'forest'}));
  assert.equal(settlementApron(pass.layout),null);
  assert.deepEqual(pass.layout.obstacles.map(o=>[o.x,o.z,o.width,o.depth]),[[-52,0,68,24],[52,0,68,24]]);
  assert.equal(nearestBattlePosition(pass,{x:30,z:0}).reachable,false);
  assert.ok(Math.abs(terrainHeight(pass,-25,25))>.2);
  assert.equal(river.layout.bridges.length,2); assert.equal(nearestBattlePosition(river,{x:0,z:0}).reachable,false);
  assert.equal(nearestBattlePosition(river,{x:-20,z:0}).reachable,true);
  assert.ok(woods.layout.obstacles.filter(o=>o.kind==='tree').length>30);
});

test('legacy battle checkpoints trim obstacles in place while preserving soldiers, wall damage and old forts',()=>{
  const e=encounter({...sites[6],terrain:'mountain',river:true});
  const original=createBattle(e); beginBattle(original);
  const saved=JSON.parse(JSON.stringify(original));
  delete saved.layout.castle; delete saved.layout.settlementTerrainVersion;
  const wall=saved.layout.walls[0]; wall.hp-=20; const savedHp=wall.hp;
  saved.layout.obstacles=saved.layout.obstacles.filter(o=>o.kind!=='rock' && o.kind!=='water');
  saved.layout.obstacles.push({id:'mountain-west',kind:'rock',x:-52,z:0,width:68,depth:24,height:13},
    {id:'water-middle',kind:'water',x:0,z:0,width:55,depth:10,height:.1},
    {id:'tree-on-road',kind:'tree',x:0,z:7,radius:1,height:7});
  saved.layout.bridges=[{x:28,z:0,width:10,depth:12}];
  saved.units[0].hp-=4;
  const snapshot=saved.units.map(u=>[u.id,u.x,u.z,u.hp,u.status]), beforeRevision=saved.layout.navRevision;
  // Leave the old deployment bounds to the existing deployment migration.
  delete saved.layout.deploymentZones; delete saved.deploymentLayoutVersion;
  assert.ok(validateBattleCheckpoint(saved,e));
  normalizeBattleCheckpoint(saved);
  assert.equal(saved.layout.castle,undefined); assert.equal(saved.layout.walls[0].hp,savedHp);
  assert.deepEqual(saved.units.map(u=>[u.id,u.x,u.z,u.hp,u.status]),snapshot);
  assert.equal(saved.layout.navRevision,beforeRevision+1); assert.ok(validateBattleCheckpoint(saved,e));
  clearApron(saved);
  const once=JSON.parse(JSON.stringify(saved)); normalizeBattleCheckpoint(saved); assert.deepEqual(saved,once);
});

test('legacy unwalled town houses protect their original streets without installing new walls',()=>{
  const b=createBattle(encounter({terrain:'hills',building:'town'}));
  const layout=b.layout; delete layout.castle; delete layout.settlementTerrainVersion; delete layout.climate;
  layout.obstacles=[{id:'house-west',kind:'building',x:-23,z:30,width:11,depth:10,height:7},
    {id:'house-east',kind:'building',x:23,z:49,width:11,depth:10,height:7},
    {id:'tree-in-town',kind:'tree',x:17,z:27,radius:1,height:8}];
  // A restored checkpoint has a fresh layout identity and no derived caches.
  const loaded=JSON.parse(JSON.stringify(b));
  normalizeBattleCheckpoint(loaded);
  assert.equal(loaded.layout.walls.length,0);
  assert.equal(loaded.layout.obstacles.length,2); clearApron(loaded);
  const forged=JSON.parse(JSON.stringify(loaded));
  forged.layout.obstacles.push({kind:'water',id:'flooded-market',x:0,z:30,width:10,depth:10,height:.1});
  assert.equal(validateBattleCheckpoint(forged,encounter({terrain:'hills',building:'town'})),false);
});
