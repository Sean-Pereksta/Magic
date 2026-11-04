import test from 'node:test';
import assert from 'node:assert/strict';
import { createBattle, beginBattle, issueOrder, stepBattle, battleResult, validateBattleCheckpoint, nearestBattlePosition } from '../battle-sim.mjs';

const royal={building:'city',levels:{city:4,wall:3},walls:180};
function encounter(tile,attack={menAtArms:8,ram:1},defend={menAtArms:24,spearman:12,archer:16}) {
  return {id:'fortress',seed:117,tile,playerOwner:'red',playerSide:0,
    participants:[{armyId:'red',owner:'red',side:0,units:attack},{armyId:'white',owner:'white',side:1,units:defend}]};
}

test('paid fort, city and wall levels change the footprint, durability, items and number of wards',()=>{
  const sites=[
    {levels:{wall:1},walls:35}, {building:'town',levels:{wall:1,town:1},walls:45},
    {building:'fort',levels:{fort:1,wall:1},walls:60,fortIntegrity:45},
    {building:'fort',levels:{fort:2,wall:2},walls:120,fortIntegrity:100}, royal
  ].map(tile=>createBattle(encounter(tile)).layout);
  for (let i=0;i<sites.length;i++) {
    const {castle,walls,ladders,stairs}=sites[i];
    assert.equal(castle.tier,i);
    if (i) { assert.ok(castle.outer.right>sites[i-1].castle.outer.right); assert.ok(walls[0].maxHp>sites[i-1].walls[0].maxHp); }
    assert.equal(!!castle.inner,i>=3);
    assert.equal(walls[0].material,i<2?'timber':'stone');
    assert.equal(ladders.length,i>=3?4:2);
    assert.equal(stairs.length,i>=3?10:5);
    assert.ok(castle.items.every(item=>item.kind==='banner' || sites[i].obstacles.some(o=>o.itemKind===item.kind && o.x===item.x && o.z===item.z)));
  }
  assert.ok(sites[4].castle.items.some(i=>i.kind==='chapel'));
  const town=createBattle(encounter({building:'town',levels:{town:1}}));
  assert.equal(town.layout.fortified,false); assert.equal(town.layout.walls.length,0);
  assert.ok(town.layout.castle.items.some(i=>i.kind==='house'));
});

test('a royal city stations real defenders on both wall walks and in the inner courtyard',()=>{
  const e=encounter(royal), b=createBattle(e), ward=b.layout.castle.inner;
  const defenders=b.units.filter(u=>u.status==='active' && u.side===1);
  const interior=defenders.filter(u=>!u.wallId && u.x>ward.left && u.x<ward.right && u.z>ward.front && u.z<ward.rear);
  assert.ok(interior.length>=15,`only ${interior.length} inside`);
  assert.ok(defenders.some(u=>u.family==='ranged' && u.wallId?.startsWith('inner-')));
  assert.ok(defenders.some(u=>u.family==='ranged' && u.wallId && !u.wallId.startsWith('inner-')));
  assert.ok(interior.some(u=>u.fortPost==='innerGate'));
  assert.ok(interior.every(u=>nearestBattlePosition(b,u,u).reachable));
  assert.ok(validateBattleCheckpoint(JSON.parse(JSON.stringify(b)),e));
});

test('the ram must enter the outer ward before it can breach the royal inner gate',()=>{
  const e=encounter(royal,{menAtArms:7,ram:1},{cavalry:1}), b=createBattle(e);
  const ram=b.units.find(u=>u.type==='ram'), gate=b.layout.walls.find(w=>w.id==='inner-gate');
  const order={ramId:ram.id,wallId:gate.id};
  beginBattle(b);
  assert.equal(issueOrder(b,'infantry','crew',order).accepted,false);
  b.layout.gate.hp=0; b.layout.gate.state='breached'; b.layout.navRevision++;
  assert.equal(issueOrder(b,'infantry','crew',order).accepted,true);
  for(let i=0;i<3600 && gate.hp>0;i++) stepBattle(b,.05,{});
  assert.equal(gate.hp,0,`ram at ${ram.x}, ${ram.z} with ${ram.crewCount} carriers`);
  assert.equal(b.layout.walls.find(w=>w.id==='inner-wall-east-front').hp,b.layout.walls.find(w=>w.id==='inner-wall-east-front').maxHp);
  const result=battleResult(b);
  assert.ok(result.wallBreaches.includes('inner-gate'));
  const next=createBattle({...e,tile:{...royal,walls:result.walls,wallSections:result.wallSections,wallBreaches:result.wallBreaches}});
  assert.equal(next.layout.walls.find(w=>w.id==='inner-gate').hp,0);
});

test('occupation requires an actual route past both defenses; foot troops can use both ladders',()=>{
  for (const [attack,occupies] of [[{cavalry:1},false],[{menAtArms:1},true]]) {
    const b=createBattle(encounter(royal,attack,{}));
    b.layout.gate.hp=0;
    beginBattle(b);
    assert.equal(b.phase,'complete'); assert.equal(battleResult(b).occupationSide===0,occupies);
  }
  const closed=createBattle(encounter(royal,{menAtArms:1},{}));
  beginBattle(closed); assert.equal(battleResult(closed).occupationSide,0);
});
test('AI assault infantry physically ascend the outer and inner ward without a breach',()=>{
  const e=encounter(royal,{menAtArms:12},{cavalry:1});
  e.playerOwner='white'; e.playerSide=1;
  const b=createBattle(e), guard=b.units.find(u=>u.side===1);
  beginBattle(b);
  Object.assign(guard,{x:0,z:b.layout.objective.z,order:{kind:'hold',x:0,z:b.layout.objective.z}});
  const outer=new Set(), inner=new Set();
  for(let i=0;i<3000 && inner.size===0;i++) {
    stepBattle(b,.05,{});
    for(const u of b.units.filter(u=>u.side===0 && u.wallId)){
      (u.wallId.startsWith('inner-')?inner:outer).add(u.id);
    }
  }
  assert.ok([...inner].some(id=>outer.has(id)),JSON.stringify(b.units.filter(u=>u.side===0).map(u=>[u.x,u.z,u.wallId,u.order.kind,u.order.ladderId])));
});
