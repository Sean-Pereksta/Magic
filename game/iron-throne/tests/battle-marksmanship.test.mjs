import test from 'node:test';
import assert from 'node:assert/strict';
import { beginBattle, createBattle, issueOrder, normalizeBattleCheckpoint, rangedAccuracy, stepBattle, validateBattleCheckpoint } from '../battle-sim.mjs';
import { ramHandle } from '../battle-siege.mjs';

function encounter(units,tile={terrain:'plains'}){
  return {id:'weapon-drill',seed:71,playerOwner:'blue',tile,participants:[
    {armyId:'vanguard',owner:'blue',side:0,units,morale:1},
    {armyId:'red',owner:'red',side:1,units:{levy:1},morale:1}
  ]};
}
function rangeFixture(type,posted){
  const b=createBattle(encounter({[type]:1}));beginBattle(b);
  delete b.layout.climate; // Level practice ground, with a clear shot into the courtyard.
  const [shooter,target]=b.units;
  Object.assign(shooter,{x:0,z:0,y:posted?6:0,facing:0,wallId:posted?'drill-wall':null,explicitOrder:true,order:{kind:'hold',x:0,z:0}});
  if(posted)b.layout.walls.push({id:'drill-wall',x:0,z:0,width:12,depth:2,hp:220,maxHp:220,height:6});
  Object.assign(target,{x:0,z:30,y:0,explicitOrder:true,order:{kind:'hold',x:0,z:30}});
  shooter.action={kind:'shoot',ranged:true,targetId:target.id,targetX:0,targetY:0,targetZ:30,
    elapsed:.73,windup:.75,duration:2,recovery:1.25,facing:0,impacted:false};
  return b;
}
test('soldiers have repeatable individual training and gain the bonus only on an intact wall',()=>{
  const b=createBattle(encounter({archer:12,crossbow:1,veteranArcher:1,ram:1}));
  const archers=b.units.filter(u=>u.type==='archer'),ratings=archers.map(u=>rangedAccuracy(b,u));
  assert.ok(new Set(ratings).size>=3);
  assert.ok(Math.min(...ratings)>=53&&Math.max(...ratings)<=63);
  assert.ok(rangedAccuracy(b,b.units.find(u=>u.type==='veteranArcher'))>rangedAccuracy(b,b.units.find(u=>u.type==='crossbow')));
  assert.equal(rangedAccuracy(b,b.units.find(u=>u.type==='ram')),null);
  b.layout.walls.push({id:'post',hp:20});archers[0].wallId='post';
  assert.equal(rangedAccuracy(b,archers[0]),ratings[0]+10);
  b.layout.walls[0].hp=0;assert.equal(rangedAccuracy(b,archers[0]),ratings[0]);
});
test('real projectiles sometimes miss at thirty metres; trained and posted shooters connect more often',t=>{
  function hits(type,posted){
    const base=rangeFixture(type,posted);let hits=0;
    for(let i=1;i<=240;i++){
      const b=structuredClone(base);b.rng=Math.imul(i,2654435761)>>>0;
      stepBattle(b,.05,{commanderAI:true});assert.equal(b.nextProjectile,1);
      while(b.projectiles.length)stepBattle(b,.05,{commanderAI:true});
      if(b.units[1].hp<b.units[1].maxHp)hits++;
    }
    return hits;
  }
  const regular=hits('archer',false), crossbow=hits('crossbow',false), veteran=hits('veteranArcher',false), wall=hits('archer',true);
  t.diagnostic(`Hits per 240 shots at 30 m: archer ${regular}, crossbows ${crossbow}, veterans ${veteran}, wall archer ${wall}.`);
  assert.ok(regular>0&&regular<crossbow&&crossbow<veteran&&veteran<240,`hits per 240: ${regular}/${crossbow}/${veteran}`);
  assert.ok(wall>regular&&wall<240,`posted ${wall}, ground ${regular}`);
});
test('an arrow in flight keeps its released destination when its target moves and after a reload',()=>{
  const b=rangeFixture('veteranArcher',false);stepBattle(b,.05,{commanderAI:true});
  const arrow=b.projectiles[0],end=[arrow.targetX,arrow.targetY,arrow.targetZ];b.units[1].x=14;b.units[1].order.x=14;
  const restored=structuredClone(b);
  for(let i=0;i<30;i++) {
    stepBattle(b,.05,{commanderAI:true});stepBattle(restored,.05,{commanderAI:true});
    assert.deepEqual([arrow.targetX,arrow.targetY,arrow.targetZ],end);
  }
  assert.equal(b.units[1].hp,b.units[1].maxHp);assert.deepEqual(b,restored);
});
test('legacy ram health scales once, retaining wounds and dead engines',()=>{
  const e=encounter({menAtArms:7,ram:2}),b=createBattle(e);beginBattle(b);
  const [hurt,dead]=b.units.filter(u=>u.type==='ram');b.battleRulesVersion=2;
  Object.assign(hurt,{hp:70,maxHp:280});Object.assign(dead,{hp:0,maxHp:280,status:'dead'});
  assert.equal(validateBattleCheckpoint(b,e),true);normalizeBattleCheckpoint(b);normalizeBattleCheckpoint(b);
  assert.equal(hurt.maxHp,2400);assert.equal(hurt.hp,600);assert.equal(dead.hp,0);assert.equal(dead.status,'dead');
  assert.equal(validateBattleCheckpoint(b,e),true);
});
test('a four-man team rolls more slowly than six pushers, and recall stops the wheels',()=>{
  function rolling(count) {
    const b=createBattle(encounter({menAtArms:7,ram:1},{building:'fort',walls:60,fortIntegrity:45}));beginBattle(b);
    const ram=b.units.find(u=>u.type==='ram'), crew=b.units.filter(u=>u.type==='menAtArms'&&!u.commander).slice(0,count);
    Object.assign(ram,{x:0,z:-18,facing:0});issueOrder(b,crew.map(u=>u.id),'crew',{ramId:ram.id,wallId:'gate'});
    for(const u of crew)Object.assign(u,ramHandle(ram,u.order.handle));
    for(let i=0;i<40;i++)stepBattle(b,.05);
    assert.equal(ram.crewCount,count);assert.ok(ram.rolledDistance>0&&ram.rolledDistance<=2.2);
    assert.ok(crew.every(u=>Math.hypot(u.x-ramHandle(ram,u.order.handle).x,u.z-ramHandle(ram,u.order.handle).z)<.01));
    issueOrder(b,crew.map(u=>u.id),'follow');const stopped=[ram.x,ram.z,ram.rolledDistance];
    for(let i=0;i<20;i++)stepBattle(b,.05);
    assert.deepEqual([ram.x,ram.z,ram.rolledDistance],stopped);
    return ram.rolledDistance;
  }
  assert.ok(rolling(6)>rolling(4)*1.3);
});
