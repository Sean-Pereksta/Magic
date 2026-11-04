import test from 'node:test';
import assert from 'node:assert/strict';
import {beginBattle,createBattle,normalizeBattleCheckpoint,stepBattle,validateBattleCheckpoint} from '../battle-sim.mjs';

function encounter(playerUnits={menAtArms:2}) {
  return {id:'hero-duel',playerOwner:'ashen',seed:31,tile:{terrain:'plains'},participants:[
    {armyId:'home',side:0,owner:'ashen',units:playerUnits},
    {armyId:'away',side:1,owner:'wintermere',units:{menAtArms:1}}
  ]};
}

test('the commander is one counted soldier, with more health and personal weapon damage than a peer',()=>{
  function strike(useHero){
    const e=encounter(),b=createBattle(e);beginBattle(b);
    const hero=b.units.find(u=>u.id===b.commanderId),peer=b.units.find(u=>u.side===0&&u!==hero);
    assert.equal(hero.maxHp,120);assert.equal(peer.maxHp,100);assert.equal(b.units.length,3);
    const fighter=useHero?hero:peer,other=useHero?peer:hero,victim=b.units.find(u=>u.side===1);
    Object.assign(other,{x:-60,z:-40});
    Object.assign(fighter,{x:0,z:-20,facing:0});
    Object.assign(victim,{x:0,z:-18.4,facing:0,shield:false,explicitOrder:true,order:{kind:'hold',x:0,z:-18.4}});
    fighter.action={kind:'attack',elapsed:.29,windup:.3,duration:.75,recovery:.45,targetId:victim.id,
      targetX:victim.x,targetZ:victim.z,targetY:victim.y,facing:0,impacted:false};
    stepBattle(b,.05,{commanderAI:true});
    return b.events.find(e=>e.type==='hit'&&e.sourceId===fighter.id)?.damageApplied;
  }
  const ordinary=strike(false),hero=strike(true);
  assert.ok(ordinary>0&&Math.abs(hero-ordinary*1.1)<1e-8);
});

test('old commander wounds scale once and a fallen commander stays dead',()=>{
  for(const hp of [0,40]){
    const e=encounter(),b=createBattle(e);beginBattle(b);
    const hero=b.units.find(u=>u.id===b.commanderId), peer=b.units.find(u=>u.side===0&&u!==hero);
    b.battleRulesVersion=3;Object.assign(hero,{hp,maxHp:100,status:hp?'active':'dead'});peer.hp=26;
    assert.ok(validateBattleCheckpoint(b,e));normalizeBattleCheckpoint(b);normalizeBattleCheckpoint(b);
    assert.equal(hero.maxHp,120);assert.equal(hero.hp,hp*1.2);assert.equal(hero.status,hp?'active':'dead');
    assert.equal(peer.hp,26);assert.ok(validateBattleCheckpoint(b,e));
    peer.maxHp=120;assert.equal(validateBattleCheckpoint(b,e),false);
  }
});

test('a siege engine does not gain hero health when it is the only player unit',()=>{
  const e=encounter({ram:1}),b=createBattle(e);
  assert.equal(b.units.find(u=>u.id===b.commanderId).maxHp,2400);
  assert.ok(validateBattleCheckpoint(b,e));
});
