import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';

// Every catalogue entry is exercised through the same combat action used by UI.
// This catches named techniques that exist in data but have no working effect.
test('every learnable technique executes in combat with a legal relay target',()=>{
  for(const [id,move]of Object.entries(C.MOVES)){
    const source=Object.values(C.SPECIES).find(p=>C.unlocked({species:p.id,types:p.types,level:60,stage:3}).includes(id));
    const s=C.newSave('Ability Trial','capuchin','practice',99);const p=s.run.monkey;
    p.species=source.id;p.types=[...source.types];p.level=60;p.stage=3;p.learned=[...new Set([...source.learnset,id])];p.loadout=['strike',id,'brace','breathe'].filter((v,i,a)=>a.indexOf(v)===i);p.energy=18;p.hp=Math.round(C.stats(p).hp*.6);
    const reserve=structuredClone(p);reserve.id='reserve';reserve.energy=1;reserve.loadout=['strike','brace','breathe'];reserve.status={};s.collection.reserve=reserve;s.run.party.push('reserve');
    C.advance(s,C.ENCOUNTER_STEP,0,C.ENCOUNTER_STEP);if(s.run.pendingEncounter)C.beginPendingEncounter(s);C.readyBattle(s);const e=s.run.battle.enemy;e.hp=10000;e.level=1;e.energy=12;e.loadout=['strike'];s.run.battle.intent='strike';s.run.battle.enemyAction={type:'move',id:'strike'};
    if(move.cleanse){p.status={burn:2};p.statusPower={};}
    assert.doesNotThrow(()=>C.turn(s,id,false,{targetId:'reserve'}),`${id} failed to execute`);
    assert.ok(s.run.battle?.round===2||s.run.phase==='replacement',`${id} failed to resolve one round`);
    if(move.charge){assert.equal(p.charging.id,id);s.run.battle.enemyAction={type:'move',id:'breathe'};C.turn(s,id,false,{targetId:'reserve'});}
    if(move.power)assert.ok(e.hp<10000,`${id} did not damage its target`);
    if(move.switch)assert.equal(s.run.activeId,'reserve',`${id} did not hand off`);
    if(move.cleanse)assert.equal(p.status.burn,undefined,`${id} did not cleanse`);
    if(move.shield)assert.ok(p.shield>0||p.status.shield||move.duration===1,`${id} did not shield`);
    if(move.bondBonus)assert.ok(s.run.battle.bondBonuses[e.id]>0,`${id} did not establish a bond bonus`);
    if(move.teamShield&&!move.switch)assert.ok(move.expansion===3?reserve.shield:s.run.battle.pendingRally?.shield,`${id} did not prepare a team shield`);
  }
});
