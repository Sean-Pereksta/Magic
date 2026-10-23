import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {snapshotTeam,newDuel,newRaidAttempt} from './social-combat.mjs';

function crew(){
  const s=C.newSave('Opening crew','capuchin','practice',38);
  for(const [index,species] of ['langur','macaque','mandrill'].entries()){
    const p=structuredClone(C.newSave('Companion',species,'practice',index+1).run.monkey);p.id=`companion-${index}`;s.collection[p.id]=p;
  }
  C.setParty(s,[s.run.activeId,'companion-0','companion-1']);return s;
}
function opening(){const s=crew();C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);return s;}

test('opening choice preserves injuries, equipment, supplies, party order, RNG and round',()=>{
  const s=opening(),p=s.collection['companion-0'];p.hp=17;p.energy=2;p.status.poison=2;p.shield=6;
  const before=structuredClone(s),selected=C.selectBattleLead(s,p.id);
  assert.equal(selected,p);assert.equal(s.run.activeId,p.id);assert.equal(s.run.monkey,p);
  assert.deepEqual(s.collection,before.collection);assert.deepEqual(s.run.party,before.run.party);
  assert.deepEqual(s.run.battle,before.run.battle);assert.deepEqual(s.run.items,before.run.items);
  assert.equal(s.run.fruit,before.run.fruit);assert.equal(s.rng,before.rng);assert.equal(s.run.battle.round,1);
});
test('only a conscious current teammate can be selected, with invalid choices atomic',()=>{
  const s=opening();s.collection['companion-1'].hp=0;
  for(const id of ['companion-1','companion-2','missing']){
    const before=structuredClone(s);assert.throws(()=>C.selectBattleLead(s,id),/conscious monkey/);assert.deepEqual(s,before);
  }
});
test('unstarted selection survives encrypted-save validation and still allows choosing a lead',()=>{
  let s=opening();C.selectBattleLead(s,'companion-0');s=C.validate(structuredClone(s));
  assert.equal(s.run.activeId,'companion-0');assert.equal(C.canChooseBattleLead(s),true);
  C.selectBattleLead(s,'companion-1');C.readyBattle(s);assert.equal(s.run.activeId,'companion-1');assert.equal(s.run.battle.round,1);
});
test('confirming locks the opening and reload cannot turn it into a free combat switch',()=>{
  let s=opening();C.selectBattleLead(s,'companion-0');C.readyBattle(s);
  assert.equal(C.canChooseBattleLead(s),false);assert.throws(()=>C.selectBattleLead(s,s.run.party[0]),/before battle/);
  s=C.validate(structuredClone(s));s.run.battle.ready=false;
  assert.equal(C.canChooseBattleLead(s),false);assert.throws(()=>C.selectBattleLead(s,'companion-1'),/before battle/);
  const rng=s.rng;C.readyBattle(s);assert.equal(s.rng,rng);assert.equal(s.run.activeId,'companion-0');
});
test('the chosen companion takes the opening action without consuming an extra round',()=>{
  const s=opening();C.selectBattleLead(s,'companion-0');s.run.battle.enemy.loadout=['breathe'];s.run.battle.enemy.energy=0;s.run.battle.enemy.hp=999;
  C.readyBattle(s);assert.equal(s.run.battle.intent,'breathe');C.turn(s,'strike');
  assert.equal(s.run.activeId,'companion-0');assert.equal(s.run.battle.round,2);
  assert.ok(s.run.battle.contributions['companion-0'].damage>0);assert.equal(s.run.battle.contributions[s.run.party[0]],undefined);
  assert.throws(()=>C.selectBattleLead(s,'companion-1'),/before battle/);
});
test('legacy ongoing battles and forced replacements retain normal switch restrictions',()=>{
  let s=opening();delete s.run.battle.openingLocked;delete s.run.battle.openingLeadId;s=C.validate(structuredClone(s));s.run.battle.ready=false;
  assert.equal(C.canChooseBattleLead(s),false);assert.throws(()=>C.selectBattleLead(s,'companion-0'),/before battle/);
  s.run.phase='replacement';assert.throws(()=>C.selectBattleLead(s,'companion-0'),/before battle/);
});
test('choosing the next starter while exploring changes no health or party composition',()=>{
  const s=crew(),before=structuredClone(s.collection),party=[...s.run.party];C.selectBattleLead(s,'companion-0');
  assert.deepEqual(s.collection,before);assert.deepEqual(s.run.party,party);C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);
  assert.equal(s.run.activeId,'companion-0');assert.equal(C.canChooseBattleLead(s),true);
});
test('friendly duel and tribe boss copies start with the selected monkey and preserve expedition injuries',()=>{
  const s=crew();s.collection['companion-0'].hp=9;s.collection['companion-0'].energy=1;C.selectBattleLead(s,'companion-0');
  const before=structuredClone(s),snapshot=snapshotTeam(s);assert.equal(snapshot[0].id,'companion-0');assert.equal(snapshot[0].hp,snapshot[0].maxHp);
  const opponent=snapshotTeam(C.newSave('Rival','howler','practice',9));
  const duel=newDuel({id:'alice',name:'Alice',team:snapshot},{id:'bob',name:'Bob',team:opponent});
  assert.equal(duel.teams[0][duel.active[0]].id,'companion-0');
  const raid=newRaidAttempt(snapshot,1,21);assert.equal(raid.team[raid.active].id,'companion-0');assert.deepEqual(s,before);
});
