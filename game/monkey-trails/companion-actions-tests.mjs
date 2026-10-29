import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as A from './companion-actions.mjs';
function fixture(){
  const s=C.newSave('Quick actions','capuchin','practice',74);
  for(let i=0;i<3;i++){const p=structuredClone(C.newSave('Reserve',i===0?'langur':'macaque','practice',i+1).run.monkey);p.id=`reserve-${i}`;s.collection[p.id]=p;}
  return s;
}
test('level-up readiness requires all actual XP and fruit, and clears at max level or knockout',()=>{
  const s=fixture(),p=s.run.monkey,cost=C.trainingCost(p);p.training=cost.xp;s.run.fruit=cost.fruit-1;
  assert.equal(A.upgradeReadiness(s,p.id).train,false);s.run.fruit++;p.training--;assert.equal(A.upgradeReadiness(s,p.id).train,false);
  p.training++;assert.equal(A.upgradeReadiness(s,p.id).train,true);C.train(s,p.id);assert.equal(p.level,2);assert.equal(A.upgradeReadiness(s,p.id).train,false);
  p.level=C.CONFIG.maxLevel;p.training=99999;s.run.fruit=99999;assert.equal(A.upgradeReadiness(s,p.id).train,false);p.level=1;p.hp=0;assert.equal(A.upgradeReadiness(s,p.id).train,false);
});
test('evolution readiness requires the current level and every required essence of a valid path',()=>{
  const s=fixture(),p=s.run.monkey;p.training=9999;for(const type of Object.keys(C.ELEMENTS))s.run.essence[type]=999;
  p.level=2;assert.equal(A.upgradeReadiness(s,p.id).evolve,false);p.level=3;
  const info=A.upgradeReadiness(s,p.id);assert.equal(info.evolve,true);assert.ok(info.elements.includes('leaf'));
  for(const type of Object.keys(s.run.essence))s.run.essence[type]=0;const offer=C.evolutionCost(s,'leaf',p.id);s.run.essence.leaf=offer.cost.leaf-1;
  assert.equal(A.upgradeReadiness(s,p.id).evolve,false);s.run.essence.leaf++;assert.equal(A.upgradeReadiness(s,p.id).evolve,true);C.evolve(s,'leaf',p.id);assert.equal(p.stage,1);
  p.hp=0;assert.equal(A.upgradeReadiness(s,p.id).evolve,false);
});
test('both ready states can coexist and checking them changes neither progress nor RNG',()=>{
  const s=fixture(),p=s.run.monkey;p.level=3;p.training=C.trainingCost(p).xp;s.run.fruit=C.trainingCost(p).fruit;s.run.essence.leaf=6;
  const before=structuredClone(s),info=A.upgradeReadiness(s,p.id);assert.equal(info.train,true);assert.equal(info.evolve,true);assert.deepEqual(s,before);
});
test('Sanctuary companions offer Add and become upgrade-ready only after joining the lineup',()=>{
  const s=fixture(),p=s.collection['reserve-0'];p.training=C.trainingCost(p).xp;s.run.fruit=C.trainingCost(p).fruit;
  assert.equal(A.companionAction(s,p.id).label,'Add to lineup');assert.equal(A.upgradeReadiness(s,p.id).train,false);assert.equal(A.upgradeReadiness(s,s.run.activeId).train,false);
  assert.throws(()=>C.train(s,p.id),/expedition companion/);
  A.addLineupCompanion(s,p.id);assert.equal(A.companionAction(s,p.id).label,'Swap');assert.equal(A.upgradeReadiness(s,p.id).train,true);C.train(s,p.id);assert.equal(p.level,2);
});
test('fully funded upgrades show ready only in phases where both actions can execute',()=>{
  const s=fixture(),p=s.run.monkey;p.level=3;p.training=9999;s.run.fruit=9999;
  for(const type of Object.keys(C.ELEMENTS))s.run.essence[type]=999;
  for(const phase of ['battle','replacement','befriend','dead']){
    s.run.phase=phase;const before=structuredClone(s);
    assert.deepEqual(A.upgradeReadiness(s,p.id),{train:false,evolve:false,elements:[]});
    assert.throws(()=>C.train(s,p.id),/between encounters/);assert.throws(()=>C.evolve(s,'leaf',p.id),/unavailable/);assert.deepEqual(s,before);
  }
  for(const phase of ['trail','camp']){
    s.run.phase=phase;const ready=A.upgradeReadiness(s,p.id);assert.equal(ready.train,true);assert.equal(ready.evolve,true);
    const training=structuredClone(s),evolution=structuredClone(s);C.train(training,p.id);C.evolve(evolution,ready.elements[0],p.id);
    assert.equal(training.collection[p.id].level,4);assert.equal(evolution.collection[p.id].stage,1);
  }
});
test('fully funded Sanctuary evolutions stay hidden until the monkey is an active teammate',()=>{
  const s=fixture(),p=s.collection['reserve-0'];p.level=3;p.training=9999;s.run.fruit=9999;
  for(const type of Object.keys(C.ELEMENTS))s.run.essence[type]=999;
  assert.deepEqual(A.upgradeReadiness(s,p.id),{train:false,evolve:false,elements:[]});assert.throws(()=>C.evolve(s,'leaf',p.id),/unavailable/);
  A.addLineupCompanion(s,p.id);assert.equal(A.upgradeReadiness(s,p.id).evolve,true);
});
test('evolution readiness requires every native element and clears at the final stage',()=>{
  const s=fixture(),species=Object.values(C.SPECIES).find(p=>p.types.length===2),p=structuredClone(s.run.monkey);
  p.id='hybrid';p.species=species.id;p.types=[...species.types];p.level=3;p.training=9999;s.collection[p.id]=p;C.setParty(s,[s.run.activeId,p.id]);
  const type=species.types[0],offer=C.evolutionCost(s,type,p.id);assert.ok(offer);assert.equal(Object.keys(offer.cost).length,2);
  for(const [element,amount]of Object.entries(offer.cost))s.run.essence[element]=amount;
  const second=species.types[1];s.run.essence[second]--;assert.equal(A.upgradeReadiness(s,p.id).evolve,false);
  s.run.essence[second]++;assert.equal(A.upgradeReadiness(s,p.id).evolve,true);C.evolve(s,type,p.id);
  p.stage=3;p.level=60;for(const element of Object.keys(C.ELEMENTS))s.run.essence[element]=9999;
  assert.equal(A.upgradeReadiness(s,p.id).evolve,false);assert.equal(A.upgradeReadiness(s,p.id).train,false);
});
test('Swap replaces exactly the chosen slot and preserves every individual injury, ability and equipment',()=>{
  const s=fixture();C.setParty(s,[s.run.activeId,'reserve-0','reserve-1']);s.collection['reserve-0'].hp=14;s.collection['reserve-2'].hp=9;s.collection['reserve-2'].energy=1;
  const before=structuredClone(s.collection),party=[...s.run.party];A.replaceLineupCompanion(s,'reserve-0','reserve-2');
  assert.deepEqual(s.run.party,[party[0],'reserve-2',party[2]]);assert.deepEqual(s.collection,before);assert.equal(s.collection['reserve-0'].hp,14);assert.equal(s.collection['reserve-2'].hp,9);
});
test('full-lineup Add requires a replacement and never duplicates or removes a companion',()=>{
  const s=fixture();C.setParty(s,[s.run.activeId,'reserve-0','reserve-1']);const before=structuredClone(s);
  assert.throws(()=>A.addLineupCompanion(s,'reserve-2'),/replace/);assert.deepEqual(s,before);assert.equal(A.companionAction(s,'reserve-2').full,true);
  assert.throws(()=>A.replaceLineupCompanion(s,'reserve-0','reserve-1'),/already/);assert.deepEqual(s,before);
  A.replaceLineupCompanion(s,'reserve-0','reserve-2');assert.equal(s.run.party.length,3);assert.equal(Object.keys(s.collection).length,4);
});
test('lineup actions work anywhere between encounters and replace fallen outgoing monkeys without healing',()=>{
  const s=fixture();s.run.distance=10000;s.run.encounters=5;assert.equal(A.lineupPermission(s).allowed,true);A.addLineupCompanion(s,'reserve-0');s.collection['reserve-0'].hp=0;
  assert.equal(A.lineupPermission(s).allowed,true);assert.equal(A.companionAction(s,'reserve-0').allowed,true);
  A.replaceLineupCompanion(s,'reserve-0','reserve-1');assert.equal(s.collection['reserve-0'].hp,0);
  assert.equal(A.companionAction(s,'reserve-0').allowed,true);A.addLineupCompanion(s,'reserve-0');assert.equal(s.collection['reserve-0'].hp,0);C.setParty(s,[s.run.activeId,'reserve-1']);
  for(const phase of ['battle','replacement','befriend','dead']){s.run.phase=phase;const before=structuredClone(s);assert.equal(A.lineupPermission(s).allowed,false);assert.throws(()=>A.addLineupCompanion(s,'reserve-2'),/between encounters/);assert.deepEqual(s,before);}
});

test('knocked-out companions can join, but each resulting lineup keeps one conscious monkey',()=>{
  const s=fixture(),lead=s.run.activeId,a=s.collection['reserve-0'],b=s.collection['reserve-1'],c=s.collection['reserve-2'];a.hp=b.hp=c.hp=0;
  A.addLineupCompanion(s,a.id);A.addLineupCompanion(s,b.id);
  assert.equal(a.hp,0);assert.equal(b.hp,0);assert.equal(A.companionAction(s,c.id).allowed,true);
  assert.equal(A.lineupChoice(s,c.id,lead).allowed,false);assert.equal(A.lineupChoice(s,c.id,a.id).allowed,true);
  const before=structuredClone(s);assert.throws(()=>A.replaceLineupCompanion(s,lead,c.id),/at least one conscious/);assert.deepEqual(s,before);
  A.replaceLineupCompanion(s,a.id,c.id);assert.deepEqual(s.run.party,[lead,c.id,b.id]);assert.equal(c.hp,0);
  assert.throws(()=>C.setParty(s,[a.id,b.id,c.id]),/at least one conscious/);
  assert.throws(()=>C.setParty(s,[a.id]),/at least one conscious/);
});
