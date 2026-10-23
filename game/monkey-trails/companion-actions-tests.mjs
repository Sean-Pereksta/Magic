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
test('Sanctuary individuals have their own readiness and offer Add instead of Swap',()=>{
  const s=fixture(),p=s.collection['reserve-0'];p.training=C.trainingCost(p).xp;s.run.fruit=C.trainingCost(p).fruit;
  assert.equal(A.companionAction(s,p.id).label,'Add to lineup');assert.equal(A.upgradeReadiness(s,p.id).train,true);assert.equal(A.upgradeReadiness(s,s.run.activeId).train,false);
  A.addLineupCompanion(s,p.id);assert.equal(A.companionAction(s,p.id).label,'Swap');C.train(s,p.id);assert.equal(p.level,2);
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
test('lineup actions honor departure, camp, battle and knockout restrictions atomically',()=>{
  const s=fixture();s.run.distance=10;assert.equal(A.lineupPermission(s).allowed,false);const before=structuredClone(s);assert.throws(()=>A.addLineupCompanion(s,'reserve-0'),/earned camp/);assert.deepEqual(s,before);
  s.run.phase='camp';assert.equal(A.lineupPermission(s).allowed,true);A.addLineupCompanion(s,'reserve-0');s.collection['reserve-0'].hp=0;
  assert.equal(A.lineupPermission(s).allowed,false);assert.throws(()=>A.replaceLineupCompanion(s,'reserve-0','reserve-1'),/Revive/);
  s.run.phase='battle';assert.equal(A.lineupPermission(s).allowed,false);
});
