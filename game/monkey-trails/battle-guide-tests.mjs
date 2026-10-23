import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {abilityGuide,damagePreview,actionAvailability,moveSummary,renderAbilityCards,renderAbilityGuide,battleActionHints} from './battle-guide.mjs';
import {configureGuardian,planGuardian} from './guardian-patterns.mjs';
import {newRaidAttempt,raidAction,raidIntent,snapshotTeam} from './social-combat.mjs';

function battle(seed=765){
  const s=C.newSave('Read only guides','macaque','gps',seed),p=s.run.monkey;
  const e=structuredClone(p);e.id='guide-enemy';e.species='cinderpaw';e.types=['fire'];e.hp=1000;e.loadout=['breathe'];e.energy=20;
  s.run.phase='battle';s.run.battle={id:'guide-battle',enemies:[e],enemy:e,enemyActive:0,round:1,turn:1,ready:true,boss:false,ultra:false,style:'balanced',intent:'breathe',enemyAction:{type:'move',id:'breathe'},rewarded:false,bondBonuses:{},pendingRally:null,enemyRally:null};
  return s;
}

test('every guide and card is pure, repeatable and leaves RNG and records untouched',()=>{
  const s=battle(),before=JSON.stringify(s);
  for(const id of Object.keys(C.MOVES)){
    const first=abilityGuide(s,id);assert.deepEqual(abilityGuide(s,id),first);assert.ok(first.summary&&first.description);
    renderAbilityGuide(s,id);
  }
  renderAbilityCards(s);battleActionHints(s);assert.equal(JSON.stringify(s),before);
});
test('normal and critical damage ranges bound the real expedition damage formula',()=>{
  for(let seed=1;seed<=80;seed++){
    const s=battle(seed),p=s.run.monkey;p.learned.push('cinder');p.loadout=['strike','cinder','brace','breathe'];p.types=['fire'];
    if(seed%2){p.status.weaken=2;p.statusPower.weaken=.25;p.prepare=.35;s.run.battle.enemy.shield=7;s.run.battle.enemy.guard=.45;}
    const preview=damagePreview(s,'cinder');C.turn(s,'cinder');
    const hit=s.run.battle.hitFeedback.find(hit=>hit.side==='player');
    assert.ok(hit.damage>=preview.min,`${seed}: min ${preview.min} vs ${hit.damage}`);assert.ok(hit.damage<=(hit.critical?preview.criticalMax:preview.max),`${seed}: max`);
  }
});
test('shield breaks ignore current shields and guard; cleanse clears weakened attack preview',()=>{
  const s=battle();s.run.battle.enemy.shield=1000;s.run.battle.enemy.guard=.3;
  const broken=damagePreview(s,'guardBreak'),normal=damagePreview(s,'strike');assert.ok(broken.min>0);assert.equal(normal.max,0);
  s.run.monkey.status.weaken=2;s.run.monkey.statusPower.weaken=.6;s.run.monkey.prepare=.9;
  const preview=damagePreview(s,'purify'),clean=structuredClone(s);clean.run.monkey.status={};clean.run.monkey.statusPower={};clean.run.monkey.prepare=0;
  assert.deepEqual(preview,damagePreview(clean,'purify'));
});
test('availability explains energy, cooldowns, uses, root and teammate restrictions precisely',()=>{
  const s=battle(),p=s.run.monkey;p.loadout=['strike','heavyPalm','rescueSwing','branchRelay'];p.energy=0;
  assert.match(actionAvailability(s,'heavyPalm').reason,/4 more energy/);p.energy=12;p.cooldowns.heavyPalm=2;assert.match(actionAvailability(s,'heavyPalm').reason,/Ready in 1 round/);
  p.uses.rescueSwing=1;assert.match(actionAvailability(s,'rescueSwing').reason,/No uses/);p.status.root=1;assert.match(actionAvailability(s,'branchRelay').reason,/Cannot switch while rooted/);
  delete p.status.root;assert.match(actionAvailability(s,'branchRelay').reason,/another conscious/);s.run.battle.itemUsedRound=1;assert.match(battleActionHints(s).supply,/Supply already used/);
});
test('guides identify actual elements, uncertainty, current effectiveness, and healing targets',()=>{
  const s=battle();s.run.battle.enemy.species='spider';s.run.battle.enemy.types=['leaf'];
  const g=abilityGuide(s,'cinder');assert.equal(g.elementLabel,'Fire');assert.equal(g.effectiveness.label,'Very effective!');assert.ok(g.damage.notes.some(note=>/dodge/.test(note)));
  assert.match(moveSummary('teamMend'),/conscious teammates/);assert.match(moveSummary('guardianHandoff'),/incoming teammate/);assert.equal(abilityGuide(s,'breathe').damage,null);
  const html=renderAbilityCards(s);assert.equal((html.match(/data-action="move-guide"/g)||[]).length,4);assert.ok(!html.includes('<button disabled class="move"><button'));
});
test('guardian intent is deterministic charge, heavy, shield, recover and changes phase',()=>{
  const s=battle();configureGuardian(s.run.battle,'emerald-jungle');const g=s.run.battle.guardianPattern,expected=['focus',g.heavy,g.shield,'breathe'];
  s.run.battle.enemy.energy=100;for(let i=0;i<4;i++){s.run.battle.round=i+1;const before=JSON.stringify(s),plan=planGuardian(s);assert.equal(plan.id,expected[i]);assert.equal(JSON.stringify(s),before);}
  s.run.battle.enemy.hp=s.run.battle.enemy.guardianMaxHp*.4;s.run.battle.round=3;assert.equal(planGuardian(s).phase,2);assert.equal(planGuardian(s).id,'breathe');
  s.run.battle.round=2;s.run.battle.enemy.energy=0;assert.equal(planGuardian(s).id,'breathe');
  s.run.battle.round=1;s.run.battle.enemy.energy=2;assert.equal(planGuardian(s).id,'breathe','Do not telegraph an unaffordable follow-up attack');
  const legacy=battle();assert.equal(planGuardian(legacy),null);
});
test('opt-in raid ward patterns preserve old tickets and expedition copies',()=>{
  const s=C.newSave('Raid pattern','capuchin','gps',5),original=JSON.stringify(s),members=snapshotTeam(s);
  const legacy=newRaidAttempt(members,3,33,2),pattern=newRaidAttempt(members,3,33,2,1),replay=JSON.parse(JSON.stringify(pattern));
  assert.equal(legacy.guardianPattern,undefined);assert.match(raidIntent(pattern).label,/forming/);
  raidAction(pattern,{type:'move',id:'strike'});raidAction(replay,{type:'move',id:'strike'});assert.deepEqual(pattern,replay);assert.equal(raidIntent(pattern).ward,true);assert.ok(pattern.boss.shield>0);assert.equal(legacy.boss.guard,0);
  for(let turn=1;turn<10;turn++){raidAction(pattern,{type:'move',id:'strike'});raidAction(replay,{type:'move',id:'strike'});assert.deepEqual(pattern,replay);assert.ok(pattern.team.every(p=>p.hp===p.maxHp));}
  assert.equal(pattern.status,'finished');assert.equal(JSON.stringify(s),original);
});

test('every damaging catalogue technique preview bounds the real hit with current effects',()=>{
  for(const [id,move]of Object.entries(C.MOVES).filter(([,move])=>move.power)){
    const s=battle(777),p=s.run.monkey,e=s.run.battle.enemy;p.stage=3;p.level=12;p.types=move.types||[move.type==='neutral'?'fire':move.type];p.loadout=['strike',id].filter((id,index,a)=>a.indexOf(id)===index);p.learned.push(id);p.energy=100;p.hp=C.stats(p).hp;p.prepare=.22;p.status.weaken=2;p.statusPower.weaken=.2;e.shield=12;e.guard=.45;e.hp=10000;
    const reserve=structuredClone(p);reserve.id='preview-reserve';reserve.loadout=['strike'];s.collection[reserve.id]=reserve;s.run.party.push(reserve.id);
    const preview=damagePreview(s,id);C.turn(s,id);const hit=s.run.battle.hitFeedback.find(hit=>hit.side==='player');assert.ok(hit,`${id} produces feedback`);
    assert.ok(hit.damage>=preview.min&&hit.damage<=(hit.critical?preview.criticalMax:preview.max),`${id} damage ${hit.damage} in ${preview.min}–${preview.max}, crit ${preview.criticalMax}`);
  }
});
