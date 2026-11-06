const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
function game(){const ctx=vm.createContext({console});ctx.window=ctx;for(const name of ['catalog','legacy','progression','living-combat','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),ctx);return {E:ctx.GameEngine,L:ctx.Legacy,P:ctx.Progression,C:ctx.Catalog};}
function veteran(E,L){const p=E.freshProfile();p.points=1000000;p.best=140;p.deaths=1;L.ensure(p);return p;}
const clone=x=>JSON.parse(JSON.stringify(x));
test('Legacy migrates original ranks, seeds, discoveries and active expedition without reset',()=>{
  const {E,L}=game(),p=E.freshProfile();p.perks={heart:6,claw:4,pace:3};p.points=74;p.best=8;p.deaths=2;E.start(p);const before=clone(p);delete before.legacy;delete before.run.legacy;delete before.run.living;
  const migrated=E.validate(before);assert.equal(migrated.points,74);assert.deepEqual(clone(migrated.perks),{heart:6,claw:4,pace:3});assert.equal(migrated.run.hp,p.run.hp);assert.deepEqual(clone(migrated.run.active),clone(p.run.active));assert.deepEqual(clone(migrated.seenItems),clone(p.seenItems));assert.equal(migrated.legacy.version,2);assert.equal(L.areaUnlocked(migrated,'forge'),true);assert.equal(E.validate(JSON.stringify(migrated)).run.hp,p.run.hp);
});
test('all permanent ranks charge escalating exact costs, cap ranks, and refund the ledger once',()=>{
  const {E,L}=game(),p=veteran(E,L),initial=p.points;let spend=0;
  for(const d of Object.values(L.upgrades).filter(d=>['ranked','mastery'].includes(d.kind))){for(let i=0;i<d.max;i++){const price=L.cost(p,d.id);spend+=price;assert.equal(L.purchase(p,d.id).ok,true,d.id+' rank '+i);}assert.equal(L.cost(p,d.id),null);assert.equal(L.purchase(p,d.id).ok,false);}
  assert.equal(p.points,initial-spend);const saved=E.validate(JSON.stringify(p));assert.equal(saved.points,p.points);assert.equal(L.respec(saved).ok,true);assert.equal(saved.points,initial);assert.equal(L.respec(saved).ok,true);assert.equal(saved.points,initial);assert.equal(Object.values(saved.perks).reduce((a,b)=>a+b,0),0);assert.equal(L.rank(saved,'mastery_fire'),0);
});
test('camp progression prerequisites and every constellation require permanent milestones',()=>{
  const {E,L}=game(),p=E.freshProfile();p.points=100000;L.ensure(p);assert.equal(L.purchase(p,'barkskin').ok,false);p.legacy.stats.expeditions=1;L.ensure(p);assert.equal(L.purchase(p,'barkskin').ok,true);assert.equal(L.purchase(p,'heirloom_temper').ok,false);p.legacy.stats.guardians=3;p.legacy.stats.highestRegion=4;L.ensure(p);
  assert.equal(Object.keys(L.constellations).length,15);for(const el of L.elements)for(let i=0;i<3;i++)assert.equal(L.purchase(p,'mastery_'+el).ok,true);for(const id of Object.keys(L.constellations)){assert.equal(L.purchase(p,id).ok,true,id);assert.equal(L.purchase(p,id).ok,false);}
  assert.equal(p.legacy.constellations.length,15);const loaded=E.validate(JSON.stringify(p));assert.equal(loaded.legacy.constellations.length,15);L.respec(loaded);assert.equal(loaded.legacy.constellations.length,0);assert.equal(loaded.legacy.unlocks.includes('grove'),true);
});
test('Heart of the Forest, mastery capstones and Convergence enforce their requirements',()=>{
  const {E,L}=game(),p=veteran(E,L);assert.equal(L.purchase(p,'heart_of_forest').ok,false);for(let i=0;i<25;i++)L.purchase(p,'heart');assert.equal(L.purchase(p,'heart_of_forest').ok,true);
  assert.equal(L.purchase(p,'ascendant_leaf').ok,false);for(let i=0;i<8;i++)L.purchase(p,'mastery_leaf');assert.equal(L.purchase(p,'ascendant_leaf').ok,true);
  for(const id of ['worldroot','phoenix'])assert.equal(L.purchase(p,'oath_'+id).ok,true);assert.equal(L.selectOaths(p,['worldroot','phoenix']).ok,false);assert.equal(L.purchase(p,'convergence').ok,true);assert.equal(L.selectOaths(p,['worldroot','phoenix']).ok,true);E.start(p);assert.equal(p.run.legacy.oaths.length,2);assert.equal(L.respec(p).ok,false);assert.equal(L.selectOaths(p,[]).ok,false);assert.equal(E.validate(JSON.stringify(p)).run.legacy.oaths.length,2);
});
test('preparation kits preserve discovery, unlock, equipment count and level rules',()=>{
  const {E,L,C}=game(),p=veteran(E,L);p.seenItems=C.starter.slice();for(let i=0;i<4;i++)L.purchase(p,'heirloom_temper');for(let i=0;i<3;i++)L.purchase(p,'second_heirloom');for(let i=0;i<2;i++)L.purchase(p,'starter_satchel');for(let i=0;i<3;i++)L.purchase(p,'mastercraft');assert.equal(L.purchase(p,'heirloom_vault').ok,true);
  const hidden=Object.values(C.items).find(d=>!d.recipe&&!p.seenItems.includes(d.id));assert.equal(L.saveKit(p,0,{name:'invalid',items:[hidden.id]}).ok,false);assert.equal(L.saveKit(p,0,{name:'too big',items:[...C.starter,C.starter[0]]}).ok,false);
  for(let i=0;i<3;i++)assert.equal(L.saveKit(p,i,{name:'Kit '+i,items:C.starter.slice().reverse()}).ok,true);assert.equal(L.selectKit(p,2).ok,true);E.start(p);assert.equal(p.run.active.length,2);assert.equal(p.run.active[0].id,C.starter[1]);assert.equal(p.run.active[0].level,5);assert.equal(p.run.active[1].level,4);assert.equal(p.run.pack.length,5);assert.equal(E.validate(JSON.stringify(p)).run.active[0].level,5);
});
test('starting blessings, five-choice drafts, rerolls, bans and echoes persist without free XP levels',()=>{
  const {E,L}=game(),p=veteran(E,L);for(const id of ['expanded_destiny','expanded_destiny','fate_rerolls','banishing_ink','echo_destiny','first_blessing'])assert.equal(L.purchase(p,id).ok,true,id);E.start(p);let r=p.run;assert.equal(r.level,1);assert.equal(r.draft.length,5);assert.equal(r.draftIsBonus,true);const echo=r.draft[0];assert.equal(E.act(p,'echoTalent',echo).ok,true);assert.equal(E.act(p,'rerollDraft').ok,true);assert.equal(r.draftRerolls,0);const banned=r.draft[0];assert.equal(E.act(p,'banishTalent',banned).ok,true);assert.equal(r.draft.includes(banned),false);E.act(p,'chooseUpgrade',r.draft[0]);assert.equal(r.level,1);assert.equal(r.bonusTalentsChosen,1);r.xp=E.xpNeeded(1);E.tick(p,.01);assert.equal(r.level,2);assert.equal(r.draft.length,5);assert.equal(E.validate(JSON.stringify(p)).run.draft.length,5);
});
test('save validation rejects forged spend, rank overflow, unknown Oaths and invalid active snapshots',()=>{
  const {E,L}=game(),p=veteran(E,L);L.purchase(p,'barkskin');const corrupt=edit=>{const x=clone(p);edit(x);assert.throws(()=>E.validate(x));};corrupt(x=>x.legacy.spent.barkskin++);corrupt(x=>x.legacy.masteries.leaf=9);corrupt(x=>x.legacy.oaths.push('fake'));E.start(p);const active=clone(p);active.run.legacy.ranks.barkskin=25;assert.throws(()=>E.validate(active));
});
test('wave and guardian rewards retain foundation and award one-time additions exactly once',()=>{
  const {E,L}=game(),p=veteran(E,L);p.legacy.stats.regions=[];p.legacy.stats.achievements=[];E.start(p);const r=p.run;r.region=3;r.round=16;L.guardian(p,r);const reward=r.legacyRewards;assert.ok(reward>=11);L.guardian(p,r);assert.equal(r.legacyRewards,reward);assert.equal(E.awardBreakdown(r).reduce((sum,row)=>sum+row.amount,0),1+r.kills+Math.floor(r.kills/4)+reward);
});
test('finished expedition summaries remain reloadable after health and travel are refunded',()=>{
  const {E,L}=game(),p=veteran(E,L);L.purchase(p,'heart');L.purchase(p,'pace');E.start(p);const r=p.run;for(let i=0;i<20000&&r.phase!=='dead';i++){if(r.draft.length)E.act(p,'chooseUpgrade',r.draft[0]);E.tick(p,.5);}assert.equal(r.phase,'dead');const before=p.points,total=Object.values(p.legacy.spent).reduce((a,b)=>a+b,0);assert.equal(L.respec(p).ok,true);assert.equal(p.points,before+total);const saved=E.validate(JSON.stringify(p));assert.equal(saved.run.maxHp,102);assert.equal(saved.run.paceChoice,2);assert.equal(saved.points,p.points);E.start(saved);assert.equal(saved.run.maxHp,88);assert.equal(saved.run.paceChoice,1);
});
test('permanent Oaths and masteries survive repeated live combat save checkpoints',()=>{
  const {E,L,C}=game();
  for(const oath of Object.keys(L.oaths)){let p=veteran(E,L);for(const el of L.elements)for(let i=0;i<3;i++)L.purchase(p,'mastery_'+el);L.purchase(p,'oath_'+oath);L.purchase(p,'barkskin');L.purchase(p,'medicinal_sap');L.purchase(p,'regenerative_roots');L.purchase(p,'fate_rerolls');E.start(p);p.run.seed=12345678;
    for(let i=0;i<300&&p.run.phase!=='dead';i++){if(p.run.draft.length)E.act(p,'chooseUpgrade',p.run.draft[0]);E.tick(p,.25);if(i%11===0)p=E.validate(JSON.stringify(p));}
    assert.ok(p.run.time>1,oath);assert.equal(p.run.legacy.oaths[0],oath);E.validate(JSON.stringify(p));
  }
});
test('fatal Phoenix revival resolves its kills and rewards before an immediate save',()=>{
  const {E,L,C}=game(),p=veteran(E,L);L.purchase(p,'oath_phoenix');E.start(p);E.tick(p,3.3);const r=p.run;assert.equal(r.phase,'fight');r.hp=1;r.shield=0;r.enemies[0].attackProgress=.999;for(const it of r.active)it.cd=C.items[it.id].cooldown;const events=E.tick(p,.1);assert.ok(r.hp>0);assert.ok(events.some(e=>e.kind==='kill'));assert.equal(r.phase,'walk');assert.equal(r.kills,1);assert.equal(r.deathCause,null);assert.equal(E.validate(JSON.stringify(p)).run.kills,1);
});
