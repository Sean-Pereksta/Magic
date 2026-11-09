const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
const clone=x=>JSON.parse(JSON.stringify(x));
function load(){
  const ctx=vm.createContext({console});ctx.window=ctx;
  for(const name of ['catalog','titans','progression','legacy','living-combat','ancient-artifacts','provisions','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),ctx);
  return {V:ctx.Provisions,E:ctx.GameEngine,L:ctx.Legacy,P:ctx.Progression};
}
function fixture(pockets=0,alchemy=0){
  const {V}=load();
  const r={phase:'fight',round:12,hp:100,maxHp:200,shield:0,venom:3,ailments:{burn:3,shock:2,chill:1,corrosion:0,snare:0,hex:0},draft:[],active:[{cd:3},{cd:.5}],abilityTimers:{spell:6},legacy:{ranks:{provision_pockets:pockets,field_alchemy:alchemy}},enemies:[{uid:1,id:'a',lane:0,hp:300,maxHp:300,freeze:0,attackProgress:.9},{uid:2,id:'b',lane:1,hp:20,maxHp:100,freeze:0,attackProgress:.5}]};
  V.start(r);return {V,r};
}
test('starting supplies and quality follow the expedition snapshot without taking relic slots',()=>{
  const {V,r}=fixture(4,3);
  assert.equal(V.count(r),6);assert.equal(V.capacity(r),14);assert.equal(V.quality(r),1.6);
  assert.equal(r.provisions.counts.moss_tonic,2);
  assert.equal(V.summary(r).items.length,5);
  for(const d of Object.values(V.definitions)){assert.match(V.icon(d.id),/^<svg/);assert.ok(V.icon(d.id).includes('<path'));assert.ok(d.description);}
});
test('tonic heals and cleanses, ward respects its cap, and shared cooldown uses simulation time',()=>{
  const {V,r}=fixture(0,1),ev=[];
  assert.equal(V.use(r,'moss_tonic',ev).ok,true);
  assert.equal(r.hp,167.2);assert.equal(r.venom,0);assert.equal(Object.values(r.ailments).some(Boolean),false);
  assert.equal(r.provisions.counts.moss_tonic,0);assert.equal(ev[0].kind,'consumable');
  const before=clone(r);assert.equal(V.use(r,'bark_ward',ev).ok,false);assert.deepEqual(clone(r),before);
  V.tick(r,.5);assert.equal(r.provisions.cooldown,.75);V.tick(r,1);assert.equal(r.provisions.cooldown,0);
  r.shield=75;assert.equal(V.use(r,'bark_ward',ev).ok,true);assert.equal(r.shield,80);
});
test('bomb, freeze and recharge change their intended combat state only',()=>{
  const {V,r}=fixture(4,3),ev=[];
  const before=clone(r.enemies);assert.equal(V.use(r,'ember_bomb',ev).ok,true);
  assert.ok(r.enemies[0].hp<before[0].hp);assert.equal(r.enemies[1].hp,0);
  assert.ok(ev.every(e=>e.kind==='consumable'&&e.pattern==='ember_bomb'&&e.targetUid));
  V.tick(r,2);assert.equal(V.use(r,'frost_bell',ev).ok,true);
  assert.ok(Math.abs(r.enemies[0].freeze-2.88)<1e-9);assert.ok(Math.abs(r.enemies[0].attackProgress-.7)<1e-9);
  assert.equal(r.enemies[1].freeze,0);
  V.tick(r,2);assert.equal(V.use(r,'quicksilver',ev).ok,true);
  assert.ok(Math.abs(r.active[0].cd-.44)<1e-9);assert.equal(r.active[1].cd,0);assert.ok(Math.abs(r.abilityTimers.spell-2.16)<1e-9);
  assert.equal(r.hp,100);assert.equal(r.shield,0);
});
test('empty, inappropriate and dead-run uses never spend a supply',()=>{
  const {V,r}=fixture(4),check=(id)=>{const before=clone(r);assert.equal(V.use(r,id,[]).ok,false);assert.deepEqual(clone(r),before);};
  check('unknown');r.phase='walk';check('ember_bomb');check('frost_bell');
  r.hp=r.maxHp;r.venom=0;for(const key of Object.keys(r.ailments))r.ailments[key]=0;check('moss_tonic');
  r.shield=r.maxHp*.4;check('bark_ward');
  r.active.forEach(it=>it.cd=0);r.abilityTimers.spell=0;check('quicksilver');
  r.phase='dead';check('bark_ward');
});
test('wave supply finds are seeded, drought bounded and satchel capacity never overflows',()=>{
  const {V,r}=fixture(),ev=[];
  for(let i=0;i<3;i++){V.afterWave(r,false,()=>.99,ev);assert.equal(V.count(r),2);}
  V.afterWave(r,false,()=>.99,ev);assert.equal(V.count(r),3);assert.equal(r.provisions.counts.quicksilver,1);assert.equal(r.provisions.dryWaves,0);
  while(V.count(r)<V.capacity(r))V.afterWave(r,true,()=>0,ev);
  const counts=clone(r.provisions.counts);V.afterWave(r,true,()=>0,ev);
  assert.deepEqual(clone(r.provisions.counts),counts);assert.equal(r.provisions.lastFound.outcome,'full');
  assert.equal(ev.at(-1).kind,'supply-find');assert.equal(r.provisions.found,4);
});
test('supply saves validate bounded counts/timing, strip display fields and migrate without refilling',()=>{
  const {V,r}=fixture(4,2);V.use(r,'bark_ward',[]);
  const raw=clone(r.provisions);raw.untrusted='extra';raw.counts.fake=1;assert.throws(()=>V.validate(raw,r));delete raw.counts.fake;
  const saved=V.validate(raw,r);assert.equal(saved.untrusted,undefined);assert.deepEqual(clone(saved),clone(r.provisions));
  for(const edit of [s=>s.cooldown=Infinity,s=>s.cooldown=2,s=>s.counts.moss_tonic=99,s=>s.counts.moss_tonic=.5,s=>s.dryWaves=4,s=>s.lastFound={id:'ember_bomb',round:99,outcome:'packed'}]){const corrupt=clone(r.provisions);edit(corrupt);assert.throws(()=>V.validate(corrupt,r));}
  const migrated=V.validate(undefined,r);assert.equal(Object.values(migrated.counts).reduce((a,b)=>a+b,0),0);
});
test('camp purchases snapshot more and stronger starting supplies, persist and refund exactly',()=>{
  const {E,L,V}=load(),p=E.freshProfile();p.deaths=1;p.points=1000;L.ensure(p);const original=p.points;
  for(let i=0;i<4;i++)assert.equal(L.purchase(p,'provision_pockets').ok,true);
  for(let i=0;i<3;i++)assert.equal(L.purchase(p,'field_alchemy').ok,true);
  assert.equal(L.purchase(p,'field_alchemy').ok,false);
  let saved=E.validate(clone(p));assert.equal(L.rank(saved,'provision_pockets'),4);assert.equal(L.rank(saved,'field_alchemy'),3);
  assert.equal(E.start(saved).ok,true);assert.equal(V.count(saved.run),6);assert.equal(V.quality(saved.run),1.6);
  saved=E.validate(clone(saved));assert.equal(V.count(saved.run),6);
  saved.run=null;assert.equal(L.respec(saved).ok,true);assert.equal(saved.points,original);assert.equal(L.rank(saved,'field_alchemy'),0);
});
test('engine supply kill is rewarded immediately and remains valid while combat is paused',()=>{
  const {E,V}=load(),p=E.freshProfile();E.start(p);const r=p.run;
  E.tick(p,3.3);assert.equal(r.phase,'fight');
  r.provisions.counts.ember_bomb=1;r.enemies[0].hp=1;
  const beforeTime=r.time,result=E.act(p,'useConsumable','ember_bomb');
  assert.equal(result.ok,true);assert.equal(r.foes,1);assert.equal(r.kills,1);assert.equal(r.time,beforeTime);
  assert.ok(result.events.some(e=>e.kind==='kill'));assert.equal(r.phase,'walk');
  const saved=E.validate(clone(p));assert.equal(V.count(saved.run),V.count(r));assert.equal(saved.run.kills,1);
});

test('a supply kill crossing the XP threshold offers its talent immediately without a simulation tick',()=>{
  const {E}=load(),p=E.freshProfile();E.start(p);const r=p.run;
  E.tick(p,3.3);r.xp=E.xpNeeded(r.level)-.1;r.provisions.counts.ember_bomb=1;r.enemies[0].hp=1;
  const beforeTime=r.time,result=E.act(p,'useConsumable','ember_bomb');
  assert.equal(result.ok,true);assert.equal(r.level,2);assert.equal(r.draft.length,3);assert.equal(r.time,beforeTime);
  assert.ok(result.events.some(e=>e.kind==='kill'));assert.ok(result.events.some(e=>e.kind==='level'&&e.label==='Level 2'));
  const saved=E.validate(clone(p));assert.equal(saved.run.level,2);assert.deepEqual(clone(saved.run.draft),clone(r.draft));
  assert.equal(E.act(saved,'chooseUpgrade',saved.run.draft[0]).ok,true);assert.ok(E.validate(clone(saved)));
});
