const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const plain=value=>JSON.parse(JSON.stringify(value));
function game(){
  const c=vm.createContext({console});c.window=c;
  for(const name of ['catalog','titans','progression','legacy','living-combat','ancient-artifacts','synergies','provisions','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);
  const E=c.GameEngine,p=E.freshProfile();E.start(p);return {C:c.Catalog,E,S:c.Synergies,p,r:p.run};
}
function equip(g,elements,style){
  const {C,r}=g;r.active=elements.map((element,slot)=>{
    const d=Object.values(C.items).find(d=>d.element===element&&!d.recipe&&(style?d.style===style:!['heal','shield','stun','freeze'].includes(d.style)));
    assert.ok(d,'fixture relic '+element+' '+(style||'offense'));return {id:d.id,uid:slot+1,slot,level:1,cd:1};
  });r.pack=[];r.nextUid=r.active.length+1;return r.active.map(it=>C.items[it.id]);
}
function foes(r){r.enemies=Array.from({length:4},(_,i)=>({uid:i+1,id:'probe'+i,lane:i,hp:10000,maxHp:10000,poison:0,stun:0,freeze:0,chill:0}));return r.enemies;}

test('exactly three two-element sets support specialization and two simultaneous pairs',()=>{
  const g=game(),{S,r}=g;assert.equal(S.sets.length,3);
  equip(g,['fire','fire','fire','fire']);assert.ok(S.describe(r).every(d=>!d.active));
  equip(g,['fire','frost','leaf','storm']);let rows=S.describe(r);
  assert.deepEqual(plain(rows.filter(d=>d.active).map(d=>[d.id,d.tier])),[['emberglass',2],['bramble_circuit',2]]);
  equip(g,['fire','frost','frost','stone']);rows=S.describe(r);assert.equal(rows.find(d=>d.id==='emberglass').tier,3);
  assert.ok(rows.every(d=>d.requirement&&d.description&&d.bonus));
  r.pack.push({id:rows[2].elements[1],level:1});assert.equal(S.describe(r).filter(d=>d.active).length,1,'stowed gear cannot activate a set');
});

test('Emberglass trades frost control for a bounded formation shatter',()=>{
  const g=game(),{S,r}=g,[fire]=equip(g,['fire','frost']),enemies=foes(r),target=enemies[0],ev=[];
  target.freeze=3;target.chill=2;S.onActivation(r,target,fire,100,ev);
  assert.equal(target.freeze,0);assert.equal(target.chill,0);
  assert.deepEqual(enemies.map(e=>e.hp),[9965,9965,9965,10000]);assert.equal(enemies[0].stun,.35);
  assert.equal(ev.length,3);assert.ok(ev.every(e=>e.kind==='synergy'&&e.pattern==='emberglass'&&e.secondaryElement==='frost'));
  target.chill=2;S.onActivation(r,target,fire,100,ev);assert.equal(ev.length,3,'secondary or rapid activations cannot loop the reaction');
  S.tick(r,1.5);S.onActivation(r,target,fire,100,ev);assert.equal(ev.length,6);
});

test('three Emberglass pieces improve the reaction without changing support damage',()=>{
  const g=game(),{S,r}=g,[fire]=equip(g,['fire','frost','frost']),enemies=foes(r),target=enemies[0],ev=[];
  target.chill=2;S.onActivation(r,target,fire,100,ev);assert.equal(target.hp,9935);assert.equal(target.stun,.6);
  S.tick(r,1.5);target.chill=2;const before=enemies.map(e=>e.hp);
  S.onActivation(r,target,{element:'fire',style:'heal'},100,ev);assert.deepEqual(enemies.map(e=>e.hp),before);
  S.onActivation(r,target,fire,0,ev);assert.deepEqual(enemies.map(e=>e.hp),before,'zero-damage support cannot create an offensive reaction');
});

test('Bramble Circuit rewards poison preparation and recharges Leaf while spreading capped poison',()=>{
  const g=game(),{S,r}=g,[leaf,storm]=equip(g,['leaf','storm']),enemies=foes(r),ev=[];
  assert.equal(S.hitMultiplier(r,storm,enemies[0]),1);enemies[0].poison=100;
  assert.equal(S.hitMultiplier(r,storm,enemies[0]),1.2);assert.equal(S.hitMultiplier(r,leaf,enemies[0]),1);
  S.onActivation(r,enemies[0],storm,100,ev);assert.equal(enemies[1].poison,40);assert.equal(r.active[0].cd,.7);
  assert.equal(ev.length,1);S.onActivation(r,enemies[0],storm,100,ev);assert.equal(r.active[0].cd,.7);
  equip(g,['leaf','leaf','storm']);S.tick(r,1.5);enemies[0].poison=100000;
  assert.equal(S.hitMultiplier(r,storm,enemies[0]),1.35);S.onActivation(r,enemies[0],storm,100,ev);
  assert.equal(enemies[1].poison,enemies[1].maxHp*.08);assert.ok(Math.abs(r.active[0].cd-.45)<1e-10);
  assert.ok(enemies.every(e=>e.hp===10000),'spreading poison does not recursively deal primary damage');
});

test('Soulstone makes defensive support a recharge engine while retaining the shield cap',()=>{
  const g=game(),{S,r}=g;equip(g,['stone','spirit','stone']);const enemies=foes(r),ev=[];
  r.maxHp=100;r.shield=0;S.onActivation(r,enemies[0],{element:'spirit',style:'heal'},0,ev);assert.equal(ev.length,0);
  S.onActivation(r,enemies[0],{element:'stone',style:'shield'},0,ev);assert.equal(r.shield,4);
  S.onActivation(r,enemies[0],{element:'spirit',style:'heal'},0,ev);assert.ok(Math.abs(r.active[0].cd-.2)<1e-10);assert.equal(r.active[1].cd,1);
  assert.ok(enemies.every(e=>e.hp===10000));assert.equal(ev.length,2);
  S.tick(r,1.5);r.shield=39;S.onActivation(r,enemies[0],{element:'stone',style:'shield'},0,ev);assert.equal(r.shield,40);
});

test('set timers migrate safely, reject invalid data and survive deterministic engine reloads',()=>{
  const {E,S,p,r}=game();r.synergies.timers.emberglass=1.25;r.synergies.timers.bramble_circuit=.8;
  const saved=E.validate(plain(p));assert.deepEqual(plain(saved.run.synergies),plain(r.synergies));
  E.tick(p,.2);E.tick(saved,.2);assert.deepEqual(plain(saved.run.synergies),plain(r.synergies));assert.equal(r.synergies.timers.emberglass,1.05);
  const old=plain(p);delete old.run.synergies;assert.ok(Object.values(E.validate(old).run.synergies.timers).every(n=>n===0));
  for(const value of [-1,2,Infinity,'1']){const bad=plain(p);bad.run.synergies.timers.emberglass=value;assert.throws(()=>E.validate(bad));}
  assert.throws(()=>S.validate({timers:[]}));
});

test('the actual primary activation path triggers a set, and removing its relic disables it immediately',()=>{
  const g=game(),{C,E,p,r}=g;equip(g,['fire','frost']);r.walkLeft=0;E.tick(p,.001);
  const target=r.enemies[0];target.chill=2;r.active[0].cd=0;r.active[1].cd=1;
  const events=E.tick(p,.01);assert.ok(events.some(e=>e.kind==='synergy'&&e.pattern==='emberglass'));
  const frost=r.active[1];assert.equal(E.act(p,'stow',frost.uid).ok,true);assert.equal(g.S.describe(r).find(d=>d.id==='emberglass').active,false);
  assert.ok(E.validate(plain(p)),'reaction state is a valid immediate save');
});
