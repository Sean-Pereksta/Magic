const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
function game(titans=true){const c=vm.createContext({console});c.window=c;for(const name of ['catalog',...(titans?['titans']:[]),'legacy','progression','living-combat','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);return {C:c.Catalog,E:c.GameEngine,T:c.Titans};}
function spawn(round=10,titans=true){const g=game(titans),{E}=g,p=E.freshProfile();E.start(p);const r=p.run;r.round=round;r.region=E.routeInfo(r).region;r.kills=r.foes=round-1;p.best=round-1;r.walkLeft=.001;const ev=E.tick(p,.01);return {...g,p,r,ev,e:r.enemies[0]};}
const plain=x=>JSON.parse(JSON.stringify(x));

test('each tenth round has one enormous distinct titan and two escorts, including guardian overlaps',()=>{
  const ids=new Set();
  for(let round=1;round<=120;round++){
    const {C,T,r,e,ev}=spawn(round);
    if(round%10){assert.ok(!r.enemies.some(e=>e.titanId));continue;}
    assert.equal(r.enemies.length,3);assert.equal(r.enemies.filter(e=>e.titanId).length,1);assert.equal(r.enemies.filter(e=>e.boss).length,1);
    assert.equal(e.id,T.bossForRound(round).id);assert.equal(e.titan.phase,1);assert.ok(e.maxHp>r.enemies[1].maxHp*4);assert.ok(ev.some(e=>e.pattern==='titan-arrival'));
    assert.ok(C.enemies.find(d=>d.id===e.id).moves.every(m=>m.name&&m.description&&m.pattern));
    if(round<=60)ids.add(e.id);
  }
  assert.equal(ids.size,6);const a=spawn(10),b=spawn(70);assert.equal(a.e.id,b.e.id);assert.ok(b.e.maxHp>a.e.maxHp*10);assert.ok(b.e.damage>a.e.damage*8);
});

test('wind-ups precede damage, pause under control, and survive save/reload exactly',()=>{
  const {E,T,p,r,e}=spawn();r.active.forEach(it=>it.cd=1);const ev=[];let damage=0;
  T.advance(r,e,3.2,ev,{damage:n=>{damage+=n;return n;}});
  assert.ok(e.titan.warning);assert.equal(damage,0);assert.equal(ev.at(-1).pattern,'titan-warning');
  const saved=E.validate(JSON.stringify(p));assert.deepEqual(plain(saved.run.enemies[0].titan),plain(e.titan));
  e.freeze=2;const before=e.titan.warning.left;E.tick(p,.1);assert.equal(e.titan.warning.left,before);
  e.freeze=0;T.advance(r,e,1.79,ev,{damage:n=>{damage+=n;return n;}});assert.equal(damage,0);
  T.advance(r,e,.02,ev,{damage:n=>{damage+=n;return n;}});assert.ok(damage>0);assert.equal(e.titan.warning,null);assert.equal(e.titan.casts,1);
  assert.ok(ev.some(e=>e.pattern==='titan-cast'&&e.abilityPattern==='titan-thorns'));assert.equal(r.ailments.snare,3);
  const bad=plain(saved);bad.run.enemies[0].titan.warning.left=99;assert.throws(()=>E.validate(bad));
});

test('all six titans resolve two distinct powers, enrage once, and respect counter gear',()=>{
  for(let round=10;round<=60;round+=10){
    const {C,T,r,e}=spawn(round),ev=[];r.hp=r.maxHp=10000;r.shield=4000;e.hp=e.maxHp*.4;
    const damage=(n)=>{const hit=Math.min(r.hp,n);r.hp-=hit;return hit;};
    T.advance(r,e,.1,ev,{damage});assert.equal(e.titan.phase,2);assert.equal(ev.filter(v=>v.pattern==='titan-enrage').length,1);
    for(let move=0;move<2;move++){e.titan.warning={move,left:.01,duration:1.35};T.advance(r,e,.02,ev,{damage});}
    const casts=ev.filter(v=>v.pattern==='titan-cast');assert.equal(casts.length,2);assert.notEqual(casts[0].abilityPattern,casts[1].abilityPattern);assert.ok(r.hp<10000);assert.ok(e.hp>=e.maxHp*.4);
    T.advance(r,e,.1,ev,{damage});assert.equal(ev.filter(v=>v.pattern==='titan-enrage').length,1);
    if(round===10)assert.equal(r.ailments.snare,3);
    if(round===20)assert.ok(r.ailments.burn>0);
    if(round===30)assert.ok(r.ailments.chill>0);
    if(round===40)assert.equal(r.ailments.shock,.8);
    if(round===50)assert.equal(r.ailments.corrosion,5);
    if(round===60){assert.equal(r.ailments.hex,4);assert.ok(r.shield<4000);}
    const counter={10:'unbinding',20:'blastguard',30:'thaw',40:'insulated',50:'purify',60:'purify'}[round],gear=Object.values(C.items).find(d=>d.counter===counter);assert.ok(gear,counter);
    r.active=[{id:gear.id}];for(const key of Object.keys(r.ailments))r.ailments[key]=0;
    e.titan.warning={move:0,left:.01,duration:1.35};T.advance(r,e,.02,[],{damage});
    if(round===10)assert.equal(r.ailments.snare,0);
    if(round===20)assert.ok(r.ailments.burn<e.damage*.7);
    if(round===30)assert.equal(r.ailments.chill,0);
    if(round===40)assert.equal(r.ailments.shock,0);
    if(round===50)assert.equal(r.ailments.corrosion,0);
    if(round===60)assert.equal(r.ailments.hex,0);
  }
});

test('titan deaths guarantee an unlocked base Mythic once, and caches reload while escorts remain',()=>{
  const {C,E,p,r,e}=spawn();r.active.forEach(it=>it.cd=C.items[it.id].cooldown);e.hp=.01;e.poison=1;e.statusClock=.99;
  const ev=E.tick(p,.04),finds=ev.filter(v=>v.kind==='loot'&&v.enemyId===e.id);
  assert.equal(finds.length,1);const d=C.items[finds[0].id];assert.equal(d.rarity,'mythic');assert.ok(!d.recipe&&!d.forgeOnly);assert.ok(d.unlockRound<=10);
  assert.equal(ev.filter(v=>v.pattern==='titan-defeated').length,1);assert.equal(r.phase,'fight');assert.equal(r.enemies.length,2);
  const saved=E.validate(JSON.stringify(p));assert.equal(saved.run.lastDrop.enemyId,e.id);assert.ok(saved.run.pack.some(it=>it.id===d.id));
  const more=E.tick(saved,.05);assert.ok(!more.some(v=>v.pattern==='titan-defeated'));
  for(const other of saved.run.enemies){other.hp=.01;other.poison=1;other.statusClock=.99;}E.tick(saved,.04);
  assert.equal(saved.run.round,11);assert.equal(saved.run.phase,'walk');assert.equal(saved.run.guardians,1);assert.ok(E.validate(JSON.stringify(saved)));
});

test('saved pre-titan round-ten encounters finish with their original foes and rules',()=>{
  const {C,p,r}=spawn(10,false),{E}=game();r.waveRules=2;
  // Pre-titan four-lane encounters remain valid with the expansion loaded.
  const saved=E.validate(JSON.stringify(p));assert.equal(saved.run.round,10);assert.equal(saved.run.waveRules,2);assert.ok(saved.run.enemies.every(e=>!e.titanId));
  for(const other of saved.run.enemies){other.hp=.01;other.poison=1;other.statusClock=.99;}saved.run.active.forEach(it=>it.cd=C.items[it.id].cooldown);E.tick(saved,.04);
  assert.equal(saved.run.round,11);assert.equal(saved.run.waveRules,3);
});
