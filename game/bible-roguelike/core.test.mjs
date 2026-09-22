import test from 'node:test';
import assert from 'node:assert/strict';
import {make,act,command,rules,forceRoom,verses,options} from './test-fixtures.mjs';
import {partyScale,refKey,createRules} from './core.mjs';
import {ABILITIES,VARIANTS,CATEGORY_KEYS} from './content.mjs';

test('party HP is sublinear, boss HP slightly higher; roster uses existing enemies',()=>{
  const solo=make();
  for(let n=1;n<=5;n++){
    const s=make(n);assert.equal(s.enemy.maxHp,Math.round(solo.enemy.maxHp*partyScale(n)));
    assert.ok(n===1||partyScale(n)<n);assert.ok(n===1||partyScale(n,true)<n);
    assert.ok(partyScale(n,true)>=partyScale(n));
    assert.equal(s.players.length,n);assert.ok(options.enemies.some(e=>e.id===s.enemy.id));
  }
  assert.deepEqual([1,2,3,4,5].map(n=>partyScale(n)),[1,1.8,2.6,3.35,4]);
});

test('canonical references claim once for the entire party; invalid attempts do not harm or claim',()=>{
  let s=make(2);s.enemy.hp=s.enemy.maxHp=500;
  const before=structuredClone(s);assert.throws(()=>act(s,'attack',{reference:'Not a verse'}),/not found/);assert.deepEqual(s,before);
  s=act(s,'attack',{reference:'Psalm 23:4'});assert.equal(s.used['psalms 23:4'].name,'Sean');
  assert.throws(()=>act(s,'attack',{playerId:'p1',reference:'Psalms 23 : 4'}),/used by Sean/);
  assert.ok(s.players[0].hp<100);assert.equal(s.players[1].hp,100);
  s=act(s,'attack',{playerId:'p1',reference:'John 3:16'});assert.equal(Object.keys(s.used).length,2);
});

test('unmatched and resisted verses still damage enemies; pure transitions do not mutate input',()=>{
  let s=make();s.enemy.hp=s.enemy.maxHp=500;s.enemy.weaknesses=[{concept:'healing',multiplier:1.3}];s.enemy.archetype='fear';
  const before=structuredClone(s), next=act(s,'attack',{reference:'Genesis 1:1'});
  assert.ok(next.enemy.hp<500);assert.deepEqual(s,before);
  const resisted=act(s,'attack',{reference:'Psalms 23:4'});assert.ok(resisted.enemy.hp<500);
  assert.ok(resisted.events.some(e=>e.text.includes('resisted')));
});

test('one final blow awards rewards once, blocks stale hits, revives allies, resets verse pool next battle',()=>{
  let s=make(2);s.enemy.hp=1;s.players[1].hp=0;
  const action=command(s,'attack',{reference:'John 3:16'});const next=rules.reduce(s,action);
  assert.equal(next.phase,'path');assert.ok(next.players[1].hp>0);assert.ok(next.players[0].gold>s.players[0].gold);
  assert.strictEqual(rules.reduce(next,action),next);
  assert.throws(()=>act(next,'attack',{reference:'John 8:32'}),/ended/);
  const progressed=act(next,'path',{path:'battle'});assert.equal(Object.keys(progressed.used).length,0);
  assert.throws(()=>rules.reduce(progressed,command(next,'attack',{reference:'John 8:32'})),/moved on/);
});

test('downed players cannot act; total defeat ends the run; teammate Second Wind revives',()=>{
  let s=make(2);s.players[1].hp=0;
  assert.throws(()=>act(s,'attack',{playerId:'p1',reference:'John 3:16'}),/downed/);
  s=act(s,'ability',{ability:'second-wind',target:'p1'});assert.ok(s.players[1].hp>0);
  s.players.forEach(p=>p.hp=1);s.enemy.hp=1000;s.enemy.damage=100;s.enemy.weaknesses=[];
  s=act(s,'attack',{reference:'John 3:16'});assert.equal(s.phase,'battle');
  s=act(s,'attack',{playerId:'p1',reference:'John 8:32'});assert.equal(s.phase,'ended');
});

test('recall is an explicit one-time team exception and cannot loop a verse',()=>{
  let s=make(2);s.enemy.hp=1000;s.players[0].abilities.push('recall');s.players[0].equipped[0]='recall';
  s=act(s,'attack',{reference:'John 3:16'});s=act(s,'ability',{ability:'recall',reference:'John 3:16'});
  assert.ok(!s.used['john 3:16']);s=act(s,'attack',{playerId:'p1',reference:'John 3:16'});
  s.players[1].inventory.push('scroll-of-recall');
  assert.throws(()=>act(s,'item',{playerId:'p1',item:'scroll-of-recall',reference:'John 3:16'}),/already been recalled/);
});

test('abilities require owned equipped slots, recharge on personal verse turns, and preserve charge on failed use',()=>{
  let s=make();s.enemy.hp=1000;s.players[0].hp=50;
  s=act(s,'ability',{ability:'second-wind'});assert.equal(s.players[0].cooldowns['second-wind'],4);
  assert.throws(()=>act(s,'ability',{ability:'second-wind'}),/recharging/);
  assert.throws(()=>act(s,'ability',{ability:'double-strike'}),/Equip/);
  assert.throws(()=>act(s,'equip',{ability:'discernment',slot:0}),/between/);
  for(const v of verses.slice(0,4))s=act(s,'attack',{reference:refKey(v)});
  s=act(s,'ability',{ability:'second-wind'});assert.equal(s.players[0].cooldowns['second-wind'],8);
  s=act(s,'ability',{ability:'discernment'});assert.equal(s.enemy.revealed,true);
  assert.throws(()=>act(s,'ability',{ability:'discernment'}),/Already used/);
});

test('variant mechanics: truth breaks armor, poison ticks, frost chills, silence suppresses boss special',()=>{
  let s=make(2);s.enemy.hp=1000;s.enemy.weaknesses=[];s.enemy.armor=true;s.enemy.variant='exalted';
  s=act(s,'attack',{reference:'John 8:32'});assert.equal(s.enemy.armor,false);
  s.enemy.variant='corrupted';s.enemy.turn=2;s=act(s,'attack',{reference:'John 3:16'});assert.equal(s.players[0].poison,2);
  const hp=s.players[0].hp;s=act(s,'attack',{reference:'Genesis 1:1'});assert.ok(s.players[0].hp<=hp-s.enemy.damage-3);
  s.enemy.variant='frozen';s.enemy.turn=2;s=act(s,'attack',{reference:'Romans 8:28'});assert.equal(s.players[0].chill,2);
  s.players[0].equipped[0]='silence';s.players[0].abilities.push('silence');s.enemy.boss=true;s.enemy.turn=2;
  const allyHP=s.players[1].hp;s=act(s,'ability',{ability:'silence'});s=act(s,'attack',{reference:'James 4:7'});
  assert.equal(s.players[1].hp,allyHP);assert.equal(s.enemy.silenced,false);
  assert.equal(Object.keys(VARIANTS).length,7);
});

test('shops persist stock, charge once, reject unaffordable or sold purchases and reroll per player',()=>{
  let s=forceRoom(make(2),'shop');s.players[0].gold=300;
  const offer=s.players[0].shop.offers[0], other=structuredClone(s.players[1].shop);
  const action=command(s,'buy',{offer:offer.key});s=rules.reduce(s,action);
  assert.equal(s.players[0].gold,300-offer.cost);assert.equal(s.players[0].shop.offers[0].sold,true);
  assert.throws(()=>act(s,'buy',{offer:offer.key}),/sold/);assert.strictEqual(rules.reduce(s,action),s);
  const afterBuy=s.players[0].gold;s=act(s,'reroll');assert.equal(s.players[0].gold,afterBuy-10);
  assert.throws(()=>act(s,'buy',{offer:offer.key}),/already sold/);
  assert.deepEqual(s.players[1].shop,other);assert.equal(s.players[0].shop.offers.length,6);
  s.players[0].relics.push('narrow-gate');s=act(s,'reroll');assert.equal(s.players[0].shop.offers.length,4);
  s.players[0].gold=0;assert.throws(()=>act(s,'buy',{offer:s.players[0].shop.offers[0].key}),/gold/);
});

test('room rewards claim once and progression waits for active peers, not disconnected peers',()=>{
  let s=forceRoom(make(2),'treasure');const before=s.players[0].gold;
  s=act(s,'claim',{choice:'gold'});assert.equal(s.players[0].gold,before+32);
  assert.throws(()=>act(s,'claim',{choice:'gold'}),/already/);
  s=act(s,'continue');assert.equal(s.phase,'treasure');
  s=act(s,'continue',{}, {activeIds:['p0']});assert.equal(s.phase,'path');
  let both=forceRoom(make(2),'rest');both=act(both,'continue');both=act(both,'continue',{playerId:'p1'});assert.equal(both.phase,'path');
});

test('scripture questions reward one answer and enforce timed limits',()=>{
  for(let seed=1;seed<=35;seed++){
    let s=forceRoom(make(1,seed),'scripture');const q=s.room.challenge,before=s.players[0].gold;
    const v=verses.find(v=>rules.tags(v).includes(q.category));
    const answer=q.options.length?q.answer:refKey(v);
    s=act(s,'answer',{answer,now:q.deadline?q.deadline-1:100001});assert.equal(s.players[0].gold,before+25);assert.equal(s.players[0].discount,.15);
    assert.throws(()=>act(s,'answer',{answer}),/no longer/);
  }
  let s=forceRoom(make(),'scripture');s.room.challenge={kind:'timed',category:'love',deadline:500,text:'',options:[],answer:''};
  const gold=s.players[0].gold;s=act(s,'answer',{answer:'John 3:16',now:501});assert.equal(s.players[0].gold,gold);
});

test('risk challenge fails on all received healing; challenge loot gains 40 percent on a clean win',()=>{
  let s=make();s.players[0].abilities.push('challenge');s.players[0].equipped[0]='challenge';s.players[0].hp=50;s.enemy.hp=1;
  s=act(s,'ability',{ability:'challenge'});const noHeal=act(s,'attack',{reference:'John 3:16'});
  const healed=act(act(s,'item',{item:'healing-draught'}),'attack',{reference:'John 3:16'});
  assert.ok(noHeal.players[0].gold>healed.players[0].gold);assert.equal(healed.players[0].challengeBroken,true);
});

test('legacy save migration preserves health, classes, inventory, relics, currency and enemy progress',()=>{
  const payload={version:2,state:{health:54,maxHealth:140,level:6,xp:3,score:800,floor:7,baseDamage:24,jewels:22,classId:'sword-of-truth',classHistory:['word-warden','sword-of-truth'],relics:['psalm-mastery'],inventory:['great-recall'],itemShields:2,runMastered:['John 3:16'],used:['john 3:16'],enemy:{...options.enemies[0],hp:9,maxHp:85}}};
  const s=rules.migrateLegacy(payload,{name:'Sean',id:'migrated',seed:23});
  assert.equal(s.floor,7);assert.equal(s.players[0].hp,54);assert.equal(s.enemy.hp,9);assert.equal(s.players[0].classId,'sword-of-truth');
  assert.equal(s.players[0].gold,69);assert.deepEqual(s.players[0].relics,['psalm-mastery']);assert.equal(s.players[0].shield,36);
  assert.ok(s.used['john 3:16']);assert.equal(rules.migrateLegacy({version:2,state:{health:0}},{name:'Sean'}),null);
});

test('all requested verse categories and all abilities are represented',()=>{
  for(const key of CATEGORY_KEYS)assert.ok(options.conceptKeys.includes(key),key);
  assert.equal(Object.keys(ABILITIES).length,8);assert.ok(Object.keys(rules.items).length>=17);assert.ok(Object.keys(rules.relics).length>=23);
});

test('long runs stay serializable and bounded; seeded transactions replay deterministically',()=>{
  let s=make(3,42);s.players.forEach(p=>{p.baseDamage=1000000;p.hp=p.maxHp=10000;});
  for(let i=0;i<120;i++){
    const action=command(s,'attack',{reference:'John 3:16'}), a=rules.reduce(s,action), b=rules.reduce(s,action);
    assert.deepEqual(a,b);s=a;
    if(!s.paths.includes('battle')&&!s.paths.includes('boss'))s.paths=['battle'];
    s=act(s,'path',{path:s.paths.includes('boss')?'boss':'battle'});
  }
  assert.ok(s.events.length<=40);assert.ok(s.actions.length<=100);assert.ok(JSON.stringify(s).length<100000);
  assert.deepEqual(JSON.parse(JSON.stringify(s)),s);assert.equal(s.floor,121);
});


test('correct answers block every damage source for solo and all five players, preserving defenses',()=>{
  for(const count of [1,5]) for(const variant of Object.keys(VARIANTS)) {
    let s=make(count);s.enemy.hp=s.enemy.maxHp=10000;s.enemy.boss=true;s.enemy.variant=variant;
    s.enemy.weaknesses=[{concept:'love',multiplier:1}];s.enemy.turn=2;
    const p=s.players[count-1];p.poison=2;p.chill=2;p.shield=11;p.guard=true;p.relics=['second-chance'];
    const before=structuredClone(s);
    const action=command(s,'attack',{playerId:p.id,reference:'John 3:16'});
    const after=rules.reduce(s,action), actor=after.players[count-1];
    assert.deepEqual(after.players.map(p=>p.hp),before.players.map(p=>p.hp),variant);
    assert.equal(actor.shield,11);assert.equal(actor.guard,true);assert.equal(actor.battle.secondChance,true);
    assert.equal(actor.poison,1);assert.equal(actor.chill,1);assert.equal(after.enemy.turn,3);
    const event=after.events.find(e=>e.effect==='answer');
    assert.equal(event.correct,true);assert.equal(event.reference,'John 3:16');assert.equal(event.playerId,p.id);
    assert.equal(event.actionId,action.id);assert.equal(event.expiresAt,action.now+2800);
    assert.strictEqual(rules.reduce(after,action),after);
  }
});

test('wrong answers retain poison, special damage and boss pulse; successful defense is turn scoped',()=>{
  let s=make(2);s.enemy.hp=10000;s.enemy.weaknesses=[{concept:'love',multiplier:1.3}];s.enemy.variant='corrupted';s.enemy.boss=true;
  s=act(s,'attack',{reference:'John 3:16'});assert.equal(s.players[0].hp,100);
  s.enemy.turn=2;s.players[1].poison=1;
  const next=act(s,'attack',{playerId:'p1',reference:'Genesis 1:1'});
  assert.ok(next.players[1].hp<100-next.enemy.damage);assert.ok(next.players[0].hp<100);
  assert.equal(next.players[1].poison,2);
  assert.equal(next.events.filter(e=>e.effect==='answer').at(-1).correct,false);
});

test('battle trials use correct protection and wrong-answer retaliation',()=>{
  const s=make();s.enemy.hp=1000;s.players[0].poison=2;
  s.players[0].quiz={kind:'book',answer:'John',relic:'book-hunter'};
  const correct=act(s,'quiz',{answer:'John'}),wrong=act(s,'quiz',{answer:'Genesis'});
  assert.equal(correct.players[0].hp,100);assert.ok(wrong.players[0].hp<100);
  assert.equal(correct.events.find(e=>e.effect==='answer').correct,true);
  assert.equal(wrong.events.find(e=>e.effect==='answer').reference,'Genesis');
});

test('old hint items and relics provide defense without revealing Scripture; v3 upgrades preserve progress',()=>{
  for(const id of ['word-fragment','book-lantern','chapter-map','reference-compass']) {
    const s=make();s.players[0].inventory=[id];const next=act(s,'item',{item:id});
    assert.equal(next.players[0].shield,18);assert.equal(next.players[0].hint,'');
    assert.ok(!rules.items[id].kind.startsWith('hint-'));
  }
  const s=make();s.players[0].relics=['verse-insight','scroll-of-context'];
  const next=act(s,'insight');assert.equal(next.players[0].shield,12);assert.equal(next.players[0].hint,'');
  assert.throws(()=>act(next,'insight'),/unavailable/);
  const battle=forceRoom(next,'battle');assert.equal(battle.players[0].shield,20);assert.equal(battle.players[0].hint,'');
  s.version=3;s.players[0].hint='John 3:16';s.players[0].hp=47;
  const upgraded=rules.upgradeRun(s);assert.equal(upgraded.version,4);assert.equal(upgraded.players[0].hp,47);
  assert.equal(upgraded.players[0].hint,'');assert.deepEqual(upgraded.events,[]);assert.equal(s.version,3);
});
