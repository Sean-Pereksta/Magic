const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const dir=path.resolve(__dirname,'..');
const copy=x=>JSON.parse(JSON.stringify(x));
function load(){
  const c=vm.createContext({console});c.window=c;
  for(const name of ['catalog','titans','progression','legacy','living-combat','ancient-artifacts','engine']){
    const file=path.join(dir,name+'.js');if(fs.existsSync(file))vm.runInContext(fs.readFileSync(file,'utf8'),c);
  }
  return {C:c.Catalog,P:c.Progression,E:c.GameEngine,A:c.AncientArtifacts,T:c.Titans};
}
function fixture(id){
  const game=load(),{C,E}=game,p=E.freshProfile();E.start(p);const r=p.run;
  r.hp=100;r.maxHp=1000;r.shield=0;r.round=40;r.region=9;r.ailments={corrosion:3,snare:3,hex:3,burn:30,chill:3,shock:3};r.venom=15;
  const source={uid:400,id,level:1,cd:0,slot:0};r.active=[source];r.skills={};
  const enemies=Array.from({length:5},(_,i)=>({uid:i+1,id:'probe'+i,lane:i,hp:100000,maxHp:100000,boss:i===0,damage:10,period:2,attackProgress:.9,ward:1,burn:0,poison:0,rooted:0,stun:0,freeze:0,chill:0,fracture:0,haunt:0,hauntTime:0,echoes:[]}));
  r.enemies=enemies;return {...game,p,r,source,enemies,d:C.items[id],ev:[]};
}
function activate(f){f.P.onActivation(f.r,f.enemies,f.enemies[0],f.d,100,f.ev,()=>.9,f.source,100);}

test('eleven forge-only Ancients require exactly two Mythics and improve both potency and rate',()=>{
  const {C,E,A}=load();assert.equal(A.ids.length,11);assert.equal(C.rarities.ancient.rank,5);
  assert.ok(C.rarities.ancient.powerScale>C.rarities.mythic.powerScale);
  for(const id of A.ids){
    const d=C.items[id];assert.equal(d.recipe.length,2);assert.equal(d.rarity,'ancient');assert.ok(d.forgeOnly&&d.ancient);
    assert.ok(d.ancientPower&&d.auraDescription&&d.aura);assert.equal(C.combo(...d.recipe),id);
    for(const parentId of d.recipe){const parent=C.items[parentId];assert.equal(parent.rarity,'mythic');assert.ok(d.power*2.6>=parent.power*1.8*1.79);assert.ok(d.power*2.6/d.cooldown>=parent.power*1.8/parent.cooldown*1.79);}
    for(const round of [1,10,140,10000])for(const boss of [true,false])assert.equal(E.dropEligible(d,boss,round),false);
  }
  assert.equal(C.combo('kingtide_conch','mycelium_crook'),'ancient_genesis_tide','early boss Mythics have a reachable Ancient recipe');
  for(const d of Object.values(C.items))if(d.recipe)assert.equal(C.combo(...d.recipe),d.id,'preserve all previous ingredient pairs');
});

test('the forge consumes two actual Mythics, preserves fractional levels, discovers and reloads every Ancient',()=>{
  const {C,E,A}=load();
  for(const id of A.ids){
    const d=C.items[id],p=E.freshProfile();E.start(p);
    p.run.active=[{uid:1,id:d.recipe[0],level:2.5,cd:0,slot:0}];p.run.pack=[{uid:2,id:d.recipe[1],level:3.25,cd:0,slot:0}];p.run.nextUid=3;
    assert.equal(E.previewCombine(p,1,2).id,id);assert.equal(E.previewCombine(p,1,2).level,4.25);
    const result=E.act(p,'combine',1,2);assert.equal(result.ok,true,id);assert.equal(p.run.active.length,1);assert.equal(p.run.pack.length,0);
    assert.equal(p.run.active[0].id,id);assert.equal(p.run.active[0].level,4.25);assert.ok(p.seenItems.includes(id));
    const restored=E.validate(copy(p));assert.equal(restored.run.active[0].id,id);assert.equal(restored.run.active[0].level,4.25);assert.ok(restored.seenItems.includes(id));
  }
});

test('every offensive Ancient resolves its distinctive group power without recursive primary activations',()=>{
  for(const id of ['ancient_genesis_tide','ancient_worldroot_crown','ancient_sunforged_cataclysm','ancient_stormsovereign','ancient_eclipse_reaper','ancient_starfall_orrery','ancient_voidheart']){
    const f=fixture(id);activate(f);const removed=f.enemies.reduce((n,e)=>n+100000-e.hp,0);
    assert.ok(removed>100,id+' must resolve more than its primary hit');assert.ok(f.ev.some(e=>e.kind==='ancient'&&e.pattern==='ancient_'+f.d.aura&&e.burst),id+' must emit its character effect');
    if(f.d.aura==='tidal'){assert.ok(f.enemies.every(e=>e.hp<100000));assert.equal(f.r.hp,180);assert.equal(f.r.shield,80);assert.equal(f.r.ailments.snare,0);assert.equal(f.r.ailments.corrosion,0);}
    if(f.d.aura==='worldroot'){assert.ok(f.enemies.every(e=>e.poison===120&&e.rooted===1.8));assert.equal(f.r.hp,160);}
    if(f.d.aura==='sunfire')assert.ok(f.enemies.every(e=>e.burn===160));
    if(f.d.aura==='storm'){assert.equal(removed,450);assert.ok(f.enemies.every(e=>e.attackProgress<.9));assert.equal(f.r.shield,80);}
    if(f.d.aura==='eclipse')assert.equal(f.r.hp,310);
    if(f.d.aura==='starfall'){assert.equal(removed,390);assert.equal(f.enemies.filter(e=>e.ward===0).length,3);assert.ok(f.enemies.some(e=>e.fracture===3));}
    if(f.d.aura==='void')assert.ok(f.enemies.every(e=>e.ward===0&&e.rooted===2.5));
  }
  const cold=fixture('ancient_sunforged_cataclysm');activate(cold);const warm=fixture('ancient_sunforged_cataclysm');for(const e of warm.enemies)e.burn=1;activate(warm);
  assert.ok(warm.enemies[0].hp<cold.enemies[0].hp,'burning foes suffer the advertised extra blast');
});

test('Ancient dedicated healing, protection and control remain harmless while providing formation-wide utility',()=>{
  for(const id of ['ancient_winterheart','ancient_tempest_hourglass','ancient_titan_aegis','ancient_everdawn_grail']){
    const f=fixture(id);f.r.active.push({uid:401,id:'stick',level:1,cd:2,slot:1});activate(f);
    assert.ok(f.enemies.every(e=>e.hp===100000),id+' must never deal damage');assert.equal(f.d.dealsDamage,false);
    if(f.d.aura==='winter'){assert.ok(f.enemies.every(e=>e.freeze>=2.8&&e.freeze<=8));assert.equal(f.r.shield,100);}
    if(f.d.aura==='hourglass'){assert.ok(f.enemies.every(e=>e.stun>=2.6&&e.stun<=8));assert.equal(f.r.active[1].cd,1.2);}
    if(f.d.aura==='titan')assert.equal(f.r.shield,400);
    if(f.d.aura==='everdawn'){assert.equal(f.r.hp,280);assert.equal(f.r.shield,150);assert.equal(f.r.venom,0);assert.ok(Object.values(f.r.ailments).every(n=>n===0));}
  }
});

test('equipped-only Ancient passives stop immediately after unequipping and do not stack duplicate immunity',()=>{
  const f=fixture('ancient_titan_aegis'),before=f.P.incoming(f.r,100,f.enemies[0],[],()=>1);
  assert.equal(before,70);f.r.active.push({...f.source,uid:401});assert.equal(f.P.incoming(f.r,100,f.enemies[0],[],()=>1),70);
  f.r.pack=[f.source];f.r.active=[];assert.equal(f.P.incoming(f.r,100,f.enemies[0],[],()=>1),100);
  const v=fixture('ancient_voidheart'),probe=v.C.items.stick,normal=v.P.hitMultiplier(v.r,probe,v.enemies[1],5);
  assert.equal(v.P.hitMultiplier(v.r,probe,v.enemies[0],5),normal*1.5);assert.equal(v.A.equipped(v.r).length,1);assert.equal(v.A.aura(v.r)[0],'void');
  v.r.active=[];assert.equal(v.P.hitMultiplier(v.r,probe,v.enemies[0],5),normal);
});

test('source potency and fractional level govern Ancient powers and control respects the eight second cap',()=>{
  const f=fixture('ancient_worldroot_crown');f.P.onActivation(f.r,f.enemies,f.enemies[0],f.d,1,f.ev,()=>1,{...f.source,level:9.25},250);
  assert.equal(f.enemies[0].poison,300,'proc uses exact full potency rather than damage clipped by a dying primary target');
  const c=fixture('ancient_winterheart');c.P.onActivation(c.r,c.enemies,c.enemies[0],c.d,0,c.ev,()=>1,{...c.source,level:100},1e9);
  assert.ok(c.enemies.every(e=>e.freeze===8));
});

test('real encrypted transfer preserves every Ancient and a live Titan warning with identical combat continuation',async()=>{
  const Cloud=require('../cloud-save.js'),{C,E,A,T}=load(),p=E.freshProfile();E.start(p);const r=p.run;
  r.round=10;r.region=E.routeInfo(r).region;r.kills=r.foes=9;p.best=9;r.walkLeft=.001;
  r.active=[{uid:r.nextUid++,id:'ancient_titan_aegis',level:2.5,cd:1,slot:0}];
  let slot=1+Math.max(-1,...r.pack.map(it=>it.slot));
  for(const id of A.ids){if(id!==r.active[0].id)r.pack.push({uid:r.nextUid++,id,level:3.25,cd:0,slot:slot++});if(!p.seenItems.includes(id))p.seenItems.push(id);}
  E.tick(p,.01);const titan=r.enemies.find(e=>e.titanId);assert.ok(titan);
  T.advance(r,titan,3.2,[],{damage:()=>assert.fail('a warning must precede damage')});assert.ok(titan.titan.warning);
  const original=E.validate(copy(p)),raw=JSON.stringify({game:'bramblebound',format:1,profile:original,settings:{motion:false,audio:false}});
  const envelope=await Cloud.encrypt(raw,'Ancient encrypted transfer test password');
  assert.equal(envelope.cipher,'AES-GCM-256');const decoded=JSON.parse(await Cloud.decrypt(envelope,'Ancient encrypted transfer test password'));
  const restored=E.validate(decoded.profile);
  assert.equal(restored.run.active[0].id,'ancient_titan_aegis');assert.equal(restored.run.active[0].level,2.5);
  assert.equal(restored.run.pack.filter(it=>C.items[it.id].ancient).length,10);
  assert.deepEqual(copy(restored.run.enemies[0].titan),copy(original.run.enemies[0].titan));
  const expected=E.tick(original,2.1),actual=E.tick(restored,2.1);
  assert.deepEqual(copy(actual),copy(expected));assert.deepEqual(copy(restored),copy(original));
  assert.ok(actual.some(ev=>ev.kind==='ancient'),'restored Ancient powers still activate');
  assert.ok(actual.some(ev=>ev.pattern==='titan-cast'),'restored warning resolves once');
});
