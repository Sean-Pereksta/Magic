const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
function load(){const c=vm.createContext({console});c.window=c;for(const id of ['catalog','progression','living-combat'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',id+'.js'),'utf8'),c);return {P:c.Progression,C:c.Catalog};}
function fixture(){
  const {P,C}=load(),defs=Object.values(C.items),active=['leaf','fire','frost','storm'].map((el,i)=>({id:defs.find(d=>d.element===el&&!d.recipe&&d.style==='strike')?.id||defs.find(d=>d.element===el&&!d.recipe).id,uid:i+1,level:1,cd:2}));
  const r={round:40,region:9,guardians:9,kills:39,maxHp:1000,hp:500,shield:0,active,pack:['stone','spirit'].map((el,i)=>({id:defs.find(d=>d.element===el&&!d.recipe).id,uid:i+10,level:1,cd:2})),skills:{},abilityTimers:{},legacy:{ranks:{},masteries:{},oaths:[],constellations:[],milestones:[],config:{},seenEnemies:[]}};
  const enemies=Array.from({length:5},(_,i)=>({uid:i+1,id:'probe'+i,lane:i,hp:100000,maxHp:100000,damage:20,period:2,burn:10,poison:10,chill:1,freeze:1,rooted:1,stun:1,fracture:2,haunt:1,hauntTime:4,statusClock:0,attackProgress:.8,echoes:[],weak:'none',resist:'none'}));r.enemies=enemies;
  return {P,C,r,enemies,ev:[]};
}
test('living catalog contains every requested element, pair, general, spell, mythic and constellation talent',()=>{
  const {P}=load(),c=P.livingCatalog;
  assert.equal(c.ordinary.length,24);assert.equal(c.pairs.length,7);assert.ok(P.upgrades.prismatic_oath);assert.equal(c.generals.length,8);assert.equal(c.spells.length,6);assert.equal(c.mythics.length,22);assert.equal(c.constellations.length,15);
  for(const d of Object.values(P.upgrades)){assert.ok(d.description);assert.ok(d.rarity);assert.equal(typeof d.explain,'function');}
});
test('all six automatic abilities resolve real effects with exact cooldowns and ranks',()=>{
  const cds={briar_barrage:6,inferno_ring:9,crystal_chorus:10,chainstorm:7,avalanche_guard:9,spectral_guardian:12};
  for(const [id,cd] of Object.entries(cds)){
    const {P,r,enemies,ev}=fixture();P.apply(r,id);r.abilityTimers[id]=0;for(const e of enemies){e.freeze=0;e.stun=0;}
    P.tickAbilities(r,enemies,.05,ev);assert.equal(r.abilityTimers[id],cd,id);assert.ok(ev.some(e=>e.pattern===id||id==='avalanche_guard'&&e.pattern==='guard'),id);
    if(id==='crystal_chorus'){assert.equal(enemies.filter(e=>e.freeze>0).length,2);assert.ok(enemies.every(e=>e.hp===e.maxHp));}
    else if(id==='avalanche_guard'){assert.ok(r.shield>0);assert.ok(r.living.buffs.guard>0);assert.ok(enemies.every(e=>e.hp===e.maxHp));}
    else if(id==='spectral_guardian'){assert.ok(r.living.companions.some(c=>c.id==='spectral'));P.tickAbilities(r,enemies,.3,ev);assert.ok(enemies.some(e=>e.hp<e.maxHp));}
    else assert.ok(enemies.some(e=>e.hp<e.maxHp),id);
    P.apply(r,id);assert.equal(P.rankOf(r,id),2);
  }
});
test('primary-only proc contract keeps all dedicated support styles harmless without separately purchased offense',()=>{
  for(const style of ['heal','shield','stun','freeze']){
    const {P,r,enemies,ev}=fixture();for(const id of ['thorn_volley','arc_jump','capacitor','rolling_stone','phantom_twin','glass_shards','thunder_tyrant'])P.apply(r,id);
    P.onActivation(r,enemies,enemies[0],{style,element:'storm',cooldown:2},100,ev,()=>0);
    assert.ok(enemies.every(e=>e.hp===e.maxHp),style);
  }
});
test('Storm support activations trigger independently purchased Tempest and Capacitor powers at their exact counts',()=>{
  for(const style of ['heal','shield','stun','freeze']){
    for(const effect of ['none','tempest','storm_kings_wrath','capacitor']){
      const {P,r,enemies,ev}=fixture();const count=effect==='capacitor'?5:6;
      if(effect==='tempest')r.legacy.oaths=['tempest'];else if(effect!=='none')P.apply(r,effect);
      for(let i=1;i<count;i++){P.onActivation(r,enemies,enemies[0],{element:'storm',style,cooldown:2},0,ev,()=>0);assert.ok(enemies.every(e=>e.hp===e.maxHp),effect+' fires too early');}
      P.onActivation(r,enemies,enemies[0],{element:'storm',style,cooldown:2},0,ev,()=>0);
      if(effect==='none')assert.ok(enemies.every(e=>e.hp===e.maxHp),'support without a purchased power stays harmless');
      else {assert.ok(enemies.some(e=>e.hp<e.maxHp),effect+' must work with '+style);assert.ok(ev.some(e=>e.pattern==='chainstorm'));}
    }
  }
});
test('elemental talents add followups, cooldowns, recovery, ailment effects and counters',()=>{
  const cases=[['thorn_volley','leaf','strike',3],['arc_jump','storm','strike',1],['capacitor','storm','strike',5],['rolling_stone','stone','strike',1],['phantom_twin','spirit','strike',1],['glass_shards','frost','strike',1],['haunt_amplifier','spirit','strike',1]];
  for(const [id,element,style,count] of cases){const {P,r,enemies,ev}=fixture();P.apply(r,id);for(let i=0;i<count;i++)P.onActivation(r,enemies,enemies[0],{element,style,cooldown:3},100,ev,()=>0);assert.ok(enemies.some(e=>e.hp<e.maxHp),id);}
  const {P,r,enemies,ev}=fixture();P.apply(r,'kindled_momentum');P.onActivation(r,enemies,enemies[0],{element:'fire',style:'heal',cooldown:2},0,ev,()=>0);assert.ok(r.active.some(it=>it.cd<2));
  P.apply(r,'nectar_bloom');for(let i=0;i<4;i++)P.onActivation(r,enemies,enemies[0],{element:'leaf',style:'heal',cooldown:2},0,ev,()=>0);assert.ok(r.hp>500);
  P.apply(r,'charged_barrier');P.onHurt(r,20,enemies[0],ev,20);assert.ok(enemies[0].hp<enemies[0].maxHp);
  P.apply(r,'wildfire_furnace');const burn=enemies[0].burn,poison=enemies[0].poison;P.tickAbilities(r,enemies,.1,ev);assert.ok(enemies[0].burn<burn);assert.equal(enemies[0].poison,poison);
  P.apply(r,'frostbite_bulwark');P.onActivation(r,enemies,enemies[0],{element:'frost',style:'freeze',cooldown:2},0,ev,()=>0);assert.ok(r.shield>0);
});
test('all cross-element talents have executable combat effects',()=>{
  for(const id of ['wildfire_bloom','stormglass','molten_fault','wraithvine','ghostflame','icehammer','tempest_bloom']){
    const {P,r,enemies,ev}=fixture();P.apply(r,id);enemies[0].poison=100;const before=JSON.stringify({hp:r.hp,enemies});
    P.onActivation(r,enemies,enemies[0],{element:P.upgrades[id].requires.includes('storm')?'storm':id==='ghostflame'?'spirit':id==='icehammer'?'stone':id==='molten_fault'?'fire':'leaf',style:'strike',cooldown:3},100,ev,()=>0);
    assert.notEqual(JSON.stringify({hp:r.hp,enemies}),before,id);
  }
});
test('15 constellations require permanent ownership and matching gear, and strengthen their associated effects',()=>{
  const {P,r,C,enemies,ev}=fixture();
  for(const id of P.livingCatalog.constellations){const d=P.upgrades['constellation_'+id];assert.equal(P.eligible(r,d),false,id);r.legacy.constellations.push(id);assert.equal(P.eligible(r,d),true,id);}
  // Combat passives require the two elements actually equipped, rather than backpack ownership.
  r.legacy.constellations=['ashroot'];P.onActivation(r,enemies,enemies[0],{element:'leaf',style:'strike',cooldown:2},100,ev,()=>0);const passiveLoss=100000-enemies[0].hp;assert.ok(passiveLoss>0);
  enemies[0].hp=100000;P.apply(r,'constellation_ashroot');P.onActivation(r,enemies,enemies[0],{element:'leaf',style:'strike',cooldown:2},100,ev,()=>0);assert.ok(100000-enemies[0].hp>passiveLoss*5);
  r.active=r.active.filter(it=>C.items[it.id].element!=='fire');enemies[0].hp=100000;P.onActivation(r,enemies,enemies[0],{element:'leaf',style:'strike',cooldown:2},100,ev,()=>0);assert.equal(enemies[0].hp,100000);
});
test('each of the 15 purchased constellations changes its advertised combat interaction',()=>{
  for(const id of fixture().P.livingCatalog.constellations){
    const {P,r,C,enemies,ev}=fixture(),d=P.upgrades['constellation_'+id];
    r.active=d.requires.map((element,i)=>({uid:i+1,id:Object.values(C.items).find(d=>d.element===element&&!d.recipe).id,level:1,cd:2}));
    r.legacy.constellations=[id];r.abilityTimers.razor_bloom=5;r.shield=id==='grave_monument'?50:0;
    enemies[0].poison=100;enemies[0].haunt=0;for(const e of enemies.slice(1)){e.poison=0;e.burn=0;}if(id==='wintervine')enemies[0].rooted=0;
    const before=JSON.stringify({hp:r.hp,shield:r.shield,timers:r.abilityTimers,enemies});
    if(['wraithbloom','moonwinter'].includes(id)){enemies[0].hp=0;P.onDeaths(r,enemies[0],enemies,ev);}
    else {
      const el=({ashroot:'fire',wintervine:'leaf',sapwire:'storm',ironroot:'leaf',thermal_fracture:'fire',voltage_furnace:'storm',magma_fault:'fire',cursed_ember:'spirit',prism_circuit:'storm',avalanche_heart:'stone',magnetfall:'storm',soul_voltage:'spirit',grave_monument:'spirit'})[id];
      P.onActivation(r,enemies,enemies[0],{element:el,style:'strike',cooldown:3},100,ev,()=>0);
    }
    assert.notEqual(JSON.stringify({hp:r.hp,shield:r.shield,timers:r.abilityTimers,enemies}),before,id);
    if(id==='grave_monument'){const foe={boss:true};r.legacy.constellations=[];const normal=P.incoming(r,100,foe,[],()=>1);r.legacy.constellations=[id];assert.ok(P.incoming(r,100,foe,[],()=>1)<normal);}
  }
});
test('mythic talents all install and enforce one rank; periodic mythics perform formation-wide transformations',()=>{
  for(const id of fixture().P.livingCatalog.mythics){const {P,r}=fixture();assert.equal(P.apply(r,id),true,id);assert.equal(P.apply(r,id),false,id);assert.equal(P.rankOf(r,id),1);}
  for(const id of ['cinder_apocalypse','solar_cataclysm','absolute_zero','chronobloom','tempest_of_seasons','the_sixfold_crown']){
    const {P,r,enemies,ev}=fixture();P.apply(r,id);enemies.forEach(e=>e.freeze=0);for(let i=0;i<260;i++)P.tickAbilities(r,enemies,.05,ev);
    if(id==='absolute_zero')assert.ok(enemies.every(e=>e.freeze>=5),id);
    else if(id==='chronobloom')assert.ok(r.active.every(it=>it.cd===0));
    else assert.ok(enemies.every(e=>e.hp<e.maxHp),id);
  }
});
test('mythic activation, guardian, revival, army and diversity behaviors are distinct and functional',()=>{
  for(const id of ['worldroot_awakening','swarm_sovereign','colossus_awakens']){const {P,r}=fixture();P.apply(r,id);assert.equal(r.living.companions.length,id==='colossus_awakens'?1:id==='swarm_sovereign'?3:2,id);}
  for(const [id,el,times] of [['storm_kings_wrath','storm',6],['thunder_tyrant','storm',1],['perpetual_circuit','leaf',7],['shattered_eternity','stone',1],['the_shattered_earth','fire',1]]){
    const {P,r,enemies,ev}=fixture();P.apply(r,id);for(let i=0;i<times;i++)P.onActivation(r,enemies,enemies[0],{element:el,style:'strike',cooldown:3},100,ev,()=>0);assert.ok(enemies.some(e=>e.hp<e.maxHp),id);
  }
  const {P,r,enemies,ev}=fixture();P.apply(r,'crystal_cathedral');enemies.forEach(e=>e.freeze=0);P.onSpawn(r,ev);assert.ok(enemies.every(e=>e.freeze===8));
  P.apply(r,'mountain_awakening');P.onSpawn(r,ev);assert.ok(r.shield>=350);assert.equal(P.incoming(r,100,enemies[0],ev,()=>1),65);for(let i=0;i<4;i++)P.onHurt(r,20,enemies[0],ev,20);assert.ok(enemies.some(e=>e.hp<e.maxHp));
  const level=r.active[0].level;P.apply(r,'ancestral_forge');assert.equal(r.active[0].level,level+2);P.victory(r,ev);assert.ok(r.active.some(it=>it.level===level+3));
  P.apply(r,'soul_of_eternity');r.hp=500;r.shield=0;P.onActivation(r,enemies,enemies[0],{element:'spirit',style:'strike',cooldown:3},100,ev,()=>0);assert.ok(r.hp>500&&r.shield>0);
});
test('companions target living foes, persist timers through saves, expire and scale with guardians',()=>{
  const {P,r,enemies,ev}=fixture();r.legacy.oaths=['worldroot'];P.onStart(r);assert.equal(r.living.companions.length,1);
  enemies[0].hp=0;P.tickAbilities(r,enemies,.3,ev);assert.ok(enemies.slice(1).some(e=>e.hp<e.maxHp));assert.ok(enemies.slice(1).some(e=>e.poison>10));
  const saved=JSON.parse(JSON.stringify(r.living));assert.deepEqual(JSON.parse(JSON.stringify(P.validateLivingState(saved))),saved);
  const prior=r.living.companions[0].power;r.guardians=16;P.tickAbilities(r,enemies,.05,ev);assert.ok(r.living.companions[0].power>prior);
  P.apply(r,'spectral_guardian');r.abilityTimers.spectral_guardian=0;P.tickAbilities(r,enemies,.05,ev);assert.ok(r.living.companions.some(c=>!c.permanent));P.onSpawn(r,ev);assert.ok(r.living.companions.every(c=>c.permanent));
});
test('death bursts process a corpse once, build armies and provide recovery without recursive attacks',()=>{
  const {P,r,enemies,ev}=fixture();P.apply(r,'infernal_domino');P.apply(r,'phantom_legion');P.apply(r,'worldroot_network');P.apply(r,'soul_satchel');enemies[0].hp=0;
  P.onDeaths(r,enemies[0],enemies,ev);const hp=enemies[1].hp,army=r.living.companions.length,recovery=r.hp;assert.ok(hp<100000);assert.equal(army,2);assert.ok(recovery>500);
  P.onDeaths(r,enemies[0],enemies,ev);assert.equal(enemies[1].hp,hp);assert.equal(r.living.companions.length,army);assert.equal(r.hp,recovery);
});
test('Phoenix oath revives exactly once; other survival talents and forest healing have wave gates',()=>{
  const {P,r,enemies,ev}=fixture();r.legacy.oaths=['phoenix'];r.hp=0;assert.equal(P.survive(r,enemies,ev),true);assert.equal(r.hp,350);r.hp=0;assert.equal(P.survive(r,enemies,ev),false);
  P.apply(r,'second_soul');assert.equal(P.survive(r,enemies,ev),true);r.hp=0;assert.equal(P.survive(r,enemies,ev),false);r.round+=8;assert.equal(P.survive(r,enemies,ev),true);
  r.legacy.milestones=['heart_of_forest'];r.hp=100;P.onHurt(r,20,enemies[0],ev);assert.equal(r.hp,250);r.hp=100;P.onHurt(r,20,enemies[0],ev);assert.equal(r.hp,100);
});
test('rarity, eligibility, expanded selections and Mythmaker respect equipment and permanent mastery',()=>{
  const {P,r}=fixture();r.region=0;assert.equal(P.talentOdds(r).mythic,0);for(let i=0;i<100;i++)assert.ok(P.offer(r,()=>.99).every(id=>P.upgrades[id].rarity!=='mythic'));
  r.legacy.ranks.expanded_destiny=2;assert.equal(P.offer(r,()=>.2).length,5);
  r.region=10;r.guardians=3;r.legacy.oaths=['mythmaker'];assert.ok(P.offer(r,()=>.3).some(id=>P.upgrades[id].rarity==='mythic'));
  assert.equal(P.eligible(r,P.upgrades.the_sixfold_crown),false);r.region=20;r.legacy.masteries=Object.fromEntries(['leaf','fire','frost','storm','stone','spirit'].map(id=>[id,8]));assert.equal(P.eligible(r,P.upgrades.the_sixfold_crown),true,'six mastery trees, not six equipped relics');
  r.banished=['long_arm'];assert.equal(P.eligible(r,P.upgrades.long_arm),false);
});
test('healing previews stay pure, actual overhealing converts to shields and all six capstones have effects',()=>{
  const {P,r,enemies,ev}=fixture();P.apply(r,'overflowing_tonic');r.hp=r.maxHp;r.shield=0;const before=JSON.stringify(r);assert.ok(P.healing(r,100)>0);assert.equal(JSON.stringify(r),before);P.afterHealing(r,100,0);assert.equal(r.shield,30);
  for(const el of ['leaf','fire','frost','storm','stone','spirit']){
    const f=fixture();f.r.legacy.milestones=['ascendant_'+el];f.enemies[0].poison=100;
    if(el==='fire'){f.enemies[0].hp=0;const burn=f.enemies[1].burn;f.P.onDeaths(f.r,f.enemies[0],f.enemies,f.ev);assert.ok(f.enemies[1].burn>burn);}
    else if(el==='storm'){for(let i=0;i<6;i++)f.P.onActivation(f.r,f.enemies,f.enemies[0],{element:el,style:'strike',cooldown:3},100,f.ev,()=>0);assert.ok(f.r.active.some(it=>it.cd<2));}
    else if(el==='stone'){f.P.onActivation(f.r,f.enemies,f.enemies[0],{element:el,style:'shield',cooldown:3},0,f.ev,()=>0);const d={element:el,style:'strike',cooldown:3};const charged=f.P.hitMultiplier(f.r,d,f.enemies[0],5);f.P.onActivation(f.r,f.enemies,f.enemies[0],d,100,f.ev,()=>0);assert.ok(charged>f.P.hitMultiplier(f.r,d,f.enemies[0],5));}
    else {const s=JSON.stringify({shield:f.r.shield,enemies:f.enemies});f.P.onActivation(f.r,f.enemies,f.enemies[0],{element:el,style:el==='spirit'?'heal':'strike',cooldown:3},100,f.ev,()=>0);assert.notEqual(JSON.stringify({shield:f.r.shield,enemies:f.enemies}),s,el);}
  }
});
test('all new skills, automatic cooldowns and companion state survive actual engine validation and a resumed expedition',()=>{
  const c=vm.createContext({console});c.window=c;for(const name of ['catalog','progression','legacy','living-combat','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c);
  const {GameEngine:E,Progression:P}=c,p=E.freshProfile();E.start(p);const r=p.run;
  for(const ids of [P.livingCatalog.ordinary,P.livingCatalog.pairs,P.livingCatalog.generals,P.livingCatalog.spells,P.livingCatalog.mythics])for(const id of ids)P.apply(r,id);
  r.level=1+Object.values(r.skills).reduce((n,v)=>n+v,0);r.xp=0;
  const copy=()=>JSON.parse(JSON.stringify(p));const restored=E.validate(copy());
  assert.deepEqual(JSON.parse(JSON.stringify(restored.run.skills)),JSON.parse(JSON.stringify(r.skills)));
  assert.deepEqual(JSON.parse(JSON.stringify(restored.run.living)),JSON.parse(JSON.stringify(r.living)));
  const originalTimers=JSON.stringify(restored.run.living);E.tick(restored,0);assert.equal(JSON.stringify(restored.run.living),originalTimers,'zero or paused time cannot move companion or mythic clocks');
  restored.run.walkLeft=.01;E.tick(restored,.01);assert.equal(restored.run.phase,'fight');const checkpoint=E.validate(JSON.parse(JSON.stringify(restored)));assert.equal(checkpoint.run.phase,'fight');
  E.tick(checkpoint,.4);E.validate(JSON.parse(JSON.stringify(checkpoint)));
});
