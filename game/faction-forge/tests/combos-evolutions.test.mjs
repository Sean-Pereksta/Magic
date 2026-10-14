import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {COMBOS,COMBO_LIMITS,comboStatus,afterComboAttack,tickCombos,noteComboDamage,noteComboAmbush} from '../combos.js';
import {EVOLUTIONS,applyEvolution,applySummonEvolution,afterEvolutionAttack,tickEvolutions} from '../evolutions.js';
import * as units from '../units.js';
const require=createRequire(import.meta.url);
const {loadModules,createCombatAdapter}=require('../../../server/faction-forge/simulator.cjs');
let serial=0;
const make=(type,props={})=>({id:`${type}-${++serial}`,type,p:0,x:3,y:3,hp:100,maxhp:100,dmg:6,cd:1800,range:4,armor:0,size:1,lvl:1,...props});
function fixture(types,rank=1){
  const allies=types.map((type,i)=>make(type,{x:2+i%3,y:3,lvl:rank})),target=make('vguard',{p:1,x:3,y:4}),neighbor=make('rifleman',{p:1,x:4,y:4});
  const st={field:{units:[...allies,target,neighbor],wards:[],comboFields:[]},comboEnhancements:{p0:[],p1:[]}},events=[];
  const hooks={event:e=>events.push(e),applyDamage:(v,d)=>{v.hp-=d;return d;},healScale:(v,now)=>(v.woundUntil||0)>now?.5:1,
    spawnSummon:(state,parent,type)=>{if(!units.canSpawnSummon(state,parent))return false;state.field.units.push(make(type,{p:parent.p,summoned:true,summonedBy:parent.id}));return true;}};
  return {st,events,hooks,allies,target,neighbor,find:type=>allies.find(v=>v.type===type)};
}
function activate(id,f,now=1000){
  let attacker;
  const {st,target,find,hooks}=f;
  switch(id){
    case 'static-tempest':attacker=find('runebinder');target.bindUntil=now+1200;break;
    case 'thunderclap':attacker=find('guardian');break;
    case 'lightning-echo':attacker=find('phaser');target.lightningChargedBy=0;target.lightningChargedUntil=now+2000;break;
    case 'arcane-fortress':st.field.wards.push({p:0,x:3,y:3,hp:3});tickCombos(st,now,hooks);return;
    case 'blood-chain':attacker=find('blood');break;
    case 'underground-detonation':attacker=find('blood');noteComboAmbush(find('burrower'),target,now);break;
    case 'plague-explosion':attacker=find('blood');target.toxicBySide=0;target.toxicUntil=now+3000;break;
    case 'brood-detonation':attacker=find('blood');break;
    case 'predators-feast':attacker=find('viper');target.woundUntil=now+2000;find('siphonleech').hp=50;break;
    case 'guided-barrage':attacker=find('vrocket');target.markedBySide=0;target.markedUntil=now+2000;break;
    case 'suppression-bombardment':attacker=find('vrocket');target.suppressedBySide=0;target.suppressedUntil=now+2000;break;
    case 'electromagnetic-barrage':attacker=find('vrocket');target.disruptedBySide=0;target.disruptedUntil=now+2000;break;
    case 'fortified-artillery':attacker=find('vrocket');noteComboDamage(find('vguard'),8,st,now,hooks);noteComboDamage(find('shieldcap'),8,st,now,hooks);break;
    case 'precision-execution':attacker=find('marksman');target.markedBySide=0;target.markedUntil=now+2000;break;
    default:throw new Error(id);
  }
  afterComboAttack(attacker,target,now,st,hooks);
}

for(const combo of COMBOS)test(`${combo.name}: Rank I automatic trigger has a real bounded effect and deterministic cooldown`,()=>{
  const f=fixture(combo.units),before=f.target.hp;activate(combo.id,f);
  const ev=f.events.find(e=>e.comboId===combo.id);assert.ok(ev,combo.id);assert.equal(ev.level,1);assert.equal(ev.sourceId!=null,true);
  assert.ok(ev.radius<=COMBO_LIMITS.radius);assert.ok(ev.targets.every(t=>Number.isFinite(t.x)&&Number.isFinite(t.y)&&t.amount<=COMBO_LIMITS.damagePerTarget));
  if(combo.id==='blood-chain'){assert.ok(f.allies[1].chainChargedUntil>1000);}
  else if(combo.id==='arcane-fortress'){assert.ok(f.find('golem').comboShield>0);assert.ok(f.st.field.wards[0].hp>3);}
  else if(combo.id==='brood-detonation')assert.ok(f.find('hellspawn').broodSurgeUntil>1000);
  else if(combo.id==='predators-feast')assert.ok(f.find('siphonleech').hp>50);
  else assert.ok(f.target.hp<before||f.neighbor.hp<100,combo.id);
  const count=f.events.filter(e=>e.comboId===combo.id).length;activate(combo.id,f,1200);assert.equal(f.events.filter(e=>e.comboId===combo.id).length,count);
  const mirrored=fixture(combo.units);activate(combo.id,mirrored);const semantic=e=>({comboId:e.comboId,level:e.level,radius:e.radius,amounts:e.targets.map(t=>t.amount)});
  assert.deepEqual(semantic(ev),semantic(mirrored.events.find(e=>e.comboId===combo.id)));
});

test('all 14 combinations reject a missing partner and promote through battle ranks or explicit enhancements',()=>{
  assert.equal(COMBOS.length,14);
  for(const c of COMBOS){const f=fixture(c.units);f.st.field.units.splice(1,1);activate(c.id,f);assert.equal(f.events.some(e=>e.comboId===c.id),false,c.id);
    const advanced=fixture(c.units,2);activate(c.id,advanced);assert.equal(advanced.events.find(e=>e.comboId===c.id)?.level,2,c.id);
    const legendary=fixture(c.units,2);legendary.allies[0].lvl=3;activate(c.id,legendary);assert.equal(legendary.events.find(e=>e.comboId===c.id)?.level,3,c.id);
    const enhanced=fixture(c.units);enhanced.st.comboEnhancements.p0=[c.id];activate(c.id,enhanced);assert.equal(enhanced.events.find(e=>e.comboId===c.id)?.level,2,c.id);
  }
});

test('a recruited BloodBurst card supplies two bodies and a charged survivor can detonate after its partner dies',()=>{
  const tracker=comboStatus([{type:'blood',count:1,lvl:1}],'Helions');assert.equal(tracker.find(c=>c.id==='blood-chain').missing.length,0);
  const f=fixture(['blood','blood']);const [first,second]=f.allies;afterComboAttack(first,f.target,1000,f.st,f.hooks);
  first.hp=0;f.st.field.units=f.st.field.units.filter(v=>v!==first);second.hp=0;
  const before=f.target.hp;afterComboAttack(second,f.target,1180,f.st,f.hooks);
  assert.ok(f.target.hp<before);assert.equal(f.events.filter(e=>e.comboId==='blood-chain').length,2);
});

test('secondary damage does not recurse, clouds expire, wound reduces healing, and shields are consumed',()=>{
  const f=fixture(['defiler','blood']);f.target.toxicBySide=0;f.target.toxicUntil=5000;afterComboAttack(f.find('blood'),f.target,1000,f.st,f.hooks);
  const count=f.events.filter(e=>e.kind==='combo').length;for(const now of [1800,2600,3400,5000])tickCombos(f.st,now,f.hooks);
  assert.equal(f.events.filter(e=>e.kind==='combo').length,count);assert.equal(f.st.field.comboFields.length,0);
  const feast=fixture(['viper','carrion','siphonleech']);feast.target.woundUntil=5000;const leech=feast.find('siphonleech');leech.hp=50;leech.woundUntil=5000;activate('predators-feast',feast);assert.equal(leech.hp,51);
  leech.comboShield=3;leech.comboShieldUntil=2000;assert.equal(units.absorbForgeShield(leech,5,1000),2);assert.equal(leech.comboShield,0);assert.equal(units.absorbForgeShield(leech,5,1000),5);
});

for(const [type,paths] of Object.entries(EVOLUTIONS))for(const path of Object.values(paths))test(`${type}: ${path.name} applies its explicit tradeoff and signature once at Rank II`,()=>{
  const u=make(type,{lvl:2,healAmt:3,healRange:3,healCd:1200,wardDur:5000,wardCd:2000,wardHp:4,wardDmg:2,wardRange:3,wardCdShoot:1400,broodCd:3500,shockDur:1500,bindDur:1200,woundDur:2400,suppressDur:900,acidDur:3500,burrowCd:5000,burrowDist:3,leapDist:2,auraRange:2,rangedReduce:2});
  const prior=structuredClone(u);applyEvolution(u,path.id);assert.equal(u.evolution,path.id);assert.equal(u.evolutionSignature,path.signature);assert.ok(path.description&&path.tradeoff);
  assert.ok(Object.keys(path.mods).some(k=>{const name=k.endsWith('Add')?k.slice(0,-3):k;return u[name]!==prior[name];}),`${type} ${path.id} must change applied behavior`);
  const once=structuredClone(u);applyEvolution(u,path.id);assert.deepEqual(u,once);
  const standard=make(type);applyEvolution(standard,path.id);assert.equal(standard.evolution,undefined);
  const legendary=make(type,{lvl:3});applyEvolution(legendary,path.id);assert.equal(legendary.evolutionPower,1.25);
});

test('evolution roster covers the authoritative 32-unit catalog and ranked specialization never derives from Forge tier',async()=>{
  const modules=await loadModules(),adapter=createCombatAdapter({seed:'evolution-catalog',modules});
  assert.deepEqual(Object.keys(EVOLUTIONS).sort(),Object.keys(adapter.troops).sort());
  const raw=make('storm',{lvl:2});const a=units.initializeForgeUnit({...raw},{ranked:true,forgeTier:1}),b=units.initializeForgeUnit({...raw},{ranked:true,forgeTier:3});applyEvolution(a,'b');applyEvolution(b,'b');
  for(const prop of ['hp','maxhp','dmg','cd','chainHops','evolutionSignature'])assert.equal(a[prop],b[prop]);
});

test('specializations produce real burst, sweep, outer explosion, fields, shields, healing, summons and control',()=>{
  for(const [type,path,kind] of [['rifleman','a','evolution-echo'],['blade','b','slash'],['vrocket','b','rocket'],['storm','b','lightning-field'],['blood','b','blood-field'],['spitter','a','toxic-cloud'],['emp','a','lightning-field'],['runebinder','a','binding'],['suppressor','a','suppression'],['carrion','a','wound']]){
    const f=fixture([type],2),u=f.allies[0];applyEvolution(u,path);for(let i=0;i<3;i++)afterEvolutionAttack(u,f.target,1000+i*180,f.st,f.hooks);assert.ok(f.events.some(e=>e.kind===kind),`${type}/${path}`);
  }
  const heavy=fixture(['vrocket'],2);heavy.neighbor.x=5;applyEvolution(heavy.allies[0],'a');afterEvolutionAttack(heavy.allies[0],heavy.target,1000,heavy.st,heavy.hooks);assert.ok(heavy.neighbor.hp<100);assert.equal(heavy.target.hp,100);
  const healer=fixture(['medic'],2);healer.allies[0].hp=50;applyEvolution(healer.allies[0],'a');tickEvolutions(healer.st,1000,healer.hooks);assert.ok(healer.allies[0].hp>50);
  const tank=fixture(['vguard'],2);applyEvolution(tank.allies[0],'b');tickEvolutions(tank.st,1000,tank.hooks);assert.ok(tank.allies[0].comboShield>0);
  const parent=make('matron',{lvl:2,broodCd:3500});applyEvolution(parent,'b');const spawn=make('hellspawn',{hp:9,maxhp:9,dmg:2});applySummonEvolution(parent,spawn);assert.equal(spawn.maxhp,12);assert.equal(spawn.dmg,3);
});

test('shared authoritative replays retain evolution/combo events, remain byte-identical, and bound accumulated bonus damage',async()=>{
  const modules=await loadModules();const roster=(types)=>types.map(type=>({type,count:2,lvl:2}));
  const st={roster:{p0:roster(['storm','runebinder','guardian']),p1:roster(['defiler','blood','burrower'])},factions:{p0:'Aesir',p1:'Helions'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},evolutions:{p0:{storm:'b',guardian:'b'},p1:{blood:'b'}},comboEnhancements:{p0:['static-tempest'],p1:['underground-detonation']}};
  const a=createCombatAdapter({seed:'real-combos',modules}).battle(st),b=createCombatAdapter({seed:'real-combos',modules}).battle(st);assert.deepEqual(a,b);
  assert.ok(a.frames.some(f=>f.syncedFx.some(e=>e.kind==='combo')));assert.ok(a.frames[0].units.some(u=>u.evolution==='b'));assert.ok(a.durationMs<200000);
  const all=fixture(['vrocket','signalhawk','suppressor','emp','vguard','shieldcap'],3);all.target.markedBySide=0;all.target.markedUntil=5000;all.target.suppressedBySide=0;all.target.suppressedUntil=5000;all.target.disruptedBySide=0;all.target.disruptedUntil=5000;all.st.field.artilleryCharge={p0:40};
  const before=all.target.hp+all.neighbor.hp;units.afterForgeAttack(all.find('vrocket'),all.target,1000,all.st,all.hooks);assert.ok(before-all.target.hp-all.neighbor.hp<=12);
});

test('every one of the 64 paths executes through the real adapter at Rank II and Rank III',async()=>{
  const modules=await loadModules();
  for(const [type,paths] of Object.entries(EVOLUTIONS))for(const path of Object.keys(paths))for(const lvl of [2,3]){
    const faction=modules.units.NEW_UNITS[type]?.faction||'Vanir';
    const st={roster:{p0:[{type,count:1,lvl}],p1:[{type:'vguard',count:1,lvl:2},{type:'rifleman',count:1,lvl:1}]},factions:{p0:faction,p1:'Vanir'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},evolutions:{p0:{[type]:path},p1:{}},comboEnhancements:{p0:[],p1:[]}};
    const battle=createCombatAdapter({seed:`path-${type}-${path}-${lvl}`,modules}).battle(st);
    assert.ok(battle.frames[0].units.filter(u=>u.p===0).every(u=>u.evolution===path),`${type}/${path}/${lvl}`);
    assert.ok(battle.durationMs<200000);assert.ok(battle.frames.every(f=>f.units.length<=192));
  }
});

test('the triggering attack budget also caps damage dealt by supporting casters and rail units',()=>{
  for(const lvl of [1,3]){
    const f=fixture(['storm','runebinder'],lvl);for(let i=0;i<3;i++)f.st.field.units.push(make('rifleman',{p:1,x:2+i,y:5}));
    f.target.bindUntil=5000;const before=f.st.field.units.filter(v=>v.p===1).reduce((sum,v)=>sum+v.hp,0);
    units.afterForgeAttack(f.find('runebinder'),f.target,1000,f.st,f.hooks);
    const after=f.st.field.units.filter(v=>v.p===1).reduce((sum,v)=>sum+v.hp,0);
    assert.ok(before-after<=(lvl===3?12:8));assert.equal(f.st.field._secondaryBudget,undefined);
    if(lvl===3)assert.equal(before-after,12);
  }
});

test('seeded counterplay: a six-card Rocket Battery can break a six-card Arcane Fortress at starting and Legendary ranks',async()=>{
  const modules=await loadModules(),fortress=['golem','wardmaker','architect','guardian','blade','phaser'],battery=['vguard','shieldcap','vrocket','vrocket','signalhawk','medic'];
  for(const lvl of [1,3]){
    let wins=0;
    for(const flip of [false,true]){
      const left=flip?battery:fortress,right=flip?fortress:battery;
      const st={roster:{p0:left.map(type=>({type,count:1,lvl})),p1:right.map(type=>({type,count:1,lvl}))},factions:{p0:flip?'Vanir':'Aesir',p1:flip?'Aesir':'Vanir'},formations:{p0:'balanced',p1:'wide'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]}};
      const result=createCombatAdapter({seed:'probe-0-3',modules}).battle(st,{ranked:true});wins+=result.winnerSide===(flip?0:1)?1:0;
    }
    assert.ok(wins>=1,`Rocket Battery needs counterplay at rank ${lvl}`);
  }
});

test('rocket impacts damage nearby enemy wards once and never damage friendly wards',()=>{
  const f=fixture(['vrocket']),u=f.allies[0];f.st.field.wards=[{p:1,x:3,y:4,hp:4,until:5000},{p:0,x:4,y:4,hp:4,until:5000},{p:1,x:7,y:7,hp:4,until:5000}];
  units.afterForgeAttack(u,f.target,1000,f.st,f.hooks);assert.deepEqual(f.st.field.wards.map(w=>w.hp),[2,4,4]);assert.ok(f.events.some(e=>e.kind==='ward-break'));
});

test('extra fields, control and swarm creation stay capped even with multiple evolved sources',()=>{
  const f=fixture(Array.from({length:12},()=> 'blood'),2);
  for(const u of f.allies){applyEvolution(u,'b');afterEvolutionAttack(u,f.target,1000,f.st,f.hooks);}
  assert.equal(f.st.field.comboFields.length,COMBO_LIMITS.fieldsPerSide);
  const brood=fixture(['matron','blood','hellspawn'],3),parent=brood.find('matron');
  for(let i=0;i<units.UNIT_LIMITS.summonsPerParent;i++)brood.st.field.units.push(make('hellspawn',{summoned:true,summonedBy:parent.id}));
  const count=brood.st.field.units.length;activate('brood-detonation',brood);assert.equal(brood.st.field.units.length,count);
});

test('seeded counterplay: Rank II Assassin Swarm pressures a concentrated Rocket Battery without a guaranteed winner',async()=>{
  const modules=await loadModules(),battery=['vguard','shieldcap','vrocket','vrocket','signalhawk','medic'],assassins=['viper','viper','burrower','hellspawn','matron','hiveherald'];
  let assassinWins=0;
  for(let seed=0;seed<3;seed++)for(const flip of [false,true]){
    const left=flip?assassins:battery,right=flip?battery:assassins;
    const st={roster:{p0:left.map(type=>({type,count:1,lvl:2})),p1:right.map(type=>({type,count:1,lvl:2}))},factions:{p0:flip?'Helions':'Vanir',p1:flip?'Vanir':'Helions'},formations:{p0:seed%2?'wide':'balanced',p1:seed%2?'balanced':'wide'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]}};
    const result=createCombatAdapter({seed:`combo-balance-2-6-7-${seed}`,modules}).battle(st,{ranked:true});assassinWins+=result.winnerSide===(flip?0:1)?1:0;
  }
  assert.ok(assassinWins>0&&assassinWins<6,`Expected meaningful counterplay, got ${assassinWins}/6`);
});
