import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {LANDS} from './lands.mjs';
import {speciesHomes,regionSpeciesPool} from './regional-adventures.mjs';

const additions=Object.values(C.SPECIES).filter(s=>s.expansion===2);
const originalIds='capuchin macaque langur mandrill spider howler mossmop thornback bloomtail vineknuckle cinderpaw ashmane lanternMarmoset kilnfist brooktail bubblecheek reefguard cascadeHowler sparkskip stormtuft cloudcap thunderfang pebblepalm quartzcrest dunetail snowmantle iciclefang frostwhisker sunbloom dawncrest duskmask nightfrill wildvine steampelt plasmafist magmajaw cinderfrost sunflare ashshade reedrunner tempestvine rootstone frostfern gloamorchid typhoontail siltguard glacierflow moonripple fulgurite auroraswing eclipsemask obsidianback solkar verdara tiderex voltaro cryomane obsidrax noctyra oroluma'.split(' ');
const save=(seed=91)=>C.newSave('Roster Trial','capuchin','practice',seed);
function specimen(s,species){const p=structuredClone(s.run.monkey);p.id=`specimen-${species}`;p.species=species;p.types=[...C.SPECIES[species].types];p.level=10;p.stage=0;p.learned=[];p.loadout=['strike','brace','breathe'];p.learned=C.unlocked(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;return p;}
function specialtyBattle(species){const s=save(),p=specimen(s,species);p.stage=1;p.learned=C.unlocked(p);p.loadout=['strike',C.SPECIES[species].specialty,'brace','breathe'];p.energy=C.stats(p).maxEnergy;p.hp=20;s.collection[p.id]=p;s.run.party=[p.id];s.run.activeId=p.id;s.run.monkey=p;const reserve=structuredClone(p);reserve.id='reserve';reserve.hp=20;reserve.energy=1;s.collection.reserve=reserve;s.run.party.push('reserve');C.advance(s,C.ENCOUNTER_STEP,0,0);C.beginPendingEncounter(s);C.readyBattle(s);const e=s.run.battle.enemy;e.hp=10000;e.loadout=['strike'];e.types=[];e.status={};e.energy=10;s.run.battle.enemyAction={type:'move',id:'focus'};s.run.battle.intent='focus';return s;}

test('the thirty appended identities preserve the original sixty catalogue numbers and requested rarity split',()=>{
  assert.deepEqual(Object.keys(C.SPECIES).slice(0,60),originalIds);
  assert.equal(Object.keys(C.SPECIES).length,110);assert.equal(additions.length,30);
  assert.equal(additions.filter(s=>['common','uncommon'].includes(s.rarity)).length,18);
  assert.equal(additions.filter(s=>s.rarity==='rare').length,8);assert.equal(additions.filter(s=>s.rarity==='ultra').length,4);
  assert.deepEqual(additions.map(s=>s.number),Array.from({length:30},(_,i)=>61+i));
  assert.equal(new Set(additions.map(s=>s.appearance)).size,30);
  assert.equal(new Set(additions.map(s=>s.specialty)).size,30);
  for(const s of additions){assert.ok(s.appearance.length>40);assert.equal(s.evolutionAppearances.length,3);assert.ok(s.types.length>=1&&s.types.length<=2);assert.ok(s.learnset.includes(s.specialty));assert.ok(s.learnset.every(id=>C.MOVES[id]));assert.ok(s.hp>=69&&s.hp<=110);assert.ok(s.atk>=9&&s.atk<=16);assert.ok(s.def>=7&&s.def<=16);assert.ok(s.speed>=3&&s.speed<=22);}
});

test('all new ordinary species enter depth-gated regional encounters, while Ultras stay exclusive map sightings',()=>{
  const s=save();s.run.distance=0;assert.ok(additions.every(p=>!C.encounterPool(s).includes(p.id)));
  s.world.totalDistance=150000;s.world.unlocked=LANDS.map(l=>l.id);
  const middle=new Set(),deep=new Set(),seen=new Set(),ultras=new Set();
  for(const land of LANDS){C.selectLand(s,land.id);s.run.distance=6000*C.FEET;C.encounterPool(s).forEach(id=>middle.add(id));s.run.distance=20000*C.FEET;C.encounterPool(s).forEach(id=>deep.add(id));
    for(let seed=1;seed<=350;seed++){
      const ordinary=save(seed*7919);ordinary.world.totalDistance=150000;ordinary.world.unlocked=LANDS.map(l=>l.id);C.selectLand(ordinary,land.id);ordinary.run.ordinaryWins=2;ordinary.run.encounters=5;ordinary.run.distance=19999*C.FEET;ordinary.run.nextEncounter=20000*C.FEET;ordinary.run.nextCamp=24000*C.FEET;C.advance(ordinary,C.FEET,0,0);C.beginPendingEncounter(ordinary);for(const e of ordinary.run.battle.enemies)seen.add(e.species);
      const hunt=save(seed*3571);hunt.world.totalDistance=150000;hunt.world.unlocked=LANDS.map(l=>l.id);C.selectLand(hunt,land.id);hunt.run.ordinaryWins=2;hunt.run.distance=5000*C.FEET;hunt.run.nextEncounter=6000*C.FEET;hunt.run.nextCamp=6000*C.FEET;C.advance(hunt,1,0,0);assert.ok(regionSpeciesPool(hunt,Object.keys(C.SPECIES),{ultra:true}).includes(hunt.run.ultra.species));ultras.add(hunt.run.ultra.species);
    }
  }
  assert.ok(additions.filter(p=>['common','uncommon'].includes(p.rarity)).every(p=>middle.has(p.id)));
  assert.ok(additions.filter(p=>['rare','ultra'].includes(p.rarity)).every(p=>!middle.has(p.id)));
  assert.equal(deep.size,94);assert.ok(additions.filter(p=>p.rarity!=='ultra').every(p=>deep.has(p.id)));assert.ok(additions.filter(p=>p.rarity==='ultra').every(p=>!deep.has(p.id)));
  assert.ok(C.SPECIES.acornbuckler.encounterWeight>C.SPECIES.pitcherwink.encounterWeight);assert.ok(C.SPECIES.pitcherwink.encounterWeight>C.SPECIES.bramblebat.encounterWeight);
  assert.ok(additions.filter(p=>p.rarity!=='ultra').every(p=>seen.has(p.id)),`unseen ordinary additions: ${additions.filter(p=>p.rarity!=='ultra'&&!seen.has(p.id)).map(p=>p.id)}`);
  assert.ok(additions.filter(p=>p.rarity==='ultra').every(p=>ultras.has(p.id)));
  assert.equal(seen.size,94,'all ordinary species are reachable through actual generated regional encounters');assert.equal(ultras.size,12,'all Ultras are reachable through regional sightings');
});

test('every addition can be captured, join a three-member team, evolve three times and retain a legal four-card loadout',()=>{
  for(const species of additions){const s=save(),candidate=specimen(s,species.id);s.run.phase='befriend';s.run.capture={id:`capture-${species.id}`,candidates:[candidate],attempts:0,guaranteed:true,ultra:false,claimed:false};s.run.items.bondCharm=1;const result=C.befriend(s,candidate.id);assert.equal(result.success,true,species.id);const p=result.monkey;assert.equal(p.species,species.id);assert.equal(p.bossOnly,undefined);C.setParty(s,[s.run.activeId,p.id]);p.level=10;for(const type of Object.keys(C.ELEMENTS))s.run.essence[type]=100;
    for(let stage=1;stage<=3;stage++){const type=stage===2?species.types[1]||species.types[0]:species.types[0];C.evolve(s,type,p.id);assert.equal(p.stage,stage);assert.equal(p.species,species.id);assert.ok(C.stats(p).hp>species.hp);}
    C.setMove(s,1,species.specialty,p.id);assert.equal(p.loadout.length,4);assert.equal(p.loadout[0],'strike');assert.equal(new Set(p.loadout).size,4);assert.ok(C.validate(JSON.parse(JSON.stringify(s))).collection[p.id].loadout.includes(species.specialty));
  }
});

test('each new Ultra appears as a map boss, equips its exclusive signature and offers one free first recruitment',()=>{
  for(const species of additions.filter(s=>s.rarity==='ultra')){
    let s;for(let seed=1;seed<=1000;seed++){const candidate=save(seed*3571);candidate.world.totalDistance=150000;candidate.world.unlocked=LANDS.map(l=>l.id);C.selectLand(candidate,speciesHomes(species.id)[0]);candidate.run.ordinaryWins=2;candidate.run.distance=5000*C.FEET;candidate.run.nextEncounter=6000*C.FEET;candidate.run.nextCamp=6000*C.FEET;C.advance(candidate,1,0,0);if(candidate.run.ultra.species===species.id){s=candidate;break;}}
    assert.ok(s,`${species.id} never sighted`);const sightingId=s.run.ultra.id;C.beginUltra(s,{gpsFresh:true,withinRadius:true});assert.equal(s.run.battle.id,sightingId);const boss=s.run.battle.enemies[0];assert.equal(boss.species,species.id);assert.equal(boss.bossOnly,true);assert.ok(boss.loadout.includes(species.signature));assert.ok(C.stats(boss).hp>C.stats({...boss,bossOnly:false}).hp);
    C.readyBattle(s);s.run.monkey.level=60;s.run.monkey.hp=C.stats(s.run.monkey).hp;for(const e of s.run.battle.enemies)e.hp=1;
    for(let i=0;i<4&&s.run.phase==='battle';i++){s.run.battle.enemyAction={type:'move',id:'focus'};s.run.battle.intent='focus';C.turn(s,'strike');}
    assert.equal(s.run.phase,'befriend');assert.equal(s.run.capture.candidates.length,1);assert.equal(s.run.capture.candidates[0].species,species.id);s.run.items.bondCharm=0;const result=C.befriend(s,s.run.capture.candidates[0].id);assert.equal(result.success,true);assert.equal(result.monkey.species,species.id);assert.equal(result.monkey.bossOnly,undefined);assert.equal(result.monkey.elite,undefined);assert.equal(s.run.ultra.status,'claimed');assert.equal(s.meta.claims[`capture:${sightingId}`],true);assert.throws(()=>C.beginUltra(s,{gpsFresh:true,withinRadius:true}));C.validate(structuredClone(s));
  }
});

test('all thirty specialties execute their actual damage, support, status, relay and cooldown effects',()=>{
  for(const species of additions){const s=specialtyBattle(species.id),p=s.run.monkey,e=s.run.battle.enemy,m=C.MOVES[species.specialty],hp=p.hp,reserve=s.collection.reserve,energy=p.energy;
    C.turn(s,species.specialty,false,{targetId:'reserve'});
    if(m.power)assert.ok(e.hp<10000,species.id);
    if(m.shield)assert.ok(p.shield>0,species.id);
    if(m.counter)assert.equal(p.counter,m.counter,species.id);
    if(m.status)assert.ok(e.status[m.status]>0,species.id);
    if(m.weaken)assert.equal(e.statusPower.weaken,m.weaken,species.id);
    if(m.drain)assert.ok(p.hp>hp,species.id);
    if(m.teamHeal)assert.ok(reserve.hp>20,species.id);
    if(m.drainEnergy)assert.ok(e.energy<=10-m.drainEnergy+1,species.id);
    if(m.switch)assert.equal(s.run.activeId,'reserve',species.id);
    if(m.teamShield&&m.switch)assert.ok(reserve.shield>0,species.id);
    if(m.transferSpeed)assert.ok(reserve.status.speed>0,species.id);
    if(m.cooldown)assert.equal(p.cooldowns[species.specialty],1+m.cooldown,species.id);
    assert.equal(p.uses[species.specialty],1);assert.ok(p.energy<=energy+1,species.id);
  }
});

test('eligibility explains stage locks and prevents foreign Ultra signatures even if copied into learned history',()=>{
  const s=save(),p=specimen(s,'lumenmoth');assert.equal(C.abilityInfo(p,'pollenBeacon').eligible,false);assert.match(C.abilityInfo(p,'pollenBeacon').lockedReason,/Awakened/);p.stage=1;assert.equal(C.abilityInfo(p,'pollenBeacon').eligible,true);
  p.learned.push('zeroHourRelay');const blocked=C.abilityInfo(p,'zeroHourRelay');assert.equal(blocked.eligible,false);assert.match(blocked.lockedReason,/Chronogibbon/);assert.equal(C.unlocked(p).includes('zeroHourRelay'),false);
  for(const ultra of additions.filter(p=>p.rarity==='ultra')){const owner=specimen(s,ultra.id);assert.equal(C.abilityInfo(owner,ultra.signature).eligible,true);assert.equal(C.MOVES[ultra.signature].family,'ultra');assert.ok(C.MOVES[ultra.signature].cost>=8);assert.ok(C.MOVES[ultra.signature].cooldown>=4);assert.equal(Object.values(C.SPECIES).filter(s=>s.learnset.includes(ultra.signature)).length,1);}
  assert.equal(C.abilityInfo(p,'strike').required,true);assert.equal(C.abilityInfo(p,'missing'),null);
});
