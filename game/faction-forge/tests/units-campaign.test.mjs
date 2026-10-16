import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import * as units from '../units.js';
import * as campaign from '../campaign.js';
import {validateDeck,rosterCount,STARTER_DECKS} from '../decks.js';

const make=(type,props={})=>units.initializeForgeUnit({id:type,p:0,type,x:3,y:3,size:1,lvl:1,
  maxhp:20,hp:20,dmg:5,cd:1000,range:3,armor:0,...props},{forgeTier:props.forgeTier||1,role:units.NEW_UNITS[type]?.role||'back'});
const state=(...troops)=>({field:{units:troops,wards:[]}});
const hooks={applyDamage:(v,d)=>v.hp-=d,healScale:(v,now)=>(v.woundUntil||0)>now?.5:1};

test('campaign and endless enemies use five-type faction decks and never grow beyond five deployed bodies',()=>{
 for(const mission of [...campaign.MISSIONS,...[1,7,30,80,10000].map(stage=>campaign.createEndlessMission(stage))]){const {deck,roster}=mission.opponent;assert.equal(validateDeck(deck).valid,true,mission.id);assert.ok(rosterCount(roster)<=5,mission.id);assert.ok(roster.every(row=>deck.troops.includes(row.type)),mission.id);assert.equal(deck.faction,mission.aiFaction);}
 const custom={id:'custom-campaign',name:'Flanking Precision',faction:'Vanir',troops:['miner','marksman','vrocket','emp','medic']};
 for(const mission of campaign.MISSIONS){const roster=campaign.playerStartingRoster('Vanir',mission,custom);assert.ok(rosterCount(roster)<=5);assert.ok(roster.every(row=>custom.troops.includes(row.type)));}
 for(let i=0;i<20;i++){const bot=campaign.practiceOpponent(1700,()=>i/20);assert.equal(validateDeck(bot.deck).valid,true);assert.equal(bot.deck.faction,bot.faction);}
});
test('expert bots select complete counter decks in response to armor or exposed ranged troops',()=>{
 const armor={troops:['vguard','shieldcap','guardian','golem','medic']},ranged={troops:['marksman','vrocket','rifleman','medic','emp']};const antiArmor=campaign.chooseAIDeck('Helions',armor,1,()=>.2),ambush=campaign.chooseAIDeck('Helions',ranged,1,()=>.2);assert.equal(validateDeck(antiArmor).valid,true);assert.equal(validateDeck(ambush).valid,true);assert.ok(antiArmor.troops.includes('spitter'));assert.ok(ambush.troops.includes('viper'));assert.notDeepEqual(antiArmor.troops,ambush.troops);assert.deepEqual(campaign.chooseAIDeck('Helions',armor,1,()=>.2),antiArmor);
});

test('the actual legacy troop tables expand once to 32 and faction counts remain correct',()=>{
  const html=readFileSync(new URL('../../crowncouncil.html',import.meta.url),'utf8');
  const context=vm.createContext({FFUnits:units});
  vm.runInContext(html.slice(html.indexOf('const R ='),html.indexOf('const TROOP_IMAGE_VERSION'))+'\nthis.TROOPS=TROOPS;',context);
  const tables={TROOPS:context.TROOPS,FACTIONS:{Vanir:Object.keys(context.TROOPS).filter(k=>context.TROOPS[k].faction==='Vanir'),Aesir:Object.keys(context.TROOPS).filter(k=>context.TROOPS[k].faction==='Aesir'),Helions:Object.keys(context.TROOPS).filter(k=>context.TROOPS[k].faction==='Helions')},LEGENDARY_ABILITIES:{},TROOP_COUNTERS:{},TROOP_ARCHETYPES:{},TROOP_VISUALS:{}};
  units.installUnits(tables);units.installUnits(tables);
  assert.equal(Object.keys(tables.TROOPS).length,32);
  assert.deepEqual(Object.fromEntries(Object.entries(tables.FACTIONS).map(([k,v])=>[k,v.length])),{Vanir:11,Aesir:10,Helions:11});
  for(const key of units.NEW_UNIT_KEYS){assert.equal(tables.TROOPS[key].unlockable,true);assert.ok(tables.LEGENDARY_ABILITIES[key].desc);assert.ok(tables.TROOP_COUNTERS[key].weakAgainst.length);}
});

test('permanent tier and temporary battle rank remain separate, with ranked stats normalized',()=>{
  const raw={id:'v',p:0,type:'railbreaker',lvl:2,hp:13,maxhp:13,dmg:8,cd:1900};
  const ranked=units.initializeForgeUnit({...raw},{forgeTier:3,ranked:true,role:'back'});
  assert.equal(ranked.forgeTier,3);assert.equal(ranked.effectiveForgeTier,1);assert.equal(ranked.battleRank,2);assert.equal(ranked.legendary,false);
  assert.equal(ranked.maxhp,raw.maxhp);assert.equal(ranked.cd,raw.cd);
  const elite=units.initializeForgeUnit({...raw,lvl:3},{forgeTier:1,role:'back'});
  assert.equal(elite.forgeTier,1);assert.equal(elite.legendary,true);
  const upgraded=units.initializeForgeUnit({...raw},{forgeTier:3,role:'back'});
  assert.ok(upgraded.maxhp>ranked.maxhp);assert.ok(upgraded.cd<ranked.cd);
});

test('Railbreaker charges, penetrates a straight line, and never hits an off-line neighbor',()=>{
  const rail=make('railbreaker',{x:1,y:1,range:7,dmg:8,lvl:3});
  const target=make('vguard',{id:'target',p:1,x:1,y:3,armor:3,armorType:'armored'});
  const behind=make('rifleman',{id:'behind',p:1,x:1,y:4});
  const off=make('rifleman',{id:'off',p:1,x:2,y:4});
  const st=state(rail,target,behind,off);
  assert.equal(units.attackForgeUnit(rail,target,1000,st,hooks),true);assert.equal(target.hp,20);
  units.attackForgeUnit(rail,target,1400,st,hooks);assert.equal(target.hp,20);
  units.attackForgeUnit(rail,target,1600,st,hooks);assert.ok(target.hp<20);assert.ok(behind.hp<20);assert.equal(off.hp,20);
  assert.ok(rail.nextAtk>1600);assert.equal(rail.railShots,1);
});

test('multiple Signal Hawks never stack the mark damage bonus and reveal only local threats',()=>{
  const hawk=make('signalhawk',{id:'hawk',x:2,y:2,lvl:3}),hawk2=make('signalhawk',{id:'hawk2',x:3,y:2});
  const ally=make('rifleman',{id:'ally',x:2,y:3});
  const target=make('burrower',{p:1,x:2,y:4}),second=make('miner',{p:1,x:4,y:4}),far=make('burrower',{id:'far',p:1,x:7,y:12});
  const st=state(hawk,hawk2,ally,target,second,far);
  units.tickForgeAbilities(st,1000);
  assert.equal(target.markedBySide,0);assert.ok(second.markedUntil>1000);assert.ok(target.revealedUntil>1000);assert.equal(far.revealedUntil,undefined);
  assert.equal(units.markedDamageBonus(ally,target,1000,st),1);
  hawk.statusShockUntil=2000;hawk2.statusShockUntil=2000;assert.equal(units.markedDamageBonus(ally,target,1000,st),0);
});

test('temporal fields have duration, side caps, and non-additive cooldown stacking',()=>{
  const casters=Array.from({length:8},(_,i)=>make('chronoweaver',{id:'c'+i}));
  const ally=make('rifleman',{x:3,y:4}),enemy=make('vguard',{p:1,x:3,y:5});
  const st=state(...casters,ally,enemy);
  units.tickForgeAbilities(st,1000);
  assert.equal(st.field.temporalFields.length,units.UNIT_LIMITS.temporalFieldsPerSide);
  assert.equal(units.attackCooldown(ally,1000,st),880);assert.equal(units.attackCooldown(enemy,1000,st),1200);
  assert.equal(units.attackCooldown(enemy,5000,st),1000);
  for(const caster of casters)caster.statusShockUntil=2000;
  assert.equal(units.attackCooldown(enemy,1000,st),1000);
});

test('decoys divert single-target attacks, expire, and respect per-side summon caps',()=>{
  const warden=make('miragewarden',{id:'warden',lvl:3}),enemy=make('marksman',{p:1,x:3,y:5,range:6});
  const decoy=make('miragewarden',{id:'illusion',x:3,y:4,p:0,illusion:true,summoned:true,summonedBy:'warden',expiresAt:2000});
  const st=state(warden,enemy,decoy);
  assert.equal(units.chooseForgeTarget(enemy,st.field.units,1000),decoy);
  assert.equal(units.chooseForgeTarget({...enemy,chain3:true},st.field.units,1000),null);
  units.tickForgeAbilities(st,2200);assert.equal(decoy.hp,0);
  const capped=state(warden,...Array.from({length:4},(_,i)=>({...decoy,id:'d'+i,hp:4})));
  assert.equal(units.canSpawnSummon(capped,warden,true),false);
  assert.equal(units.canSpawnSummon(state(warden,...Array.from({length:32},(_,i)=>make('hellspawn',{id:'s'+i,summoned:true}))),warden),false);
});

test('Siphon Leech heals actual damage with a cap, respects wound, and shares only excess',()=>{
  const leech=make('siphonleech',{lvl:3}),ally=make('hellspawn',{id:'ally',hp:10,maxhp:20,x:4});
  const victim=make('rifleman',{p:1}),st=state(leech,ally,victim);
  units.siphonAfterDamage(leech,victim,100,1000,st,hooks);assert.equal(ally.hp,11);assert.equal(leech.hp,20);
  leech.hp=5;units.siphonAfterDamage(leech,victim,100,1000,st,hooks);assert.equal(leech.hp,7);assert.equal(ally.hp,11);
  leech.woundUntil=2000;units.siphonAfterDamage(leech,victim,100,1000,st,hooks);assert.equal(leech.hp,8);
  units.siphonAfterDamage(leech,victim,0,1000,st,hooks);assert.equal(leech.hp,8);
});

test('Hive Herald improves newborn swarms once and death frenzy never stacks additively',()=>{
  const herald=make('hiveherald',{lvl:3}),swarm=make('hellspawn',{id:'swarm',summoned:true,x:4}),dead=make('hellspawn',{id:'dead',x:4,hp:0}),enemy=make('rifleman',{p:1,x:3,y:6});
  const st=state(herald,swarm,dead,enemy),initial=swarm.dmg;
  units.tickForgeAbilities(st,1000);units.tickForgeAbilities(st,1500);
  assert.equal(swarm.dmg,initial+1);assert.equal(units.attackCooldown(swarm,1500,st),900);
  units.noteForgeDeath(dead,st,1500);units.noteForgeDeath(dead,st,1500);assert.equal(units.attackCooldown(swarm,1500,st),820);
  units.tickForgeAbilities(st,5000);assert.equal(swarm.dmg,initial);assert.equal(units.attackCooldown(swarm,5000,st),900);
});

test('30 distinct campaign missions gate in order and unlock all six troops at the specified milestones',()=>{
  assert.equal(campaign.MISSIONS.length,30);assert.equal(new Set(campaign.MISSIONS.map(m=>m.id)).size,30);
  assert.equal(campaign.MISSIONS.filter(m=>m.boss).length,5);
  assert.equal(campaign.getMission('c4m2').id,'forge-20');
  assert.equal(campaign.missionAccessible('forge-02',{}),false);
  assert.equal(campaign.nextMission({}).id,'forge-01');
  const progress={missions:Object.fromEntries(campaign.MISSIONS.map(m=>[m.id,{completed:true,stars:3}]))};
  assert.deepEqual(campaign.campaignCompletion(progress),{completed:30,total:30,stars:90,maxStars:90,percent:100,endlessUnlocked:true});
  assert.deepEqual(Object.values(campaign.CAMPAIGN_UNLOCKS).sort(),[...units.NEW_UNIT_KEYS].sort());
  assert.equal(campaign.missionStars({won:false,lives:4}),0);assert.equal(campaign.missionStars({won:true,lives:3}),3);
  const endless=campaign.createEndlessMission(100);assert.equal(endless.id,'endless-100');assert.ok(endless.startingRoster.every(r=>r.count<=3&&r.lvl<=3));
});

test('strategic AI selects a counter or synergy from the issued pack with deterministic mistakes',()=>{
  const pack=[{type:'troop',key:'railbreaker'},{type:'troop',key:'hiveherald'},{type:'troop',key:'signalhawk'}];
  assert.equal(campaign.chooseAIDraft(pack,{myRoster:[{type:'vguard',count:1},{type:'rifleman',count:1}],enemyRoster:[{type:'golem',count:2}],difficulty:1},()=>.99),0);
  assert.equal(campaign.chooseAIDraft(pack,{myRoster:[{type:'hellspawn',count:2},{type:'matron',count:1}],enemyRoster:[],difficulty:1},()=>.99),1);
  assert.equal(campaign.chooseAIDraft([],{},()=>.5),-1);
  for(let i=0;i<10;i++){const index=campaign.chooseAIDraft(pack,{difficulty:0},()=>i/10);assert.ok(index>=0&&index<pack.length);}
});

const placementHooks={
  width:8,height:16,
  roleFor:type=>['vguard','golem','siphonleech','hellspawn'].includes(type)?'front':'back',
  rectangleFree:(units,x,y,size)=>x>=0&&y>=0&&x+size<=8&&y+size<=16&&!units.some(v=>v.hp>0&&x<v.x+(v.size||1)&&x+size>v.x&&y<v.y+(v.size||1)&&y+size>v.y),
  canMoveTo:(u,units,x,y)=>placementHooks.rectangleFree(units.filter(v=>v.id!==u.id),x,y,u.size||1)
};

test('formations place whole 2×2 footprints legally and drafted relics/tactics apply once',()=>{
  const golem=make('golem',{x:2,y:10,size:2,range:1}),guard=make('vguard',{x:5,y:10,range:1}),rifle=make('rifleman',{x:3,y:15,dmg:10}),medic=make('medic',{x:5,y:15});
  const enemy=make('vguard',{p:1,x:4,y:3,maxhp:22,hp:22,range:1}),enemyBack=make('rifleman',{p:1,x:4,y:0});
  const st=state(golem,guard,rifle,medic,enemy,enemyBack);
  st.formations={p0:'wide',p1:'protected_backline'};
  st.relics={p0:['banner_frontline','sharpshooter_doctrine','anti_armor_rounds','poisoned_blades','arcane_battery'],p1:[]};
  st.tactics={p0:['bodyguard','forced_march','opening_volley','sabotage'],p1:[]};
  units.applyArmyOptions(st,1000,placementHooks);
  for(const u of st.field.units)assert.equal(placementHooks.rectangleFree(st.field.units.filter(v=>v.id!==u.id),u.x,u.y,u.size||1),true,'overlap/bounds: '+u.id);
  assert.equal(golem.maxhp,22);assert.equal(rifle.maxhp,19);assert.equal(rifle.range,4);
  assert.equal(rifle.startShield,3);assert.equal(rifle.startShieldUntil,7000);
  assert.equal(rifle.antiArmorRounds,true);assert.equal(guard.poisonedBlades,true);
  assert.equal(enemy.hp,17);assert.equal(golem.y,9);
  const previous=JSON.stringify(st);units.applyArmyOptions(st,2000,placementHooks);assert.equal(JSON.stringify(st),previous);
});

test('emergency medkit fires once per battle and new summons lose temporary speed after 3 seconds',()=>{
  const injured=make('siphonleech',{hp:5,maxhp:20}),spawn=make('hellspawn',{id:'spawn',summoned:true,spd:3}),enemy=make('vguard',{p:1,y:7});
  const st=state(injured,spawn,enemy);st.relics={p0:['emergency_medkit','swarm_instinct'],p1:[]};
  units.tickForgeAbilities(st,1000);assert.equal(injured.hp,9);assert.equal(spawn.spd,4);
  injured.hp=5;units.tickForgeAbilities(st,1500);assert.equal(injured.hp,5);
  units.tickForgeAbilities(st,4100);assert.equal(spawn.spd,3);
});

test('endless challenge declares increasing pressure and applies its hazard on the authoritative clock',()=>{
  const early=campaign.createEndlessMission(1),late=campaign.createEndlessMission(80);
  assert.ok(late.pressure.startsAfterMs<early.pressure.startsAfterMs);assert.ok(late.pressure.intervalMs<early.pressure.intervalMs);assert.ok(late.pressure.damage>early.pressure.damage);
  const player=make('vguard',{hp:20}),enemy=make('vguard',{id:'foe',p:1,hp:20}),st=state(player,enemy);st.field.startedAt=1000;
  campaign.prepareBattle(st,late);const start=st.field.endlessPressure.nextAt;
  campaign.tickCampaignHazards(st,start-1);assert.equal(player.hp,20);
  campaign.tickCampaignHazards(st,start);assert.equal(player.hp,20-late.pressure.damage);assert.equal(enemy.hp,20);
  campaign.tickCampaignHazards(st,start+1);assert.equal(player.hp,20-late.pressure.damage);
});

test('campaign prepares distinct faction armies and introduces special troops in a controlled order',()=>{
  const first=campaign.getMission('forge-01'),late=campaign.getMission('forge-30');
  const factions=['Vanir','Aesir','Helions'];
  const firstTroops=factions.map(faction=>campaign.playerStartingRoster(faction,first).map(r=>r.type));
  assert.equal(new Set(firstTroops.flat()).size,9);
  for(const faction of factions){const recruits=campaign.playerStartingRoster(faction,first),elite=campaign.playerStartingRoster(faction,late);assert.equal(recruits.length,3);assert.ok(recruits.every(r=>r.lvl===1));assert.ok(elite.length>=4);assert.ok(elite.some(r=>r.lvl===3));}
  assert.deepEqual(first.opponent.unlockedTroops,[]);
  assert.ok(campaign.getMission('forge-06').opponent.unlockedTroops.includes('signalhawk'));
  assert.ok(!campaign.getMission('forge-06').opponent.unlockedTroops.includes('railbreaker'));
  assert.equal(campaign.createEndlessMission(1).opponent.unlockedTroops.length,6);
});

test('the actual engine leaves reserves undeployed rather than overlap crowded boards or cross starting halves',async()=>{
  const {createRequire}=await import('node:module'),require=createRequire(import.meta.url);
  const {createCombatAdapter,loadModules}=require('../../../server/faction-forge/simulator.cjs');
  const modules=await loadModules();
  const st={mode:'bot',roster:{p0:[{type:'golem',count:30,lvl:3},{type:'rifleman',count:100,lvl:1}],p1:[{type:'golem',count:30,lvl:3},{type:'hellspawn',count:100,lvl:1}]},factions:{p0:'Aesir',p1:'Helions'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'wide',p1:'spearhead'}};
  const replay=createCombatAdapter({seed:'full-board-reserves',modules}).battle(st);
  const troops=replay.frames[0].units;
  assert.ok(troops.length<st.roster.p0[0].count+st.roster.p0[1].count+st.roster.p1[0].count+st.roster.p1[1].count);
  for(const u of troops){const size=u.size||1;assert.ok(u.x>=0&&u.x+size<=8&&u.y>=0&&u.y+size<=16);assert.ok(u.p===0?u.y>=8:u.y+size<=8);assert.equal(placementHooks.rectangleFree(troops.filter(v=>v.id!==u.id),u.x,u.y,size),true,'overlap: '+u.id);}
});
