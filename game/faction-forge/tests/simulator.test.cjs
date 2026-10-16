'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {loadModules,createCombatAdapter}=require('../../../server/faction-forge/simulator.cjs');
const D=require('../../../server/faction-forge/domain.cjs');
const factionTypes={Vanir:['vguard','marksman','suppressor','vrocket','medic','rifleman','miner','shieldcap','emp','railbreaker','signalhawk'],Aesir:['guardian','phaser','storm','runebinder','wardmaker','blade','architect','golem','chronoweaver','miragewarden'],Helions:['hellspawn','roach','defiler','spitter','blood','matron','viper','carrion','burrower','siphonleech','hiveherald']};
const state=(p0,p1)=>{const keys={p0,p1},factions={},decks={};for(const side of ['p0','p1']){const faction=Object.keys(factionTypes).find(f=>factionTypes[f].includes(keys[side][0]))||(side==='p0'?'Vanir':'Helions');factions[side]=faction;decks[side]={id:'test-'+side,name:'Replay test '+side,faction,troops:[...new Set([...keys[side],...factionTypes[faction]])].slice(0,5)};}return {decks,roster:{p0:p0.map(type=>({type,count:1,lvl:1})),p1:p1.map(type=>({type,count:1,lvl:1}))},factions,relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'}};};

test('authoritative replay executes all 32 actual troop definitions and keeps locked troops out of issued player packs',async()=>{
  const modules=await loadModules();const a=createCombatAdapter({seed:'catalog',modules});assert.equal(Object.keys(a.troops).length,32);
  const s=state([],[]);
  for(let seed=0;seed<20;seed++){const pack=createCombatAdapter({seed:'locked-'+seed,modules}).pack(s,'p0',D.BASE_TROOPS);assert(pack.every(c=>c.type!=='troop'||!D.NEW_TROOPS.includes(c.key)));}
  for(const key of D.ALL_TROOPS){const battle=createCombatAdapter({seed:'unit-'+key,modules}).battle(state([key],['vguard']),{forgeProfiles:{0:D.newProfile('u','U'),1:D.newProfile('v','V')}});assert([0,1].includes(battle.winnerSide),key);assert(battle.frames[0].units.some(u=>u.type===key),key);assert(battle.durationMs>0&&battle.durationMs<200000,key);}
});
test('the same trusted draft and seed produce byte-identical battle replay and results',async()=>{
  const modules=await loadModules();const s=state(['vguard','railbreaker','signalhawk','rifleman'],['defiler','hellspawn','matron','hiveherald']);
  const options={forgeProfiles:{0:D.newProfile('u','U'),1:D.newProfile('v','V')}};
  const a=createCombatAdapter({seed:'fixed-seed',modules}).battle(s,options);const b=createCombatAdapter({seed:'fixed-seed',modules}).battle(s,options);assert.deepEqual(a,b);
});
test('permanent tier improves unranked role stats but ranked replay normalizes them while retaining earned appearance',async()=>{
  const modules=await loadModules();const p=D.newProfile('u','U');p.troops.vguard.forgeTier=3;p.troops.rifleman.forgeTier=3;
  const s=state(['vguard','rifleman'],['hellspawn']);s.roster.p0[0].lvl=3;
  const options={forgeProfiles:{0:p,1:D.newProfile('v','V')}};
  const ranked=createCombatAdapter({seed:'tiers',modules}).battle(s,{...options,ranked:true});const casual=createCombatAdapter({seed:'tiers',modules}).battle(s,{...options,ranked:false});
  const rg=ranked.frames[0].units.find(u=>u.type==='vguard'),cg=casual.frames[0].units.find(u=>u.type==='vguard');
  assert.equal(rg.forgeTier,3);assert.equal(rg.battleRank,3);assert(cg.maxhp>rg.maxhp);assert.equal(rg.lvl,3);
});
test('all campaign chapters have server-readable composed rosters and all six milestone unlocks match authority policy',async()=>{
  const {campaign}=await loadModules();assert.equal(campaign.CAMPAIGN_MISSIONS.length,30);
  for(const mission of campaign.CAMPAIGN_MISSIONS){assert(mission.opponent.roster.length>0);assert.equal(D.missionNumber(mission.id),mission.number);assert.equal(D.missionId(mission.number),mission.id);if(mission.unlockTroop){const p=D.newProfile('u','U');const s={id:'m-'+mission.number,phase:'game-over',serverValidated:true,mode:'campaign',missionId:mission.id,missionNumber:mission.number,participants:['u',null],winnerSide:0,lives:[3,0]};assert(D.settle(p,s,0).profile.troops[mission.unlockTroop].unlocked);}}
});

test('old overfilled and off-deck armies cannot bypass the five-combatant simulation limit',async()=>{
 const modules=await loadModules(),s=state(['vguard','marksman'],['hellspawn','blood']);s.roster.p0=[{type:'vguard',count:99,lvl:3},{type:'miner',count:99,lvl:3}];s.roster.p1=[{type:'hellspawn',count:99,lvl:3},{type:'blood',count:99,lvl:3}];
 const replay=createCombatAdapter({seed:'malformed-old-armies',modules}).battle(s);
 for(const frame of replay.frames)for(const p of [0,1]){const active=frame.units.filter(unit=>unit.p===p&&unit.hp>0&&!unit.illusion);assert.ok(active.length<=5);assert.ok(active.every(unit=>s.decks['p'+p].troops.includes(unit.type)));}
 assert.equal(replay.frames[0].units.some(unit=>unit.type==='miner'),false);
});
