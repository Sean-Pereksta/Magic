const test=require('node:test');
const assert=require('node:assert/strict');
const Rise=require('../js/campaign/rise-to-glory.js');

function memoryStorage(initial={}){
  const data=new Map(Object.entries(initial));
  return {getItem:key=>data.has(key)?data.get(key):null,setItem:(key,value)=>data.set(key,String(value)),data};
}
function winNext(state,stats={}){
  const game=Rise.nextGame(state);
  return Rise.recordGame(state,{gameId:game.id,won:true,homeScore:game.targetScore,awayScore:7,
    players:state.starters.slice(0,game.teamSize).map(id=>({id,stats}))});
}
function finishSeason(state){
  let games=0;
  while(Rise.nextGame(state)){
    assert.ok(++games<=11,'schedule must terminate after seven league games, two challenges and two playoffs');
    assert.equal(winNext(state,{assists:2,rebounds:3,blocks:1,points:4}).ok,true);
  }
  return games;
}

test('market generates 96 distinct physical athletes, 32 ratings and meaningful specializations',()=>{
  const market=Rise.generateMarket(24);
  assert.equal(market.length,96);
  assert.equal(new Set(market.map(player=>player.id)).size,96);
  assert.equal(new Set(market.map(player=>player.name)).size,96);
  assert.equal(Rise.ATTRIBUTE_KEYS.length,32);
  assert.equal(Object.keys(Rise.ATTRIBUTES).length,6);
  assert.deepEqual(market,Rise.generateMarket(24),'a seed reconstructs the same market');
  assert.ok(new Set(market.map(player=>player.archetype)).size>=9);
  assert.ok(market.some(player=>player.signingCost<200));
  assert.ok(market.some(player=>player.signingCost>2000));
  for(const player of market){
    assert.equal(Object.keys(player.ratings).length,32);
    assert.ok(player.physical.heightCm>=176&&player.physical.heightCm<=222);
    assert.ok(player.physical.weightKg>=73&&player.physical.weightKg<=132);
    for(const key of Rise.ATTRIBUTE_KEYS) assert.ok(player.ratings[key]>=25&&player.ratings[key]<=player.caps[key]&&player.caps[key]<=99);
    assert.ok(Math.min(...Object.values(player.caps))<78,'every athlete has enduring limitations');
  }
  const shooter=market.find(player=>player.archetype==='sharpshooter');
  const big=market.find(player=>player.archetype==='interior');
  assert.ok(shooter.ratings.threePoint>shooter.ratings.contactFinishing);
  assert.ok(big.ratings.strength>big.ratings.speed);
  assert.ok(big.physical.heightCm>shooter.physical.heightCm);
});

test('team creation chooses three rookies, captain appearance, identity and a playable fixture',()=>{
  const rookies=Rise.rookiePool(31),ids=[rookies[2].id,rookies[3].id,rookies[5].id];
  const state=Rise.create({seed:31,name:'Southside',captainName:'Ari Banks',captainNumber:22,captainHairStyle:'braids',
    starterIds:ids,primaryColor:'#123456',secondaryColor:'#fedcba',logo:'bolt',jerseyStyle:'chevron',offense:'post_up',defense:'protect_paint'});
  assert.deepEqual(state.roster,ids);assert.deepEqual(state.starters,ids);
  assert.equal(state.players[ids[0]].name,'Ari Banks');assert.equal(state.players[ids[0]].captain,true);
  assert.equal(state.players[ids[0]].appearance.number,'22');assert.equal(state.players[ids[0]].appearance.hairStyle,'braids');
  assert.ok(Rise.roster(state).every(player=>player.overall<62));
  const fixture=Rise.prepareMatch(state);
  assert.equal(fixture.teamSize,3);assert.equal(fixture.home.length,3);assert.equal(fixture.away.length,3);
  assert.equal(fixture.offense,'post_up');assert.equal(fixture.defense,'protect_paint');
  assert.ok(fixture.away.every(player=>Object.keys(player.ratings).length===32));
  assert.equal(state.team.primaryColor,'#123456');
});

test('market signing and releases respect money, progression, roster size and captain',()=>{
  const state=Rise.create(),before=state.currency;
  const recruit=Rise.market(state,{affordable:true,sort:'cost'})[0];
  assert.equal(Rise.sign(state,recruit.id).ok,true);
  assert.equal(state.currency,before-recruit.signingCost);
  assert.ok(Rise.roster(state).includes(recruit));
  assert.equal(Rise.sign(state,recruit.id).ok,false);
  assert.equal(Rise.release(state,state.captainId).ok,false);
  assert.equal(Rise.setStarters(state,[state.starters[0],state.starters[0],recruit.id]).ok,false);
  assert.equal(Rise.setStarters(state,[state.starters[0],state.starters[1],recruit.id]).ok,true);
  assert.equal(Rise.setRole(state,recruit.id,'anchor',3).ok,true);
  assert.equal(recruit.role,'anchor');assert.equal(recruit.priority,3);
  assert.equal(Rise.release(state,recruit.id).ok,true);
  assert.equal(state.starters.length,3);assert.ok(state.starters.every(id=>state.roster.includes(id)));
  assert.equal(Rise.release(state,state.roster[1]).ok,false);
  const locked=Object.values(state.players).find(player=>player.unlockDivision===4);
  state.currency=100000;
  assert.equal(Rise.sign(state,locked.id).ok,false);
  for(const candidate of Rise.market(state)) if(state.roster.length<10) assert.equal(Rise.sign(state,candidate.id).ok,true);
  assert.equal(state.roster.length,10);assert.equal(Rise.sign(state,Rise.market(state)[0].id).ok,false);
});

test('rating upgrades are visible in live profile multipliers and keep physical tradeoffs',()=>{
  const state=Rise.create(),athlete=state.players[state.captainId];
  athlete.upgradePoints=100;
  const before=Rise.toProfile(athlete,state.team),preview=Rise.upgradePreview(state,athlete.id,'speed');
  assert.equal(Rise.upgrade(state,athlete.id,'speed').ok,true);
  const after=Rise.toProfile(athlete,state.team);
  assert.equal(after.ratings.speed,before.ratings.speed+preview.gain);assert.ok(after.speed>before.speed);
  assert.equal(after.ratings.blocking,before.ratings.blocking);assert.equal(after.height,before.height);
  const upgradePoints=athlete.upgradePoints;
  athlete.ratings.blocking=athlete.caps.blocking;
  assert.equal(Rise.upgrade(state,athlete.id,'blocking').ok,false);assert.equal(athlete.upgradePoints,upgradePoints);
  athlete.invested=240;
  assert.equal(Rise.upgrade(state,athlete.id,'speed').ok,false);
  assert.equal(Rise.upgrade(state,athlete.id,'doesNotExist').ok,false);
});

test('ability trees enforce prerequisites, growing costs, signature potential and two branches',()=>{
  const state=Rise.create(),athlete=state.players[state.starters[1]];
  athlete.upgradePoints=30;
  assert.equal(Rise.abilityPreview(state,athlete.id,'catchAndShoot').available,true);
  const before=athlete.upgradePoints;
  assert.equal(Rise.learnAbility(state,athlete.id,'catchAndShoot').ok,true);
  assert.equal(athlete.upgradePoints,before-2);
  assert.equal(Rise.toProfile(athlete).abilities.catchAndShoot,1);
  assert.equal(Rise.abilityPreview(state,athlete.id,'catchAndShoot').cost,3);
  assert.equal(Rise.learnAbility(state,athlete.id,'posterizer').ok,false);
  athlete.ratings.threePoint=70;
  assert.equal(Rise.learnAbility(state,athlete.id,'cornerSpecialist').ok,true);
  athlete.ratings.releaseStability=80;athlete.ratings.consistency=70;
  assert.equal(Rise.learnAbility(state,athlete.id,'quickRelease').ok,true);
  athlete.ratings.blocking=80;athlete.ratings.interiorDefense=80;
  assert.ok(Rise.abilityPreview(state,athlete.id,'rimGuardian').missing.includes('Two specialization branches already selected'));
  assert.equal(Rise.learnAbility(state,athlete.id,'rimGuardian').ok,false);
  assert.deepEqual(athlete.branches,['perimeter','shotCreation']);
});

test('defensive and support contributions earn strong XP without scoring',()=>{
  const scoring=Rise.contributionXP({points:12},true);
  const defense=Rise.contributionXP({blocks:2,steals:2,rebounds:3,stops:2},true);
  const support=Rise.contributionXP({assists:4,screens:3},true);
  assert.ok(defense>scoring);assert.ok(support>=scoring);
  const state=Rise.create(),id=state.captainId;
  const result=Rise.recordGame(state,{gameId:Rise.nextGame(state).id,won:false,players:[{id,stats:{points:0,blocks:4,rebounds:6,stops:3,screens:2}}]});
  assert.equal(result.ok,true);assert.ok(state.players[id].level>1);
  assert.ok(state.players[id].stats.blocks===4&&state.players[id].stats.points===0);
  assert.ok(state.players[id].upgradePoints>4);
  const currency=state.currency;
  assert.equal(Rise.recordGame(state,{gameId:result.result.id,won:true}).ok,false);
  assert.equal(state.currency,currency,'a duplicated old game cannot award money or XP');
});

test('five full divisions have standings, rivalry formats, playoffs, championships and offseason prospects',()=>{
  const state=Rise.create();
  let lastOpponent=0;
  for(let division=0;division<5;division++){
    assert.equal(state.season.division,division);
    assert.ok(state.season.schedule.some(game=>game.teamSize===1));assert.ok(state.season.schedule.some(game=>game.teamSize===2));
    assert.ok(state.season.schedule.filter(game=>game.teamSize===3).length>=7);
    const strength=Rise.prepareMatch(state).away.reduce((sum,player)=>sum+player.overall,0);
    assert.ok(strength>lastOpponent);lastOpponent=strength;
    assert.equal(finishSeason(state),11);
    const table=Rise.getStandings(state);
    assert.equal(table[0].id,'player');assert.equal(table[0].wins,7);
    assert.ok(table.every(team=>team.wins+team.losses===7),'a balanced eight-team schedule gives each team seven league games');
    assert.equal(state.season.champion,'player');assert.equal(state.season.phase,'offseason');
    assert.equal(Rise.nextGame(state),null);assert.ok(state.awards.some(award=>award.name==='Tournament MVP'));
    const oldCount=Object.keys(state.players).length;
    assert.equal(Rise.advanceSeason(state).ok,true);
    assert.equal(Object.keys(state.players).length,oldCount+9);
    assert.equal(state.season.division,Math.min(4,division+1));
  }
  assert.equal(state.cosmetics.length,6);
  assert.equal(state.history.length,5);
  assert.equal(state.season.number,6);
  assert.equal(state.season.division,4,'champions can defend their Elite title in harder seasons');
});

test('losing season can recover in the offseason and retry the current division',()=>{
  const state=Rise.create();
  while(Rise.nextGame(state)){
    const game=Rise.nextGame(state);
    assert.equal(Rise.recordGame(state,{gameId:game.id,won:false,homeScore:2,awayScore:game.targetScore}).ok,true);
  }
  assert.equal(state.season.phase,'offseason');assert.notEqual(state.season.champion,'player');
  assert.ok(state.currency>720,'losses still fund rebuilding');
  assert.equal(Rise.advanceSeason(state).ok,true);assert.equal(state.season.division,0);
  assert.equal(state.season.number,2);assert.equal(Rise.nextGame(state).status,'upcoming');
});

test('live training proof gates awards, has tiers and bounds repeat farming',()=>{
  const state=Rise.create(),id=state.captainId,athlete=state.players[id];
  const plan=Rise.trainingPlan(state,id,'midrange',1),before=athlete.xp;
  assert.equal(Rise.awardTraining(state,id,'midrange',{attempts:plan.attempts,successes:0}).ok,false);
  assert.equal(athlete.xp,before);
  assert.equal(Rise.awardTraining(state,id,'midrange',{attempts:8,successes:8,tier:3}).ok,false);
  for(let tier=1;tier<=3;tier++) assert.equal(Rise.awardTraining(state,id,'midrange',{attempts:8,successes:8,tier}).ok,true);
  assert.equal(athlete.training.midrange.tier,3);assert.equal(athlete.development.trainingSessions,3);
  assert.equal(Rise.awardTraining(state,id,'midrange',{attempts:8,successes:8}).ok,false);
  winNext(state);
  assert.equal(Rise.awardTraining(state,id,'midrange',{attempts:8,successes:8}).ok,true);
});

test('tier-three drill mastery grants saved first-rank tuition without bypassing requirements or branches',()=>{
  const state=Rise.create(),id=state.starters[1],athlete=state.players[id];
  assert.ok(athlete.abilityPotential.includes('deepRange'));
  for(let tier=1;tier<=3;tier++) assert.equal(Rise.awardTraining(state,id,'threePoint',{attempts:8,successes:8,tier}).ok,true);
  assert.equal(athlete.trainingTokens.deepRange,true);assert.equal(athlete.abilities.deepRange,undefined);
  assert.equal(Rise.abilityPreview(state,id,'deepRange').cost,0);
  assert.equal(Rise.abilityPreview(state,id,'deepRange').available,false,'mastery tuition does not bypass shooting requirements');
  const saved=Rise.normalize(state),trained=saved.players[id];
  assert.equal(trained.trainingTokens.deepRange,true,'tuition survives reload');
  trained.ratings.threePoint=75;trained.ratings.releaseStability=65;trained.upgradePoints=0;trained.branches=['defense','midrange'];
  assert.equal(Rise.learnAbility(saved,id,'deepRange').ok,false,'mastery tuition respects specialization tradeoffs');
  trained.branches=[];
  assert.equal(Rise.learnAbility(saved,id,'deepRange').ok,true);assert.equal(trained.upgradePoints,0);
  assert.equal(trained.abilities.deepRange,1);assert.equal(trained.trainingTokens.deepRange,undefined);
  assert.equal(Rise.abilityPreview(saved,id,'deepRange').cost,3,'only the first rank is prepaid');
});

test('eligible tier-three mastery unlocks one compatible signature immediately',()=>{
  const state=Rise.create(),id=state.starters[1],athlete=state.players[id];
  athlete.ratings.threePoint=78;athlete.ratings.releaseStability=70;
  let reward;
  for(let tier=1;tier<=3;tier++) reward=Rise.awardTraining(state,id,'threePoint',{attempts:8,successes:8,tier});
  assert.equal(reward.signature.key,'deepRange');assert.equal(reward.signature.unlocked,true);
  assert.equal(athlete.abilities.deepRange,1);assert.ok(reward.message.includes('Mastery unlock'));
  assert.ok(athlete.abilityPotential.includes(reward.signature.key));
});

test('tournament MVP uses only the current season, with contributions surviving a midseason reload',()=>{
  let state=Rise.create();
  const captain=state.captainId,target=state.starters[1];state.players[captain].stats.points=9999;
  let games=0;
  while(Rise.nextGame(state)){
    const game=Rise.nextGame(state);
    Rise.recordGame(state,{gameId:game.id,won:true,players:state.starters.slice(0,game.teamSize).map(id=>({id,stats:{points:id===target?10:0,assists:id===target?3:0}}))});
    if(++games===4){const previous=state;state=Rise.normalize(state);assert.deepEqual(state.season.playerStats,previous.season.playerStats);}
  }
  const mvp=state.awards.find(award=>award.name==='Tournament MVP');
  assert.equal(mvp.playerId,target,'an old career scoring lead does not decide this season MVP');
  assert.ok(state.season.playerStats[target].points>state.season.playerStats[captain].points);
  assert.equal(state.players[captain].stats.points,9999);
  const restored=Rise.normalize(state);assert.equal(restored.awards.find(award=>award.name==='Tournament MVP').playerId,target);
  assert.equal(restored.history[0].games[0].players.length,3);
});

test('rivalry records span seasons and recover from old histories without duplicate counting',()=>{
  const state=Rise.create();finishSeason(state);
  const record=structuredClone(state.rivalries);
  assert.equal(Object.values(record).reduce((sum,result)=>sum+result.wins+result.losses,0),11);
  const restored=Rise.normalize(state);assert.deepEqual(restored.rivalries,record);
  const legacy=structuredClone(state);delete legacy.rivalries;
  assert.deepEqual(Rise.normalize(legacy).rivalries,record,'completed current season also exists in history and must count once');
  Rise.advanceSeason(restored);
  const game=Rise.nextGame(restored),before={...restored.rivalries[game.opponentId]};
  Rise.recordGame(restored,{gameId:game.id,won:false});
  assert.equal(restored.rivalries[game.opponentId].wins,before.wins);assert.equal(restored.rivalries[game.opponentId].losses,before.losses+1);
  assert.deepEqual(Rise.opponent(restored,{opponentId:game.opponentId}).rivalryRecord,restored.rivalries[game.opponentId]);
  assert.deepEqual(Rise.normalize(restored).rivalries,restored.rivalries);
});

test('complementary rosters influence chemistry and game preparation rather than free scoring',()=>{
  const players=Rise.generateMarket();
  const ids=['playmaker','sharpshooter','slasher'];
  const complementary=ids.map(archetype=>players.find(player=>player.archetype===archetype));
  const chemistry=Rise.chemistry(complementary);
  assert.ok(chemistry.synergies.some(synergy=>synergy.key==='playmakerShooter'));
  assert.ok(chemistry.synergies.some(synergy=>synergy.key==='lobConnection'));
  assert.ok(chemistry.synergies.every(synergy=>!('shootingBonus' in synergy)));
  const comparison=Rise.compare(complementary[0],complementary[1]);
  assert.equal(comparison.length,32);
  assert.equal(comparison.find(row=>row.key==='passing').difference,complementary[1].ratings.passing-complementary[0].ratings.passing);
  const state=Rise.create();
  assert.equal(Rise.setStrategy(state,{offense:'give_go',defense:'switch',substitution:'fresh_legs',matchups:{[state.captainId]:'cpu-1'}}).ok,true);
  const prepared=Rise.prepareMatch(state);
  assert.equal(prepared.offense,'give_go');assert.equal(prepared.defense,'switch');assert.equal(state.team.substitution,'fresh_legs');
});

test('versioned saves restore progress and customization while leaving National Tour untouched',()=>{
  const tour='{"version":2,"mode":"national-tour","playerWins":8,"unlocked":["lavaBurst"]}';
  const storage=memoryStorage({basketballNationalTourV2:tour,basketballTournamentSave:'legacy'});
  const state=Rise.create({captainName:'Ari Banks',captainNumber:99,captainHairStyle:'afro'});
  state.players[state.captainId].equipment={headband:{enabled:true,color:'#123456'},sleeve:{enabled:true,side:'left',color:0xfedcba},bad:{enabled:true}};
  winNext(state,{points:6,assists:3,rebounds:4});
  Rise.upgrade(state,state.captainId,'passing');
  assert.equal(Rise.save(state,storage).ok,true);
  const saved=Rise.load(storage);
  assert.equal(saved.season.completed.length,1);assert.deepEqual(saved.record,state.record);assert.equal(saved.currency,state.currency);
  assert.equal(saved.players[state.captainId].ratings.passing,state.players[state.captainId].ratings.passing);
  assert.equal(saved.players[state.captainId].appearance.number,'99');assert.equal(saved.players[state.captainId].appearance.hairStyle,'afro');
  assert.deepEqual(saved.players[state.captainId].equipment,{headband:{enabled:true,color:'#123456'},sleeve:{enabled:true,side:'left',color:0xfedcba}});
  assert.deepEqual(Rise.toProfile(saved.players[state.captainId]).equipment,saved.players[state.captainId].equipment);
  assert.equal(saved.players[state.captainId].stats.assists,3);
  assert.equal(Rise.nextGame(saved).id,Rise.nextGame(state).id);
  assert.equal(storage.getItem('basketballNationalTourV2'),tour);assert.equal(storage.getItem('basketballTournamentSave'),'legacy');
  assert.ok(!Rise.SAVE_KEY.includes('NationalTour'));
});

test('completed championships reload idempotently without repeating awards or losing history',()=>{
  const state=Rise.create();finishSeason(state);
  state.team.cosmetic='streetStripe';
  for(let cycle=0;cycle<3;cycle++){
    const restored=Rise.normalize(state);
    assert.equal(restored.season.phase,'offseason');assert.equal(restored.season.champion,'player');
    assert.deepEqual(restored.currency,state.currency);assert.equal(restored.awards.length,state.awards.length);
    assert.equal(restored.history.length,state.history.length);assert.equal(restored.history[0].games.length,11);
    assert.equal(restored.history[0].standings.length,8);assert.equal(restored.cosmetics.length,state.cosmetics.length);
    assert.equal(restored.team.cosmetic,'streetStripe');
  }
});

test('malformed saves, unsupported versions and storage denial recover safely',()=>{
  const state=Rise.create(),storage=memoryStorage();
  Rise.save(state,storage);winNext(state);Rise.save(state,storage);
  storage.setItem(Rise.SAVE_KEY,'{broken');
  const backup=Rise.load(storage);assert.ok(backup.recovered);assert.equal(backup.season.completed.length,0);
  const invalid=Rise.create();invalid.version=999;
  assert.equal(Rise.normalize(invalid),null);assert.equal(Rise.normalize(null),null);assert.equal(Rise.normalize({mode:'national-tour'}),null);
  invalid.version=1;invalid.currency=-100;invalid.players[invalid.captainId].ratings.speed=Infinity;
  invalid.players[invalid.captainId].caps.speed=999;invalid.roster.push(invalid.roster[0]);invalid.starters=['missing','missing','missing'];
  const migrated=Rise.normalize(invalid);
  assert.equal(migrated.version,Rise.VERSION);assert.equal(migrated.currency,0);
  assert.equal(new Set(migrated.starters).size,3);assert.equal(new Set(migrated.roster).size,3);
  assert.ok(migrated.players[migrated.captainId].caps.speed<=Rise.create().players[invalid.captainId].caps.speed);
  const denied={getItem(){throw Error('denied');},setItem(){throw Error('denied');}};
  assert.equal(Rise.load(denied),null);assert.equal(Rise.save(state,denied).ok,false);
  const unusual=Rise.create();unusual.roster.push('__proto__','constructor');unusual.captainId='__proto__';
  const recovered=Rise.normalize(unusual);
  assert.equal(recovered.roster.length,3);assert.notEqual(recovered.captainId,'__proto__');
  assert.equal(Object.prototype.captain,undefined);assert.equal(Rise.player(state,'__proto__'),null);
  assert.equal(Rise.abilityPreview(state,state.captainId,'constructor'),null);
});
