'use strict';
// Actual browser combat functions, extracted by the authoritative adapter.
const {loadModules,createCombatAdapter,rng}=require('../../server/faction-forge/simulator.cjs');
const {performance}=require('node:perf_hooks');
const FORMATIONS=['balanced','wide','spearhead','protected_backline','ambush'];
const BUILDS={
  vanguard:{name:'Iron Vanguard',faction:'Vanir',types:['vguard','marksman','suppressor','vrocket','medic']},
  tempest:{name:'Arcane Tempest',faction:'Aesir',types:['guardian','phaser','storm','runebinder','wardmaker']},
  blight:{name:'Blight Swarm',faction:'Helions',types:['hellspawn','roach','defiler','spitter','blood']},
  tanks:{name:'Three tanks / two supports',faction:'Aesir',types:['guardian','guardian','golem','wardmaker','architect']},
  precision:{name:'Protected precision',faction:'Vanir',types:['vguard','marksman','marksman','rifleman','medic']},
  artillery:{name:'Rocket battery',faction:'Vanir',types:['vguard','vrocket','vrocket','suppressor','medic']},
  swarm:{name:'Concentrated brood',faction:'Helions',types:['hellspawn','hellspawn','hellspawn','matron','hiveherald']},
  assassins:{name:'Ambush pressure',faction:'Helions',types:['viper','viper','burrower','hellspawn','siphonleech']},
  pureRanged:{name:'Exposed ranged line',faction:'Vanir',types:['marksman','rifleman','vrocket','signalhawk','medic']},
  corrosion:{name:'Corrosive attrition',faction:'Helions',types:['defiler','spitter','spitter','carrion','roach']},
  magic:{name:'Magic against armor',faction:'Aesir',types:['phaser','phaser','storm','runebinder','wardmaker']}
};
const SCENARIOS=[
  ['starter-vanir-aesir','vanguard','tempest'],['starter-vanir-helions','vanguard','blight'],['starter-aesir-helions','tempest','blight'],
  ['balanced-vs-tanks','tempest','tanks'],['precision-vs-armor','precision','tanks'],['artillery-vs-swarm','artillery','swarm'],
  ['assassins-vs-ranged','assassins','pureRanged'],['corrosion-vs-tanks','corrosion','tanks'],['magic-vs-armor','magic','tanks']
];
function stateFor(a,b,rank,formationA='balanced',formationB='balanced',seed='balance'){
  const types={Vanir:['vguard','marksman','rifleman','vrocket','medic','suppressor','signalhawk'],Aesir:['guardian','golem','wardmaker','architect','storm','phaser','runebinder'],Helions:['hellspawn','roach','defiler','spitter','blood','matron','hiveherald','viper','burrower','siphonleech','carrion']};
  const random=rng(seed),roster={},factions={},formations={},decks={},evolutions={};
  for(const [index,build] of [a,b].entries()){
    const side=index?'p1':'p0',unique=[...new Set(build.types)],extra=types[build.faction].filter(type=>!unique.includes(type));
    roster[side]=build.types.map(type=>({type,count:1,lvl:rank}));factions[side]=build.faction;
    formations[side]=index?formationB:formationA;
    decks[side]={id:'benchmark-'+side,name:build.name,faction:build.faction,troops:[...unique,...extra].slice(0,5)};
    evolutions[side]=Object.fromEntries(decks[side].troops.map(type=>[type,random()<.5?'a':'b']));
  }
  return {roster,factions,formations,decks,evolutions,relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},comboEnhancements:{p0:[],p1:[]}};
}
async function benchmark({seeds=5,ranks=[1,2,3],scenarios=SCENARIOS}={}){
  const modules=await loadModules(),started=performance.now(),rows=[];
  let seat0Wins=0,total=0,peakBodies=0,elapsedCombat=0;
  for(const rank of ranks)for(const [id,aKey,bKey] of scenarios){
    let aWins=0,combos=0,duration=0;
    for(let sample=0;sample<seeds;sample++)for(const reverse of [false,true]){
      const a=BUILDS[aKey],b=BUILDS[bKey],left=reverse?b:a,right=reverse?a:b;
      const fa=FORMATIONS[sample%5],fb=FORMATIONS[(sample+rank)%5],seed=`deck-balance-${id}-${rank}-${sample}`;
      const state=stateFor(left,right,rank,reverse?fb:fa,reverse?fa:fb,seed);
      // Upgrade choices follow the build when reversing seats.
      if(reverse){const original=stateFor(a,b,rank,fa,fb,seed);state.evolutions={p0:original.evolutions.p1,p1:original.evolutions.p0};}
      const start=performance.now(),result=createCombatAdapter({seed,modules}).battle(state,{ranked:true});elapsedCombat+=performance.now()-start;
      total++;seat0Wins+=result.winnerSide===0;aWins+=result.winnerSide===(reverse?1:0);duration+=result.durationMs;
      const comboIds=new Set();
      for(const frame of result.frames){
        for(const p of [0,1]){const count=frame.units.filter(unit=>unit.p===p&&unit.hp>0).length;peakBodies=Math.max(peakBodies,count);}
        for(const u of frame.units)if(!Number.isFinite(u.hp)||!Number.isFinite(u.x)||!Number.isFinite(u.y))throw Error('Non-finite combat state');
        for(const event of frame.syncedFx||[])if(event.comboId)comboIds.add(event.id);
      }
      combos+=comboIds.size;
    }
    rows.push({id,rank,a:BUILDS[aKey].name,b:BUILDS[bKey].name,aWins,matches:seeds*2,aWinRate:aWins/(seeds*2),comboActivations:combos,meanDurationMs:Math.round(duration/(seeds*2))});
  }
  return {engine:'actual shared browser/authoritative simulation',seeds,formations:FORMATIONS,ranks,mirrored:true,alternativePaths:true,matches:total,peakBodiesPerSide:peakBodies,seat0WinRate:seat0Wins/total,meanEngineMs:Math.round(elapsedCombat/total*100)/100,elapsedMs:Math.round(performance.now()-started),rows};
}
async function main(){
  const result=await benchmark({seeds:Math.max(1,Math.min(30,Number(process.argv[2])||5))});
  if(process.argv.includes('--json'))console.log(JSON.stringify(result,null,2));
  else{
    console.log(`${result.matches} seeded production battles; mirrored seats; ranks I–III; ${result.formations.length} formations; alternative paths.`);
    console.log(`Peak bodies/side ${result.peakBodiesPerSide}; seat 0 wins ${(result.seat0WinRate*100).toFixed(1)}%; mean engine ${result.meanEngineMs} ms/battle.`);
    console.log('| Rank | Composition A | Composition B | A wins | Combo activations |');console.log('| --- | --- | --- | ---: | ---: |');
    for(const row of result.rows)console.log(`| ${row.rank} | ${row.a} | ${row.b} | ${row.aWins}/${row.matches} | ${row.comboActivations} |`);
  }
}
if(require.main===module)main().catch(error=>{console.error(error);process.exitCode=1;});
module.exports={BUILDS,FORMATIONS,SCENARIOS,stateFor,benchmark,main};
