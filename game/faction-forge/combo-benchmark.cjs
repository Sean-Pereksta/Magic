'use strict';
// Representative equal-size rosters, mirrored seats, actual production engine.
// A reproducible balance sample, not a prediction of all player matchups.
const {loadModules,createCombatAdapter}=require('../../server/faction-forge/simulator.cjs');
const {performance}=require('node:perf_hooks');
const seeds=Math.max(1,Math.min(30,Number(process.argv[2])||3));
const builds=[
  {name:'Static Tempest',faction:'Aesir',types:['guardian','runebinder','storm','blade','phaser','wardmaker']},
  {name:'Arcane Fortress',faction:'Aesir',types:['golem','wardmaker','architect','guardian','blade','phaser']},
  {name:'Blood Ambush',faction:'Helions',types:['blood','burrower','defiler','hellspawn','viper','carrion']},
  {name:'Brood Invasion',faction:'Helions',types:['matron','blood','hellspawn','hiveherald','spitter','siphonleech']},
  {name:'Guided Artillery',faction:'Vanir',types:['vguard','shieldcap','vrocket','signalhawk','suppressor','medic']},
  {name:'Precision Division',faction:'Vanir',types:['vguard','miner','marksman','railbreaker','signalhawk','rifleman']},
  {name:'Rocket Battery',faction:'Vanir',types:['vguard','shieldcap','vrocket','vrocket','signalhawk','medic']},
  {name:'Assassin Swarm',faction:'Helions',types:['viper','viper','burrower','hellspawn','matron','hiveherald']}
];
async function main(){
  const modules=await loadModules(),rows=[],started=performance.now();
  for(const rank of [1,2,3])for(let a=0;a<builds.length;a++)for(let b=a+1;b<builds.length;b++){
    let wins=0,activations=0,maxUnits=0,duration=0;
    for(let seed=0;seed<seeds;seed++)for(const mirrored of [false,true]){
      const left=builds[mirrored?b:a],right=builds[mirrored?a:b];
      const state={roster:{p0:left.types.map(type=>({type,count:1,lvl:rank})),p1:right.types.map(type=>({type,count:1,lvl:rank}))},factions:{p0:left.faction,p1:right.faction},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:seed%2?'wide':'balanced',p1:seed%2?'balanced':'wide'}};
      const result=createCombatAdapter({seed:`combo-balance-${rank}-${a}-${b}-${seed}`,modules}).battle(state,{ranked:true});
      wins+=result.winnerSide===(mirrored?1:0)?1:0;duration+=result.durationMs;
      const ids=new Set();for(const frame of result.frames){maxUnits=Math.max(maxUnits,frame.units.length);for(const event of frame.syncedFx||[])if(event.comboId)ids.add(event.id);}
      activations+=ids.size;
      if(result.frames.some(frame=>frame.units.some(u=>!Number.isFinite(u.hp))))throw Error('Non-finite health');
    }
    rows.push({rank,a:builds[a].name,b:builds[b].name,aWins:wins,matches:seeds*2,comboActivations:activations,maxUnits,meanDurationMs:Math.round(duration/(seeds*2))});
  }
  const result={seeds,mirrored:true,matches:rows.length*seeds*2,elapsedMs:Math.round(performance.now()-started),rows};
  if(process.argv.includes('--json'))console.log(JSON.stringify(result,null,2));
  else{console.log(`Production engine: ${result.matches} battles, ${seeds} seeds with mirrored seats, ranks I–III.`);console.log('| Rank | Build A | Build B | A wins | Combo activations | Peak units |');console.log('| --- | --- | --- | ---: | ---: | ---: |');for(const row of rows)console.log(`| ${row.rank} | ${row.a} | ${row.b} | ${row.aWins}/${row.matches} | ${row.comboActivations} | ${row.maxUnits} |`);}
}
main().catch(error=>{console.error(error);process.exitCode=1});
