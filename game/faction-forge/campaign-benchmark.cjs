'use strict';
// Replay representative complete matches using the actual server engine.
// This is a balance sample, not a model of human skill or a statistical guarantee.
const {createCombatAdapter,loadModules}=require('../../server/faction-forge/simulator.cjs');
const D=require('../../server/faction-forge/domain.cjs');
const option=(name,fallback)=>{const index=process.argv.indexOf('--'+name);return index>=0?process.argv[index+1]:fallback;};
const seeds=Math.max(1,Math.min(100,Number(option('seeds',3))||3));
const tier=Math.max(1,Math.min(3,Number(option('tier',1))||1));
const faction=option('faction','Vanir');
const numbers=String(option('missions','1,6,12,18,24,30')).split(',').map(Number).filter(n=>n>=1&&n<=30&&Number.isInteger(n));
if(!['Vanir','Aesir','Helions'].includes(faction))throw new Error('Choose --faction Vanir, Aesir, or Helions.');
async function benchmark() {
  const modules=await loadModules(),results=[];
  for(const number of numbers)for(let seed=0;seed<seeds;seed++){
    const mission=modules.campaign.getMission(D.missionId(number)),player=D.newProfile('player','Player'),cpu=D.newProfile('cpu','CPU');
    for(const troop of Object.values(player.troops))troop.forgeTier=tier;
    for(const key of mission.opponent.unlockedTroops)cpu.troops[key].unlocked=true;
    for(const prior of modules.campaign.MISSIONS.filter(m=>m.number<number&&m.unlockTroop))player.troops[prior.unlockTroop].unlocked=true;
    const state={mode:'campaign',missionId:mission.id,players:['Player',mission.opponent.name],
      roster:{p0:modules.campaign.playerStartingRoster(faction,mission),p1:structuredClone(mission.opponent.roster)},
      factions:{p0:faction,p1:mission.aiFaction},profiles:{p0:player,p1:cpu},
      relics:{p0:[],p1:structuredClone(mission.opponent.relics)},tactics:{p0:[],p1:structuredClone(mission.opponent.tactics)},
      formations:{p0:'balanced',p1:mission.opponent.formation}};
    const lives=[4,4];let round=0,bonus=null,openingWinner=null;
    while(lives[0]&&lives[1]&&round<8){
      round++;const adapter=createCombatAdapter({seed:`benchmark-${number}-${seed}-${round}`,modules});
      const pick=side=>{
        const unlocked=D.ALL_TROOPS.filter(k=>state.profiles[side].troops[k].unlocked);
        const pack=adapter.pack(state,side,unlocked);
        const index=adapter.aiPick(pack,state,side,side==='p0'?.9:mission.difficulty);
        Object.assign(state,adapter.pick(state,side,pack[index]));
      };
      if(bonus)pick(bonus);
      for(let draft=0;draft<3;draft++){pick('p0');pick('p1');}
      const battle=adapter.battle(state,{forgeProfiles:{0:player,1:cpu},mission});
      if(openingWinner===null)openingWinner=battle.winnerSide;
      bonus='p'+(1-battle.winnerSide);lives[1-battle.winnerSide]--;
    }
    results.push({number,seed,won:lives[1]===0,openingWon:openingWinner===0,lives,rounds:round});
  }
  console.log(`Faction: ${faction}; player Forge Tier: ${tier}; player draft skill: 0.9; ${seeds} deterministic seeds per mission.\n`);
  console.log('| Mission | Player match wins | Opening round wins | Final lives (player–CPU) |');
  console.log('| --- | ---: | ---: | --- |');
  for(const number of numbers){const rows=results.filter(r=>r.number===number);console.log(`| ${number} | ${rows.filter(r=>r.won).length}/${rows.length} | ${rows.filter(r=>r.openingWon).length}/${rows.length} | ${rows.map(r=>r.lives.join('–')).join(', ')} |`);}
}
benchmark().catch(error=>{console.error(error.message);process.exitCode=1;});
