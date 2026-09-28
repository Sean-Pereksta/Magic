// Reproducible full-engine comparison; no browser, model calls or previews.
// node game/iron-throne/tests/difficulty-benchmark.mjs [rounds=30]
import { createGame, settlements } from '../core.mjs';
import { foundAIKingdoms } from '../founding.mjs';
import { endTurn } from '../diplomacy.mjs';
const rounds=Number(process.argv[2]||30),rows=[];
const samples=[[8147,'heartlands'],[991,'great-divide'],[3401,'highland-crown']].filter(([seed])=>!process.env.IRON_BENCHMARK_SEED||seed===Number(process.env.IRON_BENCHMARK_SEED));
for(const [seed,preset] of samples){
  const initial=createGame(seed,preset);initial.controllers=Object.fromEntries(initial.kingdoms.map(k=>[k.id,{kind:'ai',uid:null,name:''}]));
  const founded=foundAIKingdoms(initial);if(!founded.ok)throw new Error(founded.error);
  for(const difficulty of ['easy','medium','hard','insane'].filter(d=>!process.env.IRON_BENCHMARK_DIFFICULTY||d===process.env.IRON_BENCHMARK_DIFFICULTY)){
    const s=structuredClone(initial);s.difficulty=difficulty;const foundedIds=new Set(),captured=new Set(),completed=new Set();let shortageRounds=0;
    for(let i=0;i<rounds&&!s.outcome;i++){
      endTurn(s);
      for(const t of settlements(s))if(!t.capital)foundedIds.add(t.id);
      for(const e of s.militaryEvents)if(e.action==='capture')captured.add(e.id);
      for(const p of s.intrigue.plans)if(p.status==='Completed'&&['invasion','jointWar','infrastructure'].includes(p.type))completed.add(p.id);
      shortageRounds+=s.kingdoms.filter(k=>settlements(s,k.id).length&&(k.resources.food===0||k.resources.gold===0)).length;
    }
    const row={seed,preset,difficulty,rounds:s.turn-1,founded:foundedIds.size,captures:captured.size,campaignsCompleted:completed.size,settlements: settlements(s).length,shortageHouseRounds:shortageRounds,stalled:s.intrigue.plans.filter(p=>['Preparing','Considering','Committed','Executing'].includes(p.status)&&s.turn-p.createdTurn>=12).length,gold:s.kingdoms.reduce((n,k)=>n+k.resources.gold,0)};
    rows.push(row);console.log(JSON.stringify(row));
    if(process.env.IRON_BENCHMARK_DETAILS)console.error(JSON.stringify({difficulty,houses:s.kingdoms.map(k=>({id:k.id,population:k.population,resources:k.resources,settlements:settlements(s,k.id).length,armies:s.armies.filter(a=>a.owner===k.id).map(a=>({tile:a.tile,target:a.target,order:a.order,units:a.units})),plans:s.intrigue.plans.filter(p=>p.actor===k.id).slice(-5)}))}));
  }
}
for(const difficulty of ['easy','medium','hard','insane']){
  const group=rows.filter(r=>r.difficulty===difficulty);if(!group.length)continue;
  console.log(JSON.stringify({summary:difficulty,...Object.fromEntries(['founded','captures','campaignsCompleted','shortageHouseRounds','stalled','gold'].map(k=>[k,group.reduce((n,r)=>n+r[k],0)]))}));
}
