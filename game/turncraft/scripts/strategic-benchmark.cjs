// Run manually: node scripts/strategic-benchmark.cjs [ticks, default 80]
// Headless authoritative simulation timings include VM overhead and exclude GPU,
// browser layout, sprites and network; rendering has separate browser coverage.
const {performance}=require('node:perf_hooks');
const {simulation}=require('../tests/helpers/expansion-harness.cjs');
const ticks=Math.max(8,Math.min(1200,Number(process.argv[2])||80));
const reports=[];
for(const [map,count]of [['fourfold-divide',4],['rift-dominion',8]]){
  const factions=Array.from({length:count},(_,i)=>['frontier','ascendant','brood'][i%3]),run=simulation('frontier',{factions});
  const start=performance.now();
  const setup=run.plain(`(()=>{chosenMapId=${JSON.stringify(map)};createGame(${JSON.stringify(factions.map((factionId,i)=>({name:'Commander '+(i+1),factionId,ai:i>0,aiDifficulty:i%2?'hard':'pro',aiStyle:'balanced'})))});
    performance.now=()=>testClock;tileVisibleToFaction=(f,x,y)=>!!visionCache.get(f)?.has(x+','+y);const armies=[];
    for(const p of players){p.lastAi=0;p.aiPhaseAt=null;p.minerals=1200;p.gas=600;const base=mapLayout().starts[p.idx],types={frontier:['rifleman','medic','siegeTank'],ascendant:['zealot','dragoon','sentinel'],brood:['zergling','roach','hydralisk']}[p.factionId];
      const spots=formationTargets(base.x,base.y,225).filter(q=>dist(q,base)<=8&&!groundBlocked(q.x,q.y)&&!entityAt(q.x,q.y)&&!nodeAt(q.x,q.y));
      const army=spots.slice(0,20).map((q,i)=>addEntity('unit',types[i%types.length],p.idx,q.x,q.y));armies.push(army.map(u=>u.id));
    }
    globalThis.benchArmies=armies;globalThis.benchOrigins=entities.filter(e=>e.kind==='unit').map(e=>({id:e.id,x:e.x,y:e.y}));
    for(const p of players.filter(p=>!p.ai)){const target=mapLayout().starts[(p.idx+${Math.floor(count/2)})%players.length];orderFormation(armies[p.idx],target.x,target.y,p.idx)}
    return {width:BOARD_W,height:BOARD_H,commanders:players.length,initialUnits:entities.filter(e=>e.kind==='unit').length};})()`);
  const setupMs=performance.now()-start,profiles={movement:[],vision:[],ai:[]};
  for(let tick=0;tick<ticks;tick++){
    run('testClock+=250');
    for(const [name,code]of [['movement','updatePersistentOrders();updateFx(.25)'],['vision','updateLastSeenIntel()'],['ai','aiThinkAll()']]){const began=performance.now();run(code);profiles[name].push(performance.now()-began)}
  }
  const state=run.plain(`(()=>{const troops=entities.filter(e=>e.kind==='unit'&&e.alive!==false&&!e.garrisonedIn),ground=troops.filter(e=>!isFlying(e));return {liveUnits:troops.length,moved:benchOrigins.filter(p=>{const e=entityById(p.id);return e&&dist(e,p)>4}).length,remainingOrders:troops.filter(u=>u.moveDestination||u.attackMoveDestination).length,blocked:ground.filter(u=>groundBlocked(u.x,u.y,u)).length,overlaps:ground.length-new Set(ground.map(u=>u.x+','+u.y)).size,fields:navigationCache.fields.size,routeBuilds:navigationCache.builds,routeHits:navigationCache.hits,routeVisited:navigationCache.visited,aiWaves:players.filter(p=>p.strategy.wave).length}})()`);
  const timings=Object.fromEntries(Object.entries(profiles).map(([name,samples])=>{const sorted=samples.slice().sort((a,b)=>a-b);return [name,{meanMs:+(samples.reduce((n,x)=>n+x,0)/samples.length).toFixed(2),p95Ms:+sorted[Math.floor(sorted.length*.95)].toFixed(2),maxMs:+Math.max(...samples).toFixed(2)}]}));
  const report={map,...setup,ticks,simulatedSeconds:ticks/4,setupMs:+setupMs.toFixed(2),timings,...state};reports.push(report);console.log(JSON.stringify(report));
  if(state.blocked||state.overlaps)process.exitCode=1;
}
if(process.env.STRATEGIC_BENCH_REPORT)require('node:fs').writeFileSync(process.env.STRATEGIC_BENCH_REPORT,JSON.stringify({headless:true,reports},null,2)+'\n');
