/* Shared deterministic progression rules, used by gameplay and regression tests. */
(function(root){
  'use strict';
  const stats=['speed','cutting','turning','evasion','catching','strength','athleticism','size','tricks'];
  const clamp=(v,min,max)=>Math.max(min,Math.min(max,Number.isFinite(Number(v))?Number(v):min));
  const rating=v=>Math.round(clamp(v,1,100));
  function migratePlayer(p){
    p.athleticism=rating(p.athleticism??((p.speed+p.evasion)/2));
    p.size=rating(p.size??({Big:80,Tall:72,Compact:35,Lean:42}[p.appearance?.sizeName]||55));
    p.tricks=rating(p.tricks??((p.cutting+p.evasion)/2));
    p.trainingByStat=Object.fromEntries(stats.map(k=>[k,Math.floor(clamp(p.trainingByStat?.[k]||0,0,5))]));
    for(const k of stats)p[k]=rating(p[k]);
    return p;
  }
  function train(p,stat,cash,gain){
    if(!stats.includes(stat)||p.trainingByStat[stat]>=5||p[stat]>=100)return {ok:false,reason:'This attribute is fully trained.'};
    const cost=90+p.trainingByStat[stat]*55;
    if(cash<cost)return {ok:false,reason:`You need $${cost-cash} more.`};
    const before=p[stat];p[stat]=rating(p[stat]+clamp(gain,4,8));p.trainingByStat[stat]++;p.trainings++;
    return {ok:true,cash:cash-cost,gain:p[stat]-before,cost};
  }
  function payout(won,round,touchdowns){return (won?250+Math.min(500,(round-1)*25):100+Math.min(150,(round-1)*10))+Math.floor(clamp(touchdowns,0,3))*25;}
  function traits(p){
    return {jumpVelocity:2.8+p.athleticism*.014,highReach:3.18+p.athleticism*.005+p.size*.003,
      pursuitBurst:1.13+p.catching*.0014,diveReach:1.42+p.catching*.002+p.athleticism*.002,
      sizeScale:.94+p.size*.0018,bodyBonus:(p.size-50)*.0015,
      jukeChance:Math.min(.82,.20+p.tricks*.0045+p.evasion*.0015),jukeCooldown:3.8-p.tricks*.014};
  }
  function validateSave(raw){
    const f=typeof raw==='string'?JSON.parse(raw):JSON.parse(JSON.stringify(raw));
    if(!f||!Array.isArray(f.team)||f.team.length!==4||!Array.isArray(f.market)||f.market.length!==5)throw new Error('This save is missing its roster or market.');
    for(const p of [...f.team,...f.market]){
      if(!p||typeof p.name!=='string'||p.name.length>80)throw new Error('Invalid receiver in save.');
      for(const k of stats.slice(0,6))if(!Number.isFinite(p[k])||p[k]<1||p[k]>100)throw new Error('Invalid receiver ratings.');
      migratePlayer(p);
    }
    for(const k of ['cash','round','wins'])if(!Number.isSafeInteger(f[k])||f[k]<(k==='round'?1:0))throw new Error('Invalid franchise progress.');
    if(f.checkpoint){const c=f.checkpoint;for(const k of ['seriesOffense','seriesDefense','ballSpotYards','down','lineToGainYards'])if(!Number.isFinite(c[k]))throw new Error('Invalid drive checkpoint.');
      if(c.seriesOffense<0||c.seriesOffense>2||c.seriesDefense<0||c.seriesDefense>2||!Number.isInteger(c.down)||c.down<1||c.down>4||c.ballSpotYards<0||c.ballSpotYards>=50||c.lineToGainYards<=c.ballSpotYards||c.lineToGainYards>50)throw new Error('Invalid drive checkpoint.');}
    return f;
  }
  const api={stats,rating,migratePlayer,train,payout,traits,validateSave};
  if(typeof module!=='undefined')module.exports=api;root.QBProgression=api;
})(typeof globalThis!=='undefined'?globalThis:this);
