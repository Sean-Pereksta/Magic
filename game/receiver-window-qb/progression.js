/* Shared deterministic progression rules, used by gameplay and regression tests. */
(function(root){
  'use strict';
  const stats=['speed','cutting','turning','evasion','catching','strength','athleticism','size','tricks'];
  const clamp=(v,min,max)=>Math.max(min,Math.min(max,Number.isFinite(Number(v))?Number(v):min));
  const rating=v=>Math.round(clamp(v,1,Number.MAX_SAFE_INTEGER));
  function migratePlayer(p){
    p.prestige=Math.floor(clamp(p.prestige||0,0,1000000));
    p.trainings=Math.floor(clamp(p.trainings||0,0,Number.MAX_SAFE_INTEGER));
    p.athleticism=rating(p.athleticism??((p.speed+p.evasion)/2));
    p.size=rating(p.size??({Big:80,Tall:72,Compact:35,Lean:42}[p.appearance?.sizeName]||55));
    p.tricks=rating(p.tricks??((p.cutting+p.evasion)/2));
    p.trainingByStat=Object.fromEntries(stats.map(k=>[k,Math.floor(clamp(p.trainingByStat?.[k]||0,0,5))]));
    for(const k of stats)p[k]=rating(p[k]);
    return p;
  }
  function train(p,stat,cash,gain){
    if(!stats.includes(stat)||p.trainingByStat[stat]>=5)return {ok:false,reason:'This attribute is fully trained.'};
    const cost=90+p.trainingByStat[stat]*55;
    if(cash<cost)return {ok:false,reason:`You need $${cost-cash} more.`};
    const before=p[stat];p[stat]=rating(p[stat]+clamp(gain,4,8));p.trainingByStat[stat]++;p.trainings++;
    return {ok:true,cash:cash-cost,gain:p[stat]-before,cost};
  }

  // Ratings remain uncapped; physical benefits above 100 taper to avoid broken movement.
  const effective=v=>v<=100?v:100+40*(1-Math.exp(-(v-100)/80));
  function signingPrice(overall){
    const target=Math.max(60,rating(overall));
    return Math.round((target<80?300+(target-60)*20:target<90?1500+(target-80)*170:target<97?3500+(target-90)*350:7000+(target-97)*650)/10)*10;
  }
  function recruit(p,roll=Math.random(),random=Math.random){
    const band=roll<.85?[60,79,'Prospect']:roll<.95?[80,89,'Star']:roll<.99?[90,96,'Elite']:[97,105,'Generational'];
    const target=band[0]+Math.floor(random()*(band[1]-band[0]+1));
    const mean=stats.reduce((n,k)=>n+p[k],0)/stats.length;
    for(const k of stats)p[k]=rating(p[k]+target-mean);
    p.rarity=band[2];
    p.price=signingPrice(target);
    return p;
  }
  function prestigeCost(p){return Math.min(Number.MAX_SAFE_INTEGER,Math.round(7500*Math.pow(2.5,p.prestige||0)));}
  function prestige(p,cash){
    if(!stats.some(k=>p.trainingByStat[k]>=5))return {ok:false,reason:'Finish five sessions in an attribute before prestiging.'};
    const cost=prestigeCost(p);
    if(cash<cost)return {ok:false,reason:`You need ${cost-cash} more to prestige.`};
    p.prestige=(p.prestige||0)+1;
    for(const k of stats)p.trainingByStat[k]=0;
    return {ok:true,cash:cash-cost,cost};
  }
  function defenseProgress(round){
    const growth=Math.log1p(Math.max(0,round-1)/12);
    return {speed:growth*.65,accel:growth*1.5,turn:growth*.35,
      jump:3.05+1.65*(1-Math.exp(-growth/2)),
      depth:2+4*(1-Math.exp(-growth)),
      smartChance:.15+.65*(1-Math.exp(-growth)),
      hands:.18*(1-Math.exp(-growth/2))};
  }

  function payout(won,round,touchdowns){return (won?750+Math.min(1500,(round-1)*75):100+Math.min(150,(round-1)*10))+Math.floor(clamp(touchdowns,0,3))*25;}
  function traits(p){
    p=Object.fromEntries(stats.map(k=>[k,effective(p[k])]));
    const jumpVelocity=2.8+p.athleticism*.021;
    return {jumpVelocity,highReach:2.4*(.94+p.size*.0018)+jumpVelocity*jumpVelocity/19.62,
      pursuitBurst:1.13+p.catching*.0014,diveReach:1.42+p.catching*.002+p.athleticism*.002,
      sizeScale:.94+p.size*.0018,bodyBonus:(p.size-50)*.0015,
      jukeChance:Math.min(.82,.20+p.tricks*.0045+p.evasion*.0015),jukeCooldown:3.8-p.tricks*.014};
  }
  function matchRound(f){return Number.isSafeInteger(f.rematchRound)&&f.rematchRound>=1&&f.rematchRound<f.round?f.rematchRound:f.round;}
  function matchPayout(f,won,touchdowns){return Math.floor(payout(won,matchRound(f),touchdowns)*(matchRound(f)<f.round?.2:1));}
  function normalizeCompetition(f){
    f.pumpMemory=(Array.isArray(f.pumpMemory)?f.pumpMemory:[]).filter(x=>x&&Number.isInteger(x.target)&&x.target>=0&&x.target<4&&typeof x.concept==='string').slice(-64).map(x=>({target:x.target,concept:x.concept.slice(0,80)}));
    f.conceptMemory=(Array.isArray(f.conceptMemory)?f.conceptMemory:[]).filter(x=>typeof x==='string').slice(-24).map(x=>x.slice(0,80));
    f.rematchRound=matchRound(f)<f.round?matchRound(f):null;
    const results={};
    for(const [key,value] of Object.entries(f.opponentResults||{})){
      const n=Number(key);if(!Number.isSafeInteger(n)||n<1||n>f.round||!value||typeof value!=='object')continue;
      results[n]={wins:Math.floor(clamp(value.wins,0,1000000)),losses:Math.floor(clamp(value.losses,0,1000000))};
    }
    f.opponentResults=results;
    if(typeof f.matchInProgress!=='boolean'){const c=f.checkpoint;f.matchInProgress=!!c&&(c.down>1||c.ballSpotYards>0||c.seriesOffense>0||c.seriesDefense>0);}
    return f;
  }
  function validateSave(raw){
    const f=typeof raw==='string'?JSON.parse(raw):JSON.parse(JSON.stringify(raw));
    if(!f||!Array.isArray(f.team)||f.team.length!==4||!Array.isArray(f.market)||f.market.length!==5)throw new Error('This save is missing its roster or market.');
    for(const p of [...f.team,...f.market]){
      if(!p||typeof p.name!=='string'||p.name.length>80)throw new Error('Invalid receiver in save.');
      for(const k of stats.slice(0,6))if(!Number.isFinite(p[k])||p[k]<1||!Number.isSafeInteger(p[k]))throw new Error('Invalid receiver ratings.');
      migratePlayer(p);
    }
    for(const k of ['cash','round','wins'])if(!Number.isSafeInteger(f[k])||f[k]<(k==='round'?1:0))throw new Error('Invalid franchise progress.');
    if(f.checkpoint){const c=f.checkpoint;for(const k of ['seriesOffense','seriesDefense','ballSpotYards','down','lineToGainYards'])if(!Number.isFinite(c[k]))throw new Error('Invalid drive checkpoint.');
      if(c.seriesOffense<0||c.seriesOffense>2||c.seriesDefense<0||c.seriesDefense>2||!Number.isInteger(c.down)||c.down<1||c.down>4||c.ballSpotYards<0||c.ballSpotYards>=50||c.lineToGainYards<=c.ballSpotYards||c.lineToGainYards>50)throw new Error('Invalid drive checkpoint.');}
    return normalizeCompetition(f);
  }
  const api={matchRound,matchPayout,normalizeCompetition,stats,rating,effective,signingPrice,recruit,prestigeCost,prestige,defenseProgress,migratePlayer,train,payout,traits,validateSave};
  if(typeof module!=='undefined')module.exports=api;root.QBProgression=api;
})(typeof globalThis!=='undefined'?globalThis:this);
