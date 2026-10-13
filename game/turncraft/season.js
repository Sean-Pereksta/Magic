/* TurnCraft — Season of the Rift. Story and wave schedules, no simulation mutations. */
(function(root){
  'use strict';
  const REGIONS=Object.freeze([
    {id:'classic',name:'The Broken Frontier',mapId:'classic',waves:7,enemy:'brood',champion:'zergling',boss:'ultralisk',portrait:'marshal-vale',reward:'First Signal',medal:'Frontier Watch',accent:'#69caff',description:'Hold the northern relay while the first impossible transmissions arrive.',story:'Marshal Vale: "We thought the frontier was quiet. Then the sky began to answer us."',
      roster:['zergling','zergling','roach','hydralisk','ravager','ultralisk']},
    {id:'ember-rift',name:'Embers of the Rift',mapId:'ember-rift',waves:8,enemy:'frontier',champion:'hellion',boss:'siegeTank',portrait:'dr-oren',reward:'Cinder Seal',medal:'Ash Walker',accent:'#ff916a',description:'Heat and static ignite the old weapons foundries.',story:'Dr. Oren: "These machines are receiving orders from a century that never happened."',
      roster:['rifleman','marauder','reaper','hellion','goliath','siegeTank']},
    {id:'frostline-basin',name:'The Silent Frostline',mapId:'frostline-basin',waves:9,enemy:'ascendant',champion:'immortal',boss:'carrier',portrait:'oracle-seris',reward:'Glacial Seal',medal:'Icebreaker',accent:'#a4e5ff',description:'Defend the buried transmitter beneath the frozen ring.',story:'Oracle Seris: "The ice remembers an empire. Its defenders have forgotten who they serve."',
      roster:['zealot','dragoon','sentinel','sentry','immortal','carrier']},
    {id:'verdant-divide',name:'The Living Divide',mapId:'verdant-divide',waves:10,enemy:'brood',champion:'ravager',boss:'ultralisk',portrait:'brood-harbinger',reward:'Verdant Seal',medal:'Rootbreaker',accent:'#75dba6',description:'The jungle moves. Survive converging ambushes and protect the beacon.',story:'The Harbinger: "The roots are listening. Every footstep wakes the hive."',
      roster:['zergling','roach','hydralisk','ravager','mutalisk','ultralisk']},
    {id:'shattered-dunes',name:'Shattered Oath',mapId:'shattered-dunes',waves:11,enemy:'frontier',champion:'goliath',boss:'battlecruiser',portrait:'hollow-marshal',reward:'Oasis Seal',medal:'Dunebreaker',accent:'#f5c978',description:'Recover the last working map from the buried citadel.',story:'The Hollow Marshal: "The desert kept its promises. We did not."',
      roster:['rifleman','marauder','reaper','goliath','siegeTank','battlecruiser']},
    {id:'orbital-foundry',name:'The Null Foundry',mapId:'orbital-foundry',waves:12,enemy:'ascendant',champion:'disruptor',boss:'carrier',portrait:'rift-heart',reward:'Rift Seal',medal:'Riftwarden',accent:'#b9a6ff',description:'Make a final stand inside the machine that is rewriting the war.',story:'Oracle Seris: "It was never a gate. It was a signal calling every army home."',
      roster:['zealot','dragoon','sentry','templar','disruptor','carrier']}
  ]);
  const TIERS=Object.freeze([
    {min:2400,id:'grandmaster',name:'Grandmaster'}, {min:2100,id:'master',name:'Master'},
    {min:1800,id:'diamond',name:'Diamond'}, {min:1550,id:'platinum',name:'Platinum'},
    {min:1300,id:'gold',name:'Gold'}, {min:1100,id:'silver',name:'Silver'},
    {min:900,id:'bronze',name:'Bronze'}, {min:0,id:'recruit',name:'Recruit'}
  ]);
  function rank(rating,games=0){
    if(!games)return {id:'recruit',name:'Unranked',rating:1000,games:0};
    const value=Math.max(0,Math.round(Number(rating)||1000));
    return {...(TIERS.find(t=>value>=t.min)||TIERS[TIERS.length-1]),rating:value,games};
  }
  function mixWave(regionIndex,wave,endless=false){
    const region=REGIONS[Math.max(0,Math.min(REGIONS.length-1,regionIndex))],n=Math.max(1,Math.trunc(wave));
    const isBoss=n%(endless?10:region.waves)===0;
    const tier=Math.min(region.roster.length-1,Math.floor((n-1)/2)+(endless?Math.floor(n/8):0));
    // Hard cap prevents endless waves from flooding the 42x30 battlefield.
    const count=Math.min(24,2+Math.floor(n*1.15)+(endless?Math.floor(n/10):0)+(isBoss?1:0));
    const lineup=Array.from({length:count},(_,i)=>{
      if(isBoss&&i===count-1)return region.boss;
      const index=Math.min(tier,Math.max(0,Math.floor((i+n*3)%Math.max(1,tier+1))));
      return region.roster[index];
    });
    return {wave:n,isBoss,lineup,region,warning:isBoss?`COMMANDER WAVE ${n}`:n%5===0?'ELITE ASSAULT':'INCOMING HOSTILES',
      bonusMinerals:35+Math.min(250,n*12),bonusGas:Math.min(100,n*4)};
  }
  function safeCampaign(value={}){
    const completed=Array.isArray(value.completed)?value.completed.filter(id=>REGIONS.some(r=>r.id===id)):[];
    const bestWaves={};for(const r of REGIONS)bestWaves[r.id]=Math.max(0,Math.min(99999,Math.floor(Number(value.bestWaves?.[r.id])||0)));
    return {completed,unlocked:Math.max(1,Math.min(REGIONS.length,Math.floor(Number(value.unlocked)||1))),bestWaves,
      endlessBest:Math.max(0,Math.floor(Number(value.endlessBest)||0)),medals:Array.isArray(value.medals)?value.medals.filter(x=>typeof x==='string').slice(0,100):[]};
  }
  function advance(progress,regionId,wave,complete,endless){
    const next=safeCampaign(progress),regionIndex=REGIONS.findIndex(r=>r.id===regionId);
    if(regionIndex<0)return next;
    next.bestWaves[regionId]=Math.max(next.bestWaves[regionId],wave);
    if(endless){next.endlessBest=Math.max(next.endlessBest,wave);return next}
    if(complete&&!next.completed.includes(regionId)){next.completed.push(regionId);
      next.unlocked=Math.max(next.unlocked,Math.min(REGIONS.length,regionIndex+2));
      if(!next.medals.includes(REGIONS[regionIndex].medal))next.medals.push(REGIONS[regionIndex].medal);
    }
    return next;
  }
  root.TurncraftSeason=Object.freeze({REGIONS,TIERS,rank,mixWave,safeCampaign,advance});
})(typeof window!=='undefined'?window:globalThis);
