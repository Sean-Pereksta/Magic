// Regional goals use accepted landscape distance and real discovery/victory hooks.
// They persist with the sanctuary; collecting a reward is an explicit, one-time act.
import {ITEMS} from './data.mjs';
import {recordTribeProgress} from './tribe-adventures.mjs';
import {LANDS,landProgress,normalizeLands} from './lands.mjs';

const THEMES=[
 ['Canopy Wanderer','Hidden Fruit Trail','Temple Guardian',{fruit:3,items:{berry:2}},{fruit:2,items:{bondCharm:1}},{fruit:4,items:{tonic:1}}],
 ['Savannah Scout','Acacia Forager','Sunstone Defender',{fruit:4,items:{berry:2}},{fruit:3,items:{trailRibbon:1}},{fruit:5,items:{salve:1}}],
 ['Crystal Explorer','Prismatic Finds','Grotto Champion',{fruit:5,items:{tonic:2}},{fruit:3,items:{treasureScent:1}},{fruit:6,items:{bondCharm:2}}],
 ['Marsh Pathfinder','Firefly Forager','Lagoon Protector',{fruit:6,items:{berry:3}},{fruit:4,items:{salve:2}},{fruit:7,items:{revive:1}}],
 ['Summit Trekker','Snowbound Supplies','Aurora Guardian',{fruit:7,items:{tonic:2}},{fruit:4,items:{campfireSeed:1}},{fruit:8,items:{revive:1}}],
 ['Highland Wayfarer','Stormwatch Cache','Thunder Challenger',{fruit:8,items:{trailRibbon:1}},{fruit:5,items:{scoutFeather:1}},{fruit:9,items:{tonic:3}}],
 ['Crater Explorer','Ember Trail Finds','Volcano Vanguard',{fruit:9,items:{salve:2}},{fruit:5,items:{treasureScent:2}},{fruit:10,items:{revive:1}}],
 ['Moonwell Wanderer','Silverleaf Supplies','Moonlit Sentinel',{fruit:10,items:{berry:4}},{fruit:6,items:{bondCharm:2}},{fruit:11,items:{scoutFeather:1}}],
 ['Starlight Voyager','Sanctuary Secrets','Celestial Champion',{fruit:11,items:{tonic:3}},{fruit:6,items:{campfireSeed:2}},{fruit:12,items:{revive:2}}],
 ['Golden Crown Pilgrim','Royal Treasury Trail','Kingdom Protector',{fruit:12,items:{berry:5}},{fruit:8,items:{treasureScent:2}},{fruit:15,items:{revive:2,bondCharm:2}}]
];
const freezeReward=reward=>Object.freeze({fruit:reward.fruit,items:Object.freeze({...reward.items})});
export const REGION_CHALLENGES=Object.freeze(LANDS.flatMap((land,index)=>{
 const [walk,finds,battle,...rewards]=THEMES[index],distance=750+index*250,discoveries=8+index*2,victories=2+Math.floor(index/2);
 return[
  {kind:'distance',title:walk,goal:distance,unit:'m',description:`Walk ${distance.toLocaleString()} accepted meters in ${land.name}.`},
  {kind:'discoveries',title:finds,goal:discoveries,unit:'finds',description:`Make ${discoveries} trail discoveries while exploring ${land.name}.`},
  {kind:'victories',title:battle,goal:victories,unit:'wins',description:`Win ${victories} wild encounters in ${land.name}. Tribe and friendly battles do not count.`}
 ].map((goal,i)=>Object.freeze({...goal,id:`${land.id}:${goal.kind}`,landId:land.id,reward:freezeReward(rewards[i])}));
}));
const byId=new Map(REGION_CHALLENGES.map(challenge=>[challenge.id,challenge])),landIds=new Set(LANDS.map(land=>land.id));
const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const counter=value=>Number.isSafeInteger(value)&&value>=0?Math.min(1000000000,value):0;
const rewardCopy=reward=>({fruit:reward.fruit,items:{...reward.items}});

export function normalizeChallenges(s){
 if(!s||!record(s.meta))throw Error('Open your saved sanctuary first.');
 normalizeLands(s);
 const old=record(s.meta.regionChallenges)?s.meta.regionChallenges:{},regions={};
 for(const land of LANDS){const saved=record(old.regions?.[land.id])?old.regions[land.id]:{};regions[land.id]={discoveries:counter(saved.discoveries),victories:counter(saved.victories)};}
 const claimed=[...new Set((Array.isArray(old.claimed)?old.claimed:[]).filter(id=>byId.has(id)))];
 const tracked=typeof old.tracked==='string'&&byId.has(old.tracked)&&!claimed.includes(old.tracked)&&s.world.unlocked.includes(byId.get(old.tracked).landId)?old.tracked:null;
 return s.meta.regionChallenges={version:1,regions,claimed,tracked};
}

export function regionChallenges(s,landId=landProgress(s).activeId){
 if(!landIds.has(landId))throw Error('Choose a region from the world map.');
 const saved=normalizeChallenges(s),progress=landProgress(s),unlocked=progress.unlocked.some(land=>land.id===landId);
 return REGION_CHALLENGES.filter(goal=>goal.landId===landId).map(goal=>{
  const current=goal.kind==='distance'?Math.max(0,s.world.regions[landId]?.distance||0):saved.regions[landId][goal.kind];
  return{...goal,reward:rewardCopy(goal.reward),current,percent:Math.min(100,current/goal.goal*100),complete:current+1e-6>=goal.goal,claimed:saved.claimed.includes(goal.id),tracked:saved.tracked===goal.id,unlocked};
 });
}
export const challengeViews=regionChallenges;

export function trackedChallenge(s){
 const saved=normalizeChallenges(s),goal=byId.get(saved.tracked);
 return goal?regionChallenges(s,goal.landId).find(row=>row.id===goal.id)||null:null;
}

export function trackChallenge(s,id){
 const saved=normalizeChallenges(s);
 if(id===null){saved.tracked=null;return null;}
 const goal=byId.get(id);if(!goal)throw Error('Choose a regional challenge.');
 const row=regionChallenges(s,goal.landId).find(row=>row.id===id);
 if(!row.unlocked)throw Error('Unlock this region before tracking its challenges.');
 if(row.claimed)throw Error('This challenge reward was already claimed.');
 // The view normalizer creates the current metadata object.
 s.meta.regionChallenges.tracked=id;return{...row,tracked:true};
}

export function recordRegionEvent(s,kind,amount=1,landId=landProgress(s).activeId,{battleId}={}){
 if(!['discoveries','victories'].includes(kind)||!Number.isSafeInteger(amount)||amount<=0||amount>1000)throw Error('Invalid regional progress event.');
 const saved=normalizeChallenges(s);
 const battle=s.run?.battle,validOrigin=kind==='victories'&&battleId&&battleId===battle?.id&&(battle.originLandId||battle.landId)===landId&&s.world.unlocked.includes(landId);
 if(!landIds.has(landId)||landId!==s.world.activeLand&&!validOrigin)throw Error('Regional progress belongs to the region you are exploring.');
 saved.regions[landId][kind]=Math.min(1000000000,saved.regions[landId][kind]+amount);
 return saved.regions[landId][kind];
}

export function claimChallenge(s,id){
 const goal=byId.get(id);if(!goal)throw Error('Choose a regional challenge.');
 const row=regionChallenges(s,goal.landId).find(challenge=>challenge.id===id);
 if(row.claimed)throw Error('This challenge reward was already claimed.');
 if(!row.unlocked||!row.complete)throw Error('Complete this challenge before claiming its reward.');
 const run=s.run;
 if(!['gps','practice'].includes(s.mode)||!['trail','camp'].includes(run?.phase)||!Array.isArray(run.party)||!run.party.some(monkeyId=>s.collection?.[monkeyId]?.hp>0))throw Error('Return to a living trail or camp to claim this reward.');
 if(!Number.isSafeInteger(run.fruit)||run.fruit<0||!record(run.items)||Object.entries(goal.reward.items).some(([item])=>!Object.hasOwn(ITEMS,item)||!Number.isSafeInteger(run.items[item])||run.items[item]<0))throw Error('Your expedition supplies are unavailable.');
 if(run.fruit+goal.reward.fruit>Number.MAX_SAFE_INTEGER||Object.entries(goal.reward.items).some(([item,n])=>run.items[item]+n>Number.MAX_SAFE_INTEGER))throw Error('Your expedition supplies are full.');
 run.fruit+=goal.reward.fruit;
 for(const [item,n] of Object.entries(goal.reward.items))run.items[item]+=n;
 s.meta.regionChallenges.claimed.push(id);
 recordTribeProgress(s,{challenges:1},`regional-challenge:${id}`);
 if(s.meta.regionChallenges.tracked===id)s.meta.regionChallenges.tracked=null;
 return rewardCopy(goal.reward);
}
