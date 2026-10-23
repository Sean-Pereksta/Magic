// Shared activities contain counts only: never GPS coordinates or route history.
export const TRIBE_WEEK=604800000,TRIBE_EPOCH=345600000;
export const TRIBE_ADVENTURE={target:600,reward:{fruit:6,items:{berry:1}},keys:['meters','discoveries','challenges','raids','trades']};
const whole=n=>Math.min(1000000000,Math.max(0,Math.floor(Number(n)||0)));
export const tribeWeek=(now=Date.now())=>Math.floor((now-TRIBE_EPOCH)/TRIBE_WEEK)*TRIBE_WEEK+TRIBE_EPOCH;
export const emptyContribution=()=>({meters:0,discoveries:0,challenges:0,raids:0,trades:0});
export const cleanContribution=value=>Object.fromEntries(TRIBE_ADVENTURE.keys.map(key=>[key,whole(value?.[key])]));
export function contributionPoints(value){const c=cleanContribution(value);return Math.floor(c.meters/50)+c.discoveries*3+c.challenges*25+c.raids*25+c.trades*15;}
export function contributionBadges(value){const c=cleanContribution(value);return [c.meters>=1000?'Explorer':'',c.discoveries>=5?'Collector':'',c.challenges+c.raids>0?'Guardian Challenger':'',c.trades>0?'Helpful Trader':''].filter(Boolean);}
export function weeklyProgress(state,now=Date.now()){
 state.meta.tribeAdventure??={weeks:{},applied:{},events:{}};const saved=state.meta.tribeAdventure;saved.weeks??={};saved.applied??={};saved.events??={};
 const week=tribeWeek(now),key=String(week);saved.weeks[key]??={...emptyContribution(),meterRemainder:0};
 // Keep the current and previous two weeks for interrupted-session recovery.
 for(const old of Object.keys(saved.weeks))if(Number(old)<week-TRIBE_WEEK*2)delete saved.weeks[old];
 return saved.weeks[key];
}
export function recordTribeProgress(state,value={},eventId,now=Date.now()){
 if(state?.mode!=='gps')return false;const progress=weeklyProgress(state,now),saved=state.meta.tribeAdventure;
 if(eventId&&saved.events[eventId])return false;
 const distance=Math.max(0,Number(value.meters)||0)+(progress.meterRemainder||0);progress.meters=whole(progress.meters+Math.floor(distance));progress.meterRemainder=distance-Math.floor(distance);
 for(const key of TRIBE_ADVENTURE.keys.filter(key=>key!=='meters'))progress[key]=whole(progress[key]+whole(value[key]));
 if(eventId){saved.events[eventId]=tribeWeek(now);for(const [id,week]of Object.entries(saved.events))if(week<tribeWeek(now)-TRIBE_WEEK*2)delete saved.events[id];}
 return true;
}
export function adventureReward(weekStart){const styles=['canopy-banner','sunstone-emblem','moonwell-sanctuary','crystal-banner'];return {...TRIBE_ADVENTURE.reward,items:{...TRIBE_ADVENTURE.reward.items},cosmetic:styles[Math.abs(Math.floor(weekStart/TRIBE_WEEK))%styles.length]};}
export function applyAdventureClaim(state,claim){
 if(!claim||claim.uid==null||!claim.id||claim.saveId==null||state.mode!=='gps'||state.run.phase==='dead')return false;
 weeklyProgress(state);const saved=state.meta.tribeAdventure,key=`${claim.uid}:${claim.id}`;if(saved.applied[key])return false;
 const reward=adventureReward(claim.weekStart);state.run.fruit+=reward.fruit;for(const [id,n]of Object.entries(reward.items))state.run.items[id]=(state.run.items[id]||0)+n;
 state.meta.cosmetics??={};state.meta.cosmetics[reward.cosmetic]=true;state.meta.titles??={};for(const badge of contributionBadges(claim.contribution))state.meta.titles[badge]=true;
 saved.applied[key]=true;return true;
}
