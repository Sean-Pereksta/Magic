// Permanent, comparable strength estimates shared by the roster and crew cards.
// Recompute from catalogue stats rather than trusting a saved combatStats/power field.
import {CONFIG,ELEMENTS,GEAR,SPECIES,stats} from './core.mjs';

export const POWER_EXPLANATION='Estimated power = max HP + 4 × attack + 4 × defense + 2 × speed + 8 × max energy, including equipped gear and evolution. Team power adds your starting three. Injuries do not lower power; abilities and elemental matchups still affect battles.';

const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const bounded=(value,min,max,fallback)=>Number.isFinite(value)?Math.min(max,Math.max(min,Math.floor(value))):fallback;
const own=(object,key)=>typeof key==='string'&&Object.hasOwn(object,key);
const defaultGear=p=>p?.gear||{};
const compare=(a,b)=>a<b?-1:a>b?1:0;

export function monkeyPower(p,gear=p?.gear||{}){
 if(!record(p)||!own(SPECIES,p.species))return 0;
 const level=bounded(p.level,1,CONFIG.maxLevel,1),stage=bounded(p.stage,0,3,0);
 const types=[...new Set((Array.isArray(p.types)?p.types:SPECIES[p.species].types||[]).filter(t=>own(ELEMENTS,t)))].slice(0,2);
 const permanent={species:p.species,level,stage,types};
 if(stage===3&&types.includes(p.mastery)&&own(CONFIG.masteries,p.mastery))permanent.mastery=p.mastery;
 const tiers=Object.fromEntries(Object.entries(record(gear)?gear:{}).filter(([id,tier])=>own(GEAR,id)&&Number.isInteger(tier)&&tier>=1&&tier<=12));
 const st=stats(permanent,tiers);
 return Math.max(0,Math.round(st.hp+4*st.atk+4*st.def+2*st.speed+8*st.maxEnergy));
}

// A repeated document/object never counts as an extra companion or crew.
function distinct(values,keyOf){
 const seenIds=new Set(),seenObjects=new Set();
 return(Array.isArray(values)?values:[]).filter(value=>{
  if(!record(value)||seenObjects.has(value))return false;
  const id=keyOf(value);
  if(id&&seenIds.has(id))return false;
  seenObjects.add(value);if(id)seenIds.add(id);return true;
 });
}
const monkeyId=p=>typeof p.id==='string'?p.id:'';
const profileId=p=>[p.uid,p.id,p.playerId].find(value=>typeof value==='string'&&value)||'';
const gearFor=(get,p)=>typeof get==='function'?get(p):defaultGear(p);

export function teamPower(members,getGear=defaultGear){
 return distinct(members,monkeyId).slice(0,3).reduce((sum,p)=>sum+monkeyPower(p,gearFor(getGear,p)),0);
}

function ranked(rows){
 rows.sort((a,b)=>b.power-a.power||compare(a.key,b.key)||a.index-b.index);
 let rank=0,previous;
 return rows.map((row,index)=>{
  if(row.power!==previous)rank=index+1;
  previous=row.power;
  const {key,index:sourceIndex,...entry}=row;
  return{...entry,rank};
 });
}

export function rankMonkeys(members,getGear=defaultGear){
 return ranked(distinct(members,monkeyId).map((monkey,index)=>({monkey,power:monkeyPower(monkey,gearFor(getGear,monkey)),key:monkeyId(monkey)||`${monkey.species||''}:${monkey.nickname||''}`,index})));
}

export function rankCrews(profiles){
 return ranked(distinct(profiles,profileId).map((profile,index)=>({profile,power:teamPower(profile.team),key:profileId(profile)||String(profile.name||''),index})));
}
