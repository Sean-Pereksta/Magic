// Shared equipment math for expeditions, protected social copies and previews.
// Fractions are summed before caps; a tier can never exceed the saved tier limit.
import {GEAR,ELEMENTS} from './data.mjs';

export const EQUIPMENT_TIER_CAP=12;
export const LEGACY_GEAR_IDS=Object.freeze(['fang','shell','heart','wind','satchel']);
const legacy=new Set(LEGACY_GEAR_IDS);
const positive=value=>Number.isFinite(value)&&value>0?value:0;
const capped=(value,max)=>Math.min(max,positive(value));
const statKeys=['hp','atk','def','speed','maxEnergy'];
export const equipmentTier=tier=>Number.isInteger(tier)&&tier>=1?Math.min(EQUIPMENT_TIER_CAP,tier):0;
export const equipmentTierScale=tier=>equipmentTier(tier)?1+.08*(equipmentTier(tier)-1):0;
const entries=gear=>Object.entries(gear||{}).filter(([id,tier])=>Object.hasOwn(GEAR,id)&&equipmentTier(tier));

// Flat released bonuses retain their values; percentages add before applying.
export function equipmentPercentBonuses(gear={}, {p,team=[],includeConditional=false,hpMaximum}={}){
  const out={hp:0,atk:0,def:0,speed:0,maxEnergy:0};
  for(const[id,tier]of entries(gear)){
    const item=GEAR[id],scale=equipmentTierScale(tier),effects=item.effects||{};
    for(const key of statKeys)if(Number.isFinite(item.statsPct?.[key]))out[key]+=item.statsPct[key]*scale;
    if(!includeConditional)continue;
    const maximum=hpMaximum||p?.maxHp||p?.combatStats?.hp||0,ratio=maximum>0?p?.hp/maximum:1;
    if(effects.healthyDefense&&ratio>.5)out.def+=effects.healthyDefense*scale;
    if(effects.woundedAttack&&ratio<.5&&p?.hp>0)out.atk+=effects.woundedAttack*scale;
    if(effects.diverseTeamStats&&equipmentDiverseTeam(team)){out.atk+=effects.diverseTeamStats*scale;out.def+=effects.diverseTeamStats*scale;}
  }
  return out;
}
export function equipmentDiverseTeam(team=[]){
  if(team.length!==3)return false;
  const elements=team.map(p=>p.types||[]);
  return elements.every(types=>types.length>0)&&elements.every((types,index)=>elements.every((other,j)=>j===index||types.every(type=>!other.includes(type))));
}
export function applyEquipmentStatPercentages(stats,gear={},options={}){
 const pct=equipmentPercentBonuses(gear,options),out={...stats};
 for(const key of statKeys)if(Number.isFinite(out[key]))out[key]*=Math.max(.1,1+pct[key]);
 return out;
}

export function equipmentBonuses(gear={}, {legacyOnly=false}={}){
  const out={hp:0,atk:0,def:0,speed:0,maxEnergy:0};
  for(const[id,tier]of entries(gear)){
    const item=GEAR[id];
    if(legacy.has(id)&&statKeys.includes(item.stat))out[item.stat]+=positive(item.amount)*equipmentTier(tier);
    if(!legacyOnly)for(const key of statKeys)out[key]+=positive(item.bonuses?.[key])*equipmentTierScale(tier);
  }
  return out;
}

export function equipmentEffects(gear={}){
  const out={roundHeal:0,energyDamage:0,physicalDamage:0,elementDamage:{},elementResist:{},lifesteal:0,roundEnergy:0,roundShield:0};
  for(const[id,tier]of entries(gear)){
    const effects=GEAR[id].effects||{},scale=equipmentTierScale(tier);
    for(const key of ['roundHeal','energyDamage','physicalDamage','lifesteal','roundEnergy','roundShield'])out[key]+=positive(effects[key])*scale;
    for(const key of ['elementDamage','elementResist'])for(const[type,value]of Object.entries(effects[key]||{}))if(Object.hasOwn(ELEMENTS,type))out[key][type]=(out[key][type]||0)+positive(value)*scale;
  }
  out.roundHeal=capped(out.roundHeal,.08);out.energyDamage=capped(out.energyDamage,.60);out.physicalDamage=capped(out.physicalDamage,.45);
  out.lifesteal=capped(out.lifesteal,.25);out.roundEnergy=capped(out.roundEnergy,3);out.roundShield=capped(out.roundShield,.18);
  for(const type of Object.keys(out.elementDamage))out.elementDamage[type]=capped(out.elementDamage[type],.50);
  for(const type of Object.keys(out.elementResist))out.elementResist[type]=capped(out.elementResist[type],.50);
  return out;
}

export function equipmentDamageMultiplier(attackerGear={},defenderGear={},move={}){
  if(!move.power)return 1;
  const attacker=equipmentEffects(attackerGear),defender=equipmentEffects(defenderGear),types=move.types?.length?move.types:[move.type||'neutral'];
  const elemental=types.reduce((sum,type)=>sum+(attacker.elementDamage[type]||0),0)/types.length;
  const resistance=types.reduce((sum,type)=>sum+(defender.elementResist[type]||0),0)/types.length;
  const physical=types.every(type=>type==='neutral')?attacker.physicalDamage:0;
  return(1+((move.cost||0)>0?attacker.energyDamage:0)+physical+elemental)*(1-resistance);
}

const healthMaximum=(p,st)=>positive(st?.hp??p?.maxHp);
function recoverHealth(p,maximum,amount){
  if(!p||p.hp<=0||!maximum)return 0;
  const restored=Math.min(Math.max(0,maximum-p.hp),Math.max(0,Math.round(amount)));p.hp+=restored;return restored;
}
// Call only for the conscious active combatant, once at the round/owner-turn end.
// Passive recovery never grants training XP or benefits a resting reserve.
export function applyEquipmentRecovery(p,st,gear={}){
  const result={healing:0,energy:0,shield:0};if(!p||p.hp<=0)return result;
  const effects=equipmentEffects(gear),maximum=healthMaximum(p,st),energyMaximum=positive(st?.maxEnergy??p.maxEnergy);
  result.healing=recoverHealth(p,maximum,maximum*effects.roundHeal);
  result.energy=Math.min(Math.max(0,energyMaximum-p.energy),Math.round(effects.roundEnergy));p.energy+=result.energy;
  const shield=Math.round(maximum*effects.roundShield);result.shield=Math.max(0,shield-(p.shield||0));
  if(result.shield)p.shield=(p.shield||0)+result.shield;
  return result;
}
export function applyEquipmentLifesteal(p,st,gear={},damage=0){return recoverHealth(p,healthMaximum(p,st),positive(damage)*equipmentEffects(gear).lifesteal);}

const percentage=value=>`${Number((value*100).toFixed(1))}%`;
const number=value=>Number(value.toFixed(1));
export function equipmentSummary(gear={}){
  const bonuses=equipmentBonuses(gear),pct=equipmentPercentBonuses(gear),effects=equipmentEffects(gear),out=[],labels={hp:'maximum health',atk:'attack',def:'defense',speed:'speed',maxEnergy:'maximum energy'};
  for(const key of statKeys)if(bonuses[key])out.push(`+${number(bonuses[key])} ${labels[key]}`);
  for(const key of statKeys)if(pct[key])out.push(`${pct[key]>0?'+':'−'}${percentage(Math.abs(pct[key]))} ${labels[key]}`);
  if(effects.energyDamage)out.push(`+${percentage(effects.energyDamage)} damage with abilities that cost energy`);
  if(effects.physicalDamage)out.push(`+${percentage(effects.physicalDamage)} physical damage`);
  for(const[type,value]of Object.entries(effects.elementDamage))out.push(`+${percentage(value)} ${ELEMENTS[type].name.toLowerCase()} damage`);
  for(const[type,value]of Object.entries(effects.elementResist))out.push(`${percentage(value)} less incoming ${ELEMENTS[type].name.toLowerCase()} damage`);
  if(effects.lifesteal)out.push(`Heal ${percentage(effects.lifesteal)} of health damage dealt`);
  if(effects.roundHeal)out.push(`Heal ${percentage(effects.roundHeal)} of maximum health after each active round`);
  if(effects.roundEnergy)out.push(`Recover ${Math.round(effects.roundEnergy)} extra energy after each active round`);
  if(effects.roundShield)out.push(`Refresh a ${percentage(effects.roundShield)} maximum-health shield after each active round; shields do not stack`);
  if(entries(gear).some(([id])=>GEAR[id].discoveryReduction))out.push('5% shorter walking discoveries; total acceleration caps at 20%');
  for(const[id,tier]of entries(gear))if(GEAR[id].newItem&&Object.keys(GEAR[id].effects||{}).length){
    const scale=equipmentTierScale(tier);out.push(GEAR[id].desc+(scale>1?` Effect percentages ×${Number(scale.toFixed(2))} at rank ${equipmentTier(tier)}; activation counts and global caps stay fixed.`:''));
  }
  return out;
}
