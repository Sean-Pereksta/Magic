/** Canonical deck rules used by the browser, draft engine and replay service. */
import {COMBOS} from './combos.js';
export const DECK_SIZE=5;
export const MAX_ACTIVE_UNITS=5;
export const MAX_CUSTOM_DECKS=9;
export const FACTION_TROOPS=Object.freeze({
  Vanir:Object.freeze(['rifleman','vrocket','vguard','miner','medic','suppressor','shieldcap','marksman','emp','railbreaker','signalhawk']),
  Aesir:Object.freeze(['phaser','blade','storm','guardian','wardmaker','architect','golem','runebinder','chronoweaver','miragewarden']),
  Helions:Object.freeze(['hellspawn','roach','defiler','blood','spitter','matron','viper','carrion','burrower','siphonleech','hiveherald'])
});
export const STARTER_DECKS=Object.freeze([
  {id:'starter-vanir',name:'Iron Vanguard',faction:'Vanir',troops:['vguard','marksman','suppressor','vrocket','medic'],playstyle:'Protect a precision firing line. Suppress approaching enemies and break heavy armor.',strengths:['Armor penetration','Ranged burst','Healing and suppression'],weaknesses:['Flanking assassins','Exposed support','Close-range pressure']},
  {id:'starter-aesir',name:'Arcane Tempest',faction:'Aesir',troops:['guardian','phaser','storm','runebinder','wardmaker'],playstyle:'Hold enemies with runes while lightning and sustained magic dismantle their formation.',strengths:['Chain lightning','Magical control','Protective wards'],weaknesses:['Timed burst','Disruption','Aggressive flanks']},
  {id:'starter-helions',name:'Blight Swarm',faction:'Helions',troops:['hellspawn','roach','defiler','spitter','blood'],playstyle:'Apply relentless pressure, corrode defenses and detonate into poisoned enemies.',strengths:['Corrosion and poison','Fast pressure','Plague explosions'],weaknesses:['Concentrated explosives','Suppression','Protected precision fire']}
].map(deck=>Object.freeze({...deck,troops:Object.freeze(deck.troops),strengths:Object.freeze(deck.strengths),weaknesses:Object.freeze(deck.weaknesses)})));
const clone=deck=>({id:deck.id,name:deck.name,faction:deck.faction,troops:[...deck.troops]});
export const factionForTroop=key=>Object.keys(FACTION_TROOPS).find(faction=>FACTION_TROOPS[faction].includes(key));
export const starterDeck=faction=>clone(STARTER_DECKS.find(deck=>deck.faction===faction)||STARTER_DECKS[0]);
export function unlockedTroops(profile){return Object.keys(profile?.troops||{}).filter(key=>profile.troops[key]?.unlocked);}
export function validateDeck(value,{unlockedTypes,troops}={}){
  const errors=[];
  if(!value||typeof value!=='object'||Array.isArray(value))return {valid:false,errors:['Choose a saved deck.'],deck:null};
  const faction=value.faction,keys=Array.isArray(value.troops)?value.troops:[];
  if(!FACTION_TROOPS[faction])errors.push('Choose Vanir, Aesir or Helions.');
  if(keys.length!==DECK_SIZE||new Set(keys).size!==DECK_SIZE)errors.push('A deck needs exactly five different troops.');
  if(keys.some(key=>typeof key!=='string'||factionForTroop(key)!==faction||(troops&&!troops[key])))errors.push('All five troops must belong to the selected faction.');
  if(unlockedTypes!==undefined){const unlocked=new Set(unlockedTypes);if(keys.some(key=>!unlocked.has(key)))errors.push('Unlock every troop before using this deck.');}
  if(typeof value.id!=='string'||!/^[a-zA-Z0-9_-]{1,80}$/.test(value.id))errors.push('The deck needs a valid identifier.');
  if(typeof value.name!=='string'||!value.name.trim()||value.name.trim().length>48)errors.push('Name your deck using 1–48 characters.');
  return {valid:errors.length===0,errors,deck:errors.length?null:{id:value.id,name:value.name.trim(),faction,troops:[...keys]}};
}
/** Invalid old/custom data is ignored; presets are always canonical and additive. */
export function normalizeDecks(values){
  const decks=STARTER_DECKS.map(clone),ids=new Set(decks.map(deck=>deck.id));
  for(const value of Array.isArray(values)?values:[]){const result=validateDeck(value);if(result.valid&&!ids.has(result.deck.id)&&decks.length<MAX_CUSTOM_DECKS+3){decks.push(result.deck);ids.add(result.deck.id);}}
  return decks;
}
export function deckForState(state,side){
  const deck=state?.decks?.[side];
  // Legacy saves migrate to a complete starter; malformed explicit decks fail closed.
  if(deck!==undefined&&deck!==null)return validateDeck(deck).deck;
  return starterDeck(state?.factions?.[side]);
}
/** One recruited row body occupies one slot. Multi-body identities are abilities. */
export function rosterCount(roster=[]){return roster.reduce((n,row)=>n+Math.max(0,Math.floor(Number(row?.count)||0)),0);}
export function sanitizeRoster(roster=[],deck){
  if(!validateDeck(deck).valid)return [];
  const rows=[],allowed=new Set(deck.troops);let remaining=MAX_ACTIVE_UNITS;
  for(const row of Array.isArray(roster)?roster:[]){if(!allowed.has(row?.type)||remaining<=0)continue;const count=Math.min(remaining,Math.max(0,Math.floor(Number(row.count)||0)));if(!count)continue;const lvl=Math.max(1,Math.min(3,Math.floor(Number(row.lvl)||1))),existing=rows.find(r=>r.type===row.type);if(existing){existing.count+=count;existing.lvl=Math.max(existing.lvl,lvl);}else rows.push({type:row.type,count,lvl});remaining-=count;}
  return rows;
}
/** Recovery and received snapshots obey the same body cap as fresh spawns. */
export function sanitizeBattleUnits(units=[],state){
  const counts=[0,0],ids=new Set();
  return (Array.isArray(units)?units:[]).filter(unit=>{
    if(!unit||![0,1].includes(unit.p)||!(unit.hp>0)||ids.has(unit.id)||counts[unit.p]>=MAX_ACTIVE_UNITS)return false;
    const deck=deckForState(state,unit.p===0?'p0':'p1');if(!deck)return false;
    const legal=deck.troops.includes(unit.type)||unit.illusion&&unit.type==='miragewarden'&&deck.troops.some(key=>['phaser','miragewarden'].includes(key))||unit.summoned&&['hellspawn','blood'].includes(unit.type)&&deck.troops.includes('matron');
    if(!legal)return false;counts[unit.p]++;ids.add(unit.id);return true;
  });
}
export function deckAnalysis(deck,{troops={}}={}){
  const keys=Array.isArray(deck?.troops)?deck.troops:[],preset=STARTER_DECKS.find(value=>value.id===deck?.id),has=group=>keys.filter(key=>group.includes(key)).length;
  const ratings={antiArmor:has(['marksman','railbreaker','emp','spitter','defiler','storm','phaser']),antiSwarm:has(['vrocket','suppressor','storm','blood','defiler']),frontline:has(['vguard','shieldcap','guardian','golem','roach','defiler']),ranged:keys.filter(key=>troops[key]?.role==='back'||['marksman','vrocket','phaser','storm','spitter','rifleman','railbreaker'].includes(key)).length,support:has(['medic','wardmaker','signalhawk','chronoweaver','miragewarden','carrion','hiveherald','architect']),mobility:has(['hellspawn','miner','blade','viper','burrower','phaser','siphonleech'])};
  const combos=COMBOS.filter(combo=>combo.faction===deck?.faction&&combo.units.every(key=>keys.includes(key))).map(combo=>({...combo,upgradeTip:'Rank II partners strengthen this combo; Rank III with an upgraded partner unlocks its legendary effect.'}));
  const labels={antiArmor:'Armor penetration',antiSwarm:'Area damage',frontline:'Frontline protection',ranged:'Ranged damage',support:'Support',mobility:'Mobility'};
  return {ratings,combos,playstyle:preset?.playstyle||'Recruit your five roles, then invest in battle ranks, specializations and combinations.',strengths:preset?.strengths||Object.keys(ratings).filter(key=>ratings[key]>=2).map(key=>labels[key]),weaknesses:preset?.weaknesses||Object.keys(ratings).filter(key=>ratings[key]===0).map(key=>'Limited '+labels[key].toLowerCase())};
}
