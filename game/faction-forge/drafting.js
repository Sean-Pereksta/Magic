/** The draft is shared by local games and the authoritative simulator. Account
 * tiers never determine battle rank, evolution eligibility, or combo level. */
import {COMBOS,comboStatus} from './combos.js';
import {EVOLUTIONS,RANK_ENHANCEMENTS,evolutionDescription} from './evolutions.js';
import {starterDeck,validateDeck,deckForState,sanitizeRoster,rosterCount,MAX_ACTIVE_UNITS} from './decks.js';

export const DRAFT_WEIGHTS=Object.freeze({troop:65,upgrade:15,evolution:10,combo:6,legacy:4});
export const LEGACY_DRAFT_CARDS=Object.freeze([
  {type:'relic',key:'banner_frontline',name:'Banner of the Frontline',desc:'Frontline units gain +2 max HP.'},
  {type:'relic',key:'sharpshooter_doctrine',name:'Sharpshooter Doctrine',desc:'Backline units gain +1 range, -1 max HP.'},
  {type:'relic',key:'emergency_medkit',name:'Emergency Medkit',desc:'First allied unit below 35% HP heals 4 once.'},
  {type:'relic',key:'anti_armor_rounds',name:'Anti-Armor Rounds',desc:'Ranged units deal +1 damage to armored units.'},
  {type:'relic',key:'swarm_instinct',name:'Swarm Instinct',desc:'Small spawned units gain +1 speed for 3 seconds.'},
  {type:'relic',key:'arcane_battery',name:'Arcane Battery',desc:'First ward or spell effect starts stronger.'},
  {type:'relic',key:'poisoned_blades',name:'Poisoned Blades',desc:'Melee attacks apply a small wound effect.'},
  {type:'tactic',key:'opening_volley',name:'Opening Volley',desc:'At battle start, enemy front row takes 2 damage.'},
  {type:'tactic',key:'forced_march',name:'Forced March',desc:'Frontline units start 1 tile closer.'},
  {type:'tactic',key:'bodyguard',name:'Bodyguard',desc:'Strongest backline unit starts with a temporary shield.'},
  {type:'tactic',key:'sabotage',name:'Sabotage',desc:'Enemy highest-HP unit starts at 85% HP.'},
  {type:'tactic',key:'cleanse',name:'Cleanse',desc:'Remove initial poison, toxic and acid from your side.'},
  {type:'formation',key:'wide',name:'Wide Formation',desc:'Units spread across lanes to reduce splash damage.'},
  {type:'formation',key:'spearhead',name:'Spearhead Formation',desc:'Strongest tank starts center-front.'},
  {type:'formation',key:'protected_backline',name:'Protected Backline',desc:'Supports and ranged units start farther back.'},
  {type:'formation',key:'ambush',name:'Ambush Formation',desc:'Assassins start wider and slightly advanced.'}
]);
const rank=value=>Math.max(1,Math.min(3,Math.floor(Number(value)||1)));
const current=(roster,key)=>(roster||[]).find(r=>r.type===key&&r.count>0);
const roman=['','I','II','III'];
const identity=card=>card.type==='troop'?'troop:'+card.key:card.type==='special'?'upgrade:'+card.for:card.type+':'+card.key+(card.path?':'+card.path:'');
const unique=values=>[...new Set(values)];
const sample=(values,rng,weight=()=>1)=>{
  const total=values.reduce((sum,value)=>sum+Math.max(0,weight(value)),0);
  if(!total)return values[0];
  let remaining=Math.min(.999999999,Math.max(0,Number(rng())||0))*total;
  for(const value of values){remaining-=Math.max(0,weight(value));if(remaining<0)return value;}
  return values[values.length-1];
};

/** Only show combinations that use a collected troop and whose other partners
 * are available in this match. The tracker is deliberately bounded. */
export function relevantComboStates(roster,faction,{availableTypes,comboEnhancements=[],limit=4}={}){
  const available=availableTypes?new Set(availableTypes):null;
  const owned=new Set((roster||[]).filter(r=>r.count>0).map(r=>r.type));
  return comboStatus(roster||[],faction,comboEnhancements).filter(combo=>
    combo.units.some(key=>owned.has(key))&&(!available||combo.units.every(key=>available.has(key)))
  ).sort((a,b)=>(a.missing?.length||0)-(b.missing?.length||0)||(b.level||0)-(a.level||0)||a.id.localeCompare(b.id)).slice(0,limit);
}

/** Recruits dominate while slots remain; full teams develop ranks, paths and
 * combinations. Formation changes remain available after all upgrades. */
export function createDraftPack({roster=[],faction,deck=starterDeck(faction),availableTypes=deck?.troops||[],troops={},evolutions={},comboEnhancements=[],ownedRelics=[],ownedTactics=[],formation='balanced',size=3}={},rng=Math.random){
  if(!validateDeck(deck).valid||deck.faction!==faction)return [];
  roster=sanitizeRoster(roster,deck);
  const available=unique(availableTypes).filter(key=>deck.troops.includes(key)&&troops[key]&&troops[key].faction===faction);
  if(!available.length)return [];
  const allowed=new Set(available),owned=roster.filter(r=>r.count>0&&allowed.has(r.type));
  const name=key=>troops[key]?.name||key;
  const relevant=relevantComboStates(roster,faction,{availableTypes:available,comboEnhancements,limit:100});
  const partners=new Map();
  for(const combo of relevant)if((combo.missing||[]).length===1&&!partners.has(combo.missing[0]))partners.set(combo.missing[0],combo);
  const recruitment=(rosterCount(roster)<MAX_ACTIVE_UNITS?available:[]).map(key=>{
    const partner=partners.get(key);
    return {type:'troop',key,name:name(key),role:troops[key].role,draftCategory:'troop',
      ...(partner?{comboId:partner.id}:{}),
      desc:partner?`Recruit ${name(key)} to activate ${partner.name}.`:current(roster,key)?`Add ${name(key)} at Rank ${roman[rank(current(roster,key).lvl)]}. Every copy shares battle upgrades.`:`Recruit ${name(key)} at Rank I. Build its signature ability with battle upgrades.`};
  });
  const upgrades=owned.filter(r=>rank(r.lvl)<3).map(r=>({type:'special',special:'upgrade',for:r.type,name:'Upgrade '+name(r.type),draftCategory:'upgrade',desc:evolutionDescription(r.type,evolutions[r.type]||'a',rank(r.lvl)+1)||RANK_ENHANCEMENTS[r.type]?.[rank(r.lvl)+1]||'Improve this troop’s signature ability.'}));
  const paths=owned.filter(r=>!evolutions[r.type]&&EVOLUTIONS[r.type]).flatMap(r=>Object.values(EVOLUTIONS[r.type]).map(path=>({type:'evolution',key:r.type,for:r.type,path:path.id,name:`${name(r.type)} · ${path.name}`,draftCategory:'evolution',desc:path.description,tradeoff:path.tradeoff})));
  const enhancements=relevant.filter(combo=>!(combo.missing||[]).length&&!comboEnhancements.includes(combo.id)).map(combo=>({type:'combo',key:combo.id,for:combo.units[0],name:'Enhance '+combo.name,draftCategory:'combo',desc:combo.description,comboId:combo.id,units:combo.units}));
  const legacy=LEGACY_DRAFT_CARDS.filter(card=>card.type==='relic'?!ownedRelics.includes(card.key):card.type==='tactic'?!ownedTactics.includes(card.key):card.key!==formation).map(card=>({...card,draftCategory:'legacy'}));
  const categories={troop:recruitment,upgrade:upgrades,evolution:paths,combo:enhancements,legacy};
  const selected=[],used=new Set();
  const desired=Math.max(1,Math.min(5,Math.floor(Number(size)||3)));
  while(selected.length<desired){
    const pools=Object.entries(categories).map(([category,cards])=>({category,cards:cards.filter(card=>!used.has(identity(card)))})).filter(pool=>pool.cards.length&&(owned.length||pool.category!=='legacy'||!selected.some(card=>card.draftCategory==='legacy')));
    if(!pools.length)break;
    // Reserve a troop only if the other choices used the enhancement budget.
    // Ordinary sampling keeps recruits in any slot rather than fixing slot one.
    const requireTroop=recruitment.length>0&&selected.length===desired-1&&!selected.some(card=>card.type==='troop');
    const pool=sample(requireTroop?pools.filter(value=>value.category==='troop'):pools,rng,value=>DRAFT_WEIGHTS[value.category]);
    const card=sample(pool.cards,rng,value=>{
      if(value.type==='troop')return value.comboId?1.8:current(roster,value.key)?1.2:1.5;
      return 1;
    });
    selected.push(card);used.add(identity(card));
    // Show one clear specialization decision for a unit in a pack.
    if(card.type==='evolution')for(const path of Object.values(EVOLUTIONS[card.key]))used.add(identity({...card,path:path.id}));
  }
  return selected;
}

/** All card kinds, including old double cards, use the same bounded rules.
 * Rejected stale cards are consumed without falling through to legacy logic. */
export function applyDraftPick(state,side,card){
  if(!state||!['p0','p1'].includes(side)||!card)return false;
  const deck=deckForState(state,side);
  if(!deck)return true;
  state.roster||={p0:[],p1:[]};
  const roster=state.roster[side]=sanitizeRoster(state.roster[side]||[],deck),key=card.for||card.key,own=current(roster,key);
  const allowed=deck.troops.includes(key);
  if(card.type==='troop'){
    if(allowed&&rosterCount(roster)<MAX_ACTIVE_UNITS){if(own)own.count++;else roster.push({type:key,lvl:1,count:1});}
  }else if(card.type==='special'||card.type==='upgrade'){
    if(!allowed||!own)return true;
    if(card.type==='upgrade'||card.special==='upgrade')own.lvl=Math.min(3,rank(own.lvl)+1);
    if(card.special==='double')own.count+=Math.min(own.count,MAX_ACTIVE_UNITS-rosterCount(roster));
  }else if(card.type==='relic'||card.type==='tactic'){
    if(!LEGACY_DRAFT_CARDS.some(value=>value.type===card.type&&value.key===key))return true;
    const field=card.type==='relic'?'relics':'tactics';state[field]||={p0:[],p1:[]};state[field][side]||=[];
    if(!state[field][side].includes(key))state[field][side].push(key);
  }else if(card.type==='formation'){
    if(key!=='balanced'&&!LEGACY_DRAFT_CARDS.some(value=>value.type==='formation'&&value.key===key))return true;
    state.formations||={p0:'balanced',p1:'balanced'};state.formations[side]=key;
  }else
  if(card.type==='evolution'){
    if(!allowed||!own||!EVOLUTIONS[key]?.[card.path])return true;
    const all=state.evolutions||(state.evolutions={p0:{},p1:{}}),paths=all[side]||(all[side]={});
    if(paths[key])return true;
    paths[key]=card.path;own.lvl=Math.max(2,rank(own.lvl));
  }else if(card.type==='combo'){
    const combo=COMBOS.find(combo=>combo.id===card.key&&combo.faction===state.factions?.[side]);
    if(!combo||!combo.units.every(type=>deck.troops.includes(type)))return true;
    const status=comboStatus(roster,combo.faction,[]).find(value=>value.id===combo.id);
    if(!status||status.missing?.length)return true;
    const all=state.comboEnhancements||(state.comboEnhancements={p0:[],p1:[]}),enhanced=all[side]||(all[side]=[]);
    if(!enhanced.includes(combo.id))enhanced.push(combo.id);
  }
  return true;
}

/** Plain text metadata is safe for cards, screen readers, and detail dialogs. */
export function draftCardPresentation(card,roster=[],{faction,troops={},evolutions={},comboEnhancements=[],availableTypes}={}){
  const key=card.type==='combo'?(COMBOS.find(combo=>combo.id===card.key)?.units[0]):card.for||card.key;
  const own=current(roster,key),currentRank=own?rank(own.lvl):0;
  const upgrading=card.type==='upgrade'||card.type==='special'&&card.special==='upgrade';
  const resultingRank=upgrading?Math.min(3,currentRank+1):card.type==='evolution'?Math.max(2,currentRank):currentRank||1;
  const path=card.path||evolutions[key]||'a';
  let improvement=card.desc||'';
  if(upgrading)improvement=evolutionDescription(key,path,resultingRank)||RANK_ENHANCEMENTS[key]?.[resultingRank]||improvement;
  if(card.type==='evolution')improvement=evolutionDescription(key,path,resultingRank)||improvement;
  const next=roster.map(r=>({...r}));
  if(card.type==='troop'){const entry=current(next,key);if(entry)entry.count++;else next.push({type:key,count:1,lvl:1});}
  if(upgrading||card.type==='evolution'){const entry=current(next,key);if(entry)entry.lvl=resultingRank;}
  const before=comboStatus(roster,faction,comboEnhancements),after=comboStatus(next,faction,card.type==='combo'?unique([...comboEnhancements,card.key]):comboEnhancements);
  const relevant=after.filter(combo=>combo.units.includes(key)&&(combo.id===card.comboId||combo.id===card.key||combo.units.some(unit=>current(roster,unit)))&&(!availableTypes||combo.units.every(unit=>availableTypes.includes(unit))));
  const preview=relevant.map(combo=>{
    const prior=before.find(entry=>entry.id===combo.id),unlocks=(prior?.missing?.length||0)>0&&!(combo.missing||[]).length;
    return {...combo,unlocks,enhances:!unlocks&&(combo.level||0)>(prior?.level||0)};
  }).sort((a,b)=>Number(b.unlocks)-Number(a.unlocks)||Number(b.enhances)-Number(a.enhances)||(a.missing?.length||0)-(b.missing?.length||0)).slice(0,2);
  const rankLabel=currentRank!==resultingRank&&currentRank?`Rank ${roman[currentRank]} → ${roman[resultingRank]}`:card.type==='troop'?`Recruit · Rank ${roman[resultingRank]}`:card.type==='evolution'?`Specialize · Rank ${roman[resultingRank]}`:`Battle Rank ${roman[resultingRank]}`;
  const tradeoff=card.tradeoff||((upgrading||card.type==='evolution')?EVOLUTIONS[key]?.[path]?.tradeoff:'')||'';
  if(tradeoff&&improvement.endsWith(tradeoff))improvement=improvement.slice(0,-tradeoff.length).trim();
  return {key,currentRank,resultingRank,rankLabel,improvement,tradeoff,comboPreview:preview,unlocksCombo:preview.some(combo=>combo.unlocks),troopName:troops[key]?.name||key};
}
