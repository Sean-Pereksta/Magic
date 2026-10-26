// Walking companions are independent of the first monkey sent into battle.
// These rules consume the saved RNG only during an accepted trail discovery.
import {SPECIES,ELEMENTS,DIRECTIONS} from './data.mjs';
export const PERSONAL_MATERIAL_IDS=Object.freeze(['ingot','starOre','crownCore']);
export const isPersonalMaterial=id=>PERSONAL_MATERIAL_IDS.includes(id);

const clamp=(n,lo,hi)=>Math.max(lo,Math.min(hi,n));
const eligible=(s,id)=>!!id&&s.run?.party?.includes(id)&&!!s.collection?.[id]&&s.collection[id].hp>0;
const rarityRank={common:0,uncommon:1,rare:2,epic:2,ultra:3,legendary:3};
const overrides={
  mossmop:{name:'Orchard Gatherer',forage:.07,weights:{fruit:1.6,leaf:1.3}},
  dunetail:{name:'Buried Treasure',forage:.05,weights:{ingot:1.45,earth:1.2}},
  quartzcrest:{name:'Crystal Prospector',forage:.07,weights:{ingot:1.5,starOre:1.4}},
  magnetuft:{name:'Magnetic Prospector',forage:.08,weights:{ingot:1.65,starOre:1.6}},
  glassmanta:{name:'Crystal Sifter',forage:.07,weights:{starOre:1.6,ice:1.4}},
  rootstone:{name:'Deep Root Forager',forage:.09,weights:{fruit:1.3,ingot:1.4}},
  obsidianback:{name:'Blackstone Prospector',forage:.07,weights:{ingot:1.65,starOre:1.35}},
  verdara:{name:'Worldroot Bounty',forage:.08,weights:{fruit:1.7,leaf:1.5,ingot:1.3}},
  obsidrax:{name:'Mountain Prospector',forage:.06,weights:{ingot:1.65,starOre:1.6}},
  basaltoracle:{name:'Monolith Prospector',forage:.08,weights:{ingot:1.6,starOre:1.7}},
  dawncrest:{name:'Dawn Scout',scout:.38},
  eclipsemask:{name:'Twilight Scout',scout:.50},
  auroraswing:{name:'Aurora Scout',scout:.40},
  noctyra:{name:'Hidden Trail Scout',scout:.45},
  voltaro:{name:'Horizon Scout',scout:.38}
};

export function normalizeExplorer(s){
  const r=s.run;if(!r)return null;
  if(!eligible(s,r.explorerId))r.explorerId=eligible(s,r.activeId)?r.activeId:(r.party||[]).find(id=>eligible(s,id))||null;
  return r.explorerId?s.collection[r.explorerId]:null;
}
export const activeExplorer=s=>normalizeExplorer(s);
export function setActiveMonkey(s,id){
  if(!['trail','camp'].includes(s.run?.phase))throw Error('Choose your active walking monkey on the trail or at camp, between encounters.');
  if(!eligible(s,id))throw Error('Choose a conscious monkey from your active roster.');
  s.run.explorerId=id;return s.collection[id];
}

export function explorationProfile(p){
  const species=SPECIES[p?.species];if(!species)return{name:'Trail Companion',forageChance:0,rareEncounterBonus:0,materialWeights:{},specialties:[],description:'Select a conscious companion from your active roster.'};
  const rank=rarityRank[species.rarity]??0,types=[...new Set([...(species.types||[]),...(p.types||[])])],weights={},extra=overrides[p.species]||{};
  const weight=(key,value)=>weights[key]=Math.max(weights[key]||1,value);
  for(const type of types){
    weight(type,1.75+rank*.45);
    if(type==='leaf')weight('fruit',1.8+rank*.20);
    if(type==='earth'){weight('ingot',2.5+rank*.75);weight('starOre',1.8+rank*.50);}
    if(type==='fire')weight('ingot',1.3+rank*.15);
    if(type==='ice'){weight('starOre',1.5+rank*.35);weight('ice',2.2+rank*.40);}
    if(type==='light'){weight('charmMaterial',1.7+rank*.3);weight('starOre',1.3+rank*.30);}
    if(type==='shadow')weight('charmMaterial',1.5+rank*.25);
  }
  for(const[key,n]of Object.entries(extra.weights||{}))weights[key]=(weights[key]||1)*n;
  const gatherer=['healer','defender','drainer'].includes(species.role),scout=['relay','controller'].includes(species.role);
  const forageChance=clamp([.04,.09,.17,.25][rank]+(gatherer?.025:0)+(extra.forage||0),0,.42);
  const rareEncounterBonus=clamp([0,.06,.20,.35][rank]+(scout?.12:0)+(types.includes('shadow')?.10:types.includes('storm')?.08:types.includes('light')?.05:0)+(extra.scout||0),0,1.2);
  const resourceName=id=>id==='fruit'?'Training fruit':id==='ingot'?'Ingots':id==='starOre'?'Star ore':id==='charmMaterial'?'Charm material':`${ELEMENTS[id]?.name||id} essence`;
  const specialties=Object.entries(weights).sort((a,b)=>b[1]-a[1]).map(([id,n])=>`${resourceName(id)} ×${Number(n.toFixed(1))}`);
  const name=extra.name||(types.includes('earth')?'Mineral Forager':types.includes('leaf')?'Canopy Forager':scout?'Rare Trail Scout':'Trail Forager');
  const percent=n=>Number((n*100).toFixed(1));
  return{name,forageChance,rareEncounterBonus,materialWeights:weights,specialties,
    description:`${percent(forageChance)}% chance of one additional pickup per discovery. Rare enemy selection weight +${percent(rareEncounterBonus)}% within the eligible regional pool.`};
}

// This adjusts ordinary eligible enemies; it does not unlock rare species early
// or turn an ordinary invitation into an Ultra/guardian battle.
export function explorationEncounterWeight(s,speciesId){
  const p=activeExplorer(s),target=SPECIES[speciesId];
  if(!p||!target||!['rare','epic'].includes(target.rarity))return 1;
  return 1+explorationProfile(p).rareEncounterBonus;
}

const multiplier=n=>Number.isFinite(n)&&n>=0?n:1;
export function trailResourceWeights(s,habitat={},options={}){
  const p=activeExplorer(s),profile=explorationProfile(p),local=habitat.resourceWeights||{},special=profile.materialWeights;
  const boost=id=>multiplier(local[id])*multiplier(special[id]),direction=DIRECTIONS[s.run?.direction]||'leaf',instinct=clamp(options.instinct||0,0,.2),rows={};
  // Ingots and star ore have bounded, explicit drop chances. Earth specialists
  // can deliberately prospect in caves without making ore an assured reward.
  const ingot=clamp(.025*boost('ingot'),.002,.15),starOre=clamp(.003*boost('starOre')*multiplier(local.ingot),.0005,.02);
  const localTypes=(habitat.types||[]).filter(id=>ELEMENTS[id]),localShare=localTypes.length?.30:0;
  const essenceWeights=Object.fromEntries(Object.keys(ELEMENTS).map(id=>{
    const directional=(id===direction?.62-instinct:0)+(Object.values(DIRECTIONS).includes(id)?.04:0)+(['earth','ice','light','shadow'].includes(id)?(.22+instinct)/4:0);
    // Habitat essence remains obtainable while walking in any direction.
    return[id,(directional*(1-localShare)+(localTypes.includes(id)?localShare/localTypes.length:0))*boost(id)];
  }));
  const essenceTotal=Object.values(essenceWeights).reduce((a,b)=>a+b,0);
  for(const[id,n]of Object.entries(essenceWeights))rows[`essence:${id}`]=.60*multiplier(local.essence)*n;
  rows.fruit=.25*boost('fruit');rows['item:charmMaterial']=.015*boost('charmMaterial');
  if(!options.forage){rows.item=.12*multiplier(local.item);rows.fragment=.03*multiplier(local.fragment);}
  const total=Object.values(rows).reduce((a,b)=>a+b,0)||essenceTotal||1;
  for(const id of Object.keys(rows))rows[id]=rows[id]/total*(1-ingot-starOre);
  rows['item:ingot']=ingot;rows['item:starOre']=starOre;return rows;
}
export function rollTrailResource(s,roll,habitat={},options={}){
  const rows=Object.entries(trailResourceWeights(s,habitat,options));let remaining=clamp(roll,0,1-Number.EPSILON),id=rows.at(-1)[0];
  for(const[key,n]of rows){remaining-=n;if(remaining<0){id=key;break;}}
  if(id==='fruit')return{category:'fruit',amount:options.forage?1:2};
  const[category,key]=id.split(':');return{category,...(key?{id:key}:{}),amount:1};
}
export function foragePickup(s,rng,habitat={}){
  if(s.run?.phase!=='trail')return null;const p=activeExplorer(s);if(!p)return null;
  const profile=explorationProfile(p);if(rng(s)>=profile.forageChance)return null;
  return{...rollTrailResource(s,rng(s),habitat,{forage:true}),monkeyId:p.id};
}
