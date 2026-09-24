import { BUILDINGS, HOUSES, QUALITY, TERRAINS } from './data.mjs';
import { emptyUnits } from './economy.mjs';
import { distance, neighbors, passable, sample } from './world-hex.mjs';
export const STARTING_RADIUS=4, CAPITAL_SEPARATION=8;
export const CAPITAL_NAMES=Object.fromEntries(HOUSES.map((h,i)=>[h.id,['Emberkeep','Frostwatch','Briarhold','Solstice','Moonveil','Redhaven'][i]]));
const BASE=['farm','lumber','quarry','mine'];
// Retain the old opening structures, plus the four guaranteed basic producers.
// Resource stocks and deposit quality never depend on the House.
export const STARTING_BUILDINGS={
  ashen:BASE,wintermere:[...BASE,'farm','lumber'],thornwall:[...BASE,'quarry','mine'],
  sunspire:[...BASE,'ranch'],vesper:BASE,redharbor:[...BASE,'farm','ranch','ranch']
};
export const foundedCapitals=s=>Object.values(s.founding?.houses||{}).filter(h=>h.founded).map(h=>s.tiles[h.capital]).filter(Boolean);
export const unFoundedHouses=s=>HOUSES.map(h=>h.id).filter(id=>!s.founding?.houses[id]?.founded);
export function initializeFounding(s) {
  s.phase='founding';s.turn=0;
  s.founding={houses:Object.fromEntries(HOUSES.map(h=>[h.id,{founded:false,capital:null}]))};
}
// Reachable territory prevents claims and starter roads jumping mountains, seas or other borders.
export function startingArea(s,capital,owner,blocked=new Set()) {
  const queue=[capital],steps=new Map([[capital.id,0]]),parent=new Map();
  for(let i=0;i<queue.length;i++){
    const t=queue[i];if(steps.get(t.id)>=STARTING_RADIUS)continue;
    for(const n of neighbors(s,t))if(passable(n)&&!steps.has(n.id)&&!blocked.has(n.id)&&(!n.owner||n.owner===owner)){
      steps.set(n.id,steps.get(t.id)+1);parent.set(n.id,t.id);queue.push(n);
    }
  }
  return {tiles:queue,parent,steps};
}
export function startingFootprint(s,owner,capital,{blocked=new Set()}={}) {
  if(!passable(capital)||capital.building||capital.owner&&capital.owner!==owner||blocked.has(capital.id))return null;
  const area=startingArea(s,capital,owner,blocked),used=new Set([capital.id]),structures=[];
  const types=STARTING_BUILDINGS[owner]||BASE;
  const available=area.tiles.filter(t=>!t.building&&!t.project&&t.id!==capital.id).sort((a,b)=>distance(a,capital)-distance(b,capital)||area.steps.get(a.id)-area.steps.get(b.id)||a.id.localeCompare(b.id));
  const matches=(t,type)=>!used.has(t.id)&&BUILDINGS[type].terrain.includes(t.terrain)&&(!BUILDINGS[type].resource||BUILDINGS[type].resource===t.resource);
  // Allocate scarce deposits first so a quarry cannot consume the only iron mine site.
  const ordered=[...types].sort((a,b)=>available.filter(t=>matches(t,a)).length-available.filter(t=>matches(t,b)).length||a.localeCompare(b));
  for(const type of ordered){
    const t=available.find(t=>matches(t,type));if(!t)return null;
    used.add(t.id);structures.push({tile:t.id,type});
  }
  const roads=new Set([capital.id]);
  for(const {tile} of structures){let id=tile;while(id!==capital.id){roads.add(id);id=area.parent.get(id);} }
  return {capital:capital.id,structures,roads:[...roads],territory:area.tiles.map(t=>t.id)};
}
export function foundingOutlook(s,id) {
  const c=s.tiles[id];if(!c)return null;
  const area=[];
  for(let r=c.r-STARTING_RADIUS;r<=c.r+STARTING_RADIUS;r++)for(let q=c.q-STARTING_RADIUS;q<=c.q+STARTING_RADIUS;q++){const t=s.tiles[`${q},${r}`];if(t&&distance(c,t)<=STARTING_RADIUS)area.push(t);}
  const terrain=Object.fromEntries(Object.keys(TERRAINS).map(t=>[t,0])),resources={food:0,wood:0,stone:0,iron:0};
  for(const t of area){terrain[t.terrain]++;const quality=QUALITY[t.quality]||1;
    if(t.terrain==='plains')resources.food+=t.resource==='food'?quality*1.5:1;
    if(t.terrain==='forest')resources.wood+=quality;
    if(t.terrain==='hills'){resources.stone+=t.resource==='stone'?quality*1.5:1;if(t.resource==='iron')resources.iron+=quality*1.5;}
  }
  const rating=n=>n>=18?'Excellent':n>=10?'Good':n>=5?'Moderate':'Limited';
  return {resources,ratings:Object.fromEntries(Object.entries(resources).map(([r,n])=>[r,rating(n)])),terrain,region:s.regions?.[c.region]?.name||TERRAINS[c.terrain].name};
}
function basicError(s,owner,id,capitals=foundedCapitals(s)) {
  const t=s.tiles[id];
  if(!HOUSES.some(h=>h.id===owner)||!t)return 'Select a tile on the map.';
  if(t.terrain==='water')return 'A starting city cannot be founded on water.';
  if(t.terrain==='mountain')return 'A starting city cannot be founded on mountains.';
  if(t.building||t.project)return 'This tile already contains a settlement or structure.';
  if(capitals.some(c=>distance(t,c)<CAPITAL_SEPARATION))return 'Too close to another kingdom. Starting capitals must be at least 8 hexes apart.';
  if(t.owner&&t.owner!==owner)return 'A starting city cannot be founded in another kingdom’s territory.';
  return null;
}
export function scoreFoundingSite(s,owner,t) {
  const view=foundingOutlook(s,t.id),r=view.resources;
  return Math.min(r.food,22)*1.8+Math.min(r.wood,20)+Math.min(r.stone,18)+Math.min(r.iron,15)*1.4+
    Object.values(r).filter(n=>n>=5).length*8+(view.terrain.plains+view.terrain.forest+view.terrain.hills)*.35+
    Math.min(view.terrain.mountain,8)*.5+(TERRAINS[t.terrain].defense-1)*8+sample(s.seed,owner,t.id)*6;
}
// A completion witness, not reserved starting tiles. Every choice still uses distance >= 8.
// Bounded backtracking also protects later human claimants from a map being boxed in.
export function planFoundings(s,owners=unFoundedHouses(s),fixed=[]) {
  const existing=foundedCapitals(s),blocked=new Set(),chosen=[];
  for(const f of fixed){chosen.push(s.tiles[f.capital]);for(const id of f.territory)blocked.add(id);}
  const candidates=new Map(owners.map(owner=>[owner,Object.values(s.tiles).filter(t=>!basicError(s,owner,t.id,existing)&&startingFootprint(s,owner,t)).map(t=>({t,score:scoreFoundingSite(s,owner,t)})).sort((a,b)=>b.score-a.score||a.t.id.localeCompare(b.t.id)).map(x=>x.t)]));
  let budget=16000;
  function visit(index,taken,capitals){
    if(index===owners.length)return [];
    const owner=owners[index];
    for(const t of candidates.get(owner)){
      if(--budget<0)return null;
      if(taken.has(t.id)||capitals.some(c=>distance(c,t)<CAPITAL_SEPARATION))continue;
      const footprint=startingFootprint(s,owner,t,{blocked:taken});if(!footprint)continue;
      const next=new Set([...taken,...footprint.territory]);
      const remaining=visit(index+1,next,[...capitals,t]);
      if(remaining)return [{owner,...footprint},...remaining];
    }
    return null;
  }
  return visit(0,blocked,[...existing,...chosen]);
}
export function foundingCheck(s,owner,id,{checkRemaining=true}={}) {
  if(s.phase!=='founding')return 'The realm has already been founded.';
  if(s.founding?.houses[owner]?.founded)return 'Your House has already founded its capital.';
  const error=basicError(s,owner,id);if(error)return error;
  const footprint=startingFootprint(s,owner,s.tiles[id]);
  if(!footprint)return 'Not enough nearby usable land for the starting farm, lumber mill, quarry, mine and other structures. Choose a more varied area.';
  if(checkRemaining&&!planFoundings(s,unFoundedHouses(s).filter(h=>h!==owner),[footprint]))return 'This location leaves too little usable space for the remaining Houses. Choose another region.';
  return null;
}
export function placeStartingFootprint(s,owner,footprint) {
  const c=s.tiles[footprint.capital];
  Object.assign(c,{building:'city',owner,capital:owner,name:CAPITAL_NAMES[owner],road:true,barracks:true,range:true,levels:{city:1,road:1,barracks:1,range:1}});
  // These are pre-existing starting assets, not resource/terrain bonuses.
  if(['sunspire','vesper'].includes(owner)){c.market=true;c.levels.market=1;}
  if(owner==='vesper'){c.workshop=true;c.levels.workshop=1;}
  for(const id of footprint.territory)s.tiles[id].owner=owner;
  for(const {tile,type} of footprint.structures){const t=s.tiles[tile];t.building=type;t.levels[type]=1;}
  for(const id of footprint.roads){const t=s.tiles[id];t.road=true;t.levels.road=1;}
  s.armies.push({id:`army-${s.nextId++}`,owner,tile:c.id,units:{...emptyUnits(),levy:20,archer:6,cavalry:2},formation:'balanced',retreats:0,morale:1,order:'hold',path:[],target:null});
  s.founding.houses[owner]={founded:true,capital:c.id};
  s.events.unshift({turn:0,message:`${s.kingdoms.find(k=>k.id===owner).name} founds ${c.name}.`,kind:'council'});
}
export function finishFounding(s) {
  if(s.phase!=='founding'||unFoundedHouses(s).length)return false;
  s.phase='playing';s.turn=1;s.diplomacy.messages.turn=1;
  for(const c of Object.values(s.courts||{}))if(c.messages)c.messages.turn=1;
  s.events.unshift({turn:1,message:'THE REALM IS FOUNDED. Six houses contest the crown.',kind:'council'});
  return true;
}
export function foundCity(s,owner,id) {
  const error=foundingCheck(s,owner,id);if(error)return {ok:false,error};
  placeStartingFootprint(s,owner,startingFootprint(s,owner,s.tiles[id]));
  finishFounding(s);return {ok:true};
}
export function foundAIKingdoms(s) {
  const pending=unFoundedHouses(s);
  // Humans inspect and confirm first; AI then takes viable remaining regions.
  if(pending.some(id=>s.controllers?s.controllers[id]?.kind==='human':id==='ashen'))return {ok:true,waiting:true};
  const plan=planFoundings(s,pending);
  if(!plan)return {ok:false,error:'No viable founding plan remains.'};
  for(const footprint of plan)placeStartingFootprint(s,footprint.owner,footprint);
  finishFounding(s);return {ok:true};
}
export function validateFoundingSave(s) {
  const fail=()=>{throw new Error('Damaged founding data.');};
  if(!s.worldGeneration){if(s.phase!==undefined&&s.phase!=='playing'||s.turn<1)fail();return;}
  if(s.worldGeneration!==1||!['founding','playing'].includes(s.phase)||!s.regions||!s.founding?.houses)fail();
  if(s.phase==='founding'&&s.turn!==0||s.phase==='playing'&&s.turn<1)fail();
  const capitals=[];
  for(const h of HOUSES){
    const f=s.founding.houses[h.id];if(!f||typeof f.founded!=='boolean')fail();
    if(!f.founded){if(f.capital!==null||s.phase!=='founding'||s.armies.some(a=>a.owner===h.id)||Object.values(s.tiles).some(t=>t.owner===h.id))fail();continue;}
    const t=s.tiles[f.capital];if(!t||!passable(t)||capitals.some(c=>distance(c,t)<CAPITAL_SEPARATION))fail();
    if(s.phase==='founding'&&(t.capital!==h.id||t.owner!==h.id||t.building!=='city'))fail();
    capitals.push(t);
  }
  if(s.phase==='founding'&&capitals.length===6)fail();
}
