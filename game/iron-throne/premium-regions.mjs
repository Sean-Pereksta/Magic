import { distance, neighbors, passable, sample } from './world-hex.mjs';

// Premium output replaces deposit quality; it never multiplies it. Building
// tiers still scale normally, so a royal mine is compared with the same tier.
const PRODUCERS={food:['farm'],wood:['lumber'],stone:['quarry'],iron:['mine'],horses:['ranch'],gold:['market','tradeOutpost','harbor']};
const KINDS=[
  {kind:'river-valley',name:'Fertile River Valley',resource:'food',building:'farm',accept:t=>t.resource==='food'&&['plains','desert'].includes(t.terrain),favor:t=>t.river?4:0},
  {kind:'iron-mountains',name:'Great Iron Mountains',resource:'iron',building:'mine',accept:t=>t.resource==='iron'&&['hills','desert'].includes(t.terrain),favor:(t,s)=>neighbors(s,t).filter(n=>n.terrain==='mountain'||n.mountainPass).length*2},
  {kind:'ancient-timberlands',name:'Ancient Timberlands',resource:'wood',building:'lumber',accept:t=>t.terrain==='forest'&&t.resource==='wood',favor:(t,s)=>neighbors(s,t).filter(n=>n.terrain==='forest').length},
  {kind:'royal-quarries',name:'Royal Stone Quarries',resource:'stone',building:'quarry',accept:t=>t.resource==='stone'&&['hills','desert'].includes(t.terrain),favor:(t,s)=>neighbors(s,t).filter(n=>n.terrain==='mountain').length*2},
  {kind:'trade-crossroads',name:'Golden Trade Crossroads',resource:'gold',building:'tradeOutpost',accept:(t,s)=>['plains','coast','desert'].includes(t.terrain)&&neighbors(s,t).filter(passable).length>=4,favor:(t,s)=>neighbors(s,t).filter(n=>['forest','hills'].includes(n.terrain)).length},
  {kind:'horse-plains',name:'Royal Horse Plains',resource:'horses',building:'ranch',accept:t=>t.resource==='horses'&&['plains','coast'].includes(t.terrain),favor:(t,s)=>neighbors(s,t).filter(n=>n.terrain==='plains').length},
  {kind:'natural-harbor',name:'Protected Natural Harbor',resource:'gold',building:'harbor',accept:(t,s)=>['plains','coast','desert'].includes(t.terrain)&&neighbors(s,t).some(n=>n.terrain==='water'),favor:(t,s)=>neighbors(s,t).filter(n=>n.terrain==='water').length*2}
];

export function premiumProductionFactor(t,resource,building=null) {
  if(t?.premiumYield!==resource||!Number.isFinite(t.premiumFactor))return null;
  if(building&&!PRODUCERS[resource]?.includes(building))return null;
  // Commercial potential needs developed local roads; harbor development
  // already has the ordinary settlement and navigable-coast requirements.
  if(resource==='gold'&&building!=='harbor'&&!t.road)return null;
  return Math.max(1.5,Math.min(2.5,t.premiumFactor));
}
export function premiumRegion(s,tileOrId) {
  const t=typeof tileOrId==='string'?s.tiles[tileOrId]:tileOrId;
  if(!t||t.fog==='unknown')return null;
  return s.premiumRegions?.[t.premiumRegion]||null;
}
export function premiumFoundingBlocked(s,tile,radius=4) {
  if(!tile)return false;
  return Object.values(s.premiumRegions||{}).some(region=>region.tiles.some(id=>s.tiles[id]&&distance(tile,s.tiles[id])<=radius));
}
function discovered(s,viewer,t) {
  if(t.fog)return t.fog!=='unknown';
  return t.owner===viewer||!!s.fog?.houses[viewer]?.tiles?.[t.id]||s.phase==='founding'&&!s.founding?.houses?.[viewer]?.founded;
}
// Used after fog projection too: hidden centers and satellites are omitted,
// while ownership comes from the observed tile rather than the live board.
export function projectPremiumRegions(s,viewer) {
  const result={};
  for(const region of Object.values(s.premiumRegions||{})){
    const tiles=region.tiles.filter(id=>s.tiles[id]&&discovered(s,viewer,s.tiles[id]));
    if(!tiles.length)continue;
    const centerKnown=region.centerKnown!==false&&tiles.includes(region.center),center=centerKnown?region.center:tiles[0];
    const observed=s.tiles[center];
    const owner=observed.fog||observed.owner===viewer?observed.owner:s.fog?.houses[viewer]?.tiles?.[center]?.tile?.owner??null;
    let surveyed=centerKnown;
    const origin=s.tiles[region.center];
    if(surveyed)for(let r=origin.r-2;r<=origin.r+2;r++)for(let q=origin.q-2;q<=origin.q+2;q++){
      const t=s.tiles[`${q},${r}`];if(t&&distance(origin,t)<=2&&!discovered(s,viewer,t))surveyed=false;
    }
    result[region.id]={...region,tiles,center,centerKnown,owner,access:surveyed?region.access:'Approaches are not yet fully scouted. Explore nearby land before planning a route.',protected:surveyed?region.protected:null};
  }
  return result;
}
export const knownPremiumRegions=(s,viewer)=>Object.values(projectPremiumRegions(s,viewer));

function landComponents(s) {
  const component=new Map(),groups=[];
  for(const root of Object.values(s.tiles).filter(passable)){
    if(component.has(root.id))continue;
    const queue=[root],index=groups.length;component.set(root.id,index);
    for(let head=0;head<queue.length;head++)for(const n of neighbors(s,queue[head]))if(passable(n)&&!component.has(n.id)){component.set(n.id,index);queue.push(n);}
    groups.push(queue);
  }
  return {component,groups};
}
function approaches(s,t) {
  const adjacent=neighbors(s,t),blocked=adjacent.filter(n=>!passable(n)).length;
  let mountains=0;
  for(let r=t.r-2;r<=t.r+2;r++)for(let q=t.q-2;q<=t.q+2;q++){
    const n=s.tiles[`${q},${r}`];if(n?.terrain==='mountain'&&distance(t,n)<=2)mountains++;
  }
  const protectedSite=blocked>=2||mountains>=3||!!t.mountainPass;
  const access=blocked>=3?'A narrow land approach follows the surrounding mountains or shoreline.':mountains>=3||t.mountainPass?'Mountain routes constrain the approach; scout the nearby passes.':blocked>=2?'The shore or high ground limits the land approach.':'Open land approaches allow early expansion and competition.';
  return {protected:protectedSite,access,weight:blocked+Math.min(4,mountains)};
}
export function generatePremiumRegions(s,seed,foundingPlan) {
  s.premiumRegions={};
  const capitals=foundingPlan.map(f=>s.tiles[f.capital]),{component}=landComponents(s),inhabited=new Set(capitals.map(c=>component.get(c.id)));
  // Retain a complete viable founding witness. No geography is changed and no
  // starting package can receive a premium center, even through its radius.
  const available=Object.values(s.tiles).filter(t=>passable(t)&&inhabited.has(component.get(t.id))&&capitals.every(c=>distance(c,t)>4));
  const approachesById=new Map(available.map(t=>[t.id,approaches(s,t)])),chosen=[];
  const target=Math.min(12,Math.max(7,s.kingdoms.length));
  const order=[...KINDS,...KINDS.slice(0,target-KINDS.length)];
  for(const [index,kind] of order.entries()){
    const candidates=available.filter(t=>!t.premiumRegion&&kind.accept(t,s)&&chosen.every(c=>distance(c,t)>=4));
    const score=t=>{
      const a=approachesById.get(t.id),near=capitals.map(c=>distance(c,t)).sort((a,b)=>a-b);
      return (kind.favor?.(t,s)||0)*2+(['iron','stone'].includes(kind.resource)?a.weight*2:0)+Math.max(0,12-(near[1]||near[0]))*.4+sample(seed,'premium',index,t.id)*4;
    };
    candidates.sort((a,b)=>score(b)-score(a)||a.id.localeCompare(b.id));
    const center=candidates[0];if(!center)continue;
    const approach=approachesById.get(center.id),multiplier=approach.protected?2.5:kind.resource==='gold'?1.8:2;
    // Matching adjacent ground makes a small recognizable region. Every
    // premium tile, including satellites, is excluded from starting claims.
    const satellites=neighbors(s,center).filter(t=>available.includes(t)&&!t.premiumRegion&&kind.accept(t,s)&&chosen.every(c=>distance(c,t)>=3)).sort((a,b)=>sample(seed,'premium-edge',a.id)-sample(seed,'premium-edge',b.id)).slice(0,kind.resource==='gold'?0:1);
    const id=`premium-${index}`,tiles=[center,...satellites];
    const name=kind.kind==='river-valley'&&!center.river?'Fertile Crownlands':kind.name;
    const access=kind.kind==='trade-crossroads'?`${approach.access} A market or trading post and a local road are needed for enhanced commerce.`:kind.kind==='natural-harbor'?`${approach.access} Found a coastal settlement and build a harbor to realize maritime income.`:approach.access;
    s.premiumRegions[id]={id,kind:kind.kind,name,center:center.id,tiles:tiles.map(t=>t.id),resource:kind.resource,building:kind.building,multiplier,access,protected:approach.protected};
    for(const t of tiles)Object.assign(t,{premiumRegion:id,premiumYield:kind.resource,premiumFactor:multiplier});
    chosen.push(center);
  }
  return s.premiumRegions;
}
export function validatePremiumRegions(s) {
  // Older campaigns have no premium fields and retain their original maps.
  const fail=()=>{throw new Error('Damaged premium region data.');};
  if(s.premiumRegions===undefined){if(Object.values(s.tiles).some(t=>t.premiumRegion!==undefined||t.premiumYield!==undefined||t.premiumFactor!==undefined))fail();return;}
  if(!s.premiumRegions||Array.isArray(s.premiumRegions)||typeof s.premiumRegions!=='object'||Object.keys(s.premiumRegions).length>12)fail();
  const covered=new Set();
  for(const [id,r] of Object.entries(s.premiumRegions)){
    const kind=KINDS.find(k=>k.kind===r?.kind);
    if(!kind||r.id!==id||typeof r.name!=='string'||r.name.length>100||!Array.isArray(r.tiles)||r.tiles.length<1||r.tiles.length>2||!s.tiles[r.center]||!r.tiles.includes(r.center)||r.resource!==kind.resource||r.building!==kind.building||typeof r.access!=='string'||r.access.length>500||typeof r.protected!=='boolean'||![1.8,2,2.5].includes(r.multiplier)||Object.keys(r).some(key=>!['id','kind','name','center','tiles','resource','building','multiplier','access','protected'].includes(key)))fail();
    for(const tile of r.tiles){const t=s.tiles[tile];if(!t||!passable(t)||!kind.accept(t,s)||covered.has(tile)||t.premiumRegion!==id||t.premiumYield!==r.resource||t.premiumFactor!==r.multiplier||distance(t,s.tiles[r.center])>1)fail();covered.add(tile);}
  }
  for(const t of Object.values(s.tiles))if((t.premiumRegion!==undefined||t.premiumYield!==undefined||t.premiumFactor!==undefined)&&!covered.has(t.id))fail();
}
