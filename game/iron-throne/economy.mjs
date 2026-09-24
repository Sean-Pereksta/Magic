import { BUILDINGS, FORMATIONS, QUALITY, REGIONS, RESOURCES, UNITS } from './data.mjs';

export const emptyResources = () => Object.fromEntries(RESOURCES.map(r => [r, 0]));
export const emptyUnits = () => Object.fromEntries(Object.keys(UNITS).map(u => [u, 0]));
export function buildingLevel(t, type) {
  if (!t) return 0;
  const present = type === 'wall' ? (t.walls > 0 || t.levels?.wall) : type === 'road' ? t.road : BUILDINGS[type]?.settlement ? t[type] : t.building === type;
  return present ? t.levels?.[type] || 1 : 0;
}
export const buildingSpec = (type, level = 1) => BUILDINGS[type]?.levels[Math.max(0, level - 1)];
export function wallMaximum(t) { return buildingLevel(t, 'wall') * 60; }
export function fortMaximum(t) { return t.building === 'fort' ? [0, 45, 100, 180][buildingLevel(t, 'fort')] : 0; }
export function constructionSpec(t, type) {
  const current = buildingLevel(t, type), b = BUILDINGS[type];
  if (!b) return null;
  const repair = current === b.maxLevel && (type === 'wall' && t.walls < wallMaximum(t) || type === 'fort' && (t.fortIntegrity ?? fortMaximum(t)) < fortMaximum(t));
  const spec = buildingSpec(type, repair ? current : current + 1);
  return spec && {...spec, repair, cost: repair ? Object.fromEntries(Object.entries(spec.cost).map(([r,n])=>[r,Math.ceil(n*.4)])) : spec.cost, turns: repair ? 2 : spec.turns};
}
export function tileProduction(t, owner) {
  const output = emptyResources();
  for (const [id, b] of Object.entries(BUILDINGS)) {
    const level = buildingLevel(t, id); if (!level || !b.yield) continue;
    const resourceSite = ['farm','lumber','quarry','mine','ranch'].includes(id);
    const quality = resourceSite && (t.resource === Object.keys(b.yield)[0] || id === 'farm' && t.river) ? QUALITY[t.quality || 'normal'] : 1;
    const industry = REGIONS[owner]?.industry === id ? 1.12 : 1;
    for (const [r,n] of Object.entries(b.yield)) output[r] += Math.round((n + (id === 'farm' && t.resource === 'food' ? 4 : 0)) * (1+(level-1)*.65) * quality * industry);
  }
  if (['town','city'].includes(t.building)) {
    output.food += t.building === 'city' ? 14 : 8;
    output.gold += (t.building === 'city' ? 8 : 5) + (['sunspire','vesper'].includes(t.region) ? 6 : 0);
  }
  return output;
}
export function productionPlan(s, owner) {
  const k = s.kingdoms.find(k=>k.id === owner), income = emptyResources(), gross = emptyResources(), stalls = [];
  const tiles = Object.values(s.tiles).filter(t=>t.owner === owner);
  for (const t of tiles) {
    const production = tileProduction(t, owner);
    for (const r of RESOURCES) { income[r] += production[r]; gross[r] += production[r]; }
  }
  // Stable tile/catalog order, with a shared input budget, prevents double spending.
  for (const t of tiles) for (const [id,b] of Object.entries(BUILDINGS)) {
    const level = buildingLevel(t,id); if (!level || !b.recipe) continue;
    const multiplier = level + (REGIONS[owner]?.industry === id ? 1 : 0);
    const input = Object.fromEntries(Object.entries(b.recipe.input).map(([r,n])=>[r,n*multiplier]));
    if (Object.entries(input).some(([r,n])=>k.resources[r]+income[r]<n)) {stalls.push({tile:t.id,type:id,input}); continue;}
    for (const [r,n] of Object.entries(input)) income[r] -= n;
    for (const [r,n] of Object.entries(b.recipe.output)) {income[r] += n*multiplier; gross[r] += n*multiplier;}
  }
  return {income,gross,stalls};
}
export function storageCapacity(s, owner, resource) {
  if (resource === 'gold') return 99999;
  let capacity = 600;
  for (const t of Object.values(s.tiles)) if(t.owner===owner) capacity += buildingLevel(t,'storehouse')*400 + (resource==='food' ? buildingLevel(t,'greatGranary')*2400 + buildingLevel(t,'farm')*50 : 0);
  return capacity;
}
export function completeConstruction(t) {
  const p=t.project, type=p.type;
  if (t.structureDamage) delete t.structureDamage[type];
  t.levels ||= {}; t.levels[type]=p.level || 1;
  if(type==='wall') t.walls=wallMaximum(t);
  else if(type==='road' || BUILDINGS[type].settlement) t[type]=true;
  else {t.building=type; if(['town','city'].includes(type)) {t.road=true;t.levels.road ||= 1;t.name ||= `Outpost ${t.q}.${t.r}`;}}
  if(type==='fort') t.fortIntegrity=fortMaximum(t);
  t.project=null;
}
export function regionalize(s, roll) {
  const capitals=Object.values(s.tiles).filter(t=>t.capital), dist=(a,b)=>(Math.abs(a.q-b.q)+Math.abs(a.r-b.r)+Math.abs(a.q+a.r-b.q-b.r))/2;
  const resources=['food','wood','stone','iron','horses'];
  for(const t of Object.values(s.tiles)) {
    t.levels ||= {};
    const capital=capitals.reduce((a,b)=>dist(t,a)<dist(t,b)?a:b), region=REGIONS[capital.owner];t.region=capital.owner;
    const strong=region.weights[resources.indexOf(t.resource)] || 1;
    const n=roll(s);t.quality=capital.owner==='ashen' ? (n<.15?'poor':n<.85?'normal':'rich') : strong>=5 ? (n<.25?'normal':n<.78?'rich':'exceptional') : strong<=2 ? (n<.7?'poor':'normal') : (n<.3?'poor':n<.85?'normal':'rich');
    if(t.capital || t.terrain==='water') continue;
    let pick=roll(s)*region.weights.reduce((a,b)=>a+b,0), r=resources[0];
    for(let i=0;i<resources.length;i++){pick-=region.weights[i];if(pick<=0){r=resources[i];break;}}
    if(roll(s)<.78 && dist(t,capital)>1) {
      t.resource=r;t.terrain=r==='wood'?'forest':['stone','iron'].includes(r)?'hills':'plains';
      const w=region.weights[resources.indexOf(r)];t.quality=w>=5?(n<.65?'rich':'exceptional'):w<=2?(n<.7?'poor':'normal'):'normal';
    }
    if(t.river && t.terrain==='plains') {t.resource='food';t.quality='rich';}
  }
  const rings={ashen:['farm','lumber','quarry','mine',null,null],wintermere:['farm','lumber','lumber','quarry','farm',null],thornwall:['farm','quarry','mine','mine','quarry',null],sunspire:['farm','ranch',null,'quarry',null,null],vesper:['farm','lumber',null,'mine',null,null],redharbor:['farm','lumber','ranch','ranch','farm',null]};
  const dirs=[[1,0],[1,-1],[0,-1],[-1,0],[-1,1],[0,1]];
  for(const c of capitals){
    c.levels={city:1,road:1,barracks:1,range:1};c.barracks=true;c.range=true;
    if(['sunspire','vesper'].includes(c.owner)){c.market=true;c.levels.market=1;}
    if(c.owner==='vesper'){c.workshop=true;c.levels.workshop=1;}
    if(['redharbor','sunspire'].includes(c.owner)){c.terrain='coast';c.river=true;}
    dirs.forEach(([q,r],i)=>{const t=s.tiles[`${c.q+q},${c.r+r}`],type=rings[c.owner][i],resource=type?Object.keys(BUILDINGS[type].yield||{})[0]:t.resource;
      t.building=type;t.resource=resource;t.terrain=type?BUILDINGS[type].terrain[0]:'plains';t.levels={road:1,...(type?{[type]:1}:{})};
      const w=REGIONS[c.owner].weights[resources.indexOf(resource)]||1;
      t.quality=c.owner==='ashen'?'normal':w>=5?(roll(s)<.65?'rich':'exceptional'):w<=2?'poor':'normal';
    });
  }
}
export function migrateEconomy(s) {
  for(const k of s.kingdoms) for(const r of ['horses','tools','arms']) k.resources[r] ??= 0;
  for(const t of Object.values(s.tiles)) {
    t.levels ||= {};
    for(const [id,b] of Object.entries(BUILDINGS)) if(t.building===id || id==='wall'&&t.walls>0 || id==='road'&&t.road || b.settlement&&t[id]) t.levels[id] ||= 1;
    t.quality ||= 'normal'; t.region ||= t.owner || 'ashen';
    if(t.building==='fort') t.fortIntegrity ??= fortMaximum(t);
    if(t.project){t.project.level ??= 1;t.project.total ??= Math.max(t.project.remaining,buildingSpec(t.project.type,t.project.level).turns);}
    // Existing recruitment centers retain basic mustering on migration.
    if(['city','town','fort'].includes(t.building)) for(const type of ['barracks','range']) {t[type] ??= true;t.levels[type] ||= 1;}
  }
  for(const a of s.armies){for(const id of Object.keys(UNITS)) a.units[id] ??= 0;a.formation ||= 'balanced';a.retreats ||= 0;}
  s.commerce ||= {offers:[],lastOfferTurn:0,cooldowns:{},aiTrades:{}};
  for(const t of s.treaties) if(t.type==='recurring') t.legacyRoute ??= true;
}
export function validateExpansion(s) {
  const integer=(n,min,max)=>Number.isInteger(n)&&n>=min&&n<=max, fail=()=>{throw new Error('Damaged expansion data.');};
  for(const t of Object.values(s.tiles)) {
    if(!Object.hasOwn(QUALITY,t.quality)||!Object.hasOwn(REGIONS,t.region)||!t.levels||Array.isArray(t.levels))fail();
    for(const [id,n] of Object.entries(t.levels))if(!BUILDINGS[id]||!integer(n,1,BUILDINGS[id].maxLevel))fail();
    if(t.fortIntegrity!==undefined&&!integer(t.fortIntegrity,0,180))fail();
    if(t.siege && (!integer(t.siege.turns,0,100000)||!Number.isFinite(t.siege.morale)||t.siege.morale<0||t.siege.morale>1))fail();
    if(t.project&&(!integer(t.project.level,1,BUILDINGS[t.project.type].maxLevel)||!integer(t.project.total,1,8)||t.project.remaining>t.project.total))fail();
    for(const [id,b] of Object.entries(BUILDINGS))if(b.settlement&&t[id]!==undefined&&typeof t[id]!=='boolean')fail();
  }
  for(const a of s.armies)if(!Object.hasOwn(FORMATIONS,a.formation)||!integer(a.retreats,0,100000)||Object.keys(a.units).some(id=>!UNITS[id]))fail();
  for(const k of s.kingdoms)if(k.economicPlan&&(!s.tiles[k.economicPlan.tile]||!Object.hasOwn(BUILDINGS,k.economicPlan.type)))fail();
  for(const e of s.militaryEvents){
    if(e.winner&&!s.kingdoms.some(k=>k.id===e.winner)||e.retreat&&!s.tiles[e.retreat]||e.retreatOwner&&!s.kingdoms.some(k=>k.id===e.retreatOwner))fail();
    for(const field of ['composition','casualties'])if(e[field]!==undefined&&(!Array.isArray(e[field])||e[field].length!==2||e[field].some(counts=>!counts||Array.isArray(counts)||Object.entries(counts).some(([u,n])=>!Object.hasOwn(UNITS,u)||!integer(n,0,100000)))))fail();
    if(e.phases!==undefined&&(!Array.isArray(e.phases)||e.phases.length>7||e.phases.some(p=>!p||typeof p.name!=='string'||p.name.length>60||!Array.isArray(p.notes)||p.notes.length>8||p.notes.some(n=>typeof n!=='string'||n.length>300)||p.loss&&(!Array.isArray(p.loss)||p.loss.length!==2||p.loss.some(n=>!integer(n,0,100000))))))fail();
  }
  for(const t of s.treaties)if(t.type==='recurring'){
    if(t.legacyRoute!==undefined&&typeof t.legacyRoute!=='boolean'||t.disrupted!==undefined&&!integer(t.disrupted,0,3)||t.routeStatus!==undefined&&(typeof t.routeStatus!=='string'||t.routeStatus.length>200))fail();
    if(t.anchors!==undefined&&(!Array.isArray(t.anchors)||t.anchors.length>4||t.anchors.some(a=>!a||!s.tiles[a.tile]||!s.kingdoms.some(k=>k.id===a.owner)||!['harbor','tradeOutpost'].includes(a.type)||!integer(a.level,1,3))))fail();
  }
  const c=s.commerce;if(!c||!Array.isArray(c.offers)||c.offers.length>(s.controllers?72:12)||!integer(c.lastOfferTurn,0,100000)||!c.cooldowns||!c.aiTrades)fail();
  for(const times of [c.cooldowns,c.aiTrades])if(Object.entries(times).length>36||Object.values(times).some(n=>!integer(n,0,100000)))fail();
  for(const o of c.offers)if(!integer(o.id,1,10000000)||!s.kingdoms.some(k=>k.id===o.from)||!integer(o.created,0,100000)||!integer(o.expires,0,100000)||!['pending','reviewed','declined','accepted'].includes(o.status)||typeof o.reason!=='string'||o.reason.length>600||!o.intent||!RESOURCES.includes(o.intent.giveResource)||!RESOURCES.includes(o.intent.receiveResource)||!integer(o.intent.giveAmount,1,1000)||!integer(o.intent.receiveAmount,1,1000)||!integer(o.intent.duration,2,20)||!['EXCHANGE','RECURRING'].includes(o.intent.type))fail();
}
