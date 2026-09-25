import { marriageSupport } from './marriage.mjs';
import { knowledgeView, refreshKnowledge } from './fog.mjs';
import { armyVision } from './fog.mjs';
import { atWar, distance, findPath, kingdom, orderArmy, settlements, sizeOf, splitArmy } from './core.mjs';

// Decision code receives a detached observation projection. Only command handlers
// receive the canonical state; there is no fallback lookup for missing knowledge.
export function planningView(world, owner) {
  if (world.knowledgeView) return world;
  const view = knowledgeView(world, owner);
  view.planningHouse = owner;
  // Preserve the ruler's private memories, including previous border observations.
  view.kingdoms = view.kingdoms.map(k => k.id === owner ? {...structuredClone(kingdom(world, owner)),knownAlive:k.knownAlive} : k);
  for (const k of view.kingdoms) if (k.id !== owner) {
    // Another court's private opinions are not ours to read. Spy reports are
    // explicit disclosures, never permission to inspect the underlying court.
    for (const id of Object.keys(k.relations)) k.relations[id] = {
      opinion: 0, trust: 0, fear: 0, wariness: 0, grievance: 0, dependency: 0,
      reliability: 50, respect: 15, observations: {}, movements: {}, contacts: {}, history: []
    };
    for (const report of view.intelligence.reports.filter(r => r.house === k.id && world.turn-r.turn <= 6))
      for (const r of report.snapshot.relations || []) if (k.relations[r.house]) Object.assign(k.relations[r.house], r);
  }
  // Known original capitals are objectives to investigate, not verified holdings.
  for (const t of Object.values(view.tiles)) if (t.knownCapital && !t.owner) {
    t.owner = t.knownCapital; t.inferredOwner = true;
  }
  view.armies.push(...view.lastSeenArmies.map(r => {
    const age = Math.max(0, world.turn-r.turn), confidence = Math.max(.05, 1-age/12);
    const uncertainty = Math.ceil(r.maximum * Math.min(1.5, age*.12));
    return {id:r.id,owner:r.owner,tile:r.tile,units:{levy:Math.round((r.minimum+r.maximum)/2)},
      morale:1,formation:'balanced',retreats:0,path:[],target:null,order:'last-known',
      lastSeenTurn:r.turn,confidence,uncertaintyRadius:age*5,
      estimatedMinimum:Math.max(0,r.minimum-uncertainty),estimatedMaximum:r.maximum+uncertainty,
      remembered:true,source:r.source};
  }));
  delete view.rng; delete view.nextId;
  return view;
}

export function refreshHouseKnowledge(world, owner) { refreshKnowledge(world, [owner]); }

// Select destinations using knowledge, then let normal movement discover borders
// and resolve encounters. Never re-path around undiscovered roads or garrisons.
export function scoutOrder(world, k, army, c) {
  const view = planningView(world, k.id), location = view.tiles[army.tile];
  if (c.threats.length || c.pledges.length || k.resources.food<24 || k.resources.gold<18 || sizeOf(army) < 2) return false;
  if (world.cooperation?.operations.some(o=>['Preparing','Executing'].includes(o.status)&&o.participants.some(p=>p.house===k.id&&p.status==='accepted'))) return false;
  const own = world.armies.filter(a => a.owner === k.id);
  const scout = [...own].sort((a,b)=>armyVision(b)-armyVision(a)||sizeOf(a)-sizeOf(b)||a.id.localeCompare(b.id))[0];
  if (army.id !== scout?.id || c.war && own.length===1 && armyVision(army)<4) return false;
  // Detach a small mounted screen so the main army keeps its campaign role.
  if (own.length === 1 && armyVision(army) >= 4 && sizeOf(army) >= 36) {
    const result = splitArmy(world, k.id, army.id);
    if (result.ok) {
      const detachment=world.armies.find(a=>a.id===result.armyId);
      for(const id of Object.keys(detachment.units)){
        const keep=['scout','lightCavalry','cavalry','knight'].includes(id)?Math.min(6,detachment.units[id]):0;
        army.units[id]=(army.units[id]||0)+detachment.units[id]-keep;detachment.units[id]=keep;
      }
      // A mixed force with no splittable cavalry continues as a normal army.
      if(sizeOf(detachment)===0)world.armies=world.armies.filter(a=>a!==detachment);
      else scoutOrder(world,k,detachment,c);
    }
    return false;
  }
  const missing = view.armies.filter(a=>a.remembered&&atWar(view,k.id,a.owner)&&a.confidence>.15);
  const knownTargets = settlements(view).filter(t=>atWar(view,k.id,t.owner)&&t.fog!=='visible');
  const candidates = Object.values(view.tiles).filter(t => t.id!==army.tile &&
    (t.fog!=='visible' || missing.some(a=>a.tile===t.id)) &&
    !view.armies.some(a=>!a.remembered&&a.tile===t.id&&atWar(view,k.id,a.owner)))
    .map(t=>({tile:t,score:(missing.some(a=>a.tile===t.id)?45:0)+
      (knownTargets.some(x=>distance(x,t)<=3)?18:0)+(t.fog==='unknown'?8:0)-distance(location,t)*2}))
    .sort((a,b)=>b.score-a.score||a.tile.id.localeCompare(b.tile.id));
  for (const {tile} of candidates.slice(0,16)) {
    if (!findPath(view,army.tile,tile.id,k.id).length) continue;
    if (orderArmy(world,k.id,army.id,tile.id,'move').ok) return true;
  }
  return false;
}

// Trusted allies answer with their own dated observations. Sharing neither
// renews a timestamp nor propagates another ally's reports in a loop.
export function requestAlliedIntelligence(world, owner) {
  const ours = world.fog?.houses[owner];
  if (!ours) return 0;
  let shared=0;
  const allies=world.treaties.filter(t=>t.type==='alliance'&&t.expires>world.turn&&t.parties.includes(owner))
    .map(t=>t.parties.find(id=>id!==owner)).filter(id=>(kingdom(world,id)?.relations[owner]?.trust||0)+marriageSupport(world,id,owner)>=50);
  for (const ally of allies) {
    const source=world.fog.houses[ally]; if(!source)continue;
    for (const [id,r] of Object.entries(source.armies)) {
      if(r.source==='ally'||r.owner===owner||world.turn-r.turn>8)continue;
      if(ours.armies[id]&&ours.armies[id].turn>=r.turn)continue;
      // A newer direct observation of an empty location disproves an old report.
      if((ours.tiles[r.tile]?.turn??-1)>=r.turn)continue;
      ours.armies[id]={...structuredClone(r),source:'ally',sharedBy:ally}; shared++;
      const tile=source.tiles[r.tile];
      if(tile&&(!ours.tiles[r.tile]||ours.tiles[r.tile].turn<tile.turn))
        ours.tiles[r.tile]={...structuredClone(tile),source:'ally',sharedBy:ally};
    }
  }
  return shared;
}
