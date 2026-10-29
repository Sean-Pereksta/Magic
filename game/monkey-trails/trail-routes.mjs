// Illustrated choices change one camp-to-camp stretch; accepted walking remains authoritative.
import {SPECIES} from './data.mjs';

export const TRAIL_ROUTES=Object.freeze([
  Object.freeze({id:'berry-grove',name:'Berry Grove',art:'berry-grove',description:'Follow a sunlit path through flowering fruit trees.',benefit:'Fruit discoveries give one extra fruit.',risk:'Gentle terrain; fewer crafting and unusual-companion bonuses than the other routes.'}),
  Object.freeze({id:'ancient-ruins',name:'Ancient Ruins',art:'ancient-ruins',description:'Explore mossy arches and the quiet workshops of an old kingdom.',benefit:'Every third discovery adds one charm material. Ordinary victories add one more.',risk:'After your two introductory victories, ordinary enemies are one level stronger.'}),
  Object.freeze({id:'misty-canopy',name:'Misty Canopy',art:'misty-canopy',description:'Cross the high branches where curious silhouettes move through the mist.',benefit:'Uncommon and rare monkeys are 60% more likely relative to common ones.',risk:'Only already-eligible companions can appear; discoveries and encounters remain unpredictable.'})
]);
const byId=new Map(TRAIL_ROUTES.map(route=>[route.id,route]));
const finite=n=>Number.isFinite(n)&&n>=0;

export function normalizeRoutes(s){
  s.meta??={};const old=s.meta.trailRoutes||{};
  s.meta.trailRoutes={visited:[...new Set(Array.isArray(old.visited)?old.visited:[])].filter(id=>byId.has(id)),choices:Number.isSafeInteger(old.choices)&&old.choices>=0?Math.min(old.choices,1e9):0};
  if(!s.run)return s.meta.trailRoutes;
  const route=s.run.trailRoute;
  if(!route||!byId.has(route.id)||!finite(route.untilDistance)||!finite(route.selectedDistance)||route.selectedDistance>route.untilDistance)s.run.trailRoute=null;
  else s.run.trailRoute={id:route.id,untilDistance:route.untilDistance,selectedDistance:route.selectedDistance,discoveries:Number.isSafeInteger(route.discoveries)&&route.discoveries>=0?route.discoveries:0};
  return s.meta.trailRoutes;
}
export function routeView(s){
  normalizeRoutes(s);const r=s.run,saved=r?.trailRoute;
  const selected=saved&&saved.untilDistance===r.nextCamp&&r.distance<saved.untilDistance?byId.get(saved.id):null;
  return{options:TRAIL_ROUTES,selected,canChoose:!!r&&['trail','camp'].includes(r.phase)&&!selected,untilDistance:r?.nextCamp||0,remainingMeters:Math.max(0,(r?.nextCamp||0)-(r?.distance||0))};
}
export function chooseRoute(s,id){
  const view=routeView(s),route=byId.get(id);if(!route||!view.canChoose)throw Error('Choose one trail between encounters, once for each camp-to-camp stretch.');
  s.run.trailRoute={id,untilDistance:s.run.nextCamp,selectedDistance:s.run.distance,discoveries:0};
  if(!s.meta.trailRoutes.visited.includes(id))s.meta.trailRoutes.visited.push(id);
  s.meta.trailRoutes.choices++;return route;
}
export function routeEncounterWeight(s,species){return routeView(s).selected?.id==='misty-canopy'&&['uncommon','rare'].includes(SPECIES[species]?.rarity)?1.6:1;}
export function routeEncounterLevel(s){return routeView(s).selected?.id==='ancient-ruins'&&s.run.ordinaryWins>=2?1:0;}
export function routeDiscovery(s,pickup){
  const route=routeView(s).selected,bonuses=[];if(!route)return bonuses;
  s.run.trailRoute.discoveries++;
  if(route.id==='berry-grove'&&pickup.category==='fruit'){pickup.amount++;pickup.routeBonus=1;}
  if(route.id==='ancient-ruins'&&s.run.trailRoute.discoveries%3===0)bonuses.push({category:'item',id:'charmMaterial',amount:1,source:route.name});
  pickup.routeId=route.id;return bonuses;
}
