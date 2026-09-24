import { buildingLevel, cityBenefits } from './economy.mjs';

export const MAX_GROWTH_PER_TURN = 24;
// Capacity is housing/farm support. It never acts as a multiplier on birth rate.
export function populationCapacity(s, owner) {
  return Object.values(s.tiles).filter(t=>t.owner===owner).reduce((n,t)=>n+(cityBenefits(t)?.capacity??(t.building==='town'?80:0))+buildingLevel(t,'farm')*8,0);
}
export function calculatePopulationChange(s, owner, income) {
  const k=s.kingdoms.find(k=>k.id===owner),tiles=Object.values(s.tiles).filter(t=>t.owner===owner);
  const cities=tiles.filter(t=>t.building==='city').length,towns=tiles.filter(t=>t.building==='town').length;
  const capacity=populationCapacity(s,owner),foodAfter=k.resources.food+income.food,goldAfter=k.resources.gold+income.gold;
  const active=!!(cities+towns),shortage=active&&(foodAfter<0||goldAfter<0);
  const happinessAfter=!active?k.happiness:shortage?Math.max(5,k.happiness-6):Math.max(5,Math.min(100,k.happiness+(k.tax==='high'?-3:k.tax==='low'?3:1)));
  const bonuses=[{label:`Cities (${cities} × 3)`,amount:cities*3},{label:`Towns (${towns} × 2)`,amount:towns*2}];
  bonuses.push({label:'City upgrades',amount:tiles.reduce((n,t)=>n+((cityBenefits(t)?.growth??3)-3),0)});
  const penalties=[],blockers=[];
  const foodBonus=income.food>=35?3:income.food>=15?2:income.food>=5?1:0;
  bonuses.push({label:'Food surplus',amount:foodBonus},{label:'High happiness',amount:happinessAfter>=85?2:happinessAfter>=65?1:0},{label:'Low taxes',amount:k.tax==='low'?2:0});
  if(k.tax==='high')penalties.push({label:'High taxes',amount:-1});
  if(happinessAfter>=35&&happinessAfter<50)penalties.push({label:'Low happiness',amount:-1});
  if(!cities&&!towns)blockers.push('No surviving settlement.');
  if(foodAfter<=20)blockers.push('Food reserves must exceed 20 after upkeep.');
  if(happinessAfter<35)blockers.push('Happiness must reach 35%.');
  if(k.population>=capacity)blockers.push('Population capacity reached. Upgrade cities, add settlements or farm levels.');
  const rawGrowth=Math.max(0,bonuses.reduce((n,b)=>n+b.amount,0)+penalties.reduce((n,b)=>n+b.amount,0));
  if(rawGrowth>MAX_GROWTH_PER_TURN)penalties.push({label:`Growth limit (${MAX_GROWTH_PER_TURN}/turn)`,amount:MAX_GROWTH_PER_TURN-rawGrowth});
  let growth=blockers.length?0:Math.min(rawGrowth,MAX_GROWTH_PER_TURN,Math.max(0,capacity-k.population));
  if(!blockers.length&&growth<Math.min(rawGrowth,MAX_GROWTH_PER_TURN))penalties.push({label:'Remaining population capacity',amount:growth-Math.min(rawGrowth,MAX_GROWTH_PER_TURN)});
  // Preserve the existing shortage loss and civilian floor, including over-cap saves.
  if(shortage){growth=Math.max(20,k.population-3)-k.population;penalties.push({label:'Food or gold shortage',amount:growth});blockers.push('Shortage stops growth; the existing desertion penalty also applies.');}
  return {population:k.population,capacity,change:growth,nextPopulation:k.population+growth,happinessAfter,shortage,foodSurplus:income.food,bonuses,penalties,blockers,limit:MAX_GROWTH_PER_TURN};
}
export function populationBreakdown(p) {
  return [...p.bonuses.filter(b=>b.amount),...p.penalties.filter(b=>b.amount)].map(b=>`${b.label}: ${b.amount>0?'+':''}${b.amount}`).concat(p.blockers).join(' · ');
}
