import { SHIPS } from './naval-state.mjs';
// Pure accounting shared by resolution, forecasts, UI and economic planning.
export function foodSecurity(stock, net, consumption = 15) {
  const projected=stock+Math.min(0,net)*3;
  if(stock+net<0)return 'Famine';
  if(stock<20||projected<0)return 'Shortage';
  if(net<0||projected<Math.max(40,consumption*3))return 'Strained';
  if(net>=8&&stock>=consumption*6)return 'Abundant';
  return 'Stable';
}
export function foodAccounting(s, owner, production) {
  const k=s.kingdoms.find(k=>k.id===owner),tiles=Object.values(s.tiles).filter(t=>t.owner===owner);
  const population=Math.ceil(k.population/8);
  const units=[...s.armies,...(s.fleets||[]).filter(f=>f.owner===owner).flatMap(f=>f.cargo||[])].filter(a=>a.owner===owner);
  const armies=Math.ceil(units.reduce((n,a)=>n+Object.entries(a.units).reduce((m,[id,count])=>m+count*(/cavalry|knight|scout/i.test(id)?2:1),0),0)/4);
  const settlements=tiles.reduce((n,t)=>n+(t.building==='city'?4+(t.levels?.city||1)-1:t.building==='town'?5:0),0);
  const crews=Math.ceil((s.fleets||[]).filter(f=>f.owner===owner).reduce((n,f)=>n+f.ships.reduce((m,v)=>m+v.crew,0),0)+(s.shipQueues||[]).filter(q=>q.owner===owner).reduce((n,q)=>n+SHIPS[q.type].crew,0));
  const crewConsumption=Math.ceil(crews/4);
  const consumption=population+armies+settlements+crewConsumption,net=production-consumption;
  return {production,consumption,net,population,armies,settlements,crews:crewConsumption,status:foodSecurity(k.resources.food,net,consumption)};
}
