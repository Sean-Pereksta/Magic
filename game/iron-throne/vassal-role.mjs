import { isAiHouse } from './house-control.mjs';
import { UNITS } from './data.mjs';

export const vassalBond = (s,liege,vassal) => s.treaties.find(t => t.type==='vassalage' && t.liege===liege && t.vassal===vassal && t.expires>s.turn);
export function vassalRole(s,liege,vassal) {
  const bond=vassalBond(s,liege,vassal);
  if (!bond) return null;
  return {liege,vassal,expires:bond.expires,status:s.fealty?.[vassal]?.status||'Loyal',
    duty:'Respect the liege, report honestly, and obey lawful military commands. Explain concrete obstacles without demanding payment or questioning permitted liege troops.'};
}
// A vassal's own muster is an authorized report to its liege. This does not
// disclose army locations/orders, other courts, or a human vassal's private data.
export function vassalMuster(s,liege,vassal) {
  if (!vassalBond(s,liege,vassal) || !isAiHouse(s,vassal)) return null;
  if (s.knowledgeView) return s.vassalReports?.[vassal] || null;
  const armies=s.armies.filter(a=>a.owner===vassal),units=Object.fromEntries(Object.keys(UNITS).map(id=>[id,armies.reduce((n,a)=>n+(a.units[id]||0),0)]));
  return {armies:armies.length,troops:Object.entries(units).reduce((n,[id,count])=>n+(UNITS[id].family==='siege'?0:count),0),units};
}
