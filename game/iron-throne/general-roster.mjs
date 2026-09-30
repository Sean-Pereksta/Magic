// Campaign identities are finite. Tactical behavior keys retain the existing AI rules.
const entries = [
  ['Garrick Rowan','Steadfast','methodical','battle','Infantry, holding territory, disciplined advances'],
  ['Edric Vale','Cautious','cautious','battle','Defense, positioning, minimizing casualties'],
  ['Alaric Thorn','Bold','aggressive','battle','Assaults, breaking defensive lines, offensive campaigns'],
  ['Cedric Ashford','Honorable','protective','battle','Balanced armies, morale, organized campaigns'],
  ['Roderic Blackwell','Ruthless','aggressive','battle','Sieges, attrition, relentless pursuit'],
  ['Tristan Marlowe','Opportunistic','opportunistic','movement','Cavalry, flanking, rapid expansion'],
  ['Osric Fen','Protective','protective','battle','Fortifications, border defense, reinforcement'],
  ['Lucan Grey','Watchful','cautious','movement','Scouting, maneuvering, avoiding unfavorable battles'],
  ['Theon Harrow','Methodical','methodical','battle','Siege engines, supply lines, fortified settlements'],
  ['Merek Stone','Unyielding','protective','battle','Heavy infantry, defensive battles, holding chokepoints'],
  ['Corvin Hale','Pragmatic','methodical','mustering','Mixed armies, adapting, reinforcement'],
  ['Dorian Veyne','Ambitious','aggressive','movement','Conquest, expansion, seizing weak settlements'],
  ['Aldren Pike','Disciplined','methodical','battle','Spearmen, formations, countering cavalry'],
  ['Kael Rivers','Restless','opportunistic','movement','Maneuver, cavalry, distant expeditions'],
  ['Bram Wycliff','Patient','protective','mustering','Reinforcement, large armies, long campaigns, supply'],
  ['Reynard Crow','Cunning','opportunistic','battle','Detachments, feints, opportunistic attacks']
];
export const GENERAL_ROSTER = Object.freeze(entries.map(([name,temperament,personality,specialty,traits],i)=>Object.freeze({
  characterId:name.toLowerCase().replaceAll(' ','-'),name,temperament,personality,specialty,traits,
  portrait:`portraits/generals/${name.toLowerCase().replaceAll(' ','_')}.png`,slot:i+1
})));
export const generalIdentity = g => GENERAL_ROSTER.find(x=>x.characterId===g?.characterId);
export const generalTemperament = g => generalIdentity(g)?.temperament || g.personality;
export const generalTraits = g => generalIdentity(g)?.traits || g.specialty;
