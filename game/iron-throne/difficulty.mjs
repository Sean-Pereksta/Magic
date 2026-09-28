// Decision parameters only: never modify income, troop stats or visibility.
export const DIFFICULTIES = {
  easy: {name:'Easy', description:'Cautious expansion, forgiving negotiations and simple campaigns.', expansion:0.65, warTurn:16, ambition:0, preparation:1.5, coordination:0, negotiation:8},
  medium: {name:'Medium', description:'Balanced expansion, sustainable armies and measured opportunities.', expansion:1, warTurn:10, ambition:0.18, preparation:1.25, coordination:1, negotiation:0},
  hard: {name:'Hard', description:'Assertive settlement, concentrated forces and prepared allied campaigns.', expansion:1.45, warTurn:7, ambition:0.42, preparation:1.12, coordination:2, negotiation:-4},
  insane: {name:'Insane', description:'Relentless strategic expansion, coordinated conquest and calculated intrigue.', expansion:1.85, warTurn:5, ambition:0.65, preparation:1.05, coordination:3, negotiation:-7}
};
export const difficulty = s => DIFFICULTIES[s.difficulty] || DIFFICULTIES.medium;
export function validateDifficulty(s) {
  s.difficulty ??= 'medium';
  if(!Object.hasOwn(DIFFICULTIES,s.difficulty))throw new Error('Unknown campaign difficulty.');
}
export function settlementAmbition(s,k,c) {
  const d=difficulty(s), known=Object.values(s.tiles).filter(t=>!['mountain','water'].includes(t.terrain)&&(!t.owner||t.owner===k.id)&&t.fog!=='unknown');
  const geography=Math.max(2,Math.floor(known.length/16));
  const mapShare=Math.max(3,Math.floor(s.width*s.height/s.kingdoms.length/13));
  const capacity=Math.max(2,Math.floor((k.population+Math.max(0,c.income.food)*5)/30));
  return Math.min(geography,mapShare+Math.floor(d.expansion*2),capacity,2+Math.floor(s.turn*d.expansion/8));
}
export function difficultyOptions(selected='medium') {
  return Object.entries(DIFFICULTIES).map(([id,d])=>`<option value="${id}" ${id===selected?'selected':''}>${d.name} — ${d.description}</option>`).join('');
}
