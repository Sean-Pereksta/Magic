import { atWar, kingdom, relation, treaty } from './core.mjs';

export const ATTITUDES = ['Steadfast Ally','Trusted Friend','Friendly','Commercial Partner','Dependent Partner','Respectful Rival','Cautious','Suspicious','Paranoid','Intimidated','Resentful','Coldly Pragmatic','Opportunistic','Hostile','Vengeful','Mortal Enemy'];
function candidate(s, observer, subject) {
  const r = relation(s,observer,subject), k = kingdom(s,observer);
  if (atWar(s,observer,subject)) return r.grievance >= 75 ? 'Mortal Enemy' : r.grievance >= 45 ? 'Vengeful' : 'Hostile';
  if (r.grievance >= 65 && r.trust < 0) return 'Vengeful';
  if (r.wariness >= 65 && k.paranoia >= .6) return 'Paranoid';
  if (r.wariness >= 30 || r.trust < -15) return 'Suspicious';
  if (r.grievance >= 30) return 'Resentful';
  if (treaty(s,observer,subject,'alliance') && r.trust >= 50 && r.reliability >= 65) return 'Steadfast Ally';
  if (r.trust >= 45 && r.opinion >= 30) return 'Trusted Friend';
  if (r.dependency >= 45) return 'Dependent Partner';
  if (r.dependency >= 20 || treaty(s,observer,subject,'trade') || treaty(s,observer,subject,'recurring')) return 'Commercial Partner';
  if (r.fear >= 45 && r.trust < 30) return 'Intimidated';
  if (r.opinion >= 25 && r.trust >= 15) return 'Friendly';
  if (r.respect >= 45 && r.trust < 30) return 'Respectful Rival';
  if (k.ambition >= .8 && r.trust < 20 && r.fear < 20) return 'Opportunistic';
  if (r.opinion < -25) return 'Hostile';
  return k.greed >= .8 ? 'Coldly Pragmatic' : 'Cautious';
}
// Two consecutive rounds are required for ordinary changes. War overrides immediately.
export function updateAttitudes(s) {
  for (const k of s.kingdoms) for (const other of s.kingdoms) {
    if (k.id === other.id) continue;
    const r = relation(s,k.id,other.id), next = candidate(s,k.id,other.id);
    if (!r.political || atWar(s,k.id,other.id)) r.political = {label:next,pending:null,since:s.turn,checked:s.turn};
    else if (r.political.checked !== s.turn) {
      const p = r.political;
      if (next === p.label) p.pending = null;
      else if (p.pending === next && s.turn > p.since) { p.label=next; p.pending=null; p.since=s.turn; }
      else { p.pending=next; p.since=s.turn; }
      p.checked=s.turn;
    }
  }
}
export function politicalAttitude(s,observer,subject) {
  const r=relation(s,observer,subject);
  if (!r) return {label:'Unknown',tone:'',reasons:[]};
  const label = atWar(s,observer,subject) ? candidate(s,observer,subject) : r.political?.label || candidate(s,observer,subject);
  const reasons=[];
  if (atWar(s,observer,subject)) reasons.push('Our Houses are at war.');
  if (treaty(s,observer,subject,'alliance')) reasons.push('A signed alliance binds our Houses.');
  if (r.wariness >= 20) reasons.push('Foreign armies are concentrated near our frontier.');
  if (r.dependency >= 20) reasons.push('Our economy depends on their shipments.');
  if (r.reliability >= 65) reasons.push('Their fulfilled commitments have earned confidence.');
  if (r.grievance >= 25) reasons.push('Unresolved grievances shape our policy.');
  if (r.fear >= 35) reasons.push('Their military strength concerns our court.');
  reasons.push(...(r.history || []).filter(h=>Object.values(h.changes).some(n=>Math.abs(n)>=4)).slice(-2).map(h=>h.reason));
  if (!reasons.length) reasons.push('Limited dealings; our court is still judging their intentions.');
  const intelligence=(s.intelligence?.reports||[]).some(report=>report.owner===observer&&report.house===subject&&report.detail>=2&&report.snapshot.target===observer&&['invasion','infrastructure','jointWar'].includes(report.snapshot.type)&&s.turn-report.turn<=6);
  return {label,tone:intelligence?'Concerned by recent intelligence':r.wariness >= 20?'Concerned about your armies':r.grievance >= 30?'Unresolved grievances':r.trust >= 45?'Confident in your word':r.dependency >= 20?'Protective of trade':'Guarded',reasons:[...new Set(reasons)].slice(0,4)};
}
export function validatePolitics(s) {
  for(const k of s.kingdoms) for(const r of Object.values(k.relations)) if(r.political) {
    const p=r.political;
    if(!ATTITUDES.includes(p.label)||p.pending!==null&&!ATTITUDES.includes(p.pending)||![p.since,p.checked].every(n=>Number.isInteger(n)&&n>=0&&n<=s.turn))throw new Error('Damaged political attitudes.');
  }
}
