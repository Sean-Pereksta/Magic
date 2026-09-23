import { FORMATIONS, TERRAINS, UNITS } from './data.mjs';
import { buildingLevel, fortMaximum, wallMaximum } from './economy.mjs';

const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
export const troopTotal = a => Object.values(a.units).reduce((n,v)=>n+v,0);
export const familyCount = (a,family) => Object.entries(a.units).reduce((n,[u,v])=>n+(UNITS[u]?.family===family?v:0),0);
export const unitPower = (a,stat) => Object.entries(a.units).reduce((n,[u,v])=>n+v*(UNITS[u]?.[stat]||0),0);
export function armySpeed(a) {
  const base = a.units.trebuchet ? 1.5 : familyCount(a,'siege') ? 2 : familyCount(a,'mounted')===troopTotal(a) ? 5 : 3;
  return base*(a.formation==='defensive'?.8:1);
}
export function formationCheck(a,formation) {
  if(!Object.hasOwn(FORMATIONS,formation))return 'Unknown formation.';
  if(formation==='flanking'&&!familyCount(a,'mounted'))return 'Flanking requires mounted troops.';
  if(formation==='spearWall'&&!a.units.spearman)return 'Spear Wall requires spearmen.';
  return null;
}
export function setFormation(s,owner,id,formation) {
  const a=s.armies.find(a=>a.id===id&&a.owner===owner);
  if(!a||s.outcome)return {ok:false,error:'Select an active army.'};
  const error=formationCheck(a,formation);if(error)return {ok:false,error};
  a.formation=formation;return {ok:true};
}
export function chooseFormation(a,enemy,t) {
  if((a.units.spearman||0)>troopTotal(a)*.25&&enemy&&familyCount(enemy,'mounted')>troopTotal(enemy)*.2)return 'spearWall';
  if(familyCount(a,'ranged')>troopTotal(a)*.35)return 'skirmish';
  if(familyCount(a,'mounted')>troopTotal(a)*.3&&['plains','coast'].includes(t.terrain))return 'flanking';
  return t.owner===a.owner?'defensive':'balanced';
}
// Damage is allocated by weighted largest remainders; small unit groups are not
// automatically destroyed by rounding once per class/phase.
export function inflict(a,damage,{piercing=false,exposed=false}={}) {
  const groups=Object.entries(a.units).filter(([,n])=>n>0).map(([id,n])=>({id,n,weight:n/(1+(piercing?0:(UNITS[id].armor||0))*2)}));
  const total=groups.reduce((n,g)=>n+g.weight,0), casualties={};
  let remaining=Math.min(troopTotal(a),Math.max(0,Math.floor(damage*(exposed?1.2:1))));
  for(const g of groups){const exact=remaining*g.weight/Math.max(1,total);g.loss=Math.min(g.n,Math.floor(exact));g.remainder=exact-g.loss;}
  remaining-=groups.reduce((n,g)=>n+g.loss,0);
  for(const g of groups.sort((a,b)=>b.remainder-a.remainder||a.id.localeCompare(b.id))){if(remaining>0&&g.loss<g.n){g.loss++;remaining--;}}
  for(const g of groups){a.units[g.id]-=g.loss;casualties[g.id]=g.loss;}
  return casualties;
}
function protection(t,defending) {
  if(!defending)return 1;
  return TERRAINS[t.terrain].defense*(t.building==='fort'?1+.25*buildingLevel(t,'fort'):t.building==='city'?1.12:1)*(t.walls>0?1.65:1)*(t.building==='watchtower'?1+.15*buildingLevel(t,'watchtower'):1);
}
function spearCounter(target) {return 1/(1+(target.units.spearman||0)/Math.max(1,troopTotal(target))*3.5*(target.formation==='spearWall'?1.8:1));}
export function resolveFieldBattle(attacker,defender,t,{roll=()=>.5,riverCrossing=false,surrounded=[false,false]}={}) {
  const armies=[attacker,defender],initial=armies.map(a=>({...a.units})),before=armies.map(troopTotal),phases=[];
  const terrain=t.terrain, cavalryTerrain=terrain==='forest'?.32:terrain==='hills'?.6:['city','town','fort'].includes(t.building)?.5:1.25;
  const phase=(name,powers,notes,options={})=>{
    const count=armies.map(troopTotal),loss=[0,0];
    for(let side=0;side<2;side++){
      const target=1-side,a=armies[target],defense=protection(t,target===1)*(a.formation==='defensive'?1.25:1);
      const damage=powers[side]*(.88+roll()*.24)*armies[side].morale/defense;
      inflict(a,Math.min(count[target]*.32,damage),{piercing:options.piercing?.[side],exposed:a.formation==='charge'||name==='Missile Fire'&&a.formation==='spearWall'});
      loss[target]=count[target]-troopTotal(a);
    }
    phases.push({name,loss,notes});
  };
  const flank = armies.map((a,i)=>familyCount(a,'mounted')*(a.formation==='flanking'?1.7:1)*cavalryTerrain/(1+familyCount(armies[1-i],'infantry')/Math.max(1,troopTotal(armies[1-i]))));
  phases.push({name:'Positioning',loss:[0,0],notes:[`${terrain}: defender protection ×${protection(t,true).toFixed(2)}.`,`${FORMATIONS[attacker.formation||'balanced'].name} attacks ${FORMATIONS[defender.formation||'balanced'].name}.`,...(riverCrossing?['An undeveloped river crossing disrupts the attacking line.']:[]),...armies.map((a,i)=>a.units.scout?`${i?'Defending':'Attacking'} scouts screen the approach (${a.units.scout}).`:null).filter(Boolean)]});
  phase('Missile Fire',armies.map((a,i)=>unitPower(a,'ranged')*.11*(terrain==='forest'?.5:1)*(i===1&&terrain==='hills'?1.3:1)*(i===1&&t.walls>0?1.4:1)*(a.formation==='skirmish'?1.25:1)),['Volleys hit before contact; forest cover shortens bow range.'],{piercing:armies.map(a=>(a.units.crossbow||0)>familyCount(a,'ranged')*.35)});
  phase('Charge / Engagement',armies.map((a,i)=>unitPower(a,'charge')*.10*cavalryTerrain*spearCounter(armies[1-i])*(a.formation==='charge'?1.35:1)*(i===0&&riverCrossing?.6:1)),[...(armies.some(a=>a.units.spearman)?['Spearmen brace against mounted charges.']:[]),`${terrain==='plains'?'Open ground supports shock cavalry.':'Broken ground reduces cavalry impact.'}`]);
  phase('Main Melee',armies.map((a,i)=>Object.entries(a.units).reduce((n,[id,v])=>n+v*UNITS[id].attack*(UNITS[id].family==='ranged'?.4:UNITS[id].family==='siege'?.15:1),0)*.17*(i===0&&riverCrossing?.65:1)),['Professional infantry sustain the line; armor reduces their share of losses.']);
  phase('Flanking',flank.map((n,i)=>n*.17/(1+(armies[1-i].units.scout||0)*.04)),['Mounted wings exploit open flanks; scouts reduce surprises.']);
  for(let i=0;i<2;i++){
    const a=armies[i],lost=(before[i]-troopTotal(a))/Math.max(1,before[i]);
    const elite=(a.units.knight||0)+(a.units.heavyInfantry||0);
    a.morale=clamp(a.morale-lost*.85+(t.owner===a.owner?.04:0)+Math.min(.06,elite*.004)-(surrounded[i]?.1:0)-(a.retreats||0)*.012,.1,1);
  }
  const power=armies.map((a,i)=>unitPower(a,'attack')*a.morale*protection(t,i===1));
  const winner=power[0]*(.94+roll()*.12)>power[1]?0:1,loser=1-winner;
  const routed=armies[loser].morale<.55||troopTotal(armies[loser])<before[loser]*.65||power[winner]>power[loser]*1.65;
  phases.push({name:'Morale',loss:[0,0],notes:armies.map((a,i)=>`${i?'Defender':'Attacker'} morale ${Math.round(a.morale*100)}%: ${i===loser?(routed?'rout':'withdrawal'):a.morale<.7?'shaken, holds':'holds'}.`)});
  const pursuit=armies.map((a,i)=>i===winner&&routed?unitPower(a,'pursuit')*.13*(armies[loser].formation==='skirmish'?.55:1):0);
  phase('Pursuit',pursuit,[routed?'Fast troops pursue the broken army.':'The losing army withdraws in order; no rout pursuit.']);
  armies[winner].morale=clamp(armies[winner].morale+.07,.1,1);
  armies[loser].retreats=(armies[loser].retreats||0)+1;
  return {winner,loser,routed,phases,composition:initial,casualties:armies.map((a,i)=>Object.fromEntries(Object.entries(initial[i]).map(([id,n])=>[id,n-(a.units[id]||0)]))),morale:armies.map(a=>a.morale)};
}
export function siegePower(a,t) {
  const level=Math.max(buildingLevel(t,'wall'),buildingLevel(t,'fort'));
  return Math.max(1,Math.floor(Object.entries(a.units).reduce((sum,[id,n])=>sum+n*(UNITS[id]?.breach||0)*(level>=3?(id==='trebuchet'?1.35:id==='ram'?.35:.65):1),0)));
}
export function siegeStep(s,a,t,defender,roll) {
  t.siege ||= {turns:0,morale:1};t.siege.turns++;
  const damage=siegePower(a,t),walls=t.walls,fort=t.fortIntegrity??fortMaximum(t);
  if(walls>0)t.walls=Math.max(0,walls-damage);
  else t.fortIntegrity=Math.max(0,fort-damage);
  t.siege.morale=clamp(t.siege.morale-(damage/Math.max(60,wallMaximum(t)+fortMaximum(t)))*.23-.018,0,1);
  const notes=[`Fortifications take ${damage} damage.`,`${t.walls+(t.fortIntegrity||0)} strength remains; assault follows a breach.`];
  inflict(a,troopTotal(a)*(familyCount(a,'siege')?.01:.035));
  if(defender&&familyCount(defender,'ranged')&&roll()<.3){const engine=Object.keys(a.units).find(id=>UNITS[id].family==='siege'&&a.units[id]>0);if(engine){a.units[engine]--;notes.push('Defending missiles destroyed a siege engine.');}}
  const surrendered=t.siege.morale<.15&&(!defender||defender.morale<.45);
  if(surrendered){t.walls=0;t.fortIntegrity=0;notes.push('The exhausted garrison surrenders.');if(defender)inflict(defender,troopTotal(defender));}
  return {damage,notes,surrendered};
}
