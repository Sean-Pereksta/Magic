// Two optional combinations use ordinary battle turns and normal enemy responses.
import {SPECIES} from './data.mjs';

export const TEAM_TECHNIQUES=Object.freeze([
 {id:'canopy-relay',name:'Canopy Relay',artKey:'technique-canopy-relay',description:'A guarded opening becomes a teammate’s confident strike.',setup:'Have one monkey use a guarding move, then switch to a different conscious teammate within 3 rounds.',effect:'Palm Strike gains at least 25% preparation for this attack. Each technique is usable once per battle.',cost:3,helperCost:2,move:'strike',costText:'3 active energy + 2 helper energy · uses your turn'},
 {id:'dewfall-chorus',name:'Dewfall Chorus',artKey:'technique-dewfall-chorus',description:'Leaf and Water companions weave a gentle restorative shelter.',setup:'Use a Leaf or Water move, then switch within 4 rounds to a conscious teammate of the other element.',effect:'Recover 8% of each conscious teammate’s maximum health, then Brace normally. Once per battle; cannot revive.',cost:5,helperCost:2,move:'brace',costText:'5 active energy + 2 helper energy · uses your turn; Brace restores its usual energy'}
].map(Object.freeze));
const byId=new Map(TEAM_TECHNIQUES.map(row=>[row.id,row]));
const members=s=>(s.run?.party||[]).map(id=>s.collection?.[id]).filter(Boolean);
const count=n=>Number.isSafeInteger(n)&&n>=0?Math.min(1e9,n):0;
function normalize(b){
 const old=b.teamwork&&typeof b.teamwork==='object'?b.teamwork:{};
 const actions={};for(const[id,row]of Object.entries(old.actions||{}))if(row&&typeof row==='object')actions[id]={guardRound:count(row.guardRound),elementRound:count(row.elementRound),element:['leaf','water'].includes(row.element)?row.element:null};
 b.teamwork={actions,used:[...new Set(Array.isArray(old.used)?old.used:[])].filter(id=>byId.has(id)),lastTechnique:byId.has(old.lastTechnique?.id)?{id:old.lastTechnique.id,round:count(old.lastTechnique.round),actorId:String(old.lastTechnique.actorId||''),helperId:String(old.lastTechnique.helperId||'')}:null};return b.teamwork;
}
export function recordTeamAction(s,actor,id,move){
 const b=s.run?.battle;if(!b||b.online||b.social||b.teamTechniqueInProgress||!members(s).some(p=>p.id===actor.id))return;
 const saved=normalize(b),previous=saved.actions[actor.id]||{guardRound:0,elementRound:0,element:null};
 if(move?.guard)previous.guardRound=b.round;
 const element=(move?.types||[move?.type]).find(type=>['leaf','water'].includes(type));if(element){previous.element=element;previous.elementRound=b.round;}
 saved.actions[actor.id]=previous;
}
function helperFor(s,row,saved){
 const b=s.run.battle,p=s.run.monkey;
 return members(s).find(other=>{
  if(other.id===p?.id||other.hp<=0||other.energy<row.helperCost)return false;
  const setup=saved.actions[other.id];if(!setup)return false;
  if(row.id==='canopy-relay')return setup.guardRound>0&&setup.guardRound<b.round&&b.round-setup.guardRound<=3;
  return setup.elementRound>0&&setup.elementRound<b.round&&b.round-setup.elementRound<=4&&other.types?.includes(setup.element)&&p.types?.includes(setup.element==='leaf'?'water':'leaf');
 });
}
export function teamTechniques(s){
 const b=s.run?.battle,p=s.run?.monkey,saved=b?normalize(b):{used:[],actions:{}},inBattle=s.run?.phase==='battle'&&b?.ready&&!b.online&&!b.social;
 return TEAM_TECHNIQUES.map(row=>{const helper=b&&p?helperFor(s,row,saved):null,used=saved.used.includes(row.id);const reason=!inBattle?'Begin an offline expedition battle first.':used?'Already used in this battle.':p?.hp<=0?'Choose a conscious active monkey.':p?.charging?'Finish or interrupt your charged move first.':!p?.loadout?.includes(row.move)?`Equip ${row.move==='brace'?'Brace':'Palm Strike'} between encounters.`:p.energy<row.cost?`Needs ${row.cost} active energy.`:!helper?`${row.setup} The helper also needs ${row.helperCost} energy.`:'';return{...row,ready:!reason,reason,used,helperId:helper?.id||null,helperName:helper?(helper.nickname||SPECIES[helper.species]?.name):null};});
}
export function useTeamTechnique(s,id,{turn,stats}={}){
 if(typeof turn!=='function'||typeof stats!=='function')throw Error('The battle controls are unavailable.');
 const row=teamTechniques(s).find(technique=>technique.id===id);if(!row?.ready)throw Error(row?.reason||'Choose a team technique.');
 // Commit the entire normal turn together, so a failed action cannot spend energy or consume setup.
 const draft=structuredClone(s),b=draft.run.battle,p=draft.collection[draft.run.activeId],helper=draft.collection[row.helperId];draft.run.monkey=p;
 b.enemy=b.enemies.find(enemy=>enemy.id===b.enemy.id);p.energy-=row.cost;helper.energy-=row.helperCost;
 const saved=normalize(b);saved.used.push(id);saved.lastTechnique={id,round:b.round,actorId:p.id,helperId:helper.id};b.teamTechniqueInProgress=true;
 if(id==='canopy-relay'){p.prepare=Math.max(p.prepare||0,.25);p.status.prepare=Math.max(p.status.prepare||0,2);}
 else for(const friend of members(draft))if(friend.hp>0){const max=stats(friend,draft).hp;friend.hp=Math.min(max,friend.hp+Math.max(1,Math.round(max*.08)));}
 draft.log??=[];draft.log.push(`${row.name}! ${helper.nickname||SPECIES[helper.species]?.name||'Your teammate'} helps ${p.nickname||SPECIES[p.species]?.name||'the active monkey'}.`);draft.log=draft.log.slice(-16);
 turn(draft,row.move);delete b.teamTechniqueInProgress;b.metrics??={};b.metrics.teamTechniques=(b.metrics.teamTechniques||0)+1;
 draft.run.lastTeamTechnique={id,name:row.name,effect:row.effect,artKey:row.artKey,round:saved.lastTechnique.round,battleId:b.id,actorId:p.id,helperId:helper.id};
 for(const hit of b.lastHits||[])if(hit.side==='player'&&hit.actorId===p.id){hit.teamTechnique=id;hit.teamTechniqueArt=row.artKey;}
 Object.assign(s,draft);return{...row,round:saved.lastTechnique.round};
}
