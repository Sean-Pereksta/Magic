// Read-only combat explanations. Previews never call random(), execute a move,
// normalize a save, or change status effects. Values use the current rules.
import {MOVES,ELEMENTS,SPECIES} from './data.mjs';
import {CONFIG,stats,statsLegacy,equipped,team,moveEffect,moveEffectLegacy,effectInfo} from './core.mjs';

const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const alive=p=>p&&p.hp>0;
const combatStats=(s,p,version)=>p.combatStats||((version===1?statsLegacy:stats)(p,s?.collection?.[p.id]?equipped(s,p):p.gear||{}));

export function moveSummary(move){
  const m=typeof move==='string'?MOVES[move]:move;if(!m)return 'Unknown technique.';
  const parts=[];
  if(m.breakShield)parts.push('Break shields');
  if(m.power)parts.push('Damage');
  if(m.status)parts.push(m.status==='root'?'root the opponent':m.status==='weaken'?'weaken the next attack':m.status==='slow'?'slow the opponent':m.status);
  else if(m.root)parts.push('root the opponent');
  if(m.weaken&&m.status!=='weaken')parts.push('weaken the next attack');
  if(m.teamHeal)parts.push('heal conscious teammates');else if(m.heal)parts.push('heal yourself');
  if(m.teamShield)parts.push(m.switch?'shield the incoming teammate':'prepare a shield for the next teammate');else if(m.shield)parts.push('shield yourself');
  if(m.guard)parts.push('block most incoming damage');
  if(m.counter)parts.push('reflect the next hit');
  if(m.cleanse)parts.push('clear personal status effects');
  if(m.prepare)parts.push('strengthen your next attack');
  if(m.rally)parts.push('strengthen the next teammate');
  if(m.energy||m.guard)parts.push(`recover ${m.energy||(m.guard?2:0)} energy`);
  if(m.drain)parts.push('heal from damage dealt');
  if(m.drainEnergy)parts.push('drain energy');
  if(m.statusSelf==='speed')parts.push('gain speed');
  if(m.transferSpeed)parts.push('transfer speed');
  if(m.entryEnergy)parts.push('restore incoming energy');
  if(m.bondBonus)parts.push('improve recruitment');
  if(m.switch)parts.push('switch teammates');
  if(!parts.length)return m.desc||'Use this technique.';
  return parts.join(' · ').replace(/^./,c=>c.toUpperCase())+'.';
}

export function actionAvailability(s,id,{actor=s?.run?.monkey,members,round=s?.run?.battle?.round||1,ignoreReady=false}={}){
  const m=MOVES[id],b=s?.run?.battle;members??=s?.run?team(s):[];
  const cooldown=Math.max(0,(actor?.cooldowns?.[id]||0)-round),uses=m?.uses?Math.max(0,m.uses-(actor?.uses?.[id]||0)):null;
  let reason='';
  if(!m||!actor?.loadout?.includes(id))reason='This ability is not equipped.';
  else if(!alive(actor))reason='This companion is knocked out.';
  else if(!ignoreReady&&b&&!b.ready)reason='Stop safely and begin the battle first.';
  else if(cooldown)reason=`Ready in ${cooldown} ${cooldown===1?'round':'rounds'}.`;
  else if(uses===0)reason='No uses remaining this encounter.';
  else if(actor.energy<(m.cost||0))reason=`Needs ${m.cost-actor.energy} more energy.`;
  else if(m.switch&&actor.status?.root)reason='Cannot switch while rooted.';
  else if(m.switch&&!members.some(p=>p.id!==actor.id&&alive(p)))reason='Needs another conscious teammate.';
  return{available:!reason,reason,cooldown,uses,cost:m?.cost||0};
}

export function damagePreview(s,id,{actor=s?.run?.monkey,target=s?.run?.battle?.enemy,socialVersion}={}){
  const m=MOVES[id];if(!m?.power||!actor||!target)return null;
  const ast=combatStats(s,actor,socialVersion),tst=combatStats(s,target,socialVersion),multiplier=(socialVersion===1?moveEffectLegacy:moveEffect)(m,target.types||[]);
  const stab=(m.types||[m.type]).some(t=>(actor.types||[]).includes(t))?(socialVersion===1?1.15:CONFIG.elemental.stab):1;
  const howl=actor.species==='howler'&&m.type!=='neutral'?1.12:1;
  // Cleansing happens before damage and removes prepare/weaken in the reducer.
  const prepare=m.prepare||(m.cleanse?0:actor.prepare)||0,weaken=!m.cleanse&&actor.status?.weaken?(actor.statusPower?.weaken||.25):0;
  const factor=multiplier*stab*howl*(m.breakShield?1:target.guard||1)*(prepare?1+prepare:1)*(1-weaken),shield=m.breakShield?0:target.shield||0;
  const hit=(variance,critical)=>Math.min(target.hp,Math.max(0,Math.max(1,Math.round((m.power+ast.atk*.70-tst.def*.42)*factor*variance*(critical?1.4:1)))-shield));
  const criticalPossible=socialVersion!==undefined||!actor.tutorialHpScale||CONFIG.tutorial.enemyCriticals;
  const notes=['Estimate at the current stats and effects; actions earlier in the round can change it.'];
  if(target.species==='spider')notes.push('Spider agility can dodge a hit completely (8% chance).');
  if(target.counter)notes.push('The opponent can reflect damage.');
  if(m.status){const immune=m.status==='burn'&&target.types?.includes('fire')||m.status==='poison'&&target.types?.includes('leaf');if(immune)notes.push(`This opponent resists ${m.status}; the damage still applies.`);else notes.push(`Damage excludes later ${m.status==='burn'||m.status==='poison'?m.status+' damage':'status effects'}.`);}
  if(m.drain)notes.push('Healing depends on the damage actually dealt.');
  if(shield)notes.push(`${Math.round(shield)} current shield points are included.`);
  if(m.breakShield)notes.push('Current shields and guard are removed before damage.');
  const min=hit(.94,false),max=hit(1.06,false),criticalMax=criticalPossible?hit(1.06,true):max;
  return{min,max,criticalMax,criticalPossible,dodgePossible:target.species==='spider',multiplier,notes,label:`${min}–${max} damage${criticalPossible&&criticalMax>max?` · critical up to ${criticalMax}`:''}`};
}

export function abilityGuide(s,id,options={}){
  const m=MOVES[id];if(!m)return null;
  const target=options.target||s?.run?.battle?.enemy,availability=actionAvailability(s,id,options),damage=damagePreview(s,id,options),types=m.types||[m.type||'neutral'];
  return{id,name:m.name,types,elementLabel:types.map(t=>ELEMENTS[t]?.name||'Physical').join(' + '),summary:moveSummary(m),description:m.desc,cost:m.cost||0,
    ...availability,effectiveness:m.power&&target?effectInfo((options.socialVersion===1?moveEffectLegacy:moveEffect)(m,target.types||[])):null,damage};
}

export function renderAbilityCards(s,{busy=false,escape:esc=escape}={}){
  const p=s.run.monkey;
  return Array.from({length:4},(_,slot)=>{
    const id=p.loadout[slot],g=abilityGuide(s,id);if(!g)return '<div class="battle-ability-card"><button class="move" disabled>No ability equipped</button></div>';
    const eff=g.effectiveness,timing=g.cooldown?`Ready in ${g.cooldown} ${g.cooldown===1?'round':'rounds'}`:g.uses!==null?`${g.uses} ${g.uses===1?'use':'uses'} left`:'';
    return `<div class="battle-ability-card"><button type="button" class="move" data-action="move" data-id="${esc(id)}" ${busy||!g.available?'disabled':''} style="--move:${ELEMENTS[g.types[0]]?.color||'#d8e6bc'}" ${g.reason?`aria-describedby="move-reason-${slot}"`:''}><span class="move-meta">${g.cost} EN${timing?` · ${esc(timing)}`:''}</span><strong>${esc(g.name)}</strong><small>${esc(g.elementLabel)}</small><span class="move-summary">${esc(g.summary)}</span>${eff?`<span class="effectiveness-badge is-${eff.kind}" data-effectiveness="${eff.kind}">${esc(eff.label)} <b>×${Number(eff.multiplier.toFixed(2))}</b></span>`:''}${g.damage?`<span class="move-damage">${esc(g.damage.label)}</span>`:''}</button>${g.reason?`<p class="move-reason" id="move-reason-${slot}">${esc(g.reason)}</p>`:''}<button type="button" class="move-guide-button" data-action="move-guide" data-id="${esc(id)}" aria-label="Details for ${esc(g.name)}">ⓘ Details</button></div>`;
  }).join('');
}

export function renderAbilityGuide(s,id,options={}){
  const g=abilityGuide(s,id,options);if(!g)return '<p>Unknown ability.</p>';
  return `<section class="battle-ability-guide"><span class="eyebrow">${escape(g.elementLabel)} · ${g.cost} ENERGY</span><h2>${escape(g.name)}</h2><p>${escape(g.description)}</p>${g.effectiveness?`<p class="effectiveness-badge is-${g.effectiveness.kind}">${escape(g.effectiveness.label)} ×${Number(g.effectiveness.multiplier.toFixed(2))}</p>`:''}${g.damage?`<h3>${escape(g.damage.label)}</h3><ul>${g.damage.notes.map(note=>`<li>${escape(note)}</li>`).join('')}</ul>`:''}<p>${escape(g.reason||'Ready to use.')}${g.cooldown?` Cooldown: ${g.cooldown} rounds.`:''}${g.uses!==null?` Uses remaining: ${g.uses}.`:''}</p><p class="fine">Details do not use energy or advance the round.</p></section>`;
}

export function battleActionHints(s){
  const p=s.run.monkey,b=s.run.battle;return{switch:p.status?.root?'Cannot switch while rooted.':!team(s).some(x=>x.id!==p.id&&alive(x))?'Needs another conscious teammate.':'Switching uses your battle action.',supply:b?.itemUsedRound===b?.round?'Supply already used this round. Choose an ability or switch.':'One supply can be used alongside your battle action.'};
}
