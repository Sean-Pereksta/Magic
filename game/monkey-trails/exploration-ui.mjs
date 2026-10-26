import {ITEMS,ELEMENTS,SPECIES} from './data.mjs';
import {activeExplorer,explorationProfile} from './exploration.mjs';
import {habitatAt} from './habitats.mjs';

const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const pct=n=>`${Number((100*(n||0)).toFixed(1))}%`;
const monkeyName=p=>p?.nickname||SPECIES[p?.species]?.name||'Companion';
const materialName=id=>ITEMS[id]?.name||({fruit:'Trail fruit',essence:'Elemental essence',charmMaterial:'Charm material',fragment:'Equipment fragments'})[id]||(ELEMENTS[id]?`${ELEMENTS[id].name} essence`:id.startsWith('essence:')?`${ELEMENTS[id.slice(8)]?.name||id.slice(8)} essence`:id);

export function explorationRosterClass(p){const b=explorationProfile(p);return b.forageChance>0||b.rareEncounterBonus>0?'exploration-special':'';}
export function explorationBadge(p,{selected=false}={}){
 const b=explorationProfile(p),forager=b.forageChance>0,scout=b.rareEncounterBonus>0;
 if(!forager&&!scout&&!selected)return '';
 const label=selected?'Active monkey':[forager?'Forager':'',scout?'Rare scout':''].filter(Boolean).join(' · ');
 return `<span class="exploration-badge ${selected?'is-active':''}" data-exploration-badge title="${esc(label)}" aria-label="${esc(label)}"><span aria-hidden="true">${selected?'◉':forager?'♧':'✦'}</span> ${esc(label)}</span>`;
}
export function explorationDetails(s,p){
 const b=explorationProfile(p),selected=!!p.id&&activeExplorer(s)?.id===p.id;
 const weights=Object.entries(b.materialWeights||{}).filter(([,n])=>n>1).sort((a,b)=>b[1]-a[1]);
 return `<section class="exploration-profile" data-exploration-profile data-monkey-id="${esc(p.id)}"><div class="exploration-profile-heading"><h3>Exploration abilities</h3>${explorationBadge(p,{selected})}</div><strong>${esc(b.name)}</strong><p>${esc(b.description)}</p><div class="exploration-chances"><span><b>${pct(b.forageChance)}</b> extra pickup chance</span><span><b>+${pct(b.rareEncounterBonus)}</b> rare encounter weight</span></div><p class="fine">${selected?'This monkey is leading your walk.':'Select this conscious monkey from your lineup to use these abilities.'} Each accepted trail discovery can award one extra pickup. Rare scouting improves the weight of eligible rare enemies; region and depth still apply.</p><h4>Favored finds</h4><div class="exploration-specialties">${weights.map(([id,n])=>`<span>${esc(materialName(id))} <b>×${Number(n.toFixed(2))}</b></span>`).join('')||'<span>Balanced resource finds</span>'}</div><p class="fine">These multipliers favor your walking finds and bonus pickups. Your habitat also influences finds. Star Ore is a very rare walking find; Crown Cores come only from Ultra victories.</p>${b.specialties?.length?`<p class="fine">${b.specialties.map(esc).join(' · ')}</p>`:''}</section>`;
}
export function activeMonkeySummary(s,{portrait}={}){
 const p=activeExplorer(s);if(!p)return '';
 const b=explorationProfile(p),habitat=habitatAt(s);
 return `<section class="active-monkey-summary" aria-label="Active walking monkey"><button type="button" data-action="monkey-actions" data-id="${esc(p.id)}">${portrait?portrait(p):''}<span><small>ACTIVE MONKEY</small><strong>${esc(monkeyName(p))}</strong><span>${pct(b.forageChance)} extra finds · +${pct(b.rareEncounterBonus)} rare weight</span></span><b aria-hidden="true">↗</b></button><div class="exploration-habitat" data-current-habitat="${esc(habitat.id)}"><strong>${esc(habitat.name)}</strong><span>${esc(habitat.description)}</span></div></section>`;
}
export function trainingMaterialsReady(s,cost){return Object.entries(cost.materials||{}).every(([id,n])=>(s.run.items[id]||0)>=n);}
export function trainingMaterialText(cost){return Object.entries(cost.materials||{}).map(([id,n])=>`${n} ${materialName(id)}`).join(' + ');}
export function trainingMaterialsUI(s,cost){
 return Object.entries(cost.materials||{}).map(([id,n])=>`<div class="training-material ${(s.run.items[id]||0)>=n?'is-ready':'is-short'}" data-training-material="${esc(id)}"><strong>${esc(materialName(id))}</strong><span>${s.run.items[id]||0} / ${n}</span><small>${id==='crownCore'?'Defeat Ultra / legendary encounters to earn this.':'Very rare walking find; mineral specialists improve your chances.'}</small></div>`).join('');
}
