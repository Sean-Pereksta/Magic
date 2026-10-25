// Direct, copy-aware equipment controls shared by Team, Sanctuary and the forge.
import {GEAR,ELEMENTS,SPECIES,stats} from './core.mjs';
import {GEAR_RARITIES,LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';
import {equipmentIconSvg} from './equipment-icons.mjs';
import {equipmentSummary} from './equipment-effects.mjs';
import {portrait as defaultPortrait} from './art.mjs';
import {monkeyPower} from './power.mjs';
import * as Inventory from './equipment-inventory.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const title=value=>String(value||'').replace(/^./,c=>c.toUpperCase());
const metadata=id=>({...LEGACY_GEAR_AFFINITIES[id],...GEAR[id]});
const name=p=>p?.nickname||SPECIES[p?.species]?.name||'Companion';
const labels={weapon:'Weapon',armor:'Armor',charm:'Charm',ultra:'Ultra'};
const rarities=Object.keys(GEAR_RARITIES);
const statNames={hp:'Health',atk:'Attack',def:'Defense',speed:'Speed',maxEnergy:'Energy'};
const icon=id=>`<span class="direct-gear-icon">${equipmentIconSvg(id)}</span>`;
const flavor=g=>`${title(g.rarity||'common')} · ${ELEMENTS[g.element||g.affinity]?.name||'Neutral'} · ${labels[g.slot]||'Equipment'}`;
const effects=(id,tier)=>equipmentSummary({[id]:tier});
const effectList=(id,tier)=>`<ul class="gear-effects">${effects(id,tier).map(text=>`<li>${esc(text)}</li>`).join('')}</ul>`;
const pageSize=12;

export function slotIcon(slot){
  const paths={weapon:'<path d="M35 6l7 1 1 7-20 20-7-7z" fill="#d4dec1"/><path d="M13 25l13 13m-13-1-6 6m7-7 4 4"/>',armor:'<path d="M24 5l16 7v14c0 10-16 18-16 18S8 36 8 26V12z" fill="#c6d6b1"/><path d="M24 11v25m-9-16h18"/>',charm:'<path d="M14 8c-10 12 1 18 10 18S44 20 34 8"/><path d="M24 21l10 11-10 12-10-12z" fill="#dfc587"/>',ultra:'<path d="M24 5l5 12 13 1-10 9 4 14-12-8-12 8 4-14L6 18l13-1z" fill="#ead49c"/>'};
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[slot]||paths.charm}</svg>`;
}

export function renderCompactSlots(s,p){
  const loadout=Inventory.equipmentLoadout(s,p);
  return `<div class="compact-equipment-slots" aria-label="${esc(name(p))} equipment">${Inventory.equipmentSlots(p).map(slot=>{const id=p.equipment?.[slot],g=GEAR[id]&&metadata(id),owned=g&&loadout[id];return `<button type="button" class="compact-equipment-slot ${owned?`rarity-${g.rarity||'common'}`:'is-empty'}" data-action="gear-picker" data-id="${slot}" data-monkey="${esc(p.id)}" title="${labels[slot]}: ${esc(owned?g.name:'Empty')}" aria-label="${labels[slot]}: ${esc(owned?g.name:'Empty')}. Choose equipment">${owned?equipmentIconSvg(id):slotIcon(slot)}<span>${labels[slot]}</span></button>`;}).join('')}</div>`;
}

export function renderEquipmentSlots(s,p){
  const loadout=Inventory.equipmentLoadout(s,p);
  return `<div class="equipment-slots direct-equipment-slots">${Inventory.equipmentSlots(p).map(slot=>{const id=p.equipment?.[slot],g=GEAR[id]&&metadata(id),tier=g&&loadout[id];return `<article class="equipment-slot direct-equipment-slot ${tier?`rarity-${g.rarity||'common'}`:'is-empty'} ${slot==='ultra'?'ultra-slot':''}"><button type="button" class="direct-slot-open" data-action="gear-picker" data-id="${slot}" data-monkey="${esc(p.id)}"><span class="equipment-slot-label">${labels[slot]}</span><span class="direct-slot-art">${tier?equipmentIconSvg(id):slotIcon(slot)}</span><strong>${tier?esc(g.name):'Empty slot'}</strong><small>${tier?`${title(g.rarity||'common')} · Rank ${tier}`:'Tap to equip'}</small></button>${tier?`<button type="button" class="text-button direct-slot-remove" data-action="gear-unequip" data-id="${slot}" data-monkey="${esc(p.id)}" ${Inventory.canChangeEquipment(s)?'':'disabled'} aria-label="Remove ${esc(g.name)} from ${labels[slot]}">Remove</button>`:''}</article>`;}).join('')}</div><p class="fine">Tap a slot to compare and equip. Replaced gear stays in your pack. Existing equipment remains usable in any slot.</p>`;
}

// Calculate the complete loadout with the same conditional stats used in combat.
export function compareEquipment(s,copyId,monkeyId,slot){
  const p=s.collection?.[monkeyId];if(!p)return null;
  try{
    const view=Inventory.previewEquipmentLoadout(s,copyId,monkeyId,slot),context={team:(s.run.party||[]).map(id=>s.collection[id]).filter(Boolean),includeConditional:true};
    const beforeStats=stats(p,view.before,context),afterStats=stats(p,view.after,context),beforeEffects=equipmentSummary(view.before),afterEffects=equipmentSummary(view.after);
    return {...view,beforeStats,afterStats,beforePower:monkeyPower(p,view.before),afterPower:monkeyPower(p,view.after),gained:afterEffects.filter(text=>!beforeEffects.includes(text)),lost:beforeEffects.filter(text=>!afterEffects.includes(text))};
  }catch(error){return {reason:error.message};}
}

export function renderEquipmentComparison(view){
  if(!view)return '';if(view.reason)return `<p class="notice">${esc(view.reason)}</p>`;
  const delta=(a,b)=>b>a?`<strong class="stat-gain">+${b-a}</strong>`:b<a?`<strong class="stat-loss">${b-a}</strong>`:'<span class="stat-same">—</span>';
  return `<section class="direct-gear-comparison" aria-label="Equipment stat comparison"><div class="direct-comparison-heading"><h3>Your whole loadout</h3><span>Before → After</span></div><div class="direct-stat-grid">${Object.entries(statNames).map(([key,label])=>`<div><span>${label}</span><b>${view.beforeStats[key]} <i>→</i> ${view.afterStats[key]}</b>${delta(view.beforeStats[key],view.afterStats[key])}</div>`).join('')}</div><p class="fine direct-power-change">Power ${view.beforePower.toLocaleString()} → ${view.afterPower.toLocaleString()} ${delta(view.beforePower,view.afterPower)}</p>${view.gained.length?`<p class="direct-effect-gain"><strong>Gained:</strong> ${esc(view.gained.join(' '))}</p>`:''}${view.lost.length?`<p class="direct-effect-loss"><strong>Replaced:</strong> ${esc(view.lost.join(' '))}</p>`:''}</section>`;
}

function renderEquipConfirmation(s,p,copy,slot){
  const view=compareEquipment(s,copy.id,p.id,slot),owner=s.collection[copy.ownerId],already=copy.ownerId===p.id&&copy.slot===slot,replaced=p.equipment?.[slot];
  return `<section class="direct-equip-confirmation"><div class="equipment-slot-heading">${icon(copy.itemId)}<div><h3>${esc(GEAR[copy.itemId].name)}</h3><small>${esc(name(p))} · ${labels[slot]} · Rank ${copy.tier}</small></div></div>${effectList(copy.itemId,copy.tier)}${owner&&owner.id!==p.id?`<p class="notice">Moves this copy from ${esc(name(owner))}. Their slot becomes empty.</p>`:''}${replaced&&replaced!==copy.itemId?`<p class="fine">${esc(GEAR[replaced]?.name)} returns to your pack.</p>`:''}${renderEquipmentComparison(view)}<button type="button" class="primary wide direct-equip-button" data-action="gear-equip" data-id="${esc(copy.id)}" data-monkey="${esc(p.id)}" data-slot="${slot}" ${!Inventory.canChangeEquipment(s)||already||view?.reason?'disabled':''}>${already?'Equipped':'Equip'}</button>${!Inventory.canChangeEquipment(s)?'<p class="notice">Finish the encounter before changing equipment.</p>':''}</section>`;
}

export function renderGearPicker(s,picker={}){
  const p=s.collection?.[picker.monkeyId];if(!p)return '';
  const slot=Inventory.equipmentSlots(p).includes(picker.slot)?picker.slot:'weapon',all=Inventory.ownedEquipmentCopies(s),element=picker.element||'all',rarity=picker.rarity||'all';
  const filtered=all.filter(c=>{const g=metadata(c.itemId);return (element==='all'||(g.element||g.affinity||'neutral')===element)&&(rarity==='all'||(g.rarity||'common')===rarity);}).sort((a,b)=>Number(metadata(b.itemId).slot===slot)-Number(metadata(a.itemId).slot===slot)||Number(Boolean(a.ownerId))-Number(Boolean(b.ownerId))||rarities.indexOf(metadata(b.itemId).rarity)-rarities.indexOf(metadata(a.itemId).rarity)||b.tier-a.tier||GEAR[a.itemId].name.localeCompare(GEAR[b.itemId].name));
  const pages=Math.max(1,Math.ceil(filtered.length/pageSize)),page=Math.max(0,Math.min(pages-1,picker.page||0)),copy=filtered.find(c=>c.id===picker.copyId)||filtered.find(c=>c.id===p.equipmentCopies?.[slot])||filtered[page*pageSize];
  return `<section class="gear-picker direct-gear-picker"><span class="eyebrow">${esc(name(p))} · ${labels[slot]}</span><h2>Choose equipment</h2><p class="fine">${filtered.length} copies · ${labels[slot]} gear shown first. All owned gear remains usable.</p><div class="forge-filter-row"><label>Element<select data-gear-filter="element"><option value="all">All elements</option>${[...Object.entries(ELEMENTS),['neutral',{name:'Neutral'}]].map(([id,e])=>`<option value="${id}" ${element===id?'selected':''}>${e.name}</option>`).join('')}</select></label><label>Rarity<select data-gear-filter="rarity"><option value="all">All rarities</option>${rarities.map(id=>`<option value="${id}" ${rarity===id?'selected':''}>${title(id)}</option>`).join('')}</select></label></div><div class="direct-gear-grid">${filtered.slice(page*pageSize,page*pageSize+pageSize).map(row=>{const g=metadata(row.itemId),owner=s.collection[row.ownerId];return `<button type="button" class="direct-gear-tile rarity-${g.rarity||'common'}" data-action="gear-select" data-id="${esc(row.id)}" aria-pressed="${copy?.id===row.id}" aria-label="${esc(g.name)}, ${title(g.rarity||'common')}, rank ${row.tier}${owner?`, equipped by ${esc(name(owner))}`:', available'}"><span class="direct-tile-rarity">${GEAR_RARITIES[g.rarity||'common'].symbol}</span>${equipmentIconSvg(row.itemId)}<strong>${esc(g.name)}</strong><small>${title(g.rarity||'common')} · Rank ${row.tier}</small><span class="direct-tile-owner ${owner?'is-equipped':''}">${owner?esc(name(owner)):'Available'}</span>${row.favorite||row.protected?`<span class="direct-tile-flags">${row.favorite?'♥ ':''}${row.protected?'Protected':''}</span>`:''}</button>`;}).join('')||'<p class="muted">No equipment matches. Try another filter or discover more gear at the forge.</p>'}</div>${pages>1?`<div class="forge-pagination"><button type="button" data-action="gear-page" data-id="-1" ${page===0?'disabled':''}>Previous</button><span>${page+1} / ${pages}</span><button type="button" data-action="gear-page" data-id="1" ${page===pages-1?'disabled':''}>Next</button></div>`:''}${copy?renderEquipConfirmation(s,p,copy,slot):''}<button type="button" data-action="close-dialog" class="text-button wide">Done</button></section>`;
}

export function renderForgeResult(s,p,{unsaved=false,reveal=true}={}){
  const receipt=s.meta.forge?.last;if(!receipt?.itemId||!GEAR[receipt.itemId])return '';
  const g=metadata(receipt.itemId),copy=receipt.copyId?Inventory.equipmentCopy(s,receipt.copyId):null,tier=copy?.tier||receipt.tier||1,rarity=g.rarity||'common';
  return `<article class="forge-result direct-forge-result rarity-${rarity} ${reveal?'gear-reveal':''}" data-forge-reveal="${esc(receipt.operationId)}" aria-label="Crafted equipment"><div class="direct-reveal-top"><span class="eyebrow">${unsaved?'SAVE PENDING':rarity==='mythic'?'MYTHIC DISCOVERY':'SMELT COMPLETE · SAVED'}</span>${reveal?'<button type="button" class="text-button" data-action="forge-skip-reveal">Skip effects</button>':''}</div><div class="direct-reveal-art">${equipmentIconSvg(receipt.itemId,{size:120})}<span class="direct-reveal-spark" aria-hidden="true">✦</span></div><h2>${esc(g.name)}</h2><p class="direct-reveal-meta">${esc(flavor(g))} · Rank ${tier}</p><p class="fine">${receipt.kind==='salvaged'?'This older crafting receipt returned fragments.':receipt.kind==='upgraded'?`Your available duplicate is now rank ${tier}.`:'A permanent equipment copy is ready for your crew.'}</p>${effectList(receipt.itemId,tier)}${receipt.costSummary?.length?`<p class="fine">Spent ${receipt.costSummary.map(row=>`${row.count} ${esc(row.name)}`).join(' · ')}${receipt.campBonus?' · Camp bonus applied':''}</p>`:''}<div class="direct-reveal-actions">${copy?`<button type="button" class="primary wide" data-action="forge-equip-roster" data-id="${esc(copy.id)}" ${unsaved||!Inventory.canChangeEquipment(s)?'disabled':''}>Equip to Monkey</button>`:''}<button type="button" class="wide" data-action="forge-keep">Keep in Pack</button></div>${unsaved?'<p class="notice">Saving your equipment. Retry saving before equipping or smelting again.</p>':''}</article>`;
}

export function renderEquipRoster(s,selection={},{portrait=defaultPortrait}={}){
  const copy=Inventory.equipmentCopy(s,selection.copyId);if(!copy)return '<p class="notice">This equipment copy is unavailable.</p><button data-action="close-dialog">Close</button>';
  const g=metadata(copy.itemId),partyIds=s.run.party||[],party=partyIds.map(id=>s.collection[id]).filter(Boolean),query=String(selection.search||'').trim().toLowerCase(),all=Object.values(s.collection||{}),sanctuary=all.filter(p=>!partyIds.includes(p.id)&&(!query||`${name(p)} ${SPECIES[p.species]?.name||''}`.toLowerCase().includes(query))),pages=Math.max(1,Math.ceil(sanctuary.length/36)),page=Math.max(0,Math.min(pages-1,selection.page||0)),p=s.collection[selection.monkeyId];
  const target=monkey=>`<button type="button" class="direct-roster-monkey" data-action="gear-roster-monkey" data-id="${esc(monkey.id)}" aria-pressed="${monkey.id===p?.id}">${portrait(monkey,{state:'idle',reducedMotion:true})}<strong>${esc(name(monkey))}</strong><small>Lv. ${monkey.level||1}</small><span class="direct-roster-dots" aria-label="${Object.keys(Inventory.equipmentLoadout(s,monkey)).length} slots equipped">${Inventory.equipmentSlots(monkey).map(slot=>`<i class="${monkey.equipment?.[slot]?'filled':''}"></i>`).join('')}</span></button>`;
  const slots=p?Inventory.equipmentSlots(p):[],slot=slots.includes(selection.slot)?selection.slot:slots.includes(g.slot)?g.slot:slots[0];
  return `<section class="direct-equip-roster"><span class="eyebrow">EQUIP YOUR DISCOVERY</span><div class="equipment-slot-heading">${icon(copy.itemId)}<div><h2>${esc(g.name)}</h2><small>${esc(flavor(g))} · Rank ${copy.tier}</small></div></div><h3>Expedition team</h3><div class="direct-roster-grid direct-roster-team">${party.map(target).join('')}</div><details class="direct-sanctuary" ${p&&!partyIds.includes(p.id)||!party.length||query?'open':''}><summary>Sanctuary · ${all.length-party.length} companions</summary><label><span class="visually-hidden">Find a sanctuary monkey</span><input type="search" data-gear-roster-search placeholder="Find a companion…" value="${esc(selection.search||'')}"></label><div class="direct-roster-grid">${sanctuary.slice(page*36,page*36+36).map(target).join('')||'<p class="fine">No sanctuary companions match.</p>'}</div>${pages>1?`<div class="forge-pagination"><button type="button" data-action="gear-roster-page" data-id="-1" ${page===0?'disabled':''}>Previous</button><span>${page+1} / ${pages}</span><button type="button" data-action="gear-roster-page" data-id="1" ${page===pages-1?'disabled':''}>Next</button></div>`:''}</details>${p?`<h3>${esc(name(p))} · choose a slot</h3><div class="direct-roster-slot-picker">${slots.map(id=>`<button type="button" data-action="gear-roster-slot" data-id="${id}" aria-pressed="${slot===id}">${slotIcon(id)}<span>${labels[id]}</span><small>${esc(GEAR[p.equipment?.[id]]?.name||'Empty')}</small></button>`).join('')}</div>${renderEquipConfirmation(s,p,copy,slot)}`:'<p class="direct-roster-instruction">Choose a companion to compare and equip.</p>'}<button type="button" class="text-button wide" data-action="close-dialog">Cancel</button></section>`;
}
