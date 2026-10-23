import {GEAR,ELEMENTS,SPECIES} from './data.mjs';
import {forgeInventory,previewForge} from './forge.mjs';
import {LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';
import {equipmentSummary} from './equipment-effects.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const gearData=id=>({...LEGACY_GEAR_AFFINITIES[id],...GEAR[id]});
const slots={weapon:'Weapon',armor:'Armor',charm:'Charm'};
const rarityOrder={common:0,uncommon:1,rare:2,epic:3,legendary:4};
const monkeyName=p=>p?.nickname||SPECIES[p?.species]?.name||'Companion';
const title=s=>s?String(s)[0].toUpperCase()+String(s).slice(1):'';
// The forge draws from a million tickets; four percentage decimals retain every ticket.
export const percent=n=>`${Number((Number(n)*100).toFixed(4))}%`;
const canEquip=s=>['trail','camp'].includes(s.run.phase);
const ownedTier=(s,id)=>s.meta.gear[id]||0;
const owner=(s,id)=>Object.values(s.collection).find(p=>Object.values(p.equipment||{}).includes(id));
const gearEffects=(id,tier)=>equipmentSummary({[id]:tier});

export function gearIcon(id){
  const g=gearData(id),color=ELEMENTS[g?.element]?.color||'#d6b56e';
  const shape=g?.slot==='weapon'?'<path d="m13 29 12-15 6 5-12 15m-7-8 10 8M25 14l7-5 3 3-4 7"/>':g?.slot==='armor'?'<path d="m12 13 7-4h10l7 4-4 10-4-2v15H16V21l-4 2-4-10z"/>':'<path d="M16 10q8-6 16 0l-2 12H18z"/><path d="m24 20 11 10-11 10-11-10z"/><circle cx="24" cy="30" r="3"/>';
  return `<svg viewBox="0 0 48 48" aria-hidden="true" class="forge-gear-icon"><rect x="1" y="1" width="46" height="46" rx="12" fill="${color}" fill-opacity=".13"/><g fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${shape}</g></svg>`;
}
function gearMeta(g){return `${title(g.rarity||'common')} · ${ELEMENTS[g.element]?.name||'Adaptable'} · ${slots[g.slot]||g.slot}`;}
function effectList(id,tier){return `<ul class="gear-effects">${gearEffects(id,tier).map(text=>`<li>${esc(text)}</li>`).join('')}</ul>`;}

export function renderEquipmentSlots(s,p){
  return `<div class="equipment-slots">${Object.entries(slots).map(([slot,label])=>{
    const id=p.equipment?.[slot],g=GEAR[id]&&gearData(id),tier=ownedTier(s,id);
    return `<article class="equipment-slot"><span class="equipment-slot-label">${label}</span><div class="equipment-slot-heading">${gearIcon(id||({weapon:'fang',armor:'shell',charm:'heart'}[slot]))}<div><h3>${g&&tier?esc(g.name):'Empty slot'}</h3><small>${g&&tier?`${esc(gearMeta(g))} · Rank ${tier}`:`Choose an owned ${label.toLowerCase()}`}</small></div></div>${g&&tier?effectList(id,tier):''}<div class="button-row"><button type="button" data-action="gear-picker" data-id="${slot}" data-monkey="${esc(p.id)}">${g&&tier?'Change':'Choose'} ${label.toLowerCase()}</button>${g&&tier?`<button type="button" class="text-button" data-action="gear-unequip" data-id="${slot}" data-monkey="${esc(p.id)}" ${canEquip(s)?'':'disabled'}>Remove</button>`:''}</div></article>`;
  }).join('')}</div><p class="fine">Each item has one owner. Changing equipment keeps current HP; extra maximum health never grants free healing. Equipment changes unlock on the trail and at camp.</p>`;
}

export function renderGearPicker(s,picker){
  const p=s.collection[picker.monkeyId];if(!p)return '';
  const all=Object.keys(GEAR).filter(id=>ownedTier(s,id)>0&&GEAR[id].slot===picker.slot);
  const filtered=all.filter(id=>(picker.element==='all'||gearData(id).element===picker.element)&&(picker.rarity==='all'||(gearData(id).rarity||'common')===picker.rarity)).sort((a,b)=>(rarityOrder[gearData(b).rarity]||0)-(rarityOrder[gearData(a).rarity]||0)||GEAR[a].name.localeCompare(GEAR[b].name));
  const pages=Math.max(1,Math.ceil(filtered.length/6)),page=Math.max(0,Math.min(pages-1,picker.page||0));
  return `<section class="gear-picker"><span class="eyebrow">${esc(monkeyName(p))} · ${slots[picker.slot]}</span><h2>Choose equipment</h2><p class="muted">${all.length} owned for this slot. Each item shows its effects at its current rank.</p><div class="forge-filter-row"><label>Element<select data-gear-filter="element"><option value="all">All elements</option>${Object.entries(ELEMENTS).map(([id,e])=>`<option value="${id}" ${picker.element===id?'selected':''}>${e.name}</option>`).join('')}</select></label><label>Rarity<select data-gear-filter="rarity"><option value="all">All rarities</option>${Object.keys(rarityOrder).filter(id=>all.some(key=>(gearData(key).rarity||'common')===id)).map(id=>`<option value="${id}" ${picker.rarity===id?'selected':''}>${title(id)}</option>`).join('')}</select></label></div><div class="gear-picker-items">${filtered.slice(page*6,page*6+6).map(id=>{
    const g=gearData(id),tier=ownedTier(s,id),wearer=owner(s,id),on=wearer?.id===p.id;
    return `<article class="gear-choice rarity-${g.rarity||'common'}"><div class="equipment-slot-heading">${gearIcon(id)}<div><h3>${esc(g.name)} <small>Rank ${tier}</small></h3><small>${esc(gearMeta(g))}</small></div></div>${effectList(id,tier)}<p class="fine">${wearer?`Equipped by ${esc(monkeyName(wearer))}`:'Available in your equipment inventory'}</p><button type="button" class="wide" data-action="equip" data-id="${id}" data-monkey="${esc(p.id)}" ${on||!canEquip(s)?'disabled':''}>${on?'Equipped':wearer?`Move from ${esc(monkeyName(wearer))}`:`Equip to ${esc(monkeyName(p))}`}</button></article>`;
  }).join('')||'<p class="muted">No owned items match. Mix resources in the forge to discover equipment.</p>'}</div>${pages>1?`<div class="forge-pagination"><button data-action="gear-page" data-id="-1" ${page===0?'disabled':''}>Previous</button><span>${page+1} / ${pages}</span><button data-action="gear-page" data-id="1" ${page===pages-1?'disabled':''}>Next</button></div>`:''}${!canEquip(s)?'<p class="notice">Finish the encounter before changing equipment.</p>':''}<button type="button" data-action="close-dialog" class="text-button wide">Done</button></section>`;
}

export function renderIngredientOptions(s,mix){
  const rows=forgeInventory(s).filter(row=>row.owned>0);
  return rows.length?rows.map(row=>`<option value="${esc(row.id)}">${esc(row.name)} · ${Math.max(0,row.owned-(mix[row.id]||0))} available · ${esc(title(row.rarity||'common'))} · ${row.value} value</option>`).join(''):'<option value="">Walk to gather ingredients</option>';
}
export function renderMix(s,mix){
  const inventory=new Map(forgeInventory(s).map(row=>[row.id,row]));
  return Object.entries(mix).map(([id,quantity])=>{
    const row=inventory.get(id);
    return `<div class="forge-ingredient"><div><strong>${esc(row?.name||id)}</strong><small data-forge-available="${esc(id)}">${row?.owned||0} owned · ${esc(title(row?.rarity||'common'))} · ${row?.value||0} value each${row?.element?` · ${ELEMENTS[row.element]?.name||''}`:''}</small></div><label class="forge-quantity-label"><span class="visually-hidden">Quantity of ${esc(row?.name||id)}</span><input type="number" min="1" max="60" step="1" inputmode="numeric" value="${Number.isFinite(quantity)?quantity:''}" data-forge-quantity="${esc(id)}"></label><button type="button" class="icon-button" data-action="forge-remove" data-id="${esc(id)}" aria-label="Remove ${esc(row?.name||id)}">×</button></div>`;
  }).join('')||'<p class="forge-empty">Choose ingredients to see your odds. Try fruit with your spare essence.</p>';
}
export function renderForgePreview(s,mix){
  const preview=previewForge(s,mix);
  const rarity=preview.rarityOdds||[],elements=preview.elementOdds||[];
  return `<div class="forge-cost"><strong>${preview.units||0} / 60 ingredients</strong><span>${preview.value||0} mixing value · 8 needed</span></div>${preview.campBonus?'<p class="forge-camp-bonus">Camp bonus · 20% ingredient discount and improved rare/legendary odds</p>':'<p class="fine">Craft anywhere along the trail. Camps give an ingredient discount and better rare/legendary odds.</p>'}${preview.costSummary?.length?`<p class="forge-spend"><strong>You will spend:</strong> ${preview.costSummary.map(row=>`${row.count} ${esc(row.name)}${row.offeredCount>row.count?` (${row.offeredCount-row.count} saved)`:""}`).join(' · ')}</p>`:''}<h3>Rarity odds</h3><div class="forge-odds" aria-label="Item rarity probabilities">${rarity.map(row=>`<div class="forge-odds-row rarity-${row.id}"><span>${esc(row.name||title(row.id))}</span><strong>${percent(row.chance)}</strong><div class="forge-odds-track"><i style="width:${Math.max(0,Math.min(100,row.chance*100))}%"></i></div></div>`).join('')}</div><h3 class="section-gap">Element odds</h3><p class="fine">Add more of an element’s essence to favor its equipment.</p><div class="forge-elements">${elements.map(row=>`<span><b>${ELEMENTS[row.id]?.icon||'✧'} ${esc(row.name||ELEMENTS[row.id]?.name||row.id)}</b> ${percent(row.chance)}</span>`).join('')}</div><button type="button" class="text-button wide" data-action="forge-odds" ${preview.outcomes?.length?'':'disabled'}>View every item’s exact odds & effects</button><p class="fine" data-forge-reason>${esc(preview.canCraft?'Ready: one guaranteed equipment roll.':preview.reason||'Add ingredients to begin.')}</p>`;
}

export function renderForgeResult(s,p,{unsaved=false}={}){
  const receipt=s.meta.forge?.last;if(!receipt?.itemId||!GEAR[receipt.itemId])return '';
  const g=gearData(receipt.itemId),tier=ownedTier(s,receipt.itemId)||receipt.tier||1,wearer=owner(s,receipt.itemId),on=wearer?.id===p.id;
  return `<article class="forge-result rarity-${g.rarity||'common'}" aria-label="Last crafting result"><span class="eyebrow">${unsaved?'SAVE PENDING':'LAST CRAFT · SAVED'}</span><div class="equipment-slot-heading">${gearIcon(receipt.itemId)}<div><h3>${esc(g.name)}</h3><small>${esc(gearMeta(g))} · Rank ${tier}</small></div></div><p>${receipt.kind==='salvaged'?`Already at maximum rank. ${esc(receipt.salvage?`${receipt.salvage.count} ${receipt.salvage.name} returned (${receipt.salvage.value} forge value).`:'The duplicate was converted into crafting materials.')}`:receipt.kind==='upgraded'?`Duplicate combined: your item reached rank ${receipt.tier}.`:'New equipment discovered. It stays in your permanent inventory.'}</p>${receipt.costSummary?.length?`<p class="fine">Paid: ${receipt.costSummary.map(row=>`${row.count} ${esc(row.name)}`).join(' · ')}${receipt.campBonus?' · Camp bonus applied':''}</p>`:''}${effectList(receipt.itemId,tier)}${wearer?`<p class="fine">Equipped by ${esc(monkeyName(wearer))}</p>`:''}${receipt.kind!=='salvaged'?`<button type="button" class="wide" data-action="equip" data-id="${receipt.itemId}" data-monkey="${esc(p.id)}" ${!canEquip(s)||on||unsaved?'disabled':''}>${on?'Equipped':wearer?`Move to ${esc(monkeyName(p))}`:`Equip to ${esc(monkeyName(p))}`}</button>`:''}${unsaved?'<p class="notice">Your result is held on this page. Retry saving before another craft.</p>':''}</article>`;
}

export function renderForge(s,mix,{busy=false,unsaved=false}={}){
  const preview=previewForge(s,mix);
  return `<section class="panel forge-panel" data-forge-panel><span class="eyebrow">CANOPY FORGE</span><h2>Mix, craft, discover.</h2><p class="muted">Mix spare resources for one random equipment item anywhere on the trail. Rarer ingredients improve rarity odds. Essence favors its element. Camps offer cheaper crafting and better rarity odds.</p><details class="forge-help"><summary>How the forge works</summary><p>Choose up to 60 ingredients with at least 8 mixing value. Check the percentages before spending. At camp, ingredient costs get a 20% discount (each quantity rounds up), and rare/legendary odds improve. A duplicate upgrades the item you already own, up to rank 12; a maximum-rank duplicate returns salvage. One item can be equipped by one monkey at a time.</p><p>Ingredients are consumed. Fruit and essence are also used for monkey upgrades; supplies and species bond materials keep their other uses.</p></details><div class="forge-add-row"><label>Ingredient<select id="forge-resource">${renderIngredientOptions(s,mix)}</select></label><label>Amount<input type="number" id="forge-amount" min="1" max="60" step="1" inputmode="numeric" value="1"></label><button type="button" data-action="forge-add">Add</button></div><div data-forge-mix>${renderMix(s,mix)}</div><button type="button" class="text-button" data-action="forge-clear" ${Object.keys(mix).length?'':'disabled'}>Clear mix</button><div data-forge-preview aria-live="polite">${renderForgePreview(s,mix)}</div><button type="button" class="primary wide" data-action="forge-roll" ${busy||(!unsaved&&!preview.canCraft)?'disabled':''}>${busy?'Saving crafted item…':unsaved?'Retry saving crafted item':'Craft 1 random item · spend mix'}</button></section>`;
}

export function renderForgeOdds(s,mix,{rarity='all',element='all',page=0}={}){
  const preview=previewForge(s,mix),all=preview.outcomes||[];
  const filtered=all.filter(row=>(rarity==='all'||gearData(row.id).rarity===rarity)&&(element==='all'||gearData(row.id).element===element)).sort((a,b)=>b.chance-a.chance||GEAR[a.id].name.localeCompare(GEAR[b.id].name));
  const pages=Math.max(1,Math.ceil(filtered.length/8));page=Math.max(0,Math.min(page,pages-1));
  return `<section class="forge-item-odds"><span class="eyebrow">YOUR CURRENT MIX</span><h2>Every possible item</h2><p class="muted">Each percentage is for one craft. ${all.length} items share a total 100% chance. Effects shown at rank 1; owned duplicates gain one rank.</p><div class="forge-filter-row"><label>Element<select data-odds-filter="element"><option value="all">All elements</option>${Object.entries(ELEMENTS).map(([id,e])=>`<option value="${id}" ${element===id?'selected':''}>${e.name}</option>`).join('')}</select></label><label>Rarity<select data-odds-filter="rarity"><option value="all">All rarities</option>${[...new Set(all.map(row=>gearData(row.id).rarity||'common'))].map(id=>`<option value="${id}" ${rarity===id?'selected':''}>${title(id)}</option>`).join('')}</select></label></div><div class="forge-outcomes">${filtered.slice(page*8,page*8+8).map(row=>{const g=gearData(row.id);return `<article class="forge-outcome rarity-${g.rarity||'common'}"><div class="equipment-slot-heading">${gearIcon(row.id)}<div><h3>${esc(g.name)}</h3><small>${esc(gearMeta(g))}</small></div><strong>${percent(row.chance)}</strong></div>${effectList(row.id,1)}</article>`;}).join('')||'<p class="muted">No possible items match these filters.</p>'}</div><div class="forge-pagination"><button data-action="forge-odds-page" data-id="-1" ${page===0?'disabled':''}>Previous</button><span>${page+1} / ${pages} · ${filtered.length} items</span><button data-action="forge-odds-page" data-id="1" ${page===pages-1?'disabled':''}>Next</button></div><button type="button" data-action="close-dialog" class="wide">Back to mix</button></section>`;
}
