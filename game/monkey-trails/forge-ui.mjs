import {GEAR,ELEMENTS,SPECIES} from './data.mjs';
import {forgeInventory,previewForge} from './forge.mjs';
import {LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';
import {equipmentSummary} from './equipment-effects.mjs';
import {equipmentIconSvg} from './equipment-icons.mjs';
import {equipmentSlots,equipmentLoadout,ownedEquipmentCopies,equipmentCopy,canChangeEquipment} from './equipment-inventory.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const gearData=id=>({...LEGACY_GEAR_AFFINITIES[id],...GEAR[id]});
const slots={weapon:'Slot 1',armor:'Slot 2',charm:'Slot 3',ultra:'Ultra Slot'};
const rarityOrder={common:0,uncommon:1,rare:2,epic:3,legendary:4,mythic:5};
const monkeyName=p=>p?.nickname||SPECIES[p?.species]?.name||'Companion';
const title=s=>s?String(s)[0].toUpperCase()+String(s).slice(1):'';
// The forge draws from a million tickets; four percentage decimals retain every ticket.
export const percent=n=>`${Number((Number(n)*100).toFixed(4))}%`;
const canEquip=canChangeEquipment;
const ownedTier=(s,id)=>s.meta.gear[id]||0;
const owner=(s,id)=>Object.values(s.collection).find(p=>Object.values(p.equipment||{}).includes(id));
const gearEffects=(id,tier)=>equipmentSummary({[id]:tier});

export function gearIcon(id){
  return `<span class="forge-gear-icon">${equipmentIconSvg(id)}</span>`;
}
function gearMeta(g){return `${title(g.rarity||'common')} · ${ELEMENTS[g.element]?.name||'Neutral'}`;}
function effectList(id,tier){return `<ul class="gear-effects">${gearEffects(id,tier).map(text=>`<li>${esc(text)}</li>`).join('')}</ul>`;}

export function renderEquipmentSlots(s,p){
  const loadout=equipmentLoadout(s,p);
  return `<div class="equipment-slots">${equipmentSlots(p).map(slot=>{const label=slots[slot],id=p.equipment?.[slot],g=GEAR[id]&&gearData(id),tier=loadout[id];
    return `<article class="equipment-slot ${slot==='ultra'?'ultra-slot':''}"><span class="equipment-slot-label">${slot==='ultra'?'✺ ':''}${label}</span><div class="equipment-slot-heading">${gearIcon(id||({weapon:'fang',armor:'shell',charm:'heart',ultra:'wind'}[slot]))}<div><h3>${g&&tier?esc(g.name):'Empty slot'}</h3><small>${g&&tier?`${esc(gearMeta(g))} · Rank ${tier}`:'Accepts any owned equipment'}</small></div></div>${g&&tier?effectList(id,tier):''}<div class="button-row"><button type="button" data-action="gear-picker" data-id="${slot}" data-monkey="${esc(p.id)}">${g&&tier?'Change equipment':'Choose equipment'}</button>${g&&tier?`<button type="button" class="text-button" data-action="gear-unequip" data-id="${slot}" data-monkey="${esc(p.id)}" ${canEquip(s)?'':'disabled'}>Remove</button>`:''}</div></article>`;
  }).join('')}</div><p class="fine">One copy of each named item per monkey. Every slot accepts any equipment. Loadouts stay assigned in the sanctuary. Changing equipment never refills current health.</p>`;
}

export function renderGearPicker(s,picker){
  const p=s.collection[picker.monkeyId];if(!p)return '';
  const all=[...new Set(ownedEquipmentCopies(s).map(c=>c.itemId))];
  const filtered=all.filter(id=>(picker.element==='all'||gearData(id).element===picker.element)&&(picker.rarity==='all'||(gearData(id).rarity||'common')===picker.rarity)).sort((a,b)=>(rarityOrder[gearData(b).rarity]||0)-(rarityOrder[gearData(a).rarity]||0)||GEAR[a].name.localeCompare(GEAR[b].name));
  const pages=Math.max(1,Math.ceil(filtered.length/6)),page=Math.max(0,Math.min(pages-1,picker.page||0));
  return `<section class="gear-picker"><span class="eyebrow">${esc(monkeyName(p))} · ${slots[picker.slot]}</span><h2>Choose equipment</h2><p class="muted">${all.length} owned for this slot. Each item shows its effects at its current rank.</p><div class="forge-filter-row"><label>Element<select data-gear-filter="element"><option value="all">All elements</option>${Object.entries(ELEMENTS).map(([id,e])=>`<option value="${id}" ${picker.element===id?'selected':''}>${e.name}</option>`).join('')}</select></label><label>Rarity<select data-gear-filter="rarity"><option value="all">All rarities</option>${Object.keys(rarityOrder).filter(id=>all.some(key=>(gearData(key).rarity||'common')===id)).map(id=>`<option value="${id}" ${picker.rarity===id?'selected':''}>${title(id)}</option>`).join('')}</select></label></div><div class="gear-picker-items">${filtered.slice(page*6,page*6+6).map(id=>{
    const g=gearData(id),tier=ownedTier(s,id),wearer=owner(s,id),on=wearer?.id===p.id;
    return `<article class="gear-choice rarity-${g.rarity||'common'}"><div class="equipment-slot-heading">${gearIcon(id)}<div><h3>${esc(g.name)} <small>Rank ${tier}</small></h3><small>${esc(gearMeta(g))}</small></div></div>${effectList(id,tier)}<p class="fine">${wearer?`Equipped by ${esc(monkeyName(wearer))}`:'Available in your equipment inventory'}</p><button type="button" class="wide" data-action="inventory-item" data-id="${id}" data-monkey="${esc(p.id)}" data-slot="${picker.slot}">Compare & choose copy</button></article>`;
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
  return `<div class="forge-cost"><strong>${preview.units||0} / 60 ingredients</strong><span>${preview.value||0} mixing value · 8 needed</span></div>${preview.campBonus?'<p class="forge-camp-bonus">Camp bonus · 20% ingredient discount and improved higher-rarity odds</p>':'<p class="fine">Craft anywhere outside battle. Camps give an ingredient discount and better higher-rarity odds.</p>'}${preview.costSummary?.length?`<p class="forge-spend"><strong>You will spend:</strong> ${preview.costSummary.map(row=>`${row.count} ${esc(row.name)}${row.offeredCount>row.count?` (${row.offeredCount-row.count} saved)`:""}`).join(' · ')}</p>`:''}<h3>Rarity odds</h3><div class="forge-odds" aria-label="Item rarity probabilities">${rarity.map(row=>`<div class="forge-odds-row rarity-${row.id}"><span>${esc(row.name||title(row.id))}</span><strong>${percent(row.chance)}</strong><div class="forge-odds-track"><i style="width:${Math.max(0,Math.min(100,row.chance*100))}%"></i></div></div>`).join('')}</div><h3 class="section-gap">Element odds</h3><p class="fine">Add more of an element’s essence to favor its equipment.</p><div class="forge-elements">${elements.map(row=>`<span><b>${ELEMENTS[row.id]?.icon||'✧'} ${esc(row.name||ELEMENTS[row.id]?.name||row.id)}</b> ${percent(row.chance)}</span>`).join('')}</div><button type="button" class="text-button wide" data-action="forge-odds" ${preview.outcomes?.length?'':'disabled'}>View Possible Items</button><p class="fine" data-forge-reason>${esc(preview.canCraft?'Ready: one guaranteed equipment roll.':preview.reason||'Add ingredients to begin.')}</p>`;
}

export function renderForgeResult(s,p,{unsaved=false}={}){
  const receipt=s.meta.forge?.last;if(!receipt?.itemId||!GEAR[receipt.itemId])return '';
  const g=gearData(receipt.itemId),copy=receipt.copyId?equipmentCopy(s,receipt.copyId):null,tier=copy?.tier||receipt.tier||1;
  return `<article class="forge-result rarity-${g.rarity||'common'}" aria-label="Last crafting result"><span class="eyebrow">${unsaved?'SAVE PENDING':'LAST CRAFT · SAVED'}</span><div class="equipment-slot-heading">${gearIcon(receipt.itemId)}<div><h3>${esc(g.name)}</h3><small>${esc(gearMeta(g))} · Rank ${tier}</small></div></div><p>${receipt.kind==='salvaged'?`Maximum-rank available duplicate returned ${esc(receipt.salvage?`${receipt.salvage.count} ${receipt.salvage.name}`:'crafting materials')}. Equipped and protected copies were preserved.`:receipt.kind==='upgraded'?`An available, unprotected duplicate combined into rank ${receipt.tier}. Equipped and favorited copies were preserved.`:'Equipment added to your permanent inventory. Extra copies can equip another monkey or combine with an available matching rank to upgrade.'}</p>${receipt.costSummary?.length?`<p class="fine">Paid: ${receipt.costSummary.map(row=>`${row.count} ${esc(row.name)}`).join(' · ')}${receipt.campBonus?' · Camp discount applied':''}</p>`:''}${effectList(receipt.itemId,tier)}<div class="button-row"><button type="button" data-action="inventory-item" data-id="${receipt.itemId}" data-copy="${esc(copy?.id||'')}">View Item</button>${receipt.kind!=='salvaged'?`<button type="button" class="primary" data-action="inventory-item" data-id="${receipt.itemId}" data-copy="${esc(copy?.id||'')}" data-monkey="${esc(p.id)}" data-equip-start="true" ${!canEquip(s)||unsaved?'disabled':''}>Equip</button>`:''}</div>${unsaved?'<p class="notice">Your result is held on this page. Retry saving before another craft.</p>':''}</article>`;
}

export function renderForge(s,mix,{busy=false,unsaved=false}={}){
  const preview=previewForge(s,mix);
  return `<section class="panel forge-panel" data-forge-panel><span class="eyebrow">CANOPY FORGE</span><h2>Mix, craft, discover.</h2><p class="muted">Mix spare resources for one random equipment item anywhere on the trail. Rarer ingredients improve rarity odds. Essence favors its element. Camps offer cheaper crafting and better rarity odds.</p><details class="forge-help"><summary>How the forge works</summary><p>Choose up to 60 ingredients with at least 8 mixing value. Check the percentages before spending. At camp, ingredient costs get a 20% discount (each quantity rounds up), and higher-rarity odds improve. A duplicate upgrades an available, unprotected, unfavorited copy, up to rank 12; an available maximum-rank duplicate returns salvage. If all copies are equipped or protected, you receive a separate copy. Each monkey can equip one copy of a named item.</p><p>Ingredients are consumed. Fruit and essence are also used for monkey upgrades; supplies and species bond materials keep their other uses.</p></details><div class="forge-add-row"><label>Ingredient<select id="forge-resource">${renderIngredientOptions(s,mix)}</select></label><label>Amount<input type="number" id="forge-amount" min="1" max="60" step="1" inputmode="numeric" value="1"></label><button type="button" data-action="forge-add">Add</button></div><div data-forge-mix>${renderMix(s,mix)}</div><button type="button" class="text-button" data-action="forge-clear" ${Object.keys(mix).length?'':'disabled'}>Clear mix</button><div data-forge-preview aria-live="polite">${renderForgePreview(s,mix)}</div><button type="button" class="primary wide" data-action="forge-roll" ${busy||(!unsaved&&!preview.canCraft)?'disabled':''}>${busy?'Saving crafted item…':unsaved?'Retry saving crafted item':'Craft 1 random item · spend mix'}</button></section>`;
}

export function renderForgeOdds(s,mix,{rarity='all',element='all',page=0}={}){
  const preview=previewForge(s,mix),all=preview.outcomes||[];
  const filtered=all.filter(row=>(rarity==='all'||gearData(row.id).rarity===rarity)&&(element==='all'||gearData(row.id).element===element)).sort((a,b)=>b.chance-a.chance||GEAR[a.id].name.localeCompare(GEAR[b.id].name));
  const pages=Math.max(1,Math.ceil(filtered.length/12));page=Math.max(0,Math.min(page,pages-1));
  return `<section class="forge-item-odds"><span class="eyebrow">YOUR CURRENT MIX</span><h2>Possible items</h2><p class="muted">Every percentage is the actual overall chance for one craft. ${all.length} eligible items share 100%. Tap an item for its full effects and acquisition details.</p><div class="forge-filter-row"><label>Affinity<select data-odds-filter="element"><option value="all">All affinities</option>${[...Object.entries(ELEMENTS),['neutral',{name:'Neutral'}]].map(([id,e])=>`<option value="${id}" ${element===id?'selected':''}>${e.name}</option>`).join('')}</select></label><label>Rarity<select data-odds-filter="rarity"><option value="all">All rarities</option>${[...new Set(all.map(row=>gearData(row.id).rarity||'common'))].map(id=>`<option value="${id}" ${rarity===id?'selected':''}>${title(id)}</option>`).join('')}</select></label></div><div class="forge-outcomes compact-outcome-grid">${filtered.slice(page*12,page*12+12).map(row=>{const g=gearData(row.id);return `<article class="forge-outcome rarity-${g.rarity||'common'}"><button type="button" data-action="inventory-item" data-id="${row.id}" data-from-odds="true" aria-label="${esc(g.name)}, ${esc(g.rarity)}, ${percent(row.chance)} overall chance">${gearIcon(row.id)}<span>${esc(g.name)}</span><small>${title(g.rarity)}</small><strong>${percent(row.chance)}</strong></button></article>`;}).join('')||'<p class="muted">No possible items match these filters.</p>'}</div><div class="forge-pagination"><button data-action="forge-odds-page" data-id="-1" ${page===0?'disabled':''}>Previous</button><span>${page+1} / ${pages} · ${filtered.length} items</span><button data-action="forge-odds-page" data-id="1" ${page===pages-1?'disabled':''}>Next</button></div><button type="button" data-action="close-dialog" class="wide">Back to mix</button></section>`;
}
