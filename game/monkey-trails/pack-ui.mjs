import * as C from './core.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const bonuses=['trailRibbon','treasureScent','scoutFeather','campfireSeed'];
const recipeIds=['berry','revive','bondCharm'];
const companionName=p=>p?.nickname||C.SPECIES[p?.species]?.name||'Companion';

export function renderPackNav(section){
  return `<div class="pack-tabs pack-navigation" role="tablist" aria-label="Pack sections">${[['supplies','Supplies'],['equipment','Equipment'],['forge','Forge'],['materials','Materials']].map(([id,label])=>`<button type="button" role="tab" id="pack-tab-${id}" data-action="pack-section" data-id="${id}" aria-selected="${section===id}" aria-controls="pack-content" tabindex="${section===id?'0':'-1'}">${label}</button>`).join('')}</div>`;
}

// Match the use restrictions without spending supplies or changing a saved state.
export function supplyAvailability(s,id,targetId){
  const r=s?.run,p=s?.collection?.[targetId];
  const unavailable=reason=>({ok:false,reason});
  if(id==='bondCharm')return unavailable('Use during befriending after a battle.');
  if(!r||r.phase==='dead')return unavailable('Begin a new expedition to use supplies.');
  if(bonuses.includes(id)){
    if(!['trail','camp'].includes(r.phase))return unavailable('Activate on the trail or at camp, between encounters.');
    return r.items[id]>0?{ok:true,reason:''}:unavailable('Find more along the trail.');
  }
  if(['replacement','befriend'].includes(r.phase))return unavailable('Finish choosing your companion first.');
  if(r.phase==='battle'){
    if(!r.battle?.ready)return unavailable('Stop safely and begin the encounter first.');
    if(r.battle.itemUsedRound===r.battle.round)return unavailable('One supply per round. Choose an ability or attack next.');
  }
  if(!(r.items[id]>0))return unavailable(recipeIds.includes(id)?'None owned · craft one below.':'Find more along the trail.');
  if(!p||!r.party.includes(p.id))return unavailable('Choose an expedition teammate.');
  if(id==='revive')return p.hp>0?unavailable('Choose a knocked-out teammate.'): {ok:true,reason:''};
  if(p.hp<=0)return unavailable('Use a Revival Seed on this teammate first.');
  const stats=C.stats(p,C.equipped(s,p));
  if(id==='berry'&&p.hp>=stats.hp)return unavailable('This teammate is already at full health.');
  if(id==='tonic'&&p.energy>=stats.maxEnergy)return unavailable('This teammate is already at full energy.');
  if(id==='salve'&&!Object.keys(p.status||{}).length&&p.hp>=stats.hp)return unavailable('This teammate has full health and no effects to clear.');
  return ['berry','tonic','salve'].includes(id)?{ok:true,reason:''}:unavailable('This supply cannot be used here.');
}

export function renderRecipe(s,id,essence='leaf',{pickupIcon=()=>''}={}){
  if(!Object.hasOwn(C.CONSUMABLE_RECIPES,id))return '';
  const recipe=C.CONSUMABLE_RECIPES[id],r=s.run,validEssence=Object.hasOwn(C.ELEMENTS,essence),element=validEssence?C.ELEMENTS[essence]:null,available=C.consumableCraftAvailability(s,id,essence);
  const ingredient=(key,label,owned,required,icon)=>`<li data-recipe-ingredient="${key}" data-owned="${owned}" data-required="${required}" class="${owned<required?'ingredient-missing':'ingredient-ready'}"><span class="recipe-ingredient-icon" aria-hidden="true">${icon}</span><span>${esc(label)}<small>${owned} owned · ${required} needed</small></span><strong>${owned<required?`${required-owned} missing`:'Ready'}</strong></li>`;
  const costs=[`${recipe.fruit} fruit`,recipe.material?`${recipe.material} charm material`:null,recipe.essence?`${recipe.essence} ${element?.name||'chosen'} essence`:null].filter(Boolean).join(' + ');
  return `<section class="supply-recipe" id="supply-recipe-${id}" data-supply-recipe="${id}" aria-labelledby="recipe-title-${id}"><div class="supply-recipe-heading"><h4 id="recipe-title-${id}" tabindex="-1">Craft ${esc(C.ITEMS[id].name)}</h4><button type="button" class="text-button" data-action="supply-recipe" data-id="${id}" aria-label="Close ${esc(C.ITEMS[id].name)} recipe">Close recipe</button></div>${recipe.essence?`<label for="consumable-essence">Essence to use<select id="consumable-essence"><option value="" disabled ${validEssence?'':'selected'}>Choose an essence</option>${Object.entries(C.ELEMENTS).map(([key,e])=>`<option value="${key}" ${key===essence?'selected':''}>${esc(e.name)} · ${r.essence[key]||0} owned</option>`).join('')}</select></label>`:''}<ul class="recipe-ingredients">${ingredient('fruit','Fruit',r.fruit,recipe.fruit,pickupIcon('fruit'))}${recipe.material?ingredient('material','Charm material',r.items.charmMaterial||0,recipe.material,pickupIcon('fragment')):''}${recipe.essence?ingredient('essence',`${element?.name||'Chosen'} essence`,validEssence?r.essence[essence]||0:0,recipe.essence,element?.icon||'◇'):''}</ul><div class="supply-recipe-footer"><p id="recipe-reason-${id}" data-consumable-reason class="${available.ok?'recipe-ready':'recipe-blocked'}">${available.ok?'Ready to craft one.':esc(available.reason)}</p><button type="button" class="primary" data-action="craft-consumable" data-id="${id}" aria-describedby="recipe-reason-${id}" ${available.ok?'':'disabled'}>Craft ${esc(C.ITEMS[id].name)}</button></div><p class="recipe-cost fine" data-consumable-cost>Cost: ${esc(costs)}</p></section>`;
}

function supplyCard(s,id,target,{recipeId,essence,pickupIcon}){
  const item=C.ITEMS[id],quantity=s.run.items[id]||0,available=supplyAvailability(s,id,target?.id),craftable=recipeIds.includes(id),expanded=recipeId===id;
  return `<article class="supply-card ${craftable?'supply-card-featured':''} ${expanded?'recipe-open':''}" data-supply-card="${id}"><div class="supply-card-row"><span class="supply-art" aria-hidden="true">${pickupIcon(id)}</span><div class="supply-copy"><h3>${esc(item.name)} <span class="supply-quantity" data-supply-count="${id}" aria-label="${quantity} owned">×${quantity}</span></h3><p>${id==='bondCharm'?'Use one during post-battle befriending to invite a wild companion.':esc(item.desc)}</p>${id!=='bondCharm'?`<small id="supply-reason-${id}" class="supply-use-reason" data-supply-use-reason="${id}">${available.ok?'':esc(available.reason)}</small>`:''}</div><div class="supply-actions">${id!=='bondCharm'?`<button type="button" data-action="item" data-id="${id}" aria-label="${bonuses.includes(id)?'Activate':'Use'} ${esc(item.name)}" aria-describedby="supply-reason-${id}" ${available.ok?'':'disabled'}>${bonuses.includes(id)?'Activate':'Use'}</button>`:''}${craftable?`<button type="button" class="supply-craft" data-action="supply-recipe" data-id="${id}" aria-expanded="${expanded}" aria-controls="supply-recipe-${id}">${expanded?'Recipe open':'Craft'}</button>`:''}</div></div>${expanded?renderRecipe(s,id,essence,{pickupIcon}):craftable?`<div id="supply-recipe-${id}" hidden></div>`:''}</article>`;
}

export function renderSupplies(s,target,{recipeId=null,essence='leaf',pickupIcon=()=>'',name=companionName}={}){
  const party=s.run.party.map(id=>s.collection[id]).filter(Boolean),selected=party.find(p=>p.id===(typeof target==='string'?target:target?.id))||party[0],options={recipeId,essence,pickupIcon};
  return `<section class="panel pack-supplies-panel" aria-labelledby="supplies-title"><div class="pack-supplies-heading"><div><span class="eyebrow">READY FOR THE TRAIL</span><h2 id="supplies-title">Your supplies</h2><p>Care for your crew. Craft what comes next.</p></div><label for="supply-target">Use recovery supplies on<select id="supply-target">${party.map(p=>`<option value="${esc(p.id)}" ${selected?.id===p.id?'selected':''}>${esc(name(p))} · ${Math.round(p.hp)} HP${p.hp<=0?' · knocked out':''}</option>`).join('')}</select></label></div><div class="supply-featured-list">${recipeIds.map(id=>supplyCard(s,id,selected,options)).join('')}</div><div class="supply-group-heading"><h3>More recovery</h3><p>Energy and cleansing for your selected teammate.</p></div><div class="supply-secondary-grid">${['tonic','salve'].map(id=>supplyCard(s,id,selected,options)).join('')}</div><div class="supply-group-heading"><h3>Trail bonuses</h3><p>Activate on the trail or at camp.</p></div><div class="supply-secondary-grid">${bonuses.map(id=>supplyCard(s,id,selected,options)).join('')}</div><p class="pack-save-note">Temporary supplies reset after full-team defeat. Your permanent collection and equipment stay saved.</p></section>`;
}
