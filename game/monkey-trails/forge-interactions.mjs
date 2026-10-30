// Furnace reservations are UI state only. Inventory is spent by craftForge,
// after the player has reviewed the exact cost and presses Smelt.
import {forgeInventory,FORGE_LIMITS} from './forge.mjs';
import {ELEMENTS} from './data.mjs';
import {forgeResourceArt,renderMix,renderForgeInventory,renderForgePreview,cachedForgePreview,forgeViewSignature} from './forge-ui.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const total=mix=>Object.values(mix||{}).reduce((sum,count)=>sum+(Number.isSafeInteger(count)&&count>0?count:0),0);
export function forgeQuantityBounds(s,mix,id,{edit=false}={}){
  const row=forgeInventory(s).find(row=>row.id===id);if(!row)throw Error('This ingredient is no longer in your pack.');
  const selected=Number.isSafeInteger(mix?.[id])?mix[id]:0,available=Math.max(0,row.owned-(edit?0:selected)),room=Math.max(0,FORGE_LIMITS.maximumUnits-total(mix)+(edit?selected:0));
  return {row,selected,available,room,max:Math.min(available,room)};
}
export function chooseForgeQuantity(s,mix,id,amount,{edit=false}={}){
  const bounds=forgeQuantityBounds(s,mix,id,{edit});
  if(!Number.isSafeInteger(amount)||amount<1)throw Error('Choose a whole quantity of at least one.');
  if(amount>bounds.available)throw Error(`Only ${bounds.available} ${bounds.row.name.toLowerCase()} available.`);
  if(amount>bounds.room)throw Error(`Use at most ${FORGE_LIMITS.maximumUnits} ingredients per smelt.`);
  return {...mix,[id]:(edit?0:bounds.selected)+amount};
}

export function renderForgeQuantityPicker(s,mix,id,{edit=false}={}){
  const bounds=forgeQuantityBounds(s,mix,id,{edit}),quantity=Math.max(1,Math.min(bounds.max,edit?bounds.selected:1)),row=bounds.row;
  return `<section class="forge-quantity-dialog" data-forge-quantity-dialog data-id="${esc(id)}" data-edit="${edit}"><span class="eyebrow">${edit?'ADJUST YOUR MIX':'ADD TO YOUR MIX'}</span><div class="forge-quantity-hero rarity-${esc(row.rarity)}">${forgeResourceArt(row)}</div><h2>${esc(row.name)}</h2><p class="muted">Available: <strong>${bounds.available}</strong>${bounds.max<bounds.available?` · ${bounds.max} fit in this furnace`:''}</p><label class="forge-quantity-readout" for="forge-picker-number">Quantity ${edit?'in furnace':'to add'}<span class="forge-quantity-stepper"><button type="button" data-forge-picker-step="-1" aria-label="Decrease quantity" ${quantity<=1?'disabled':''}>−</button><input id="forge-picker-number" type="number" min="1" max="${bounds.max}" step="1" value="${quantity}" inputmode="numeric" data-forge-picker-number aria-label="Quantity of ${esc(row.name)}"><button type="button" data-forge-picker-step="1" aria-label="Increase quantity" ${quantity>=bounds.max?'disabled':''}>+</button></span></label><input type="range" min="1" max="${bounds.max}" step="1" value="${quantity}" data-forge-picker-range aria-label="Quantity slider for ${esc(row.name)}"><div class="forge-range-labels"><span>1</span><span>${bounds.max}</span></div><p class="fine">${row.value} crafting power each · <strong data-forge-picker-power>${quantity*row.value}</strong> power selected. Your pack is only charged when you Smelt.${row.kind==='equipment'?' This exact equipment copy will be destroyed and its crafting power added to the mix.':''}</p><div class="button-row"><button type="button" class="primary" data-forge-picker-confirm>${edit?'Update Furnace':'Add to Furnace'}</button><button type="button" data-forge-picker-cancel>Cancel</button></div>${edit?`<button type="button" class="text-button wide" data-forge-picker-remove>Remove from furnace</button>`:''}</section>`;
}

export function attachForgeInteractions({getState,getMix,setMix,isLocked=()=>false,getStatus=()=>({busy:isLocked(),unsaved:false}),onChange=()=>{},modal,closeModal,toast=()=>{},root=globalThis.document}={}){
  if(!root||!getState||!getMix||!setMix||!modal||!closeModal)throw Error('The forge needs its game state and dialog controls.');
  let filter='all',search='',favorites=new Set(),lastPanel=null,lastSignature='',picker=null,drag=null,touch=null,suppressClickUntil=0,suppressClickSource=null,feedbackTimer=0,destroyed=false;
  const scrollMemory=new Map(),detailMemory=new Map(),scrollSelectors=['.forge-inventory-scroll','[data-forge-mix]','[data-forge-preview]'];
  try{const saved=JSON.parse(globalThis.localStorage?.getItem('monkey-trails.forge-favorites')||'[]');if(Array.isArray(saved))favorites=new Set(saved.filter(id=>typeof id==='string').slice(0,500));}catch{}
  const preferences=()=>({filter,search,favorites:[...favorites]});
  const panel=()=>root.querySelector('[data-forge-panel]');
  const locked=()=>Boolean(isLocked());
  function notify(message){toast(message);}
  function safe(action){try{return action();}catch(error){notify(error.message||'Try choosing your ingredient again.');}}
  const detailKey=(node,index)=>node.dataset?.forgeDetailsKey||node.id||node.className||`details-${index}`;
  function rememberView(current){
    if(!current)return;
    // Detached nodes can report zero offsets after an app rerender; retain the
    // offsets captured by the scrolling event until their replacements exist.
    if(current.isConnected!==false)for(const selector of scrollSelectors){const node=current.querySelector(selector);if(node)scrollMemory.set(selector,{top:node.scrollTop,left:node.scrollLeft});}
    current.querySelectorAll('details').forEach((node,index)=>detailMemory.set(detailKey(node,index),node.open));
  }
  function restoreView(current){
    for(const selector of scrollSelectors){const node=current.querySelector(selector),position=scrollMemory.get(selector);if(node&&position){node.scrollTop=position.top;node.scrollLeft=position.left;}}
    current.querySelectorAll('details').forEach((node,index)=>{const key=detailKey(node,index);if(detailMemory.has(key))node.open=detailMemory.get(key);});
  }
  function updateGrid(current=panel()){
    if(!current||!getState())return;const grid=current.querySelector('[data-forge-inventory]'),scroll=current.querySelector('.forge-inventory-scroll'),position=scroll?.scrollTop||0;
    if(grid)grid.innerHTML=renderForgeInventory(getState(),getMix(),preferences());
    if(scroll)scroll.scrollTop=position;
    current.querySelectorAll('[data-forge-filter]').forEach(button=>{const active=button.dataset.forgeFilter===filter;button.classList.toggle('active',active);button.setAttribute('aria-pressed',String(active));});
    const input=current.querySelector('[data-forge-search]');if(input&&input.value!==search)input.value=search;
    const resources=new Map(forgeInventory(getState()).map(row=>[row.id,row])),mix=getMix();
    current.querySelectorAll('[data-forge-item]').forEach(button=>{button.disabled=locked()||(resources.get(button.dataset.forgeItem)?.owned||0)<=(mix[button.dataset.forgeItem]||0);button.draggable=!button.disabled;});
  }
  function refresh({force=false}={}){
    if(destroyed)return;const s=getState(),current=panel();if(!s||!current){rememberView(lastPanel);lastPanel=null;clearDrag();clearTouch();return;}
    const mix=getMix(),status=getStatus(),viewSignature=forgeViewSignature(s,mix),signature=`${viewSignature}|${Boolean(status.busy)}|${Boolean(status.unsaved)}|${locked()}`;
    if(!force&&current===lastPanel&&signature===lastSignature)return;
    if(lastPanel)rememberView(lastPanel);
    if(current!==lastPanel||signature!==lastSignature){clearDrag();clearTouch();}
    lastPanel=current;lastSignature=signature;current.dataset.forgeLocked=String(locked());
    const preview=cachedForgePreview(s,mix),mixNode=current.querySelector('[data-forge-mix]'),previewNode=current.querySelector('[data-forge-preview]');
    if(mixNode)mixNode.innerHTML=renderMix(s,mix);if(previewNode)previewNode.innerHTML=renderForgePreview(s,mix,preview);updateGrid(current);restoreView(current);
    const furnace=current.querySelector('[data-forge-drop]');
    // Use engine weights, including its split contribution for dual-type bond
    // materials, so the furnace glow agrees with the actual elemental influence.
    const element=Object.entries(preview.weights||{}).filter(([id,weight])=>ELEMENTS[id]&&weight>2).sort((a,b)=>b[1]-a[1])[0]?.[0]||'neutral';
    if(furnace){furnace.dataset.forgeElement=element;furnace.classList.toggle('has-ingredients',preview.units>0);furnace.style.setProperty('--forge-flame',ELEMENTS[element]?.color||'#f4b76b');furnace.classList.toggle('is-smelting',Boolean(status.busy));}
    const button=current.querySelector('[data-action="forge-roll"]');if(button){button.disabled=Boolean(status.busy)||(!status.unsaved&&!preview.canCraft);button.innerHTML=`<span aria-hidden="true">♨</span> ${status.busy?'Saving your equipment…':status.unsaved?'Retry saving equipment':'SMELT'}<small>${status.busy?'Your equipment is held safely':status.unsaved?'Finish saving before mixing again':(preview.canCraft?'Create one equipment item':preview.value<8?'8 power needed':'Finish encounter to smelt')}</small>`;}
    current.querySelectorAll('[data-action="forge-clear"],[data-action="forge-remove"],[data-forge-edit]').forEach(button=>{button.disabled=locked()||(button.dataset.action==='forge-clear'&&!Object.keys(mix).length);});
  }
  function commit(next,{pulse=false}={}){
    if(locked())throw Error('Finish saving your equipment before changing this mix.');
    setMix(next);refresh({force:true});onChange();
    if(pulse){const furnace=panel()?.querySelector('[data-forge-drop]');if(furnace){furnace.classList.remove('forge-added');void furnace.offsetWidth;furnace.classList.add('forge-added');globalThis.clearTimeout(feedbackTimer);feedbackTimer=globalThis.setTimeout(()=>furnace.classList.remove('forge-added'),450);}}
  }
  function remove(id){const next={...getMix()};delete next[id];commit(next);}
  function focusIngredient(id,{inFurnace=false}={}){
    const key=inFurnace?'forgeEdit':'forgeItem',button=[...root.querySelectorAll(inFurnace?'[data-forge-edit]':'[data-forge-item]')].find(button=>button.dataset[key]===id);button?.focus({preventScroll:true});
    // A compact furnace may use one horizontally scrolling ingredient row.
    // Reveal the newly selected stack inside that row without moving the pack.
    const container=inFurnace?panel()?.querySelector('[data-forge-mix]'):null;
    if(button&&container){const item=button.getBoundingClientRect(),bounds=container.getBoundingClientRect();container.scrollLeft+=item.right>bounds.right?item.right-bounds.right+4:item.left<bounds.left?item.left-bounds.left-4:0;container.scrollTop+=item.bottom>bounds.bottom?item.bottom-bounds.bottom+4:item.top<bounds.top?item.top-bounds.top-4:0;}
  }
  function openQuantity(id,edit=false){
    if(locked())return;const bounds=forgeQuantityBounds(getState(),getMix(),id,{edit});
    if(!bounds.max){notify(bounds.room?'That stack is already in the furnace. Tap it there to adjust.':'The furnace holds 60 ingredients. Remove a stack first.');return;}
    picker={id,edit};modal(renderForgeQuantityPicker(getState(),getMix(),id,{edit}),`${bounds.row.name} quantity`,'forge-dialog forge-quantity-modal');
  }
  function pickerDialog(){
    const current=root.querySelector('[data-forge-quantity-dialog]'),host=current?.closest('dialog');
    if(!picker||!current||current.dataset.id!==picker.id||host&&!host.open){picker=null;return null;}return current;
  }
  function syncPicker(value){
    const dialog=pickerDialog();if(!dialog)return;
    const number=dialog.querySelector('[data-forge-picker-number]'),range=dialog.querySelector('[data-forge-picker-range]'),max=Number(range.max),amount=Math.max(1,Math.min(max,Math.round(Number(value)||1)));
    number.value=String(amount);range.value=String(amount);dialog.querySelector('[data-forge-picker-step="-1"]').disabled=amount<=1;dialog.querySelector('[data-forge-picker-step="1"]').disabled=amount>=max;
    const row=forgeQuantityBounds(getState(),getMix(),picker.id,{edit:picker.edit}).row;dialog.querySelector('[data-forge-picker-power]').textContent=String(amount*row.value);
  }
  function onClick(event){
    const target=event.target.closest?.('[data-forge-item],[data-forge-edit],[data-forge-filter],[data-forge-favorite],[data-forge-picker-step],[data-forge-picker-confirm],[data-forge-picker-cancel],[data-forge-picker-remove]');if(!target)return;
    event.preventDefault();event.stopPropagation();if(Date.now()<suppressClickUntil&&(target===suppressClickSource||suppressClickSource?.contains?.(target)))return;
    safe(()=>{
      if(target.hasAttribute('data-forge-item'))return openQuantity(target.dataset.forgeItem);
      if(target.hasAttribute('data-forge-edit'))return openQuantity(target.dataset.forgeEdit,true);
      if(target.hasAttribute('data-forge-filter')){filter=target.dataset.forgeFilter;refresh({force:true});return;}
      if(target.hasAttribute('data-forge-favorite')){const id=target.dataset.forgeFavorite;if(favorites.has(id))favorites.delete(id);else favorites.add(id);try{globalThis.localStorage?.setItem('monkey-trails.forge-favorites',JSON.stringify([...favorites]));}catch{}updateGrid();[...root.querySelectorAll('[data-forge-favorite]')].find(button=>button.dataset.forgeFavorite===id)?.focus({preventScroll:true});return;}
      if(target.hasAttribute('data-forge-picker-step')){const number=pickerDialog()?.querySelector('[data-forge-picker-number]');if(number)syncPicker(Number(number.value)+Number(target.dataset.forgePickerStep));return;}
      if(target.hasAttribute('data-forge-picker-cancel')){picker=null;closeModal();return;}
      if(target.hasAttribute('data-forge-picker-remove')){const id=pickerDialog()?picker?.id:null;if(id)remove(id);picker=null;closeModal();if(id)focusIngredient(id);return;}
      if(target.hasAttribute('data-forge-picker-confirm')){const current=pickerDialog();if(!current)return;const {id,edit}=picker,amount=Number(current.querySelector('[data-forge-picker-number]')?.value),next=chooseForgeQuantity(getState(),getMix(),id,amount,{edit});commit(next,{pulse:true});picker=null;closeModal();focusIngredient(id,{inFurnace:true});}
    });
  }
  function onInput(event){
    if(event.target.matches?.('[data-forge-search]')){search=String(event.target.value).slice(0,100);updateGrid();return;}
    if(event.target.matches?.('[data-forge-picker-range]'))safe(()=>syncPicker(event.target.value));
    if(event.target.matches?.('[data-forge-picker-number]')&&event.target.value!==''&&Number(event.target.value)>=1)safe(()=>syncPicker(event.target.value));
  }
  function findDrop(x,y){return root.elementFromPoint?.(x,y)?.closest?.('[data-forge-drop]')||null;}
  function finishDrop(id,origin,destination){
    if(locked())return;if(destination){if(origin!=='mix')openQuantity(id);}else if(origin==='mix')remove(id);
  }
  function dragFeedback(over){panel()?.querySelector('[data-forge-drop]')?.classList.toggle('is-drag-over',Boolean(over));}
  function onDragStart(event){
    const source=event.target.closest?.('[data-forge-drag]');if(!source)return;if(locked()||touch?.active){event.preventDefault();return;}
    drag={id:source.dataset.forgeDrag,origin:source.dataset.forgeOrigin||'inventory',source};source.classList.add('is-dragging');
    if(event.dataTransfer){event.dataTransfer.setData('application/x-monkey-trails-forge',drag.id);event.dataTransfer.setData('text/plain',drag.id);event.dataTransfer.effectAllowed='move';}
  }
  function onDragOver(event){if(!drag)return;event.preventDefault();if(event.dataTransfer)event.dataTransfer.dropEffect='move';dragFeedback(event.target.closest?.('[data-forge-drop]'));}
  function clearDrag(){drag?.source?.classList.remove('is-dragging');drag=null;dragFeedback(false);}
  function onDrop(event){if(!drag)return;event.preventDefault();const current=drag,destination=event.target.closest?.('[data-forge-drop]');clearDrag();safe(()=>finishDrop(current.id,current.origin,destination));}
  function clearTouch(){if(touch){globalThis.clearTimeout(touch.timer);touch.source?.classList.remove('is-dragging','is-hold-ready');touch.ghost?.remove();}touch=null;dragFeedback(false);}
  function startTouch(event){
    const source=event.target.closest?.('[data-forge-drag]');if(!source||locked()||event.touches.length!==1){clearTouch();return;}
    clearTouch();
    const point=event.touches[0];touch={id:source.dataset.forgeDrag,origin:source.dataset.forgeOrigin||'inventory',source,identifier:point.identifier,startX:point.clientX,startY:point.clientY,x:point.clientX,y:point.clientY,active:false};
    touch.timer=globalThis.setTimeout(()=>{if(!touch||locked()||!touch.source.isConnected){clearTouch();return;}touch.active=true;touch.source.classList.add('is-dragging','is-hold-ready');const ghost=touch.source.cloneNode(true);ghost.removeAttribute('id');ghost.removeAttribute('draggable');ghost.setAttribute('aria-hidden','true');ghost.classList.add('forge-drag-ghost');root.body.append(ghost);touch.ghost=ghost;moveGhost(touch.x,touch.y);globalThis.navigator?.vibrate?.(12);},340);
  }
  function moveGhost(x,y){if(!touch?.ghost)return;touch.ghost.style.transform=`translate3d(${x-37}px,${y-42}px,0)`;dragFeedback(findDrop(x,y));}
  function moveTouch(event){
    if(!touch)return;const point=[...event.touches].find(point=>point.identifier===touch.identifier);if(!point||event.touches.length!==1){clearTouch();return;}
    touch.x=point.clientX;touch.y=point.clientY;
    if(!touch.active){if(Math.hypot(point.clientX-touch.startX,point.clientY-touch.startY)>10)clearTouch();return;}
    event.preventDefault();moveGhost(point.clientX,point.clientY);
  }
  function endTouch(event){
    if(!touch)return;const current=touch,point=[...event.changedTouches].find(point=>point.identifier===current.identifier);if(!point)return;
    const destination=findDrop(point.clientX,point.clientY);if(current.active){event.preventDefault();suppressClickUntil=Date.now()+700;suppressClickSource=current.source;}clearTouch();if(current.active)safe(()=>finishDrop(current.id,current.origin,destination));
  }
  function dismissDialog(event){if(event.target.querySelector?.('[data-forge-quantity-dialog]'))picker=null;}
  function preventTouchMenu(event){if(touch?.active&&event.target.closest?.('[data-forge-drag]'))event.preventDefault();}
  function rememberScroll(event){if(!event.target.closest?.('[data-forge-panel]'))return;for(const selector of scrollSelectors)if(event.target.matches?.(selector))scrollMemory.set(selector,{top:event.target.scrollTop,left:event.target.scrollLeft});}
  function rememberDetails(event){if(event.target.matches?.('details')&&event.target.closest?.('[data-forge-panel]'))rememberView(panel());}
  const listeners=[['click',onClick,true],['input',onInput,false],['close',dismissDialog,true],['cancel',dismissDialog,true],['scroll',rememberScroll,true],['toggle',rememberDetails,true],['contextmenu',preventTouchMenu,false],['dragstart',onDragStart,false],['dragover',onDragOver,false],['drop',onDrop,false],['dragend',clearDrag,false],['touchstart',startTouch,{passive:true}],['touchmove',moveTouch,{passive:false}],['touchend',endTouch,{passive:false}],['touchcancel',clearTouch,{passive:true}]];
  for(const[name,handler,options]of listeners)root.addEventListener(name,handler,options);
  return {refresh,openQuantity,preferences,destroy(){destroyed=true;clearDrag();clearTouch();globalThis.clearTimeout(feedbackTimer);for(const[name,handler,options]of listeners)root.removeEventListener(name,handler,options);}};
}
