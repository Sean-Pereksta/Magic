import { WorldMap } from './map.mjs';
import { strategicLocationInfo, strategicPickerView } from './strategic-locations.mjs';

export function installStrategicMapPicker(doc,{getState,getViewer,getCamera=()=>null,onError=()=>{}}) {
  const dialog=doc.createElement('dialog');dialog.id='strategic-map-picker';dialog.setAttribute('aria-labelledby','strategic-map-title');
  dialog.innerHTML=`<header class="dialog-header"><div><span class="eyebrow">CAMPAIGN MAP</span><h2 id="strategic-map-title">Select location</h2></div><button type="button" data-map-cancel aria-label="Cancel location selection">×</button></header><div class="strategic-map-board"><canvas tabindex="0" aria-label="Choose a hex. Drag to pan, scroll or pinch to zoom, or use arrow keys."></canvas><div class="strategic-map-zoom"><button type="button" data-map-zoom="1.25" aria-label="Zoom in">+</button><button type="button" data-map-zoom="0.8" aria-label="Zoom out">−</button><button type="button" data-map-fit>Full map</button></div></div><footer><div class="strategic-map-info" role="status" aria-live="polite">Choose a hex on the map.</div><div class="button-row"><button type="button" data-map-cancel>Cancel</button><button type="button" class="primary" data-map-confirm disabled>Select Location</button></div></footer>`;
  doc.body.append(dialog);
  const canvas=dialog.querySelector('canvas'),confirm=dialog.querySelector('[data-map-confirm]'),info=dialog.querySelector('.strategic-map-info');
  let view=null,viewer=null,map=null,chosen=null,resolve=null,origin=null;
  function select(id){
    chosen=strategicLocationInfo(view,viewer,id);if(!chosen)return;
    map.selected=id;map.draw();confirm.disabled=false;
    const owner=chosen.knownOwner&&view.kingdoms.find(k=>k.id===chosen.knownOwner)?.name;
    info.textContent=[chosen.displayName,chosen.displayName===`Hex ${chosen.q},${chosen.r}`?'':`Hex ${chosen.q},${chosen.r}`,chosen.terrain,owner,chosen.fogState==='explored'?`Last observed T${chosen.lastObservedTurn}`:chosen.fogState==='visible'?'Currently visible':'Information unknown'].filter(Boolean).join(' · ');
  }
  function finish(value){const done=resolve;resolve=null;dialog.close();chosen=null;done?.(value);origin?.focus();}
  dialog.addEventListener('cancel',e=>{e.preventDefault();e.stopPropagation();finish(null);});
  dialog.addEventListener('close',()=>{if(resolve)finish(null);});
  dialog.addEventListener('keydown',e=>{if(e.key==='Escape')e.stopPropagation();});
  dialog.addEventListener('click',e=>{
    const b=e.target.closest('button');if(!b)return;
    if(b.hasAttribute('data-map-cancel'))finish(null);
    if(b.hasAttribute('data-map-confirm')&&chosen)finish({...chosen});
    if(b.dataset.mapZoom)map.setZoom(map.zoom*Number(b.dataset.mapZoom));
    if(b.hasAttribute('data-map-fit'))map.fit();
  });
  function openStrategicMapPicker({initialTile=null,title='Select location'}={}) {
    if(resolve)finish(null);
    origin=doc.activeElement;viewer=getViewer();view=strategicPickerView(getState(),viewer);chosen=null;confirm.disabled=true;info.textContent='Choose a hex on the map.';dialog.querySelector('h2').textContent=title;
    dialog.showModal();
    if(!map)map=new WorldMap(canvas,{getState:()=>view,onSelect:select,selectionOnly:true});
    map.pointers.clear();map.drag=null;map.moved=false;map.hovered=null;map.selected=null;map.resize();
    const camera=getCamera();if(camera){map.x=camera.x;map.y=camera.y;map.zoom=camera.zoom;}else map.fit();
    if(initialTile&&view.tiles[initialTile]){select(initialTile);map.center(initialTile);}else map.draw();
    canvas.focus();return new Promise(done=>{resolve=done;});
  }
  // The same control works inside any form or modal, including multi-target
  // general orders. Cancelling never mutates the select or the surrounding draft.
  doc.addEventListener('click',async e=>{
    const button=e.target.closest('[data-select-map]');if(!button||button.disabled)return;
    e.preventDefault();const selectEl=button.closest('label')?.querySelector('select');if(!selectEl)return;
    const selected=await openStrategicMapPicker({initialTile:selectEl.value,title:button.dataset.selectMap||'Select location'});if(!selected||!selectEl.isConnected)return;
    let option=[...selectEl.options].find(o=>o.value===selected.tileId);
    if(selectEl.multiple&&!option?.selected&&selectEl.selectedOptions.length>=3){onError('Choose up to three locations. Deselect a location before adding another.');return;}
    if(!option){option=doc.createElement('option');option.value=selected.tileId;selectEl.append(option);}
    option.textContent=selected.displayName.includes(selected.tileId)?selected.displayName:`${selected.displayName} · Hex ${selected.tileId}`;
    if(!selectEl.multiple)selectEl.value=selected.tileId;option.selected=true;button.textContent=selectEl.multiple?'Add Location on Map':'Change Location';selectEl.dispatchEvent(new Event('change',{bubbles:true}));
  });
  return {openStrategicMapPicker,dialog};
}
