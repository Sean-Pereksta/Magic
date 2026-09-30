import {UNITS,FORMATIONS} from './data.mjs';
import {sizeOf,strength,mergeArmies} from './core.mjs';
import {armySpeed,formationCheck} from './warfare.mjs';
import {GENERAL_QUALITIES,generalForArmy} from './command-state.mjs';
import {allArmies} from './naval-state.mjs';
import {createArmyDraft,addDraftFormation,moveDraftUnits,applyDraftPreset,validateArmyDraft,reorganizeArmy,armyName} from './army-organization.mjs';
import {esc,generalPortrait,generalSummary} from './general-presentation.mjs';
import {ART,displayArtURL} from './asset-manifest.mjs';
const UI_LIMIT=8;
export function installArmySortUI({getState,getOwner,perform,canAct=()=>true,toast}){
  const dialog=document.createElement('dialog');dialog.id='army-sort';dialog.className='army-sort-dialog';document.body.append(dialog);
  let draft=null,initial=null,selected=null,packet=null,target=1,quick=false,returnFocus=null,selector=null;
  const roster=()=>getState().commanders?.roster.filter(g=>g.owner===getOwner())||[];
  const source=()=>getState().armies.find(a=>a.id===draft?.army);
  function open(id,compact=false){
    draft=createArmyDraft(getState(),getOwner(),id);if(!draft){toast('Select your army first.');return;}
    initial=structuredClone(draft);selected=null;packet=null;selector=null;target=1;quick=compact;returnFocus=document.activeElement;draw();dialog.showModal();
  }
  function refresh(){packet=null;draw();}
  function move(from,to,u,n){const r=moveDraftUnits(draft,from,to,u,n);if(!r.ok){toast(r.error);return;}if(!draft.formations[from].units[u])selected=null;refresh();}
  function renderFormation(f,i){
    const a=source(),g=roster().find(g=>g.id===f.generalId),q=g&&GENERAL_QUALITIES[g.quality];
    const preview={...a,...f,commandBonus:q?.bonus||0,commandMove:g?.specialty==='movement'?(g.quality>=3?2:1):0};
    const troops=sizeOf(f),speed=armySpeed(preview),spent=a.movementTurn===getState().turn?a.movementSpent||0:0;
    return `<section class="sort-formation" data-drop="${i}" aria-label="Army ${String.fromCharCode(65+i)}"><div class="sort-formation-heading"><span class="eyebrow">ARMY ${String.fromCharCode(65+i)}</span><strong data-total="${i}" aria-live="polite">${troops} troops</strong></div><label>Army name<input data-name="${i}" value="${esc(f.name)}" maxlength="60" placeholder="${i===0?esc(armyName(a)):'New formation'}"></label><button class="sort-general" data-pick-general="${i}">${g?`${generalPortrait(g)}<span>${esc(g.name)}<small>${generalSummary(g)}</small></span>`:'⚔ General: None · Assign commander'}</button><label>Formation<select data-formation="${i}">${Object.entries(FORMATIONS).map(([id,x])=>`<option value="${id}" ${f.formation===id?'selected':''} ${formationCheck(f,id)?'disabled':''}>${esc(x.name)}</option>`).join('')}</select></label><div class="sort-grid">${Object.entries(UNITS).filter(([u])=>f.units[u]>0).map(([u,unit])=>{const src=displayArtURL(ART.units[u]);return `<button class="troop-tile ${selected?.from===i&&selected.unit===u?'active':''}" data-unit="${u}" data-from="${i}" draggable="true" aria-pressed="${selected?.from===i&&selected.unit===u?'true':'false'}">${src?`<img data-iron-art src="${esc(src)}" alt="">`:''}<span>${unit.icon} ${esc(unit.name)}</span><b>${f.units[u]}</b></button>`;}).join('')||'<p class="sort-empty">Drop troops here<br>or select a troop card and choose this destination.</p>'}${packet?.from===i?`<button class="troop-packet" draggable="true" data-packet>Drag ${packet.count} ${esc(UNITS[packet.unit].name)}</button>`:''}</div><p class="fine">Approx. strength ${Math.round(strength(preview))} · Movement ${speed.toFixed(1)} / turn · ${(a.resolvedTurn===getState().turn?0:Math.max(0,speed-spent)).toFixed(1)} remaining<br>${esc(getState().tiles[a.tile]?.name||a.tile)} · ${esc(a.tile)}${!troops?'<br>Empty formations are discarded.':''}</p>${selected&&selected.from!==i?`<button class="full" data-drop-button="${i}">${packet?`Place ${packet.count}`:'Move selected troops'} here</button>`:''}</section>`;
  }
  function draw(){
    if(!source()){dialog.close();return;}
    const a=source(),available=selected?draft.formations[selected.from].units[selected.unit]:0;
    if(selected&&target===selected.from)target=draft.formations.findIndex((f,i)=>i!==selected.from);
    dialog.classList.toggle('quick-sort',quick);
    dialog.innerHTML=`<header class="dialog-header"><div><span class="eyebrow">MILITARY COMMAND</span><h2>${quick?'Quick Split':'Reorganize Army'}</h2><p>${esc(armyName(a))} · ${sizeOf(a)} troops · ${esc(getState().tiles[a.tile]?.name||a.tile)}</p></div><button data-sort-close aria-label="Cancel reorganization">×</button></header><div class="sort-body"><p class="sort-note">Arrange your formations, then confirm. No turn is spent. Issued movement, attacks and boarding are cleared for these troops; approved general objectives remain. New detachments start under manual control unless assigned a commander. Spent movement is retained.</p><div class="sort-presets" aria-label="Quick split presets">${[['half','Split Evenly'],['quarter','Detach 25%'],['half','Detach 50%'],['archers','Detach Archers'],['cavalry','Detach Cavalry'],['siege','Detach Siege']].map(([id,label])=>`<button data-preset="${id}">${label}</button>`).join('')}</div><div class="sort-formations">${draft.formations.map(renderFormation).join('')}</div><button data-add-formation ${draft.formations.length>=UI_LIMIT?'disabled':''}>+ Create another formation</button>${quick?'<button data-full-sort>Full Sort Army</button>':''}<section class="sort-tools" aria-label="Selected troop controls">${selected?`<strong>${esc(UNITS[selected.unit].name)} — ${available} available</strong><label>Destination<select id="sort-destination">${draft.formations.map((f,i)=>i===selected.from?'':`<option value="${i}" ${target===i?'selected':''}>${esc(f.name||`Army ${String.fromCharCode(65+i)}`)}</option>`).join('')}</select></label><div class="button-row"><button data-move-all>Move All</button><button data-split-even>Split Evenly</button></div><div class="sort-amount"><label>Move amount<input id="sort-amount" type="number" min="1" max="${available}" step="1" value="${packet?.count||Math.ceil(available/2)}"></label><label>Keep here<input id="sort-remain" type="number" min="0" max="${available-1}" step="1" value="${available-(packet?.count||Math.ceil(available/2))}"></label><button data-move-amount>Move Amount</button><button data-prepare-packet>Prepare Partial Card</button></div>${packet?'<p>A partial card is ready beside the source troops. Drag it or tap a destination.</p>':''}`:'Select a troop card to move all, split evenly, or enter an exact amount.'}</section><p class="sort-error" role="alert"></p></div><footer class="sort-footer"><span>${draft.formations.filter(f=>sizeOf(f)).length} armies · ${draft.formations.reduce((n,f)=>n+sizeOf(f),0)} troops</span><button data-sort-reset>Reset</button><button data-sort-close>Cancel</button><button class="primary" data-sort-confirm ${!canAct()?'disabled':''}>Confirm Reorganization</button></footer>`;
  }
  function chooseGeneral(index){
    selector=index;
    const section=document.createElement('section');section.className='sort-general-picker';section.setAttribute('aria-label','Choose general');
    section.innerHTML=`<h3>General — ${esc(draft.formations[index].name||`Army ${String.fromCharCode(65+index)}`)}</h3><button data-general-select="">None · Manual control</button>${roster().map(g=>{const assigned=allArmies(getState()).filter(a=>a.commandId===g.commandId),inDraft=draft.formations.findIndex(f=>f.generalId===g.id),unavailable=['dead','wounded','unavailable'].includes(g.status);return `<button data-general-select="${g.id}" ${unavailable?'disabled':''}>${generalPortrait(g)}<span><strong>${esc(g.name)}</strong><small>${generalSummary(g)}</small><small>${unavailable?esc(g.status):inDraft>=0?`In draft: Army ${String.fromCharCode(65+inDraft)}`:assigned.length?`Commanding ${assigned.map(a=>esc(armyName(a))).join(', ')}`:'Available · Awaiting Command'}</small></span></button>`;}).join('')||'<p>No recruited generals.</p>'}<button data-picker-close>Back to formations</button>`;
    dialog.querySelector('.sort-body').append(section);section.querySelector('button').focus();
  }
  dialog.addEventListener('close',()=>{draft=null;initial=null;packet=null;returnFocus?.focus();});
  dialog.addEventListener('input',e=>{
    if(e.target.dataset.name!==undefined){draft.formations[Number(e.target.dataset.name)].name=e.target.value;return;}
    if(!selected)return;
    const n=draft.formations[selected.from].units[selected.unit],amount=dialog.querySelector('#sort-amount'),remain=dialog.querySelector('#sort-remain');
    if(e.target===amount)remain.value=n-Number(amount.value);if(e.target===remain)amount.value=n-Number(remain.value);
  });
  dialog.addEventListener('change',e=>{
    if(e.target.dataset.formation!==undefined){draft.formations[Number(e.target.dataset.formation)].formation=e.target.value;refresh();}
    if(e.target.id==='sort-destination')target=Number(e.target.value);
  });
  dialog.addEventListener('click',e=>{
    const b=e.target.closest('button');if(!b||b.disabled)return;const d=b.dataset;
    if('sortClose'in d){dialog.close();return;}
    if('sortReset'in d){draft=structuredClone(initial);selected=null;refresh();return;}
    if('fullSort'in d){quick=false;draw();return;}
    if('addFormation'in d){if(draft.formations.length<UI_LIMIT)addDraftFormation(draft);quick=false;refresh();return;}
    if(d.preset){draft=structuredClone(initial);applyDraftPreset(draft,d.preset);selected=null;refresh();return;}
    if(d.unit){selected={from:Number(d.from),unit:d.unit};refresh();return;}
    if(d.pickGeneral!==undefined){chooseGeneral(Number(d.pickGeneral));return;}
    if('pickerClose'in d){dialog.querySelector('.sort-general-picker')?.remove();selector=null;return;}
    if('generalSelect'in d){
      const id=d.generalSelect||null,g=roster().find(g=>g.id===id),other=draft.formations.findIndex((f,i)=>i!==selector&&f.generalId===id&&id),assigned=g?allArmies(getState()).filter(a=>a.commandId===g.commandId&&a.id!==draft.army):[];
      if(id&&(other>=0||assigned.length)&&!confirm(`Transfer Command: ${g.name} currently commands ${other>=0?`Army ${String.fromCharCode(65+other)}`:assigned.map(armyName).join(', ')}. Transfer to this formation? Previous forces return to manual control.`))return;
      if(other>=0)draft.formations[other].generalId=null;
      if(assigned.length&&!draft.transfers.includes(id))draft.transfers.push(id);
      draft.formations[selector].generalId=id;selector=null;refresh();return;
    }
    if('sortConfirm'in d){const r=validateArmyDraft(getState(),getOwner(),draft);if(!r.ok){dialog.querySelector('.sort-error').textContent=r.error;return;}const payload=structuredClone(draft);if(perform('reorganize',payload,()=>reorganizeArmy(getState(),getOwner(),payload)))dialog.close();return;}
    if(!selected)return;
    const {from,unit}=selected,count=draft.formations[from].units[unit];
    if('moveAll'in d)move(from,target,unit,count);
    if('splitEven'in d)move(from,target,unit,Math.ceil(count/2));
    if('moveAmount'in d)move(from,target,unit,Number(dialog.querySelector('#sort-amount').value));
    if('preparePacket'in d){const amount=Number(dialog.querySelector('#sort-amount').value);if(!Number.isSafeInteger(amount)||amount<1||amount>count){toast('Choose a valid whole number to move.');return;}packet={...selected,count:amount};draw();}
    if(d.dropButton!==undefined)move(from,Number(d.dropButton),unit,packet?.count||count);
  });
  dialog.addEventListener('dragstart',e=>{
    const card=e.target.closest('[data-unit],[data-packet]');if(!card)return;
    if(card.dataset.unit){selected={from:Number(card.dataset.from),unit:card.dataset.unit};packet={...selected,count:draft.formations[selected.from].units[selected.unit]};}
    if(!packet)return;e.dataTransfer.effectAllowed='move';e.dataTransfer.setData('text/plain',JSON.stringify(packet));
    for(const box of dialog.querySelectorAll('[data-drop]')){const i=Number(box.dataset.drop),n=sizeOf(draft.formations[i]);box.classList.toggle('drop-valid',i!==packet.from);box.querySelector('[data-total]').textContent=i===packet.from?`${n} → ${n-packet.count} troops`:`${n} → ${n+packet.count} troops`;}
  });
  dialog.addEventListener('dragover',e=>{const box=e.target.closest('[data-drop]');if(packet&&box&&Number(box.dataset.drop)!==packet.from){e.preventDefault();e.dataTransfer.dropEffect='move';}});
  dialog.addEventListener('drop',e=>{const box=e.target.closest('[data-drop]');if(!packet||!box)return;e.preventDefault();const p=packet;move(p.from,Number(box.dataset.drop),p.unit,p.count);});
  dialog.addEventListener('dragend',()=>{if(draft)draw();});
  document.addEventListener('click',e=>{const b=e.target.closest('button');if(!b||b.disabled)return;if(b.dataset.sortArmy)open(b.dataset.sortArmy);if(b.dataset.quickSplit)open(b.dataset.quickSplit,true);});
  function merge(tile){
    const s=getState(),owner=getOwner(),group=s.armies.filter(a=>a.owner===owner&&a.tile===tile),generals=[...new Set(group.map(a=>generalForArmy(s,a)?.id).filter(Boolean))];
    if(generals.length<2){perform('merge',{tile},()=>mergeArmies(s,owner,tile));return;}
    const chooser=document.createElement('dialog');chooser.className='merge-commander';
    chooser.innerHTML=`<h2>Choose commanding general</h2><p>The other commanders return to Awaiting Command. All troops remain.</p>${generals.map(id=>{const g=roster().find(g=>g.id===id);return `<button data-merge-general="${id}">${generalPortrait(g)} ${esc(g.name)}</button>`;}).join('')}<button data-cancel>Cancel</button>`;
    document.body.append(chooser);chooser.addEventListener('close',()=>chooser.remove());chooser.addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;if(b.dataset.mergeGeneral){const generalId=b.dataset.mergeGeneral;if(!perform('merge',{tile,generalId},()=>mergeArmies(getState(),getOwner(),tile,'player',generalId)))return;}chooser.close();});chooser.showModal();
  }
  return {open,merge};
}
