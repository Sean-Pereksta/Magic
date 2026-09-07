'use strict';

/* Arcane Wilds — interactive Spellbook loadout management.
 * Loaded after progression-expansion.js so the spellbook uses the same dynamic
 * 3–5 slot limit as the HUD and level-up flow.
 */
(function(){
  if(window.__arcaneWildsSpellbookLoadoutLoaded)return;
  window.__arcaneWildsSpellbookLoadoutLoaded=true;
  if(typeof renderInventory!=='function'||typeof game==='undefined')return;

  const baseRenderInventory=renderInventory;
  let selected=null;

  function slotLimit(){
    return typeof window.awSpellSlotLimit==='function'?window.awSpellSlotLimit():3;
  }

  function activeSpells(){
    if(!game.player)return [];
    return game.player.activeSpells||(game.player.activeSpells=[]);
  }

  function activeSlotOf(id){
    return activeSpells().slice(0,slotLimit()).indexOf(id);
  }

  function firstOpenSlot(){
    const active=activeSpells(),limit=slotLimit();
    for(let i=0;i<limit;i++)if(!active[i])return i;
    return -1;
  }

  function commitLoadout(message){
    if(typeof saveGame==='function')saveGame();
    if(typeof updateHUD==='function')updateHUD();
    else if(typeof renderSpellBar==='function')renderSpellBar();
    renderInventory();
    if(message&&typeof toastMsg==='function')toastMsg(message);
  }

  function moveSpellToSlot(id,targetSlot,sourceSlot=null){
    const p=game.player,limit=slotLimit();
    if(!p||!SPELLS[id]||targetSlot<0||targetSlot>=limit)return false;
    const active=activeSpells();
    let source=Number.isInteger(sourceSlot)?sourceSlot:active.slice(0,limit).indexOf(id);
    if(source<0||source>=limit)source=-1;
    if(source===targetSlot)return true;

    const displaced=active[targetSlot]||null;
    for(let i=0;i<active.length;i++)if(i!==targetSlot&&active[i]===id)active[i]=null;
    active[targetSlot]=id;
    if(source>=0&&source!==targetSlot)active[source]=displaced;

    selected=null;
    commitLoadout(`${SPELLS[id].name} equipped in slot ${targetSlot+1}${displaced&&source<0?` • ${SPELLS[displaced]?.name||'previous spell'} returned to the Spellbook`:''}.`);
    return true;
  }

  function unequipSlot(slot){
    const active=activeSpells(),limit=slotLimit();
    if(slot<0||slot>=limit||!active[slot])return false;
    const id=active[slot];
    active[slot]=null;
    selected=null;
    commitLoadout(`${SPELLS[id]?.name||'Spell'} unequipped from slot ${slot+1}.`);
    return true;
  }

  function chooseSpell(id,sourceSlot=null){
    if(!SPELLS[id])return;
    selected={id,sourceSlot:Number.isInteger(sourceSlot)?sourceSlot:activeSlotOf(id)};
    renderInventory();
  }

  function quickEquip(id){
    const current=activeSlotOf(id);
    if(current>=0){chooseSpell(id,current);return}
    const open=firstOpenSlot();
    if(open>=0){moveSpellToSlot(id,open,-1);return}
    selected={id,sourceSlot:-1};
    renderInventory();
    if(typeof toastMsg==='function')toastMsg(`${SPELLS[id].name} selected • choose a spell slot to replace.`);
  }

  function dragPayload(e){
    try{return JSON.parse(e.dataTransfer?.getData('text/plain')||'')}catch(_){return null}
  }

  function mutationText(id){
    const ups=game.player?.upgrades?.[id]||[];
    const labels=ups.map(uid=>UPGRADE_POOLS[id]?.find(v=>v[0]===uid)?.[1]).filter(Boolean);
    return labels.length?labels.join(' • '):'No mutations yet';
  }

  function slotMarkup(i,limit,active){
    if(i>=limit)return `<div class="aw-book-slot locked" data-aw-slot="${i}" aria-disabled="true"><span class="aw-book-slot-key">${i+1}</span><span class="aw-book-slot-icon">🔒</span><b>Locked Spell Slot</b><small>${i===3?'Rare spell-channeling gear can unlock slot 4.':'Additional Rare/Legendary gear can unlock slot 5.'}</small></div>`;
    const id=active[i],s=id&&SPELLS[id];
    if(!s)return `<div class="aw-book-slot empty${selected?' selectable':''}" data-aw-slot="${i}"><span class="aw-book-slot-key">${i+1}</span><span class="aw-book-slot-icon">＋</span><b>Empty Slot</b><small>${selected?'Click to equip selected spell':'Drag a known spell here'}</small></div>`;
    const isSelected=selected?.id===id&&selected?.sourceSlot===i;
    return `<div class="aw-book-slot equipped${isSelected?' selected':''}" draggable="true" data-aw-slot="${i}" data-aw-spell-id="${id}" style="border-color:${colorAlpha(rarityColors[s.rarity],.44)}"><span class="aw-book-slot-key">${i+1}</span><span class="aw-book-slot-icon">${s.icon}</span><b>${s.name}</b><small>${s.rarity} • ${(game.player.upgrades[id]||[]).length} mutations</small><button class="aw-book-unequip" type="button" data-aw-unequip="${i}">Unequip</button></div>`;
  }

  function spellMarkup(id){
    const s=SPELLS[id];if(!s)return '';
    const slot=activeSlotOf(id),isActive=slot>=0,isSelected=selected?.id===id;
    return `<div class="aw-book-spell${isActive?' active':''}${isSelected?' selected':''}" draggable="true" data-aw-spell-id="${id}" data-aw-source-slot="${slot}" style="border-color:${colorAlpha(rarityColors[s.rarity],.3)}"><div class="aw-book-spell-head"><b>${s.icon} ${s.name}</b><span style="color:${rarityColors[s.rarity]}">${isActive?`Slot ${slot+1}`:s.rarity}</span></div><span>${s.desc}</span><small>${mutationText(id)}</small><button type="button" class="aw-book-action" data-aw-${isActive?'unequip-card':'equip'}="${isActive?slot:id}">${isActive?'Unequip':firstOpenSlot()>=0?'Equip':'Choose Slot'}</button></div>`;
  }

  function wireSpellbook(root){
    root.querySelectorAll('[data-aw-spell-id][draggable="true"]').forEach(el=>{
      el.addEventListener('dragstart',e=>{
        const id=el.dataset.awSpellId,source=Number(el.dataset.awSlot??el.dataset.awSourceSlot);
        e.dataTransfer.effectAllowed='move';
        e.dataTransfer.setData('text/plain',JSON.stringify({id,sourceSlot:Number.isInteger(source)&&source>=0?source:-1}));
        el.classList.add('dragging');
      });
      el.addEventListener('dragend',()=>el.classList.remove('dragging'));
    });

    root.querySelectorAll('.aw-book-slot:not(.locked)').forEach(slotEl=>{
      const target=Number(slotEl.dataset.awSlot);
      slotEl.addEventListener('dragover',e=>{e.preventDefault();e.dataTransfer.dropEffect='move';slotEl.classList.add('drag-over')});
      slotEl.addEventListener('dragleave',()=>slotEl.classList.remove('drag-over'));
      slotEl.addEventListener('drop',e=>{
        e.preventDefault();slotEl.classList.remove('drag-over');
        const data=dragPayload(e);if(data?.id)moveSpellToSlot(data.id,target,Number(data.sourceSlot));
      });
      slotEl.addEventListener('click',e=>{
        if(e.target.closest('button'))return;
        if(selected){moveSpellToSlot(selected.id,target,selected.sourceSlot);return}
        const id=slotEl.dataset.awSpellId;if(id)chooseSpell(id,target);
      });
    });

    root.querySelectorAll('[data-aw-unequip]').forEach(b=>b.onclick=e=>{e.stopPropagation();unequipSlot(Number(b.dataset.awUnequip))});
    root.querySelectorAll('[data-aw-unequip-card]').forEach(b=>b.onclick=e=>{e.stopPropagation();unequipSlot(Number(b.dataset.awUnequipCard))});
    root.querySelectorAll('[data-aw-equip]').forEach(b=>b.onclick=e=>{e.stopPropagation();quickEquip(b.dataset.awEquip)});
    root.querySelectorAll('.aw-book-spell').forEach(card=>card.addEventListener('click',e=>{if(e.target.closest('button'))return;chooseSpell(card.dataset.awSpellId,Number(card.dataset.awSourceSlot))}));

    const library=root.querySelector('.aw-spell-library');
    if(library){
      library.addEventListener('dragover',e=>{const data=dragPayload(e);if(data&&Number(data.sourceSlot)>=0){e.preventDefault();library.classList.add('drag-over')}});
      library.addEventListener('dragleave',()=>library.classList.remove('drag-over'));
      library.addEventListener('drop',e=>{const data=dragPayload(e);library.classList.remove('drag-over');if(data&&Number(data.sourceSlot)>=0){e.preventDefault();unequipSlot(Number(data.sourceSlot))}});
    }

    const clear=root.querySelector('[data-aw-clear-selection]');
    if(clear)clear.onclick=()=>{selected=null;renderInventory()};
  }

  renderInventory=function(){
    baseRenderInventory();
    if(!game.player)return;
    const root=$('inventoryContent');if(!root)return;
    const heading=[...root.querySelectorAll('h3')].find(h=>h.textContent.trim().startsWith('Spellbook'));
    if(!heading)return;
    const originalGrid=heading.nextElementSibling;
    if(!originalGrid)return;

    const limit=slotLimit(),active=activeSpells(),known=(game.player.unlocked||[]).filter(id=>SPELLS[id]);
    const manager=document.createElement('section');
    manager.className='aw-book-manager';
    manager.innerHTML=`<div class="aw-book-manager-head"><div><b>Active Spell Loadout • ${limit} / 5 slots available</b><span>Drag spells into slots, drag equipped spells between slots to swap them, or use the buttons. On touch, tap a spell and then tap its destination slot.</span></div>${selected?`<button type="button" class="aw-book-clear" data-aw-clear-selection>Cancel ${SPELLS[selected.id]?.name||'selection'}</button>`:''}</div><div class="aw-book-slots">${Array.from({length:5},(_,i)=>slotMarkup(i,limit,active)).join('')}</div><div class="aw-book-capacity">${limit===3?'Your three core slots are available. Rare and Legendary spell-channeling equipment can expand this loadout to four or five.':limit===4?'A spell-channeling item has opened slot 4. Find another bonus or a +2 Legendary item to reach five.':'Maximum five-spell loadout unlocked.'}</div>`;
    heading.insertAdjacentElement('afterend',manager);

    originalGrid.className='aw-spell-library';
    originalGrid.removeAttribute('style');
    originalGrid.innerHTML=known.map(spellMarkup).join('')||'<div class="muted-exp">No spells discovered yet.</div>';
    originalGrid.insertAdjacentHTML('beforebegin','<div class="aw-book-library-title"><b>Known Spells</b><span>Equipped spells remain here so you can inspect their retained mutations or move them to another slot.</span></div>');
    wireSpellbook(root);
  };

  const style=document.createElement('style');style.id='awSpellbookLoadoutStyles';style.textContent=`
    .aw-book-manager{margin:0 0 16px;padding:14px;border:1px solid rgba(144,190,255,.18);border-radius:16px;background:linear-gradient(145deg,rgba(33,50,78,.28),rgba(78,47,112,.13))}
    .aw-book-manager-head,.aw-book-library-title{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:11px}.aw-book-manager-head>div,.aw-book-library-title{flex-direction:column}.aw-book-manager-head b,.aw-book-library-title b{font-size:13px}.aw-book-manager-head span,.aw-book-library-title span{font-size:10px;color:#9fb3c8;line-height:1.45;margin-top:3px}.aw-book-clear,.aw-book-action,.aw-book-unequip{border:1px solid rgba(168,204,255,.24);background:rgba(100,147,205,.12);color:#dceeff;border-radius:9px;font:800 9px system-ui;padding:6px 8px;cursor:pointer}.aw-book-clear:hover,.aw-book-action:hover,.aw-book-unequip:hover{background:rgba(119,173,239,.22)}
    .aw-book-slots{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px}.aw-book-slot{position:relative;min-height:126px;padding:12px 9px 9px;border:1px dashed rgba(159,185,219,.28);border-radius:13px;background:rgba(5,12,22,.48);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;text-align:center;cursor:pointer;transition:transform .15s,border-color .15s,background .15s}.aw-book-slot.equipped{border-style:solid;background:linear-gradient(180deg,rgba(41,62,91,.58),rgba(8,15,27,.62))}.aw-book-slot.empty.selectable,.aw-book-slot.drag-over{border-color:#a7ccff;background:rgba(92,139,197,.2);transform:translateY(-2px)}.aw-book-slot.selected{box-shadow:0 0 0 2px rgba(229,197,255,.42),0 0 24px rgba(142,92,216,.2)}.aw-book-slot.locked{opacity:.52;cursor:not-allowed}.aw-book-slot-key{position:absolute;top:7px;left:8px;font-size:9px;font-weight:950;color:#a8bfd8}.aw-book-slot-icon{font-size:27px}.aw-book-slot b{font-size:10px}.aw-book-slot small{font-size:8px;color:#8fa5bb;line-height:1.35}.aw-book-unequip{margin-top:auto;width:100%}.aw-book-capacity{margin-top:9px;font-size:9px;color:#a8bad0;line-height:1.4}
    .aw-book-library-title{display:flex;margin:14px 0 8px}.aw-spell-library{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:9px;margin-top:0}.aw-book-spell{padding:12px;border:1px solid rgba(255,255,255,.1);border-radius:13px;background:rgba(255,255,255,.035);display:flex;flex-direction:column;gap:7px;cursor:pointer;transition:transform .15s,background .15s,box-shadow .15s}.aw-book-spell:hover{background:rgba(255,255,255,.06);transform:translateY(-1px)}.aw-book-spell.active{background:rgba(93,141,202,.08)}.aw-book-spell.selected{box-shadow:0 0 0 2px rgba(198,161,255,.4)}.aw-book-spell-head{display:flex;justify-content:space-between;gap:9px;align-items:center}.aw-book-spell-head b{font-size:11px}.aw-book-spell-head span{font-size:9px;font-weight:900}.aw-book-spell>span{font-size:9px;color:#a9bbce;line-height:1.42}.aw-book-spell>small{font-size:8px;color:#d8e5f2;line-height:1.35}.aw-book-action{margin-top:auto;align-self:flex-start}.aw-spell-library.drag-over{outline:2px dashed rgba(180,211,255,.42);outline-offset:6px;border-radius:10px}.dragging{opacity:.48}
    @media(max-width:760px){.aw-book-slots{grid-template-columns:repeat(3,1fr)}.aw-book-slot{min-height:112px}.aw-book-manager-head{flex-direction:column}.aw-book-clear{align-self:flex-start}.aw-spell-library{grid-template-columns:1fr 1fr}}
    @media(max-width:480px){.aw-book-slots,.aw-spell-library{grid-template-columns:1fr 1fr}.aw-book-slot{min-height:104px}.aw-book-slot-icon{font-size:23px}}
  `;document.head.appendChild(style);

  window.awSpellbookMoveSpell=moveSpellToSlot;
  window.awSpellbookUnequipSlot=unequipSlot;
})();