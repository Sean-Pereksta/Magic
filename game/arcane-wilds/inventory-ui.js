'use strict';
/* Inventory owns browsing only; the existing loadout gate owns spell assignment.
 * No copied spell state, upgrade grants, cooldown resets, or journey schema changes. */
(() => {
  if(window.AWInventory)return;
  const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let page='gear',query='',filter='all',selected=null;
  const root=$('inventoryContent'),overlay=$('inventoryOverlay');
  overlay.querySelector('.panel').setAttribute('role','dialog');
  overlay.querySelector('.panel').setAttribute('aria-modal','true');
  overlay.querySelector('.panel').setAttribute('aria-labelledby','inventoryTitle');
  function mutations(id){return (game.player.upgrades[id]||[]).map(uid=>UPGRADE_POOLS[id]?.find(u=>u[0]===uid)).filter(Boolean);}
  function glyph(id){return window.AWModernUI.icon(id);}
  function render(){
    if(!game.player)return;
    ensureExpansionState();
    const p=game.player;
    if(selected&&!p.unlocked.includes(selected))selected=null;
    root.innerHTML=`<div class="aw-inventory-summary"><span>LEVEL <b>${game.level}</b></span><span>GOLD <b>${Math.floor(game.gold)}</b></span><span>SPELL SLOTS <b>${awCurrentSpellLimit()} / 5</b></span></div>
      <div class="aw-inventory-tabs" role="tablist" aria-label="Inventory sections">${[['gear','Equipment'],['spells','Spells'],['journal','Journal']].map(([id,label])=>`<button id="aw-tab-${id}" role="tab" aria-controls="awInventoryPage" aria-selected="${page===id}" tabindex="${page===id?0:-1}" data-page="${id}">${label}</button>`).join('')}</div>
      <section id="awInventoryPage" role="tabpanel" aria-labelledby="aw-tab-${page}" tabindex="0"></section>`;
    root.querySelectorAll('[data-page]').forEach(b=>{
      b.onclick=()=>{page=b.dataset.page;render();$('aw-tab-'+page).focus();};
      b.onkeydown=e=>{const tabs=['gear','spells','journal'],i=tabs.indexOf(page);let next;
        if(e.key==='ArrowRight')next=tabs[(i+1)%3];else if(e.key==='ArrowLeft')next=tabs[(i+2)%3];else if(e.key==='Home')next=tabs[0];else if(e.key==='End')next=tabs[2];
        if(next){e.preventDefault();page=next;render();$('aw-tab-'+page).focus();}
      };
    });
    const body=$('awInventoryPage');
    if(page==='gear'){
      body.innerHTML=`<h3>Equipped gear</h3><div class="aw-equipped-grid">${gearCard(p.weapon,'Weapon')}${gearCard(p.armorGear,'Armor')}</div><h3>Trinkets <small>3 slots</small></h3><div class="aw-trinkets">${p.trinkets.map((item,i)=>gearCard(item,`Trinket ${i+1}`)).join('')}</div><p class="aw-menu-hint">${awCurrentSpellLimit()>3?'Your equipment grants additional active spell slots.':'Rare spell-channeling gear can unlock slots 4 and 5.'} Compare new finds when inspecting ground loot.</p>`;
    }else if(page==='journal'){
      body.innerHTML=`<h3>Quest journal</h3><div class="aw-journal">${game.quests.active.filter(q=>!q.claimed).map(q=>`<article><b>${esc(q.title)}</b><span>${q.ready?'Ready to claim':`${q.progress||0} / ${q.target}`}</span><progress aria-label="${esc(q.title)}" value="${q.progress||0}" max="${q.target}"></progress></article>`).join('')||'<p class="aw-menu-hint">No active quests. Visit a village Quest Keeper for local work.</p>'}</div><h3>Forging materials</h3><div class="aw-materials">${Object.entries(MATERIALS).map(([id,m])=>`<div><span>${esc(m.name)}</span><b>${game.materials[id]||0}</b></div>`).join('')}</div>`;
    }else renderSpellbook(body);
  }
  function renderSpellbook(body){
    const p=game.player,limit=awCurrentSpellLimit();
    body.innerHTML=`<h3>Active loadout <small>${limit} slots</small></h3><div class="aw-loadout">${Array.from({length:limit},(_,i)=>{
      const id=p.activeSpells[i],s=SPELLS[id];return `<button class="aw-loadout-slot" data-active-slot="${i}" aria-label="Slot ${i+1}: ${esc(s?.name||'Empty')}"><small>SLOT ${i+1}</small><span class="aw-menu-icon">${s?glyph(id):'＋'}</span><b>${esc(s?.name||'Choose spell')}</b></button>`;
    }).join('')}</div><div id="awSpellDetail"></div>
      <div class="aw-spell-toolbar"><label>Find a spell<input id="awSpellSearch" type="search" placeholder="Name, effect or mutation" value="${esc(query)}" autocomplete="off"></label><label>Show<select id="awSpellFilter"><option value="all">All learned</option><option value="active">Equipped</option><option value="inactive">Not equipped</option></select></label></div>
      <p id="awSpellCount" class="aw-menu-hint" role="status"></p><div id="awSpellLibrary" class="aw-spell-library"></div>`;
    body.querySelectorAll('[data-active-slot]').forEach(b=>b.onclick=()=>{
      const id=p.activeSpells[Number(b.dataset.activeSlot)];
      if(id){selected=id;renderDetail();}else{selected=null;filter='inactive';$('awSpellFilter').value=filter;renderDetail();renderLibrary();$('awSpellSearch').focus();}
    });
    $('awSpellSearch').oninput=e=>{query=e.target.value;renderLibrary();};
    $('awSpellFilter').value=filter;$('awSpellFilter').onchange=e=>{filter=e.target.value;renderLibrary();};
    renderDetail();renderLibrary();
  }
  function renderLibrary(){
    const p=game.player,term=query.trim().toLocaleLowerCase();
    const ids=[...new Set(p.unlocked)].filter(id=>{
      const s=SPELLS[id];if(!s)return false;
      const active=awIsSpellActive(id);
      return (filter==='all'||(filter==='active'?active:!active))&&[s.name,s.desc,s.rarity,...mutations(id).map(u=>u[1])].join(' ').toLocaleLowerCase().includes(term);
    }).sort((a,b)=>Number(awIsSpellActive(b))-Number(awIsSpellActive(a))||SPELLS[a].name.localeCompare(SPELLS[b].name));
    $('awSpellCount').textContent=`${ids.length} shown · ${p.unlocked.length} learned. Mutations remain with a spell when you change slots.`;
    const library=$('awSpellLibrary');
    library.innerHTML=ids.map(id=>{const s=SPELLS[id],slot=awActiveSpellSlot(id);return `<button class="aw-library-spell" data-spell="${esc(id)}" style="--rar:${rarityColors[s.rarity]}" aria-pressed="${selected===id}"><span class="aw-menu-icon">${glyph(id)}</span><span><small>${esc(s.rarity)} · ${slot>=0?'Slot '+(slot+1):'Learned'}</small><b>${esc(s.name)}</b><span>${esc(s.desc)}</span><small>${(s.cooldown*spellMods(id).cdr).toFixed(1)}s cooldown · ${mutations(id).length} mutations</small></span></button>`;}).join('')||'<p class="aw-menu-hint">No spells match. Try another search or show all learned spells.</p>';
    library.querySelectorAll('[data-spell]').forEach(b=>b.onclick=()=>{
      selected=b.dataset.spell;renderDetail();
      library.querySelectorAll('[data-spell]').forEach(n=>n.setAttribute('aria-pressed',String(n.dataset.spell===selected)));
      $('awSpellDetail').scrollIntoView?.({block:'nearest'});$('awSpellDetail').focus();
    });
  }
  function renderDetail(){
    const detail=$('awSpellDetail'),s=SPELLS[selected];
    detail.tabIndex=-1;
    if(!s){detail.innerHTML='<p class="aw-menu-hint">Select a learned spell to inspect its mutations and choose a slot.</p>';return;}
    const p=game.player,ups=mutations(selected),active=awActiveSpellSlot(selected);
    detail.innerHTML=`<article class="aw-spell-detail" style="--rar:${rarityColors[s.rarity]}"><div class="aw-detail-heading"><span class="aw-menu-icon">${glyph(selected)}</span><div><small>${esc(s.rarity)} · ${active>=0?'Equipped in slot '+(active+1):'Learned'}</small><h3>${esc(s.name)}</h3></div></div><p>${esc(s.desc)}</p>${ups.length?`<ul>${ups.map(u=>`<li><b>${esc(u[1])}</b> ${esc(u[3])}</li>`).join('')}</ul>`:'<p class="aw-menu-hint">No mutations yet. Evolve equipped spells through level-up choices.</p>'}<p class="aw-menu-hint">Choose a slot. Replaced spells remain learned.</p><div class="aw-assign-slots">${Array.from({length:awCurrentSpellLimit()},(_,i)=>`<button class="btn secondary" data-equip-slot="${i}" ${i===active?'disabled':''}><b>${i===active?'Equipped':'Equip'} · Slot ${i+1}</b><small>${esc(SPELLS[p.activeSpells[i]]?.name||'Empty')}</small></button>`).join('')}</div><p id="awEquipStatus" role="status"></p></article>`;
    detail.querySelectorAll('[data-equip-slot]').forEach(b=>b.onclick=()=>{
      // Revalidate against the live player and equipment, never a stale menu snapshot.
      const id=selected,slot=Number(b.dataset.equipSlot);
      if(!game.player.unlocked.includes(id)||!SPELLS[id]||!awEquipSpellInSlot(id,slot))return;
      saveGame();updateHUD();render();$('awEquipStatus').textContent=`${SPELLS[id].name} equipped in slot ${slot+1}.`;
      $('awSpellDetail').focus();
    });
  }
  window.AWInventory={render};
})();
