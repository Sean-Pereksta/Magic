'use strict';
/* One adventure menu, with touch/keyboard/controller fallbacks for every action. */
(() => {
  const A=window.AWCampaign,D=A.data,overlay=$('inventoryOverlay'),panel=overlay.querySelector('.panel'),root=$('inventoryContent');
  const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const icons={wildland:'♣',danger:'⚔',town:'⌂',village:'⌂',portalCity:'◉',boss:'♜',ruler:'♛',dungeon:'▥',shrine:'✦',merchant:'⚖',landmark:'◆',mount:'♞',event:'!',passage:'⚓'};
  let page='Character',selectedSpell=null,category='All',search='',selectedNode=null,selectedContinent='verdant',travelMode='road',previousFocus=null;
  let view={x:0,y:0,zoom:.8},pointers=new Map(),gesture=null,dragged=false;
  panel.classList.add('aw-adventure-menu');panel.setAttribute('role','dialog');panel.setAttribute('aria-modal','true');panel.setAttribute('aria-label','Adventure menu');
  overlay.querySelector('h2').textContent='The Wanderer’s Journal';overlay.querySelector('.overlay-head p').textContent='Three continents. One journey.';
  const closeButton=overlay.querySelector('[data-close]');closeButton.onclick=()=>close();
  function isOpen(){return !overlay.classList.contains('hidden');}
  function close(){overlay.classList.add('hidden');pointers.clear();gesture=null;document.body.classList.remove('aw-menu-open');modalPause=Array.from(document.querySelectorAll('.overlay')).some(el=>!el.classList.contains('hidden'));window.AWInput?.clear();previousFocus?.focus?.();}
  function open(next='Character',mode='road'){
    if(!game.player||!running||roomTransition)return;
    if(!isOpen())previousFocus=document.activeElement;
    travelMode=mode;selectedContinent=A.current()?.continent||'verdant';selectedNode=A.current()?.id;
    document.querySelectorAll('#npcPanel,#townPanel').forEach(el=>el.classList.add('hidden'));
    renderPage(next);showOverlay('inventoryOverlay');document.body.classList.add('aw-menu-open');window.AWInput?.clear();root.querySelector('[aria-current="page"]')?.focus();
  }
  function renderPage(next=page){
    page=next;window.AWInput?.clear();
    root.innerHTML=`<nav class="aw-journal-tabs" aria-label="Journal pages">${['Character','Spellbook','Inventory','Map'].map(x=>`<button type="button" data-page="${x}" aria-current="${page===x?'page':'false'}">${x}</button>`).join('')}</nav><section id="awJournalPage" class="aw-journal-page"></section>`;
    root.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>renderPage(b.dataset.page));
    ({Character:renderCharacter,Spellbook:renderBook,Inventory:renderGear,Map:renderMap})[page]();
  }
  function content(){return $('awJournalPage');}
  function action(label,fn,parent=content(),cls='btn secondary'){const b=document.createElement('button');b.type='button';b.className=cls;b.textContent=label;b.onclick=fn;parent.appendChild(b);return b;}
  function renderCharacter(){
    const s=A.state(),n=A.current(),p=game.player;if(!s||!n)return;
    content().innerHTML=`<div class="aw-journey-banner"><small>${esc(D.continent(n.continent).name)}</small><h2>Wanderer • Level ${game.level}</h2><p>${esc(n.name)} · ${Math.ceil(p.hp)} / ${Math.ceil(p.maxHp)} health · ${game.gold} gold</p><p>Sanctuary: ${esc(D.nodes[s.checkpoint].name)}</p></div><div class="aw-seal-grid">${D.continents.map(c=>`<section class="exp-card"><h3>${esc(c.name)}</h3><p>${D.sealCount(s,c)} / 4 regional seals</p><p>${s.defeated.includes(c.boss)?'✓ Ruler defeated':s.unlocked.includes(c.id)?'Journey in progress':'Awaiting passage'}</p></section>`).join('')}</div><h3>Companions</h3><div id="awMountList" class="aw-card-grid"></div><h3>Quest Journal</h3><div>${(game.quests?.active||[]).filter(q=>!q.claimed).map(q=>`<p>${esc(q.title)} · ${q.progress||0}/${q.target}${q.ready?' · Return to a Quest Keeper':''}</p>`).join('')||'<p>Visit a town’s Quest Keeper for local work.</p>'}</div>`;
    const mountList=$('awMountList');
    if(!s.mounts.length)mountList.textContent='Find a Stablemaster or a hidden mount location to earn your first companion.';
    for(const id of s.mounts){const m=D.mounts[id];action(`${m.icon} ${m.name} • ${s.activeMount===id&&s.riding?'Dismiss':'Ride'}`,()=>{A.mount(id);renderCharacter();},mountList);}
    if(s.rewards.length)action(`Claim regional reward: ${D.items[s.rewards[0]].name}`,()=>{if(!A.claimReward())toastMsg('Inspect or leave pending loot first.');});
    if(game.loot)action(`Inspect pending loot: ${game.loot.name}`,()=>{close();openLootOverlay();});
    action('Save journey',()=>{saveGame();toastMsg('Journey saved on this device.');});
  }
  function renderGear(){
    const p=game.player;
    content().innerHTML=`<div class="gear-layout">${gearCard(p.weapon,'Weapon')}${gearCard(p.armorGear,'Armor')}</div><h3>Trinkets</h3><div class="trinket-grid">${p.trinkets.map((t,i)=>gearCard(t,`Slot ${i+1}`)).join('')}</div><h3>Regional materials</h3><div class="aw-card-grid">${Object.entries(MATERIALS).map(([k,m])=>`<div class="exp-card"><b>${m.icon} ${esc(m.name)}</b><span>${game.materials[k]||0}</span></div>`).join('')}</div>`;
    if(game.loot)action(`Compare ${game.loot.name}`,()=>{close();openLootOverlay();});
  }
  function categories(id){
    const s=SPELLS[id];if(s.category)return [s.category,...(s.tags||[])];
    const text=id+' '+s.name+' '+s.desc,result=[];
    for(const [name,pattern] of [['Fire',/fire|ember|burn|phoenix|meteor|furnace/i],['Frost',/frost|ice|glacial|rime|winter/i],['Storm',/storm|lightning|gust|wind|thunder|tempest/i],['Nature',/nature|thorn|root|poison|briar|bloom|venom|garden/i],['Shadow/Void',/void|soul|shadow|dark|eventide|curse/i],['Celestial',/star|celestial|seraph|sun|astral|radiant/i],['Defensive',/shield|ward|defend|aegis|barrier/i],['Summoning',/summon|wisp|companion|puppet/i],['Utility',/slow|teleport|blink|time|movement|pull/i]])if(pattern.test(text))result.push(name);
    return result.length?result:['Arcane'];
  }
  function assign(id,index){
    if(!Number.isInteger(index)||index<0||index>=awCurrentSpellLimit()||!game.player.unlocked.includes(id)||!SPELLS[id])return false;
    const active=game.player.activeSpells,from=active.indexOf(id),displaced=active[index];
    if(from>=0&&from!==index)active[from]=displaced||null;active[index]=id;
    saveGame();updateHUD();renderBook();return true;
  }
  function renderBook(){
    const p=game.player,known=p.unlocked.filter(id=>SPELLS[id]),limit=awCurrentSpellLimit();selectedSpell=known.includes(selectedSpell)?selectedSpell:known[0];
    content().classList.add('aw-book');
    content().innerHTML=`<div class="aw-book-heading"><div><small>THE LIVING GRIMOIRE</small><h2>Spellbook</h2></div><span>${known.length} known · ${limit}/5 active slots</span></div><p>Select a spell, then Equip and choose a slot. Every mutation stays with its spell.</p><div class="aw-loadout" id="awLoadout"></div><div class="aw-book-filter"><label>School <select id="awSchool">${['All','Fire','Frost','Storm','Nature','Arcane','Shadow/Void','Celestial','Defensive','Summoning','Utility'].map(x=>`<option${x===category?' selected':''}>${x}</option>`).join('')}</select></label><label>Find spell <input id="awSpellSearch" type="search" placeholder="Name or effect" value="${esc(search)}"></label></div><div class="aw-book-pages"><div id="awKnownSpells" class="aw-known-spells"></div><section id="awSpellDetail" aria-live="polite"></section></div>`;
    for(let i=0;i<5;i++){
      const id=p.activeSpells[i],s=SPELLS[id],b=action(i>=limit?`🔒 Slot ${i+1}`:`${i+1} · ${s?s.icon+' '+s.name:'Empty'}`,()=>{if(selectedSpell)assign(selectedSpell,i);},$('awLoadout'),'aw-loadout-slot');
      b.disabled=i>=limit;b.title=i>=limit?'Rare or Legendary spell-channeling gear unlocks this slot.':'Choose a known spell, then this slot.';b.dataset.slot=i;
      b.draggable=!!s;b.ondragstart=e=>e.dataTransfer.setData('text/plain',id);b.ondragover=e=>e.preventDefault();b.ondrop=e=>{e.preventDefault();assign(e.dataTransfer.getData('text/plain'),i);};
    }
    $('awSchool').onchange=e=>{category=e.target.value;renderKnown();};
    $('awSpellSearch').oninput=e=>{search=e.target.value;renderKnown();};
    renderKnown();renderSpellDetail();
  }
  function renderKnown(){
    const p=game.player,list=$('awKnownSpells');list.replaceChildren();
    const ids=p.unlocked.filter(id=>SPELLS[id]&&(category==='All'||categories(id).includes(category))&&`${SPELLS[id].name} ${SPELLS[id].desc}`.toLowerCase().includes(search.toLowerCase()));
    if(!ids.length)list.textContent='No known spells match this search.';
    for(const id of ids){const s=SPELLS[id],active=p.activeSpells.indexOf(id),upgradeable=(UPGRADE_POOLS[id]||[]).some(u=>!(p.upgrades[id]||[]).includes(u[0]));const b=action(`${s.icon} ${s.name}${active>=0?' · '+(active+1):''}${upgradeable?' ✧':''}`,()=>{selectedSpell=id;renderKnown();renderSpellDetail();},list,'aw-spell-entry');b.setAttribute('aria-pressed',String(id===selectedSpell));b.draggable=true;b.ondragstart=e=>e.dataTransfer.setData('text/plain',id);}
  }
  function renderSpellDetail(){
    const id=selectedSpell,s=SPELLS[id],detail=$('awSpellDetail');if(!s){detail.textContent='Learn spells from shrines, bosses and town specialists.';return;}
    const p=game.player,m=spellMods(id),ups=p.upgrades[id]||[],reactions=Object.entries(INTENSITY_REACTIONS).filter(([key])=>key.split('+').includes(id));
    detail.innerHTML=`<span class="aw-spell-illustration">${s.icon}</span><h2>${esc(s.name)}</h2><p>${esc(s.rarity)} · ${categories(id).join(' / ')}</p><p>${esc(s.desc)}</p><div class="aw-spell-stats"><b>${Math.round(s.damage*m.power)} power</b><b>${(s.cooldown*m.cdr).toFixed(1)}s cooldown</b></div><div id="awSpellActions"></div><h3>Mutation paths</h3><ul class="aw-mutation-paths">${(UPGRADE_POOLS[id]||[]).map(u=>`<li class="${ups.includes(u[0])?'earned':''}"><b>${ups.includes(u[0])?'✓':'◇'} ${esc(u[1])}</b><span>${esc(u[3])}</span></li>`).join('')}</ul><h3>Reactions</h3><p>${reactions.map(([key,name])=>`${esc(SPELLS[key.split('+').find(x=>x!==id)]?.name||'')} → ${esc(name)}`).join('<br>')||'No paired reaction. Use its effects to support your other spells.'}</p>`;
    action('Equip → Choose a slot',()=>{$('awSlotPicker')?.remove();const slots=document.createElement('div');slots.id='awSlotPicker';slots.className='aw-slot-picker';$('awSpellActions').appendChild(slots);for(let i=0;i<awCurrentSpellLimit();i++)action(`Slot ${i+1}`,()=>assign(id,i),slots);slots.querySelector('button')?.focus();},$('awSpellActions'),'btn');
    if(p.activeSpells.includes(id))action('Unequip',()=>{p.activeSpells=p.activeSpells.map(x=>x===id?null:x);saveGame();updateHUD();renderBook();},$('awSpellActions'));
  }
  function renderMap(){
    const s=A.state();if(!s)return;
    const c=D.continent(selectedContinent)||D.continent(A.current().continent);selectedContinent=c.id;
    content().innerHTML=`<div class="aw-continent-tabs">${D.continents.map(x=>`<button data-continent="${x.id}" aria-pressed="${x.id===c.id}">${s.unlocked.includes(x.id)?'':'🔒 '}${esc(x.name)}</button>`).join('')}</div><div class="aw-map-title"><div><h2>${esc(c.name)}</h2><p>${esc(c.description)}</p></div><span>Threat ${c.range.join('–')} · ${D.sealCount(s,c)}/4 seals</span></div><div class="aw-map-tools"><button id="awMapMinus" aria-label="Zoom out">−</button><button id="awMapPlus" aria-label="Zoom in">+</button><button id="awMapCenter">Center</button><span>Drag to pan · Pinch or scroll to zoom · Arrows to explore</span></div><div class="aw-map-layout"><div id="awMapViewport" tabindex="0" aria-label="World map"><div id="awMapLayer"></div></div><section id="awNodeCard" class="aw-node-card" aria-live="polite"></section></div>`;
    root.querySelectorAll('[data-continent]').forEach(b=>b.onclick=()=>{selectedContinent=b.dataset.continent;selectedNode=D.continent(selectedContinent).start;renderMap();});
    const layer=$('awMapLayer'),visible=c.nodes.map(id=>D.nodes[id]).filter(n=>D.visible(s,n)||n.id===c.start),set=new Set(visible.map(n=>n.id));
    let roads='';for(const n of visible)for(const id of n.connections)if(set.has(id)&&n.id<id&&D.nodes[id].type!=='ruler'&&n.type!=='ruler'){const other=D.nodes[id];roads+=`<path d="M ${n.x} ${n.y} Q ${(n.x+other.x)/2+20} ${(n.y+other.y)/2-15} ${other.x} ${other.y}" class="${s.visited.includes(n.id)&&s.visited.includes(id)?'known':''}"/>`;}
    layer.innerHTML=`<svg class="aw-map-roads" viewBox="0 0 1220 820" aria-hidden="true"><path class="aw-landmass" d="M 85 235 Q 170 10 490 5 L 880 40 Q 1200 25 1190 345 L 1150 725 Q 960 825 665 750 L 220 770 Q 20 635 85 235 Z"/>${roads}</svg>`;
    for(const n of visible){
      const known=s.visited.includes(n.id)||s.scouted.includes(n.id),b=action(`${known?icons[n.type]:'?'} ${known?n.name:'Unexplored '+capitalize(n.biome)}`,()=>{if(dragged)return;selectedNode=n.id;renderNodeCard();},layer,'aw-map-node');
      b.dataset.node=n.id;b.style.left=n.x+'px';b.style.top=n.y+'px';b.style.setProperty('--biome',biomePalette[n.biome]?.accent||c.color);b.classList.toggle('current',n.id===s.current);b.classList.toggle('cleared',s.cleared.includes(n.id));b.classList.toggle('unknown',!known);
      if(s.cleared.includes(n.id))b.textContent+=' ✓';b.title=known?n.name:'Unexplored location';
    }
    $('awMapMinus').onclick=()=>zoomAt(view.zoom/1.2);$('awMapPlus').onclick=()=>zoomAt(view.zoom*1.2);$('awMapCenter').onclick=centerMap;
    bindMapGestures();centerMap();renderNodeCard();
  }
  function transformMap(){const layer=$('awMapLayer');if(!layer)return;layer.style.transform=`translate(${view.x}px,${view.y}px) scale(${view.zoom})`;layer.style.setProperty('--node-scale',1/Math.max(.85,view.zoom));}
  function centerMap(){const box=$('awMapViewport'),n=D.nodes[selectedNode]||D.nodes[D.continent(selectedContinent).start];view={zoom:.85,x:(box.clientWidth||600)/2-n.x*.85,y:(box.clientHeight||440)/2-n.y*.85};transformMap();}
  function zoomAt(value,cx,cy){const box=$('awMapViewport');cx??=(box.clientWidth||600)/2;cy??=(box.clientHeight||440)/2;const old=view.zoom;view.zoom=clamp(value,.65,1.8);view.x=cx-(cx-view.x)*view.zoom/old;view.y=cy-(cy-view.y)*view.zoom/old;transformMap();}
  function bindMapGestures(){
    const box=$('awMapViewport');pointers.clear();gesture=null;
    const local=e=>{const r=box.getBoundingClientRect();return{x:e.clientX-r.left,y:e.clientY-r.top};};
    box.onwheel=e=>{e.preventDefault();const p=local(e);zoomAt(view.zoom*(e.deltaY>0?.88:1.12),p.x,p.y);};
    box.onpointerdown=e=>{if(e.button&&e.button!==0)return;const p=local(e);pointers.set(e.pointerId,p);dragged=false;gesture={start:p,last:p};if(!e.target.closest('button'))box.setPointerCapture?.(e.pointerId);};
    box.onpointermove=e=>{
      if(!pointers.has(e.pointerId))return;const p=local(e),old=[...pointers.values()];pointers.set(e.pointerId,p);
      if(pointers.size===2){const next=[...pointers.values()],before=Math.hypot(old[0].x-old[1].x,old[0].y-old[1].y),after=Math.hypot(next[0].x-next[1].x,next[0].y-next[1].y);if(before>2)zoomAt(view.zoom*after/before,(next[0].x+next[1].x)/2,(next[0].y+next[1].y)/2);dragged=true;}
      else if(gesture){if(Math.hypot(p.x-gesture.start.x,p.y-gesture.start.y)>6)dragged=true;if(dragged){view.x+=p.x-gesture.last.x;view.y+=p.y-gesture.last.y;transformMap();}gesture.last=p;}
    };
    const end=e=>{pointers.delete(e.pointerId);gesture=null;if(!pointers.size)setTimeout(()=>dragged=false,0);};box.onpointerup=end;box.onpointercancel=end;box.onlostpointercapture=end;
  }
  function renderNodeCard(){
    const n=D.nodes[selectedNode]||A.current(),s=A.state(),known=s.visited.includes(n.id)||s.scouted.includes(n.id),c=D.continent(n.continent),card=$('awNodeCard');if(!card)return;
    const reason=D.travelReason(s,n.id,travelMode);
    card.innerHTML=`<small>${known?esc(n.type.replace(/([A-Z])/g,' $1')):'Undiscovered location'}</small><h3>${known?esc(n.name):'Unexplored '+capitalize(n.biome)}</h3><p>${capitalize(n.biome)} · Threat ${n.threat}</p><p>${known&&n.town?esc(D.towns[n.town].culture):'Possible materials: '+esc(MATERIALS[c.material].name)}</p>${known&&!n.town?`<p>Enemies: ${A.pool(n).map(id=>esc(ENEMY_TYPES[id].name)).join(', ')}</p>`:''}<p>${s.cleared.includes(n.id)?'✓ Cleared':s.visited.includes(n.id)?'Discovered':'Follow a connected road to reveal this location.'}</p>`;
    if(known&&n.rewardSpells?.length)card.insertAdjacentHTML('beforeend',`<p>Spell discoveries: ${n.rewardSpells.map(id=>esc(SPELLS[id].name)).join(', ')}</p>`);
    if(n.id===s.current){
      card.insertAdjacentHTML('beforeend','<p>You are here.</p>');
      if(n.type==='ruler'&&s.defeated.includes(n.id))action('Return to Portal City',()=>A.travel(c.portalCity),card);
      if(n.type==='dungeon'){card.insertAdjacentHTML('beforeend',`<p>Rooms cleared: ${(s.dungeonClears[n.id]||[]).length}/5. Use the doors inside to explore the branches.</p>`);}
    }else{const b=action(travelMode==='portal'?'Use Town Portal':'Travel',()=>A.travel(n.id,travelMode),card,'btn');b.disabled=!!reason;if(reason)card.insertAdjacentHTML('beforeend',`<p class="aw-travel-reason">${esc(reason)}</p>`);}
    if(known&&n.town)card.insertAdjacentHTML('beforeend',`<p><b>Specialties:</b> ${D.towns[n.town].stock.map(id=>esc(D.items[id].name)).join(', ')}</p>`);
    if(A.current().town){action(travelMode==='portal'?'Follow roads instead':'Visited town network',()=>{travelMode=travelMode==='portal'?'road':'portal';renderMap();},card);if(travelMode==='portal')for(const id of s.visited.filter(id=>D.isTown(D.nodes[id])&&id!==s.current))action(`⌂ ${D.nodes[id].name}`,()=>A.travel(id,'portal'),card);}
  }
  function openTown(npc){
    const n=A.current(),town=D.towns[n.town];if(!town)return;
    $('npcName').textContent=`${npc.name} • ${npc.role} • ${town.name}`;const body=$('npcBody');body.innerHTML=`<p class="exp-dialogue">${esc(town.line)}</p><p>${esc(town.culture)} · ${game.gold} gold</p><div id="awTownServices" class="aw-card-grid"></div>`;
    const list=$('awTownServices'),role=npc.role;
    if(['Merchant','Weaponsmith','Armorer','Relic Dealer','Blacksmith','Enchanter','Alchemist'].includes(role)){
      const stock=town.stock.filter(id=>{const t=D.items[id];if(role==='Weaponsmith')return t.slot==='weapon';if(role==='Armorer')return t.slot==='armor';if(role==='Alchemist')return t.kind==='heal'||t.kind==='material';if(role==='Relic Dealer'||role==='Enchanter')return t.slot==='trinket'||t.kind==='material';return true;});
      for(const id of stock){const t=D.items[id],locked=t.requires&&!A.state().cleared.includes(t.requires);const b=action(`${t.name} · ${t.price} gold${t.cost?' + '+materialCostText(t.cost):''}${locked?' · Clear '+D.nodes[t.requires].name:''}`,()=>{if(A.buy(id)&&!t.slot)openTown(npc);},list,'exp-buy');b.disabled=!!locked;b.title=t.desc||'Regional material bundle';}
    }
    if(['Blacksmith','Weaponsmith','Armorer','Enchanter'].includes(role))action('Material forge',()=>{closeOverlay('npcPanel');openForgePanel();},list);
    if(['Arcanist','Spell Scribe'].includes(role)||role==='Enchanter'){
      for(const id of town.spells){const s=SPELLS[id];if(!s)continue;const known=game.player.unlocked.includes(id),price=70+rarityRank[s.rarity]*110;const b=action(`${s.icon} ${s.name} · ${known?'Learned':price+' gold'}`,()=>{A.learn(id);openTown(npc);},list,'exp-buy');b.disabled=known;}
    }
    // A fortress can sell a local spell focus even without hosting a whole academy.
    if(role==='Weaponsmith'&&town.spells.length)for(const id of town.spells){const s=SPELLS[id];if(s)action(`Local spell: ${s.name} · ${70+rarityRank[s.rarity]*110} gold`,()=>{A.learn(id);openTown(npc);},list);}
    if(role==='Stablemaster')for(const id of town.mounts){const m=D.mounts[id],owned=A.state().mounts.includes(id);action(`${m.icon} ${m.name} · ${owned?'Ride / Dismiss':m.cost+' gold'}`,()=>{if(owned)A.mount(id);else if(!A.buyMount(id))toastMsg('Not enough gold.');openTown(npc);},list);}
    if(role==='Quest Keeper'){body.insertAdjacentHTML('beforeend','<div id="npcQuestList"></div>');renderNPCPanelQuestList();}
    if(role==='Cartographer')action('Scout neighboring paths · 30 gold',()=>{if(game.gold<30)return toastMsg('Not enough gold.');game.gold-=30;for(const id of n.connections)if(!A.state().scouted.includes(id))A.state().scouted.push(id);saveGame();toastMsg('Nearby locations are marked. Reach them by road to discover their portals.');},list);
    if(role==='Villager')list.textContent=D.portalReady(A.state(),D.continent(n.continent))?'The great portal is singing. The ruler awaits.':`The city’s portal needs all four regional seals. ${D.sealCount(A.state(),D.continent(n.continent))}/4 are lit.`;
    showOverlay('npcPanel');
  }
  function openPortal(){
    const n=A.current(),s=A.state(),c=D.continent(n.continent);$('npcName').textContent=`${D.nodes[c.portalCity].name} • Great Portal`;
    const body=$('npcBody');body.innerHTML=`<p class="exp-dialogue">${s.defeated.includes(c.boss)?'The continent is free. Your next road awaits.':D.portalReady(s,c)?'All runes are burning. Step through and face '+esc(c.ruler)+'.':'The dark stone waits for four regional seals.'}</p><div class="aw-seal-grid">${c.requiredBosses.map(id=>`<p>${s.defeated.includes(id)?'◆':'◇'} ${esc(D.nodes[id].name)} · ${s.defeated.includes(id)?'Defeated':'Alive'}</p>`).join('')}</div>`;
    const b=action(s.defeated.includes(c.boss)?'Ruler defeated':'Enter the Continent Boss portal',()=>A.enterPortal(),body,'btn');b.disabled=!D.portalReady(s,c)||s.defeated.includes(c.boss);
    showOverlay('npcPanel');
  }
  function openSite(){
    const n=A.current(),s=A.state(),c=D.continent(n.continent),body=$('npcBody');$('npcName').textContent=n.name;
    body.innerHTML=`<p class="exp-dialogue">${esc(({mount:'A wild companion watches you from the clearing.',landmark:'Old stones remember the roads between the continents.',shrine:'A forgotten spell waits in the light.',passage:c.travel,merchant:'A small caravan trades local supplies.',event:'The danger has passed. Search the aftermath for supplies and a traveler’s blessing.'})[n.type]||n.name)}</p>`;
    if(n.rewardSpells?.length)body.insertAdjacentHTML('beforeend',`<p>Spell discoveries: ${n.rewardSpells.map(id=>esc(SPELLS[id].name)).join(', ')}</p>`);
    if(n.type==='passage'){
      const i=D.continents.indexOf(c);for(const target of D.continents.filter((_,j)=>Math.abs(j-i)===1)){const b=action(`Travel to ${target.name}`,()=>A.travel(target.start,'passage'),body,'btn');b.disabled=!s.unlocked.includes(target.id);}
      if(i===2)body.insertAdjacentHTML('beforeend','<p>The final horizon is yours. Return through the towns to finish optional discoveries.</p>');
    }else if(n.type==='merchant'){openCamp();return;}
    else{const b=action(s.claimed.includes(n.id)?'Reward claimed':'Discover reward',()=>{A.claimSite();openSite();},body,'btn');b.disabled=s.claimed.includes(n.id);}
    showOverlay('npcPanel');
  }
  function openCamp(){
    const body=$('npcBody');$('npcName').textContent=A.current().name;body.innerHTML='<p>A traveling caravan offers healing and the local material.</p>';
    action('Healing draught · 25 gold',()=>spendGold(25,()=>healPlayer(game.player.maxHp*.5)),body);
    const key=D.continent(A.current().continent).material;action(`${MATERIALS[key].name} × 4 · 50 gold`,()=>spendGold(50,()=>addMaterial(key,4,true)),body);showOverlay('npcPanel');
  }
  renderInventory=function(){selectedContinent=A.current()?.continent||'verdant';selectedNode=A.current()?.id;renderPage('Character');};
  $('inventoryBtn').onclick=()=>open('Character');$('inventoryBtn').title='Adventure Journal';$('inventoryBtn').textContent='☰';
  const mapButton=action('🗺',()=>open('Map'),$('buttons'),'round-btn');mapButton.id='awMapButton';mapButton.title='World Map (M)';mapButton.setAttribute('aria-label','World Map');
  const mountButton=action('♞',()=>{if(!A.mount())open('Character');},$('buttons'),'round-btn');mountButton.id='awMountButton';mountButton.title='Mount / Dismiss (R)';mountButton.setAttribute('aria-label','Mount or dismiss');
  // Capture menu keys before the gameplay input layer sees them.
  addEventListener('keydown',e=>{
    if(!game.player||e.ctrlKey||e.metaKey||e.altKey)return;
    const typing=/INPUT|SELECT|TEXTAREA/.test(e.target?.tagName),visible=isOpen();
    if(visible&&e.key==='Escape'){e.preventDefault();e.stopImmediatePropagation();close();return;}
    if(visible&&e.key==='Tab'){
      const list=Array.from(panel.querySelectorAll('button,input,select')).filter(el=>!el.disabled);const i=list.indexOf(document.activeElement);
      if(e.shiftKey&&i<=0){e.preventDefault();list.at(-1)?.focus();}else if(!e.shiftKey&&i===list.length-1){e.preventDefault();list[0]?.focus();}return;
    }
    if(visible&&!typing&&page==='Map'&&['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','w','a','s','d'].includes(e.key)){
      e.preventDefault();e.stopImmediatePropagation();const buttons=Array.from(root.querySelectorAll('[data-node]')),from=D.nodes[document.activeElement?.dataset.node]||D.nodes[selectedNode]||A.current();
      const dir=({ArrowLeft:[-1,0],a:[-1,0],ArrowRight:[1,0],d:[1,0],ArrowUp:[0,-1],w:[0,-1],ArrowDown:[0,1],s:[0,1]})[e.key];
      const next=buttons.map(b=>({b,n:D.nodes[b.dataset.node]})).filter(({n})=>(n.x-from.x)*dir[0]+(n.y-from.y)*dir[1]>10).sort((a,b)=>Math.hypot(a.n.x-from.x,a.n.y-from.y)-Math.hypot(b.n.x-from.x,b.n.y-from.y))[0];
      if(next){selectedNode=next.n.id;next.b.focus({preventScroll:true});renderNodeCard();centerMap();}return;
    }
    if(visible&&!typing&&e.key==='Enter'&&document.activeElement?.dataset.node){e.preventDefault();e.stopImmediatePropagation();document.activeElement.click();return;}
    if(!visible&&!typing&&!modalPause&&!paused&&!e.repeat){
      // User-remapped actions take precedence over the journal convenience keys.
      const mapped=Object.values(window.AWInput?.mapping()||{}).includes(e.code);if(mapped)return;
      if(e.code==='KeyM'||e.code==='KeyB'||e.code==='KeyR'){e.preventDefault();e.stopImmediatePropagation();if(e.code==='KeyR')A.mount();else open(e.code==='KeyM'?'Map':'Spellbook');}
    }
  },true);
  // Keep all spell controls routed through the existing buffered action input.
  $('mobileDodge').onclick=()=>window.AWInput.press('dodge');
  window.AWCampaignUI={open,close,isOpen,openTown,openPortal,openSite,openCamp,assign,categories};
})();
