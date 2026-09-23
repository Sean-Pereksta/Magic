'use strict';
/* Contextual home UI: world placement, not a separate economy dashboard. */
(() => {
  const H=AWHome,C=AWHomeCore,D=AWCampaignData;
  let page='overview',selected=null,placing=null,previousFocus=null,lastTick=0;
  const el=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
  const overlay=el('div',undefined,'overlay hidden');overlay.id='awHomePanel';
  const panel=el('section',undefined,'panel aw-home-panel');panel.setAttribute('role','dialog');panel.setAttribute('aria-modal','true');panel.setAttribute('aria-label','Hearthglade home');
  const head=el('header',undefined,'aw-home-head'),heading=el('div');heading.append(el('small','YOUR PLACE BETWEEN ADVENTURES'),el('h2','Hearthglade'));head.append(heading);
  const body=el('div',undefined,'aw-home-body'),status=el('p','Local save','aw-home-status'),notice=el('p','','aw-home-message');status.setAttribute('role','status');notice.setAttribute('role','alert');
  const nav=el('nav',undefined,'aw-home-tabs');nav.setAttribute('aria-label','Homestead sections');
  function button(parent,text,fn,disabled=false){const b=el('button',text,'btn secondary');b.type='button';b.disabled=disabled;b.onclick=()=>{if(!H.pending())fn();};parent.appendChild(b);return b;}
  button(head,'Close',()=>close());panel.append(head,status,notice,nav,body);overlay.append(panel);document.body.append(overlay);
  const tray=el('div',undefined,'aw-home-tray hidden');tray.id='awHomeTray';tray.setAttribute('aria-label','Home controls');button(tray,'⌂ Home',()=>open());button(tray,'Build',()=>open('build'));button(tray,'Garden',()=>open('garden'));document.body.append(tray);
  const placeBar=el('div',undefined,'aw-home-place hidden');placeBar.setAttribute('role','region');placeBar.setAttribute('aria-label','Building placement controls');document.body.append(placeBar);
  const time=ms=>ms<=0?'Ready':ms<C.HOUR?`${Math.ceil(ms/C.MINUTE)}m`:`${Math.ceil(ms/C.HOUR*10)/10}h`;
  const cost=x=>Object.entries(x).map(([k,v])=>`${v} ${k==='gold'?'gold':MATERIALS[k]?.name||k}`).join(' · ');
  function paragraph(text,parent=body){return parent.appendChild(el('p',text));}
  function card(title,description,parent=body){const c=el('section',undefined,'aw-home-card');c.append(el('h3',title));if(description)c.append(el('p',description));parent.appendChild(c);return c;}
  const doAction=async(type,args={})=>{notice.textContent='';await H.action(type,args);};
  function visible(){return !overlay.classList.contains('hidden');}
  function open(next='overview',id=null){
    if(!game.player||!running||roomTransition)return;
    if(H.pending())return;
    if(!H.atHome()&&!D.isTown(AWCampaign.current()))return H.startRecall();
    previousFocus=document.activeElement;AWCampaignUI.close();document.querySelectorAll('#npcPanel,#townPanel').forEach(e=>e.classList.add('hidden'));
    cancelPlacement(false);page=next;selected=id;overlay.classList.remove('hidden');modalPause=true;AWInput.clear();notice.textContent='';render();head.querySelector('button').focus();
  }
  function close(){if(H.pending())return;overlay.classList.add('hidden');cancelPlacement(false);modalPause=Array.from(document.querySelectorAll('.overlay')).some(e=>!e.classList.contains('hidden'));AWInput.clear();previousFocus?.focus?.();}
  function refreshStatus(){status.textContent=window.AWHomeCloud?.status()||'Local save';head.querySelector('button').disabled=H.pending();placeBar.querySelectorAll('button').forEach(b=>b.disabled=H.pending());}
  function refresh(){tray.classList.toggle('hidden',!running||!H.atHome()||!!placing);if(visible())render();refreshStatus();}
  function render(){
    if(!game.player)return;const h=H.snapshot();body.replaceChildren();nav.replaceChildren();
    for(const [id,name]of [['overview','Overview'],['build','Build'],['garden','Garden'],['pantry','Pantry'],['portals','Portals'],['supplies','Supplies']]){
      const b=button(nav,name,()=>{page=id;selected=null;render();});b.setAttribute('aria-current',page===id?'page':'false');
    }
    refreshStatus();
    if(!H.atHome()&&!['supplies','pantry','overview'].includes(page)){paragraph('Return to Hearthglade to build and tend your home.');button(body,'Recall home',()=>{close();H.startRecall();});return;}
    ({overview,build,garden,pantry,portals,supplies,plot:plotView,item:itemView,water:waterView})[page]?.(h);
  }
  function overview(h){
    const ready=h.plots.filter(C.ready).length,dry=h.plots.filter(p=>p.plant&&!C.ready(p)&&p.plant.wateredUntil<=H.now()).length,stored=h.items.reduce((n,i)=>n+(i.stored||0),0);
    card(C.tiers[h.tier].name,`${ready} harvests ready · ${dry} growing spaces dry · ${stored} stored supplies`);
    const quest=card('A Place to Return',!h.deed?'Clear a marked dangerous multi-wave site, then follow the western Sunmere road to Hearthglade.':!h.tier?'Your deed and starter supplies are ready. Build the cottage, place furniture, then plant and water a crop.':!h.firstWatered?'Furnish your house, prepare a garden bed, and plant and water your first crop.':'Your home is established. Explore for materials, expand the house, and maintain your garden.');
    if(!H.atHome()){button(quest,'Open world map',()=>{close();AWCampaignUI.open('Map');});if(h.tier)button(quest,'Recall home',()=>{close();H.startRecall();});}
    else{
      if(!h.tier)button(quest,'Build cottage · '+cost(C.tiers[1].cost),()=>doAction('buildHouse'),!h.deed);
      else button(quest,H.inside()?'Leave house':'Enter house',()=>{close();H.inside()?H.leaveHouse():H.enterHouse();});
      button(body,'Collect ready supplies',()=>doAction('collect'));
      button(body,'Return to adventure',()=>{close();H.returnAdventure();});
    }
    paragraph('Dry plants pause rather than die. Ready harvests never spoil. A fruit tree stores only one harvest.');
    paragraph(h.mode==='cloud'?'Cloud home actions are confirmed by Firebase before resources change. Reopen this named journey on another device; concurrent edits are rejected.':'This home is local until you assign an Official Firebase Save. Local clocks are device-based.');
    button(body,'Official Firebase Save',()=>{close();awOfficialSave();});
    if(h.mode==='cloud')button(body,'Reopen cloud journey',()=>{close();awCloudOpenModal('load');});
  }
  function build(h){
    paragraph(`${game.gold} gold · ${game.materials.timber||0} Timber · ${game.materials.stone||0} Stone · ${game.materials.fiber||0} Plant Fiber`);
    if(h.tier<4){const t=C.tiers[h.tier+1];button(body,`${h.tier?'Expand to':'Build'} ${t.name} · ${cost(t.cost)}`,()=>doAction('buildHouse'),!h.deed);}
    if(!h.tier){paragraph('Earn the deed and build the cottage to unlock furniture placement.');return;}
    button(body,'Move house footprint',()=>startPlacement('house'));
    paragraph('Craft a furnishing, then place its preview in the world. Interior partitions form rooms; doors, rugs and floor inlays do not block movement.');
    if(h.items.length){const owned=el('div',undefined,'aw-home-grid');body.append(el('h3','Owned furnishings'),owned);
      for(const i of h.items){const d=C.items[i.kind],c=card(d.icon+' '+d.name,`${i.packed?'Packed':'Placed'}${d.producer?' · Level '+i.level:''}`,owned);button(c,i.packed?'Place':'Move',()=>startPlacement('item',i.id));button(c,'Inspect',()=>open('item',i.id));}
    }
    const grid=el('div',undefined,'aw-home-grid');body.append(el('h3','Blueprints'),grid);
    for(const [id,d]of Object.entries(C.items)){const c=card(`${d.icon} ${d.name}`,cost(d.cost),grid);paragraph(d.outdoor?'Workshop yard':d.portal?'House portal':d.producer?'Indoor production':'Interior furnishing',c);button(c,`Craft${d.tier>h.tier?' · Tier '+d.tier+' required':''}`,()=>doAction('craft',{kind:id}),h.tier<(d.tier||1));}
  }
  function garden(h){
    paragraph(`${h.plots.filter(p=>!p.tree).length}/${C.tiers[h.tier].beds} crop beds · ${h.plots.filter(p=>p.tree).length}/${C.tiers[h.tier].trees} orchard plots · ${h.water}/${h.canLevel>1?24:6} watering charges`);
    button(body,'Place garden bed · 3 Timber + 1 Fiber',()=>startPlacement('bed'),!h.tier);
    button(body,'Place orchard plot · 10 gold + 4 Timber',()=>startPlacement('tree'),h.tier<2);
    button(body,'Water beds needing attention',()=>doAction('water'));
    button(body,'Well and irrigation',()=>open('water'));
    paragraph('Click a planted bed in the world to tend it. Basic beds hold 8 hours of water; improved beds 12, greenhouse beds 16, and trees 24. Watering early refills, rather than stacks, that limit.');
    const grid=el('div',undefined,'aw-home-grid');body.append(grid);
    for(const p of h.plots){const d=C.crops[p.plant?.crop],remaining=p.plant?p.plant.requiredGrowthMs-p.plant.growthMs:0;
      const c=card(d?.name||(p.tree?'Empty orchard plot':'Empty garden bed'),!p.plant?'Plant a seed':C.ready(p)?'Harvest ready':`${Math.round(p.plant.growthMs/p.plant.requiredGrowthMs*100)}% · ${time(remaining)} watered growth remaining · ${p.plant.wateredUntil>H.now()?'Watered':'Dry / paused'}`,grid);
      c.querySelector('p').dataset.homePlot=p.id;
      button(c,'Tend',()=>open('plot',p.id));const harvest=button(c,'Harvest',()=>doAction('harvest',{id:p.id}),!C.ready(p));harvest.dataset.homeHarvest=p.id;
    }
    paragraph('Buy seeds from settlement home suppliers. Later continents sell their own crops and saplings.');
  }
  function plotView(h){
    const p=h.plots.find(p=>p.id===selected);if(!p)return garden(h);const crop=C.crops[p.plant?.crop];
    card(crop?.name||(p.tree?'Orchard plot':'Garden bed'),`Position ${p.x}, ${p.y} · ${C.waterHours(p)} hours of moisture`);
    if(!p.plant){for(const [id,c]of Object.entries(C.crops))if(!!c.tree===p.tree){const count=h.seeds[id]||0;button(body,`Plant ${c.name} · ${count} seeds`,()=>doAction('plant',{id:p.id,crop:id}),!count);}}
    else{
      paragraph(`Planted ${new Date(p.plant.plantedAt).toLocaleString()} · Harvest cycle ${p.plant.harvestNumber+1}`);
      paragraph(C.ready(p)?'Ready to harvest. It will not spoil.':`${time(p.plant.requiredGrowthMs-p.plant.growthMs)} of watered growth remains. ${p.plant.wateredUntil>H.now()?'Moisture lasts '+time(p.plant.wateredUntil-H.now()):'Dry: growth is paused.'}`);
      button(body,'Water · 1 watering charge',()=>doAction('water',{id:p.id}),C.ready(p));
      button(body,'Harvest',()=>doAction('harvest',{id:p.id}),!C.ready(p));
      button(body,`Fertilize · ${h.fertilizer} available`,()=>doAction('fertilize',{id:p.id}),!h.fertilizer||p.plant.fertilized||C.ready(p));
      button(body,'Remove plant',()=>{if(confirm('Remove this plant without a harvest? Seeds are not refunded.'))doAction('removePlant',{id:p.id});});
    }
    if(!p.tree&&p.soil<2)button(body,p.soil?'Build greenhouse bed · 40 gold + 2 Frost Crystal + 2 Emberglass':'Improve soil · 15 gold + 4 Plant Fiber',()=>doAction('upgradeSoil',{id:p.id}));
  }
  function waterView(h){
    paragraph(`Watering can: ${h.water}/${h.canLevel>1?24:6} charges. Refill from the home well. Reservoirs are finite and continue supplying connected beds while you are away.`);
    button(body,'Refill watering can',()=>doAction('refillCan'));
    if(h.canLevel===1)button(body,'Upgrade can · 50 gold + 4 Iron Shards',()=>doAction('upgradeCan'));
    button(body,'Water beds needing attention',()=>doAction('water'));
    for(const i of h.items.filter(i=>i.kind==='cistern'&&!i.packed)){const c=card('Water cistern',`${i.water||0}/32 bed-waterings remaining`);button(c,'Refill reservoir',()=>doAction('refillCistern',{id:i.id}));}
    paragraph('Each placed sprinkler feeds its four nearest annual beds through buried garden channels. It consumes one reservoir unit when a growing bed dries. Mature crops consume no water.');
  }
  function itemView(h){
    const i=h.items.find(i=>i.id===selected);if(!i)return build(h);const d=C.items[i.kind];card(d.icon+' '+d.name,i.packed?'Packed furnishings are inactive.':`Placed at ${i.x}, ${i.y}`);
    button(body,i.packed?'Place':'Move',()=>startPlacement('item',i.id));
    if(!i.packed)button(body,'Pack furnishing',()=>doAction('pack',{id:i.id}));
    if(d.producer){paragraph(`${i.stored||0}/${d.producer.cap*(i.level||1)} ${d.producer.resource} stored. Production pauses at capacity and while packed.`);button(body,'Collect output',()=>doAction('collect',{id:i.id}));if(i.level<3)button(body,`Upgrade production · ${cost({gold:60*i.level,iron:4*i.level,dust:2*i.level})}`,()=>doAction('upgradeProducer',{id:i.id}));}
    if(i.packed){button(body,'Dismantle for 50% base materials',()=>{if(confirm('Dismantle this packed furnishing? Gift furniture has no refund.'))doAction('dismantle',{id:i.id});});return;}
    if(['bed','hearth'].includes(i.kind)){button(body,i.kind==='bed'?'Rest · Full health + 5% experience for 3 cleared encounters':'Rest · Full health',()=>doAction('rest',{id:i.id}));}
    if(d.portal){paragraph(i.destination?'Attuned to '+i.destination.name:'Attune this frame to an earned destination.');for(const dest of destinations(i)){button(body,'Attune: '+dest.name,()=>doAction('attune',{id:i.id,destination:dest}));}button(body,'Travel through portal',()=>{close();H.travelPortal(i.id);},!i.destination);}
    if(['stove','alchemy','pantry','composter'].includes(i.kind))button(body,'Food, recipes and fertilizer',()=>open('pantry'));
    if(['cistern','sprinkler'].includes(i.kind))button(body,'Manage water',()=>open('water'));
    if(i.kind==='lectern')button(body,'Open spellbook',()=>{close();AWCampaignUI.open('Spellbook');});
    if(i.kind==='chest')button(body,'Open shared inventory',()=>{close();AWCampaignUI.open('Inventory');});
    if(i.kind==='workbench')button(body,'Building blueprints',()=>open('build'));
    if(i.kind==='seedCabinet')button(body,'Manage garden',()=>open('garden'));
    if(i.kind==='stable')button(body,'Choose owned mount',()=>{close();AWCampaignUI.open('Character');});
    if(i.kind==='trophy')paragraph(AWCampaign.state().defeated.map(id=>D.nodes[id]?.name||id).join(' · ')||'Defeat a regional boss to display its achievement here.');
  }
  function destinations(i){const ctx=H.context(),list=Object.values(D.nodes).filter(n=>n.town).map(n=>({id:n.id,name:n.name,town:true,continent:n.continent}));if(ctx.shadowUnlocked)for(const depth of ctx.shadowWaystones)list.push({shadow:true,depth,name:'Shadow Sanctuary · Depth '+depth});return list.filter(dest=>!C.portalReason(i,dest,ctx));}
  function portals(h){paragraph('Walk up to an attuned portal or use its Travel action. Portals never unlock undiscovered settlements, continents or Shadow checkpoints.');
    for(const i of h.items.filter(i=>C.items[i.kind].portal)){const c=card(C.items[i.kind].name,i.packed?'Packed':i.destination?.name||'Unattuned');button(c,'Manage',()=>open('item',i.id));}
    if(!h.items.some(i=>C.items[i.kind].portal))button(body,'Craft a portal frame',()=>open('build'));
    if(H.atHome())button(body,'Return to adventure',()=>{close();H.returnAdventure();});
  }
  function pantry(h){
    paragraph('Produce is shared by your recipes, merchant sales and composter. Prepared food restores health without adding combat HUD buttons.');
    const grid=el('div',undefined,'aw-home-grid');body.append(grid);
    for(const [id,c]of Object.entries(C.crops))if(h.produce[id]){const box=card(c.name,`${h.produce[id]} harvested`,grid);if(H.atHome())button(box,'Compost 2 + 1 Fiber',()=>doAction('compost',{crop:id}));else button(box,'Sell one · '+c.value+' gold',()=>doAction('sell',{crop:id}));}
    for(const [id,r]of Object.entries(C.recipes)){const box=card(r.name,`${Object.entries(r.needs).map(([k,v])=>v+' '+C.crops[k].name).join(' + ')} · Heals ${r.heal*100}%`,grid);if(H.atHome())button(box,'Prepare at '+C.items[r.station].name,()=>doAction('cook',{recipe:id}));button(box,`Eat · ${h.meals[id]||0} prepared`,()=>doAction('eat',{recipe:id}),!h.meals[id]);}
  }
  function supplies(h){
    if(!D.isTown(AWCampaign.current())){paragraph('Visit any settlement home supplier for building basics. Seeds are sold on their native continent.');button(body,'World map',()=>{close();AWCampaignUI.open('Map');});return;}
    card(AWCampaign.current().name+' · Home Supplier',`${game.gold} gold · Timber, Stone and Plant Fiber replenish here.`);
    for(const id of ['timber','stone','fiber'])button(body,`Buy 10 ${MATERIALS[id].name} · ${id==='fiber'?10:20} gold`,()=>doAction('buyMaterials',{resource:id,count:10}));
    const grid=el('div',undefined,'aw-home-grid');body.append(grid);
    for(const [id,c]of Object.entries(C.crops))if(c.continent===AWCampaign.current().continent){const box=card(c.name,`${c.tree?'Sapling':'Seeds'} · ${c.hours} watered hours${c.repeat?' to establish, then '+c.repeat+'h per harvest':''} · Owned ${h.seeds[id]||0}`,grid);button(box,'Buy one · '+c.seed+' gold',()=>doAction('buySeeds',{crop:id,count:1}));}
    button(body,'Sell produce / eat meals',()=>open('pantry'));
  }
  function placementRect(){
    if(!placing)return null;const h=H.state();let w=1,hh=1,reason='';
    if(placing.type==='item'){const i=h.items.find(i=>i.id===placing.id),candidate={...i,x:placing.x,y:placing.y,rotation:placing.rotation,packed:false};const r=C.footprint(candidate);w=r.w;hh=r.h;reason=C.placement(h,candidate);}
    else if(placing.type==='house'){w=4;hh=3;if(!C.contains(C.clearing,{x:placing.x,y:placing.y,w,h:hh}))reason='Keep the house in its clearing.';}
    else{const tree=placing.type==='tree';w=hh=tree?2:1;const r={x:placing.x,y:placing.y,w,h:hh};if(!C.contains(tree?C.orchard:C.garden,r))reason=tree?'Use the orchard grounds.':'Use the fertile garden.';else if(h.plots.some(p=>C.overlaps(r,{x:p.x,y:p.y,w:p.tree?2:1,h:p.tree?2:1})))reason='This growing space is already occupied.';}
    return{x:placing.x,y:placing.y,w,h:hh,valid:!reason,reason};
  }
  function updatePlacement(){const r=placementRect();H.setPreview(r);const text=placeBar.querySelector('p');text.textContent=`${r.valid?'Ready to place':r.reason} · ${placing.x}, ${placing.y} · Tap the ground or use arrows`;const confirmButton=placeBar.querySelector('[data-confirm]');confirmButton.disabled=!r.valid||H.pending();}
  function startPlacement(type,id){
    const h=H.state(),item=h.items.find(i=>i.id===id),outside=type!=='item'||C.items[item.kind].outdoor;
    close();if(outside&&H.inside())H.leaveHouse();if(!outside&&!H.inside())H.enterHouse();
    let zone=type==='house'?C.clearing:type==='tree'?C.orchard:type==='bed'?C.garden:outside?C.workshop:C.interior(h);
    placing={type,id,x:item?.x??zone.x,y:item?.y??zone.y,rotation:item?.rotation||0};
    if(type==='item'&&item.packed||type==='bed'||type==='tree'){
      let found=false;for(let y=zone.y;y<zone.y+zone.h&&!found;y++)for(let x=zone.x;x<zone.x+zone.w&&!found;x++){placing.x=x;placing.y=y;if(placementRect().valid)found=true;}
    }
    placeBar.replaceChildren();placeBar.append(el('p',''));const actions=el('div',undefined,'aw-home-place-actions');placeBar.append(actions);
    for(const [label,dx,dy]of[['←',-1,0],['↑',0,-1],['↓',0,1],['→',1,0]])button(actions,label,()=>{placing.x+=dx;placing.y+=dy;updatePlacement();});
    if(type==='item')button(actions,'Rotate',()=>{placing.rotation=(placing.rotation+1)%4;updatePlacement();});
    const confirmButton=button(actions,'Confirm',async()=>{
      const p={...placing};let ok=false;
      if(p.type==='item')ok=await H.action('place',{id:p.id,x:p.x,y:p.y,rotation:p.rotation});
      else if(p.type==='house')ok=await H.action('moveHouse',{x:p.x,y:p.y});
      else ok=await H.action('bed',{x:p.x,y:p.y,tree:p.type==='tree'});
      if(ok){cancelPlacement();open(p.type==='item'||p.type==='house'?'build':'garden');}else if(placing)updatePlacement();
    });confirmButton.dataset.confirm='true';button(actions,'Cancel',()=>{cancelPlacement();open('build');});
    document.body.classList.add('aw-home-building');placeBar.classList.remove('hidden');tray.classList.add('hidden');modalPause=true;AWInput.clear();updatePlacement();
  }
  function cancelPlacement(resume=true){placing=null;H.setPreview(null);placeBar.classList.add('hidden');document.body.classList.remove('aw-home-building');if(resume)modalPause=visible();AWInput.clear();}
  canvas.addEventListener('pointerdown',e=>{
    if(!placing||H.pending())return;e.preventDefault();e.stopPropagation();const r=canvas.getBoundingClientRect(),zoom=AWPresentation.camera.zoom||1,origin=worldToScreen(0,0),sx=(e.clientX-r.left)*W/(r.width||W),sy=(e.clientY-r.top)*Hh()/(r.height||Hh());
    const a=(sx-origin.x)/(TILE_W*.5*zoom),b=(sy-origin.y)/(TILE_H*.5*zoom);placing.x=Math.floor((a+b)/2);placing.y=Math.floor((b-a)/2);updatePlacement();
  },true);
  // Avoid shadowing the game's global H (canvas height) with the home adapter.
  function Hh(){return window.innerHeight;}
  document.addEventListener('keydown',e=>{
    if(placing){const delta={ArrowLeft:[-1,0],ArrowRight:[1,0],ArrowUp:[0,-1],ArrowDown:[0,1]}[e.key];if(delta){e.preventDefault();e.stopImmediatePropagation();placing.x+=delta[0];placing.y+=delta[1];updatePlacement();}else if(e.key.toLowerCase()==='r'&&placing.type==='item'){e.preventDefault();placing.rotation=(placing.rotation+1)%4;updatePlacement();}else if(e.key==='Escape'){e.preventDefault();cancelPlacement();open('build');}return;}
    if(visible()&&e.key==='Escape'){e.preventDefault();e.stopImmediatePropagation();close();}
    if(visible()&&e.key==='Tab'){const focusables=[...panel.querySelectorAll('button:not(:disabled)')];if(!focusables.length)return;const first=focusables[0],last=focusables.at(-1);if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}
  },true);
  const oldTown=AWCampaignUI.openTown;AWCampaignUI.openTown=function(npc){oldTown(npc);button($('npcBody'),'⌂ Home supplies and seeds',()=>open('supplies'));};
  // Context actions live in the existing journal, never over the combat spell buttons.
  const oldOpen=AWCampaignUI.open;AWCampaignUI.open=function(...args){oldOpen(...args);installJournalLinks();};
  function installJournalLinks(){const root=$('inventoryContent');if(!root||!AWCampaignUI.isOpen()||root.querySelector('[data-home-link]'))return;const b=button(root,H.atHome()?'⌂ Manage Hearthglade':'⌂ Hearthglade / Recall',()=>H.atHome()?open():H.startRecall());b.dataset.homeLink='true';}
  function liveGarden(){
    if(!visible())return;const h=H.snapshot();
    for(const text of body.querySelectorAll('[data-home-plot]')){const p=h.plots.find(p=>p.id===text.dataset.homePlot);if(!p)continue;text.textContent=!p.plant?'Plant a seed':C.ready(p)?'Harvest ready':`${Math.round(p.plant.growthMs/p.plant.requiredGrowthMs*100)}% · ${time(p.plant.requiredGrowthMs-p.plant.growthMs)} watered growth remaining · ${p.plant.wateredUntil>H.now()?'Watered':'Dry / paused'}`;}
    for(const b of body.querySelectorAll('[data-home-harvest]'))b.disabled=H.pending()||!C.ready(h.plots.find(p=>p.id===b.dataset.homeHarvest)||{});
  }
  const oldRender=render;render=function(){const result=oldRender();if(running&&performance.now()-lastTick>1000){lastTick=performance.now();tray.classList.toggle('hidden',!H.atHome()||!!placing);installJournalLinks();liveGarden();refreshStatus();}return result;};
  window.AWHomeUI={open,close,refresh,refreshStatus,message:text=>{notice.textContent=text||'';},startPlacement,cancelPlacement};
})();
