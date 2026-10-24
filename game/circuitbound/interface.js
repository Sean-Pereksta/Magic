/* Circuitbound's DOM interface. Gameplay remains in app.js and its simulation modules. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitInterface = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const modes = {
    mine: 'Hold on a block to mine',
    combat: 'Attack toward your aim · Tab changes weapon · Z heals',
    build: 'Place the selected component · R rotates',
    interact: 'Use doors, switches, storage and machines',
    inspect: 'Aim at a component to read its connections'
  };
  const craftCategoryLabels={all:'All',resources:'Resources',mining:'Mining',structure:'Structures',vertical:'Vertical Movement',power:'Power',motion:'Mechanical',logic:'Logic',logistics:'Logistics',weapons:'Weapons and Defense',defense:'Defenses',automation:'Automation'};
  const difficultyText = {
    casual: 'Slower enemy escalation and forgiving recovery. Room to learn and explore.',
    standard: 'A steady survival challenge with recoverable equipment.',
    hardcore: 'Stronger enemies, faster raids, and greater pressure on your resources.'
  };
  function placement(key,x,y,rot=0,meta) { return {key,x,y,rot,...(meta?{meta}: {})}; }
  function plan(id,title,subtitle,description,placements,notes,links=[]) {
    const cost={};
    for (const block of placements) cost[block.key]=(cost[block.key]||0)+1;
    return {id,title,name:title,subtitle,description,placements,cost,notes,links};
  }
  const blueprints = [
    plan('automatic-miner','Automatic miner','Power → drive → real ore',
      'A downward drill sends ore through a hopper into storage. Both generators are required: the motor supplies rotation and the drill receives electrical power.',[
        placement('generator',3,0),placement('motor',3,1,1),placement('drill',3,2,1),placement('generator',4,2),placement('hopper',2,2,2),placement('storage',1,2,2)
      ],'Place the drill above mineable rock, leaving the machine cells clear. It stops safely when storage is full; take items from storage to restart.'),
    plan('elevator','Mine elevator','Drive → winch → linked deck',
      'A powered winch lifts a separately linked three-block platform. The switch changes the lift between raising and lowering; keep the shaft clear for the platform and its rider.',[
        placement('generator',3,0),placement('motor',3,1,1),placement('winch',3,2,1),placement('switch',4,2,2,{on:false}),placement('platform',2,5),placement('platform',3,5),placement('platform',4,5)
      ],'The platform is linked as its own structure. The drive and winch stay anchored. Toggle the switch to reverse the lift. Use ladders as a return route while extending your shaft.',[[[2,5],[3,5],[4,5]]]),
    plan('counterweight-lift','Counterweighted lift','Linked load ↔ weight · guided track',
      'The driven winch moves the linked deck and a separately linked counterweight in opposite directions. Guide rails stop the lift at the track ends. Both bodies require a clear shaft.',[
        placement('generator',3,0),placement('motor',3,1,1),placement('winch',3,2,1),placement('switch',4,2,2,{on:true}),placement('platform',2,6),placement('platform',3,6),placement('platform',4,6),placement('counterweight',7,5),
        ...[4,5,6,7,8].flatMap(y=>[placement('rail',1,y),placement('rail',5,y),placement('rail',8,y)])
      ],'Only the deck and counterweight are linked, as two separate structures. Raising the deck lowers the weight; the switch reverses both. Extend matching guide rails before travelling farther.',[[[2,6],[3,6],[4,6]],[[7,5]]]),
    plan('ore-processing','Ore-processing line','Iron → crushed ore → ingots',
      'A motor drives the crusher. Electrical power reaches the crusher and smelter from below; ingots pass into the storage bin.',[
        placement('generator',3,0),placement('motor',3,1,1),placement('crusher',3,2,0,{recipe:'crushedIron'}),placement('generator',3,3),placement('hopper',2,2),placement('smelter',4,2,0,{recipe:'ironIngot'}),placement('generator',4,3),placement('storage',5,2)
      ],'Insert raw iron into the input hopper. Select recipes by interacting with a machine. The line pauses if an ingredient, power, drive, or output space is missing.'),
    plan('defense-turret','Automated defense','Sensor → signal → armed turret',
      'An enemy sensor sends a signal down to the powered turret. A hopper feeds physical ammunition into its rear input.',[
        placement('turret',3,2),placement('generator',3,3),placement('hopper',2,2),placement('enemySensor',3,0,1),placement('signal',3,1)
      ],'Load ammunition into the hopper before a raid. With no ammunition or power, the turret cannot fire. Keep its field of fire clear.'),
    plan('sensor-door','Sensor-operated door','Player → sensor → doorway',
      'A player sensor sends a signal into the door’s top port. A generator below supplies the power needed to operate it.',[
        placement('sensor',1,0,1),placement('signal',1,1),placement('autoDoor',1,2),placement('generator',1,3)
      ],'The door opens when the nearby player activates the sensor. Use the Lens to distinguish the signal path from the power connection.')
  ];
  const icons={generator:'⚡',motor:'⚙',drill:'▼',excavator:'▧',hopper:'▽',storage:'▤',platform:'═',winch:'◎',switch:'⇄',crusher:'✥',smelter:'♨',turret:'⌖',enemySensor:'◉',sensor:'◉',signal:'⌁',autoDoor:'▯'};
  const names={autoDoor:'Automated Door',enemySensor:'Enemy Sensor',ammoFabricator:'Ammunition Fabricator',platform:'Wood Platform',storage:'Storage Bin',signal:'Signal Wire'};
  function readable(key) {return names[key]||String(key).replace(/([a-z])([A-Z])/g,'$1 $2').replace(/^./,s=>s.toUpperCase());}
  function create(adapter={}) {
    if (typeof document==='undefined') return {refresh(){},destroy(){},openPanel(){},closePanels(){}};
    const byId=id=>document.getElementById(id), events=[];
    let currentPlan=blueprints[0], currentDialog=null, previousFocus=null, lastJournal='',destroyed=false;
    const on=(el,name,fn,options)=>{if(!el)return;el.addEventListener(name,fn,options);events.push(()=>el.removeEventListener(name,fn,options));};
    const state=()=>adapter.getState?.()||{};
    const el=(tag,className,text)=>{const node=document.createElement(tag);if(className)node.className=className;if(text!==undefined)node.textContent=String(text);return node;};
    function closePanels() {
      const visible=document.querySelector('.modal.show');
      if (visible) adapter.openModal?.(visible.id,false);
      for(const modal of document.querySelectorAll('.modal.show'))modal.classList.remove('show');
      syncDialog();
    }
    function openPanel(id,force) {
      const target=byId(id);if(!target)return;
      const open=force===undefined?!target.classList.contains('show'):force;
      if(adapter.openModal)adapter.openModal(id,open);
      else {for(const modal of document.querySelectorAll('.modal'))modal.classList.remove('show');if(open)target.classList.add('show');}
      if(open&&id==='journalPanel')renderJournal();
      if(open&&id==='blueprintPanel')renderBlueprint();
      syncDialog();
    }
    function syncDialog() {
      const visible=document.querySelector('.modal.show');
      document.body.classList.toggle('dialog-open',!!visible);
      if(visible?.id==='craftPanel')for(const category of document.querySelectorAll('[data-craft-category]'))category.textContent=craftCategoryLabels[category.dataset.craftCategory]||readable(category.dataset.craftCategory);
      if(visible===currentDialog)return;
      if(visible){if(!currentDialog)previousFocus=document.activeElement;currentDialog=visible;queueMicrotask(()=>{if(!destroyed&&visible.classList.contains('show'))(visible.querySelector('button,input,select,[tabindex="0"]')||visible).focus?.();});}
      else {currentDialog=null;previousFocus?.focus?.();previousFocus=null;}
    }
    function setMode(mode) {if(!modes[mode])return;adapter.setMode?.(mode);refresh();}
    on(byId('journalBtn'),'click',()=>openPanel('journalPanel'));
    on(byId('blueprintBtn'),'click',()=>openPanel('blueprintPanel'));
    on(byId('startBtn'),'click',()=>{adapter.setDifficulty?.(byId('welcomeDifficulty')?.value||'standard');openPanel('welcomePanel',false);adapter.start?.();});
    on(byId('inspectorClose'),'click',()=>{if(adapter.closeInspector)adapter.closeInspector();else byId('lensBtn')?.click();});
    on(byId('missionToggle'),'click',()=>{const missions=byId('missions'),collapsed=missions.classList.toggle('collapsed');byId('missionToggle').textContent=collapsed?'+':'−';byId('missionToggle').setAttribute('aria-expanded',String(!collapsed));byId('missionToggle').setAttribute('aria-label',collapsed?'Expand objectives':'Collapse objectives');});
    for(const button of document.querySelectorAll('[data-mode]'))on(button,'click',()=>setMode(button.dataset.mode));
    for(const id of ['difficultySelect','welcomeDifficulty'])on(byId(id),'change',e=>{adapter.setDifficulty?.(e.target.value);refresh();});
    for(const id of ['soundVolume','musicVolume'])on(byId(id),'input',e=>{const value=Number(e.target.value)/100;adapter.setAudio?.({[id]:value});const output=byId(id==='soundVolume'?'soundValue':'musicValue');if(output)output.textContent=`${Math.round(value*100)}%`;});
    on(byId('smartPlacement'),'change',event=>{adapter.setSmartPlacement?.(event.target.checked);});
    on(byId('blueprintCancel'),'click',()=>{adapter.cancelBlueprint?.();refresh();});
    // App.js owns the original controls; these dialogs also work if it only knows older menu IDs.
    for(const button of document.querySelectorAll('#journalPanel [data-close],#blueprintPanel [data-close],#welcomePanel [data-close]'))on(button,'click',()=>openPanel(button.dataset.close,false));
    on(document,'keydown',event=>{
      const typing=event.target?.matches?.('input,select,textarea,[contenteditable="true"]');
      if(event.code==='Escape'){
        if(document.querySelector('.modal.show')){event.preventDefault();event.stopImmediatePropagation();closePanels();return;}
        if(state().blueprint){event.preventDefault();event.stopImmediatePropagation();adapter.cancelBlueprint?.();refresh();return;}
      }
      if(event.code==='Tab'&&currentDialog){const controls=[...currentDialog.querySelectorAll('button:not(:disabled),input:not(:disabled),select:not(:disabled),[tabindex="0"]')].filter(n=>n.getClientRects().length);if(!controls.length){event.preventDefault();currentDialog.focus();return;}const first=controls[0],last=controls[controls.length-1];if(event.shiftKey&&(document.activeElement===first||!currentDialog.contains(document.activeElement))){event.preventDefault();last.focus();}else if(!event.shiftKey&&(document.activeElement===last||!currentDialog.contains(document.activeElement))){event.preventDefault();first.focus();}}
      if(typing||event.repeat||event.ctrlKey||event.metaKey||event.altKey)return;
      if(event.code==='KeyJ'){event.preventDefault();event.stopImmediatePropagation();openPanel('journalPanel');}
      if(event.code==='KeyB'){event.preventDefault();event.stopImmediatePropagation();openPanel('blueprintPanel');}
    },true);
    const lensTools=el('div','lens-mobile-tools');
    for(const [source,label] of [['viewBtn','Cycle lens view'],['linkModeBtn','Cycle link mode'],['manualBtn','Field manual']]){const button=el('button','icon-btn',label);on(button,'click',()=>byId(source)?.click());lensTools.append(button);}
    byId('lensPanel')?.append(lensTools);
    function renderJournal() {
      const s=state(),journal=s.progression||s.journal||{},chapters=journal.chapters||[];
      const summary=byId('journalSummary'),body=byId('journalChapters');if(!summary||!body)return;
      const fingerprint=JSON.stringify(journal);if(fingerprint===lastJournal&&body.children.length)return;lastJournal=fingerprint;
      summary.replaceChildren();
      const chapter=journal.chapter||1;summary.append(el('span','',chapter>5?'Campaign complete · Sandbox continues':`Chapter ${chapter} of 5`));
      summary.append(el('span','',`${(journal.unlocked||journal.unlocks||[]).length} technologies unlocked`));
      if(journal.discoveredRegions)summary.append(el('span','',`${journal.discoveredRegions.length} regions discovered`));
      if(Number.isFinite(journal.raidsSurvived))summary.append(el('span','',`${journal.raidsSurvived} raids survived`));
      body.replaceChildren();
      if(!chapters.length){body.append(el('p','muted','Mine nearby copper and open the Fabricator to begin your expedition.'));return;}
      for(const entry of chapters){const card=el('article','chapter'+(entry.complete?' complete':entry.active?' active':'')),heading=el('div','chapter-heading');heading.append(el('h3','',`${entry.id?`${entry.id}. `:''}${entry.title}`),el('span','chapter-state',entry.complete?'Complete':entry.active?'In progress':'Ahead'));card.append(heading);
        const objectives=el('ul','journal-objectives');for(const task of entry.objectives||[]){const active=task.active||(!task.complete&&entry.active),li=el('li',(task.complete?'complete':'')+(active?' current':''));let label=task.label||task.title||task.id||'Objective';if(!task.complete&&Number.isFinite(task.current)&&Number.isFinite(task.target)&&task.target>1)label+=` (${Math.min(task.current,task.target)}/${task.target})`;li.textContent=label;objectives.append(li);}card.append(objectives);
        if(entry.reward||entry.unlocks?.length){const reward=el('p','chapter-reward');reward.append(el('b','',entry.complete?'Earned: ':'Reward: '),document.createTextNode([entry.reward,(entry.unlocks||[]).join(', ')].filter(Boolean).join(' · ')));card.append(reward);}body.append(card);}
    }
    function itemDetails(key) {const item=(state().items||[]).find(item=>item.key===key);return {name:item?.name||readable(key),icon:item?.icon||icons[key]||'▪'};}
    function renderBlueprint() {
      const list=byId('blueprintList'),detail=byId('blueprintDetail');if(!list||!detail)return;
      list.replaceChildren();
      for(const blueprint of blueprints){const button=el('button','blueprint-choice'+(blueprint===currentPlan?' active':''));button.setAttribute('aria-pressed',String(blueprint===currentPlan));button.append(el('b','',blueprint.title),el('small','',blueprint.subtitle));button.onclick=()=>{currentPlan=blueprint;renderBlueprint();};list.append(button);}
      detail.replaceChildren(el('h3','',currentPlan.title),el('p','',currentPlan.description));
      const blocks=currentPlan.placements,minX=Math.min(...blocks.map(b=>b.x))-1,maxX=Math.max(...blocks.map(b=>b.x))+1,minY=Math.min(...blocks.map(b=>b.y)),maxY=Math.max(...blocks.map(b=>b.y)),grid=el('div','blueprint-grid');
      grid.style.setProperty('--cols',String(maxX-minX+1));grid.setAttribute('role','img');grid.setAttribute('aria-label',`${currentPlan.title}, ${blocks.length} real components. ${currentPlan.subtitle}.`);
      for(let y=minY;y<=maxY;y++)for(let x=minX;x<=maxX;x++){const block=blocks.find(b=>b.x===x&&b.y===y),cell=el('span','blueprint-cell');if(block){const part=itemDetails(block.key),kind=['generator'].includes(block.key)?'power':['motor','winch','platform'].includes(block.key)?'mechanical':['sensor','enemySensor','signal','autoDoor'].includes(block.key)?'signal':'item';cell.classList.add('filled',kind);cell.textContent=part.icon;cell.title=`${part.name} · ${['right','down','left','up'][block.rot||0]}`;cell.append(el('span','orientation',['→','↓','←','↑'][block.rot||0]));}grid.append(cell);}detail.append(grid);
      const inventory=state().inventory||{},costs=el('div','blueprint-costs');let missing=0;
      for(const [key,count] of Object.entries(currentPlan.cost)){const have=inventory[key]||0,part=itemDetails(key);if(have<count)missing+=count-have;costs.append(el('span','blueprint-cost'+(have<count?' missing':''),`${part.name} ${have}/${count}`));}detail.append(costs,el('p','blueprint-note',currentPlan.notes));
      const preview=el('button','blueprint-preview','Preview in the world →');preview.onclick=()=>{adapter.selectBlueprint?.(currentPlan);openPanel('blueprintPanel',false);refresh();};detail.append(preview);
      const status=adapter.getBlueprintStatus?.(currentPlan);detail.append(el('div','blueprint-status'+(missing||status?.canBuild===false?' warning':''),status?.reason||(missing?`Missing ${missing} component${missing===1?'':'s'}. You can preview now and fabricate the remaining parts.`:'Components ready. Aim at clear space; confirm placement in Build mode.')));
    }
    function refresh() {
      if(destroyed)return;const s=state(),mode=modes[s.mode]?s.mode:'mine';
      for(const button of document.querySelectorAll('[data-mode]')){const selected=button.dataset.mode===mode;button.classList.toggle('active',selected);button.setAttribute('aria-pressed',String(selected));}
      if(byId('modeHint'))byId('modeHint').textContent=modes[mode];
      if(byId('smartPlacement'))byId('smartPlacement').checked=s.smartPlacement!==false;
      for(const button of document.querySelectorAll('.context-action')){const visible=button.dataset.touch===(mode==='build'?'place':mode==='interact'?'interact':mode==='mine'?'mine':null);button.setAttribute('aria-hidden',String(!visible));}
      const difficulty=difficultyText[s.difficulty]?s.difficulty:'standard';for(const id of ['difficultySelect','welcomeDifficulty'])if(byId(id)&&document.activeElement!==byId(id))byId(id).value=difficulty;if(byId('difficultyDescription'))byId('difficultyDescription').textContent=difficultyText[difficulty];
      for(const [id,fallback,output] of [['soundVolume',.65,'soundValue'],['musicVolume',.2,'musicValue']]){const value=Number.isFinite(s[id])?s[id]:fallback;if(byId(id)&&document.activeElement!==byId(id))byId(id).value=String(Math.round(value*100));if(byId(output))byId(output).textContent=`${Math.round(value*100)}%`;}
      const hud=s.hud||{},region=typeof hud.region==='object'?hud.region?.name:hud.region;if(region&&byId('regionReadout'))byId('regionReadout').textContent=region;
      const inventory=s.inventory||{},miningTool=inventory.poweredMiningTool?'Powered Mining Tool':inventory.steelDrillTool?'Steel Drill Tool':inventory.reinforcedPickaxe?'Reinforced Pickaxe':'Basic Pickaxe';
      for(const button of document.querySelectorAll('.combat-action'))button.hidden=mode!=='combat';
      if(byId('healBtn'))byId('healBtn').textContent='✚'+((inventory.medkit||0)+(inventory.repairKit||0));
      const equipment=mode==='build'&&s.selected?.name?`Build: ${s.selected.name}`:mode==='combat'?hud.weapon:hud.tool||s.toolName||miningTool;
      if(equipment&&byId('equipmentReadout'))byId('equipmentReadout').textContent=typeof equipment==='object'?equipment.name:equipment;
      if(byId('energyReadout')){byId('energyReadout').hidden=!Number.isFinite(hud.energy);if(Number.isFinite(hud.energy))byId('energyReadout').textContent=`Energy ${Math.max(0,Math.round(hud.energy))}`;}
      const journal=s.progression||s.journal||{},activeChapter=(journal.chapters||[]).find(chapter=>chapter.active);
      if(byId('missionTitle'))byId('missionTitle').textContent=activeChapter?.title||(journal.chapter>5?'Endless engineering':'Wake the Foundry');
      const raid=hud.raid||{},raidActive=raid.state==='warning'||raid.state==='attack',raidElement=byId('raidReadout');
      document.body.classList.toggle('raid-active',raidActive);
      if(raidElement){raidElement.hidden=!raidActive;raidElement.classList.toggle('attack',raid.state==='attack');if(raidActive)raidElement.textContent=raid.state==='warning'?`⚠ Base attack in ${Math.max(0,Math.ceil(raid.remaining||0))}s · Prepare your defenses`:`⚔ Base under attack · ${Math.max(0,raid.count||0)} hostiles remaining`;}
      const active=s.blueprint,activePanel=byId('blueprintActive');if(activePanel){activePanel.hidden=!active;if(active&&byId('blueprintActiveText'))byId('blueprintActiveText').textContent=`${active.title||active.name||readable(active.id||active)} · Aim to preview, place to build`;}
      if(byId('journalPanel')?.classList.contains('show'))renderJournal();
      syncDialog();
    }
    const observer=typeof MutationObserver==='function'?new MutationObserver(syncDialog):null;for(const modal of document.querySelectorAll('.modal'))observer?.observe(modal,{attributes:true,attributeFilter:['class']});
    refresh();
    if(state().firstRun)openPanel('welcomePanel',true);
    return {refresh,renderJournal,renderBlueprint,openPanel,closePanels,destroy(){destroyed=true;for(const dispose of events)dispose();observer?.disconnect();lensTools.remove();}};
  }
  return {create,blueprints,modes,difficultyText,craftCategoryLabels};
});
