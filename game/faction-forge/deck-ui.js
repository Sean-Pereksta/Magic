import {STARTER_DECKS,normalizeDecks,validateDeck,deckAnalysis,DECK_SIZE} from './decks.js';
import {COMBOS} from './combos.js';

const FACTIONS=['Vanir','Aesir','Helions'];
const FACTION_DETAILS={
  Vanir:{emblem:'⌖',identity:'Precision & Technology',playstyle:'Hold a narrow frontline and protect precision shooters, suppression and explosive artillery.',strengths:['Armor penetration','Coordinated ranged fire'],weaknesses:['Fast flanks','Exposed support troops']},
  Aesir:{emblem:'✧',identity:'Magic & Control',playstyle:'Protect your casters while binds, wards and lightning create powerful chain reactions.',strengths:['Magical damage','Disruption and wards'],weaknesses:['Timed burst damage','Aggressive flanking']},
  Helions:{emblem:'◈',identity:'Aggression & Attrition',playstyle:'Pressure the enemy with corrosion and poison, then exploit openings with ambushes and explosions.',strengths:['Poison and corrosion','Relentless pressure'],weaknesses:['Concentrated explosives','Suppression and precision fire']}
};
const ROLE_ICONS={front:'◈',back:'⌖',support:'✦',artillery:'◉',assassin:'➶'};
const RATING_LABELS={antiArmor:'Anti-armor',antiSwarm:'Anti-swarm',frontline:'Protection',ranged:'Ranged damage',support:'Support',mobility:'Mobility'};
const copyDeck=deck=>({id:deck.id,name:deck.name,faction:deck.faction,troops:[...deck.troops]});
const makeId=()=>`deck-${globalThis.crypto?.randomUUID?.()||`${Date.now()}-${Math.random().toString(36).slice(2,10)}`}`;

/** DOM-only deck authoring. The save/select callbacks own persistence and match entry. */
export function mountDeckLibrary({container,profile={},getProfile, troops={},counters={},portrait,unlockText=()=> 'Collect 15 blueprints or complete its campaign mission.',onSelect,onSave,close,onBeforeRender=()=>{},onViewChange=()=>{},isLocked=()=>false,selectionMode=false}={}){
  if(!container)throw new Error('A deck library requires a container.');
  const document=container.ownerDocument,root=document.createElement('div');root.className='ff-deck-library';container.appendChild(root);
  let currentProfile=profile,filter='All',draft=null,selectedSlot=null,search='',busy=false,disposed=false,message='',localDecks=[],selectedId=profile.selectedDeckId,owner=profile.uid||profile.username||'';
  const element=(tag,className,text)=>{const node=document.createElement(tag);if(className)node.className=className;if(text!==undefined)node.textContent=String(text);return node;};
  const button=(text,click,className='ff-button')=>{const node=element('button',className,text);node.type='button';node.onclick=click;return node;};
  const player=()=>getProfile?.()||currentProfile||{};
  const unlocked=key=>!!player().troops?.[key]?.unlocked || (!troops[key]?.unlockable && player().troops?.[key]?.unlocked!==false);
  const starter=deck=>STARTER_DECKS.some(item=>item.id===deck.id);
  const allDecks=()=>{
    const saved=Array.isArray(player().decks)?player().decks:[];
    return normalizeDecks([...localDecks,...saved.filter(deck=>!localDecks.some(item=>item.id===deck.id))]);
  };
  function validation(deck){
    const errors=[];
    if(!String(deck.name||'').trim())errors.push('Give your deck a name.');
    if(deck.troops.length!==DECK_SIZE || deck.troops.some(key=>!key))errors.push(`Choose exactly ${DECK_SIZE} troop types.`);
    if(new Set(deck.troops.filter(Boolean)).size!==deck.troops.filter(Boolean).length)errors.push('Each troop type can appear only once.');
    if(deck.troops.some(key=>key && troops[key]?.faction!==deck.faction))errors.push('All five troops must belong to the same faction.');
    if(deck.troops.some(key=>key && !unlocked(key)))errors.push('Unlock every troop before saving this deck.');
    const result=validateDeck(deck,{unlockedTypes:Object.keys(troops).filter(unlocked),troops});
    if(result===false || result?.valid===false || result?.ok===false){const shared=result?.errors||[result?.error||result?.reason||'This deck is not valid.'];for(const error of shared)if(error&&!errors.includes(error))errors.push(error);}
    return {valid:errors.length===0,errors};
  }
  function beginRender(){onBeforeRender();onViewChange(draft?'builder':'library');root.replaceChildren();root.className='ff-deck-library'+(draft?' ff-deck-editing':'');}
  function status(parent){const node=element('p','ff-deck-status',message);node.setAttribute('role','status');node.setAttribute('aria-live','polite');parent.appendChild(node);return node;}
  function troopPortrait(key,{locked=false}={}){
    const tier=player().troops?.[key]?.forgeTier||1;
    if(portrait)return portrait(key,tier,{locked,animate:false});
    const node=element('div','ff-portrait'+(locked?' ff-locked-portrait':''));node.appendChild(element('span','ff-portrait-symbol',troops[key]?.emoji||'◆'));node.appendChild(element('span',`ff-tier-badge ff-tier-${tier}`,['','I','II','III'][tier]||'I'));return node;
  }
  function role(key){return counters[key]?.roleLabel||troops[key]?.roleLabel||(troops[key]?.role==='front'?'Frontline':'Backline');}
  function roleIcon(key){const label=role(key).toLowerCase();return /support|heal|ward|recon|summon/.test(label)?ROLE_ICONS.support:/assassin|diver|ambush/.test(label)?ROLE_ICONS.assassin:/artillery/.test(label)?ROLE_ICONS.artillery:ROLE_ICONS[troops[key]?.role]||'◆';}
  function analysis(deck){
    const meta=FACTION_DETAILS[deck.faction]||FACTION_DETAILS.Vanir;
    const result=deckAnalysis(deck,{troops,counters})||{};
    return {...meta,...result,playstyle:result.playstyle&&!result.playstyle.startsWith('Recruit your five roles')?result.playstyle:meta.playstyle,strengths:result.strengths?.length?result.strengths:meta.strengths,weaknesses:result.weaknesses?.length?result.weaknesses:meta.weaknesses};
  }
  function availableCombos(deck){return COMBOS.filter(combo=>combo.faction===deck.faction&&combo.units.every(key=>deck.troops.includes(key)));}
  function miniTroops(deck){
    const row=element('div','ff-deck-portraits');
    for(const key of deck.troops){const tile=element('div','ff-deck-mini-troop');tile.append(troopPortrait(key),element('strong','',troops[key]?.name||key),element('span','ff-deck-mini-role',`${roleIcon(key)} ${role(key)}`));tile.title=`${troops[key]?.name||key} · ${role(key)} · Forge Tier ${player().troops?.[key]?.forgeTier||1}`;row.appendChild(tile);}return row;
  }
  function counterList(title,items,kind){const row=element('div',`ff-deck-counter-list ff-deck-${kind}`);row.appendChild(element('strong','',title));const list=element('div','');for(const text of items.slice(0,3))list.appendChild(element('span','',text));row.appendChild(list);return row;}
  function comboChips(deck){const row=element('div','ff-deck-combo-chips'),combos=availableCombos(deck);row.appendChild(element('span','ff-deck-combo-label',`${combos.length} combo${combos.length===1?'':'s'}`));for(const combo of combos){const chip=element('span','ff-deck-combo-chip',combo.name);chip.title=combo.description;row.appendChild(chip);}if(!combos.length)row.appendChild(element('span','ff-deck-combo-empty','Swap troops to build a combination'));return row;}
  async function select(deck){
    if(busy||isLocked())return;const check=validation(deck);if(!check.valid){message=check.errors.join(' ');renderLibrary();return;}
    const requestOwner=owner;busy=true;message=`Preparing ${deck.name}…`;renderLibrary();
    try{const result=await onSelect?.(copyDeck(deck));if(result===false)throw new Error('Unable to select this deck. Try again.');if(disposed||owner!==requestOwner)return;selectedId=deck.id;currentProfile={...player(),selectedDeckId:deck.id};busy=false;message=`${deck.name} selected.`;renderLibrary();}
    catch(error){if(disposed||owner!==requestOwner)return;busy=false;message=error?.message||'Unable to select this deck.';renderLibrary();}
  }
  function renderLibrary(){
    if(disposed)return;draft=null;beginRender();
    const toolbar=element('div','ff-deck-toolbar'),intro=element('div','ff-deck-intro');intro.append(element('p','ff-deck-eyebrow','FIVE TROOPS. ONE STRATEGY.'),element('p','ff-deck-lead',selectionMode?'Choose the team you will take into battle. Your deck stays locked for the match.':'Build a team around the troops, counters and combinations you want to master.'));
    toolbar.append(intro,button('+ Create deck',()=>editDeck(null),'ff-button ff-deck-create'));toolbar.lastChild.disabled=busy||isLocked();root.appendChild(toolbar);
    const controls=element('div','ff-deck-filters');controls.setAttribute('aria-label','Filter decks by faction');
    for(const faction of ['All',...FACTIONS]){const pick=button(faction==='All'?'All decks':`${FACTION_DETAILS[faction].emblem} ${faction}`,()=>{filter=faction;message='';renderLibrary();},'ff-button ff-secondary'+(filter===faction?' ff-selected':''));pick.setAttribute('aria-pressed',String(filter===faction));pick.disabled=busy;controls.appendChild(pick);}root.appendChild(controls);
    if(isLocked())root.appendChild(element('p','ff-match-lock','Your current match has locked its deck. Finish the match to edit or select another team.'));
    status(root);const grid=element('div','ff-deck-grid');
    for(const deck of allDecks().filter(item=>filter==='All'||item.faction===filter)){
      const details=analysis(deck),card=element('article',`ff-deck-card ff-faction-${deck.faction.toLowerCase()}`),active=deck.id===(selectedId||player().selectedDeckId),header=element('header','ff-deck-card-header'),heading=element('div','');
      card.dataset.deckId=deck.id;card.classList.toggle('ff-deck-current',active);header.appendChild(element('span','ff-deck-emblem',FACTION_DETAILS[deck.faction]?.emblem||'◆'));heading.append(element('span','ff-deck-eyebrow',`${deck.faction} · ${starter(deck)?'Starter deck':'Custom deck'}`),element('h2','',deck.name));header.appendChild(heading);if(active)header.appendChild(element('span','ff-deck-selected-badge','Selected'));card.append(header,miniTroops(deck),element('p','ff-deck-playstyle',details.playstyle));
      const countersRow=element('div','ff-deck-counters');countersRow.append(counterList('Strengths',details.strengths,'strength'),counterList('Watch for',details.weaknesses,'weakness'));card.append(countersRow,comboChips(deck));
      const actions=element('div','ff-deck-card-actions'),edit=button('Edit',()=>editDeck(deck),'ff-button ff-secondary'),duplicate=button('Duplicate',()=>editDeck({...deck,id:makeId(),name:`${deck.name} copy`},true),'ff-button ff-secondary'),choose=button(selectionMode?'Select & continue':active?'Selected':'Select',()=>select(deck),'ff-button ff-deck-select');
      edit.setAttribute('aria-label',`Edit ${deck.name}`);duplicate.setAttribute('aria-label',`Duplicate ${deck.name}`);choose.setAttribute('aria-label',`${selectionMode?'Select and continue with':'Select'} ${deck.name}`);for(const action of [edit,duplicate,choose])action.disabled=busy||isLocked();const check=validation(deck);if(!check.valid){choose.disabled=true;choose.title=check.errors.join(' ');}actions.append(edit,duplicate,choose);card.appendChild(actions);grid.appendChild(card);
    }root.appendChild(grid);
    if(close)root.appendChild(button('← Back',close,'ff-button ff-secondary ff-deck-close'));
  }
  function editDeck(deck,alreadyCopied=false){
    if(busy||isLocked())return;const original=deck?copyDeck(deck):{id:makeId(),name:'',faction:filter==='All'?'Vanir':filter,troops:[]};
    if(deck&&starter(deck)&&!alreadyCopied){original.id=makeId();original.name=`${original.name} copy`;message='Starter decks stay available. Edit this custom copy and save it to your collection.';}else message='';
    draft=original;selectedSlot=null;search='';renderBuilder();queueMicrotask(()=>root.querySelector('.ff-deck-name-input')?.focus());
  }
  function addTroop(key,index=selectedSlot){
    if(busy||isLocked()||!troops[key]||troops[key].faction!==draft.faction||!unlocked(key))return;
    const existing=draft.troops.indexOf(key),target=Number.isInteger(index)?index:draft.troops.findIndex(item=>!item);
    if(existing>=0){
      if(Number.isInteger(index)&&index!==existing){const other=draft.troops[index]||'';draft.troops[index]=key;draft.troops[existing]=other;message=`${troops[key].name} moved to slot ${index+1}.`;}
      else{draft.troops[existing]='';message=`${troops[key].name} removed.`;}
    }else{const slot=target>=0?target:draft.troops.length<DECK_SIZE?draft.troops.length:-1;if(slot<0){message='Select one of your five slots, then tap a troop to swap it.';renderBuilder();return;}draft.troops[slot]=key;message=`${troops[key].name} added to slot ${slot+1}.`;}
    selectedSlot=null;renderBuilder();
  }
  function readDrag(event){return event.dataTransfer?.getData('application/x-faction-forge-troop')||event.dataTransfer?.getData('text/plain');}
  function dragSource(node,key){node.draggable=true;node.ondragstart=event=>{if(busy||isLocked()){event.preventDefault();return;}event.dataTransfer.setData('application/x-faction-forge-troop',key);event.dataTransfer.setData('text/plain',key);event.dataTransfer.effectAllowed='copyMove';};}
  function renderSlots(parent){
    const slots=element('div','ff-deck-slots');slots.setAttribute('aria-label','Your five troop slots');
    for(let i=0;i<DECK_SIZE;i++){
      const key=draft.troops[i],slot=element('div','ff-deck-slot'+(key?' ff-deck-slot-filled':'')+(selectedSlot===i?' ff-deck-slot-target':'')),pick=button('',()=>{selectedSlot=selectedSlot===i?null:i;message=selectedSlot===null?'':`Choose a troop for slot ${i+1}.`;renderBuilder();},'ff-deck-slot-pick');
      slot.dataset.slot=String(i);pick.disabled=busy||isLocked();pick.setAttribute('aria-label',`Slot ${i+1}: ${key?troops[key]?.name||key:'empty'}. Select to swap.`);pick.setAttribute('aria-pressed',String(selectedSlot===i));
      pick.appendChild(element('span','ff-deck-slot-number',String(i+1)));
      if(key){pick.append(troopPortrait(key),element('strong','',troops[key]?.name||key),element('small','',`${roleIcon(key)} ${role(key)}`));dragSource(pick,key);}else pick.append(element('span','ff-deck-slot-plus','+'),element('strong','','Add troop'));
      slot.appendChild(pick);
      if(key){const remove=button('×',()=>{draft.troops[i]='';selectedSlot=null;message='Troop removed. Choose a replacement.';renderBuilder();},'ff-deck-slot-remove');remove.disabled=busy||isLocked();remove.setAttribute('aria-label',`Remove ${troops[key]?.name||key} from deck`);slot.appendChild(remove);}
      slot.ondragover=event=>{if(busy||isLocked())return;event.preventDefault();slot.classList.add('ff-deck-drop-target');event.dataTransfer.dropEffect='move';};slot.ondragleave=()=>slot.classList.remove('ff-deck-drop-target');slot.ondrop=event=>{event.preventDefault();slot.classList.remove('ff-deck-drop-target');addTroop(readDrag(event),i);};slots.appendChild(slot);
    }parent.appendChild(slots);
  }
  function renderInsights(parent){
    const deck={...draft,troops:draft.troops.filter(Boolean)},details=analysis(deck),insights=element('aside','ff-deck-insights');insights.append(element('h3','','Your strategy'),element('p','ff-deck-playstyle',details.playstyle));
    const ratings=details.ratings||{};const bars=element('div','ff-deck-rating-bars');
    for(const [key,label] of Object.entries(RATING_LABELS)){const value=Number(ratings[key]??ratings[label]??0),row=element('div','ff-deck-rating-row'),meter=element('meter','');meter.min=0;meter.max=5;meter.value=Math.max(0,Math.min(5,value));meter.setAttribute('aria-label',`${label}: ${meter.value} out of 5`);row.append(element('span','',label),meter);bars.appendChild(row);}if(Object.keys(ratings).length)insights.appendChild(bars);
    insights.append(counterList('Strengths',details.strengths,'strength'),counterList('Weaknesses',details.weaknesses,'weakness'));
    const combos=element('section','ff-deck-builder-combos');combos.appendChild(element('h3','','Combinations'));const relevant=COMBOS.filter(combo=>combo.faction===deck.faction&&combo.units.some(key=>deck.troops.includes(key)));
    if(!relevant.length)combos.appendChild(element('p','ff-deck-combo-empty','Add complementary troops to reveal their combinations.'));
    for(const combo of relevant){const missing=[...new Set(combo.units)].filter(key=>!deck.troops.includes(key)),row=element('div','ff-deck-builder-combo'+(missing.length?'':' ff-deck-combo-ready'));row.append(element('strong','',`${missing.length?'○':'✓'} ${combo.name}`),element('span','',combo.description),element('small','',missing.length?`Add ${missing.map(key=>troops[key]?.name||key).join(' + ')}`:'Ready · Rank II partners or a combo enhancement strengthen this effect. Rank III plus an upgraded partner unlocks Legendary.'));combos.appendChild(row);}insights.appendChild(combos);parent.appendChild(insights);
  }
  function renderCollection(parent){
    const collection=element('section','ff-deck-collection-section'),heading=element('div','ff-deck-collection-heading'),title=element('h3','',`${draft.faction} troop collection`),input=element('input','ff-deck-search');input.type='search';input.placeholder='Search troops or roles';input.setAttribute('aria-label','Search troops or roles');input.value=search;input.disabled=busy;heading.append(title,input);collection.appendChild(heading);
    const grid=element('div','ff-deck-collection');
    function fill(){
      grid.replaceChildren();const text=search.trim().toLowerCase();
      for(const [key,troop] of Object.entries(troops).filter(([,troop])=>troop.faction===draft.faction)){
        if(text&&!`${troop.name} ${role(key)} ${counters[key]?.archetype||''}`.toLowerCase().includes(text))continue;
        const locked=!unlocked(key),selected=draft.troops.includes(key),card=button('',()=>addTroop(key),'ff-deck-troop-card'+(locked?' ff-deck-troop-locked':'')+(selected?' ff-deck-troop-added':''));card.dataset.troop=key;card.disabled=busy||isLocked()||locked;card.setAttribute('aria-label',`${troop.name}, ${role(key)}${locked?', locked':selected?', in deck. Tap to remove or swap':'. Add to deck'}`);card.setAttribute('aria-pressed',String(selected));
        card.append(troopPortrait(key,{locked}),element('strong','',troop.name),element('span','ff-deck-troop-role',`${roleIcon(key)} ${role(key)}`));
        if(locked){card.appendChild(element('small','ff-deck-unlock',unlockText(key)));card.title=unlockText(key);}else{const meta=counters[key]||troop;card.appendChild(element('small','ff-deck-troop-tip',meta.counterTip||meta.description||troop.description||'Protect its role and coordinate it with your team.'));if(meta.strongAgainst?.length)card.appendChild(element('small','ff-deck-troop-counter',`Strong: ${meta.strongAgainst.slice(0,2).join(', ')}`));if(meta.weakAgainst?.length)card.appendChild(element('small','ff-deck-troop-danger',`Weak: ${meta.weakAgainst.slice(0,2).join(', ')}`));dragSource(card,key);}
        if(selected)card.appendChild(element('span','ff-deck-included-mark','✓ In deck'));grid.appendChild(card);
      }if(!grid.children.length)grid.appendChild(element('p','ff-deck-combo-empty','No troops match your search.'));
    }input.oninput=()=>{search=input.value;fill();};fill();collection.appendChild(grid);parent.appendChild(collection);
  }
  async function save(){
    if(busy||isLocked())return;const deck={...draft,name:draft.name.trim(),troops:draft.troops.filter(Boolean)},check=validation(deck);if(!check.valid){message=check.errors.join(' ');renderBuilder();return;}
    const requestOwner=owner;busy=true;message='Saving your deck…';renderBuilder();
    try{if(!onSave)throw new Error('Deck saving is unavailable. Reconnect your profile and try again.');const result=await onSave(copyDeck(deck));if(result===false)throw new Error('Unable to save this deck. Try again.');if(disposed||owner!==requestOwner)return;if(result?.profile)currentProfile=result.profile;localDecks=localDecks.filter(item=>item.id!==deck.id);if(!player().decks?.some(item=>item.id===deck.id&&item.name===deck.name&&item.faction===deck.faction&&JSON.stringify(item.troops)===JSON.stringify(deck.troops)))localDecks.push(deck);busy=false;message=`${deck.name} saved. It is ready to take into battle.`;filter=deck.faction;renderLibrary();}
    catch(error){if(disposed||owner!==requestOwner)return;busy=false;message=error?.message||'Unable to save this deck.';renderBuilder();}
  }
  function renderBuilder(){
    if(disposed||!draft)return;beginRender();root.classList.add(`ff-faction-${draft.faction.toLowerCase()}`);
    const header=element('div','ff-deck-builder-header'),back=button('← Deck library',()=>{message='';renderLibrary();},'ff-button ff-secondary');back.disabled=busy;header.append(back,element('span','ff-deck-eyebrow','DECK BUILDER'));root.appendChild(header);
    const form=element('div','ff-deck-builder-form'),label=element('label','ff-deck-name-label','Deck name'),name=element('input','ff-deck-name-input');name.id='ff-deck-name';name.maxLength=48;name.placeholder='Name your strategy';name.value=draft.name;name.disabled=busy||isLocked();label.htmlFor=name.id;form.append(label,name);root.appendChild(form);
    const factions=element('div','ff-deck-filters');factions.setAttribute('aria-label','Deck faction');for(const faction of FACTIONS){const pick=button(`${FACTION_DETAILS[faction].emblem} ${faction}`,()=>{if(draft.faction===faction)return;draft.faction=faction;draft.troops=[];selectedSlot=null;search='';message='Faction changed. Choose five troops for your new team.';renderBuilder();},'ff-button ff-secondary'+(faction===draft.faction?' ff-selected':''));pick.disabled=busy||isLocked();pick.setAttribute('aria-pressed',String(faction===draft.faction));factions.appendChild(pick);}root.appendChild(factions);
    root.appendChild(element('p','ff-deck-builder-hint',selectedSlot===null?'Tap troops to add them. Select a filled slot to swap, or drag a troop into any slot.':`Slot ${selectedSlot+1} selected. Tap a troop to place or swap it here.`));renderSlots(root);const messageNode=status(root);
    const layout=element('div','ff-deck-builder-layout');renderCollection(layout);renderInsights(layout);root.appendChild(layout);
    const footer=element('div','ff-deck-builder-footer'),summary=element('div','ff-deck-validation'),saveButton=button('Save deck',save,'ff-button ff-deck-save');
    function updateValidation(){const check=validation({...draft,troops:draft.troops.filter(Boolean)}),count=draft.troops.filter(Boolean).length;summary.replaceChildren(element('strong','',`${count}/${DECK_SIZE} troops`),element('span','',check.valid?'Ready for battle · One faction · All unlocked':check.errors[0]));saveButton.disabled=busy||isLocked()||!check.valid||!onSave;saveButton.textContent=busy?'Saving…':'Save deck';}name.oninput=()=>{draft.name=name.value;updateValidation();if(messageNode.textContent==='Give your deck a name.')messageNode.textContent='';};updateValidation();footer.append(summary,saveButton);root.appendChild(footer);
  }
  renderLibrary();
  return {refresh(next){if(disposed)return;currentProfile=next||player();const nextOwner=currentProfile.uid||currentProfile.username||'';if(nextOwner!==owner){owner=nextOwner;localDecks=[];selectedId=currentProfile.selectedDeckId;draft=null;filter='All';message='';busy=false;}else{if(next?.selectedDeckId)selectedId=next.selectedDeckId;if(Array.isArray(next?.decks))localDecks=localDecks.filter(deck=>!next.decks.some(saved=>saved.id===deck.id));}if(draft)renderBuilder();else renderLibrary();},edit:editDeck,show:renderLibrary,get editing(){return !!draft;},destroy(){disposed=true;root.remove();}};
}
