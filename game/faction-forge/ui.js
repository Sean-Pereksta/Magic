import {animationDefinition,frameRectangle,drawSpriteFrame} from "./assets.js";
import {draftCardPresentation,relevantComboStates} from "./drafting.js";
import {FORGE_PACKS,packEligibility,randomPackPool,ROUND_WIN_CARDS} from "./packs.js";
import {mountDeckLibrary,sameDeckOwner} from "./deck-ui.js";
import {normalizeDecks} from "./decks.js";

const ROMAN=["","I","II","III"], TIER_NAMES=["","Recruit","Veteran","Elite"];
const DEFAULT_COSTS={1:{cards:10,dust:100},2:{cards:30,dust:350}};
const CHAPTER_NAMES=["Sparks on the Frontier","The Aesir Awakening","The Blight Rising","The Broken Pact","The Last Crucible"];
export function forgeEligibility(profile,key,{active=false,costs=DEFAULT_COSTS}={}){
  const troop=profile?.troops?.[key], tier=Math.max(1,Math.min(3,troop?.forgeTier||1)), cost=(profile?.upgradeCosts || costs)[tier];
  if(active || profile?.activeSessionId)return {available:false,reason:"Finish your current match before upgrading.",tier,cost};
  if(!troop?.unlocked)return {available:false,reason:"Unlock this troop first.",tier,cost};
  if(!cost)return {available:false,reason:"Elite tier reached.",tier,cost:null};
  if((troop.cards||0)<cost.cards || (profile.forgeDust||0)<cost.dust)return {available:false,reason:`Requires ${cost.cards} cards and ${cost.dust} Forge Dust.`,tier,cost};
  return {available:true,reason:"Ready to forge",tier,cost};
}
export function upgradeCount(profile,costs=DEFAULT_COSTS){return Object.keys(profile?.troops||{}).filter(key=>forgeEligibility(profile,key,{costs}).available).length;}

export function createExpansionUI({host=globalThis.document?.body,assets,troops={},counters={},legendaryAbilities={},getProfile=()=>null,getPendingPackRequest=()=>null,canSignIn=()=>!getProfile()?.uid,canRetryConnection=()=>false,isActiveMatch=()=>false,missionDefinitions=[],costs=DEFAULT_COSTS,fallbackPath=()=>"",onCampaignMission,onPractice,onEndless,onFindMatch,onUpgrade,onBuyPack,onTargetReward,onSaveDeck,onSelectDeck,onMenu,onBrowseLobbies,onSignIn,onRetryConnection,onResume,onAbandon,unlockDescriptions={},getConnectionStatus=()=>""}={}){
  if(!host)throw new Error("Faction Forge UI requires a DOM host.");
  const document=host.ownerDocument, shell=document.createElement("div"), fab=document.createElement("button");
  shell.className="ff-expansion";shell.hidden=true;shell.setAttribute("aria-label","Faction Forge");host.appendChild(shell);
  fab.className="ff-forge-fab";fab.type="button";fab.setAttribute("aria-label","Open The Forge");fab.innerHTML='<span class="ff-forge-fab-icon" aria-hidden="true">⚒</span><span class="ff-upgrade-count" hidden></span>';fab.onclick=()=>showForge();host.appendChild(fab);
  let currentView="",filter="All",selectedTroop=null,previewTier=1,matchmakingState={},previousFocus=null,generation=0;
  let forgeTab="collection",packBusy=false,lastPackPurchase=null,deckUI=null;
  const portraitTimers=new Set(), artFrames=new Set(), lazyPortraits=new Map();
  const observer=typeof globalThis.IntersectionObserver==="function"?new IntersectionObserver(entries=>{for(const entry of entries)if(entry.isIntersecting){const start=lazyPortraits.get(entry.target);if(start){lazyPortraits.delete(entry.target);observer.unobserve(entry.target);start();}}},{rootMargin:"180px"}):null;
  const element=(tag,className,text)=>{const node=document.createElement(tag);if(className)node.className=className;if(text!==undefined)node.textContent=String(text);return node;};
  const button=(text,onClick,className="ff-button")=>{const node=element("button",className,text);node.type="button";node.onclick=onClick;return node;};
  const profile=()=>getProfile() || {forgeDust:0,troops:{},campaign:{missions:{}}};
  const matchActive=()=>isActiveMatch() || !!profile().activeSessionId;
  const countComplete=()=>Object.values(profile().campaign?.missions || {}).filter(m=>m.completed).length;
  function stopPortraits(){for(const timer of portraitTimers)clearInterval(timer);portraitTimers.clear();for(const id of artFrames)cancelAnimationFrame(id);artFrames.clear();observer?.disconnect();lazyPortraits.clear();}
  async function illustration(parent,key,{className="ff-illustration",group="ui",frame:chosenFrame=0,animate=key==="reward-chest"}={}){
    if(!assets?.enabled())return false;
    try{
      const asset=await assets.asset(group,key);if(!asset)return false;
      if(asset.spec.frameWidth && asset.spec.frameHeight){
        const canvas=element("canvas",className),spec=asset.spec,columns=spec.columns||Math.floor(asset.image.width/spec.frameWidth),rows=spec.rows||Math.floor(asset.image.height/spec.frameHeight),count=columns*rows;
        canvas.setAttribute("aria-hidden","true");canvas.width=spec.frameWidth;canvas.height=spec.frameHeight;parent.prepend(canvas);
        const draw=index=>{const rect=frameRectangle({...spec,columns},Math.max(0,Math.min(count-1,index)),asset.image),ctx=canvas.getContext("2d");if(ctx){ctx.clearRect(0,0,canvas.width,canvas.height);drawSpriteFrame(ctx,asset.image,rect);}};
        draw(chosenFrame);
        if(animate && count>1 && !globalThis.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches){const start=performance.now(),duration=spec.durationMs||spec.duration||900;let handle=0,lastIndex=-1;const step=time=>{artFrames.delete(handle);if(!canvas.isConnected)return;const index=Math.min(count-1,Math.floor(Math.max(0,time-start)/duration*count));if(index!==lastIndex){draw(index);lastIndex=index;}if(time-start<duration){handle=requestAnimationFrame(step);artFrames.add(handle);}};handle=requestAnimationFrame(step);artFrames.add(handle);}
      }else{const img=element("img",className);img.src=asset.url;img.alt="";img.decoding="async";img.draggable=false;parent.prepend(img);}return true;
    }catch{return false;}
  }
  async function panelArt(parent,key,group="ui"){
    if(!assets?.enabled())return;try{const item=await assets.asset(group,key);if(item){const shade=parent.classList.contains("ff-panel")?"#07101bd9":"#07101b55";parent.style.backgroundImage=`linear-gradient(${shade},${shade}),url("${item.url}")`;parent.classList.add("ff-has-art");}}catch{}
  }
  illustration(fab,"forge-button",{className:"ff-forge-fab-art"});
  function badge(tier){const node=element("span",`ff-tier-badge ff-tier-${tier}`,ROMAN[tier]||"I");node.setAttribute("aria-label",`Permanent Forge Tier ${tier}: ${TIER_NAMES[tier]}`);illustration(node,`tier-${tier}`,{className:"ff-tier-badge-art"});return node;}
  function portrait(key,tier=1,{locked=false,animate=false}={}){
    const node=element("div","ff-portrait"+(locked?" ff-locked-portrait":"")),troop=troops[key]||{};
    node.setAttribute("role","img");node.setAttribute("aria-label",`${troop.name||key}, ${TIER_NAMES[tier]}`);
    const fallback=fallbackPath(key);
    if(fallback){const image=element("img","ff-portrait-fallback");image.src=fallback;image.alt="";image.decoding="async";image.onerror=()=>{image.remove();node.prepend(element("span","ff-portrait-symbol",troop.emoji||"◆"));};node.appendChild(image);}else node.appendChild(element("span","ff-portrait-symbol",troop.emoji||"◆"));
    node.appendChild(badge(tier));
    if(locked)node.appendChild(element("span","ff-lock-icon","🔒"));
    const loadPortrait=()=> (async()=>{for(let artTier=Math.max(1,Math.min(3,Number(tier)||1));artTier>=1;artTier--){try{const sheet=await assets.unit(key,artTier);if(sheet)return sheet;}catch{}}return null;})().then(sheet=>{
      if(!sheet)return;node.querySelector(".ff-portrait-fallback")?.remove();node.querySelector(".ff-portrait-symbol")?.remove();
      const canvas=element("canvas","ff-portrait-canvas");const definition=animationDefinition(sheet.spec,"idle");let index=0;
      function draw(){const rect=frameRectangle(sheet.spec,definition.frames[index%definition.frames.length],sheet.image);if(canvas.width!==rect.width || canvas.height!==rect.height){canvas.width=rect.width;canvas.height=rect.height;}const ctx=canvas.getContext("2d");if(!ctx)return;ctx.clearRect(0,0,canvas.width,canvas.height);drawSpriteFrame(ctx,sheet.image,rect);}
      draw();node.prepend(canvas);
      if(animate && !globalThis.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches && globalThis.matchMedia?.("(hover: hover)")?.matches){let timer=null;node.onpointerenter=()=>{if(timer)return;timer=setInterval(()=>{if(!node.isConnected){clearInterval(timer);portraitTimers.delete(timer);timer=null;return;}index++;draw();},Math.max(80,definition.duration/definition.frames.length));portraitTimers.add(timer);};node.onpointerleave=()=>{clearInterval(timer);portraitTimers.delete(timer);timer=null;index=0;draw();};}
    }).catch(()=>{});
    if(assets?.enabled()){if(observer){lazyPortraits.set(node,loadPortrait);observer.observe(node);}else loadPortrait();}
    return node;
  }
  function frame(node,faction){node.classList.add(`ff-faction-${String(faction||"neutral").toLowerCase()}`);illustration(node,`card-frame-${String(faction||"neutral").toLowerCase()}`,{className:"ff-card-frame"});}
  function factionArtwork(faction,{animate=false}={}){
    const stage=element("div","ff-faction-artwork"),key=Object.keys(troops).find(k=>troops[k].faction===faction);
    frame(stage,faction);
    if(key)stage.appendChild(portrait(key,profile().troops?.[key]?.forgeTier||1,{animate}));
    return stage;
  }
  function open(view,title){
    deckUI?.destroy();deckUI=null;stopPortraits();generation++;if(shell.hidden)previousFocus=document.activeElement;shell.hidden=false;shell.replaceChildren();currentView=view;fab.hidden=view==="decks"||view==="menu";
    const panel=element("section","ff-panel ff-view-"+view),header=element("header","ff-header"),titleNode=element("h1","ff-title",title);
    header.appendChild(titleNode);if(view!=="menu")header.appendChild(button("← Menu",()=>showMenu(),"ff-button ff-back"));
    if(matchActive())header.appendChild(button("Return to match",async()=>{if(onResume){const status=shell.querySelector(".ff-status");await invoke(onResume,[profile().activeSessionId],status,{hideOnSuccess:true});}else hide();},"ff-button ff-back"));
    panel.appendChild(header);shell.appendChild(panel);panelArt(panel,view==="campaign"?"campaign-map":view==="forge"?"forge-panel":"menu-panel");
    const connection=getConnectionStatus();if(connection)panel.appendChild(element("p","ff-connection",connection));
    const status=element("p","ff-status");status.setAttribute("role","status");status.setAttribute("aria-live","polite");panel.appendChild(status);
    queueMicrotask(()=>shell.querySelector("button")?.focus());return {panel,status};
  }
  function hide(){deckUI?.destroy();deckUI=null;stopPortraits();shell.hidden=true;currentView="";previousFocus?.focus?.();}
  async function invoke(callback,args,status,{hideOnSuccess=false}={}){if(!callback){status.textContent="This action is temporarily unavailable.";return null;}status.textContent="Preparing…";try{const result=await callback(...args);if(result===false){status.textContent="Unable to complete this action. Try again.";return null;}if(hideOnSuccess)hide();else status.textContent="";updateProfile();return result??{ok:true};}catch(error){status.textContent=error?.message || "Unable to complete this action. Try again.";return null;}}
  function playerSummary(parent){const p=profile(),strip=element("div","ff-player-summary"),mmr=p.crowncouncilElo??1000;strip.append(element("strong","",p.username||"Commander"),element("span","",`MMR ${mmr}`),element("span","",`${countComplete()}/30 missions`),element("span","",`${p.forgeDust||0} Forge Dust`));illustration(strip,"rank-emblems",{className:"ff-rank-emblem",frame:mmr>=2500?4:mmr>=2000?3:mmr>=1600?2:mmr>=1300?1:0,animate:false});parent.appendChild(strip);}
  function menuDeckState(){
    const p=profile(),decks=normalizeDecks(p.decks),deck=decks.find(item=>item.id===p.selectedDeckId)||decks[0];
    return {p,deck,signature:JSON.stringify([p.uid,p.username,deck,deck?.troops.map(key=>p.troops?.[key]?.forgeTier||1)])};
  }
  function selectedDeckSummary(parent,previous){
    const {p,deck,signature}=menuDeckState();
    if(!deck)return;
    const strip=element("section",`ff-menu-deck ff-faction-${deck.faction.toLowerCase()}`),heading=element("div","ff-menu-deck-heading"),portraits=element("div","ff-menu-deck-troops");strip.setAttribute("aria-label","Selected deck");strip.dataset.deckId=deck.id;strip.dataset.signature=signature;
    heading.append(element("span","ff-deck-eyebrow",`${deck.faction} · Selected deck`),element("h2","",deck.name));
    for(const key of deck.troops){const troop=element("div","ff-menu-deck-troop"),name=troops[key]?.name||key;troop.title=name;troop.dataset.troop=key;troop.append(portrait(key,p.troops?.[key]?.forgeTier||1),element("span","",name));portraits.appendChild(troop);}
    strip.append(heading,portraits,button("Change deck",()=>showDecks(),"ff-button ff-secondary ff-menu-change-deck"));if(previous)previous.replaceWith(strip);else parent.appendChild(strip);
  }
  function showMenu(){
    const {panel,status}=open("menu","FACTION FORGE");panelArt(shell,"menu-background");panel.appendChild(element("p","ff-subtitle","The War for the Forge"));playerSummary(panel);
    if(onSignIn && canSignIn())panel.appendChild(button("Sign in to a saved profile",()=>showSignIn(),"ff-button ff-secondary"));
    if(onRetryConnection && canRetryConnection())panel.appendChild(button("Retry profile connection",()=>invoke(onRetryConnection,[],status),"ff-button ff-secondary"));
    if(matchActive()){
      const activeActions=element("div","ff-active-match-actions");activeActions.appendChild(element("p","","A match is in progress. Resume it or leave before starting another."));
      activeActions.appendChild(button("Resume match",()=>invoke(onResume,[profile().activeSessionId],status,{hideOnSuccess:true})));
      if(onAbandon)activeActions.appendChild(button("Leave match",async()=>{const result=await invoke(onAbandon,[profile().activeSessionId],status);if(result)showMenu();},"ff-button ff-secondary"));panel.appendChild(activeActions);
    }
    selectedDeckSummary(panel);const actions=element("div","ff-menu-actions"),collectionActions=element("div","ff-menu-collection-actions");
    const next=missionDefinitions.find(m=>!profile().campaign?.missions?.[m.id]?.completed);
    for(const [title,detail,icon,click] of [
      ["Campaign",next?`Continue: ${next.title}`:"The Last Crucible conquered","campaign-map",()=>showCampaign()],
      ["Play Online","Choose a deck and find an opposing commander","matchmaking",()=>chooseDeck(deck=>{if(onFindMatch)return invoke(onFindMatch,[deck],shell.querySelector(".ff-status")||status);showMatchmaking({status:"Open the CatnMice multiplayer browser to find a match."});})],
      ["Your Decks",`${normalizeDecks(profile().decks).length} teams · Build, customize and prepare for battle`,"how-to-play",()=>showDecks()],
      ["The Forge",`${upgradeCount(profile(),costs)} upgrades ready · Your troop collection`,`forge-button`,()=>showForge()],
      ["How to Play","Draft, position, counter and conquer","how-to-play",()=>showHowToPlay()]
    ]){const primary=["Campaign","Play Online"].includes(title),item=button("",click,"ff-menu-action"+(primary?" ff-menu-primary":" ff-menu-collection-action"));if(matchActive() && primary)item.disabled=true;item.append(element("strong","",title),element("span","",detail));illustration(item,icon,{className:"ff-menu-action-art"});(primary?actions:collectionActions).appendChild(item);}
    panel.append(actions,collectionActions);
    const factionDetails=element("details","ff-menu-factions"),factionShowcase=element("div","ff-faction-showcase");factionDetails.appendChild(element("summary","","Explore the factions"));for(const faction of ["Vanir","Aesir","Helions"]){const card=element("div","ff-faction-banner");card.append(factionArtwork(faction,{animate:true}),element("strong","ff-faction-name",faction));factionShowcase.appendChild(card);}factionDetails.appendChild(factionShowcase);panel.appendChild(factionDetails);
    if(onPractice){const practice=button("Practice against AI",()=>chooseFaction(f=>invoke(onPractice,[f],shell.querySelector(".ff-status")||status,{hideOnSuccess:true})),"ff-button ff-secondary");practice.disabled=matchActive();panel.appendChild(practice);}
  }
  function showSignIn(){
    const {panel,status}=open("sign-in","Cloud progression sign-in"),form=element("form","ff-sign-in");
    const emailLabel=element("label","","Email"),email=element("input","");email.type="email";email.required=true;email.autocomplete="username";email.id="ff-sign-in-email";email.name="email";emailLabel.htmlFor=email.id;
    const passwordLabel=element("label","","Password"),password=element("input","");password.type="password";password.required=true;password.autocomplete="current-password";password.id="ff-sign-in-password";password.name="password";passwordLabel.htmlFor=password.id;
    const submit=button("Sign in",null);submit.type="submit";form.append(emailLabel,email,passwordLabel,password,submit);
    form.onsubmit=async event=>{event.preventDefault();submit.disabled=true;const result=await invoke(onSignIn,[email.value.trim(),password.value],status);password.value="";submit.disabled=false;if(result)showMenu();};
    panel.appendChild(element("p","","Use the same Firebase email account to load its Faction Forge progress on another device. Guest progress belongs to this browser's guest account."));panel.appendChild(form);email.focus();
  }
  function showDecks(callback){
    const {panel,status}=open("decks",callback?"Choose your deck":"Your Decks");
    deckUI=mountDeckLibrary({container:panel,profile:profile(),getProfile:profile,troops,counters,portrait,unlockText,onBeforeRender:stopPortraits,onViewChange:view=>{panel.querySelector(".ff-title").textContent=view==="builder"?"Deck Builder":callback?"Choose your deck":"Your Decks";},isLocked:matchActive,selectionMode:!!callback,close:showMenu,onSave:onSaveDeck,onSelect:async deck=>{
      if(matchActive())throw new Error("Finish your current match before selecting another deck.");
      const owner={uid:profile().uid,username:profile().username};
      if(onSelectDeck){const result=await onSelectDeck(deck);if(result===false)return false;}
      if(!sameDeckOwner(owner,profile()))throw new Error("Your account changed. Select a deck for the current profile.");
      if(callback){const result=await callback(deck);return result===null?false:result;}
      return {ok:true};
    }});return status;
  }
  function chooseDeck(callback){return showDecks(callback);}
  // Kept as an API alias for embedded hosts; callbacks now receive the full deck.
  function chooseFaction(callback){return chooseDeck(callback);}
  function showCampaign(){
    const {panel,status}=open("campaign","The War for the Forge");playerSummary(panel);const completed=profile().campaign?.missions || {},list=element("div","ff-campaign-chapters");
    for(let chapter=1;chapter<=5;chapter++){
      const chapterPanel=element("section","ff-chapter"),heading=element("h2","",`Chapter ${chapter} · ${CHAPTER_NAMES[chapter-1]}`),nodes=element("div","ff-mission-nodes");chapterPanel.appendChild(heading);
      const entries=missionDefinitions.filter(m=>m.chapter===chapter);
      for(const mission of entries){const ordinal=missionDefinitions.findIndex(m=>m.id===mission.id),accessible=ordinal===0 || !!completed[missionDefinitions[ordinal-1]?.id]?.completed,progress=completed[mission.id],node=button("",()=>chooseFaction(f=>{const loading=shell.querySelector(".ff-status") || status;invoke(onCampaignMission,[mission.id,f],loading,{hideOnSuccess:true});}),"ff-mission"+(mission.boss?" ff-boss":"")+(progress?.completed?" ff-completed":"")+(!accessible?" ff-mission-locked":""));node.disabled=!accessible || matchActive();node.setAttribute("aria-label",`${mission.title}${mission.boss?", boss":""}${accessible?"":", locked"}`);node.append(element("span","ff-mission-number",accessible?(mission.boss?"♛":mission.index):"🔒"),element("strong","",mission.title),element("span","ff-stars",progress?.completed?"★".repeat(progress.stars||1)+"☆".repeat(3-(progress.stars||1)):"☆☆☆"),element("span","ff-mission-reward",`${mission.rewards?.cards||3} cards · ${mission.rewards?.dust||20} dust`));if(mission.unlockTroop)node.appendChild(element("span","ff-unlock-reward",`Unlock: ${troops[mission.unlockTroop]?.name||mission.unlockTroop}`));illustration(node,mission.boss?"boss-marker":"mission-marker",{className:"ff-mission-art"});nodes.appendChild(node);}chapterPanel.appendChild(nodes);list.appendChild(chapterPanel);
    }
    panel.appendChild(list);panel.appendChild(button("Open The Forge",()=>showForge(),"ff-button ff-secondary"));
    if(profile().campaign?.endlessUnlocked && onEndless)panel.appendChild(button("Enter the Endless Crucible",()=>chooseFaction(f=>invoke(onEndless,[f],shell.querySelector(".ff-status")||status,{hideOnSuccess:true}))));
  }
  function unlockText(key){const mission=missionDefinitions.find(m=>m.unlockTroop===key),blueprint=`15 blueprints (${profile().troops?.[key]?.blueprints||0}/15)`;return unlockDescriptions[key] || (mission?`Chapter ${mission.chapter}, ${mission.boss?"boss":`Mission ${mission.index}`} or ${blueprint}`:`Collect ${blueprint} to unlock`);}
  function showForge(key=selectedTroop,tab=forgeTab){
    forgeTab=tab==="packs"?"packs":"collection";
    const {panel,status}=open("forge","The Forge");playerSummary(panel);if(matchActive())panel.appendChild(element("p","ff-match-lock","Your collection is viewable during battle. Upgrade and buy packs after the match."));
    const tabs=element("div","ff-forge-tabs");tabs.setAttribute("role","tablist");tabs.setAttribute("aria-label","The Forge");
    for(const [id,label] of [["collection","Collection"],["packs","Buy packs"]]){const node=button(label,()=>showForge(selectedTroop,id),"ff-button ff-filter"+(forgeTab===id?" ff-selected":""));node.setAttribute("role","tab");node.setAttribute("aria-selected",String(forgeTab===id));tabs.appendChild(node);}panel.appendChild(tabs);
    if(forgeTab==="packs"){showPacks(panel,status);return;}
    const filterBar=element("div","ff-filters");for(const faction of ["All","Vanir","Aesir","Helions"]){const node=button(faction,()=>{filter=faction;showForge();},"ff-button ff-filter"+(filter===faction?" ff-selected":""));node.setAttribute("aria-pressed",String(filter===faction));filterBar.appendChild(node);}panel.appendChild(filterBar);
    const body=element("div","ff-forge-body"),grid=element("div","ff-collection"),keys=Object.keys(troops).filter(k=>filter==="All" || troops[k].faction===filter);
    if(!keys.includes(key))key=keys[0];selectedTroop=key;
    for(const troopKey of keys){const data=profile().troops?.[troopKey],tier=data?.forgeTier||1,locked=!data?.unlocked,node=button("",()=>{selectedTroop=troopKey;previewTier=profile().troops?.[troopKey]?.forgeTier||1;showForge(troopKey);},"ff-collection-card"+(troopKey===key?" ff-selected":"")+(locked?" ff-locked-card":""));frame(node,troops[troopKey].faction);if(locked)illustration(node,"locked-card",{className:"ff-locked-card-art"});node.appendChild(portrait(troopKey,tier,{locked}));node.append(element("strong","ff-troop-name",troops[troopKey].name),element("span","ff-troop-role",troops[troopKey].roleLabel||troops[troopKey].role));node.appendChild(element("span","ff-copy-count",locked?unlockText(troopKey):`${data?.cards||0} matching cards`));if(forgeEligibility(profile(),troopKey,{costs}).available)node.appendChild(element("span","ff-ready-tag","Upgrade ready"));grid.appendChild(node);}body.appendChild(grid);
    if(key){const detail=element("section","ff-forge-detail"),data=profile().troops?.[key],eligibility=forgeEligibility(profile(),key,{active:isActiveMatch(),costs}),preview=Math.max(1,Math.min(3,previewTier || data?.forgeTier||1));frame(detail,troops[key].faction);detail.appendChild(portrait(key,preview,{locked:!data?.unlocked,animate:true}));detail.appendChild(element("h2","",troops[key].name));const tiers=element("div","ff-tier-preview");for(let t=1;t<=3;t++){const tab=button(`${ROMAN[t]} · ${TIER_NAMES[t]}`,()=>{previewTier=t;showForge(key);},"ff-button ff-tier-tab"+(t===preview?" ff-selected":""));tab.setAttribute("aria-pressed",String(t===preview));tiers.appendChild(tab);}detail.appendChild(tiers);detail.appendChild(element("p","",troops[key].description||troops[key].counterTip||"Draft this troop where its role can safely support your army."));detail.appendChild(element("p","ff-permanent-note",`Permanent tier: ${TIER_NAMES[data?.forgeTier||1]}. Battle ranks I–III are earned separately during each match.`));
      detail.appendChild(troopDetails(key));
      if(!data?.unlocked)detail.appendChild(element("p","ff-unlock-condition",unlockText(key)));
      if(eligibility.cost){const progress=element("div","ff-upgrade-progress"),cards=element("progress",""),dust=element("progress","");cards.max=eligibility.cost.cards;cards.value=Math.min(cards.max,data?.cards||0);cards.setAttribute("aria-label","Matching card progress");dust.max=eligibility.cost.dust;dust.value=Math.min(dust.max,profile().forgeDust||0);dust.setAttribute("aria-label","Forge Dust progress");progress.append(element("span","",`${data?.cards||0}/${eligibility.cost.cards} cards`),cards,element("span","",`${profile().forgeDust||0}/${eligibility.cost.dust} Forge Dust`),dust);detail.appendChild(progress);}
      const upgrade=button(eligibility.cost?`Forge ${TIER_NAMES[(data?.forgeTier||1)+1]}`:"Elite forged",()=>confirmUpgrade(key,eligibility,status),"ff-button ff-upgrade-button");upgrade.disabled=!eligibility.available;detail.appendChild(upgrade);detail.appendChild(element("p","ff-upgrade-reason",eligibility.reason));if(data?.unlocked && onTargetReward)detail.appendChild(button("Favor this troop in rewards",()=>invoke(onTargetReward,[key],status),"ff-button ff-secondary"));body.appendChild(detail);
    }panel.appendChild(body);
  }
  function showPacks(panel,status){
    panel.appendChild(element("p","ff-subtitle","Choose a fixed card set or open random reinforcements. Every card goes straight into your collection."));
    if(lastPackPurchase&&(lastPackPurchase.uid!==profile().uid||lastPackPurchase.username!==profile().username))lastPackPurchase=null;
    if(lastPackPurchase){const receipt=element("p","ff-pack-receipt",`Opened ${lastPackPurchase.name}: ${lastPackPurchase.cards.map(card=>`${card.quantity} ${troops[card.troopKey]?.name||card.troopKey} cards`).join(", ")}.`);receipt.setAttribute("role","status");panel.appendChild(receipt);}
    const list=element("div","ff-pack-list");
    for(const item of FORGE_PACKS){
      const purchaseState=()=>{const pending=getPendingPackRequest(item.id),normal=packEligibility(profile(),item.id,{active:isActiveMatch()});return pending&&profile().uid&&!matchActive()?{available:true,pending,reason:"Finish revealing your previous purchase."}:normal;};
      const card=element("section","ff-pack"),eligibility=purchaseState();if(item.faction)card.classList.add(`ff-faction-${item.faction.toLowerCase()}`);
      card.dataset.packId=item.id;
      card.append(element("h2","",item.name),element("strong","ff-pack-price",`${item.dust} Forge Dust · ${item.totalCards} cards`));
      const contents=element("ul","ff-pack-contents");
      if(item.random){contents.appendChild(element("li","",`${item.draws} random unlocked troop types × ${item.cardsPerDraw} cards`));contents.appendChild(element("li","",`Pool: all ${randomPackPool(profile()).length} unlocked troop types across every faction.`));contents.appendChild(element("li","","Contents revealed after opening. Different troop types whenever at least three are unlocked."));}
      else for(const reward of item.cards)contents.appendChild(element("li","",`${reward.quantity} × ${troops[reward.troopKey]?.name||reward.troopKey}`));card.appendChild(contents);
      if(eligibility.pending)card.appendChild(element("p","ff-upgrade-reason","Your previous purchase is waiting to reveal its cards. Retry the same purchase safely."));
      const buy=button(packBusy?"Opening pack…":eligibility.pending?`Finish opening ${item.name}`:`Buy ${item.name}`,async()=>{
        const latest=purchaseState();if(packBusy||!latest.available){status.textContent=latest.reason;return;}
        packBusy=true;const owner={uid:profile().uid,username:profile().username};showForge(selectedTroop,"packs");
        const requestId=latest.pending||globalThis.crypto?.randomUUID?.()||`pack-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const result=await invoke(onBuyPack,[item.id,{requestId}],shell.querySelector(".ff-status")||status);packBusy=false;
        if(result){if(owner.uid===profile().uid&&owner.username===profile().username)lastPackPurchase={...owner,name:item.name,cards:result.transaction?.cards||item.cards};showForge(selectedTroop,"packs");}
        else {const message=shell.querySelector(".ff-status")?.textContent||"Unable to open this pack. Retry safely.";showForge(selectedTroop,"packs");shell.querySelector(".ff-status").textContent=message;}
      },"ff-button ff-buy-pack");
      buy.disabled=packBusy||!eligibility.available||!onBuyPack;card.appendChild(buy);
      if(!eligibility.available)card.appendChild(element("p","ff-upgrade-reason",eligibility.reason));list.appendChild(card);
    }
    panel.appendChild(list);panel.appendChild(button("View collection",()=>showForge(selectedTroop,"collection"),"ff-button ff-secondary"));
  }
  function troopDetails(key){
    const content=element("div","ff-troop-details"),troop=troops[key]||{},base=troop.base||{},meta=counters[key]||troop;
    const stats=element("div","ff-troop-stats");
    for(const [label,value] of [["Base HP",base.hp],["Damage",base.dmg],["Armor",base.armor],["Range",base.range],["Attack delay",base.cd?`${base.cd}ms`:undefined],["Footprint",troop.size===2?"2×2":"1×1"]])if(value!==undefined){const item=element("div","ff-troop-stat");item.append(element("strong","",value),element("span","",label));stats.appendChild(item);}
    if(stats.children.length)content.appendChild(stats);
    for(const [label,values] of [["Strengths",meta.strongAgainst],["Weaknesses",meta.weakAgainst],["Pairs with",meta.bestWith]])if(values?.length){const row=element("div","ff-forge-counter");row.append(element("strong","",label+": "),document.createTextNode(Array.isArray(values)?values.join(", "):String(values)));content.appendChild(row);}
    const legendary=legendaryAbilities[key]||troop.legendary;
    if(legendary){const row=element("div","ff-legendary");row.append(element("strong","",`Battle Rank III · ${legendary.name}`),element("p","",legendary.desc||legendary.description||""));content.appendChild(row);}
    return content;
  }
  function confirmUpgrade(key,eligibility,status){
    // Recheck immediately before confirming; account changes or a match start invalidate it.
    const current=forgeEligibility(profile(),key,{active:isActiveMatch(),costs});if(!current.available){status.textContent=current.reason;return;}
    const requestId=globalThis.crypto?.randomUUID?.() || `upgrade-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const backdrop=element("div","ff-confirm"),dialog=element("div","ff-confirm-dialog");dialog.setAttribute("role","dialog");dialog.setAttribute("aria-modal","true");dialog.setAttribute("aria-label","Confirm troop upgrade");dialog.append(element("h2","",`Forge ${troops[key].name} to ${TIER_NAMES[current.tier+1]}?`),element("p","",`Spend ${current.cost.cards} matching cards and ${current.cost.dust} Forge Dust.`));const message=element("p","ff-status");message.setAttribute("role","status");dialog.appendChild(message);const cancel=button("Cancel",()=>{backdrop.remove();}),confirm=button("Confirm upgrade",async()=>{const latest=forgeEligibility(profile(),key,{active:isActiveMatch(),costs});if(!latest.available){message.textContent=latest.reason;confirm.disabled=true;return;}confirm.disabled=true;cancel.disabled=true;backdrop.dataset.pending="true";const result=await invoke(onUpgrade,[key,{requestId}],message);if(result){backdrop.remove();previewTier=latest.tier+1;showForge(key);}else{confirm.disabled=false;cancel.disabled=false;backdrop.dataset.pending="false";}});dialog.append(cancel,confirm);backdrop.appendChild(dialog);shell.appendChild(backdrop);confirm.focus();
  }
  function showResults({won=false,faction="",opponent="",mmr,mmrChange=0,estimatedMMR=false,receipt=null,rewardsPending=false,stars=0,campaignComplete=false,unlockedTroops=[],saveError="",retrySave=null}={}){
    const {panel,status}=open("results",campaignComplete?"The Forge is Yours":won?"Victory":"Defeat"),resultArt=won?"victory":"defeat";if(faction)illustration(panel,`${resultArt}-${String(faction).toLowerCase()}`,{className:"ff-result-art"}).then(loaded=>{if(!loaded)illustration(panel,resultArt,{className:"ff-result-art"});});else illustration(panel,resultArt,{className:"ff-result-art"});if(opponent)panel.appendChild(element("p","ff-subtitle",`${won?"Triumphed over":"Battle against"} ${opponent}`));if(mmr!==undefined)panel.appendChild(element("p","ff-rating",`${estimatedMMR?"Estimated MMR":"MMR"} ${mmr}${mmrChange?` (${mmrChange>0?"+":""}${mmrChange})`:""}`));if(stars)panel.appendChild(element("p","ff-result-stars","★".repeat(stars)+"☆".repeat(3-stars)));if(rewardsPending)status.textContent="Validating result and saving rewards…";
    const reward=receipt?.rewards || receipt;if(reward){const area=element("section","ff-rewards");area.appendChild(element("h2","","Spoils of battle"));illustration(area,"reward-chest",{className:"ff-reward-chest"});area.appendChild(element("strong","ff-dust-reward",`+${reward.dust ?? reward.forgeDust ?? 0} Forge Dust`));const cards=reward.cards || reward.troopCards || {};const cardEntries=Array.isArray(cards)?cards.map(c=>[c.troopKey||c.key||c.troop,c.quantity||c.count||1,c.blueprint]):Object.entries(cards);const grid=element("div","ff-reward-cards");for(const [key,quantity,blueprint] of cardEntries){const troop=troops[key];if(!troop)continue;const card=element("div","ff-reward-card");frame(card,troop.faction);card.appendChild(portrait(key,profile().troops?.[key]?.forgeTier||1));card.append(element("strong","",troop.name),element("span","",`+${quantity} ${blueprint?"blueprints":"cards"}`));const eligibility=forgeEligibility(profile(),key,{costs});if(eligibility.cost)card.appendChild(element("small","",`${profile().troops?.[key]?.cards||0}/${eligibility.cost.cards} toward next tier`));grid.appendChild(card);}area.appendChild(grid);panel.appendChild(area);}
    for(const key of unlockedTroops.length?unlockedTroops:(receipt?.unlocks||[])){const notice=element("div","ff-unlocked-notice");notice.append(element("strong","",`Troop unlocked: ${troops[key]?.name||key}`),portrait(key,1));panel.appendChild(notice);}
    if(saveError){status.textContent=saveError;if(retrySave)panel.appendChild(button("Retry Firebase save",async()=>{status.textContent="Saving your result…";await retrySave();}));}
    const actions=element("div","ff-result-actions");actions.append(button("Campaign",()=>showCampaign()),button("The Forge",()=>showForge()),button("Find Online Match",()=>chooseDeck(deck=>invoke(onFindMatch,[deck],shell.querySelector(".ff-status")||status))),button("Main Menu",()=>{onMenu?.();showMenu();}));panel.appendChild(actions);
  }
  function showMatchmaking(state={}){matchmakingState={...matchmakingState,...state};const {panel,status}=open("matchmaking","Find Online Match");status.textContent=matchmakingState.status||"Searching CatnMice Faction Forge lobbies…";const rows=element("div","ff-match-list");for(const lobby of matchmakingState.rows || []){const row=element("div","ff-match-row"),name=lobby.name||lobby.host||"Open lobby";row.append(element("strong","",name),element("span","",`${lobby.host|| (Array.isArray(lobby.players)?lobby.players[0]:"") || "Commander"} · MMR ${lobby.mmr??1000} · ${lobby.hostUid?(lobby.forgeRanked===false?"Casual":"Ranked"):"Hosted practice"}`));const join=button("Join",async()=>{join.disabled=true;const result=await invoke(matchmakingState.onJoin,[lobby],status);if(result)hide();else join.disabled=false;});row.appendChild(join);rows.appendChild(row);}panel.appendChild(rows);if(matchmakingState.offerAI){const ai=matchmakingState.offerAI,card=element("section","ff-ai-offer");card.append(element("strong","",`${ai.name} · AI`),element("p","",`Estimated MMR ${ai.mmr??ai.estimatedMmr??1000}. Bot rewards; competitive MMR is preserved.`),button("Battle this AI",()=>chooseFaction(f=>invoke(ai.onStart,[f],shell.querySelector(".ff-status")||status,{hideOnSuccess:true}))));panel.appendChild(card);}panel.append(button("Open Multiplayer Browser",()=>invoke(matchmakingState.onBrowse||onBrowseLobbies,[],status),"ff-button ff-secondary"),button("Cancel search",()=>{matchmakingState.onCancel?.();showMenu();},"ff-button ff-secondary"));illustration(panel,"matchmaking",{className:"ff-matchmaking-art"});}
  function showHowToPlay(){const {panel}=open("help","Command the Forge");for(const [title,text] of [
    ["Build your five-troop deck","Select a saved deck before battle. Each deck has five different unlocked troops from one faction. Every recruit, upgrade and combination comes from that team, and at most five combatants can be deployed at once. Edit or duplicate the three starter decks in Your Decks."],
    ["Develop your troops","Protect your damage dealers and support with a frontline. Upgrade battle ranks, then choose a specialization. Rank II develops the signature ability; Rank III amplifies it. Each specialization shows its tradeoff. A full five-unit team receives useful upgrades and combinations instead of unusable recruits."],
    ["Build automatic combinations","Collect complementary troops to activate Basic combos immediately. Upgrade their ranks or draft a combo enhancement for Advanced interactions. Rank III troops with upgraded partners unlock Legendary combinations. The compact tracker shows your relevant partners and counters."],
    ["Know your counters","Armor stalls weak attacks, swarms surround isolated troops, artillery punishes crowded positions and assassins threaten exposed backlines. Read strengths and weaknesses in every troop's details."],
    ["Two separate progression systems","Battle ranks I–III reset each match and retain their legendary abilities. Permanent Forge tiers Recruit, Veteran and Elite spend matching troop cards and Forge Dust and stay on your profile."],
    ["Campaign and collection",`Every round you win grants ${ROUND_WIN_CARDS} troop cards, including rounds in a match you lose. Match wins grant additional cards and Forge Dust; campaign wins also earn stars and six special troops. Daily reward limits apply. Complete all 30 missions to enter the Endless Crucible.`],
    ["Fair online matches","Ranked PvP keeps permanent statistical advantages normalized while showing your earned artwork. Real human results update your existing MMR. AI opponents show Estimated MMR and never change competitive ratings."],
    ["The Forge","Open the circular Forge button to inspect your collection, preview tier artwork and confirm an upgrade. The Buy packs tab offers fixed card sets for Forge Dust and shows every card before purchase. Active battles lock upgrades and pack purchases until the result is complete."]]){panel.append(element("h2","",title),element("p","",text));}}
  function decorateDraftCard(node,{key,type="troop",faction,tier=1,recommended="",copies,detail="",card,roster=[],evolutions={},comboEnhancements=[],availableTypes}={}){
    const presentation=card?draftCardPresentation(card,roster,{faction,troops,evolutions,comboEnhancements,availableTypes}):null;
    key=presentation?.key||key;
    node.classList.add("ff-draft-card");frame(node,faction||troops[key]?.faction);
    const image=node.querySelector(".icon");if(image){image.replaceChildren();if(troops[key]&&["troop","upgrade","double","evolution","combo"].includes(type)){const art=portrait(key,Math.max(tier,presentation?.resultingRank||1),{animate:true});if(presentation){art.querySelector(".ff-tier-badge")?.remove();const battleBadge=element("span","ff-battle-rank",presentation.currentRank&&presentation.currentRank!==presentation.resultingRank?`${ROMAN[presentation.currentRank]} → ${ROMAN[presentation.resultingRank]}`:ROMAN[presentation.resultingRank]);battleBadge.setAttribute("aria-label",presentation.rankLabel);art.appendChild(battleBadge);}image.appendChild(art);}else{const symbol=element("span","ff-special-fallback",image.textContent||"✦");image.appendChild(symbol);illustration(image,`special-${type}`,{className:"ff-special-card-art"}).then(loaded=>{if(loaded)symbol.remove();});}}
    if(presentation){
      node.dataset.battleRank=String(presentation.resultingRank);node.dataset.cardCategory=card.draftCategory||type;
      const oldStars=node.querySelector(".troopStars");if(oldStars)oldStars.remove();
      if(troops[key])node.appendChild(element("div","ff-draft-rank-line",presentation.rankLabel));
      if(presentation.improvement){const short=node.querySelector(".desc.short");if(short){short.textContent=presentation.improvement;short.classList.add("ff-mechanical-improvement");}else node.appendChild(element("p","ff-mechanical-improvement",presentation.improvement));}
      if(presentation.tradeoff)node.appendChild(element("small","ff-draft-tradeoff",presentation.tradeoff));
      for(const combo of presentation.comboPreview){const label=combo.unlocks?"UNLOCKS":combo.enhances?"ENHANCES":combo.missing?.length?`${combo.missing.length} missing`:"ACTIVE";const line=element("div","ff-draft-combo"+(combo.unlocks?" ff-combo-unlock":""));line.append(element("strong","",`${label} · ${combo.name}`),element("span","",combo.description));node.appendChild(line);}
    }
    if(recommended){const tag=element("span","ff-recommendation",recommended);tag.title=String(detail||"").replace(/<[^>]*>/g," ").replace(/\s+/g," ").trim();node.appendChild(tag);}if(copies!==undefined)node.appendChild(element("span","ff-draft-copies",`${copies} in roster`));
    const info=node.querySelector(".cardInfoBtn");if(info)info.setAttribute("aria-label",`Inspect ${troops[key]?.name||key||type}`);
    node.addEventListener("click",event=>{if(event.target.closest?.(".cardInfoBtn"))return;node.classList.add("ff-card-selected");});
  }
  function updateComboTracker({host:target=host,roster=[],faction,availableTypes,comboEnhancements=[]}={}){
    let tracker=target.querySelector(":scope > .ff-combo-tracker");
    const combos=relevantComboStates(roster,faction,{availableTypes,comboEnhancements});
    if(!combos.length){tracker?.remove();return;}
    if(!tracker){tracker=element("details","ff-combo-tracker");tracker.open=!globalThis.matchMedia?.("(max-width: 600px)")?.matches;tracker.setAttribute("aria-label","Your unit combinations");for(const eventName of ["pointerdown","mousedown","touchstart","wheel"])tracker.addEventListener(eventName,event=>event.stopPropagation());target.appendChild(tracker);}
    const signature=JSON.stringify(combos.map(combo=>[combo.id,combo.level,combo.missing]));
    if(tracker.dataset.signature===signature)return;
    tracker.dataset.signature=signature;tracker.replaceChildren();
    const active=combos.filter(combo=>!combo.missing?.length).length;tracker.appendChild(element("summary","ff-combo-summary",`Combos · ${active} active${combos.length>active?` · ${combos.length-active} developing`:""}`));
    for(const combo of combos){const row=element("div","ff-combo-row"+(combo.missing?.length?"":" ff-combo-active")),level=combo.level>=3?"LEGENDARY":combo.level>=2?"ADVANCED":"BASIC",status=combo.missing?.length?`${combo.missing.length} UNIT${combo.missing.length===1?"":"S"} MISSING`:`${level} · ACTIVE`;
      row.append(element("strong","",combo.name),element("small","ff-combo-status",status),element("span","ff-combo-partners",combo.units.map(key=>troops[key]?.name||key).join(" + ")),element("span","ff-combo-description",combo.description));
      if(!combo.missing?.length&&combo.level===1&&!comboEnhancements.includes(combo.id))row.appendChild(element("small","ff-combo-ready","Ready for a combo enhancement"));
      if(combo.counterplay)row.title=combo.counterplay;tracker.appendChild(row);
    }
  }
  function announceBattleUpgrade(card,roster=[],options={}){
    if(!(card?.type==="evolution"||card?.type==="special"&&card.special==="upgrade"||card?.type==="upgrade"))return;
    const presentation=draftCardPresentation(card,roster,{troops,...options});if(!troops[presentation.key])return;
    host.querySelector(".ff-battle-upgrade-toast")?.remove();
    const toast=element("aside","ff-battle-upgrade-toast"),art=portrait(presentation.key,Math.max(options.tier||1,presentation.resultingRank));
    art.querySelector(".ff-tier-badge")?.remove();art.appendChild(element("span","ff-battle-rank",ROMAN[presentation.resultingRank]));
    toast.setAttribute("role","status");toast.setAttribute("aria-live","polite");toast.append(art,element("strong","",`${presentation.troopName} · ${presentation.rankLabel}`),element("span","",presentation.improvement));host.appendChild(toast);
    setTimeout(()=>toast.remove(),2600);
  }
  function updateProfile(){const count=upgradeCount(profile(),costs),indicator=fab.querySelector(".ff-upgrade-count");indicator.hidden=count===0;indicator.textContent=String(count);fab.title=`The Forge${count?` · ${count} upgrades ready`:""}`;if(currentView==="decks")deckUI?.refresh(profile());if(currentView==="menu"){const strip=shell.querySelector(".ff-menu-deck");if(strip&&strip.dataset.signature!==menuDeckState().signature)selectedDeckSummary(strip.parentNode,strip);}}
  function refresh(){if(currentView==="forge")showForge();else if(currentView==="campaign")showCampaign();else if(currentView==="menu")showMenu();updateProfile();}
  const keyboard=event=>{if(shell.hidden)return;if(event.key==="Escape"){const confirmation=shell.querySelector(".ff-confirm");if(confirmation){if(confirmation.dataset.pending!=="true")confirmation.remove();else event.preventDefault();}else if(matchActive())hide();else if(currentView!=="menu")showMenu();}if(event.key==="Tab"){const confirmation=shell.querySelector(".ff-confirm"),scope=confirmation||shell,warning=document.getElementById("ffGraphicsWarnings"),warningControls=!confirmation&&warning&&!warning.hidden?[warning.querySelector("summary"),...(warning.open?warning.querySelectorAll("button:not(:disabled),a[href]"):[])]:[],focusable=[...scope.querySelectorAll("button:not(:disabled),a[href],input,summary"),...warningControls].filter(e=>e&&!e.hidden&&e.getClientRects().length);if(!focusable.length)return;const index=focusable.indexOf(document.activeElement);if(index>=0){event.preventDefault();focusable[(index+(event.shiftKey?-1:1)+focusable.length)%focusable.length].focus();}}};document.addEventListener("keydown",keyboard);
  updateProfile();
  function notify(message){let status=shell.querySelector(".ff-status");if(!status){status=element("div","ff-toast");status.setAttribute("role","status");host.appendChild(status);setTimeout(()=>status.remove(),7000);}status.textContent=String(message||"");}
  return {showMenu,showCampaign,showForge,showDecks,showResults,showMatchmaking,setMatchmakingState:showMatchmaking,showHowToPlay,showSignIn,chooseDeck,chooseFaction,decorateDraftCard,updateComboTracker,announceBattleUpgrade,portrait,badge,hide,refresh,updateProfile,notify,get view(){return currentView;},destroy(){deckUI?.destroy();stopPortraits();document.removeEventListener("keydown",keyboard);observer?.disconnect();lazyPortraits.clear();host.querySelector(".ff-battle-upgrade-toast")?.remove();shell.remove();fab.remove();}};
}




