import {animationDefinition,frameRectangle,drawSpriteFrame} from "./assets.js";

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

export function createExpansionUI({host=globalThis.document?.body,assets,troops={},counters={},legendaryAbilities={},getProfile=()=>null,isActiveMatch=()=>false,missionDefinitions=[],costs=DEFAULT_COSTS,fallbackPath=()=>"",onCampaignMission,onPractice,onEndless,onFindMatch,onUpgrade,onTargetReward,onMenu,onBrowseLobbies,onSignIn,onResume,onAbandon,unlockDescriptions={},getConnectionStatus=()=>""}={}){
  if(!host)throw new Error("Faction Forge UI requires a DOM host.");
  const document=host.ownerDocument, shell=document.createElement("div"), fab=document.createElement("button");
  shell.className="ff-expansion";shell.hidden=true;shell.setAttribute("aria-label","Faction Forge");host.appendChild(shell);
  fab.className="ff-forge-fab";fab.type="button";fab.setAttribute("aria-label","Open The Forge");fab.innerHTML='<span class="ff-forge-fab-icon" aria-hidden="true">⚒</span><span class="ff-upgrade-count" hidden></span>';fab.onclick=()=>showForge();host.appendChild(fab);
  let currentView="",filter="All",selectedTroop=null,previewTier=1,matchmakingState={},previousFocus=null,generation=0;
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
    if(!assets?.enabled())return;try{const item=await assets.asset(group,key);if(item){parent.style.backgroundImage=`url("${item.url}")`;parent.classList.add("ff-has-art");}}catch{}
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
    const loadPortrait=()=>assets.unit(key,tier).then(sheet=>{
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
  function open(view,title){
    stopPortraits();generation++;if(shell.hidden)previousFocus=document.activeElement;shell.hidden=false;shell.replaceChildren();currentView=view;
    const panel=element("section","ff-panel ff-view-"+view),header=element("header","ff-header"),titleNode=element("h1","ff-title",title);
    header.appendChild(titleNode);if(view!=="menu")header.appendChild(button("← Menu",()=>showMenu(),"ff-button ff-back"));
    if(matchActive())header.appendChild(button("Return to match",async()=>{if(onResume){const status=shell.querySelector(".ff-status");await invoke(onResume,[profile().activeSessionId],status,{hideOnSuccess:true});}else hide();},"ff-button ff-back"));
    panel.appendChild(header);shell.appendChild(panel);panelArt(panel,view==="campaign"?"campaign-map":view==="forge"?"forge-panel":"menu-panel");
    const connection=getConnectionStatus();if(connection)panel.appendChild(element("p","ff-connection",connection));
    const status=element("p","ff-status");status.setAttribute("role","status");status.setAttribute("aria-live","polite");panel.appendChild(status);
    queueMicrotask(()=>shell.querySelector("button")?.focus());return {panel,status};
  }
  function hide(){stopPortraits();shell.hidden=true;currentView="";previousFocus?.focus?.();}
  async function invoke(callback,args,status,{hideOnSuccess=false}={}){if(!callback){status.textContent="This action is temporarily unavailable.";return null;}status.textContent="Preparing…";try{const result=await callback(...args);if(result===false){status.textContent="Unable to complete this action. Try again.";return null;}if(hideOnSuccess)hide();else status.textContent="";updateProfile();return result??{ok:true};}catch(error){status.textContent=error?.message || "Unable to complete this action. Try again.";return null;}}
  function playerSummary(parent){const p=profile(),strip=element("div","ff-player-summary"),mmr=p.crowncouncilElo??1000;strip.append(element("strong","",p.username||"Commander"),element("span","",`MMR ${mmr}`),element("span","",`${countComplete()}/30 missions`),element("span","",`${p.forgeDust||0} Forge Dust`));illustration(strip,"rank-emblems",{className:"ff-rank-emblem",frame:mmr>=2500?4:mmr>=2000?3:mmr>=1600?2:mmr>=1300?1:0,animate:false});parent.appendChild(strip);}
  function showMenu(){
    const {panel,status}=open("menu","FACTION FORGE");panelArt(shell,"menu-background");panel.appendChild(element("p","ff-subtitle","The War for the Forge"));playerSummary(panel);
    if(onSignIn && !profile().uid)panel.appendChild(button("Sign in for cloud progression",()=>showSignIn(),"ff-button ff-secondary"));
    if(matchActive()){
      const activeActions=element("div","ff-active-match-actions");activeActions.appendChild(element("p","","A match is in progress. Resume it or leave before starting another."));
      activeActions.appendChild(button("Resume match",()=>invoke(onResume,[profile().activeSessionId],status,{hideOnSuccess:true})));
      if(onAbandon)activeActions.appendChild(button("Leave match",async()=>{const result=await invoke(onAbandon,[profile().activeSessionId],status);if(result)showMenu();},"ff-button ff-secondary"));panel.appendChild(activeActions);
    }
    const actions=element("div","ff-menu-actions");
    const next=missionDefinitions.find(m=>!profile().campaign?.missions?.[m.id]?.completed);
    for(const [title,detail,icon,click] of [
      ["Campaign",next?`Continue: ${next.title}`:"The Last Crucible conquered","campaign-map",()=>showCampaign()],
      ["Play Online","Find a commander in the CatnMice lobbies","matchmaking",()=>{if(onFindMatch)invoke(onFindMatch,[],status);else showMatchmaking({status:"Open the CatnMice multiplayer browser to find a match."});}],
      ["The Forge",`${upgradeCount(profile(),costs)} upgrades ready · Your troop collection`,`forge-button`,()=>showForge()],
      ["How to Play","Draft, position, counter and conquer","how-to-play",()=>showHowToPlay()]
    ]){const item=button("",click,"ff-menu-action");if(matchActive() && ["Campaign","Play Online"].includes(title))item.disabled=true;item.append(element("strong","",title),element("span","",detail));illustration(item,icon,{className:"ff-menu-action-art"});actions.appendChild(item);}
    panel.appendChild(actions);
    const factionShowcase=element("div","ff-faction-showcase");for(const faction of ["Vanir","Aesir","Helions"]){const card=element("div","ff-faction-banner"),key=Object.keys(troops).find(k=>troops[k].faction===faction);frame(card,faction);if(key)card.appendChild(portrait(key,profile().troops?.[key]?.forgeTier||1,{animate:true}));card.appendChild(element("strong","",faction));factionShowcase.appendChild(card);}panel.appendChild(factionShowcase);
    if(onPractice){const practice=button("Practice against AI",()=>chooseFaction(f=>invoke(onPractice,[f],shell.querySelector(".ff-status")||status,{hideOnSuccess:true})),"ff-button ff-secondary");practice.disabled=matchActive();panel.appendChild(practice);}
  }
  function showSignIn(){
    const {panel,status}=open("sign-in","Cloud progression sign-in"),form=element("form","ff-sign-in");
    const emailLabel=element("label","","Email"),email=element("input","");email.type="email";email.required=true;email.autocomplete="username";email.id="ff-sign-in-email";email.name="email";emailLabel.htmlFor=email.id;
    const passwordLabel=element("label","","Password"),password=element("input","");password.type="password";password.required=true;password.autocomplete="current-password";password.id="ff-sign-in-password";password.name="password";passwordLabel.htmlFor=password.id;
    const submit=button("Sign in",null);submit.type="submit";form.append(emailLabel,email,passwordLabel,password,submit);
    form.onsubmit=async event=>{event.preventDefault();submit.disabled=true;const result=await invoke(onSignIn,[email.value.trim(),password.value],status);password.value="";submit.disabled=false;if(result)showMenu();};
    panel.appendChild(element("p","","Sign in with the verified email account linked to your CatnMice profile to continue on another device."));panel.appendChild(form);email.focus();
  }
  function chooseFaction(callback){
    const {panel,status}=open("faction","Choose your faction"),grid=element("div","ff-faction-choices");
    for(const faction of ["Vanir","Aesir","Helions"]){const pick=button("",()=>callback(faction),"ff-faction-choice");frame(pick,faction);const key=Object.keys(troops).find(k=>troops[k].faction===faction);if(key)pick.appendChild(portrait(key,profile().troops?.[key]?.forgeTier||1));pick.append(element("strong","",faction),element("span","",faction==="Vanir"?"Industrial firepower and armor":faction==="Aesir"?"Arcane control and protective magic":"Swarm aggression and corruption"));grid.appendChild(pick);}panel.appendChild(grid);return status;
  }
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
  function showForge(key=selectedTroop){
    const {panel,status}=open("forge","The Forge");playerSummary(panel);if(matchActive())panel.appendChild(element("p","ff-match-lock","Your collection is viewable during battle. Upgrade after the match."));
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
  function showResults({won=false,faction="",opponent="",mmr,mmrChange=0,estimatedMMR=false,receipt=null,rewardsPending=false,stars=0,campaignComplete=false,unlockedTroops=[]}={}){
    const {panel,status}=open("results",campaignComplete?"The Forge is Yours":won?"Victory":"Defeat"),resultArt=won?"victory":"defeat";if(faction)illustration(panel,`${resultArt}-${String(faction).toLowerCase()}`,{className:"ff-result-art"}).then(loaded=>{if(!loaded)illustration(panel,resultArt,{className:"ff-result-art"});});else illustration(panel,resultArt,{className:"ff-result-art"});if(opponent)panel.appendChild(element("p","ff-subtitle",`${won?"Triumphed over":"Battle against"} ${opponent}`));if(mmr!==undefined)panel.appendChild(element("p","ff-rating",`${estimatedMMR?"Estimated MMR":"MMR"} ${mmr}${mmrChange?` (${mmrChange>0?"+":""}${mmrChange})`:""}`));if(stars)panel.appendChild(element("p","ff-result-stars","★".repeat(stars)+"☆".repeat(3-stars)));if(rewardsPending)status.textContent="Validating result and saving rewards…";
    const reward=receipt?.rewards || receipt;if(reward){const area=element("section","ff-rewards");area.appendChild(element("h2","","Spoils of battle"));illustration(area,"reward-chest",{className:"ff-reward-chest"});area.appendChild(element("strong","ff-dust-reward",`+${reward.dust ?? reward.forgeDust ?? 0} Forge Dust`));const cards=reward.cards || reward.troopCards || {};const cardEntries=Array.isArray(cards)?cards.map(c=>[c.troopKey||c.key||c.troop,c.quantity||c.count||1,c.blueprint]):Object.entries(cards);const grid=element("div","ff-reward-cards");for(const [key,quantity,blueprint] of cardEntries){const troop=troops[key];if(!troop)continue;const card=element("div","ff-reward-card");frame(card,troop.faction);card.appendChild(portrait(key,profile().troops?.[key]?.forgeTier||1));card.append(element("strong","",troop.name),element("span","",`+${quantity} ${blueprint?"blueprints":"cards"}`));const eligibility=forgeEligibility(profile(),key,{costs});if(eligibility.cost)card.appendChild(element("small","",`${profile().troops?.[key]?.cards||0}/${eligibility.cost.cards} toward next tier`));grid.appendChild(card);}area.appendChild(grid);panel.appendChild(area);}
    for(const key of unlockedTroops.length?unlockedTroops:(receipt?.unlocks||[])){const notice=element("div","ff-unlocked-notice");notice.append(element("strong","",`Troop unlocked: ${troops[key]?.name||key}`),portrait(key,1));panel.appendChild(notice);}
    const actions=element("div","ff-result-actions");actions.append(button("Campaign",()=>showCampaign()),button("The Forge",()=>showForge()),button("Find Online Match",()=>invoke(onFindMatch,[],status)),button("Main Menu",()=>{onMenu?.();showMenu();}));panel.appendChild(actions);
  }
  function showMatchmaking(state={}){matchmakingState={...matchmakingState,...state};const {panel,status}=open("matchmaking","Find Online Match");status.textContent=matchmakingState.status||"Searching CatnMice Faction Forge lobbies…";const rows=element("div","ff-match-list");for(const lobby of matchmakingState.rows || []){const row=element("div","ff-match-row"),name=lobby.name||lobby.host||"Open lobby";row.append(element("strong","",name),element("span","",`${lobby.host|| (Array.isArray(lobby.players)?lobby.players[0]:"") || "Commander"} · MMR ${lobby.mmr??1000} · ${lobby.hostUid?(lobby.forgeRanked===false?"Casual":"Ranked"):"Hosted practice"}`));const join=button("Join",async()=>{join.disabled=true;const result=await invoke(matchmakingState.onJoin,[lobby],status);if(result)hide();else join.disabled=false;});row.appendChild(join);rows.appendChild(row);}panel.appendChild(rows);if(matchmakingState.offerAI){const ai=matchmakingState.offerAI,card=element("section","ff-ai-offer");card.append(element("strong","",`${ai.name} · AI`),element("p","",`Estimated MMR ${ai.mmr??ai.estimatedMmr??1000}. Bot rewards; competitive MMR is preserved.`),button("Battle this AI",()=>chooseFaction(f=>invoke(ai.onStart,[f],shell.querySelector(".ff-status")||status,{hideOnSuccess:true}))));panel.appendChild(card);}panel.append(button("Open Multiplayer Browser",()=>invoke(matchmakingState.onBrowse||onBrowseLobbies,[],status),"ff-button ff-secondary"),button("Cancel search",()=>{matchmakingState.onCancel?.();showMenu();},"ff-button ff-secondary"));illustration(panel,"matchmaking",{className:"ff-matchmaking-art"});}
  function showHowToPlay(){const {panel}=open("help","Command the Forge");for(const [title,text] of [
    ["Draft a coherent army","Choose a faction, then draft troops and supporting relics, tactics and formations. Inspect a card with its ? button before selecting it. Combine a durable frontline with damage and support."],
    ["Know your counters","Armor stalls weak attacks, swarms surround isolated troops, artillery punishes crowded positions and assassins threaten exposed backlines. Read strengths and weaknesses in every troop's details."],
    ["Two separate progression systems","Battle ranks I–III reset each match and retain their legendary abilities. Permanent Forge tiers Recruit, Veteran and Elite spend matching troop cards and Forge Dust and stay on your profile."],
    ["Campaign and collection","Win missions to earn stars, cards, Forge Dust and six special troops. Replay earlier missions for reduced rewards. Complete all 30 missions to enter the Endless Crucible."],
    ["Fair online matches","Ranked PvP keeps permanent statistical advantages normalized while showing your earned artwork. Real human results update your existing MMR. AI opponents show Estimated MMR and never change competitive ratings."],
    ["The Forge","Open the circular Forge button to inspect your collection, preview tier artwork and confirm one upgrade. Active battles lock permanent upgrades until the result is complete."]]){panel.append(element("h2","",title),element("p","",text));}}
  function decorateDraftCard(node,{key,type="troop",faction,tier=1,recommended="",copies,detail=""}={}){
    node.classList.add("ff-draft-card");frame(node,faction||troops[key]?.faction);
    const image=node.querySelector(".icon");if(image){image.replaceChildren();if(type==="troop")image.appendChild(portrait(key,tier,{animate:true}));else{const symbol=element("span","ff-special-fallback",image.textContent||"✦");image.appendChild(symbol);illustration(image,`special-${type}`,{className:"ff-special-card-art"}).then(loaded=>{if(loaded)symbol.remove();});}}
    if(recommended){const tag=element("span","ff-recommendation",recommended);tag.title=String(detail||"").replace(/<[^>]*>/g," ").replace(/\s+/g," ").trim();node.appendChild(tag);}if(copies!==undefined)node.appendChild(element("span","ff-draft-copies",`${copies} in roster`));
    const info=node.querySelector(".cardInfoBtn");if(info)info.setAttribute("aria-label",`Inspect ${troops[key]?.name||key||type}`);
    node.addEventListener("click",event=>{if(event.target.closest?.(".cardInfoBtn"))return;node.classList.add("ff-card-selected");});
  }
  function updateProfile(){const count=upgradeCount(profile(),costs),indicator=fab.querySelector(".ff-upgrade-count");indicator.hidden=count===0;indicator.textContent=String(count);fab.title=`The Forge${count?` · ${count} upgrades ready`:""}`;}
  function refresh(){if(currentView==="forge")showForge();else if(currentView==="campaign")showCampaign();else if(currentView==="menu")showMenu();updateProfile();}
  const keyboard=event=>{if(shell.hidden)return;if(event.key==="Escape"){const confirmation=shell.querySelector(".ff-confirm");if(confirmation){if(confirmation.dataset.pending!=="true")confirmation.remove();else event.preventDefault();}else if(matchActive())hide();else if(currentView!=="menu")showMenu();}if(event.key==="Tab"){const scope=shell.querySelector(".ff-confirm")||shell,focusable=[...scope.querySelectorAll("button:not(:disabled),a[href],input")].filter(e=>!e.hidden);if(!focusable.length)return;const first=focusable[0],last=focusable.at(-1);if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}}};document.addEventListener("keydown",keyboard);
  updateProfile();
  function notify(message){let status=shell.querySelector(".ff-status");if(!status){status=element("div","ff-toast");status.setAttribute("role","status");host.appendChild(status);setTimeout(()=>status.remove(),7000);}status.textContent=String(message||"");}
  return {showMenu,showCampaign,showForge,showResults,showMatchmaking,setMatchmakingState:showMatchmaking,showHowToPlay,showSignIn,chooseFaction,decorateDraftCard,portrait,badge,hide,refresh,updateProfile,notify,get view(){return currentView;},destroy(){stopPortraits();document.removeEventListener("keydown",keyboard);observer?.disconnect();lazyPortraits.clear();shell.remove();fab.remove();}};
}
