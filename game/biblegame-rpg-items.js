const RARITY_META={
  common:{label:"Common",rank:0},
  uncommon:{label:"Uncommon",rank:1},
  rare:{label:"Rare",rank:2},
  epic:{label:"Epic",rank:3}
};

const ITEM_DEFS={
  "healing-draught":{name:"Healing Draught",icon:"🧪",rarity:"common",desc:"Restore 25% of maximum health.",kind:"heal",amount:.25},
  "word-fragment":{name:"Word Fragment",icon:"🕯️",rarity:"common",desc:"Reveal a short word fragment from a verse that can hurt this enemy, but no book or reference.",kind:"hint-words"},
  "greater-healing":{name:"Greater Healing Potion",icon:"❤️",rarity:"uncommon",desc:"Restore 45% of maximum health.",kind:"heal",amount:.45},
  "book-lantern":{name:"Book Lantern",icon:"📗",rarity:"uncommon",desc:"Reveal the Bible book containing one unused verse that can hurt this enemy.",kind:"hint-book"},
  "category-rune":{name:"Category Rune",icon:"🔷",rarity:"uncommon",desc:"Add one random Scripture category to this enemy for the rest of the fight.",kind:"category",count:1,multiplier:1},
  "armor-of-faith":{name:"Armor of Faith",icon:"🛡️",rarity:"uncommon",desc:"Block the next enemy counterattack caused by a failed verse.",kind:"shield"},
  "restoration-flask":{name:"Restoration Flask",icon:"💖",rarity:"rare",desc:"Restore all health.",kind:"heal",amount:1},
  "chapter-map":{name:"Chapter Map",icon:"🗺️",rarity:"rare",desc:"Reveal the book and chapter of one unused verse that can hurt this enemy.",kind:"hint-chapter"},
  "wild-scripture":{name:"Wild Scripture",icon:"✨",rarity:"rare",desc:"Add two random Scripture categories to this enemy for the rest of the fight.",kind:"category",count:2,multiplier:1.05},
  "scroll-of-recall":{name:"Scroll of Recall",icon:"📜",rarity:"rare",desc:"Refresh every verse already used during this run so those references can be played again.",kind:"refresh"},
  "reference-compass":{name:"Reference Compass",icon:"🧭",rarity:"epic",desc:"Reveal the exact reference of one unused verse that can hurt this enemy, without revealing its text.",kind:"hint-reference"},
  "many-doors-seal":{name:"Seal of Many Doors",icon:"🌟",rarity:"epic",desc:"Add three random Scripture categories at strong effectiveness for the rest of this fight.",kind:"category",count:3,multiplier:1.2},
  "great-recall":{name:"Great Scroll of Recall",icon:"📖",rarity:"epic",desc:"Refresh used verses, restore 35% health, and grant one Armor of Faith charge.",kind:"great-recall"}
};

const ITEMS_BY_RARITY=Object.entries(ITEM_DEFS).reduce((out,[id,item])=>{
  (out[item.rarity]??=[]).push(id);
  return out;
},{});

function randomChoice(list){return list[Math.floor(Math.random()*list.length)]}
function escapeHtml(value){return String(value??"").replace(/[&<>'\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'\"':"&quot;"}[c]))}
function itemName(id){return ITEM_DEFS[id]?.name||id}

export function installRpgItems({getRpg,getVerses,conceptKeys,conceptLabel,matchWeightedConcepts,matchVerseConcept,refKey,displayRef,refresh}){
  let uiReady=false;

  function ensureState(){
    const rpg=getRpg();
    if(!rpg)return null;
    if(!Array.isArray(rpg.inventory))rpg.inventory=[];
    if(!Number.isFinite(rpg.itemShields))rpg.itemShields=0;
    return rpg;
  }

  function installUi(){
    if(uiReady)return;
    const battlefield=document.getElementById("battlefield"),input=document.getElementById("rpgRefInput"),attack=document.getElementById("rpgAttackBtn");
    if(!battlefield||!input||!attack)return;
    uiReady=true;

    const style=document.createElement("style");
    style.textContent=`
      .rpg-item-actions{grid-template-columns:minmax(0,1fr) auto auto!important}
      .rpg-item-button{position:relative;white-space:nowrap}
      .rpg-item-badge{display:inline-grid;place-items:center;min-width:20px;height:20px;margin-left:5px;padding:0 5px;border-radius:999px;background:#fbbf24;color:#291b03;font-size:.68rem;font-weight:1000}
      .rpg-item-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;max-height:390px;overflow:auto;margin-top:10px}
      .rpg-item-card{border:1px solid #334155;border-radius:13px;background:#0c1325;color:#fff;padding:11px;text-align:left;cursor:pointer}
      .rpg-item-card:hover{border-color:#8b5cf6}.rpg-item-card:disabled{opacity:.48;cursor:not-allowed}
      .rpg-item-top{display:flex;justify-content:space-between;gap:8px;align-items:flex-start}.rpg-item-name{font-weight:1000}.rpg-item-count{color:#fde68a;font-weight:1000}
      .rpg-item-rarity{font-size:.62rem;text-transform:uppercase;letter-spacing:.1em;color:#c4b5fd;font-weight:1000;margin:3px 0 6px}.rpg-item-desc{font-size:.76rem;line-height:1.4;color:#a5b4cc}
      .rpg-hint-reveal{margin:8px 0 0;padding:8px 10px;border:1px solid #854d0e;border-radius:10px;background:#2a1d08;color:#fde68a;font-size:.79rem;font-weight:850}
      .rpg-loot-notice{margin-top:7px;padding:7px 9px;border:1px solid #14532d;border-radius:10px;background:#0d2819;color:#bbf7d0;font-size:.78rem;font-weight:850}
      .rpg-item-summary{font-size:.7rem;color:#cbd5e1;margin-top:5px;text-align:center}
      @media(max-width:570px){.rpg-item-actions{grid-template-columns:1fr!important}.rpg-item-grid{grid-template-columns:1fr}.rpg-item-button{width:100%}}
    `;
    document.head.appendChild(style);

    const row=input.closest(".input-row");
    if(row){
      row.classList.add("rpg-item-actions");
      const button=document.createElement("button");
      button.className="ghost rpg-item-button";
      button.id="rpgItemsBtn";
      button.type="button";
      button.innerHTML='🎒 Items <span class="rpg-item-badge" id="rpgItemCount">0</span>';
      button.onclick=openPanel;
      row.appendChild(button);
    }

    const combatLog=document.getElementById("combatLog");
    if(combatLog){
      const hint=document.createElement("div");hint.id="rpgHintReveal";hint.className="rpg-hint-reveal hidden";combatLog.before(hint);
      const loot=document.createElement("div");loot.id="rpgLootNotice";loot.className="rpg-loot-notice hidden";combatLog.after(loot);
      const summary=document.createElement("div");summary.id="rpgItemSummary";summary.className="rpg-item-summary";loot.after(summary);
    }

    const overlay=document.createElement("div");
    overlay.className="choice-overlay hidden";
    overlay.id="itemOverlay";
    overlay.innerHTML=`<div class="choice-card"><div class="row between"><div><div class="eyebrow">Run Inventory</div><h2 style="margin:6px 0">Use an item</h2></div><button class="ghost" id="itemCloseBtn" type="button">Close</button></div><p class="muted" id="itemPanelHint">Items are consumable and apply to the current fight.</p><div class="rpg-item-grid" id="rpgItemGrid"></div></div>`;
    battlefield.appendChild(overlay);
    document.getElementById("itemCloseBtn").onclick=closePanel;
    overlay.addEventListener("click",event=>{if(event.target===overlay)closePanel()});
  }

  function reset(){
    installUi();
    const rpg=ensureState();
    if(!rpg)return;
    rpg.inventory=[];
    rpg.itemShields=0;
    hideHint();
    hideLoot();
    closePanel();
    render();
  }

  function onSpawn(){
    installUi();
    hideHint();
    hideLoot();
    closePanel();
    render();
  }

  function activeFight(){
    const rpg=ensureState();
    return !!(rpg&&!rpg.dead&&rpg.enemy&&rpg.enemy.hp>0);
  }

  function openPanel(){
    installUi();
    if(!activeFight())return;
    renderPanel();
    document.getElementById("itemOverlay")?.classList.remove("hidden");
  }

  function closePanel(){document.getElementById("itemOverlay")?.classList.add("hidden")}
  function hideHint(){const el=document.getElementById("rpgHintReveal");if(el){el.textContent="";el.classList.add("hidden")}}
  function hideLoot(){const el=document.getElementById("rpgLootNotice");if(el){el.textContent="";el.classList.add("hidden")}}

  function showHint(text){
    const el=document.getElementById("rpgHintReveal");
    if(!el)return;
    el.textContent=text;
    el.classList.remove("hidden");
  }

  function showLoot(drops,foe){
    const el=document.getElementById("rpgLootNotice");
    if(!el)return;
    if(!drops.length){el.classList.add("hidden");el.textContent="";return}
    const names=drops.map(id=>`${ITEM_DEFS[id].icon} ${ITEM_DEFS[id].name}`).join(" • ");
    const source=foe.boss?"Boss reward":foe.elite?"Elite reward":"Victory loot";
    el.textContent=`${source}: ${names}`;
    el.classList.remove("hidden");
  }

  function rarityRoll(foe){
    const rpg=ensureState();
    const depthBoost=Math.min(.22,Math.max(0,(rpg?.floor||1)-1)*.009);
    const challengeBoost=(foe?.elite?.22?0:0);
    const explicitChallengeBoost=(foe?.elite?0.22:0)+(foe?.boss?0.36:0);
    const roll=Math.random()+depthBoost+explicitChallengeBoost;
    if(roll>=1.17)return "epic";
    if(roll>=.83)return "rare";
    if(roll>=.47)return "uncommon";
    return "common";
  }

  function rollItem(foe){
    const rarity=rarityRoll(foe),pool=ITEMS_BY_RARITY[rarity]||ITEMS_BY_RARITY.common;
    return randomChoice(pool);
  }

  function awardLoot(foe){
    installUi();
    const rpg=ensureState();
    if(!rpg)return [];
    closePanel();
    const baseChance=Math.min(.80,.48+Math.max(0,rpg.floor-1)*.012);
    const chance=foe?.boss?1:(foe?.elite?.98?0:baseChance);
    const explicitChance=foe?.boss?1:(foe?.elite?0.98:baseChance);
    if(Math.random()>explicitChance){showLoot([],foe);render();return []}
    let count=1;
    if(foe?.elite&&Math.random()<.38)count++;
    if(foe?.boss)count=2+(Math.random()<.28?1:0);
    const drops=[];
    for(let i=0;i<count;i++){const id=rollItem(foe);rpg.inventory.push(id);drops.push(id)}
    showLoot(drops,foe);
    render();
    return drops;
  }

  function groupedInventory(){
    const rpg=ensureState(),map=new Map();
    for(const id of rpg?.inventory||[])map.set(id,(map.get(id)||0)+1);
    return [...map.entries()].sort((a,b)=>(RARITY_META[ITEM_DEFS[b[0]]?.rarity]?.rank??0)-(RARITY_META[ITEM_DEFS[a[0]]?.rarity]?.rank??0)||itemName(a[0]).localeCompare(itemName(b[0])));
  }

  function render(){
    installUi();
    const rpg=ensureState(),count=rpg?.inventory?.length||0,button=document.getElementById("rpgItemsBtn"),badge=document.getElementById("rpgItemCount"),summary=document.getElementById("rpgItemSummary");
    if(badge)badge.textContent=String(count);
    if(button)button.disabled=!activeFight()||count===0;
    if(summary)summary.textContent=count?`🎒 ${count} item${count===1?"":"s"} • 🛡 ${rpg.itemShields||0} shield charge${rpg.itemShields===1?"":"s"}`:"Defeat enemies to find consumable items. Elites and bosses improve loot quality.";
    if(!document.getElementById("itemOverlay")?.classList.contains("hidden"))renderPanel();
  }

  function renderPanel(){
    const grid=document.getElementById("rpgItemGrid"),hint=document.getElementById("itemPanelHint");
    if(!grid)return;
    const groups=groupedInventory();
    if(!groups.length){grid.innerHTML='<div class="muted">Your inventory is empty. Elite and boss victories have the best reward odds.</div>';return}
    grid.innerHTML="";
    if(hint)hint.textContent="Choose one consumable. Hint and category items apply to the enemy you are fighting now.";
    for(const[id,count]of groups){
      const item=ITEM_DEFS[id];if(!item)continue;
      const btn=document.createElement("button");btn.type="button";btn.className="rpg-item-card";
      btn.innerHTML=`<div class="rpg-item-top"><span class="rpg-item-name">${item.icon} ${escapeHtml(item.name)}</span><span class="rpg-item-count">×${count}</span></div><div class="rpg-item-rarity">${RARITY_META[item.rarity].label}</div><div class="rpg-item-desc">${escapeHtml(item.desc)}</div>`;
      btn.onclick=()=>useItem(id);
      grid.appendChild(btn);
    }
  }

  function removeOne(id){
    const rpg=ensureState(),index=rpg?.inventory?.indexOf(id)??-1;
    if(index<0)return false;
    rpg.inventory.splice(index,1);
    return true;
  }

  function heal(amount){
    const rpg=ensureState();if(!rpg)return null;
    if(rpg.health>=rpg.maxHealth)return 0;
    const before=rpg.health;
    rpg.health=Math.min(rpg.maxHealth,rpg.health+Math.max(1,Math.round(rpg.maxHealth*amount)));
    return rpg.health-before;
  }

  function hintCandidates(){
    const rpg=ensureState();if(!rpg?.enemy)return [];
    return getVerses().filter(v=>!rpg.used?.has(refKey(v))&&!!matchWeightedConcepts(v.text,rpg.enemy.weaknesses));
  }

  function pickHintVerse(){const candidates=hintCandidates();return candidates.length?randomChoice(candidates):null}
  function fragment(text,count=7){
    const words=String(text||"").replace(/[“”]/g,'"').split(/\s+/).filter(Boolean);
    if(words.length<=count)return words.join(" ");
    const start=Math.floor(Math.random()*(words.length-count+1));
    return words.slice(start,start+count).join(" ");
  }

  function bonusConcepts(count,multiplier){
    const rpg=ensureState();if(!rpg?.enemy)return [];
    const existing=new Set((rpg.enemy.weaknesses||[]).map(w=>w.concept));
    const candidates=conceptKeys.filter(key=>!existing.has(key)&&getVerses().some(v=>!!matchVerseConcept(v.text,key)));
    const selected=[];
    while(selected.length<count&&candidates.length){const index=Math.floor(Math.random()*candidates.length),key=candidates.splice(index,1)[0];selected.push(key);rpg.enemy.weaknesses.push({concept:key,multiplier})}
    if(selected.length)rpg.lastWeaknesses=rpg.enemy.weaknesses.map(w=>({...w}));
    return selected;
  }

  function useItem(id){
    if(!activeFight())return;
    const rpg=ensureState(),item=ITEM_DEFS[id];if(!item||!rpg.inventory.includes(id))return;
    let used=true,message="";
    if(item.kind==="heal"){
      const healed=heal(item.amount);
      if(!healed){used=false;message="You are already at full health."}else message=`${item.name} restores ${healed} health.`;
    }else if(item.kind==="shield"){
      rpg.itemShields=Math.min(3,(rpg.itemShields||0)+1);message=`Armor of Faith is ready. Your next failed verse will not cost health.`;
    }else if(item.kind==="refresh"){
      rpg.used.clear();message="Scroll of Recall refreshes every verse used during this run.";
    }else if(item.kind==="great-recall"){
      rpg.used.clear();const healed=heal(.35)||0;rpg.itemShields=Math.min(3,(rpg.itemShields||0)+1);message=`Great Scroll of Recall refreshes your verses, restores ${healed} health, and grants a shield charge.`;
    }else if(item.kind==="category"){
      const added=bonusConcepts(item.count||1,item.multiplier||1);
      if(!added.length){used=false;message="No additional Scripture categories are available for this fight."}else message=`New weakness${added.length>1?"es":""}: ${added.map(conceptLabel).join(" + ")}. You may now attack with verses from ${added.length>1?"those categories":"that category"}.`;
    }else{
      const verse=pickHintVerse();
      if(!verse){used=false;message="No unused matching verse is available for a hint."}
      else if(item.kind==="hint-words")message=`Word fragment — “${fragment(verse.text,7)}” — no book or reference revealed.`;
      else if(item.kind==="hint-book")message=`Book Lantern: one unused valid verse is in ${verse.book}.`;
      else if(item.kind==="hint-chapter")message=`Chapter Map: one unused valid verse is in ${verse.book} ${verse.chapter}.`;
      else if(item.kind==="hint-reference")message=`Reference Compass: ${displayRef(verse)} can damage this enemy.`;
      else{used=false;message="That item cannot be used right now."}
    }
    if(used)removeOne(id);
    showHint(message);
    render();
    if(typeof refresh==="function")refresh();
    renderPanel();
  }

  function consumeShield(){
    const rpg=ensureState();
    if(!rpg||!rpg.itemShields)return false;
    rpg.itemShields--;
    render();
    return true;
  }

  function dropSummary(drops){return drops.map(id=>`${ITEM_DEFS[id]?.icon||"🎁"} ${itemName(id)}`).join(" • ")}

  installUi();
  return {reset,onSpawn,awardLoot,consumeShield,render,openPanel,closePanel,dropSummary,defs:ITEM_DEFS};
}
