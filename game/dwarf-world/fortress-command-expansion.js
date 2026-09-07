/* Dwarf World — Fortress Command & Identity Expansion
 * Loaded by the DwarfWorld shell and injected into the existing core IIFE.
 * The core remains authoritative for world generation, mining, combat, loot,
 * perks and Firebase/local serialization; this layer extends those systems.
 */
(() => {
  "use strict";

  const INJECT_MARKER = "/*__DW_FORTRESS_COMMAND_EXPANSION__*/";

  function sourcePatch(coreSource){
    if(typeof coreSource!=="string" || !coreSource.includes("requestAnimationFrame(tick);")){
      throw new Error("Dwarf World core source is not compatible with the fortress expansion loader.");
    }
    if(coreSource.includes(INJECT_MARKER)) return coreSource;

    const expansion = String.raw`
${INJECT_MARKER}
  // ============================================================
  // Fortress Command & Identity Expansion
  // ============================================================
  const DWX_VERSION = 1;
  const DWX_BUILDINGS = {
    great_hall:{name:"Great Hall",icon:"🏛️",cost:420,depth:0,range:8,color:"#e5b95f",desc:"Population, recruitment and advanced civic command."},
    tavern:{name:"Tavern",icon:"🍺",cost:280,depth:0,range:8,color:"#d99a64",desc:"Recovery, expedition rest and visiting recruits."},
    treasury:{name:"Treasury",icon:"💰",cost:460,depth:120,range:6,color:"#f5cf62",desc:"Raises returned treasure value and stores wealth."},
    barracks:{name:"Barracks",icon:"⚔️",cost:520,depth:120,range:10,color:"#c77b65",desc:"Military staging with a modest local combat bonus."},
    workshop:{name:"Workshop",icon:"🛠️",cost:500,depth:120,range:7,color:"#9aa8b8",desc:"Improves forge efficiency and construction costs."},
    mushroom_farm:{name:"Mushroom Farm",icon:"🍄",cost:390,depth:260,range:9,color:"#b38ce8",desc:"Settlement support and Deepcap ration buffs."},
    infirmary:{name:"Infirmary",icon:"✚",cost:560,depth:260,range:11,color:"#7fe0ad",desc:"Powerful recovery and emergency veteran rescue."},
    survey_hall:{name:"Survey Hall",icon:"🧭",cost:680,depth:420,range:28,color:"#78dcff",desc:"Hints at rich deposits and danger beyond the frontier."},
    lift_shaft:{name:"Lift Shaft",icon:"↕️",cost:900,depth:420,range:3,color:"#a5b4c5",desc:"Paired rapid-travel infrastructure for deep expeditions."},
    shrine:{name:"Shrine of the Ancestors",icon:"🕯️",cost:820,depth:620,range:10,color:"#d7a7ff",desc:"Inspires veteran dwarves and rewards survival."},
    great_vault:{name:"Great Vault",icon:"🔐",cost:1050,depth:860,range:5,color:"#ffd66f",desc:"Protects elite gear and improves relic discovery."}
  };
  const DWX_RELICS = [
    {id:"worldsplitter",name:"WORLDSPLITTER",category:"pickaxe",flavor:"The mountain remembers the first wound.",stats:{miningPower:34,miningRate:1.48},effect:"Mining strikes can fracture adjacent rock."},
    {id:"heart_of_mountain",name:"HEART OF THE MOUNTAIN",category:"pickaxe",flavor:"It beats harder where the sun has never reached.",stats:{miningPower:27,miningRate:1.35},effect:"Mining power rises with depth."},
    {id:"fortunes_end",name:"FORTUNE'S END",category:"pickaxe",flavor:"Every stone owes the delver a secret.",stats:{miningPower:24,miningRate:1.30},effect:"Greatly improves deep treasure returns."},
    {id:"king_under_stone",name:"KING UNDER STONE",category:"weapon",weaponType:"axe",flavor:"The crown vanished. The axe remembered.",stats:{dmg:46,range:1.35,rate:1.08,ranged:false},effect:"Damage scales with the wielder's level."},
    {id:"spawnbreaker",name:"SPAWNBREAKER",category:"weapon",weaponType:"axe",flavor:"Nests learned to fear a name.",stats:{dmg:42,range:1.35,rate:1.12,ranged:false},effect:"Devastating against the Deepwild's nest defenders."},
    {id:"deepward_bow",name:"DEEPWARD BOW",category:"weapon",weaponType:"bow",flavor:"Its string hums loudest beyond the lamps.",stats:{dmg:30,range:12.5,rate:1.34,ranged:true},effect:"Stronger far from the fortress."},
    {id:"grudgeplate",name:"GRUDGEPLATE",category:"armor",armorTier:"titanium",flavor:"Every scar is entered into the ledger.",stats:{defense:.48,bonusHp:135},effect:"Major kills record permanent defensive Grudges."},
    {id:"last_kings_bulwark",name:"LAST KING'S BULWARK",category:"armor",armorTier:"titanium",flavor:"A kingdom may fall. Its last guard does not.",stats:{defense:.52,bonusHp:170},effect:"Once per long cooldown, lethal damage leaves the wearer at 1 HP and sends them home."},
    {id:"armor_deep_road",name:"ARMOR OF THE DEEP ROAD",category:"armor",armorTier:"steel",flavor:"Made for roads no map admits exist.",stats:{defense:.42,bonusHp:120},effect:"Movement increases far from the fortress."},
    {id:"lantern_first_delver",name:"LANTERN OF THE FIRST DELVER",category:"armor",armorTier:"steel",flavor:"The first light below still refuses to die.",stats:{defense:.32,bonusHp:90},effect:"Greatly expands exploration vision and pulses near valuable ore."},
    {id:"voidhook",name:"VOIDHOOK",category:"armor",armorTier:"titanium",flavor:"When the Deep closes its hand, the hook pulls back.",stats:{defense:.38,bonusHp:110},effect:"Critical health can pull the wearer to a nearby safe explored tile."},
    {id:"ancestors_token",name:"ANCESTOR'S TOKEN",category:"armor",armorTier:"steel",flavor:"A thousand names travel with the bearer.",stats:{defense:.28,bonusHp:75},effect:"Veterans gain additional experience."}
  ];

  function dwxFresh(){
    return {v:DWX_VERSION,buildings:[],protectedIds:[],relicsFound:[],presentedRelics:[],surveys:[],maxDepth:0,latestDiscovery:null,visitorReady:false,visitorAt:0,stats:{treasureBonus:0,relicDrops:0}};
  }
  let dwx=dwxFresh();
  let dwxBuildType=null;
  let dwxRosterNeed="all";
  let dwxInventoryMode="all";
  let dwxSelectedBuilding=null;
  let dwxLastSurvey=0;
  let dwxLastMinimap=0;
  let dwxLastCivicTick=0;
  let dwxMapOpen=false;
  const dwxProtected=()=>new Set(dwx.protectedIds||[]);
  const dwxBuilding=(type)=>dwx.buildings.filter(b=>b.type===type);
  const dwxCount=(type)=>dwxBuilding(type).length;
  const dwxNearest=(type,x,y)=>{
    let best=null,bd=Infinity;
    for(const b of dwxBuilding(type)){const q=dist2(x,y,b.x,b.y);if(q<bd){bd=q;best=b}}
    return best?{b:best,d:Math.sqrt(bd)}:null;
  };
  const dwxDeepest=()=>Math.max(dwx.maxDepth||0,...dwarves.map(d=>depthAt(Math.round(d.x),Math.round(d.y))),...dwx.buildings.map(b=>depthAt(b.x,b.y)));
  const dwxHasGreatHall=()=>dwxCount("great_hall")>0;
  const dwxPopulationCap=()=>12+dwxBuilding("great_hall").reduce((n,b)=>n+(b.level>=3?16:b.level===2?8:3),0);
  const dwxWorkshopDiscount=()=>Math.min(.10,dwxBuilding("workshop").reduce((n,b)=>n+(b.level>=3?.05:b.level===2?.03:.015),0));
  const dwxTreasuryBonus=()=>Math.min(.10,dwxBuilding("treasury").reduce((n,b)=>n+(b.level>=3?.10:b.level===2?.06:.03),0));
  const dwxVaultBonus=()=>Math.min(.018,dwxCount("great_vault")*.006);
  const dwxRarityLabel=(it)=>it?.relicId?"✦ RELIC":String(it?.rarity||"none").toUpperCase();

  function dwxInjectStyle(){
    const style=document.createElement("style");
    style.id="dwxStyles";
    style.textContent=`
      #dwarfList{gap:7px;border:0;background:transparent;overflow:visible}
      .dwx-card{position:relative!important;display:grid!important;grid-template-columns:42px 1fr!important;gap:8px!important;min-height:116px!important;padding:9px!important;border:1px solid rgba(255,255,255,.10)!important;border-radius:12px!important;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(0,0,0,.18))!important;overflow:hidden!important}
      .dwx-card.sel{border-color:rgba(120,220,255,.45)!important;box-shadow:0 0 0 1px rgba(120,220,255,.12) inset}
      .dwx-avatar{width:42px;height:48px;border-radius:10px;position:relative;background:linear-gradient(180deg,#80705e,#312b28);border:1px solid rgba(255,255,255,.16);overflow:hidden;box-shadow:inset 0 -10px 18px rgba(0,0,0,.25)}
      .dwx-avatar:before{content:"";position:absolute;width:22px;height:22px;border-radius:50%;background:#d5a879;left:9px;top:9px;box-shadow:0 -6px 0 2px #7a838d}
      .dwx-avatar:after{content:"";position:absolute;width:24px;height:18px;border-radius:2px 2px 12px 12px;background:var(--beard,#8d5c38);left:8px;top:25px;clip-path:polygon(0 0,100% 0,80% 100%,50% 76%,20% 100%)}
      .dwx-main{min-width:0;text-align:left}.dwx-name{font-size:12px;font-weight:1000;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.dwx-act{font-size:10px;color:rgba(231,238,247,.72);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.dwx-gear{display:grid;grid-template-columns:repeat(3,1fr);gap:4px;margin-top:6px}.dwx-slot{padding:4px;border:1px solid rgba(255,255,255,.08);border-radius:7px;background:rgba(0,0,0,.16);font-size:9px;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.dwx-slot.empty{border-color:rgba(255,110,120,.28);color:#ffabb5;background:rgba(255,70,90,.07)}.dwx-perks{display:flex;gap:3px;margin-top:5px;min-height:16px}.dwx-perk{width:16px;height:16px;border-radius:50%;display:grid;place-items:center;background:rgba(255,214,111,.10);border:1px solid rgba(255,214,111,.22);font-size:9px}.dwx-hp{position:absolute;left:0;right:0;bottom:0;height:4px;background:rgba(255,255,255,.06)}.dwx-hp>i{display:block;height:100%;background:linear-gradient(90deg,#ff6f79,#f1cc62,#7be0a4)}
      .dwx-needbar{display:grid;gap:7px;margin:9px 0}.dwx-needrow{display:flex;align-items:center;justify-content:space-between;gap:6px}.dwx-needbuttons{display:flex;gap:4px;flex-wrap:wrap}.dwx-chip{height:29px;padding:0 8px;border-radius:999px;font-size:10px}.dwx-chip.on{background:rgba(120,220,255,.15);border-color:rgba(120,220,255,.35)}
      #dwxAutoEquip{width:100%;height:36px;background:linear-gradient(180deg,rgba(215,171,84,.18),rgba(215,171,84,.08));border-color:rgba(215,171,84,.38)}
      .dwx-build-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px}.dwx-build{height:auto;min-height:54px;padding:6px;text-align:left}.dwx-build b{display:block;font-size:10px}.dwx-build span{display:block;font-size:9px;color:rgba(231,238,247,.58);margin-top:2px}.dwx-build.locked{opacity:.48}.dwx-build.on{outline:1px solid rgba(120,220,255,.65);background:rgba(120,220,255,.14)}
      #dwxMap{position:fixed;right:12px;top:80px;width:228px;z-index:45;border:1px solid rgba(255,255,255,.14);border-radius:14px;background:rgba(7,10,14,.90);box-shadow:0 16px 38px rgba(0,0,0,.4);overflow:hidden;backdrop-filter:blur(10px)}#dwxMap.closed .dwx-mapbody{display:none}.dwx-maphead{display:flex;justify-content:space-between;align-items:center;padding:7px 8px;border-bottom:1px solid rgba(255,255,255,.08);font-size:11px;font-weight:1000}.dwx-mapbody{padding:7px}.dwx-mapwrap{position:relative;height:188px;border-radius:9px;overflow:hidden;background:#030507;border:1px solid rgba(255,255,255,.08)}#dwxMini{width:100%;height:100%;display:block;image-rendering:pixelated;cursor:pointer}.dwx-mapnav{display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-top:6px}.dwx-mapnav button{height:28px;font-size:9px;padding:0 5px}
      #dwxPlacementCard{position:fixed;left:50%;bottom:20px;transform:translateX(-50%);z-index:58;min-width:260px;max-width:min(430px,92vw);padding:9px 11px;border-radius:12px;border:1px solid rgba(255,255,255,.15);background:rgba(8,11,15,.92);box-shadow:0 14px 34px rgba(0,0,0,.4);font-size:10px;pointer-events:none;display:none}.dwx-quality{font-weight:1000;letter-spacing:.08em}.dwx-quality.excellent{color:#7be0a4}.dwx-quality.good{color:#78dcff}.dwx-quality.poor{color:#f1cc62}.dwx-quality.invalid{color:#ff7d8e}
      #dwxRelicModal{position:fixed;inset:0;z-index:110;display:none;place-items:center;background:rgba(2,3,6,.78);backdrop-filter:blur(8px)}#dwxRelicModal.open{display:grid}.dwx-reliccard{width:min(520px,92vw);padding:22px;border-radius:18px;border:1px solid rgba(255,214,111,.48);background:radial-gradient(circle at 50% 0,rgba(255,214,111,.14),transparent 44%),linear-gradient(180deg,#171716,#08090b);box-shadow:0 0 60px rgba(255,214,111,.14),0 30px 80px #000}.dwx-relictitle{text-align:center;color:#ffd66f;font-size:11px;font-weight:1000;letter-spacing:.22em}.dwx-relicname{text-align:center;font-size:25px;font-weight:1000;margin:8px 0}.dwx-relicflavor{text-align:center;color:#b9a888;font-style:italic;font-size:12px}.dwx-relicstats{margin:14px 0;padding:10px;border-radius:10px;background:rgba(255,255,255,.04);font-size:11px;line-height:1.55}.dwx-relicactions{display:flex;gap:7px;justify-content:center;flex-wrap:wrap}.dwx-relicactions button{height:34px}
      .dwx-invfilters{display:flex;gap:4px;flex-wrap:wrap;margin-top:7px}.dwx-protect{height:26px!important;padding:0 6px!important;font-size:10px!important}.dwx-protect.on{color:#ffd66f;border-color:rgba(255,214,111,.4)}
      .dwx-buildingGlow{filter:drop-shadow(0 0 4px currentColor)}
      @media(max-width:820px),(pointer:coarse){#dwxMap{left:6px;right:6px;top:54px;width:auto;bottom:60px}#dwxMap.closed{left:auto;right:8px;top:54px;bottom:auto;width:116px}.dwx-mapwrap{height:min(62vh,520px)}.dwx-build-grid{grid-template-columns:1fr}.dwx-card{min-height:110px!important}.dwx-gear{grid-template-columns:repeat(3,minmax(0,1fr))}#dwxPlacementCard{bottom:64px}}
    `;
    document.head.appendChild(style);
  }

  function dwxActivity(d){
    const depth=depthAt(Math.round(d.x),Math.round(d.y));
    if(d.hp<=d.maxHp*.22) return `Fleeing / wounded — Depth ${depth}`;
    if(d.carrying) return `Returning loot — Depth ${depth}`;
    if(d.guardPostId) return `Guarding defensive zone — Depth ${depth}`;
    if(d.weapon && enemies.some(e=>dist2(d.x,d.y,e.x,e.y)<80)) return `Fighting in the Deepwild — Depth ${depth}`;
    if(d.pickaxe && d.path?.length) return `Mining frontier — Depth ${depth}`;
    if(d.path?.length) return `Traveling — Depth ${depth}`;
    if(depth<18) return "At the fortress";
    return `Exploring — Depth ${depth}`;
  }
  function dwxGearSlot(icon,it){
    if(!it) return `<span class="dwx-slot empty">${icon} EMPTY</span>`;
    const rr=it.relicId?"✦":String(it.rarity||"?").slice(0,1).toUpperCase();
    return `<span class="dwx-slot" title="${it.name||"Equipment"}">${icon} ${rr}</span>`;
  }
  const __dwxBaseDwarfList=updateDwarfListUI;
  updateDwarfListUI=function(){
    UI.dwarfList.innerHTML="";
    const needCount={armor:0,weapon:0,pickaxe:0};
    for(const d of dwarves){for(const k of Object.keys(needCount))if(!d[k])needCount[k]++}
    for(const d of dwarves){
      if(dwxRosterNeed!=="all" && d[dwxRosterNeed]) continue;
      const btn=document.createElement("button");
      btn.type="button";btn.dataset.dwarfId=d.id;
      btn.className="dwarfBtn dwx-card"+((selected.kind==="dwarf"&&selected.id===d.id)?" sel":"");
      const h=Array.from(String(d.id||d.name||"x")).reduce((a,c)=>a+c.charCodeAt(0),0);
      const beard=["#7b4a2b","#9b6a3a","#c7c7c7","#332d2a","#caa06a"][h%5];
      const perks=(d.perks||[]).slice(0,6).map(id=>`<span class="dwx-perk" title="${PERKS[id]?.name||id}">✦</span>`).join("");
      const hp=Math.max(0,Math.min(100,(d.hp/Math.max(1,d.maxHp))*100));
      btn.innerHTML=`<span class="dwx-avatar" style="--beard:${beard}"></span><span class="dwx-main"><span class="dwx-name">${d.name} · Lv. ${d.level}</span><span class="dwx-act">${dwxActivity(d)} · ❤️ ${Math.round(d.hp)}/${d.maxHp}</span><span class="dwx-gear">${dwxGearSlot("🛡",d.armor)}${dwxGearSlot("⚔",d.weapon)}${dwxGearSlot("⛏",d.pickaxe)}</span><span class="dwx-perks">${perks}</span></span><span class="dwx-hp"><i style="width:${hp}%"></i></span>`;
      UI.dwarfList.appendChild(btn);
    }
    const total=dwarves.length;
    UI.dwCount.textContent=dwxRosterNeed==="all"?String(total):`${UI.dwarfList.childElementCount}/${total}`;
    dwxUpdateNeedBar();
  };

  function dwxNeedCounts(){
    const n={armor:0,weapon:0,pickaxe:0};
    for(const d of dwarves)for(const k of Object.keys(n))if(!d[k])n[k]++;
    return n;
  }
  function dwxUpdateNeedBar(){
    const n=dwxNeedCounts(),root=document.getElementById("dwxNeedBar");if(!root)return;
    root.querySelector("[data-need=armor]").textContent=`🛡 ${n.armor}`;
    root.querySelector("[data-need=weapon]").textContent=`⚔ ${n.weapon}`;
    root.querySelector("[data-need=pickaxe]").textContent=`⛏ ${n.pickaxe}`;
    const label=root.querySelector(".dwx-needlabel");
    label.textContent=(n.armor+n.weapon+n.pickaxe)===0?"✓ All dwarves equipped":"Equipment Needed";
  }
  function dwxVisibleDwarfOrder(){
    const ids=[...UI.dwarfList.querySelectorAll("button[data-dwarf-id]")].map(b=>b.dataset.dwarfId);
    const mapped=ids.map(id=>dwarves.find(d=>String(d.id)===String(id))).filter(Boolean);
    return mapped.length?mapped:dwarves.slice();
  }
  function dwxAutoEquip(){
    const order=dwxVisibleDwarfOrder();
    const counts={armor:0,weapon:0,pickaxe:0};
    for(const cat of ["armor","weapon","pickaxe"]){
      const targets=order.filter(d=>!d[cat]);
      const items=loot.filter(it=>it.category===cat).sort((a,b)=>itemScore(b)-itemScore(a));
      for(let i=0;i<Math.min(targets.length,items.length);i++){
        const d=targets[i],it=items[i],ix=loot.findIndex(x=>x.id===it.id);
        if(ix<0||d[cat])continue;
        loot.splice(ix,1);d[cat]=it;if(it.relicId)dwxProtect(it.id,true);applyDwarfStats(d);counts[cat]++;
      }
    }
    loot.sort(sortLoot);markUI("all");
    const n=dwxNeedCounts(),empty=n.armor+n.weapon+n.pickaxe;
    toast(`Auto Equipped — ${counts.weapon} Weapons • ${counts.armor} Armor • ${counts.pickaxe} Pickaxes • ${empty} empty slots remain`);
  }

  function dwxProtect(id,on=true){
    const set=dwxProtected();
    if(on)set.add(id);else set.delete(id);
    dwx.protectedIds=[...set];markUI("inv");
  }
  function dwxIsProtected(it){return !!it&&(!!it.relicId||dwxProtected().has(it.id))}
  const __dwxSellItem=sellItemById;
  sellItemById=function(id){const it=loot.find(x=>x.id===id);if(dwxIsProtected(it)){toast("Protected equipment cannot be sold.");return}return __dwxSellItem(id)};
  function dwxRunSellSafely(fn){
    const held=loot.filter(dwxIsProtected);if(!held.length)return fn();
    for(const it of held){const i=loot.indexOf(it);if(i>=0)loot.splice(i,1)}
    try{return fn()}finally{loot.push(...held);loot.sort(sortLoot);markUI("inv")}
  }
  const __dwxSellAll=sellAll; sellAll=function(){return dwxRunSellSafely(__dwxSellAll)};
  const __dwxSellJunk=sellJunk; sellJunk=function(){return dwxRunSellSafely(__dwxSellJunk)};

  function dwxInventoryPass(it){
    if(dwxInventoryMode==="all")return true;
    if(dwxInventoryMode==="relics")return !!it.relicId;
    if(dwxInventoryMode==="legendary")return !!it.relicId||it.rarity==="legendary";
    if(dwxInventoryMode==="protected")return dwxIsProtected(it);
    if(dwxInventoryMode==="needed")return dwarves.some(d=>!d[it.category]);
    if(dwxInventoryMode==="upgrades"){
      const d=getSelectedDwarf();if(!d)return false;return itemScore(it)>itemScore(d[it.category]);
    }
    return true;
  }
  const __dwxRecomputeInv=recomputeInvFiltered;
  recomputeInvFiltered=function(){
    __dwxRecomputeInv();
    if(dwxInventoryMode!=="all"){
      invFiltered=invFiltered.filter(dwxInventoryPass);
      UI.invSpacer.style.height=(invFiltered.length*INV_ROW_H)+"px";
    }
  };
  const __dwxRenderInv=renderInv;
  renderInv=function(){
    __dwxRenderInv();
    for(const row of rowPool){
      const id=row._sell?.dataset.id;if(!id||row.style.display==="none")continue;
      let b=row.querySelector(".dwx-protect");
      if(!b){b=document.createElement("button");b.type="button";b.className="btn dwx-protect";b.addEventListener("click",e=>{e.stopPropagation();const item=loot.find(x=>x.id===b.dataset.id);if(!item)return;dwxProtect(item.id,!dwxIsProtected(item));renderInv()});row.querySelector(".actions")?.prepend(b)}
      const it=loot.find(x=>x.id===id);b.dataset.id=id;b.textContent=dwxIsProtected(it)?"🔒":"🔓";b.classList.toggle("on",dwxIsProtected(it));
      if(it?.relicId){row._rar.textContent="✦ RELIC";row._rar.className="rar legendary"}
    }
  };

  function dwxMaxDepthUpdate(){dwx.maxDepth=Math.max(dwx.maxDepth||0,...dwarves.map(d=>depthAt(Math.round(d.x),Math.round(d.y))))}
  function dwxCanBuildType(type){const def=DWX_BUILDINGS[type];return !!def&&dwxDeepest()>=def.depth&&(type==="great_hall"||type==="tavern"||dwxHasGreatHall())}
  function dwxBuildCost(type){const def=DWX_BUILDINGS[type];return Math.max(1,Math.round(def.cost*(1-dwxWorkshopDiscount())))}
  function dwxCanPlace(type,x,y){
    if(!dwxCanBuildType(type)||!inBounds(x,y)||!isWalkable(tType[idx(x,y)]))return false;
    if(!tSeen[idx(x,y)])return false;
    if(Math.hypot(x-fortress.x,y-fortress.y)<2)return false;
    if(forge&&forge.x===x&&forge.y===y)return false;
    if(depots.some(b=>b.x===x&&b.y===y)||towers.some(b=>b.x===x&&b.y===y)||dwx.buildings.some(b=>b.x===x&&b.y===y))return false;
    if(type==="lift_shaft"&&dwxCount("lift_shaft")%2===1){
      const mate=dwxBuilding("lift_shaft").find(b=>!b.pairId);if(mate&&Math.hypot(mate.x-x,mate.y-y)<40)return false;
    }
    return true;
  }
  function dwxPlacement(type,x,y){
    const def=DWX_BUILDINGS[type];if(!def)return{quality:"INVALID",reason:"Unknown structure",score:0};
    if(!dwxCanPlace(type,x,y))return{quality:"INVALID",reason:"Requires explored, clear, unlocked tunnel space",score:0};
    const nearbyD=dwarves.filter(d=>dist2(d.x,d.y,x,y)<=def.range*def.range).length;
    const nearbyE=enemies.filter(e=>dist2(e.x,e.y,x,y)<=Math.max(10,def.range)*Math.max(10,def.range)).length;
    const nearSp=spawners.filter(s=>dist2(s.x,s.y,x,y)<=Math.max(15,def.range+6)**2).length;
    let score=30+nearbyD*8;
    let reason=`${nearbyD} dwarves in influence`;
    if(type==="depot")score+=nearbyD*10;
    if(type==="barracks"||type==="infirmary"){score+=nearbyE*10+nearSp*14;reason+=` • ${nearbyE+nearSp} nearby threats`}
    if(type==="survey_hall"){score+=Math.min(35,depthAt(x,y)/12);reason=`Survey depth ${depthAt(x,y)}`}
    if(type==="lift_shaft"){
      const mate=dwxBuilding("lift_shaft").find(b=>!b.pairId);if(mate){const saving=Math.round(Math.hypot(mate.x-x,mate.y-y));score+=Math.min(50,saving/5);reason=`Estimated trip reduction: ${saving} tiles`}
    }
    const same=dwxBuilding(type).reduce((m,b)=>Math.min(m,Math.hypot(b.x-x,b.y-y)),Infinity);if(same<def.range*1.6){score-=25;reason+=" • overlaps existing coverage"}
    const quality=score>=72?"EXCELLENT":score>=48?"GOOD":"POOR";
    return{quality,reason,score,nearbyD,nearbyE};
  }
  function dwxPlace(type,x,y){
    const p=dwxPlacement(type,x,y),cost=dwxBuildCost(type);if(p.quality==="INVALID"){toast(p.reason);return false}if(gold<cost){toast("Not enough gold.");return false}
    gold-=cost;const def=DWX_BUILDINGS[type],b={id:uuid(),type,x,y,level:1,hp:300,maxHp:300,builtAt:Date.now(),stats:{serviced:0,goldReturned:0}};
    if(type==="lift_shaft"){
      const mate=dwxBuilding("lift_shaft").find(v=>!v.pairId);if(mate){b.pairId=mate.id;mate.pairId=b.id}
    }
    dwx.buildings.push(b);dwx.latestDiscovery={kind:"building",id:b.id,x,y};dwxBuildType=null;markUI("all");dwxRefreshBuildButtons();toast(`${def.name} built — ${p.quality}.`);return true;
  }

  function dwxDrawBuildings(){
    for(const b of dwx.buildings){
      const def=DWX_BUILDINGS[b.type];if(!def)continue;const p=worldToScreen(b.x,b.y),s=p.s;if(p.x<-s||p.y<-s||p.x>innerWidth+s||p.y>innerHeight+s)continue;
      ctx.save();ctx.globalAlpha=.96;ctx.shadowColor=def.color;ctx.shadowBlur=Math.max(2,s*.28);ctx.fillStyle="rgba(20,21,23,.96)";ctx.fillRect(p.x+s*.12,p.y+s*.18,s*.76,s*.64);ctx.shadowBlur=0;ctx.strokeStyle=def.color;ctx.lineWidth=Math.max(1,s*.05);ctx.strokeRect(p.x+s*.12,p.y+s*.18,s*.76,s*.64);ctx.font=`${Math.max(10,s*.52)}px system-ui`;ctx.textAlign="center";ctx.textBaseline="middle";ctx.fillText(def.icon,p.x+s*.5,p.y+s*.48);ctx.restore();
    }
    if(dwxBuildType){
      const x=Math.round(mouseWorld.x),y=Math.round(mouseWorld.y),def=DWX_BUILDINGS[dwxBuildType],place=dwxPlacement(dwxBuildType,x,y);if(inBounds(x,y)){
        const p=worldToScreen(x,y),s=p.s;ctx.save();ctx.globalAlpha=.7;ctx.fillStyle=place.quality==="INVALID"?"rgba(255,80,100,.4)":"rgba(120,220,255,.35)";ctx.fillRect(p.x,p.y,s,s);ctx.strokeStyle=def.color;ctx.lineWidth=2;ctx.beginPath();ctx.arc(p.x+s*.5,p.y+s*.5,def.range*s,0,Math.PI*2);ctx.stroke();ctx.restore();
      }
    }
  }
  const __dwxDraw=draw;
  draw=function(){__dwxDraw();dwxDrawBuildings()};

  function dwxSafeTileNear(x,y,r=12){
    let best=null,bd=Infinity;for(let yy=Math.max(0,y-r);yy<=Math.min(H-1,y+r);yy++)for(let xx=Math.max(0,x-r);xx<=Math.min(W-1,x+r);xx++){
      const i=idx(xx,yy);if(!tSeen[i]||!isWalkable(tType[i]))continue;const threat=enemies.some(e=>dist2(e.x,e.y,xx,yy)<36);if(threat)continue;const dd=dist2(x,y,xx,yy);if(dd<bd){bd=dd;best={x:xx,y:yy}}
    }return best;
  }
  const __dwxApplyStats=applyDwarfStats;
  applyDwarfStats=function(d){
    __dwxApplyStats(d);
    const dep=depthAt(Math.round(d.x),Math.round(d.y));
    if(d.pickaxe?.relicId==="heart_of_mountain")d.miningPower=Math.round(d.miningPower*(1+Math.min(.75,dep/1000)));
    if(d.weapon?.relicId==="king_under_stone")d.dmg=Math.round(d.dmg*(1+Math.min(.8,(d.level||1)*.035)));
    if(d.weapon?.relicId==="deepward_bow")d.dmg=Math.round(d.dmg*(1+Math.min(.55,dep/900)));
    if(d.armor?.relicId==="grudgeplate")d.defense=Math.min(.78,d.defense+(d.dwxGrudges||0)*.006);
    if(d.armor?.relicId==="armor_deep_road")d.speed*=1+Math.min(.28,dep/1500);
    if(d.dwxInspiredUntil>nowSec()){d.dmg=Math.round(d.dmg*1.10);d.miningPower=Math.round(d.miningPower*1.10);d.speed*=1.08}
    if(d.dwxRationUntil>nowSec()){d.miningRate*=1.12;d.speed*=1.08}
  };
  const __dwxUpdateDwarf=updateDwarf;
  updateDwarf=function(d,dt){
    const xp0=d.xp||0,gold0=gold;
    applyDwarfStats(d);
    const bar=dwxNearest("barracks",d.x,d.y);if(bar&&bar.d<=DWX_BUILDINGS.barracks.range){d.dmg=Math.round(d.dmg*1.08);d.defense=Math.min(.8,d.defense+.05)}
    const inf=dwxNearest("infirmary",d.x,d.y);if(inf&&inf.d<=DWX_BUILDINGS.infirmary.range&&d.hp<d.maxHp)d.hp=Math.min(d.maxHp,d.hp+dt*9);
    const tav=dwxNearest("tavern",d.x,d.y);if(tav&&tav.d<=DWX_BUILDINGS.tavern.range&&d.hp<d.maxHp)d.hp=Math.min(d.maxHp,d.hp+dt*3.5);
    const shr=dwxNearest("shrine",d.x,d.y);if(shr&&shr.d<=DWX_BUILDINGS.shrine.range&&(d.level||1)>=6&&Math.random()<dt*.05)d.dwxInspiredUntil=nowSec()+40;
    if(d.armor?.relicId==="voidhook"&&d.hp<d.maxHp*.16&&(d.dwxVoidhookCd||0)<nowSec()){
      const safe=dwxSafeTileNear(fortress.x,fortress.y,18);if(safe){d.x=safe.x;d.y=safe.y;d.path=null;d.dwxVoidhookCd=nowSec()+90;fxBurst(d.x,d.y,"rgba(190,130,255,.95)",1.8)}
    }
    const lifts=dwxBuilding("lift_shaft").filter(b=>b.pairId);if(d.path?.length>70&&lifts.length){
      const goal=d.path[d.path.length-1],entry=lifts.find(b=>dist2(b.x,b.y,d.x,d.y)<5);if(entry&&(d.dwxLiftCd||0)<nowSec()){
        const exit=dwx.buildings.find(b=>b.id===entry.pairId);if(exit&&Math.hypot(exit.x-goal.x,exit.y-goal.y)+20<Math.hypot(d.x-goal.x,d.y-goal.y)){d.x=exit.x;d.y=exit.y;d.path=bfsPath(Math.round(d.x),Math.round(d.y),(x,y)=>x===goal.x&&y===goal.y,90000)||d.path;d.dwxLiftCd=nowSec()+5;fxText(d.x,d.y,"LIFT","rgba(180,220,255,.95)")}
      }
    }
    __dwxUpdateDwarf(d,dt);
    if(gold>gold0){const bonus=Math.round((gold-gold0)*dwxTreasuryBonus());if(bonus>0){gold+=bonus;dwx.stats.goldReturned=(dwx.stats.goldReturned||0)+bonus}}
    const gained=(d.xp||0)-xp0;if(gained>0){let bonus=0;if(d.armor?.relicId==="ancestors_token")bonus+=Math.ceil(gained*.15);if((d.level||1)>=6&&dwxCount("shrine"))bonus+=Math.ceil(gained*.04);d.xp+=bonus}
    applyDwarfStats(d);
  };
  const __dwxVisibility=refreshVisibility;
  refreshVisibility=function(dt=0){__dwxVisibility(dt);for(const d of dwarves)if(d.armor?.relicId==="lantern_first_delver")stampVision(Math.round(d.x),Math.round(d.y),DW_VIS_R+8)};

  const __dwxRemoveDead=removeDead;
  removeDead=function(){
    for(const d of dwarves){
      if(d.hp>0)continue;
      if(d.armor?.relicId==="last_kings_bulwark"&&(d.dwxBulwarkCd||0)<nowSec()){
        d.hp=1;d.dwxBulwarkCd=nowSec()+150;const safe=dwxSafeTileNear(fortress.x,fortress.y,14);if(safe){d.x=safe.x;d.y=safe.y;d.path=null}fxText(d.x,d.y,"LAST STAND","rgba(255,214,111,.98)",1.3);continue;
      }
      const inf=dwxNearest("infirmary",d.x,d.y);if(inf&&(inf.b.dwxRescueCd||0)<nowSec()){
        d.hp=1;inf.b.dwxRescueCd=nowSec()+90;d.x=inf.b.x;d.y=inf.b.y;d.path=null;fxText(d.x,d.y,"RESCUED","rgba(120,255,170,.98)",1.2)
      }
    }
    return __dwxRemoveDead();
  };
  const __dwxEnemyDeath=handleEnemyDeath;
  handleEnemyDeath=function(e){
    const killer=e?.lastHit?.kind==="dwarf"?dwarves.find(d=>d.id===e.lastHit.id):null;
    if(killer?.armor?.relicId==="grudgeplate"&&(e.maxHp||0)>110)killer.dwxGrudges=Math.min(30,(killer.dwxGrudges||0)+1);
    __dwxEnemyDeath(e);
    const depth=e?.sourceDepth||0;if(depth<220)return;
    const chance=Math.min(.012,.0012+depth/180000+dwxVaultBonus());if(Math.random()>chance)return;
    const available=DWX_RELICS.filter(r=>!dwx.relicsFound.includes(r.id));if(!available.length)return;
    const def=pickRand(available),item=dwxMakeRelic(def);dwx.relicsFound.push(def.id);dwx.protectedIds.push(item.id);dwx.stats.relicDrops=(dwx.stats.relicDrops||0)+1;
    groundLoot.push({id:uuid(),x:Math.round(e.x),y:Math.round(e.y),payload:{kind:"item",item}});dwx.latestDiscovery={kind:"relic",id:def.id,x:Math.round(e.x),y:Math.round(e.y)};fxBurst(e.x,e.y,"rgba(255,214,111,.95)",2.2);
  };
  function dwxMakeRelic(def){
    const base={id:uuid(),name:def.name,category:def.category,rarity:"legendary",relicId:def.id,relic:true,flavor:def.flavor,relicEffect:def.effect,effectName:"Relic",effectDesc:def.effect,sell:2500,identityDone:true,protected:true};
    if(def.category==="weapon")Object.assign(base,{weaponType:def.weaponType||"axe",...def.stats});
    if(def.category==="pickaxe")Object.assign(base,def.stats);
    if(def.category==="armor")Object.assign(base,{armorTier:def.armorTier||"titanium",...def.stats});
    return base;
  }
  function dwxCheckRelicPresentation(){
    for(const it of loot){if(!it.relicId||dwx.presentedRelics.includes(it.relicId))continue;dwx.presentedRelics.push(it.relicId);dwxShowRelic(it);break}
  }

  function dwxCivicTick(dt){
    dwxMaxDepthUpdate();
    const now=nowSec();
    if(now-dwxLastCivicTick>.8){
      dwxLastCivicTick=now;
      for(const farm of dwxBuilding("mushroom_farm"))for(const d of dwarves)if(dist2(d.x,d.y,farm.x,farm.y)<=DWX_BUILDINGS.mushroom_farm.range**2&&Math.random()<.12)d.dwxRationUntil=now+28;
      if(dwxCount("tavern")&&!dwx.visitorReady&&now>(dwx.visitorAt||0)+120){dwx.visitorReady=true;dwx.visitorAt=now;toast("🍺 A visiting dwarf has arrived at the Tavern. Your next recruit is discounted.")}
      dwxRunSurvey(now);dwxCheckRelicPresentation();dwxRefreshBuildButtons();
    }
  }
  const __dwxCombat=updateSpawnersAndCombat;
  updateSpawnersAndCombat=function(dt){__dwxCombat(dt);dwxCivicTick(dt)};

  const __dwxRecruitCost=recruitCost;
  recruitCost=function(){let c=__dwxRecruitCost();const halls=dwxBuilding("great_hall");if(halls.length)c*=1-Math.min(.10,halls.reduce((n,b)=>n+(b.level>=3?.10:b.level===2?.05:0),0));if(dwx.visitorReady)c*=.82;return Math.max(1,Math.round(c))};
  UI.btnRecruit.addEventListener("click",e=>{
    if(dwarves.length>=dwxPopulationCap()){e.stopImmediatePropagation();toast(`Great Hall capacity reached (${dwxPopulationCap()}). Upgrade or build another Great Hall.`);return}
    if(dwx.visitorReady)setTimeout(()=>{dwx.visitorReady=false},0);
  },true);
  const __dwxForgeCraftCost=forgeCraftCost;forgeCraftCost=function(level){return Math.max(1,Math.round(__dwxForgeCraftCost(level)*(1-dwxWorkshopDiscount())))};
  const __dwxForgeUpgradeCost=forgeUpgradeCost;forgeUpgradeCost=function(level){return Math.max(1,Math.round(__dwxForgeUpgradeCost(level)*(1-dwxWorkshopDiscount())))};

  function dwxRunSurvey(now){
    if(now-dwxLastSurvey<24||!dwxCount("survey_hall"))return;dwxLastSurvey=now;
    for(const hall of dwxBuilding("survey_hall")){
      const r=72+hall.level*30;let best=null,bv=-1;
      for(let n=0;n<260;n++){
        const a=Math.random()*Math.PI*2,rr=24+Math.random()*r,x=Math.round(hall.x+Math.cos(a)*rr),y=Math.round(hall.y+Math.sin(a)*rr);if(!inBounds(x,y)||tSeen[idx(x,y)])continue;
        const tt=tType[idx(x,y)];let value=0;if(tt===TILE_DIAMOND)value=5;else if(tt===TILE_EMERALD)value=4;else if(tt===TILE_SAPPHIRE)value=3;else if(tt===TILE_GOLD)value=2;else if(tt===TILE_SPAWNER_SEALED)value=2.5;if(value>bv){bv=value;best={x,y,kind:tt===TILE_SPAWNER_SEALED?"danger":"resource",value}}
      }
      if(best){best.expires=now+75;best.radius=Math.max(14,28-hall.level*4);dwx.surveys.push(best);dwx.surveys=dwx.surveys.filter(s=>s.expires>now).slice(-8);dwx.latestDiscovery={kind:"survey",x:best.x,y:best.y};toast(best.kind==="danger"?"Surveyors report suspicious Deepwild activity beyond the frontier.":"Surveyors report a valuable formation beyond the explored frontier.")}
    }
  }

  function dwxInstallNeedUI(){
    const card=UI.dwarfList.closest(".card");if(!card||document.getElementById("dwxNeedBar"))return;
    const root=document.createElement("div");root.id="dwxNeedBar";root.className="dwx-needbar";root.innerHTML=`<div class="dwx-needrow"><b class="dwx-needlabel">Equipment Needed</b><span class="tiny">tap to filter</span></div><div class="dwx-needbuttons"><button class="dwx-chip on" data-need="all">All</button><button class="dwx-chip" data-need="armor">🛡 0</button><button class="dwx-chip" data-need="weapon">⚔ 0</button><button class="dwx-chip" data-need="pickaxe">⛏ 0</button></div><button id="dwxAutoEquip" type="button">⭐ Auto Equip Empty Slots</button>`;
    card.insertBefore(root,UI.dwarfList);root.addEventListener("click",e=>{const b=e.target.closest("[data-need]");if(!b)return;dwxRosterNeed=b.dataset.need;root.querySelectorAll("[data-need]").forEach(x=>x.classList.toggle("on",x===b));markUI("dwarves")});root.querySelector("#dwxAutoEquip").addEventListener("click",dwxAutoEquip);dwxUpdateNeedBar();
  }
  function dwxInstallInventoryUI(){
    if(document.getElementById("dwxInvFilters"))return;const filters=UI.invSearch.closest(".filters");if(!filters)return;const row=document.createElement("div");row.id="dwxInvFilters";row.className="dwx-invfilters";row.innerHTML=`<button class="dwx-chip on" data-im="all">All</button><button class="dwx-chip" data-im="needed">Equipment Needed</button><button class="dwx-chip" data-im="upgrades">Upgrades</button><button class="dwx-chip" data-im="relics">Relics</button><button class="dwx-chip" data-im="legendary">Legendary+</button><button class="dwx-chip" data-im="protected">Protected</button>`;filters.after(row);row.addEventListener("click",e=>{const b=e.target.closest("[data-im]");if(!b)return;dwxInventoryMode=b.dataset.im;row.querySelectorAll("[data-im]").forEach(x=>x.classList.toggle("on",x===b));markUI("inv")});
  }
  function dwxInstallBuildUI(){
    const section=UI.tabBuild;if(!section||document.getElementById("dwxBuildGrid"))return;const card=document.createElement("div");card.className="card";card.innerHTML=`<div class="row" style="justify-content:space-between"><b>Fortress Civic Works</b><span class="pill" id="dwxDepthUnlock">Depth 0</span></div><div class="tiny" style="margin:6px 0 8px">Strategic settlement structures. Great Hall unlocks advanced civic construction.</div><div id="dwxBuildGrid" class="dwx-build-grid"></div>`;section.appendChild(card);dwxRefreshBuildButtons();
  }
  function dwxRefreshBuildButtons(){
    const grid=document.getElementById("dwxBuildGrid");if(!grid)return;document.getElementById("dwxDepthUnlock").textContent=`Depth ${Math.round(dwxDeepest())}`;
    const old=dwxBuildType;grid.innerHTML="";for(const [type,def] of Object.entries(DWX_BUILDINGS)){
      const unlocked=dwxCanBuildType(type),b=document.createElement("button");b.type="button";b.className="dwx-build"+(unlocked?"":" locked")+(old===type?" on":"");b.dataset.type=type;b.disabled=!unlocked;b.innerHTML=`<b>${def.icon} ${def.name} — ${dwxBuildCost(type)}g</b><span>${unlocked?def.desc:`Unlock: ${def.depth?`Depth ${def.depth}`:"Great Hall"}`}</span>`;grid.appendChild(b)
    }
    grid.onclick=e=>{const b=e.target.closest("[data-type]");if(!b||b.disabled)return;dwxBuildType=dwxBuildType===b.dataset.type?null:b.dataset.type;setBuildMode(BUILD_NONE);dwxRefreshBuildButtons();toast(dwxBuildType?`Place ${DWX_BUILDINGS[dwxBuildType].name}.`:"Civic build mode off.")};
  }

  function dwxInstallMap(){
    if(document.getElementById("dwxMap"))return;const box=document.createElement("aside");box.id="dwxMap";box.className="closed";box.innerHTML=`<div class="dwx-maphead"><span>🗺 Underground Map</span><button id="dwxMapToggle" class="btn icon" type="button">▾</button></div><div class="dwx-mapbody"><div class="dwx-mapwrap"><canvas id="dwxMini" width="210" height="188"></canvas></div><div class="dwx-mapnav"><button data-nav="fortress">Fortress</button><button data-nav="selected">Selected Dwarf</button><button data-nav="deepest">Deepest Explored</button><button data-nav="latest">Latest Discovery</button></div></div>`;document.body.appendChild(box);
    const btn=document.createElement("button");btn.id="dwxHudMap";btn.className="btn";btn.textContent="🗺 Map";UI.btnFocus.parentElement.insertBefore(btn,UI.btnFocus);btn.addEventListener("click",()=>dwxToggleMap(true));box.querySelector("#dwxMapToggle").addEventListener("click",()=>dwxToggleMap());
    const mb=document.createElement("button");mb.className="mbtn";mb.type="button";mb.innerHTML="<b>🗺</b><span>Map</span>";UI.mobileBar?.insertBefore(mb,UI.mFocus);mb.addEventListener("click",()=>dwxToggleMap(true));
    box.querySelector(".dwx-mapnav").addEventListener("click",e=>{const b=e.target.closest("[data-nav]");if(!b)return;dwxMapNavigate(b.dataset.nav)});box.querySelector("#dwxMini").addEventListener("click",e=>{const r=e.currentTarget.getBoundingClientRect();cam.x=clamp((e.clientX-r.left)/r.width*W,0,W-1);cam.y=clamp((e.clientY-r.top)/r.height*H,0,H-1)});
  }
  function dwxToggleMap(force){const box=document.getElementById("dwxMap");if(!box)return;dwxMapOpen=force===true?true:!dwxMapOpen;box.classList.toggle("closed",!dwxMapOpen);if(dwxMapOpen)dwxDrawMinimap(true)}
  function dwxMapNavigate(kind){
    if(kind==="fortress"){cam.x=fortress.x;cam.y=fortress.y}
    if(kind==="selected"){const d=getSelectedDwarf();if(d){cam.x=d.x;cam.y=d.y}}
    if(kind==="deepest"){const d=dwarves.slice().sort((a,b)=>depthAt(b.x,b.y)-depthAt(a.x,a.y))[0];if(d){cam.x=d.x;cam.y=d.y}}
    if(kind==="latest"&&dwx.latestDiscovery){cam.x=dwx.latestDiscovery.x;cam.y=dwx.latestDiscovery.y}
    if(isMobileLayout())dwxToggleMap(false);
  }
  function dwxDrawMinimap(force=false){
    const c=document.getElementById("dwxMini");if(!c||(!dwxMapOpen&&!force))return;const now=performance.now();if(!force&&now-dwxLastMinimap<800)return;dwxLastMinimap=now;const g=c.getContext("2d"),w=c.width,h=c.height;g.fillStyle="#030507";g.fillRect(0,0,w,h);const step=isMobileLayout()?10:8;
    for(let y=0;y<H;y+=step)for(let x=0;x<W;x+=step){let seen=false,empty=false,ore=0;for(let yy=y;yy<Math.min(H,y+step)&&!seen;yy+=2)for(let xx=x;xx<Math.min(W,x+step);xx+=2){const i=idx(xx,yy);if(tSeen[i]){seen=true;empty=empty||isWalkable(tType[i]);if(tType[i]>=TILE_GOLD&&tType[i]<=TILE_DIAMOND)ore=Math.max(ore,tType[i])}}if(!seen)continue;const px=x/W*w,py=y/H*h,pw=Math.max(1,step/W*w),ph=Math.max(1,step/H*h);g.fillStyle=empty?"#24313a":"#11171d";g.fillRect(px,py,pw,ph);if(ore){g.fillStyle=ore===TILE_DIAMOND?"#d8f2ff":ore===TILE_EMERALD?"#7be0a4":ore===TILE_SAPPHIRE?"#78dcff":"#d7ab54";g.fillRect(px,py,2,2)}}
    const dot=(x,y,col,r=2)=>{g.fillStyle=col;g.beginPath();g.arc(x/W*w,y/H*h,r,0,Math.PI*2);g.fill()};dot(fortress.x,fortress.y,"#ffd66f",3);for(const b of dwx.buildings)dot(b.x,b.y,DWX_BUILDINGS[b.type]?.color||"#fff",2.4);for(const d of depots)dot(d.x,d.y,"#a5b4c5",2);for(const t of towers)dot(t.x,t.y,"#ffb36b",2);for(const d of dwarves)dot(d.x,d.y,(selected.kind==="dwarf"&&selected.id===d.id)?"#ffffff":"#7be0a4",selected.id===d.id?3:1.5);for(const s of spawners)if(tSeen[idx(Math.round(s.x),Math.round(s.y))])dot(s.x,s.y,"#d66cff",2.5);for(const e of enemies)if((e.maxHp||0)>120&&inBounds(Math.round(e.x),Math.round(e.y))&&tSeen[idx(Math.round(e.x),Math.round(e.y))])dot(e.x,e.y,"#ff6f79",2);for(const s of dwx.surveys.filter(s=>s.expires>nowSec())){g.strokeStyle=s.kind==="danger"?"#ff6f79":"#78dcff";g.globalAlpha=.55;g.beginPath();g.arc(s.x/W*w,s.y/H*h,Math.max(4,s.radius/W*w),0,Math.PI*2);g.stroke();g.globalAlpha=1}
    const vx=cam.x/W*w,vy=cam.y/H*h;g.strokeStyle="#fff";g.strokeRect(vx-5,vy-4,10,8);
  }

  function dwxInstallPlacementCard(){const d=document.createElement("div");d.id="dwxPlacementCard";document.body.appendChild(d);window.addEventListener("mousemove",dwxUpdatePlacementCard);canvas.addEventListener("touchmove",dwxUpdatePlacementCard,{passive:true})}
  function dwxUpdatePlacementCard(){const d=document.getElementById("dwxPlacementCard");if(!d)return;if(!dwxBuildType){d.style.display="none";return}const x=Math.round(mouseWorld.x),y=Math.round(mouseWorld.y),def=DWX_BUILDINGS[dwxBuildType],p=dwxPlacement(dwxBuildType,x,y);d.style.display="block";d.innerHTML=`<b>${def.icon} ${def.name}</b> · Cost: ${dwxBuildCost(dwxBuildType)}g<br><span>${def.desc}</span><br>Coverage radius: ${def.range} tiles · Placement: <span class="dwx-quality ${p.quality.toLowerCase()}">${p.quality}</span><br><span class="tiny">${p.reason}</span>`}

  canvas.addEventListener("mousedown",e=>{
    if(e.button!==0)return;const rect=canvas.getBoundingClientRect(),w=screenToWorld(e.clientX-rect.left,e.clientY-rect.top),x=Math.round(w.x),y=Math.round(w.y);
    if(dwxBuildType){e.preventDefault();e.stopImmediatePropagation();dwxPlace(dwxBuildType,x,y);return}
    const b=dwx.buildings.find(v=>Math.hypot(v.x-w.x,v.y-w.y)<1.0);if(b){e.preventDefault();e.stopImmediatePropagation();dwxSelectedBuilding=b;cam.x=b.x;cam.y=b.y;dwxShowBuildingInfo(b)}
  },true);

  function dwxShowBuildingInfo(b){const def=DWX_BUILDINGS[b.type];const card=document.getElementById("dwxPlacementCard");if(!card||!def)return;card.style.display="block";card.innerHTML=`<b>${def.icon} ${def.name} · Lv ${b.level}</b><br>HP ${Math.round(b.hp||b.maxHp)}/${b.maxHp}<br>${def.desc}<br><span class="tiny">Depth ${depthAt(b.x,b.y)} • ${(b.stats?.serviced||0)} dwarves serviced • ${(b.stats?.goldReturned||0)} bonus gold returned</span>`;clearTimeout(card._hide);card._hide=setTimeout(()=>{if(!dwxBuildType)card.style.display="none"},4200)}

  function dwxInstallRelicModal(){const m=document.createElement("div");m.id="dwxRelicModal";m.innerHTML=`<div class="dwx-reliccard"><div class="dwx-relictitle">✦ RELIC DISCOVERED ✦</div><div class="dwx-relicname"></div><div class="dwx-relicflavor"></div><div class="dwx-relicstats"></div><div class="dwx-relicactions"><button data-ra="equip" class="btn primary">Equip</button><button data-ra="vault" class="btn">Send to Vault</button><button data-ra="continue" class="btn">Continue</button></div></div>`;document.body.appendChild(m);m.addEventListener("click",e=>{const b=e.target.closest("[data-ra]");if(!b)return;const it=loot.find(x=>x.id===m.dataset.itemId);if(b.dataset.ra==="equip"&&it){const d=getSelectedDwarf();if(!d){toast("Select a dwarf first.");return}if(d[it.category]){toast("That slot is occupied. Relic left protected in inventory.");return}const i=loot.indexOf(it);if(i>=0)loot.splice(i,1);d[it.category]=it;applyDwarfStats(d);markUI("all")}if(b.dataset.ra==="vault"&&it)dwxProtect(it.id,true);m.classList.remove("open")})}
  function dwxShowRelic(it){const m=document.getElementById("dwxRelicModal");if(!m)return;m.dataset.itemId=it.id;m.querySelector(".dwx-relicname").textContent=it.name;m.querySelector(".dwx-relicflavor").textContent=`“${it.flavor||"An old power returns to the Deep."}”`;let stats=it.category==="weapon"?`Ancient Weapon • Damage ${it.dmg} • ${it.ranged?`Range ${it.range}`:"Melee"}`:it.category==="pickaxe"?`Ancient Pickaxe • Mining ${it.miningPower} • Speed x${Number(it.miningRate||1).toFixed(2)}`:`Ancient Armor • Defense ${Math.round((it.defense||0)*100)}% • +${it.bonusHp||0} HP`;m.querySelector(".dwx-relicstats").innerHTML=`${stats}<br><b>Relic Effect:</b> ${it.relicEffect||it.effectDesc||"Unknown"}`;m.classList.add("open")}

  // Save the expansion inside the authoritative compressed world payload.
  const __dwxSaveAsync=saveGameAsync;
  saveGameAsync=async function(){
    const realToast=toast;let pendingToast="Saved locally.";toast=function(msg){if(/^Saved locally/i.test(String(msg)))pendingToast=msg;else realToast(msg)};
    try{
      await __dwxSaveAsync();const raw=localStorage.getItem(SAVE_KEY);if(raw){let jsonStr=raw;if(raw.startsWith(SAVE_MAGIC))jsonStr=await gunzipB64ToStr(raw.slice(SAVE_MAGIC.length));const save=JSON.parse(jsonStr);save.fortressExpansion=dwx;const packed=SAVE_MAGIC+await gzipStrToB64(JSON.stringify(save));localStorage.setItem(SAVE_KEY,packed)}
      realToast(pendingToast);
    }catch(err){realToast("Save failed (fortress expansion).");throw err}finally{toast=realToast}
  };
  const __dwxLoadAsync=loadGameAsync;
  loadGameAsync=async function(){
    const realToast=toast;let finalToast="Loaded local save.";toast=function(msg){if(/Loaded local save/i.test(String(msg)))finalToast=msg;else realToast(msg)};
    try{
      const raw=localStorage.getItem(SAVE_KEY);let ext=null;if(raw){let jsonStr=raw;if(raw.startsWith(SAVE_MAGIC))jsonStr=await gunzipB64ToStr(raw.slice(SAVE_MAGIC.length));ext=JSON.parse(jsonStr)?.fortressExpansion||null}
      await __dwxLoadAsync();dwx=Object.assign(dwxFresh(),ext||{});dwx.buildings=Array.isArray(dwx.buildings)?dwx.buildings:[];dwx.protectedIds=Array.isArray(dwx.protectedIds)?dwx.protectedIds:[];dwx.relicsFound=Array.isArray(dwx.relicsFound)?dwx.relicsFound:[];for(const it of loot)if(it.relicId)dwxProtect(it.id,true);dwxRefreshBuildButtons();markUI("all");realToast(finalToast);
    }catch(err){realToast("Load failed (fortress expansion).");throw err}finally{toast=realToast}
  };

  dwxInjectStyle();dwxInstallNeedUI();dwxInstallInventoryUI();dwxInstallBuildUI();dwxInstallMap();dwxInstallPlacementCard();dwxInstallRelicModal();
  const __dwxFlush=flushUI;flushUI=function(nowMs){__dwxFlush(nowMs);dwxDrawMinimap(false)};
  markUI("all");
  toast("Fortress Command expansion loaded — roster, minimap, civic buildings, safe auto-equip and relics online.");
`;

    const needle = "\n})();\n</script>";
    if(!coreSource.includes(needle)) throw new Error("Could not find the Dwarf World core injection point.");
    return coreSource.replace(needle, `\n${expansion}\n})();\n</script>`);
  }

  window.DwarfWorldFortressExpansion={prepareCore:sourcePatch};
})();
