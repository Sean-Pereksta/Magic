const RARITY_META={common:{label:"Common",rank:0},uncommon:{label:"Uncommon",rank:1},rare:{label:"Rare",rank:2},epic:{label:"Epic",rank:3}};

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

const JW=(concept,multiplier=1)=>({concept,multiplier});
const JOURNEY_ENEMIES=[
  {id:"rebel-warden",name:"Rebel Warden",art:"rebel-warden",minFloor:4,maxFloor:12,hp:48,damage:12,xp:19,desc:"He turns every command into a challenge and every instruction into rebellion.",weaknesses:[JW("obedience",1.5),JW("law",1.0),JW("word",.82)]},
  {id:"unrepentant-shade",name:"Unrepentant Shade",art:"unrepentant-shade",minFloor:5,maxFloor:15,hp:52,damage:14,xp:21,desc:"A shadow that refuses to turn back even when mercy is offered.",weaknesses:[JW("repentance",1.55),JW("forgiveness",1.05),JW("grace",.82)]},
  {id:"fear-monger",name:"Fear Monger",art:"fear-monger",minFloor:6,maxFloor:17,hp:58,damage:15,xp:23,desc:"It magnifies every danger until courage feels impossible.",weaknesses:[JW("courage",1.55),JW("faith",1.0),JW("hope",.82)]},
  {id:"crooked-magistrate",name:"Crooked Magistrate",art:"crooked-magistrate",minFloor:8,maxFloor:20,hp:66,damage:18,xp:27,desc:"A corrupt judge who bends every scale toward the powerful.",weaknesses:[JW("justice",1.55),JW("righteousness",1.12),JW("mercy",.78)]},
  {id:"furnace-of-haste",name:"Furnace of Haste",art:"furnace-of-haste",minFloor:9,maxFloor:22,hp:71,damage:19,xp:29,desc:"It punishes waiting and tries to make every faithful step feel too slow.",weaknesses:[JW("patience",1.55),JW("hope",1.02),JW("faith",.78)]},
  {id:"thankless-devourer",name:"Thankless Devourer",art:"thankless-devourer",minFloor:10,maxFloor:24,hp:76,damage:20,xp:31,desc:"It consumes every gift and immediately demands another.",weaknesses:[JW("thanksgiving",1.55),JW("praise",1.08),JW("joy",.82)]},
  {id:"housebreaker",name:"Housebreaker",art:"housebreaker",minFloor:12,maxFloor:26,hp:84,damage:22,xp:35,desc:"It tears at households, loyalties, and the bonds people were meant to protect.",weaknesses:[JW("family",1.45),JW("love",1.12),JW("unity",.9)]},
  {id:"creation-mocker",name:"Creation Mocker",art:"creation-mocker",minFloor:13,maxFloor:28,hp:90,damage:23,xp:37,desc:"It points at the created order and denies the hand of the Creator.",weaknesses:[JW("creation",1.6),JW("god",1.05),JW("praise",.78)]},
  {id:"false-disciple",name:"False Disciple",art:"false-disciple",minFloor:15,maxFloor:30,hp:98,damage:24,xp:40,desc:"It knows the language of discipleship but refuses the path of following Christ.",weaknesses:[JW("discipleship",1.6),JW("obedience",1.08),JW("cross",.84)]},
  {id:"throne-of-self",name:"Throne of Self",art:"throne-of-self",minFloor:17,hp:108,damage:26,xp:44,desc:"It insists that greatness means being served rather than becoming a servant.",weaknesses:[JW("service",1.6),JW("humility",1.12),JW("giving",.78)]}
];

const PLAYER_ROOT="https://pub-15a649bcaae84f2ca6c610e6ef0dde51.r2.dev/players";
const CLASS_DEFS={
  "word-warden":{name:"Word Warden",art:"wordwarden",tier:0,parent:null,desc:"The traveling Scripture warrior at the beginning of the path."},
  "sword-of-truth":{name:"Sword of Truth",art:"sword-of-truth",tier:5,parent:"word-warden",desc:"A martial Scripture path focused on truth, the Word, and obedience."},
  "herald-of-grace":{name:"Herald of Grace",art:"herald-of-grace",tier:5,parent:"word-warden",desc:"A grace-centered path focused on mercy, forgiveness, and salvation."},
  "faithful-servant":{name:"Faithful Servant",art:"faithful-servant",tier:5,parent:"word-warden",desc:"A servant path focused on prayer, fellowship, humility, and stewardship."},
  "scripture-knight":{name:"Scripture Knight",art:"scripture-knight",tier:10,parent:"sword-of-truth",desc:"A disciplined defender grounded in Scripture."},
  "lightbearer":{name:"Lightbearer",art:"lightbearer",tier:10,parent:"sword-of-truth",desc:"A radiant path of truth, light, and holiness."},
  "kingdom-sentinel":{name:"Kingdom Sentinel",art:"kingdom-sentinel",tier:10,parent:"sword-of-truth",desc:"A watchman path centered on the kingdom and righteous defense."},
  "redeemed-witness":{name:"Redeemed Witness",art:"redeemed-witness",tier:10,parent:"herald-of-grace",desc:"A testimony path centered on redemption, the cross, and salvation."},
  "mercy-keeper":{name:"Mercy Keeper",art:"mercy-keeper",tier:10,parent:"herald-of-grace",desc:"A protector shaped by mercy, forgiveness, and compassion."},
  "hope-herald":{name:"Hope Herald",art:"hope-herald",tier:10,parent:"herald-of-grace",desc:"A witness of hope, joy, and the promises of God."},
  "prayer-disciple":{name:"Prayer Disciple",art:"prayer-disciple",tier:10,parent:"faithful-servant",desc:"A disciplined path of prayer and intercession."},
  "fellowship-shepherd":{name:"Fellowship Shepherd",art:"fellowship-shepherd",tier:10,parent:"faithful-servant",desc:"A shepherding path of unity, fellowship, and care."},
  "humble-steward":{name:"Humble Steward",art:"humble-steward",tier:10,parent:"faithful-servant",desc:"A path of service, humility, and faithful stewardship."},
  "lawkeeper":{name:"Lawkeeper",art:"lawkeeper",tier:15,parent:"scripture-knight",desc:"Law, obedience, and the Word carried with disciplined strength."},
  "living-scroll":{name:"Living Scroll",art:"living-scroll",tier:15,parent:"scripture-knight",desc:"Wisdom and covenant embodied through the living Word."},
  "beacon-saint":{name:"Beacon Saint",art:"beacon-saint",tier:15,parent:"lightbearer",desc:"A brilliant beacon of light, holiness, and truth."},
  "spirit-flame":{name:"Spirit Flame",art:"spirit-flame",tier:15,parent:"lightbearer",desc:"A final path of holy fire, Spirit, and truth."},
  "kingdom-champion":{name:"Kingdom Champion",art:"kingdom-champion",tier:15,parent:"kingdom-sentinel",desc:"A champion of the kingdom and its righteous cause."},
  "righteous-judge":{name:"Righteous Judge",art:"righteous-judge",tier:15,parent:"kingdom-sentinel",desc:"Justice and righteousness brought to the final path."},
  "cross-bearer":{name:"Cross Bearer",art:"cross-bearer",tier:15,parent:"redeemed-witness",desc:"The cross, blood, and salvation carried through hardship."},
  "resurrection-herald":{name:"Resurrection Herald",art:"resurrection-herald",tier:15,parent:"redeemed-witness",desc:"Resurrection, life, and eternal hope proclaimed boldly."},
  "forgiver":{name:"Forgiver",art:"forgiver",tier:15,parent:"mercy-keeper",desc:"A final path of forgiveness and released burdens."},
  "compassion-keeper":{name:"Compassion Keeper",art:"compassion-keeper",tier:15,parent:"mercy-keeper",desc:"Mercy and compassion guarded as a sacred calling."},
  "eternal-pilgrim":{name:"Eternal Pilgrim",art:"eternal-pilgrim",tier:15,parent:"hope-herald",desc:"A traveler fixed on eternal hope."},
  "joyful-witness":{name:"Joyful Witness",art:"joyful-witness",tier:15,parent:"hope-herald",desc:"Joy and testimony carried to the end of the road."},
  "intercessor":{name:"Intercessor",art:"intercessor",tier:15,parent:"prayer-disciple",desc:"A final class built around prayer and intercession."},
  "spirit-walker":{name:"Spirit Walker",art:"spirit-walker",tier:15,parent:"prayer-disciple",desc:"A prayerful path of Spirit-led perseverance."},
  "shepherd-of-one-body":{name:"Shepherd of One Body",art:"shepherd-of-one-body",tier:15,parent:"fellowship-shepherd",desc:"A shepherd focused on unity and one body."},
  "house-guardian":{name:"House Guardian",art:"house-guardian",tier:15,parent:"fellowship-shepherd",desc:"A guardian of fellowship, family, and the household of faith."},
  "servant-disciple":{name:"Servant Disciple",art:"servant-disciple",tier:15,parent:"humble-steward",desc:"Service and humility brought to maturity."},
  "generous-steward":{name:"Generous Steward",art:"generous-steward",tier:15,parent:"humble-steward",desc:"A final path of stewardship, generosity, and faithful care."}
};
const CLASS_CHILDREN=Object.keys(CLASS_DEFS).reduce((out,id)=>{const parent=CLASS_DEFS[id].parent;if(parent)(out[parent]??=[]).push(id);return out;},{});

const ITEMS_BY_RARITY=Object.entries(ITEM_DEFS).reduce((out,[id,item])=>{(out[item.rarity]??=[]).push(id);return out;},{});
const randomChoice=list=>list[Math.floor(Math.random()*list.length)];
const escapeHtml=value=>String(value??"").replace(/[&<>'\"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'\"':"&quot;"}[c]));
const itemName=id=>ITEM_DEFS[id]?.name||id;
const clone=value=>value==null?value:JSON.parse(JSON.stringify(value));

export function installRpgItems({getRpg,getVerses,conceptKeys,conceptLabel,matchWeightedConcepts,matchVerseConcept,refKey,displayRef,refresh}){
  let uiReady=false,lastSupplementalEnemyId="",pendingRestoredEnemy=null,restoreRequested=false,lastSaveStamp="";
  const username=(new URL(location.href).searchParams.get("username")||localStorage.getItem("magicUsername")||"Solo Pilgrim").trim().toLowerCase();
  const RUN_SAVE_KEY=`bibleSolo:${username}:rpgRunSave:v2`;

  function ensureState(){
    const rpg=getRpg();
    if(!rpg)return null;
    if(!Array.isArray(rpg.inventory))rpg.inventory=[];
    if(!Number.isFinite(rpg.itemShields))rpg.itemShields=0;
    if(!Array.isArray(rpg.runMastered))rpg.runMastered=[];
    if(!(rpg.used instanceof Set))rpg.used=new Set(Array.isArray(rpg.used)?rpg.used:[]);
    if(!CLASS_DEFS[rpg.classId])rpg.classId="word-warden";
    if(!Array.isArray(rpg.classHistory))rpg.classHistory=[rpg.classId];
    return rpg;
  }

  function readRunSave(){
    try{const raw=localStorage.getItem(RUN_SAVE_KEY);if(!raw)return null;const parsed=JSON.parse(raw);if(!parsed||parsed.version!==2||!parsed.state)return null;return parsed}catch{return null}
  }
  function serializeRun(rpg){
    return {version:2,savedAt:new Date().toISOString(),state:{
      health:rpg.health,maxHealth:rpg.maxHealth,level:rpg.level,xp:rpg.xp,score:rpg.score,floor:rpg.floor,streak:rpg.streak,baseDamage:rpg.baseDamage,
      used:[...(rpg.used instanceof Set?rpg.used:[])],runMastered:[...(rpg.runMastered||[])],enemy:clone(rpg.enemy),lastWeaknesses:clone(rpg.lastWeaknesses||[]),
      eliteNext:!!rpg.eliteNext,damageMod:Number(rpg.damageMod)||1,zone:Number(rpg.zone)||0,inventory:[...(rpg.inventory||[])],itemShields:Number(rpg.itemShields)||0,
      classId:CLASS_DEFS[rpg.classId]?rpg.classId:"word-warden",classHistory:[...(rpg.classHistory||[])],dead:false
    }};
  }
  function writeRunSave({manual=false,allowBetweenFights=false}={}){
    const rpg=ensureState();
    if(!rpg||rpg.dead||rpg.health<=0)return false;
    if(!allowBetweenFights&&(!rpg.enemy||rpg.enemy.hp<=0))return false;
    const payload=serializeRun(rpg);localStorage.setItem(RUN_SAVE_KEY,JSON.stringify(payload));lastSaveStamp=payload.savedAt;renderPersistenceUi();
    if(manual)showHint(`💾 Run saved locally • Floor ${rpg.floor} • Level ${rpg.level} • ${CLASS_DEFS[rpg.classId].name}`);
    return true;
  }
  function clearRunSave(){localStorage.removeItem(RUN_SAVE_KEY);lastSaveStamp="";renderPersistenceUi()}
  function applySavedState(rpg,payload,{deferEnemy=false}={}){
    const s=payload?.state;if(!rpg||!s)return false;
    const nums=["health","maxHealth","level","xp","score","floor","streak","baseDamage","damageMod","zone","itemShields"];
    for(const key of nums)if(Number.isFinite(Number(s[key])))rpg[key]=Number(s[key]);
    rpg.used=new Set(Array.isArray(s.used)?s.used:[]);rpg.runMastered=Array.isArray(s.runMastered)?[...s.runMastered]:[];
    rpg.lastWeaknesses=clone(s.lastWeaknesses||[]);rpg.eliteNext=!!s.eliteNext;rpg.inventory=Array.isArray(s.inventory)?[...s.inventory]:[];rpg.dead=false;
    rpg.classId=CLASS_DEFS[s.classId]?s.classId:"word-warden";rpg.classHistory=Array.isArray(s.classHistory)&&s.classHistory.length?[...s.classHistory]:[rpg.classId];
    if(deferEnemy){pendingRestoredEnemy=s.enemy&&Number(s.enemy.hp)>0?clone(s.enemy):null;rpg.enemy=null}else rpg.enemy=s.enemy&&Number(s.enemy.hp)>0?clone(s.enemy):rpg.enemy;
    return true;
  }
  function requestContinue(){const save=readRunSave();if(!save)return;restoreRequested=true;const launcher=document.querySelector('[data-mode="rpg"]');if(launcher)launcher.click()}
  function manualLoad(){const payload=readRunSave();if(!payload){showHint("No local Scripture Quest save exists yet.");return}const rpg=ensureState();if(!rpg){requestContinue();return}pendingRestoredEnemy=null;applySavedState(rpg,payload);document.getElementById("deathOverlay")?.classList.add("hidden");document.getElementById("pathOverlay")?.classList.add("hidden");document.getElementById("classOverlay")?.classList.add("hidden");if(typeof refresh==="function")refresh();render();showHint(`↻ Loaded local save from ${new Date(payload.savedAt).toLocaleString()}.`)}

  function classDef(rpg=ensureState()){return CLASS_DEFS[rpg?.classId]||CLASS_DEFS["word-warden"]}
  function classOptions(rpg=ensureState()){
    if(!rpg)return[];const current=classDef(rpg);
    if(rpg.level>=15&&current.tier===10)return CLASS_CHILDREN[rpg.classId]||[];
    if(rpg.level>=10&&current.tier===5)return CLASS_CHILDREN[rpg.classId]||[];
    if(rpg.level>=5&&current.tier===0)return CLASS_CHILDREN["word-warden"]||[];
    return[];
  }
  function renderClass(){
    const rpg=ensureState();if(!rpg)return;const def=classDef(rpg),img=document.getElementById("playerArt");
    if(img&&img.dataset.classArt!==def.art){img.dataset.classArt=def.art;img.alt=def.name;img.onerror=()=>{img.onerror=null;img.src=`${PLAYER_ROOT}/wordwarden.png`};img.src=`${PLAYER_ROOT}/${def.art}.png`;}
    const fighter=document.getElementById("playerFighter");if(fighter){const barName=fighter.querySelector(".bar-label span:first-child"),name=fighter.querySelector(".fighter-name"),sub=fighter.querySelector(".fighter-sub");if(barName)barName.textContent=def.name;if(name)name.textContent=def.name;if(sub)sub.textContent=def.desc;}
    const firstCard=document.querySelector("#rpgView .side-stack .side-card:first-child h3");if(firstCard)firstCard.textContent=`🛡 ${def.name}`;
  }
  function maybeOfferClassChoice(){
    const rpg=ensureState(),overlay=document.getElementById("classOverlay");if(!rpg||!overlay||rpg.dead)return;const ids=classOptions(rpg);if(!ids.length){overlay.classList.add("hidden");return}
    const tier=CLASS_DEFS[ids[0]]?.tier||rpg.level;document.getElementById("classTitle").textContent=tier===5?"Choose your first calling.":tier===10?"Your path deepens.":"Choose your final class.";
    document.getElementById("classHint").textContent=`Level ${tier} specialization • Your choice changes your character identity and artwork and is saved with this run.`;
    const box=document.getElementById("classChoices");box.innerHTML="";
    for(const id of ids){const def=CLASS_DEFS[id],btn=document.createElement("button");btn.className="path-choice";btn.innerHTML=`<strong>${escapeHtml(def.name)}</strong><small>${escapeHtml(def.desc)}</small>`;btn.onclick=()=>chooseClass(id);box.appendChild(btn)}
    overlay.classList.remove("hidden");
  }
  function chooseClass(id){const rpg=ensureState(),def=CLASS_DEFS[id];if(!rpg||!def||!classOptions(rpg).includes(id))return;rpg.classId=id;if(!rpg.classHistory.includes(id))rpg.classHistory.push(id);document.getElementById("classOverlay")?.classList.add("hidden");renderClass();writeRunSave({allowBetweenFights:true});if(typeof refresh==="function")refresh();showHint(`✨ Class chosen: ${def.name}. Character art now loads from /players/${def.art}.png.`)}

  function journeyEnemyHealthScale(floor){const depth=Math.max(0,(floor||1)-1);return 1+depth*.10+Math.pow(depth,1.55)*.012}
  function maybeSwapJourneyEnemy(){const rpg=ensureState();if(!rpg?.enemy||rpg.dead||rpg.enemy.boss||rpg.floor%5===0||Math.random()>.52)return false;let pool=JOURNEY_ENEMIES.filter(enemy=>rpg.floor>=enemy.minFloor&&(!enemy.maxFloor||rpg.floor<=enemy.maxFloor));if(pool.length>1)pool=pool.filter(enemy=>enemy.id!==lastSupplementalEnemyId);if(!pool.length)return false;const base=randomChoice(pool),elite=!!rpg.enemy.elite,scale=journeyEnemyHealthScale(rpg.floor),hp=Math.round(base.hp*scale*(elite?1.50:1)),damage=Math.round(base.damage*(1+(rpg.floor-1)*.04)*(elite?1.28:1));rpg.enemy={...base,maxHp:hp,hp,damage,elite,journeyExpansion:true};rpg.lastWeaknesses=base.weaknesses.map(weakness=>({...weakness}));lastSupplementalEnemyId=base.id;return true}

  function installViewportLayout(){
    const rpgView=document.getElementById("rpgView"),battlefield=document.getElementById("battlefield"),layout=battlefield?.closest(".rpg-layout"),sideStack=layout?.querySelector(":scope > .side-stack");if(!rpgView||!battlefield||!layout)return null;
    let controls=document.getElementById("rpgControlPane");if(!controls){controls=document.createElement("section");controls.id="rpgControlPane";controls.className="rpg-control-pane";layout.appendChild(controls);const combatBox=battlefield.querySelector(".combat-box"),pathOverlay=document.getElementById("pathOverlay"),deathOverlay=document.getElementById("deathOverlay");if(combatBox)controls.appendChild(combatBox);if(sideStack)controls.appendChild(sideStack);if(pathOverlay)controls.appendChild(pathOverlay);if(deathOverlay)controls.appendChild(deathOverlay);}rpgView.classList.add("rpg-viewport-mode");return controls;
  }

  function installUi(){
    if(uiReady)return;const battlefield=document.getElementById("battlefield"),input=document.getElementById("rpgRefInput"),attack=document.getElementById("rpgAttackBtn");if(!battlefield||!input||!attack)return;const controls=installViewportLayout();if(!controls)return;uiReady=true;
    const style=document.createElement("style");style.id="rpgViewportSplitStyles";style.textContent=`
      #rpgView.rpg-viewport-mode:not(.hidden){position:fixed!important;inset:0!important;z-index:1000!important;display:block!important;width:100vw!important;height:100dvh!important;max-width:none!important;margin:0!important;padding:0!important;background:#070b15!important;overflow:hidden!important}
      #rpgView.rpg-viewport-mode .rpg-layout{display:grid!important;grid-template-columns:minmax(0,3fr) minmax(280px,2fr)!important;grid-template-rows:1fr!important;gap:0!important;width:100%!important;height:100%!important;min-height:0!important;max-height:none!important;margin:0!important;padding:0!important;overflow:hidden!important}
      #rpgView.rpg-viewport-mode .battlefield{grid-column:1!important;grid-row:1!important;width:100%!important;height:100%!important;min-width:0!important;min-height:0!important;margin:0!important;border-radius:0!important;border-width:0 1px 0 0!important;padding:clamp(8px,1.5vw,16px)!important;background-position:center!important;background-size:cover!important;overflow:hidden!important}
      #rpgView.rpg-viewport-mode .rpg-control-pane{grid-column:2!important;grid-row:1!important;position:relative!important;display:flex!important;flex-direction:column!important;gap:8px!important;width:100%!important;height:100%!important;min-width:0!important;min-height:0!important;padding:10px!important;padding-bottom:max(10px,env(safe-area-inset-bottom))!important;background:linear-gradient(180deg,#0d1425,#080d18)!important;overflow-y:auto!important;overscroll-behavior:contain!important}
      #rpgView.rpg-viewport-mode .combat-box{position:relative!important;inset:auto!important;left:auto!important;right:auto!important;bottom:auto!important;width:100%!important;flex:0 0 auto!important;margin:0!important;padding:10px!important;border-radius:14px!important;z-index:2!important}
      #rpgView.rpg-viewport-mode .side-stack{display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px!important;align-content:start!important;width:100%!important;min-width:0!important}
      #rpgView.rpg-viewport-mode .side-card{min-width:0!important;margin:0!important;padding:9px!important}
      #rpgView.rpg-viewport-mode .side-card:nth-child(2),#rpgView.rpg-viewport-mode .side-card:nth-child(3){max-height:190px!important;overflow:auto!important}
      #rpgView.rpg-viewport-mode .side-stack>.ghost,#rpgView.rpg-viewport-mode .rpg-save-row{grid-column:1/-1!important}
      #rpgView.rpg-viewport-mode .choice-overlay{position:absolute!important;inset:0!important;width:100%!important;height:100%!important;padding:10px!important;z-index:40!important;border-radius:0!important;overflow:auto!important;background:rgba(5,8,16,.97)!important}
      #rpgView.rpg-viewport-mode #classOverlay{z-index:60!important}
      #rpgView.rpg-viewport-mode .choice-card{width:100%!important;max-width:none!important;max-height:100%!important;margin:auto!important;overflow:auto!important;padding:12px!important}
      #rpgView.rpg-viewport-mode .rpg-item-actions{grid-template-columns:minmax(0,1fr) auto auto!important}.rpg-item-button{position:relative;white-space:nowrap}
      .rpg-item-badge{display:inline-grid;place-items:center;min-width:20px;height:20px;margin-left:5px;padding:0 5px;border-radius:999px;background:#fbbf24;color:#291b03;font-size:.68rem;font-weight:1000}
      .rpg-item-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;max-height:none;overflow:auto;margin-top:8px}.rpg-item-card{border:1px solid #334155;border-radius:13px;background:#0c1325;color:#fff;padding:10px;text-align:left;cursor:pointer}.rpg-item-card:hover{border-color:#8b5cf6}.rpg-item-card:disabled{opacity:.48;cursor:not-allowed}
      .rpg-item-top{display:flex;justify-content:space-between;gap:8px;align-items:flex-start}.rpg-item-name{font-weight:1000}.rpg-item-count{color:#fde68a;font-weight:1000}.rpg-item-rarity{font-size:.62rem;text-transform:uppercase;letter-spacing:.1em;color:#c4b5fd;font-weight:1000;margin:3px 0 6px}.rpg-item-effect-label{font-size:.6rem;text-transform:uppercase;letter-spacing:.12em;color:#fde68a;font-weight:1000;margin-bottom:2px}.rpg-item-desc{font-size:.76rem;line-height:1.35;color:#d4dbe7}
      .rpg-hint-reveal{margin:7px 0 0;padding:8px 10px;border:1px solid #854d0e;border-radius:10px;background:#2a1d08;color:#fde68a;font-size:.76rem;font-weight:850;line-height:1.4}.rpg-loot-notice{margin-top:7px;padding:7px 9px;border:1px solid #14532d;border-radius:10px;background:#0d2819;color:#bbf7d0;font-size:.76rem;font-weight:850}.rpg-item-summary{font-size:.68rem;color:#cbd5e1;margin-top:5px;text-align:center}
      .rpg-item-float{position:absolute;z-index:9;left:50%;top:35%;width:min(76%,560px);transform:translate(-50%,18px) scale(.96);opacity:0;pointer-events:none;text-align:center;padding:12px 14px;border:1px solid rgba(251,191,36,.72);border-radius:16px;background:rgba(7,11,21,.88);box-shadow:0 12px 36px rgba(0,0,0,.46);backdrop-filter:blur(6px)}.rpg-item-float.active{animation:rpgItemFloat 4.2s ease-out forwards}.rpg-item-float-title{font-size:.88rem;font-weight:1000;color:#fde68a;text-transform:uppercase;letter-spacing:.07em}.rpg-item-float-text{margin-top:5px;color:#fff;font-size:clamp(.84rem,2vw,1.08rem);line-height:1.35;font-weight:850;text-shadow:0 2px 5px #000}@keyframes rpgItemFloat{0%{opacity:0;transform:translate(-50%,18px) scale(.96)}10%{opacity:1;transform:translate(-50%,0) scale(1)}72%{opacity:1;transform:translate(-50%,-4px) scale(1)}100%{opacity:0;transform:translate(-50%,-28px) scale(.98)}}
      .rpg-save-row{display:grid;grid-template-columns:1fr 1fr;gap:7px}.rpg-save-status{grid-column:1/-1;text-align:center;font-size:.66rem;color:#a5b4cc}.rpg-continue{border-color:#166534!important;color:#bbf7d0!important}
      #rpgView.rpg-viewport-mode .arena-fighter{bottom:clamp(18px,5vh,50px)!important;width:min(42%,310px)!important}#rpgView.rpg-viewport-mode .arena-fighter.player{left:4%!important}#rpgView.rpg-viewport-mode .arena-fighter.enemy{right:4%!important}#rpgView.rpg-viewport-mode .character-art{height:min(43vh,330px)!important}#rpgView.rpg-viewport-mode .character-art img{max-height:min(43vh,330px)!important}#rpgView.rpg-viewport-mode .verse-echo{top:16%!important;width:88%!important}
      @media (orientation:portrait){#rpgView.rpg-viewport-mode .rpg-layout{grid-template-columns:1fr!important;grid-template-rows:minmax(0,3fr) minmax(0,2fr)!important}#rpgView.rpg-viewport-mode .battlefield{grid-column:1!important;grid-row:1!important;border-width:0 0 1px 0!important}#rpgView.rpg-viewport-mode .rpg-control-pane{grid-column:1!important;grid-row:2!important;padding:8px!important;padding-bottom:max(8px,env(safe-area-inset-bottom))!important}#rpgView.rpg-viewport-mode .arena-fighter{bottom:10px!important;width:min(43%,250px)!important}#rpgView.rpg-viewport-mode .character-art{height:min(29vh,250px)!important}#rpgView.rpg-viewport-mode .character-art img{max-height:min(29vh,250px)!important}#rpgView.rpg-viewport-mode .fighter-sub,#rpgView.rpg-viewport-mode .resistance-row{display:none!important}#rpgView.rpg-viewport-mode .side-stack{grid-template-columns:repeat(3,minmax(0,1fr))!important}#rpgView.rpg-viewport-mode .side-card{font-size:.72rem!important;padding:7px!important}#rpgView.rpg-viewport-mode .side-card h3{font-size:.78rem!important;margin-bottom:5px!important}#rpgView.rpg-viewport-mode .mini-grid{gap:4px!important}.mini-stat{padding:5px!important}.mini-stat b{font-size:.9rem!important}.mini-stat span{font-size:.53rem!important}#rpgView.rpg-viewport-mode .mastery-list,#rpgView.rpg-viewport-mode .high-list{max-height:90px!important}#rpgView.rpg-viewport-mode .side-stack>.ghost{grid-column:1/-1!important;min-height:34px!important;padding:6px 10px!important}#rpgView.rpg-viewport-mode .rpg-item-float{top:28%;width:min(88%,520px);padding:9px 11px}}
      @media (max-width:620px){#rpgView.rpg-viewport-mode .rpg-item-actions{grid-template-columns:minmax(0,1fr) auto!important}#rpgView.rpg-viewport-mode .rpg-item-actions .rpg-item-button{grid-column:1/-1!important;width:100%!important;min-height:34px!important;padding:6px 9px!important}#rpgView.rpg-viewport-mode .rpg-item-grid{grid-template-columns:1fr!important}#rpgView.rpg-viewport-mode .requirement{font-size:.8rem!important;margin-bottom:3px!important}#rpgView.rpg-viewport-mode .requirement b{font-size:.88rem!important}#rpgView.rpg-viewport-mode .weakness-hint{font-size:.64rem!important;margin-bottom:6px!important}#rpgView.rpg-viewport-mode .combat-log{font-size:.7rem!important;min-height:20px!important;margin-top:4px!important}#rpgView.rpg-viewport-mode input[type=text]{min-height:38px!important;padding:7px 9px!important}#rpgView.rpg-viewport-mode .btn{min-height:38px!important;padding:7px 10px!important}}
    `;document.head.appendChild(style);

    const row=input.closest(".input-row");if(row){row.classList.add("rpg-item-actions");const button=document.createElement("button");button.className="ghost rpg-item-button";button.id="rpgItemsBtn";button.type="button";button.innerHTML='🎒 Items <span class="rpg-item-badge" id="rpgItemCount">0</span>';button.onclick=openPanel;row.appendChild(button)}
    const combatLog=document.getElementById("combatLog");if(combatLog){const hint=document.createElement("div");hint.id="rpgHintReveal";hint.className="rpg-hint-reveal hidden";combatLog.before(hint);const loot=document.createElement("div");loot.id="rpgLootNotice";loot.className="rpg-loot-notice hidden";combatLog.after(loot);const summary=document.createElement("div");summary.id="rpgItemSummary";summary.className="rpg-item-summary";loot.after(summary)}
    if(!document.getElementById("rpgItemFloat")){const floater=document.createElement("div");floater.id="rpgItemFloat";floater.className="rpg-item-float";floater.setAttribute("aria-live","polite");floater.innerHTML='<div class="rpg-item-float-title" id="rpgItemFloatTitle"></div><div class="rpg-item-float-text" id="rpgItemFloatText"></div>';battlefield.appendChild(floater)}
    const itemOverlay=document.createElement("div");itemOverlay.className="choice-overlay hidden";itemOverlay.id="itemOverlay";itemOverlay.innerHTML=`<div class="choice-card"><div class="row between"><div><div class="eyebrow">Run Inventory</div><h2 style="margin:6px 0">Use an item</h2></div><button class="ghost" id="itemCloseBtn" type="button">Close</button></div><p class="muted" id="itemPanelHint">Every item shows its exact effect before use. Hint results also appear below and briefly fade over the battlefield.</p><div class="rpg-item-grid" id="rpgItemGrid"></div></div>`;controls.appendChild(itemOverlay);document.getElementById("itemCloseBtn").onclick=closePanel;itemOverlay.addEventListener("click",event=>{if(event.target===itemOverlay)closePanel()});
    const classOverlay=document.createElement("div");classOverlay.className="choice-overlay hidden";classOverlay.id="classOverlay";classOverlay.innerHTML=`<div class="choice-card"><div class="eyebrow">Class Advancement</div><h2 id="classTitle" style="margin:6px 0">Choose your calling.</h2><p class="muted" id="classHint"></p><div class="path-choices" id="classChoices"></div></div>`;controls.appendChild(classOverlay);
    const sideStack=document.querySelector("#rpgView .side-stack");if(sideStack&&!document.getElementById("rpgSaveRow")){const leave=sideStack.querySelector(":scope > .ghost"),saveRow=document.createElement("div");saveRow.id="rpgSaveRow";saveRow.className="rpg-save-row";saveRow.innerHTML='<button class="ghost" id="rpgSaveBtn" type="button">💾 Save Run</button><button class="ghost" id="rpgLoadBtn" type="button">↻ Load Run</button><div class="rpg-save-status" id="rpgSaveStatus">Local save ready.</div>';if(leave)sideStack.insertBefore(saveRow,leave);else sideStack.appendChild(saveRow);document.getElementById("rpgSaveBtn").onclick=()=>writeRunSave({manual:true,allowBetweenFights:true});document.getElementById("rpgLoadBtn").onclick=manualLoad}
    const heroRow=document.querySelector("#hubView .hero-copy .row");if(heroRow&&!document.getElementById("rpgContinueBtn")){const continueBtn=document.createElement("button");continueBtn.id="rpgContinueBtn";continueBtn.className="ghost rpg-continue hidden";continueBtn.type="button";continueBtn.textContent="↻ Continue Saved Run";continueBtn.onclick=requestContinue;heroRow.appendChild(continueBtn)}
    window.addEventListener("beforeunload",()=>writeRunSave({allowBetweenFights:true}));renderPersistenceUi();
  }

  function renderPersistenceUi(){const save=readRunSave(),load=document.getElementById("rpgLoadBtn"),cont=document.getElementById("rpgContinueBtn"),status=document.getElementById("rpgSaveStatus");if(load)load.disabled=!save;if(cont)cont.classList.toggle("hidden",!save);if(status){if(save){const d=new Date(save.savedAt);status.textContent=`Saved locally ${Number.isNaN(d.getTime())?"":"• "+d.toLocaleString()}`}else status.textContent="No local run saved yet."}}
  function reset(){installUi();const rpg=ensureState();if(!rpg)return;lastSupplementalEnemyId="";hideHint();hideLoot();closePanel();document.getElementById("classOverlay")?.classList.add("hidden");if(restoreRequested){const payload=readRunSave();restoreRequested=false;if(payload){applySavedState(rpg,payload,{deferEnemy:true});showHint(`↻ Restoring local save • Floor ${rpg.floor} • Level ${rpg.level}.`)}else{rpg.inventory=[];rpg.itemShields=0;rpg.classId="word-warden";rpg.classHistory=["word-warden"]}}else{rpg.inventory=[];rpg.itemShields=0;rpg.classId="word-warden";rpg.classHistory=["word-warden"];pendingRestoredEnemy=null}renderClass();renderPersistenceUi();render()}
  function onSpawn(){installUi();hideHint();hideLoot();closePanel();const rpg=ensureState();let restored=false;if(rpg&&pendingRestoredEnemy){rpg.enemy=clone(pendingRestoredEnemy);rpg.lastWeaknesses=clone(rpg.enemy.weaknesses||rpg.lastWeaknesses||[]);pendingRestoredEnemy=null;restored=true}else maybeSwapJourneyEnemy();if(restored){const log=document.getElementById("combatLog");if(log)log.textContent=`Local save restored. Continue the fight against ${rpg.enemy.name}.`}renderClass();render();writeRunSave()}
  function activeFight(){const rpg=ensureState();return !!(rpg&&!rpg.dead&&rpg.enemy&&rpg.enemy.hp>0)}
  function openPanel(){installUi();if(!activeFight())return;renderPanel();document.getElementById("itemOverlay")?.classList.remove("hidden")}
  function closePanel(){document.getElementById("itemOverlay")?.classList.add("hidden")}
  function hideHint(){const el=document.getElementById("rpgHintReveal");if(el){el.textContent="";el.classList.add("hidden")}}
  function hideLoot(){const el=document.getElementById("rpgLootNotice");if(el){el.textContent="";el.classList.add("hidden")}}
  function showHint(text){const el=document.getElementById("rpgHintReveal");if(!el)return;el.textContent=text;el.classList.remove("hidden")}
  function showFloatingItem(item,message){const box=document.getElementById("rpgItemFloat"),title=document.getElementById("rpgItemFloatTitle"),text=document.getElementById("rpgItemFloatText");if(!box||!title||!text||!item)return;title.textContent=`${item.icon} ${item.name}`;text.textContent=message;box.classList.remove("active");void box.offsetWidth;box.classList.add("active")}
  function showItemFeedback(item,message,used){showHint(`${item?.icon||"🎒"} ${item?.name||"Item"} — ${message}`);if(used)showFloatingItem(item,message)}
  function showLoot(drops,foe){const el=document.getElementById("rpgLootNotice");if(!el)return;if(!drops.length){el.classList.add("hidden");el.textContent="";return}const names=drops.map(id=>`${ITEM_DEFS[id].icon} ${ITEM_DEFS[id].name}`).join(" • "),source=foe.boss?"Boss reward":foe.elite?"Elite reward":"Victory loot";el.textContent=`${source}: ${names}`;el.classList.remove("hidden")}
  function rarityRoll(foe){const rpg=ensureState(),depthBoost=Math.min(.22,Math.max(0,(rpg?.floor||1)-1)*.009),challengeBoost=(foe?.elite?0.22:0)+(foe?.boss?0.36:0),roll=Math.random()+depthBoost+challengeBoost;if(roll>=1.17)return"epic";if(roll>=.83)return"rare";if(roll>=.47)return"uncommon";return"common"}
  function rollItem(foe){const rarity=rarityRoll(foe),pool=ITEMS_BY_RARITY[rarity]||ITEMS_BY_RARITY.common;return randomChoice(pool)}
  function awardLoot(foe){installUi();const rpg=ensureState();if(!rpg)return[];closePanel();const baseChance=Math.min(.80,.48+Math.max(0,rpg.floor-1)*.012),chance=foe?.boss?1:(foe?.elite?0.98:baseChance);if(Math.random()>chance){showLoot([],foe);render();return[]}let count=1;if(foe?.elite&&Math.random()<.38)count++;if(foe?.boss)count=2+(Math.random()<.28?1:0);const drops=[];for(let i=0;i<count;i++){const id=rollItem(foe);rpg.inventory.push(id);drops.push(id)}showLoot(drops,foe);render();return drops}
  function groupedInventory(){const rpg=ensureState(),map=new Map();for(const id of rpg?.inventory||[])map.set(id,(map.get(id)||0)+1);return[...map.entries()].sort((a,b)=>(RARITY_META[ITEM_DEFS[b[0]]?.rarity]?.rank??0)-(RARITY_META[ITEM_DEFS[a[0]]?.rarity]?.rank??0)||itemName(a[0]).localeCompare(itemName(b[0])))}
  function render(){installUi();const rpg=ensureState(),count=rpg?.inventory?.length||0,button=document.getElementById("rpgItemsBtn"),badge=document.getElementById("rpgItemCount"),summary=document.getElementById("rpgItemSummary");if(rpg&&rpg.health<=0)clearRunSave();if(badge)badge.textContent=String(count);if(button)button.disabled=!activeFight()||count===0;if(summary)summary.textContent=count?`🎒 ${count} item${count===1?"":"s"} • 🛡 ${rpg.itemShields||0} shield charge${rpg.itemShields===1?"":"s"} • Open Items to see exact effects.`:"Defeat enemies to find consumable items. Elites and bosses improve loot quality.";renderClass();renderPersistenceUi();if(rpg&&!rpg.dead&&rpg.health>0)writeRunSave();maybeOfferClassChoice();if(!document.getElementById("itemOverlay")?.classList.contains("hidden"))renderPanel()}
  function renderPanel(){const grid=document.getElementById("rpgItemGrid"),hint=document.getElementById("itemPanelHint");if(!grid)return;const groups=groupedInventory();if(!groups.length){grid.innerHTML='<div class="muted">Your inventory is empty. Elite and boss victories have the best reward odds.</div>';return}grid.innerHTML="";if(hint)hint.textContent="Choose one consumable. The exact effect is shown on every card; after use, the result stays below and briefly fades over the battlefield.";for(const[id,count]of groups){const item=ITEM_DEFS[id];if(!item)continue;const btn=document.createElement("button");btn.type="button";btn.className="rpg-item-card";btn.innerHTML=`<div class="rpg-item-top"><span class="rpg-item-name">${item.icon} ${escapeHtml(item.name)}</span><span class="rpg-item-count">×${count}</span></div><div class="rpg-item-rarity">${RARITY_META[item.rarity].label}</div><div class="rpg-item-effect-label">Effect</div><div class="rpg-item-desc">${escapeHtml(item.desc)}</div>`;btn.onclick=()=>useItem(id);grid.appendChild(btn)}}
  function removeOne(id){const rpg=ensureState(),index=rpg?.inventory?.indexOf(id)??-1;if(index<0)return false;rpg.inventory.splice(index,1);return true}
  function heal(amount){const rpg=ensureState();if(!rpg)return null;if(rpg.health>=rpg.maxHealth)return 0;const before=rpg.health;rpg.health=Math.min(rpg.maxHealth,rpg.health+Math.max(1,Math.round(rpg.maxHealth*amount)));return rpg.health-before}
  function hintCandidates(){const rpg=ensureState();if(!rpg?.enemy)return[];return getVerses().filter(v=>!rpg.used?.has(refKey(v))&&!!matchWeightedConcepts(v.text,rpg.enemy.weaknesses))}
  function pickHintVerse(){const candidates=hintCandidates();return candidates.length?randomChoice(candidates):null}
  function fragment(text,count=7){const words=String(text||"").replace(/[“”]/g,'"').split(/\s+/).filter(Boolean);if(words.length<=count)return words.join(" ");const start=Math.floor(Math.random()*(words.length-count+1));return words.slice(start,start+count).join(" ")}
  function bonusConcepts(count,multiplier){const rpg=ensureState();if(!rpg?.enemy)return[];const existing=new Set((rpg.enemy.weaknesses||[]).map(w=>w.concept)),candidates=conceptKeys.filter(key=>!existing.has(key)&&getVerses().some(v=>!!matchVerseConcept(v.text,key))),selected=[];while(selected.length<count&&candidates.length){const index=Math.floor(Math.random()*candidates.length),key=candidates.splice(index,1)[0];selected.push(key);rpg.enemy.weaknesses.push({concept:key,multiplier})}if(selected.length)rpg.lastWeaknesses=rpg.enemy.weaknesses.map(w=>({...w}));return selected}

  function useItem(id){if(!activeFight())return;const rpg=ensureState(),item=ITEM_DEFS[id];if(!item||!rpg.inventory.includes(id))return;let used=true,message="";if(item.kind==="heal"){const healed=heal(item.amount);if(!healed){used=false;message="You are already at full health."}else message=`Restored ${healed} health.`}else if(item.kind==="shield"){rpg.itemShields=Math.min(3,(rpg.itemShields||0)+1);message="Shield ready: your next failed verse will not cost health."}else if(item.kind==="refresh"){rpg.used.clear();message="Every verse used during this run is refreshed and can be played again."}else if(item.kind==="great-recall"){rpg.used.clear();const healed=heal(.35)||0;rpg.itemShields=Math.min(3,(rpg.itemShields||0)+1);message=`Used verses refreshed, ${healed} health restored, and 1 shield charge gained.`}else if(item.kind==="category"){const added=bonusConcepts(item.count||1,item.multiplier||1);if(!added.length){used=false;message="No additional Scripture categories are available for this fight."}else message=`New weakness${added.length>1?"es":""}: ${added.map(conceptLabel).join(" + ")}. Verses from ${added.length>1?"those categories":"that category"} can now damage this enemy.`}else{const verse=pickHintVerse();if(!verse){used=false;message="No unused matching verse is available for a hint."}else if(item.kind==="hint-words")message=`Hint: “${fragment(verse.text,7)}” — no book or reference revealed.`;else if(item.kind==="hint-book")message=`Hint: one unused valid verse is in ${verse.book}.`;else if(item.kind==="hint-chapter")message=`Hint: one unused valid verse is in ${verse.book} ${verse.chapter}.`;else if(item.kind==="hint-reference")message=`Hint: ${displayRef(verse)} can damage this enemy.`;else{used=false;message="That item cannot be used right now."}}if(used){removeOne(id);closePanel()}showItemFeedback(item,message,used);if(typeof refresh==="function")refresh();render();if(!used)renderPanel()}

  function consumeShield(){const rpg=ensureState();if(!rpg||!rpg.itemShields)return false;rpg.itemShields--;render();return true}
  function dropSummary(drops){return drops.map(id=>`${ITEM_DEFS[id]?.icon||"🎁"} ${itemName(id)}`).join(" • ")}
  installUi();
  return{reset,onSpawn,awardLoot,consumeShield,render,openPanel,closePanel,dropSummary,saveRun:()=>writeRunSave({manual:true,allowBetweenFights:true}),loadRun:manualLoad,clearRunSave,defs:ITEM_DEFS,journeyEnemies:JOURNEY_ENEMIES,classDefs:CLASS_DEFS};
}
