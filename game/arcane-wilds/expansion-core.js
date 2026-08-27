'use strict';

/* Arcane Wilds world/loot/village expansion. Loaded after the core runtime so it can
   extend the existing systems without duplicating the base game. */
const EXPANSION_SAVE_KEY='arcaneWildsExpansionV2';

const MATERIALS={
 iron:{name:'Iron Shard',icon:'⛓️',color:'#b9c2cc'},
 dust:{name:'Arcane Dust',icon:'✨',color:'#c69cff'},
 hide:{name:'Beast Hide',icon:'🟫',color:'#b9845e'},
 frost:{name:'Frost Crystal',icon:'💠',color:'#aeeeff'},
 ember:{name:'Emberglass',icon:'🔶',color:'#ff8b5c'},
 bone:{name:'Gravebone',icon:'🦴',color:'#e2dccb'}
};

const TRINKET_BASES=[
 {name:'Ember Sigil',icon:'🔸',mods:{damage:.07},desc:'+7% weapon damage.'},
 {name:'Iron Locket',icon:'🧿',mods:{hp:.08,armor:.012},desc:'+8% maximum health and +1.2% armor.'},
 {name:'Traveler Compass',icon:'🧭',mods:{move:.06},desc:'+6% movement speed.'},
 {name:'Seer Prism',icon:'🔮',mods:{spell:.08},desc:'+8% spell potency.'},
 {name:'Lucky Coin',icon:'🪙',mods:{gold:.12,materials:.10},desc:'+12% gold and +10% material yield.'},
 {name:'Stormglass Shard',icon:'⚡',mods:{spell:.045,cdr:.04},desc:'+4.5% spell potency and 4% cooldown recovery.'},
 {name:'Hunter Fang',icon:'🦷',mods:{damage:.035,elite:.12},desc:'+3.5% weapon damage and stronger hits against elites.'},
 {name:'Moon Thread',icon:'🌙',mods:{move:.035,cdr:.035},desc:'+3.5% movement and cooldown recovery.'},
 {name:'Ward Knot',icon:'🪢',mods:{hp:.04,ward:.10},desc:'+4% health and stronger warding effects.'}
];

const PREFIXES=[
 {key:'savage',name:'Savage',slots:['weapon'],text:'+16% base power.',apply:i=>i.power*=1.16},
 {key:'swift',name:'Swift',slots:['weapon'],text:'+13% attack speed.',apply:i=>i.attack*=1.13},
 {key:'farshot',name:'Farshot',slots:['weapon'],text:'+14% range and +8% projectile speed.',apply:i=>{i.range*=1.14;i.speed*=1.08}},
 {key:'colossal',name:'Colossal',slots:['weapon'],text:'+24% power, -8% attack speed.',apply:i=>{i.power*=1.24;i.attack*=.92}},
 {key:'stalwart',name:'Stalwart',slots:['armor'],text:'+24% armor health bonus.',apply:i=>i.hpBonus*=1.24},
 {key:'fleet',name:'Fleet',slots:['armor'],text:'+7% movement speed.',apply:i=>i.move*=1.07},
 {key:'bastioned',name:'Bastioned',slots:['armor'],text:'+3.5% armor.',apply:i=>i.armorBonus+=.035},
 {key:'vital',name:'Vital',slots:['trinket'],text:'+6% maximum health.',apply:i=>i.mods.hp=(i.mods.hp||0)+.06},
 {key:'keen',name:'Keen',slots:['trinket'],text:'+5% weapon damage.',apply:i=>i.mods.damage=(i.mods.damage||0)+.05},
 {key:'mystic',name:'Mystic',slots:['trinket'],text:'+6% spell potency.',apply:i=>i.mods.spell=(i.mods.spell||0)+.06},
 {key:'masterwork',name:'Masterwork',slots:['weapon','armor','trinket'],text:'Improved primary statistics.',apply:i=>{
   if(i.slot==='weapon')i.power*=1.1;
   else if(i.slot==='armor'){i.hpBonus*=1.1;i.armorBonus+=.012}
   else Object.keys(i.mods||{}).forEach(k=>i.mods[k]*=1.18);
 }}
];

const SUFFIXES=[
 {key:'embers',name:'of Embers',slots:['weapon','trinket'],text:'Weapon hits can ignite enemies.'},
 {key:'frost',name:'of Rime',slots:['weapon','trinket'],text:'Weapon hits can slow enemies.'},
 {key:'storm',name:'of Storms',slots:['weapon','trinket'],text:'Weapon hits can arc lightning to another foe.'},
 {key:'mending',name:'of Mending',slots:['armor','trinket'],text:'Enemy kills restore a small amount of health.'},
 {key:'warding',name:'of Warding',slots:['armor','trinket'],text:'Taking damage can create a protective ward.'},
 {key:'thorns',name:'of Thorns',slots:['armor','trinket'],text:'Reflect a portion of received damage.'},
 {key:'fortune',name:'of Fortune',slots:['weapon','armor','trinket'],text:'Increases gold and material finds.'},
 {key:'renewal',name:'of Renewal',slots:['armor','trinket'],text:'Clearing a dangerous room restores health.'},
 {key:'execution',name:'of the Hunt',slots:['weapon','trinket'],text:'Deals bonus weapon damage to elites and bosses.'}
];

const VILLAGER_NAMES=['Mara','Tovin','Ilyra','Benn','Nessa','Orin','Kest','Pella','Dorn','Sera','Venn','Alia','Rook','Miri','Calder','Ysolde','Fen','Tamsin'];
const VILLAGER_COLORS=['#6d8db2','#a56862','#6e9a73','#9b7aaf','#b58e58','#557c86','#8b6978'];
const VILLAGER_LINES=[
 'The outer roads change after every hard winter. Keep your map close.',
 'Boss tracks are worth following. Their hoards carry far better gear.',
 'The smith pays attention to Emberglass and Frost Crystal. Do not sell those lightly.',
 'Some weapons are plain. Some carry a name that changes everything.',
 'Three trinkets is plenty until you find a fourth you cannot bear to leave behind.',
 'Threat rises quickly beyond the old roads. A fast dodge matters more than thick armor out there.'
];

function expansionDefault(){return {materials:{iron:0,dust:0,hide:0,frost:0,ember:0,bone:0},quests:{active:[],completed:[],rep:0},trinkets:[null,null,null],wardCooldown:0};}
function ensureExpansionState(){
 const d=expansionDefault();
 game.materials=game.materials||d.materials;
 for(const k of Object.keys(MATERIALS))game.materials[k]=Number(game.materials[k]||0);
 game.quests=game.quests||d.quests;game.quests.active=game.quests.active||[];game.quests.completed=game.quests.completed||[];game.quests.rep=Number(game.quests.rep||0);
 if(game.player){game.player.trinkets=Array.isArray(game.player.trinkets)?game.player.trinkets.slice(0,3):[null,null,null];while(game.player.trinkets.length<3)game.player.trinkets.push(null)}
 game.expansion=game.expansion||{wardCooldown:0};
}

function saveExpansion(){
 if(!game.player)return;ensureExpansionState();
 try{localStorage.setItem(EXPANSION_SAVE_KEY,JSON.stringify({materials:game.materials,quests:game.quests,trinkets:game.player.trinkets}))}catch(err){console.warn('Arcane Wilds expansion save failed',err)}
}
function loadExpansion(){
 ensureExpansionState();
 try{const raw=localStorage.getItem(EXPANSION_SAVE_KEY);if(!raw)return;const d=JSON.parse(raw);game.materials={...game.materials,...(d.materials||{})};game.quests={...game.quests,...(d.quests||{})};game.quests.active=game.quests.active||[];game.quests.completed=game.quests.completed||[];game.player.trinkets=Array.isArray(d.trinkets)?d.trinkets.slice(0,3):[null,null,null];while(game.player.trinkets.length<3)game.player.trinkets.push(null)}catch(err){console.warn('Arcane Wilds expansion load failed',err)}
}

function rarityForLevel(level=game.level,boost=0){
 const r=Math.random()-boost;let rarity='Common';
 if(level>=18&&r>.955)rarity='Legendary';
 else if(level>=12&&r>.86)rarity='Epic';
 else if(level>=7&&r>.68)rarity='Rare';
 else if(level>=3&&r>.43)rarity='Uncommon';
 return rarity;
}
function makeTrinket(base,level,rarity){
 const m=gearRarityMult[rarity]||1,mods={};for(const [k,v] of Object.entries(base.mods||{}))mods[k]=v*m;
 return {...base,baseName:base.name,slot:'trinket',rarity,level,mods,id:Math.random().toString(36).slice(2)};
}
function affixRates(rarity,crafted=false){const rank=rarityRank[rarity]||0;return {prefix:Math.min(.96,.14+rank*.18+(crafted?.14:0)),suffix:Math.min(.94,.08+rank*.19+(crafted?.16:0))}}
function rollAffixes(item,crafted=false){
 item.baseName=item.baseName||item.name;const rates=affixRates(item.rarity,crafted);
 if(Math.random()<rates.prefix){const pool=PREFIXES.filter(p=>p.slots.includes(item.slot));const p=pool[irnd(pool.length)];if(p){item.prefixKey=p.key;item.prefixName=p.name;item.prefixText=p.text;p.apply(item)}}
 if(Math.random()<rates.suffix){const pool=SUFFIXES.filter(s=>s.slots.includes(item.slot));const s=pool[irnd(pool.length)];if(s){item.suffixKey=s.key;item.suffixName=s.name;item.suffixText=s.text}}
 item.name=`${item.prefixName?item.prefixName+' ':''}${item.baseName}${item.suffixName?' '+item.suffixName:''}`;return item;
}
function makeRandomGear({source='enemy',crafted=false,forceSlot=null,rarity=null}={}){
 const lv=Math.max(1,game.level+(crafted?1:0)),boost=source==='boss'?.055:source==='quest'?.035:crafted?.025:0,rar=rarity||rarityForLevel(lv,boost);
 let slot=forceSlot;if(!slot){const r=Math.random();slot=r<.44?'weapon':r<.76?'armor':'trinket'}
 let item;if(slot==='weapon'){const base=WEAPON_BASES[irnd(WEAPON_BASES.length)];item=makeWeapon(base,lv,rar)}
 else if(slot==='armor'){const base=ARMOR_SETS[irnd(ARMOR_SETS.length)];item=makeArmor(base,lv,rar)}
 else item=makeTrinket(TRINKET_BASES[irnd(TRINKET_BASES.length)],lv,rar);
 return rollAffixes(item,crafted);
}
function dropGearNow(source='enemy',opts={}){
 if(game.loot)return false;game.loot=makeRandomGear({source,...opts});setTimeout(openLootOverlay,180);return true;
}

function equippedItems(){if(!game.player)return [];return [game.player.weapon,game.player.armorGear,...(game.player.trinkets||[])].filter(Boolean)}
function suffixCount(key){return equippedItems().filter(i=>i.suffixKey===key).length}
function aggregateMods(){
 const out={damage:0,hp:0,armor:0,move:0,spell:0,cdr:0,gold:0,materials:0,elite:0,ward:0};
 for(const t of game.player?.trinkets||[])if(t?.mods)for(const k of Object.keys(out))out[k]+=Number(t.mods[k]||0);
 const fortune=suffixCount('fortune');out.gold+=fortune*.11;out.materials+=fortune*.12;out.ward+=suffixCount('warding')*.06;return out;
}

function materialForEnemy(e){
 if(['golem','sentinel','stormknight','chainwarden','executioner'].includes(e.type))return 'iron';
 if(['wisp','mage','voideye','mirrormage','warpriest','stormcaller'].includes(e.type))return 'dust';
 if(['wolf','charger','serpent','drake','burrower'].includes(e.type))return 'hide';
 if(game.roomData?.biome==='frost'||['frostwitch'].includes(e.type))return 'frost';
 if(game.roomData?.biome==='volcanic'||['bomber','phoenixling'].includes(e.type))return 'ember';
 if(game.roomData?.biome==='crypt'||['skeleton','necro','vampire'].includes(e.type))return 'bone';
 return Math.random()<.55?'iron':'dust';
}
function addMaterial(key,count=1,quiet=false){
 ensureExpansionState();if(!MATERIALS[key])return;const bonus=aggregateMods().materials;count=Math.max(1,Math.round(count*(1+bonus)));game.materials[key]=(game.materials[key]||0)+count;
 if(!quiet){const m=MATERIALS[key];floatText(game.player.x,game.player.y-0.15,`+${count} ${m.name}`,m.color)}
 recordQuestEvent('gather',{material:key,count});updateExpansionHUD();
}
function spendMaterials(cost={}){for(const [k,v] of Object.entries(cost))if((game.materials[k]||0)<v)return false;for(const [k,v] of Object.entries(cost))game.materials[k]-=v;return true}
function materialCostText(cost){return Object.entries(cost).map(([k,v])=>`${MATERIALS[k].icon} ${v}`).join('  ')}

/* More dangerous outer world and more frequent safe villages. */
const _baseRoomDifficulty=roomDifficulty;
roomDifficulty=function(x,y){const d=Math.hypot(x,y);return Math.max(_baseRoomDifficulty(x,y),1+Math.floor(d*.88)+Math.floor(Math.max(0,d-7)*.16))};
const _baseIsTownCoord=isTownCoord;
isTownCoord=function(x,y){if(x===0&&y===0)return true;const d=Math.hypot(x,y);return d>2&&hash2(x,y,game.seed+71)>.945};
const _baseGetRoomData=getRoomData;
getRoomData=function(x,y){const r=_baseGetRoomData(x,y);augmentRoom(r);return r};
function augmentRoom(room){
 if(room._expanded)return room;room._expanded=true;const d=Math.hypot(room.x,room.y);room.regionTier=Math.max(1,1+Math.floor(d/5));room.villageLevel=room.town?Math.max(1,Math.min(5,1+Math.floor(d/6))):0;
 if(!room.town&&!room.boss&&room.difficulty>=4){const h=hash2(room.x,room.y,room.seed+4421);if(h>.80){const types=['Champion Patrol','Arcane Crossfire','Relentless Hunt','Execution Ground'];room.challenge=types[Math.floor(hash2(room.x,room.y,room.seed+4422)*types.length)]}}
 return room;
}
