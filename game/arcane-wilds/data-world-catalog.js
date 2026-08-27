const WEAPON_BASES=[
 {name:'Ashwood Staff',icon:'🪄',type:'staff',damage:1,attack:1,range:8.5,speed:9,color:'#ff9b63'},
 {name:'Moonbow',icon:'🏹',type:'bow',damage:.85,attack:1.35,range:10,speed:12,color:'#86c8ff'},
 {name:'Runeblade',icon:'🗡️',type:'blade',damage:1.35,attack:.78,range:6.5,speed:8,color:'#cf9cff'},
 {name:'Storm Scepter',icon:'⚡',type:'scepter',damage:.92,attack:1.15,range:8.2,speed:10,color:'#78e8ff'},
 {name:'Sunlance',icon:'🔱',type:'spear',damage:1.18,attack:.92,range:11,speed:14,color:'#ffd66c'},
 {name:'Void Chakram',icon:'🌙',type:'chakram',damage:1.1,attack:1.02,range:9,speed:10,color:'#b585ff'}
];
const ARMOR_SETS=[
 {name:'Wayfarer Cloth',icon:'🧥',hp:1,move:1,armor:0,color:'#5c7fa5',trim:'#d8e8f8',helm:'hood',aura:null},
 {name:'Emberguard',icon:'🔥',hp:1.16,move:.98,armor:.07,color:'#8e3d32',trim:'#ffbd6c',helm:'crest',aura:'ember'},
 {name:'Frostbound',icon:'❄️',hp:1.12,move:1.03,armor:.05,color:'#3d6f8e',trim:'#bdefff',helm:'horns',aura:'frost'},
 {name:'Verdant Oath',icon:'🌿',hp:1.2,move:.98,armor:.08,color:'#3d7450',trim:'#91d57e',helm:'antler',aura:'leaf'},
 {name:'Nightglass',icon:'🌙',hp:1.08,move:1.08,armor:.04,color:'#403e6e',trim:'#bb9dff',helm:'veil',aura:'void'},
 {name:'Sunforged',icon:'☀️',hp:1.28,move:.94,armor:.12,color:'#896326',trim:'#ffe29a',helm:'crown',aura:'sun'},
 {name:'Stormcaller',icon:'⛈️',hp:1.1,move:1.1,armor:.05,color:'#315f75',trim:'#78e8ff',helm:'wing',aura:'storm'},
 {name:'Bone Regent',icon:'💀',hp:1.34,move:.91,armor:.14,color:'#5a554f',trim:'#e6dfcb',helm:'skull',aura:'soul'}
];
const gearRarityMult={Common:1,Uncommon:1.12,Rare:1.26,Epic:1.45,Legendary:1.72};

const ENEMY_TYPES={
 slime:{name:'Moss Slime',icon:'●',biomes:['meadow','swamp'],min:1,hp:28,speed:1.35,damage:8,r:0.34,ai:'melee',color:'#63bd6c',xp:8},
 wolf:{name:'Briar Wolf',icon:'◆',biomes:['meadow','forest'],min:1,hp:34,speed:2.05,damage:10,r:.32,ai:'melee',color:'#769769',xp:10},
 bandit:{name:'Road Bandit',icon:'▲',biomes:['meadow','ruins','desert'],min:1,hp:40,speed:1.45,damage:9,r:.34,ai:'ranged',range:6.2,color:'#b98f64',proj:'#e8ca93',xp:12},
 beetle:{name:'Iron Beetle',icon:'⬢',biomes:['ruins','desert'],min:2,hp:64,speed:.9,damage:12,r:.42,ai:'shield',color:'#7d6652',xp:15},
 wisp:{name:'Wild Wisp',icon:'✦',biomes:['forest','swamp','frost'],min:2,hp:26,speed:1.7,damage:9,r:.27,ai:'orbiter',color:'#8ce6ce',proj:'#bafbef',xp:12},
 archer:{name:'Ash Archer',icon:'➤',biomes:['forest','ruins'],min:3,hp:38,speed:1.25,damage:12,r:.31,ai:'sniper',range:9,color:'#ca9d79',proj:'#ffcb8e',xp:15},
 charger:{name:'Horned Charger',icon:'⬟',biomes:['meadow','desert','frost'],min:3,hp:72,speed:1.1,damage:18,r:.46,ai:'charger',color:'#a56252',xp:20},
 shaman:{name:'Mire Shaman',icon:'♠',biomes:['swamp','forest'],min:4,hp:58,speed:1.0,damage:9,r:.34,ai:'bomber',color:'#7b9c58',proj:'#b6e56f',xp:21},
 skeleton:{name:'Crypt Blade',icon:'†',biomes:['crypt','ruins'],min:4,hp:54,speed:1.6,damage:13,r:.34,ai:'melee',color:'#c7c0a8',xp:18},
 mage:{name:'Rift Adept',icon:'✧',biomes:['ruins','crypt'],min:5,hp:48,speed:1.15,damage:13,r:.32,ai:'spread',range:7.5,color:'#8567c9',proj:'#be8cff',xp:22},
 serpent:{name:'Glass Serpent',icon:'≈',biomes:['desert','frost'],min:5,hp:50,speed:2.2,damage:11,r:.29,ai:'skirmish',range:5.8,color:'#77b9b0',proj:'#8ef2df',xp:23},
 assassin:{name:'Veil Assassin',icon:'✕',biomes:['ruins','crypt','town'],min:6,hp:60,speed:2.1,damage:17,r:.31,ai:'blinker',color:'#7e5b96',xp:26},
 bomber:{name:'Cinder Lobber',icon:'●',biomes:['volcanic','desert'],min:6,hp:70,speed:.9,damage:20,r:.38,ai:'bomber',color:'#b54d3f',proj:'#ff7659',xp:28},
 necro:{name:'Bone Caller',icon:'☠',biomes:['crypt'],min:7,hp:85,speed:.9,damage:10,r:.38,ai:'summoner',color:'#8c82a0',proj:'#c7a5ff',xp:34},
 sentinel:{name:'Rune Sentinel',icon:'⬣',biomes:['ruins','frost'],min:7,hp:120,speed:.72,damage:16,r:.48,ai:'turret',color:'#69859d',proj:'#89d6ff',xp:36},
 harpy:{name:'Storm Harpy',icon:'⌁',biomes:['frost','volcanic'],min:8,hp:68,speed:1.9,damage:14,r:.34,ai:'diver',color:'#5f8199',proj:'#95e7ff',xp:31},
 cultist:{name:'Sunless Cultist',icon:'⌃',biomes:['crypt','volcanic'],min:8,hp:75,speed:1.15,damage:15,r:.35,ai:'beam',color:'#724c73',proj:'#e45bdd',xp:35},
 golem:{name:'Basalt Golem',icon:'■',biomes:['volcanic','ruins'],min:9,hp:190,speed:.62,damage:23,r:.58,ai:'shockwave',color:'#66504d',xp:48},
 frostwitch:{name:'Frost Witch',icon:'✢',biomes:['frost'],min:9,hp:92,speed:1.05,damage:15,r:.36,ai:'frostmage',color:'#6998b3',proj:'#bfefff',xp:40},
 vampire:{name:'Night Drinker',icon:'▼',biomes:['crypt','forest'],min:10,hp:105,speed:1.75,damage:18,r:.36,ai:'vampire',color:'#7b354e',xp:44},
 phoenixling:{name:'Cinderwing',icon:'⌁',biomes:['volcanic'],min:11,hp:90,speed:2.0,damage:17,r:.34,ai:'phoenix',color:'#d06443',proj:'#ffb05e',xp:42},
 stormknight:{name:'Storm Knight',icon:'♜',biomes:['frost','ruins'],min:12,hp:150,speed:1.25,damage:20,r:.43,ai:'stormknight',color:'#52738c',proj:'#79dcff',xp:52},
 voideye:{name:'Void Eye',icon:'◉',biomes:['crypt','volcanic'],min:13,hp:118,speed:1.0,damage:18,r:.42,ai:'voideye',color:'#633b86',proj:'#c168ff',xp:54},
 drake:{name:'Ash Drake',icon:'◇',biomes:['volcanic','desert'],min:14,hp:210,speed:1.4,damage:24,r:.56,ai:'drake',color:'#9b4434',proj:'#ff7b55',xp:68}
};

const BOSSES=[
 {base:'golem',name:'The Stonebell Colossus',color:'#8c6c62',hp:5.2,damage:1.25,ai:'bossGolem'},
 {base:'necro',name:'Morrow, Bone Regent',color:'#aa88cf',hp:4.5,damage:1.3,ai:'bossNecro'},
 {base:'drake',name:'Vaelith the Cinder Sky',color:'#cf553e',hp:4.8,damage:1.35,ai:'bossDrake'},
 {base:'voideye',name:'The Watching Dark',color:'#8e4cc5',hp:5.0,damage:1.3,ai:'bossVoid'}
];

const game={
 seed:Math.floor(Math.random()*1e9),room:{x:0,y:0},roomData:null,rooms:{},level:1,xp:0,xpNeed:80,gold:0,kills:0,
 player:null,enemies:[],projectiles:[],effects:[],particles:[],hazards:[],telegraphs:[],loot:null,interactables:[],summons:[],
 pendingLevelUps:0,selectedSpell:null,lastSave:0
};
const camera={x:W/2,y:H/2};

function newPlayer(){
 return {x:ROOM_W/2,y:ROOM_H/2,hp:100,maxHp:100,baseMaxHp:100,speed:3.35,baseSpeed:3.35,armor:0,r:.34,facing:{x:1,y:0},attackTimer:0,dodgeCd:0,dodgeTime:0,invuln:0,shield:0,shieldTime:0,haste:0,tailwind:0,
  activeSpells:['firebolt'],unlocked:['firebolt'],spellState:{},upgrades:{},weapon:makeWeapon(WEAPON_BASES[0],1,'Common'),armorGear:makeArmor(ARMOR_SETS[0],1,'Common')};
}
function makeWeapon(base,level,rarity){const m=gearRarityMult[rarity]||1;return {...base,slot:'weapon',rarity,level,power:(8+level*2.3)*m,forge:0,id:Math.random().toString(36).slice(2)}}
function makeArmor(base,level,rarity){const m=gearRarityMult[rarity]||1;return {...base,slot:'armor',rarity,level,hpBonus:(10+level*3.2)*m,armorBonus:(base.armor+(rarityRank[rarity]||0)*.012),id:Math.random().toString(36).slice(2)}}

function resize(){dpr=Math.min(devicePixelRatio||1,2);W=innerWidth;H=innerHeight;canvas.width=Math.floor(W*dpr);canvas.height=Math.floor(H*dpr);canvas.style.width=W+'px';canvas.style.height=H+'px';ctx.setTransform(dpr,0,0,dpr,0,0);mouse.x=W/2;mouse.y=H/2}
addEventListener('resize',resize);resize();