'use strict';

const canvas = document.getElementById('game');
const ctx = canvas.getContext('2d', { alpha:false });
const $ = id => document.getElementById(id);
const TAU = Math.PI*2;
const TILE_W = 96, TILE_H = 48;
const ROOM_W = 18, ROOM_H = 14;
const WORLD_PAD = .7;
const SAVE_KEY = 'arcaneWildsSaveV1';
const isTouch = matchMedia('(pointer:coarse)').matches || navigator.maxTouchPoints > 0;
let dpr=1, W=innerWidth, H=innerHeight, last=performance.now(), elapsed=0;
let running=false, paused=true, modalPause=false, roomTransition=false;
let shake=0, toastTimer=0;
const keys = new Set();
const mouse = {x:W/2,y:H/2,active:false};
const moveStick={x:0,y:0,active:false,pointer:null}, aimStick={x:0,y:0,active:false,pointer:null};

const rarityColors={Common:'#dbe7f5',Uncommon:'#63e69f',Rare:'#59a8ff',Epic:'#bf79ff',Legendary:'#ffcc61'};
const rarityMin={Common:1,Uncommon:3,Rare:7,Epic:12,Legendary:18};
const rarityRank={Common:0,Uncommon:1,Rare:2,Epic:3,Legendary:4};
const biomePalette={
  meadow:{name:'Emerald Verge',floor:'#263f35',floor2:'#2e4a3b',edge:'#15261f',accent:'#78ca86',sky:'#08120f'},
  forest:{name:'Whisperwood',floor:'#1f382e',floor2:'#274237',edge:'#11251d',accent:'#57b574',sky:'#06100b'},
  ruins:{name:'Shattered March',floor:'#38383c',floor2:'#44434a',edge:'#202025',accent:'#a89bcf',sky:'#0c0c12'},
  swamp:{name:'Mire of Lanterns',floor:'#2e3b30',floor2:'#374536',edge:'#1a261a',accent:'#a9c75f',sky:'#0c120a'},
  frost:{name:'Glassfrost Reach',floor:'#3b5260',floor2:'#476372',edge:'#20313d',accent:'#9de4ff',sky:'#08141b'},
  desert:{name:'Sunscar Expanse',floor:'#5b4931',floor2:'#6a5639',edge:'#30261b',accent:'#f4c66b',sky:'#1a1007'},
  volcanic:{name:'Cinder Crown',floor:'#3f302e',floor2:'#4c3733',edge:'#221717',accent:'#ff7659',sky:'#160806'},
  crypt:{name:'Hollow Crypt',floor:'#2e2d38',floor2:'#383646',edge:'#191820',accent:'#bb83ff',sky:'#0b0911'},
  town:{name:'Sunmere Haven',floor:'#3b4136',floor2:'#474d3f',edge:'#20241c',accent:'#f7d477',sky:'#0d100a'}
};

function clamp(v,a,b){return Math.max(a,Math.min(b,v))}
function lerp(a,b,t){return a+(b-a)*t}
function rnd(a=1,b=0){return b+Math.random()*(a-b)}
function irnd(a,b=0){return Math.floor(rnd(a,b))}
function dist(a,b){return Math.hypot(a.x-b.x,a.y-b.y)}
function norm(x,y){const l=Math.hypot(x,y)||1;return {x:x/l,y:y/l}}
function hash2(x,y,s=0){let h=(x*374761393+y*668265263+s*1442695041)|0;h=(h^(h>>>13))*1274126177;return ((h^(h>>>16))>>>0)/4294967295}
function roomKey(x,y){return `${x},${y}`}
function screenToWorldDir(sx,sy){return norm(sy+sx,sy-sx)}
function worldToScreen(x,y,z=0){return {x:(x-y)*TILE_W*.5 + camera.x, y:(x+y)*TILE_H*.5 + camera.y - z}}
function colorAlpha(hex,a){if(hex.startsWith('#')){const n=parseInt(hex.slice(1),16);return `rgba(${(n>>16)&255},${(n>>8)&255},${n&255},${a})`}return hex}
function capitalize(s){return s.charAt(0).toUpperCase()+s.slice(1)}

const SPELLS = {
 firebolt:{name:'Ember Orb',icon:'🔥',rarity:'Common',desc:'Launch a blazing orb that explodes on impact.',cooldown:3.2,damage:32,cast:'firebolt'},
 frostnova:{name:'Frost Nova',icon:'❄️',rarity:'Common',desc:'Burst cold around you, damaging and slowing nearby foes.',cooldown:5.6,damage:25,cast:'frostnova'},
 thorns:{name:'Thorn Halo',icon:'🌿',rarity:'Common',desc:'Grow a ring of thorns that erupts outward.',cooldown:4.8,damage:22,cast:'thorns'},
 arcaneMissiles:{name:'Arcane Missiles',icon:'💠',rarity:'Common',desc:'Fire seeking arcane bolts at nearby enemies.',cooldown:4.2,damage:15,cast:'missiles'},
 gust:{name:'Gale Burst',icon:'🌪️',rarity:'Common',desc:'Blast a cone of wind that knocks enemies away.',cooldown:4.4,damage:18,cast:'gust'},
 ward:{name:'Prism Ward',icon:'🛡️',rarity:'Common',desc:'Gain a short shield that absorbs incoming damage.',cooldown:9,damage:0,cast:'ward'},
 chain:{name:'Chain Lightning',icon:'⚡',rarity:'Uncommon',desc:'Lightning jumps between several enemies.',cooldown:5.2,damage:27,cast:'chain'},
 poison:{name:'Venom Garden',icon:'☠️',rarity:'Uncommon',desc:'Plant a toxic bloom that pulses damage in an area.',cooldown:6.5,damage:13,cast:'poison'},
 chakram:{name:'Astral Chakram',icon:'🌙',rarity:'Uncommon',desc:'Throw a crescent blade that returns through enemies.',cooldown:4.6,damage:24,cast:'chakram'},
 spirits:{name:'Spirit Wisps',icon:'🧚',rarity:'Uncommon',desc:'Summon orbiting wisps that seek targets for a time.',cooldown:9.5,damage:12,cast:'spirits'},
 quake:{name:'Stonewake',icon:'🪨',rarity:'Uncommon',desc:'Send a cracking shockwave in your aimed direction.',cooldown:5.8,damage:34,cast:'quake'},
 meteor:{name:'Meteor Sigil',icon:'☄️',rarity:'Rare',desc:'Mark a location; a meteor crashes there after a warning.',cooldown:8,damage:70,cast:'meteor'},
 voidrift:{name:'Void Rift',icon:'🕳️',rarity:'Rare',desc:'Open a rift that drags foes inward and tears them apart.',cooldown:10,damage:12,cast:'voidrift'},
 icelance:{name:'Glacial Lance',icon:'🔷',rarity:'Rare',desc:'Fire a piercing lance that freezes wounded enemies.',cooldown:5.4,damage:48,cast:'icelance'},
 soulflame:{name:'Soulflame',icon:'🟣',rarity:'Rare',desc:'Ignite a cursed flame that leaps when its victim dies.',cooldown:7.2,damage:42,cast:'soulflame'},
 tempest:{name:'Tempest Crown',icon:'⛈️',rarity:'Epic',desc:'Call a moving storm that repeatedly strikes enemies.',cooldown:12,damage:24,cast:'tempest'},
 timestop:{name:'Chrono Field',icon:'⏳',rarity:'Epic',desc:'Create a field that drastically slows enemies and projectiles.',cooldown:14,damage:0,cast:'timestop'},
 phoenix:{name:'Phoenix Arc',icon:'🦅',rarity:'Epic',desc:'A blazing phoenix sweeps across the room and returns.',cooldown:11,damage:65,cast:'phoenix'},
 starfall:{name:'Starfall',icon:'🌠',rarity:'Legendary',desc:'Rain a constellation of explosive stars across the battlefield.',cooldown:16,damage:48,cast:'starfall'},
 singularity:{name:'World Singularity',icon:'🌌',rarity:'Legendary',desc:'Create a massive gravity well that ends in a violent collapse.',cooldown:18,damage:90,cast:'singularity'}
};