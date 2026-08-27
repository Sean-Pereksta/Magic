'use strict';

const BIOME_PROP_SETS={
 meadow:['oak','oak','flowers','grass','boulder','shrineStone'], forest:['pine','oak','pine','mushroom','fern','stump','boulder'],
 ruins:['column','brokenWall','runeStone','brazier','boulder','deadTree'], swamp:['willow','reeds','mushroom','bogLight','stump','reeds'],
 frost:['snowPine','iceCrystal','snowRock','iceCrystal','frostShrub','snowPine'], desert:['cactus','sunRock','bones','dryShrub','obelisk','sunRock'],
 volcanic:['basalt','lavaVent','deadTree','emberCrystal','basalt','skullPile'], crypt:['grave','deadTree','purpleCrystal','grave','brazier','runeStone'],
 town:['house','house','market','lamp','fountain','banner','barrel','garden']
};
const BIOME_NAMES={
 meadow:['Greenwake Fields','Cloverrun','The Gentle Verge','Highgrass Crossing'],forest:['Whisperwood','Mossheart Grove','Oldroot Weald','Lantern Pines'],ruins:['Shattered March','Kingfall Ruins','The Fallen Arcade','Greywatch Remains'],swamp:['Mire of Lanterns','Drowned Fen','Glowmoss Mire','Stillwater Bog'],frost:['Glassfrost Reach','Whitewind Shelf','Rimeglass Hollow','Snowbound Expanse'],desert:['Sunscar Expanse','Amber Waste','Sundial Barrens','The Long Dune'],volcanic:['Cinder Crown','Ashen Rift','Redglass Caldera','Emberfall'],crypt:['Hollow Crypt','Moonless Graves','The Ossuary Roads','Blackbell Necropolis'],town:['Sunmere Haven','Bellflower Rest','Starbridge','Wayfarer Hold']
};

function biomeFor(x,y){
 if(x===0&&y===0)return 'town';
 const d=Math.hypot(x,y), h=hash2(Math.floor(x/2),Math.floor(y/2),game.seed);
 if(d>8&&h>.82)return 'volcanic';
 if(d>5&&h<.14)return 'crypt';
 if(h<.17)return 'meadow'; if(h<.34)return 'forest'; if(h<.49)return 'ruins'; if(h<.62)return 'swamp'; if(h<.76)return 'frost'; if(h<.88)return 'desert'; return 'volcanic';
}
function isTownCoord(x,y){if(x===0&&y===0)return true;const d=Math.hypot(x,y);return d>2&&hash2(x,y,game.seed+71)>.962}
function isBossCoord(x,y){const d=Math.hypot(x,y);if(d<4||isTownCoord(x,y))return false;return hash2(x,y,game.seed+1701)>.935 || ((Math.abs(x)+Math.abs(y))%9===0&&hash2(x,y,game.seed+4)>.72)}
function roomDifficulty(x,y){return Math.max(1,1+Math.floor(Math.hypot(x,y)*.72))}

function getRoomData(x,y){
 const key=roomKey(x,y); if(game.rooms[key])return game.rooms[key];
 const town=isTownCoord(x,y), boss=isBossCoord(x,y), biome=town?'town':biomeFor(x,y), diff=roomDifficulty(x,y);
 const names=BIOME_NAMES[biome], idx=Math.floor(hash2(x,y,game.seed+202)*names.length);
 const room={x,y,key,biome,town,boss,elite:!town&&!boss&&diff>3&&hash2(x,y,game.seed+881)>.84,difficulty:diff,name:town?(x===0&&y===0?'Sunmere Haven':names[idx]):names[idx],seen:false,cleared:town,scenery:[],deco:[],chests:[],seed:Math.floor(hash2(x,y,game.seed)*1e9)};
 generateScenery(room); game.rooms[key]=room; return room;
}

function seeded(room,salt){return hash2(room.x*19+salt,room.y*23-salt,room.seed+salt)}
function generateScenery(room){
 const set=BIOME_PROP_SETS[room.biome]||BIOME_PROP_SETS.meadow;
 let count=room.town?28:22+Math.floor(seeded(room,2)*18);
 for(let i=0;i<count;i++){
  let x=.8+seeded(room,20+i*5)*(ROOM_W-1.6), y=.8+seeded(room,21+i*5)*(ROOM_H-1.6);
  const edge=Math.min(x,ROOM_W-x,y,ROOM_H-y), center=Math.hypot(x-ROOM_W/2,y-ROOM_H/2);
  if(!room.town&&center<2.3&&i<10){x=x<ROOM_W/2?1.2:ROOM_W-1.2;y=y<ROOM_H/2?1.2:ROOM_H-1.2}
  if(room.town&&center<3.2&&i<18){const a=(i/18)*TAU;x=ROOM_W/2+Math.cos(a)*(4.2+seeded(room,300+i)*1.1);y=ROOM_H/2+Math.sin(a)*(3.3+seeded(room,600+i)*.8)}
  const type=set[Math.floor(seeded(room,70+i*7)*set.length)];
  room.scenery.push({x,y,type,scale:.72+seeded(room,90+i*11)*.65,phase:seeded(room,120+i)*TAU,depth:y+x*.01,edge});
 }
 const sparkleCount=10+Math.floor(seeded(room,818)*14);
 for(let i=0;i<sparkleCount;i++)room.deco.push({x:seeded(room,900+i*3)*ROOM_W,y:seeded(room,901+i*3)*ROOM_H,phase:seeded(room,902+i*3)*TAU,size:.5+seeded(room,903+i*3)*1.7});
 if(!room.town&&seeded(room,991)>.73)room.chests.push({x:2+seeded(room,992)*(ROOM_W-4),y:2+seeded(room,993)*(ROOM_H-4),opened:false});
}

function doorOpen(dir){return game.roomData?.cleared || game.roomData?.town || game.enemies.length===0}
function doorRect(dir){
 if(dir==='N')return {x:ROOM_W/2,y:.15}; if(dir==='S')return {x:ROOM_W/2,y:ROOM_H-.15}; if(dir==='W')return {x:.15,y:ROOM_H/2}; return {x:ROOM_W-.15,y:ROOM_H/2};
}
function tryRoomExit(){
 if(roomTransition||modalPause)return;
 const p=game.player, margin=.08;
 if(p.y<-margin&&Math.abs(p.x-ROOM_W/2)<1.8){if(doorOpen('N'))transitionRoom(0,-1,'N');else p.y=.15}
 else if(p.y>ROOM_H+margin&&Math.abs(p.x-ROOM_W/2)<1.8){if(doorOpen('S'))transitionRoom(0,1,'S');else p.y=ROOM_H-.15}
 else if(p.x<-margin&&Math.abs(p.y-ROOM_H/2)<1.8){if(doorOpen('W'))transitionRoom(-1,0,'W');else p.x=.15}
 else if(p.x>ROOM_W+margin&&Math.abs(p.y-ROOM_H/2)<1.8){if(doorOpen('E'))transitionRoom(1,0,'E');else p.x=ROOM_W-.15}
}
function transitionRoom(dx,dy,from){
 roomTransition=true;$('roomFade').style.opacity='1';saveGame();
 setTimeout(()=>{
  game.room.x+=dx;game.room.y+=dy;loadRoom();
  const p=game.player;
  if(from==='N'){p.x=ROOM_W/2;p.y=ROOM_H-.55}else if(from==='S'){p.x=ROOM_W/2;p.y=.55}else if(from==='W'){p.x=ROOM_W-.55;p.y=ROOM_H/2}else{p.x=.55;p.y=ROOM_H/2}
  $('roomFade').style.opacity='0';setTimeout(()=>roomTransition=false,220);
 },210);
}
function loadRoom(){
 const room=getRoomData(game.room.x,game.room.y);game.roomData=room;room.seen=true;
 game.enemies.length=0;game.projectiles.length=0;game.hazards.length=0;game.telegraphs.length=0;game.summons.length=0;game.interactables.length=0;
 if(room.town){
  game.interactables.push({type:'well',x:ROOM_W/2-1.3,y:ROOM_H/2+.6,label:'Healing Well'});
  game.interactables.push({type:'forge',x:ROOM_W/2+1.4,y:ROOM_H/2+.55,label:'Runeforge'});
  game.interactables.push({type:'seer',x:ROOM_W/2,y:ROOM_H/2-1.35,label:'Arcane Seer'});
 }else if(!room.cleared){spawnRoomEnemies(room)}
 for(const c of room.chests)if(!c.opened)game.interactables.push({type:'chest',x:c.x,y:c.y,label:'Arcane Cache',ref:c});
 ambientBurst(room.biome);updateHUD();
}
function ambientBurst(biome){
 const pal=biomePalette[biome]||biomePalette.meadow;
 for(let i=0;i<18;i++)game.particles.push({x:rnd(ROOM_W),y:rnd(ROOM_H),z:rnd(12,3),vx:rnd(.2,-.2),vy:rnd(.2,-.2),vz:rnd(.3,-.1),life:rnd(4,1),maxLife:4,size:rnd(3,1),color:pal.accent,soft:true,ambient:true});
}
function currentInteraction(){
 let best=null,bd=1.45;for(const o of game.interactables){const d=dist(game.player,o);if(d<bd){bd=d;best=o}}return best;
}
function interact(){
 const o=currentInteraction();if(!o)return toastMsg('Nothing nearby to use.');
 if(o.type==='chest'){o.ref.opened=true;game.interactables=game.interactables.filter(v=>v!==o);game.gold+=18+game.roomData.difficulty*4;burst(o.x,o.y,'#ffdd79',24,1.8);rollLoot(.5);toastMsg('Arcane cache opened!');updateHUD();saveGame();return}
 if(game.roomData.town)openTownPanel(o.type);
}
function markRoomCleared(){
 if(!game.roomData||game.roomData.cleared)return;game.roomData.cleared=true;burst(ROOM_W/2,ROOM_H/2,biomePalette[game.roomData.biome].accent,34,2.5);toastMsg(game.roomData.boss?'Boss defeated — the roads open.':'Room cleared — doors unlocked.');
 if(game.roomData.elite||game.roomData.boss||Math.random()<.16)rollLoot(game.roomData.boss?1:.6);saveGame();updateHUD();
}
