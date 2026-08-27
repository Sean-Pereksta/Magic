'use strict';

/*
 * Arcane Wilds navigation clarity layer.
 * Keeps the hero centered, turns room exits into readable isometric doorways,
 * and reserves the entrance side of every room as an enemy-free spawn zone.
 */
(()=>{
 const ENTRY_SPAWN_BUFFER=4.15;
 const ENTRY_PLAYER_CLEARANCE=3.0;
 const oppositeSide={N:'S',S:'N',E:'W',W:'E'};
 const baseWorldToScreen=worldToScreen;
 const baseSpawnEnemy=spawnEnemy;

 function currentEntrySide(){return game.arcaneEntrySide||null}
 function onEntrySide(p,side=currentEntrySide()){
  if(!side||!p)return false;
  if(side==='N')return p.y<ENTRY_SPAWN_BUFFER;
  if(side==='S')return p.y>ROOM_H-ENTRY_SPAWN_BUFFER;
  if(side==='W')return p.x<ENTRY_SPAWN_BUFFER;
  return p.x>ROOM_W-ENTRY_SPAWN_BUFFER;
 }
 function safeFromPlayer(p){return !game.player||Math.hypot(p.x-game.player.x,p.y-game.player.y)>=ENTRY_PLAYER_CLEARANCE}
 function legalSpawn(p){return p&&!onEntrySide(p)&&safeFromPlayer(p)}
 function fallbackSpawnForSide(side=currentEntrySide()){
  if(side==='N')return {x:rnd(ROOM_W-1.5,1.5),y:rnd(ROOM_H-1.4,ENTRY_SPAWN_BUFFER+.55)};
  if(side==='S')return {x:rnd(ROOM_W-1.5,1.5),y:rnd(ROOM_H-ENTRY_SPAWN_BUFFER-.55,1.4)};
  if(side==='W')return {x:rnd(ROOM_W-1.4,ENTRY_SPAWN_BUFFER+.55),y:rnd(ROOM_H-1.5,1.5)};
  if(side==='E')return {x:rnd(ROOM_W-ENTRY_SPAWN_BUFFER-.55,1.4),y:rnd(ROOM_H-1.5,1.5)};
  return {x:rnd(ROOM_W-1.2,1.2),y:rnd(ROOM_H-1.2,1.2)};
 }
 function chooseSafeSpawn(){
  for(let i=0;i<90;i++){
   const p={x:rnd(ROOM_W-1.2,1.2),y:rnd(ROOM_H-1.2,1.2)};
   if(legalSpawn(p))return p;
  }
  for(let i=0;i<30;i++){
   const p=fallbackSpawnForSide();
   if(legalSpawn(p))return p;
  }
  const side=currentEntrySide();
  if(side==='N')return {x:ROOM_W/2,y:ROOM_H-2};
  if(side==='S')return {x:ROOM_W/2,y:2};
  if(side==='W')return {x:ROOM_W-2,y:ROOM_H/2};
  if(side==='E')return {x:2,y:ROOM_H/2};
  return {x:ROOM_W/2+3.2,y:ROOM_H/2};
 }

 /* Lock world projection to the hero while preserving the existing screen-shake signal. */
 worldToScreen=function(x,y,z=0){
  const p=game.player;
  if(!p)return baseWorldToScreen(x,y,z);
  const roomCameraY=H*.46-((ROOM_W+ROOM_H)*TILE_H*.25);
  const shakeX=camera.x-W/2;
  const shakeY=camera.y-roomCameraY;
  return {
   x:W*.5+((x-y)-(p.x-p.y))*TILE_W*.5+shakeX,
   y:H*.5+((x+y)-(p.x+p.y))*TILE_H*.5-z+shakeY
  };
 };

 /* Record the actual destination entrance and place the hero before the room spawns. */
 transitionRoom=function(dx,dy,from){
  roomTransition=true;$('roomFade').style.opacity='1';saveGame();
  setTimeout(()=>{
   game.room.x+=dx;game.room.y+=dy;
   game.arcaneEntrySide=oppositeSide[from]||null;
   const p=game.player;
   if(from==='N'){p.x=ROOM_W/2;p.y=ROOM_H-.55}
   else if(from==='S'){p.x=ROOM_W/2;p.y=.55}
   else if(from==='W'){p.x=ROOM_W-.55;p.y=ROOM_H/2}
   else{p.x=.55;p.y=ROOM_H/2}
   loadRoom();
   $('roomFade').style.opacity='0';setTimeout(()=>roomTransition=false,220);
  },210);
 };

 /* All ordinary random spawns respect both the entrance band and the hero clearance. */
 randomEnemySpawn=function(){return chooseSafeSpawn()};

 /* Final safety net: explicit/summoned/wave spawns cannot appear in the entrance band either. */
 spawnEnemy=function(id,pos,elite=false,scaleOverride=1){
  const requested=pos?{x:clamp(pos.x,.7,ROOM_W-.7),y:clamp(pos.y,.7,ROOM_H-.7)}:null;
  const safe=legalSpawn(requested)?requested:chooseSafeSpawn();
  return baseSpawnEnemy(id,safe,elite,scaleOverride);
 };

 /* Reinforcement rifts use any edge except the wall the hero entered through. */
 if(typeof intensityWavePoints==='function'){
  intensityWavePoints=function(count){
   const pts=[];
   const edgeSides=['W','E','N','S'].filter(side=>side!==currentEntrySide());
   const side=edgeSides.length?edgeSides[irnd(edgeSides.length)]:['W','E','N','S'][irnd(4)];
   for(let i=0;i<count;i++){
    const lane=(i+1)/(count+1);
    let p;
    if(side==='W')p={x:.9+rnd(.55,-.25),y:1+lane*(ROOM_H-2)};
    else if(side==='E')p={x:ROOM_W-.9+rnd(.25,-.55),y:1+lane*(ROOM_H-2)};
    else if(side==='N')p={x:1+lane*(ROOM_W-2),y:.9+rnd(.55,-.25)};
    else p={x:1+lane*(ROOM_W-2),y:ROOM_H-.9+rnd(.25,-.55)};
    if(!legalSpawn(p))p=chooseSafeSpawn();
    pts.push(intensityClampPoint(p));
   }
   return pts;
  };
 }

 function doorPoint(dir,lateral,z=0){
  const d=doorRect(dir),alongNS=dir==='N'||dir==='S';
  return worldToScreen(d.x+(alongNS?lateral:0),d.y+(alongNS?0:lateral),z);
 }
 function quad(a,b,c,d,fill,stroke=null,width=1){
  ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.lineTo(c.x,c.y);ctx.lineTo(d.x,d.y);ctx.closePath();
  ctx.fillStyle=fill;ctx.fill();if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=width;ctx.stroke()}
 }
 function doorwayBar(dir,a,b,z0,z1,fill,stroke){
  quad(doorPoint(dir,a,z0),doorPoint(dir,b,z0),doorPoint(dir,b,z1),doorPoint(dir,a,z1),fill,stroke,1.2);
 }
 function doorwayLine(dir,a,z0,b,z1,color,width=1.5){
  const p=doorPoint(dir,a,z0),q=doorPoint(dir,b,z1);ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(q.x,q.y);ctx.stroke();
 }

 /* Replace glowing floor strokes with upright, framed isometric doorways. */
 drawDoors=function(room,pal){
  for(const dir of ['N','E','S','W']){
   const open=doorOpen(dir),accent=open?pal.accent:'#ff6672';
   const outerL=-1.38,innerL=-1.04,innerR=1.04,outerR=1.38;
   const baseZ=2,lintelBottom=57,topZ=68;
   const portalA=doorPoint(dir,innerL,baseZ),portalB=doorPoint(dir,innerR,baseZ),portalC=doorPoint(dir,innerR,lintelBottom),portalD=doorPoint(dir,innerL,lintelBottom);
   ctx.save();ctx.lineJoin='round';ctx.lineCap='round';ctx.shadowColor=accent;ctx.shadowBlur=open?11:7;
   quad(portalA,portalB,portalC,portalD,open?'rgba(2,8,13,.88)':'rgba(72,12,20,.92)',colorAlpha(accent,.72),2);
   ctx.shadowBlur=0;
   const frameDark=open?'#34434c':'#5c252b',frameMid=open?'#60717a':'#8f3a43',frameLight=open?'#94aab2':'#c85c66';
   doorwayBar(dir,outerL,innerL,0,topZ,frameDark,colorAlpha(accent,.5));
   doorwayBar(dir,innerR,outerR,0,topZ,frameMid,colorAlpha(accent,.55));
   doorwayBar(dir,outerL,outerR,lintelBottom,topZ,frameDark,colorAlpha(accent,.65));
   doorwayLine(dir,innerL,5,innerL,lintelBottom-2,frameLight,1.4);
   doorwayLine(dir,innerR,5,innerR,lintelBottom-2,colorAlpha('#ffffff',.32),1.1);
   doorwayLine(dir,outerL,topZ,outerR,topZ,colorAlpha(accent,.8),1.4);
   doorwayBar(dir,-1.48,1.48,0,5,open?colorAlpha(pal.accent,.22):'rgba(120,28,35,.42)',colorAlpha(accent,.62));
   if(open){
    doorwayLine(dir,-.82,10,.82,10,colorAlpha(accent,.3),1);
    doorwayLine(dir,-.82,48,.82,48,colorAlpha(accent,.2),1);
    const glow=doorPoint(dir,0,31);ctx.fillStyle=colorAlpha(accent,.16);ctx.beginPath();ctx.arc(glow.x,glow.y,12,0,TAU);ctx.fill();
   }else{
    doorwayBar(dir,-.93,.93,8,53,'rgba(107,25,34,.78)',colorAlpha('#ff9aa2',.36));
    doorwayLine(dir,-.82,15,.82,46,'rgba(255,171,176,.42)',3);
    doorwayLine(dir,-.82,46,.82,15,'rgba(255,171,176,.42)',3);
    const label=doorPoint(dir,0,73);ctx.fillStyle='#ff9da5';ctx.font='900 9px system-ui';ctx.textAlign='center';ctx.fillText('SEALED',label.x,label.y);
   }
   ctx.restore();
  }
 };
})();
